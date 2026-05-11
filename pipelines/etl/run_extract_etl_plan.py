from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from pipelines.crawl.extract_source import extract_source
from pipelines.etl.extract_gate import evaluate_extract_gate, write_extract_gate_report
from pipelines.etl.processed_frontmatter_to_jsonl import process_directory as process_frontmatter_directory
from pipelines.etl.source_groups import (
    get_group_source_ids,
    should_default_extract_source,
    should_reconcile_source,
    source_adapter,
    unique_source_ids,
)
from pipelines.etl.vien_dinh_duong_etl import run_partitioned_etl as run_vien_dinh_duong_partitioned_etl
from pipelines.etl.vmj_ojs_etl import run_partitioned_etl as run_vmj_ojs_partitioned_etl
from pipelines.etl.vn.vn_txt_to_jsonl import process_directory as process_vn_directory
from services.utils.data_paths import preferred_processed_dir, source_records_path
from tools.promote_etl_sources_to_data_proceed import promote_source


def _default_actions_for_group(group_name: str) -> dict[str, bool]:
    if group_name == "group1":
        return {"reconcile": False, "extract": False, "etl": True, "promote": True}
    if group_name == "group2":
        return {"reconcile": True, "extract": True, "etl": True, "promote": True}
    if group_name == "group3":
        return {"reconcile": False, "extract": True, "etl": True, "promote": True}
    if group_name == "group4":
        return {"reconcile": True, "extract": True, "etl": False, "promote": False}
    return {"reconcile": False, "extract": False, "etl": False, "promote": False}


def _etl_source(source_id: str, *, max_files: int = 0, dry_run: bool = False) -> dict[str, Any]:
    adapter = source_adapter(source_id)
    source_dir = preferred_processed_dir(source_id)
    output_path = source_records_path(source_id)

    if adapter == "existing_records":
        return {
            "source_id": source_id,
            "adapter": adapter,
            "records_path": str(output_path),
            "records_exists": output_path.exists(),
            "reused": True,
        }

    if adapter == "frontmatter_text":
        return process_frontmatter_directory(
            source_id=source_id,
            source_dir=source_dir,
            output_path=output_path,
            max_files=max_files or None,
            dry_run=dry_run,
        )

    if adapter == "vien_dinh_duong_partitioned":
        return run_vien_dinh_duong_partitioned_etl(dry_run=dry_run, max_assets=max_files)

    if adapter == "vmj_ojs_partitioned":
        return run_vmj_ojs_partitioned_etl(dry_run=dry_run, max_assets=max_files)

    return process_vn_directory(
        source_dir=str(source_dir),
        output_path=str(output_path),
        source_id=source_id,
        max_files=max_files or None,
        dry_run=dry_run,
        verbose=False,
    )


def run_group(
    *,
    group_name: str,
    source_ids: list[str] | None = None,
    reconcile: bool | None = None,
    extract: bool | None = None,
    etl: bool | None = None,
    promote: bool | None = None,
    allow_gate_fail: bool = False,
    max_files: int = 0,
    dry_run: bool = False,
) -> dict[str, Any]:
    defaults = _default_actions_for_group(group_name)
    explicit_extract = extract is not None
    actions = {
        "reconcile": defaults["reconcile"] if reconcile is None else reconcile,
        "extract": defaults["extract"] if extract is None else extract,
        "etl": defaults["etl"] if etl is None else etl,
        "promote": defaults["promote"] if promote is None else promote,
    }
    planned_source_ids = list(get_group_source_ids(group_name))
    if source_ids:
        planned_source_ids.extend(source_ids)
    resolved_source_ids = unique_source_ids(planned_source_ids)

    source_reports: list[dict[str, Any]] = []
    for source_id in resolved_source_ids:
        source_report: dict[str, Any] = {
            "source_id": source_id,
            "actions": {},
        }

        if actions["reconcile"] and should_reconcile_source(source_id):
            source_report["actions"]["reconcile"] = extract_source(source_id, reconcile_only=True)

        if actions["extract"]:
            if not explicit_extract and not should_default_extract_source(group_name, source_id):
                source_report["actions"]["extract"] = {
                    "source_id": source_id,
                    "skipped": True,
                    "reason": "complex_reconcile_only_default",
                }
            else:
                source_report["actions"]["extract"] = extract_source(source_id)

        gate = write_extract_gate_report(source_id)
        source_report["gate"] = gate

        if actions["etl"]:
            if gate["gate_passed"] or gate.get("etl_done_only_allowed") or allow_gate_fail:
                source_report["actions"]["etl"] = _etl_source(source_id, max_files=max_files, dry_run=dry_run)
            else:
                source_report["actions"]["etl"] = {
                    "source_id": source_id,
                    "skipped": True,
                    "reason": f"extract_gate:{gate['gate_reason']}",
                }

        if actions["promote"]:
            if dry_run:
                source_report["actions"]["promote"] = {
                    "source_id": source_id,
                    "skipped": True,
                    "reason": "dry_run",
                }
            else:
                source_report["actions"]["promote"] = promote_source(source_id, allow_gate_fail=allow_gate_fail)

        source_reports.append(source_report)

    return {
        "group": group_name,
        "source_ids": list(resolved_source_ids),
        "actions": actions,
        "allow_gate_fail": allow_gate_fail,
        "dry_run": dry_run,
        "sources": source_reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run grouped extract/ETL/promotion actions following the staged source plan.")
    parser.add_argument("--group", required=True, choices=("group1", "group2", "group3", "group4", "group5", "etl_ready", "all_extract_planned"))
    parser.add_argument("--source-id", action="append", default=[], dest="source_ids")
    parser.add_argument("--reconcile", action="store_true")
    parser.add_argument("--extract", action="store_true")
    parser.add_argument("--etl", action="store_true")
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--allow-gate-fail", action="store_true")
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    explicit_actions = any((args.reconcile, args.extract, args.etl, args.promote))
    report = run_group(
        group_name=args.group,
        source_ids=args.source_ids,
        reconcile=args.reconcile if explicit_actions else None,
        extract=args.extract if explicit_actions else None,
        etl=args.etl if explicit_actions else None,
        promote=args.promote if explicit_actions else None,
        allow_gate_fail=args.allow_gate_fail,
        max_files=args.max_files,
        dry_run=args.dry_run,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
