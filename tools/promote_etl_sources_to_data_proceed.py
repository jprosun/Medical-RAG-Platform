from __future__ import annotations

import argparse
import json
import shutil
import sys
from hashlib import sha1
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from services.utils.data_paths import (
    KNOWN_SOURCE_IDS,
    data_proceed_processed_dir,
    data_proceed_records_path,
    data_proceed_summary_path,
    ensure_rag_data_layout,
    source_records_path,
)
from pipelines.etl.extract_gate import evaluate_extract_gate
from pipelines.etl.source_groups import GROUP_1_ETL_READY, GROUP_2_RECONCILE_THEN_ETL, get_group_source_ids


STABLE_SOURCE_IDS = GROUP_1_ETL_READY + GROUP_2_RECONCILE_THEN_ETL


def _iter_jsonl(path: Path):
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            yield json.loads(line)


def _safe_name(processed_path: str) -> str:
    return Path(processed_path).name


def _synthetic_processed_name(source_id: str, record: dict[str, Any]) -> str:
    doc_id = str(record.get("doc_id", "")).strip() or "doc"
    title = str(record.get("title", "")).strip()
    safe_title = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in title)[:60].strip("_")
    digest = sha1(f"{source_id}:{doc_id}:{title}".encode("utf-8")).hexdigest()[:10]
    if safe_title:
        return f"{source_id}__{safe_title}__{digest}.txt"
    return f"{source_id}__{doc_id}__{digest}.txt"


def _write_synthetic_processed(target_processed: Path, source_id: str, record: dict[str, Any]) -> Path | None:
    body = str(record.get("body", "")).strip()
    if len(body) < 40:
        return None

    title = str(record.get("title", "")).strip() or str(record.get("doc_id", "")).strip() or source_id
    source_url = str(record.get("source_url", "")).strip()
    out_path = target_processed / _synthetic_processed_name(source_id, record)
    frontmatter = [
        f"source_id: {source_id}",
        f"title: {title}",
    ]
    if source_url:
        frontmatter.append(f"source_url: {source_url}")
    out_text = "---\n" + "\n".join(frontmatter) + "\n---\n\n" + body + "\n"
    out_path.write_text(out_text, encoding="utf-8")
    return out_path


def promote_source(source_id: str, *, allow_gate_fail: bool = False) -> dict[str, Any]:
    if source_id not in KNOWN_SOURCE_IDS:
        raise ValueError(f"Unknown source_id: {source_id}")

    ensure_rag_data_layout(source_ids=[source_id])
    source_records = source_records_path(source_id)
    target_records = data_proceed_records_path(source_id)
    target_processed = data_proceed_processed_dir(source_id)
    target_summary = data_proceed_summary_path(source_id)

    target_processed.mkdir(parents=True, exist_ok=True)
    target_records.parent.mkdir(parents=True, exist_ok=True)
    gate = evaluate_extract_gate(source_id)

    if not source_records.exists():
        report = {
            "source_id": source_id,
            "records_exists": False,
            "gate": gate,
            "records_promoted": 0,
            "processed_promoted": 0,
            "synthetic_processed": 0,
            "missing_processed": 0,
        }
        target_summary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report

    if not gate["gate_passed"] and not gate.get("etl_done_only_allowed") and not allow_gate_fail:
        if target_records.exists():
            target_records.unlink()
        for stale in target_processed.iterdir():
            if stale.is_file():
                stale.unlink()
        report = {
            "source_id": source_id,
            "records_exists": True,
            "gate": gate,
            "blocked_by_gate": True,
            "records_promoted": 0,
            "processed_promoted": 0,
            "synthetic_processed": 0,
            "missing_processed": 0,
        }
        target_summary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report

    shutil.copy2(source_records, target_records)

    for stale in target_processed.iterdir():
        if stale.is_file():
            stale.unlink()

    seen_processed: set[str] = set()
    promoted = 0
    synthetic = 0
    missing = 0
    for record in _iter_jsonl(source_records):
        processed_path = str(record.get("processed_path", "")).strip()
        if not processed_path or processed_path in seen_processed:
            out_path = _write_synthetic_processed(target_processed, source_id, record)
            if out_path is not None:
                synthetic += 1
            continue
        seen_processed.add(processed_path)
        source_processed = Path(processed_path)
        if not source_processed.is_absolute():
            source_processed = REPO_ROOT / processed_path
        if not source_processed.exists() or not source_processed.is_file():
            missing += 1
            out_path = _write_synthetic_processed(target_processed, source_id, record)
            if out_path is not None:
                synthetic += 1
            continue
        shutil.copy2(source_processed, target_processed / _safe_name(processed_path))
        promoted += 1

    report = {
        "source_id": source_id,
        "records_exists": True,
        "gate": gate,
        "blocked_by_gate": False,
        "records_promoted": sum(1 for _ in _iter_jsonl(source_records)),
        "processed_promoted": promoted,
        "synthetic_processed": synthetic,
        "missing_processed": missing,
        "records_path": str(target_records),
        "processed_dir": str(target_processed),
    }
    target_summary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote ETL-ready sources into rag-data/data_proceed for easy observation.")
    parser.add_argument("--source-id", action="append", dest="source_ids", default=[])
    parser.add_argument("--group", choices=("group1", "group2", "group3", "group4", "group5", "etl_ready"))
    parser.add_argument("--stable", action="store_true", help="Promote the current stable source set.")
    parser.add_argument("--allow-gate-fail", action="store_true", help="Allow promotion even if extract gate is not green.")
    args = parser.parse_args()

    source_ids = list(args.source_ids)
    if args.group:
        for source_id in get_group_source_ids(args.group):
            if source_id not in source_ids:
                source_ids.append(source_id)
    if args.stable:
        for source_id in STABLE_SOURCE_IDS:
            if source_id not in source_ids:
                source_ids.append(source_id)
    if not source_ids:
        raise SystemExit("Provide --source-id or --stable")

    reports = [promote_source(source_id, allow_gate_fail=args.allow_gate_fail) for source_id in source_ids]
    print(json.dumps(reports, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
