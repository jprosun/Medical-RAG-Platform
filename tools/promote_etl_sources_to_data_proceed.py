from __future__ import annotations

import argparse
import json
import shutil
import sys
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


STABLE_SOURCE_IDS = (
    "medlineplus",
    "who",
    "ncbi_bookshelf",
    "nhs_health_a_z",
    "msd_manual_consumer",
    "msd_manual_professional",
)


def _iter_jsonl(path: Path):
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            yield json.loads(line)


def _safe_name(processed_path: str) -> str:
    return Path(processed_path).name


def promote_source(source_id: str) -> dict[str, Any]:
    if source_id not in KNOWN_SOURCE_IDS:
        raise ValueError(f"Unknown source_id: {source_id}")

    ensure_rag_data_layout(source_ids=[source_id])
    source_records = source_records_path(source_id)
    target_records = data_proceed_records_path(source_id)
    target_processed = data_proceed_processed_dir(source_id)
    target_summary = data_proceed_summary_path(source_id)

    target_processed.mkdir(parents=True, exist_ok=True)
    target_records.parent.mkdir(parents=True, exist_ok=True)

    if not source_records.exists():
        report = {
            "source_id": source_id,
            "records_exists": False,
            "records_promoted": 0,
            "processed_promoted": 0,
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
    missing = 0
    for record in _iter_jsonl(source_records):
        processed_path = str(record.get("processed_path", "")).strip()
        if not processed_path or processed_path in seen_processed:
            continue
        seen_processed.add(processed_path)
        source_processed = Path(processed_path)
        if not source_processed.is_absolute():
            source_processed = REPO_ROOT / processed_path
        if not source_processed.exists() or not source_processed.is_file():
            missing += 1
            continue
        shutil.copy2(source_processed, target_processed / _safe_name(processed_path))
        promoted += 1

    report = {
        "source_id": source_id,
        "records_exists": True,
        "records_promoted": sum(1 for _ in _iter_jsonl(source_records)),
        "processed_promoted": promoted,
        "missing_processed": missing,
        "records_path": str(target_records),
        "processed_dir": str(target_processed),
    }
    target_summary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote ETL-ready sources into rag-data/data_proceed for easy observation.")
    parser.add_argument("--source-id", action="append", dest="source_ids", default=[])
    parser.add_argument("--stable", action="store_true", help="Promote the current stable source set.")
    args = parser.parse_args()

    source_ids = list(args.source_ids)
    if args.stable:
        for source_id in STABLE_SOURCE_IDS:
            if source_id not in source_ids:
                source_ids.append(source_id)
    if not source_ids:
        raise SystemExit("Provide --source-id or --stable")

    reports = [promote_source(source_id) for source_id in source_ids]
    print(json.dumps(reports, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
