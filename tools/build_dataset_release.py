"""Build an ingest-facing dataset release from canonical source records."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from app.document_schema import DocumentRecord
from services.utils.data_lineage import file_sha256, make_run_id
from services.utils.data_paths import (
    KNOWN_SOURCE_IDS,
    dataset_manifest_path,
    dataset_processed_dir,
    dataset_processed_manifest_path,
    dataset_qa_dir,
    dataset_records_path,
    ensure_rag_data_layout,
    source_records_path,
)


EN_SOURCE_IDS = ("medlineplus", "who", "ncbi_bookshelf")
VI_SOURCE_IDS = tuple(source_id for source_id in KNOWN_SOURCE_IDS if source_id not in EN_SOURCE_IDS)
SOURCE_GROUPS = {
    "en": EN_SOURCE_IDS,
    "vi": VI_SOURCE_IDS,
    "all": KNOWN_SOURCE_IDS,
}


def _iter_jsonl_dicts(path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            if not raw.strip():
                continue
            yield lineno, json.loads(raw)


def _record_key(record: dict[str, Any], fallback_source_id: str, lineno: int, dedup_key: str) -> tuple[str, str]:
    if dedup_key == "none":
        return fallback_source_id, f"line:{lineno}"

    doc_id = str(record.get("doc_id", "")).strip()
    if not doc_id:
        doc_id = f"line:{lineno}"

    if dedup_key == "doc_id":
        return "", doc_id

    source_id = str(record.get("source_id", "")).strip() or fallback_source_id
    return source_id, doc_id


def _resolve_source_ids(source_ids: list[str], source_group: str) -> tuple[str, ...]:
    resolved = list(SOURCE_GROUPS[source_group])
    for source_id in source_ids:
        if source_id not in resolved:
            resolved.append(source_id)
    return tuple(resolved)


def _safe_dataset_processed_name(source_id: str, processed_path: str) -> str:
    src_name = Path(processed_path).name
    stem = Path(src_name).stem
    suffix = Path(src_name).suffix or ".txt"
    safe_stem = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in stem).strip("._")
    digest = hashlib.sha1(processed_path.encode("utf-8")).hexdigest()[:10]
    return f"{source_id}__{safe_stem}__{digest}{suffix}"


def build_dataset_release(
    *,
    dataset_id: str,
    source_ids: Iterable[str],
    dedup_key: str = "source_doc_id",
) -> dict[str, Any]:
    source_ids = tuple(source_ids)
    ensure_rag_data_layout(source_ids=source_ids, dataset_ids=[dataset_id])

    output_path = dataset_records_path(dataset_id)
    processed_dir = dataset_processed_dir(dataset_id)
    processed_manifest = dataset_processed_manifest_path(dataset_id)
    qa_dir = dataset_qa_dir(dataset_id)
    qa_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    for stale in processed_dir.iterdir():
        if stale.is_file():
            stale.unlink()

    seen: set[tuple[str, str]] = set()
    seen_processed_paths: set[str] = set()
    source_summaries: dict[str, dict[str, Any]] = {}
    processed_entries: list[dict[str, Any]] = []
    records_written = 0
    duplicates_skipped = 0
    validation_errors = 0
    invalid_json_lines = 0
    processed_files_copied = 0
    etl_run_id = make_run_id("build_dataset", dataset_id)

    with open(output_path, "w", encoding="utf-8") as out:
        for source_id in source_ids:
            path = source_records_path(source_id)
            summary: dict[str, Any] = {
                "path": str(path),
                "exists": path.exists(),
                "input_records": 0,
                "written_records": 0,
                "duplicates_skipped": 0,
                "validation_errors": 0,
                "invalid_json_lines": 0,
                "sha256": file_sha256(path) if path.exists() else "",
            }
            source_summaries[source_id] = summary
            if not path.exists():
                continue

            try:
                iterator = _iter_jsonl_dicts(path)
                for lineno, record in iterator:
                    summary["input_records"] += 1
                    try:
                        validation = DocumentRecord.from_dict(record).validate()
                    except Exception:
                        validation = ["schema_exception"]
                    if validation:
                        summary["validation_errors"] += 1
                        validation_errors += 1

                    key = _record_key(record, source_id, lineno, dedup_key)
                    if key in seen:
                        duplicates_skipped += 1
                        summary["duplicates_skipped"] += 1
                        continue
                    seen.add(key)

                    out.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
                    records_written += 1
                    summary["written_records"] += 1

                    processed_path = str(record.get("processed_path", "")).strip()
                    if processed_path and processed_path not in seen_processed_paths:
                        source_processed = Path(processed_path)
                        if not source_processed.is_absolute():
                            source_processed = REPO_ROOT / processed_path
                        if source_processed.exists() and source_processed.is_file():
                            seen_processed_paths.add(processed_path)
                            dataset_name = _safe_dataset_processed_name(source_id, processed_path)
                            dataset_processed = processed_dir / dataset_name
                            shutil.copy2(source_processed, dataset_processed)
                            processed_entries.append(
                                {
                                    "source_id": source_id,
                                    "doc_id": str(record.get("doc_id", "")).strip(),
                                    "title": str(record.get("title", "")).strip(),
                                    "source_processed_path": processed_path,
                                    "dataset_processed_path": str(dataset_processed),
                                }
                            )
                            processed_files_copied += 1
            except json.JSONDecodeError:
                invalid_json_lines += 1
                summary["invalid_json_lines"] += 1

    with open(processed_manifest, "w", encoding="utf-8") as fh:
        for entry in processed_entries:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    generated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    report = {
        "kind": "dataset_release",
        "dataset_id": dataset_id,
        "etl_run_id": etl_run_id,
        "generated_at_utc": generated_at,
        "records_path": str(output_path),
        "records_sha256": file_sha256(output_path) if output_path.exists() else "",
        "source_ids": list(source_ids),
        "source_records": source_summaries,
        "dedup_key": dedup_key,
        "record_count": records_written,
        "duplicates_skipped": duplicates_skipped,
        "validation_errors": validation_errors,
        "invalid_json_lines": invalid_json_lines,
        "processed_dir": str(processed_dir),
        "processed_manifest_path": str(processed_manifest),
        "processed_files_copied": processed_files_copied,
    }

    qa_path = qa_dir / "build_summary.json"
    with open(qa_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)

    manifest = {
        "kind": "dataset_release",
        "dataset_id": dataset_id,
        "etl_run_id": etl_run_id,
        "generated_at_utc": generated_at,
        "records_path": str(output_path),
        "records_sha256": report["records_sha256"],
        "qa_summary_path": str(qa_path),
        "source_ids": list(source_ids),
        "record_count": records_written,
        "dedup_key": dedup_key,
        "processed_dir": str(processed_dir),
        "processed_manifest_path": str(processed_manifest),
        "processed_files_copied": processed_files_copied,
    }
    manifest_path = dataset_manifest_path(dataset_id)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a canonical dataset release from source records.")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--source-group", choices=sorted(SOURCE_GROUPS), default="all")
    parser.add_argument("--source-id", action="append", default=[], dest="source_ids")
    parser.add_argument("--dedup-key", choices=("source_doc_id", "doc_id", "none"), default="source_doc_id")
    args = parser.parse_args()

    source_ids = _resolve_source_ids(args.source_ids, args.source_group)
    report = build_dataset_release(
        dataset_id=args.dataset_id,
        source_ids=source_ids,
        dedup_key=args.dedup_key,
    )
    print(json.dumps({
        "dataset_id": report["dataset_id"],
        "record_count": report["record_count"],
        "duplicates_skipped": report["duplicates_skipped"],
        "validation_errors": report["validation_errors"],
        "records_path": report["records_path"],
    }, ensure_ascii=False, indent=2))

    if report["invalid_json_lines"] or report["validation_errors"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
