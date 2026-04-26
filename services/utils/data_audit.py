"""Migration audit helpers for canonical `rag-data/` parity checks."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable

from services.utils.data_lineage import file_sha256
from services.utils.data_paths import (
    dataset_records_path,
    legacy_dataset_records_candidates,
    legacy_records_path,
    migration_audit_path,
    source_records_path,
)


def summarize_jsonl(path: str | Path) -> dict[str, Any]:
    jsonl_path = Path(path)
    summary: dict[str, Any] = {
        "path": str(jsonl_path),
        "exists": jsonl_path.exists(),
        "bytes": 0,
        "sha256": "",
        "record_count": 0,
        "invalid_json_lines": 0,
        "doc_id_count": 0,
        "duplicate_doc_ids": 0,
    }
    if not jsonl_path.exists():
        return summary

    summary["bytes"] = jsonl_path.stat().st_size
    summary["sha256"] = file_sha256(jsonl_path)
    doc_ids: set[str] = set()
    duplicate_doc_ids = 0

    with open(jsonl_path, "r", encoding="utf-8") as fh:
        for raw in fh:
            if not raw.strip():
                continue
            summary["record_count"] += 1
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                summary["invalid_json_lines"] += 1
                continue
            doc_id = str(record.get("doc_id", "")).strip()
            if not doc_id:
                continue
            if doc_id in doc_ids:
                duplicate_doc_ids += 1
            doc_ids.add(doc_id)

    summary["doc_id_count"] = len(doc_ids)
    summary["duplicate_doc_ids"] = duplicate_doc_ids
    summary["_doc_ids"] = doc_ids
    return summary


def compare_jsonl_records(canonical: str | Path, legacy: str | Path) -> dict[str, Any]:
    canonical_summary = summarize_jsonl(canonical)
    legacy_summary = summarize_jsonl(legacy)
    canonical_ids = canonical_summary.pop("_doc_ids", set())
    legacy_ids = legacy_summary.pop("_doc_ids", set())

    status = "missing"
    if canonical_summary["exists"] and legacy_summary["exists"]:
        same_doc_ids = canonical_ids == legacy_ids
        same_count = canonical_summary["record_count"] == legacy_summary["record_count"]
        status = "match" if same_doc_ids and same_count else "mismatch"
    elif canonical_summary["exists"]:
        status = "canonical_only"
    elif legacy_summary["exists"]:
        status = "legacy_only"

    return {
        "status": status,
        "canonical": canonical_summary,
        "legacy": legacy_summary,
        "missing_in_canonical": sorted(legacy_ids - canonical_ids)[:50],
        "missing_in_legacy": sorted(canonical_ids - legacy_ids)[:50],
    }


def audit_records(
    *,
    source_ids: Iterable[str] = (),
    dataset_ids: Iterable[str] = (),
) -> dict[str, Any]:
    sources = {
        source_id: compare_jsonl_records(source_records_path(source_id), legacy_records_path(source_id))
        for source_id in source_ids
    }
    datasets: dict[str, Any] = {}
    for dataset_id in dataset_ids:
        canonical = dataset_records_path(dataset_id)
        legacy_candidates = legacy_dataset_records_candidates(dataset_id)
        legacy = next((path for path in legacy_candidates if path.exists()), legacy_candidates[0])
        datasets[dataset_id] = compare_jsonl_records(canonical, legacy)

    return {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sources": sources,
        "datasets": datasets,
    }


def write_migration_audit(report: dict[str, Any], output_path: str | Path | None = None) -> Path:
    target = Path(output_path) if output_path else migration_audit_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    return target
