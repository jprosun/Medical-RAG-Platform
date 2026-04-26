"""Audit alignment between chunk exports and returned embedding artifacts."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_lineage import file_sha256
from services.utils.data_paths import (
    DEFAULT_EMBEDDING_PROFILE,
    chunk_metadata_export_path,
    chunk_texts_export_path,
    embedding_ids_path,
    embedding_vectors_path,
    migration_audit_path,
)


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_jsonl_ids(path: Path, *, field: str = "id") -> tuple[list[str], int]:
    ids: list[str] = []
    invalid = 0
    if not path.exists():
        return ids, invalid

    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
                ids.append(str(record.get(field, "")))
            except json.JSONDecodeError:
                invalid += 1
    return ids, invalid


def _dupe_count(ids: list[str]) -> int:
    return len(ids) - len(set(ids))


def _summarize_file(path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "bytes": 0,
        "sha256": "",
    }
    if path.exists():
        summary["bytes"] = path.stat().st_size
        summary["sha256"] = file_sha256(path)
    return summary


def audit_embedding_artifacts(dataset_id: str, profile: str) -> dict[str, Any]:
    ids_path = embedding_ids_path(dataset_id=dataset_id, profile=profile)
    vectors_path = embedding_vectors_path(dataset_id=dataset_id, profile=profile)
    metadata_path = chunk_metadata_export_path(dataset_id=dataset_id, profile=profile)
    texts_path = chunk_texts_export_path(dataset_id=dataset_id, profile=profile)

    chunk_ids: list[str] = []
    ids_error = ""
    if ids_path.exists():
        try:
            chunk_ids = [str(item) for item in _load_json(ids_path)]
        except Exception as exc:  # pragma: no cover - defensive reporting
            ids_error = str(exc)

    metadata_ids, metadata_invalid = _load_jsonl_ids(metadata_path)
    text_ids, text_invalid = _load_jsonl_ids(texts_path)

    vector_shape: list[int] | None = None
    vector_dtype = ""
    vector_error = ""
    if vectors_path.exists():
        try:
            import numpy as np

            vectors = np.load(vectors_path, mmap_mode="r")
            vector_shape = list(vectors.shape)
            vector_dtype = str(vectors.dtype)
        except Exception as exc:  # pragma: no cover - depends on local numpy/file state
            vector_error = str(exc)

    chunk_id_set = set(chunk_ids)
    metadata_set = set(metadata_ids)
    text_set = set(text_ids)
    vector_count = vector_shape[0] if vector_shape else 0

    checks = {
        "ids_loaded": bool(chunk_ids) and not ids_error,
        "metadata_loaded": bool(metadata_ids) and metadata_invalid == 0,
        "texts_loaded": bool(text_ids) and text_invalid == 0,
        "vectors_loaded": vector_shape is not None and not vector_error,
        "ids_match_vectors": bool(chunk_ids) and vector_shape is not None and len(chunk_ids) == vector_count,
        "ids_match_metadata_set": bool(chunk_ids) and chunk_id_set == metadata_set,
        "ids_match_text_set": bool(chunk_ids) and chunk_id_set == text_set,
        "metadata_order_matches_ids": bool(chunk_ids) and metadata_ids == chunk_ids,
        "text_order_matches_ids": bool(chunk_ids) and text_ids == chunk_ids,
        "no_duplicate_ids": _dupe_count(chunk_ids) == 0,
        "no_duplicate_metadata_ids": _dupe_count(metadata_ids) == 0,
        "no_duplicate_text_ids": _dupe_count(text_ids) == 0,
    }
    status = "pass" if all(checks.values()) else "fail"

    return {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset_id": dataset_id,
        "profile": profile,
        "status": status,
        "files": {
            "ids": _summarize_file(ids_path),
            "vectors": _summarize_file(vectors_path),
            "metadata": _summarize_file(metadata_path),
            "texts": _summarize_file(texts_path),
        },
        "counts": {
            "chunk_ids": len(chunk_ids),
            "vectors": vector_count,
            "metadata": len(metadata_ids),
            "texts": len(text_ids),
            "duplicate_chunk_ids": _dupe_count(chunk_ids),
            "duplicate_metadata_ids": _dupe_count(metadata_ids),
            "duplicate_text_ids": _dupe_count(text_ids),
            "metadata_invalid_json_lines": metadata_invalid,
            "text_invalid_json_lines": text_invalid,
        },
        "vector_shape": vector_shape,
        "vector_dtype": vector_dtype,
        "errors": {
            "ids": ids_error,
            "vectors": vector_error,
        },
        "checks": checks,
        "samples": {
            "missing_metadata_for_ids": sorted(chunk_id_set - metadata_set)[:20],
            "extra_metadata_not_in_ids": sorted(metadata_set - chunk_id_set)[:20],
            "missing_text_for_ids": sorted(chunk_id_set - text_set)[:20],
            "extra_text_not_in_ids": sorted(text_set - chunk_id_set)[:20],
        },
    }


def write_report(report: dict[str, Any], output: str | Path | None = None) -> Path:
    target = Path(output) if output else migration_audit_path(
        f"embedding_alignment_{report['dataset_id']}_{report['profile']}.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit embedding export/vector alignment.")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--profile", default=DEFAULT_EMBEDDING_PROFILE)
    parser.add_argument("--output", default="")
    parser.add_argument("--no-fail", action="store_true", help="Always exit 0 after writing the report.")
    args = parser.parse_args()

    report = audit_embedding_artifacts(args.dataset_id, args.profile)
    output = write_report(report, args.output or None)
    print(json.dumps({
        "status": report["status"],
        "dataset_id": args.dataset_id,
        "profile": args.profile,
        "counts": report["counts"],
        "output": str(output),
    }, ensure_ascii=False, indent=2))

    if report["status"] != "pass" and not args.no_fail:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
