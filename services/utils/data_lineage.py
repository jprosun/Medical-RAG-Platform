"""
Small helpers for attaching source lineage to ETL records.

The fields are intentionally flat so they survive JSONL, schema validation,
chunk metadata, and Qdrant payload storage without special handling.
"""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from typing import Any

from services.utils.data_paths import REPO_ROOT


def make_run_id(stage: str, name: str | None = None) -> str:
    suffix = f"_{name}" if name else ""
    return f"{stage}{suffix}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"


def file_sha256(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def relative_repo_path(path: str | Path) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(resolved)


def build_file_lineage(
    path: str | Path,
    *,
    source_id: str = "",
    etl_run_id: str = "",
    crawl_run_id: str = "",
    parent_file: str = "",
) -> dict[str, Any]:
    source_path = Path(path)
    rel_path = relative_repo_path(source_path)
    parts = {part.lower() for part in source_path.parts}
    lineage: dict[str, Any] = {
        "source_id": source_id,
        "source_file": rel_path,
        "source_sha256": file_sha256(source_path) if source_path.is_file() else "",
        "crawl_run_id": crawl_run_id or os.getenv("CRAWL_RUN_ID", ""),
        "etl_run_id": etl_run_id or os.getenv("ETL_RUN_ID", ""),
        "parent_file": parent_file,
        "raw_path": "",
        "processed_path": "",
        "intermediate_path": "",
    }

    if "raw" in parts or "data_raw" in parts:
        lineage["raw_path"] = rel_path
    if "processed" in parts or "data_processed" in parts:
        lineage["processed_path"] = rel_path
    if "intermediate" in parts or "data_intermediate" in parts:
        lineage["intermediate_path"] = rel_path

    return lineage
