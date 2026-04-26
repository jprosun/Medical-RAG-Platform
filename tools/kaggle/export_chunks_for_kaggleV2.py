"""
Export chunk texts and metadata for offline embedding.

By default this reads either:
  - a dataset release under `rag-data/datasets/<dataset_id>/records/`
  - or a source record file under `rag-data/sources/<source_id>/records/`

Legacy fallback to the old top-level `data/` layout is preserved during migration.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "services" / "qdrant-ingestor"))

from services.utils.data_paths import (  # noqa: E402
    chunk_metadata_export_path,
    chunk_texts_export_path,
    ensure_rag_data_layout,
    preferred_dataset_records_path,
    preferred_records_path,
)
from app.ingest import ingest_enriched_jsonl  # noqa: E402


def _write_jsonl_atomic(path: Path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description="Export chunk texts for offline embedding.")
    parser.add_argument("--dataset-id", default="", help="Dataset release ID under rag-data/datasets/")
    parser.add_argument("--source-id", default="vmj_ojs", help="Source ID under rag-data/sources/ when --dataset-id is omitted")
    parser.add_argument("--input-jsonl", default="")
    parser.add_argument("--profile", default="multilingual", help="Embedding profile name")
    parser.add_argument("--chunk-size", type=int, default=900)
    parser.add_argument("--overlap", type=int, default=150)
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing export files.")
    args = parser.parse_args()

    if args.input_jsonl:
        source_file = Path(args.input_jsonl)
        dataset_id = args.dataset_id or source_file.stem
    elif args.dataset_id:
        dataset_id = args.dataset_id
        source_file = preferred_dataset_records_path(dataset_id)
    else:
        dataset_id = args.source_id
        source_file = preferred_records_path(args.source_id)

    ensure_rag_data_layout(dataset_ids=[dataset_id], embedding_profiles=[args.profile])
    texts_output = chunk_texts_export_path(dataset_id=dataset_id, profile=args.profile)
    metadata_output = chunk_metadata_export_path(dataset_id=dataset_id, profile=args.profile)
    texts_output.parent.mkdir(parents=True, exist_ok=True)

    existing_outputs = [path for path in (texts_output, metadata_output) if path.exists()]
    if existing_outputs and not args.overwrite:
        existing = ", ".join(str(path) for path in existing_outputs)
        raise SystemExit(f"Refusing to overwrite existing export(s): {existing}. Use --overwrite if intentional.")

    print(f"Chunking {source_file}...")
    print(f"Dataset: {dataset_id} | Profile: {args.profile}")
    t0 = time.time()

    tmpdir = tempfile.mkdtemp(prefix="export_")
    try:
        tmp_file = Path(tmpdir) / source_file.name
        try:
            os.symlink(source_file, tmp_file)
        except (OSError, NotImplementedError):
            shutil.copy2(source_file, tmp_file)

        chunks = ingest_enriched_jsonl(
            input_path=tmpdir,
            patterns=["*.jsonl"],
            chunk_size=args.chunk_size,
            overlap=args.overlap,
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    print(f"  {len(chunks)} chunks in {time.time() - t0:.1f}s")

    print(f"Writing {texts_output}...")
    _write_jsonl_atomic(texts_output, ({"id": chunk.id, "text": chunk.text} for chunk in chunks))
    _write_jsonl_atomic(metadata_output, ({"id": chunk.id, "metadata": chunk.metadata} for chunk in chunks))

    size_mb = texts_output.stat().st_size / 1024 / 1024
    print(f"Done! {texts_output} ({size_mb:.1f} MB)")
    print(f"Metadata: {metadata_output}")


if __name__ == "__main__":
    main()
