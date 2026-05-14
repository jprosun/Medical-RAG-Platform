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
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "services" / "qdrant-ingestor"))

from services.utils.data_paths import (  # noqa: E402
    chunk_metadata_export_path,
    chunk_texts_export_path,
    ensure_rag_data_layout,
    kaggle_embedding_input_path,
    preferred_dataset_records_path,
    preferred_records_path,
)
from app.ingest import iter_enriched_jsonl_chunks  # noqa: E402


def _atomic_output(path: Path) -> tuple[Path, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    if tmp.exists():
        tmp.unlink()
    return tmp, open(tmp, "w", encoding="utf-8")


def _has_metadata_value(metadata: dict, key: str) -> bool:
    if key not in metadata:
        return False
    value = metadata.get(key)
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


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
    kaggle_input_output = kaggle_embedding_input_path(dataset_id=dataset_id, profile=args.profile)
    manifest_output = chunk_texts_export_path(
        filename="embedding_manifest.json",
        dataset_id=dataset_id,
        profile=args.profile,
    )
    texts_output.parent.mkdir(parents=True, exist_ok=True)

    existing_outputs = [
        path
        for path in (texts_output, metadata_output, kaggle_input_output, manifest_output)
        if path.exists()
    ]
    if existing_outputs and not args.overwrite:
        existing = ", ".join(str(path) for path in existing_outputs)
        raise SystemExit(f"Refusing to overwrite existing export(s): {existing}. Use --overwrite if intentional.")

    print(f"Chunking {source_file}...")
    print(f"Dataset: {dataset_id} | Profile: {args.profile}")
    t0 = time.time()

    required_metadata = [
        "doc_id",
        "article_id",
        "title",
        "canonical_title",
        "source_id",
        "source_name",
        "source_url",
        "doc_type",
        "specialty",
        "chunk_index",
        "section_title",
    ]

    print(f"Writing streaming export to {texts_output.parent}...")
    tmpdir = tempfile.mkdtemp(prefix="export_")
    tmp_texts = tmp_metadata = tmp_kaggle = None
    text_fh = metadata_fh = kaggle_fh = None
    missing_metadata_counts = {key: 0 for key in required_metadata}
    chunk_count = 0
    duplicate_ids = 0
    empty_texts = 0
    seen_ids: set[str] = set()
    try:
        tmp_file = Path(tmpdir) / source_file.name
        try:
            os.symlink(source_file, tmp_file)
        except (OSError, NotImplementedError):
            shutil.copy2(source_file, tmp_file)

        tmp_texts, text_fh = _atomic_output(texts_output)
        tmp_metadata, metadata_fh = _atomic_output(metadata_output)
        tmp_kaggle, kaggle_fh = _atomic_output(kaggle_input_output)

        for chunk in iter_enriched_jsonl_chunks(
            input_path=tmpdir,
            patterns=["*.jsonl"],
            chunk_size=args.chunk_size,
            overlap=args.overlap,
        ):
            chunk_count += 1
            if chunk.id in seen_ids:
                duplicate_ids += 1
            seen_ids.add(chunk.id)
            if not str(chunk.text).strip():
                empty_texts += 1
            for key in required_metadata:
                if not _has_metadata_value(chunk.metadata, key):
                    missing_metadata_counts[key] += 1

            text_fh.write(json.dumps({"id": chunk.id, "text": chunk.text}, ensure_ascii=False) + "\n")
            metadata_fh.write(json.dumps({"id": chunk.id, "metadata": chunk.metadata}, ensure_ascii=False) + "\n")
            kaggle_fh.write(
                json.dumps(
                    {"id": chunk.id, "text": chunk.text, "metadata": chunk.metadata},
                    ensure_ascii=False,
                )
                + "\n"
            )
            if chunk_count % 25000 == 0:
                for fh in (text_fh, metadata_fh, kaggle_fh):
                    fh.flush()
                print(f"  streamed {chunk_count} chunks...", flush=True)

        for fh in (text_fh, metadata_fh, kaggle_fh):
            fh.close()
        text_fh = metadata_fh = kaggle_fh = None
        tmp_texts.replace(texts_output)
        tmp_metadata.replace(metadata_output)
        tmp_kaggle.replace(kaggle_input_output)
    finally:
        for fh in (text_fh, metadata_fh, kaggle_fh):
            if fh is not None:
                fh.close()
        for tmp in (tmp_texts, tmp_metadata, tmp_kaggle):
            if tmp is not None and tmp.exists():
                tmp.unlink()
        shutil.rmtree(tmpdir, ignore_errors=True)

    print(f"  {chunk_count} chunks in {time.time() - t0:.1f}s")

    manifest = {
        "kind": "kaggle_embedding_export",
        "dataset_id": dataset_id,
        "profile": args.profile,
        "source_file": str(source_file),
        "chunk_size": args.chunk_size,
        "overlap": args.overlap,
        "chunk_count": chunk_count,
        "duplicate_ids": duplicate_ids,
        "empty_texts": empty_texts,
        "required_metadata": required_metadata,
        "missing_metadata_counts": missing_metadata_counts,
        "files": {
            "kaggle_embedding_input": str(kaggle_input_output),
            "chunk_texts": str(texts_output),
            "chunk_metadata": str(metadata_output),
        },
    }
    manifest_output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    size_mb = texts_output.stat().st_size / 1024 / 1024
    print(f"Done! {texts_output} ({size_mb:.1f} MB)")
    print(f"Metadata: {metadata_output}")
    print(f"Kaggle input: {kaggle_input_output}")
    print(f"Manifest: {manifest_output}")
    if duplicate_ids or empty_texts:
        raise SystemExit(f"Invalid export: duplicate_ids={duplicate_ids}, empty_texts={empty_texts}")


if __name__ == "__main__":
    main()
