"""Filter an existing chunk export into a smaller Kaggle embedding bundle."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_paths import (  # noqa: E402
    chunk_metadata_export_path,
    chunk_texts_export_path,
    embeddings_export_dir,
    kaggle_embedding_input_path,
)


def _has_metadata_value(metadata: dict[str, Any], key: str) -> bool:
    value = metadata.get(key)
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def filter_chunk_export(
    *,
    input_dataset_id: str,
    output_dataset_id: str,
    profile: str,
    include_source_ids: set[str],
    exclude_source_ids: set[str],
    overwrite: bool,
) -> dict[str, Any]:
    input_texts = chunk_texts_export_path(dataset_id=input_dataset_id, profile=profile)
    input_metadata = chunk_metadata_export_path(dataset_id=input_dataset_id, profile=profile)
    input_kaggle = kaggle_embedding_input_path(dataset_id=input_dataset_id, profile=profile)

    output_dir = embeddings_export_dir(dataset_id=output_dataset_id, profile=profile)
    output_texts = chunk_texts_export_path(dataset_id=output_dataset_id, profile=profile)
    output_metadata = chunk_metadata_export_path(dataset_id=output_dataset_id, profile=profile)
    output_kaggle = kaggle_embedding_input_path(dataset_id=output_dataset_id, profile=profile)
    output_manifest = output_dir / "embedding_manifest.json"
    output_dir.mkdir(parents=True, exist_ok=True)

    existing = [path for path in (output_texts, output_metadata, output_kaggle, output_manifest) if path.exists()]
    if existing and not overwrite:
        raise SystemExit(f"Refusing to overwrite existing export files: {existing}. Use --overwrite.")

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
    missing_metadata_counts = {key: 0 for key in required_metadata}
    seen_ids: set[str] = set()
    input_count = 0
    chunk_count = 0
    duplicate_ids = 0
    empty_texts = 0
    skipped_by_source = 0

    with (
        open(input_texts, "r", encoding="utf-8") as texts_in,
        open(input_metadata, "r", encoding="utf-8") as metadata_in,
        open(input_kaggle, "r", encoding="utf-8") as kaggle_in,
        open(output_texts, "w", encoding="utf-8") as texts_out,
        open(output_metadata, "w", encoding="utf-8") as metadata_out,
        open(output_kaggle, "w", encoding="utf-8") as kaggle_out,
    ):
        for text_raw, metadata_raw, kaggle_raw in zip(texts_in, metadata_in, kaggle_in):
            if not text_raw.strip() and not metadata_raw.strip() and not kaggle_raw.strip():
                continue
            input_count += 1
            text_row = json.loads(text_raw)
            metadata_row = json.loads(metadata_raw)
            kaggle_row = json.loads(kaggle_raw)
            chunk_id = str(kaggle_row.get("id", ""))
            if chunk_id != str(text_row.get("id", "")) or chunk_id != str(metadata_row.get("id", "")):
                raise RuntimeError(f"Input export alignment mismatch at row {input_count}: {chunk_id}")

            metadata = kaggle_row.get("metadata") or metadata_row.get("metadata") or {}
            if not isinstance(metadata, dict):
                metadata = {}
            source_id = str(metadata.get("source_id", "")).strip()
            if include_source_ids and source_id not in include_source_ids:
                skipped_by_source += 1
                continue
            if exclude_source_ids and source_id in exclude_source_ids:
                skipped_by_source += 1
                continue

            chunk_count += 1
            if chunk_id in seen_ids:
                duplicate_ids += 1
            seen_ids.add(chunk_id)
            if not str(kaggle_row.get("text", "")).strip():
                empty_texts += 1
            for key in required_metadata:
                if not _has_metadata_value(metadata, key):
                    missing_metadata_counts[key] += 1

            texts_out.write(json.dumps(text_row, ensure_ascii=False) + "\n")
            metadata_out.write(json.dumps(metadata_row, ensure_ascii=False) + "\n")
            kaggle_out.write(json.dumps(kaggle_row, ensure_ascii=False) + "\n")

    manifest = {
        "kind": "kaggle_embedding_export",
        "dataset_id": output_dataset_id,
        "profile": profile,
        "source_dataset_id": input_dataset_id,
        "include_source_ids": sorted(include_source_ids),
        "exclude_source_ids": sorted(exclude_source_ids),
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "input_count": input_count,
        "chunk_count": chunk_count,
        "skipped_by_source": skipped_by_source,
        "duplicate_ids": duplicate_ids,
        "empty_texts": empty_texts,
        "required_metadata": required_metadata,
        "missing_metadata_counts": missing_metadata_counts,
        "files": {
            "kaggle_embedding_input": str(output_kaggle),
            "chunk_texts": str(output_texts),
            "chunk_metadata": str(output_metadata),
        },
    }
    output_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    if duplicate_ids or empty_texts:
        raise SystemExit(f"Invalid export: duplicate_ids={duplicate_ids}, empty_texts={empty_texts}")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter chunk exports by source_id.")
    parser.add_argument("--input-dataset-id", required=True)
    parser.add_argument("--output-dataset-id", required=True)
    parser.add_argument("--profile", default="multilingual")
    parser.add_argument("--include-source-id", action="append", default=[])
    parser.add_argument("--exclude-source-id", action="append", default=[])
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    filter_chunk_export(
        input_dataset_id=args.input_dataset_id,
        output_dataset_id=args.output_dataset_id,
        profile=args.profile,
        include_source_ids=set(args.include_source_id),
        exclude_source_ids=set(args.exclude_source_id),
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()
