from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


def _data_paths_module():
    from services.utils import data_paths

    return importlib.reload(data_paths)


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            if raw.strip():
                rows.append(json.loads(raw))
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def merge_chunk_exports(
    *,
    input_dataset_ids: list[str],
    output_dataset_id: str,
    profile: str,
) -> dict[str, Any]:
    data_paths = _data_paths_module()
    data_paths.ensure_rag_data_layout(dataset_ids=[output_dataset_id], embedding_profiles=[profile])

    merged_texts: list[dict[str, Any]] = []
    merged_metadata: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    duplicate_ids: list[str] = []
    input_counts: dict[str, dict[str, int]] = {}

    for dataset_id in input_dataset_ids:
        texts_path = data_paths.preferred_chunk_texts_export_path(dataset_id=dataset_id, profile=profile)
        metadata_path = data_paths.preferred_chunk_metadata_export_path(dataset_id=dataset_id, profile=profile)
        text_rows = _read_jsonl_rows(texts_path)
        metadata_rows = _read_jsonl_rows(metadata_path)

        text_ids = [str(row["id"]) for row in text_rows]
        metadata_ids = [str(row["id"]) for row in metadata_rows]
        if len(text_rows) != len(metadata_rows):
            raise RuntimeError(
                f"Count mismatch for {dataset_id}: texts={len(text_rows)} metadata={len(metadata_rows)}"
            )
        if text_ids != metadata_ids:
            raise RuntimeError(f"ID order mismatch between texts and metadata for {dataset_id}")

        for text_row, metadata_row in zip(text_rows, metadata_rows):
            chunk_id = str(text_row["id"])
            if chunk_id in seen_ids:
                duplicate_ids.append(chunk_id)
                continue
            seen_ids.add(chunk_id)
            merged_texts.append(text_row)
            merged_metadata.append(metadata_row)

        input_counts[dataset_id] = {
            "texts": len(text_rows),
            "metadata": len(metadata_rows),
        }

    if duplicate_ids:
        sample = duplicate_ids[:10]
        raise RuntimeError(
            f"Duplicate chunk IDs detected across inputs: total={len(duplicate_ids)} sample={sample}"
        )

    texts_output = data_paths.chunk_texts_export_path(dataset_id=output_dataset_id, profile=profile)
    metadata_output = data_paths.chunk_metadata_export_path(dataset_id=output_dataset_id, profile=profile)
    report_output = data_paths.qa_root_dir() / f"merged_chunk_export_{output_dataset_id}_{profile}.json"

    _write_jsonl(texts_output, merged_texts)
    _write_jsonl(metadata_output, merged_metadata)

    report = {
        "output_dataset_id": output_dataset_id,
        "profile": profile,
        "inputs": input_dataset_ids,
        "input_counts": input_counts,
        "merged_counts": {
            "texts": len(merged_texts),
            "metadata": len(merged_metadata),
        },
        "duplicate_ids": 0,
        "texts_output": str(texts_output),
        "metadata_output": str(metadata_output),
    }
    report_output.parent.mkdir(parents=True, exist_ok=True)
    report_output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["report_output"] = str(report_output)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge multiple chunk export datasets into one pre-embedding export.")
    parser.add_argument("--dataset-id", required=True, help="Output merged dataset ID")
    parser.add_argument("--profile", default="multilingual")
    parser.add_argument("--input-dataset-id", action="append", required=True, help="Input dataset ID to merge")
    args = parser.parse_args()

    report = merge_chunk_exports(
        input_dataset_ids=args.input_dataset_id,
        output_dataset_id=args.dataset_id,
        profile=args.profile,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
