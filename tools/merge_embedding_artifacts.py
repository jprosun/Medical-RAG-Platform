from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]
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


def merge_embedding_artifacts(
    *,
    input_dataset_ids: list[str],
    output_dataset_id: str,
    profile: str,
) -> dict[str, Any]:
    data_paths = _data_paths_module()
    data_paths.ensure_rag_data_layout(dataset_ids=[output_dataset_id], embedding_profiles=[profile])

    merged_texts: list[dict[str, Any]] = []
    merged_metadata: list[dict[str, Any]] = []
    merged_ids: list[str] = []
    embedding_parts: list[np.ndarray] = []
    seen_ids: set[str] = set()
    input_counts: dict[str, dict[str, int]] = {}
    duplicate_ids: list[str] = []
    vector_dim: int | None = None

    for dataset_id in input_dataset_ids:
        texts_path = data_paths.preferred_chunk_texts_export_path(dataset_id=dataset_id, profile=profile)
        metadata_path = data_paths.preferred_chunk_metadata_export_path(dataset_id=dataset_id, profile=profile)
        ids_path = data_paths.preferred_embedding_ids_path(dataset_id=dataset_id, profile=profile)
        vectors_path = data_paths.preferred_embedding_vectors_path(dataset_id=dataset_id, profile=profile)

        text_rows = _read_jsonl_rows(texts_path)
        metadata_rows = _read_jsonl_rows(metadata_path)
        with open(ids_path, "r", encoding="utf-8") as fh:
            chunk_ids = [str(item) for item in json.load(fh)]
        vectors = np.load(vectors_path)

        text_ids = [str(row["id"]) for row in text_rows]
        metadata_ids = [str(row["id"]) for row in metadata_rows]
        if len(chunk_ids) != int(vectors.shape[0]):
            raise RuntimeError(f"Vector count mismatch for {dataset_id}")
        if text_ids != metadata_ids or text_ids != chunk_ids:
            raise RuntimeError(f"Artifact alignment mismatch for {dataset_id}")
        current_dim = int(vectors.shape[1]) if vectors.ndim == 2 and vectors.shape[0] else 0
        if vector_dim is None:
            vector_dim = current_dim
        elif current_dim != vector_dim:
            raise RuntimeError(
                f"Vector dimension mismatch for {dataset_id}: expected={vector_dim} actual={current_dim}"
            )

        for chunk_id in chunk_ids:
            if chunk_id in seen_ids:
                duplicate_ids.append(chunk_id)
            else:
                seen_ids.add(chunk_id)
        if duplicate_ids:
            sample = duplicate_ids[:10]
            raise RuntimeError(
                f"Duplicate chunk IDs detected across inputs: total={len(duplicate_ids)} sample={sample}"
            )

        merged_texts.extend(text_rows)
        merged_metadata.extend(metadata_rows)
        merged_ids.extend(chunk_ids)
        embedding_parts.append(vectors)
        input_counts[dataset_id] = {
            "texts": len(text_rows),
            "metadata": len(metadata_rows),
            "ids": len(chunk_ids),
            "vectors": int(vectors.shape[0]),
            "vector_dim": current_dim,
        }

    merged_vectors = np.concatenate(embedding_parts, axis=0) if embedding_parts else np.zeros((0, 0), dtype=np.float32)

    texts_output = data_paths.chunk_texts_export_path(dataset_id=output_dataset_id, profile=profile)
    metadata_output = data_paths.chunk_metadata_export_path(dataset_id=output_dataset_id, profile=profile)
    ids_output = data_paths.embedding_ids_path(dataset_id=output_dataset_id, profile=profile)
    vectors_output = data_paths.embedding_vectors_path(dataset_id=output_dataset_id, profile=profile)
    manifest_output = data_paths.embeddings_staging_dir(profile, dataset_id=output_dataset_id) / "embedding_manifest.json"
    report_output = data_paths.qa_root_dir() / f"merged_embedding_artifacts_{output_dataset_id}_{profile}.json"

    _write_jsonl(texts_output, merged_texts)
    _write_jsonl(metadata_output, merged_metadata)
    ids_output.parent.mkdir(parents=True, exist_ok=True)
    ids_output.write_text(json.dumps(merged_ids, ensure_ascii=False), encoding="utf-8")
    np.save(vectors_output, merged_vectors)
    manifest = {
        "dataset_id": output_dataset_id,
        "profile": profile,
        "inputs": input_dataset_ids,
        "vector_count": int(merged_vectors.shape[0]),
        "vector_dim": int(merged_vectors.shape[1]) if merged_vectors.ndim == 2 and merged_vectors.shape[0] else 0,
        "chunk_id_count": len(merged_ids),
    }
    manifest_output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "output_dataset_id": output_dataset_id,
        "profile": profile,
        "inputs": input_dataset_ids,
        "input_counts": input_counts,
        "merged_counts": {
            "texts": len(merged_texts),
            "metadata": len(merged_metadata),
            "ids": len(merged_ids),
            "vectors": int(merged_vectors.shape[0]),
            "vector_dim": int(merged_vectors.shape[1]) if merged_vectors.ndim == 2 and merged_vectors.shape[0] else 0,
        },
        "texts_output": str(texts_output),
        "metadata_output": str(metadata_output),
        "ids_output": str(ids_output),
        "vectors_output": str(vectors_output),
        "manifest_output": str(manifest_output),
    }
    report_output.parent.mkdir(parents=True, exist_ok=True)
    report_output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["report_output"] = str(report_output)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge pre-embedded artifact datasets into one reusable dataset.")
    parser.add_argument("--dataset-id", required=True, help="Output merged dataset ID")
    parser.add_argument("--profile", default="multilingual")
    parser.add_argument("--input-dataset-id", action="append", required=True, help="Input dataset ID to merge")
    args = parser.parse_args()

    report = merge_embedding_artifacts(
        input_dataset_ids=args.input_dataset_id,
        output_dataset_id=args.dataset_id,
        profile=args.profile,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
