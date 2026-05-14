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


def _load_selected_record_keys(paths: list[Path]) -> tuple[set[str], set[str]]:
    selected_titles: set[str] = set()
    selected_doc_ids: set[str] = set()
    for path in paths:
        for row in _read_jsonl_rows(path):
            title = str(row.get("title") or "").strip()
            doc_id = str(row.get("doc_id") or "").strip()
            if title:
                selected_titles.add(title)
            if doc_id:
                selected_doc_ids.add(doc_id)
    return selected_titles, selected_doc_ids


def build_vmj_release_v2_supplement(
    *,
    output_dataset_id: str,
    profile: str,
    mode: str,
) -> dict[str, Any]:
    data_paths = _data_paths_module()
    data_paths.ensure_rag_data_layout(dataset_ids=[output_dataset_id], embedding_profiles=[profile])

    if mode not in {"backfilled", "high_confidence", "strict_union", "all_salvage"}:
        raise ValueError(f"Unsupported mode: {mode}")

    base_dataset_id = "vmj_ojs_release_v2"
    salvage_root = data_paths.dataset_records_dir("vmj_ojs_release_v2_salvage")
    records_by_mode = {
        "backfilled": [salvage_root / "document_records_backfilled_article_url.jsonl"],
        "high_confidence": [salvage_root / "document_records_issue_url_only_high_confidence.jsonl"],
        "strict_union": [
            salvage_root / "document_records_backfilled_article_url.jsonl",
            salvage_root / "document_records_issue_url_only_high_confidence.jsonl",
        ],
        "all_salvage": [salvage_root / "document_records.jsonl"],
    }
    selected_record_paths = records_by_mode[mode]
    selected_titles, selected_doc_ids = _load_selected_record_keys(selected_record_paths)

    source_texts_path = data_paths.preferred_chunk_texts_export_path(dataset_id=base_dataset_id, profile=profile)
    source_metadata_path = data_paths.preferred_chunk_metadata_export_path(dataset_id=base_dataset_id, profile=profile)
    source_ids_path = data_paths.preferred_embedding_ids_path(dataset_id=base_dataset_id, profile=profile)
    source_vectors_path = data_paths.preferred_embedding_vectors_path(dataset_id=base_dataset_id, profile=profile)

    text_rows = _read_jsonl_rows(source_texts_path)
    metadata_rows = _read_jsonl_rows(source_metadata_path)
    with open(source_ids_path, "r", encoding="utf-8") as fh:
        chunk_ids = [str(item) for item in json.load(fh)]
    vectors = np.load(source_vectors_path)

    text_ids = [str(row["id"]) for row in text_rows]
    metadata_ids = [str(row["id"]) for row in metadata_rows]
    if text_ids != metadata_ids or text_ids != chunk_ids:
        raise RuntimeError("Source VMJv2 artifact alignment mismatch; audit before building supplement.")
    if len(chunk_ids) != int(vectors.shape[0]):
        raise RuntimeError("Source VMJv2 embeddings do not align with chunk IDs.")

    kept_text_rows: list[dict[str, Any]] = []
    kept_metadata_rows: list[dict[str, Any]] = []
    kept_chunk_ids: list[str] = []
    kept_indices: list[int] = []
    matched_titles: set[str] = set()
    matched_doc_ids: set[str] = set()

    for idx, (chunk_id, text_row, metadata_row) in enumerate(zip(chunk_ids, text_rows, metadata_rows)):
        metadata = metadata_row.get("metadata") or {}
        title = str(metadata.get("title") or "").strip()
        doc_id = str(metadata.get("doc_id") or "").strip()
        if title not in selected_titles and doc_id not in selected_doc_ids:
            continue
        kept_text_rows.append(text_row)
        kept_metadata_rows.append(metadata_row)
        kept_chunk_ids.append(chunk_id)
        kept_indices.append(idx)
        if title:
            matched_titles.add(title)
        if doc_id:
            matched_doc_ids.add(doc_id)

    kept_vectors = vectors[np.array(kept_indices, dtype=np.int64)]

    output_texts_path = data_paths.chunk_texts_export_path(dataset_id=output_dataset_id, profile=profile)
    output_metadata_path = data_paths.chunk_metadata_export_path(dataset_id=output_dataset_id, profile=profile)
    output_ids_path = data_paths.embedding_ids_path(dataset_id=output_dataset_id, profile=profile)
    output_vectors_path = data_paths.embedding_vectors_path(dataset_id=output_dataset_id, profile=profile)
    output_manifest_path = data_paths.embeddings_staging_dir(profile, dataset_id=output_dataset_id) / "embedding_manifest.json"
    report_output = data_paths.qa_root_dir() / f"vmj_v2_supplement_{output_dataset_id}_{profile}.json"

    _write_jsonl(output_texts_path, kept_text_rows)
    _write_jsonl(output_metadata_path, kept_metadata_rows)
    output_ids_path.parent.mkdir(parents=True, exist_ok=True)
    output_ids_path.write_text(json.dumps(kept_chunk_ids, ensure_ascii=False), encoding="utf-8")
    np.save(output_vectors_path, kept_vectors)
    manifest = {
        "dataset_id": output_dataset_id,
        "profile": profile,
        "source_dataset_id": base_dataset_id,
        "mode": mode,
        "vector_count": int(kept_vectors.shape[0]),
        "vector_dim": int(kept_vectors.shape[1]) if kept_vectors.ndim == 2 and kept_vectors.shape[0] else 0,
        "chunk_id_count": len(kept_chunk_ids),
    }
    output_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "output_dataset_id": output_dataset_id,
        "profile": profile,
        "mode": mode,
        "source_dataset_id": base_dataset_id,
        "selected_record_paths": [str(path) for path in selected_record_paths],
        "selected_title_count": len(selected_titles),
        "selected_doc_id_count": len(selected_doc_ids),
        "matched_title_count": len(matched_titles),
        "matched_doc_id_count": len(matched_doc_ids),
        "kept_chunk_count": len(kept_chunk_ids),
        "source_chunk_count": len(chunk_ids),
        "texts_output": str(output_texts_path),
        "metadata_output": str(output_metadata_path),
        "ids_output": str(output_ids_path),
        "vectors_output": str(output_vectors_path),
        "manifest_output": str(output_manifest_path),
    }
    report_output.parent.mkdir(parents=True, exist_ok=True)
    report_output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["report_output"] = str(report_output)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a reusable embedded supplement from legacy VMJ release v2.")
    parser.add_argument("--dataset-id", required=True, help="Output dataset ID for the supplement artifacts")
    parser.add_argument("--profile", default="multilingual")
    parser.add_argument(
        "--mode",
        default="strict_union",
        choices=["backfilled", "high_confidence", "strict_union", "all_salvage"],
        help="Which salvage subset to keep from the legacy VMJ release",
    )
    args = parser.parse_args()

    report = build_vmj_release_v2_supplement(
        output_dataset_id=args.dataset_id,
        profile=args.profile,
        mode=args.mode,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
