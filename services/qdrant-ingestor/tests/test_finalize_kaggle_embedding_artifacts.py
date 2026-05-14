import json
from pathlib import Path

import numpy as np

from services.utils import data_paths
from tools.kaggle.finalize_kaggle_embedding_artifacts import finalize_artifacts


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _metadata(index: int) -> dict:
    return {
        "doc_id": f"doc-{index}",
        "article_id": f"article-{index}",
        "title": f"Title {index}",
        "canonical_title": f"Title {index}",
        "source_id": "unit_test",
        "source_name": "Unit Test",
        "doc_type": "research_article",
        "specialty": "general",
        "chunk_index": index,
        "section_title": "Abstract",
    }


def _write_export_files(rag_root: Path, dataset_id: str, profile: str, ids: list[str]) -> None:
    export_dir = rag_root / "embeddings" / "exports" / dataset_id / profile
    text_rows = [{"id": chunk_id, "text": f"Text {idx}"} for idx, chunk_id in enumerate(ids)]
    metadata_rows = [
        {"id": chunk_id, "metadata": _metadata(idx)}
        for idx, chunk_id in enumerate(ids)
    ]
    kaggle_rows = [
        {"id": chunk_id, "text": f"Text {idx}", "metadata": _metadata(idx)}
        for idx, chunk_id in enumerate(ids)
    ]
    _write_jsonl(export_dir / "chunk_texts_for_embed.jsonl", text_rows)
    _write_jsonl(export_dir / "chunk_metadata.jsonl", metadata_rows)
    _write_jsonl(export_dir / "kaggle_embedding_input.jsonl", kaggle_rows)


def _write_kaggle_output(input_dir: Path, ids: list[str], vectors: np.ndarray) -> None:
    input_dir.mkdir(parents=True, exist_ok=True)
    np.save(input_dir / "embeddings.npy", vectors.astype(np.float32))
    (input_dir / "chunk_ids.json").write_text(
        json.dumps(ids, ensure_ascii=False),
        encoding="utf-8",
    )
    (input_dir / "embedding_manifest.json").write_text(
        json.dumps(
            {
                "model_name": "BAAI/bge-m3",
                "chunk_count": len(ids),
                "vector_shape": list(vectors.shape),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_finalize_kaggle_artifacts_copies_and_audits(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    monkeypatch.setattr(data_paths, "RAG_DATA_ROOT", rag_root)
    dataset_id = "unit_release"
    profile = "multilingual"
    ids = ["chunk-1", "chunk-2"]
    _write_export_files(rag_root, dataset_id, profile, ids)
    input_dir = tmp_path / "kaggle-output"
    _write_kaggle_output(input_dir, ids, np.ones((2, 4), dtype=np.float32))

    result = finalize_artifacts(
        input_dir=input_dir,
        dataset_id=dataset_id,
        profile=profile,
        collection="unit_collection",
        expected_dim=4,
    )

    staging_dir = rag_root / "embeddings" / "staging" / dataset_id / profile
    assert result["status"] == "pass"
    assert (staging_dir / "embeddings.npy").exists()
    assert (staging_dir / "chunk_ids.json").exists()
    assert result["validation"]["vector_shape"] == [2, 4]
    assert "unit_collection" in result["import_commands"]["powershell"]


def test_finalize_kaggle_artifacts_rejects_vector_id_mismatch(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    monkeypatch.setattr(data_paths, "RAG_DATA_ROOT", rag_root)
    dataset_id = "unit_release"
    profile = "multilingual"
    ids = ["chunk-1", "chunk-2"]
    _write_export_files(rag_root, dataset_id, profile, ids)
    input_dir = tmp_path / "kaggle-output"
    _write_kaggle_output(input_dir, ids, np.ones((1, 4), dtype=np.float32))

    try:
        finalize_artifacts(
            input_dir=input_dir,
            dataset_id=dataset_id,
            profile=profile,
            expected_dim=4,
        )
    except ValueError as exc:
        assert "Vector/id mismatch" in str(exc)
    else:
        raise AssertionError("Expected vector/id mismatch to fail")
