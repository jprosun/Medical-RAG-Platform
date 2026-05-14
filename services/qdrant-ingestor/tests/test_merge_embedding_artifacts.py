from __future__ import annotations

import importlib
import json
from pathlib import Path

import numpy as np


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _write_dataset(base: Path, dataset_id: str, ids: list[str], vectors: np.ndarray) -> None:
    export_dir = base / "embeddings" / "exports" / dataset_id / "multilingual"
    staging_dir = base / "embeddings" / "staging" / dataset_id / "multilingual"
    _write_jsonl(
        export_dir / "chunk_texts_for_embed.jsonl",
        [{"id": chunk_id, "text": f"text-{chunk_id}"} for chunk_id in ids],
    )
    _write_jsonl(
        export_dir / "chunk_metadata.jsonl",
        [{"id": chunk_id, "metadata": {"source_id": dataset_id}} for chunk_id in ids],
    )
    staging_dir.mkdir(parents=True, exist_ok=True)
    (staging_dir / "chunk_ids.json").write_text(json.dumps(ids), encoding="utf-8")
    np.save(staging_dir / "embeddings.npy", vectors)


def test_merge_embedding_artifacts_merges_exports_and_staging(tmp_path: Path, monkeypatch) -> None:
    rag_root = tmp_path / "rag-data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    module = importlib.import_module("tools.merge_embedding_artifacts")
    module = importlib.reload(module)

    _write_dataset(rag_root, "a", ["a1"], np.ones((1, 3), dtype=np.float32))
    _write_dataset(rag_root, "b", ["b1", "b2"], np.ones((2, 3), dtype=np.float32) * 2)

    report = module.merge_embedding_artifacts(
        input_dataset_ids=["a", "b"],
        output_dataset_id="merged_embed",
        profile="multilingual",
    )

    assert report["merged_counts"]["ids"] == 3
    out_export = rag_root / "embeddings" / "exports" / "merged_embed" / "multilingual"
    out_staging = rag_root / "embeddings" / "staging" / "merged_embed" / "multilingual"
    assert (out_export / "chunk_texts_for_embed.jsonl").exists()
    assert (out_export / "chunk_metadata.jsonl").exists()
    ids = json.loads((out_staging / "chunk_ids.json").read_text(encoding="utf-8"))
    assert ids == ["a1", "b1", "b2"]
    vectors = np.load(out_staging / "embeddings.npy")
    assert vectors.shape == (3, 3)
