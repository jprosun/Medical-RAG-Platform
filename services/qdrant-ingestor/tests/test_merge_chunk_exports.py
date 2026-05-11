from __future__ import annotations

import importlib
import json
from pathlib import Path


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_merge_chunk_exports_merges_without_duplicate_ids(tmp_path: Path, monkeypatch) -> None:
    rag_root = tmp_path / "rag-data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    module = importlib.import_module("tools.kaggle.merge_chunk_exports")
    module = importlib.reload(module)

    export_a = rag_root / "embeddings" / "exports" / "a" / "multilingual"
    export_b = rag_root / "embeddings" / "exports" / "b" / "multilingual"

    _write_jsonl(export_a / "chunk_texts_for_embed.jsonl", [{"id": "a1", "text": "A"}])
    _write_jsonl(export_a / "chunk_metadata.jsonl", [{"id": "a1", "metadata": {"source_id": "a"}}])
    _write_jsonl(export_b / "chunk_texts_for_embed.jsonl", [{"id": "b1", "text": "B"}])
    _write_jsonl(export_b / "chunk_metadata.jsonl", [{"id": "b1", "metadata": {"source_id": "b"}}])

    report = module.merge_chunk_exports(
        input_dataset_ids=["a", "b"],
        output_dataset_id="merged",
        profile="multilingual",
    )

    assert report["merged_counts"]["texts"] == 2
    out_dir = rag_root / "embeddings" / "exports" / "merged" / "multilingual"
    assert (out_dir / "chunk_texts_for_embed.jsonl").exists()
    assert (out_dir / "chunk_metadata.jsonl").exists()
