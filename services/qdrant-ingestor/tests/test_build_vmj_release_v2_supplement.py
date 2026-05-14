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


def test_build_vmj_release_v2_supplement_strict_union_reuses_matching_chunks(tmp_path: Path, monkeypatch) -> None:
    rag_root = tmp_path / "rag-data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    module = importlib.import_module("tools.build_vmj_release_v2_supplement")
    module = importlib.reload(module)

    export_dir = rag_root / "embeddings" / "exports" / "vmj_ojs_release_v2" / "multilingual"
    staging_dir = rag_root / "embeddings" / "staging" / "vmj_ojs_release_v2" / "multilingual"
    salvage_dir = rag_root / "datasets" / "vmj_ojs_release_v2_salvage" / "records"

    _write_jsonl(
        export_dir / "chunk_texts_for_embed.jsonl",
        [
            {"id": "c1", "text": "A"},
            {"id": "c2", "text": "B"},
            {"id": "c3", "text": "C"},
        ],
    )
    _write_jsonl(
        export_dir / "chunk_metadata.jsonl",
        [
            {"id": "c1", "metadata": {"doc_id": "d1", "title": "Title A"}},
            {"id": "c2", "metadata": {"doc_id": "d2", "title": "Title B"}},
            {"id": "c3", "metadata": {"doc_id": "d3", "title": "Title C"}},
        ],
    )
    staging_dir.mkdir(parents=True, exist_ok=True)
    (staging_dir / "chunk_ids.json").write_text(json.dumps(["c1", "c2", "c3"]), encoding="utf-8")
    np.save(staging_dir / "embeddings.npy", np.arange(12, dtype=np.float32).reshape(3, 4))

    _write_jsonl(
        salvage_dir / "document_records_backfilled_article_url.jsonl",
        [{"doc_id": "d1", "title": "Title A"}],
    )
    _write_jsonl(
        salvage_dir / "document_records_issue_url_only_high_confidence.jsonl",
        [{"doc_id": "d3", "title": "Title C"}],
    )

    report = module.build_vmj_release_v2_supplement(
        output_dataset_id="vmj_sup",
        profile="multilingual",
        mode="strict_union",
    )

    assert report["kept_chunk_count"] == 2
    out_export = rag_root / "embeddings" / "exports" / "vmj_sup" / "multilingual"
    out_staging = rag_root / "embeddings" / "staging" / "vmj_sup" / "multilingual"
    out_ids = json.loads((out_staging / "chunk_ids.json").read_text(encoding="utf-8"))
    assert out_ids == ["c1", "c3"]
    out_vecs = np.load(out_staging / "embeddings.npy")
    assert out_vecs.shape == (2, 4)
    assert (out_export / "chunk_texts_for_embed.jsonl").exists()
    assert (out_export / "chunk_metadata.jsonl").exists()
