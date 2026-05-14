from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_audit(monkeypatch, rag_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    audit = importlib.import_module("tools.audit_embedding_artifacts")
    return importlib.reload(audit)


def test_embedding_artifact_audit_passes_when_ids_texts_metadata_and_vectors_align(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    export_dir = rag_root / "embeddings" / "exports" / "demo" / "multilingual"
    staging_dir = rag_root / "embeddings" / "staging" / "demo" / "multilingual"
    export_dir.mkdir(parents=True)
    staging_dir.mkdir(parents=True)
    ids = ["c1", "c2"]
    (staging_dir / "chunk_ids.json").write_text(json.dumps(ids), encoding="utf-8")
    np.save(staging_dir / "embeddings.npy", np.zeros((2, 3), dtype=np.float32))
    (export_dir / "chunk_metadata.jsonl").write_text(
        "\n".join(json.dumps({"id": cid, "metadata": {}}) for cid in ids) + "\n",
        encoding="utf-8",
    )
    (export_dir / "chunk_texts_for_embed.jsonl").write_text(
        "\n".join(json.dumps({"id": cid, "text": cid}) for cid in ids) + "\n",
        encoding="utf-8",
    )
    (export_dir / "kaggle_embedding_input.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "id": cid,
                    "text": cid,
                    "metadata": {
                        "doc_id": cid,
                        "article_id": cid,
                        "title": f"Title {cid}",
                        "canonical_title": f"Title {cid}",
                        "source_id": "demo",
                        "source_name": "Demo",
                        "doc_type": "research_article",
                        "specialty": "general",
                        "chunk_index": 0,
                        "section_title": "Full text",
                    },
                }
            )
            for cid in ids
        ) + "\n",
        encoding="utf-8",
    )

    audit = _reload_audit(monkeypatch, rag_root)

    report = audit.audit_embedding_artifacts("demo", "multilingual")

    assert report["status"] == "pass"
    assert report["counts"]["chunk_ids"] == 2


def test_embedding_artifact_audit_fails_when_text_ids_do_not_match(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    export_dir = rag_root / "embeddings" / "exports" / "demo" / "multilingual"
    staging_dir = rag_root / "embeddings" / "staging" / "demo" / "multilingual"
    export_dir.mkdir(parents=True)
    staging_dir.mkdir(parents=True)
    (staging_dir / "chunk_ids.json").write_text(json.dumps(["c1", "c2"]), encoding="utf-8")
    np.save(staging_dir / "embeddings.npy", np.zeros((2, 3), dtype=np.float32))
    (export_dir / "chunk_metadata.jsonl").write_text(
        '{"id":"c1","metadata":{}}\n{"id":"c2","metadata":{}}\n',
        encoding="utf-8",
    )
    (export_dir / "chunk_texts_for_embed.jsonl").write_text(
        '{"id":"c1","text":"c1"}\n',
        encoding="utf-8",
    )
    (export_dir / "kaggle_embedding_input.jsonl").write_text(
        '{"id":"c1","text":"c1","metadata":{}}\n',
        encoding="utf-8",
    )

    audit = _reload_audit(monkeypatch, rag_root)

    report = audit.audit_embedding_artifacts("demo", "multilingual")

    assert report["status"] == "fail"
    assert report["checks"]["ids_match_text_set"] is False
