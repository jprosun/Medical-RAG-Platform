from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
if str(INGESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INGESTOR_ROOT))


def _reload_module(monkeypatch, rag_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    module = importlib.import_module("tools.promote_etl_sources_to_data_proceed")
    return importlib.reload(module)


def test_promote_source_copies_records_and_processed(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    source_records = rag_root / "sources" / "nhs_health_a_z" / "records" / "document_records.jsonl"
    processed = rag_root / "sources" / "nhs_health_a_z" / "processed" / "acute-pancreatitis.txt"
    source_records.parent.mkdir(parents=True, exist_ok=True)
    processed.parent.mkdir(parents=True, exist_ok=True)
    processed.write_text("body", encoding="utf-8")
    source_records.write_text(
        json.dumps(
            {
                "doc_id": "a",
                "title": "Acute pancreatitis",
                "body": "body",
                "source_name": "NHS Health A-Z",
                "source_id": "nhs_health_a_z",
                "processed_path": "rag-data/sources/nhs_health_a_z/processed/acute-pancreatitis.txt",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    module = _reload_module(monkeypatch, rag_root)
    report = module.promote_source("nhs_health_a_z")

    target_records = rag_root / "data_proceed" / "nhs_health_a_z" / "records" / "document_records.jsonl"
    target_processed = rag_root / "data_proceed" / "nhs_health_a_z" / "processed" / "acute-pancreatitis.txt"
    target_summary = rag_root / "data_proceed" / "nhs_health_a_z" / "summary.json"

    assert report["records_exists"] is True
    assert report["processed_promoted"] == 1
    assert target_records.exists()
    assert target_processed.exists()
    assert target_summary.exists()
