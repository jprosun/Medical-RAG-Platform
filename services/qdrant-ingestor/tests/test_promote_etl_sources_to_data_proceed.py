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
    crawl_manifest = importlib.import_module("services.utils.crawl_manifest")
    importlib.reload(crawl_manifest)
    extract_gate = importlib.import_module("pipelines.etl.extract_gate")
    importlib.reload(extract_gate)
    module = importlib.import_module("tools.promote_etl_sources_to_data_proceed")
    return importlib.reload(module), crawl_manifest


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

    module, crawl_manifest = _reload_module(monkeypatch, rag_root)
    crawl_manifest.write_manifest(
        "nhs_health_a_z",
        [
            {
                "source_id": "nhs_health_a_z",
                "item_id": "a",
                "relative_path": "sources/nhs_health_a_z/raw/acute-pancreatitis.html",
                "content_class": "html",
                "extract_status": "done",
            }
        ],
    )
    report = module.promote_source("nhs_health_a_z")

    target_records = rag_root / "data_proceed" / "nhs_health_a_z" / "records" / "document_records.jsonl"
    target_processed = rag_root / "data_proceed" / "nhs_health_a_z" / "processed" / "acute-pancreatitis.txt"
    target_summary = rag_root / "data_proceed" / "nhs_health_a_z" / "summary.json"

    assert report["records_exists"] is True
    assert report["processed_promoted"] == 1
    assert target_records.exists()
    assert target_processed.exists()
    assert target_summary.exists()


def test_promote_source_blocks_when_extract_gate_fails(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    source_records = rag_root / "sources" / "who_vietnam" / "records" / "document_records.jsonl"
    source_records.parent.mkdir(parents=True, exist_ok=True)
    source_records.write_text(
        json.dumps(
            {
                "doc_id": "wv1",
                "title": "WHO Vietnam",
                "body": "body",
                "source_name": "WHO Vietnam",
                "source_id": "who_vietnam",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    module, crawl_manifest = _reload_module(monkeypatch, rag_root)
    crawl_manifest.write_manifest(
        "who_vietnam",
        [
            {
                "source_id": "who_vietnam",
                "item_id": "wv-pending",
                "relative_path": "sources/who_vietnam/raw/pending.html",
                "content_class": "html",
                "extract_status": "pending",
            }
        ],
    )

    report = module.promote_source("who_vietnam")
    target_records = rag_root / "data_proceed" / "who_vietnam" / "records" / "document_records.jsonl"

    assert report["blocked_by_gate"] is True
    assert report["gate"]["gate_reason"] == "pending_assets_remaining"
    assert not target_records.exists()


def test_promote_source_synthesizes_processed_when_missing_processed_path(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    source_records = rag_root / "sources" / "medlineplus" / "records" / "document_records.jsonl"
    source_records.parent.mkdir(parents=True, exist_ok=True)
    source_records.write_text(
        json.dumps(
            {
                "doc_id": "med1",
                "title": "Hypertension",
                "body": "Hypertension is persistently elevated blood pressure that increases cardiovascular risk." * 2,
                "source_name": "MedlinePlus",
                "source_id": "medlineplus",
                "source_url": "https://medlineplus.gov/hypertension.html",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    module, crawl_manifest = _reload_module(monkeypatch, rag_root)
    crawl_manifest.write_manifest(
        "medlineplus",
        [
            {
                "source_id": "medlineplus",
                "item_id": "xml1",
                "relative_path": "sources/medlineplus/raw/mplus_topics.xml",
                "content_class": "xml",
                "extract_status": "done",
            }
        ],
    )

    report = module.promote_source("medlineplus")
    target_processed_dir = rag_root / "data_proceed" / "medlineplus" / "processed"
    synthetic_files = sorted(target_processed_dir.glob("*.txt"))

    assert report["blocked_by_gate"] is False
    assert report["synthetic_processed"] == 1
    assert len(synthetic_files) == 1
    assert "Hypertension" in synthetic_files[0].read_text(encoding="utf-8")
