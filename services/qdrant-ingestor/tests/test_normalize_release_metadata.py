from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module(monkeypatch, rag_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    module = importlib.import_module("tools.normalize_release_metadata")
    return importlib.reload(module)


def test_normalize_record_backfills_core_metadata(monkeypatch, tmp_path):
    module = _reload_module(monkeypatch, tmp_path / "rag-data")
    record = {
        "source_id": "who",
        "source_name": "",
        "title": "Climate change",
        "body": "Body content long enough to keep the record valid for downstream embedding.",
        "section_title": "",
        "canonical_title": "",
        "heading_path": "Climate change",
        "source_url": "  ",
    }

    normalized, changes = module.normalize_record(record)

    assert normalized["source_name"] == "World Health Organization"
    assert normalized["section_title"] == "Full text"
    assert normalized["canonical_title"] == "Climate change"
    assert normalized["heading_path"] == "Climate change > Full text"
    assert "section_title" in changes
    assert "canonical_title" in changes


def test_normalize_record_repairs_uspstf_slug_title_and_source_url(monkeypatch, tmp_path):
    module = _reload_module(monkeypatch, tmp_path / "rag-data")
    record = {
        "source_id": "uspstf_recommendations",
        "source_name": "USPSTF Recommendations",
        "title": "abdominal-aortic-aneurysm-screening-1996",
        "body": "Abdominal Aortic Aneurysm: Screening, 1996\n\nRecommendation summary and supporting evidence.",
        "section_title": "Full text",
        "canonical_title": "abdominal-aortic-aneurysm-screening-1996",
        "heading_path": "abdominal-aortic-aneurysm-screening-1996 > Full text",
        "source_url": "",
        "processed_path": "rag-data/sources/uspstf_recommendations/processed/abdominal-aortic-aneurysm-screening-1996.txt",
    }

    normalized, changes = module.normalize_record(record)

    assert normalized["title"] == "Abdominal Aortic Aneurysm: Screening, 1996"
    assert normalized["canonical_title"] == "Abdominal Aortic Aneurysm: Screening, 1996"
    assert normalized["source_url"].endswith("/abdominal-aortic-aneurysm-screening-1996")
    assert "uspstf_title_from_body" in changes


def test_normalize_record_repairs_mojibake(monkeypatch, tmp_path):
    module = _reload_module(monkeypatch, tmp_path / "rag-data")
    record = {
        "source_id": "nci_pdq",
        "source_name": "NCI PDQ",
        "title": "Acupuncture (PDQÂ®)â€“Health Professional Version",
        "body": "Acupuncture (PDQÂ®)â€“Health Professional Version\n\nBody text.",
        "section_title": "Full text",
        "canonical_title": "",
        "heading_path": "",
        "source_url": "https://example.test/acupuncture-pdq",
    }

    normalized, _ = module.normalize_record(record)

    assert normalized["title"] == "Acupuncture (PDQ®)–Health Professional Version"
    assert "PDQÂ®" not in normalized["body"]


def test_normalize_record_flags_short_release_body(monkeypatch, tmp_path):
    module = _reload_module(monkeypatch, tmp_path / "rag-data")
    record = {
        "source_id": "ncbi_bookshelf",
        "source_name": "NCBI Bookshelf",
        "title": "Short section",
        "body": "Too short for embedding.",
        "section_title": "Full text",
        "canonical_title": "Short section",
        "heading_path": "Short section > Full text",
        "source_url": "https://example.test/short",
        "quality_status": "go",
        "quality_flags": [],
    }

    normalized, _ = module.normalize_record(record)

    assert "release_body_too_short" in normalized["quality_flags"]
    assert normalized["quality_status"] == "review"


def test_looks_like_mojibake_does_not_flag_valid_vietnamese(monkeypatch, tmp_path):
    module = _reload_module(monkeypatch, tmp_path / "rag-data")

    assert module._looks_like_mojibake("Âu Xuân Sâm", language="vi") is False
    assert module._looks_like_mojibake("PHÂN LẬP ĐƯỢC", language="vi") is False
    assert module._looks_like_mojibake("PDQÂ®", language="en") is True


def test_normalize_record_cleans_residual_artifacts(monkeypatch, tmp_path):
    module = _reload_module(monkeypatch, tmp_path / "rag-data")
    record = {
        "source_id": "nccih_health",
        "source_name": "NCCIH Health Topics",
        "title": "Download the HerbList TM appÂ today.",
        "body": "Download the HerbList TM appÂ today.",
        "section_title": "Full text",
        "canonical_title": "Download the HerbList TM appÂ today.",
        "heading_path": "Download the HerbList TM appÂ today. > Full text",
        "source_url": "https://www.nccih.nih.gov/health/herblist-app",
    }

    normalized, _ = module.normalize_record(record)

    assert "Â " not in normalized["title"]
    assert normalized["title"] == "Download the HerbList TM app today."
