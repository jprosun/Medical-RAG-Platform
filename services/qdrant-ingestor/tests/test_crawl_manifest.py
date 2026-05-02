from __future__ import annotations

import csv
import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_manifest_module(monkeypatch, rag_root: Path, legacy_root: Path | None = None):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root or (rag_root.parent / "data")))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    module = importlib.import_module("services.utils.crawl_manifest")
    return importlib.reload(module)


def test_bootstrap_source_manifest_from_raw_and_catalog(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    raw_file = rag_root / "sources" / "who" / "raw" / "topic.html"
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    raw_file.write_text("<html>Topic</html>", encoding="utf-8")

    legacy_seed = rag_root / "legacy_seed" / "seed.pdf"
    legacy_seed.parent.mkdir(parents=True, exist_ok=True)
    legacy_seed.write_bytes(b"%PDF-1.4 seed")

    catalog = rag_root / "corpus_catalog.csv"
    with open(catalog, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "source_id",
                "institution_or_journal",
                "file_name",
                "relative_path",
                "extension",
                "file_size_kb",
                "title",
                "item_type",
                "item_url",
                "file_url",
                "sha256",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "source_id": "who",
                "institution_or_journal": "WHO",
                "file_name": "seed.pdf",
                "relative_path": "legacy_seed/seed.pdf",
                "extension": ".pdf",
                "file_size_kb": "1",
                "title": "Seed PDF",
                "item_type": "seed_asset",
                "item_url": "https://example.org/item",
                "file_url": "https://example.org/file.pdf",
                "sha256": "abc123",
            }
        )

    module = _reload_manifest_module(monkeypatch, rag_root)
    report = module.bootstrap_source_manifest("who")
    rows = module.read_manifest("who")

    assert report["added"] == 2
    assert len(rows) == 2
    notes = {row["notes"] for row in rows}
    assert "bootstrapped_from_existing_raw" in notes
    assert "bootstrapped_from_corpus_catalog" in notes


def test_build_corpus_catalog_dedups_by_relative_path(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_manifest_module(monkeypatch, rag_root)

    asset_a = rag_root / "sources" / "who" / "raw" / "a.html"
    asset_b = rag_root / "sources" / "who" / "raw" / "b.html"
    asset_a.parent.mkdir(parents=True, exist_ok=True)
    asset_a.write_text("a", encoding="utf-8")
    asset_b.write_text("b", encoding="utf-8")

    module.write_manifest(
        "who",
        [
            {
                "source_id": "who",
                "item_id": "1",
                "relative_path": "sources/who/raw/a.html",
                "extension": ".html",
                "item_type": "fact_sheet",
                "title_hint": "A",
                "sha256": "sha-a",
            },
            {
                "source_id": "who",
                "item_id": "2",
                "relative_path": "sources/who/raw/a.html",
                "extension": ".html",
                "item_type": "fact_sheet",
                "title_hint": "Alias",
                "sha256": "sha-a",
                "duplicate_status": "alias_same_source",
                "duplicate_of": "1",
            },
            {
                "source_id": "who",
                "item_id": "3",
                "relative_path": "sources/who/raw/b.html",
                "extension": ".html",
                "item_type": "fact_sheet",
                "title_hint": "B",
                "sha256": "sha-b",
            },
        ],
    )

    report = module.build_corpus_catalog(source_ids=["who"])
    rows = list(csv.DictReader((rag_root / "corpus_catalog.csv").open("r", encoding="utf-8")))

    assert report["rows"] == 2
    assert len(rows) == 2
    assert {row["relative_path"] for row in rows} == {"sources/who/raw/a.html", "sources/who/raw/b.html"}


def test_complete_row_and_sha_alias_ignore_missing_blob_but_accept_legacy_blob(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_blob = tmp_path / "medical_crawl_seed" / "data_raw" / "vmj_ojs" / "files" / "legacy.pdf"
    legacy_blob.parent.mkdir(parents=True, exist_ok=True)
    legacy_blob.write_bytes(b"%PDF-1.4 legacy")

    module = _reload_manifest_module(monkeypatch, rag_root)

    legacy_row = {
        "source_id": "vmj_ojs",
        "item_id": "legacy",
        "relative_path": "medical_crawl_seed/data_raw/vmj_ojs/files/legacy.pdf",
        "http_status": "200",
        "sha256": "sha-legacy",
    }
    missing_row = {
        "source_id": "vmj_ojs",
        "item_id": "missing",
        "relative_path": "sources/vmj_ojs/raw/missing.pdf",
        "http_status": "200",
        "sha256": "sha-missing",
    }

    assert module.is_complete_row(legacy_row) is True
    assert module.is_complete_row(missing_row) is False
    assert module.first_row_for_sha([missing_row, legacy_row], "sha-missing") is None
    assert module.first_row_for_sha([missing_row, legacy_row], "sha-legacy") == legacy_row
