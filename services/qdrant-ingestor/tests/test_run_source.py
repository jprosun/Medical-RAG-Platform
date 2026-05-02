from __future__ import annotations

import csv
import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_run_source(monkeypatch, rag_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(rag_root.parent / "data"))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    crawl_manifest = importlib.import_module("services.utils.crawl_manifest")
    importlib.reload(crawl_manifest)
    module = importlib.import_module("pipelines.crawl.run_source")
    return importlib.reload(module)


def _write_catalog(path: Path, row: dict[str, str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
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
        writer.writerow(row)


def test_run_source_seed_catalog_skips_complete_manifest_entry(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    raw_file = rag_root / "sources" / "cantho_med_journal" / "raw" / "seed.pdf"
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    raw_file.write_bytes(b"seed-pdf")

    catalog = rag_root / "corpus_catalog.csv"
    _write_catalog(
        catalog,
        {
            "source_id": "cantho_med_journal",
            "institution_or_journal": "Cantho",
            "file_name": "seed.pdf",
            "relative_path": "sources/cantho_med_journal/raw/seed.pdf",
            "extension": ".pdf",
            "file_size_kb": "1",
            "title": "Seed PDF",
            "item_type": "journal_pdf",
            "item_url": "https://example.org/item",
            "file_url": "https://example.org/file.pdf",
            "sha256": "sha-seed",
        },
    )

    module = _reload_run_source(monkeypatch, rag_root)

    def _should_not_download(url: str):
        raise AssertionError(f"Unexpected download attempt for {url}")

    monkeypatch.setattr(module, "_download_bytes", _should_not_download)

    report = module.run_source(source_id="cantho_med_journal", resume=True)
    assert report["skipped"] == 1
    assert report["downloaded"] == 0


def test_register_download_marks_alias_same_source(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_run_source(monkeypatch, rag_root)

    existing_path = rag_root / "sources" / "who" / "raw" / "existing.html"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_bytes(b"same-body")
    existing_sha = module.file_sha256(existing_path)
    rows = [
        {
            "source_id": "who",
            "item_id": "old1",
            "item_url": "https://example.org/old",
            "file_url": "",
            "relative_path": "sources/who/raw/existing.html",
            "sha256": existing_sha,
            "http_status": "200",
        }
    ]

    row, status = module._register_download(
        source_id="who",
        rows=rows,
        item_type="fact_sheet",
        title_hint="New",
        item_url="https://example.org/new",
        file_url="",
        parent_item_url="",
        filename_hint="new.html",
        content=b"same-body",
        metadata={"http_status": "200", "mime_type": "text/html"},
        crawl_run_id="crawl_x",
        discovered_at="2026-04-27T00:00:00Z",
        downloaded_at="2026-04-27T00:00:01Z",
    )

    assert status == "alias_same_source"
    assert row["duplicate_status"] == "alias_same_source"
    assert row["duplicate_of"] == "old1"
    assert row["relative_path"] == "sources/who/raw/existing.html"


def test_register_download_marks_updated_asset_for_same_key(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_run_source(monkeypatch, rag_root)

    existing_path = rag_root / "sources" / "who" / "raw" / "topic.html"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_bytes(b"old-body")
    existing_sha = module.file_sha256(existing_path)
    rows = [
        {
            "source_id": "who",
            "item_id": "old1",
            "item_url": "https://example.org/topic",
            "file_url": "",
            "relative_path": "sources/who/raw/topic.html",
            "sha256": existing_sha,
            "http_status": "200",
        }
    ]

    row, status = module._register_download(
        source_id="who",
        rows=rows,
        item_type="fact_sheet",
        title_hint="Topic",
        item_url="https://example.org/topic",
        file_url="",
        parent_item_url="",
        filename_hint="topic.html",
        content=b"new-body",
        metadata={"http_status": "200", "mime_type": "text/html"},
        crawl_run_id="crawl_x",
        discovered_at="2026-04-27T00:00:00Z",
        downloaded_at="2026-04-27T00:00:01Z",
    )

    assert status == "updated_asset"
    assert row["duplicate_status"] == "updated_asset"
    assert row["duplicate_of"] == "old1"
    assert row["relative_path"] != "sources/who/raw/topic.html"


def test_who_candidate_filters_keep_scope(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_run_source(monkeypatch, rag_root)

    assert module._is_who_candidate_url("https://www.who.int/health-topics/diabetes")
    assert module._is_who_candidate_url("https://www.who.int/news-room/fact-sheets/detail/hypertension")
    assert module._is_who_candidate_url("https://cdn.who.int/media/docs/default-source/test.pdf")
    assert not module._is_who_candidate_url("https://www.who.int/mega-menu/about-us/governance")
    assert not module._is_who_candidate_url("https://example.org/fact-sheets/detail/test")


def test_who_vietnam_candidate_filters_focus_on_vietnam(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_run_source(monkeypatch, rag_root)

    assert module._is_who_vietnam_candidate_url("https://www.who.int/vietnam/news/detail/test")
    assert module._is_who_vietnam_candidate_url("https://www.who.int/westernpacific/countries/viet-nam")
    assert module._is_who_vietnam_candidate_url("https://cdn.who.int/media/docs/default-source/vietnam/test.pdf")
    assert not module._is_who_vietnam_candidate_url("https://www.who.int/news-room/fact-sheets/detail/hypertension")
    assert not module._is_who_vietnam_candidate_url("https://www.who.int/mega-menu/about-us/governance")


def test_ncbi_bookshelf_runner_uses_broader_discovery_without_legacy_100_cap(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_run_source(monkeypatch, rag_root)

    discovered_ids = [str(1000 + index) for index in range(150)]
    etl_module = importlib.import_module("pipelines.etl.ncbi_bookshelf_scraper")
    monkeypatch.setattr(etl_module, "discover_bookshelf_ids", lambda **kwargs: discovered_ids)
    monkeypatch.setattr(
        module,
        "_download_bytes",
        lambda url: (f"<html><body>{url}</body></html>".encode("utf-8"), {"http_status": "200", "mime_type": "text/html"}),
    )
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    report = module.run_source(source_id="ncbi_bookshelf", resume=True)
    assert report["downloaded"] == 150
    manifest = (rag_root / "sources" / "ncbi_bookshelf" / "manifest.csv").read_text(encoding="utf-8")
    assert "https://www.ncbi.nlm.nih.gov/books/1149/" in manifest


def test_extract_vmj_issue_urls_from_html(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_run_source(monkeypatch, rag_root)

    html = """
    <html><body>
      <a href="/index.php/vmj/issue/view/389">Issue 389</a>
      <a href="/index.php/vmj/issue/archive/2">Next</a>
    </body></html>
    """
    issues, pages = module._extract_vmj_issue_urls_from_html("https://tapchiyhocvietnam.vn/index.php/vmj/issue/archive", html)

    assert "https://tapchiyhocvietnam.vn/index.php/vmj/issue/view/389" in issues
    assert "https://tapchiyhocvietnam.vn/index.php/vmj/issue/archive/2" in pages


def test_extract_vmj_article_entries_from_html(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_run_source(monkeypatch, rag_root)

    html = """
    <html><body>
      <a href="/index.php/vmj/article/view/17924">Example Article</a>
      <a href="/index.php/vmj/article/view/17924/15235">PDF</a>
    </body></html>
    """
    entries = module._extract_vmj_article_entries_from_html(
        "https://tapchiyhocvietnam.vn/index.php/vmj/issue/view/389",
        html,
    )

    assert len(entries) == 1
    assert entries[0]["article_url"] == "https://tapchiyhocvietnam.vn/index.php/vmj/article/view/17924"
    assert entries[0]["file_url"] == "https://tapchiyhocvietnam.vn/index.php/vmj/article/view/17924/15235"
    assert entries[0]["title"] == "Example Article"


def test_extract_vmj_direct_download_url_from_wrapper_html(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_run_source(monkeypatch, rag_root)

    html = """
    <html><body>
      <a class="download" href="https://tapchiyhocvietnam.vn/index.php/vmj/article/download/17924/15235/30360">Tải xuống</a>
      <iframe src="https://tapchiyhocvietnam.vn/plugins/generic/pdfJsViewer/pdf.js/web/viewer.html?file=https%3A%2F%2Ftapchiyhocvietnam.vn%2Findex.php%2Fvmj%2Farticle%2Fdownload%2F17924%2F15235%2F30360"></iframe>
    </body></html>
    """
    direct = module._extract_vmj_direct_download_url_from_html(
        "https://tapchiyhocvietnam.vn/index.php/vmj/article/view/17924/15235",
        html,
    )

    assert direct == "https://tapchiyhocvietnam.vn/index.php/vmj/article/download/17924/15235/30360"


def test_repair_vmj_ojs_rows_rewrites_view_links_and_resets_status(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_run_source(monkeypatch, rag_root)

    raw_path = rag_root / "sources" / "vmj_ojs" / "raw" / "15077_12912.pdf"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(
        '<html><body><a class="download" href="https://tapchiyhocvietnam.vn/index.php/vmj/article/download/15077/12912/30001">Tải xuống</a></body></html>',
        encoding="utf-8",
    )
    processed_path = rag_root / "sources" / "vmj_ojs" / "processed" / "15077_12912.txt"
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed_path.write_text("bad extracted html", encoding="utf-8")

    rows = [
        {
            "source_id": "vmj_ojs",
            "file_url": "https://tapchiyhocvietnam.vn/index.php/vmj/article/view/15077/12912",
            "relative_path": "sources/vmj_ojs/raw/15077_12912.pdf",
            "content_class": "html",
            "mime_type": "text/html",
            "http_status": "200",
            "sha256": "abc",
            "extract_status": "done",
            "extract_strategy": "html_text",
        }
    ]

    repaired = module._repair_vmj_ojs_rows(rows)

    assert repaired == 1
    assert rows[0]["file_url"] == "https://tapchiyhocvietnam.vn/index.php/vmj/article/download/15077/12912/30001"
    assert rows[0]["content_class"] == "pdf"
    assert rows[0]["extract_status"] == "pending"
    assert rows[0]["sha256"] == ""
    assert not raw_path.exists()
    assert not processed_path.exists()
