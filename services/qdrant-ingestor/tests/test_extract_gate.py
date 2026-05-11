from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(monkeypatch, rag_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(rag_root.parent / "data"))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    crawl_manifest = importlib.import_module("services.utils.crawl_manifest")
    importlib.reload(crawl_manifest)
    module = importlib.import_module("pipelines.etl.extract_gate")
    return importlib.reload(module), crawl_manifest


def test_extract_gate_allows_medlineplus_multi_output(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_modules(monkeypatch, rag_root)

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
    records_path = rag_root / "sources" / "medlineplus" / "records" / "document_records.jsonl"
    records_path.parent.mkdir(parents=True, exist_ok=True)
    records_path.write_text(
        json.dumps({"doc_id": "m1", "title": "Hypertension", "body": "Body", "source_name": "MedlinePlus"}) + "\n",
        encoding="utf-8",
    )

    report = module.evaluate_extract_gate("medlineplus")

    assert report["gate_passed"] is True
    assert report["gate_reason"] == "medlineplus_multi_output_ok"
    assert report["records_count"] == 1


def test_extract_gate_breaks_down_missing_asset_siblings(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_modules(monkeypatch, rag_root)

    crawl_manifest.write_manifest(
        "vmj_ojs",
        [
            {
                "source_id": "vmj_ojs",
                "item_id": "legacy-done",
                "relative_path": "medical_crawl_seed/data_raw/vmj_ojs/files/sample.pdf",
                "content_class": "pdf",
                "extract_status": "done",
            },
            {
                "source_id": "vmj_ojs",
                "item_id": "sibling-missing",
                "relative_path": "sources/vmj_ojs/raw/sample.pdf",
                "content_class": "pdf",
                "extract_status": "missing_asset",
            },
            {
                "source_id": "vmj_ojs",
                "item_id": "needs-recrawl",
                "relative_path": "sources/vmj_ojs/raw/orphan.pdf",
                "content_class": "pdf",
                "extract_status": "missing_asset",
            },
        ],
    )
    processed_dir = rag_root / "sources" / "vmj_ojs" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    (processed_dir / "sample.txt").write_text("body", encoding="utf-8")

    report = module.evaluate_extract_gate("vmj_ojs")

    assert report["gate_passed"] is False
    assert report["missing_assets"] == 2
    assert report["missing_asset_breakdown"]["stale_sibling_missing"] == 1
    assert report["missing_asset_breakdown"]["needs_recrawl"] == 1
    assert report["etl_done_only_allowed"] is True


def test_extract_gate_allows_case_only_processed_alias_on_windows(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_modules(monkeypatch, rag_root)

    crawl_manifest.write_manifest(
        "nci_pdq",
        [
            {
                "source_id": "nci_pdq",
                "item_id": "a",
                "relative_path": "sources/nci_pdq/raw/GI-complications-hp-pdq.html",
                "content_class": "html",
                "extract_status": "done",
            },
            {
                "source_id": "nci_pdq",
                "item_id": "b",
                "relative_path": "sources/nci_pdq/raw/gi-complications-hp-pdq.html",
                "content_class": "html",
                "extract_status": "done",
            },
        ],
    )
    processed_dir = rag_root / "sources" / "nci_pdq" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    (processed_dir / "GI-complications-hp-pdq.txt").write_text("body", encoding="utf-8")

    report = module.evaluate_extract_gate("nci_pdq")

    assert report["processed_files"] == 1
    if sys.platform.startswith("win"):
        assert report["logical_done_outputs"] == 1
        assert report["gate_passed"] is True
    else:
        assert report["logical_done_outputs"] == 2


def test_extract_gate_marks_vien_dinh_duong_as_extract_healthy_but_not_article_ready(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_modules(monkeypatch, rag_root)

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "item_id": "html1",
                "relative_path": "sources/vien_dinh_duong/raw/bai-viet.html",
                "content_class": "html",
                "extract_status": "done",
                "extract_strategy": "html_text",
            },
            {
                "source_id": "vien_dinh_duong",
                "item_id": "pdf1",
                "relative_path": "sources/vien_dinh_duong/raw/so-tay.pdf",
                "content_class": "pdf",
                "extract_status": "deferred",
                "extract_strategy": "long_pdf_book",
            },
            {
                "source_id": "vien_dinh_duong",
                "item_id": "pdf2",
                "relative_path": "sources/vien_dinh_duong/raw/poster.pdf",
                "content_class": "pdf",
                "extract_status": "deferred",
                "extract_strategy": "image_pdf_backlog",
            },
            {
                "source_id": "vien_dinh_duong",
                "item_id": "office1",
                "relative_path": "sources/vien_dinh_duong/raw/quyet-dinh.docx",
                "content_class": "docx",
                "extract_status": "deferred",
                "extract_strategy": "office_backlog",
            },
        ],
    )
    processed_dir = rag_root / "sources" / "vien_dinh_duong" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    (processed_dir / "bai-viet.txt").write_text("body", encoding="utf-8")

    report = module.evaluate_extract_gate("vien_dinh_duong")

    assert report["extract_health_passed"] is True
    assert report["extract_health_reason"] == "ok"
    assert report["gate_passed"] is True
    assert report["gate_reason"] == "ok"
    assert report["quality_gate_status"] == "go"
    assert report["deferred_strategy_counts"]["long_pdf_book"] == 1
    assert report["deferred_strategy_counts"]["image_pdf_backlog"] == 1
    assert report["deferred_strategy_counts"]["office_backlog"] == 1
    assert report["unexpected_deferred_assets"] == 0
    assert report["etl_done_only_allowed"] is True


def test_extract_gate_allows_backlog_only_vien_dinh_duong_without_done(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_modules(monkeypatch, rag_root)

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "item_id": "pdf1",
                "relative_path": "sources/vien_dinh_duong/raw/so-tay.pdf",
                "content_class": "pdf",
                "extract_status": "deferred",
                "extract_strategy": "long_pdf_book",
            }
        ],
    )

    report = module.evaluate_extract_gate("vien_dinh_duong")

    assert report["extract_health_passed"] is True
    assert report["extract_health_reason"] == "backlog_only_deferred_assets"
    assert report["gate_passed"] is True
    assert report["gate_reason"] == "backlog_only_deferred_assets"
    assert report["etl_done_only_allowed"] is False


def test_extract_gate_allows_vmj_partition_release_when_only_needs_recrawl_backlog_remains(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_modules(monkeypatch, rag_root)

    crawl_manifest.write_manifest(
        "vmj_ojs",
        [
            {
                "source_id": "vmj_ojs",
                "relative_path": "medical_crawl_seed/data_raw/vmj_ojs/files/article-1.pdf",
                "content_class": "pdf",
                "extract_status": "done",
            },
            {
                "source_id": "vmj_ojs",
                "relative_path": "sources/vmj_ojs/raw/article-2.pdf",
                "content_class": "pdf",
                "extract_status": "missing_asset",
                "item_url": "https://example.org/article-2.pdf",
            },
            {
                "source_id": "vmj_ojs",
                "relative_path": "sources/vmj_ojs/raw/article-1.pdf",
                "content_class": "pdf",
                "extract_status": "deferred",
                "extract_strategy": "stale_sibling_backlog",
            },
        ],
    )
    processed_dir = rag_root / "sources" / "vmj_ojs" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    (processed_dir / "article-1.txt").write_text("body", encoding="utf-8")

    report = module.evaluate_extract_gate("vmj_ojs")

    assert report["extract_health_passed"] is False
    assert report["partition_release_allowed"] is True
    assert report["gate_passed"] is True
    assert report["gate_reason"] == "article_partition_backlog_isolated"
