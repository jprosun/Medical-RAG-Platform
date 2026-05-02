from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_extract_module(monkeypatch, rag_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(rag_root.parent / "data"))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    crawl_manifest = importlib.import_module("services.utils.crawl_manifest")
    importlib.reload(crawl_manifest)
    module = importlib.import_module("pipelines.crawl.extract_source")
    return importlib.reload(module), crawl_manifest


def test_extract_source_marks_missing_assets(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    crawl_manifest.write_manifest(
        "who_vietnam",
        [
            {
                "source_id": "who_vietnam",
                "item_id": "missing1",
                "relative_path": "sources/who_vietnam/raw/missing.html",
                "content_class": "html",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("who_vietnam")
    rows = crawl_manifest.read_manifest("who_vietnam")
    summary = json.loads((rag_root / "sources" / "who_vietnam" / "qa" / "extract_summary.json").read_text(encoding="utf-8"))

    assert report["failed"] == 1
    assert report["missing_assets"] == 1
    assert rows[0]["extract_status"] == "missing_asset"
    assert summary["missing_assets"] == 1
