from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_source_registry_exposes_wave_grouping():
    module = importlib.import_module("pipelines.crawl.source_registry")
    source_ids = module.source_ids_for_wave(1)

    assert "medlineplus" in source_ids
    assert "who" in source_ids
    assert "vmj_ojs" not in source_ids


def test_run_wave_executes_sources_sequentially(monkeypatch):
    module = importlib.import_module("pipelines.crawl.run_wave")

    crawl_calls: list[str] = []
    extract_calls: list[str] = []

    monkeypatch.setattr(module, "run_source", lambda **kwargs: crawl_calls.append(kwargs["source_id"]) or {"source_id": kwargs["source_id"]})
    monkeypatch.setattr(module, "extract_source", lambda source_id: extract_calls.append(source_id) or {"source_id": source_id})

    report = module.run_wave(wave=1, resume=True, max_items=0, extract=True)

    assert report["wave"] == 1
    assert crawl_calls == report["sources"]
    assert extract_calls == report["sources"]
