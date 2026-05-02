from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_run_reference_concept_sources_calls_run_source_and_optional_extract(monkeypatch):
    module = importlib.import_module("pipelines.crawl.run_reference_concept_sources")

    crawl_calls: list[str] = []
    extract_calls: list[str] = []

    monkeypatch.setattr(module, "run_source", lambda **kwargs: crawl_calls.append(kwargs["source_id"]) or {"source_id": kwargs["source_id"]})
    monkeypatch.setattr(module, "extract_source", lambda source_id: extract_calls.append(source_id) or {"source_id": source_id})

    report = module.run_reference_concept_sources(
        source_ids=["nice_guidance", "nccih_health"],
        resume=True,
        max_items=0,
        extract=True,
    )

    assert crawl_calls == ["nice_guidance", "nccih_health"]
    assert extract_calls == ["nice_guidance", "nccih_health"]
    assert report["sources"] == ["nice_guidance", "nccih_health"]
