from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_run_source_batches_stops_after_idle(monkeypatch, tmp_path):
    module = importlib.import_module("pipelines.crawl.run_source_batches")

    monkeypatch.setattr(module, "source_qa_dir", lambda source_id: tmp_path / source_id / "qa")
    monkeypatch.setattr(module, "make_run_id", lambda prefix, source_id: f"{prefix}_{source_id}_test")

    reports = iter(
        [
            {"source_id": "vmj_ojs", "downloaded": 3, "skipped": 0, "failed": 0},
            {"source_id": "vmj_ojs", "downloaded": 2, "skipped": 1, "failed": 0},
            {"source_id": "vmj_ojs", "downloaded": 0, "skipped": 5, "failed": 0},
        ]
    )
    monkeypatch.setattr(module, "run_source", lambda **kwargs: next(reports))

    result = module.run_source_batches(
        source_id="vmj_ojs",
        batch_size=25,
        resume=True,
        stop_after_idle_batches=1,
    )

    assert result["batches"] == 3
    assert result["totals"] == {"downloaded": 5, "skipped": 6, "failed": 0}

    summary_path = Path(result["summary_path"])
    assert summary_path.exists()
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["source_id"] == "vmj_ojs"
    assert len(payload["batches"]) == 3
