from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_run_repair_cycles_stops_after_idle(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))

    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    module = importlib.import_module("pipelines.crawl.run_repair_cycles")
    module = importlib.reload(module)

    repair_reports = iter(
        [
            {"repair_downloaded": 3, "repair_failed": 0},
            {"repair_downloaded": 0, "repair_failed": 1},
        ]
    )
    extract_reports = iter(
        [
            {"processed": 10, "missing_assets": 5, "pending": 0},
            {"processed": 12, "missing_assets": 5, "pending": 0},
        ]
    )
    gate_reports = iter(
        [
            {"missing_assets": 5, "pending": 0},
            {"missing_assets": 5, "pending": 0},
        ]
    )

    monkeypatch.setattr(module, "run_source", lambda **kwargs: next(repair_reports))
    monkeypatch.setattr(module, "extract_source", lambda source_id: next(extract_reports))
    monkeypatch.setattr(module, "write_extract_gate_report", lambda source_id: next(gate_reports))
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    report = module.run_repair_cycles(
        source_id="vmj_ojs",
        repair_batch_size=25,
        stop_after_idle_cycles=1,
    )

    assert report["cycles"] == 2
    assert report["totals"] == {"repair_downloaded": 3, "repair_failed": 1}

    summary_path = Path(report["summary_path"])
    assert summary_path.exists()
    snapshot = json.loads(summary_path.read_text(encoding="utf-8"))
    assert len(snapshot["cycles"]) == 2
    assert snapshot["cycles"][0]["repair_report"]["repair_downloaded"] == 3
    assert snapshot["cycles"][1]["gate_report"]["missing_assets"] == 5


def test_run_repair_cycles_validates_batch_size(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))

    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    module = importlib.import_module("pipelines.crawl.run_repair_cycles")
    module = importlib.reload(module)

    try:
        module.run_repair_cycles(source_id="vmj_ojs", repair_batch_size=0)
    except ValueError as exc:
        assert "repair_batch_size must be > 0" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid repair_batch_size")
