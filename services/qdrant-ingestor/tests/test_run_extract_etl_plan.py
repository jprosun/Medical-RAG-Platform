from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_group4_defaults_extract_only_selected_complex_sources(monkeypatch):
    module = importlib.import_module("pipelines.etl.run_extract_etl_plan")
    module = importlib.reload(module)

    calls: list[tuple[str, bool]] = []

    def fake_extract_source(source_id: str, *, force: bool = False, reconcile_only: bool = False):
        calls.append((source_id, reconcile_only))
        return {"source_id": source_id, "reconcile_only": reconcile_only}

    monkeypatch.setattr(module, "extract_source", fake_extract_source)
    monkeypatch.setattr(
        module,
        "write_extract_gate_report",
        lambda source_id: {"source_id": source_id, "gate_passed": False, "gate_reason": "stub"},
    )

    report = module.run_group(group_name="group4", dry_run=True)

    source_reports = {item["source_id"]: item for item in report["sources"]}

    assert report["actions"] == {"reconcile": True, "extract": True, "etl": False, "promote": False}
    assert ("vmj_ojs", True) in calls
    assert ("who_vietnam", True) in calls
    assert ("vien_dinh_duong", False) in calls
    assert ("mayo_diseases_conditions", False) in calls
    assert ("cdc_health_topics", False) in calls
    assert source_reports["vmj_ojs"]["actions"]["extract"]["reason"] == "complex_reconcile_only_default"
    assert source_reports["who_vietnam"]["actions"]["extract"]["reason"] == "complex_reconcile_only_default"
    assert source_reports["vien_dinh_duong"]["actions"]["extract"]["reconcile_only"] is False
