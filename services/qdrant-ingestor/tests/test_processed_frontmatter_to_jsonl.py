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
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(rag_root.parent / "data"))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    lineage = importlib.import_module("services.utils.data_lineage")
    importlib.reload(lineage)
    module = importlib.import_module("pipelines.etl.processed_frontmatter_to_jsonl")
    return importlib.reload(module)


def test_process_directory_builds_records_from_frontmatter_txt(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_module(monkeypatch, rag_root)

    processed_dir = rag_root / "sources" / "nhs_health_a_z" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    (processed_dir / "acute-pancreatitis.txt").write_text(
        """---
source_id: nhs_health_a_z
title: Pancreatitis (acute)
item_url: https://www.nhs.uk/conditions/acute-pancreatitis/
---

Pancreatitis (acute)

Acute pancreatitis is inflammation of the pancreas that needs urgent care.
Symptoms include severe tummy pain, vomiting, and fever. Treatment is usually in hospital.
""",
        encoding="utf-8",
    )

    output_path = rag_root / "sources" / "nhs_health_a_z" / "records" / "document_records.jsonl"
    report = module.process_directory(
        source_id="nhs_health_a_z",
        source_dir=processed_dir,
        output_path=output_path,
    )

    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    record = json.loads(lines[0])

    assert report["records"] == 1
    assert record["title"] == "Pancreatitis (acute)"
    assert record["source_url"] == "https://www.nhs.uk/conditions/acute-pancreatitis/"
    assert record["source_id"] == "nhs_health_a_z"
    assert record["source_name"] == "NHS Health A-Z"
    assert record["doc_type"] == "patient_education"
    assert record["processed_path"].endswith("acute-pancreatitis.txt")


def test_process_file_skips_too_short_body(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module = _reload_module(monkeypatch, rag_root)

    processed_dir = rag_root / "sources" / "msd_manual_consumer" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    short_path = processed_dir / "short.txt"
    short_path.write_text(
        """---
source_id: msd_manual_consumer
title: Short note
item_url: https://example.org/short
---

Too short.
""",
        encoding="utf-8",
    )

    record = module.process_file(short_path, source_id="msd_manual_consumer", etl_run_id="test")
    assert record is None
