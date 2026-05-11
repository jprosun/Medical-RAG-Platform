from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module(monkeypatch, rag_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    module = importlib.import_module("pipelines.etl.processed_frontmatter_to_jsonl")
    return importlib.reload(module)


def test_process_file_applies_frontmatter_overrides(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module = _reload_module(monkeypatch, rag_root)
    path = rag_root / "sources" / "nci_pdq" / "processed" / "acupuncture-pdq.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """---
source_id: nci_pdq
title: Acupuntura (PDQ)
language: es
audience: clinician
doc_type: reference
specialty: oncology
trust_tier: 1
section_title: Summary
canonical_title: Acupuntura
published_at: 2026-05-01
---

Acupuntura se usa a veces para tratar síntomas relacionados con el cáncer. Este resumen describe la evidencia clínica y las consideraciones de seguridad para profesionales de la salud.
""",
        encoding="utf-8",
    )

    rec = module.process_file(path, source_id="nci_pdq", etl_run_id="etl-test")

    assert rec is not None
    assert rec.language == "es"
    assert rec.audience == "clinician"
    assert rec.doc_type == "reference"
    assert rec.specialty == "oncology"
    assert rec.trust_tier == 1
    assert rec.section_title == "Summary"
    assert rec.canonical_title == "Acupuntura"
    assert rec.heading_path == "Acupuntura > Summary"


def test_process_file_repairs_nci_pdq_generic_title_and_audience(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module = _reload_module(monkeypatch, rag_root)
    path = rag_root / "sources" / "nci_pdq" / "processed" / "acupuncture-pdq.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """---
source_id: nci_pdq
title: health professional
item_url: https://www.cancer.gov/about-cancer/treatment/cam/hp/acupuncture-pdq
language: en
audience: clinician
---

Acupuncture (PDQÂ®)â€“Health Professional Version

Acupuncture is a complementary therapy used by cancer patients to manage symptoms related to cancer and its treatment.
This summary describes clinical evidence, symptom management, adverse effects, and implementation considerations for clinicians.
""",
        encoding="utf-8",
    )

    rec = module.process_file(path, source_id="nci_pdq", etl_run_id="etl-test")

    assert rec is not None
    assert rec.title == "Acupuncture (PDQ®)–Health Professional Version"
    assert rec.canonical_title == "Acupuncture (PDQ®)"
    assert rec.audience == "clinician"
    assert rec.language == "en"
    assert "PDQÂ®" not in rec.title
    assert rec.body.startswith("Acupuncture is a complementary therapy")
