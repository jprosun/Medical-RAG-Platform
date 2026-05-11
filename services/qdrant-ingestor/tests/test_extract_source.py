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

    assert report["failed"] == 0
    assert report["missing_assets"] == 1
    assert report["processed"] == 0
    assert rows[0]["extract_status"] == "missing_asset"
    assert summary["missing_assets"] == 1
    assert summary["failed"] == 0


def test_extract_source_uses_nhs_jsonld_for_clean_text(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "nhs_health_a_z" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "acute-pancreatitis.html"
    raw_path.write_text(
        """
        <html><head>
        <script type="application/ld+json">
        {
          "@type": "MedicalWebPage",
          "name": "Acute pancreatitis",
          "description": "Find out about acute pancreatitis.",
          "hasPart": [
            {
              "@type": "HealthTopicContent",
              "headline": "Symptoms of acute pancreatitis",
              "hasPart": [
                {"@type": "WebPageElement", "text": "<p>Severe tummy pain and vomiting.</p>"}
              ]
            },
            {
              "@type": "HealthTopicContent",
              "headline": "Treatment for acute pancreatitis",
              "hasPart": [
                {"@type": "WebPageElement", "text": "<p>Hospital treatment includes fluids and painkillers.</p>"}
              ]
            }
          ]
        }
        </script>
        </head><body><main><p>boilerplate</p></main></body></html>
        """,
        encoding="utf-8",
    )

    crawl_manifest.write_manifest(
        "nhs_health_a_z",
        [
            {
                "source_id": "nhs_health_a_z",
                "item_id": "nhs1",
                "relative_path": "sources/nhs_health_a_z/raw/acute-pancreatitis.html",
                "content_class": "html",
                "title_hint": "Acute pancreatitis",
                "item_url": "https://www.nhs.uk/conditions/acute-pancreatitis/",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("nhs_health_a_z")
    out_path = rag_root / "sources" / "nhs_health_a_z" / "processed" / "acute-pancreatitis.txt"
    text = out_path.read_text(encoding="utf-8")

    assert report["processed"] == 1
    assert "Symptoms of acute pancreatitis" in text
    assert "Severe tummy pain and vomiting." in text
    assert "Hospital treatment includes fluids and painkillers." in text


def test_extract_source_reconcile_resets_done_without_processed_to_pending(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "vmj_ojs" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "sample.pdf"
    raw_path.write_bytes(b"%PDF-1.4 fake")

    crawl_manifest.write_manifest(
        "vmj_ojs",
        [
            {
                "source_id": "vmj_ojs",
                "item_id": "vmj1",
                "relative_path": "sources/vmj_ojs/raw/sample.pdf",
                "content_class": "pdf",
                "title_hint": "Sample PDF",
                "extract_strategy": "digital_pdf_text",
                "extract_status": "done",
            }
        ],
    )

    report = module.extract_source("vmj_ojs", reconcile_only=True)
    rows = crawl_manifest.read_manifest("vmj_ojs")
    summary = json.loads((rag_root / "sources" / "vmj_ojs" / "qa" / "extract_summary.json").read_text(encoding="utf-8"))

    assert rows[0]["extract_status"] == "pending"
    assert report["pending"] == 1
    assert report["processed"] == 0
    assert report["processed_files"] == 0
    assert summary["pending"] == 1


def test_extract_source_reconcile_restores_legacy_processed_and_preserves_done_stem(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    legacy_raw_dir = rag_root / "medical_crawl_seed" / "data_raw" / "vmj_ojs" / "files"
    legacy_raw_dir.mkdir(parents=True, exist_ok=True)
    (legacy_raw_dir / "sample.pdf").write_bytes(b"%PDF-1.4 fake")

    legacy_processed_dir = rag_root / "data_processed" / "vmj_ojs"
    legacy_processed_dir.mkdir(parents=True, exist_ok=True)
    legacy_processed = legacy_processed_dir / "sample.txt"
    legacy_processed.write_text("legacy vmj text", encoding="utf-8")

    crawl_manifest.write_manifest(
        "vmj_ojs",
        [
            {
                "source_id": "vmj_ojs",
                "item_id": "vmj-legacy",
                "relative_path": "medical_crawl_seed/data_raw/vmj_ojs/files/sample.pdf",
                "content_class": "pdf",
                "title_hint": "Legacy Sample",
                "extract_strategy": "digital_pdf_text",
                "extract_status": "done",
            },
            {
                "source_id": "vmj_ojs",
                "item_id": "vmj-missing",
                "relative_path": "sources/vmj_ojs/raw/sample.pdf",
                "content_class": "pdf",
                "title_hint": "Legacy Sample",
                "extract_strategy": "classify_pdf",
                "extract_status": "missing_asset",
            },
        ],
    )

    report = module.extract_source("vmj_ojs", reconcile_only=True)
    rows = crawl_manifest.read_manifest("vmj_ojs")
    new_processed = rag_root / "sources" / "vmj_ojs" / "processed" / "sample.txt"

    assert new_processed.exists()
    assert new_processed.read_text(encoding="utf-8") == "legacy vmj text"
    assert report["processed"] == 1
    assert report["processed_files"] == 1
    assert report["missing_assets"] == 0
    assert report["deferred"] == 1
    assert any(row["relative_path"] == "medical_crawl_seed/data_raw/vmj_ojs/files/sample.pdf" and row["extract_status"] == "done" for row in rows)
    assert any(
        row["relative_path"] == "sources/vmj_ojs/raw/sample.pdf"
        and row["extract_status"] == "deferred"
        and row["extract_strategy"] == "stale_sibling_backlog"
        for row in rows
    )


def test_extract_source_runtime_preserves_done_sibling_processed_output(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    legacy_raw_dir = rag_root / "medical_crawl_seed" / "data_raw" / "vmj_ojs" / "files"
    legacy_raw_dir.mkdir(parents=True, exist_ok=True)
    (legacy_raw_dir / "sample.pdf").write_bytes(b"%PDF-1.4 fake")

    processed_dir = rag_root / "sources" / "vmj_ojs" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    processed_path = processed_dir / "sample.txt"
    processed_path.write_text("existing vmj text", encoding="utf-8")

    crawl_manifest.write_manifest(
        "vmj_ojs",
        [
            {
                "source_id": "vmj_ojs",
                "item_id": "vmj-done",
                "relative_path": "medical_crawl_seed/data_raw/vmj_ojs/files/sample.pdf",
                "content_class": "pdf",
                "title_hint": "Legacy Sample",
                "extract_strategy": "digital_pdf_text",
                "extract_status": "done",
            },
            {
                "source_id": "vmj_ojs",
                "item_id": "vmj-missing",
                "relative_path": "sources/vmj_ojs/raw/sample.pdf",
                "content_class": "pdf",
                "title_hint": "Legacy Sample",
                "extract_strategy": "classify_pdf",
                "extract_status": "missing_asset",
            },
        ],
    )

    report = module.extract_source("vmj_ojs")
    rows = crawl_manifest.read_manifest("vmj_ojs")

    assert processed_path.exists()
    assert processed_path.read_text(encoding="utf-8") == "existing vmj text"
    assert report["processed"] == 1
    assert report["missing_assets"] == 0
    assert report["deferred"] == 1
    assert any(row["relative_path"] == "medical_crawl_seed/data_raw/vmj_ojs/files/sample.pdf" and row["extract_status"] == "done" for row in rows)
    assert any(
        row["relative_path"] == "sources/vmj_ojs/raw/sample.pdf"
        and row["extract_status"] == "deferred"
        and row["extract_strategy"] == "stale_sibling_backlog"
        for row in rows
    )


def test_extract_source_reconcile_quarantines_stale_processed_outputs(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "who_vietnam" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "article-a.html").write_text("<html><body><main><h1>A</h1><p>Body A.</p></main></body></html>", encoding="utf-8")

    processed_dir = rag_root / "sources" / "who_vietnam" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    stale_path = processed_dir / "stale-extra.txt"
    stale_path.write_text("stale content", encoding="utf-8")

    crawl_manifest.write_manifest(
        "who_vietnam",
        [
            {
                "source_id": "who_vietnam",
                "item_id": "whovn-a",
                "relative_path": "sources/who_vietnam/raw/article-a.html",
                "content_class": "html",
                "title_hint": "Article A",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("who_vietnam", reconcile_only=True)
    quarantined_path = rag_root / "sources" / "who_vietnam" / "qa" / "stale_processed" / "stale-extra.txt"

    assert report["processed_files"] == 0
    assert not stale_path.exists()
    assert quarantined_path.exists()
    assert quarantined_path.read_text(encoding="utf-8") == "stale content"


def test_extract_source_reconcile_defers_unrecoverable_bootstrapped_missing_asset(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    crawl_manifest.write_manifest(
        "who_vietnam",
        [
            {
                "source_id": "who_vietnam",
                "item_id": "whovn-legacy-missing",
                "relative_path": "sources/who_vietnam/raw/legacy-missing.html",
                "content_class": "html",
                "title_hint": "legacy-missing",
                "extract_strategy": "html_text",
                "extract_status": "missing_asset",
                "notes": "bootstrapped_from_existing_raw",
                "item_url": "",
                "file_url": "",
            }
        ],
    )

    report = module.extract_source("who_vietnam", reconcile_only=True)
    rows = crawl_manifest.read_manifest("who_vietnam")

    assert report["missing_assets"] == 0
    assert report["deferred"] == 1
    assert rows[0]["extract_status"] == "deferred"
    assert rows[0]["extract_strategy"] == "legacy_missing_backlog"


def test_extract_source_reconcile_restores_exact_quarantined_processed_output(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "who_vietnam" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "covax-1.18-update").write_text("<html><body><main><h1>COVAX</h1><p>Body.</p></main></body></html>", encoding="utf-8")

    stale_dir = rag_root / "sources" / "who_vietnam" / "qa" / "stale_processed"
    stale_dir.mkdir(parents=True, exist_ok=True)
    stale_file = stale_dir / "covax-1.18-update.txt"
    stale_file.write_text("restorable content", encoding="utf-8")

    crawl_manifest.write_manifest(
        "who_vietnam",
        [
            {
                "source_id": "who_vietnam",
                "item_id": "whovn-covax",
                "relative_path": "sources/who_vietnam/raw/covax-1.18-update",
                "content_class": "binary",
                "title_hint": "COVAX Update",
                "extract_strategy": "html_text",
                "extract_status": "done",
            }
        ],
    )

    report = module.extract_source("who_vietnam", reconcile_only=True)
    restored_path = rag_root / "sources" / "who_vietnam" / "processed" / "covax-1.18-update.txt"
    rows = crawl_manifest.read_manifest("who_vietnam")

    assert report["processed"] == 1
    assert report["pending"] == 0
    assert restored_path.exists()
    assert restored_path.read_text(encoding="utf-8") == "restorable content"
    assert rows[0]["extract_status"] == "done"


def test_extract_source_uses_msd_main_topic_and_filters_utility_page(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "msd_manual_professional" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    good_raw = raw_dir / "cardiac-arrest.html"
    good_raw.write_text(
        """
        <html><body>
          <main id="mainContainer">
            <h1 id="topicHeaderTitle">Cardiac Arrest</h1>
            <p data-testid="topicDefinition">Cardiac arrest is the cessation of cardiac mechanical activity.</p>
            <div data-testid="Topic-subnavigation">noise nav</div>
            <div data-testid="topic-main-content">
              <p>Sudden cardiac arrest occurs outside the hospital.</p>
              <section><h2>Etiology</h2><p>Most cases are caused by cardiac disease.</p></section>
              <section><h2>General references</h2><p>Should be removed.</p></section>
            </div>
          </main>
        </body></html>
        """,
        encoding="utf-8",
    )

    utility_raw = raw_dir / "3d-models.html"
    utility_raw.write_text("<html><body><main><h1>3D Models</h1></main></body></html>", encoding="utf-8")

    crawl_manifest.write_manifest(
        "msd_manual_professional",
        [
            {
                "source_id": "msd_manual_professional",
                "item_id": "msd1",
                "relative_path": "sources/msd_manual_professional/raw/cardiac-arrest.html",
                "content_class": "html",
                "title_hint": "Cardiac Arrest",
                "item_url": "https://www.msdmanuals.com/professional/topic/cardiac-arrest",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            },
            {
                "source_id": "msd_manual_professional",
                "item_id": "msd2",
                "relative_path": "sources/msd_manual_professional/raw/3d-models.html",
                "content_class": "html",
                "title_hint": "3D Models",
                "item_url": "https://www.msdmanuals.com/professional/pages-with-widgets/3d-models",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            },
        ],
    )

    report = module.extract_source("msd_manual_professional")
    out_path = rag_root / "sources" / "msd_manual_professional" / "processed" / "cardiac-arrest.txt"
    text = out_path.read_text(encoding="utf-8")
    rows = crawl_manifest.read_manifest("msd_manual_professional")

    assert report["processed"] == 1
    assert report["deferred"] == 1
    assert "Cardiac arrest is the cessation of cardiac mechanical activity." in text
    assert "Most cases are caused by cardiac disease." in text
    assert "Should be removed." not in text
    assert "Contact us" not in text
    assert any(row["relative_path"].endswith("3d-models.html") and row["extract_status"] == "deferred" for row in rows)


def test_extract_source_normalizes_common_msd_mojibake(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "msd_manual_professional" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "aaa.html"
    raw_html = (
        "<html><body>"
        "<main id='mainContainer'>"
        "<h1 id='topicHeaderTitle'>AAA</h1>"
        "<div data-testid='topic-main-content'>"
        "<p>Abdominal aortic diameter \u00e2\u2030\u00a5 3 cm and risk 4\u00e2\u20ac\u201c5%.</p>"
        "<p>Most aneurysms grow slowly and may be detected incidentally during imaging or clinical evaluation.</p>"
        "<p>Diagnosis is usually confirmed with ultrasound or CT scanning, and treatment depends on aneurysm size and rupture risk.</p>"
        "<p>Contact us</p>"
        "<p>\u00c2\u00a9 2017 Example MD.</p>"
        "</div>"
        "</main>"
        "</body></html>"
    )
    raw_path.write_text(raw_html, encoding="utf-8")

    crawl_manifest.write_manifest(
        "msd_manual_professional",
        [
            {
                "source_id": "msd_manual_professional",
                "item_id": "msd3",
                "relative_path": "sources/msd_manual_professional/raw/aaa.html",
                "content_class": "html",
                "title_hint": "AAA",
                "item_url": "https://www.msdmanuals.com/professional/topic/aaa",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("msd_manual_professional")
    out_path = rag_root / "sources" / "msd_manual_professional" / "processed" / "aaa.txt"
    text = out_path.read_text(encoding="utf-8")

    assert report["processed"] == 1
    assert "≥ 3 cm" in text
    assert "4–5%" in text
    assert "Contact us" not in text
    assert "© 2017" not in text


def test_extract_source_uses_uspstf_main_content_and_frontmatter(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "uspstf_recommendations" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "abdominal-aortic-aneurysm-screening-2014.html"
    raw_path.write_text(
        """
        <html lang="en"><body>
        <header><nav>Recommendation topics</nav></header>
        <main id="main-content">
          <h1>Abdominal Aortic Aneurysm: Screening</h1>
          <p>The USPSTF recommends one-time screening for abdominal aortic aneurysm with ultrasonography in selected adults.</p>
          <h2>Recommendation Summary</h2>
          <p>Men aged 65 to 75 years who have ever smoked should be screened once.</p>
          <h2>References</h2>
          <p>Should stop before here.</p>
        </main>
        </body></html>
        """,
        encoding="utf-8",
    )

    crawl_manifest.write_manifest(
        "uspstf_recommendations",
        [
            {
                "source_id": "uspstf_recommendations",
                "item_id": "uspstf1",
                "relative_path": "sources/uspstf_recommendations/raw/abdominal-aortic-aneurysm-screening-2014.html",
                "content_class": "html",
                "title_hint": "Abdominal Aortic Aneurysm: Screening",
                "item_url": "https://www.uspreventiveservicestaskforce.org/uspstf/recommendation/abdominal-aortic-aneurysm-screening",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("uspstf_recommendations")
    out_path = rag_root / "sources" / "uspstf_recommendations" / "processed" / "abdominal-aortic-aneurysm-screening-2014.txt"
    text = out_path.read_text(encoding="utf-8")

    assert report["processed"] == 1
    assert "Recommendation Summary" in text
    assert "Men aged 65 to 75 years" in text
    assert "Should stop before here." not in text
    assert "doc_type: guideline" in text
    assert "audience: clinician" in text


def test_extract_source_uses_nccih_topic_content(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "nccih_health" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "10-things-to-know-about-the-science-of-health.html"
    raw_path.write_text(
        """
        <html lang="en">
          <head>
            <meta property="og:title" content="10 Things To Know About the Science of Health" />
            <meta name="description" content="A short summary of key points about complementary health approaches." />
          </head>
          <body>
            <main>
              <h1>10 Things To Know About the Science of Health</h1>
              <p>Researchers study complementary health approaches using rigorous scientific methods.</p>
              <h2>For More Information</h2>
              <p>Stop here.</p>
            </main>
          </body>
        </html>
        """,
        encoding="utf-8",
    )

    crawl_manifest.write_manifest(
        "nccih_health",
        [
            {
                "source_id": "nccih_health",
                "item_id": "nccih1",
                "relative_path": "sources/nccih_health/raw/10-things-to-know-about-the-science-of-health.html",
                "content_class": "html",
                "title_hint": "10 Things To Know About the Science of Health",
                "item_url": "https://www.nccih.nih.gov/health/10-things-to-know-about-the-science-of-health",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("nccih_health")
    out_path = rag_root / "sources" / "nccih_health" / "processed" / "10-things-to-know-about-the-science-of-health.txt"
    text = out_path.read_text(encoding="utf-8")

    assert report["processed"] == 1
    assert "A short summary of key points" in text
    assert "rigorous scientific methods" in text
    assert "Stop here." not in text


def test_extract_source_uses_nci_pdq_metadata_overrides(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "nci_pdq" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "acupuncture-pdq.html"
    raw_path.write_text(
        """
        <html lang="es">
          <head>
            <meta name="description" content="Resumen del PDQ sobre acupuntura para profesionales de la salud." />
            <meta name="dcterms.audience" content="Health Professionals" />
          </head>
          <body>
            <main id="main-content">
              <h1>Acupuntura (PDQ®)</h1>
              <div class="summary-sections">
                <p>La acupuntura se usa para aliviar algunos síntomas en pacientes con cáncer.</p>
                <h2>Referencias</h2>
                <p>No incluir.</p>
              </div>
            </main>
          </body>
        </html>
        """,
        encoding="utf-8",
    )

    crawl_manifest.write_manifest(
        "nci_pdq",
        [
            {
                "source_id": "nci_pdq",
                "item_id": "nci1",
                "relative_path": "sources/nci_pdq/raw/acupuncture-pdq.html",
                "content_class": "html",
                "title_hint": "Acupuntura (PDQ®)",
                "item_url": "https://www.cancer.gov/espanol/cancer/acupuncture-pdq",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("nci_pdq")
    out_path = rag_root / "sources" / "nci_pdq" / "processed" / "acupuncture-pdq.txt"
    text = out_path.read_text(encoding="utf-8")

    assert report["processed"] == 1
    assert "La acupuntura se usa" in text
    assert "No incluir." not in text
    assert "language: es" in text
    assert "audience: clinician" in text

def test_extract_source_prunes_mayo_footer_and_reference_noise(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "mayo_diseases_conditions" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "syc-20350244.html"
    raw_path.write_text(
        """
        <html><body>
          <article id="main-content">
            <h1>Angiosarcoma</h1>
            <p>Angiosarcoma is a rare cancer of blood and lymph vessels.</p>
            <h2>Symptoms</h2>
            <p>Symptoms vary by where the cancer occurs.</p>
            <h2>Products &amp; Services</h2>
            <p>A Book: Mayo Clinic Family Health Book</p>
            <p>Request an appointment</p>
            <p>By Mayo Clinic Staff</p>
            <p>Apr 30, 2025</p>
            <p>Diagnosis &amp; treatment</p>
            <p>CON-20128705</p>
          </article>
        </body></html>
        """,
        encoding="utf-8",
    )

    crawl_manifest.write_manifest(
        "mayo_diseases_conditions",
        [
            {
                "source_id": "mayo_diseases_conditions",
                "item_id": "mayo1",
                "relative_path": "sources/mayo_diseases_conditions/raw/syc-20350244.html",
                "content_class": "html",
                "title_hint": "Angiosarcoma",
                "item_url": "https://www.mayoclinic.org/diseases-conditions/angiosarcoma/symptoms-causes/syc-20350244",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("mayo_diseases_conditions")
    out_path = rag_root / "sources" / "mayo_diseases_conditions" / "processed" / "syc-20350244.txt"
    text = out_path.read_text(encoding="utf-8")

    assert report["processed"] == 1
    assert "Angiosarcoma is a rare cancer of blood and lymph vessels." in text
    assert "Products & Services" not in text
    assert "Request an appointment" not in text
    assert "By Mayo Clinic Staff" not in text
    assert "CON-20128705" not in text


def test_extract_source_keeps_mayo_article_when_global_header_exists(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "mayo_diseases_conditions" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "syc-20350688.html"
    raw_path.write_text(
        """
        <html><body>
          <header><nav>Global nav</nav></header>
          <h1>Abdominal aortic aneurysm</h1>
          <article id="main-content">
            <h2>Overview</h2>
            <p>An abdominal aortic aneurysm is an enlarged area in the lower part of the body's main artery.</p>
            <h2>Symptoms</h2>
            <p>You may not notice symptoms until the aneurysm becomes large or ruptures.</p>
          </article>
          <footer>Footer</footer>
        </body></html>
        """,
        encoding="utf-8",
    )

    crawl_manifest.write_manifest(
        "mayo_diseases_conditions",
        [
            {
                "source_id": "mayo_diseases_conditions",
                "item_id": "mayo2",
                "relative_path": "sources/mayo_diseases_conditions/raw/syc-20350688.html",
                "content_class": "html",
                "title_hint": "Abdominal aortic aneurysm",
                "item_url": "https://www.mayoclinic.org/diseases-conditions/abdominal-aortic-aneurysm/symptoms-causes/syc-20350688",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("mayo_diseases_conditions")
    out_path = rag_root / "sources" / "mayo_diseases_conditions" / "processed" / "syc-20350688.txt"
    text = out_path.read_text(encoding="utf-8")

    assert report["processed"] == 1
    assert "An abdominal aortic aneurysm is an enlarged area" in text
    assert "You may not notice symptoms" in text


def test_extract_source_prunes_cdc_more_information_and_stale_output(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "cdc_health_topics" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir = rag_root / "sources" / "cdc_health_topics" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    article_raw = raw_dir / "healthy-eating-tips.html"
    article_raw.write_text(
        """
        <html><head><title>Healthy Eating Tips | CDC</title></head><body>
          <main>
            <h1>Healthy Eating Tips</h1>
            <p>EspaÃ±ol</p>
            <p>At a glance</p>
            <p>Healthy eating means focusing on whole foods, fruits, vegetables, whole grains, beans, nuts, and lower-sodium choices over time.</p>
            <p>Small practical changes can improve dietary quality and reduce the intake of added sugars and highly processed foods.</p>
            <h2>More information</h2>
            <p>General links should be trimmed.</p>
            <p>Content Source: NCCDPHP</p>
          </main>
        </body></html>
        """,
        encoding="utf-8",
    )
    utility_raw = raw_dir / "contact-us.html"
    utility_raw.write_text("<html><body><main><h1>Contact Us</h1></main></body></html>", encoding="utf-8")
    stale_out = processed_dir / "contact-us.txt"
    stale_out.write_text("stale", encoding="utf-8")

    crawl_manifest.write_manifest(
        "cdc_health_topics",
        [
            {
                "source_id": "cdc_health_topics",
                "item_id": "cdc1",
                "relative_path": "sources/cdc_health_topics/raw/healthy-eating-tips.html",
                "content_class": "html",
                "title_hint": "Healthy Eating Tips",
                "item_url": "https://www.cdc.gov/nutrition/features/healthy-eating-tips.html",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            },
            {
                "source_id": "cdc_health_topics",
                "item_id": "cdc2",
                "relative_path": "sources/cdc_health_topics/raw/contact-us.html",
                "content_class": "html",
                "title_hint": "Contact Us",
                "item_url": "https://www.cdc.gov/cdc-info/index.html",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            },
        ],
    )

    report = module.extract_source("cdc_health_topics")
    out_path = processed_dir / "healthy-eating-tips.txt"
    text = out_path.read_text(encoding="utf-8")
    rows = crawl_manifest.read_manifest("cdc_health_topics")

    assert report["processed"] == 1
    assert report["deferred"] == 1
    assert "Healthy eating means focusing on whole foods" in text
    assert "EspaÃ±ol" not in text
    assert "More information" not in text
    assert "Content Source:" not in text
    assert stale_out.exists() is False
    assert any(row["relative_path"].endswith("contact-us.html") and row["extract_status"] == "deferred" for row in rows)


def test_extract_source_vien_dinh_duong_defers_long_book_pdf(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "vien_dinh_duong" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "so-tay-dinh-duong.pdf"
    raw_path.write_bytes(b"%PDF-1.4 fake")

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "item_id": "vdd1",
                "relative_path": "sources/vien_dinh_duong/raw/so-tay-dinh-duong.pdf",
                "content_class": "pdf",
                "title_hint": "Sổ tay dinh dưỡng",
                "item_url": "https://viendinhduong.vn/vi/so-tay-dinh-duong.pdf",
                "extract_strategy": "classify_pdf",
                "extract_status": "pending",
            }
        ],
    )

    monkeypatch.setattr(module, "classify_pdf", lambda path: ("digital", 48, 24000))

    report = module.extract_source("vien_dinh_duong")
    rows = crawl_manifest.read_manifest("vien_dinh_duong")

    assert report["processed"] == 0
    assert report["deferred"] == 1
    assert report["long_pdf_books"] == 1
    assert any(
        row["relative_path"].endswith("so-tay-dinh-duong.pdf")
        and row["extract_strategy"] == "long_pdf_book"
        and row["extract_status"] == "deferred"
        for row in rows
    )


def test_extract_source_vien_dinh_duong_defers_image_like_pdf(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "vien_dinh_duong" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "poster-dinh-duong.pdf"
    raw_path.write_bytes(b"%PDF-1.4 fake")

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "item_id": "vdd2",
                "relative_path": "sources/vien_dinh_duong/raw/poster-dinh-duong.pdf",
                "content_class": "pdf",
                "title_hint": "Poster dinh dưỡng",
                "item_url": "https://viendinhduong.vn/vi/poster-dinh-duong.pdf",
                "extract_strategy": "classify_pdf",
                "extract_status": "pending",
            }
        ],
    )

    monkeypatch.setattr(module, "classify_pdf", lambda path: ("digital", 1, 90))

    report = module.extract_source("vien_dinh_duong")
    rows = crawl_manifest.read_manifest("vien_dinh_duong")

    assert report["processed"] == 0
    assert report["deferred"] == 1
    assert report["image_like_pdfs"] == 1
    assert any(
        row["relative_path"].endswith("poster-dinh-duong.pdf")
        and row["extract_strategy"] == "image_pdf_backlog"
        and row["extract_status"] == "deferred"
        for row in rows
    )


def test_extract_source_who_vietnam_routes_long_and_image_like_pdfs(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "who_vietnam" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    long_pdf = raw_dir / "bao-cao.pdf"
    image_pdf = raw_dir / "poster.pdf"
    long_pdf.write_bytes(b"%PDF-1.4 fake")
    image_pdf.write_bytes(b"%PDF-1.4 fake")

    crawl_manifest.write_manifest(
        "who_vietnam",
        [
            {
                "source_id": "who_vietnam",
                "item_id": "wv-long",
                "relative_path": "sources/who_vietnam/raw/bao-cao.pdf",
                "content_class": "pdf",
                "title_hint": "Bao cao WHO",
                "extract_strategy": "classify_pdf",
                "extract_status": "pending",
            },
            {
                "source_id": "who_vietnam",
                "item_id": "wv-image",
                "relative_path": "sources/who_vietnam/raw/poster.pdf",
                "content_class": "pdf",
                "title_hint": "Poster WHO",
                "extract_strategy": "classify_pdf",
                "extract_status": "pending",
            },
        ],
    )

    profiles = iter(
        [
            ("digital", 40, 18000),
            ("digital", 1, 120),
        ]
    )
    monkeypatch.setattr(module, "classify_pdf", lambda path: next(profiles))

    report = module.extract_source("who_vietnam")
    rows = crawl_manifest.read_manifest("who_vietnam")

    assert report["processed"] == 0
    assert report["deferred"] == 2
    assert report["long_pdf_books"] == 1
    assert report["image_like_pdfs"] == 1
    assert any(
        row["relative_path"].endswith("bao-cao.pdf")
        and row["extract_strategy"] == "long_pdf_book"
        and row["extract_status"] == "deferred"
        for row in rows
    )
    assert any(
        row["relative_path"].endswith("poster.pdf")
        and row["extract_strategy"] == "image_pdf_backlog"
        and row["extract_status"] == "deferred"
        for row in rows
    )


def test_extract_source_defers_misclassified_docx_before_xml_parser(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "vien_dinh_duong" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "quyet-dinh.docx"
    raw_path.write_bytes(b"PK\x03\x04fake-docx")

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "item_id": "vdd-docx",
                "relative_path": "sources/vien_dinh_duong/raw/quyet-dinh.docx",
                "content_class": "xml",
                "extension": ".docx",
                "title_hint": "Quyet dinh",
                "extract_strategy": "xml_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("vien_dinh_duong")
    rows = crawl_manifest.read_manifest("vien_dinh_duong")

    assert report["processed"] == 0
    assert report["deferred"] == 1
    assert any(
        row["relative_path"].endswith("quyet-dinh.docx")
        and row["extract_strategy"] == "office_backlog"
        and row["extract_status"] == "deferred"
        for row in rows
    )


def test_extract_source_filters_vien_dinh_duong_professional_landing_page(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    module, crawl_manifest = _reload_extract_module(monkeypatch, rag_root)

    raw_dir = rag_root / "sources" / "vien_dinh_duong" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "dinh-duong-cong-dong.html"
    raw_path.write_text(
        """
        <html><body>
          <main>
            <h1>Dinh dưỡng cộng đồng</h1>
            <p>Landing page for a professional-activities section.</p>
          </main>
        </body></html>
        """,
        encoding="utf-8",
    )

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "item_id": "vdd-landing",
                "relative_path": "sources/vien_dinh_duong/raw/dinh-duong-cong-dong.html",
                "content_class": "html",
                "title_hint": "Dinh dưỡng cộng đồng",
                "item_url": "https://viendinhduong.vn/vi/professional-activities/dinh-duong-cong-dong",
                "extract_strategy": "html_text",
                "extract_status": "pending",
            }
        ],
    )

    report = module.extract_source("vien_dinh_duong")
    rows = crawl_manifest.read_manifest("vien_dinh_duong")

    assert report["processed"] == 0
    assert report["deferred"] == 1
    assert any(
        row["relative_path"].endswith("dinh-duong-cong-dong.html")
        and row["extract_strategy"] == "html_filtered"
        and row["extract_status"] == "deferred"
        for row in rows
    )

