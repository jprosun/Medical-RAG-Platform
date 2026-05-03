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

