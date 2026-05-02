from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_discover_items_for_nice_guidance_includes_guidance_and_pdf():
    module = importlib.import_module("pipelines.crawl.sources.reference_sites")
    html_by_url = {
        "https://www.nice.org.uk/guidance/published": """
            <html><body>
              <a href="/guidance/ng12">Guideline NG12</a>
              <a href="/guidance/ng12/resources/ng12-pdf-123456789.pdf">PDF</a>
            </body></html>
        """,
    }

    items = module.discover_items(
        "nice_guidance",
        get_text=lambda url: html_by_url.get(url),
        max_items=10,
    )

    urls = {item["url"] for item in items}
    assert "https://www.nice.org.uk/guidance/ng12" in urls
    assert "https://www.nice.org.uk/guidance/ng12/resources/ng12-pdf-123456789.pdf" in urls


def test_vaac_candidate_filter_accepts_hiv_material_and_rejects_contact():
    module = importlib.import_module("pipelines.crawl.sources.reference_sites")

    assert module.is_candidate_url("vaac_hiv_aids", "https://vaac.gov.vn/xet-nghiem-hiv") is True
    assert module.is_candidate_url("vaac_hiv_aids", "https://vaac.gov.vn/lien-he") is False
