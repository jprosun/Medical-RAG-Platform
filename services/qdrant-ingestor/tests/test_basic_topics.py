from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_discover_items_for_nhs_health_a_z_from_seed_page():
    module = importlib.import_module("pipelines.crawl.sources.basic_topics")
    html_by_url = {
        "https://www.nhs.uk/health-a-to-z/": """
            <html><body>
              <a href="/conditions/">Conditions A to Z</a>
              <a href="/conditions/asthma/">Asthma</a>
              <a href="/medicines/paracetamol-for-adults/">Paracetamol</a>
            </body></html>
        """,
        "https://www.nhs.uk/conditions/": """
            <html><body>
              <a href="/conditions/bronchitis/">Bronchitis</a>
            </body></html>
        """,
    }

    items = module.discover_items(
        "nhs_health_a_z",
        get_text=lambda url: html_by_url.get(url),
        max_items=10,
    )

    urls = {item["url"] for item in items}
    assert "https://www.nhs.uk/conditions/asthma/" in urls
    assert "https://www.nhs.uk/medicines/paracetamol-for-adults/" in urls
    assert "https://www.nhs.uk/conditions/bronchitis/" in urls


def test_cdc_candidate_filter_excludes_health_topics_index():
    module = importlib.import_module("pipelines.crawl.sources.basic_topics")

    assert module.is_candidate_url("cdc_health_topics", "https://cdc.gov/health-topics.html") is False
    assert module.is_candidate_url("cdc_health_topics", "https://cdc.gov/diabetes/index.html") is True
