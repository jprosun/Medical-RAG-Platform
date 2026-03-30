"""
ETL: WHO Health Topics Scraper
===============================

Crawls WHO health topics fact sheets and converts them into enriched JSONL.

WHO publishes health topic fact sheets at:
  https://www.who.int/health-topics/
  https://www.who.int/news-room/fact-sheets

Usage:
    python -m etl.who_scraper \\
        --raw-dir ../../data/data_raw/who \\
        --output  ../../data/data_final/who.jsonl \\
        --max-topics 50
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print(
        "ERROR: This script requires 'requests' and 'beautifulsoup4'.\n"
        "Install with:  pip install requests beautifulsoup4",
        file=sys.stderr,
    )
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.document_schema import DocumentRecord


# ── Constants ────────────────────────────────────────────────────────
WHO_FACT_SHEETS_URL = "https://www.who.int/news-room/fact-sheets"
WHO_BASE = "https://www.who.int"
USER_AGENT = (
    "MedQA-RAG-ETL/1.0 (academic research; "
    "https://github.com/lehuyphuong/LLM-MedQA-Assistant)"
)
_config = {"delay": 2.0}  # mutable so main() can update
REQUEST_TIMEOUT = 30


# ── Specialty classifier (same as MedlinePlus) ──────────────────────
SPECIALTY_KEYWORDS = {
    "cardiology": ["heart", "cardiac", "cardiovascular", "hypertension", "blood pressure", "cholesterol"],
    "endocrinology": ["diabetes", "thyroid", "insulin", "hormone", "obesity"],
    "pulmonology": ["lung", "asthma", "respiratory", "copd", "pneumonia", "tuberculosis"],
    "neurology": ["brain", "neurological", "epilepsy", "stroke", "dementia", "alzheimer"],
    "oncology": ["cancer", "tumor", "chemotherapy", "carcinoma", "leukaemia", "lymphoma"],
    "infectious_disease": ["infection", "virus", "bacteria", "vaccine", "hiv", "malaria",
                           "hepatitis", "tuberculosis", "influenza", "covid", "ebola",
                           "cholera", "dengue", "measles", "rabies", "plague"],
    "psychiatry": ["mental health", "depression", "anxiety", "suicide", "substance abuse"],
    "pediatrics": ["child", "infant", "neonatal", "adolescent", "immunization"],
    "obstetrics_gynecology": ["pregnancy", "maternal", "reproductive", "contraception"],
    "gastroenterology": ["diarrhoea", "diarrhea", "hepatitis", "liver", "foodborne"],
    "ophthalmology": ["blindness", "vision", "trachoma"],
    "dermatology": ["skin", "burns", "leishmaniasis"],
    "hematology": ["blood", "anaemia", "anemia"],
    "environmental_health": ["air pollution", "water", "sanitation", "climate", "radiation"],
    "nutrition": ["nutrition", "malnutrition", "micronutrient", "breastfeeding", "obesity"],
}


def classify_specialty(title: str, body: str) -> str:
    combined = (title + " " + body).lower()
    scores: Dict[str, int] = {}
    for spec, keywords in SPECIALTY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > 0:
            scores[spec] = score
    return max(scores, key=scores.get) if scores else "general"


# ── HTTP helpers ─────────────────────────────────────────────────────
def _get(url: str, retries: int = 3) -> Optional[str]:
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            print(f"  [WARN] attempt {attempt+1}/{retries} for {url}: {exc}")
            time.sleep(_config["delay"] * (attempt + 1))
    return None


# ── Discover fact sheet URLs ─────────────────────────────────────────
def discover_fact_sheets(max_topics: int = 0) -> List[Dict[str, str]]:
    """Scrape WHO fact sheets listing page to get topic URLs."""
    print("[INFO] Discovering WHO fact sheets...")
    topics: List[Dict[str, str]] = []

    # WHO fact sheets page may have pagination or AJAX loading
    # Try the main listing page first
    page = 1
    while True:
        url = f"{WHO_FACT_SHEETS_URL}?page={page}" if page > 1 else WHO_FACT_SHEETS_URL
        html = _get(url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")

        # WHO uses various selectors for fact sheets - try common patterns
        links_found = 0
        for a in soup.select("a.sf-list-vertical__item, a.list-view--item, a[href*='/fact-sheets/detail/']"):
            href = a.get("href", "")
            title = a.get_text(strip=True)
            if not href or not title:
                continue
            if "/fact-sheets/detail/" in href:
                full_url = href if href.startswith("http") else WHO_BASE + href
                if not any(t["url"] == full_url for t in topics):
                    topics.append({"title": title, "url": full_url})
                    links_found += 1

        if links_found == 0:
            # Also try generic content links
            for a in soup.find_all("a", href=True):
                href = a["href"]
                title = a.get_text(strip=True)
                if "/fact-sheets/detail/" in href and title and len(title) > 3:
                    full_url = href if href.startswith("http") else WHO_BASE + href
                    if not any(t["url"] == full_url for t in topics):
                        topics.append({"title": title, "url": full_url})

        # Check for next page
        if page > 5 or (max_topics > 0 and len(topics) >= max_topics):
            break
        if not soup.select("a.next, a[rel='next'], .pagination__next"):
            break
        page += 1
        time.sleep(_config["delay"])

    if max_topics > 0:
        topics = topics[:max_topics]

    print(f"[INFO] Discovered {len(topics)} WHO fact sheets.")
    return topics


# ── Scrape individual fact sheet ─────────────────────────────────────
def scrape_fact_sheet(url: str, title: str, raw_dir: str) -> List[DocumentRecord]:
    """Scrape a single WHO fact sheet page."""
    html = _get(url)
    if not html:
        return []

    # Save raw HTML
    slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")[:60]
    raw_path = os.path.join(raw_dir, f"{slug}.html")
    with open(raw_path, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")

    # WHO fact sheets have main content in <article> or specific divs
    content = (
        soup.find("div", class_="sf-detail-body-wrapper")
        or soup.find("article")
        or soup.find("div", class_="content-section")
        or soup.find("main")
    )
    if not content:
        return []

    # Extract sections by heading
    records: List[DocumentRecord] = []
    current_section = "Overview"
    current_parts: List[str] = []
    _sec_idx = 0

    def _flush():
        body = "\n".join(current_parts).strip()
        if not body or len(body) < 50:
            return
        nonlocal _sec_idx
        _sec_idx += 1
        section_slug = re.sub(r"[^a-z0-9]+", "_", current_section.lower()).strip("_")[:30]
        doc_id = f"who_{slug}_{_sec_idx}_{section_slug}"
        specialty = classify_specialty(title, body)

        records.append(DocumentRecord(
            doc_id=doc_id,
            title=title,
            section_title=current_section,
            body=body,
            source_name="WHO",
            source_url=url,
            doc_type="guideline",
            specialty=specialty,
            audience="clinician",
            language="en",
            trust_tier=1,
            published_at="",
            updated_at="",
            tags=[specialty, "WHO", "fact sheet"],
            heading_path=f"{title} > {current_section}",
        ))

    from etl.html_utils import html_elem_to_text

    seen_li_ids = set()
    for elem in content.descendants:
        if not hasattr(elem, "name") or elem.name is None:
            continue
        if id(elem) in seen_li_ids:
            continue
        if elem.name in ("h1", "h2", "h3", "h4"):
            _flush()
            current_section = elem.get_text(strip=True)
            current_parts = []
        elif elem.name in ("ul", "ol"):
            # Process list items with formatting preserved
            for li in elem.find_all("li", recursive=False):
                seen_li_ids.add(id(li))
                text = html_elem_to_text(li)
                if text and len(text.strip("- ")) > 10:
                    current_parts.append(text)
            # Mark all nested <li> as processed
            for nested in elem.find_all("li"):
                seen_li_ids.add(id(nested))
        elif elem.name == "li":
            text = html_elem_to_text(elem)
            if text and len(text.strip("- ")) > 10:
                current_parts.append(text)
        elif elem.name in ("p", "td"):
            text = elem.get_text(separator=" ", strip=True)
            if text and len(text) > 10:
                current_parts.append(text)

    _flush()

    # If no sections found, try getting all text as one record
    if not records:
        all_text = content.get_text(separator="\n", strip=True)
        if all_text and len(all_text) > 100:
            doc_id = f"who_{slug}_full"
            specialty = classify_specialty(title, all_text)
            records.append(DocumentRecord(
                doc_id=doc_id,
                title=title,
                section_title="Full Article",
                body=all_text,
                source_name="WHO",
                source_url=url,
                doc_type="guideline",
                specialty=specialty,
                audience="clinician",
                language="en",
                trust_tier=1,
                tags=[specialty, "WHO"],
                heading_path=f"{title} > Full Article",
            ))

    return records


# ── Main ─────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="WHO fact sheets -> enriched JSONL")
    ap.add_argument("--raw-dir", required=True, help="Dir to save raw HTML")
    ap.add_argument("--output", required=True, help="Output JSONL path")
    ap.add_argument("--max-topics", type=int, default=0, help="Limit topics (0 = all)")
    ap.add_argument("--delay", type=float, default=_config["delay"])
    args = ap.parse_args()

    _config["delay"] = args.delay

    os.makedirs(args.raw_dir, exist_ok=True)

    # Discover
    topics = discover_fact_sheets(max_topics=args.max_topics)

    # Scrape
    all_records: List[DocumentRecord] = []
    for i, topic in enumerate(topics):
        print(f"  [{i+1}/{len(topics)}] {topic['title']}...")
        records = scrape_fact_sheet(topic["url"], topic["title"], args.raw_dir)
        all_records.extend(records)
        time.sleep(_config["delay"])

    # Write JSONL
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        for rec in all_records:
            fh.write(rec.to_jsonl_line() + "\n")

    print(f"\n[DONE] Wrote {len(all_records)} records to {args.output}")


if __name__ == "__main__":
    main()
