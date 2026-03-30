"""
ETL: MedlinePlus Health Topics Scraper (XML Feed)
==================================================

Downloads the official MedlinePlus Health Topics XML data dump and converts
each topic into enriched JSONL conforming to DocumentRecord schema.

MedlinePlus publishes daily XML dumps (public domain, NLM/NIH):
  https://medlineplus.gov/xml/mplus_topics_2025-03-10.xml

Usage:
    python -m etl.medlineplus_scraper \\
        --raw-dir ../../data/data_raw/medlineplus \\
        --output  ../../data/data_final/medlineplus.jsonl
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
from datetime import date

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print(
        "ERROR: This script requires 'requests' and 'beautifulsoup4'.\n"
        "Install with:  pip install requests beautifulsoup4 lxml",
        file=sys.stderr,
    )
    sys.exit(1)

# Add parent to path so we can import from app
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.document_schema import DocumentRecord


# ── Constants ────────────────────────────────────────────────────────
MEDLINEPLUS_XML_URL = "https://medlineplus.gov/xml/mplus_topics_2026-03-10.xml"
MEDLINEPLUS_XML_FALLBACK = "https://medlineplus.gov/xml/mplus_topics.xml"
USER_AGENT = (
    "MedQA-RAG-ETL/1.0 (academic research; "
    "https://github.com/lehuyphuong/LLM-MedQA-Assistant)"
)
REQUEST_TIMEOUT = 60


# ── Specialty classifier ─────────────────────────────────────────────
SPECIALTY_KEYWORDS = {
    "cardiology": ["heart", "cardiac", "cardiovascular", "hypertension", "blood pressure",
                    "cholesterol", "arrhythmia", "coronary", "atrial", "aorta"],
    "endocrinology": ["diabetes", "thyroid", "insulin", "hormone", "endocrine",
                      "metabolic", "adrenal", "pituitary", "glucose"],
    "pulmonology": ["lung", "asthma", "respiratory", "breathing", "copd", "pneumonia",
                    "bronchitis", "pulmonary", "tuberculosis"],
    "neurology": ["brain", "neurological", "seizure", "epilepsy", "stroke", "headache",
                  "migraine", "alzheimer", "parkinson", "multiple sclerosis", "neuropathy"],
    "orthopedics": ["bone", "joint", "fracture", "arthritis", "sports injury",
                    "musculoskeletal", "osteoporosis", "spine", "scoliosis"],
    "oncology": ["cancer", "tumor", "chemotherapy", "oncology", "malignant",
                 "leukemia", "lymphoma", "carcinoma"],
    "gastroenterology": ["stomach", "liver", "intestine", "digestive", "gastric",
                         "hepatitis", "cirrhosis", "colon", "crohn", "celiac"],
    "nephrology": ["kidney", "renal", "dialysis", "urinary"],
    "dermatology": ["skin", "dermatitis", "rash", "eczema", "psoriasis", "acne", "melanoma"],
    "pediatrics": ["child", "infant", "pediatric", "newborn", "adolescent", "toddler"],
    "obstetrics_gynecology": ["pregnancy", "prenatal", "obstetric", "gynecol",
                              "menstrual", "uterine", "fertility", "contraception"],
    "infectious_disease": ["infection", "virus", "bacteria", "vaccine", "hiv",
                           "hepatitis", "tuberculosis", "malaria", "influenza", "covid"],
    "psychiatry": ["mental health", "depression", "anxiety", "psychiatric",
                   "bipolar", "schizophrenia", "ptsd", "eating disorder"],
    "hematology": ["blood", "anemia", "hemophilia", "sickle cell", "platelet", "clotting"],
    "ophthalmology": ["eye", "vision", "glaucoma", "cataract", "retina", "macular"],
    "ent": ["ear", "hearing", "throat", "tonsil", "sinus", "nose"],
}


def classify_specialty(title: str, body: str) -> str:
    combined = (title + " " + body).lower()
    scores: Dict[str, int] = {}
    for spec, keywords in SPECIALTY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > 0:
            scores[spec] = score
    if scores:
        return max(scores, key=scores.get)
    return "general"


# ── Download XML ─────────────────────────────────────────────────────
def download_xml(raw_dir: str) -> str:
    """Download MedlinePlus XML dump to raw_dir. Returns path to saved XML."""
    os.makedirs(raw_dir, exist_ok=True)
    xml_path = os.path.join(raw_dir, "mplus_topics.xml")

    if os.path.exists(xml_path):
        size_mb = os.path.getsize(xml_path) / (1024 * 1024)
        if size_mb > 1:
            print(f"[INFO] Using existing XML file: {xml_path} ({size_mb:.1f} MB)")
            return xml_path

    headers = {"User-Agent": USER_AGENT}
    for url in [MEDLINEPLUS_XML_URL, MEDLINEPLUS_XML_FALLBACK]:
        print(f"[INFO] Downloading MedlinePlus XML from {url} ...")
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, stream=True)
            resp.raise_for_status()
            with open(xml_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            size_mb = os.path.getsize(xml_path) / (1024 * 1024)
            print(f"[INFO] Saved {size_mb:.1f} MB to {xml_path}")
            return xml_path
        except requests.RequestException as exc:
            print(f"[WARN] Failed to download from {url}: {exc}")
            continue

    raise SystemExit("[ERROR] Could not download MedlinePlus XML from any URL.")


# ── Parse XML ────────────────────────────────────────────────────────
def _clean_html(html_text: str) -> str:
    """Strip HTML tags and clean whitespace, preserving list formatting."""
    if not html_text:
        return ""
    from etl.html_utils import clean_html_preserve_lists
    return clean_html_preserve_lists(html_text)


def _extract_tags_from_groups(topic_elem) -> List[str]:
    """Extract group names as tags."""
    tags = []
    for group in topic_elem.find_all("group"):
        name = group.get_text(strip=True)
        if name:
            tags.append(name.lower())
    return tags


def parse_xml_to_records(xml_path: str, max_topics: int = 0) -> List[DocumentRecord]:
    """Parse MedlinePlus XML into DocumentRecord objects."""
    print(f"[INFO] Parsing XML: {xml_path}")

    with open(xml_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "lxml-xml")

    topics = soup.find_all("health-topic")
    if max_topics > 0:
        topics = topics[:max_topics]

    print(f"[INFO] Found {len(topics)} health topics in XML.")

    records: List[DocumentRecord] = []
    for topic in topics:
        title = topic.get("title", "").strip()
        url = topic.get("url", "").strip()
        topic_id = topic.get("id", "").strip()
        language = topic.get("language", "English").strip()
        lang_code = "en" if language.lower() == "english" else "es"
        date_created = topic.get("date-created", "").strip()

        # Extract full summary (the main content)
        full_summary_elem = topic.find("full-summary")
        full_summary = ""
        if full_summary_elem:
            full_summary = _clean_html(full_summary_elem.get_text())

        if not full_summary or len(full_summary) < 50:
            # Try also-called or other content
            also_called = topic.find("also-called")
            if also_called:
                full_summary = f"Also known as: {also_called.get_text(strip=True)}"

        if not full_summary or len(full_summary) < 30:
            continue

        # Tags from groups
        tags = _extract_tags_from_groups(topic)

        # Specialty classification
        specialty = classify_specialty(title, full_summary)

        # Build doc_id (include language to avoid EN/ES overlaps)
        slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")[:60]
        doc_id = f"medlineplus_{lang_code}_{slug}"

        records.append(DocumentRecord(
            doc_id=doc_id,
            title=title,
            section_title="Full Summary",
            body=full_summary,
            source_name="MedlinePlus",
            source_url=url,
            doc_type="patient_education",
            specialty=specialty,
            audience="patient",
            language=lang_code,
            trust_tier=3,
            published_at=date_created,
            updated_at=date_created,
            tags=tags if tags else [specialty],
            heading_path=f"{title} > Full Summary",
        ))

        # Also extract "see also" / related topics as tags (useful for linking)

    print(f"[INFO] Extracted {len(records)} records from XML.")
    return records


# ── Main ─────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="MedlinePlus XML → enriched JSONL")
    ap.add_argument("--raw-dir", required=True, help="Dir to save raw XML (data/data_raw/medlineplus)")
    ap.add_argument("--output", required=True, help="Output JSONL path (data/data_final/medlineplus.jsonl)")
    ap.add_argument("--max-topics", type=int, default=0, help="Limit topics (0 = all)")
    ap.add_argument("--skip-download", action="store_true", help="Skip download, use existing XML")
    args = ap.parse_args()

    # Download
    if not args.skip_download:
        xml_path = download_xml(args.raw_dir)
    else:
        xml_path = os.path.join(args.raw_dir, "mplus_topics.xml")
        if not os.path.exists(xml_path):
            raise SystemExit(f"[ERROR] XML not found: {xml_path}")

    # Parse
    records = parse_xml_to_records(xml_path, max_topics=args.max_topics)

    # Write JSONL
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(rec.to_jsonl_line() + "\n")

    print(f"[DONE] Wrote {len(records)} records to {args.output}")


if __name__ == "__main__":
    main()
