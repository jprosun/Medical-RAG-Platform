"""
ETL: NCBI Bookshelf Scraper (StatPearls / InformedHealth.org)
=============================================================

Downloads open-access medical textbook chapters from NCBI Bookshelf
using the NCBI E-utilities API (free, public, no key required for low volume).

Primary sources:
  - StatPearls (NBK430685) - comprehensive clinical reference
  - InformedHealth.org (NBK390356) - patient-friendly explanations

Usage:
    python -m etl.ncbi_bookshelf_scraper \\
        --raw-dir ../../data/data_raw/ncbi_bookshelf \\
        --output  ../../data/data_final/ncbi_bookshelf.jsonl \\
        --max-chapters 100
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import hashlib
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
NCBI_ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
NCBI_EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
NCBI_BOOKSHELF_BASE = "https://www.ncbi.nlm.nih.gov/books"

# Key medical topics to search for in StatPearls
MEDICAL_TOPICS = [
    "hypertension", "diabetes mellitus", "asthma", "pneumonia",
    "heart failure", "myocardial infarction", "stroke",
    "chronic obstructive pulmonary disease", "tuberculosis",
    "hepatitis", "cirrhosis", "chronic kidney disease",
    "anemia", "thyroid disorders", "epilepsy",
    "depression", "anxiety disorders", "schizophrenia",
    "breast cancer", "lung cancer", "colorectal cancer",
    "osteoarthritis", "rheumatoid arthritis",
    "HIV AIDS", "malaria", "dengue fever",
    "pregnancy complications", "gestational diabetes",
    "pediatric asthma", "neonatal jaundice",
    "migraine", "Parkinson disease", "Alzheimer disease",
    "appendicitis", "cholecystitis", "pancreatitis",
    "urinary tract infection", "sepsis", "meningitis",
    "atrial fibrillation", "deep vein thrombosis",
    "psoriasis", "eczema", "acne vulgaris",
    "glaucoma", "cataracts",
    "hypothyroidism", "hyperthyroidism",
    "iron deficiency anemia", "vitamin D deficiency",
    "obesity", "metabolic syndrome",
]

USER_AGENT = (
    "MedQA-RAG-ETL/1.0 (academic research; "
    "https://github.com/lehuyphuong/LLM-MedQA-Assistant)"
)
_config = {"delay": 0.5}  # mutable so main() can update
REQUEST_TIMEOUT = 30


# ── Specialty classifier ─────────────────────────────────────────────
SPECIALTY_KEYWORDS = {
    "cardiology": ["heart", "cardiac", "cardiovascular", "hypertension", "coronary",
                    "atrial fibrillation", "myocardial", "thrombosis"],
    "endocrinology": ["diabetes", "thyroid", "insulin", "hormone", "metabolic",
                      "hypothyroid", "hyperthyroid", "obesity"],
    "pulmonology": ["lung", "asthma", "respiratory", "copd", "pneumonia", "pulmonary",
                    "tuberculosis"],
    "neurology": ["brain", "neurological", "epilepsy", "stroke", "migraine",
                  "parkinson", "alzheimer", "seizure", "meningitis"],
    "oncology": ["cancer", "tumor", "chemotherapy", "carcinoma", "lymphoma", "leukemia"],
    "gastroenterology": ["liver", "hepatitis", "cirrhosis", "pancreatitis",
                         "cholecystitis", "appendicitis", "gastric"],
    "nephrology": ["kidney", "renal", "urinary tract"],
    "infectious_disease": ["infection", "virus", "bacteria", "hiv", "aids", "malaria",
                           "dengue", "sepsis", "tuberculosis"],
    "psychiatry": ["depression", "anxiety", "schizophrenia", "mental", "psychiatric",
                   "bipolar"],
    "obstetrics_gynecology": ["pregnancy", "gestational", "prenatal", "neonatal"],
    "orthopedics": ["arthritis", "osteoarthritis", "rheumatoid", "joint", "fracture"],
    "dermatology": ["psoriasis", "eczema", "acne", "skin", "dermatitis"],
    "ophthalmology": ["glaucoma", "cataract", "eye", "retina"],
    "hematology": ["anemia", "blood", "iron deficiency", "thrombosis"],
    "pediatrics": ["pediatric", "neonatal", "child", "infant"],
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
def _get(url: str, params: dict = None, retries: int = 3) -> Optional[str]:
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            print(f"  [WARN] attempt {attempt+1}/{retries}: {exc}")
            time.sleep(_config["delay"] * (attempt + 1))
    return None


# ── Search NCBI Bookshelf ────────────────────────────────────────────
def search_bookshelf(query: str, max_results: int = 5) -> List[str]:
    """Search NCBI Bookshelf for a topic, return list of book chapter IDs."""
    params = {
        "db": "books",
        "term": f"{query}[title] AND statpearls[book]",
        "retmax": max_results,
        "retmode": "json",
    }
    text = _get(NCBI_ESEARCH, params=params)
    if not text:
        return []

    try:
        data = json.loads(text)
        ids = data.get("esearchresult", {}).get("idlist", [])
        return ids
    except (json.JSONDecodeError, KeyError):
        return []


# ── Fetch chapter content ────────────────────────────────────────────
def fetch_chapter(chapter_id: str, raw_dir: str) -> Optional[Dict]:
    """Fetch a chapter from NCBI Bookshelf and return parsed content."""
    # Check cache first
    cache_path = os.path.join(raw_dir, f"{chapter_id}.html")
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            html = f.read()
    else:
        url = f"{NCBI_BOOKSHELF_BASE}/{chapter_id}/"
        html = _get(url)
        if not html:
            return None
        # Save raw
        os.makedirs(raw_dir, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(html)
        time.sleep(_config["delay"])

    soup = BeautifulSoup(html, "html.parser")

    # Extract title - prioritize <title> tag because <h1> is often the generic 'Bookshelf' site nav
    title_elem = soup.title
    title = title_elem.get_text(strip=True) if title_elem else f"Chapter {chapter_id}"
    
    # Clean title
    title = re.sub(r"\s*-\s*StatPearls.*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*-\s*NCBI Bookshelf.*$", "", title, flags=re.IGNORECASE)
    title = title.strip()
    if title.lower() == "bookshelf" or not title:
        title = f"StatPearls Chapter {chapter_id}"

    # Extract main content
    content = (
        soup.find("div", class_="body-content")
        or soup.find("div", id="body-content")
        or soup.find("article")
        or soup.find("div", class_="jig-ncbiinpagenav")
        or soup.find("main")
    )
    if not content:
        return None

    # Parse into sections
    sections: List[Dict[str, str]] = []
    current_section = "Introduction"
    current_parts: List[str] = []

    # Boilerplate sections to skip (they cause massive cross-doc duplication)
    BOILERPLATE_SECTIONS = {"review questions", "references", "continuing education activity"}
    
    def _flush_section():
        body = "\n".join(current_parts).strip()
        if body and len(body) > 50:
            if current_section.lower() not in BOILERPLATE_SECTIONS:
                sections.append({
                    "section_title": current_section,
                    "body": body,
                })

    from etl.html_utils import html_elem_to_text

    seen_elems = set()
    for elem in content.find_all(["h2", "h3", "p", "ul", "ol", "li"]):
        # Skip elements inside a <ul>/<ol> we already processed
        if id(elem) in seen_elems:
            continue
        if elem.name in ("h2", "h3"):
            _flush_section()
            current_section = elem.get_text(strip=True)
            current_parts = []
        elif elem.name in ("ul", "ol"):
            # Process all <li> children and mark as seen
            for li in elem.find_all("li", recursive=False):
                seen_elems.add(id(li))
                text = html_elem_to_text(li)
                if text and len(text.strip("- ")) > 10:
                    current_parts.append(text)
            # Mark this ul/ol and its nested li as processed
            for nested in elem.find_all("li"):
                seen_elems.add(id(nested))
        elif elem.name == "li":
            # Only process if not already handled by parent ul/ol
            text = html_elem_to_text(elem)
            if text and len(text.strip("- ")) > 10:
                current_parts.append(text)
        elif elem.name == "p":
            text = elem.get_text(separator=" ", strip=True)
            if text and len(text) > 10:
                current_parts.append(text)

    _flush_section()

    if not sections:
        # Fallback: get all text
        all_text = content.get_text(separator="\n", strip=True)
        if all_text and len(all_text) > 100:
            sections.append({
                "section_title": "Full Content",
                "body": all_text[:5000],  # Cap at 5000 chars for safety
            })

    return {"title": title, "chapter_id": chapter_id, "sections": sections}


# ── Main ─────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="NCBI Bookshelf -> enriched JSONL")
    ap.add_argument("--raw-dir", required=True, help="Dir to save raw HTML")
    ap.add_argument("--output", required=True, help="Output JSONL path")
    ap.add_argument("--max-chapters", type=int, default=100, help="Max chapters total")
    ap.add_argument("--delay", type=float, default=_config["delay"])
    args = ap.parse_args()

    _config["delay"] = args.delay

    os.makedirs(args.raw_dir, exist_ok=True)

    # Search for chapters on each medical topic
    all_chapter_ids: List[str] = []
    seen_ids: set = set()

    print(f"[INFO] Searching NCBI Bookshelf for {len(MEDICAL_TOPICS)} medical topics...")
    for topic in MEDICAL_TOPICS:
        if len(all_chapter_ids) >= args.max_chapters:
            break
        ids = search_bookshelf(topic, max_results=3)
        for cid in ids:
            if cid not in seen_ids and len(all_chapter_ids) < args.max_chapters:
                all_chapter_ids.append(cid)
                seen_ids.add(cid)
        time.sleep(_config["delay"])

    print(f"[INFO] Found {len(all_chapter_ids)} unique chapters to fetch.")

    # Fetch and parse each chapter
    all_records: List[DocumentRecord] = []
    seen_body_hashes: set = set()
    
    for i, chapter_id in enumerate(all_chapter_ids):
        print(f"  [{i+1}/{len(all_chapter_ids)}] Fetching {chapter_id}...")
        result = fetch_chapter(chapter_id, args.raw_dir)
        if not result:
            continue

        title = result["title"]
        for sec_idx, sec in enumerate(result["sections"]):
            body = sec["body"].strip()
            # Deduplicate exact section body texts across chapters
            norm_body = " ".join(body.lower().split())[:500]
            body_hash = hashlib.md5(norm_body.encode("utf-8")).hexdigest()
            if body_hash in seen_body_hashes:
                continue
            seen_body_hashes.add(body_hash)

            slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")[:40]
            sec_slug = re.sub(r"[^a-z0-9]+", "_", sec["section_title"].lower()).strip("_")[:30]
            # Use chapter_id to guarantee global uniqueness, and sec_idx for local uniqueness
            doc_id = f"ncbi_{chapter_id}_{sec_idx}_{sec_slug}"
            specialty = classify_specialty(title, body)

            all_records.append(DocumentRecord(
                doc_id=doc_id,
                title=title,
                section_title=sec["section_title"],
                body=body,
                source_name="NCBI Bookshelf",
                source_url=f"{NCBI_BOOKSHELF_BASE}/{chapter_id}/",
                doc_type="textbook",
                specialty=specialty,
                audience="student",
                language="en",
                trust_tier=2,
                tags=[specialty, "StatPearls"],
                heading_path=f"{title} > {sec['section_title']}",
            ))

    # Write JSONL
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        for rec in all_records:
            fh.write(rec.to_jsonl_line() + "\n")

    print(f"\n[DONE] Wrote {len(all_records)} records to {args.output}")


if __name__ == "__main__":
    main()
