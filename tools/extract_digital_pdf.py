"""
Extract text from all digital PDFs identified by classify_pdfs.py.

For each PDF, outputs a .txt file preserving the canonical source structure:
  rag-data/sources/{source_id}/processed/{filename}.txt

Each .txt file includes metadata header + clean text per page.
"""
import csv
import json
import re
import sys
import time
from pathlib import Path

import fitz  # pymupdf

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_paths import RAG_DATA_ROOT, source_processed_dir

CATALOG = RAG_DATA_ROOT / "corpus_catalog.csv"
REPORT = REPO_ROOT / "tools" / "pdf_classification_report.json"
BASE_DIR = RAG_DATA_ROOT

# Load classification report to get set of scanned files (to skip)
with open(REPORT, "r", encoding="utf-8") as f:
    report = json.load(f)
scanned_paths = set(item["path"] for item in report.get("scanned_files", []))
corrupted_paths = set(item["path"] for item in report.get("corrupted_files", []))
non_pdf_paths = set(item["path"] for item in report.get("non_pdf_files", []))
skip_paths = scanned_paths | corrupted_paths | non_pdf_paths


def clean_text(text):
    """Basic text cleaning for PDF-extracted content."""
    # Remove excessive whitespace but keep paragraph breaks
    text = re.sub(r'[ \t]+', ' ', text)
    # Remove lines that are just numbers (page numbers)
    text = re.sub(r'^\s*\d{1,3}\s*$', '', text, flags=re.MULTILINE)
    # Collapse 3+ newlines into 2
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf(filepath):
    """Extract all text from a PDF. Returns (text, page_count)."""
    try:
        doc = fitz.open(filepath)
        pages = []
        for i, page in enumerate(doc):
            text = page.get_text()
            if text.strip():
                pages.append(text)
        doc.close()
        full_text = "\n\n".join(pages)
        return clean_text(full_text), len(pages)
    except Exception as e:
        return f"[ERROR extracting: {e}]", 0


def main():
    with open(CATALOG, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    total = 0
    extracted = 0
    skipped = 0
    errors = 0
    total_chars = 0
    total_pages = 0
    start = time.time()

    stats_by_source = {}

    for row in rows:
        rel_path = row.get("relative_path", "")
        source = row.get("source_id", "unknown")
        ext = row.get("extension", "").lower()

        if source not in stats_by_source:
            stats_by_source[source] = {"extracted": 0, "skipped": 0, "chars": 0, "pages": 0}

        total += 1

        # Skip non-digital PDFs
        if rel_path in skip_paths or ext != ".pdf":
            skipped += 1
            stats_by_source[source]["skipped"] += 1
            continue

        filepath = BASE_DIR / rel_path
        if not filepath.exists():
            skipped += 1
            stats_by_source[source]["skipped"] += 1
            continue

        # Extract text
        text, pages = extract_pdf(filepath)

        if not text or text.startswith("[ERROR"):
            errors += 1
            continue

        # Create output path
        basename = filepath.stem
        out_source_dir = source_processed_dir(source)
        out_source_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_source_dir / f"{basename}.txt"

        # Build metadata header
        header = f"""---
source_id: {source}
institution: {row.get('institution_or_journal', '')}
title: {row.get('title', basename)}
source_url: {row.get('item_url', '')}
file_url: {row.get('file_url', '')}
pages: {pages}
chars: {len(text)}
---

"""
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header + text)

        extracted += 1
        total_chars += len(text)
        total_pages += pages
        stats_by_source[source]["extracted"] += 1
        stats_by_source[source]["chars"] += len(text)
        stats_by_source[source]["pages"] += pages

        if extracted % 500 == 0:
            elapsed = time.time() - start
            print(f"  Extracted {extracted} files ({elapsed:.1f}s) ...")

    elapsed = time.time() - start

    print(f"\n{'='*60}")
    print(f"EXTRACTION COMPLETE in {elapsed:.1f}s")
    print(f"{'='*60}")
    print(f"  Files extracted: {extracted}")
    print(f"  Files skipped:   {skipped} (scanned/non-pdf/missing)")
    print(f"  Errors:          {errors}")
    print(f"  Total pages:     {total_pages}")
    print(f"  Total text:      {total_chars:,} chars ({total_chars/1024/1024:.1f} MB)")
    print()
    print("BY SOURCE:")
    for src, s in sorted(stats_by_source.items()):
        mb = s["chars"] / 1024 / 1024
        print(f"  {src:30s} | extracted={s['extracted']:>4} skipped={s['skipped']:>3} | pages={s['pages']:>5} | {mb:.1f} MB text")
    print(f"\nOutput root: {RAG_DATA_ROOT / 'sources'}")


if __name__ == "__main__":
    main()
