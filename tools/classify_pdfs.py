"""
Classify all PDFs in the corpus into categories:
  - digital: has extractable text layer (fast CPU extraction)
  - scanned: no text layer, needs OCR (GPU recommended)
  - corrupted: cannot be opened by pymupdf
  - non_pdf: not a PDF file (docx, xlsx, etc.)

Outputs a JSON report to tools/pdf_classification_report.json
"""
import os
import csv
import json
import time
import fitz  # pymupdf

CATALOG = r"d:\CODE\DATN\LLM-MedQA-Assistant\rag-data\corpus_catalog.csv"
BASE_DIR = r"d:\CODE\DATN\LLM-MedQA-Assistant\rag-data"
REPORT_PATH = r"d:\CODE\DATN\LLM-MedQA-Assistant\tools\pdf_classification_report.json"

# Minimum chars on first 3 pages to consider "digital"
MIN_TEXT_CHARS = 50

def classify_pdf(filepath):
    """Returns (category, page_count, sample_text_length)"""
    try:
        doc = fitz.open(filepath)
        page_count = len(doc)
        if page_count == 0:
            doc.close()
            return "corrupted", 0, 0

        # Sample first 3 pages (or all if fewer)
        total_text = 0
        for i in range(min(3, page_count)):
            text = doc[i].get_text().strip()
            total_text += len(text)

        doc.close()

        if total_text >= MIN_TEXT_CHARS:
            return "digital", page_count, total_text
        else:
            return "scanned", page_count, total_text

    except Exception as e:
        return "corrupted", 0, 0


def main():
    # Read catalog
    with open(CATALOG, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Total catalog entries: {len(rows)}")

    results = {
        "digital": [],
        "scanned": [],
        "corrupted": [],
        "non_pdf": [],
    }

    stats_by_source = {}
    start = time.time()
    processed = 0

    for row in rows:
        ext = row.get("extension", "").lower()
        rel_path = row.get("relative_path", "")
        source = row.get("source_id", "unknown")
        filepath = os.path.join(BASE_DIR, rel_path)

        if source not in stats_by_source:
            stats_by_source[source] = {"digital": 0, "scanned": 0, "corrupted": 0, "non_pdf": 0, "total_pages": 0}

        if ext != ".pdf":
            results["non_pdf"].append({
                "source": source,
                "path": rel_path,
                "ext": ext,
            })
            stats_by_source[source]["non_pdf"] += 1
            processed += 1
            continue

        if not os.path.exists(filepath):
            results["corrupted"].append({
                "source": source,
                "path": rel_path,
                "reason": "file_not_found",
            })
            stats_by_source[source]["corrupted"] += 1
            processed += 1
            continue

        category, pages, text_len = classify_pdf(filepath)
        results[category].append({
            "source": source,
            "path": rel_path,
            "pages": pages,
            "sample_text_chars": text_len,
        })
        stats_by_source[source][category] += 1
        stats_by_source[source]["total_pages"] += pages
        processed += 1

        if processed % 500 == 0:
            elapsed = time.time() - start
            print(f"  Processed {processed}/{len(rows)} ({elapsed:.1f}s)")

    elapsed = time.time() - start

    # Summary
    summary = {
        "total_files": len(rows),
        "digital_count": len(results["digital"]),
        "scanned_count": len(results["scanned"]),
        "corrupted_count": len(results["corrupted"]),
        "non_pdf_count": len(results["non_pdf"]),
        "total_pages_digital": sum(r["pages"] for r in results["digital"]),
        "total_pages_scanned": sum(r["pages"] for r in results["scanned"]),
        "elapsed_seconds": round(elapsed, 1),
        "by_source": stats_by_source,
    }

    report = {
        "summary": summary,
        "scanned_files": results["scanned"],  # List these — they need OCR
        "corrupted_files": results["corrupted"],
        "non_pdf_files": results["non_pdf"],
        # Don't list all digital files (too many), just count
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"CLASSIFICATION COMPLETE in {elapsed:.1f}s")
    print(f"{'='*60}")
    print(f"  Digital (text extractable): {summary['digital_count']:>5} files, {summary['total_pages_digital']:>6} pages")
    print(f"  Scanned (needs OCR):        {summary['scanned_count']:>5} files, {summary['total_pages_scanned']:>6} pages")
    print(f"  Corrupted/Missing:          {summary['corrupted_count']:>5} files")
    print(f"  Non-PDF (docx/xlsx/etc):    {summary['non_pdf_count']:>5} files")
    print()
    print("BY SOURCE:")
    for src, s in sorted(stats_by_source.items()):
        print(f"  {src:30s} | digital={s['digital']:>4} scanned={s['scanned']:>3} corrupted={s['corrupted']:>3} non_pdf={s['non_pdf']:>3} | pages={s['total_pages']:>5}")
    print(f"\nFull report saved to: {REPORT_PATH}")


if __name__ == "__main__":
    main()
