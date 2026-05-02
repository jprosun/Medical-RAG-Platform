"""
Classify all PDFs in the corpus into categories:
  - digital: has extractable text layer
  - scanned: no text layer, needs OCR
  - corrupted: cannot be opened
  - non_pdf: not a PDF file

Outputs a JSON report to tools/pdf_classification_report.json by default.
"""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path

import fitz  # pymupdf

from services.utils.data_paths import RAG_DATA_ROOT, REPO_ROOT


CATALOG = RAG_DATA_ROOT / "corpus_catalog.csv"
REPORT_PATH = REPO_ROOT / "tools" / "pdf_classification_report.json"
BASE_DIR = RAG_DATA_ROOT
MIN_TEXT_CHARS = 50


def classify_pdf(filepath: str | Path) -> tuple[str, int, int]:
    """Returns (category, page_count, sample_text_length)."""
    try:
        doc = fitz.open(filepath)
        page_count = len(doc)
        if page_count == 0:
            doc.close()
            return "corrupted", 0, 0

        total_text = 0
        for i in range(min(3, page_count)):
            text = doc[i].get_text().strip()
            total_text += len(text)

        doc.close()
        if total_text >= MIN_TEXT_CHARS:
            return "digital", page_count, total_text
        return "scanned", page_count, total_text
    except Exception:
        return "corrupted", 0, 0


def classify_catalog(catalog_path: Path = CATALOG, report_path: Path = REPORT_PATH) -> dict:
    with open(catalog_path, "r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    results = {
        "digital": [],
        "scanned": [],
        "corrupted": [],
        "non_pdf": [],
    }
    stats_by_source: dict[str, dict[str, int]] = {}
    start = time.time()

    for index, row in enumerate(rows, start=1):
        ext = row.get("extension", "").lower()
        rel_path = row.get("relative_path", "")
        source = row.get("source_id", "unknown")
        filepath = BASE_DIR / rel_path

        stats = stats_by_source.setdefault(
            source,
            {"digital": 0, "scanned": 0, "corrupted": 0, "non_pdf": 0, "total_pages": 0},
        )

        if ext != ".pdf":
            results["non_pdf"].append({"source": source, "path": rel_path, "ext": ext})
            stats["non_pdf"] += 1
            continue

        if not filepath.exists():
            results["corrupted"].append({"source": source, "path": rel_path, "reason": "file_not_found"})
            stats["corrupted"] += 1
            continue

        category, pages, text_len = classify_pdf(filepath)
        results[category].append(
            {
                "source": source,
                "path": rel_path,
                "pages": pages,
                "sample_text_chars": text_len,
            }
        )
        stats[category] += 1
        stats["total_pages"] += pages

        if index % 500 == 0:
            elapsed = time.time() - start
            print(f"  Processed {index}/{len(rows)} ({elapsed:.1f}s)")

    elapsed = time.time() - start
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
        "scanned_files": results["scanned"],
        "corrupted_files": results["corrupted"],
        "non_pdf_files": results["non_pdf"],
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    return report


def main() -> None:
    report = classify_catalog()
    summary = report["summary"]
    print(f"\n{'=' * 60}")
    print(f"CLASSIFICATION COMPLETE in {summary['elapsed_seconds']:.1f}s")
    print(f"{'=' * 60}")
    print(f"  Digital (text extractable): {summary['digital_count']:>5} files, {summary['total_pages_digital']:>6} pages")
    print(f"  Scanned (needs OCR):        {summary['scanned_count']:>5} files, {summary['total_pages_scanned']:>6} pages")
    print(f"  Corrupted/Missing:          {summary['corrupted_count']:>5} files")
    print(f"  Non-PDF (docx/xlsx/etc):    {summary['non_pdf_count']:>5} files")
    print()
    print("BY SOURCE:")
    for src, stats in sorted(summary["by_source"].items()):
        print(
            f"  {src:30s} | digital={stats['digital']:>4} scanned={stats['scanned']:>3} "
            f"corrupted={stats['corrupted']:>3} non_pdf={stats['non_pdf']:>3} | pages={stats['total_pages']:>5}"
        )
    print(f"\nFull report saved to: {REPORT_PATH}")


if __name__ == "__main__":
    main()

