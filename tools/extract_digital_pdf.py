"""
Extract text from digital PDFs listed in rag-data/corpus_catalog.csv.

Outputs one .txt per extracted PDF under:
  rag-data/sources/{source_id}/processed/{filename}.txt
"""

from __future__ import annotations

import csv
import json
import re
import time
from pathlib import Path

import fitz  # pymupdf

from services.utils.data_paths import RAG_DATA_ROOT, REPO_ROOT, source_processed_dir


CATALOG = RAG_DATA_ROOT / "corpus_catalog.csv"
REPORT = REPO_ROOT / "tools" / "pdf_classification_report.json"
BASE_DIR = RAG_DATA_ROOT


def load_skip_paths(report_path: Path = REPORT) -> set[str]:
    if not report_path.exists():
        return set()
    with open(report_path, "r", encoding="utf-8") as fh:
        report = json.load(fh)
    scanned_paths = {item["path"] for item in report.get("scanned_files", [])}
    corrupted_paths = {item["path"] for item in report.get("corrupted_files", [])}
    non_pdf_paths = {item["path"] for item in report.get("non_pdf_files", [])}
    return scanned_paths | corrupted_paths | non_pdf_paths


def clean_text(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"^\s*\d{1,3}\s*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_pdf(filepath: str | Path) -> tuple[str, int]:
    try:
        doc = fitz.open(filepath)
        pages = []
        for page in doc:
            text = page.get_text()
            if text.strip():
                pages.append(text)
        doc.close()
        return clean_text("\n\n".join(pages)), len(pages)
    except Exception as exc:
        return f"[ERROR extracting: {exc}]", 0


def write_processed_pdf_text(row: dict[str, str], *, base_dir: Path = BASE_DIR) -> tuple[Path, int, int] | None:
    rel_path = row.get("relative_path", "")
    source = row.get("source_id", "unknown")
    filepath = base_dir / rel_path
    if not filepath.exists():
        return None

    text, pages = extract_pdf(filepath)
    if not text or text.startswith("[ERROR"):
        return None

    out_source_dir = source_processed_dir(source)
    out_source_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_source_dir / f"{filepath.stem}.txt"
    header = f"""---
source_id: {source}
institution: {row.get('institution_or_journal', '')}
title: {row.get('title', filepath.stem)}
source_url: {row.get('item_url', '')}
file_url: {row.get('file_url', '')}
pages: {pages}
chars: {len(text)}
---

"""
    out_path.write_text(header + text, encoding="utf-8")
    return out_path, len(text), pages


def extract_catalog(catalog_path: Path = CATALOG, report_path: Path = REPORT) -> dict:
    skip_paths = load_skip_paths(report_path)
    with open(catalog_path, "r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    extracted = 0
    skipped = 0
    errors = 0
    total_chars = 0
    total_pages = 0
    start = time.time()
    stats_by_source: dict[str, dict[str, int]] = {}

    for row in rows:
        rel_path = row.get("relative_path", "")
        source = row.get("source_id", "unknown")
        ext = row.get("extension", "").lower()
        stats = stats_by_source.setdefault(source, {"extracted": 0, "skipped": 0, "chars": 0, "pages": 0})

        if rel_path in skip_paths or ext != ".pdf":
            skipped += 1
            stats["skipped"] += 1
            continue

        result = write_processed_pdf_text(row)
        if result is None:
            errors += 1
            continue

        _, chars, pages = result
        extracted += 1
        total_chars += chars
        total_pages += pages
        stats["extracted"] += 1
        stats["chars"] += chars
        stats["pages"] += pages

        if extracted % 500 == 0:
            elapsed = time.time() - start
            print(f"  Extracted {extracted} files ({elapsed:.1f}s) ...")

    return {
        "extracted": extracted,
        "skipped": skipped,
        "errors": errors,
        "total_chars": total_chars,
        "total_pages": total_pages,
        "elapsed_seconds": round(time.time() - start, 1),
        "by_source": stats_by_source,
    }


def main() -> None:
    report = extract_catalog()
    print(f"\n{'=' * 60}")
    print(f"EXTRACTION COMPLETE in {report['elapsed_seconds']:.1f}s")
    print(f"{'=' * 60}")
    print(f"  Files extracted: {report['extracted']}")
    print(f"  Files skipped:   {report['skipped']} (scanned/non-pdf/missing)")
    print(f"  Errors:          {report['errors']}")
    print(f"  Total pages:     {report['total_pages']}")
    print(f"  Total text:      {report['total_chars']:,} chars ({report['total_chars'] / 1024 / 1024:.1f} MB)")
    print()
    print("BY SOURCE:")
    for src, stats in sorted(report["by_source"].items()):
        mb = stats["chars"] / 1024 / 1024
        print(
            f"  {src:30s} | extracted={stats['extracted']:>4} skipped={stats['skipped']:>3} "
            f"| pages={stats['pages']:>5} | {mb:.1f} MB text"
        )
    print(f"\nOutput root: {RAG_DATA_ROOT / 'sources'}")


if __name__ == "__main__":
    main()

