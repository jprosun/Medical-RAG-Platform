from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from services.utils.crawl_manifest import build_corpus_catalog, read_manifest, write_manifest
from services.utils.data_paths import RAG_DATA_ROOT, source_processed_dir, source_qa_dir
from tools.classify_pdfs import classify_pdf
from tools.extract_digital_pdf import write_processed_pdf_text


def _clean_text(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _html_to_text(raw_html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _clean_text(re.sub(r"<[^>]+>", " ", raw_html))

    soup = BeautifulSoup(raw_html, "html.parser")
    return _clean_text(soup.get_text(separator="\n", strip=True))


def _write_text_asset(source_id: str, stem: str, header: dict[str, str], body: str) -> Path:
    out_dir = source_processed_dir(source_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{stem}.txt"
    frontmatter = "---\n" + "\n".join(f"{key}: {value}" for key, value in header.items()) + "\n---\n\n"
    out_path.write_text(frontmatter + body, encoding="utf-8")
    return out_path


def _extract_medlineplus_xml(raw_path: Path) -> int:
    from pipelines.etl.medlineplus_scraper import parse_xml_to_records

    records = parse_xml_to_records(str(raw_path))
    for index, record in enumerate(records, start=1):
        stem = f"{Path(raw_path).stem}_{index:04d}"
        _write_text_asset(
            "medlineplus",
            stem,
            {
                "source_id": "medlineplus",
                "title": record.title,
                "source_url": record.source_url,
                "language": record.language,
            },
            record.body,
        )
    return len(records)


def extract_source(source_id: str) -> dict[str, int | str]:
    rows = read_manifest(source_id)
    unique_rows: dict[str, dict[str, str]] = {}
    for row in rows:
        rel = row.get("relative_path", "").strip()
        if rel:
            unique_rows[rel] = row

    processed = 0
    failed = 0
    missing_assets = 0
    deferred = 0
    digital_pdfs = 0
    scanned_pdfs = 0

    for rel_path, row in unique_rows.items():
        asset_path = RAG_DATA_ROOT / rel_path
        if not asset_path.exists():
            failed += 1
            missing_assets += 1
            _set_extract_status(rows, rel_path, strategy=row.get("extract_strategy", "backlog"), status="missing_asset")
            continue

        content_class = row.get("content_class", "")
        if content_class in {"image", "doc", "docx", "xls", "xlsx", "binary"}:
            deferred += 1
            _set_extract_status(rows, rel_path, strategy=row.get("extract_strategy", "backlog"), status="deferred")
            continue

        if content_class in {"html", "html_book"}:
            body = _html_to_text(asset_path.read_text(encoding="utf-8", errors="ignore"))
            _write_text_asset(
                source_id,
                asset_path.stem,
                {
                    "source_id": source_id,
                    "title": row.get("title_hint", asset_path.stem),
                    "item_url": row.get("item_url", ""),
                    "file_url": row.get("file_url", ""),
                },
                body,
            )
            processed += 1
            _set_extract_status(rows, rel_path, strategy="html_text", status="done")
            continue

        if content_class == "xml":
            count = _extract_medlineplus_xml(asset_path)
            processed += max(1, count)
            _set_extract_status(rows, rel_path, strategy="xml_text", status="done")
            continue

        if content_class == "pdf":
            category, _, _ = classify_pdf(asset_path)
            if category == "digital":
                result = write_processed_pdf_text(
                    {
                        "relative_path": rel_path,
                        "source_id": source_id,
                        "institution_or_journal": "",
                        "title": row.get("title_hint", asset_path.stem),
                        "item_url": row.get("item_url", ""),
                        "file_url": row.get("file_url", ""),
                        "extension": row.get("extension", ".pdf"),
                    }
                )
                if result is None:
                    failed += 1
                    _set_extract_status(rows, rel_path, strategy="digital_pdf_text", status="failed")
                else:
                    processed += 1
                    digital_pdfs += 1
                    _set_extract_status(rows, rel_path, strategy="digital_pdf_text", status="done")
            elif category == "scanned":
                deferred += 1
                scanned_pdfs += 1
                _set_extract_status(rows, rel_path, strategy="ocr_backlog", status="deferred")
            else:
                failed += 1
                _set_extract_status(rows, rel_path, strategy="classify_pdf", status="failed")
            continue

        if content_class in {"txt", "md", "csv", "jsonl", "json"}:
            body = asset_path.read_text(encoding="utf-8", errors="ignore")
            _write_text_asset(
                source_id,
                asset_path.stem,
                {
                    "source_id": source_id,
                    "title": row.get("title_hint", asset_path.stem),
                    "item_url": row.get("item_url", ""),
                    "file_url": row.get("file_url", ""),
                },
                body,
            )
            processed += 1
            _set_extract_status(rows, rel_path, strategy="universal_loader", status="done")
            continue

        deferred += 1
        _set_extract_status(rows, rel_path, strategy=row.get("extract_strategy", "backlog"), status="deferred")

    write_manifest(source_id, rows)
    build_corpus_catalog()

    report = {
        "source_id": source_id,
        "processed": processed,
        "failed": failed,
        "missing_assets": missing_assets,
        "deferred": deferred,
        "digital_pdfs": digital_pdfs,
        "scanned_pdfs": scanned_pdfs,
    }
    qa_dir = source_qa_dir(source_id)
    qa_dir.mkdir(parents=True, exist_ok=True)
    (qa_dir / "extract_summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def _set_extract_status(rows: list[dict[str, str]], rel_path: str, *, strategy: str, status: str) -> None:
    for row in rows:
        if row.get("relative_path", "").strip() == rel_path:
            row["extract_strategy"] = strategy
            row["extract_status"] = status


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract processed text for one source using manifest.csv.")
    parser.add_argument("--source-id", required=True)
    args = parser.parse_args()

    report = extract_source(args.source_id)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
