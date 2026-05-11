from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sys
from hashlib import sha1
from pathlib import Path
from typing import Any

import fitz  # pymupdf

REPO_ROOT = Path(__file__).resolve().parents[2]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from pipelines.crawl.extract_source import _clean_text, _fix_common_mojibake, _processed_asset_stem
from pipelines.etl.vn import vn_metadata_enricher, vn_quality_scorer, vn_text_cleaner, vn_title_extractor
from pipelines.etl.vn.vn_txt_to_jsonl import _parse_frontmatter, process_file
from services.utils.crawl_manifest import read_manifest, resolve_asset_path
from services.utils.data_lineage import build_file_lineage, make_run_id, relative_repo_path
from services.utils.data_paths import (
    source_partition_records_path,
    source_processed_dir,
    source_qa_dir,
    source_records_path,
)


SOURCE_ID = "vien_dinh_duong"
ARTICLE_PARTITION = "article_only"
LONG_BOOK_PARTITION = "long_pdf_book"
LONG_BOOK_OCR_PARTITION = "long_pdf_book_ocr"
ALL_EXTRACTABLE_PARTITION = "all_extractable"

logger = logging.getLogger(__name__)

_RE_TOC_DOTS = re.compile(r"\.{6,}\s*\d+\s*$")
_RE_TOC_NUMBERED_LINE = re.compile(r"^\s*(\d+|[IVXLC]+)\s+.+\s+\d+\s*$", re.IGNORECASE)
_BOOK_TITLE_MARKERS = (
    "mục lục",
    "sổ tay",
    "cẩm nang",
    "giáo trình",
    "dinh dưỡng lâm sàng",
    "handbook",
)
_NON_ARTICLE_TITLE_MARKERS = (
    "slide trình chiếu",
    "slide trinh chieu",
)
_GENERIC_TITLE_MARKERS = {
    "mục lục",
    "contents",
    "pdf",
    "document",
    "tải xuống",
    "tai xuong",
    "download",
}
_FILENAME_LIKE_TITLE = re.compile(r"^[A-Za-z0-9_\-]+(?:\.[A-Za-z0-9]+)?$")
_RE_DOWNLOAD_DATE_LINE = re.compile(r"^\s*\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\s*$")
_ARTICLE_WRAPPER_URL_PARTS = (
    "/about/",
    "/gioi-thieu",
    "/don-vi-trong-vien",
    "/chuc-nang-nhiem-vu",
    "/hop-tac-quoc-te",
    "/co-cau-to-chuc",
    "/site-map",
    "/tim-kiem",
)
_ARTICLE_WRAPPER_TITLE_MARKERS = (
    "trang ch",
    "gioi thiu",
    "giới thiệu",
    "hợp tác quốc tế",
    "chức năng nhiệm vụ",
    "xem chi tiết",
)


def _unique_manifest_rows(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    unique_rows: dict[str, dict[str, str]] = {}
    for row in rows:
        rel_path = (row.get("relative_path") or "").strip()
        if rel_path:
            unique_rows[rel_path] = row
    return unique_rows


def _processed_path_for_row(row: dict[str, str]) -> Path:
    rel_path = (row.get("relative_path") or "").strip()
    stem = _processed_asset_stem(rel_path, row.get("content_class", ""))
    return source_processed_dir(SOURCE_ID) / f"{stem}.txt"


def _records_jsonl_count(path: Path) -> int:
    if not path.exists():
        return 0
    with open(path, "r", encoding="utf-8") as fh:
        return sum(1 for raw in fh if raw.strip())


def _normalize_marker_text(text: str) -> str:
    cleaned = vn_text_cleaner.clean(_fix_common_mojibake(text or ""))
    cleaned = cleaned.lower()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _extract_body_from_processed(path: Path) -> tuple[dict[str, str], str]:
    raw_text = path.read_text(encoding="utf-8")
    meta, body = _parse_frontmatter(raw_text)
    return meta, vn_text_cleaner.clean(_fix_common_mojibake(body))


def _row_source_url(row: dict[str, str], meta: dict[str, str]) -> str:
    return (
        (meta.get("source_url") or meta.get("file_url") or meta.get("item_url") or "").strip()
        or (row.get("item_url") or row.get("file_url") or row.get("parent_item_url") or "").strip()
    )


def _looks_like_booklet_or_toc(processed_path: Path) -> tuple[bool, str]:
    try:
        meta, body = _extract_body_from_processed(processed_path)
    except Exception:
        return False, ""

    sample = body[:5000]
    lines = [line.strip() for line in sample.splitlines() if line.strip()]
    normalized_title = _normalize_marker_text(meta.get("title", ""))
    normalized_sample = _normalize_marker_text("\n".join(lines[:40]))
    pages = int(str(meta.get("pages", "0")).strip() or "0")

    if any(marker in normalized_title for marker in _BOOK_TITLE_MARKERS):
        return True, "title_book_marker"
    if any(marker in normalized_title for marker in _NON_ARTICLE_TITLE_MARKERS):
        return True, "title_non_article_marker"

    if "mục lục" in normalized_sample:
        return True, "body_toc_marker"

    toc_hits = sum(1 for line in lines[:80] if _RE_TOC_DOTS.search(line) or _RE_TOC_NUMBERED_LINE.search(line))
    if toc_hits >= 6:
        return True, "toc_line_density"

    if pages >= 4 and toc_hits >= 3:
        return True, "multi_page_toc_pattern"

    download_hits = sum(1 for line in lines[:80] if _normalize_marker_text(line) in {"tải xuống", "tai xuong", "ti xung"})
    date_hits = sum(1 for line in lines[:80] if _RE_DOWNLOAD_DATE_LINE.match(line))
    if download_hits >= 3 and date_hits >= 3:
        return True, "download_listing_pattern"

    return False, ""


def _looks_like_article_wrapper(processed_path: Path, row: dict[str, str]) -> tuple[bool, str]:
    try:
        meta, body = _extract_body_from_processed(processed_path)
    except Exception:
        return False, ""

    source_url = _row_source_url(row, meta).lower()
    normalized_title = _normalize_marker_text(meta.get("title", ""))
    normalized_sample = _normalize_marker_text("\n".join(line for line in body.splitlines()[:40] if line.strip()))

    if any(part in source_url for part in _ARTICLE_WRAPPER_URL_PARTS):
        return True, "admin_about_page"

    download_hits = sum(
        1
        for line in body.splitlines()[:80]
        if _normalize_marker_text(line) in {"tải xuống", "tai xuong", "ti xung", "download"}
    )
    date_hits = sum(1 for line in body.splitlines()[:80] if _RE_DOWNLOAD_DATE_LINE.match(line.strip()))
    if download_hits >= 3 and date_hits >= 3:
        return True, "download_listing_pattern"

    return False, ""


def _iter_article_only_processed_paths(max_assets: int | None = None) -> tuple[list[Path], list[dict[str, str]]]:
    rows = _unique_manifest_rows(read_manifest(SOURCE_ID))
    included: list[Path] = []
    excluded: list[dict[str, str]] = []

    for rel_path in sorted(rows):
        row = rows[rel_path]
        if (row.get("extract_status") or "").strip() != "done":
            continue

        processed_path = _processed_path_for_row(row)
        if not processed_path.exists():
            continue

        is_book_like, reason = _looks_like_booklet_or_toc(processed_path)
        if is_book_like:
            excluded.append(
                {
                    "relative_path": rel_path,
                    "processed_path": relative_repo_path(processed_path),
                    "extract_strategy": (row.get("extract_strategy") or "").strip(),
                    "source_url": (row.get("item_url") or row.get("file_url") or "").strip(),
                    "reason": reason,
                }
            )
            continue

        is_wrapper, wrapper_reason = _looks_like_article_wrapper(processed_path, row)
        if is_wrapper:
            excluded.append(
                {
                    "relative_path": rel_path,
                    "processed_path": relative_repo_path(processed_path),
                    "extract_strategy": (row.get("extract_strategy") or "").strip(),
                    "source_url": (row.get("item_url") or row.get("file_url") or "").strip(),
                    "reason": wrapper_reason,
                }
            )
            continue

        included.append(processed_path)
        if max_assets and len(included) >= max_assets:
            break

    return included, excluded


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(raw) for raw in fh if raw.strip()]


def _process_vn_files(paths: list[Path], *, output_path: Path, dry_run: bool = False) -> dict[str, Any]:
    etl_run_id = os.getenv("ETL_RUN_ID") or make_run_id("vien_dinh_duong_article_only", SOURCE_ID)

    total_records = 0
    skipped_files = 0
    score_sum = 0
    status_counts = {"go": 0, "review": 0, "hold": 0}
    all_records: list[dict[str, Any]] = []

    for path in paths:
        try:
            records = process_file(str(path), source_id=SOURCE_ID, etl_run_id=etl_run_id)
        except Exception as exc:
            logger.error("ERROR processing %s: %s", path, exc)
            skipped_files += 1
            continue

        if not records:
            skipped_files += 1
            continue

        for record in records:
            all_records.append(record)
            total_records += 1
            score_sum += int(record.get("quality_score", 0) or 0)
            status = str(record.get("quality_status", "hold") or "hold")
            status_counts[status] = status_counts.get(status, 0) + 1

    if not dry_run:
        _write_jsonl(output_path, all_records)

    avg_score = round(score_sum / max(1, total_records), 1)
    return {
        "source_id": SOURCE_ID,
        "subset": ARTICLE_PARTITION,
        "total_files": len(paths),
        "total_records": total_records,
        "skipped_files": skipped_files,
        "avg_quality_score": avg_score,
        "status_counts": status_counts,
        "output": str(output_path),
        "etl_run_id": etl_run_id,
    }


def run_article_only_etl(*, dry_run: bool = False, max_assets: int = 0) -> dict[str, Any]:
    article_paths, excluded_book_like = _iter_article_only_processed_paths(max_assets=max_assets or None)
    partition_output = source_partition_records_path(SOURCE_ID, ARTICLE_PARTITION)
    canonical_output = source_records_path(SOURCE_ID)
    summary_path = source_qa_dir(SOURCE_ID) / "article_only_etl_summary.json"
    excluded_path = source_qa_dir(SOURCE_ID) / "article_only_excluded_book_like.jsonl"

    summary = _process_vn_files(article_paths, output_path=partition_output, dry_run=dry_run)
    summary["excluded_book_like_assets"] = len(excluded_book_like)
    summary["excluded_book_like_manifest"] = str(excluded_path)

    if not dry_run:
        if partition_output.exists():
            canonical_output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(partition_output, canonical_output)
        excluded_path.parent.mkdir(parents=True, exist_ok=True)
        with open(excluded_path, "w", encoding="utf-8") as fh:
            for item in excluded_book_like:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    return summary


def _fallback_title_from_row(row: dict[str, str]) -> str:
    rel_path = (row.get("relative_path") or "").strip()
    stem = Path(rel_path).stem
    return re.sub(r"[_\-]+", " ", stem).strip() or stem or "Vien Dinh Duong Book"


def _is_usable_title_hint(title: str) -> bool:
    cleaned = _clean_text(_fix_common_mojibake(title or ""))
    normalized = _normalize_marker_text(cleaned)
    if not cleaned or normalized in _GENERIC_TITLE_MARKERS:
        return False
    return not _FILENAME_LIKE_TITLE.fullmatch(cleaned)


def _score_title_candidate(line: str) -> int:
    normalized = _normalize_marker_text(line)
    score = 0
    word_count = len(line.split())

    if normalized in _GENERIC_TITLE_MARKERS:
        return -100
    if _RE_TOC_DOTS.search(line) or _RE_TOC_NUMBERED_LINE.search(line):
        return -80
    if re.fullmatch(r"[\d.\-_/ ]+", line):
        return -90

    score += min(word_count, 8) * 5
    if len(line) >= 16:
        score += 6
    if line == line.upper():
        score += 10
    if re.search(r"\b\d+\s*[~\-]\s*\d+\b", line):
        score -= 12
    if normalized.startswith(("số ", "so ", "issue ", "vol ")):
        score -= 16
    if any(token in normalized for token in ("hội đồng biên tập", "chịu trách nhiệm", "thư ký", "ban biên tập")):
        score -= 16
    if any(token in normalized for token in ("pgs", "gs", "ts.", "bs.", "ths.")):
        score -= 8
    if any(token in normalized for token in ("viện dinh dưỡng", "bộ y tế", "trung tâm kiểm soát bệnh tật")):
        score -= 6
    return score


def _guess_book_title(
    row: dict[str, str],
    page_texts: list[tuple[int, str]],
    *,
    processed_hints: dict[str, str] | None = None,
) -> str:
    explicit = _clean_text(_fix_common_mojibake(row.get("title_hint", "")))
    if _is_usable_title_hint(explicit):
        return explicit

    if processed_hints:
        processed_title = str(processed_hints.get("title", "")).strip()
        if _is_usable_title_hint(processed_title):
            return _clean_text(_fix_common_mojibake(processed_title))

    best_line = ""
    best_score = -100
    for _, page_text in page_texts[:4]:
        for line in page_text.splitlines()[:40]:
            cleaned = _clean_text(_fix_common_mojibake(line))
            if not cleaned:
                continue
            if len(cleaned) < 8 or len(cleaned) > 180:
                continue
            score = _score_title_candidate(cleaned)
            if score > best_score:
                best_line = cleaned
                best_score = score

    if best_line and best_score >= 12:
        return best_line
    return _fallback_title_from_row(row)


def _processed_frontmatter_hints(row: dict[str, str]) -> dict[str, str]:
    processed_path = _processed_path_for_row(row)
    if not processed_path.exists():
        return {}
    try:
        meta, _ = _extract_body_from_processed(processed_path)
    except Exception:
        return {}
    return meta


def _extract_pdf_page_texts(asset_path: Path) -> tuple[list[tuple[int, str]], int]:
    doc = fitz.open(asset_path)
    try:
        page_count = len(doc)
        page_texts: list[tuple[int, str]] = []
        for index, page in enumerate(doc, start=1):
            text = _clean_text(_fix_common_mojibake(page.get_text("text", sort=True)))
            if text:
                page_texts.append((index, text))
        return page_texts, page_count
    finally:
        doc.close()


def _chunk_page_windows(
    page_texts: list[tuple[int, str]],
    *,
    max_pages: int = 4,
    max_chars: int = 5000,
) -> list[tuple[int, int, str]]:
    windows: list[tuple[int, int, str]] = []
    current_pages: list[int] = []
    current_texts: list[str] = []
    current_chars = 0

    for page_no, text in page_texts:
        if current_pages and (len(current_pages) >= max_pages or current_chars + len(text) > max_chars):
            windows.append((current_pages[0], current_pages[-1], "\n\n".join(current_texts)))
            current_pages = []
            current_texts = []
            current_chars = 0

        current_pages.append(page_no)
        current_texts.append(text)
        current_chars += len(text)

    if current_pages:
        windows.append((current_pages[0], current_pages[-1], "\n\n".join(current_texts)))
    return windows


def _make_book_doc_id(rel_path: str, start_page: int, end_page: int) -> str:
    raw = f"{SOURCE_ID}:{rel_path}:{start_page}:{end_page}".encode("utf-8")
    return sha1(raw).hexdigest()[:16]


def _build_book_record(
    row: dict[str, str],
    *,
    asset_path: Path,
    title: str,
    source_url: str,
    page_start: int,
    page_end: int,
    body: str,
    etl_run_id: str,
) -> dict[str, Any]:
    cleaned_body = vn_text_cleaner.clean(_fix_common_mojibake(body))
    enriched = vn_metadata_enricher.enrich(SOURCE_ID, title, cleaned_body)
    section_title = f"Pages {page_start}-{page_end}"
    heading_path = f"{title} > {section_title}"
    lineage = build_file_lineage(
        asset_path,
        source_id=SOURCE_ID,
        etl_run_id=etl_run_id,
        parent_file=(row.get("parent_item_url") or row.get("item_url") or row.get("file_url") or "").strip(),
    )

    record: dict[str, Any] = {
        "doc_id": _make_book_doc_id((row.get("relative_path") or "").strip(), page_start, page_end),
        "title": title,
        "body": cleaned_body,
        "source_name": enriched["source_name"],
        "section_title": section_title,
        "source_url": source_url,
        "source_id": SOURCE_ID,
        **lineage,
        "doc_type": "textbook",
        "specialty": enriched["specialty"],
        "audience": enriched["audience"],
        "language": enriched["language"],
        "canonical_title": title,
        "language_confidence": enriched["language_confidence"],
        "is_mixed_language": enriched["is_mixed_language"],
        "trust_tier": enriched["trust_tier"],
        "published_at": "",
        "updated_at": "",
        "tags": ["book_chunk", "long_pdf_book"],
        "heading_path": heading_path,
        "_section_count": 1,
        "_section_bodies": [cleaned_body],
    }

    quality = vn_quality_scorer.score(record)
    record["quality_score"] = quality["quality_score"]
    record["quality_status"] = quality["quality_status"]
    record["quality_flags"] = quality["quality_flags"]
    record.pop("_section_count", None)
    record.pop("_section_bodies", None)
    return record


def _iter_long_book_rows() -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    unique_rows = _unique_manifest_rows(read_manifest(SOURCE_ID))
    article_paths, excluded_book_like = _iter_article_only_processed_paths()
    excluded_rel_paths = {item["relative_path"] for item in excluded_book_like}

    deferred_rows: list[dict[str, str]] = []
    migrated_done_rows: list[dict[str, str]] = []
    for rel_path in sorted(unique_rows):
        row = unique_rows[rel_path]
        status = (row.get("extract_status") or "").strip()
        strategy = (row.get("extract_strategy") or "").strip()
        if status == "deferred" and strategy == "long_pdf_book":
            deferred_rows.append(row)
        elif status == "done" and rel_path in excluded_rel_paths:
            migrated_done_rows.append(row)
    return deferred_rows, migrated_done_rows


def run_long_pdf_book_etl(*, dry_run: bool = False, max_assets: int = 0) -> dict[str, Any]:
    deferred_rows, migrated_done_rows = _iter_long_book_rows()
    rows = deferred_rows + migrated_done_rows
    if max_assets:
        rows = rows[:max_assets]

    output_path = source_partition_records_path(SOURCE_ID, LONG_BOOK_PARTITION)
    summary_path = source_qa_dir(SOURCE_ID) / "long_pdf_book_etl_summary.json"
    etl_run_id = os.getenv("ETL_RUN_ID") or make_run_id("vien_dinh_duong_long_pdf_book", SOURCE_ID)

    all_records: list[dict[str, Any]] = []
    skipped_assets = 0
    extracted_assets = 0

    for row in rows:
        asset_path = resolve_asset_path((row.get("relative_path") or "").strip())
        if not asset_path.exists():
            skipped_assets += 1
            continue

        try:
            page_texts, page_count = _extract_pdf_page_texts(asset_path)
        except Exception as exc:
            logger.error("ERROR extracting long book %s: %s", asset_path, exc)
            skipped_assets += 1
            continue

        if not page_texts:
            skipped_assets += 1
            continue

        processed_hints = _processed_frontmatter_hints(row)
        title = _guess_book_title(row, page_texts, processed_hints=processed_hints)
        source_url = (
            (row.get("item_url") or row.get("file_url") or "").strip()
            or str(processed_hints.get("source_url", "")).strip()
            or str(processed_hints.get("file_url", "")).strip()
            or relative_repo_path(asset_path)
        )
        windows = _chunk_page_windows(page_texts)
        if not windows:
            skipped_assets += 1
            continue

        for page_start, page_end, body in windows:
            if len(body) < 300:
                continue
            record = _build_book_record(
                row,
                asset_path=asset_path,
                title=title,
                source_url=source_url,
                page_start=page_start,
                page_end=page_end,
                body=body,
                etl_run_id=etl_run_id,
            )
            record["tags"] = list(dict.fromkeys(record.get("tags", []) + [f"pages:{page_start}-{page_end}"]))
            all_records.append(record)

        extracted_assets += 1

    if not dry_run:
        _write_jsonl(output_path, all_records)

    summary = {
        "source_id": SOURCE_ID,
        "subset": LONG_BOOK_PARTITION,
        "input_assets": len(rows),
        "deferred_long_book_assets": len(deferred_rows),
        "migrated_book_like_done_assets": len(migrated_done_rows),
        "extracted_assets": extracted_assets,
        "skipped_assets": skipped_assets,
        "record_count": len(all_records),
        "output": str(output_path),
        "etl_run_id": etl_run_id,
    }
    if not dry_run:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _guess_ocr_title(row: dict[str, str]) -> str:
    title_hint = _clean_text(_fix_common_mojibake(row.get("title_hint", "")))
    if title_hint:
        return title_hint
    return _fallback_title_from_row(row)


def run_long_pdf_book_ocr_pipeline(*, dry_run: bool = False, max_assets: int = 0) -> dict[str, Any]:
    unique_rows = _unique_manifest_rows(read_manifest(SOURCE_ID))
    rows = [
        row
        for row in unique_rows.values()
        if (row.get("extract_status") or "").strip() == "deferred"
        and (row.get("extract_strategy") or "").strip() == "long_pdf_book_ocr"
    ]
    rows = sorted(rows, key=lambda row: (row.get("relative_path") or "").strip())
    if max_assets:
        rows = rows[:max_assets]

    output_path = source_partition_records_path(SOURCE_ID, LONG_BOOK_OCR_PARTITION, "ocr_jobs.jsonl")
    summary_path = source_qa_dir(SOURCE_ID) / "long_pdf_book_ocr_summary.json"
    backlog: list[dict[str, Any]] = []

    for row in rows:
        asset_path = resolve_asset_path((row.get("relative_path") or "").strip())
        page_count = 0
        if asset_path.exists():
            try:
                with fitz.open(asset_path) as doc:
                    page_count = len(doc)
            except Exception:
                page_count = 0

        backlog.append(
            {
                "source_id": SOURCE_ID,
                "relative_path": (row.get("relative_path") or "").strip(),
                "raw_path": relative_repo_path(asset_path) if asset_path.exists() else str(asset_path),
                "source_url": (row.get("item_url") or row.get("file_url") or "").strip(),
                "title_guess": _guess_ocr_title(row),
                "extract_strategy": "long_pdf_book_ocr",
                "page_count": page_count,
                "ocr_required": True,
                "reason": "scanned_long_book_requires_ocr",
            }
        )

    if not dry_run:
        _write_jsonl(output_path, backlog)

    summary = {
        "source_id": SOURCE_ID,
        "subset": LONG_BOOK_OCR_PARTITION,
        "input_assets": len(rows),
        "ocr_jobs": len(backlog),
        "output": str(output_path),
    }
    if not dry_run:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_all_extractable_export(*, dry_run: bool = False) -> dict[str, Any]:
    article_path = source_partition_records_path(SOURCE_ID, ARTICLE_PARTITION)
    book_path = source_partition_records_path(SOURCE_ID, LONG_BOOK_PARTITION)
    output_path = source_partition_records_path(SOURCE_ID, ALL_EXTRACTABLE_PARTITION)
    summary_path = source_qa_dir(SOURCE_ID) / "all_extractable_etl_summary.json"

    article_records = _read_jsonl(article_path)
    book_records = _read_jsonl(book_path)
    combined = article_records + book_records

    if not dry_run:
        _write_jsonl(output_path, combined)

    summary = {
        "source_id": SOURCE_ID,
        "subset": ALL_EXTRACTABLE_PARTITION,
        "article_records": len(article_records),
        "long_pdf_book_records": len(book_records),
        "record_count": len(combined),
        "output": str(output_path),
    }
    if not dry_run:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_partitioned_etl(*, dry_run: bool = False, max_assets: int = 0) -> dict[str, Any]:
    article_only = run_article_only_etl(dry_run=dry_run, max_assets=max_assets)
    long_pdf_book = run_long_pdf_book_etl(dry_run=dry_run, max_assets=max_assets)
    long_pdf_book_ocr = run_long_pdf_book_ocr_pipeline(dry_run=dry_run, max_assets=max_assets)
    all_extractable = run_all_extractable_export(dry_run=dry_run)
    return {
        "source_id": SOURCE_ID,
        "article_only": article_only,
        "long_pdf_book": long_pdf_book,
        "long_pdf_book_ocr": long_pdf_book_ocr,
        "all_extractable": all_extractable,
        "canonical_records_path": str(source_records_path(SOURCE_ID)),
        "article_partition_path": str(source_partition_records_path(SOURCE_ID, ARTICLE_PARTITION)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Partitioned ETL for vien_dinh_duong article-only and book backlogs.")
    parser.add_argument(
        "--mode",
        choices=("all", "article_only", "long_pdf_book", "long_pdf_book_ocr", "all_extractable"),
        default="all",
    )
    parser.add_argument("--max-assets", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if args.mode == "article_only":
        report = run_article_only_etl(dry_run=args.dry_run, max_assets=args.max_assets)
    elif args.mode == "long_pdf_book":
        report = run_long_pdf_book_etl(dry_run=args.dry_run, max_assets=args.max_assets)
    elif args.mode == "long_pdf_book_ocr":
        report = run_long_pdf_book_ocr_pipeline(dry_run=args.dry_run, max_assets=args.max_assets)
    elif args.mode == "all_extractable":
        report = run_all_extractable_export(dry_run=args.dry_run)
    else:
        report = run_partitioned_etl(dry_run=args.dry_run, max_assets=args.max_assets)

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
