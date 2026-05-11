from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from pipelines.crawl.extract_source import _clean_text, _fix_common_mojibake, _processed_asset_stem
from pipelines.etl.vn import vn_text_cleaner
from pipelines.etl.vn.vn_txt_to_jsonl import _parse_frontmatter, process_file
from services.utils.crawl_manifest import read_manifest
from services.utils.data_lineage import make_run_id, relative_repo_path
from services.utils.data_paths import (
    source_partition_records_path,
    source_processed_dir,
    source_qa_dir,
    source_records_path,
)


SOURCE_ID = "vmj_ojs"
ARTICLE_PARTITION = "article_only"
ISSUE_BUNDLE_PARTITION = "issue_bundle_backlog"

logger = logging.getLogger(__name__)

_GENERIC_TITLE_MARKERS = {"pdf", "document"}
_CONFERENCE_TITLE_MARKERS = (
    "hội nghị",
    "hội thảo",
    "khoa học thường niên",
    "toàn quốc",
    "kỷ yếu",
    "ky yeu",
    "proceedings",
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


def _normalize_text(text: str) -> str:
    cleaned = vn_text_cleaner.clean(_fix_common_mojibake(text or ""))
    cleaned = cleaned.lower()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _extract_body_from_processed(path: Path) -> tuple[dict[str, str], str]:
    raw_text = path.read_text(encoding="utf-8")
    meta, body = _parse_frontmatter(raw_text)
    return meta, vn_text_cleaner.clean(_fix_common_mojibake(body))


def _issue_bundle_reason(processed_path: Path) -> tuple[bool, dict[str, Any]]:
    try:
        meta, body = _extract_body_from_processed(processed_path)
    except Exception:
        return False, {}

    title = _clean_text(_fix_common_mojibake(meta.get("title", "")))
    normalized_title = _normalize_text(title)
    pages = int(str(meta.get("pages", "0")).strip() or "0")
    chars = int(str(meta.get("chars", "0")).strip() or "0")
    normalized_body = _normalize_text(body[:20000])

    reason = ""
    if normalized_title in _GENERIC_TITLE_MARKERS:
        reason = "generic_pdf_title"
    elif pages >= 100 or chars >= 200000:
        reason = "oversized_issue_bundle"
    elif pages >= 50 or chars >= 100000:
        reason = "large_multi_article_pdf"
    elif any(marker in normalized_title for marker in _CONFERENCE_TITLE_MARKERS):
        reason = "conference_or_proceedings_title"
    else:
        abstract_hits = normalized_body.count("tóm tắt") + normalized_body.count("tom tat") + normalized_body.count("summary")
        if abstract_hits >= 3:
            reason = "multi_abstract_pattern"

    return bool(reason), {
        "title": title,
        "pages": pages,
        "chars": chars,
        "source_url": (meta.get("source_url") or meta.get("file_url") or "").strip(),
        "reason": reason,
    }


def _iter_article_only_processed_paths(max_assets: int | None = None) -> tuple[list[Path], list[dict[str, Any]]]:
    rows = _unique_manifest_rows(read_manifest(SOURCE_ID))
    included: list[Path] = []
    excluded: list[dict[str, Any]] = []

    for rel_path in sorted(rows):
        row = rows[rel_path]
        if (row.get("extract_status") or "").strip() != "done":
            continue

        processed_path = _processed_path_for_row(row)
        if not processed_path.exists():
            continue

        is_bundle, details = _issue_bundle_reason(processed_path)
        if is_bundle:
            excluded.append(
                {
                    "relative_path": rel_path,
                    "processed_path": relative_repo_path(processed_path),
                    "extract_strategy": (row.get("extract_strategy") or "").strip(),
                    "source_url": (row.get("item_url") or row.get("file_url") or details.get("source_url") or "").strip(),
                    **details,
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


def _process_vn_files(paths: list[Path], *, output_path: Path, dry_run: bool = False) -> dict[str, Any]:
    etl_run_id = os.getenv("ETL_RUN_ID") or make_run_id("vmj_ojs_article_only", SOURCE_ID)

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
    article_paths, excluded_issue_bundles = _iter_article_only_processed_paths(max_assets=max_assets or None)
    partition_output = source_partition_records_path(SOURCE_ID, ARTICLE_PARTITION)
    canonical_output = source_records_path(SOURCE_ID)
    summary_path = source_qa_dir(SOURCE_ID) / "article_only_etl_summary.json"
    excluded_path = source_qa_dir(SOURCE_ID) / "issue_bundle_backlog.jsonl"

    summary = _process_vn_files(article_paths, output_path=partition_output, dry_run=dry_run)
    summary["excluded_issue_bundle_assets"] = len(excluded_issue_bundles)
    summary["excluded_issue_bundle_manifest"] = str(excluded_path)

    if not dry_run:
        if partition_output.exists():
            canonical_output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(partition_output, canonical_output)
        excluded_path.parent.mkdir(parents=True, exist_ok=True)
        with open(excluded_path, "w", encoding="utf-8") as fh:
            for item in excluded_issue_bundles:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    return summary


def run_issue_bundle_backlog_export(*, dry_run: bool = False, max_assets: int = 0) -> dict[str, Any]:
    _, excluded_issue_bundles = _iter_article_only_processed_paths(max_assets=max_assets or None)
    output_path = source_partition_records_path(SOURCE_ID, ISSUE_BUNDLE_PARTITION, "backlog.jsonl")
    summary_path = source_qa_dir(SOURCE_ID) / "issue_bundle_backlog_summary.json"

    if not dry_run:
        _write_jsonl(output_path, excluded_issue_bundles)

    summary = {
        "source_id": SOURCE_ID,
        "subset": ISSUE_BUNDLE_PARTITION,
        "backlog_assets": len(excluded_issue_bundles),
        "output": str(output_path),
    }
    if not dry_run:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_partitioned_etl(*, dry_run: bool = False, max_assets: int = 0) -> dict[str, Any]:
    article_only = run_article_only_etl(dry_run=dry_run, max_assets=max_assets)
    issue_bundle_backlog = run_issue_bundle_backlog_export(dry_run=dry_run, max_assets=max_assets)
    return {
        "source_id": SOURCE_ID,
        "article_only": article_only,
        "issue_bundle_backlog": issue_bundle_backlog,
        "canonical_records_path": str(source_records_path(SOURCE_ID)),
        "article_partition_path": str(source_partition_records_path(SOURCE_ID, ARTICLE_PARTITION)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Partitioned ETL for vmj_ojs article-safe subset and issue-bundle backlog.")
    parser.add_argument(
        "--mode",
        choices=("all", "article_only", "issue_bundle_backlog"),
        default="all",
    )
    parser.add_argument("--max-assets", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if args.mode == "article_only":
        report = run_article_only_etl(dry_run=args.dry_run, max_assets=args.max_assets)
    elif args.mode == "issue_bundle_backlog":
        report = run_issue_bundle_backlog_export(dry_run=args.dry_run, max_assets=args.max_assets)
    else:
        report = run_partitioned_etl(dry_run=args.dry_run, max_assets=args.max_assets)

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
