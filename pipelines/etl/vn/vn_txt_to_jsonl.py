"""
Vietnamese TXT → DocumentRecord JSONL Converter
==================================================
Master converter that orchestrates the full pipeline:
  1. Parse YAML frontmatter
  2. Clean text (vn_text_cleaner)
  3. Extract title (vn_title_extractor)
  4. Enrich metadata (vn_metadata_enricher)
  5. Sectionize (vn_sectionizer)
  6. Score quality (vn_quality_scorer)
  7. Emit DocumentRecord JSONL

Usage:
    python -m pipelines.etl.vn.vn_txt_to_jsonl \\
        --source-id vmj_ojs \\
        [--max-files N] [--dry-run] [--verbose]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote, urlparse

# Add parent dirs to path
REPO_ROOT = Path(__file__).resolve().parents[3]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from pipelines.crawl.extract_source import _processed_asset_stem
from . import vn_text_cleaner
from . import vn_title_extractor
from . import vn_metadata_enricher
from . import vn_sectionizer
from . import vn_quality_scorer
from services.utils.crawl_manifest import read_manifest
from services.utils.data_lineage import build_file_lineage, make_run_id
from services.utils.data_paths import preferred_processed_dir, source_records_path

logger = logging.getLogger(__name__)

_ADMIN_STUB_MARKERS = (
    "ký bởi:",
    "cơ quan:",
    "ngày ký:",
    "sonht.kcb_",
    "độc lập - tự do - hạnh phúc",
)
_GENERIC_BAD_TITLES = {
    "skip to main content",
    "pdf",
    "document",
    "xem chi tiết",
    "xem chi tiet",
    "trang ch tin tc gii thiu",
}
_SLUG_TITLE_RE = re.compile(r"^[a-z0-9]+(?:[-_][a-z0-9]+){2,}$")
_FILENAME_TITLE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{8,}$")
_HEX_TITLE_RE = re.compile(r"^[0-9a-f]{24,}$", re.IGNORECASE)
_DATE_ONLY_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}:\d{2})?$")
_TOC_DOT_RE = re.compile(r"\.{6,}\s*\d+\s*$")
_TOC_NUMBERED_RE = re.compile(r"^\s*(\d+|[IVXLC]+)\s+.+\s+\d+\s*$", re.IGNORECASE)
_TAIL_HEX_RE = re.compile(r"([0-9a-f]{24,})$", re.IGNORECASE)
_VIEN_DINH_DUONG_ADMIN_URL_PARTS = (
    "/about/",
    "/gioi-thieu",
    "/don-vi-trong-vien",
    "/chuc-nang-nhiem-vu",
    "/hop-tac-quoc-te",
    "/co-cau-to-chuc",
    "/site-map",
    "/tim-kiem",
)
_VIEN_DINH_DUONG_ADMIN_TITLE_MARKERS = (
    "trang ch",
    "gioi thiu",
    "giới thiệu",
    "hợp tác quốc tế",
    "chức năng nhiệm vụ",
    "xem chi tiết",
)


def _resolve_source_dir(source_id: str | None, source_dir: str | None) -> Path:
    if source_dir:
        return Path(source_dir)
    if not source_id:
        raise ValueError("source_id is required when --source-dir is omitted")
    return preferred_processed_dir(source_id)


def _resolve_output_path(source_id: str | None, output_path: str | None) -> Path:
    if output_path:
        return Path(output_path)
    if not source_id:
        raise ValueError("source_id is required when --output is omitted")
    return source_records_path(source_id)


def _infer_source_id_from_dir(source_dir: Path) -> str:
    if source_dir.name in {"raw", "processed", "intermediate", "records", "qa"}:
        return source_dir.parent.name
    return source_dir.name


# ---------- YAML frontmatter parser ----------

_RE_FRONTMATTER = re.compile(r"^---\s*\r?\n(.*?)\r?\n---\s*\r?\n", re.DOTALL)


def _parse_frontmatter(raw_text: str) -> tuple[dict, str]:
    """Parse YAML-like frontmatter from raw text.

    Returns:
        Tuple of (frontmatter dict, body text after frontmatter).
    """
    match = _RE_FRONTMATTER.match(raw_text)
    if not match:
        return {}, raw_text

    yaml_block = match.group(1)
    body = raw_text[match.end():]

    # Simple key: value parser (avoids PyYAML dependency)
    meta: dict = {}
    for line in yaml_block.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip()
            # Remove surrounding quotes
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            elif value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            meta[key] = value

    return meta, body


def _make_doc_id(source_id: str, filepath: str, section_idx: int) -> str:
    """Generate a stable document ID."""
    basename = Path(filepath).stem
    raw = f"{source_id}:{basename}:{section_idx}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _make_article_id(source_id: str, filepath: str, title: str) -> str:
    """Generate a stable article-level ID shared by all sections/chunks."""
    basename = Path(filepath).stem
    raw = f"{source_id}:{basename}:{title}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _normalize_marker_text(text: str) -> str:
    lowered = vn_text_cleaner.clean(str(text or "")).lower()
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def _title_needs_cleanup(title: str) -> bool:
    cleaned = vn_text_cleaner.clean(str(title or ""))
    normalized = _normalize_marker_text(cleaned)
    if not cleaned or len(cleaned) < 10:
        return True
    if normalized in _GENERIC_BAD_TITLES:
        return True
    if _HEX_TITLE_RE.fullmatch(cleaned):
        return True
    if _SLUG_TITLE_RE.fullmatch(normalized):
        return True
    if _FILENAME_TITLE_RE.fullmatch(cleaned) and " " not in cleaned:
        return True
    return False


def _extract_tail_hex_token(text: str) -> str:
    match = _TAIL_HEX_RE.search(str(text or ""))
    return match.group(1).lower() if match else ""


def _title_from_url_slug(source_url: str) -> str:
    if not source_url:
        return ""
    parsed = urlparse(source_url)
    slug = unquote(parsed.path.rsplit("/", 1)[-1]).strip().lower()
    slug = re.sub(r"\.(html?|pdf|docx?|xlsx?)$", "", slug)
    slug = slug.replace("_", " ").replace("-", " ").strip()
    return " ".join(part for part in slug.split() if part)


def _clean_title_candidate(title: str) -> str:
    cleaned = vn_text_cleaner.clean(str(title or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -–—")
    return cleaned


def _best_body_heading(cleaned_body: str) -> str:
    for raw_line in cleaned_body.splitlines()[:40]:
        line = _clean_title_candidate(raw_line)
        normalized = _normalize_marker_text(line)
        if not line or len(line) < 10 or len(line) > 180:
            continue
        if normalized in _GENERIC_BAD_TITLES:
            continue
        if _DATE_ONLY_RE.fullmatch(line):
            continue
        if _TOC_DOT_RE.search(line) or _TOC_NUMBERED_RE.search(line):
            continue
        if _FILENAME_TITLE_RE.fullmatch(line) and " " not in line:
            continue
        return line
    return ""


def _row_source_url(row: dict[str, str]) -> str:
    return (
        str(row.get("item_url") or "").strip()
        or str(row.get("file_url") or "").strip()
        or str(row.get("parent_item_url") or "").strip()
    )


def _manifest_row_score(row: dict[str, str]) -> int:
    title_hint = _clean_title_candidate(row.get("title_hint", ""))
    source_url = _row_source_url(row)
    notes = str(row.get("notes") or "")
    rel_path = str(row.get("relative_path") or "")

    score = 0
    if source_url:
        score += 80
    if title_hint and not _title_needs_cleanup(title_hint):
        score += 60
    elif title_hint:
        score += 10
    if str(row.get("extract_status") or "").strip() == "done":
        score += 10
    if rel_path.startswith("sources/"):
        score += 5
    if "bootstrapped_from_existing_raw" in notes:
        score -= 25
    if "bootstrapped_from_corpus_catalog" in notes:
        score -= 15
    if _normalize_marker_text(title_hint) in _GENERIC_BAD_TITLES:
        score -= 50
    if _title_needs_cleanup(title_hint):
        score -= 10
    return score


@lru_cache(maxsize=None)
def _manifest_candidates_for_source(source_id: str) -> tuple[dict[str, list[dict[str, str]]], dict[str, list[dict[str, str]]]]:
    by_stem: dict[str, list[dict[str, str]]] = {}
    by_tail_hex: dict[str, list[dict[str, str]]] = {}

    for row in read_manifest(source_id):
        rel_path = str(row.get("relative_path") or "").strip()
        if not rel_path:
            continue
        stem = _processed_asset_stem(rel_path, row.get("content_class", ""))
        by_stem.setdefault(stem, []).append(row)

        for token in {
            _extract_tail_hex_token(stem),
            _extract_tail_hex_token(rel_path),
            _extract_tail_hex_token(str(row.get("title_hint") or "")),
            _extract_tail_hex_token(str(row.get("item_url") or "")),
        }:
            if token:
                by_tail_hex.setdefault(token, []).append(row)

    for groups in (by_stem, by_tail_hex):
        for key, rows in groups.items():
            groups[key] = sorted(rows, key=_manifest_row_score, reverse=True)
    return by_stem, by_tail_hex


def _best_manifest_rows(source_id: str, filepath: str) -> list[dict[str, str]]:
    stem = Path(filepath).stem
    by_stem, by_tail_hex = _manifest_candidates_for_source(source_id)
    candidates = list(by_stem.get(stem, []))
    tail_hex = _extract_tail_hex_token(stem)
    if tail_hex:
        for row in by_tail_hex.get(tail_hex, []):
            if row not in candidates:
                candidates.append(row)
    return sorted(candidates, key=_manifest_row_score, reverse=True)


def _resolve_title_and_source_url(
    source_id: str,
    filepath: str,
    meta: dict[str, str],
    title: str,
    cleaned_body: str,
    source_url: str,
) -> tuple[str, str]:
    manifest_rows = _best_manifest_rows(source_id, filepath)
    best_row = manifest_rows[0] if manifest_rows else {}

    resolved_source_url = source_url or _row_source_url(best_row)
    resolved_title = _clean_title_candidate(title)
    if not _title_needs_cleanup(resolved_title):
        return resolved_title, resolved_source_url

    body_heading = _best_body_heading(cleaned_body)
    if body_heading:
        resolved_title = body_heading
    else:
        manifest_title = _clean_title_candidate(best_row.get("title_hint", ""))
        if manifest_title and not _title_needs_cleanup(manifest_title):
            resolved_title = manifest_title
        else:
            slug_title = _clean_title_candidate(_title_from_url_slug(resolved_source_url))
            if slug_title:
                resolved_title = slug_title
            else:
                yaml_title = _clean_title_candidate(meta.get("title", ""))
                if yaml_title:
                    resolved_title = yaml_title

    return resolved_title, resolved_source_url


def _looks_like_download_listing(cleaned_body: str) -> bool:
    lines = [line.strip() for line in cleaned_body.splitlines()[:80] if line.strip()]
    download_hits = sum(1 for line in lines if _normalize_marker_text(line) in {"tải xuống", "tai xuong", "download"})
    date_hits = sum(1 for line in lines if _DATE_ONLY_RE.fullmatch(line))
    toc_hits = sum(1 for line in lines if _TOC_DOT_RE.search(line) or _TOC_NUMBERED_RE.search(line))
    return download_hits >= 3 and date_hits >= 3 and toc_hits == 0


def _should_quarantine_file(source_id: str, title: str, source_url: str, cleaned_body: str) -> bool:
    normalized_title = _normalize_marker_text(title)
    normalized_body = _normalize_marker_text("\n".join(cleaned_body.splitlines()[:30]))
    normalized_url = source_url.lower()

    if normalized_title in {"skip to main content", "xem chi tiết", "xem chi tiet"}:
        return True

    if source_id == "who_vietnam":
        if normalized_title == "skip to main content":
            return True
        if not source_url:
            return True
        return False

    if source_id == "vien_dinh_duong":
        if any(part in normalized_url for part in _VIEN_DINH_DUONG_ADMIN_URL_PARTS):
            return True
        if _looks_like_download_listing(cleaned_body):
            return True
        if not source_url:
            return True
        return False

    if source_id == "vmj_ojs" and not source_url:
        return True

    return False


def _looks_like_admin_stub(source_id: str, cleaned_body: str) -> bool:
    if source_id not in {"dav_gov", "kcb_moh"}:
        return False
    if len(cleaned_body) > 450:
        return False
    lowered = cleaned_body.lower()
    marker_hits = sum(1 for marker in _ADMIN_STUB_MARKERS if marker in lowered)
    line_count = sum(1 for line in cleaned_body.splitlines() if line.strip())
    return marker_hits >= 2 and line_count <= 18


# ---------- Main Processing ----------

def process_file(filepath: str, source_id: str | None = None, etl_run_id: str = "") -> list[dict]:
    """Process a single TXT file into DocumentRecord dicts.

    v3: Source-aware sectionization replaces old KCB procedure splitter.
    Flow: parse → clean → extract title → enrich → sectionize(source) → score

    Args:
        filepath: Path to the .txt file.
        source_id: Override source_id (otherwise from frontmatter).

    Returns:
        List of DocumentRecord-compatible dicts.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        raw_text = f.read()

    # 1. Parse frontmatter
    meta, body = _parse_frontmatter(raw_text)
    src_id = source_id or meta.get("source_id", "unknown")
    lineage = build_file_lineage(
        filepath,
        source_id=src_id,
        etl_run_id=etl_run_id,
        parent_file=meta.get("parent_file", meta.get("issue_file", "")),
    )

    # 2. Clean text
    cleaned_body = vn_text_cleaner.clean(body)

    # Validation gate: body too short
    if len(cleaned_body) < 200:
        logger.warning(f"SKIP {filepath}: body too short ({len(cleaned_body)} chars)")
        return []

    if _looks_like_admin_stub(src_id, cleaned_body):
        logger.warning(f"SKIP {filepath}: admin/signature stub")
        return []

    # 3. Extract title
    yaml_title = meta.get("title", "")
    title = vn_title_extractor.extract(
        src_id,
        cleaned_body,
        yaml_title,
        file_url=(meta.get("file_url") or meta.get("source_url") or meta.get("item_url") or ""),
    )

    source_url = (meta.get("source_url") or meta.get("file_url") or meta.get("item_url") or "")
    title, source_url = _resolve_title_and_source_url(src_id, filepath, meta, title, cleaned_body, source_url)

    if _should_quarantine_file(src_id, title, source_url, cleaned_body):
        logger.warning(f"SKIP {filepath}: wrapper/listing page")
        return []

    # Validation gate: title extraction failed
    if not title or len(title) < 10 or title.strip().lower() in {"pdf", "document"}:
        logger.warning(f"SKIP {filepath}: title extraction failed ({title!r})")
        return []

    # 4. Enrich metadata
    enriched = vn_metadata_enricher.enrich(
        source_id=src_id,
        title=title,
        body=cleaned_body,
        institution=meta.get("institution", ""),
    )

    # 5. Sectionize (source-aware v3)
    sections = vn_sectionizer.sectionize(title, cleaned_body, source_id=src_id)
    article_id = _make_article_id(src_id, filepath, title)
    institution = meta.get("institution", "")

    # 6. Create records for each section
    records: list[dict] = []
    for sec_idx, section in enumerate(sections):
        section_body = section.body.strip()
        if len(section_body) < 50:
            continue

        doc_id = _make_doc_id(src_id, filepath, sec_idx)

        # For procedure/table modes, use section_title as the record title
        # (each section is a distinct concept)
        rec_title = title
        if src_id in ("kcb_moh", "dav_gov") and section.section_title != title:
            rec_title = section.section_title

        record = {
            "doc_id": doc_id,
            "article_id": article_id,
            "title": rec_title,
            "body": section_body,
            "source_name": enriched["source_name"],
            "institution": institution,
            "section_title": section.section_title,
            "source_url": source_url,
            "source_id": src_id,
            **lineage,
            "doc_type": enriched["doc_type"],
            "specialty": enriched["specialty"],
            "audience": enriched["audience"],
            "language": enriched["language"],
            "trust_tier": enriched["trust_tier"],
            "published_at": "",
            "updated_at": "",
            "tags": [],
            "heading_path": section.heading_path,
            # Extra quality fields (consumed by scorer, removed before output)
            "_section_count": len(sections),
            "_section_bodies": [s.body for s in sections],
            "language_confidence": enriched["language_confidence"],
            "is_mixed_language": enriched["is_mixed_language"],
        }

        # 7. Score quality
        quality = vn_quality_scorer.score(record)
        if src_id == "vmj_ojs" and quality["quality_status"] == "hold":
            continue
        record["quality_score"] = quality["quality_score"]
        record["quality_status"] = quality["quality_status"]
        record["quality_flags"] = quality["quality_flags"]

        # Remove internal fields before output
        record.pop("_section_count", None)
        record.pop("_section_bodies", None)

        records.append(record)

    return records


def process_directory(
    source_dir: str,
    output_path: str,
    source_id: str | None = None,
    max_files: int | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Process all .txt files in a directory.

    Args:
        source_dir: Path to directory containing .txt files.
        output_path: Path to output JSONL file.
        source_id: Override source_id (otherwise from directory name).
        max_files: Maximum number of files to process (for pilot mode).
        dry_run: If True, don't write output.
        verbose: If True, print detailed progress.

    Returns:
        Summary dict with stats.
    """
    source_dir = Path(source_dir)
    if not source_dir.is_dir():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    txt_files = sorted(source_dir.glob("*.txt"))
    if max_files:
        txt_files = txt_files[:max_files]

    # Infer source_id from directory name if not provided
    if not source_id:
        source_id = source_dir.name
    etl_run_id = os.getenv("ETL_RUN_ID") or make_run_id("vn_txt_to_jsonl", source_id)

    total_files = len(txt_files)
    total_records = 0
    total_skipped = 0
    score_sum = 0
    status_counts = {"go": 0, "review": 0, "hold": 0}

    all_records: list[dict] = []

    for i, fpath in enumerate(txt_files):
        if verbose:
            print(f"  [{i+1}/{total_files}] {fpath.name}")

        try:
            records = process_file(str(fpath), source_id=source_id, etl_run_id=etl_run_id)
        except Exception as e:
            logger.error(f"ERROR processing {fpath}: {e}")
            total_skipped += 1
            continue

        if not records:
            total_skipped += 1
            continue

        for rec in records:
            all_records.append(rec)
            total_records += 1
            score_sum += rec.get("quality_score", 0)
            status = rec.get("quality_status", "hold")
            status_counts[status] = status_counts.get(status, 0) + 1

    # Write output
    if not dry_run and all_records:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, "w", encoding="utf-8") as f:
            for rec in all_records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    avg_score = score_sum / max(1, total_records)

    summary = {
        "source_id": source_id,
        "total_files": total_files,
        "total_records": total_records,
        "skipped_files": total_skipped,
        "avg_quality_score": round(avg_score, 1),
        "status_counts": status_counts,
        "output": str(output_path) if not dry_run else "(dry-run)",
        "etl_run_id": etl_run_id,
    }

    return summary


def main():
    ap = argparse.ArgumentParser(
        description="Convert Vietnamese medical TXT files to DocumentRecord JSONL"
    )
    ap.add_argument("--source-dir", required=False, help="Directory with .txt files")
    ap.add_argument("--output", required=False, help="Output JSONL file path")
    ap.add_argument("--source-id", required=False, help="Override source ID (default: infer from dir name)")
    ap.add_argument("--max-files", type=int, help="Max files to process (pilot mode)")
    ap.add_argument("--dry-run", action="store_true", help="Don't write output")
    ap.add_argument("--verbose", action="store_true", help="Print progress")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    resolved_source_dir = _resolve_source_dir(args.source_id, args.source_dir)
    inferred_source_id = args.source_id or _infer_source_id_from_dir(resolved_source_dir)
    resolved_output = _resolve_output_path(inferred_source_id, args.output)

    print(f"\n{'='*60}")
    print(f"  VN TXT -> JSONL Converter")
    print(f"  Source: {resolved_source_dir}")
    print(f"  Output: {resolved_output}")
    if args.max_files:
        print(f"  Pilot mode: max {args.max_files} files")
    print(f"{'='*60}\n")

    summary = process_directory(
        source_dir=str(resolved_source_dir),
        output_path=str(resolved_output),
        source_id=inferred_source_id,
        max_files=args.max_files,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    # Print summary
    print(f"\n{'='*60}")
    print(f"  Conversion Summary: {summary['source_id']}")
    print(f"{'='*60}")
    print(f"  Files processed: {summary['total_files']}")
    print(f"  Records created: {summary['total_records']}")
    print(f"  Files skipped:   {summary['skipped_files']}")
    print(f"  Avg quality:     {summary['avg_quality_score']} / 100")
    print(f"\n  Quality Distribution:")
    for status, count in summary["status_counts"].items():
        pct = count / max(1, summary["total_records"]) * 100
        print(f"    {status:8s}: {count:4d} ({pct:.1f}%)")
    print(f"\n  Output: {summary['output']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
