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
from pathlib import Path

# Add parent dirs to path
REPO_ROOT = Path(__file__).resolve().parents[3]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from . import vn_text_cleaner
from . import vn_title_extractor
from . import vn_metadata_enricher
from . import vn_sectionizer
from . import vn_quality_scorer
from services.utils.data_lineage import build_file_lineage, make_run_id
from services.utils.data_paths import preferred_processed_dir, source_records_path

logger = logging.getLogger(__name__)


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

    # 3. Extract title
    yaml_title = meta.get("title", "")
    title = vn_title_extractor.extract(
        src_id, cleaned_body, yaml_title,
        file_url=meta.get("file_url", meta.get("source_url", "")),
    )

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

    # 6. Create records for each section
    records: list[dict] = []
    for sec_idx, section in enumerate(sections):
        doc_id = _make_doc_id(src_id, filepath, sec_idx)

        # For procedure/table modes, use section_title as the record title
        # (each section is a distinct concept)
        rec_title = title
        if src_id in ("kcb_moh", "dav_gov") and section.section_title != title:
            rec_title = section.section_title

        record = {
            "doc_id": doc_id,
            "title": rec_title,
            "body": section.body,
            "source_name": enriched["source_name"],
            "section_title": section.section_title,
            "source_url": meta.get("source_url", meta.get("file_url", "")),
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
