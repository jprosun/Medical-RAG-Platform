"""
ETL: Master Normalize Script
==============================

Orchestrates the full ETL pipeline:
1. Runs all scrapers (or skips if data_raw already populated)
2. Validates all enriched JSONL outputs
3. Merges into a canonical dataset release
4. Prints statistics

Usage:
    python -m pipelines.etl.normalize_all \\
        --dataset-id en_core_v1 \\
        --max-medlineplus 200 \\
        --max-who 50 \\
        --max-ncbi 100

    # Skip scraping (just validate + merge existing data):
    python -m pipelines.etl.normalize_all --dataset-id en_core_v1 --skip-scrape
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parents[2]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))
from app.document_schema import DocumentRecord, iter_jsonl
from services.utils.data_lineage import make_run_id
from services.utils.data_paths import (
    dataset_manifest_path,
    dataset_qa_dir,
    dataset_records_path,
    ensure_rag_data_layout,
    source_raw_dir,
    source_records_path,
)


EN_SOURCE_IDS = ("medlineplus", "who", "ncbi_bookshelf")


def run_scraper(cmd: List[str], name: str) -> bool:
    """Run a scraper subprocess. Returns True on success."""
    print(f"\n{'='*60}")
    print(f"  Running: {name}")
    print(f"  Command: {' '.join(cmd)}")
    print(f"{'='*60}\n")

    try:
        result = subprocess.run(
            cmd,
            capture_output=False,
            text=True,
            cwd=str(REPO_ROOT),
        )
        if result.returncode != 0:
            print(f"\n[WARN] {name} exited with code {result.returncode}")
            return False
        return True
    except Exception as exc:
        print(f"\n[ERROR] {name} failed: {exc}")
        return False


def validate_jsonl(path: str) -> Dict:
    """Validate a JSONL file, return stats."""
    stats = {"path": path, "total": 0, "valid": 0, "errors": 0, "specialties": {}, "trust_tiers": {}}

    if not os.path.exists(path):
        stats["errors"] = -1  # file missing
        return stats

    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line:
                continue
            stats["total"] += 1
            try:
                obj = json.loads(line)
                rec = DocumentRecord.from_dict(obj)
                errors = rec.validate()
                if errors:
                    stats["errors"] += 1
                else:
                    stats["valid"] += 1
                    # Track specialty distribution
                    spec = rec.specialty
                    stats["specialties"][spec] = stats["specialties"].get(spec, 0) + 1
                    tier = rec.trust_tier
                    stats["trust_tiers"][tier] = stats["trust_tiers"].get(tier, 0) + 1
            except Exception:
                stats["errors"] += 1

    return stats


def _body_hash(body: str) -> str:
    """Normalized body hash for near-duplicate detection."""
    import hashlib
    norm = " ".join(body.lower().split())[:500]
    return hashlib.md5(norm.encode("utf-8")).hexdigest()


def _has_better_metadata(new_rec, old_rec) -> bool:
    """Return True if new_rec has richer metadata than old_rec."""
    score_new = 0
    score_old = 0
    for field in ["title", "source_url", "section_title", "specialty"]:
        new_val = getattr(new_rec, field, "") or ""
        old_val = getattr(old_rec, field, "") or ""
        # Penalize generic/default values
        if new_val and new_val.lower() not in ("bookshelf", "general", "reference", ""):
            score_new += 1
        if old_val and old_val.lower() not in ("bookshelf", "general", "reference", ""):
            score_old += 1
    return score_new > score_old


def dedup_within_source(records: List) -> List:
    """
    Dedup records within a single source by body hash.
    If two records have near-identical body text, keep the one with
    richer metadata (better title, source_url, etc.).
    """
    by_hash: Dict[str, list] = {}
    for rec in records:
        h = _body_hash(rec.body)
        if h not in by_hash:
            by_hash[h] = rec
        elif _has_better_metadata(rec, by_hash[h]):
            by_hash[h] = rec
    return list(by_hash.values())


def _source_label_for_path(path: str) -> str:
    fp = Path(path)
    if fp.name == "document_records.jsonl" and fp.parent.name == "records":
        return fp.parent.parent.name
    return fp.stem


def merge_jsonl(input_files: List[str], output_path: str) -> int:
    """
    Merge multiple JSONL files with smart dedup strategy:
    
    1. Within each source file: dedup by body hash, keep best metadata
    2. Cross-source: keep ALL records (different sources for same topic = valuable)
    3. Only skip exact same doc_id + same source_name duplicates
    
    Returns total record count written.
    """
    # Phase 1: Load and dedup within each source
    all_records: List = []
    per_source_stats: Dict[str, Dict] = {}

    for fp in input_files:
        if not os.path.exists(fp):
            continue
        
        source_records = list(iter_jsonl(fp))
        original_count = len(source_records)
        
        deduped = dedup_within_source(source_records)
        source_name = deduped[0].source_name if deduped else _source_label_for_path(fp)
        
        per_source_stats[source_name] = {
            "original": original_count,
            "after_dedup": len(deduped),
            "dropped": original_count - len(deduped),
        }
        all_records.extend(deduped)

    # Phase 2: Cross-source dedup — only exact (doc_id + source_name) pairs
    seen_keys: set = set()
    final_records: List = []
    cross_dupes = 0
    
    for rec in all_records:
        key = (rec.doc_id, rec.source_name)
        if key in seen_keys:
            cross_dupes += 1
            continue
        seen_keys.add(key)
        final_records.append(rec)

    # Phase 3: Metadata preservation test
    print(f"\n  {'─' * 50}")
    print(f"  METADATA PRESERVATION TEST")
    print(f"  {'─' * 50}")
    
    source_counts: Dict[str, int] = {}
    title_issues = 0
    missing_url = 0
    missing_section = 0
    missing_title = 0
    
    for rec in final_records:
        src = rec.source_name
        source_counts[src] = source_counts.get(src, 0) + 1
        
        if not rec.title or rec.title.lower() in ("bookshelf", ""):
            title_issues += 1
        if not rec.source_url:
            missing_url += 1
        if not rec.section_title:
            missing_section += 1
        if not rec.title:
            missing_title += 1

    total = len(final_records)
    print(f"  Records per source:")
    for src, cnt in sorted(source_counts.items()):
        orig_cnt = per_source_stats.get(src, {}).get("original", "?")
        print(f"    {src:25s} {cnt:5d} records (from {orig_cnt})")
    
    print(f"\n  Within-source dedup:")
    for fname, stats in per_source_stats.items():
        print(f"    {fname:30s} {stats['original']:5d} → {stats['after_dedup']:5d} (dropped {stats['dropped']})")
    
    print(f"  Cross-source dupes dropped: {cross_dupes}")
    print(f"\n  Metadata quality:")
    print(f"    Title = generic/empty:  {title_issues}/{total} ({title_issues/max(1,total)*100:.1f}%)")
    print(f"    Missing source_url:     {missing_url}/{total} ({missing_url/max(1,total)*100:.1f}%)")
    print(f"    Missing section_title:  {missing_section}/{total} ({missing_section/max(1,total)*100:.1f}%)")
    
    if title_issues > 0:
        print(f"\n  ⚠️  WARNING: {title_issues} records have generic/empty titles!")
        # Show samples
        bad = [r for r in final_records if not r.title or r.title.lower() in ("bookshelf", "")][:5]
        for r in bad:
            print(f"    doc_id={r.doc_id[:40]} title='{r.title}' heading='{r.heading_path[:50]}'")
    
    if title_issues == 0 and missing_url == 0:
        print(f"\n  ✅ Metadata preservation: PASS")
    
    print(f"  {'─' * 50}")

    # Phase 4: Write output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as out:
        for rec in final_records:
            out.write(rec.to_jsonl_line() + "\n")

    return total


def _legacy_dataset_output_path(dataset_id: str, legacy_data_dir: Path) -> Path:
    filename = "combined.jsonl" if dataset_id in {"combined", "en_core", "en_core_v1"} else f"{dataset_id}.jsonl"
    return legacy_data_dir / "data_final" / filename


def _resolve_source_paths(source_id: str, legacy_data_dir: Path | None) -> tuple[Path, Path]:
    if legacy_data_dir:
        raw_dir = legacy_data_dir / "data_raw" / source_id
        output_path = legacy_data_dir / "data_final" / f"{source_id}.jsonl"
        raw_dir.mkdir(parents=True, exist_ok=True)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return raw_dir, output_path
    return source_raw_dir(source_id), source_records_path(source_id)


def _write_dataset_metadata(
    dataset_id: str,
    output_path: Path,
    source_outputs: dict[str, Path],
    validation_stats: list[Dict],
    total: int,
    etl_run_id: str,
) -> None:
    qa_dir = dataset_qa_dir(dataset_id)
    qa_dir.mkdir(parents=True, exist_ok=True)

    validation_summary = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset_id": dataset_id,
        "etl_run_id": etl_run_id,
        "output_path": str(output_path),
        "sources": {source_id: str(path) for source_id, path in source_outputs.items()},
        "record_count": total,
        "validation": validation_stats,
    }
    validation_path = qa_dir / "validation_summary.json"
    with open(validation_path, "w", encoding="utf-8") as fh:
        json.dump(validation_summary, fh, ensure_ascii=False, indent=2)

    manifest = {
        "kind": "dataset_release",
        "dataset_id": dataset_id,
        "etl_run_id": etl_run_id,
        "generated_at_utc": validation_summary["generated_at_utc"],
        "records_path": str(output_path),
        "qa_summary_path": str(validation_path),
        "source_ids": list(source_outputs.keys()),
        "record_count": total,
    }
    manifest_path = dataset_manifest_path(dataset_id)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser(description="Master ETL: scrape + validate + merge")
    ap.add_argument(
        "--dataset-id",
        default="en_core_v1",
        help="Canonical dataset ID for the merged release under rag-data/datasets/",
    )
    ap.add_argument(
        "--data-dir",
        default="",
        help="Deprecated legacy root (e.g., ../../data). New runs should omit this and use rag-data/.",
    )
    ap.add_argument("--max-medlineplus", type=int, default=200)
    ap.add_argument("--max-who", type=int, default=50)
    ap.add_argument("--max-ncbi", type=int, default=100)
    ap.add_argument("--skip-scrape", action="store_true", help="Skip scraping, just validate + merge")
    args = ap.parse_args()

    legacy_data_dir = Path(args.data_dir).resolve() if args.data_dir else None
    etl_run_id = os.getenv("ETL_RUN_ID") or make_run_id("normalize_all", args.dataset_id)
    os.environ["ETL_RUN_ID"] = etl_run_id
    if legacy_data_dir:
        print("[WARN] --data-dir uses the legacy layout. Canonical output is rag-data/.")
    else:
        ensure_rag_data_layout(source_ids=EN_SOURCE_IDS, dataset_ids=[args.dataset_id])

    source_paths = {
        source_id: _resolve_source_paths(source_id, legacy_data_dir)
        for source_id in EN_SOURCE_IDS
    }
    source_outputs = {source_id: output_path for source_id, (_, output_path) in source_paths.items()}
    dataset_output = (
        _legacy_dataset_output_path(args.dataset_id, legacy_data_dir)
        if legacy_data_dir
        else dataset_records_path(args.dataset_id)
    )

    python = sys.executable

    # ── Step 1: Run scrapers ──────────────────────────────────────────
    if not args.skip_scrape:
        scrapers = [
            {
                "name": "MedlinePlus",
                "cmd": [
                    python, "-m", "pipelines.etl.medlineplus_scraper",
                    "--raw-dir", str(source_paths["medlineplus"][0]),
                    "--output", str(source_outputs["medlineplus"]),
                    "--max-topics", str(args.max_medlineplus),
                ],
            },
            {
                "name": "WHO",
                "cmd": [
                    python, "-m", "pipelines.etl.who_scraper",
                    "--raw-dir", str(source_paths["who"][0]),
                    "--output", str(source_outputs["who"]),
                    "--max-topics", str(args.max_who),
                ],
            },
            {
                "name": "NCBI Bookshelf",
                "cmd": [
                    python, "-m", "pipelines.etl.ncbi_bookshelf_scraper",
                    "--raw-dir", str(source_paths["ncbi_bookshelf"][0]),
                    "--output", str(source_outputs["ncbi_bookshelf"]),
                    "--max-chapters", str(args.max_ncbi),
                ],
            },
        ]

        for scraper in scrapers:
            run_scraper(scraper["cmd"], scraper["name"])

    # ── Step 2: Validate all JSONL ────────────────────────────────────
    print(f"\n{'='*60}")
    print("  VALIDATION RESULTS")
    print(f"{'='*60}\n")

    jsonl_files = [str(path) for path in source_outputs.values()]

    all_stats = []
    for fp in jsonl_files:
        stats = validate_jsonl(fp)
        all_stats.append(stats)
        name = os.path.basename(fp)
        if stats["errors"] == -1:
            print(f"  {name}: [SKIP] file not found")
        elif stats["errors"] == 0:
            print(f"  {name}: [OK] {stats['valid']} records valid")
        else:
            print(f"  {name}: [WARN] {stats['valid']} valid, {stats['errors']} errors")

    # ── Step 3: Merge into dataset release ────────────────────────────
    existing_files = [fp for fp in jsonl_files if os.path.exists(fp)]
    total = merge_jsonl(existing_files, str(dataset_output))

    print(f"\n{'='*60}")
    print("  MERGE RESULTS")
    print(f"{'='*60}")
    print(f"  Records:   {total} unique records")
    print(f"  Output:    {dataset_output}")

    if not legacy_data_dir:
        _write_dataset_metadata(
            dataset_id=args.dataset_id,
            output_path=dataset_output,
            source_outputs=source_outputs,
            validation_stats=all_stats,
            total=total,
            etl_run_id=etl_run_id,
        )

    # ── Step 4: Statistics ────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  STATISTICS")
    print(f"{'='*60}")

    # Aggregate specialties and tiers across all sources
    total_specialties: Dict[str, int] = {}
    total_tiers: Dict[int, int] = {}
    for s in all_stats:
        for spec, count in s.get("specialties", {}).items():
            total_specialties[spec] = total_specialties.get(spec, 0) + count
        for tier, count in s.get("trust_tiers", {}).items():
            total_tiers[tier] = total_tiers.get(tier, 0) + count

    if total_specialties:
        print("\n  Specialties:")
        for spec in sorted(total_specialties, key=total_specialties.get, reverse=True):
            print(f"    {spec:30s} {total_specialties[spec]:5d}")

    if total_tiers:
        print("\n  Trust Tiers:")
        tier_names = {1: "Canonical (WHO/CDC)", 2: "Reference (NCBI)", 3: "Patient (MedlinePlus)"}
        for tier in sorted(total_tiers):
            name = tier_names.get(tier, f"Tier {tier}")
            print(f"    Tier {tier} - {name:30s} {total_tiers[tier]:5d}")

    if legacy_data_dir:
        print(f"\n[DONE] Pipeline complete. Output in legacy dir: {legacy_data_dir / 'data_final'}")
    else:
        print(f"\n[DONE] Pipeline complete. Dataset release: {dataset_output}")


if __name__ == "__main__":
    main()
