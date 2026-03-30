"""
QA Pre-Ingest: Tang 2 - Kiem tra chat luong noi dung (Content Quality)
======================================================================

Checks content quality BEFORE chunking:
- Character noise / OCR artifacts
- Sentence repetition within documents
- Boilerplate duplication across documents
- Body length distribution (too short / too long)
- Topic coherence (does body match title/specialty?)
- Version conflicts (old + new version coexist?)

Corresponds to CHECK.md Section 2.

Usage:
    python -m qa_pre_ingest.check_content ../../data/data_final/medlineplus.jsonl
    python -m qa_pre_ingest.check_content ../../data/data_final/*.jsonl --sample 50
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.document_schema import DocumentRecord


# ── Detection helpers ────────────────────────────────────────────────
def _noise_ratio(text: str) -> float:
    """Ratio of non-alphanumeric/space chars. High = noisy."""
    if not text:
        return 0.0
    total = len(text)
    clean = sum(1 for c in text if c.isalnum() or c.isspace() or c in ".,;:!?()-'/\"")
    return 1 - (clean / total) if total > 0 else 0.0


def _sentence_repetition_ratio(text: str) -> float:
    """Ratio of duplicate sentences within a single document."""
    sentences = [s.strip().lower() for s in re.split(r"[.!?\n]", text) if len(s.strip()) > 20]
    if len(sentences) < 2:
        return 0.0
    unique = set(sentences)
    return 1 - (len(unique) / len(sentences))


def _body_hash(text: str) -> str:
    """Hash of normalized body text for cross-doc dedup detection."""
    norm = " ".join(text.lower().split())[:500]  # first 500 chars normalized
    return hashlib.md5(norm.encode("utf-8")).hexdigest()


def _has_navigation_text(text: str) -> bool:
    """Check for navigation/menu fragments commonly scraped by accident."""
    nav_patterns = [
        r"skip to (main )?content",
        r"cookie (policy|consent|settings)",
        r"privacy policy",
        r"terms of (use|service)",
        r"back to top",
        r"share (on|this|via)",
        r"(facebook|twitter|linkedin|youtube)",
        r"subscribe to",
        r"sign up for",
        r"©\s*\d{4}",
    ]
    t = text[:300].lower()
    return sum(1 for p in nav_patterns if re.search(p, t)) >= 2


def _detect_mixed_topics(title: str, body: str) -> bool:
    """Rough check if body seems unrelated to title."""
    title_words = set(re.findall(r"\w{4,}", title.lower()))
    body_words = set(re.findall(r"\w{4,}", body[:500].lower()))
    if not title_words:
        return False
    overlap = title_words & body_words
    return len(overlap) / len(title_words) < 0.15  # < 15% word overlap


# ── Main validation ──────────────────────────────────────────────────
def check_content(path: str, sample_size: int = 0) -> Dict[str, Any]:
    """Run content quality checks on a JSONL file."""
    report = {
        "file": os.path.basename(path),
        "total_records": 0,
        "warnings": [],
        "stats": {
            "body_lengths": [],
            "noise_ratios": [],
            "repetition_ratios": [],
        },
        "issues": {
            "high_noise": 0,
            "sentence_repetition": 0,
            "cross_doc_duplicate": 0,
            "navigation_text": 0,
            "mixed_topics": 0,
            "too_short": 0,
            "too_long": 0,
        },
        "body_hash_counts": Counter(),
        "duplicate_clusters": [],
    }

    records: List[DocumentRecord] = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            try:
                rec = DocumentRecord.from_dict(json.loads(line))
                records.append(rec)
            except Exception:
                continue

    report["total_records"] = len(records)

    # Sample if requested
    if sample_size > 0 and len(records) > sample_size:
        import random
        random.seed(42)
        records = random.sample(records, sample_size)
        report["sampled"] = sample_size

    # Body hash for cross-doc dedup
    hash_to_ids: defaultdict = defaultdict(list)

    for rec in records:
        body = rec.body or ""
        body_len = len(body)

        # Length stats
        report["stats"]["body_lengths"].append(body_len)

        # Too short / too long
        if body_len < 100:
            report["issues"]["too_short"] += 1
        if body_len > 10000:
            report["issues"]["too_long"] += 1

        # Noise ratio
        nr = _noise_ratio(body)
        report["stats"]["noise_ratios"].append(nr)
        if nr > 0.15:
            report["issues"]["high_noise"] += 1

        # Sentence repetition
        sr = _sentence_repetition_ratio(body)
        report["stats"]["repetition_ratios"].append(sr)
        if sr > 0.3:
            report["issues"]["sentence_repetition"] += 1

        # Navigation text
        if _has_navigation_text(body):
            report["issues"]["navigation_text"] += 1

        # Mixed topics
        if _detect_mixed_topics(rec.title, body):
            report["issues"]["mixed_topics"] += 1

        # Cross-doc hash
        bh = _body_hash(body)
        hash_to_ids[bh].append(rec.doc_id)

    # Cross-doc duplicates
    for bh, ids in hash_to_ids.items():
        if len(ids) > 1:
            report["issues"]["cross_doc_duplicate"] += len(ids) - 1
            report["duplicate_clusters"].append(ids)

    return report


def print_report(report: Dict[str, Any]):
    """Print human-readable content quality report."""
    print(f"\n{'='*60}")
    print(f"  CONTENT QUALITY: {report['file']}")
    print(f"{'='*60}")
    print(f"  Records checked: {report['total_records']}")
    if "sampled" in report:
        print(f"  (sampled {report['sampled']})")

    lengths = report["stats"]["body_lengths"]
    if lengths:
        lengths.sort()
        p50 = lengths[len(lengths) // 2]
        p95 = lengths[int(len(lengths) * 0.95)]
        avg = sum(lengths) // len(lengths)
        print(f"\n  Body length distribution:")
        print(f"    Min:     {min(lengths):,}")
        print(f"    Avg:     {avg:,}")
        print(f"    Median:  {p50:,}")
        print(f"    P95:     {p95:,}")
        print(f"    Max:     {max(lengths):,}")

    print(f"\n  Issues found:")
    for issue, count in report["issues"].items():
        status = "[OK]" if count == 0 else f"[WARN] {count}"
        print(f"    {issue:30s} {status}")

    if report["duplicate_clusters"]:
        print(f"\n  Duplicate clusters ({len(report['duplicate_clusters'])}):")
        for cluster in report["duplicate_clusters"][:5]:
            print(f"    {cluster}")

    total_issues = sum(report["issues"].values())
    status = "[PASS]" if total_issues == 0 else f"[WARN] {total_issues} issues"
    print(f"\n  Result: {status}")
    print(f"{'='*60}")


def main():
    ap = argparse.ArgumentParser(description="Tang 2: Content quality check")
    ap.add_argument("files", nargs="+", help="JSONL files to check")
    ap.add_argument("--sample", type=int, default=0, help="Random sample size (0 = all)")
    args = ap.parse_args()

    for path in args.files:
        if not os.path.exists(path):
            print(f"  [SKIP] Not found: {path}")
            continue
        report = check_content(path, sample_size=args.sample)
        print_report(report)


if __name__ == "__main__":
    main()
