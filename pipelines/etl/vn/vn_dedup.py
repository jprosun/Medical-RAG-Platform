"""
Vietnamese Document Deduplication
===================================
Detects duplicate documents within a JSONL file using:
  1. SHA256 body hash (exact duplicates)
  2. Text fingerprint (near-duplicates via 3-gram hash of first 500 chars)
  3. Title similarity (Levenshtein ratio > 0.85)

Usage:
    python -m pipelines.etl.vn.vn_dedup --input data.jsonl --report
    python -m pipelines.etl.vn.vn_dedup --input data.jsonl --output deduped.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


def _hash_body(body: str) -> str:
    """SHA256 hash of full body text."""
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _fingerprint(body: str, n: int = 3) -> str:
    """Hash of character 3-grams from first 500 chars of body."""
    preview = body[:500].lower().replace(" ", "").replace("\n", "")
    if len(preview) < n:
        return _hash_body(body)

    grams = set()
    for i in range(len(preview) - n + 1):
        grams.add(preview[i:i + n])

    gram_str = "".join(sorted(grams))
    return hashlib.md5(gram_str.encode("utf-8")).hexdigest()


def _levenshtein_ratio(s1: str, s2: str) -> float:
    """Compute Levenshtein similarity ratio between two strings."""
    if not s1 or not s2:
        return 0.0

    s1 = s1.lower().strip()
    s2 = s2.lower().strip()

    if s1 == s2:
        return 1.0

    len1, len2 = len(s1), len(s2)
    max_len = max(len1, len2)

    if max_len == 0:
        return 1.0

    # Quick reject: if length difference is > 50%, similarity must be low
    if abs(len1 - len2) / max_len > 0.5:
        return 0.0

    # Simple Levenshtein distance
    if len1 > len2:
        s1, s2 = s2, s1
        len1, len2 = len2, len1

    prev_row = list(range(len2 + 1))
    for i in range(1, len1 + 1):
        curr_row = [i]
        for j in range(1, len2 + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            curr_row.append(min(
                prev_row[j] + 1,
                curr_row[j - 1] + 1,
                prev_row[j - 1] + cost,
            ))
        prev_row = curr_row

    distance = prev_row[len2]
    return 1.0 - distance / max_len


def find_duplicates(records: list[dict]) -> list[dict]:
    """Find and tag duplicate records.

    Args:
        records: List of record dicts with 'title' and 'body' keys.

    Returns:
        Same records list with added fields:
        - is_duplicate_suspect (bool)
        - duplicate_group_id (str or None)
        - duplicate_reason (str or None)
    """
    # Build hashes
    body_hashes: dict[str, list[int]] = {}
    fingerprints: dict[str, list[int]] = {}

    for i, rec in enumerate(records):
        body = rec.get("body", "")

        # Exact body hash
        bh = _hash_body(body)
        body_hashes.setdefault(bh, []).append(i)

        # Fingerprint
        fp = _fingerprint(body)
        fingerprints.setdefault(fp, []).append(i)

    # Mark duplicates
    dup_groups: dict[int, str] = {}
    dup_reasons: dict[int, str] = {}
    group_counter = 0

    # 1. Exact body duplicates
    for bh, indices in body_hashes.items():
        if len(indices) > 1:
            group_id = f"exact_{group_counter}"
            group_counter += 1
            for idx in indices:
                dup_groups[idx] = group_id
                dup_reasons[idx] = "exact_body_match"

    # 2. Fingerprint near-duplicates
    for fp, indices in fingerprints.items():
        if len(indices) > 1:
            for idx in indices:
                if idx not in dup_groups:
                    group_id = f"near_{group_counter}"
                    group_counter += 1
                    for ii in indices:
                        if ii not in dup_groups:
                            dup_groups[ii] = group_id
                            dup_reasons[ii] = "fingerprint_match"
                    break

    # 3. Title similarity (only check non-duplicate records, O(n²) but limited)
    titles = [(i, rec.get("title", "")) for i, rec in enumerate(records)]
    for i in range(len(titles)):
        if titles[i][0] in dup_groups:
            continue
        for j in range(i + 1, len(titles)):
            if titles[j][0] in dup_groups:
                continue
            ratio = _levenshtein_ratio(titles[i][1], titles[j][1])
            if ratio > 0.85:
                group_id = f"title_{group_counter}"
                group_counter += 1
                dup_groups[titles[i][0]] = group_id
                dup_groups[titles[j][0]] = group_id
                dup_reasons[titles[i][0]] = f"title_similar({ratio:.2f})"
                dup_reasons[titles[j][0]] = f"title_similar({ratio:.2f})"

    # Apply tags
    for i, rec in enumerate(records):
        rec["is_duplicate_suspect"] = i in dup_groups
        rec["duplicate_group_id"] = dup_groups.get(i)
        rec["duplicate_reason"] = dup_reasons.get(i)

    return records


def report(records: list[dict]) -> dict:
    """Generate deduplication report."""
    total = len(records)
    suspects = sum(1 for r in records if r.get("is_duplicate_suspect"))
    groups = set(r.get("duplicate_group_id") for r in records if r.get("duplicate_group_id"))

    return {
        "total_records": total,
        "duplicate_suspects": suspects,
        "duplicate_rate": round(suspects / max(1, total) * 100, 1),
        "duplicate_groups": len(groups),
    }


def main():
    ap = argparse.ArgumentParser(description="Vietnamese document deduplication")
    ap.add_argument("--input", required=True, help="Input JSONL file")
    ap.add_argument("--output", help="Output deduplicated JSONL (optional)")
    ap.add_argument("--report", action="store_true", help="Print dedup report")
    args = ap.parse_args()

    # Load records
    records = []
    with open(args.input, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    # Find duplicates
    records = find_duplicates(records)

    if args.report:
        rep = report(records)
        print(f"\n{'='*50}")
        print(f"  Deduplication Report: {Path(args.input).name}")
        print(f"{'='*50}")
        print(f"  Total records:      {rep['total_records']}")
        print(f"  Duplicate suspects: {rep['duplicate_suspects']}")
        print(f"  Duplicate rate:     {rep['duplicate_rate']}%")
        print(f"  Duplicate groups:   {rep['duplicate_groups']}")
        print(f"{'='*50}")

        gate_pass = rep["duplicate_rate"] <= 5.0
        status = "PASS" if gate_pass else "FAIL"
        print(f"  Gate (≤5%): [{status}]")

    if args.output:
        # Write only non-duplicate records
        with open(args.output, "w", encoding="utf-8") as f:
            for rec in records:
                if not rec.get("is_duplicate_suspect"):
                    # Clean temp fields
                    rec.pop("is_duplicate_suspect", None)
                    rec.pop("duplicate_group_id", None)
                    rec.pop("duplicate_reason", None)
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        total_in = len(records)
        total_out = sum(1 for r in records if not r.get("is_duplicate_suspect"))
        print(f"\n  Written {total_out}/{total_in} records to {args.output}")


if __name__ == "__main__":
    main()
