"""
QA Pre-Ingest: Tang 1 - Kiem tra cau truc du lieu (Schema Validation)
=====================================================================

Validates enriched JSONL files against the DocumentRecord schema.
Checks required fields, enums, doc_id uniqueness, URL validity,
date parsing, body quality, and HTML remnants.

Corresponds to CHECK.md Section 1.

Usage:
    python -m qa_pre_ingest.check_schema ../../data/data_final/medlineplus.jsonl
    python -m qa_pre_ingest.check_schema ../../data/data_final/*.jsonl --report report.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.document_schema import DocumentRecord, VALID_DOC_TYPES, VALID_AUDIENCES


# ── Validation rules ─────────────────────────────────────────────────
REQUIRED_FIELDS = ["doc_id", "title", "body", "source_name"]
REQUIRED_FOR_CITATION = ["source_name", "title"]  # must produce readable citation
MIN_BODY_LENGTH = 50
HTML_PATTERNS = [
    re.compile(r"<(div|span|table|tr|td|ul|ol|li|nav|footer|header|script|style)\b", re.I),
    re.compile(r"class=['\"]"),
    re.compile(r"id=['\"]"),
]
DATE_RE = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")  # YYYY or YYYY-MM or YYYY-MM-DD
US_DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")  # MM/DD/YYYY


def _is_valid_url(url: str) -> bool:
    if not url:
        return True  # optional
    try:
        r = urlparse(url)
        return all([r.scheme in ("http", "https"), r.netloc])
    except Exception:
        return False


def _has_html_remnants(text: str) -> bool:
    return any(p.search(text) for p in HTML_PATTERNS)


def _can_parse_date(d: str) -> bool:
    if not d:
        return True  # optional
    d = d.strip()
    return bool(DATE_RE.match(d)) or bool(US_DATE_RE.match(d))


def validate_record(rec: DocumentRecord, seen_ids: set) -> List[str]:
    """Validate a single record. Returns list of error strings."""
    errors = []

    # Required fields
    for f in REQUIRED_FIELDS:
        val = getattr(rec, f, "")
        if not val or not str(val).strip():
            errors.append(f"MISSING required field: {f}")

    # doc_id uniqueness
    if rec.doc_id in seen_ids:
        errors.append(f"DUPLICATE doc_id: {rec.doc_id}")

    # Enum validation
    if rec.doc_type and rec.doc_type not in VALID_DOC_TYPES:
        errors.append(f"INVALID doc_type: {rec.doc_type!r} (valid: {VALID_DOC_TYPES})")
    if rec.audience and rec.audience not in VALID_AUDIENCES:
        errors.append(f"INVALID audience: {rec.audience!r} (valid: {VALID_AUDIENCES})")
    if rec.trust_tier not in (1, 2, 3):
        errors.append(f"INVALID trust_tier: {rec.trust_tier} (must be 1, 2, or 3)")

    # URL validation
    if rec.source_url and not _is_valid_url(rec.source_url):
        errors.append(f"INVALID source_url: {rec.source_url!r}")

    # Date parsing
    if rec.published_at and not _can_parse_date(rec.published_at):
        errors.append(f"UNPARSEABLE published_at: {rec.published_at!r}")
    if rec.updated_at and not _can_parse_date(rec.updated_at):
        errors.append(f"UNPARSEABLE updated_at: {rec.updated_at!r}")

    # Body quality
    body = rec.body.strip() if rec.body else ""
    if body and len(body) < MIN_BODY_LENGTH:
        errors.append(f"BODY too short: {len(body)} chars (min {MIN_BODY_LENGTH})")
    if body == rec.title:
        errors.append("BODY is identical to title")

    # HTML remnants
    if body and _has_html_remnants(body):
        errors.append("HTML remnants detected in body")

    # Citation check
    if not rec.source_name or not rec.title:
        errors.append("CITATION impossible: missing source_name or title")

    return errors


def validate_file(path: str) -> Dict[str, Any]:
    """Validate a JSONL file. Returns report dict."""
    report = {
        "file": path,
        "total_records": 0,
        "valid_records": 0,
        "error_records": 0,
        "errors_by_type": Counter(),
        "errors_detail": [],  # first 20 errors with details
        "duplicate_ids": [],
        "specialty_dist": Counter(),
        "audience_dist": Counter(),
        "trust_tier_dist": Counter(),
        "source_dist": Counter(),
    }

    seen_ids: set = set()

    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line:
                continue

            report["total_records"] += 1

            # Parse JSON
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                report["error_records"] += 1
                report["errors_by_type"]["JSON_PARSE_ERROR"] += 1
                if len(report["errors_detail"]) < 20:
                    report["errors_detail"].append(
                        {"line": lineno, "error": f"JSON parse error: {exc}"}
                    )
                continue

            # Build record
            try:
                rec = DocumentRecord.from_dict(obj)
            except Exception as exc:
                report["error_records"] += 1
                report["errors_by_type"]["RECORD_BUILD_ERROR"] += 1
                if len(report["errors_detail"]) < 20:
                    report["errors_detail"].append(
                        {"line": lineno, "error": f"Cannot build record: {exc}"}
                    )
                continue

            # Validate
            errors = validate_record(rec, seen_ids)
            if rec.doc_id:
                if rec.doc_id in seen_ids:
                    report["duplicate_ids"].append(rec.doc_id)
                seen_ids.add(rec.doc_id)

            if errors:
                report["error_records"] += 1
                for e in errors:
                    etype = e.split(":")[0].strip()
                    report["errors_by_type"][etype] += 1
                if len(report["errors_detail"]) < 20:
                    report["errors_detail"].append(
                        {"line": lineno, "doc_id": rec.doc_id, "errors": errors}
                    )
            else:
                report["valid_records"] += 1

            # Stats
            report["specialty_dist"][rec.specialty] += 1
            report["audience_dist"][rec.audience] += 1
            report["trust_tier_dist"][rec.trust_tier] += 1
            report["source_dist"][rec.source_name] += 1

    return report


def print_report(report: Dict[str, Any]):
    """Print a human-readable report."""
    print(f"\n{'='*60}")
    print(f"  SCHEMA VALIDATION: {os.path.basename(report['file'])}")
    print(f"{'='*60}")
    print(f"  Total records:  {report['total_records']}")
    print(f"  Valid:          {report['valid_records']}")
    print(f"  Errors:         {report['error_records']}")

    if report["errors_by_type"]:
        print(f"\n  Error breakdown:")
        for etype, count in report["errors_by_type"].most_common():
            print(f"    {etype:40s} {count:5d}")

    if report["duplicate_ids"]:
        print(f"\n  Duplicate doc_ids ({len(report['duplicate_ids'])}):")
        for did in report["duplicate_ids"][:10]:
            print(f"    - {did}")

    print(f"\n  Source distribution:")
    for src, count in report["source_dist"].most_common():
        print(f"    {src:30s} {count:5d}")

    print(f"\n  Trust tier distribution:")
    tier_names = {1: "Canonical", 2: "Reference", 3: "Patient"}
    for tier, count in sorted(report["trust_tier_dist"].items()):
        print(f"    Tier {tier} ({tier_names.get(tier, '?'):10s}) {count:5d}")

    print(f"\n  Top specialties:")
    for spec, count in report["specialty_dist"].most_common(10):
        print(f"    {spec:30s} {count:5d}")

    # First few error details
    if report["errors_detail"]:
        print(f"\n  Sample errors (first {len(report['errors_detail'])}):")
        for detail in report["errors_detail"][:5]:
            did = detail.get("doc_id", "?")
            errs = detail.get("errors", [detail.get("error", "?")])
            print(f"    line {detail['line']} (doc_id={did}):")
            for e in (errs if isinstance(errs, list) else [errs]):
                print(f"      - {e}")

    # Pass/fail
    pass_rate = (report["valid_records"] / max(1, report["total_records"])) * 100
    status = "[PASS]" if report["error_records"] == 0 else "[FAIL]"
    print(f"\n  Result: {status} ({pass_rate:.1f}% valid)")
    print(f"{'='*60}")


def main():
    ap = argparse.ArgumentParser(description="Tang 1: Schema validation")
    ap.add_argument("files", nargs="+", help="JSONL files to validate")
    ap.add_argument("--report", help="Save JSON report to file")
    args = ap.parse_args()

    all_reports = []
    total_errors = 0

    for path in args.files:
        if not os.path.exists(path):
            print(f"  [SKIP] File not found: {path}")
            continue
        report = validate_file(path)
        print_report(report)
        all_reports.append(report)
        total_errors += report["error_records"]

    if args.report:
        # Convert Counter to dict for JSON serialization
        for r in all_reports:
            r["errors_by_type"] = dict(r["errors_by_type"])
            r["specialty_dist"] = dict(r["specialty_dist"])
            r["audience_dist"] = dict(r["audience_dist"])
            r["trust_tier_dist"] = dict(r["trust_tier_dist"])
            r["source_dist"] = dict(r["source_dist"])
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(all_reports, f, indent=2, ensure_ascii=False)
        print(f"\n[INFO] Report saved to {args.report}")

    sys.exit(1 if total_errors > 0 else 0)


if __name__ == "__main__":
    main()
