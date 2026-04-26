"""
JSONL Schema Validator
======================

Validates enriched JSONL files against the DocumentRecord schema.

Usage:
    python -m pipelines.etl.validate_jsonl data/sample_enriched.jsonl
    python -m pipelines.etl.validate_jsonl data/*.jsonl --strict
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Add parent paths so we can import from app
REPO_ROOT = Path(__file__).resolve().parents[2]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))
from app.document_schema import DocumentRecord


def validate_file(path: str, strict: bool = False) -> int:
    """
    Validate a single JSONL file. Returns number of errors found.
    """
    errors = 0
    total = 0

    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line:
                continue
            total += 1

            # Parse JSON
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  ERROR line {lineno}: invalid JSON – {exc}")
                errors += 1
                continue

            if not isinstance(obj, dict):
                print(f"  ERROR line {lineno}: expected JSON object, got {type(obj).__name__}")
                errors += 1
                continue

            # Build record and validate
            try:
                record = DocumentRecord.from_dict(obj)
            except Exception as exc:
                print(f"  ERROR line {lineno}: cannot create DocumentRecord – {exc}")
                errors += 1
                continue

            validation_errors = record.validate()
            if validation_errors:
                for ve in validation_errors:
                    print(f"  ERROR line {lineno} (doc_id={record.doc_id!r}): {ve}")
                errors += len(validation_errors)

            # Strict mode: warn about missing optional fields
            if strict:
                optional_fields = [
                    "section_title", "source_url", "published_at",
                    "updated_at", "heading_path",
                ]
                for field_name in optional_fields:
                    val = getattr(record, field_name, "")
                    if not val or not str(val).strip():
                        print(f"  WARN  line {lineno} (doc_id={record.doc_id!r}): '{field_name}' is empty")

    return errors, total


def main():
    ap = argparse.ArgumentParser(description="Validate enriched JSONL files.")
    ap.add_argument("files", nargs="+", help="JSONL file(s) to validate")
    ap.add_argument("--strict", action="store_true", help="Warn about empty optional fields")
    args = ap.parse_args()

    total_errors = 0
    total_records = 0

    for path in args.files:
        print(f"\nValidating: {path}")
        errors, records = validate_file(path, strict=args.strict)
        total_errors += errors
        total_records += records
        if errors == 0:
            print(f"  [OK] {records} records - all valid")
        else:
            print(f"  [FAIL] {records} records - {errors} error(s)")

    print(f"\n{'='*50}")
    print(f"Total: {total_records} records, {total_errors} error(s)")

    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
