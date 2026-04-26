from __future__ import annotations

import json
import os
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR))

from services.utils.data_paths import preferred_records_path  # noqa: E402


INPUT_JSONL = preferred_records_path("vmj_ojs")
REPORT_OUT = BASE_DIR / "benchmark" / "reports" / "vmj_pre_ingest_validate.json"


def validate() -> None:
    print("Running Pre-Ingest Validator...")

    total_records = 0
    errors = []
    seen_ids = set()
    warnings = []

    with open(INPUT_JSONL, "r", encoding="utf-8") as fh:
        for line_num, line in enumerate(fh, start=1):
            total_records += 1
            rec = json.loads(line)

            point_id = rec.get("doc_id")
            if not point_id:
                errors.append(f"Line {line_num}: Missing 'doc_id'")
            else:
                if point_id in seen_ids:
                    errors.append(f"Line {line_num}: Duplicate 'doc_id': {point_id}")
                seen_ids.add(point_id)

            title = rec.get("title", "").strip()
            if not title or title.lower() in ("pdf", "document"):
                errors.append(f"Line {line_num}: Invalid or empty 'title'")

            body = rec.get("body", "").strip()
            if not body or len(body) < 10:
                errors.append(f"Line {line_num}: Empty or too short 'body'")

            if len(body) < 100:
                warnings.append(f"Line {line_num}: Very short chunk ({len(body)} chars)")

            src_url = rec.get("source_url", "")
            if not src_url and not rec.get("file_url"):
                errors.append(f"Line {line_num}: Missing source_url and file_url")

    fail_rate = len(errors) / total_records if total_records else 0

    report = {
        "status": "PASS" if fail_rate <= 0.01 and len(errors) < 50 else "FAIL",
        "total_records": total_records,
        "unique_doc_ids": len(seen_ids),
        "total_errors": len(errors),
        "total_warnings": len(warnings),
        "error_rate": fail_rate,
        "sample_errors": errors[:10],
        "gate_g1_passed": fail_rate <= 0.01 and "Duplicate 'doc_id'" not in str(errors),
        "input_jsonl": str(INPUT_JSONL),
    }

    os.makedirs(REPORT_OUT.parent, exist_ok=True)
    with open(REPORT_OUT, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=4, ensure_ascii=False)

    print(
        f"Validation complete. Records: {total_records}. "
        f"Errors: {len(errors)}. Gate Status: {report['status']}"
    )


if __name__ == "__main__":
    validate()
