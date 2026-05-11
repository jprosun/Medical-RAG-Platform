from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


BAD_TITLES = {
    "skip to main content",
    "pdf",
    "document",
    "trang ch tin tc gii thiu",
}
DROP_FLAGS = {
    "release_missing_source_url",
    "release_body_too_short",
    "title_has_admin_wrapper",
}
HEX_LIKE_TITLE = re.compile(r"^[0-9a-f]{24,}$", re.IGNORECASE)


def _normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", (title or "").strip().lower())


def filter_record(record: dict[str, Any], *, min_body_chars: int = 120) -> list[str]:
    reasons: list[str] = []
    title = str(record.get("title") or "").strip()
    body = str(record.get("body") or "").strip()
    source_url = str(record.get("source_url") or "").strip()
    quality_status = str(record.get("quality_status") or "").strip().lower()
    flags = {str(flag).strip() for flag in (record.get("quality_flags") or []) if str(flag).strip()}

    if not title:
        reasons.append("missing_title")
    else:
        normalized_title = _normalize_title(title)
        if normalized_title in BAD_TITLES:
            reasons.append("bad_title")
        if HEX_LIKE_TITLE.fullmatch(title):
            reasons.append("hex_like_title")

    if not body:
        reasons.append("missing_body")
    elif len(body) < min_body_chars:
        reasons.append("body_too_short")

    if not source_url:
        reasons.append("missing_source_url")
    if quality_status == "hold":
        reasons.append("quality_hold")

    for flag in sorted(flags & DROP_FLAGS):
        reasons.append(f"flag:{flag}")

    return reasons


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(raw) for raw in fh if raw.strip()]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def build_embed_ready_release(
    *,
    input_path: Path,
    output_path: Path,
    report_path: Path,
    min_body_chars: int = 120,
) -> dict[str, Any]:
    records = _read_jsonl(input_path)
    kept: list[dict[str, Any]] = []
    dropped_by_reason: Counter[str] = Counter()
    dropped_by_source: Counter[str] = Counter()
    kept_by_source: Counter[str] = Counter()

    for record in records:
        source_id = str(record.get("source_id") or "").strip() or "unknown"
        reasons = filter_record(record, min_body_chars=min_body_chars)
        if reasons:
            dropped_by_source[source_id] += 1
            for reason in reasons:
                dropped_by_reason[reason] += 1
            continue
        kept.append(record)
        kept_by_source[source_id] += 1

    _write_jsonl(output_path, kept)
    report = {
        "input_records": len(records),
        "output_records": len(kept),
        "dropped_records": len(records) - len(kept),
        "min_body_chars": min_body_chars,
        "input_path": str(input_path),
        "output_path": str(output_path),
        "drop_reasons": dict(sorted(dropped_by_reason.items())),
        "kept_by_source": dict(sorted(kept_by_source.items())),
        "dropped_by_source": dict(sorted(dropped_by_source.items())),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter a dataset release down to embed-ready records.")
    parser.add_argument("--input-jsonl", required=True)
    parser.add_argument("--output-jsonl", required=True)
    parser.add_argument("--report-json", required=True)
    parser.add_argument("--min-body-chars", type=int, default=120)
    args = parser.parse_args()

    report = build_embed_ready_release(
        input_path=Path(args.input_jsonl),
        output_path=Path(args.output_jsonl),
        report_path=Path(args.report_json),
        min_body_chars=args.min_body_chars,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
