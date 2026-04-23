# -*- coding: utf-8 -*-
"""Merge concatenated ChatGPT batch arrays into one valid JSON file.

This utility is tailored for `output/chatgpt_batches/final.json`, which may
contain multiple top-level JSON arrays pasted one after another. It merges all
records into a single JSON array, validates the expected schema, and emits a
quality report covering structure, schema, encoding, and lightweight content
heuristics.
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
BATCH_DIR = BASE_DIR / "output" / "chatgpt_batches"
DEFAULT_INPUT = BATCH_DIR / "final.json"
DEFAULT_OUTPUT = BATCH_DIR / "final_merged.json"
DEFAULT_REPORT_JSON = BATCH_DIR / "final_quality_report.json"
DEFAULT_REPORT_MD = BATCH_DIR / "final_quality_report.md"

REQUIRED_FIELDS = (
    "query_id",
    "ground_truth",
    "short_answer",
    "must_have_concepts",
    "must_not_claim",
)
STRING_FIELDS = ("query_id", "ground_truth", "short_answer")
LIST_FIELDS = ("must_have_concepts", "must_not_claim")
MOJIBAKE_MARKERS = (
    "Ã",
    "Â",
    "Ä",
    "Å",
    "Æ",
    "á»",
    "áº",
    "â€“",
    "â€œ",
    "â€",
    "ï»¿",
)
VIETNAMESE_CHAR_RE = re.compile(
    r"[àáảãạăắằẳẵặâấầẩẫậđ"
    r"èéẻẽẹêếềểễệ"
    r"ìíỉĩị"
    r"òóỏõọôốồổỗộơớờởỡợ"
    r"ùúủũụưứừửữự"
    r"ỳýỷỹỵ]",
    re.IGNORECASE,
)
TOKEN_RE = re.compile(r"\w+", re.UNICODE)
QUERY_ID_RE = re.compile(r"q_(\d+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge concatenated ChatGPT batch arrays and generate a quality report."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to the concatenated JSON file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path for the merged JSON array output.",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=DEFAULT_REPORT_JSON,
        help="Path for the machine-readable quality report.",
    )
    parser.add_argument(
        "--report-md",
        type=Path,
        default=DEFAULT_REPORT_MD,
        help="Path for the Markdown quality report.",
    )
    return parser.parse_args()


def read_utf8_text(path: Path) -> tuple[bytes, str]:
    raw_bytes = path.read_bytes()
    text = raw_bytes.decode("utf-8")
    return raw_bytes, text


def try_parse_json_document(text: str) -> tuple[bool, str | None]:
    try:
        json.loads(text)
        return True, None
    except json.JSONDecodeError as exc:
        return False, f"{exc.msg} (line {exc.lineno}, column {exc.colno})"


def parse_concatenated_arrays(text: str) -> list[list[Any]]:
    decoder = json.JSONDecoder()
    arrays: list[list[Any]] = []
    idx = 0

    while idx < len(text):
        while idx < len(text) and text[idx].isspace():
            idx += 1
        if idx >= len(text):
            break

        value, next_idx = decoder.raw_decode(text, idx)
        if not isinstance(value, list):
            raise ValueError(f"Expected a top-level JSON array at index {idx}, got {type(value).__name__}.")
        arrays.append(value)
        idx = next_idx

    if not arrays:
        raise ValueError("No JSON arrays were found in the input file.")
    return arrays


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text).lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_for_matching(text: str) -> str:
    text = normalize_text(text)
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(text: str) -> list[str]:
    return [token for token in TOKEN_RE.findall(normalize_for_matching(text)) if len(token) > 1]


def phrase_present(phrase: str, text: str) -> bool:
    normalized_text = normalize_for_matching(text)
    normalized_phrase = normalize_for_matching(phrase)
    if not normalized_phrase:
        return True
    if normalized_phrase in normalized_text:
        return True

    phrase_tokens = [token for token in normalized_phrase.split() if len(token) > 1]
    if not phrase_tokens:
        return True

    text_tokens = set(normalized_text.split())
    overlap = sum(1 for token in phrase_tokens if token in text_tokens)
    return overlap / len(phrase_tokens) >= 0.8


def list_phrase_coverage(phrases: list[str], text: str) -> tuple[float, list[str]]:
    if not phrases:
        return 1.0, []

    missing = [phrase for phrase in phrases if not phrase_present(phrase, text)]
    matched = len(phrases) - len(missing)
    return matched / len(phrases), missing


def lexical_coverage(source_text: str, target_text: str) -> float:
    source_tokens = tokenize(source_text)
    if not source_tokens:
        return 1.0
    target_tokens = set(tokenize(target_text))
    matched = sum(1 for token in source_tokens if token in target_tokens)
    return matched / len(source_tokens)


def count_sentences(text: str) -> int:
    parts = re.split(r"[.!?]+", text)
    return sum(1 for part in parts if part.strip())


def mojibake_marker_count(text: str) -> int:
    return sum(text.count(marker) for marker in MOJIBAKE_MARKERS)


def vietnamese_score(text: str) -> int:
    return len(VIETNAMESE_CHAR_RE.findall(text)) * 3 - mojibake_marker_count(text) * 2


def attempt_mojibake_repair(text: str) -> str | None:
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None


def iter_string_fields(record: dict[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    for field in STRING_FIELDS:
        value = record.get(field)
        if isinstance(value, str):
            values.append((field, value))
    for field in LIST_FIELDS:
        value = record.get(field)
        if isinstance(value, list):
            for idx, item in enumerate(value):
                if isinstance(item, str):
                    values.append((f"{field}[{idx}]", item))
    return values


def validate_record(record: Any, index: int) -> dict[str, Any]:
    errors: list[str] = []
    if not isinstance(record, dict):
        return {"record_index": index, "query_id": None, "errors": ["record is not a JSON object"]}

    query_id = record.get("query_id")
    for field in REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"missing field: {field}")

    for field in STRING_FIELDS:
        value = record.get(field)
        if field in record and not isinstance(value, str):
            errors.append(f"{field} must be a string")
        elif isinstance(value, str) and not value.strip():
            errors.append(f"{field} must not be empty")

    for field in LIST_FIELDS:
        value = record.get(field)
        if field in record and not isinstance(value, list):
            errors.append(f"{field} must be a list")
        elif isinstance(value, list):
            if any(not isinstance(item, str) for item in value):
                errors.append(f"{field} must contain only strings")

    return {"record_index": index, "query_id": query_id, "errors": errors}


def analyze_schema(records: list[dict[str, Any]]) -> dict[str, Any]:
    validation_results = [validate_record(record, idx) for idx, record in enumerate(records)]
    schema_errors = [item for item in validation_results if item["errors"]]

    query_ids = [record.get("query_id") for record in records if isinstance(record, dict)]
    query_counter = Counter(qid for qid in query_ids if isinstance(qid, str))
    duplicate_query_ids = sorted(qid for qid, count in query_counter.items() if count > 1)

    parsed_ids = []
    nonstandard_query_ids = []
    for query_id in query_counter:
        match = QUERY_ID_RE.fullmatch(query_id)
        if not match:
            nonstandard_query_ids.append(query_id)
            continue
        parsed_ids.append(int(match.group(1)))

    missing_query_ids: list[str] = []
    if parsed_ids:
        for numeric_id in range(min(parsed_ids), max(parsed_ids) + 1):
            if numeric_id not in parsed_ids:
                missing_query_ids.append(f"q_{numeric_id:03d}")

    return {
        "schema_error_count": len(schema_errors),
        "schema_errors": schema_errors,
        "duplicate_query_ids": duplicate_query_ids,
        "missing_query_ids": missing_query_ids,
        "nonstandard_query_ids": sorted(nonstandard_query_ids),
    }


def analyze_format(blocks: list[list[Any]], records: list[dict[str, Any]]) -> dict[str, Any]:
    block_sizes = [len(block) for block in blocks]
    key_signatures = Counter()
    for record in records:
        if isinstance(record, dict):
            key_signatures[tuple(sorted(record.keys()))] += 1

    return {
        "block_count": len(blocks),
        "block_sizes": block_sizes,
        "distinct_key_signatures": [
            {"keys": list(signature), "count": count}
            for signature, count in key_signatures.items()
        ],
    }


def analyze_encoding(raw_bytes: bytes, records: list[dict[str, Any]]) -> dict[str, Any]:
    suspicious_examples = []
    suspicious_string_count = 0
    improved_repairs = 0
    total_string_fields = 0
    vietnamese_like_string_count = 0
    suspicious_record_ids = set()

    for record in records:
        query_id = record.get("query_id", "<missing>")
        for field_name, value in iter_string_fields(record):
            total_string_fields += 1
            if VIETNAMESE_CHAR_RE.search(value):
                vietnamese_like_string_count += 1
            marker_count = mojibake_marker_count(value)
            if marker_count <= 0:
                continue

            suspicious_string_count += 1
            suspicious_record_ids.add(query_id)
            repaired = attempt_mojibake_repair(value)
            improved = False
            if repaired is not None and vietnamese_score(repaired) > vietnamese_score(value):
                improved_repairs += 1
                improved = True

            if len(suspicious_examples) < 5:
                suspicious_examples.append(
                    {
                        "query_id": query_id,
                        "field": field_name,
                        "original_sample": value[:160],
                        "repaired_sample": repaired[:160] if repaired else None,
                        "repair_improves_text": improved,
                    }
                )

    raw_byte_markers = {
        "utf8_bytes_for_A_tilde": raw_bytes.count(b"\xc3\x83"),
        "utf8_bytes_for_A_circumflex": raw_bytes.count(b"\xc3\x82"),
    }

    likely_data_level_mojibake = suspicious_string_count > 0 and improved_repairs >= max(1, suspicious_string_count // 2)
    if likely_data_level_mojibake:
        verdict = "The mojibake is present in the stored file content, not only in terminal rendering."
    elif suspicious_string_count == 0 and vietnamese_like_string_count > 0:
        verdict = (
            "Parsed strings contain normal Vietnamese Unicode characters and no strong mojibake markers. "
            "If shell output looks corrupted, the problem is likely terminal rendering or console encoding."
        )
    elif suspicious_string_count > 0:
        verdict = "Suspicious encoding markers exist, but the repair heuristic is inconclusive."
    else:
        verdict = "No strong mojibake markers were detected in parsed string fields."

    return {
        "verdict": verdict,
        "likely_data_level_mojibake": likely_data_level_mojibake,
        "total_string_fields": total_string_fields,
        "vietnamese_like_string_count": vietnamese_like_string_count,
        "suspicious_string_count": suspicious_string_count,
        "improved_repair_count": improved_repairs,
        "suspicious_record_count": len(suspicious_record_ids),
        "suspicious_record_ids": sorted(suspicious_record_ids),
        "raw_byte_markers": raw_byte_markers,
        "examples": suspicious_examples,
    }


def analyze_content(records: list[dict[str, Any]]) -> dict[str, Any]:
    flagged_records = []
    issue_counter: Counter[str] = Counter()

    for record in records:
        query_id = record.get("query_id")
        issues: list[str] = []

        ground_truth = record.get("ground_truth", "")
        short_answer = record.get("short_answer", "")
        must_have = record.get("must_have_concepts", [])
        must_not = record.get("must_not_claim", [])

        if not isinstance(ground_truth, str) or not isinstance(short_answer, str):
            continue
        if not isinstance(must_have, list) or not isinstance(must_not, list):
            continue

        ground_truth_sentence_count = count_sentences(ground_truth)
        short_answer_token_ratio = len(tokenize(short_answer)) / max(1, len(tokenize(ground_truth)))
        short_to_ground_overlap = lexical_coverage(short_answer, ground_truth)
        must_have_ground_ratio, missing_in_ground = list_phrase_coverage(must_have, ground_truth)
        must_have_short_ratio, missing_in_short = list_phrase_coverage(must_have, short_answer)

        if ground_truth_sentence_count < 2 or ground_truth_sentence_count > 5:
            issues.append("ground_truth_sentence_count_outside_2_to_5")
        if len(must_have) < 2 or len(must_have) > 5:
            issues.append("must_have_concepts_count_outside_2_to_5")
        if len(must_not) > 3:
            issues.append("must_not_claim_count_above_3")
        if short_answer_token_ratio > 0.85:
            issues.append("short_answer_not_much_shorter_than_ground_truth")
        if short_to_ground_overlap < 0.55:
            issues.append("short_answer_low_lexical_overlap_with_ground_truth")
        if must_have_ground_ratio < 0.8:
            issues.append("must_have_concepts_weakly_supported_by_ground_truth")
        if must_have_short_ratio < 0.5:
            issues.append("short_answer_covers_too_few_must_have_concepts")
        if any(mojibake_marker_count(value) > 0 for _, value in iter_string_fields(record)):
            issues.append("encoding_mojibake_detected")

        metrics = {
            "ground_truth_sentence_count": ground_truth_sentence_count,
            "must_have_count": len(must_have),
            "must_not_count": len(must_not),
            "short_answer_token_ratio": round(short_answer_token_ratio, 3),
            "short_to_ground_overlap": round(short_to_ground_overlap, 3),
            "must_have_ground_ratio": round(must_have_ground_ratio, 3),
            "must_have_short_ratio": round(must_have_short_ratio, 3),
            "missing_must_have_in_ground_truth": missing_in_ground[:3],
            "missing_must_have_in_short_answer": missing_in_short[:3],
        }

        if issues:
            for issue in issues:
                issue_counter[issue] += 1
            flagged_records.append(
                {
                    "query_id": query_id,
                    "issues": issues,
                    "metrics": metrics,
                }
            )

    return {
        "heuristic_note": (
            "Content checks are heuristic. They are intended to flag likely weak spots for review, "
            "not to replace manual medical-quality auditing."
        ),
        "issue_counts": dict(issue_counter.most_common()),
        "flagged_record_count": len(flagged_records),
        "flagged_records": flagged_records,
    }


def build_report(
    input_path: Path,
    output_path: Path,
    raw_bytes: bytes,
    original_text: str,
    blocks: list[list[Any]],
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    original_valid_json, original_parse_error = try_parse_json_document(original_text)
    schema_analysis = analyze_schema(records)
    format_analysis = analyze_format(blocks, records)
    encoding_analysis = analyze_encoding(raw_bytes, records)
    content_analysis = analyze_content(records)

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path),
        "output_path": str(output_path),
        "summary": {
            "original_file_is_valid_json": original_valid_json,
            "original_parse_error": original_parse_error,
            "merged_file_record_count": len(records),
            "merged_file_block_count": len(blocks),
            "schema_error_count": schema_analysis["schema_error_count"],
            "duplicate_query_id_count": len(schema_analysis["duplicate_query_ids"]),
            "missing_query_id_count": len(schema_analysis["missing_query_ids"]),
            "flagged_record_count": content_analysis["flagged_record_count"],
            "encoding_likely_data_level_mojibake": encoding_analysis["likely_data_level_mojibake"],
        },
        "structure": {
            "original_file_is_valid_json": original_valid_json,
            "original_parse_error": original_parse_error,
            "detected_top_level_array_blocks": len(blocks),
            "block_sizes": format_analysis["block_sizes"],
        },
        "schema_validation": schema_analysis,
        "format_consistency": format_analysis,
        "encoding_analysis": encoding_analysis,
        "content_quality": content_analysis,
    }


def report_to_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    schema_validation = report["schema_validation"]
    encoding = report["encoding_analysis"]
    content = report["content_quality"]
    structure = report["structure"]
    format_consistency = report["format_consistency"]

    lines = [
        "# ChatGPT Batch Merge Quality Report",
        "",
        "## Summary",
        f"- Input file: `{report['input_path']}`",
        f"- Merged output: `{report['output_path']}`",
        f"- Original `final.json` valid JSON: `{summary['original_file_is_valid_json']}`",
        f"- Original parse error: `{summary['original_parse_error']}`",
        f"- Top-level array blocks detected: `{summary['merged_file_block_count']}`",
        f"- Total merged records: `{summary['merged_file_record_count']}`",
        f"- Schema errors: `{summary['schema_error_count']}`",
        f"- Duplicate query IDs: `{summary['duplicate_query_id_count']}`",
        f"- Missing query IDs in numeric sequence: `{summary['missing_query_id_count']}`",
        f"- Records flagged by heuristic content checks: `{summary['flagged_record_count']}`",
        "",
        "## Structural Findings",
        f"- Block sizes: `{structure['block_sizes']}`",
        f"- Distinct key signatures: `{len(format_consistency['distinct_key_signatures'])}`",
        "",
        "## Encoding Assessment",
        f"- Verdict: {encoding['verdict']}",
        f"- Suspicious string fields: `{encoding['suspicious_string_count']}` / `{encoding['total_string_fields']}`",
        f"- Suspicious records: `{encoding['suspicious_record_count']}`",
        f"- Heuristic repairs that improved text: `{encoding['improved_repair_count']}`",
        f"- Raw-byte marker counts: `{encoding['raw_byte_markers']}`",
        "",
        "## Schema Findings",
        f"- Duplicate query IDs: `{schema_validation['duplicate_query_ids']}`",
        f"- Missing query IDs: `{schema_validation['missing_query_ids']}`",
        f"- Nonstandard query IDs: `{schema_validation['nonstandard_query_ids']}`",
        "",
        "## Content Heuristic Findings",
        f"- Issue counts: `{content['issue_counts']}`",
        f"- Note: {content['heuristic_note']}",
        "",
        "## Sample Encoding Examples",
    ]

    if summary["original_file_is_valid_json"]:
        lines.insert(
            14,
            "- The input file is already a valid JSON document. No concatenated-array recovery was needed."
        )
    else:
        lines.insert(
            14,
            "- The input file is not a single valid JSON document because it contains multiple top-level arrays pasted sequentially."
        )

    if encoding["examples"]:
        for example in encoding["examples"]:
            lines.append(f"- `{example['query_id']}` `{example['field']}`")
            lines.append(f"  - Original: `{example['original_sample']}`")
            if example["repaired_sample"]:
                lines.append(f"  - Repaired: `{example['repaired_sample']}`")
            lines.append(f"  - Repair improves text: `{example['repair_improves_text']}`")
    else:
        lines.append("- No suspicious examples collected.")

    lines.extend(
        [
            "",
            "## Sample Flagged Records",
        ]
    )
    if content["flagged_records"]:
        for item in content["flagged_records"][:15]:
            lines.append(f"- `{item['query_id']}`: `{', '.join(item['issues'])}`")
            lines.append(f"  - Metrics: `{item['metrics']}`")
    else:
        lines.append("- No records were flagged by the current heuristics.")

    return "\n".join(lines) + "\n"


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main() -> None:
    args = parse_args()

    raw_bytes, original_text = read_utf8_text(args.input)
    blocks = parse_concatenated_arrays(original_text)
    merged_records = [record for block in blocks for record in block]

    save_json(args.output, merged_records)

    report = build_report(
        input_path=args.input,
        output_path=args.output,
        raw_bytes=raw_bytes,
        original_text=original_text,
        blocks=blocks,
        records=merged_records,
    )
    save_json(args.report_json, report)
    save_text(args.report_md, report_to_markdown(report))

    print(f"Merged {len(blocks)} blocks into {args.output}")
    print(f"Total records: {len(merged_records)}")
    print(f"Schema errors: {report['summary']['schema_error_count']}")
    print(f"Duplicate query IDs: {report['summary']['duplicate_query_id_count']}")
    print(f"Likely data-level mojibake: {report['summary']['encoding_likely_data_level_mojibake']}")
    print(f"Quality report JSON: {args.report_json}")
    print(f"Quality report Markdown: {args.report_md}")


if __name__ == "__main__":
    main()
