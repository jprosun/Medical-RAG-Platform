from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_paths import preferred_dataset_records_path, preferred_records_path  # noqa: E402


GENERIC_TITLE_PATTERNS = [
    re.compile(r"^\s*pdf\s*$", re.IGNORECASE),
    re.compile(r"^\s*document\s*$", re.IGNORECASE),
    re.compile(r"^\s*skip to main content\s*$", re.IGNORECASE),
    re.compile(r"^\s*tại bệnh viện\b", re.IGNORECASE),
    re.compile(r"^\s*thành phố\b", re.IGNORECASE),
    re.compile(r"^\s*tóm tắt\d*\b", re.IGNORECASE),
]

CONFERENCE_PATTERNS = [
    re.compile(r"\bkỷ yếu\b", re.IGNORECASE),
    re.compile(r"\bhội nghị\b", re.IGNORECASE),
    re.compile(r"\bhội thảo\b", re.IGNORECASE),
    re.compile(r"\bspecial issue\b", re.IGNORECASE),
    re.compile(r"\bproceedings\b", re.IGNORECASE),
    re.compile(r"\bquản trị bệnh viện\b", re.IGNORECASE),
]

ISSUE_URL_RE = re.compile(r"/issue/view/\d+", re.IGNORECASE)
CONFERENCE_BANNER_PATTERNS = [
    re.compile(r"h[ộo]i ngh[ịi]\s+khoa h[ọo]c\s+c[ôo]ng ngh[ệe].{0,80}y khoa vinh", re.IGNORECASE),
    re.compile(r"conference.{0,80}vinh medical university", re.IGNORECASE),
]

HIGH_CONFIDENCE_BANNED_FLAGS = {
    "body_noisy",
    "body_too_short",
    "title_looks_like_reference",
    "title_looks_like_table_header",
    "title_too_short",
    "too_many_sections_relative_to_length",
}


def _normalize_text(text: str) -> str:
    value = unicodedata.normalize("NFKC", (text or "").strip())
    value = re.sub(r"\s+", " ", value)
    return value


def _normalize_title_key(text: str) -> str:
    value = _normalize_text(text).casefold()
    value = re.sub(r"[^\w\s]", " ", value, flags=re.UNICODE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _iter_jsonl(path: Path):
    with open(path, "r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            if not raw.strip():
                continue
            try:
                yield line_no, json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {path}:{line_no}: {exc}") from exc


def _matches_any(text: str, patterns: list[re.Pattern[str]]) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def _normalize_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        value_str = str(value).strip()
        if value_str and value_str not in seen:
            out.append(value_str)
            seen.add(value_str)
    return out


def _merge_unique(existing: list[str], additions: list[str]) -> list[str]:
    out = list(existing)
    seen = set(existing)
    for item in additions:
        if item and item not in seen:
            out.append(item)
            seen.add(item)
    return out


def _strip_conference_banner(text: str) -> str:
    cleaned = text
    for pattern in CONFERENCE_BANNER_PATTERNS:
        cleaned = pattern.sub(" ", cleaned)
    return _normalize_text(cleaned)


def _looks_like_article_body(text: str) -> bool:
    lowered = (text or "").casefold()
    markers = (
        "mục tiêu",
        "objective",
        "đối tượng và phương pháp",
        "phương pháp nghiên cứu",
        "subjects and methods",
        "methods",
        "thiết kế nghiên cứu",
        "kết quả",
        "results",
    )
    matched = sum(1 for marker in markers if marker in lowered)
    return len(text) >= 500 and matched >= 2


def _salvage_title_from_body(title: str, body: str, section_title: str) -> tuple[str, str, list[str]]:
    stripped_body = _strip_conference_banner(body)
    if not _looks_like_article_body(stripped_body):
        return title, body, []

    repair_flags: list[str] = []
    if stripped_body != body:
        repair_flags.append("conference_banner_stripped")

    title_candidate = _normalize_text(section_title)
    if (
        not title_candidate
        or len(title_candidate) < 35
        or _matches_any(title_candidate, CONFERENCE_PATTERNS)
        or title_candidate.casefold().startswith(("summary", "tóm tắt", "kết quả"))
    ):
        title_candidate = ""

    if not title_candidate:
        match = re.search(
            r"(?:^|\bTitle:\s*)([A-Z][A-Z0-9 ,;:()\-\/]{40,220}?)(?:\bObjective\b|\bIntroduction\b|\bSUMMARY\b)",
            stripped_body,
        )
        if match:
            title_candidate = _normalize_text(match.group(1))

    if title_candidate and title_candidate != title:
        repair_flags.append("legacy_title_salvaged_from_body")
        return title_candidate, stripped_body, repair_flags

    return title, stripped_body, repair_flags


def build_v4_title_index(v4_path: Path) -> dict[str, dict[str, Any]]:
    title_index: dict[str, dict[str, Any]] = {}
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for _, row in _iter_jsonl(v4_path):
        key = _normalize_title_key(row.get("title") or row.get("canonical_title") or "")
        if key:
            grouped_rows[key].append(row)

    for key, rows in grouped_rows.items():
        title = _normalize_text(rows[0].get("title") or rows[0].get("canonical_title") or "")
        url_counter = Counter(str(row.get("source_url", "") or "").strip() for row in rows if row.get("source_url"))
        file_counter = Counter(str(row.get("source_file", "") or "").strip() for row in rows if row.get("source_file"))
        processed_counter = Counter(str(row.get("processed_path", "") or "").strip() for row in rows if row.get("processed_path"))
        sha_counter = Counter(str(row.get("source_sha256", "") or "").strip() for row in rows if row.get("source_sha256"))
        specialty_counter = Counter(str(row.get("specialty", "") or "").strip() for row in rows if row.get("specialty"))
        doc_type_counter = Counter(str(row.get("doc_type", "") or "").strip() for row in rows if row.get("doc_type"))

        title_index[key] = {
            "title": title,
            "url_values": [value for value in url_counter if value],
            "best_source_url": url_counter.most_common(1)[0][0] if url_counter else "",
            "best_source_file": file_counter.most_common(1)[0][0] if file_counter else "",
            "best_processed_path": processed_counter.most_common(1)[0][0] if processed_counter else "",
            "best_source_sha256": sha_counter.most_common(1)[0][0] if sha_counter else "",
            "best_specialty": specialty_counter.most_common(1)[0][0] if specialty_counter else "",
            "best_doc_type": doc_type_counter.most_common(1)[0][0] if doc_type_counter else "",
        }

    return title_index


def repair_row(row: dict[str, Any], title_index: dict[str, dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    title = _normalize_text(str(row.get("title", "") or ""))
    body = _normalize_text(str(row.get("body", "") or ""))
    section_title = _normalize_text(str(row.get("section_title", "") or ""))
    source_url = str(row.get("source_url", "") or "").strip()
    quality_status = str(row.get("quality_status", "") or "review").strip() or "review"
    quality_flags = _normalize_list(row.get("quality_flags"))
    tags = _normalize_list(row.get("tags"))
    title, body, salvage_flags = _salvage_title_from_body(title, body, section_title)
    quality_flags = _merge_unique(quality_flags, salvage_flags)
    reasons: list[str] = []

    if _matches_any(title, GENERIC_TITLE_PATTERNS):
        reasons.append("generic_title")
    if _matches_any(title, CONFERENCE_PATTERNS):
        reasons.append("conference_title")
    if _matches_any(body[:500], CONFERENCE_PATTERNS):
        reasons.append("conference_body")
    if len(title) < 35:
        reasons.append("short_title")
    if len(body) < 400:
        reasons.append("short_body")
    if "title_looks_like_reference" in quality_flags:
        reasons.append("reference_like_title")

    if salvage_flags and all(
        reason in {"generic_title", "conference_body", "short_title", "short_body"}
        for reason in reasons
    ):
        reasons = []

    if reasons:
        return "quarantine", {
            "doc_id": row.get("doc_id", ""),
            "title": title,
            "source_url": source_url,
            "reasons": reasons,
            "section_title": section_title,
            "body_prefix": body[:260],
        }

    repaired = dict(row)
    repaired["title"] = title
    repaired["body"] = body
    repaired["tags"] = tags
    repaired["quality_flags"] = quality_flags

    title_key = _normalize_title_key(title)
    index_entry = title_index.get(title_key)
    repair_flags: list[str] = ["legacy_vmj_v2_salvage"]

    if index_entry and len(index_entry["url_values"]) == 1:
        repaired["title"] = index_entry["title"] or title
        repaired["source_url"] = index_entry["best_source_url"] or source_url
        if index_entry["best_source_file"]:
            repaired["source_file"] = index_entry["best_source_file"]
        if index_entry["best_processed_path"]:
            repaired["processed_path"] = index_entry["best_processed_path"]
        if index_entry["best_source_sha256"]:
            repaired["source_sha256"] = index_entry["best_source_sha256"]
        if not repaired.get("specialty") and index_entry["best_specialty"]:
            repaired["specialty"] = index_entry["best_specialty"]
        if repaired.get("doc_type") in ("", "unknown") and index_entry["best_doc_type"]:
            repaired["doc_type"] = index_entry["best_doc_type"]
        repaired["quality_status"] = "go" if quality_status == "go" else quality_status
        repair_flags.append("source_url_backfilled_from_v4")
    else:
        repaired["source_url"] = source_url
        repaired["quality_status"] = "review" if quality_status == "go" else quality_status
        repair_flags.append("legacy_issue_url_only")
        if index_entry and len(index_entry["url_values"]) > 1:
            repair_flags.append("ambiguous_v4_title_match")

    repaired["source_id"] = "vmj_ojs"
    repaired["source_name"] = repaired.get("source_name") or "vmj_ojs"
    repaired["tags"] = _merge_unique(repaired["tags"], ["legacy_vmj_v2_salvage"])
    repaired["quality_flags"] = _merge_unique(repaired["quality_flags"], repair_flags)
    return "keep", repaired


def repair_dataset(input_path: Path, v4_path: Path, output_dir: Path) -> dict[str, Any]:
    title_index = build_v4_title_index(v4_path)
    records_dir = output_dir / "records"
    qa_dir = output_dir / "qa"
    records_dir.mkdir(parents=True, exist_ok=True)
    qa_dir.mkdir(parents=True, exist_ok=True)

    canonical_path = records_dir / "document_records.jsonl"
    backfilled_path = records_dir / "document_records_backfilled_article_url.jsonl"
    review_issue_path = records_dir / "document_records_issue_url_only_review.jsonl"
    review_issue_high_conf_path = records_dir / "document_records_issue_url_only_high_confidence.jsonl"
    quarantine_path = qa_dir / "quarantine.jsonl"
    summary_path = qa_dir / "repair_summary.json"

    kept = 0
    quarantined = 0
    backfilled = 0
    review_issue_url_only = 0
    review_issue_high_confidence = 0
    quality_status_counter: Counter[str] = Counter()
    quarantine_reason_counter: Counter[str] = Counter()

    with open(canonical_path, "w", encoding="utf-8") as keep_fh, open(
        backfilled_path, "w", encoding="utf-8"
    ) as backfilled_fh, open(review_issue_path, "w", encoding="utf-8") as issue_review_fh, open(
        review_issue_high_conf_path, "w", encoding="utf-8"
    ) as issue_high_conf_fh, open(
        quarantine_path, "w", encoding="utf-8"
    ) as quarantine_fh:
        for _, row in _iter_jsonl(input_path):
            decision, payload = repair_row(row, title_index)
            if decision == "keep":
                kept += 1
                quality_status_counter[str(payload.get("quality_status", "unknown"))] += 1
                flags = set(_normalize_list(payload.get("quality_flags")))
                if "source_url_backfilled_from_v4" in flags:
                    backfilled += 1
                    backfilled_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
                if "legacy_issue_url_only" in flags:
                    review_issue_url_only += 1
                    issue_review_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    quality_score = int(payload.get("quality_score", 0) or 0)
                    body = _normalize_text(str(payload.get("body", "") or ""))
                    title = _normalize_text(str(payload.get("title", "") or ""))
                    if (
                        quality_score >= 90
                        and len(title) >= 50
                        and len(body) >= 700
                        and not (flags & HIGH_CONFIDENCE_BANNED_FLAGS)
                        and str(payload.get("quality_status", "")) != "hold"
                    ):
                        review_issue_high_confidence += 1
                        issue_high_conf_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
                keep_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
            else:
                quarantined += 1
                for reason in payload.get("reasons", []):
                    quarantine_reason_counter[str(reason)] += 1
                quarantine_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    summary = {
        "input_records_path": str(input_path),
        "comparison_v4_vmj_path": str(v4_path),
        "output_dataset_dir": str(output_dir),
        "kept_records": kept,
        "quarantined_records": quarantined,
        "backfilled_source_url_from_v4": backfilled,
        "legacy_issue_url_only_records": review_issue_url_only,
        "legacy_issue_url_only_high_confidence_records": review_issue_high_confidence,
        "quality_status_counts": dict(quality_status_counter),
        "quarantine_reasons": dict(quarantine_reason_counter.most_common()),
        "canonical_records_path": str(canonical_path),
        "backfilled_records_path": str(backfilled_path),
        "issue_url_only_review_path": str(review_issue_path),
        "issue_url_only_high_confidence_path": str(review_issue_high_conf_path),
        "quarantine_path": str(quarantine_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a cautious salvage dataset from legacy vmj_ojs_release_v2.")
    parser.add_argument(
        "--input",
        default=str(preferred_dataset_records_path("vmj_ojs_release_v2")),
        help="Path to legacy vmj_ojs_release_v2 records JSONL.",
    )
    parser.add_argument(
        "--compare-v4",
        default=str(preferred_records_path("vmj_ojs")),
        help="Path to current cleaned vmj_ojs canonical records JSONL.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "rag-data" / "datasets" / "vmj_ojs_release_v2_salvage"),
        help="Directory for the salvage dataset.",
    )
    args = parser.parse_args()

    summary = repair_dataset(Path(args.input), Path(args.compare_v4), Path(args.output_dir))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
