from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


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
ARTICLE_URL_RE = re.compile(r"/article/(?:view|download)/\d+", re.IGNORECASE)
DOWNLOAD_PAIR_RE = re.compile(r"/article/download/(\d+)/(\d+)", re.IGNORECASE)


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


def _body_prefix(body: str, length: int = 220) -> str:
    return _normalize_text(body)[:length]


def audit(v2_path: Path, v4_vmj_path: Path) -> dict:
    v4_title_keys: set[str] = set()
    for _, row in _iter_jsonl(v4_vmj_path):
        title = row.get("title") or row.get("canonical_title") or ""
        key = _normalize_title_key(title)
        if key:
            v4_title_keys.add(key)

    total = 0
    issue_url_records = 0
    article_url_records = 0
    missing_source_url = 0
    generic_title_records = 0
    conference_title_records = 0
    conference_body_records = 0
    short_title_records = 0
    short_body_records = 0
    all_caps_title_records = 0
    same_issue_counter: Counter[str] = Counter()
    top_title_counter: Counter[str] = Counter()
    doc_type_counter: Counter[str] = Counter()
    specialty_counter: Counter[str] = Counter()
    quality_status_counter: Counter[str] = Counter()
    quality_flag_counter: Counter[str] = Counter()
    overlap_title_records = 0
    overlap_title_unique: set[str] = set()
    legacy_title_unique: set[str] = set()
    conference_issue_records = 0
    likely_good_unique_records = 0

    samples = defaultdict(list)

    for _, row in _iter_jsonl(v2_path):
        total += 1
        title = _normalize_text(row.get("title", ""))
        body = _normalize_text(row.get("body", ""))
        source_url = str(row.get("source_url", "") or "").strip()
        section_title = _normalize_text(row.get("section_title", ""))
        doc_type = _normalize_text(str(row.get("doc_type", "unknown"))) or "unknown"
        specialty = _normalize_text(str(row.get("specialty", "unknown"))) or "unknown"
        quality_status = _normalize_text(str(row.get("quality_status", "unknown"))) or "unknown"
        quality_flags = row.get("quality_flags") or []

        top_title_counter[title] += 1
        doc_type_counter[doc_type] += 1
        specialty_counter[specialty] += 1
        quality_status_counter[quality_status] += 1
        for flag in quality_flags:
            quality_flag_counter[str(flag)] += 1

        if not source_url:
            missing_source_url += 1
            if len(samples["missing_source_url"]) < 5:
                samples["missing_source_url"].append({"title": title, "section_title": section_title})
        elif ISSUE_URL_RE.search(source_url):
            issue_url_records += 1
            same_issue_counter[source_url] += 1
        elif ARTICLE_URL_RE.search(source_url):
            article_url_records += 1

        if title and title == title.upper():
            all_caps_title_records += 1

        if len(title) < 35:
            short_title_records += 1

        if len(body) < 400:
            short_body_records += 1

        if _matches_any(title, GENERIC_TITLE_PATTERNS):
            generic_title_records += 1
            if len(samples["generic_titles"]) < 10:
                samples["generic_titles"].append({"title": title, "section_title": section_title, "source_url": source_url})

        conference_title = _matches_any(title, CONFERENCE_PATTERNS)
        conference_body = _matches_any(body[:500], CONFERENCE_PATTERNS)
        if conference_title:
            conference_title_records += 1
            if len(samples["conference_titles"]) < 10:
                samples["conference_titles"].append({"title": title, "source_url": source_url})
        if conference_body:
            conference_body_records += 1
            if len(samples["conference_body"]) < 10:
                samples["conference_body"].append({"title": title, "body_prefix": _body_prefix(body), "source_url": source_url})
        if conference_title and ISSUE_URL_RE.search(source_url):
            conference_issue_records += 1

        title_key = _normalize_title_key(title)
        if title_key:
            legacy_title_unique.add(title_key)
            if title_key in v4_title_keys:
                overlap_title_records += 1
                overlap_title_unique.add(title_key)
            elif (
                source_url
                and not conference_title
                and not conference_body
                and not _matches_any(title, GENERIC_TITLE_PATTERNS)
                and len(title) >= 35
                and len(body) >= 700
            ):
                likely_good_unique_records += 1
                if len(samples["likely_good_unique"]) < 10:
                    samples["likely_good_unique"].append(
                        {
                            "title": title,
                            "source_url": source_url,
                            "section_title": section_title,
                            "body_prefix": _body_prefix(body),
                        }
                    )

    top_issue_urls = [
        {"source_url": source_url, "records": count}
        for source_url, count in same_issue_counter.most_common(15)
    ]
    repeated_titles = [
        {"title": title, "records": count}
        for title, count in top_title_counter.most_common(20)
    ]

    return {
        "input_records_path": str(v2_path),
        "comparison_v4_vmj_path": str(v4_vmj_path),
        "total_records": total,
        "source_url": {
            "missing": missing_source_url,
            "issue_url_records": issue_url_records,
            "article_url_records": article_url_records,
            "unique_issue_urls": len(same_issue_counter),
        },
        "title_quality": {
            "generic_title_records": generic_title_records,
            "conference_title_records": conference_title_records,
            "short_title_records": short_title_records,
            "all_caps_title_records": all_caps_title_records,
        },
        "body_quality": {
            "short_body_records": short_body_records,
            "conference_body_records": conference_body_records,
        },
        "legacy_vs_v4": {
            "legacy_unique_titles": len(legacy_title_unique),
            "v4_overlap_unique_titles": len(overlap_title_unique),
            "overlap_records": overlap_title_records,
            "likely_good_unique_records": likely_good_unique_records,
        },
        "risk_signals": {
            "conference_issue_records": conference_issue_records,
            "top_issue_urls": top_issue_urls,
            "repeated_titles": repeated_titles,
        },
        "metadata_distribution": {
            "doc_type_top": dict(doc_type_counter.most_common(15)),
            "specialty_top": dict(specialty_counter.most_common(15)),
            "quality_status": dict(quality_status_counter.most_common()),
            "quality_flags_top": dict(quality_flag_counter.most_common(20)),
        },
        "samples": dict(samples),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit legacy vmj_ojs_release_v2 before salvage.")
    parser.add_argument(
        "--input",
        default=str(preferred_dataset_records_path("vmj_ojs_release_v2")),
        help="Path to legacy vmj_ojs_release_v2 records JSONL.",
    )
    parser.add_argument(
        "--compare-v4",
        default=str(preferred_records_path("vmj_ojs")),
        help="Path to current cleaned vmj_ojs canonical records JSONL for overlap estimation.",
    )
    parser.add_argument(
        "--output",
        default=str(REPO_ROOT / "rag-data" / "qa" / "vmj_ojs_release_v2_audit.json"),
        help="Path to write the audit JSON report.",
    )
    args = parser.parse_args()

    report = audit(Path(args.input), Path(args.compare_v4))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote audit report to {output_path}")
    print(json.dumps({
        "total_records": report["total_records"],
        "issue_url_records": report["source_url"]["issue_url_records"],
        "generic_title_records": report["title_quality"]["generic_title_records"],
        "conference_title_records": report["title_quality"]["conference_title_records"],
        "conference_body_records": report["body_quality"]["conference_body_records"],
        "likely_good_unique_records": report["legacy_vs_v4"]["likely_good_unique_records"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
