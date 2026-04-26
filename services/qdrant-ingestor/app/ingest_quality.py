"""
Ingestion-side quality helpers for enriched medical corpora.

This module is intentionally lightweight and self-contained so the deployed
qdrant-ingestor image can use it without depending on the ETL package tree.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any


QUALITY_RANK = {"hold": 0, "review": 1, "go": 2}

_BAD_TITLES = {"pdf", "document", "untitled"}

_MAJOR_HEADINGS = [
    "ĐỐI TƯỢNG VÀ PHƯƠNG PHÁP NGHIÊN CỨU",
    "VẬT LIỆU VÀ PHƯƠNG PHÁP NGHIÊN CỨU",
    "THEO DÕI VÀ XỬ TRÍ TAI BIẾN",
    "CÁC BƯỚC TIẾN HÀNH",
    "TIẾN HÀNH QTKT",
    "TÀI LIỆU THAM KHẢO",
    "CHỐNG CHỈ ĐỊNH",
    "ĐẶT VẤN ĐỀ",
    "GIỚI THIỆU",
    "MỞ ĐẦU",
    "ĐẠI CƯƠNG",
    "TÓM TẮT",
    "ABSTRACT",
    "SUMMARY",
    "ĐỐI TƯỢNG VÀ PHƯƠNG PHÁP",
    "VẬT LIỆU VÀ PHƯƠNG PHÁP",
    "KẾT QUẢ NGHIÊN CỨU",
    "KẾT QUẢ",
    "BÀN LUẬN",
    "KẾT LUẬN",
    "KIẾN NGHỊ",
    "CHỈ ĐỊNH",
    "CHUẨN BỊ",
    "THEO DÕI",
    "PHỤ LỤC",
    "APPENDIX",
    "REFERENCES",
]

_PLAIN_SECTION_RE = re.compile(
    r"^(?:\d+(?:\.\d+)*[.)]?\s*)?"
    r"(TÓM TẮT|ABSTRACT|SUMMARY|ĐẶT VẤN ĐỀ|GIỚI THIỆU|MỞ ĐẦU|"
    r"ĐỐI TƯỢNG VÀ PHƯƠNG PHÁP(?: NGHIÊN CỨU)?|VẬT LIỆU VÀ PHƯƠNG PHÁP(?: NGHIÊN CỨU)?|"
    r"KẾT QUẢ(?: NGHIÊN CỨU)?|BÀN LUẬN|KẾT LUẬN|KIẾN NGHỊ|"
    r"CHỈ ĐỊNH|CHỐNG CHỈ ĐỊNH|CHUẨN BỊ|THEO DÕI(?: VÀ XỬ TRÍ TAI BIẾN)?|"
    r"ĐẠI CƯƠNG|CÁC BƯỚC TIẾN HÀNH|TIẾN HÀNH QTKT|"
    r"TÀI LIỆU THAM KHẢO|REFERENCES|PHỤ LỤC|APPENDIX)"
    r"\s*:?\s*$",
    re.IGNORECASE,
)

_INLINE_SECTION_RE = re.compile(
    r"^(?:\d+(?:\.\d+)*[.)]?\s*)?"
    r"(TÃ“M Táº®T|ABSTRACT|SUMMARY|Äáº¶T Váº¤N Äá»€|GIá»šI THIá»†U|Má»ž Äáº¦U|"
    r"Äá»I TÆ¯á»¢NG VÃ€ PHÆ¯Æ NG PHÃP(?: NGHIÃŠN Cá»¨U)?|Váº¬T LIá»†U VÃ€ PHÆ¯Æ NG PHÃP(?: NGHIÃŠN Cá»¨U)?|"
    r"Káº¾T QUáº¢(?: NGHIÃŠN Cá»¨U)?|BÃ€N LUáº¬N|Káº¾T LUáº¬N|KIáº¾N NGHá»Š|"
    r"CHá»ˆ Äá»ŠNH|CHá»NG CHá»ˆ Äá»ŠNH|CHUáº¨N Bá»Š|THEO DÃ•I(?: VÃ€ Xá»¬ TRÃ TAI BIáº¾N)?|"
    r"Äáº I CÆ¯Æ NG|CÃC BÆ¯á»šC TIáº¾N HÃ€NH|TIáº¾N HÃ€NH QTKT)"
    r"(?:\s*\d+)?(?:\s*[:.-]\s*|\s+)(.+)$",
    re.IGNORECASE,
)

_REFERENCE_LINE_RE = re.compile(
    r"^\s*(?:\[\d+\]|\d+\.)\s+.*(?:doi:\s*10\.|PMID:|\(\d{4}\)|et al\.?|pp?\.\s*\d+)"
    r"|(?:Pan Afr Med J|Lancet|BMJ|N Engl J Med|Cochrane Database)"
    r"|https?://"
    r"|doi:\s*10\.",
    re.IGNORECASE,
)

_TABLE_LINE_RE = re.compile(
    r"^\s*(?:TT|STT|Tên hoạt chất|Tên thuốc|Hàm lượng|Đơn vị|Số lượng|Nồng độ)\b"
    r"|^\s*\|.+\|\s*$"
    r"|^\s*[\w\s/%().-]+(?:\t| {2,}|,\s*)[\w\s/%().-]+(?:\t| {2,}|,\s*)",
    re.IGNORECASE,
)

_TITLE_REFERENCE_RE = re.compile(
    r"^\d+\.\s+.*(?:doi:\s*10\.|\(\d{4}\)|et al\.?|pp?\.\s*\d+)"
    r"|Pan Afr Med J|N Engl J Med|BMJ|Lancet|Cochrane Database|PMID:",
    re.IGNORECASE,
)

_TITLE_TABLE_RE = re.compile(
    r"^(TT|STT)\b|^Tên hoạt chất\b|^Tên thuốc\b|^Hàm lượng\b|^Đơn vị\b|^Số lượng\b",
    re.IGNORECASE,
)


def normalize_heading(text: str) -> str:
    value = unicodedata.normalize("NFKC", text or "")
    value = re.sub(r"\s+", " ", value).strip().strip(":")
    value = re.sub(r"^(?:\d+(?:\.\d+)*[.)]?\s*)", "", value)
    return value.strip()


def _heading_variants() -> list[str]:
    return sorted(_MAJOR_HEADINGS, key=len, reverse=True)


def detect_plain_heading(line: str) -> str:
    candidate = normalize_heading(line)
    if not candidate or len(candidate) > 120:
        return ""
    for heading in _heading_variants():
        if re.fullmatch(rf"{re.escape(heading)}\s*:?", candidate, flags=re.IGNORECASE):
            return heading
    return ""


def detect_inline_heading(line: str) -> tuple[str, str]:
    candidate = re.sub(r"\s+", " ", (line or "")).strip()
    if not candidate or len(candidate) > 400:
        return "", ""
    for heading in _heading_variants():
        match = re.match(
            rf"^(?:\d+(?:\.\d+)*[.)]?\s*)?{re.escape(heading)}(?:\s*\d+)?(?:\s*[:.-]\s*|\s+)(.+)$",
            candidate,
            flags=re.IGNORECASE,
        )
        if not match:
            continue
        remainder = match.group(1).strip()
        if remainder:
            return heading, remainder
    return "", ""


def classify_section_title(title: str) -> str:
    heading = normalize_heading(title).lower()
    if not heading:
        return "body"
    if heading in {"tài liệu tham khảo", "references"}:
        return "references"
    if heading in {"tóm tắt", "abstract", "summary"}:
        return "abstract"
    if heading in {"đặt vấn đề", "giới thiệu", "mở đầu", "đại cương"}:
        return "introduction"
    if heading.startswith("đối tượng và phương pháp") or heading.startswith("vật liệu và phương pháp"):
        return "methods"
    if heading.startswith("kết quả"):
        return "results"
    if heading == "bàn luận":
        return "discussion"
    if heading in {"kết luận", "kiến nghị"}:
        return "conclusion"
    if heading in {"phụ lục", "appendix"}:
        return "appendix"
    if heading in {"chỉ định", "chống chỉ định", "chuẩn bị", "theo dõi", "theo dõi và xử trí tai biến", "các bước tiến hành", "tiến hành qtkt"}:
        return "procedure"
    return "body"


def infer_chunk_role(section_type: str) -> str:
    if section_type in {"abstract", "conclusion"}:
        return "high_signal"
    if section_type in {"results", "discussion"}:
        return "evidence"
    if section_type == "methods":
        return "methods"
    if section_type == "introduction":
        return "context"
    if section_type == "references":
        return "noise"
    if section_type == "appendix":
        return "appendix"
    if section_type == "procedure":
        return "instruction"
    return "body"


def reference_line_ratio(text: str) -> float:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    if not lines:
        return 0.0
    hits = sum(1 for line in lines if _REFERENCE_LINE_RE.search(line))
    return round(hits / len(lines), 3)


def table_line_ratio(text: str) -> float:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    if not lines:
        return 0.0
    hits = sum(1 for line in lines if _TABLE_LINE_RE.search(line))
    return round(hits / len(lines), 3)


def _is_bad_title(title: str) -> bool:
    value = normalize_heading(title).lower()
    return not value or value in _BAD_TITLES


def _merge_quality(existing: dict[str, Any], computed: dict[str, Any]) -> dict[str, Any]:
    existing_status = str(existing.get("quality_status") or "").lower()
    existing_score = existing.get("quality_score")
    existing_flags = existing.get("quality_flags") or []

    if existing_status in QUALITY_RANK:
        best_status = min(existing_status, computed["quality_status"], key=lambda item: QUALITY_RANK[item])
    else:
        best_status = computed["quality_status"]

    if isinstance(existing_score, (int, float)):
        best_score = int(min(existing_score, computed["quality_score"]))
    else:
        best_score = computed["quality_score"]

    merged_flags = sorted({*existing_flags, *computed["quality_flags"]})
    merged = dict(computed)
    merged["quality_status"] = best_status
    merged["quality_score"] = best_score
    merged["quality_flags"] = merged_flags
    return merged


def evaluate_document_quality(record: dict[str, Any]) -> dict[str, Any]:
    title = record.get("canonical_title") or record.get("title") or ""
    body = record.get("body") or ""
    section_count = int(record.get("_section_count") or 0)
    section_title = normalize_heading(record.get("section_title") or "")
    heading_path = str(record.get("heading_path") or "")
    normalized_title = normalize_heading(title)
    already_sectionized = bool(
        (section_title and section_title != normalized_title)
        or (" > " in heading_path)
    )

    score = 100
    flags: list[str] = []

    if _is_bad_title(title):
        score -= 45
        flags.append("title_missing_or_bad")
    else:
        if len(normalize_heading(title)) < 20:
            score -= 10
            flags.append("title_too_short")
        if _TITLE_REFERENCE_RE.search(title):
            score -= 30
            flags.append("title_looks_like_reference")
        if _TITLE_TABLE_RE.search(title):
            score -= 30
            flags.append("title_looks_like_table_header")

    body_len = len(body.strip())
    if body_len < 200:
        score -= 45
        flags.append("body_very_short")
    elif body_len < 500:
        score -= 20
        flags.append("body_too_short")

    ref_ratio = reference_line_ratio(body)
    table_ratio = table_line_ratio(body)
    if ref_ratio >= 0.55:
        score -= 40
        flags.append("reference_heavy_body")
    elif ref_ratio >= 0.25:
        score -= 20
        flags.append("reference_noise_detected")

    if table_ratio >= 0.65:
        score -= 20
        flags.append("table_heavy_body")

    if not (record.get("source_url") or "").strip():
        score -= 10
        flags.append("missing_source_url")
    if not (record.get("source_name") or "").strip():
        score -= 10
        flags.append("missing_source_name")
    if not (record.get("language") or "").strip():
        score -= 5
        flags.append("missing_language")
    if not record.get("doc_type"):
        score -= 5
        flags.append("missing_doc_type")

    if section_count == 0 and not already_sectionized:
        score -= 10
        flags.append("no_sections_detected")
    elif section_count == 1 and body_len > 4000 and not already_sectionized:
        score -= 10
        flags.append("single_large_section")

    score = max(0, int(score))
    if score >= 85:
        status = "go"
    elif score >= 70:
        status = "review"
    else:
        status = "hold"

    computed = {
        "quality_score": score,
        "quality_status": status,
        "quality_flags": sorted(set(flags)),
        "reference_line_ratio": ref_ratio,
        "table_line_ratio": table_ratio,
    }
    return _merge_quality(record, computed)


def passes_quality_gate(quality: dict[str, Any], min_quality_status: str = "hold") -> bool:
    min_status = (min_quality_status or "hold").lower()
    if min_status not in QUALITY_RANK:
        min_status = "hold"
    current = str(quality.get("quality_status") or "hold").lower()
    current_rank = QUALITY_RANK.get(current, QUALITY_RANK["hold"])
    return current_rank >= QUALITY_RANK[min_status]


def should_skip_chunk(section_type: str, text: str) -> bool:
    if section_type == "references":
        return True
    return reference_line_ratio(text) >= 0.7
