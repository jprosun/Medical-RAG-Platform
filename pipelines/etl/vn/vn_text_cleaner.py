"""
Vietnamese Text Cleaner
========================
Cleans raw text extracted from Vietnamese medical PDFs:
  - Unicode NFC normalization (critical for Vietnamese diacritics)
  - Line merge (fix mid-sentence line breaks from PDF extraction)
  - Noise removal (page numbers, repeated headers, digital signatures)
  - Whitespace normalization

Usage:
    from pipelines.etl.vn.vn_text_cleaner import clean
    cleaned = clean(raw_body)
"""

from __future__ import annotations

import re
import unicodedata


# ---------- Noise patterns ----------

# Digital signature lines (e.g. "ngoctlv.kcb_Truong Le Van Ngoc_29/10/2025 17:15:41")
_RE_DIGITAL_SIG = re.compile(
    r"^[a-zA-Z0-9_.]+_[A-Za-zÀ-ỹ\s]+_\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}$"
)

# Standalone page numbers (line is just a number, possibly with whitespace)
_RE_PAGE_NUMBER = re.compile(r"^\s*\d{1,5}\s*$")

# Repeated journal headers
_RE_JOURNAL_HEADERS = [
    re.compile(r"^TẠP CHÍ Y\s*.{0,60}(SỐ|TẬP|THÁNG).*$", re.IGNORECASE),
    re.compile(r"^HUE JOURNAL OF MEDICINE AND PHARMACY\s+ISSN.*$", re.IGNORECASE),
    re.compile(r"^VIETNAM MEDICAL JOURNAL.*$", re.IGNORECASE),
    re.compile(r"^vietnam medical journal.*$"),
    re.compile(r"^BÀI NGHIÊN CỨU\s*$", re.IGNORECASE),
]

# Government header noise (only at very beginning)
_RE_GOV_HEADER = re.compile(
    r"^(CỘNG HÒA XÃ HỘI CHỦ NGHĨA VIỆT NAM|Độc lập\s*-\s*Tự do\s*-\s*Hạnh phúc)\s*$"
)

# Lines that are just whitespace or non-breaking spaces
_RE_BLANK = re.compile(r"^\s*$")


# ---------- Heading detection (preserve, do NOT merge) ----------

_RE_HEADING_LIKE = re.compile(
    r"^("
    r"(I{1,3}V?|VI{0,3}|IX|X)\.\s+.+"           # Roman numeral headings
    r"|\d+\.\s+[A-ZÀ-Ỹ].{5,}"                    # Numbered headings (1. ABC...)
    r"|[A-ZÀ-Ỹ][A-ZÀ-Ỹ\s,()]{15,}"              # ALL-CAPS lines (likely headings)
    r"|TÓM TẮT|ABSTRACT|SUMMARY"
    r"|ĐẶT VẤN ĐỀ|GIỚI THIỆU|MỞ ĐẦU"
    r"|ĐỐI TƯỢNG VÀ PHƯƠNG PHÁP.*"
    r"|KẾT QUẢ.*|BÀN LUẬN|KẾT LUẬN|KIẾN NGHỊ"
    r"|TÀI LIỆU THAM KHẢO"
    r"|LỜI NÓI ĐẦU|MỤC LỤC|DANH MỤC.*"
    r"|ĐẠI CƯƠNG|CHỈ ĐỊNH|CHỐNG CHỈ ĐỊNH|CHUẨN BỊ"
    r"|CÁC BƯỚC TIẾN HÀNH|THEO DÕI|TAI BIẾN"
    r"|Chương\s+\d+"
    r")$",
    re.IGNORECASE,
)

# ---------- Strong sentence terminators ----------
_STRONG_TERMINATORS = set(".?!;:。）)")


def clean(raw_text: str) -> str:
    """Clean raw Vietnamese medical text.

    Args:
        raw_text: Text body after YAML frontmatter.

    Returns:
        Cleaned text with noise removed, lines merged, and Unicode normalized.
    """
    # Step 1: Unicode NFC normalize
    text = unicodedata.normalize("NFC", raw_text)

    # Step 2: Process line by line
    lines = text.splitlines()
    cleaned_lines: list[str] = []

    for line in lines:
        stripped = line.strip()

        # Remove digital signatures
        if _RE_DIGITAL_SIG.match(stripped):
            continue

        # Remove standalone page numbers
        if _RE_PAGE_NUMBER.match(stripped):
            continue

        # Remove repeated journal headers
        if any(pat.match(stripped) for pat in _RE_JOURNAL_HEADERS):
            continue

        # Remove government ceremony headers (CỘNG HÒA...)
        if _RE_GOV_HEADER.match(stripped):
            continue

        # Keep blank lines (but will collapse later)
        if _RE_BLANK.match(stripped):
            cleaned_lines.append("")
            continue

        cleaned_lines.append(stripped)

    # Step 3: Line merge — join lines that were broken mid-sentence
    merged: list[str] = []
    for line in cleaned_lines:
        if not line:
            merged.append("")
            continue

        if (
            merged
            and merged[-1]  # previous line is not blank
            and not _RE_HEADING_LIKE.match(line)  # current is not a heading
            and not _RE_HEADING_LIKE.match(merged[-1])  # prev is not heading
            and merged[-1][-1] not in _STRONG_TERMINATORS  # prev doesn't end sentence
            and not merged[-1].endswith(":")  # prev doesn't end with colon
            and len(line) > 5  # current line is non-trivial
        ):
            # Merge with previous line
            merged[-1] = merged[-1] + " " + line
        else:
            merged.append(line)

    # Step 4: Collapse multiple blank lines to max 1
    result: list[str] = []
    prev_blank = False
    for line in merged:
        if not line:
            if not prev_blank:
                result.append("")
            prev_blank = True
        else:
            result.append(line)
            prev_blank = False

    return "\n".join(result).strip()
