"""
Vietnamese Document Quality Scorer
=====================================
Computes a document-level quality score (0-100) for each record
after normalization. Used as a gate before ingest.

Score breakdown (total = 100):
  - Title Quality:        25 points
  - Metadata Completeness: 20 points
  - Body Cleanliness:      20 points
  - Sectionization:        15 points
  - Source Trust:           10 points
  - Language Confidence:    10 points

Semantic penalties (can push score below 0 in a category):
  - title_looks_like_reference: -20
  - title_looks_like_table_header: -20
  - too_many_sections_relative_to_length: -10
  - section_short_ratio_high: -10

Status:
  - >= 85: "go"
  - 70-84: "review"
  - < 70:  "hold"
"""

from __future__ import annotations

import re


_BAD_TITLES = {"pdf", "PDF", "", "Document", "document", "Untitled"}

# Filename-like patterns
_RE_FILENAME = re.compile(
    r"^[A-Za-z0-9_\-]+\.(pdf|txt|docx?)$", re.IGNORECASE
)

# --- Semantic title penalties ---

# Title looks like a numbered reference line: "2. WHO, Geneva (2020).", "3. Smith et al..."
# NOTE: "^\d+\.\s+" alone is TOO BROAD — it would flag procedure titles like "5. TRẮC NGHIỆM..."
# Must combine with citation markers to only catch actual references.
_RE_TITLE_REFERENCE = re.compile(
    r"^\d+\.\s+.*(?:et al|pp?\.\s*\d|\(\d{4}\)|doi:\s*10\.)"  # numbered + citation marker
    r"|^WHO\s*,"                    # starts with "WHO,"
    r"|^Quyết định số"             # starts with "Quyết định số"
    r"|Pan Afr Med J"               # citation journal name
    r"|(?<!\w)et al\."              # citation pattern (not part of a word)
    r"|pp?\.\s*\d+"                 # page reference "p. 123"
    r"|doi:\s*10\."                 # DOI reference
    r"|\(\d{4}\)\s*\."             # year citation "(2021)."
    r"|Luận [áa]n"                 # thesis citation "Luận án..."
    r"|Tạp chí Y"                  # journal name as title
)

# Title looks like a table header
_RE_TITLE_TABLE_HEADER = re.compile(
    r"^(TT|STT)\s"                 # starts with TT/STT (table number column)
    r"|^Đường dùng"                # "Đường dùng, dạng bào chế"
    r"|^Tên hoạt chất"             # drug table header
    r"|^Tên thuốc"                 # drug table header
    r"|^Hàm lượng"                 # dosage table header
    r"|^Số lượng"                  # quantity header
    r"|^Đơn vị"                    # unit header
    r"|,\s*$"                      # ends with comma (truncated header)
)

# Title has admin wrapper: "Về việc ban hành tài liệu..."
_RE_TITLE_ADMIN_WRAPPER = re.compile(
    r"^Về việc ban hành"
    r"|^Về việc phê duyệt"
    r"|^Về việc công bố"
    r"|^Số:\s*/[A-Z]"             # "Số: /QĐ-BYT"
)


def score(record: dict) -> dict:
    """Compute quality score for a normalized record.

    Args:
        record: Dict with keys like title, body, doc_type, specialty,
                audience, trust_tier, language, language_confidence,
                source_url, section_title, is_mixed_language.

    Returns:
        Dict with quality_score (int), quality_status (str),
        and quality_flags (list of str).
    """
    total = 0
    flags: list[str] = []

    # --- A. Title Quality (25) ---
    title = record.get("title", "")
    title_score = 25

    if not title or title.strip().lower() in {t.lower() for t in _BAD_TITLES}:
        title_score = 0
        flags.append("title_missing_or_bad")
    else:
        if len(title) < 20:
            title_score -= 10
            flags.append("title_too_short")
        if _RE_FILENAME.match(title):
            title_score -= 5
            flags.append("title_looks_like_filename")
        if title == title.upper() and len(title) > 100:
            title_score -= 5
            flags.append("title_all_caps")

        # --- NEW: Semantic title penalties ---
        if _RE_TITLE_REFERENCE.search(title):
            title_score -= 20
            flags.append("title_looks_like_reference")

        if _RE_TITLE_TABLE_HEADER.search(title):
            title_score -= 20
            flags.append("title_looks_like_table_header")

        if _RE_TITLE_ADMIN_WRAPPER.search(title):
            title_score -= 5
            flags.append("title_has_admin_wrapper")

        if title_score >= 20:
            flags.append("title_extracted")

    total += max(0, title_score)

    # --- B. Metadata Completeness (20) ---
    metadata_score = 20
    required_fields = ["doc_type", "specialty", "audience", "trust_tier", "language", "source_url"]
    for field in required_fields:
        val = record.get(field)
        if val is None or val == "" or val == 0:
            metadata_score -= 3
            flags.append(f"missing_{field}")

    total += max(0, metadata_score)

    # --- C. Body Cleanliness (20) ---
    body = record.get("body", "")
    body_score = 20

    if len(body) < 500:
        body_score -= 10
        flags.append("body_too_short")
    elif len(body) < 200:
        body_score -= 15
        flags.append("body_very_short")

    # Check for excessive noise (simple heuristic)
    if body:
        lines = body.splitlines()
        short_lines = sum(1 for l in lines if len(l.strip()) < 5 and l.strip())
        if lines and short_lines / max(1, len(lines)) > 0.3:
            body_score -= 5
            flags.append("body_noisy")

    total += max(0, body_score)

    # --- D. Sectionization Quality (15) ---
    section_count = record.get("_section_count", 1)
    section_score = 15

    if section_count == 0:
        section_score = 0
        flags.append("no_sections")
    elif section_count == 1:
        section_score -= 5
        flags.append("single_section")
    elif section_count > 20:
        section_score -= 5
        flags.append("too_many_sections")

    # NEW: sections-relative-to-body-length penalty
    if section_count > 5 and len(body) > 0:
        chars_per_section = len(body) / section_count
        if chars_per_section < 150:
            section_score -= 10
            flags.append("too_many_sections_relative_to_length")

    # NEW: short section ratio penalty
    section_bodies = record.get("_section_bodies", [])
    if section_bodies:
        short_sections = sum(1 for sb in section_bodies if len(sb) < 200)
        short_ratio = short_sections / max(1, len(section_bodies))
        if short_ratio > 0.4:
            section_score -= 10
            flags.append("section_short_ratio_high")

    total += max(0, section_score)

    # --- E. Source Trust (10) ---
    trust_tier = record.get("trust_tier", 2)
    if trust_tier == 1:
        trust_score = 10
    elif trust_tier == 2:
        trust_score = 7
    else:
        trust_score = 4

    total += trust_score

    # --- F. Language Confidence (10) ---
    lang_conf = record.get("language_confidence", 0.5)
    is_mixed = record.get("is_mixed_language", False)

    lang_score = round(lang_conf * 10)
    if is_mixed:
        lang_score = max(0, lang_score - 3)
        flags.append("mixed_language")

    total += min(10, max(0, lang_score))

    # --- Status ---
    if total >= 85:
        status = "go"
    elif total >= 70:
        status = "review"
    else:
        status = "hold"

    return {
        "quality_score": total,
        "quality_status": status,
        "quality_flags": flags,
    }
