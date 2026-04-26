"""
Vietnamese Title Extractor (v2)
=================================
Extracts real document titles from Vietnamese medical text files.
Uses source-specific logic because each source group has different
title placement patterns.

v2 fixes:
  - Journal: better reference/citation filtering
  - Pharma (dav_gov): extract from URL/filename, not table headers
  - Guideline (kcb_moh): extract quoted title core, flag admin wrapper
  - vmj_ojs: article boundary detection for multi-article files

Groups:
  A — Journal (vmj_ojs, hue_jmp_ojs, mil_med_pharm_journal,
               cantho_med_journal, trad_med_pharm_journal)
  B — Guideline (kcb_moh)
  C — Pharma (dav_gov)
  D — WHO (who_vietnam)
"""

from __future__ import annotations

import re
from pathlib import Path

# ---------- Constants ----------

_BAD_TITLES = {"pdf", "PDF", "", "Document", "document", "Untitled"}

_JOURNAL_SOURCES = {
    "vmj_ojs", "hue_jmp_ojs", "mil_med_pharm_journal",
    "cantho_med_journal", "trad_med_pharm_journal",
}

# Lines to skip when looking for journal titles
_SKIP_PATTERNS = [
    re.compile(r"^TẠP CHÍ\s*$", re.IGNORECASE),
    re.compile(r"^SỐ\s+\d+.*$", re.IGNORECASE),
    re.compile(r"^TẠP CHÍ Y\s*.{0,60}(SỐ|TẬP).*$", re.IGNORECASE),
    re.compile(r"^HUE JOURNAL.*ISSN.*$", re.IGNORECASE),
    re.compile(r"^VIETNAM MEDICAL JOURNAL.*$", re.IGNORECASE),
    re.compile(r"^vietnam medical journal.*$"),
    re.compile(r"^BÀI NGHIÊN CỨU\s*$", re.IGNORECASE),
    re.compile(r"^ISSN\s+\d+.*$"),
    re.compile(r"^DOI:.*$"),
    re.compile(r"^TẠP CHÍ SỐ\s+\d+.*$", re.IGNORECASE),
]

# Author line patterns
_RE_AUTHOR = re.compile(
    r"^[A-ZÀ-Ỹa-zà-ỹ\s.,]+\d+[*,]?\s*$"   # "Nguyễn Văn A1*, Trần B2"
    r"|^\d+[A-ZÀ-Ỹ]"                          # "1Trường Đại học..."
    r"|^[*]\s*Tác giả"                         # "*Tác giả liên hệ"
    r"|^Chịu trách nhiệm"
    r"|^Email:"
    r"|^Ngày nhận bài"
    r"|^Ngày phản biện"
    r"|^Ngày duyệt bài"
    r"|^Ngày chấp nhận"
    r"|^http://doi\.org"
    r"|^Tác giả liên hệ"
)

# Section headings (not titles)
_RE_SECTION_HEADING = re.compile(
    r"^(TÓM TẮT|ABSTRACT|SUMMARY|ĐẶT VẤN ĐỀ|ĐỐI TƯỢNG|KẾT QUẢ"
    r"|BÀN LUẬN|KẾT LUẬN|KIẾN NGHỊ|TÀI LIỆU THAM KHẢO"
    r"|V\.\s|IV\.\s|III\.\s|II\.\s|I\.\s"
    r"|QUYẾT ĐỊNH|DANH MỤC|PHỤ LỤC|MỤC LỤC|LỜI NÓI ĐẦU"
    r"|BỘ Y TẾ|BỘ TRƯỞNG)\s*",
    re.IGNORECASE,
)

# --- NEW: Negative title patterns ---

# Lines that look like references/citations (MUST NEVER be titles)
_RE_REFERENCE_LINE = re.compile(
    r"^\d+\.\s+[A-ZÀ-Ỹa-zà-ỹ].*(?:et al|pp?\.\s*\d|\(\d{4}\))"  # "1. Author et al (2020)"
    r"|Pan Afr Med J"
    r"|doi:\s*10\."
    r"|PMID:\s*\d+"
    r"|Lancet|NEJM|BMJ|PLoS"
    r"|J\s+[A-Z][a-z]+\s+[A-Z][a-z]+"                              # "J Clin Oncol"
    r"|Am\s+J\s+[A-Z]"                                               # "Am J Cardiol"
    r"|Int\s+J\s+[A-Z]"                                               # "Int J Med"
    r"|World\s+J\s+[A-Z]"                                             # "World J Gastroenterol"
    r"|Prev\s+Chronic\s+Dis"                                          # "Prev Chronic Dis"
    r"|N\s+Engl\s+J\s+Med"                                            # "N Engl J Med"
    r"|Cochrane\s+Database"                                           # "Cochrane Database"
    r"|ISSN\s+\d"                                                     # "ISSN 1234"
    r"|[A-Z][a-z]+\s+et\s+al"                                         # "Smith et al"
    r"|\(\d{4}\)\s*[.,]"                                              # "(2020)," or "(2020)."
    r"|https?://"                                                     # URLs
    r"|pp\.\s*\d+-\d+"                                                # "pp. 123-456"
)

# Lines that are table headers
_RE_TABLE_HEADER = re.compile(
    r"^TT\s"                        # "TT Tên hoạt chất"
    r"|^STT\s"                      # "STT Tên thuốc"
    r"|^Tên hoạt chất"
    r"|^Tên thuốc"
    r"|^Đường dùng"                 # "Đường dùng, dạng bào chế"
    r"|^Hàm lượng"
    r"|^Đơn vị"
    r"|^Số lượng"
    r"|^Nồng độ"
    r"|,\s*$"                       # ends with comma (truncated column)
)


def _is_bad_title(text: str) -> bool:
    """Check if extracted text is a bad/invalid title."""
    if not text or len(text.strip()) < 10:
        return True
    t = text.strip()
    if t.lower() in {x.lower() for x in _BAD_TITLES}:
        return True
    if _RE_REFERENCE_LINE.search(t):
        return True
    if _RE_TABLE_HEADER.match(t):
        return True
    return False


def extract(
    source_id: str,
    cleaned_body: str,
    yaml_title: str | None = None,
    file_url: str | None = None,
) -> str:
    """Extract the real title from a Vietnamese medical text file.

    Args:
        source_id: Source identifier (e.g. "vmj_ojs", "kcb_moh").
        cleaned_body: Cleaned text body (after vn_text_cleaner).
        yaml_title: Title from YAML frontmatter (may be "PDF" or blank).
        file_url: URL from YAML frontmatter for URL-based extraction.

    Returns:
        Extracted title string, or empty string if extraction fails.
    """
    # If YAML title is valid (not "PDF", not blank, not bad), use it
    if yaml_title and yaml_title.strip() not in _BAD_TITLES and len(yaml_title.strip()) >= 15:
        if not _is_bad_title(yaml_title):
            return yaml_title.strip()

    # Dispatch by source group
    if source_id in _JOURNAL_SOURCES:
        return _extract_journal_title(cleaned_body, source_id)
    elif source_id == "kcb_moh":
        return _extract_guideline_title(cleaned_body)
    elif source_id == "dav_gov":
        return _extract_pharma_title(cleaned_body, file_url)
    elif source_id == "who_vietnam":
        return _extract_who_title(cleaned_body)
    else:
        return _extract_generic_title(cleaned_body)


def _extract_journal_title(body: str, source_id: str) -> str:
    """Extract title from Vietnamese medical journal article.

    v3 Strategy:
      1. Skip noise lines (journal headers, ISSN, blank)
      2. For cantho_med: skip reference/citation lines at start (from previous article)
      3. Find article opening block: ALL CAPS Vietnamese title ≥20 chars
      4. Merge continuation lines (max 3 lines for multi-line titles)
      5. Validate extracted title is not a reference or table header
    """
    lines = body.splitlines()
    start_idx = 0

    # === cantho_med special handling: skip reference noise at start ===
    # Some cantho files start with tail-references of the previous article
    if source_id == "cantho_med_journal":
        # Skip up to 100 lines looking for article opening signal
        for i in range(min(100, len(lines))):
            stripped = lines[i].strip()
            if not stripped:
                continue
            # Journal header = valid start
            if any(pat.match(stripped) for pat in _SKIP_PATTERNS):
                start_idx = i
                break
            # ALL CAPS Vietnamese line ≥20 chars = likely title
            if (len(stripped) >= 20 and stripped == stripped.upper()
                    and not _RE_REFERENCE_LINE.search(stripped)
                    and any(c in stripped for c in 'ẮẰẲẴẶĂẤẦẨẪẬÂÉÈẺẼẸÊẾỀỂỄỆÍÌỈĨỊÓÒỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÚÙỦŨỤƯỨỪỬỮỰÝỲỶỸỴĐĐ')):
                start_idx = i
                break
            # Reference/citation line = skip it
            if _RE_REFERENCE_LINE.search(stripped):
                continue
            # English-heavy line (>80% ASCII) at start = likely reference tail
            ascii_ratio = sum(1 for c in stripped if ord(c) < 128) / max(1, len(stripped))
            if ascii_ratio > 0.8 and len(stripped) > 20:
                continue
            # If we see a meaningful Vietnamese line, start here
            if len(stripped) >= 20:
                start_idx = i
                break

    # Search for title
    candidate_lines: list[str] = []
    found_title = False

    for i in range(start_idx, min(start_idx + 80, len(lines))):
        stripped = lines[i].strip()

        if not stripped:
            if found_title:
                break
            continue

        # Skip known noise patterns
        if any(pat.match(stripped) for pat in _SKIP_PATTERNS):
            continue

        # Skip author lines
        if _RE_AUTHOR.match(stripped):
            if found_title:
                break
            continue

        # Skip section headings (short ones that aren't titles)
        if _RE_SECTION_HEADING.match(stripped) and len(stripped) < 30:
            if found_title:
                break
            continue

        # Skip reference/citation lines
        if _RE_REFERENCE_LINE.search(stripped):
            continue

        # Skip table headers
        if _RE_TABLE_HEADER.match(stripped):
            continue

        # Skip English-heavy lines at start (before title found)
        if not found_title:
            ascii_ratio = sum(1 for c in stripped if ord(c) < 128) / max(1, len(stripped))
            if ascii_ratio > 0.85 and len(stripped) > 30:
                continue

        # Check if line is plausible title (15-300 chars)
        if 15 <= len(stripped) <= 300:
            if not found_title:
                candidate_lines.append(stripped)
                found_title = True
            else:
                # Continuation line?
                if len(stripped) > 10 and not _RE_AUTHOR.match(stripped):
                    if len(candidate_lines) < 3:
                        candidate_lines.append(stripped)
                    else:
                        break
                else:
                    break
        elif found_title:
            break

    if candidate_lines:
        title = " ".join(candidate_lines)
        title = re.sub(r"\d{1,3}$", "", title).strip()
        if not _is_bad_title(title):
            return title

    return ""


def _extract_guideline_title(body: str) -> str:
    """Extract title from KCB/MOH guideline documents.

    v3 Strategy:
      1. Look for quoted title (\"...\") across multiple lines
      2. Look for content title after admin preamble (HƯỚNG DẪN QUY TRÌNH...)
      3. Fallback: look for \"Về việc...\" pattern and extract the object
    """
    lines = body.splitlines()

    # Strategy 1: Look for quoted title across first 100 lines
    # Concatenate first 100 lines to catch multiline quotes
    head_text = " ".join(l.strip() for l in lines[:100])
    # Find quoted content like "Hướng dẫn quy trình kỹ thuật...Tập 1.1"
    quoted = re.findall(r'\u201c([^\u201d]{15,})\u201d|"([^"]{15,})"', head_text)
    if quoted:
        # Take the longest quoted string
        candidates = [q[0] or q[1] for q in quoted if q[0] or q[1]]
        if candidates:
            best = max(candidates, key=len)
            return best.strip()

    # Also try simple quotes
    for line in lines[:100]:
        match = re.search(r'"([^"]{15,})"', line)
        if match:
            return match.group(1).strip()

    # Strategy 2: Look for content title headers
    for i, line in enumerate(lines[:200]):
        stripped = line.strip()
        if re.match(r'^HƯỚNG DẪN QUY TRÌNH', stripped, re.IGNORECASE):
            # Merge next lines for full title
            title_lines = [stripped]
            for j in range(i + 1, min(i + 4, len(lines))):
                next_l = lines[j].strip()
                if next_l and len(next_l) > 5 and not next_l.startswith('('):
                    title_lines.append(next_l)
                else:
                    break
            return " ".join(title_lines)

    # Strategy 3: "Về việc..." → extract the subject
    for i, line in enumerate(lines[:50]):
        stripped = line.strip()
        if stripped.startswith("Về việc"):
            inner = re.search(r'"([^"]{10,})"', stripped)
            if inner:
                return inner.group(1).strip()
            title = stripped
            if i + 1 < len(lines) and lines[i + 1].strip():
                next_stripped = lines[i + 1].strip()
                if not next_stripped.startswith("BỘ TRƯỞNG") and len(next_stripped) > 10:
                    title += " " + next_stripped
            return title

    return _extract_generic_title(body)


def _extract_pharma_title(body: str, file_url: str | None = None) -> str:
    """Extract title from dav.gov pharmaceutical documents.

    v3 Strategy:
      1. Extract from file URL + humanize (proper Vietnamese capitalization)
      2. Look for descriptive lines (not table headers)
      3. NEVER use table column headers as titles
    """
    # Common VN word fixes for URL slugs
    _URL_WORD_MAP = {
        'dm': 'danh mục', 'thuoc': 'thuốc', 'dieu': 'điều',
        'tri': 'trị', 'benh': 'bệnh', 'hiem': 'hiếm',
        'phu': 'phụ', 'luc': 'lục', 'nguyen': 'nguyên',
        'tac': 'tắc', 'co': 'cơ', 'ban': 'bản', 'gmp': 'GMP',
        'who': 'WHO', 'pics': 'PICS', 'eu': 'EU',
        'san': 'sản', 'pham': 'phẩm', 'mau': 'mẫu',
        'duoc': 'dược', 'lieu': 'liệu', 'truyen': 'truyền',
        'vi': 'vị', 'ho': 'hồ', 'so': 'sơ', 'tong': 'tổng',
        'the': 'thể', 'phan': 'phân', 'loai': 'loại',
        'ton': 'tồn', 'tai': 'tại', 'bieu': 'biểu',
        'van': 'văn', 'viii': 'VIII', 'vii': 'VII',
        'ix': 'IX', 'iv': 'IV', 'iii': 'III', 'ii': 'II',
        'i': 'I', 'x': 'X', 'v': 'V',
    }

    if file_url:
        fname = file_url.rsplit("/", 1)[-1] if "/" in file_url else file_url
        fname = re.sub(r"_\d{8,}\.pdf$", "", fname, flags=re.IGNORECASE)
        fname = re.sub(r"\.pdf(\.txt)?$", "", fname, flags=re.IGNORECASE)
        fname = re.sub(r"^\d+[-_]?", "", fname)
        # Split by hyphens/underscores and map to proper Vietnamese
        words = re.split(r'[-_]+', fname)
        polished_words = []
        for w in words:
            w_lower = w.lower().strip()
            if w_lower in _URL_WORD_MAP:
                polished_words.append(_URL_WORD_MAP[w_lower])
            elif w_lower:
                polished_words.append(w_lower)
        title = " ".join(polished_words).strip()
        if title:
            title = title[0].upper() + title[1:]
            # Clean up double spaces
            title = re.sub(r'\s+', ' ', title)
            return title

    # Strategy 2: Find non-table-header descriptive line
    lines = body.splitlines()
    for line in lines[:40]:
        stripped = line.strip()
        if not stripped or len(stripped) < 15:
            continue
        if _RE_TABLE_HEADER.match(stripped):
            continue
        if stripped.endswith(","):
            continue
        if re.match(r"^\d+\s+[A-Z]", stripped):
            continue
        if "chỉ định" in stripped.lower() or "điều trị" in stripped.lower() or "danh mục" in stripped.lower():
            return stripped[:200]
        if len(stripped) >= 30:
            return stripped[:200]

    return ""


def _extract_who_title(body: str) -> str:
    """Extract title from WHO Vietnam documents."""
    return _extract_generic_title(body)


def _extract_generic_title(body: str) -> str:
    """Fallback: extract first meaningful long line as title."""
    lines = body.splitlines()

    for line in lines[:40]:
        stripped = line.strip()
        if not stripped or len(stripped) < 20:
            continue
        if any(pat.match(stripped) for pat in _SKIP_PATTERNS):
            continue
        if _RE_REFERENCE_LINE.search(stripped):
            continue
        if _RE_TABLE_HEADER.match(stripped):
            continue
        return stripped[:300]

    return ""
