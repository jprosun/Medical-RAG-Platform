"""
Vietnamese Document Sectionizer v3
=====================================
Source-aware parent-record-first strategy.
Each source type gets its own mode determining the unit of record.

Modes:
  A — publication_mode (who_vietnam): 1 doc = 1 record, split only if >6000 chars
  B — article_mode (journals): keep major sections only (6 max), no sub-headings
  C — procedure_mode (kcb_moh): strip admin/TOC, 1 procedure = 1 record
  D — table_entry_mode (dav_gov): parse table rows into structured records
  E — generic_mode: fallback, similar to v2
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class Section:
    section_title: str
    heading_path: str
    body: str


# ---------- Source → mode mapping ----------

_SOURCE_MODES = {
    "who_vietnam": "publication",
    "kcb_moh": "procedure",
    "dav_gov": "table_entry",
    "hue_jmp_ojs": "article",
    "mil_med_pharm_journal": "article",
    "trad_med_pharm_journal": "article",
    "cantho_med_journal": "article",
    "vmj_ojs": "article",  # until boundary splitter exists
}

# ---------- Shared patterns ----------

_DROP_SECTIONS = {"TÀI LIỆU THAM KHẢO", "REFERENCES"}

_RE_TOC_LINE = re.compile(r"\.{3,}")  # lines with "....." page dots

_RE_ADMIN_PREAMBLE_END = re.compile(
    r"^(QUYẾT ĐỊNH:|Điều \d+\.|KT\. BỘ TRƯỞNG|Nơi nhận:)", re.IGNORECASE
)

# Major article headings only — no sub-headings
_MAJOR_ARTICLE_HEADINGS = re.compile(
    r"^(TÓM TẮT|ABSTRACT|SUMMARY"
    r"|ĐẶT VẤN ĐỀ|GIỚI THIỆU|MỞ ĐẦU"
    r"|ĐỐI TƯỢNG VÀ PHƯƠNG PHÁP.*"
    r"|VẬT LIỆU VÀ PHƯƠNG PHÁP.*"
    r"|KẾT QUẢ.*"
    r"|BÀN LUẬN"
    r"|KẾT LUẬN"
    r"|KIẾN NGHỊ"
    r"|TÀI LIỆU THAM KHẢO|REFERENCES)\s*\d*$",
    re.IGNORECASE,
)

# Procedure start marker for KCB
_RE_PROCEDURE_START = re.compile(
    r"^(?:QUY TRÌNH KỸ THUẬT|HƯỚNG DẪN QUY TRÌNH)\s*\d*\s*$"
    r"|^[A-ZÀ-Ỹ][A-ZÀ-Ỹ\s,()–\-]{15,}$",
)

_RE_KCB_PROCEDURE_HEADING = re.compile(
    r"^(ĐẠI CƯƠNG|CHỈ ĐỊNH|CHỐNG CHỈ ĐỊNH|CHUẨN BỊ"
    r"|CÁC BƯỚC TIẾN HÀNH|THEO DÕI|TAI BIẾN.*)\s*$",
    re.IGNORECASE,
)


def sectionize(title: str, body: str, source_id: str = "") -> list[Section]:
    """Split document into sections using source-aware mode.

    Args:
        title: Document title.
        body: Cleaned body text.
        source_id: Source identifier for mode selection.

    Returns:
        List of Section objects.
    """
    mode = _SOURCE_MODES.get(source_id, "generic")

    if mode == "publication":
        return _sectionize_publication(title, body)
    elif mode == "article":
        return _sectionize_article(title, body)
    elif mode == "procedure":
        return _sectionize_procedure(title, body)
    elif mode == "table_entry":
        return _sectionize_table_entry(title, body)
    else:
        return _sectionize_generic(title, body)


# ============================================================
# Mode A: Publication (WHO Vietnam)
# ============================================================

def _sectionize_publication(title: str, body: str) -> list[Section]:
    """1 document = 1 record. Only split if body > 6000 chars."""
    body = _strip_references(body)

    if len(body) <= 6000:
        return [Section(section_title=title, heading_path=title, body=body.strip())]

    # Split into chunks of ~5000 chars at paragraph boundaries
    sections = []
    paragraphs = body.split("\n\n")
    current_chunk: list[str] = []
    current_len = 0

    for para in paragraphs:
        if current_len + len(para) > 5000 and current_chunk:
            chunk_body = "\n\n".join(current_chunk).strip()
            idx = len(sections) + 1
            sections.append(Section(
                section_title=f"{title} (phần {idx})",
                heading_path=f"{title} > phần {idx}",
                body=chunk_body,
            ))
            current_chunk = [para]
            current_len = len(para)
        else:
            current_chunk.append(para)
            current_len += len(para)

    if current_chunk:
        chunk_body = "\n\n".join(current_chunk).strip()
        if len(sections) == 0:
            sections.append(Section(section_title=title, heading_path=title, body=chunk_body))
        else:
            idx = len(sections) + 1
            sections.append(Section(
                section_title=f"{title} (phần {idx})",
                heading_path=f"{title} > phần {idx}",
                body=chunk_body,
            ))

    return sections if sections else [Section(section_title=title, heading_path=title, body=body.strip())]


# ============================================================
# Mode B: Article (Journals)
# ============================================================

def _sectionize_article(title: str, body: str) -> list[Section]:
    """Split by major headings only (TÓM TẮT, KẾT QUẢ, etc). No sub-headings."""
    body = _strip_references(body)
    lines = body.splitlines()
    sections: list[Section] = []

    current_heading = ""
    current_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        match = _MAJOR_ARTICLE_HEADINGS.match(stripped)

        if match:
            # Save previous section
            if current_lines:
                section_body = "\n".join(current_lines).strip()
                if len(section_body) >= 100:
                    sections.append(Section(
                        section_title=current_heading or title,
                        heading_path=f"{title} > {current_heading}" if current_heading else title,
                        body=section_body,
                    ))
            current_heading = match.group(1).strip()
            current_lines = []
        else:
            current_lines.append(line)

    # Save last section
    if current_lines:
        section_body = "\n".join(current_lines).strip()
        if len(section_body) >= 100:
            sections.append(Section(
                section_title=current_heading or title,
                heading_path=f"{title} > {current_heading}" if current_heading else title,
                body=section_body,
            ))

    # Drop reference sections
    sections = [s for s in sections if s.section_title.upper() not in _DROP_SECTIONS]

    # Merge very short sections with next
    merged: list[Section] = []
    for sec in sections:
        if merged and len(merged[-1].body) < 300:
            # Merge with previous
            merged[-1] = Section(
                section_title=merged[-1].section_title,
                heading_path=merged[-1].heading_path,
                body=merged[-1].body + "\n\n" + sec.body,
            )
        else:
            merged.append(sec)

    if not merged:
        return [Section(section_title=title, heading_path=title, body=body.strip())]

    return merged


# ============================================================
# Mode C: Procedure (KCB/MOH) — v4 rewrite
# ============================================================

# Procedure anchor headings used for 2-step boundary validation
# Includes both standard (Hô hấp/Tâm thần) and Vi sinh anchors
_PROCEDURE_ANCHORS = {
    # Standard anchors (Hô hấp, Tâm thần)
    "ĐẠI CƯƠNG", "CHỈ ĐỊNH", "CHỐNG CHỈ ĐỊNH", "THẬN TRỌNG",
    "CHUẨN BỊ", "CÁC BƯỚC TIẾN HÀNH", "TIẾN HÀNH QTKT",
    "THEO DÕI VÀ XỬ TRÍ TAI BIẾN",
    # Vi sinh-specific anchors
    "AN TOÀN", "NHỮNG SAI SÓT VÀ XỬ TRÍ",
    "TIÊU CHUẨN ĐÁNH GIÁ VÀ ĐẢM BẢO CHẤT LƯỢNG",
}

# Sub-section names that are NEVER procedure titles (even if they match line pattern)
_SUBSECTION_NAMES = _PROCEDURE_ANCHORS | {
    "TIẾN HÀNH", "THEO DÕI", "TAI BIẾN",
    "TÀI LIỆU THAM KHẢO", "REFERENCES",
    "NHỮNG SAI SÓT", "TIÊU CHUẨN ĐÁNH GIÁ",
    "MỤC ĐÍCH", "NGUYÊN LÝ",
}

# Pattern: "1. ĐẠI CƯƠNG" or "2. CHUẨN BỊ" or "3. AN TOÀN" etc. (numbered anchor)
_RE_NUMBERED_ANCHOR = re.compile(
    r"^\d+\.\s*(ĐẠI CƯƠNG|CHỈ ĐỊNH|CHỐNG CHỈ ĐỊNH|THẬN TRỌNG"
    r"|CHUẨN BỊ|CÁC BƯỚC TIẾN HÀNH|TIẾN HÀNH QTKT"
    r"|THEO DÕI VÀ XỬ TRÍ TAI BIẾN"
    r"|AN TOÀN|NHỮNG SAI SÓT VÀ XỬ TRÍ"
    r"|TIÊU CHUẨN ĐÁNH GIÁ VÀ ĐẢM BẢO CHẤT LƯỢNG"
    r"|MỤC ĐÍCH)\s*$",
    re.IGNORECASE,
)

# Pattern: "N. text" (numbered line)
_RE_NUMBERED_LINE = re.compile(r"^(\d{1,3})\.\s+(.+)$")

# Admin noise lines to always skip
_ADMIN_NOISE = re.compile(
    r"^(BỘ Y TẾ|CỘNG HÒA|QUYẾT ĐỊNH|KT\. BỘ TRƯỞNG|Nơi nhận"
    r"|THỨ TRƯỞNG|Điều \d+|Căn cứ|Theo đề nghị|Số:|Hà Nội"
    r"|HƯỚNG DẪN QUY TRÌNH|VỀ HÔ HẤP|NGUYÊN TẮC XÂY DỰNG"
    r"|Chỉ đạo biên soạn|Chủ biên|Tham gia biên soạn"
    r"|GS\.TS\.|PGS\.TS\.|TS\.|ThS\.|BSCKII\.|BS\."
    r"|Phó Cục trưởng|Cục trưởng|Giám đốc|Phó Giám đốc"
    r"|Trưởng khoa|STT\s|Mã liên|Tên kỹ thuật)",
    re.IGNORECASE,
)


def _is_procedure_boundary(lines: list[str], idx: int) -> tuple[bool, str]:
    """Check if line at idx is a procedure boundary using 2-step validation.
    
    Step 1: Line matches "N. PROCEDURE NAME" pattern (numbered + name ≥15 chars)
            Name must NOT be a known sub-section heading.
    Step 2: Within next 12 lines, at least 2 anchor headings present.
    
    This works for both ALL-CAPS (Hô hấp/Tâm thần) and mixed-case (Vi sinh) titles.
    
    Returns:
        (is_boundary, procedure_title)
    """
    stripped = lines[idx].strip()
    
    # Step 1: Check if this looks like a procedure title
    match = _RE_NUMBERED_LINE.match(stripped)
    if not match:
        return False, ""
    
    proc_num = match.group(1)
    proc_name = match.group(2).strip()
    
    # Reject if this IS a known sub-section heading
    name_upper = proc_name.upper().strip()
    # Exact match or startswith for sub-section names
    for sub in _SUBSECTION_NAMES:
        if name_upper == sub or name_upper.startswith(sub):
            return False, ""
    
    # Reject very short names (likely sub-headings)
    if len(proc_name) < 15:
        return False, ""
    
    # Step 2: Look ahead for anchor headings
    anchor_count = 0
    for j in range(idx + 1, min(idx + 12, len(lines))):
        ahead = lines[j].strip()
        if _RE_NUMBERED_ANCHOR.match(ahead):
            anchor_count += 1
        # Also match plain anchor text
        for anchor in _PROCEDURE_ANCHORS:
            if ahead.upper() == anchor or ahead.upper().startswith(anchor):
                anchor_count += 1
                break
        if anchor_count >= 2:
            return True, f"{proc_num}. {proc_name}"
    
    return False, ""


def _sectionize_procedure(title: str, body: str) -> list[Section]:
    """Split KCB/MOH document into procedures using 2-step boundary validation.
    
    v4 strategy (per user review):
      1. Filter out admin preamble, TOC, signatures, page numbers
      2. Find procedure boundaries: numbered title + ≥2 anchor headings ahead
      3. Each procedure = 1 record with all sub-sections merged as body
      4. Strip TÀI LIỆU THAM KHẢO from each procedure
      5. Don't merge short procedures — only drop true fragments
    """
    lines = body.splitlines()

    # Step 1: Filter noise lines
    filtered: list[str] = []
    for line in lines:
        stripped = line.strip()
        # Skip TOC lines (with page dots)
        if _RE_TOC_LINE.search(stripped):
            continue
        # Skip standalone page numbers
        if re.match(r"^\d{1,4}\s*$", stripped):
            continue
        # Skip digital signatures
        if "ngoctlv.kcb" in stripped or "Ký bởi" in stripped:
            continue
        filtered.append(line)

    # Step 2: Find all procedure boundaries
    boundaries: list[tuple[int, str]] = []
    for i in range(len(filtered)):
        is_boundary, proc_title = _is_procedure_boundary(filtered, i)
        if is_boundary:
            boundaries.append((i, proc_title))

    # Step 3: Split into procedure blocks
    sections: list[Section] = []
    for b_idx, (start, proc_title) in enumerate(boundaries):
        # End = next boundary or EOF
        end = boundaries[b_idx + 1][0] if b_idx + 1 < len(boundaries) else len(filtered)
        
        proc_lines = filtered[start:end]
        proc_body = "\n".join(proc_lines).strip()
        
        # Strip TÀI LIỆU THAM KHẢO at end of this procedure
        proc_body = _strip_references(proc_body)
        
        # Strip admin noise lines if present in procedure body
        clean_lines = []
        for pl in proc_body.splitlines():
            ps = pl.strip()
            if _ADMIN_NOISE.match(ps):
                continue
            clean_lines.append(pl)
        proc_body = "\n".join(clean_lines).strip()
        
        # Only keep if body is substantial
        if len(proc_body) >= 200:
            # Use Title Case for procedure title
            display_title = proc_title.strip()
            sections.append(Section(
                section_title=display_title,
                heading_path=f"{title} > {display_title}",
                body=proc_body,
            ))

    # If no procedures found, return whole document as 1 section
    if not sections:
        clean_body = "\n".join(filtered).strip()
        clean_body = _strip_references(clean_body)
        if clean_body and len(clean_body) >= 200:
            return [Section(section_title=title, heading_path=title, body=clean_body)]
        return []

    return sections


# ============================================================
# Mode D: Table Entry (DAV gov)
# ============================================================

def _sectionize_table_entry(title: str, body: str) -> list[Section]:
    """Parse table data into individual drug/entry records.
    
    Each numbered entry (1, 2, 3...) = 1 record with structured body.
    """
    lines = body.splitlines()
    entries: list[dict] = []

    # Detect table entries: lines starting with a number followed by drug name
    current_entry: dict | None = None
    buffer: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Skip table header lines
        if stripped in ("TT", "STT") or stripped.startswith("Tên hoạt chất"):
            continue
        if stripped.startswith("Đường dùng") or stripped.startswith("Chỉ định phòng"):
            continue

        # New entry: starts with number + space + drug name
        match = re.match(r"^(\d+)\s+(.+)$", stripped)
        if match and len(match.group(2)) >= 3:
            entry_num = int(match.group(1))
            # Heuristic: if number is reasonable sequence, it's an entry
            expected = len(entries) + 1
            if abs(entry_num - expected) <= 2 or entry_num == expected:
                # Save previous entry
                if current_entry and buffer:
                    current_entry["extra_lines"] = buffer
                    entries.append(current_entry)
                current_entry = {
                    "num": entry_num,
                    "name": match.group(2).strip(),
                }
                buffer = []
                continue

        # Continuation lines for current entry
        if current_entry is not None:
            buffer.append(stripped)

    # Save last entry
    if current_entry and buffer:
        current_entry["extra_lines"] = buffer
        entries.append(current_entry)

    # Convert entries to sections
    sections: list[Section] = []
    for entry in entries:
        name = entry["name"]
        extra = " ".join(entry.get("extra_lines", []))

        # Build structured body
        body_parts = [f"Hoạt chất: {name}"]
        # Try to extract route and indication from extra lines
        route_match = re.search(r"(Tiêm|Uống|Dùng ngoài|Nhỏ mắt|Hít):?\s*(.*?)(?:Điều trị|Chỉ định|$)", extra)
        if route_match:
            body_parts.append(f"Đường dùng: {route_match.group(1)} {route_match.group(2).strip()}")

        indication_match = re.search(r"(Điều trị|Chỉ định)\s+(.+?)(?:\.\s*(?:Điều trị|Chỉ định)|$)", extra, re.DOTALL)
        if indication_match:
            body_parts.append(f"Chỉ định: {indication_match.group(1)} {indication_match.group(2).strip()}")
        elif extra:
            body_parts.append(f"Thông tin: {extra[:500]}")

        entry_body = "\n".join(body_parts)

        if len(entry_body) >= 30:
            sections.append(Section(
                section_title=f"{title} - {name}",
                heading_path=f"{title} > {name}",
                body=entry_body,
            ))

    # If no entries found, fall back to single section
    if not sections:
        return [Section(section_title=title, heading_path=title, body=body.strip())]

    return sections


# ============================================================
# Mode E: Generic fallback
# ============================================================

def _sectionize_generic(title: str, body: str) -> list[Section]:
    """Fallback: keep entire body as 1 section, split only if very long."""
    body = _strip_references(body)

    if len(body) <= 8000:
        return [Section(section_title=title, heading_path=title, body=body.strip())]

    # Split at paragraph boundaries
    return _sectionize_publication(title, body)


# ============================================================
# Helpers
# ============================================================

def _strip_references(body: str) -> str:
    """Remove TÀI LIỆU THAM KHẢO section from end of body."""
    # Find last occurrence of reference section header
    patterns = [
        r"\n\s*TÀI LIỆU THAM KHẢO\s*\n",
        r"\n\s*REFERENCES\s*\n",
    ]
    for pat in patterns:
        match = list(re.finditer(pat, body, re.IGNORECASE))
        if match:
            # Cut from last match
            body = body[:match[-1].start()]
            break
    return body
