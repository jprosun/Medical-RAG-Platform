"""
Chunk Quality Filter — Phase 4
================================
Pre-filters raw Qdrant chunks before article aggregation.

Two-tier approach:
  - Hard reject: bibliography fragments, DOI lines, citation-only text
  - Soft penalty: digit-heavy, punctuation-dense, low-information chunks

Usage:
    from .chunk_quality_filter import filter_chunks
    clean_chunks = filter_chunks(raw_chunks, min_quality=0.4)
"""

from __future__ import annotations

import re
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from .retriever import RetrievedChunk


# ── Hard reject patterns ────────────────────────────────────────────

# Bibliography / reference section markers
_REFERENCE_MARKERS = re.compile(
    r'(?:^|\n)\s*(?:'
    r'tài\s*liệu\s*tham\s*khảo'
    r'|references'
    r'|bibliography'
    r'|danh\s*mục\s*tài\s*liệu'
    r')\s*(?:$|:|\n)',
    re.IGNORECASE,
)

# Citation line: "2019;156(7):1951-1968" or "2021;9:e026813"
_CITATION_LINE = re.compile(
    r'\d{4}\s*;\s*\d+\s*(?:\(\d+\))?\s*:\s*(?:e?\d+|[A-Z]\d+)',
)

# DOI pattern
_DOI_PATTERN = re.compile(r'10\.\d{4,}/')

# Volume(issue):page dense pattern — multiple occurrences = bibliography block  
_BIBLIO_DENSE = re.compile(
    r'(?:\d{4}\s*;\s*\d+\s*(?:\(\d+\))?\s*:\s*\d+)',
)

# Journal name patterns commonly found in reference lists
_JOURNAL_REF = re.compile(
    r'(?:J\s+\w+\s+\w+|Lancet|JAMA|BMJ|N\s+Engl\s+J\s+Med'
    r'|Ann\s+\w+|Clin\s+\w+|Am\s+J|Eur\s+J'
    r'|PLoS\s+\w+|Cochrane|Gastroenterology'
    r'|Hepatology|Circulation|Chest)\s*[.,;]?\s*\d{4}',
    re.IGNORECASE,
)

_JUNK_SECTION_MARKERS = re.compile(
    r'(?:tài\s*liệu\s*tham\s*khảo|references|bibliography|phụ\s*lục|appendix|acknowledg(?:e)?ments?)',
    re.IGNORECASE,
)


def is_junk_chunk(text: str) -> bool:
    """
    Hard reject: returns True if chunk is clearly garbage.
    
    Conservative approach — only reject when very confident.
    """
    if not text or not text.strip():
        return True
    
    t = text.strip()
    
    # 1. Too short to be useful (< 30 chars with no sentence structure)
    if len(t) < 30 and '.' not in t:
        return True
    
    # 2. Reference section marker
    if _REFERENCE_MARKERS.search(t):
        # Only reject if it's primarily a reference block
        # (marker near start or chunk is short)
        marker_match = _REFERENCE_MARKERS.search(t)
        if marker_match and (marker_match.start() < 50 or len(t) < 200):
            return True
    
    # 3. Dense bibliography block (3+ citation lines in a short chunk)
    citation_count = len(_BIBLIO_DENSE.findall(t))
    if citation_count >= 3 and len(t) < 500:
        return True
    
    # 4. DOI-dominated text
    doi_count = len(_DOI_PATTERN.findall(t))
    if doi_count >= 2:
        return True
    
    # 5. Predominantly citation/journal references
    journal_count = len(_JOURNAL_REF.findall(t))
    if journal_count >= 2 and citation_count >= 2:
        return True
    
    # 6. Extremely high punctuation ratio (> 40%)
    if len(t) > 10:
        punct_chars = sum(1 for c in t if c in '.,;:()[]{}!?/-–—"\'')
        if punct_chars / len(t) > 0.40:
            return True
    
    return False


def chunk_quality_score(text: str) -> float:
    """
    Soft penalty scoring: returns 0.0–1.0.
    
    1.0 = high quality study/clinical content
    0.0 = garbage
    
    Starts at 1.0 and applies penalties.
    """
    if not text or not text.strip():
        return 0.0
    
    t = text.strip()
    score = 1.0
    
    # ── Digit ratio ──────────────────────────────────────────────
    if len(t) > 20:
        digit_count = sum(1 for c in t if c.isdigit())
        digit_ratio = digit_count / len(t)
        if digit_ratio > 0.35:
            score -= 0.20
        elif digit_ratio > 0.25:
            score -= 0.10
    
    # ── Punctuation density ──────────────────────────────────────
    if len(t) > 20:
        punct_count = sum(1 for c in t if c in '.,;:()[]{}!?/-–—"\'')
        punct_ratio = punct_count / len(t)
        if punct_ratio > 0.30:
            score -= 0.15
        elif punct_ratio > 0.20:
            score -= 0.05
    
    # ── Citation fragments present ───────────────────────────────
    citation_count = len(_CITATION_LINE.findall(t))
    if citation_count >= 1:
        score -= 0.10 * min(citation_count, 3)
    
    doi_count = len(_DOI_PATTERN.findall(t))
    if doi_count >= 1:
        score -= 0.10 * min(doi_count, 2)
    
    # ── Information density (unique words / total words) ─────────
    words = t.lower().split()
    if len(words) > 5:
        unique_ratio = len(set(words)) / len(words)
        if unique_ratio < 0.3:
            score -= 0.10
    
    # ── No complete sentence (lacks period) ──────────────────────
    if '.' not in t and len(t) > 50:
        score -= 0.10
    
    # ── Very short text ──────────────────────────────────────────
    if len(t) < 80:
        score -= 0.10
    
    # ── Domain term bonus (reward medical content) ───────────────
    _DOMAIN_TERMS = {
        'bệnh', 'điều trị', 'chẩn đoán', 'triệu chứng', 'phẫu thuật',
        'lâm sàng', 'xét nghiệm', 'thuốc', 'liều', 'viêm', 'nhiễm',
        'ung thư', 'tim', 'thận', 'gan', 'phổi', 'máu', 'tế bào',
        'miễn dịch', 'kháng sinh', 'enzyme', 'protein', 'gen',
        'tỷ lệ', 'nghiên cứu', 'bệnh nhân', 'kết quả', 'phương pháp',
        'patient', 'treatment', 'diagnosis', 'clinical', 'study',
        'disease', 'infection', 'cancer', 'therapy', 'outcome',
    }
    words_lower = set(t.lower().split())
    domain_hits = len(words_lower & _DOMAIN_TERMS)
    if domain_hits >= 3:
        score += 0.05  # small bonus, don't over-reward
    elif domain_hits == 0 and len(words) > 10:
        score -= 0.10  # no medical content at all
    
    return max(0.0, min(1.0, score))


def filter_chunks(
    chunks: "List[RetrievedChunk]",
    min_quality: float = 0.4,
) -> "List[RetrievedChunk]":
    """
    Filter out junk chunks and apply quality-based score adjustment.
    
    Phase 4 pipeline position:
        Raw Qdrant Results → **Chunk Quality Filter** → Clean Candidate Pool → Article Aggregator
    
    Args:
        chunks: Raw chunks from retriever
        min_quality: Minimum quality score to keep (0.0–1.0)
    
    Returns:
        Filtered list of chunks, sorted by quality-adjusted retrieval score
    """
    clean = []
    
    for chunk in chunks:
        # Hard reject on text
        if is_junk_chunk(chunk.text):
            continue

        section_title = str(chunk.metadata.get("section_title", "") or "")
        heading_path = str(chunk.metadata.get("heading_path", "") or "")
        if _JUNK_SECTION_MARKERS.search(section_title) or _JUNK_SECTION_MARKERS.search(heading_path):
            continue
        
        # Hard reject on title metadata
        title = chunk.metadata.get("title", "") or ""
        if _is_junk_title(title):
            continue
        
        # Soft penalty
        quality = chunk_quality_score(chunk.text)
        
        # Title quality penalty
        if _is_weak_title(title):
            quality -= 0.15
        
        if quality < min_quality:
            continue
        
        # Adjust retrieval score by quality factor
        # This ensures high-quality chunks rank higher even if cosine is slightly lower
        chunk.score = chunk.score * (0.7 + 0.3 * quality)
        clean.append(chunk)
    
    # Re-sort by adjusted score
    clean.sort(key=lambda c: -c.score)
    
    return clean


# ── Title quality checks ────────────────────────────────────────────

# Placeholder titles
_PLACEHOLDER_TITLES = re.compile(
    r'(?:^tên bài viết$|^untitled$|^no title$|^\.+$)',
    re.IGNORECASE,
)

# Title that is actually a citation line
_CITATION_TITLE = re.compile(
    r'^\d{4}[\s.;]+\d+\s*\(\d+\)',
)

_DENSE_CITATION_TITLE = re.compile(
    r'^\d{4}[\s.;:]+\d+(?:\(\d+\))?[:;]\s*[A-Za-z]?\d+',
)

_LOWERCASE_FRAGMENT_TITLE = re.compile(
    r'^[a-zà-ỹ].{0,80}$',
)


def _is_junk_title(title: str) -> bool:
    """Hard reject titles that are clearly garbage."""
    t = title.strip()
    if not t:
        return False  # missing title is OK, not junk
    
    # Placeholder
    if _PLACEHOLDER_TITLES.match(t):
        return True
    
    # Title is a citation line (e.g., "2022. 21(1), p. 96. associated with...")
    if _CITATION_TITLE.match(t):
        return True
    
    # Title is too short and looks like a fragment
    if len(t) < 10 and _CITATION_LINE.search(t):
        return True
    
    # Title starts with volume/page pattern
    if re.match(r'^\d{4}\.\s*\d+\(\d+\)', t):
        return True

    # Dense citation-like title fragment (e.g. "2024;12:1098765...")
    if _DENSE_CITATION_TITLE.match(t):
        return True

    if sum(1 for c in t if c.isdigit()) / max(len(t), 1) > 0.25 and not re.search(r'[A-Za-zÀ-ỹ]{5,}', t):
        return True

    # Common crawl artifact: truncated lowercase fragment used as title
    if _LOWERCASE_FRAGMENT_TITLE.match(t) and len(t.split()) <= 4:
        return True
    
    return False


def _is_weak_title(title: str) -> bool:
    """Detect titles that are weak (penalize but don't reject)."""
    t = title.strip()
    if not t:
        return True  # missing title = weak
    
    # Very short title
    if len(t) < 15:
        return True

    # Mid-sentence fragment or broken lowercase heading
    if _LOWERCASE_FRAGMENT_TITLE.match(t):
        return True
    
    # Contains citation patterns
    if _CITATION_LINE.search(t):
        return True
    
    # Contains DOI
    if _DOI_PATTERN.search(t):
        return True
    
    return False
