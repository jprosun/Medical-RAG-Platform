"""
Utility helpers for data ingestion.

Includes text normalisation, heading-based section splitting,
heading-path construction, and slug generation for stable chunk IDs.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import List


# ── existing helpers (kept for backward compat) ──────────────────────
def read_file(fp: str) -> str:
    with open(fp, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def normalize_whitespace(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


# ── heading-based section splitting ──────────────────────────────────
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)


@dataclass
class Section:
    """A contiguous section carved out of a markdown document."""
    level: int          # 1..6 (# = 1, ## = 2, …)
    title: str
    body: str           # text *under* this heading (up to next same-or-higher heading)
    heading_path: str   # "Parent > Child > …"


def split_by_headings(text: str) -> List[Section]:
    """
    Split markdown *text* into sections along heading boundaries.

    Each Section receives the text between its heading and the next heading
    of equal or higher level.  If the document has **no headings at all** an
    empty list is returned so the caller can fall back to character chunking.
    """
    matches = list(_HEADING_RE.finditer(text))
    if not matches:
        return []

    sections: List[Section] = []
    heading_stack: List[str] = []  # tracks the current heading hierarchy

    for idx, m in enumerate(matches):
        level = len(m.group(1))
        title = m.group(2).strip()

        # start of body = right after this heading line
        body_start = m.end()
        # end of body = start of next heading (or end of document)
        body_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        body = text[body_start:body_end].strip()

        # maintain heading hierarchy stack
        # pop everything at the same or deeper level
        while heading_stack and len(heading_stack) >= level:
            heading_stack.pop()
        heading_stack.append(title)

        heading_path = " > ".join(heading_stack)

        sections.append(Section(
            level=level,
            title=title,
            body=body,
            heading_path=heading_path,
        ))

    return sections


def build_heading_path(titles: List[str]) -> str:
    """Join a list of heading titles into a path string."""
    return " > ".join(t.strip() for t in titles if t.strip())


# ── slug / ID helpers ────────────────────────────────────────────────
def sanitize_for_id(text: str, max_len: int = 60) -> str:
    """
    Turn an arbitrary string into a lower-case, ASCII-safe slug suitable
    for use as part of a chunk ID.

    Example:  "Hypertension in Adults – Diagnosis"
           →  "hypertension_in_adults_diagnosis"
    """
    # normalise unicode, strip accents
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    # replace non-alphanum with underscore, collapse runs
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_")
    if len(text) > max_len:
        text = text[:max_len].rstrip("_")
    return text or "untitled"