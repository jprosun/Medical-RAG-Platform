"""
Utility helpers for data ingestion.

Includes text normalization, heading-based section splitting,
heading-path construction, and slug generation for stable chunk IDs.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import List

from .ingest_quality import detect_inline_heading, detect_plain_heading


def read_file(fp: str) -> str:
    with open(fp, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def normalize_whitespace(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)


@dataclass
class Section:
    """A contiguous section carved out of a markdown or plain-text document."""

    level: int
    title: str
    body: str
    heading_path: str


def _split_by_plain_headings(text: str) -> List[Section]:
    lines = text.splitlines()
    heading_rows: List[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        title = detect_plain_heading(line)
        if title:
            heading_rows.append((idx, title))

    if not heading_rows:
        return []

    sections: List[Section] = []
    for idx, (line_idx, title) in enumerate(heading_rows):
        body_start = line_idx + 1
        body_end = heading_rows[idx + 1][0] if idx + 1 < len(heading_rows) else len(lines)
        body = "\n".join(lines[body_start:body_end]).strip()
        if not body:
            continue
        sections.append(
            Section(
                level=1,
                title=title,
                body=body,
                heading_path=title,
            )
        )

    return sections


def _split_by_inline_headings(text: str) -> List[Section]:
    lines = text.splitlines()
    heading_rows: List[tuple[int, str, str]] = []
    for idx, line in enumerate(lines):
        title, remainder = detect_inline_heading(line)
        if title:
            heading_rows.append((idx, title, remainder))

    if not heading_rows:
        return []

    sections: List[Section] = []
    for idx, (line_idx, title, remainder) in enumerate(heading_rows):
        body_lines = [remainder] if remainder else []
        body_end = heading_rows[idx + 1][0] if idx + 1 < len(heading_rows) else len(lines)
        body_lines.extend(lines[line_idx + 1:body_end])
        body = "\n".join(body_lines).strip()
        if not body:
            continue
        sections.append(
            Section(
                level=1,
                title=title,
                body=body,
                heading_path=title,
            )
        )

    return sections


def split_by_headings(text: str) -> List[Section]:
    """
    Split text into sections along heading boundaries.

    Markdown headings are preferred. If none are found, plain-text Vietnamese/
    scientific section headings are detected. If the document has no headings at
    all, an empty list is returned so the caller can fall back to character
    chunking.
    """
    matches = list(_HEADING_RE.finditer(text))
    if matches:
        sections: List[Section] = []
        heading_stack: List[str] = []

        for idx, match in enumerate(matches):
            level = len(match.group(1))
            title = match.group(2).strip()
            body_start = match.end()
            body_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            body = text[body_start:body_end].strip()

            while heading_stack and len(heading_stack) >= level:
                heading_stack.pop()
            heading_stack.append(title)

            sections.append(
                Section(
                    level=level,
                    title=title,
                    body=body,
                    heading_path=" > ".join(heading_stack),
                )
            )

        return sections

    plain_sections = _split_by_plain_headings(text)
    if plain_sections:
        return plain_sections

    return _split_by_inline_headings(text)


def build_heading_path(titles: List[str]) -> str:
    return " > ".join(t.strip() for t in titles if t.strip())


def sanitize_for_id(text: str, max_len: int = 60) -> str:
    """
    Turn an arbitrary string into a lower-case, ASCII-safe slug suitable
    for use as part of a chunk ID.
    """
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_")
    if len(text) > max_len:
        text = text[:max_len].rstrip("_")
    return text or "untitled"
