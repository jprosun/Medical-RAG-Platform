"""
Standardized document schema for the medical knowledge RAG corpus.

Every knowledge unit (article section, guideline recommendation, FAQ entry, etc.)
is normalised into a DocumentRecord before chunking and indexing.  The JSONL files
produced by ETL pipelines MUST conform to this schema.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional


# ── valid enum values ────────────────────────────────────────────────
VALID_DOC_TYPES = frozenset(
    {"guideline", "textbook", "faq", "patient_education", "review", "reference"}
)
VALID_AUDIENCES = frozenset({"patient", "student", "clinician"})
VALID_TRUST_TIERS = frozenset({1, 2, 3})


# ── core dataclass ───────────────────────────────────────────────────
@dataclass
class DocumentRecord:
    """A single knowledge unit ready for chunking and indexing."""

    doc_id: str
    title: str
    body: str
    source_name: str

    # optional but strongly recommended
    section_title: str = ""
    source_url: str = ""
    doc_type: str = "reference"          # guideline | textbook | faq | patient_education | review | reference
    specialty: str = "general"           # cardiology, endocrinology, orthopedics, …
    audience: str = "patient"            # patient | student | clinician
    language: str = "en"
    trust_tier: int = 3                  # 1 = canonical guidance, 2 = reference, 3 = patient-friendly
    published_at: str = ""               # ISO-8601 date string
    updated_at: str = ""
    tags: List[str] = field(default_factory=list)
    heading_path: str = ""               # "Hypertension > Management > First-line"

    # ── validation ───────────────────────────────────────────────────
    def validate(self) -> List[str]:
        """Return a list of human-readable validation errors (empty = valid)."""
        errors: List[str] = []

        if not self.doc_id or not self.doc_id.strip():
            errors.append("doc_id is required")
        if not self.title or not self.title.strip():
            errors.append("title is required")
        if not self.body or not self.body.strip():
            errors.append("body is required")
        if not self.source_name or not self.source_name.strip():
            errors.append("source_name is required")
        if self.doc_type not in VALID_DOC_TYPES:
            errors.append(f"doc_type '{self.doc_type}' not in {sorted(VALID_DOC_TYPES)}")
        if self.audience not in VALID_AUDIENCES:
            errors.append(f"audience '{self.audience}' not in {sorted(VALID_AUDIENCES)}")
        if self.trust_tier not in VALID_TRUST_TIERS:
            errors.append(f"trust_tier must be one of {sorted(VALID_TRUST_TIERS)}")
        return errors

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    # ── serialise ────────────────────────────────────────────────────
    def to_jsonl_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DocumentRecord":
        """Create a DocumentRecord from a dict, ignoring unknown keys."""
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        # coerce tags to list
        tags = filtered.get("tags", [])
        if isinstance(tags, str):
            filtered["tags"] = [t.strip() for t in tags.split(",") if t.strip()]
        return cls(**filtered)


# ── JSONL reader ─────────────────────────────────────────────────────
def iter_jsonl(path: str) -> Iterator[DocumentRecord]:
    """Yield DocumentRecord objects from a JSONL file, skipping blank lines."""
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{lineno}: invalid JSON – {exc}") from exc
            yield DocumentRecord.from_dict(obj)
