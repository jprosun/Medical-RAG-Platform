"""
Standardized document schema for the medical knowledge RAG corpus.

Every knowledge unit (article section, guideline recommendation, FAQ entry, etc.)
is normalized into a DocumentRecord before chunking and indexing. JSONL files
produced by ETL pipelines should conform to this schema.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterator, List, Optional


VALID_DOC_TYPES = frozenset(
    {
        "guideline",
        "textbook",
        "faq",
        "patient_education",
        "review",
        "reference",
        "research_article",
        "journal_article",
        "original_research",
        "case_report",
        "meta_analysis",
    }
)
VALID_AUDIENCES = frozenset({"patient", "student", "clinician"})
VALID_TRUST_TIERS = frozenset({1, 2, 3})


@dataclass
class DocumentRecord:
    """A single knowledge unit ready for chunking and indexing."""

    doc_id: str
    title: str
    body: str
    source_name: str

    section_title: str = ""
    source_url: str = ""
    source_id: str = ""
    source_file: str = ""
    article_id: str = ""
    institution: str = ""
    raw_path: str = ""
    processed_path: str = ""
    intermediate_path: str = ""
    parent_file: str = ""
    source_sha256: str = ""
    crawl_run_id: str = ""
    etl_run_id: str = ""
    doc_type: str = "reference"
    specialty: str = "general"
    audience: str = "patient"
    language: str = "en"
    canonical_title: str = ""
    language_confidence: float = 0.0
    is_mixed_language: bool = False
    trust_tier: int = 3
    published_at: str = ""
    updated_at: str = ""
    quality_score: Optional[int] = None
    quality_status: str = ""
    quality_flags: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    heading_path: str = ""

    def validate(self) -> List[str]:
        """Return a list of human-readable validation errors (empty means valid)."""
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

    def to_jsonl_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DocumentRecord":
        """Create a DocumentRecord from a dict, ignoring unknown keys."""
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}

        tags = filtered.get("tags", [])
        if isinstance(tags, str):
            filtered["tags"] = [t.strip() for t in tags.split(",") if t.strip()]

        quality_flags = filtered.get("quality_flags", [])
        if isinstance(quality_flags, str):
            filtered["quality_flags"] = [t.strip() for t in quality_flags.split(",") if t.strip()]

        trust_tier = filtered.get("trust_tier")
        if isinstance(trust_tier, str) and trust_tier.isdigit():
            filtered["trust_tier"] = int(trust_tier)

        quality_score = filtered.get("quality_score")
        if isinstance(quality_score, str) and quality_score.isdigit():
            filtered["quality_score"] = int(quality_score)

        language_confidence = filtered.get("language_confidence")
        if isinstance(language_confidence, str):
            try:
                filtered["language_confidence"] = float(language_confidence)
            except ValueError:
                pass

        is_mixed_language = filtered.get("is_mixed_language")
        if isinstance(is_mixed_language, str):
            filtered["is_mixed_language"] = is_mixed_language.strip().lower() in {
                "1",
                "true",
                "yes",
            }

        return cls(**filtered)


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
                raise ValueError(f"{path}:{lineno}: invalid JSON - {exc}") from exc
            yield DocumentRecord.from_dict(obj)
