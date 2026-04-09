"""
Article Aggregator v2
======================
Groups retrieved chunks by article, computes article-level scores,
and selects primary + secondary sources per review.md §5.

v2 improvements:
  - trust_tier boost: prefer authoritative sources (guideline > journal > patient)
  - doc_type boost: prefer doc types relevant to query
  - query_type_fit: match doc_type to query intent
  - selected_reason: explain why each article was chosen

Ported from gate_g3_eval.py with adaptations for RetrievedChunk objects.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING
from collections import defaultdict

from .retriever import RetrievedChunk

if TYPE_CHECKING:
    from .query_router import RouterOutput


# ── Title normalization (ported from evaluator) ──────────────────────

_RE_BAD_CHARS = re.compile(r'[\[\]\\]')

def title_norm(title: str) -> str:
    """Normalize title for grouping: NFC, lowercase, clean."""
    t = unicodedata.normalize('NFC', title)
    t = _RE_BAD_CHARS.sub('', t)
    t = t.lower().strip()
    t = re.sub(r'\d{1,3}$', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    t = t.strip('.,;:- ')
    return t


# ── Vietnamese stopwords ─────────────────────────────────────────────

_VN_STOPS = set(
    "và của ở tại có là được cho trong với các những một này đó từ đến theo về trên"
    " không cũng đã sẽ hay hoặc nhưng nếu khi sau trước như thì bằng"
    " qua giữa nào đều vẫn ra vào lên xuống đi mà do vì để khi nên"
    " người bệnh nhân viện tỷ lệ nghiên cứu đánh giá khảo sát thực trạng".split()
)


def _extract_keywords(text: str) -> set:
    """Extract meaningful keywords from text."""
    words = title_norm(text).split()
    return {w for w in words if w not in _VN_STOPS and len(w) > 1}


# ── Data structures ──────────────────────────────────────────────────

@dataclass
class ArticleGroup:
    """An article formed by grouping chunks with the same title."""
    title: str
    title_norm: str
    chunks: List[RetrievedChunk] = field(default_factory=list)
    max_score: float = 0.0
    avg_score: float = 0.0
    article_score: float = 0.0  # final composite score
    chunk_count: int = 0
    # v2 fields
    relevance_score: float = 0.0
    authority_score: float = 0.0
    query_fit_score: float = 0.0
    selected_reason: str = ""


@dataclass
class AggregatedResult:
    """Output of article aggregation."""
    primary: ArticleGroup
    secondary: List[ArticleGroup] = field(default_factory=list)
    all_articles: List[ArticleGroup] = field(default_factory=list)


# ── Core logic ───────────────────────────────────────────────────────

def _group_chunks_by_article(chunks: List[RetrievedChunk]) -> List[ArticleGroup]:
    """Group chunks by normalized title, compute per-article scores."""
    groups: dict[str, ArticleGroup] = {}

    for chunk in chunks:
        raw_title = chunk.metadata.get("title", "") or ""
        tn = title_norm(raw_title)
        if not tn:
            tn = f"_unknown_{chunk.id[:8]}"

        if tn not in groups:
            groups[tn] = ArticleGroup(
                title=raw_title,
                title_norm=tn,
                chunks=[],
            )

        groups[tn].chunks.append(chunk)

    # Compute scores per group
    for g in groups.values():
        scores = [c.score for c in g.chunks]
        g.chunk_count = len(scores)
        g.max_score = max(scores)
        g.avg_score = sum(scores) / len(scores)

    return list(groups.values())


# ── Authority & query-fit tables ─────────────────────────────────────

_TRUST_TIER_BOOST = {1: 0.08, 2: 0.04, 3: 0.0}

_DOC_TYPE_BOOST = {
    "guideline": 0.06,
    "textbook": 0.04,
    "reference": 0.04,
    "review": 0.02,
    "patient_education": 0.0,
}

# query_type → which doc_types get a fitness boost
_QUERY_TYPE_FIT = {
    "guideline_comparison":    {"guideline": 0.05, "reference": 0.02},
    "teaching_explainer":      {"textbook": 0.05, "reference": 0.04, "guideline": 0.02},
    "study_result_extraction":  {"review": 0.05},
    "research_appraisal":      {"review": 0.05},
    "comparative_synthesis":   {"review": 0.04, "guideline": 0.03},
    "fact_extraction":         {"guideline": 0.03, "textbook": 0.03, "patient_education": 0.02},
}


def _get_article_metadata(article: ArticleGroup) -> dict:
    """Extract representative metadata from the first chunk with data."""
    for chunk in article.chunks:
        md = chunk.metadata
        if md.get("trust_tier") is not None or md.get("doc_type"):
            return md
    return article.chunks[0].metadata if article.chunks else {}


def _compute_article_score(
    article: ArticleGroup,
    query: str,
    router_output: Optional["RouterOutput"] = None,
) -> float:
    """
    Composite article score v2:
      Relevance  = 0.35*max + 0.20*avg + 0.15*diversity + 0.15*numeric + 0.15*kw
      Authority  = trust_tier_boost + doc_type_boost
      Query-fit  = doc_type fitness for this query_type

    Returns final score and sets sub-scores on the article object.
    """
    # ── Relevance signals (unchanged) ────────────────────────────
    section_diversity = min(article.chunk_count / 6.0, 1.0)

    num_pattern = re.compile(r'\d+[.,]?\d*\s*%|p\s*[<>=]|OR\s*=|HR\s*=|AUC|n\s*=', re.IGNORECASE)
    numeric_chunks = sum(1 for c in article.chunks if num_pattern.search(c.text))
    numeric_density = min(numeric_chunks / max(article.chunk_count, 1), 1.0)

    query_kw = _extract_keywords(query)
    title_kw = _extract_keywords(article.title)
    if query_kw and title_kw:
        overlap = len(query_kw & title_kw)
        kw_score = min(overlap / len(query_kw), 1.0)
    else:
        kw_score = 0.0

    relevance = (
        0.35 * article.max_score
        + 0.20 * article.avg_score
        + 0.15 * section_diversity
        + 0.15 * numeric_density
        + 0.15 * kw_score
    )

    # ── Authority signals (NEW v2) ───────────────────────────────
    md = _get_article_metadata(article)
    tier = md.get("trust_tier", 3)
    if not isinstance(tier, int):
        try:
            tier = int(tier)
        except (ValueError, TypeError):
            tier = 3
    trust_boost = _TRUST_TIER_BOOST.get(tier, 0.0)

    doc_type = md.get("doc_type", "") or ""
    dtype_boost = _DOC_TYPE_BOOST.get(doc_type, 0.0)

    authority = trust_boost + dtype_boost

    # ── Query-fit signals (NEW v2) ───────────────────────────────
    query_fit = 0.0
    if router_output is not None:
        fit_map = _QUERY_TYPE_FIT.get(router_output.query_type, {})
        query_fit = fit_map.get(doc_type, 0.0)

    # ── Composite ────────────────────────────────────────────────
    score = relevance + authority + query_fit

    # Store sub-scores for transparency
    article.relevance_score = round(relevance, 4)
    article.authority_score = round(authority, 4)
    article.query_fit_score = round(query_fit, 4)

    return round(score, 4)


def _build_selected_reason(article: ArticleGroup, is_primary: bool) -> str:
    """Build a human-readable explanation of why this article was selected."""
    md = _get_article_metadata(article)
    parts = []
    if is_primary:
        parts.append("highest composite score")
    else:
        parts.append("supporting source")

    tier = md.get("trust_tier")
    if tier == 1:
        parts.append("authoritative source (Tier 1)")
    doc_type = md.get("doc_type", "")
    if doc_type:
        parts.append(f"type={doc_type}")

    parts.append(f"relevance={article.relevance_score}")
    if article.authority_score > 0:
        parts.append(f"authority=+{article.authority_score}")
    if article.query_fit_score > 0:
        parts.append(f"query_fit=+{article.query_fit_score}")

    return "; ".join(parts)


def aggregate_articles(
    chunks: List[RetrievedChunk],
    query: str,
    router_output: Optional["RouterOutput"] = None,
    max_secondary: int = 2,
) -> AggregatedResult:
    """
    Main entry point: group chunks → score articles → select primary + secondary.

    Args:
        chunks: Retrieved chunks from Qdrant
        query: The user's search query
        router_output: Router classification (used for query-type fit scoring)
        max_secondary: Max secondary sources to include

    Returns:
        AggregatedResult with primary, secondary, and all articles
    """
    if router_output:
        mode = getattr(router_output, "retrieval_mode", "article_centric")
        if mode == "mechanistic_synthesis":
            max_secondary = 5
            score_threshold = 0.70
            max_primary_chunks = 2
            max_sec_chunks = 1
        elif mode == "topic_summary":
            max_secondary = 3
            score_threshold = 0.75
            max_primary_chunks = 3
            max_sec_chunks = 2
        else:
            max_secondary = 1
            score_threshold = 0.85
            max_primary_chunks = 5
            max_sec_chunks = 2
    else:
        score_threshold = 0.80
        max_primary_chunks = 4
        max_sec_chunks = 2
    if not chunks:
        empty = ArticleGroup(title="", title_norm="", chunks=[])
        return AggregatedResult(primary=empty, secondary=[], all_articles=[])

    # Step 1: Group
    articles = _group_chunks_by_article(chunks)

    # Step 2: Score (v2 — with authority + query-fit)
    for art in articles:
        art.article_score = _compute_article_score(art, query, router_output)

    # Step 3: Sort by composite score
    articles.sort(key=lambda a: -a.article_score)

    # Step 4: Select primary + secondary
    primary = articles[0]
    primary.selected_reason = _build_selected_reason(primary, is_primary=True)
    primary.chunks = primary.chunks[:max_primary_chunks]

    secondary = []
    for art in articles[1:]:
        if len(secondary) >= max_secondary:
            break
        # Dynamic threshold based on retrieval_mode
        if art.article_score >= primary.article_score * score_threshold:
            art.selected_reason = _build_selected_reason(art, is_primary=False)
            art.chunks = art.chunks[:max_sec_chunks]
            secondary.append(art)

    return AggregatedResult(
        primary=primary,
        secondary=secondary,
        all_articles=articles,
    )
