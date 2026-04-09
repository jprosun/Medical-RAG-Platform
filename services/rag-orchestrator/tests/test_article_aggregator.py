"""
Tests for Article Aggregator v2.
Validates trust_tier boost, doc_type boost, query_type_fit, and selected_reason.
"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataclasses import dataclass
from app.retriever import RetrievedChunk
from app.article_aggregator import (
    aggregate_articles,
    _compute_article_score,
    _group_chunks_by_article,
    ArticleGroup,
    title_norm,
)


def _make_chunk(title, score, trust_tier=3, doc_type="", chunk_id="c1", text="sample text"):
    """Helper: create a RetrievedChunk with minimal metadata."""
    return RetrievedChunk(
        id=chunk_id,
        text=text,
        score=score,
        metadata={
            "title": title,
            "trust_tier": trust_tier,
            "doc_type": doc_type,
            "source_name": "TestSource",
        },
    )


@dataclass
class MockRouterOutput:
    """Minimal mock for RouterOutput."""
    query_type: str = "fact_extraction"
    retrieval_profile: str = "light"
    top_k_override: int = 8
    needs_extractor: bool = False
    requires_numbers: bool = False
    requires_limitations: bool = False


# ── Trust tier boost ─────────────────────────────────────────────────

def test_trust_tier_1_boosted_over_tier_3():
    """Tier 1 guideline should score higher than Tier 3 journal, all else equal."""
    tier1 = [_make_chunk("Guideline A", 0.80, trust_tier=1, doc_type="guideline")]
    tier3 = [_make_chunk("Journal B", 0.80, trust_tier=3, doc_type="")]

    articles_1 = _group_chunks_by_article(tier1)
    articles_3 = _group_chunks_by_article(tier3)

    score_1 = _compute_article_score(articles_1[0], "test query")
    score_3 = _compute_article_score(articles_3[0], "test query")

    assert score_1 > score_3, f"tier1={score_1} should > tier3={score_3}"
    # Tier 1 gets +0.08 trust + 0.06 doc_type = +0.14
    assert score_1 - score_3 >= 0.10, "trust_tier + doc_type boost should be >= 0.10"


def test_trust_tier_2_boosted_over_tier_3():
    """Tier 2 should get a smaller boost than Tier 1."""
    tier2 = [_make_chunk("Textbook C", 0.80, trust_tier=2, doc_type="textbook")]
    tier3 = [_make_chunk("Journal D", 0.80, trust_tier=3, doc_type="")]

    articles_2 = _group_chunks_by_article(tier2)
    articles_3 = _group_chunks_by_article(tier3)

    score_2 = _compute_article_score(articles_2[0], "test query")
    score_3 = _compute_article_score(articles_3[0], "test query")

    assert score_2 > score_3


# ── Query type fit ───────────────────────────────────────────────────

def test_query_type_fit_guideline_comparison():
    """When query type is guideline_comparison, guidelines get extra boost."""
    guideline = [_make_chunk("Guideline X", 0.75, trust_tier=1, doc_type="guideline")]
    journal = [_make_chunk("Journal Y", 0.75, trust_tier=3, doc_type="")]

    router = MockRouterOutput(query_type="guideline_comparison")

    g_articles = _group_chunks_by_article(guideline)
    j_articles = _group_chunks_by_article(journal)

    score_g = _compute_article_score(g_articles[0], "guideline test", router)
    score_j = _compute_article_score(j_articles[0], "guideline test", router)

    # guideline gets: +0.08 trust + 0.06 doc_type + 0.05 query_fit = +0.19
    assert score_g > score_j
    assert score_g - score_j >= 0.15


def test_query_type_fit_teaching_prefers_textbook():
    """teaching_explainer should prefer textbooks."""
    textbook = [_make_chunk("Textbook Z", 0.75, trust_tier=2, doc_type="textbook")]
    journal = [_make_chunk("Journal W", 0.75, trust_tier=3, doc_type="")]

    router = MockRouterOutput(query_type="teaching_explainer")

    t_articles = _group_chunks_by_article(textbook)
    j_articles = _group_chunks_by_article(journal)

    score_t = _compute_article_score(t_articles[0], "mechanism test", router)
    score_j = _compute_article_score(j_articles[0], "mechanism test", router)

    assert score_t > score_j


# ── Backward compatibility ───────────────────────────────────────────

def test_aggregate_without_router_output():
    """aggregate_articles should work without router_output (backward compat)."""
    chunks = [
        _make_chunk("Article A", 0.90, chunk_id="c1"),
        _make_chunk("Article A", 0.85, chunk_id="c2"),
        _make_chunk("Article B", 0.80, chunk_id="c3"),
    ]
    result = aggregate_articles(chunks, "test query")  # no router_output
    assert result.primary.title == "Article A"
    assert result.primary.article_score > 0


def test_aggregate_with_router_output():
    """aggregate_articles should accept router_output."""
    chunks = [
        _make_chunk("Guideline A", 0.80, trust_tier=1, doc_type="guideline", chunk_id="c1"),
        _make_chunk("Journal B", 0.82, trust_tier=3, doc_type="", chunk_id="c2"),
    ]
    router = MockRouterOutput(query_type="guideline_comparison")
    result = aggregate_articles(chunks, "guideline test", router)

    # With guideline_comparison, Guideline A should be primary despite lower raw score
    assert result.primary.title == "Guideline A", (
        f"Expected Guideline A as primary, got {result.primary.title}"
    )


# ── Selected reason ──────────────────────────────────────────────────

def test_selected_reason_populated():
    """Primary article should have a selected_reason."""
    chunks = [_make_chunk("Article X", 0.90, trust_tier=1, doc_type="guideline")]
    result = aggregate_articles(chunks, "test")
    assert result.primary.selected_reason
    assert "highest composite score" in result.primary.selected_reason
    assert "Tier 1" in result.primary.selected_reason


# ── Sub-scores stored ────────────────────────────────────────────────

def test_sub_scores_stored():
    """Article should have relevance, authority, query_fit sub-scores."""
    chunks = [_make_chunk("Test Article", 0.80, trust_tier=1, doc_type="guideline")]
    router = MockRouterOutput(query_type="fact_extraction")
    result = aggregate_articles(chunks, "test", router)
    art = result.primary

    assert art.relevance_score > 0
    assert art.authority_score > 0  # tier 1 + guideline
    assert art.query_fit_score >= 0  # fact_extraction + guideline = 0.03


# ── Empty chunks ─────────────────────────────────────────────────────

def test_empty_chunks():
    """Empty chunk list should return empty result."""
    result = aggregate_articles([], "test")
    assert result.primary.title == ""
    assert result.secondary == []
