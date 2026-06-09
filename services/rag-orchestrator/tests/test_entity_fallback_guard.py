"""Tests for the entity-fallback guard that skips the expensive second
hybrid retrieval on general Standard queries."""

import os
from types import SimpleNamespace

from app.pipeline_flags import should_skip_entity_fallback as _should_skip_entity_fallback


def _router(query_type="fact_extraction", requires_numbers=False, requires_comparison=False):
    return SimpleNamespace(
        query_type=query_type,
        requires_numbers=requires_numbers,
        requires_comparison=requires_comparison,
        answer_style="summary",
    )


def test_skips_general_standard_query():
    assert _should_skip_entity_fallback(_router("fact_extraction"), "standard") is True
    assert _should_skip_entity_fallback(_router("teaching_explainer"), "standard") is True
    assert _should_skip_entity_fallback(_router("professional_explainer"), "standard") is True


def test_keeps_thinking_mode():
    assert _should_skip_entity_fallback(_router("fact_extraction"), "thinking") is False


def test_keeps_article_specific_query_types():
    assert _should_skip_entity_fallback(_router("study_result_extraction"), "standard") is False
    assert _should_skip_entity_fallback(_router("research_appraisal"), "standard") is False
    assert _should_skip_entity_fallback(_router("source_discovery"), "standard") is False


def test_keeps_when_numbers_or_comparison_required():
    assert _should_skip_entity_fallback(_router("fact_extraction", requires_numbers=True), "standard") is False
    assert _should_skip_entity_fallback(_router("fact_extraction", requires_comparison=True), "standard") is False


def test_guard_can_be_disabled_via_env():
    os.environ["ENTITY_FALLBACK_GUARD_ENABLED"] = "false"
    try:
        assert _should_skip_entity_fallback(_router("fact_extraction"), "standard") is False
    finally:
        del os.environ["ENTITY_FALLBACK_GUARD_ENABLED"]
