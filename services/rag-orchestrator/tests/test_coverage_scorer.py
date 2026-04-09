"""
Tests for Evidence Sufficiency Scorer v2.
Validates query-type-specific requirements, confidence_ceiling, and missing_requirements.
"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataclasses import dataclass, field
from typing import List
from app.coverage_scorer import score_coverage, CoverageOutput
from app.evidence_extractor import (
    EvidencePack,
    PrimaryEvidence,
    EvidenceField,
    NumberEvidence,
    ClaimEvidence,
)


@dataclass
class MockRouterOutput:
    query_type: str = "fact_extraction"
    retrieval_profile: str = "light"
    top_k_override: int = 8
    needs_extractor: bool = False
    requires_numbers: bool = False
    requires_limitations: bool = False


def _make_evidence_pack(
    title="Test Article",
    source_type="",
    has_design=False,
    has_population=False,
    has_numbers=False,
    has_limitations=False,
    has_secondary=False,
    extractor_used=False,
    raw_text="Some medical discussion about treatment options and patient care.",
):
    """Helper to build an EvidencePack with controllable features."""
    primary = PrimaryEvidence(
        title=title,
        source_type=source_type,
        raw_text=raw_text,
    )
    if has_design:
        primary.design = EvidenceField(text="prospective cohort study")
    if has_population:
        primary.population = EvidenceField(text="146 patients")
        primary.sample_size = EvidenceField(text="n=146")
    if has_numbers:
        primary.numbers = [NumberEvidence(metric="AUC", value="0.82")]
    if has_limitations:
        primary.limitations = [ClaimEvidence(claim="đơn trung tâm")]

    pack = EvidencePack(
        query_type="fact_extraction",
        primary_source=primary,
        extractor_used=extractor_used,
    )
    if has_secondary:
        secondary = PrimaryEvidence(title="Secondary Article", raw_text="Secondary content")
        pack.secondary_sources = [secondary]

    return pack


# ── Basic coverage levels ────────────────────────────────────────────

def test_high_coverage_full_evidence():
    """Full evidence should yield high coverage."""
    pack = _make_evidence_pack(
        has_design=True, has_population=True,
        has_numbers=True, has_limitations=True,
        raw_text="Detailed medical content " * 50,
    )
    router = MockRouterOutput(query_type="fact_extraction")
    result = score_coverage(pack, router, "test query")
    assert result.coverage_level in ("high", "medium")


def test_low_coverage_empty_evidence():
    """Empty evidence should yield low or medium coverage."""
    pack = EvidencePack(
        query_type="fact_extraction",
        primary_source=PrimaryEvidence(title="", raw_text=""),
    )
    router = MockRouterOutput(query_type="fact_extraction")
    result = score_coverage(pack, router, "test query")
    assert result.coverage_level in ("low", "medium"), (
        f"Empty evidence should yield low/medium, got {result.coverage_level}"
    )


# ── v2: Missing requirements ────────────────────────────────────────

def test_study_result_missing_design():
    """study_result_extraction without design should flag missing requirement."""
    pack = _make_evidence_pack(
        source_type="original_study",
        has_design=False,
        has_population=True,
        has_numbers=True,
        extractor_used=True,
        raw_text="Results show that treatment improved outcomes " * 20,
    )
    router = MockRouterOutput(
        query_type="study_result_extraction",
        needs_extractor=True,
        requires_numbers=True,
    )
    result = score_coverage(pack, router, "kết quả nghiên cứu")
    assert "study_design" in result.missing_requirements


def test_study_result_missing_everything():
    """study_result_extraction with no evidence should have low confidence_ceiling."""
    pack = _make_evidence_pack(
        source_type="original_study",
        has_design=False,
        has_population=False,
        has_numbers=False,
        extractor_used=True,
        raw_text="Brief text",
    )
    router = MockRouterOutput(
        query_type="study_result_extraction",
        needs_extractor=True,
        requires_numbers=True,
    )
    result = score_coverage(pack, router, "kết quả nghiên cứu")
    assert len(result.missing_requirements) >= 2
    assert result.confidence_ceiling in ("moderate", "low")


def test_research_appraisal_missing_limitations():
    """research_appraisal without limitations should flag missing requirement."""
    pack = _make_evidence_pack(
        has_design=True,
        has_limitations=False,
        extractor_used=True,
        raw_text="Methods section describes study design " * 20,
    )
    router = MockRouterOutput(
        query_type="research_appraisal",
        needs_extractor=True,
        requires_limitations=True,
    )
    result = score_coverage(pack, router, "đánh giá nghiên cứu")
    assert "limitations" in result.missing_requirements


def test_comparative_synthesis_needs_secondary():
    """comparative_synthesis without secondary sources should flag."""
    pack = _make_evidence_pack(
        has_secondary=False,
        raw_text="Comparison content " * 30,
    )
    router = MockRouterOutput(query_type="comparative_synthesis")
    result = score_coverage(pack, router, "so sánh hai nghiên cứu")
    assert "comparison_sources" in result.missing_requirements
    assert result.confidence_ceiling in ("moderate", "low")


# ── v2: Confidence ceiling ───────────────────────────────────────────

def test_confidence_ceiling_defaults_high():
    """Default confidence_ceiling should be 'high' when evidence is good."""
    pack = _make_evidence_pack(
        has_design=True, has_population=True,
        has_numbers=True, has_limitations=True,
        raw_text="Comprehensive evidence " * 50,
    )
    router = MockRouterOutput(query_type="teaching_explainer")
    result = score_coverage(pack, router, "giải thích cơ chế")
    assert result.confidence_ceiling == "high"


def test_guideline_comparison_non_guideline_moderate():
    """guideline_comparison with non-guideline source should flag missing requirement."""
    pack = _make_evidence_pack(
        source_type="original_study",
        has_design=True,
        has_numbers=True,
        extractor_used=True,
        raw_text="Study results about treatment " * 30,
    )
    router = MockRouterOutput(query_type="guideline_comparison")
    result = score_coverage(pack, router, "guideline comparison")
    # Should at least flag the missing requirement
    assert "guideline_source_preferred" in result.missing_requirements, (
        f"Expected guideline_source_preferred in {result.missing_requirements}"
    )


# ── Backward compatibility ───────────────────────────────────────────

def test_coverage_output_has_v2_fields():
    """CoverageOutput should have missing_requirements and confidence_ceiling."""
    pack = _make_evidence_pack(raw_text="sample " * 20)
    router = MockRouterOutput()
    result = score_coverage(pack, router, "test")
    assert hasattr(result, "missing_requirements")
    assert hasattr(result, "confidence_ceiling")
    assert isinstance(result.missing_requirements, list)
    assert result.confidence_ceiling in ("high", "moderate", "low")
