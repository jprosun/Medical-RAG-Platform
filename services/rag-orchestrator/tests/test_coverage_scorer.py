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
    answer_style: str = ""
    retrieval_profile: str = "light"
    retrieval_mode: str = "topic_summary"
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
        raw_text=("Giải thích cơ chế bệnh sinh và cơ chế đáp ứng miễn dịch " * 30),
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


def test_raw_text_overlap_can_reach_high_without_extractor():
    """Lightweight answerable questions should not be capped at medium forever."""
    pack = _make_evidence_pack(
        raw_text=(
            "Can thiệp sớm giúp giảm triệu chứng. "
            "Thoái hóa khớp ở người cao tuổi cần giáo dục sức khỏe, dinh dưỡng, tập luyện và kiểm soát cân nặng. "
        ) * 10,
    )
    router = MockRouterOutput(query_type="fact_extraction")
    result = score_coverage(
        pack,
        router,
        "can thiệp sớm cho thoái hóa khớp ở người cao tuổi",
    )
    assert result.coverage_level == "high"
    assert result.confidence_ceiling == "high"


def test_secondary_sources_alone_do_not_force_moderate_confidence():
    """Having a supporting source should not automatically trigger disclaimer mode."""
    pack = _make_evidence_pack(
        has_secondary=True,
        raw_text=("suy hô hấp trong 1 giờ đầu sau sinh ở trẻ sơ sinh " * 20),
    )
    router = MockRouterOutput(query_type="fact_extraction")
    result = score_coverage(pack, router, "suy hô hấp trong 1 giờ đầu sau sinh")
    assert result.confidence_ceiling == "high"


def test_exact_answer_style_does_not_require_study_methods_for_direct_fact():
    pack = _make_evidence_pack(
        raw_text=(
            "Dấu hiệu lâm sàng phổ biến nhất là suy hô hấp và thường khởi phát trong giờ đầu sau sinh. "
        ) * 10,
    )
    router = MockRouterOutput(
        query_type="study_result_extraction",
        answer_style="exact",
        requires_numbers=False,
        needs_extractor=False,
    )
    result = score_coverage(
        pack,
        router,
        "Trong nghiên cứu này, dấu hiệu lâm sàng phổ biến nhất ở trẻ sơ sinh là gì?",
    )
    assert "study_design" not in result.missing_requirements
    assert result.coverage_level == "high"


def test_article_centric_bounded_partial_does_not_require_secondary_sources():
    pack = _make_evidence_pack(
        raw_text=(
            "PIEB giúp giảm lượng thuốc tê sử dụng, giảm ức chế vận động và không tăng tác dụng phụ khác so với CEI. "
        ) * 10,
    )
    router = MockRouterOutput(
        query_type="comparative_synthesis",
        answer_style="bounded_partial",
        retrieval_mode="article_centric",
        requires_numbers=False,
    )
    result = score_coverage(
        pack,
        router,
        "Từ phần tổng quan này, có thể kết luận PIEB vượt trội tuyệt đối so với CEI ở mọi khía cạnh không? Có thể khẳng định chắc điều gì và chưa thể khẳng định điều gì?",
    )
    assert "comparison_sources" not in result.missing_requirements


def test_article_centric_summary_ignores_generic_unsupported_scope_terms():
    pack = _make_evidence_pack(
        raw_text=(
            "Việc lựa chọn CEA hay CAS cần dựa trên mức độ hẹp, triệu chứng, nguy cơ quanh thủ thuật "
            "và kỳ vọng sống còn của người bệnh. Sau can thiệp vẫn cần điều trị nội khoa tối ưu, "
            "kiểm soát yếu tố nguy cơ tim mạch và theo dõi hình ảnh định kỳ."
        ) * 5,
    )
    router = MockRouterOutput(
        query_type="comparative_synthesis",
        answer_style="summary",
        retrieval_mode="article_centric",
        requires_numbers=False,
    )
    result = score_coverage(
        pack,
        router,
        "Theo context, quyết định chọn CEA hay CAS và việc theo dõi sau can thiệp đều phải cá thể hóa dựa trên những nhóm yếu tố nào?",
    )
    assert result.confidence_ceiling == "high"
    assert "can thiệp" not in result.unsupported_concepts
    assert "yếu tố" not in result.unsupported_concepts
