import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.prompt import build_prompt_v2
from app.coverage_scorer import CoverageOutput
from app.evidence_extractor import ClaimEvidence, EvidencePack, PrimaryEvidence, CoverageScores


class MockRouterOutput:
    query_type = "fact_extraction"
    answer_style = "exact"
    retrieval_profile = "light"
    retrieval_mode = "topic_summary"
    top_k_override = 8
    needs_extractor = False
    requires_numbers = False
    requires_limitations = False


def _coverage(level="high", ceiling="high", missing=None, unsupported=None, allowed_scope=""):
    return CoverageOutput(
        coverage_level=level,
        scores=CoverageScores(direct_answerability=0.8),
        allow_external=False,
        max_external_sources=0,
        force_abstain_parts=[],
        missing_requirements=missing or [],
        confidence_ceiling=ceiling,
        unsupported_concepts=unsupported or [],
        allowed_answer_scope=allowed_scope,
    )


def _pack():
    return EvidencePack(
        query_type="fact_extraction",
        primary_source=PrimaryEvidence(
            title="Test Article",
            raw_text="Can thiệp sớm gồm giáo dục sức khỏe, dinh dưỡng và tập luyện.",
        ),
    )


def test_prompt_v2_does_not_force_generic_disclaimer_for_medium_supported_answer():
    messages = build_prompt_v2(
        "Can thiệp sớm là gì?",
        _pack(),
        MockRouterOutput(),
        _coverage(level="medium", ceiling="moderate", missing=["study_design"]),
    )
    user_msg = messages[-1]["content"]
    assert "KHÔNG được mở đầu bằng disclaimer chung chung" in user_msg
    assert "BẮT BUỘC mở đầu câu trả lời bằng đúng câu sau" not in user_msg


def test_prompt_v2_forces_disclaimer_when_evidence_is_low():
    messages = build_prompt_v2(
        "Điều gì được hỗ trợ?",
        _pack(),
        MockRouterOutput(),
        _coverage(level="low", ceiling="low", missing=["numeric_data"]),
    )
    user_msg = messages[-1]["content"]
    assert "BẮT BUỘC mở đầu câu trả lời bằng đúng câu sau" in user_msg


def test_prompt_v2_hides_secondary_sources_for_article_centric_queries():
    pack = _pack()
    pack.secondary_sources.append(
        PrimaryEvidence(
            title="Secondary Article",
            raw_text="Đây là nguồn phụ không nên chen vào kết luận chính.",
        )
    )

    class ArticleCentricRouter(MockRouterOutput):
        query_type = "comparative_synthesis"
        retrieval_mode = "article_centric"

    messages = build_prompt_v2(
        "Theo context, chọn CEA hay CAS dựa trên yếu tố nào?",
        pack,
        ArticleCentricRouter(),
        _coverage(level="high"),
    )
    user_msg = messages[-1]["content"]
    assert "PHẠM VI NGUỒN" in user_msg
    assert "Secondary Article" not in user_msg


def test_prompt_v2_secondary_sources_are_marked_as_additive_only():
    pack = _pack()
    pack.secondary_sources.append(
        PrimaryEvidence(
            title="Secondary Article",
            raw_text="Nguồn phụ chỉ nên dùng khi bổ sung điểm mới.",
        )
    )

    class ComparativeRouter(MockRouterOutput):
        query_type = "comparative_synthesis"
        retrieval_mode = "topic_summary"

    messages = build_prompt_v2(
        "So sánh hai chiến lược điều trị theo tài liệu đã truy hồi.",
        pack,
        ComparativeRouter(),
        _coverage(level="high"),
    )
    user_msg = messages[-1]["content"]
    assert "NGUYÊN TẮC NGUỒN PHỤ" in user_msg


def test_prompt_v2_uses_exact_template_for_exact_answer_style():
    class ExactRouter(MockRouterOutput):
        query_type = "study_result_extraction"
        answer_style = "exact"

    messages = build_prompt_v2(
        "Trong nghiên cứu này, dấu hiệu lâm sàng phổ biến nhất là gì?",
        _pack(),
        ExactRouter(),
        _coverage(level="high"),
    )
    user_msg = messages[-1]["content"]
    assert "KIỂU TRẢ LỜI EXACT" in user_msg
    assert "## Câu trả lời trực tiếp" in user_msg


def test_prompt_v2_prioritizes_direct_answer_spans_for_exact_answers():
    pack = EvidencePack(
        query_type="study_result_extraction",
        primary_source=PrimaryEvidence(
            title="Test Article",
            raw_text="Nội dung đầy đủ dài và có nhiều số liệu khác nhau.",
            direct_answer_spans=[
                ClaimEvidence(
                    claim="Tỷ lệ hồi phục chức năng vận động đạt 96,18%.",
                    supporting_span="Tỷ lệ hồi phục chức năng vận động đạt 96,18%.",
                    section_title="Kết quả",
                ),
                ClaimEvidence(
                    claim="Chỉ số Karnofsky được duy trì trên 80 điểm trong 3-6 tháng.",
                    supporting_span="Chỉ số Karnofsky được duy trì trên 80 điểm trong 3-6 tháng.",
                    section_title="Kết quả",
                ),
            ],
        ),
    )

    class ExactRouter(MockRouterOutput):
        query_type = "study_result_extraction"
        answer_style = "exact"

    messages = build_prompt_v2(
        "Tỷ lệ hồi phục chức năng vận động là bao nhiêu và Karnofsky được duy trì trên 80 điểm trong bao lâu?",
        pack,
        ExactRouter(),
        _coverage(level="high"),
    )
    user_msg = messages[-1]["content"]
    assert "TRÍCH ĐOẠN TRẢ LỜI TRỰC TIẾP" in user_msg
    assert "96,18%" in user_msg
    assert "Karnofsky được duy trì trên 80 điểm trong 3-6 tháng" in user_msg
    assert "phải ưu tiên tuyệt đối các span này" in user_msg


def test_prompt_v2_surfaces_query_focused_findings_for_summary_answers():
    pack = EvidencePack(
        query_type="comparative_synthesis",
        primary_source=PrimaryEvidence(
            title="Test Article",
            raw_text="Nội dung đầy đủ của tài liệu chính.",
            key_findings=[
                ClaimEvidence(
                    claim="Việc lựa chọn CEA hay CAS cần dựa trên mức độ hẹp, triệu chứng, nguy cơ quanh thủ thuật và kỳ vọng sống còn.",
                    section_title="Tóm tắt",
                )
            ],
        ),
    )

    class SummaryRouter(MockRouterOutput):
        query_type = "comparative_synthesis"
        answer_style = "summary"
        retrieval_mode = "article_centric"

    messages = build_prompt_v2(
        "Theo context, quyết định chọn CEA hay CAS và việc theo dõi sau can thiệp đều phải cá thể hóa dựa trên những nhóm yếu tố nào?",
        pack,
        SummaryRouter(),
        _coverage(level="high"),
    )
    user_msg = messages[-1]["content"]
    assert "Phát hiện chính gần nhất với câu hỏi" in user_msg
    assert "mức độ hẹp, triệu chứng, nguy cơ quanh thủ thuật" in user_msg
    assert "giữ nguyên ở mức nhóm chung" in user_msg


def test_prompt_v2_uses_bounded_template_for_bounded_partial_style():
    class BoundedRouter(MockRouterOutput):
        query_type = "comparative_synthesis"
        answer_style = "bounded_partial"
        retrieval_mode = "article_centric"

    messages = build_prompt_v2(
        "Có thể kết luận chắc điều gì và chưa thể kết luận điều gì?",
        _pack(),
        BoundedRouter(),
        _coverage(level="high"),
    )
    user_msg = messages[-1]["content"]
    assert "KIỂU TRẢ LỜI BOUNDED_PARTIAL" in user_msg
    assert "## Có thể khẳng định" in user_msg
    assert "## Chưa thể khẳng định" in user_msg
