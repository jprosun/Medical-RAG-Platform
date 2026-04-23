import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.prompt import build_prompt_v2
from app.coverage_scorer import CoverageOutput
from app.evidence_extractor import ClaimEvidence, CoverageScores, EvidencePack, PrimaryEvidence


class MockRouterOutput:
    query_type = "comparative_synthesis"
    answer_style = "summary"
    retrieval_profile = "standard"
    retrieval_mode = "article_centric"
    needs_extractor = False
    requires_numbers = False
    requires_limitations = False


def _coverage():
    return CoverageOutput(
        coverage_level="high",
        scores=CoverageScores(direct_answerability=0.8),
        allow_external=False,
        max_external_sources=0,
        force_abstain_parts=[],
        missing_requirements=[],
        confidence_ceiling="high",
        unsupported_concepts=[],
        allowed_answer_scope="",
    )


def test_prompt_v2_adds_scope_guard_for_criteria_queries_without_direct_spans():
    pack = EvidencePack(
        query_type="comparative_synthesis",
        primary_source=PrimaryEvidence(
            title="Test Article",
            raw_text="Nội dung đầy đủ có thêm nhiều chỉ số khác nhau.",
            key_findings=[
                ClaimEvidence(
                    claim="Nghiên cứu đánh giá tình trạng nhập viện trên lâm sàng và cận lâm sàng cùng kết quả điều trị.",
                    section_title="Tóm tắt",
                )
            ],
        ),
    )

    messages = build_prompt_v2(
        "Theo nghiên cứu này, kết quả điều trị ở bệnh nhân suy tim mạn tính được đánh giá qua những chỉ số hoặc tiêu chí nào?",
        pack,
        MockRouterOutput(),
        _coverage(),
    )
    user_msg = messages[-1]["content"]
    assert "chưa nêu bộ chỉ số cụ thể" in user_msg
    assert "Không được đào sang raw text để tự ráp thêm EF" in user_msg


def test_prompt_v2_adds_scope_guard_for_grouped_factor_queries():
    pack = EvidencePack(
        query_type="comparative_synthesis",
        primary_source=PrimaryEvidence(
            title="Test Article",
            raw_text="Nội dung đầy đủ của tài liệu chính.",
            key_findings=[
                ClaimEvidence(
                    claim="Việc lựa chọn CEA hay CAS cần dựa trên mức độ hẹp, triệu chứng, nguy cơ quanh thủ thuật và kỳ vọng sống còn.",
                    section_title="Tóm tắt",
                ),
                ClaimEvidence(
                    claim="Sau can thiệp, điều trị nội khoa tối ưu, kiểm soát yếu tố nguy cơ tim mạch và theo dõi hình ảnh định kỳ vẫn giữ vai trò thiết yếu.",
                    section_title="Tóm tắt",
                ),
            ],
        ),
    )

    messages = build_prompt_v2(
        "Theo context, quyết định chọn CEA hay CAS và việc theo dõi sau can thiệp đều phải cá thể hóa dựa trên những nhóm yếu tố nào?",
        pack,
        MockRouterOutput(),
        _coverage(),
    )
    user_msg = messages[-1]["content"]
    assert "phải liệt kê đủ các nhóm tiêu chí mà nguồn chính nêu" in user_msg
    assert "phải nêu cả hai" in user_msg
    assert "không được thu hẹp phần follow-up chỉ còn lịch tái khám đơn thuần" in user_msg
