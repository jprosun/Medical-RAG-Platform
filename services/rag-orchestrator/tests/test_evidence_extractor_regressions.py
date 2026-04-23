import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataclasses import dataclass

from app.evidence_extractor import _build_simple_evidence
from app.article_aggregator import ArticleGroup
from app.retriever import RetrievedChunk


def _make_chunk(chunk_id, text, section_title=""):
    return RetrievedChunk(
        id=chunk_id,
        text=text,
        score=0.85,
        metadata={"title": "Test", "section_title": section_title},
    )


def _make_article(chunks):
    return ArticleGroup(
        title="Test Article",
        title_norm="test article",
        chunks=chunks,
        max_score=0.9,
        avg_score=0.85,
        chunk_count=len(chunks),
    )


@dataclass
class MockRouterOutput:
    answer_style: str = "summary"


def test_focus_selection_merges_single_newlines_for_criteria_queries():
    article = _make_article(
        [
            _make_chunk(
                "c0",
                (
                    "Mục tiêu nghiên cứu:\n"
                    "Đánh giá tình trạng nhập viện của người bệnh trên lâm sàng và cận lâm sàng\n"
                    "cùng kết quả điều trị của người bệnh."
                ),
                "SUMMARY",
            ),
        ]
    )

    evidence = _build_simple_evidence(
        article,
        query="Theo nghiên cứu này, kết quả điều trị ở bệnh nhân suy tim mạn tính được đánh giá qua những chỉ số hoặc tiêu chí nào?",
        router_output=MockRouterOutput(),
    )

    claims = [claim.claim for claim in evidence.key_findings]
    assert any("tình trạng nhập viện" in claim and "lâm sàng và cận lâm sàng" in claim for claim in claims), claims


def test_focus_selection_keeps_follow_up_factors_for_grouped_factor_queries():
    article = _make_article(
        [
            _make_chunk(
                "c0",
                (
                    "Việc lựa chọn CEA hay CAS cần dựa trên mức độ hẹp, triệu chứng, nguy cơ quanh thủ thuật và kỳ vọng sống còn. "
                    "Sau can thiệp, điều trị nội khoa tối ưu, kiểm soát yếu tố nguy cơ tim mạch và theo dõi hình ảnh định kỳ vẫn giữ vai trò thiết yếu trong giảm nguy cơ tái hẹp và đột quỵ."
                ),
                "Tóm tắt",
            ),
        ]
    )

    evidence = _build_simple_evidence(
        article,
        query="Theo context, quyết định chọn CEA hay CAS và việc theo dõi sau can thiệp đều phải cá thể hóa dựa trên những nhóm yếu tố nào?",
        router_output=MockRouterOutput(),
    )

    claims = [claim.claim for claim in evidence.key_findings]
    assert any("mức độ hẹp" in claim and "kỳ vọng sống còn" in claim for claim in claims), claims
    assert any("điều trị nội khoa tối ưu" in claim and "theo dõi hình ảnh định kỳ" in claim for claim in claims), claims


def test_focus_selection_splits_follow_up_heading_from_medical_support_clause():
    article = _make_article(
        [
            _make_chunk(
                "c0",
                (
                    "Theo dõi sau mổ: o Bệnh nhân được hẹn tái khám sau mổ 1 tháng, 3 tháng, 6 tháng. "
                    "Điều trị nội khoa hỗ trợ: o Kiểm soát huyết áp, đái tháo đường, rối loạn lipid máu. "
                    "o Siêu âm duplex động mạch cảnh và chụp cắt lớp điện toán động mạch cảnh khi nghi ngờ hẹp tái phát."
                ),
                "Tóm tắt",
            ),
        ]
    )

    evidence = _build_simple_evidence(
        article,
        query="Theo context, quyết định chọn CEA hay CAS và việc theo dõi sau can thiệp đều phải cá thể hóa dựa trên những nhóm yếu tố nào?",
        router_output=MockRouterOutput(),
    )

    claims = [claim.claim for claim in evidence.key_findings]
    assert any("Kiểm soát huyết áp" in claim and "rối loạn lipid máu" in claim for claim in claims), claims
