import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.coverage_scorer import CoverageOutput
from app.evidence_extractor import CoverageScores, EvidencePack, PrimaryEvidence
from app.prompt import build_prompt_v2
from app.query_router import route_query


def test_theory_question_routes_to_open_enriched_professional_explainer():
    routed = route_query("Vì sao sinh thiết đầy đủ và hóa mô miễn dịch lại quan trọng?")

    assert routed.query_type == "professional_explainer"
    assert routed.answer_policy == "open_enriched"
    assert routed.retrieval_mode == "mechanistic_synthesis"


def test_real_topic_question_uses_open_enriched_not_article_centric():
    routed = route_query("Cac yeu to nguy co dai thao duong thai ky la gi?")

    assert routed.answer_policy == "open_enriched"
    assert routed.retrieval_mode != "article_centric"


def test_article_bound_question_stays_strict_rag():
    routed = route_query("Theo nghien cuu nay, ket qua chinh la gi?")

    assert routed.answer_policy == "strict_rag"
    assert routed.retrieval_mode == "article_centric"


def test_real_comparison_question_uses_topic_retrieval():
    routed = route_query("So sanh CEA va CAS trong hep dong mach canh: quyet dinh lua chon phu thuoc nhom yeu to nao?")

    assert routed.query_type == "comparative_synthesis"
    assert routed.answer_policy == "open_enriched"
    assert routed.retrieval_mode == "topic_summary"


def test_open_enriched_prompt_allows_background_without_fake_citation():
    routed = route_query("U diệp thể ác ở vú là gì và vì sao cần hóa mô miễn dịch?")
    evidence = EvidencePack(
        query_type=routed.query_type,
        primary_source=PrimaryEvidence(
            title="U diệp thể ác ở vú",
            raw_text="Tài liệu chính nêu vai trò sinh thiết và hóa mô miễn dịch trong phân biệt chẩn đoán.",
        ),
    )
    coverage = CoverageOutput(
        coverage_level="medium",
        scores=CoverageScores(direct_answerability=0.4),
        allow_external=True,
        max_external_sources=1,
        force_abstain_parts=[],
        coverage_mode="open_knowledge",
    )

    messages = build_prompt_v2("U diệp thể ác ở vú là gì?", evidence, routed, coverage)
    system = messages[0]["content"]
    user = messages[-1]["content"]

    assert "OPEN-KNOWLEDGE ENRICHED RAG" in system
    assert "không gắn citation" in user.lower()


def test_open_enriched_prompt_moves_short_conclusion_to_the_end():
    routed = route_query("So sanh CEA va CAS trong hep dong mach canh")
    evidence = EvidencePack(
        query_type=routed.query_type,
        primary_source=PrimaryEvidence(
            title="Can thiep dong mach canh",
            raw_text="Tai lieu neu cac yeu to can nhac khi lua chon can thiep dong mach canh.",
        ),
    )
    coverage = CoverageOutput(
        coverage_level="medium",
        scores=CoverageScores(direct_answerability=0.4),
        allow_external=True,
        max_external_sources=1,
        force_abstain_parts=[],
        coverage_mode="open_knowledge",
    )

    messages = build_prompt_v2("So sanh CEA va CAS trong hep dong mach canh", evidence, routed, coverage)
    user = messages[-1]["content"]

    assert user.rfind("## Kết luận ngắn") > user.rfind("## Nguồn tham khảo")
    assert "Không viết danh sách phủ định dài" in user
    assert "Không đặt 'Kết luận ngắn' ở đầu" in user
    assert "Không đưa câu kiểu 'tài liệu không đề cập/không cung cấp' vào phần Kết luận ngắn" in user
