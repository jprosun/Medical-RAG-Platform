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
