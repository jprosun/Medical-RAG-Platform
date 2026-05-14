"""
Tests for Evidence Extractor v1.5.
Validates supporting_span, chunk_id extraction, and backward compatibility.
"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataclasses import dataclass
from app.evidence_extractor import (
    ClaimEvidence,
    _parse_extractor_response,
    _build_simple_evidence,
    EvidencePack,
    PrimaryEvidence,
)
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
    answer_style: str = "exact"


# ── ClaimEvidence v1.5 fields ────────────────────────────────────────

def test_claim_evidence_has_v15_fields():
    """ClaimEvidence should have supporting_span, chunk_id, section_title."""
    c = ClaimEvidence(
        claim="CK19 liên quan tái phát",
        supporting_span="Phân tích đa biến cho thấy CK19...",
        chunk_id="vmj_001_ketqua_chunk02",
        section_title="Kết quả",
    )
    assert c.supporting_span
    assert c.chunk_id
    assert c.section_title == "Kết quả"


def test_claim_evidence_backward_compat():
    """Old-style ClaimEvidence (just claim + support_text) should still work."""
    c = ClaimEvidence(claim="finding 1", support_text="from chunk")
    assert c.supporting_span == ""
    assert c.chunk_id == ""


# ── Parser: structured key_findings ──────────────────────────────────

def test_parse_structured_findings():
    """Parser should handle v1.5 structured key_findings with claim + supporting_span."""
    chunks = [
        _make_chunk("c0", "Chunk 0 text about population", "Đặt vấn đề"),
        _make_chunk("c1", "Chunk 1 text about results", "Kết quả"),
    ]
    article = _make_article(chunks)

    raw_response = '''{
        "source_type": "original_study",
        "population": "146 bệnh nhân ung thư tuyến giáp",
        "sample_size": "n=146",
        "design": "đoàn hệ hồi cứu",
        "setting": null,
        "intervention_or_exposure": null,
        "comparator": null,
        "outcomes": ["tái phát"],
        "key_findings": [
            {
                "claim": "CK19 liên quan với tái phát sớm",
                "supporting_span": "HR = 2.1, p = 0.03",
                "chunk_index": 1
            }
        ],
        "numbers": [{"metric": "HR", "value": "2.1", "unit": ""}],
        "limitations": [
            {
                "claim": "đơn trung tâm, cỡ mẫu nhỏ",
                "supporting_span": "Nghiên cứu có một số hạn chế"
            }
        ],
        "conclusion": "CK19 có giá trị tiên lượng"
    }'''

    evidence = _parse_extractor_response(raw_response, article)

    # Key findings with supporting_span
    assert len(evidence.key_findings) == 1
    finding = evidence.key_findings[0]
    assert finding.claim == "CK19 liên quan với tái phát sớm"
    assert finding.supporting_span == "HR = 2.1, p = 0.03"
    assert finding.chunk_id == "c1"  # chunk_index=1 → chunks[1].id

    # Limitations with supporting_span
    assert len(evidence.limitations) == 1
    lim = evidence.limitations[0]
    assert "đơn trung tâm" in lim.claim
    assert lim.supporting_span


def test_parse_plain_string_findings():
    """Parser should handle old-style plain string key_findings (backward compat)."""
    chunks = [_make_chunk("c0", "test chunk")]
    article = _make_article(chunks)

    raw_response = '''{
        "source_type": "review",
        "key_findings": ["finding 1", "finding 2"],
        "numbers": [],
        "limitations": ["limitation 1"]
    }'''

    evidence = _parse_extractor_response(raw_response, article)
    assert len(evidence.key_findings) == 2
    assert evidence.key_findings[0].claim == "finding 1"
    assert evidence.key_findings[0].supporting_span == ""  # no span for plain strings


# ── Simple extraction with chunk provenance ──────────────────────────

def test_simple_evidence_tracks_chunk_ids():
    """Simple regex extraction should track which chunk each number came from."""
    chunks = [
        _make_chunk("c0", "Background text without numbers"),
        _make_chunk("c1", "Kết quả cho thấy AUC = 0.82, p < 0.001, n = 250"),
    ]
    article = _make_article(chunks)

    evidence = _build_simple_evidence(article)

    # Numbers should be found
    assert len(evidence.numbers) > 0

    # At least one number should come from c1
    c1_numbers = [n for n in evidence.numbers if n.support_text == "c1"]
    assert len(c1_numbers) > 0, "Numbers from chunk c1 should have chunk_id"

    # Sample size should be found
    assert evidence.sample_size
    assert "250" in evidence.sample_size.text


def test_simple_evidence_no_numbers():
    """Simple extraction should handle chunks with no numbers."""
    chunks = [_make_chunk("c0", "Plain text about diseases without any statistics")]
    article = _make_article(chunks)

    evidence = _build_simple_evidence(article)
    assert len(evidence.numbers) == 0
    assert evidence.sample_size is None


def test_simple_evidence_extracts_direct_answer_spans_for_exact_numeric_query():
    chunks = [
        _make_chunk(
            "c0",
            (
                "Kết quả cho thấy 88,1% người bệnh cải thiện triệu chứng đau và 92,2% cải thiện "
                "một số hoạt động sinh hoạt."
            ),
            "Kết quả",
        ),
        _make_chunk(
            "c1",
            (
                "Tỷ lệ hồi phục chức năng vận động đạt 96,18%. "
                "Chỉ số Karnofsky được duy trì trên 80 điểm trong 3-6 tháng."
            ),
            "Kết quả",
        ),
    ]
    article = _make_article(chunks)

    evidence = _build_simple_evidence(
        article,
        query="Tỷ lệ hồi phục chức năng vận động là bao nhiêu và Karnofsky được duy trì trên 80 điểm trong bao lâu?",
        router_output=MockRouterOutput(),
    )

    spans = [span.supporting_span for span in evidence.direct_answer_spans]
    assert any("96,18%" in span for span in spans), spans
    assert any("Karnofsky" in span and "3-6 tháng" in span for span in spans), spans


def test_simple_evidence_extracts_query_focused_claims_for_summary_question():
    chunks = [
        _make_chunk(
            "c0",
            (
                "Biểu hiện lâm sàng ở bệnh nhân ghép thận mắc lao thường không điển hình, "
                "nhiều trường hợp là lao ngoài phổi hoặc lao lan tỏa. "
                "Rifampicin làm giảm nồng độ tacrolimus và cyclosporine, làm tăng nguy cơ thải ghép nên cần theo dõi và chỉnh liều chặt chẽ. "
                "Thuốc kháng lao cũng có thể gây độc tính gan."
            ),
            "Tóm tắt",
        ),
    ]
    article = _make_article(chunks)

    evidence = _build_simple_evidence(
        article,
        query="Ở bệnh nhân ghép thận nghi mắc lao, vì sao việc chẩn đoán và điều trị phải được cân nhắc cùng nhau thay vì tách rời?",
        router_output=MockRouterOutput(answer_style="summary"),
    )

    claims = [claim.claim for claim in evidence.key_findings]
    assert any("Rifampicin" in claim and "thải ghép" in claim for claim in claims), claims


def test_simple_evidence_extracts_study_design_span_for_exact_design_query():
    chunks = [
        _make_chunk(
            "c0",
            "Ket qua: ty le dieu tri thanh cong la 94,7% va tac dung phu chiem 16%.",
            "Ket qua",
        ),
        _make_chunk(
            "c1",
            (
                "Doi tuong va phuong phap nghien cuu: Nghien cuu mo ta cat ngang tren 75 benh nhan. "
                "Phuong phap chon mau thuan tien."
            ),
            "Doi tuong va phuong phap nghien cuu",
        ),
    ]
    article = _make_article(chunks)

    evidence = _build_simple_evidence(
        article,
        query="Nghien cuu nay su dung thiet ke nao va phuong phap chon mau ra sao?",
        router_output=MockRouterOutput(),
    )

    spans = [span.supporting_span for span in evidence.direct_answer_spans]
    assert any("mo ta cat ngang" in span.lower() for span in spans), spans
    assert any("chon mau thuan tien" in span.lower() for span in spans), spans


def test_simple_evidence_strips_metadata_wrappers_before_summary_focus_selection():
    chunks = [
        _make_chunk(
            "c0",
            (
                "Title: BỆNH VIỆN ĐA KHOA HUYỆN HOÀI ĐỨC Source: Tạp chí Y học Việt Nam "
                "Audience: clinician Body: Nghiên cứu đánh giá tình trạng nhập viện của người bệnh "
                "trên lâm sàng và cận lâm sàng cùng kết quả điều trị."
            ),
            "SUMMARY",
        ),
    ]
    article = _make_article(chunks)

    evidence = _build_simple_evidence(
        article,
        query="Theo nghiên cứu này, kết quả điều trị ở bệnh nhân suy tim mạn tính được đánh giá qua những chỉ số hoặc tiêu chí nào?",
        router_output=MockRouterOutput(answer_style="summary"),
    )

    claims = [claim.claim for claim in evidence.key_findings]
    assert any("lâm sàng và cận lâm sàng" in claim for claim in claims), claims
    assert all("Title:" not in claim for claim in claims), claims
