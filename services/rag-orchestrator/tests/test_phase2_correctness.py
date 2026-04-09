"""
Tests for Phase 2 Evidence Correctness modules.
Validates Mini Normalizer and Conflict Detector heuristics.
"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.evidence_extractor import EvidencePack, PrimaryEvidence, NumberEvidence, ClaimEvidence
from app.evidence_normalizer import normalize_evidence
from app.conflict_detector import detect_conflicts


def _make_pack(primary_claims, sec_claims_list, primary_is_guideline=False):
    """Helper to mock EvidencePack."""
    pack = EvidencePack(
        query_type="test",
        primary_source=PrimaryEvidence(
            title="Primary",
            source_type="guideline" if primary_is_guideline else "original_study",
            raw_text="...",
            key_findings=[ClaimEvidence(claim=c) for c in primary_claims]
        ),
        secondary_sources=[]
    )
    for i, claims in enumerate(sec_claims_list):
        pack.secondary_sources.append(PrimaryEvidence(
            title=f"Secondary {i}",
            source_type="original_study",
            raw_text="...",
            key_findings=[ClaimEvidence(claim=c) for c in claims]
        ))
    return pack


# ── Tests for Mini Normalizer ──────────────────────────────────────

def test_normalizer_metric_names():
    pack = EvidencePack(
        query_type="test",
        primary_source=PrimaryEvidence(
            title="test",
            raw_text="...",
            numbers=[
                NumberEvidence(metric="Hazard ratio", value="1.5"),
                NumberEvidence(metric="tỉ số số chênh", value="2.0"),
                NumberEvidence(metric="diện tích dưới đường cong", value="0.8"),
                NumberEvidence(metric="giá trị p", value="0.01"),
            ]
        )
    )
    norm = normalize_evidence(pack)
    metrics = [n.metric for n in norm.primary_source.numbers]
    assert metrics == ["HR", "OR", "AUC", "p-value"]


def test_normalizer_polarity_tags():
    pack = _make_pack(
        primary_claims=[
            "Thuốc làm tăng nguy cơ tử vong",
            "Thuốc cải thiện triệu chứng",
        ],
        sec_claims_list=[
            ["Không thấy sự khác biệt có ý nghĩa"]
        ]
    )
    norm = normalize_evidence(pack)

    p_tags = [getattr(c, "claim_type", None) for c in norm.primary_source.key_findings]
    assert p_tags == ["NEGATIVE_OUTCOME", "POSITIVE_OUTCOME"]

    s_tags = [getattr(c, "claim_type", None) for c in norm.secondary_sources[0].key_findings]
    assert s_tags == ["NEUTRAL_OUTCOME"]


def test_normalizer_recommendation_tags():
    pack = _make_pack(
        primary_claims=["Khuyến cáo mạnh nên sử dụng"],
        sec_claims_list=[]
    )
    norm = normalize_evidence(pack)
    p_tags = [getattr(c, "claim_type", None) for c in norm.primary_source.key_findings]
    assert p_tags == ["STRONG_RECOMMENDATION"]


# ── Tests for Conflict Detector ─────────────────────────────────────

def test_detect_polarity_mismatch():
    pack = _make_pack(
        primary_claims=["Thuốc làm giảm nguy cơ"], # POSITIVE_OUTCOME
        sec_claims_list=[["Thuốc làm tăng nguy cơ biến chứng"]] # NEGATIVE_OUTCOME
    )
    pack = normalize_evidence(pack)
    pack = detect_conflicts(pack)
    
    assert hasattr(pack, "conflict_notes")
    assert len(pack.conflict_notes) == 1
    assert "Trái ngược" in pack.conflict_notes[0]


def test_detect_significant_vs_neutral():
    pack = _make_pack(
        primary_claims=["Phương pháp mới an toàn"], # POSITIVE_OUTCOME
        sec_claims_list=[["Không có sự khác biệt giữa hai nhóm"]] # NEUTRAL_OUTCOME
    )
    pack = normalize_evidence(pack)
    pack = detect_conflicts(pack)
    
    assert len(pack.conflict_notes) == 1
    assert "Có/Không ý nghĩa" in pack.conflict_notes[0]


def test_detect_guideline_study_mismatch():
    # Guideline recommends strongly, but study says negative outcome
    pack = _make_pack(
        primary_claims=["Khuyến cáo mạnh phẫu thuật"], # STRONG_RECOMMENDATION
        sec_claims_list=[["Phẫu thuật tăng nguy cơ tai biến"]], # NEGATIVE_OUTCOME
        primary_is_guideline=True
    )
    pack = normalize_evidence(pack)
    pack = detect_conflicts(pack)
    
    assert len(pack.conflict_notes) == 1
    assert "Khuyến cáo mạnh" in pack.conflict_notes[0]

    # Guideline against, but study says positive
    pack2 = _make_pack(
        primary_claims=["Chống chỉ định dùng thuốc A"], # NEGATIVE_RECOMMENDATION
        sec_claims_list=[["Thuốc A đem lại lợi ích sống còn"]], # POSITIVE_OUTCOME
        primary_is_guideline=True
    )
    pack2 = normalize_evidence(pack2)
    pack2 = detect_conflicts(pack2)
    assert len(pack2.conflict_notes) == 1
    assert "Chống chỉ định" in pack2.conflict_notes[0]


def test_no_conflicts():
    pack = _make_pack(
        primary_claims=["Cải thiện rõ rệt"], # POSITIVE
        sec_claims_list=[["Hiệu quả bảo vệ cao"]] # POSITIVE
    )
    pack = normalize_evidence(pack)
    pack = detect_conflicts(pack)
    
    assert len(pack.conflict_notes) == 0
