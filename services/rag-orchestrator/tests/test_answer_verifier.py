from types import SimpleNamespace

from app.answer_verifier import should_verify_answer, verify_answer


def _coverage(mode="evidence_strong", level="high"):
    return SimpleNamespace(coverage_mode=mode, coverage_level=level)


def _router(policy="strict_rag"):
    return SimpleNamespace(answer_policy=policy)


def _evidence(source_count=1):
    primary = SimpleNamespace(title="Primary", raw_text="Evidence text")
    secondary = [
        SimpleNamespace(title=f"Secondary {idx}", raw_text="More evidence")
        for idx in range(max(0, source_count - 1))
    ]
    return SimpleNamespace(primary_source=primary, secondary_sources=secondary)


def _external(*ids):
    sources = [SimpleNamespace(id=source_id, title="External", snippet="", url="") for source_id in ids]
    return SimpleNamespace(used=bool(ids), sources=sources)


def test_should_verify_open_enriched_even_when_evidence_strong():
    assert should_verify_answer(
        "Giải thích nền tảng.",
        _coverage("evidence_strong"),
        _router("open_enriched"),
    )


def test_verifier_rejects_missing_external_source_id_without_llm():
    result = verify_answer(
        question="Q",
        answer="Một khuyến cáo cụ thể cần nguồn [E2].",
        evidence_pack=_evidence(),
        coverage=_coverage(),
        router_output=_router(),
        external_pack=_external("E1"),
        llm_client=None,
    )

    assert result.status == "revise"
    assert any("external citation [E2]" in issue for issue in result.issues)


def test_verifier_rejects_rag_citation_without_matching_source_without_llm():
    result = verify_answer(
        question="Q",
        answer="Claim này được gắn sai nguồn [3].",
        evidence_pack=_evidence(source_count=1),
        coverage=_coverage(),
        router_output=_router(),
        llm_client=None,
    )

    assert result.status == "revise"
    assert any("RAG citation [3]" in issue for issue in result.issues)


def test_verifier_rejects_risky_uncited_claim_without_llm():
    result = verify_answer(
        question="Q",
        answer="Tỷ lệ đáp ứng là 42% trong nhóm bệnh nhân này.",
        evidence_pack=_evidence(),
        coverage=_coverage(),
        router_output=_router(),
        llm_client=None,
    )

    assert result.status == "revise"
    assert any("has no citation" in issue for issue in result.issues)


def test_verifier_rejects_short_open_enriched_answer_without_llm():
    result = verify_answer(
        question="Vì sao cần sinh thiết?",
        answer="Cần sinh thiết để xác định bản chất mô học.",
        evidence_pack=_evidence(),
        coverage=_coverage("title_anchored"),
        router_output=_router("open_enriched"),
        llm_client=None,
    )

    assert result.status == "revise"
    assert any("too short" in issue for issue in result.issues)
