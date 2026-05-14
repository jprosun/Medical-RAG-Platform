"""
Evidence Sufficiency Scorer v2
===============================
Scores how well the evidence pack answers the query per review.md §7.
Pure heuristic — no LLM call.

v2 improvements:
  - query-type-specific evidence requirements
  - source quality scoring using trust_tier
  - missing_requirements tracking
  - confidence_ceiling for prompt control

Coverage levels:
  high   → ingest-only answer, no external needed
  medium → answer possible but missing some elements
  low    → insufficient evidence, must warn user
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .query_router import RouterOutput
from .evidence_extractor import EvidencePack, CoverageScores
from .article_aggregator import _extract_keywords


# ── Vietnamese medical compound term extraction ──────────────────

# Common Vietnamese medical compound terms (bigrams/trigrams)
_VN_MEDICAL_COMPOUNDS = {
    "xơ vữa", "động mạch", "đái tháo", "tháo đường", "bệnh thận",
    "thận mạn", "ung thư", "suy tim", "suy thận", "suy gan",
    "viêm mạn", "viêm cấp", "miễn dịch", "tự miễn", "nhiễm trùng",
    "nhiễm khuẩn", "vi tuần", "tuần hoàn", "nội tiết", "chuyển hoá",
    "thần kinh", "tế bào", "kháng thể", "kháng sinh", "cytokine",
    "đông máu", "huyết áp", "tim mạch", "đột quỵ", "nhồi máu",
    "tiểu đường", "gan nhiễm", "phổi tắc", "xơ hoá", "nội mô",
    "dự trữ", "sinh lý", "sửa chữa", "chẩn đoán", "điều trị",
    "can thiệp", "phát hiện", "tiên lượng", "dịch tễ", "lâm sàng",
    "cơ chế", "bệnh sinh", "yếu tố", "nguy cơ", "tỷ lệ",
    "overdiagnosis", "overtreatment", "checkpoint", "frailty",
    "burnout", "sepsis", "vaccine", "apoptosis",
    "sa sút", "trầm cảm", "lo âu", "kiệt quệ", "suy kiệt",
    "hội tụ", "bệnh mạn", "mạn tính", "cấp tính",
    "kiểm soát", "chất lượng", "vệ sinh", "thời gian",
}

_VN_STOPS_BIGRAM = {
    "có thể", "và các", "trong đó", "của các", "được các",
    "cho các", "với các", "là một", "như một", "ở các",
    "hay không", "tại sao", "thế nào", "bao gồm",
}

_GENERIC_SCOPE_CONCEPTS = {
    "can thiệp", "yếu tố", "quyết định", "theo dõi", "điều trị",
    "chẩn đoán", "kết quả", "thời gian", "kiểm soát", "chất lượng",
}


def _extract_bigram_concepts(text: str) -> list:
    """
    Extract meaningful Vietnamese compound concepts from text.
    Returns list of bigram/trigram concepts, not single syllables.
    
    Vietnamese medical terms are typically 2-4 syllable compounds:
      'xơ vữa động mạch', 'đái tháo đường', 'bệnh thận mạn'
    """
    import unicodedata
    t = unicodedata.normalize('NFC', text.lower().strip())
    # Clean
    t = re.sub(r'["\'\[\](){}?!,;:…]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    
    words = t.split()
    concepts = []
    
    # Extract bigrams that match known medical compounds
    for i in range(len(words) - 1):
        bigram = f"{words[i]} {words[i+1]}"
        if bigram in _VN_MEDICAL_COMPOUNDS:
            concepts.append(bigram)
    
    # Also extract trigrams for 3-word terms
    for i in range(len(words) - 2):
        trigram = f"{words[i]} {words[i+1]} {words[i+2]}"
        # Check if it's a known compound or contains medical terms
        if trigram in {"đái tháo đường", "xơ vữa động", "vữa động mạch",
                       "bệnh thận mạn", "suy đa tạng", "viêm mạn tính",
                       "suy đa cơ", "bệnh tim bẩm", "tim bẩm sinh",
                       "nhiễm trùng kéo", "kiểm soát nhiễm", "miễn dịch ung",
                       "ức chế miễn", "điểm kiểm soát", "chất lượng xét",
                       "vi sinh vật", "sinh vật chí", "phát hiện sớm",
                       "can thiệp sớm", "chẩn đoán sớm"}:
            concepts.append(trigram)
    
    # Also pick up single English/medical loanwords that are themselves concepts
    for w in words:
        if w in {"sepsis", "overdiagnosis", "overtreatment", "checkpoint",
                 "frailty", "burnout", "vaccine", "apoptosis", "cytokine",
                 "biomarker", "pathway"}:
            concepts.append(w)
    
    # Deduplicate preserving order
    seen = set()
    unique = []
    for c in concepts:
        if c not in seen and c not in _VN_STOPS_BIGRAM:
            seen.add(c)
            unique.append(c)
    
    return unique


def _filter_generic_scope_concepts(concepts: list) -> list:
    return [concept for concept in concepts if concept not in _GENERIC_SCOPE_CONCEPTS]

@dataclass
class CoverageOutput:
    """Coverage assessment result."""
    coverage_level: str  # high | medium | low
    scores: CoverageScores
    allow_external: bool
    max_external_sources: int
    force_abstain_parts: list  # fields where we must say "không đủ dữ liệu"
    # v2 fields
    missing_requirements: list = None  # what evidence is missing for this query type
    confidence_ceiling: str = "high"   # high | moderate | low — max certainty allowed
    # Phase 4 fields
    unsupported_concepts: list = None   # concepts user asked about but evidence doesn't cover
    concept_evidence_gap: bool = False  # True if significant gap detected
    allowed_answer_scope: str = ""      # what evidence actually supports
    coverage_mode: str = "evidence_strong"  # evidence_strong | title_anchored | open_knowledge | retrieval_failed

    def __post_init__(self):
        if self.missing_requirements is None:
            self.missing_requirements = []
        if self.unsupported_concepts is None:
            self.unsupported_concepts = []


def _asks_for_numeric_value(query: str) -> bool:
    q = (query or "").lower()
    return any(
        marker in q
        for marker in (
            "bao nhiêu", "bao nhieu", "tỷ lệ", "ty le", "phần trăm", "phan tram",
            "karnofsky", "auc", " hr ", " or ", " rr ", " ci ", " điểm ", " diem ",
        )
    )


def score_coverage(
    evidence_pack: EvidencePack,
    router_output: RouterOutput,
    query: str = "",
) -> CoverageOutput:
    """
    Score how well evidence covers the query.
    All heuristic, zero LLM calls.
    """
    ev = evidence_pack.primary_source
    scores = CoverageScores()
    abstain_parts = []
    answer_style = getattr(router_output, "answer_style", "")

    # ── 1. Direct answerability ──────────────────────────────────────
    # How many key_findings were extracted?
    query_kw = _extract_keywords(query) if query else set()
    if ev.key_findings:
        findings_text = " ".join(f.claim for f in ev.key_findings)
        if query_kw:
            findings_kw = _extract_keywords(findings_text)
            overlap = len(query_kw & findings_kw)
            scores.direct_answerability = min(overlap / max(len(query_kw), 1), 1.0)
        else:
            scores.direct_answerability = 0.7  # has findings but can't compare
    elif ev.raw_text:
        # Lightweight flows often rely on raw text only; score overlap instead
        # of hard-capping them at medium forever.
        if query_kw:
            raw_kw = _extract_keywords(ev.raw_text[:4000])
            overlap = len(query_kw & raw_kw)
            overlap_score = min(overlap / max(len(query_kw), 1), 1.0)
            scores.direct_answerability = max(0.45, overlap_score)
        else:
            scores.direct_answerability = 0.6
    else:
        scores.direct_answerability = 0.0

    # ── 2. Numeric coverage ──────────────────────────────────────────
    if router_output.requires_numbers:
        if answer_style == "exact" and not _asks_for_numeric_value(query):
            scores.numeric_coverage = 1.0
        elif answer_style == "bounded_partial" and not _asks_for_numeric_value(query):
            scores.numeric_coverage = 1.0
        else:
            if ev.numbers and len(ev.numbers) >= 2:
                scores.numeric_coverage = 1.0
            elif ev.numbers:
                scores.numeric_coverage = 0.5
            else:
                # Check raw text for numbers
                num_pattern = re.compile(r'\d+[.,]?\d*\s*%|p\s*[<>=]|OR|HR|AUC|n\s*=', re.IGNORECASE)
                if ev.raw_text and num_pattern.search(ev.raw_text):
                    scores.numeric_coverage = 0.3
                else:
                    scores.numeric_coverage = 0.0
                    abstain_parts.append("numeric_data")
    else:
        scores.numeric_coverage = 1.0  # not required

    # ── 3. Methods coverage ──────────────────────────────────────────
    methods_present = sum([
        ev.design is not None,
        ev.population is not None,
        ev.sample_size is not None,
        ev.setting is not None,
    ])
    scores.methods_coverage = methods_present / 4.0

    # ── 4. Limitations coverage ──────────────────────────────────────
    if router_output.requires_limitations:
        if ev.limitations:
            scores.limitations_coverage = 1.0
        else:
            # Check raw text for limitation keywords
            lim_kw = ["hạn chế", "limitation", "bias", "thiếu", "chưa", "cỡ mẫu nhỏ"]
            if ev.raw_text and any(kw in ev.raw_text.lower() for kw in lim_kw):
                scores.limitations_coverage = 0.5
            else:
                scores.limitations_coverage = 0.0
                abstain_parts.append("limitations")
    else:
        scores.limitations_coverage = 1.0  # not required

    # ── 5. Conflict risk ─────────────────────────────────────────────
    # Secondary sources alone should not force a disclaimer; reserve heavier
    # penalties for explicit conflict detection later in the pipeline.
    if evidence_pack.secondary_sources:
        scores.conflict_risk = 0.05
    else:
        scores.conflict_risk = 0.0

    # ── Compute overall level ──────────────────────────────────────
    # Weighted average of applicable scores
    if answer_style == "exact":
        key_scores = [
            scores.direct_answerability,
            scores.numeric_coverage,
        ]
    elif answer_style in {"summary", "bounded_partial"}:
        key_scores = [
            scores.direct_answerability,
            scores.numeric_coverage,
            1.0,
        ]
    else:
        key_scores = [
            scores.direct_answerability,
            scores.numeric_coverage,
            scores.methods_coverage if evidence_pack.extractor_used else 0.5,
            scores.limitations_coverage,
        ]
    avg_score = sum(key_scores) / len(key_scores)

    if avg_score >= 0.7 and scores.direct_answerability >= 0.5:
        level = "high"
    elif avg_score >= 0.4:
        level = "medium"
    else:
        level = "low"

    # ── Query-type-specific requirements (NEW v2) ────────────────────
    missing_reqs = []
    confidence_ceiling = "high"

    # Source quality check
    primary_tier = 3
    if ev.title:  # has a primary source
        # Check chunks for trust_tier metadata
        for chunk in (evidence_pack.primary_source.key_findings or []):
            pass  # tier comes from article metadata, not evidence pack directly

    query_type = router_output.query_type

    if query_type == "guideline_comparison":
        # Needs a guideline-quality source
        src_type = ev.source_type or ""
        if src_type and src_type not in ("guideline", "meta_analysis"):
            missing_reqs.append("guideline_source_preferred")
            if level == "high":
                confidence_ceiling = "moderate"

    elif query_type == "study_result_extraction":
        # Needs design + population + numbers
        if answer_style == "exact":
            if _asks_for_numeric_value(query):
                if not ev.numbers and not re.search(r'\d+[.,]?\d*\s*%', ev.raw_text or ""):
                    missing_reqs.append("numeric_findings")
            if scores.direct_answerability < 0.35:
                missing_reqs.append("direct_answer_span")
            if len(missing_reqs) >= 1:
                confidence_ceiling = "moderate"
            if len(missing_reqs) >= 2:
                confidence_ceiling = "low"
        else:
            if not ev.design:
                missing_reqs.append("study_design")
            if not ev.population:
                missing_reqs.append("study_population")
            if not ev.numbers or len(ev.numbers) < 1:
                missing_reqs.append("numeric_findings")
            if len(missing_reqs) >= 2:
                confidence_ceiling = "moderate"
            if len(missing_reqs) >= 3:
                confidence_ceiling = "low"

    elif query_type == "research_appraisal":
        # Needs methods + limitations
        if not ev.design:
            missing_reqs.append("study_design")
        if not ev.limitations:
            missing_reqs.append("limitations")
        if scores.methods_coverage < 0.5:
            missing_reqs.append("methods_detail")
        if len(missing_reqs) >= 2:
            confidence_ceiling = "moderate"

    elif query_type == "teaching_explainer":
        # Lower bar: just needs content
        if scores.direct_answerability < 0.3:
            missing_reqs.append("topic_content")
            confidence_ceiling = "low"

    elif query_type == "comparative_synthesis":
        # Needs multiple sources
        retrieval_mode = getattr(router_output, "retrieval_mode", "")
        if retrieval_mode != "article_centric" and not evidence_pack.secondary_sources:
            missing_reqs.append("comparison_sources")
            confidence_ceiling = "moderate"

    # Downgrade level if many requirements missing
    if len(missing_reqs) >= 3 and level == "high":
        level = "medium"
    if len(missing_reqs) >= 4:
        level = "low"

    # Conflict penalty
    if scores.conflict_risk > 0.1 and confidence_ceiling == "high":
        confidence_ceiling = "moderate"

    # ── Gating policy ────────────────────────────────────────────
    allow_external = level != "high"
    max_external = 0 if level == "high" else (1 if level == "medium" else 2)

    # Update the evidence pack coverage
    evidence_pack.coverage = scores

    # ── Phase 4: Concept-Evidence Gap Detection ──────────────────
    # Fix: Vietnamese is monosyllabic — use bigrams for concept extraction
    unsupported_concepts = []
    concept_evidence_gap = False
    allowed_answer_scope = ""

    if query:
        # Extract bigram concepts from query (Vietnamese compound terms)
        query_bigrams = _extract_bigram_concepts(query)
        
        # Gather all evidence text
        evidence_texts = []
        if ev.raw_text:
            evidence_texts.append(ev.raw_text)
        for finding in (ev.key_findings or []):
            evidence_texts.append(finding.claim)
        for sec in evidence_pack.secondary_sources:
            if sec.raw_text:
                evidence_texts.append(sec.raw_text)
            for finding in (sec.key_findings or []):
                evidence_texts.append(finding.claim)
        
        all_evidence_text = " ".join(evidence_texts).lower()
        
        # Check which bigram concepts are NOT mentioned in evidence
        supported = []
        for concept in query_bigrams:
            if concept.lower() in all_evidence_text:
                supported.append(concept)
            else:
                unsupported_concepts.append(concept)
        
        supported = _filter_generic_scope_concepts(supported)
        unsupported_concepts = _filter_generic_scope_concepts(unsupported_concepts)

        # Cap unsupported at max 7 to avoid prompt bloat
        unsupported_concepts = unsupported_concepts[:7]
        
        # Build allowed_answer_scope from what IS covered
        if supported:
            allowed_answer_scope = ", ".join(supported[:10])
        
        # Flag gap if significant portion unsupported
        total = len(query_bigrams) if query_bigrams else 1
        unsupported_ratio = len(unsupported_concepts) / total if total else 0.0
        if answer_style == "bounded_partial":
            gap_threshold = 0.35
        else:
            gap_threshold = 0.6
        if unsupported_concepts and supported and unsupported_ratio <= gap_threshold:
            concept_evidence_gap = False
        elif unsupported_concepts and unsupported_ratio > gap_threshold:
            concept_evidence_gap = True
            if confidence_ceiling == "high":
                confidence_ceiling = "moderate"

    answer_policy = getattr(router_output, "answer_policy", "strict_rag")
    if not ev.title and not ev.raw_text:
        coverage_mode = "retrieval_failed"
    elif level == "high" and not concept_evidence_gap:
        coverage_mode = "evidence_strong"
    elif ev.title and ev.raw_text and scores.direct_answerability >= 0.25:
        coverage_mode = "title_anchored"
    elif answer_policy == "open_enriched" or allow_external:
        coverage_mode = "open_knowledge"
    else:
        coverage_mode = "retrieval_failed"

    return CoverageOutput(
        coverage_level=level,
        scores=scores,
        allow_external=allow_external,
        max_external_sources=max_external,
        force_abstain_parts=abstain_parts,
        missing_requirements=missing_reqs,
        confidence_ceiling=confidence_ceiling,
        unsupported_concepts=unsupported_concepts,
        concept_evidence_gap=concept_evidence_gap,
        allowed_answer_scope=allowed_answer_scope,
        coverage_mode=coverage_mode,
    )
