from __future__ import annotations

import json
import re
from dataclasses import dataclass, field


_LEGACY_RISKY_RE = re.compile(
    r"\b(\d+[.,]?\d*\s*%|p\s*[<>=]|OR|HR|RR|AUC|liều|lieu|phác đồ|phac do|guideline|hướng dẫn|huong dan|khuyến cáo|khuyen cao)\b",
    re.IGNORECASE,
)
_CITATION_RE = re.compile(r"\[(E?\d+)\]")
_RISKY_RE = re.compile(
    r"(\d+[.,]?\d*\s*%|p\s*[<>=]|"
    r"\b(?:OR|HR|RR|AUC)\b|dose|"
    r"li\u1ec1u\s+(?:thu\u1ed1c|d\u00f9ng|dung|khuy\u1ebfn\s+c\u00e1o|khuyen cao|\u0111i\u1ec1u\s+tr\u1ecb|dieu tri)|"
    r"lieu\s+(?:thuoc|dung|khuyen cao|dieu tri)|"
    r"ph\u00e1c\s+\u0111\u1ed3|phac do|"
    r"guideline|h\u01b0\u1edbng\s+d\u1eabn|huong dan|"
    r"khuy\u1ebfn\s+c\u00e1o|khuyen cao)",
    re.IGNORECASE,
)


@dataclass
class VerificationResult:
    enabled: bool = False
    status: str = "skipped"  # skipped | pass | revise | block | error
    issues: list[str] = field(default_factory=list)
    revised_answer: str = ""


def should_verify_answer(answer: str, coverage, router_output, external_pack=None) -> bool:
    mode = getattr(coverage, "coverage_mode", "")
    if mode != "evidence_strong":
        return True
    if getattr(router_output, "answer_policy", "strict_rag") == "open_enriched":
        return True
    if external_pack is not None and getattr(external_pack, "used", False):
        return True
    if len(answer.split()) > 800:
        return True
    return bool(_RISKY_RE.search(answer or ""))


def _evidence_text(evidence_pack, external_pack=None) -> str:
    parts: list[str] = []
    primary = getattr(evidence_pack, "primary_source", None)
    if primary:
        parts.append(getattr(primary, "title", "") or "")
        parts.append(getattr(primary, "raw_text", "") or "")
        for finding in getattr(primary, "key_findings", []) or []:
            parts.append(getattr(finding, "claim", "") or "")
            parts.append(getattr(finding, "supporting_span", "") or "")
    for sec in getattr(evidence_pack, "secondary_sources", []) or []:
        parts.append(getattr(sec, "title", "") or "")
        parts.append(getattr(sec, "raw_text", "") or "")
    if external_pack:
        for source in getattr(external_pack, "sources", []) or []:
            parts.append(source.title)
            parts.append(source.snippet)
            parts.append(source.url)
    return "\n".join(part for part in parts if part)


def _rag_source_count(evidence_pack) -> int:
    count = 0
    primary = getattr(evidence_pack, "primary_source", None)
    if primary and (getattr(primary, "title", "") or getattr(primary, "raw_text", "")):
        count += 1
    count += len(getattr(evidence_pack, "secondary_sources", []) or [])
    return count


def _has_any_citation(answer: str) -> bool:
    return bool(_CITATION_RE.search(answer or ""))


def verify_answer(
    *,
    question: str,
    answer: str,
    evidence_pack,
    coverage,
    router_output,
    external_pack=None,
    llm_client=None,
) -> VerificationResult:
    citations = set(_CITATION_RE.findall(answer or ""))
    external_ids = {source.id for source in getattr(external_pack, "sources", [])} if external_pack else set()
    rag_source_count = _rag_source_count(evidence_pack)
    issues: list[str] = []
    for citation in citations:
        if citation.startswith("E") and citation not in external_ids:
            issues.append(f"external citation [{citation}] has no matching source")
            continue
        if citation.isdigit() and (rag_source_count <= 0 or int(citation) > rag_source_count):
            issues.append(f"RAG citation [{citation}] has no matching evidence source")

    if _RISKY_RE.search(answer or "") and not _has_any_citation(answer):
        issues.append("numeric/guideline/dose/regimen-like claim has no citation")

    if (
        getattr(router_output, "answer_policy", "strict_rag") == "open_enriched"
        and len((answer or "").split()) < 180
    ):
        issues.append("answer too short for open_enriched professional explainer")

    needs_llm_verification = should_verify_answer(answer, coverage, router_output, external_pack)
    if not needs_llm_verification:
        if issues:
            return VerificationResult(enabled=True, status="revise", issues=issues)
        return VerificationResult(enabled=False, status="skipped")

    if llm_client is None:
        if issues:
            return VerificationResult(enabled=True, status="revise", issues=issues)
        return VerificationResult(enabled=True, status="pass")

    verifier_prompt = {
        "question": question,
        "answer": answer,
        "coverage_level": getattr(coverage, "coverage_level", ""),
        "coverage_mode": getattr(coverage, "coverage_mode", ""),
        "answer_policy": getattr(router_output, "answer_policy", ""),
        "evidence": _evidence_text(evidence_pack, external_pack)[:12000],
        "checks": [
            "citation attached to unsupported claim",
            "fabricated numeric, guideline, dose, or regimen claim only when the claim is specific and source-dependent",
            "unsafe personalized medical advice, not general education about diagnosis or clinical reasoning",
            "answer too short for professional explainer",
        ],
        "revision_rules": [
            "When answer_policy is open_enriched, preserve the explanatory structure and useful background knowledge.",
            "Do not shorten a professional explainer into one short paragraph.",
            "Remove or generalize only unsafe or unsupported specific claims.",
            "If the answer is broadly safe and only contains general medical education, return status pass.",
        ],
        "return_json": {
            "status": "pass|revise|block",
            "issues": ["..."],
            "revised_answer": "optional revised answer when status=revise",
        },
    }
    try:
        raw = llm_client.generate(
            [
                {"role": "system", "content": "Bạn là verifier RAG y khoa. Trả về JSON hợp lệ, không markdown."},
                {"role": "user", "content": json.dumps(verifier_prompt, ensure_ascii=False)},
            ],
            max_tokens=2200,
            temperature=0.0,
            attempt_budget=1,
        )
        start = raw.find("{")
        end = raw.rfind("}")
        data = json.loads(raw[start:end + 1]) if start >= 0 and end >= start else {}
    except Exception as exc:
        return VerificationResult(enabled=True, status="error", issues=[f"verifier_error:{type(exc).__name__}"] + issues)

    status = str(data.get("status") or "pass").lower()
    if status not in {"pass", "revise", "block"}:
        status = "pass"
    merged_issues = issues + [str(item) for item in data.get("issues", []) if str(item).strip()]
    revised_answer = str(data.get("revised_answer") or "").strip()
    if (
        status == "revise"
        and revised_answer
        and getattr(router_output, "answer_policy", "strict_rag") == "open_enriched"
    ):
        original_words = len((answer or "").split())
        revised_words = len(revised_answer.split())
        min_revised_words = min(450, max(220, int(original_words * 0.45)))
        if revised_words < min_revised_words:
            merged_issues.append("verifier revised_answer too short; original answer preserved")
            revised_answer = ""
            status = "pass"
    return VerificationResult(
        enabled=True,
        status=status,
        issues=merged_issues,
        revised_answer=revised_answer,
    )
