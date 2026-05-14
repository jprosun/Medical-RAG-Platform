from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class AnswerPlan:
    enabled: bool = False
    status: str = "disabled"
    sections: list[str] = field(default_factory=list)
    must_cover: list[str] = field(default_factory=list)
    rag_supported_claims: list[str] = field(default_factory=list)
    open_knowledge_topics: list[str] = field(default_factory=list)
    risky_claims_need_source: list[str] = field(default_factory=list)


def should_plan_answer(router_output, coverage) -> bool:
    answer_policy = getattr(router_output, "answer_policy", "strict_rag")
    coverage_mode = getattr(coverage, "coverage_mode", "")
    return answer_policy == "open_enriched" or coverage_mode in {"open_knowledge", "title_anchored"}


def build_answer_plan(question: str, evidence_pack, coverage, router_output, llm_client=None) -> AnswerPlan:
    if not should_plan_answer(router_output, coverage):
        return AnswerPlan(enabled=False)
    if llm_client is None:
        return AnswerPlan(
            enabled=True,
            status="heuristic",
            sections=[
                "Kết luận trực tiếp",
                "Giải thích nền tảng",
                "Phân tích chuyên sâu",
                "Bằng chứng từ tài liệu truy hồi",
                "Ý nghĩa thực hành và lưu ý an toàn",
            ],
            must_cover=[question],
            open_knowledge_topics=getattr(coverage, "unsupported_concepts", []) or [],
            risky_claims_need_source=[],
        )

    ev = getattr(evidence_pack, "primary_source", None)
    evidence_preview = (getattr(ev, "raw_text", "") or "")[:3500]
    system = (
        "Bạn là answer planner cho RAG y khoa. Trả về JSON hợp lệ, không markdown. "
        "Không viết câu trả lời cuối cùng."
    )
    user = {
        "question": question,
        "coverage_level": getattr(coverage, "coverage_level", ""),
        "coverage_mode": getattr(coverage, "coverage_mode", ""),
        "primary_title": getattr(ev, "title", ""),
        "evidence_preview": evidence_preview,
        "required_json": {
            "sections": ["..."],
            "must_cover": ["..."],
            "rag_supported_claims": ["..."],
            "open_knowledge_topics": ["..."],
            "risky_claims_need_source": ["..."],
        },
    }
    try:
        raw = llm_client.generate(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
            ],
            max_tokens=800,
            temperature=0.0,
            attempt_budget=1,
        )
        start = raw.find("{")
        end = raw.rfind("}")
        data: dict[str, Any] = json.loads(raw[start:end + 1]) if start >= 0 and end >= start else {}
    except Exception as exc:
        return AnswerPlan(enabled=True, status=f"planner_error:{type(exc).__name__}")

    return AnswerPlan(
        enabled=True,
        status="ok",
        sections=[str(x) for x in data.get("sections", []) if str(x).strip()],
        must_cover=[str(x) for x in data.get("must_cover", []) if str(x).strip()],
        rag_supported_claims=[str(x) for x in data.get("rag_supported_claims", []) if str(x).strip()],
        open_knowledge_topics=[str(x) for x in data.get("open_knowledge_topics", []) if str(x).strip()],
        risky_claims_need_source=[str(x) for x in data.get("risky_claims_need_source", []) if str(x).strip()],
    )


def format_answer_plan_for_prompt(plan: AnswerPlan) -> str:
    if not plan.enabled:
        return ""
    lines = [f"ANSWER PLAN status={plan.status}"]
    if plan.sections:
        lines.append("Sections: " + "; ".join(plan.sections))
    if plan.must_cover:
        lines.append("Must cover: " + "; ".join(plan.must_cover[:8]))
    if plan.rag_supported_claims:
        lines.append("RAG-supported claims: " + "; ".join(plan.rag_supported_claims[:8]))
    if plan.open_knowledge_topics:
        lines.append("Open-knowledge topics allowed: " + "; ".join(plan.open_knowledge_topics[:8]))
    if plan.risky_claims_need_source:
        lines.append("Risky claims need source: " + "; ".join(plan.risky_claims_need_source[:8]))
    return "\n".join(lines)
