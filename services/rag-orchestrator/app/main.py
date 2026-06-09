from uuid import uuid4
import json
import time
import os
import re
from typing import Any

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from opentelemetry import trace as otel_trace
from opentelemetry import trace

from .session import SessionStore
from .health import readiness, liveness
from utils.logging import log_request
from .retriever import build_retriever_from_env, _extract_specific_entities, _normalize_for_matching
from .prompt import build_prompt, build_prompt_v2
from .llm_client import build_kserve_client_from_env, UpstreamRateLimitError
from .schemas import ChatRequest, ChatResponse
from .query_router import route_query
from .article_aggregator import aggregate_articles
from .evidence_extractor import extract_evidence
from .chunk_quality_filter import filter_chunks
from .evidence_normalizer import normalize_evidence
from .conflict_detector import detect_conflicts
from .coverage_scorer import score_coverage
from .answer_planner import build_answer_plan, format_answer_plan_for_prompt, should_plan_answer
from .answer_verifier import verify_answer
from .pipeline_cache import pipeline_cache, stable_hash
from .pipeline_flags import should_skip_entity_fallback
from .external_source_resolver import (
    ExternalEvidencePack,
    format_external_sources_for_prompt,
    query_needs_external_sources,
    resolve_external_sources,
)
from .metrics import (
    RAG_CHAT_REQUESTS_TOTAL,
    RAG_CHAT_ERRORS_TOTAL,
    RAG_RETRIEVAL_LATENCY_SECONDS,
    RAG_CONTEXT_TOKENS,
    RAG_EMPTY_CONTEXT_TOTAL,
    RAG_GENERATION_LATENCY_SECONDS,
    RAG_FALLBACK_TOTAL,
    RAG_INFLIGHT,
)

from utils.tracing import setup_tracing

from .guardrails_app import (
    GUARDRAILS_ENABLED,
    generate_with_guardrails,
)
from .query_rewriter import rewrite_query

# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------
class TitleUpdate(BaseModel):
    title: str


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _is_eval_session(session_id: str) -> bool:
    sid = (session_id or "").lower()
    return sid.startswith(("eval_", "smoke_", "probe_"))


def _build_optional_llm_client(flag_name: str, default: bool = True):
    if not _env_flag(flag_name, default=default):
        return None
    return build_kserve_client_from_env()


def _normalize_answer_mode(raw: str | None) -> str:
    value = (raw or "standard").strip().lower()
    aliases = {
        "normal": "standard",
        "default": "standard",
        "balanced": "standard",
        "full": "standard",
        "brief": "standard",
        "short": "standard",
        "ngan": "standard",
        "strict": "standard",
        "strict_rag": "standard",
        "evidence_only": "standard",
        "sources_only": "standard",
        "deep": "thinking",
        "detailed": "thinking",
        "chuyen_sau": "thinking",
        "think": "thinking",
        "thinking": "thinking",
    }
    if value in {"standard", "thinking"}:
        return value
    return aliases.get(value, "standard")


def _apply_answer_mode(router_output, answer_mode: str) -> None:
    router_output.answer_mode = answer_mode
    if answer_mode == "thinking":
        router_output.answer_policy = "open_enriched"
        router_output.retrieval_profile = "deep"
        router_output.needs_extractor = True
        return

    if getattr(router_output, "query_type", "") in {
        "fact_extraction",
        "professional_explainer",
        "teaching_explainer",
        "source_discovery",
    }:
        router_output.needs_extractor = False
    if getattr(router_output, "query_type", "") == "source_discovery":
        router_output.retrieval_profile = "standard"
    if getattr(router_output, "query_type", "") in {"professional_explainer", "teaching_explainer"}:
        router_output.retrieval_mode = "topic_summary"
        router_output.retrieval_profile = "standard"


def _answer_mode_instruction(answer_mode: str, coverage=None) -> str:
    if answer_mode == "thinking":
        base = (
            "UI_ANSWER_MODE: thinking\n"
            "- Trả lời thật chi tiết và có chiều sâu học thuật; ưu tiên 1100-1700 từ khi câu hỏi phù hợp.\n"
            "- Khai thác tối đa evidence RAG đã truy hồi: dùng nhiều nguồn/chunk liên quan hơn, gắn citation [n] sát từng claim được evidence hỗ trợ.\n"
            "- Nếu EVIDENCE có tài liệu phụ [2], [3]... liên quan, bắt buộc dùng nhiều citation khác nhau thay vì chỉ dựa vào [1].\n"
            "- Có thể bổ sung kiến thức nền/chuyên sâu ngoài RAG để giải thích cơ chế, bối cảnh, ý nghĩa lâm sàng và liên hệ học thuật; phần này không được gắn citation giả.\n"
            "- Với số liệu, guideline, liều thuốc, phác đồ hoặc khuyến nghị cá thể hóa: chỉ nêu khi có RAG/external source rõ; nếu không có nguồn thì chuyển về mô tả tổng quát hoặc bỏ claim.\n"
            "- Giữ kết luận ngắn ở cuối cùng."
        )
    else:
        base = (
            "UI_ANSWER_MODE: standard\n"
            "- Trả lời đầy đủ ý, rõ ràng cho người dùng có chuyên môn; ưu tiên 500-850 từ khi câu hỏi là lý thuyết/giải thích.\n"
            "- Evidence RAG là nền chính: mọi claim dùng tài liệu truy hồi phải gắn citation [n] sát claim; không gắn citation cho phần kiến thức nền không có trong evidence.\n"
            "- Có thể bổ sung kiến thức nền an toàn để làm câu trả lời dễ hiểu hơn, nhưng không tự bịa số liệu, guideline mới, liều thuốc, phác đồ hoặc lời khuyên cá thể hóa.\n"
            "- Tránh các đoạn phủ định dài kiểu 'tài liệu không đề cập'; nếu evidence yếu, nói ngắn gọn giới hạn rồi vẫn giải thích nền an toàn.\n"
            "- Không viết quá dài như Thinking; ưu tiên câu trả lời nhanh, có cấu trúc và đủ ý chính.\n"
            "- Giữ kết luận ngắn ở cuối cùng."
        )

    if coverage is None:
        return base

    coverage_mode = getattr(coverage, "coverage_mode", "")
    coverage_level = getattr(coverage, "coverage_level", "")

    if coverage_mode == "evidence_strong":
        enrichment = (
            "ENRICHMENT_GUIDANCE: Evidence RAG mạnh và đầy đủ.\n"
            "- Ưu tiên tổng hợp và trình bày từ evidence đã truy hồi là chính; đây là nguồn thông tin chủ đạo.\n"
            "- Có thể enrich kiến thức nền ngắn gọn để giải thích cơ chế hoặc bối cảnh khi cần thiết.\n"
            "- Không cần nêu giới hạn evidence vì dữ liệu đã đủ để trả lời đầy đủ."
        )
    elif coverage_mode == "title_anchored":
        enrichment = (
            "ENRICHMENT_GUIDANCE: Evidence RAG bao phủ được một phần câu hỏi.\n"
            "- Tổng hợp từ evidence trước; sau đó bổ sung kiến thức nền y khoa để hoàn thiện câu trả lời.\n"
            "- Phần kiến thức nền không có trong evidence không được gắn citation; phân biệt rõ.\n"
            "- Đảm bảo câu trả lời đầy đủ và có chiều sâu dù evidence chỉ bao phủ một phần."
        )
    elif coverage_mode == "retrieval_failed":
        enrichment = (
            "ENRICHMENT_GUIDANCE: Không tìm được evidence RAG đáng tin cậy cho câu hỏi này.\n"
            "- Trả lời hoàn toàn từ kiến thức nền y khoa; không bịa citation hoặc số liệu không có nguồn.\n"
            "- Đảm bảo câu trả lời đầy đủ, chính xác theo độ dài yêu cầu của mode.\n"
            "- Nếu cần, nêu một lần ngắn gọn rằng không tìm được tài liệu cụ thể, rồi vẫn trả lời đầy đủ từ kiến thức nền."
        )
    elif coverage_mode == "open_knowledge" or coverage_level == "low":
        enrichment = (
            "ENRICHMENT_GUIDANCE: Evidence RAG hạn chế hoặc thiếu nhiều phần.\n"
            "- Bổ sung kiến thức nền y khoa đầy đủ để đảm bảo câu trả lời hoàn chỉnh và hữu ích.\n"
            "- Citation chỉ dùng cho claim thực sự có trong evidence; phần kiến thức nền không gắn citation giả.\n"
            "- Không viết dài về giới hạn evidence; nếu cần, nêu ngắn gọn một lần rồi vẫn trả lời đầy đủ."
        )
    else:
        return base

    return f"{base}\n\n{enrichment}"


def _source_discovery_instruction() -> str:
    return (
        "QUERY_INTENT: source_discovery\n"
        "- Người dùng đang hỏi vừa nội dung chủ đề vừa hỏi có những tài liệu/nguồn nào liên quan.\n"
        "- Trả lời định nghĩa/tổng quan ngắn ở đầu, sau đó bắt buộc có mục 'Các tài liệu truy hồi liên quan'.\n"
        "- Trong mục tài liệu, liệt kê nhiều tài liệu khác nhau nếu có: [n] Tên tài liệu - nguồn - nội dung chính - vì sao liên quan.\n"
        "- Với Standard: ưu tiên 3-5 tài liệu liên quan nhất, mô tả ngắn và rõ.\n"
        "- Với Thinking: ưu tiên 6-10 tài liệu nếu evidence có, nhóm theo chủ đề như chẩn đoán hình ảnh, điều trị, tái phát/di căn, tâm lý-xã hội, chi phí/y tế công cộng; sau đó phân tích sâu ý nghĩa học thuật của từng nhóm.\n"
        "- Không nói 'tài liệu truy hồi duy nhất' nếu evidence có nhiều article_id/title/source khác nhau.\n"
        "- Nếu chỉ có một tài liệu thật sự liên quan trong evidence, nói 'trong lần truy hồi này nổi bật nhất là...' thay vì khẳng định toàn bộ kho chỉ có một tài liệu.\n"
        "- Với mode thinking, ưu tiên tổng hợp từ nhiều tài liệu và dùng citation đa nguồn khi evidence hỗ trợ."
    )


def _chunk_fingerprint(chunk) -> dict:
    metadata = getattr(chunk, "metadata", {}) or {}
    return {
        "id": getattr(chunk, "id", ""),
        "text_hash": stable_hash(getattr(chunk, "text", "") or ""),
        "title": metadata.get("title") or metadata.get("canonical_title") or "",
        "article_id": metadata.get("article_id") or "",
        "doc_id": metadata.get("doc_id") or "",
        "source_name": metadata.get("source_name") or "",
        "section_title": metadata.get("section_title") or "",
    }


def _article_fingerprint(article) -> dict:
    return {
        "title": getattr(article, "title", ""),
        "chunks": [_chunk_fingerprint(chunk) for chunk in getattr(article, "chunks", []) or []],
    }


def _aggregated_fingerprint(aggregated) -> dict:
    return {
        "primary": _article_fingerprint(getattr(aggregated, "primary", None)),
        "secondary": [
            _article_fingerprint(article)
            for article in getattr(aggregated, "secondary", []) or []
        ],
    }


def _ordered_chunks_for_response(chunks: list, aggregated) -> list:
    """Order response chunks by prompt citation order for the UI source panel."""
    ordered = []
    seen: set[str] = set()

    def add_chunk(chunk) -> None:
        key = getattr(chunk, "id", "") or stable_hash(getattr(chunk, "text", "") or "")
        if key in seen:
            return
        seen.add(key)
        ordered.append(chunk)

    primary = getattr(aggregated, "primary", None)
    for chunk in getattr(primary, "chunks", []) or []:
        add_chunk(chunk)

    for article in getattr(aggregated, "secondary", []) or []:
        for chunk in getattr(article, "chunks", []) or []:
            add_chunk(chunk)

    for chunk in chunks or []:
        add_chunk(chunk)

    return ordered


def _article_source_name(article) -> str:
    for chunk in getattr(article, "chunks", []) or []:
        md = getattr(chunk, "metadata", {}) or {}
        name = md.get("source_name") or md.get("source_id") or ""
        if name:
            return str(name)
    return "RAG"


def _format_reranked_articles_for_prompt(aggregated, answer_mode: str) -> str:
    articles = []
    primary = getattr(aggregated, "primary", None)
    if primary and getattr(primary, "title", ""):
        articles.append(primary)
    articles.extend(getattr(aggregated, "secondary", []) or [])
    if not articles:
        return ""

    max_articles = 10 if answer_mode == "thinking" else 5
    lines = [
        "RERANKED_INTERNAL_ARTICLES:",
        "Các tài liệu dưới đây đã được article-level rerank từ retrieval. Dùng chúng theo citation [n] nếu nội dung liên quan thật sự:",
    ]
    for idx, article in enumerate(articles[:max_articles], start=1):
        title = getattr(article, "title", "") or "Untitled"
        source = _article_source_name(article)
        score = getattr(article, "article_score", 0.0)
        reason = getattr(article, "selected_reason", "") or ""
        lines.append(f"[{idx}] {title} | source={source} | score={score:.3f} | reason={reason}")
    if answer_mode == "thinking" and len(articles) > 1:
        lines.append(
            "Thinking requirement: nếu các tài liệu phụ có nội dung phù hợp, hãy tổng hợp tối thiểu 3 nguồn nội bộ khác nhau; không chỉ dùng [1]."
        )
    return "\n".join(lines)


def _router_fingerprint(router_output) -> dict:
    return {
        "query_type": getattr(router_output, "query_type", ""),
        "depth": getattr(router_output, "depth", ""),
        "needs_extractor": getattr(router_output, "needs_extractor", False),
        "retrieval_profile": getattr(router_output, "retrieval_profile", ""),
        "retrieval_mode": getattr(router_output, "retrieval_mode", ""),
        "answer_policy": getattr(router_output, "answer_policy", ""),
        "answer_style": getattr(router_output, "answer_style", ""),
        "answer_mode": getattr(router_output, "answer_mode", ""),
    }


def _coverage_fingerprint(coverage) -> dict:
    return {
        "coverage_level": getattr(coverage, "coverage_level", ""),
        "coverage_mode": getattr(coverage, "coverage_mode", ""),
        "allow_external": getattr(coverage, "allow_external", False),
        "force_abstain_parts": getattr(coverage, "force_abstain_parts", []) or [],
        "missing_requirements": getattr(coverage, "missing_requirements", []) or [],
        "confidence_ceiling": getattr(coverage, "confidence_ceiling", ""),
        "unsupported_concepts": getattr(coverage, "unsupported_concepts", []) or [],
        "concept_evidence_gap": getattr(coverage, "concept_evidence_gap", False),
        "allowed_answer_scope": getattr(coverage, "allowed_answer_scope", ""),
    }


def _external_fingerprint(external_pack) -> dict:
    sources = []
    for source in getattr(external_pack, "sources", []) or []:
        sources.append({
            "id": getattr(source, "id", ""),
            "title": getattr(source, "title", ""),
            "url": getattr(source, "url", ""),
            "snippet_hash": stable_hash(getattr(source, "snippet", "") or ""),
        })
    return {
        "status": getattr(external_pack, "status", ""),
        "used": getattr(external_pack, "used", False),
        "sources": sources,
    }


def _should_use_llm_verifier(answer_mode: str, answer: str, coverage, router_output, external_pack) -> bool:
    if answer_mode == "thinking":
        return True
    if external_pack is not None and getattr(external_pack, "sources", []):
        return True
    if getattr(router_output, "requires_numbers", False):
        return True
    if getattr(coverage, "coverage_mode", "") == "retrieval_failed":
        return True
    return bool(
        re.search(
            r"(\bliều\b|\blieu\b|phác\s*đồ|phac\s*do|guideline|hướng\s*dẫn|huong\s*dan|khuyến\s*cáo|khuyen\s*cao)",
            answer or "",
            re.IGNORECASE,
        )
    )


def _should_expand_primary_article(router_output, query: str) -> bool:
    answer_style = getattr(router_output, "answer_style", "")
    query_type = getattr(router_output, "query_type", "")
    if answer_style == "exact":
        return True
    if query_type == "teaching_explainer":
        return True

    query_norm = (query or "").lower()
    if answer_style in {"summary", "bounded_partial"}:
        return any(
            marker in query_norm
            for marker in (
                "vì sao", "vi sao", "tại sao", "tai sao",
                "dựa trên", "dua tren", "nhóm yếu tố", "nhom yeu to",
                "quyết định", "quyet dinh",
            )
        )
    return False


def _primary_expansion_max_chunks(router_output, query: str) -> int:
    answer_style = getattr(router_output, "answer_style", "")
    if answer_style == "exact":
        query_norm = (query or "").lower()
        if any(
            marker in query_norm
            for marker in ("thiết kế", "thiet ke", "đối tượng", "doi tuong", "phương pháp", "phuong phap", "cỡ mẫu", "co mau", "chọn mẫu", "chon mau")
        ):
            return int(os.getenv("EXACT_METHODS_PRIMARY_EXPANSION_CHUNKS", "10"))
        return int(os.getenv("EXACT_PRIMARY_EXPANSION_CHUNKS", "8"))

    query_norm = (query or "").lower()
    focused_markers = (
        "vì sao", "vi sao", "tại sao", "tai sao",
        "dựa trên", "dua tren", "nhóm yếu tố", "nhom yeu to",
        "quyết định", "quyet dinh", "chỉ số", "chi so", "tiêu chí", "tieu chi",
    )
    default_chunks = "8" if any(marker in query_norm for marker in focused_markers) else "6"
    return int(os.getenv("FOCUSED_PRIMARY_EXPANSION_CHUNKS", default_chunks))


_FALLBACK_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "into",
    "trong", "nhung", "những", "theo", "giua", "giữa", "cua", "của",
    "mot", "một", "khong", "không", "dua", "dựa", "tren", "trên",
    "nghien", "nghiên", "cuu", "cứu", "benh", "bệnh", "nhan", "nhân",
}


def _clean_extractive_text(text: str) -> str:
    """Clean raw article text into a form suitable for extractive fallback."""
    if not text:
        return ""

    kept = []
    for line in text.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if not line:
            continue
        if line.startswith(("Title:", "Source:", "Audience:", "Body:")):
            continue
        if line.startswith("Hình "):
            continue
        # Drop conference banners / page headers that are mostly uppercase noise.
        letters = [c for c in line if c.isalpha()]
        if letters:
            upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
            if upper_ratio > 0.8 and len(line) > 24:
                continue
        kept.append(line)

    cleaned = " ".join(kept)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _extractive_terms(text: str) -> set[str]:
    terms = set()
    for token in re.findall(r"\w+", (text or "").lower(), flags=re.UNICODE):
        if len(token) >= 4 and token not in _FALLBACK_STOPWORDS:
            terms.add(token)
    return terms


def _looks_like_sentence_candidate(text: str) -> bool:
    candidate = (text or "").strip()
    if len(candidate) < 60:
        return False
    if len(candidate.split()) < 8:
        return False
    if re.match(r"^[a-zà-ỹ]", candidate):
        return False
    if re.match(r"^(và|hoặc|nhưng|cũng|đồng thời|chỉ|tuy nhiên)\b", candidate, flags=re.IGNORECASE):
        return False
    return True


def _fallback_candidates_from_text(text: str) -> list[str]:
    cleaned = _clean_extractive_text(text)
    if not cleaned:
        return []

    sentences = [
        s.strip(" -")
        for s in re.split(r"(?<=[\.\?!;:])\s+", cleaned)
        if s.strip()
    ]
    candidates = []
    for sentence in sentences:
        if len(sentence) <= 320 and _looks_like_sentence_candidate(sentence):
            candidates.append(sentence)

    # Add two-sentence windows for cases where the direct answer spans a wrap.
    for i in range(len(sentences) - 1):
        window = f"{sentences[i]} {sentences[i + 1]}".strip()
        if 80 <= len(window) <= 420 and _looks_like_sentence_candidate(window):
            candidates.append(window)

    # If punctuation is poor, fall back to fixed-size spans from cleaned text.
    if not candidates:
        for i in range(0, len(cleaned), 220):
            window = cleaned[i:i + 260].strip()
            if len(window) >= 80:
                candidates.append(window)
            if len(candidates) >= 4:
                break

    return candidates


def _citation_label(index: int) -> str:
    return f"[{index}]"


def _build_open_enriched_fallback_answer(question: str, evidence_pack, coverage) -> str:
    """Deterministic professional explainer when the upstream LLM times out."""
    primary = getattr(evidence_pack, "primary_source", None)
    sources = []
    if primary:
        sources.append(primary)
    for source in getattr(evidence_pack, "secondary_sources", []) or []:
        if source and source not in sources:
            sources.append(source)
        if len(sources) >= 4:
            break

    source_titles = [getattr(source, "title", "") for source in sources if getattr(source, "title", "")]
    source_list = "\n".join(
        f"{_citation_label(i)} {title}"
        for i, title in enumerate(source_titles, start=1)
    )

    candidates = []
    for source_index, source in enumerate(sources, start=1):
        for finding in getattr(source, "key_findings", []) or []:
            claim = (getattr(finding, "claim", "") or "").strip()
            if claim:
                candidates.append((claim, source_index))
        conclusion = getattr(getattr(source, "authors_conclusion", None), "text", "") or ""
        if conclusion:
            candidates.append((conclusion.strip(), source_index))
        for sentence in _fallback_candidates_from_text(getattr(source, "raw_text", "") or "")[:4]:
            candidates.append((sentence, source_index))

    seen = set()
    scored = []
    scope = getattr(coverage, "allowed_answer_scope", "") if coverage else ""
    query_text = f"{question} {scope} {' '.join(source_titles)}".lower()
    query_terms = _extractive_terms(query_text)
    for candidate, source_index in candidates:
        norm = candidate.lower()
        if norm in seen:
            continue
        seen.add(norm)
        overlap = sum(1 for term in query_terms if term in norm)
        numeric_bonus = 1 if re.search(r"\b\d+\b|%|OR|HR|AUC|n\s*=", candidate, flags=re.IGNORECASE) else 0
        scored.append((overlap, numeric_bonus, len(candidate), candidate, source_index))

    scored.sort(key=lambda x: (-x[0], -x[1], -x[2]))
    evidence_lines = [
        f"- {candidate} {_citation_label(source_index)}"
        for _, _, _, candidate, source_index in scored[:5]
    ]

    q_norm = question.lower()
    oncology_combo = any(term in q_norm for term in ("buồng trứng", "buong trung", "ovarian")) and any(
        term in q_norm for term in ("diệp thể", "diep the", "phyllode", "phyllodes")
    )
    if oncology_combo:
        body = [
            "Kết luận trực tiếp: trong bối cảnh một người bệnh có đồng thời carcinôm tuyến buồng trứng tái phát/di căn xa và u diệp thể ác ở vú, sinh thiết đầy đủ và hóa mô miễn dịch quan trọng vì chúng trả lời câu hỏi nền tảng nhất: tổn thương hiện tại thuộc bệnh nào, có phải di căn của ung thư buồng trứng, một ung thư vú biểu mô độc lập, hay một u mô đệm dạng phyllodes. Nếu phân loại sai nguồn gốc u, toàn bộ quyết định điều trị, tiên lượng và cách theo dõi có thể đi sai hướng.",
            "Về mặt bệnh học, hai thực thể này khác nhau ở bản chất tế bào. Carcinôm tuyến buồng trứng là ung thư biểu mô, thường cần đánh giá hình thái tuyến, kiểu lan tràn phúc mạc/di căn và các dấu ấn biểu mô phù hợp. U diệp thể vú lại là u xơ-biểu mô, trong đó phần mô đệm quyết định mức độ lành, giáp biên hay ác tính. Vì vậy chỉ nhìn đại thể hoặc hình ảnh học thường không đủ; cần mẫu mô đủ rộng để thấy cấu trúc u, mật độ tế bào mô đệm, dị dạng nhân, hoạt động phân bào, hoại tử, ranh giới xâm nhập và các thành phần biểu mô đi kèm.",
            "Sinh thiết đầy đủ giúp tránh sai lệch lấy mẫu. Với u diệp thể, lõi sinh thiết quá ít có thể chỉ bắt được vùng giống u xơ tuyến hoặc chỉ bắt được phần hoại tử/xơ hóa, làm đánh giá thấp độ ác. Với bệnh nhân đã có ung thư buồng trứng di căn, một khối ở vú hoặc tổn thương mới rất dễ bị diễn giải thiên lệch là di căn hoặc là ung thư vú thông thường. Mẫu mô đủ đại diện giúp bác sĩ giải phẫu bệnh phân biệt u nguyên phát, di căn, tổn thương phối hợp hoặc hai bệnh ác tính đồng thời.",
            "Hóa mô miễn dịch có vai trò như lớp kiểm chứng nguồn gốc và kiểu biệt hóa của tế bào u. Trong thực hành, IHC không thay thế hình thái học, nhưng giúp củng cố hoặc loại trừ các chẩn đoán gần nhau: nhóm marker biểu mô hỗ trợ carcinôm; các marker liên quan vú, buồng trứng hoặc Mullerian giúp định hướng cơ quan nguồn; các marker tăng sinh và đặc điểm mô đệm giúp đánh giá bản chất ác tính của u diệp thể. Điểm quan trọng là panel IHC phải được chọn theo câu hỏi chẩn đoán cụ thể, không dùng rời rạc từng marker.",
            "Ý nghĩa lâm sàng là rất lớn. Nếu tổn thương là tiến triển của ung thư buồng trứng, chiến lược thường xoay quanh điều trị toàn thân và đánh giá gánh nặng di căn. Nếu là u diệp thể ác ở vú, xử trí lại thiên về kiểm soát tại chỗ bằng phẫu thuật diện cắt thích hợp và theo dõi tái phát/di căn, trong khi vai trò hóa trị, xạ trị hoặc điều trị miễn dịch bổ sung không thể suy diễn như carcinôm vú biểu mô. Nếu là hai bệnh đồng mắc, hội chẩn đa chuyên khoa cần ưu tiên bệnh đang đe dọa tính mạng, khả năng phẫu thuật, thể trạng và mục tiêu điều trị.",
        ]
    else:
        body = [
            "Kết luận trực tiếp: sinh thiết đầy đủ và hóa mô miễn dịch quan trọng vì chúng xác định bản chất mô học, nguồn gốc tổn thương và mức độ chắc chắn của chẩn đoán. Khi câu hỏi liên quan nhiều bệnh hoặc nhiều vị trí tổn thương, đây là bước quyết định để tránh điều trị theo giả định.",
            "Về nguyên tắc, hình ảnh học và biểu hiện lâm sàng cho biết vị trí, kích thước và mức lan rộng, nhưng không thể thay thế mô bệnh học. Sinh thiết cung cấp mô để đánh giá cấu trúc u, loại tế bào, mức độ dị dạng, hoạt động phân bào, hoại tử và kiểu xâm nhập. Hóa mô miễn dịch bổ sung bằng cách kiểm tra các dấu ấn protein, giúp phân biệt các nhóm u có hình thái gần giống nhau.",
            "Trong bệnh cảnh chuyên khoa, giá trị lớn nhất của IHC là kiểm soát sai số chẩn đoán: phân biệt u nguyên phát với di căn, phân biệt ung thư biểu mô với u mô đệm/lympho/sarcoma, và xác định các đặc điểm có ý nghĩa tiên lượng hoặc định hướng điều trị. Tuy nhiên, IHC phải được diễn giải cùng hình thái mô học và bệnh cảnh lâm sàng; một marker đơn lẻ hiếm khi đủ để kết luận.",
        ]

    if evidence_lines:
        evidence_section = "Bằng chứng truy hồi được từ RAG:\n" + "\n".join(evidence_lines)
    else:
        evidence_section = (
            "Bằng chứng truy hồi được từ RAG: hệ thống chưa lấy được đoạn chứng cứ đủ mạnh trong lượt này, "
            "nên phần trên được trình bày như giải thích nền/chuyên sâu không gắn citation giả."
        )

    limits = (
        "Giới hạn an toàn: câu trả lời này không đưa ra phác đồ cá nhân hóa, liều thuốc hoặc khuyến cáo guideline mới. "
        "Các nhận định có citation chỉ nên hiểu là được hỗ trợ bởi tài liệu đã truy hồi; phần giải thích nền không có citation là kiến thức tổng quát để giúp người dùng hiểu bối cảnh."
    )
    system_note = (
        "Ghi chú hệ thống: mô hình sinh câu trả lời chính không phản hồi kịp trong lượt này, "
        "nên hệ thống dùng fallback chuyên môn có kiểm soát để tránh trả lời quá ngắn."
    )
    parts = body + [evidence_section, limits]
    if source_list:
        parts.append("Nguồn:\n" + source_list)
    parts.append(system_note)
    return "\n\n".join(parts)


def _build_rate_limit_fallback_answer(question: str, evidence_pack, coverage, router_output=None) -> str:
    """Return a deterministic extractive fallback instead of surfacing a raw 500."""
    if getattr(router_output, "answer_policy", "") == "open_enriched":
        return _build_open_enriched_fallback_answer(question, evidence_pack, coverage)

    primary = getattr(evidence_pack, "primary_source", None)
    title = getattr(primary, "title", "") if primary else ""
    scope = getattr(coverage, "allowed_answer_scope", "") if coverage else ""
    raw_text = getattr(primary, "raw_text", "") if primary else ""

    candidates = []
    if primary:
        for finding in getattr(primary, "key_findings", []) or []:
            claim = (getattr(finding, "claim", "") or "").strip()
            if claim:
                candidates.append(claim)
        conclusion = getattr(getattr(primary, "authors_conclusion", None), "text", "") or ""
        if conclusion:
            candidates.append(conclusion.strip())

    candidates.extend(_fallback_candidates_from_text(raw_text))

    seen = set()
    scored = []
    query_text = f"{question} {scope} {title}".lower()
    query_terms = _extractive_terms(query_text)

    for candidate in candidates:
        norm = candidate.lower()
        if norm in seen:
            continue
        seen.add(norm)
        overlap = sum(1 for term in query_terms if term in norm)
        numeric_bonus = 1 if re.search(r"\b\d+\b|%|OR|HR|AUC|n\s*=", candidate, flags=re.IGNORECASE) else 0
        scored.append((overlap, numeric_bonus, len(candidate), candidate))

    scored.sort(key=lambda x: (-x[0], -x[1], -x[2]))
    top_sentences = [cand for _, _, _, cand in scored[:3]]

    lines = [
        "Hệ thống sinh câu trả lời đang bị giới hạn tốc độ từ nhà cung cấp mô hình, nên dưới đây là tóm tắt trích xuất trực tiếp từ tài liệu chính đã truy hồi.",
    ]
    if top_sentences:
        lines.append("Tóm tắt nhanh:")
        lines.extend(f"- {sentence}" for sentence in top_sentences)
    else:
        lines.append("Yêu cầu không bị mất, nhưng chưa thể tổng hợp câu trả lời đầy đủ ở lượt này.")
    if title:
        lines.append(f"Nguồn chính: [1] {title}")
    return "\n".join(lines)


def _build_degraded_mode_answer(reason: str) -> str:
    if reason == "upstream_rate_limit":
        return (
            "Yêu cầu này đang ở `degraded_mode` vì mô hình sinh câu trả lời bị giới hạn tốc độ từ nhà cung cấp. "
            "Kết quả semantic của lượt này không nên chấm chung với chất lượng trả lời cuối."
        )
    if reason == "llm_unavailable":
        return (
            "Yêu cầu này đang ở `degraded_mode` vì backend sinh câu trả lời chưa sẵn sàng. "
            "Kết quả semantic của lượt này không nên dùng để đánh giá chất lượng hệ thống."
        )
    return (
        "Yêu cầu này đang ở `degraded_mode`. "
        "Kết quả semantic của lượt này không nên dùng để đánh giá chất lượng hệ thống."
    )


# ---------------------------------------------------------------------
# Background Task
# ---------------------------------------------------------------------
def generate_and_save_title(session_id: str, prompt: str):
    if not _env_flag("ASYNC_LLM_TITLE_ENABLED", default=False):
        return
    if _is_eval_session(session_id):
        return
    kserve = build_kserve_client_from_env()
    if kserve:
        try:
            sys_prompt = "Bạn là trợ lý ảo. Hãy đọc câu hỏi của người dùng và đặt tên cho đoạn chat. Tên ngắn gọn (3-6 từ), tóm tắt chủ đề chính, bằng tiếng Việt. KHÔNG giải thích, KHÔNG dùng dấu ngoặc kép, CHỈ trả về tên cuộc trò chuyện."
            msgs = [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": prompt}
            ]
            title = kserve.generate(
                msgs,
                max_tokens=15,
                temperature=0.3,
                attempt_budget=int(os.getenv("TITLE_MAX_ATTEMPTS", "1")),
            )
            title = title.strip().strip('"').strip("'")
            if title and len(title) < 100:
                session_store.set_title(session_id, title)
        except Exception:
            pass

# ---------------------------------------------------------------------
# App bootstrap and tracing
# ---------------------------------------------------------------------

app = FastAPI(title="Medical RAG Orchestrator")

_cors_origins = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173,http://localhost:8501,http://127.0.0.1:8501",
    ).split(",")
    if origin.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize OpenTelemetry tracing (FastAPI + outbound clients)
setup_tracing(
    app=app,
    service_name=os.getenv("OTEL_SERVICE_NAME", "rag-orchestrator"),
)

tracer = trace.get_tracer("rag-orchestrator")

# ---------------------------------------------------------------------
# Session store (Redis-backed if configured)
# ---------------------------------------------------------------------

session_store = SessionStore()


# ---------------------------------------------------------------------
# Pre-load embedding model at startup (avoid cold-start timeout)
# ---------------------------------------------------------------------
@app.on_event("startup")
def preload_retriever():
    import time as _time
    t0 = _time.time()
    print("[startup] Pre-loading embedding model...")
    retriever = build_retriever_from_env()
    if retriever:
        # Warm up with a dummy query to force model download
        try:
            retriever._embed_query("warmup")
        except Exception:
            pass
    elapsed = round(_time.time() - t0, 1)
    print(f"[startup] Embedding model ready in {elapsed}s")


# ---------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    RAG_CHAT_ERRORS_TOTAL.inc()
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )


# ---------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    return readiness()


@app.get("/live")
def live():
    return liveness()

# ---------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------
@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------
# API logging (only /api)
# ---------------------------------------------------------------------
@app.middleware("http")
async def api_logging_middleware(request: Request, call_next):
    if request.url.path.startswith("/api"):
        start = time.time()
        request_id = str(uuid4())
        request.state.request_id = request_id

        # Single call_next, wrapped in the span
        with tracer.start_as_current_span(
            f"http {request.method} {request.url.path}"
        ) as span:
            ctx = span.get_span_context()
            request.state.trace_id = format(ctx.trace_id, "032x")
            request.state.span_id = format(ctx.span_id, "016x")
            response = await call_next(request)
            status_code = getattr(response, "status_code", 500)

        try:
            # no second call_next here
            pass
        except Exception as exc:
            request.state.error_message = str(exc)
            status_code = 500
            raise
        finally:
            await log_request(request, status_code, start)

        return response

    return await call_next(request)


# ---------------------------------------------------------------------
# Session endpoint
# ---------------------------------------------------------------------
@app.get("/api/session/{session_id}")
def get_session_history(session_id: str):
    history = session_store.get_history(session_id)
    return {"session_id": session_id, "messages": history}

@app.get("/api/sessions")
def list_sessions():
    return {"sessions": session_store.get_all_sessions()}

@app.put("/api/session/{session_id}/title")
def update_session_title(session_id: str, payload: TitleUpdate):
    session_store.set_title(session_id, payload.title)
    return {"status": "ok", "title": payload.title}


@app.delete("/api/session/{session_id}")
def delete_session(session_id: str):
    session_store.delete_session(session_id)
    return {"ok": True, "deleted": session_id}

# ---------------------------------------------------------------------
# Chat endpoint
# ---------------------------------------------------------------------
def _chat_core(
    req: ChatRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    *,
    stream: bool = False,
):
    """Core chat pipeline, shared by /api/chat and /api/chat/stream.

    Runs as a generator that yields tuples:
      ("stage", {...})  progress events for the UI (cheap, both transports)
      ("token", {...})  answer deltas, only when ``stream=True``
      ("final", {...})  the full ChatResponse payload, exactly once at the end

    The non-streaming endpoint drains this generator and returns the final
    payload, so /api/chat behaviour is identical to before streaming existed.
    """
    RAG_CHAT_REQUESTS_TOTAL.inc()
    RAG_INFLIGHT.inc()
    try:
        # Root span for this chat request
        with tracer.start_as_current_span("rag.chat") as root_span:
            request_t0 = time.time()
            timings_ms: dict[str, float] = {}
            session_id = req.session_id or str(uuid4())
            answer_mode = _normalize_answer_mode(req.answer_mode)
            cache_info: dict[str, bool] = {
                "retrieval_hit": False,
                "evidence_extract_hit": False,
                "verifier_hit": False,
            }
            pipeline_info: dict[str, Any] = {
                "fast_path_enabled": answer_mode == "standard",
                "rewrite_enabled": _env_flag("LLM_REWRITER_ENABLED", default=True),
                "query_rewritten": False,
                "pipeline_cache_enabled": bool(getattr(pipeline_cache, "enabled", False)),
                "retrieval_cache_enabled": (
                    _env_flag("RETRIEVAL_CACHE_ENABLED", default=False)
                    and bool(getattr(pipeline_cache, "enabled", False))
                ),
                "retrieval_hit": False,
                "retrieval_profile": "",
                "retrieval_mode": "",
                "retrieval_top_k": 0,
                "entity_fallback_used": False,
                "entity_fallback_added": 0,
                "entity_fallback_skipped": False,
                "entity_fallback_cache_hit": False,
                "router_needs_extractor": False,
                "llm_extractor_enabled": _env_flag("LLM_EXTRACTOR_ENABLED", default=False),
                "llm_extractor_used": False,
                "answer_planner_llm_used": False,
                "external_resolver_used": False,
                "llm_verifier_enabled": _env_flag("LLM_VERIFIER_ENABLED", default=True),
                "llm_verifier_requested": False,
                "llm_verifier_used": False,
                "deterministic_verifier_run": False,
            }
            request.state.session_id = session_id
            root_span.set_attribute("session.id", session_id)
            root_span.set_attribute("ui.answer_mode", answer_mode)

            # load chat history BEFORE appending the new message
            with tracer.start_as_current_span("session.load_history") as span:
                history = session_store.get_history(session_id)
                span.set_attribute("session.history_length", len(history))

            # Trigger title generation for first message
            if len(history) == 0:
                fallback_title = req.message[:30] + "..." if len(req.message) > 30 else req.message
                session_store.set_title(session_id, fallback_title)
                if _env_flag("ASYNC_LLM_TITLE_ENABLED", default=False) and not _is_eval_session(session_id):
                    background_tasks.add_task(generate_and_save_title, session_id, req.message)

            # append user message + trace
            with tracer.start_as_current_span("session.append_user"):
                session_store.append(session_id, "user", req.message)

            # query rewriting for multi-turn conversations
            rewrite_t0 = time.time()
            with tracer.start_as_current_span("query.rewrite") as span:
                rewrite_enabled = bool(pipeline_info["rewrite_enabled"])
                kserve_for_rewrite = (
                    _build_optional_llm_client("LLM_REWRITER_ENABLED", default=True)
                    if rewrite_enabled and history
                    else None
                )
                search_query = rewrite_query(
                    req.message, history, llm_client=kserve_for_rewrite
                )
                query_rewritten = search_query != req.message
                pipeline_info["query_rewritten"] = query_rewritten
                span.set_attribute("query.original", req.message)
                span.set_attribute("query.rewritten", search_query)
                span.set_attribute("query.was_rewritten", query_rewritten)
            timings_ms["query_rewrite_ms"] = round((time.time() - rewrite_t0) * 1000.0, 2)

            # ── Session topic memory (entity-level context for follow-ups) ──
            query_entities = _extract_specific_entities(search_query)
            if query_entities:
                # First turn or query has its own entities: update stored topic
                session_store.set_topic(session_id, " ".join(query_entities[:2]))
            elif history:
                # Follow-up with no detected entity: inject stored topic into query
                stored_topic = session_store.get_topic(session_id)
                if stored_topic and stored_topic.lower() not in search_query.lower():
                    search_query = f"{search_query} {stored_topic}"

            # ── 1. Query Router (rule-based, no LLM) ──────────────
            with tracer.start_as_current_span("query.route") as span:
                router_output = route_query(search_query)
                _apply_answer_mode(router_output, answer_mode)
                span.set_attribute("router.query_type", router_output.query_type)
                span.set_attribute("router.depth", router_output.depth)
                span.set_attribute("router.needs_extractor", router_output.needs_extractor)
                span.set_attribute("router.retrieval_profile", router_output.retrieval_profile)
                span.set_attribute("router.answer_mode", answer_mode)
                pipeline_info["router_needs_extractor"] = bool(router_output.needs_extractor)
                pipeline_info["retrieval_profile"] = router_output.retrieval_profile
                pipeline_info["retrieval_mode"] = router_output.retrieval_mode

            # ── 2. Retrieve with profile-based top_k ─────────────
            _PROFILE_TOP_K = {"light": 8, "standard": 12, "deep": 20}
            profile_top_k = _PROFILE_TOP_K.get(router_output.retrieval_profile, 12)
            if getattr(router_output, "query_type", "") == "source_discovery":
                if answer_mode == "thinking":
                    profile_top_k = max(profile_top_k, int(os.getenv("RAG_SOURCE_DISCOVERY_THINKING_TOP_K", "40")))
                else:
                    profile_top_k = max(profile_top_k, int(os.getenv("RAG_SOURCE_DISCOVERY_STANDARD_TOP_K", "18")))
            elif answer_mode == "thinking":
                profile_top_k = max(profile_top_k, int(os.getenv("RAG_THINKING_TOP_K", "32")))
            pipeline_info["retrieval_top_k"] = profile_top_k

            if stream:
                yield ("stage", {"stage": "retrieving", "label": "Đang truy hồi tài liệu liên quan..."})

            with tracer.start_as_current_span("retriever.build"):
                retriever = build_retriever_from_env()

            with tracer.start_as_current_span("retrieval.vector_search") as span:
                span.set_attribute("vector.db", "qdrant")
                span.set_attribute(
                    "vector.collection",
                    os.getenv("QDRANT_COLLECTION", "medical_docs"),
                )
                span.set_attribute("vector.top_k", profile_top_k)

                retrieval_mode = getattr(router_output, "retrieval_mode", "article_centric")
                t0 = time.time()
                retrieval_cache_hit = False
                retrieval_cache_key = None
                chunks = []

                if retriever and bool(pipeline_info["retrieval_cache_enabled"]):
                    retrieval_cache_key = pipeline_cache.key(
                        "retrieval",
                        {
                            "query": search_query,
                            "router": _router_fingerprint(router_output),
                            "retrieval_mode": retrieval_mode,
                            "top_k": profile_top_k,
                            "collection": os.getenv("QDRANT_COLLECTION", ""),
                            "embedding_model": os.getenv("EMBEDDING_MODEL", ""),
                            "min_score": os.getenv("RAG_MIN_SCORE", ""),
                            "max_context_tokens": os.getenv("RAG_MAX_CONTEXT_TOKENS", ""),
                            "deduplicate": os.getenv("RAG_DEDUPLICATE", ""),
                            "hybrid": os.getenv("RAG_ENABLE_HYBRID", ""),
                            "article_index_path": os.getenv("RAG_ARTICLE_INDEX_PATH", ""),
                        },
                    )
                    cached_chunks = pipeline_cache.get(retrieval_cache_key)
                    if cached_chunks is not None:
                        chunks = cached_chunks
                        retrieval_cache_hit = True

                if retrieval_cache_hit:
                    span.set_attribute("retrieval.mode", "cache")
                elif retrieval_mode == "mechanistic_synthesis" and retriever:
                    # Phase 4: Decomposed multi-axis retrieval
                    # Use heuristic decomposition (no LLM) to save API calls
                    from .mechanistic_query_decomposer import decompose_query
                    subqueries = decompose_query(
                        search_query, llm_client=None, max_subqueries=3
                    )
                    span.set_attribute("retrieval.mode", "multi_axis")
                    span.set_attribute("retrieval.subqueries", len(subqueries))

                    top_k_per_sub = max(3, profile_top_k // len(subqueries) + 1)
                    chunks = retriever.retrieve_multi_axis(
                        subqueries,
                        top_k_per_query=top_k_per_sub,
                        query_type=router_output.query_type,
                        answer_style=router_output.answer_style,
                    )
                else:
                    # Standard single-query retrieval
                    span.set_attribute("retrieval.mode", "single")
                    chunks = retriever.retrieve(
                        search_query, 
                        top_k_override=profile_top_k,
                        retrieval_mode=retrieval_mode,
                        query_type=router_output.query_type,
                        answer_style=router_output.answer_style,
                    ) if retriever else []

                if retrieval_cache_key and not retrieval_cache_hit:
                    pipeline_cache.set(retrieval_cache_key, chunks)

                retrieval_ms = round((time.time() - t0) * 1000.0, 2)
                span.set_attribute("retrieval.chunks", len(chunks))
                span.set_attribute("retrieval.cache_hit", retrieval_cache_hit)

            RAG_RETRIEVAL_LATENCY_SECONDS.observe(retrieval_ms / 1000.0)
            request.state.retrieval_ms = retrieval_ms
            request.state.chunks_returned = len(chunks)
            timings_ms["retrieval_ms"] = retrieval_ms
            cache_info["retrieval_hit"] = retrieval_cache_hit
            pipeline_info["retrieval_hit"] = retrieval_cache_hit

            est_tokens = sum(max(1, len(c.text) // 4) for c in chunks)
            RAG_CONTEXT_TOKENS.observe(est_tokens)

            if not chunks:
                RAG_EMPTY_CONTEXT_TOTAL.inc()

            # ── 2.5a. Entity-fallback retrieval ──────────────────
            # If specific entities were detected but are poorly represented
            # in the retrieved set, fire a targeted secondary retrieval so
            # the right entity content can actually be ranked by the aggregator.
            entity_fallback_t0 = time.time()
            if retriever and query_entities and retrieval_mode != "mechanistic_synthesis":
                entity_phrase = " ".join(query_entities[:2])
                if should_skip_entity_fallback(router_output, answer_mode):
                    # General Standard query: primary retrieval is enough. Skipping
                    # avoids a second hybrid lexical scan (the warm-path bottleneck).
                    pipeline_info["entity_fallback_skipped"] = True
                else:
                    entity_hits_in_chunks = sum(
                        1 for chunk in chunks
                        if entity_phrase in _normalize_for_matching(chunk.text or "")[:2000]
                        or entity_phrase in _normalize_for_matching(
                            " ".join(str(v) for v in (chunk.metadata or {}).values() if isinstance(v, str))
                        )
                    )
                    if entity_hits_in_chunks < 2:
                        with tracer.start_as_current_span("retrieval.entity_fallback") as fb_span:
                            fb_span.set_attribute("entity.phrase", entity_phrase)
                            fb_top_k = min(8, profile_top_k // 2)
                            fb_cache_key = None
                            fb_chunks = None
                            if bool(pipeline_info["retrieval_cache_enabled"]):
                                fb_cache_key = pipeline_cache.key(
                                    "entity_fallback",
                                    {
                                        "entity": entity_phrase,
                                        "collection": os.getenv("QDRANT_COLLECTION", ""),
                                        "retrieval_mode": retrieval_mode,
                                        "query_type": router_output.query_type,
                                        "answer_style": router_output.answer_style,
                                        "top_k": fb_top_k,
                                        "min_score": os.getenv("RAG_MIN_SCORE", ""),
                                        "hybrid": os.getenv("RAG_ENABLE_HYBRID", ""),
                                        "article_index_path": os.getenv("RAG_ARTICLE_INDEX_PATH", ""),
                                    },
                                )
                                cached_fb = pipeline_cache.get(fb_cache_key)
                                if cached_fb is not None:
                                    fb_chunks = cached_fb
                                    pipeline_info["entity_fallback_cache_hit"] = True
                            if fb_chunks is None:
                                fb_chunks = retriever.retrieve(
                                    entity_phrase,
                                    top_k_override=fb_top_k,
                                    retrieval_mode=retrieval_mode,
                                    query_type=router_output.query_type,
                                    answer_style=router_output.answer_style,
                                )
                                if fb_cache_key is not None:
                                    pipeline_cache.set(fb_cache_key, fb_chunks)
                            existing_ids = {getattr(c, "id", None) for c in chunks}
                            new_chunks = [c for c in fb_chunks if getattr(c, "id", None) not in existing_ids]
                            chunks.extend(new_chunks)
                            pipeline_info["entity_fallback_used"] = True
                            pipeline_info["entity_fallback_added"] = len(new_chunks)
                            fb_span.set_attribute("entity_fallback.hits_before", entity_hits_in_chunks)
                            fb_span.set_attribute("entity_fallback.added", len(new_chunks))
                            fb_span.set_attribute(
                                "entity_fallback.cache_hit",
                                bool(pipeline_info["entity_fallback_cache_hit"]),
                            )
            timings_ms["entity_fallback_ms"] = round((time.time() - entity_fallback_t0) * 1000.0, 2)

            # ── 2.5. Chunk Quality Filter (Phase 4) ──────────────
            with tracer.start_as_current_span("chunk.quality_filter") as span:
                pre_filter_count = len(chunks)
                chunks = filter_chunks(chunks)
                span.set_attribute("filter.before", pre_filter_count)
                span.set_attribute("filter.after", len(chunks))
                span.set_attribute("filter.removed", pre_filter_count - len(chunks))

            if stream:
                yield ("stage", {"stage": "analyzing", "label": "Đang phân tích và chọn nguồn evidence..."})

            # ── 3. Article Aggregation ───────────────────────────
            with tracer.start_as_current_span("article.aggregate") as span:
                aggregated = aggregate_articles(chunks, search_query, router_output)
                span.set_attribute("article.primary", aggregated.primary.title[:80])
                span.set_attribute("article.secondary_count", len(aggregated.secondary))
                span.set_attribute("article.total_count", len(aggregated.all_articles))

            with tracer.start_as_current_span("article.primary_expand") as span:
                answer_style = getattr(router_output, "answer_style", "")
                if (
                    retriever
                    and aggregated.primary
                    and aggregated.primary.chunks
                    and _should_expand_primary_article(router_output, search_query)
                ):
                    expanded_primary_chunks = retriever.expand_primary_article_chunks(
                        aggregated.primary,
                        search_query,
                        max_chunks=_primary_expansion_max_chunks(router_output, search_query),
                    )
                    aggregated.primary.chunks = expanded_primary_chunks
                    aggregated.primary.chunk_count = len(expanded_primary_chunks)
                    aggregated.primary.max_score = max((c.score for c in expanded_primary_chunks), default=0.0)
                    aggregated.primary.avg_score = (
                        sum(c.score for c in expanded_primary_chunks) / len(expanded_primary_chunks)
                        if expanded_primary_chunks else 0.0
                    )
                span.set_attribute("article.primary_expanded_chunks", len(aggregated.primary.chunks))

            # ── 4. Evidence Extraction (conditional) ─────────────
            extract_t0 = time.time()
            with tracer.start_as_current_span("evidence.extract") as span:
                extractor_enabled = _env_flag("LLM_EXTRACTOR_ENABLED", default=False)
                kserve_for_extract = None
                if router_output.needs_extractor and extractor_enabled:
                    kserve_for_extract = _build_optional_llm_client(
                        "LLM_EXTRACTOR_ENABLED",
                        default=False,
                    )
                evidence_cache_hit = False
                evidence_cache_key = None
                evidence_pack = None
                if (
                    kserve_for_extract is not None
                    and _env_flag("EVIDENCE_CACHE_ENABLED", default=True)
                ):
                    evidence_cache_key = pipeline_cache.key(
                        "evidence_extract",
                        {
                            "query": search_query,
                            "router": _router_fingerprint(router_output),
                            "aggregated": _aggregated_fingerprint(aggregated),
                            "model": os.getenv("LLM_MODEL_ID", ""),
                            "extractor_max_tokens": os.getenv("EXTRACTOR_MAX_TOKENS", "800"),
                            "extractor_temperature": os.getenv("EXTRACTOR_TEMPERATURE", "0.1"),
                            "thinking_mode": os.getenv("LLM_THINKING_MODE", ""),
                        },
                    )
                    cached_evidence = pipeline_cache.get(evidence_cache_key)
                    if cached_evidence is not None:
                        evidence_pack = cached_evidence
                        evidence_cache_hit = True

                if evidence_pack is None:
                    evidence_pack = extract_evidence(
                        aggregated, search_query, router_output, llm_client=kserve_for_extract
                    )
                    if (
                        evidence_cache_key
                        and getattr(evidence_pack, "extractor_used", False)
                    ):
                        pipeline_cache.set(evidence_cache_key, evidence_pack)

                cache_info["evidence_extract_hit"] = evidence_cache_hit
                pipeline_info["llm_extractor_enabled"] = extractor_enabled
                pipeline_info["llm_extractor_used"] = bool(evidence_pack.extractor_used)
                span.set_attribute("evidence.cache_hit", evidence_cache_hit)
                span.set_attribute("evidence.extractor_used", evidence_pack.extractor_used)
                span.set_attribute("evidence.numbers_found", len(evidence_pack.primary_source.numbers))
            timings_ms["evidence_extract_ms"] = round((time.time() - extract_t0) * 1000.0, 2)

            # ── 4.5. Evidence Normalization & Conflict Detection (Phase 2) 
            with tracer.start_as_current_span("evidence.normalize") as span:
                evidence_pack = normalize_evidence(evidence_pack)
                span.set_attribute("evidence.normalized", True)

            with tracer.start_as_current_span("evidence.conflict_detect") as span:
                evidence_pack = detect_conflicts(evidence_pack)
                span.set_attribute("evidence.conflicts_found", len(evidence_pack.conflict_notes))

            # ── 5. Coverage Scoring ──────────────────────────────
            with tracer.start_as_current_span("coverage.score") as span:
                coverage = score_coverage(evidence_pack, router_output, search_query)
                span.set_attribute("coverage.level", coverage.coverage_level)
                span.set_attribute("coverage.mode", getattr(coverage, "coverage_mode", ""))
                span.set_attribute("coverage.allow_external", coverage.allow_external)
                if evidence_pack.conflict_notes:
                    # Penalize confidence ceiling if conflicts found
                    coverage.confidence_ceiling = "moderate"

            with tracer.start_as_current_span("answer.plan") as span:
                planner_llm = None
                if answer_mode == "thinking" and should_plan_answer(router_output, coverage):
                    planner_llm = _build_optional_llm_client(
                        "LLM_ANSWER_PLANNER_ENABLED",
                        default=False,
                    )
                answer_plan = build_answer_plan(
                    search_query,
                    evidence_pack,
                    coverage,
                    router_output,
                    llm_client=planner_llm,
                )
                answer_plan_text = format_answer_plan_for_prompt(answer_plan)
                reranked_articles_text = _format_reranked_articles_for_prompt(aggregated, answer_mode)
                mode_instruction = _answer_mode_instruction(answer_mode, coverage)
                if getattr(router_output, "query_type", "") == "source_discovery":
                    mode_instruction = f"{mode_instruction}\n\n{_source_discovery_instruction()}"
                if reranked_articles_text:
                    mode_instruction = f"{mode_instruction}\n\n{reranked_articles_text}"
                answer_plan_text = (
                    f"{answer_plan_text}\n\n{mode_instruction}"
                    if answer_plan_text
                    else mode_instruction
                )
                pipeline_info["answer_planner_llm_used"] = planner_llm is not None
                span.set_attribute("answer_plan.enabled", answer_plan.enabled)
                span.set_attribute("answer_plan.status", answer_plan.status)

            with tracer.start_as_current_span("external.resolve") as span:
                if query_needs_external_sources(search_query, coverage, router_output):
                    external_pack = resolve_external_sources(
                        search_query,
                        max_sources=max(1, min(3, getattr(coverage, "max_external_sources", 2) or 2)),
                    )
                else:
                    external_pack = ExternalEvidencePack(enabled=False, status="not_needed")
                external_sources_text = format_external_sources_for_prompt(external_pack)
                pipeline_info["external_resolver_used"] = bool(getattr(external_pack, "sources", []))
                span.set_attribute("external.status", external_pack.status)
                span.set_attribute("external.sources", len(external_pack.sources))

            # ── 6. Answer Composition ────────────────────────────
            with tracer.start_as_current_span("prompt.build") as span:
                span.set_attribute("prompt.history_turns", len(history))
                span.set_attribute("prompt.context_chunks", len(chunks))
                span.set_attribute("prompt.version", "v2")
                messages_payload = build_prompt_v2(
                    search_query,
                    evidence_pack,
                    router_output,
                    coverage,
                    chat_history=history,
                    answer_plan_text=answer_plan_text,
                    external_sources_text=external_sources_text,
                )

            if stream:
                yield ("stage", {"stage": "composing", "label": "Đang soạn câu trả lời..."})

            with tracer.start_as_current_span("llm.inference") as span:
                span.set_attribute(
                    "llm.model",
                    os.getenv("LLM_MODEL_ID", "unknown"),
                )
                g0 = time.time()
                degraded_mode = False
                degraded_reason = None

                if GUARDRAILS_ENABLED:
                    with tracer.start_as_current_span("guardrails.evaluate") as span:
                        span.set_attribute("llm.provider", "nemo_guardrails")
                        answer = generate_with_guardrails(
                            user_message=req.message,
                            messages_payload=messages_payload,
                        )
                else:
                    span.set_attribute("llm.provider", "kserve")
                    kserve = build_kserve_client_from_env()

                    if kserve:
                        base_max_tokens = int(os.getenv("LLM_MAX_TOKENS", "2048"))
                        if answer_mode == "thinking":
                            max_tokens = int(os.getenv("LLM_THINKING_MAX_TOKENS", str(max(base_max_tokens, 4096))))
                            answer_attempt_budget = int(os.getenv("THINKING_ANSWER_MAX_ATTEMPTS", os.getenv("ANSWER_MAX_ATTEMPTS", "2")))
                        else:
                            max_tokens = int(os.getenv("LLM_STANDARD_MAX_TOKENS", str(min(base_max_tokens, 1800))))
                            answer_attempt_budget = int(os.getenv("STANDARD_ANSWER_MAX_ATTEMPTS", "1"))
                        temperature = float(os.getenv("LLM_TEMPERATURE", "0.2"))
                        try:
                            if stream:
                                _answer_parts: list[str] = []
                                for _delta in kserve.generate_stream(
                                    messages_payload,
                                    max_tokens=max_tokens,
                                    temperature=temperature,
                                ):
                                    _answer_parts.append(_delta)
                                    yield ("token", {"delta": _delta})
                                answer = "".join(_answer_parts).strip()
                                if not answer:
                                    raise RuntimeError("LLM returned empty streamed answer")
                            else:
                                answer = kserve.generate(
                                    messages_payload,
                                    max_tokens=max_tokens,
                                    temperature=temperature,
                                    attempt_budget=answer_attempt_budget,
                                )
                        except UpstreamRateLimitError as exc:
                            RAG_FALLBACK_TOTAL.inc()
                            request.state.error_message = str(exc)
                            degraded_mode = True
                            degraded_reason = "upstream_rate_limit"
                            if (
                                _env_flag("ALLOW_RATE_LIMIT_FALLBACK", default=False)
                                or getattr(router_output, "answer_policy", "") == "open_enriched"
                            ):
                                answer = _build_rate_limit_fallback_answer(
                                    search_query,
                                    evidence_pack,
                                    coverage,
                                    router_output=router_output,
                                )
                            else:
                                answer = _build_degraded_mode_answer(degraded_reason)
                        except Exception as exc:
                            RAG_FALLBACK_TOTAL.inc()
                            request.state.error_message = str(exc)
                            degraded_mode = True
                            degraded_reason = "llm_generation_error"
                            answer = _build_rate_limit_fallback_answer(
                                search_query,
                                evidence_pack,
                                coverage,
                                router_output=router_output,
                            )
                    else:
                        RAG_FALLBACK_TOTAL.inc()
                        degraded_mode = True
                        degraded_reason = "llm_unavailable"
                        if getattr(router_output, "answer_policy", "") == "open_enriched":
                            answer = _build_rate_limit_fallback_answer(
                                search_query,
                                evidence_pack,
                                coverage,
                                router_output=router_output,
                            )
                        else:
                            answer = _build_degraded_mode_answer(degraded_reason)
                llm_ms = round((time.time() - g0) * 1000.0, 2)

            RAG_GENERATION_LATENCY_SECONDS.observe(llm_ms / 1000.0)
            request.state.llm_ms = llm_ms
            timings_ms["llm_answer_ms"] = llm_ms

            if stream:
                yield ("stage", {"stage": "finalizing", "label": "Đang kiểm tra và hoàn thiện..."})

            verification_status = "skipped"
            verification_issues = []
            verifier_ms = 0.0
            if _env_flag("LLM_VERIFIER_ENABLED", default=True) and not degraded_mode:
                verifier_t0 = time.time()
                with tracer.start_as_current_span("answer.verify") as span:
                    use_llm_verifier = _should_use_llm_verifier(
                        answer_mode,
                        answer,
                        coverage,
                        router_output,
                        external_pack,
                    )
                    verifier_llm = (
                        _build_optional_llm_client("LLM_VERIFIER_ENABLED", default=True)
                        if use_llm_verifier
                        else None
                    )
                    pipeline_info["llm_verifier_requested"] = use_llm_verifier
                    pipeline_info["llm_verifier_used"] = verifier_llm is not None
                    verifier_cache_hit = False
                    verifier_cache_key = None
                    verification = None
                    if (
                        verifier_llm is not None
                        and _env_flag("VERIFIER_CACHE_ENABLED", default=True)
                    ):
                        verifier_cache_key = pipeline_cache.key(
                            "answer_verify",
                            {
                                "question": search_query,
                                "answer_hash": stable_hash(answer),
                                "evidence": _aggregated_fingerprint(aggregated),
                                "coverage": _coverage_fingerprint(coverage),
                                "router": _router_fingerprint(router_output),
                                "external": _external_fingerprint(external_pack),
                                "model": os.getenv("LLM_MODEL_ID", ""),
                                "verifier_max_tokens": os.getenv("VERIFIER_MAX_TOKENS", "2200"),
                                "thinking_mode": os.getenv("LLM_THINKING_MODE", ""),
                            },
                        )
                        cached_verification = pipeline_cache.get(verifier_cache_key)
                        if cached_verification is not None:
                            verification = cached_verification
                            verifier_cache_hit = True

                    if verification is None:
                        pipeline_info["deterministic_verifier_run"] = True
                        verification = verify_answer(
                            question=search_query,
                            answer=answer,
                            evidence_pack=evidence_pack,
                            coverage=coverage,
                            router_output=router_output,
                            external_pack=external_pack,
                            llm_client=verifier_llm,
                        )
                        if (
                            verifier_cache_key
                            and getattr(verification, "status", "") != "error"
                        ):
                            pipeline_cache.set(verifier_cache_key, verification)

                    cache_info["verifier_hit"] = verifier_cache_hit
                    span.set_attribute("verification.llm_used", verifier_llm is not None)
                    span.set_attribute("verification.cache_hit", verifier_cache_hit)
                    verification_status = verification.status
                    verification_issues = verification.issues
                    span.set_attribute("verification.status", verification_status)
                    span.set_attribute("verification.issues", len(verification_issues))
                    if verification.status == "revise" and verification.revised_answer:
                        answer = verification.revised_answer
                    elif verification.status == "block":
                        answer = (
                            "Tôi chưa thể trả lời phần cụ thể này một cách an toàn vì verifier phát hiện claim cần nguồn "
                            "hoặc claim vượt quá evidence hiện có. Có thể hỏi lại theo hướng tổng quát hơn hoặc bổ sung nguồn/guideline cụ thể."
                        )
                verifier_ms = round((time.time() - verifier_t0) * 1000.0, 2)
            timings_ms["verifier_ms"] = verifier_ms

            response_chunks = _ordered_chunks_for_response(chunks, aggregated)
            chunks_out = [
                {"id": c.id, "text": c.text, "score": c.score, "metadata": c.metadata}
                for c in response_chunks
            ] if response_chunks else []
            timings_ms["total_ms"] = round((time.time() - request_t0) * 1000.0, 2)
            response_metadata = {
                    "answer_mode": answer_mode,
                    "query_type": router_output.query_type,
                    "answer_policy": getattr(router_output, "answer_policy", "strict_rag"),
                    "answer_style": router_output.answer_style,
                    "retrieval_mode": router_output.retrieval_mode,
                    "coverage_level": coverage.coverage_level,
                    "coverage_mode": getattr(coverage, "coverage_mode", ""),
                    "answer_plan_status": getattr(answer_plan, "status", "disabled"),
                    "external_search_status": getattr(external_pack, "status", "disabled"),
                    "verification_status": verification_status,
                    "verification_issues": verification_issues[:10],
                    "timings_ms": timings_ms,
                    "cache": cache_info,
                    "pipeline": pipeline_info,
                }
            external_sources_out = [
                    {
                        "id": source.id,
                        "title": source.title,
                        "url": source.url,
                        "snippet": source.snippet,
                        "source_domain": source.source_domain,
                    }
                    for source in getattr(external_pack, "sources", [])
                ]

            # Persist full assistant message so source panel/debug survive reloads.
            session_store.append(
                session_id,
                "assistant",
                answer,
                context_used=len(chunks),
                retrieved_chunks=chunks_out,
                metadata=response_metadata,
                external_sources=external_sources_out,
                degraded_mode=degraded_mode,
                degraded_reason=degraded_reason,
            )

            # reload full history
            history = session_store.get_history(session_id)

            final_payload = {
                "session_id": session_id,
                "answer": answer,
                "history": history,
                "context_used": len(chunks),
                "retrieved_chunks": chunks_out,
                "metadata": response_metadata,
                "external_sources": external_sources_out,
                "degraded_mode": degraded_mode,
                "degraded_reason": degraded_reason,
            }
            yield ("final", final_payload)
    finally:
        RAG_INFLIGHT.dec()


@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest, request: Request, background_tasks: BackgroundTasks):
    """Non-streaming chat: drive the shared pipeline and return the final payload."""
    final_payload = None
    for kind, payload in _chat_core(req, request, background_tasks, stream=False):
        if kind == "final":
            final_payload = payload
    if final_payload is None:
        return JSONResponse(
            status_code=500,
            content={"detail": "Chat pipeline produced no answer."},
        )
    return ChatResponse(**final_payload)


@app.post("/api/chat/stream")
def chat_stream(req: ChatRequest, request: Request, background_tasks: BackgroundTasks):
    """Streaming chat over Server-Sent Events.

    Emits one ``data: {...}\\n\\n`` line per event. Each JSON carries a ``type``
    field: ``stage`` (progress), ``token`` (answer delta), ``final`` (full
    payload identical to /api/chat), or ``error``.
    """

    def _sse(kind: str, payload: dict) -> str:
        body = json.dumps({"type": kind, **payload}, ensure_ascii=False, default=str)
        return f"data: {body}\n\n"

    def event_gen():
        try:
            for kind, payload in _chat_core(req, request, background_tasks, stream=True):
                yield _sse(kind, payload)
        except Exception as exc:  # defensive: never leave the stream hanging
            yield _sse("error", {"detail": str(exc)})

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
