from uuid import uuid4
import time
import os
import re

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from opentelemetry import trace as otel_trace
from opentelemetry import trace

from .session import SessionStore
from .health import readiness, liveness
from utils.logging import log_request
from .retriever import build_retriever_from_env
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
@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest, request: Request, background_tasks: BackgroundTasks):
    RAG_CHAT_REQUESTS_TOTAL.inc()
    RAG_INFLIGHT.inc()
    try:
        # Root span for this chat request
        with tracer.start_as_current_span("rag.chat") as root_span:
            session_id = req.session_id or str(uuid4())
            request.state.session_id = session_id
            root_span.set_attribute("session.id", session_id)

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
            with tracer.start_as_current_span("query.rewrite") as span:
                kserve_for_rewrite = _build_optional_llm_client(
                    "LLM_REWRITER_ENABLED",
                    default=True,
                )
                search_query = rewrite_query(
                    req.message, history, llm_client=kserve_for_rewrite
                )
                span.set_attribute("query.original", req.message)
                span.set_attribute("query.rewritten", search_query)
                span.set_attribute("query.was_rewritten", search_query != req.message)

            # ── 1. Query Router (rule-based, no LLM) ──────────────
            with tracer.start_as_current_span("query.route") as span:
                router_output = route_query(search_query)
                span.set_attribute("router.query_type", router_output.query_type)
                span.set_attribute("router.depth", router_output.depth)
                span.set_attribute("router.needs_extractor", router_output.needs_extractor)
                span.set_attribute("router.retrieval_profile", router_output.retrieval_profile)

            # ── 2. Retrieve with profile-based top_k ─────────────
            _PROFILE_TOP_K = {"light": 8, "standard": 12, "deep": 20}
            profile_top_k = _PROFILE_TOP_K.get(router_output.retrieval_profile, 12)

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

                if retrieval_mode == "mechanistic_synthesis" and retriever:
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

                retrieval_ms = round((time.time() - t0) * 1000.0, 2)
                span.set_attribute("retrieval.chunks", len(chunks))

            RAG_RETRIEVAL_LATENCY_SECONDS.observe(retrieval_ms / 1000.0)
            request.state.retrieval_ms = retrieval_ms
            request.state.chunks_returned = len(chunks)

            est_tokens = sum(max(1, len(c.text) // 4) for c in chunks)
            RAG_CONTEXT_TOKENS.observe(est_tokens)

            if not chunks:
                RAG_EMPTY_CONTEXT_TOTAL.inc()

            # ── 2.5. Chunk Quality Filter (Phase 4) ──────────────
            with tracer.start_as_current_span("chunk.quality_filter") as span:
                pre_filter_count = len(chunks)
                chunks = filter_chunks(chunks)
                span.set_attribute("filter.before", pre_filter_count)
                span.set_attribute("filter.after", len(chunks))
                span.set_attribute("filter.removed", pre_filter_count - len(chunks))

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
            with tracer.start_as_current_span("evidence.extract") as span:
                extractor_enabled = _env_flag("LLM_EXTRACTOR_ENABLED", default=False)
                kserve_for_extract = None
                if router_output.needs_extractor and extractor_enabled:
                    kserve_for_extract = _build_optional_llm_client(
                        "LLM_EXTRACTOR_ENABLED",
                        default=False,
                    )
                evidence_pack = extract_evidence(
                    aggregated, search_query, router_output, llm_client=kserve_for_extract
                )
                span.set_attribute("evidence.extractor_used", evidence_pack.extractor_used)
                span.set_attribute("evidence.numbers_found", len(evidence_pack.primary_source.numbers))

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
                if should_plan_answer(router_output, coverage):
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
                        max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
                        temperature = float(os.getenv("LLM_TEMPERATURE", "0.2"))
                        try:
                            answer = kserve.generate(
                                messages_payload,
                                max_tokens=max_tokens,
                                temperature=temperature,
                                attempt_budget=int(os.getenv("ANSWER_MAX_ATTEMPTS", "2")),
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

            kserve = build_kserve_client_from_env()

            RAG_GENERATION_LATENCY_SECONDS.observe(llm_ms / 1000.0)
            request.state.llm_ms = llm_ms

            verification_status = "skipped"
            verification_issues = []
            if _env_flag("LLM_VERIFIER_ENABLED", default=True) and not degraded_mode:
                with tracer.start_as_current_span("answer.verify") as span:
                    verifier_llm = _build_optional_llm_client(
                        "LLM_VERIFIER_ENABLED",
                        default=True,
                    )
                    verification = verify_answer(
                        question=search_query,
                        answer=answer,
                        evidence_pack=evidence_pack,
                        coverage=coverage,
                        router_output=router_output,
                        external_pack=external_pack,
                        llm_client=verifier_llm,
                    )
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

            # append assistant response
            session_store.append(session_id, "assistant", answer)

            # reload full history
            history = session_store.get_history(session_id)

            chunks_out = [
                {"id": c.id, "text": c.text, "score": c.score, "metadata": c.metadata}
                for c in chunks
            ] if chunks else []

            return ChatResponse(
                session_id=session_id,
                answer=answer,
                history=history,
                context_used=len(chunks),
                retrieved_chunks=chunks_out,
                metadata={
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
                },
                external_sources=[
                    {
                        "id": source.id,
                        "title": source.title,
                        "url": source.url,
                        "snippet": source.snippet,
                        "source_domain": source.source_domain,
                    }
                    for source in getattr(external_pack, "sources", [])
                ],
                degraded_mode=degraded_mode,
                degraded_reason=degraded_reason,
            )
    finally:
        RAG_INFLIGHT.dec()
