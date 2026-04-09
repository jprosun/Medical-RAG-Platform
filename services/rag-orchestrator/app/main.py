from uuid import uuid4
import time
import os

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
from .llm_client import build_kserve_client_from_env
from .schemas import ChatRequest, ChatResponse
from .query_router import route_query
from .article_aggregator import aggregate_articles
from .evidence_extractor import extract_evidence
from .chunk_quality_filter import filter_chunks
from .evidence_normalizer import normalize_evidence
from .conflict_detector import detect_conflicts
from .coverage_scorer import score_coverage
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

# ---------------------------------------------------------------------
# Background Task
# ---------------------------------------------------------------------
def generate_and_save_title(session_id: str, prompt: str):
    kserve = build_kserve_client_from_env()
    if kserve:
        try:
            sys_prompt = "Bạn là trợ lý ảo. Hãy đọc câu hỏi của người dùng và đặt tên cho đoạn chat. Tên ngắn gọn (3-6 từ), tóm tắt chủ đề chính, bằng tiếng Việt. KHÔNG giải thích, KHÔNG dùng dấu ngoặc kép, CHỈ trả về tên cuộc trò chuyện."
            msgs = [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": prompt}
            ]
            title = kserve.generate(msgs, max_tokens=15, temperature=0.3)
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
                background_tasks.add_task(generate_and_save_title, session_id, req.message)

            # append user message + trace
            with tracer.start_as_current_span("session.append_user"):
                session_store.append(session_id, "user", req.message)

            # query rewriting for multi-turn conversations
            with tracer.start_as_current_span("query.rewrite") as span:
                kserve_for_rewrite = build_kserve_client_from_env()
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
                        subqueries, top_k_per_query=top_k_per_sub
                    )
                else:
                    # Standard single-query retrieval
                    span.set_attribute("retrieval.mode", "single")
                    chunks = retriever.retrieve(
                        search_query, 
                        top_k_override=profile_top_k,
                        retrieval_mode=retrieval_mode,
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

            # ── 4. Evidence Extraction (conditional) ─────────────
            with tracer.start_as_current_span("evidence.extract") as span:
                kserve_for_extract = build_kserve_client_from_env() if router_output.needs_extractor else None
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
                span.set_attribute("coverage.allow_external", coverage.allow_external)
                if evidence_pack.conflict_notes:
                    # Penalize confidence ceiling if conflicts found
                    coverage.confidence_ceiling = "moderate"

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
                )

            with tracer.start_as_current_span("llm.inference") as span:
                span.set_attribute(
                    "llm.model",
                    os.getenv("LLM_MODEL_ID", "unknown"),
                )
                g0 = time.time()

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
                        answer = kserve.generate(
                            messages_payload,
                            max_tokens=max_tokens,
                            temperature=temperature,
                        )
                    else:
                        RAG_FALLBACK_TOTAL.inc()
                        if chunks:
                            answer = (
                                "General information based on available context:\n\n"
                                + "\n\n".join(
                                    f"- {c.text} [source:{c.id}]"
                                    for c in chunks[:3]
                                )
                                + "\n\n(Configure KSERVE_URL for full generation.)"
                            )
                        else:
                            answer = (
                                "I don't have enough context. "
                                "Ingest documents into Qdrant first."
                            )
                llm_ms = round((time.time() - g0) * 1000.0, 2)

            kserve = build_kserve_client_from_env()

            RAG_GENERATION_LATENCY_SECONDS.observe(llm_ms / 1000.0)
            request.state.llm_ms = llm_ms

            # append assistant response
            session_store.append(session_id, "assistant", answer)

            # reload full history
            history = session_store.get_history(session_id)

            chunks_out = [
                {"id": c.id, "text": c.text, "metadata": c.metadata} 
                for c in chunks
            ] if chunks else []

            return ChatResponse(
                session_id=session_id,
                answer=answer,
                history=history,
                context_used=len(chunks),
                retrieved_chunks=chunks_out,
            )
    finally:
        RAG_INFLIGHT.dec()
