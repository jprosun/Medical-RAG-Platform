import html
import json
import logging
import os
import re
import time
import uuid
from typing import Any

import requests
import streamlit as st
from opentelemetry import trace
from utils.tracing import setup_tracing


# -----------------------
# OpenTelemetry tracing
# -----------------------
setup_tracing()
tracer = trace.get_tracer("streamlit-ui")


# -----------------------
# Configuration
# -----------------------
RAG_API_URL = os.getenv(
    "RAG_API_URL",
    "http://rag-orchestrator.model-serving.svc.cluster.local:8000",
)
REQUEST_TIMEOUT_S = int(os.getenv("RAG_API_TIMEOUT_S", "180"))
SHOW_TIMINGS = os.getenv("SHOW_TIMINGS", "false").lower() in ("1", "true", "yes", "y", "on")

ANSWER_MODES = {
    "Standard": "standard",
    "Thinking": "thinking",
}


# -----------------------
# Structured stdout logger
# -----------------------
logger = logging.getLogger("streamlit_ui")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)


# -----------------------
# UI setup
# -----------------------
st.set_page_config(
    page_title="MedQA Assistant",
    page_icon="MedQA",
    layout="wide",
)

st.markdown(
    """
    <style>
    :root {
      --medqa-ink: #13201c;
      --medqa-muted: #5f6f6a;
      --medqa-line: #d8e2dd;
      --medqa-surface: #f7faf7;
      --medqa-accent: #0f766e;
      --medqa-warm: #b7791f;
    }
    .block-container {
      padding-top: 1.25rem;
      max-width: 1500px;
    }
    .medqa-hero {
      border: 1px solid var(--medqa-line);
      background:
        radial-gradient(circle at 10% 0%, rgba(15, 118, 110, 0.16), transparent 28%),
        linear-gradient(135deg, #fbf8ef 0%, #f2faf6 55%, #edf5f2 100%);
      border-radius: 22px;
      padding: 1.2rem 1.35rem;
      margin-bottom: 1rem;
      box-shadow: 0 12px 35px rgba(19, 32, 28, 0.07);
    }
    .medqa-title {
      color: var(--medqa-ink);
      font-family: Georgia, "Times New Roman", serif;
      font-size: 2.1rem;
      line-height: 1.1;
      margin: 0;
      letter-spacing: -0.03em;
    }
    .medqa-subtitle {
      color: var(--medqa-muted);
      font-size: 0.98rem;
      margin-top: 0.35rem;
    }
    .source-card {
      border: 1px solid var(--medqa-line);
      border-radius: 16px;
      padding: 0.75rem 0.85rem;
      background: #ffffff;
      margin-bottom: 0.7rem;
    }
    .source-kicker {
      color: var(--medqa-accent);
      font-weight: 700;
      font-size: 0.82rem;
      letter-spacing: 0.02em;
    }
    .source-title {
      color: var(--medqa-ink);
      font-weight: 700;
      margin: 0.12rem 0 0.2rem;
    }
    .source-meta {
      color: var(--medqa-muted);
      font-size: 0.82rem;
      margin-bottom: 0.4rem;
    }
    .evidence-snippet {
      border-left: 3px solid var(--medqa-accent);
      padding-left: 0.65rem;
      color: #263a35;
      background: #f8fbf9;
      border-radius: 0 10px 10px 0;
      font-size: 0.88rem;
    }
    .status-pill {
      display: inline-block;
      padding: 0.15rem 0.5rem;
      border-radius: 999px;
      border: 1px solid var(--medqa-line);
      background: var(--medqa-surface);
      color: var(--medqa-muted);
      font-size: 0.78rem;
      margin-right: 0.25rem;
      margin-bottom: 0.25rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="medqa-hero">
      <h1 class="medqa-title">MedQA Assistant</h1>
      <div class="medqa-subtitle">
        RAG chatbot y khoa với citation, evidence viewer và chế độ trả lời tùy chỉnh.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# -----------------------
# Session state
# -----------------------
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    st.session_state.messages = []
if "feedback" not in st.session_state:
    st.session_state.feedback = {}
if "selected_evidence_message_id" not in st.session_state:
    st.session_state.selected_evidence_message_id = None
if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = None


# -----------------------
# Helpers
# -----------------------
def _ensure_message_id(message: dict[str, Any]) -> str:
    if "_ui_id" not in message:
        message["_ui_id"] = str(uuid.uuid4())
    return message["_ui_id"]


def _shorten(text: str, limit: int = 900) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0] + "..."


def _source_key(metadata: dict[str, Any]) -> str:
    url = str(metadata.get("source_url") or "").strip()
    if url:
        return "url:" + url
    title = str(metadata.get("title") or metadata.get("canonical_title") or "").strip().lower()
    source = str(metadata.get("source_name") or metadata.get("source_id") or "").strip().lower()
    return f"title:{source}:{title}"


def build_rag_sources(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    index_by_key: dict[str, int] = {}
    for chunk in chunks or []:
        metadata = chunk.get("metadata") or {}
        key = _source_key(metadata)
        if not key.strip(":"):
            key = "chunk:" + str(chunk.get("id") or len(sources))
        if key not in index_by_key:
            index_by_key[key] = len(sources)
            sources.append(
                {
                    "number": len(sources) + 1,
                    "title": metadata.get("title") or metadata.get("canonical_title") or "Nguồn không có tiêu đề",
                    "source_name": metadata.get("source_name") or metadata.get("source_id") or "RAG",
                    "source_url": metadata.get("source_url") or "",
                    "doc_type": metadata.get("doc_type") or "",
                    "section_title": metadata.get("section_title") or "",
                    "score": chunk.get("score"),
                    "snippets": [],
                }
            )
        source = sources[index_by_key[key]]
        if len(source["snippets"]) < 3:
            source["snippets"].append(
                {
                    "text": _shorten(chunk.get("text") or "", 850),
                    "section_title": metadata.get("section_title") or "",
                    "score": chunk.get("score"),
                }
            )
    return sources[:8]


def build_external_sources(external_sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for idx, source in enumerate(external_sources or [], start=1):
        result.append(
            {
                "id": source.get("id") or f"E{idx}",
                "title": source.get("title") or "External source",
                "url": source.get("url") or "",
                "snippet": source.get("snippet") or "",
                "source_domain": source.get("source_domain") or "",
            }
        )
    return result


def linkify_citations(answer: str, rag_sources: list[dict[str, Any]], external_sources: list[dict[str, Any]]) -> str:
    url_by_num = {
        str(source["number"]): source.get("source_url")
        for source in rag_sources
        if source.get("source_url")
    }
    url_by_external = {
        str(source["id"]).upper(): source.get("url")
        for source in external_sources
        if source.get("url")
    }

    def replace_external(match: re.Match) -> str:
        label = match.group(1).upper()
        url = url_by_external.get(label)
        return f"[[{label}]]({url})" if url else f"[{label}]"

    def replace_rag(match: re.Match) -> str:
        label = match.group(1)
        url = url_by_num.get(label)
        return f"[[{label}]]({url})" if url else f"[{label}]"

    text = re.sub(r"\[(E\d+)\]", replace_external, answer or "")
    text = re.sub(r"\[(\d+)\]", replace_rag, text)
    return text


def latest_assistant_with_sources() -> dict[str, Any] | None:
    selected = st.session_state.get("selected_evidence_message_id")
    if selected:
        for message in st.session_state.messages:
            if message.get("_ui_id") == selected and message.get("role") == "assistant":
                return message
    for message in reversed(st.session_state.messages):
        if message.get("role") == "assistant":
            return message
    return None


def mode_caption(mode: str) -> str:
    if mode == "thinking":
        return "Trả lời thật chi tiết, học thuật hơn, retrieval sâu hơn và có thể bổ sung kiến thức nền/chuyên sâu an toàn."
    return "Trả lời đầy đủ ý, đủ dài, bám RAG evidence và citation rõ ràng."


def build_followups(user_question: str, metadata: dict[str, Any]) -> list[str]:
    query_type = (metadata or {}).get("query_type", "")
    if query_type == "comparative_synthesis":
        return [
            f"Giải thích sâu hơn các tiêu chí ra quyết định trong câu hỏi: {user_question}",
            f"Tóm tắt ngắn gọn điểm khác nhau quan trọng nhất: {user_question}",
            f"Chỉ dùng tài liệu truy hồi để trả lời lại câu hỏi: {user_question}",
        ]
    if query_type in {"professional_explainer", "teaching_explainer"}:
        return [
            f"Giải thích cơ chế và ý nghĩa lâm sàng sâu hơn: {user_question}",
            f"Tóm tắt cho sinh viên y dễ nhớ hơn: {user_question}",
            f"Nêu ví dụ lâm sàng minh họa cho chủ đề: {user_question}",
        ]
    return [
        f"Giải thích kỹ hơn câu trả lời trên: {user_question}",
        f"Tóm tắt câu trả lời trên thành các ý chính: {user_question}",
        f"Trả lời lại chỉ dựa trên tài liệu có citation: {user_question}",
    ]


def render_status(metadata: dict[str, Any], context_used: int, duration_ms: float | None = None) -> None:
    if not metadata and not context_used:
        return
    pills = []
    if metadata.get("answer_mode"):
        pills.append(f"Mode: {metadata.get('answer_mode')}")
    if metadata.get("answer_policy"):
        pills.append(f"Policy: {metadata.get('answer_policy')}")
    if metadata.get("coverage_mode"):
        pills.append(f"Coverage: {metadata.get('coverage_mode')}")
    if context_used:
        pills.append(f"Chunks: {context_used}")
    if duration_ms:
        pills.append(f"{round(duration_ms / 1000, 1)}s")
    st.markdown(
        " ".join(f'<span class="status-pill">{html.escape(str(item))}</span>' for item in pills),
        unsafe_allow_html=True,
    )


def render_feedback(message_id: str) -> None:
    current = st.session_state.feedback.get(message_id)
    options = [
        ("Hữu ích", "useful"),
        ("Thiếu nguồn", "missing_source"),
        ("Quá ngắn", "too_short"),
        ("Nguồn chưa đúng", "bad_source"),
    ]
    labels = [label for label, _value in options]
    values_by_label = {label: value for label, value in options}
    current_label = next((label for label, value in options if value == current), None)
    selected_label = st.radio(
        "Feedback",
        labels,
        index=labels.index(current_label) if current_label in labels else None,
        horizontal=True,
        key=f"feedback_{message_id}",
        label_visibility="collapsed",
    )
    if selected_label:
        selected_value = values_by_label[selected_label]
        if current != selected_value:
            st.session_state.feedback[message_id] = selected_value
            st.toast(f"Đã ghi feedback: {selected_label}")


def render_followups(message: dict[str, Any], index: int) -> None:
    user_question = ""
    for prev in reversed(st.session_state.messages[:index]):
        if prev.get("role") == "user":
            user_question = prev.get("content", "")
            break
    suggestions = build_followups(user_question, message.get("metadata") or {})
    with st.expander("Gợi ý hỏi tiếp", expanded=False):
        for idx, suggestion in enumerate(suggestions, start=1):
            if st.button(suggestion, key=f"followup_{message.get('_ui_id')}_{idx}", use_container_width=True):
                st.session_state.pending_prompt = suggestion
                st.rerun()


def render_sources_panel(message: dict[str, Any] | None, show_debug: bool) -> None:
    st.subheader("Nguồn và evidence")
    if not message:
        st.info("Chưa có câu trả lời nào có nguồn để hiển thị.")
        return

    chunks = message.get("retrieved_chunks") or []
    rag_sources = build_rag_sources(chunks)
    external_sources = build_external_sources(message.get("external_sources") or [])
    metadata = message.get("metadata") or {}

    if rag_sources:
        st.caption("RAG sources. Citation trong câu trả lời như [1], [2] tương ứng các nguồn dưới đây theo thứ tự truy hồi.")
        for source in rag_sources:
            with st.expander(f"[{source['number']}] {source['title']}", expanded=source["number"] <= 2):
                st.markdown(f"**Nguồn:** {source['source_name']}")
                meta_bits = [bit for bit in [source.get("doc_type"), source.get("section_title")] if bit]
                if meta_bits:
                    st.caption(" | ".join(str(bit) for bit in meta_bits))
                if source.get("source_url"):
                    st.link_button("Mở link nguồn", source["source_url"], use_container_width=True)
                for idx, snippet in enumerate(source["snippets"], start=1):
                    score = snippet.get("score")
                    score_text = f" | score={score:.3f}" if isinstance(score, (int, float)) else ""
                    st.markdown(f"**Đoạn evidence {idx}{score_text}**")
                    st.markdown(
                        f"<div class='evidence-snippet'>{html.escape(snippet.get('text') or '')}</div>",
                        unsafe_allow_html=True,
                    )
    else:
        st.warning("Response này chưa có retrieved_chunks để hiển thị. Với các câu trả lời cũ, metadata/source có thể chưa được lưu vào session; hãy hỏi lại để hệ thống lưu đầy đủ nguồn.")

    if external_sources:
        st.divider()
        st.caption("External web sources")
        for source in external_sources:
            with st.expander(f"[{source['id']}] {source['title']}", expanded=False):
                st.caption(source.get("source_domain") or "")
                if source.get("url"):
                    st.link_button("Mở nguồn web", source["url"], use_container_width=True)
                if source.get("snippet"):
                    st.write(source["snippet"])

    if show_debug:
        st.divider()
        st.subheader("Debug RAG")
        if metadata or message.get("context_used") is not None:
            st.json(
                {
                    "metadata": metadata,
                    "context_used": message.get("context_used"),
                    "degraded_mode": message.get("degraded_mode"),
                    "degraded_reason": message.get("degraded_reason"),
                }
            )
        else:
            st.info("Debug metadata chưa có cho response này.")


def render_assistant_message(message: dict[str, Any], index: int) -> None:
    message_id = _ensure_message_id(message)
    rag_sources = build_rag_sources(message.get("retrieved_chunks") or [])
    external_sources = build_external_sources(message.get("external_sources") or [])
    answer = linkify_citations(message.get("content", ""), rag_sources, external_sources)

    with st.chat_message("assistant"):
        render_status(message.get("metadata") or {}, int(message.get("context_used") or 0), message.get("duration_ms"))
        st.markdown(answer)
        col_a, col_b = st.columns([1, 1])
        has_sources = bool(message.get("retrieved_chunks") or message.get("external_sources") or message.get("metadata"))
        if col_a.button("Xem nguồn trong panel", key=f"sources_{message_id}", use_container_width=True, disabled=not has_sources):
            st.session_state.selected_evidence_message_id = message_id
            st.rerun()
        with col_b:
            render_feedback(message_id)
        render_followups(message, index)


def load_available_sessions() -> list[dict[str, Any]]:
    try:
        response = requests.get(f"{RAG_API_URL}/api/sessions", timeout=5)
        if response.status_code == 200:
            sessions = response.json().get("sessions", [])
            return sorted(sessions, key=lambda item: item.get("updated_at") or 0, reverse=True)
    except Exception:
        pass
    return []


# -----------------------
# Sidebar
# -----------------------
available_sessions = load_available_sessions()

with st.sidebar:
    st.header("Trợ lý Y Khoa MedQA")

    selected_label = st.radio(
        "Chế độ trả lời",
        list(ANSWER_MODES.keys()),
        index=list(ANSWER_MODES.values()).index(st.session_state.get("answer_mode", "standard"))
        if st.session_state.get("answer_mode", "standard") in ANSWER_MODES.values()
        else 0,
    )
    st.session_state.answer_mode = ANSWER_MODES[selected_label]
    st.caption(mode_caption(st.session_state.answer_mode))

    st.session_state.show_debug_metadata = st.toggle(
        "Debug metadata",
        value=bool(st.session_state.get("show_debug_metadata", False)),
        help="Hiển thị query_type, answer_policy, coverage, timings và cache.",
    )

    st.divider()

    if st.button("Cuộc trò chuyện mới", use_container_width=True):
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.messages = []
        st.session_state.editing_session = None
        st.session_state.selected_evidence_message_id = None
        st.rerun()

    if st.session_state.messages:
        if st.button("Xóa cuộc trò chuyện này", use_container_width=True, type="secondary"):
            try:
                requests.delete(f"{RAG_API_URL}/api/session/{st.session_state.session_id}", timeout=5)
            except Exception:
                pass
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state.messages = []
            st.session_state.editing_session = None
            st.session_state.selected_evidence_message_id = None
            st.rerun()

    st.divider()
    st.subheader("Lịch sử trò chuyện")
    for session in available_sessions:
        sid = session.get("id", "")
        title = session.get("title", "Cuộc trò chuyện")
        if not sid:
            continue
        if st.session_state.get("editing_session") == sid:
            new_title = st.text_input("Tên mới:", value=title, key=f"edit_input_{sid}")
            col_save, col_cancel = st.columns(2)
            if col_save.button("Lưu", key=f"save_{sid}", use_container_width=True):
                requests.put(f"{RAG_API_URL}/api/session/{sid}/title", json={"title": new_title}, timeout=5)
                st.session_state.editing_session = None
                st.rerun()
            if col_cancel.button("Hủy", key=f"cancel_{sid}", use_container_width=True):
                st.session_state.editing_session = None
                st.rerun()
            continue

        col_title, col_edit, col_delete = st.columns([7, 1.5, 1.5])
        display_text = title if len(title) < 28 else title[:25] + "..."
        btn_type = "primary" if sid == st.session_state.session_id else "secondary"
        if col_title.button(display_text, key=f"btn_{sid}", use_container_width=True, type=btn_type):
            if sid != st.session_state.session_id:
                st.session_state.session_id = sid
                try:
                    history_response = requests.get(f"{RAG_API_URL}/api/session/{sid}", timeout=5)
                    if history_response.status_code == 200:
                        st.session_state.messages = history_response.json().get("messages", [])
                        st.session_state.selected_evidence_message_id = None
                        st.session_state.editing_session = None
                        st.rerun()
                except Exception:
                    pass
        if col_edit.button("Sửa", key=f"edit_btn_{sid}", use_container_width=True):
            st.session_state.editing_session = sid
            st.rerun()
        if col_delete.button("Xóa", key=f"del_btn_{sid}", use_container_width=True):
            try:
                requests.delete(f"{RAG_API_URL}/api/session/{sid}", timeout=5)
            except Exception:
                pass
            if st.session_state.session_id == sid:
                st.session_state.session_id = str(uuid.uuid4())
                st.session_state.messages = []
                st.session_state.selected_evidence_message_id = None
            st.rerun()


# -----------------------
# Main layout
# -----------------------
chat_col, evidence_col = st.columns([0.68, 0.32], gap="large")

with chat_col:
    for idx, message in enumerate(st.session_state.messages):
        _ensure_message_id(message)
        if message.get("role") == "assistant":
            render_assistant_message(message, idx)
        else:
            with st.chat_message(message.get("role", "user")):
                st.markdown(message.get("content", ""))

with evidence_col:
    render_sources_panel(latest_assistant_with_sources(), bool(st.session_state.get("show_debug_metadata", False)))


# -----------------------
# Chat input and request handling
# -----------------------
pending_prompt = st.session_state.pop("pending_prompt", None)
typed_prompt = st.chat_input("Nhập câu hỏi y khoa...")
prompt = pending_prompt or typed_prompt

if prompt:
    with tracer.start_as_current_span("ui.submit_question") as span:
        span.set_attribute("ui.framework", "streamlit")
        span.set_attribute("session_id", st.session_state.session_id)
        span.set_attribute("prompt.length", len(prompt))
        span.set_attribute("answer_mode", st.session_state.answer_mode)

        user_message = {"role": "user", "content": prompt, "_ui_id": str(uuid.uuid4())}
        st.session_state.messages.append(user_message)
        with chat_col:
            with st.chat_message("user"):
                st.markdown(prompt)

        start = time.time()
        status_code = None
        error_msg = None
        answer = ""
        context_used = 0
        metadata: dict[str, Any] = {}
        retrieved_chunks: list[dict[str, Any]] = []
        external_sources: list[dict[str, Any]] = []
        degraded_mode = False
        degraded_reason = None
        timings_ms: dict[str, Any] = {}
        cache_info: dict[str, Any] = {}

        try:
            response = requests.post(
                f"{RAG_API_URL}/api/chat",
                json={
                    "session_id": st.session_state.session_id,
                    "message": prompt,
                    "answer_mode": st.session_state.answer_mode,
                },
                timeout=REQUEST_TIMEOUT_S,
            )
            status_code = response.status_code
            response.raise_for_status()
            data = response.json()
            answer = data.get("answer", "")
            context_used = int(data.get("context_used", 0))
            retrieved_chunks = data.get("retrieved_chunks") or []
            external_sources = data.get("external_sources") or []
            degraded_mode = bool(data.get("degraded_mode"))
            degraded_reason = data.get("degraded_reason")
            metadata = data.get("metadata") or {}
            if isinstance(metadata, dict):
                timings_ms = metadata.get("timings_ms") or {}
                cache_info = metadata.get("cache") or {}
        except Exception as exc:
            error_msg = str(exc)
            answer = f"Lỗi khi gọi RAG API: {exc}"
            context_used = 0

        duration_ms = round((time.time() - start) * 1000.0, 2)

        span.set_attribute("http.status_code", status_code or 0)
        span.set_attribute("rag.duration_ms", duration_ms)
        span.set_attribute("rag.context_used", context_used)
        if error_msg:
            span.record_exception(Exception(error_msg))

        logger.info(
            json.dumps(
                {
                    "service": "streamlit-ui",
                    "event": "chat_request",
                    "session_id": st.session_state.session_id,
                    "status": status_code,
                    "duration_ms": duration_ms,
                    "context_used": context_used,
                    "answer_mode": st.session_state.answer_mode,
                    "timings_ms": timings_ms,
                    "cache": cache_info,
                    "error": error_msg,
                },
                ensure_ascii=False,
            )
        )

        assistant_message = {
            "role": "assistant",
            "content": answer,
            "_ui_id": str(uuid.uuid4()),
            "context_used": context_used,
            "metadata": metadata,
            "retrieved_chunks": retrieved_chunks,
            "external_sources": external_sources,
            "duration_ms": duration_ms,
            "degraded_mode": degraded_mode,
            "degraded_reason": degraded_reason,
        }
        st.session_state.messages.append(assistant_message)
        st.session_state.selected_evidence_message_id = assistant_message["_ui_id"]

        with chat_col:
            render_assistant_message(assistant_message, len(st.session_state.messages) - 1)

        st.rerun()
