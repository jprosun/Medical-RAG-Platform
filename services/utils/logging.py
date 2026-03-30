import json
import logging
import time
from typing import Any, Dict, Optional

from fastapi import Request

logger = logging.getLogger("rag_api")
logger.setLevel(logging.INFO)

log_handler = logging.StreamHandler()
# We log pure JSON so Logstash's json filter can parse it
log_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(log_handler)


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


async def log_request(
    request: Request,
    status_code: int,
    start: float,
) -> None:
    """
    Structured JSON log for /api calls.

    Extra timing + RAG info is pulled from request.state if the
    handler populated it (see main.py).
    """
    now = time.time()
    duration_ms = round((now - start) * 1000.0, 2)

    state = _safe_getattr(request, "state", None) or object()

    payload: Dict[str, Any] = {
        "service": "rag-orchestrator",
        "timestamp": now,
        "method": request.method,
        "path": request.url.path,
        "status": status_code,
        "duration_ms": duration_ms,
        "client": request.client.host if request.client else None,
        # Correlation / session IDs
        "request_id": _safe_getattr(state, "request_id", None),
        "session_id": _safe_getattr(state, "session_id", None),
        # RAG-specific timings 
        "retrieval_ms": _safe_getattr(state, "retrieval_ms", None),
        "llm_ms": _safe_getattr(state, "llm_ms", None),
        "chunks_returned": _safe_getattr(state, "chunks_returned", None),
        # High-level error info, if any
        "error": _safe_getattr(state, "error_message", None),
        # Add Add trace_id/span_id in json
        "trace_id": _safe_getattr(state, "trace_id", None),
        "span_id": _safe_getattr(state, "span_id", None),
    }

    logger.info(json.dumps(payload, ensure_ascii=False))
