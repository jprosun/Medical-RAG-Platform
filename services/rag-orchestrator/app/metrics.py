from __future__ import annotations

from prometheus_client import Counter, Histogram, Gauge

# --- Request-level metrics (low-cardinality) ---
RAG_CHAT_REQUESTS_TOTAL = Counter(
    "rag_chat_requests_total",
    "Total number of /api/chat requests",
)

RAG_CHAT_ERRORS_TOTAL = Counter(
    "rag_chat_errors_total",
    "Total number of /api/chat errors",
)

# --- Retrieval metrics ---
RAG_RETRIEVAL_LATENCY_SECONDS = Histogram(
    "rag_retrieval_latency_seconds",
    "Latency for vector retrieval (Qdrant search)",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)

RAG_CONTEXT_TOKENS = Histogram(
    "rag_context_tokens",
    "Estimated tokens included in the retrieved context",
    buckets=(0, 128, 256, 512, 1024, 1536, 2048, 3072, 4096, 8192),
)

RAG_EMPTY_CONTEXT_TOTAL = Counter(
    "rag_empty_context_total",
    "Number of times retrieval produced no usable context",
)

# --- Generation metrics ---
RAG_GENERATION_LATENCY_SECONDS = Histogram(
    "rag_generation_latency_seconds",
    "Latency for answer generation (KServe or fallback)",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 60, 120),
)

RAG_FALLBACK_TOTAL = Counter(
    "rag_fallback_total",
    "Number of times the orchestrator used fallback (no KServe)",
)

# --- In-flight gauge (optional) ---
RAG_INFLIGHT = Gauge(
    "rag_inflight_requests",
    "Number of in-flight /api/chat requests",
)
