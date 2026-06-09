"""Lightweight, dependency-free pipeline decision helpers.

Kept separate from ``main`` so the decision logic can be unit-tested without
importing the full FastAPI app (which pulls in runtime-only deps like ``utils``,
opentelemetry, the retriever stack, etc.).
"""

import os

GENERAL_EXPLAINER_QUERY_TYPES = {
    "fact_extraction",
    "teaching_explainer",
    "professional_explainer",
    "guideline_comparison",
}


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def should_skip_entity_fallback(router_output, answer_mode: str) -> bool:
    """Skip the expensive entity-fallback retrieval for general Standard queries.

    Entity fallback fires a second hybrid retrieval (a lexical scan over the large
    article index) to surface a specific entity that is poorly represented in the
    primary results. That payoff only matters for article-specific / numeric /
    comparison questions. For a general "X là gì" explainer in Standard mode the
    primary retrieval is already adequate, so the extra pass is pure latency and
    can over-anchor the answer to a single article. Thinking mode keeps it for
    broader source coverage; specific/numeric/comparison queries keep it too.
    """
    if not _env_flag("ENTITY_FALLBACK_GUARD_ENABLED", default=True):
        return False
    if answer_mode == "thinking":
        return False
    if getattr(router_output, "query_type", "") not in GENERAL_EXPLAINER_QUERY_TYPES:
        return False
    if getattr(router_output, "requires_numbers", False):
        return False
    if getattr(router_output, "requires_comparison", False):
        return False
    return True
