"""
Query Rewriter for Multi-Turn Conversations
=============================================

Rewrites follow-up questions (like "what about children?" or
"any side effects?") into standalone queries using the LLM,
so the retriever gets a complete question to search.

Usage (from rag-orchestrator main.py):
    from .query_rewriter import rewrite_query
    standalone_q = rewrite_query(user_message, chat_history, llm_client)
"""

from __future__ import annotations

import os
from typing import List, Dict, Optional


REWRITE_SYSTEM = """You are a query rewriter for a medical knowledge RAG system.
Your job: take a follow-up question from a conversation and rewrite it as a
STANDALONE search query that captures the full intent.

Rules:
1. If the message is already self-contained, return it unchanged.
2. Include key context from recent conversation (e.g., the disease or topic being discussed).
3. Keep the rewrite concise — one clear question or search phrase.
4. Preserve the original language (English or Vietnamese).
5. DO NOT answer the question — only rewrite it.
6. Return ONLY the rewritten query, nothing else."""


# Patterns that make a question self-contained (it carries its own subject),
# so it must NOT inherit the previous turn's topic.
_SELF_CONTAINED_PATTERNS = (
    "là gì", "la gi", "là bệnh gì", "la benh gi", "là loại", "nghĩa là", "nghia la",
    "thế nào là", "the nao la", "định nghĩa", "dinh nghia",
    "what is", "what are", "define ",
)

# Disease / condition subject terms. If the message names one of these, it has
# its own topic and is a (possibly short) standalone question, not a follow-up
# fragment that depends on the previous turn.
_TOPIC_SUBJECT_TERMS = (
    "ung thư", "ung thu", "viêm", "viem", "hội chứng", "hoi chung",
    "rối loạn", "roi loan", "đái tháo đường", "dai thao duong", "tiểu đường", "tieu duong",
    "tăng huyết áp", "tang huyet ap", "hen phế quản", "hen suyễn", "hen suyen",
    "trầm cảm", "tram cam", "đột quỵ", "dot quy", "nhồi máu", "nhoi mau",
    "xơ gan", "xo gan", "suy thận", "suy than", "suy tim", "lao phổi", "lao phoi",
    "sốt xuất huyết", "sot xuat huyet", "viêm gan", "viem gan", "đột biến gen",
)

_REFERENT_WORDS = {
    "it", "this", "that", "those", "they", "them",
    "its", "their", "what about", "how about",
    "and", "also", "too", "else", "more",
    "the same", "similar",
}


def _is_self_contained(msg: str) -> bool:
    """A definition question or one naming its own disease subject is standalone."""
    if any(pattern in msg for pattern in _SELF_CONTAINED_PATTERNS):
        return True
    return any(term in msg for term in _TOPIC_SUBJECT_TERMS)


def _needs_rewriting(message: str, history: list) -> bool:
    """
    Heuristic: does this message need rewriting?
    Short follow-ups and pronoun-heavy messages need it — but a self-contained
    question (its own subject or a "X là gì" pattern) must be left alone, or it
    inherits the previous turn's topic (e.g. "ung thư máu là gì" after a
    cervical-cancer turn would get rewritten/anchored to cervical cancer).
    """
    if not history:
        return False

    msg = message.strip().lower()
    # Explicit referents always need resolving against context.
    if any(r in msg for r in _REFERENT_WORDS):
        return True
    # Self-contained questions carry their own topic: never rewrite them.
    if _is_self_contained(msg):
        return False
    # Otherwise, very short messages are likely context-dependent fragments.
    return len(msg.split()) <= 5


def build_rewrite_prompt(message: str, history: list) -> List[Dict[str, str]]:
    """Build the prompt for the LLM to rewrite the query."""
    messages = [{"role": "system", "content": REWRITE_SYSTEM}]

    # Include last 2 turns for context; truncate long assistant replies
    recent = history[-4:] if history else []  # last 2 pairs (user+assistant)
    if recent:
        context_lines = []
        for m in recent:
            role = m.get("role", "user").upper()
            content = m.get("content", "")
            if role == "ASSISTANT" and len(content) > 300:
                content = content[:300] + "…"
            context_lines.append(f"{role}: {content}")
        context = "\n".join(context_lines)
        messages.append({
            "role": "user",
            "content": f"Conversation context:\n{context}\n\nFollow-up message to rewrite:\n{message}",
        })
    else:
        messages.append({"role": "user", "content": message})

    return messages


def rewrite_query(
    message: str,
    chat_history: list,
    llm_client=None,
) -> str:
    """
    Rewrite a follow-up question into a standalone query.

    Args:
        message: The user's current message
        chat_history: List of previous messages [{"role": ..., "content": ...}]
        llm_client: Optional LLM client with .generate() method

    Returns:
        The rewritten standalone query, or the original message if no rewrite needed.
    """
    # Skip if no history or message is self-contained
    if not _needs_rewriting(message, chat_history):
        return message

    # If no LLM available, do a simple rule-based rewrite
    if llm_client is None:
        return _rule_based_rewrite(message, chat_history)

    # LLM-based rewrite
    try:
        messages_payload = build_rewrite_prompt(message, chat_history)
        max_tokens = int(os.getenv("REWRITE_MAX_TOKENS", "150"))
        temperature = float(os.getenv("REWRITE_TEMPERATURE", "0.1"))

        rewritten = llm_client.generate(
            messages_payload,
            max_tokens=max_tokens,
            temperature=temperature,
            attempt_budget=int(os.getenv("REWRITE_MAX_ATTEMPTS", "1")),
        )
        rewritten = rewritten.strip().strip('"').strip("'")

        # Sanity check: rewritten should be reasonable length
        if rewritten and 5 < len(rewritten) < 500:
            return rewritten
    except Exception as exc:
        print(f"[QueryRewriter] LLM rewrite failed, using rule-based: {exc}")

    return _rule_based_rewrite(message, chat_history)


def _rule_based_rewrite(message: str, history: list) -> str:
    """
    Simple rule-based rewrite: prepend the topic from last user message.
    """
    if not history:
        return message

    # Find last user message in history
    last_user_msg = ""
    for m in reversed(history):
        if m.get("role") == "user":
            last_user_msg = m.get("content", "")
            break

    if not last_user_msg:
        return message

    # Extract likely topic (first noun phrase or key medical term)
    # Simple heuristic: use first 8 words of last user question
    last_words = last_user_msg.split()[:8]
    topic_hint = " ".join(last_words)

    return f"Regarding {topic_hint}: {message}"
