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


def _needs_rewriting(message: str, history: list) -> bool:
    """
    Heuristic: does this message need rewriting?
    Short follow-ups and pronoun-heavy messages need it.
    """
    if not history:
        return False

    msg = message.strip().lower()
    # Very short messages are likely follow-ups
    if len(msg.split()) <= 5:
        return True
    # Contains pronouns/referents without full context
    referent_words = {"it", "this", "that", "those", "they", "them",
                      "its", "their", "what about", "how about",
                      "and", "also", "too", "else", "more",
                      "the same", "similar"}
    return any(r in msg for r in referent_words)


def build_rewrite_prompt(message: str, history: list) -> List[Dict[str, str]]:
    """Build the prompt for the LLM to rewrite the query."""
    messages = [{"role": "system", "content": REWRITE_SYSTEM}]

    # Include last 2 turns for context
    recent = history[-4:] if history else []  # last 2 pairs (user+assistant)
    if recent:
        context = "\n".join(
            f"{m.get('role', 'user').upper()}: {m.get('content', '')}"
            for m in recent
        )
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
