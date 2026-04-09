"""
Mechanistic Query Decomposer — Phase 4
========================================
Decomposes complex multi-axis queries into 2–3 focused sub-queries
for parallel retrieval from Qdrant.

Only activated when retrieval_mode == "mechanistic_synthesis".

Two-tier approach:
  1. LLM decomposition (preferred): asks LLM to split query
  2. Heuristic fallback: splits by conjunctions / concept clusters

Usage:
    from .mechanistic_query_decomposer import decompose_query
    subqueries = decompose_query(query, llm_client=kserve)
"""

from __future__ import annotations

import json
import re
from typing import List, Optional


# ── LLM Decomposition ───────────────────────────────────────────────

DECOMPOSE_SYSTEM = """Bạn là query decomposer cho hệ thống RAG y khoa.
Nhiệm vụ: tách câu hỏi phức tạp thành 2-3 sub-query ngắn, đơn trục.

Quy tắc:
1. Mỗi sub-query tối đa 10 từ.
2. Mỗi sub-query chỉ nhắm 1 trục ý nghĩa / 1 khái niệm cụ thể.
3. Giữ nguyên ngôn ngữ gốc (tiếng Việt hoặc tiếng Anh).
4. Trả về JSON array, KHÔNG giải thích: ["sub-query 1", "sub-query 2"]
5. Tối đa 3 sub-queries.
6. Nếu câu hỏi đã đơn giản, trả về array chứa đúng 1 phần tử là câu gốc.

Ví dụ:
Input: "nhiễm khuẩn bệnh viện, thời gian chờ, áp lực nhân viên liên hệ thế nào với sụp đổ chất lượng hệ thống"
Output: ["kiểm soát nhiễm khuẩn bệnh viện", "thời gian chờ khám chất lượng dịch vụ", "áp lực nhân viên y tế burnout"]

Input: "cơ chế viêm mạn tính mức thấp trong xơ vữa động mạch đái tháo đường ung thư bệnh thận"
Output: ["viêm mạn tính xơ vữa động mạch", "viêm mạn tính đái tháo đường", "viêm mạn tính ung thư bệnh thận mạn"]"""


def _llm_decompose(
    query: str,
    llm_client,
    max_subqueries: int = 3,
) -> Optional[List[str]]:
    """
    Use LLM to decompose query into sub-queries.
    Returns None if LLM call fails.
    """
    try:
        messages = [
            {"role": "system", "content": DECOMPOSE_SYSTEM},
            {"role": "user", "content": query},
        ]
        raw = llm_client.generate(
            messages,
            max_tokens=200,
            temperature=0.1,
        )
        
        # Parse JSON array from response
        raw = raw.strip()
        
        # Handle case where LLM wraps in markdown code block
        if raw.startswith("```"):
            raw = re.sub(r'^```\w*\n?', '', raw)
            raw = re.sub(r'\n?```$', '', raw)
            raw = raw.strip()
        
        parsed = json.loads(raw)
        
        if isinstance(parsed, list) and all(isinstance(s, str) for s in parsed):
            # Enforce max subqueries
            result = [s.strip() for s in parsed if s.strip()][:max_subqueries]
            if result:
                return result
        
        return None
        
    except Exception as exc:
        print(f"[QueryDecomposer] LLM decomposition failed: {exc}")
        return None


# ── Heuristic Decomposition ─────────────────────────────────────────

# Vietnamese conjunctions and separators
_SPLIT_PATTERNS = re.compile(
    r'[,;]|\s+(?:và|hoặc|hay|cùng với|đồng thời|bên cạnh đó)\s+',
    re.IGNORECASE,
)

# Medical domain concept markers
_CONCEPT_MARKERS = re.compile(
    r'(?:cơ chế|trục|hệ thống|pathway|mechanism|axis|'
    r'liên hệ|mối quan hệ|vai trò|ảnh hưởng|tác động|'
    r'giữa|từ|đến|qua|và)',
    re.IGNORECASE,
)


def _heuristic_decompose(
    query: str,
    max_subqueries: int = 3,
) -> List[str]:
    """
    Fallback: split query by conjunctions and concept boundaries.
    """
    # Split by conjunctions/separators
    segments = _SPLIT_PATTERNS.split(query)
    
    # Filter: keep segments with meaningful content (> 5 words)
    meaningful = []
    for seg in segments:
        seg = seg.strip()
        words = seg.split()
        if len(words) >= 3:  # at least 3 words to be a meaningful sub-query
            # Truncate overly long segments
            if len(words) > 12:
                seg = " ".join(words[:12])
            meaningful.append(seg)
    
    if not meaningful:
        # Can't split meaningfully, return original
        return [query]
    
    # Deduplicate similar segments
    unique = []
    for seg in meaningful:
        seg_words = set(seg.lower().split())
        is_dup = False
        for existing in unique:
            existing_words = set(existing.lower().split())
            overlap = len(seg_words & existing_words)
            if overlap / max(len(seg_words), 1) > 0.7:
                is_dup = True
                break
        if not is_dup:
            unique.append(seg)
    
    return unique[:max_subqueries]


# ── Public API ───────────────────────────────────────────────────────

def decompose_query(
    query: str,
    llm_client=None,
    max_subqueries: int = 3,
) -> List[str]:
    """
    Decompose a complex multi-axis query into 2–3 focused sub-queries.
    
    Priority:
      1. LLM decomposition (if client available)
      2. Heuristic fallback (conjunction/concept splitting)
      3. Original query as single-element list
    
    Args:
        query: The original complex query
        llm_client: Optional LLM client with .generate() method
        max_subqueries: Maximum number of sub-queries (default: 3)
    
    Returns:
        List of 1–3 sub-query strings
    """
    # Short queries don't need decomposition
    if len(query.split()) <= 10:
        return [query]
    
    # Try LLM first
    if llm_client is not None:
        result = _llm_decompose(query, llm_client, max_subqueries)
        if result:
            print(f"[QueryDecomposer] LLM split into {len(result)} subqueries: {result}")
            return result
    
    # Fallback to heuristic
    result = _heuristic_decompose(query, max_subqueries)
    print(f"[QueryDecomposer] Heuristic split into {len(result)} subqueries: {result}")
    return result
