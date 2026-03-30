from __future__ import annotations

import os
import hashlib
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Iterable

from qdrant_client import QdrantClient
from qdrant_client.http import models as qm
from fastembed import TextEmbedding


_RETRIEVER_CACHE: Optional["QdrantRetriever"] = None
_RETRIEVER_CACHE_KEY: Optional[tuple] = None


def _estimate_tokens(text: str) -> int:
    """Rough token estimate without external tokenizers.
    Empirically, ~4 characters per token for English-like text.
    """
    if not text:
        return 0
    return max(1, len(text) // 4)


def _stable_text_hash(text: str) -> str:
    norm = " ".join((text or "").split()).strip().lower()
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


@dataclass
class RetrievedChunk:
    id: str
    text: str
    score: float
    metadata: Dict[str, Any]


@dataclass
class MetadataFilters:
    """Filters applied to Qdrant payload fields during retrieval."""
    specialty: Optional[str] = None
    audience: Optional[str] = None       # "patient" | "student" | "clinician"
    trust_tier: Optional[int] = None     # 1 | 2 | 3
    doc_type: Optional[str] = None       # "guideline" | "textbook" | "patient_education" | ...
    language: Optional[str] = None       # "en" | "vi"
    source_name: Optional[str] = None    # "MedlinePlus" | "WHO" | "NCBI Bookshelf"

    def to_qdrant_filter(self) -> Optional[qm.Filter]:
        """Convert to Qdrant Filter object. Returns None if no filters."""
        conditions: List[qm.FieldCondition] = []

        if self.specialty:
            conditions.append(
                qm.FieldCondition(key="specialty", match=qm.MatchValue(value=self.specialty))
            )
        if self.audience:
            conditions.append(
                qm.FieldCondition(key="audience", match=qm.MatchValue(value=self.audience))
            )
        if self.trust_tier is not None:
            conditions.append(
                qm.FieldCondition(key="trust_tier", match=qm.MatchValue(value=self.trust_tier))
            )
        if self.doc_type:
            conditions.append(
                qm.FieldCondition(key="doc_type", match=qm.MatchValue(value=self.doc_type))
            )
        if self.language:
            conditions.append(
                qm.FieldCondition(key="language", match=qm.MatchValue(value=self.language))
            )
        if self.source_name:
            conditions.append(
                qm.FieldCondition(key="source_name", match=qm.MatchValue(value=self.source_name))
            )

        if not conditions:
            return None
        return qm.Filter(must=conditions)


def detect_filters_from_query(query: str) -> MetadataFilters:
    """
    Auto-detect metadata filters from query intent.
    
    Strategy:
      - Source detection: only when query EXPLICITLY mentions source name
        or uses strong organizational context (e.g., "WHO recommends")
      - Audience routing: route patient-level vs clinical vs textbook queries
        to the most appropriate source as a SOFT preference
      - Avoid false positives: "who is at risk" ≠ WHO organization
    """
    import re
    q = query.lower()
    filters = MetadataFilters()

    # ── Explicit source mention (high confidence) ────────────────────
    if "medlineplus" in q or "medline plus" in q:
        filters.source_name = "MedlinePlus"
    elif "ncbi" in q or "statpearls" in q or "pubmed" in q:
        filters.source_name = "NCBI Bookshelf"
    elif "world health" in q:
        filters.source_name = "WHO"
    # "WHO" as organization: only when followed by action verbs/org context
    elif re.search(r"\bwho\b\s+(recommend|guideline|report|says|publish|statistic|response|data)", q):
        filters.source_name = "WHO"

    # ── Audience-based source routing (soft preference) ──────────────
    # Patient-level → prefer MedlinePlus
    if any(kw in q for kw in ["explain simply", "easy to understand", "patient",
                               "in simple terms", "for beginners", "what is",
                               "what are the symptoms", "how is it treated"]):
        filters.audience = "patient"
        if not filters.source_name:
            filters.source_name = None  # don't hard-filter, let scoring handle it

    # Clinical/textbook → prefer NCBI
    elif any(kw in q for kw in ["mechanism", "pathophysiology", "pathogenesis",
                                 "textbook", "histopathology", "differential diagnosis",
                                 "clinical presentation", "staging"]):
        filters.audience = "student"
        if not filters.source_name:
            filters.source_name = "NCBI Bookshelf"

    # Guideline/global health → prefer WHO
    elif any(kw in q for kw in ["global", "worldwide", "guideline",
                                 "recommendation", "evidence-based",
                                 "prevention strategy", "public health"]):
        filters.audience = "clinician"
        if not filters.source_name:
            filters.source_name = "WHO"

    # ── Trust tier from explicit mentions ─────────────────────────────
    if any(kw in q for kw in ["guideline", "protocol", "cdc recommends"]):
        filters.trust_tier = 1
        filters.doc_type = "guideline"
    elif any(kw in q for kw in ["textbook", "pathophysiology"]):
        filters.trust_tier = 2

    # ── Language detection (Vietnamese) ───────────────────────────────
    if re.search(r"[\u00C0-\u024F\u1E00-\u1EFF\u0110\u0111]", query):
        filters.language = "vi"

    return filters


class QdrantRetriever:
    def __init__(
        self,
        qdrant_url: str,
        collection: str,
        embedding_model: str = "BAAI/bge-small-en-v1.5",
        top_k: int = 4,
        score_threshold: float = 0.25,
        max_context_tokens: int = 2048,
        deduplicate: bool = True,
    ):
        self.client = QdrantClient(url=qdrant_url)
        self.collection = collection
        self.top_k = top_k
        self.score_threshold = score_threshold
        self.max_context_tokens = max_context_tokens
        self.deduplicate = deduplicate
        self.embedder = TextEmbedding(model_name=embedding_model)

    def retrieve(
        self,
        query: str,
        filters: Optional[MetadataFilters] = None,
        auto_filter: bool = True,
    ) -> List[RetrievedChunk]:
        """
        Retrieve relevant chunks from Qdrant.

        Args:
            query: The user's question
            filters: Explicit metadata filters (if provided, auto_filter is skipped)
            auto_filter: If True and no explicit filters, detect filters from query
        """
        try:
            # Auto-detect filters from query intent
            if filters is None and auto_filter:
                filters = detect_filters_from_query(query)

            qdrant_filter = filters.to_qdrant_filter() if filters else None

            # Embed query
            qvec = next(self.embedder.embed([query])).tolist()

            query_kwargs = {
                "collection_name": self.collection,
                "query": qvec,
                "limit": self.top_k,
                "with_payload": True,
            }
            if qdrant_filter:
                query_kwargs["query_filter"] = qdrant_filter

            response = self.client.query_points(**query_kwargs)
            res = response.points if hasattr(response, 'points') else response

            # Fallback: if filtered search returned too few results, widen search
            min_results = 2
            if qdrant_filter and len(res) < min_results:
                print(f"[RAG] Filtered search returned {len(res)} results, retrying without filter")
                fallback_kwargs = {
                    "collection_name": self.collection,
                    "query": qvec,
                    "limit": self.top_k,
                    "with_payload": True,
                }
                fb_response = self.client.query_points(**fallback_kwargs)
                fb_res = fb_response.points if hasattr(fb_response, 'points') else fb_response
                # Merge: keep filtered results first, then add unfiltered
                seen_ids = {p.id for p in res}
                for p in fb_res:
                    if p.id not in seen_ids:
                        res.append(p)
                        seen_ids.add(p.id)

        except Exception as e:
            # Graceful fallback: no retrieval, no crash
            print(f"[RAG] Retrieval skipped: {e}")

            # If filtered search failed, try without filters
            if filters:
                try:
                    qvec = next(self.embedder.embed([query])).tolist()
                    response = self.client.query_points(
                        collection_name=self.collection,
                        query=qvec,
                        limit=self.top_k,
                        with_payload=True,
                    )
                    res = response.points if hasattr(response, 'points') else response
                except Exception:
                    return []
            else:
                return []

        chunks: List[RetrievedChunk] = []
        seen: set[str] = set()
        used_tokens = 0

        for p in res:
            if p.score is None or p.score < self.score_threshold:
                continue

            payload = p.payload or {}
            text = str(payload.get("text", "") or "")
            if not text.strip():
                continue

            if self.deduplicate:
                h = _stable_text_hash(text)
                if h in seen:
                    continue
                seen.add(h)

            tks = _estimate_tokens(text)
            if self.max_context_tokens and used_tokens + tks > self.max_context_tokens:
                break

            used_tokens += tks

            # Extract metadata - support both enriched (top-level) and legacy (nested) formats
            md = {}
            # Enriched format: metadata fields are top-level in payload
            for key in ["source_name", "doc_type", "specialty", "audience",
                        "trust_tier", "language", "title", "section_title",
                        "source_url", "updated_at", "heading_path", "tags",
                        "doc_id", "chunk_index"]:
                if key in payload:
                    md[key] = payload[key]

            # Legacy fallback: nested metadata dict
            if not md and "metadata" in payload:
                legacy = payload.get("metadata", {})
                if isinstance(legacy, dict):
                    md = legacy

            chunks.append(
                RetrievedChunk(
                    id=str(p.id),
                    text=text,
                    score=float(p.score),
                    metadata=md,
                )
            )

        return chunks



def build_retriever_from_env() -> Optional[QdrantRetriever]:
    global _RETRIEVER_CACHE
    global _RETRIEVER_CACHE_KEY

    qdrant_url = os.getenv("QDRANT_URL", "").strip()
    if not qdrant_url:
        return None

    # Retrieval controls (strict wiring via env; chart sets these explicitly)
    top_k = int(os.getenv("RAG_TOP_K", os.getenv("TOP_K", "4")))
    score_threshold = float(os.getenv("RAG_MIN_SCORE", os.getenv("SCORE_THRESHOLD", "0.25")))
    max_context_tokens = int(os.getenv("RAG_MAX_CONTEXT_TOKENS", "2048"))
    dedup = os.getenv("RAG_DEDUPLICATE", "true").strip().lower() in ("1", "true", "yes", "y", "on")

    cache_key = (
        qdrant_url,
        os.getenv("QDRANT_COLLECTION", "medical_docs"),
        os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5"),
        top_k,
        score_threshold,
        max_context_tokens,
        dedup,
    )

    if _RETRIEVER_CACHE is not None and _RETRIEVER_CACHE_KEY == cache_key:
        return _RETRIEVER_CACHE

    _RETRIEVER_CACHE = QdrantRetriever(
        qdrant_url=qdrant_url,
        collection=os.getenv("QDRANT_COLLECTION", "medical_docs"),
        embedding_model=os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5"),
        top_k=top_k,
        score_threshold=score_threshold,
        max_context_tokens=max_context_tokens,
        deduplicate=dedup,
    )
    _RETRIEVER_CACHE_KEY = cache_key
    return _RETRIEVER_CACHE
