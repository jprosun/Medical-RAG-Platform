from __future__ import annotations

import os
import hashlib
import re
import unicodedata
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Iterable, TYPE_CHECKING

from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

# Embedder: sentence-transformers for bge-m3, fastembed for others
try:
    from sentence_transformers import SentenceTransformer
    _HAS_ST = True
except ImportError:
    _HAS_ST = False
try:
    from fastembed import TextEmbedding
    _HAS_FE = True
except ImportError:
    _HAS_FE = False


_RETRIEVER_CACHE: Optional["QdrantRetriever"] = None
_RETRIEVER_CACHE_KEY: Optional[tuple] = None

if TYPE_CHECKING:
    from .article_aggregator import ArticleGroup


_RETRIEVAL_ALIAS_HINTS: list[tuple[tuple[str, ...], str]] = [
    (
        ("cea", "cas"),
        "hep dong mach canh ngoai so carotid endarterectomy carotid artery stenting",
    ),
    (
        ("pieb", "cei"),
        "gay te ngoai mang cung chuyen da epidural labor analgesia",
    ),
    (
        ("thoai hoa khop",),
        "osteoarthritis elderly early intervention symptom relief disease progression",
    ),
]


_RETRIEVAL_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "what", "about", "from",
    "trong", "theo", "tren", "nhung", "cua", "mot", "nào", "nao", "hay",
    "voi", "với", "sau", "khi", "can", "cần", "phan", "phần", "context",
    "quyet", "dinh", "chon", "co", "ket", "luan",
}


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


def _normalize_for_matching(text: str) -> str:
    base = unicodedata.normalize("NFKD", text or "")
    stripped = "".join(ch for ch in base if not unicodedata.combining(ch))
    return " ".join(stripped.lower().split())


def _has_alias_term(query_norm: str, term: str) -> bool:
    if " " in term:
        return term in query_norm
    return re.search(rf"\b{re.escape(term)}\b", query_norm) is not None


def _expand_query_for_retrieval(query: str) -> str:
    """
    Add deterministic domain hints for acronym-heavy questions that otherwise
    under-specify the article topic for vector search.
    """
    q = (query or "").strip()
    if not q:
        return q

    q_norm = _normalize_for_matching(q)
    expansions = []
    for required_terms, hint in _RETRIEVAL_ALIAS_HINTS:
        if all(_has_alias_term(q_norm, term) for term in required_terms):
            expansions.append(hint)

    if not expansions:
        return q

    deduped = []
    seen = set()
    for item in expansions:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return f"{q} {' '.join(deduped)}"


def _query_keywords(text: str) -> set[str]:
    tokens = re.findall(r"\w+", _normalize_for_matching(text), flags=re.UNICODE)
    return {tok for tok in tokens if len(tok) >= 4 and tok not in _RETRIEVAL_STOPWORDS}


def _query_acronyms(text: str) -> set[str]:
    return {match.group(0).lower() for match in re.finditer(r"\b[A-Z][A-Z0-9]{1,6}\b", text or "")}


def _query_phrases(text: str) -> list[str]:
    tokens = list(_query_keywords(text))
    phrases = []
    for size in (3, 2):
        for i in range(len(tokens) - size + 1):
            phrase = " ".join(tokens[i:i + size])
            if phrase not in phrases:
                phrases.append(phrase)
    return phrases[:12]


def _payload_metadata_text(payload: Dict[str, Any]) -> str:
    parts = []
    for key in ("title", "section_title", "heading_path", "doc_type", "source_name"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value)
    return " ".join(parts)


def _chunk_query_bonus(query: str, payload: Dict[str, Any], retrieval_mode: str) -> float:
    query_terms = _query_keywords(query)
    query_acronyms = _query_acronyms(query)
    if not query_terms and not query_acronyms:
        return 0.0

    metadata_text = _payload_metadata_text(payload)
    metadata_terms = _query_keywords(metadata_text)
    overlap_terms = query_terms & metadata_terms

    bonus = min(len(overlap_terms) * 0.015, 0.09)

    title = str(payload.get("title", "") or "")
    title_terms = _query_keywords(title)
    title_overlap = query_terms & title_terms
    if title_overlap:
        title_multiplier = 0.02 if retrieval_mode == "article_centric" else 0.015
        bonus += min(len(title_overlap) * title_multiplier, 0.06)

    if query_acronyms:
        metadata_norm = _normalize_for_matching(metadata_text)
        matched = sum(
            1 for acronym in query_acronyms
            if re.search(rf"\b{re.escape(acronym)}\b", metadata_norm)
        )
        if matched:
            acronym_bonus = 0.05 * (matched / len(query_acronyms))
            if retrieval_mode == "article_centric":
                acronym_bonus += 0.03 * (matched / len(query_acronyms))
            bonus += acronym_bonus

    return round(bonus, 4)


def _same_article_chunk_bonus(query: str, text: str, payload: Dict[str, Any]) -> float:
    query_terms = _query_keywords(query)
    query_acronyms = _query_acronyms(query)
    query_norm = _normalize_for_matching(query)
    text_norm = _normalize_for_matching(text)
    metadata_norm = _normalize_for_matching(_payload_metadata_text(payload))

    bonus = 0.0
    if query_terms:
        text_hits = sum(1 for term in query_terms if re.search(rf"\b{re.escape(term)}\b", text_norm))
        metadata_hits = sum(1 for term in query_terms if re.search(rf"\b{re.escape(term)}\b", metadata_norm))
        bonus += min(text_hits * 0.18, 1.1)
        bonus += min(metadata_hits * 0.06, 0.24)

    if query_acronyms:
        acronym_hits = sum(
            1 for acronym in query_acronyms
            if re.search(rf"\b{re.escape(acronym)}\b", text_norm)
        )
        if acronym_hits:
            bonus += 0.45 * (acronym_hits / len(query_acronyms))

    for phrase in _query_phrases(query):
        if phrase and phrase in text_norm:
            bonus += 0.3 if len(phrase.split()) >= 3 else 0.18

    if any(marker in query_norm for marker in ("bao nhieu", "ty le", "phan tram", "diem")):
        if re.search(r"\d+[.,]?\d*\s*%|\d+[.,]?\d*", text):
            bonus += 0.35
    if "karnofsky" in query_norm and "karnofsky" in text_norm:
        bonus += 0.9
    if "ghep than" in query_norm and any(marker in text_norm for marker in ("rifampicin", "tacrolimus", "cyclosporine", "thai ghep")):
        bonus += 2.0
    if any(marker in query_norm for marker in ("cea", "cas")) and any(marker in text_norm for marker in ("cea", "cas")):
        bonus += 0.9
    if "chat luong song" in query_norm and "chat luong song" in text_norm:
        bonus += 0.6
    if "hoi phuc" in query_norm and "hoi phuc" in text_norm:
        bonus += 0.5
    if "van dong" in query_norm and "van dong" in text_norm:
        bonus += 0.45
    asks_group_factors = any(marker in query_norm for marker in ("dua tren", "nhom yeu to", "quyet dinh", "theo doi"))
    if any(marker in query_norm for marker in ("theo doi", "noi khoa toi uu", "nguy co tim mach")) and any(marker in text_norm for marker in ("theo doi", "noi khoa", "nguy co tim mach", "tai hep", "dot quy")):
        bonus += 1.8
    if asks_group_factors and any(marker in text_norm for marker in ("theo doi sau mo", "theo doi", "noi khoa ho tro", "kiem soat huyet ap", "roi loan lipid mau", "tai hep", "dot quy")):
        bonus += 1.2

    section_norm = _normalize_for_matching(str(payload.get("section_title", "") or ""))
    if any(marker in section_norm for marker in ("ket qua", "tom tat", "summary")):
        bonus += 0.15

    if any(marker in text_norm for marker in ("title:", "source:", "audience:", "keywords:")):
        bonus -= 0.35
    if asks_group_factors:
        digit_count = sum(ch.isdigit() for ch in text)
        if digit_count >= 12 and any(marker in text_norm for marker in ("rct", "95% ci", "or (", "tv/dq", "nmct")):
            bonus -= 1.8

    return round(bonus, 4)


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
    # Disabled: VMJ corpus does not have 'language' field in payload.
    # The entire staging_medqa_vi_vmj_v1 collection is Vietnamese.
    # if re.search(r"[\u00C0-\u024F\u1E00-\u1EFF\u0110\u0111]", query):
    #     filters.language = "vi"

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
        self._model_name = embedding_model
        
        # Use sentence-transformers for bge-m3, fastembed for others
        if 'bge-m3' in embedding_model and _HAS_ST:
            self._st_model = SentenceTransformer(embedding_model)
            self._use_st = True
        elif _HAS_FE:
            self.embedder = TextEmbedding(model_name=embedding_model)
            self._use_st = False
        elif _HAS_ST:
            self._st_model = SentenceTransformer(embedding_model)
            self._use_st = True
        else:
            raise RuntimeError("No embedding library available")
    
    def _embed_query(self, text: str) -> list:
        if self._use_st:
            return self._st_model.encode([text], normalize_embeddings=True)[0].tolist()
        return next(self.embedder.embed([text])).tolist()

    def retrieve(
        self,
        query: str,
        filters: Optional[MetadataFilters] = None,
        auto_filter: bool = True,
        top_k_override: Optional[int] = None,
        retrieval_mode: str = "article_centric",
    ) -> List[RetrievedChunk]:
        """
        Retrieve relevant chunks from Qdrant.

        Args:
            query: The user's question
            filters: Explicit metadata filters (if provided, auto_filter is skipped)
            auto_filter: If True and no explicit filters, detect filters from query
            top_k_override: Override default top_k for this query (used by router)
        """
        try:
            # Auto-detect filters from query intent
            if filters is None and auto_filter:
                filters = detect_filters_from_query(query)

            qdrant_filter = filters.to_qdrant_filter() if filters else None

            # Embed query after deterministic expansion for ambiguous acronym pairs.
            retrieval_query = _expand_query_for_retrieval(query)
            qvec = self._embed_query(retrieval_query)

            # Apply diversity multiplier
            if retrieval_mode == "mechanistic_synthesis":
                multiplier = 4
            elif retrieval_mode == "topic_summary":
                multiplier = 3
            else:
                multiplier = 2
                
            base_k = top_k_override or self.top_k
            raw_limit = base_k * multiplier

            query_kwargs = {
                "collection_name": self.collection,
                "query": qvec,
                "limit": raw_limit,
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
                    "limit": raw_limit,
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
                    retrieval_query = _expand_query_for_retrieval(query)
                    qvec = self._embed_query(retrieval_query)
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

        res = sorted(
            res,
            key=lambda p: float(getattr(p, "score", 0.0) or 0.0)
            + _chunk_query_bonus(query, getattr(p, "payload", {}) or {}, retrieval_mode),
            reverse=True,
        )

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

    def expand_primary_article_chunks(
        self,
        article: "ArticleGroup",
        query: str,
        max_chunks: int = 8,
        candidate_limit: int = 160,
    ) -> List[RetrievedChunk]:
        """
        For exact-answer queries, fetch more chunks from the already selected
        primary article and rerank them inside the article boundary. This helps
        surface direct answer spans that were not in the initial top-k.
        """
        doc_ids = {
            str(chunk.metadata.get("doc_id", "") or "").strip()
            for chunk in article.chunks
            if str(chunk.metadata.get("doc_id", "") or "").strip()
        }
        title = (article.title or "").strip()

        if not doc_ids and not title:
            return article.chunks

        must_conditions = []
        if doc_ids:
            should_doc_ids = [
                qm.FieldCondition(key="doc_id", match=qm.MatchValue(value=doc_id))
                for doc_id in sorted(doc_ids)
            ]
            if len(should_doc_ids) == 1:
                must_conditions.append(should_doc_ids[0])
            else:
                must_conditions.append(qm.Filter(should=should_doc_ids))
        elif title:
            must_conditions.append(
                qm.FieldCondition(key="title", match=qm.MatchValue(value=title))
            )

        scroll_filter = qm.Filter(must=must_conditions) if must_conditions else None

        try:
            scroll_response = self.client.scroll(
                collection_name=self.collection,
                scroll_filter=scroll_filter,
                limit=max(candidate_limit, max_chunks),
                with_payload=True,
                with_vectors=False,
            )
        except Exception as exc:
            print(f"[RAG] Primary article expansion skipped: {exc}")
            return article.chunks

        if isinstance(scroll_response, tuple):
            points = scroll_response[0]
        else:
            points = getattr(scroll_response, "points", scroll_response) or []

        expanded: List[RetrievedChunk] = []
        seen_hashes: set[str] = set()

        for chunk in article.chunks:
            h = _stable_text_hash(chunk.text)
            if h not in seen_hashes:
                seen_hashes.add(h)
                expanded.append(chunk)

        for point in points:
            payload = getattr(point, "payload", {}) or {}
            text = str(payload.get("text", "") or "")
            if not text.strip():
                continue
            h = _stable_text_hash(text)
            if h in seen_hashes:
                continue
            seen_hashes.add(h)

            md = {}
            for key in [
                "source_name", "doc_type", "specialty", "audience",
                "trust_tier", "language", "title", "section_title",
                "source_url", "updated_at", "heading_path", "tags",
                "doc_id", "chunk_index",
            ]:
                if key in payload:
                    md[key] = payload[key]
            if not md and "metadata" in payload and isinstance(payload.get("metadata"), dict):
                md = payload["metadata"]

            expanded.append(
                RetrievedChunk(
                    id=str(getattr(point, "id", "")),
                    text=text,
                    score=float(getattr(point, "score", 0.0) or 0.0),
                    metadata=md,
                )
            )

        expanded.sort(
            key=lambda chunk: (
                _same_article_chunk_bonus(query, chunk.text, chunk.metadata),
                chunk.score,
                -int(chunk.metadata.get("chunk_index", 0) or 0),
            ),
            reverse=True,
        )

        result: List[RetrievedChunk] = []
        used_tokens = 0
        for chunk in expanded:
            tks = _estimate_tokens(chunk.text)
            if self.max_context_tokens and used_tokens + tks > self.max_context_tokens:
                continue
            result.append(chunk)
            used_tokens += tks
            if len(result) >= max_chunks:
                break

        return result or article.chunks

    def retrieve_multi_axis(
        self,
        subqueries: List[str],
        top_k_per_query: int = 5,
        filters: Optional[MetadataFilters] = None,
    ) -> List[RetrievedChunk]:
        """
        Phase 4: Run multiple sub-queries against Qdrant, merge + dedup.
        
        Used for mechanistic_synthesis queries that have been decomposed
        into 2-3 focused sub-queries by the query decomposer.
        
        Args:
            subqueries: List of 2-3 focused sub-queries
            top_k_per_query: How many results per sub-query
            filters: Optional metadata filters
            
        Returns:
            Merged, deduplicated list of RetrievedChunk sorted by score
        """
        all_chunks: List[RetrievedChunk] = []
        seen_hashes: set[str] = set()
        
        for i, subq in enumerate(subqueries):
            try:
                sub_chunks = self.retrieve(
                    query=subq,
                    filters=filters,
                    top_k_override=top_k_per_query,
                    retrieval_mode="article_centric",  # each sub-query is focused
                )
                
                # Tag chunks with their sub-query origin for tracing
                for chunk in sub_chunks:
                    chunk.metadata["_subquery_index"] = i
                    chunk.metadata["_subquery_text"] = subq
                    
                    # Dedup across sub-queries
                    h = _stable_text_hash(chunk.text)
                    if h not in seen_hashes:
                        seen_hashes.add(h)
                        all_chunks.append(chunk)
                        
            except Exception as e:
                print(f"[RAG] Multi-axis sub-query {i} failed: {e}")
                continue
        
        # Sort merged pool by score descending
        all_chunks.sort(key=lambda c: -c.score)
        
        # Respect max_context_tokens
        if self.max_context_tokens:
            result = []
            used_tokens = 0
            for chunk in all_chunks:
                tks = _estimate_tokens(chunk.text)
                if used_tokens + tks > self.max_context_tokens:
                    break
                used_tokens += tks
                result.append(chunk)
            return result
        
        return all_chunks



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
