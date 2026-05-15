from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


_STOPWORDS = {
    "the", "and", "for", "with", "from", "this", "that",
    "trong", "theo", "cua", "mot", "nhung", "giua", "dua", "tren",
    "nghien", "cuu", "danh", "gia", "benh", "nhan", "ket", "qua",
}
_KEEP_SHORT_TOKENS = {"vu", "ct", "mr", "er", "pr", "ivf"}
_CRITICAL_TERMS = {
    "phyllode", "phyllodes", "carcinom", "carcinoma", "buong", "trung",
    "ihc", "her2", "ki67", "brca", "bevacizumab",
}


def _normalize(text: str) -> str:
    base = unicodedata.normalize("NFKD", text or "")
    stripped = "".join(ch for ch in base if not unicodedata.combining(ch))
    stripped = stripped.replace("đ", "d").replace("Đ", "D")
    return " ".join(stripped.lower().split())


def _tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"\w+", _normalize(text), flags=re.UNICODE)
        if (len(token) >= 3 or token in _KEEP_SHORT_TOKENS) and token not in _STOPWORDS
    }


def _phrases(text: str) -> list[str]:
    ordered = [
        tok
        for tok in re.findall(r"\w+", _normalize(text), flags=re.UNICODE)
        if (len(tok) >= 3 or tok in _KEEP_SHORT_TOKENS) and tok not in _STOPWORDS
    ]
    result: list[str] = []
    for size in (5, 4, 3, 2):
        for i in range(len(ordered) - size + 1):
            phrase = " ".join(ordered[i:i + size])
            if phrase not in result:
                result.append(phrase)
    return result[:24]


def _acronyms(text: str) -> set[str]:
    return {match.group(0).lower() for match in re.finditer(r"\b[A-Z][A-Z0-9]{1,6}\b", text or "")}


def _expand_query_aliases(query: str) -> str:
    q_norm = _normalize(query)
    aliases: list[str] = []
    if "diep the" in q_norm or "u diep" in q_norm:
        aliases.append("phyllode phyllodes")
    if "hoa mo mien dich" in q_norm:
        aliases.append("ihc immunohistochemistry")
    if "buong trung" in q_norm:
        aliases.append("ovarian ovary")
    if not aliases:
        return query
    return f"{query} {' '.join(aliases)}"


@dataclass
class IndexedArticleChunk:
    id: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ArticleCandidate:
    article_key: str
    article_id: str = ""
    doc_id: str = ""
    title: str = ""
    source_name: str = ""
    source_url: str = ""
    score: float = 0.0
    reason: str = ""
    chunks: list[IndexedArticleChunk] = field(default_factory=list)


class ArticleLexicalIndex:
    def __init__(self, articles: list[dict[str, Any]]):
        self.articles = articles
        self._by_article_id = {
            str(article.get("article_id") or ""): article
            for article in articles
            if str(article.get("article_id") or "")
        }
        self._by_doc_id = {
            str(article.get("doc_id") or ""): article
            for article in articles
            if str(article.get("doc_id") or "")
        }
        self._by_title_source = {
            (
                _normalize(str(article.get("title") or "")),
                _normalize(str(article.get("source_name") or "")),
            ): article
            for article in articles
            if str(article.get("title") or "")
        }

    @classmethod
    def from_jsonl(cls, path: str | Path) -> "ArticleLexicalIndex":
        articles: dict[str, dict[str, Any]] = {}
        max_chunks_per_article = max(1, int(os.getenv("RAG_ARTICLE_INDEX_MAX_CHUNKS_PER_ARTICLE", "12")))
        max_chunk_chars = max(400, int(os.getenv("RAG_ARTICLE_INDEX_MAX_CHUNK_CHARS", "3500")))
        p = Path(path)
        with open(p, "r", encoding="utf-8") as fh:
            for raw in fh:
                if not raw.strip():
                    continue
                row = json.loads(raw)
                metadata = row.get("metadata") or row
                if not isinstance(metadata, dict):
                    continue
                title = str(metadata.get("canonical_title") or metadata.get("title") or "").strip()
                article_id = str(metadata.get("article_id") or "").strip()
                doc_id = str(metadata.get("doc_id") or "").strip()
                source_name = str(metadata.get("source_name") or "").strip()
                source_url = str(metadata.get("source_url") or "").strip()
                if not title and not article_id and not doc_id:
                    continue
                article_key = article_id or doc_id or f"{source_name}:{_normalize(title)}"
                current = articles.setdefault(
                    article_key,
                    {
                        "article_key": article_key,
                        "article_id": article_id,
                        "doc_id": doc_id,
                        "title": title,
                        "source_name": source_name,
                        "source_url": source_url,
                        "institution": str(metadata.get("institution") or ""),
                        "source_id": str(metadata.get("source_id") or ""),
                        "text": "",
                        "chunks": [],
                    },
                )
                text = str(row.get("text") or "")
                if not current.get("text") and text:
                    current["text"] = text[:800]
                chunks = current.setdefault("chunks", [])
                if len(chunks) < max_chunks_per_article and text:
                    chunk_metadata = {
                        key: metadata.get(key)
                        for key in (
                            "source_name", "source_id", "doc_type", "specialty", "audience",
                            "trust_tier", "language", "title", "canonical_title", "section_title",
                            "section_type", "chunk_role", "source_url", "updated_at", "heading_path",
                            "tags", "doc_id", "article_id", "institution", "chunk_index",
                        )
                        if key in metadata
                    }
                    chunks.append(
                        IndexedArticleChunk(
                            id=str(row.get("id") or ""),
                            text=text[:max_chunk_chars],
                            metadata=chunk_metadata,
                        )
                    )
        return cls(list(articles.values()))

    def chunks_for_identity(
        self,
        *,
        article_id: str = "",
        doc_id: str = "",
        title: str = "",
        source_name: str = "",
    ) -> list[IndexedArticleChunk]:
        article = None
        if article_id:
            article = self._by_article_id.get(article_id)
        if article is None and doc_id:
            article = self._by_doc_id.get(doc_id)
        if article is None and title:
            article = self._by_title_source.get((_normalize(title), _normalize(source_name)))
            if article is None and not source_name:
                title_norm = _normalize(title)
                for (candidate_title, _candidate_source), candidate_article in self._by_title_source.items():
                    if candidate_title == title_norm:
                        article = candidate_article
                        break
        if not article:
            return []
        return list(article.get("chunks") or [])

    def search(self, query: str, *, limit: int = 5) -> list[ArticleCandidate]:
        expanded_query = _expand_query_aliases(query)
        q_norm = _normalize(expanded_query)
        q_tokens = _tokens(expanded_query)
        q_phrases = _phrases(expanded_query)
        q_acronyms = _acronyms(expanded_query)
        scored: list[ArticleCandidate] = []

        for article in self.articles:
            title = str(article.get("title") or "")
            haystack = " ".join(
                str(article.get(key) or "")
                for key in ("title", "source_name", "source_id", "institution", "text")
            )
            h_norm = _normalize(haystack)
            h_tokens = _tokens(haystack)
            if not h_tokens:
                continue

            overlap = len(q_tokens & h_tokens)
            score = overlap * 0.08
            reasons: list[str] = []
            if overlap:
                reasons.append(f"token_overlap={overlap}")

            critical_overlap = (q_tokens & h_tokens) & _CRITICAL_TERMS
            if critical_overlap:
                score += 0.28 * len(critical_overlap)
                reasons.append(f"critical={','.join(sorted(critical_overlap))}")
                if critical_overlap & {"phyllode", "phyllodes"}:
                    score += 1.0
                    reasons.append("critical_entity=phyllodes")

            title_tokens = _tokens(title)
            title_overlap = q_tokens & title_tokens
            if title_overlap:
                score += min(0.06 * len(title_overlap), 0.36)
                reasons.append(f"title_overlap={len(title_overlap)}")

            for phrase in q_phrases:
                if phrase and phrase in h_norm:
                    score += 0.16 if len(phrase.split()) >= 3 else 0.08
                    reasons.append(f"phrase={phrase}")

            if title and _normalize(title) in q_norm:
                score += 0.8
                reasons.append("title_in_query")

            if q_acronyms:
                matched = sum(1 for acronym in q_acronyms if re.search(rf"\b{re.escape(acronym)}\b", h_norm))
                if matched:
                    score += 0.24 * (matched / len(q_acronyms))
                    reasons.append(f"acronym={matched}/{len(q_acronyms)}")

            if score <= 0:
                continue
            scored.append(
                ArticleCandidate(
                    article_key=str(article.get("article_key") or ""),
                    article_id=str(article.get("article_id") or ""),
                    doc_id=str(article.get("doc_id") or ""),
                    title=title,
                    source_name=str(article.get("source_name") or ""),
                    source_url=str(article.get("source_url") or ""),
                    score=round(score, 4),
                    reason="; ".join(reasons[:4]),
                    chunks=list(article.get("chunks") or []),
                )
            )

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored[:limit]
