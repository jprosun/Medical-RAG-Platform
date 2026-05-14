from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_STOPWORDS = {
    "the", "and", "for", "with", "from", "this", "that",
    "trong", "theo", "cua", "của", "mot", "một", "nhung", "những",
    "nghien", "nghiên", "cuu", "cứu", "danh", "gia", "đánh", "giá",
    "benh", "bệnh", "nhan", "nhân", "ket", "qua", "kết", "quả",
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
        if len(token) >= 3 and token not in _STOPWORDS
    }


def _phrases(text: str) -> list[str]:
    ordered = [tok for tok in re.findall(r"\w+", _normalize(text), flags=re.UNICODE) if len(tok) >= 3 and tok not in _STOPWORDS]
    result: list[str] = []
    for size in (5, 4, 3, 2):
        for i in range(len(ordered) - size + 1):
            phrase = " ".join(ordered[i:i + size])
            if phrase not in result:
                result.append(phrase)
    return result[:24]


def _acronyms(text: str) -> set[str]:
    return {match.group(0).lower() for match in re.finditer(r"\b[A-Z][A-Z0-9]{1,6}\b", text or "")}


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


class ArticleLexicalIndex:
    def __init__(self, articles: list[dict[str, Any]]):
        self.articles = articles

    @classmethod
    def from_jsonl(cls, path: str | Path) -> "ArticleLexicalIndex":
        articles: dict[str, dict[str, Any]] = {}
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
                    },
                )
                if not current.get("text") and row.get("text"):
                    current["text"] = str(row.get("text") or "")[:800]
        return cls(list(articles.values()))

    def search(self, query: str, *, limit: int = 5) -> list[ArticleCandidate]:
        q_norm = _normalize(query)
        q_tokens = _tokens(query)
        q_phrases = _phrases(query)
        q_acronyms = _acronyms(query)
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
                )
            )

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored[:limit]
