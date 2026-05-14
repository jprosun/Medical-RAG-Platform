from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any

import requests


TRUSTED_DOMAINS = (
    "who.int",
    "ncbi.nlm.nih.gov",
    "nih.gov",
    "cdc.gov",
    "msdmanuals.com",
    "mayoclinic.org",
    "nice.org.uk",
    "escardio.org",
    "heart.org",
    "moh.gov.vn",
    "kcb.vn",
)

_RISKY_MARKERS = (
    "guideline", "hướng dẫn", "huong dan", "khuyến cáo", "khuyen cao",
    "liều", "lieu", "phác đồ", "phac do", "protocol", "regimen",
    "tỷ lệ", "ty le", "%", "auc", "or", "hr", "rr", "ci",
    "tiêu chuẩn", "tieu chuan", "chẩn đoán", "chan doan",
)


@dataclass
class ExternalSource:
    id: str
    title: str
    url: str
    snippet: str = ""
    source_domain: str = ""


@dataclass
class ExternalEvidencePack:
    enabled: bool = False
    used: bool = False
    status: str = "disabled"
    sources: list[ExternalSource] = field(default_factory=list)


def query_needs_external_sources(query: str, coverage, router_output) -> bool:
    if os.getenv("EXTERNAL_SEARCH_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "y", "on"}:
        return False
    answer_policy = getattr(router_output, "answer_policy", "strict_rag")
    coverage_mode = getattr(coverage, "coverage_mode", "")
    if answer_policy != "open_enriched" and coverage_mode not in {"open_knowledge", "title_anchored"}:
        return False
    haystack = (query or "").lower()
    if any(marker in haystack for marker in _RISKY_MARKERS):
        return True
    return bool(getattr(coverage, "allow_external", False) and getattr(coverage, "max_external_sources", 0) > 0)


def _domain_allowed(url: str) -> bool:
    lowered = (url or "").lower()
    return any(domain in lowered for domain in TRUSTED_DOMAINS)


def _extract_domain(url: str) -> str:
    match = re.search(r"https?://([^/]+)", url or "")
    return match.group(1).lower() if match else ""


def resolve_external_sources(query: str, *, max_sources: int = 3) -> ExternalEvidencePack:
    if os.getenv("EXTERNAL_SEARCH_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "y", "on"}:
        return ExternalEvidencePack(enabled=False, status="disabled")
    api_key = os.getenv("TAVILY_API_KEY", "").strip()
    if not api_key:
        return ExternalEvidencePack(enabled=True, status="missing_api_key")

    payload: dict[str, Any] = {
        "api_key": api_key,
        "query": query,
        "search_depth": os.getenv("TAVILY_SEARCH_DEPTH", "basic"),
        "max_results": max(max_sources * 2, max_sources),
        "include_answer": False,
        "include_raw_content": False,
        "include_domains": list(TRUSTED_DOMAINS),
    }
    try:
        response = requests.post(
            "https://api.tavily.com/search",
            json=payload,
            timeout=float(os.getenv("EXTERNAL_SEARCH_TIMEOUT_S", "6")),
        )
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        return ExternalEvidencePack(enabled=True, status=f"search_error:{type(exc).__name__}")

    sources: list[ExternalSource] = []
    for item in data.get("results", []) or []:
        url = str(item.get("url") or "")
        if not _domain_allowed(url):
            continue
        title = str(item.get("title") or "").strip()
        snippet = str(item.get("content") or item.get("snippet") or "").strip()
        if not title and not snippet:
            continue
        sources.append(
            ExternalSource(
                id=f"E{len(sources) + 1}",
                title=title or url,
                url=url,
                snippet=snippet[:900],
                source_domain=_extract_domain(url),
            )
        )
        if len(sources) >= max_sources:
            break

    return ExternalEvidencePack(
        enabled=True,
        used=bool(sources),
        status="ok" if sources else "no_trusted_results",
        sources=sources,
    )


def format_external_sources_for_prompt(pack: ExternalEvidencePack) -> str:
    if not pack.sources:
        return ""
    lines = ["EXTERNAL SOURCES (chỉ dùng cho số liệu/guideline/liều/phác đồ cụ thể nếu snippet hỗ trợ):"]
    for source in pack.sources:
        lines.append(f"[{source.id}] {source.title} - {source.url}")
        if source.snippet:
            lines.append(f"Snippet: {source.snippet}")
    return "\n".join(lines)
