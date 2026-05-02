from __future__ import annotations

import re
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin, urlparse


SOURCE_CONFIGS: dict[str, dict[str, object]] = {
    "nice_guidance": {
        "seed_urls": [
            "https://www.nice.org.uk/guidance/published",
            "https://www.nice.org.uk/guidance/conditions-and-diseases",
        ],
        "allowed_hosts": {"www.nice.org.uk", "nice.org.uk"},
        "candidate_prefixes": ["/guidance/"],
        "candidate_contains": ["/resources/"],
        "follow_prefixes": ["/guidance/"],
        "exclude_tokens": ["/about/", "/news/", "/media/", "/terms-and-conditions"],
        "max_pages": 240,
        "max_depth": 3,
        "item_type": "guidance_page",
    },
    "uspstf_recommendations": {
        "seed_urls": [
            "https://www.uspreventiveservicestaskforce.org/uspstf/recommendation-topics",
            "https://www.uspreventiveservicestaskforce.org/uspstf/recommendation-topics/uspstf-a-and-b-recommendations",
        ],
        "allowed_hosts": {"www.uspreventiveservicestaskforce.org", "uspreventiveservicestaskforce.org"},
        "candidate_prefixes": ["/uspstf/recommendation/"],
        "follow_prefixes": ["/uspstf/recommendation-topics", "/uspstf/recommendation/"],
        "exclude_tokens": ["/search", "/about-uspstf", "/tools", "/announcements"],
        "max_pages": 120,
        "max_depth": 2,
        "item_type": "recommendation_page",
    },
    "nccih_health": {
        "seed_urls": [
            "https://www.nccih.nih.gov/health/atoz/",
            "https://www.nccih.nih.gov/health/herbsataglance",
        ],
        "allowed_hosts": {"www.nccih.nih.gov", "nccih.nih.gov"},
        "candidate_prefixes": ["/health/"],
        "follow_prefixes": ["/health/atoz", "/health/"],
        "exclude_tokens": ["/news/", "/research/", "/grants/", "/about/", "/training/"],
        "max_pages": 180,
        "max_depth": 2,
        "item_type": "health_topic",
    },
    "nci_pdq": {
        "seed_urls": [
            "https://www.cancer.gov/publications/pdq/information-summaries",
            "https://www.cancer.gov/publications/pdq/information-summaries/adult-treatment",
            "https://www.cancer.gov/publications/pdq/information-summaries/screening",
            "https://www.cancer.gov/publications/pdq/information-summaries/prevention",
            "https://www.cancer.gov/publications/pdq/information-summaries/cam",
        ],
        "allowed_hosts": {"www.cancer.gov", "cancer.gov"},
        "candidate_prefixes": ["/publications/pdq/", "/about-cancer/"],
        "candidate_contains": ["-pdq"],
        "follow_prefixes": ["/publications/pdq/", "/about-cancer/"],
        "exclude_tokens": ["/news-events/", "/about-nci/", "/grants-training/", "/research/"],
        "max_pages": 260,
        "max_depth": 3,
        "item_type": "pdq_summary",
    },
    "vncdc_documents": {
        "seed_urls": [
            "https://vncdc.gov.vn/",
            "https://vncdc.gov.vn/vi/tai-lieu-truyen-thong.html",
            "https://vncdc.gov.vn/vi/van-ban.html",
        ],
        "allowed_hosts": {"www.vncdc.gov.vn", "vncdc.gov.vn"},
        "candidate_prefixes": ["/vi/"],
        "candidate_contains": ["/files/", "/media/", "/tai-lieu", "/huong-dan", "/van-ban", "/so-tay"],
        "follow_prefixes": ["/vi/"],
        "exclude_tokens": ["/lien-he", "/gioi-thieu", "/tim-kiem", "/video"],
        "max_pages": 220,
        "max_depth": 2,
        "item_type": "document_page",
    },
    "vaac_hiv_aids": {
        "seed_urls": [
            "https://vaac.gov.vn/",
            "https://vaac.gov.vn/chuyen-muc/tai-lieu-chuyen-mon",
            "https://vaac.gov.vn/xet-nghiem-hiv",
        ],
        "allowed_hosts": {"www.vaac.gov.vn", "vaac.gov.vn"},
        "candidate_prefixes": ["/"],
        "candidate_contains": ["/tai-lieu", "/hiv", "/arv", "/prep", "/pep", "/methadone", "/xet-nghiem", "/dieu-tri"],
        "follow_prefixes": ["/"],
        "exclude_tokens": ["/tim-kiem", "/lien-he", "/chuyen-trang"],
        "max_pages": 220,
        "max_depth": 2,
        "item_type": "hiv_guidance_page",
    },
    "vien_dinh_duong": {
        "seed_urls": [
            "https://viendinhduong.vn/vi/trang-chu.html",
            "https://viendinhduong.vn/vi/tin-tuc.html",
        ],
        "allowed_hosts": {"www.viendinhduong.vn", "viendinhduong.vn"},
        "candidate_prefixes": ["/vi/"],
        "candidate_contains": ["/dinh-duong", "/tai-lieu", "/hoi-dap", "/vi-chat", "/an-toan-thuc-pham", "/nhu-cau-dinh-duong"],
        "follow_prefixes": ["/vi/"],
        "exclude_tokens": ["/lien-he", "/dang-nhap", "/tim-kiem"],
        "max_pages": 220,
        "max_depth": 2,
        "item_type": "nutrition_page",
    },
}

FILE_ASSET_EXTENSIONS = {
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".zip",
}


def normalize_href(base_url: str, href: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
        return ""
    return urljoin(base_url, href).split("#", 1)[0]


def _looks_like_file_asset(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in FILE_ASSET_EXTENSIONS)


def _clean_title(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _filename_hint(url: str, fallback: str) -> str:
    parsed = urlparse(url)
    stem = Path(parsed.path).name or fallback
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._")
    return safe or fallback


def _config(source_id: str) -> dict[str, object]:
    if source_id not in SOURCE_CONFIGS:
        raise ValueError(f"Unsupported reference source: {source_id}")
    return SOURCE_CONFIGS[source_id]


def is_candidate_url(source_id: str, url: str) -> bool:
    config = _config(source_id)
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path_lower = parsed.path.lower()
    if host not in config["allowed_hosts"]:
        return False
    if any(token in path_lower for token in config["exclude_tokens"]):
        return False
    if _looks_like_file_asset(url):
        return True
    if any(path_lower.startswith(prefix.lower()) for prefix in config.get("candidate_prefixes", [])):
        return True
    return any(token.lower() in path_lower for token in config.get("candidate_contains", []))


def is_follow_url(source_id: str, url: str) -> bool:
    config = _config(source_id)
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path_lower = parsed.path.lower()
    if host not in config["allowed_hosts"]:
        return False
    if _looks_like_file_asset(url):
        return False
    if any(token in path_lower for token in config["exclude_tokens"]):
        return False
    if any(path_lower.startswith(prefix.lower()) for prefix in config.get("follow_prefixes", [])):
        return True
    return any(token.lower() in path_lower for token in config.get("candidate_contains", []))


def discover_items(
    source_id: str,
    *,
    get_text: Callable[[str], str | None],
    max_items: int = 0,
) -> list[dict[str, str]]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    config = _config(source_id)
    queue: list[tuple[str, int]] = [(url, 0) for url in config["seed_urls"]]  # type: ignore[index]
    seen_pages: set[str] = set()
    discovered: list[dict[str, str]] = []
    discovered_urls: set[str] = set()
    max_pages = int(config["max_pages"])
    max_depth = int(config["max_depth"])

    while queue and len(seen_pages) < max_pages:
        page_url, depth = queue.pop(0)
        if page_url in seen_pages:
            continue
        seen_pages.add(page_url)
        html = get_text(page_url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a", href=True):
            target_url = normalize_href(page_url, anchor.get("href", ""))
            if not target_url:
                continue

            if is_candidate_url(source_id, target_url) and target_url not in discovered_urls:
                discovered_urls.add(target_url)
                title = _clean_title(anchor.get_text(" ", strip=True))
                if not title:
                    title = _filename_hint(target_url, source_id)
                discovered.append({"url": target_url, "title": title})
                if max_items and len(discovered) >= max_items:
                    return discovered

            if depth < max_depth and is_follow_url(source_id, target_url) and target_url not in seen_pages:
                queue.append((target_url, depth + 1))

    return discovered


def crawl(
    *,
    source_id: str,
    rows: list[dict[str, str]],
    crawl_run_id: str,
    max_items: int,
    resume: bool,
    get_text: Callable[[str], str | None],
    download_bytes: Callable[[str], tuple[bytes, dict[str, str]]],
    register_download: Callable[..., tuple[dict[str, str], str]],
    should_skip: Callable[..., bool],
    utc_now: Callable[[], str],
    sleep_fn: Callable[[float], None],
) -> dict[str, int | str]:
    config = _config(source_id)
    items = discover_items(source_id, get_text=get_text, max_items=max_items)
    downloaded = 0
    skipped = 0
    failed = 0

    print(f"[{source_id}] discovered {len(items)} candidate item(s)", flush=True)

    for item in items:
        item_url = item["url"]
        if resume and should_skip(rows, item_url=item_url):
            skipped += 1
            continue

        discovered_at = utc_now()
        try:
            content, metadata = download_bytes(item_url)
        except Exception:
            failed += 1
            continue

        downloaded_at = utc_now()
        filename_hint = _filename_hint(item_url, source_id)
        if not Path(filename_hint).suffix:
            filename_hint = f"{Path(filename_hint).stem}.html"

        register_download(
            source_id=source_id,
            rows=rows,
            item_type=str(config["item_type"]),
            title_hint=item["title"],
            item_url=item_url,
            file_url="",
            parent_item_url="",
            filename_hint=filename_hint,
            content=content,
            metadata=metadata,
            crawl_run_id=crawl_run_id,
            discovered_at=discovered_at,
            downloaded_at=downloaded_at,
        )
        downloaded += 1
        sleep_fn(0.25)

    return {
        "source_id": source_id,
        "mode": "reference_site",
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
    }
