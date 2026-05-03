from __future__ import annotations

import re
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin, urlparse


SOURCE_CONFIGS: dict[str, dict[str, object]] = {
    "nhs_health_a_z": {
        "seed_urls": ["https://www.nhs.uk/health-a-to-z/"],
        "allowed_hosts": {"www.nhs.uk", "nhs.uk"},
        "candidate_prefixes": ["/conditions/", "/medicines/", "/tests-and-treatments/", "/symptoms/"],
        "follow_prefixes": ["/health-a-to-z/", "/conditions/", "/medicines/", "/tests-and-treatments/", "/symptoms/"],
        "exclude_tokens": ["/search-results", "/live-well/", "/services/", "/conditions-a-to-z/"],
        "max_pages": 80,
        "max_depth": 2,
        "item_type": "health_topic",
    },
    "msd_manual_consumer": {
        "seed_urls": ["https://www.msdmanuals.com/home/health-topics"],
        "allowed_hosts": {"www.msdmanuals.com", "msdmanuals.com"},
        "candidate_prefixes": ["/home/"],
        "follow_prefixes": ["/home/health-topics", "/home/"],
        "exclude_tokens": ["/home/multimedia", "/home/resources", "/home/professional", "/home/news", "/home/quizzes"],
        "max_pages": 120,
        "max_depth": 2,
        "item_type": "consumer_topic",
    },
    "msd_manual_professional": {
        "seed_urls": ["https://www.msdmanuals.com/professional/health-topics"],
        "allowed_hosts": {"www.msdmanuals.com", "msdmanuals.com"},
        "candidate_prefixes": ["/professional/"],
        "follow_prefixes": ["/professional/health-topics", "/professional/"],
        "exclude_tokens": ["/professional/multimedia", "/professional/resources", "/professional/news", "/professional/quizzes", "/professional/commentary"],
        "max_pages": 140,
        "max_depth": 2,
        "item_type": "professional_topic",
    },
    "cdc_health_topics": {
        "seed_urls": ["https://cdc.gov/health-topics.html"],
        "allowed_hosts": {"www.cdc.gov", "cdc.gov"},
        "candidate_prefixes": ["/"],
        "follow_prefixes": ["/health-topics", "/"],
        "exclude_tokens": [
            "/media/",
            "/about/",
            "/other/",
            "/agency/",
            "/museum/",
            "/training/",
            "/vaccines/",
            "/search",
            "/spanish/",
            "/nchs/",
            "/mmwr/",
            "/surveillance/",
            "/data-statistics/",
            "/cdc-info/",
            "/fellowships/",
            "/budget/",
            "/foia/",
            "/oeeo/",
            "/jobs/",
            "/contact-us/",
            "/about-cdc/",
        ],
        "max_pages": 80,
        "max_depth": 1,
        "item_type": "health_topic",
    },
    "mayo_diseases_conditions": {
        "seed_urls": ["https://www.mayoclinic.org/diseases-conditions/index.aspx"],
        "allowed_hosts": {"www.mayoclinic.org", "mayoclinic.org"},
        "candidate_prefixes": ["/diseases-conditions/"],
        "follow_prefixes": ["/diseases-conditions/"],
        "exclude_tokens": ["/symptom-checker", "/doctors-departments", "/tests-procedures", "/drugs-supplements"],
        "max_pages": 120,
        "max_depth": 2,
        "item_type": "disease_condition",
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
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".tiff",
    ".webp",
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
        raise ValueError(f"Unsupported basic topic source: {source_id}")
    return SOURCE_CONFIGS[source_id]


def is_candidate_url(source_id: str, url: str) -> bool:
    config = _config(source_id)
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path
    path_lower = path.lower()
    if host not in config["allowed_hosts"]:
        return False
    if _looks_like_file_asset(url):
        return False
    if any(token in path_lower for token in config["exclude_tokens"]):
        return False
    if source_id == "cdc_health_topics":
        if path_lower in {"", "/", "/index.html", "/index.htm"} or path_lower == "/health-topics.html":
            return False
        if any(
            token in path_lower
            for token in ("/cdc-info/", "/fellowships/", "/budget/", "/foia/", "/oeeo/", "/jobs/", "/contact-us/", "/about-cdc/")
        ):
            return False
        if path_lower.count("/") > 3:
            return False
        return path_lower.endswith(".html") or path_lower.endswith("/")
    if source_id == "mayo_diseases_conditions":
        if "index?letter=" in url.lower():
            return False
        return "/symptoms-causes/" in path_lower or "/diagnosis-treatment/" in path_lower
    return any(path_lower.startswith(prefix.lower()) for prefix in config["candidate_prefixes"])


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
    return any(path_lower.startswith(prefix.lower()) for prefix in config["follow_prefixes"])


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
                if source_id == "cdc_health_topics":
                    lowered_title = title.lower()
                    if lowered_title in {"contact us", "budget", "foia", "index"}:
                        continue
                    if any(
                        token in lowered_title
                        for token in (
                            "affirmative employment",
                            "alternative dispute resolution",
                            "equal employment opportunity",
                            "contact us",
                            "budget",
                            "foia",
                            "about cdc",
                        )
                    ):
                        continue
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

    print(
        f"[{source_id}] discovered {len(items)} candidate topic page(s)",
        flush=True,
    )

    for index, item in enumerate(items, start=1):
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
        if downloaded == 1 or downloaded % 25 == 0 or index == len(items):
            print(
                f"[{source_id}] downloaded={downloaded} skipped={skipped} failed={failed}",
                flush=True,
            )
        sleep_fn(0.25)

    return {
        "source_id": source_id,
        "mode": "basic_topic_site",
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
    }
