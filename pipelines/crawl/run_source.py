from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from services.utils.crawl_manifest import (
    bootstrap_source_manifest,
    build_corpus_catalog,
    default_extract_status,
    default_extract_strategy,
    first_row_for_sha,
    infer_content_class,
    infer_extension,
    is_complete_row,
    latest_row_for_key,
    make_item_id,
    make_resume_key,
    read_manifest,
    utc_now,
    write_manifest,
)
from services.utils.data_lineage import file_sha256, make_run_id
from services.utils.data_paths import KNOWN_SOURCE_IDS, RAG_DATA_ROOT, ensure_rag_data_layout, source_processed_dir, source_raw_dir

from .source_registry import SOURCE_REGISTRY, SourceConfig


USER_AGENT = (
    "MedQA-RAG-Crawl/1.0 (academic research; "
    "https://github.com/lehuyphuong/LLM-MedQA-Assistant)"
)
REQUEST_TIMEOUT = 60
WHO_DISCOVERY_URLS = [
    "https://www.who.int/news-room/fact-sheets",
    "https://www.who.int/health-topics",
    "https://www.who.int/publications",
]
WHO_VIETNAM_DISCOVERY_URLS = [
    "https://www.who.int/vietnam",
    "https://www.who.int/vietnam/news",
    "https://www.who.int/vietnam/publications",
    "https://www.who.int/westernpacific/countries/viet-nam",
    "https://www.who.int/westernpacific/countries/viet-nam/news-room",
    "https://www.who.int/westernpacific/countries/viet-nam/publications",
]
VMJ_ARCHIVE_URL = "https://tapchiyhocvietnam.vn/index.php/vmj/issue/archive"
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
}


def _safe_filename(name: str, fallback: str = "asset") -> str:
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return stem or fallback


def _filename_from_url(url: str, fallback: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    return _safe_filename(name or fallback, fallback=fallback)


def _response_metadata(response) -> dict[str, str]:
    headers = response.headers
    return {
        "http_status": str(response.status_code),
        "content_length": headers.get("Content-Length", ""),
        "etag": headers.get("ETag", ""),
        "last_modified": headers.get("Last-Modified", ""),
        "mime_type": headers.get("Content-Type", "").split(";")[0].strip(),
    }


def _download_bytes(url: str) -> tuple[bytes, dict[str, str]]:
    import requests

    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.content, _response_metadata(response)


def _get_text(url: str) -> str | None:
    import requests

    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text
    except Exception:
        return None


def _normalize_href(base_url: str, href: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
        return ""
    return urljoin(base_url, href).split("#", 1)[0]


def _looks_like_file_asset(url: str) -> bool:
    return infer_extension(url=url).lower() in FILE_ASSET_EXTENSIONS


def _clean_title_hint(text: str, fallback: str = "") -> str:
    text = re.sub(r"\s+", " ", (text or "")).strip()
    return text or fallback


def _classify_who_item_type(url: str) -> str:
    lowered = url.lower()
    if _looks_like_file_asset(url):
        return "document_asset"
    if "/fact-sheets/detail/" in lowered:
        return "fact_sheet"
    if "/health-topics/" in lowered:
        return "health_topic"
    if "/publications/" in lowered:
        return "publication_page"
    if "/news-room/" in lowered or "/news/" in lowered:
        return "news_page"
    return "html_page"


def _is_who_candidate_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if host not in {"www.who.int", "who.int", "cdn.who.int"}:
        return False
    if "mega-menu" in path:
        return False
    if _looks_like_file_asset(url):
        return True
    return any(
        marker in path
        for marker in (
            "/fact-sheets/detail/",
            "/health-topics/",
            "/publications/",
            "/news-room/",
        )
    )


def _is_who_follow_page(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if host not in {"www.who.int", "who.int"}:
        return False
    if _looks_like_file_asset(url) or "mega-menu" in path:
        return False
    return any(
        marker in path
        for marker in (
            "/health-topics/",
            "/publications/",
            "/news-room/",
        )
    )


def _is_who_vietnam_candidate_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    url_lower = url.lower()
    if host not in {"www.who.int", "who.int", "cdn.who.int", "apps.who.int"}:
        return False
    if "mega-menu" in path:
        return False
    has_vietnam_signal = any(token in url_lower for token in ("viet-nam", "vietnam", "westernpacific"))
    if not has_vietnam_signal:
        return False
    if _looks_like_file_asset(url):
        return True
    return any(
        marker in path
        for marker in (
            "/vietnam",
            "/viet-nam",
            "/westernpacific/",
            "/publications/",
            "/news-room/",
            "/news/",
        )
    )


def _is_who_vietnam_follow_page(url: str) -> bool:
    return _is_who_vietnam_candidate_url(url) and not _looks_like_file_asset(url)


def _discover_seeded_links(
    seed_urls: list[str],
    *,
    candidate_predicate,
    follow_predicate,
    max_items: int = 0,
    max_depth: int = 1,
    max_pages: int = 100,
) -> list[dict[str, str]]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    discovered: list[dict[str, str]] = []
    discovered_urls: set[str] = set()
    seen_pages: set[str] = set()
    queue: list[tuple[str, int]] = [(url, 0) for url in seed_urls]

    while queue and len(seen_pages) < max_pages:
        page_url, depth = queue.pop(0)
        if page_url in seen_pages:
            continue
        seen_pages.add(page_url)
        html = _get_text(page_url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a", href=True):
            target_url = _normalize_href(page_url, anchor.get("href", ""))
            if not target_url:
                continue
            if candidate_predicate(target_url) and target_url not in discovered_urls:
                discovered_urls.add(target_url)
                discovered.append(
                    {
                        "url": target_url,
                        "title": _clean_title_hint(anchor.get_text(" ", strip=True), Path(urlparse(target_url).path).stem),
                    }
                )
                if max_items and len(discovered) >= max_items:
                    return discovered
            if depth < max_depth and follow_predicate(target_url) and target_url not in seen_pages:
                queue.append((target_url, depth + 1))

    return discovered


def _extract_vmj_issue_urls_from_html(base_url: str, html: str) -> tuple[list[str], list[str]]:
    from .sources import vmj_ojs as vmj

    return vmj.extract_issue_urls_from_html(base_url, html)


def _extract_vmj_article_entries_from_html(issue_url: str, html: str) -> list[dict[str, str]]:
    from .sources import vmj_ojs as vmj

    return vmj.extract_article_entries_from_html(issue_url, html)


def _discover_vmj_issue_urls(max_pages: int = 0) -> list[str]:
    from .sources import vmj_ojs as vmj

    return vmj.discover_issue_urls(_get_text, max_pages=max_pages)


def _vmj_download_url_from_view_url(file_url: str) -> str:
    from .sources import vmj_ojs as vmj

    return vmj.fallback_download_url_from_view_url(file_url)


def _extract_vmj_direct_download_url_from_html(base_url: str, html: str) -> str:
    from .sources import vmj_ojs as vmj

    return vmj.extract_direct_download_url_from_html(base_url, html)


def _resolve_vmj_download_url(wrapper_view_url: str) -> str:
    from .sources import vmj_ojs as vmj

    return vmj.resolve_download_url(wrapper_view_url, _get_text)


def _repair_vmj_ojs_rows(rows: list[dict[str, str]]) -> int:
    from .sources import vmj_ojs as vmj

    return vmj.repair_rows(rows, rag_data_root=RAG_DATA_ROOT, processed_dir=source_processed_dir("vmj_ojs"))


def _write_versioned_asset(
    source_id: str,
    *,
    filename_hint: str,
    extension: str,
    content: bytes,
    previous_row: dict[str, str] | None = None,
    downloaded_at: str,
) -> tuple[Path, str]:
    raw_dir = source_raw_dir(source_id)
    raw_dir.mkdir(parents=True, exist_ok=True)

    ext = extension or infer_extension(filename=filename_hint)
    base_name = Path(filename_hint).stem if Path(filename_hint).suffix else filename_hint
    filename = _safe_filename(base_name, fallback="asset")
    if ext and not filename.endswith(ext):
        filename = f"{filename}{ext}"

    target = raw_dir / filename
    if previous_row and previous_row.get("sha256") and target.exists():
        stamp = downloaded_at.replace("-", "").replace(":", "")
        target = raw_dir / f"{Path(filename).stem}__{stamp}{Path(filename).suffix}"

    target.write_bytes(content)
    rel_path = target.resolve().relative_to(RAG_DATA_ROOT.resolve()).as_posix()
    return target, rel_path


def _register_download(
    *,
    source_id: str,
    rows: list[dict[str, str]],
    item_type: str,
    title_hint: str,
    item_url: str,
    file_url: str,
    parent_item_url: str,
    filename_hint: str,
    content: bytes,
    metadata: dict[str, str],
    crawl_run_id: str,
    discovered_at: str,
    downloaded_at: str,
) -> tuple[dict[str, str], str]:
    key = make_resume_key(file_url, item_url)
    previous = latest_row_for_key(rows, key)
    extension = infer_extension(
        filename=filename_hint,
        url=file_url or item_url,
        mime_type=metadata.get("mime_type", ""),
    )
    content_class = infer_content_class(extension, metadata.get("mime_type", ""))

    asset_path, rel_path = _write_versioned_asset(
        source_id,
        filename_hint=filename_hint,
        extension=extension,
        content=content,
        previous_row=previous,
        downloaded_at=downloaded_at,
    )
    sha256 = file_sha256(asset_path)

    alias = first_row_for_sha(rows, sha256)
    duplicate_status = ""
    duplicate_of = ""
    notes = ""
    if previous and previous.get("sha256") and previous["sha256"] != sha256:
        duplicate_status = "updated_asset"
        duplicate_of = previous.get("item_id", "")
    elif alias and make_resume_key(alias.get("file_url", ""), alias.get("item_url", "")) != key:
        duplicate_status = "alias_same_source"
        duplicate_of = alias.get("item_id", "")
        notes = "content_alias_reused_existing_blob"
        rel_path = alias.get("relative_path", rel_path)
        if asset_path.exists():
            asset_path.unlink()

    row = {
        "source_id": source_id,
        "crawl_run_id": crawl_run_id,
        "item_id": make_item_id(source_id, key or rel_path, downloaded_at),
        "item_type": item_type,
        "title_hint": title_hint,
        "item_url": item_url,
        "file_url": file_url,
        "parent_item_url": parent_item_url,
        "relative_path": rel_path,
        "extension": extension,
        "mime_type": metadata.get("mime_type", ""),
        "content_class": "html_book" if source_id == "ncbi_bookshelf" and content_class == "html" else content_class,
        "http_status": metadata.get("http_status", "200"),
        "content_length": metadata.get("content_length", str(len(content))),
        "etag": metadata.get("etag", ""),
        "last_modified": metadata.get("last_modified", ""),
        "sha256": sha256,
        "discovered_at_utc": discovered_at,
        "downloaded_at_utc": downloaded_at,
        "duplicate_status": duplicate_status,
        "duplicate_of": duplicate_of,
        "extract_strategy": default_extract_strategy(content_class if source_id != "ncbi_bookshelf" else "html_book"),
        "extract_status": default_extract_status(content_class),
        "notes": notes,
    }
    rows.append(row)
    return row, duplicate_status or "downloaded"


def _should_skip(rows: list[dict[str, str]], *, item_url: str = "", file_url: str = "") -> bool:
    key = make_resume_key(file_url, item_url)
    latest = latest_row_for_key(rows, key)
    return bool(latest and is_complete_row(latest))


def _catalog_seed_rows(source_id: str) -> list[dict[str, str]]:
    catalog_path = RAG_DATA_ROOT / "corpus_catalog.csv"
    if not catalog_path.exists():
        return []
    with open(catalog_path, "r", encoding="utf-8", newline="") as fh:
        return [
            row for row in csv.DictReader(fh)
            if (row.get("source_id") or "").strip() == source_id
        ]


def _crawl_seed_catalog_source(
    config: SourceConfig,
    *,
    rows: list[dict[str, str]],
    crawl_run_id: str,
    max_items: int,
    resume: bool,
) -> dict[str, int | str]:
    seeds = _catalog_seed_rows(config.source_id)
    downloaded = 0
    skipped = 0
    failed = 0

    for seed in seeds[:max_items or None]:
        file_url = (seed.get("file_url") or "").strip()
        item_url = (seed.get("item_url") or "").strip()
        primary_url = file_url or item_url
        if not primary_url:
            failed += 1
            continue
        if resume and _should_skip(rows, item_url=item_url, file_url=file_url):
            skipped += 1
            continue
        discovered_at = utc_now()
        try:
            content, metadata = _download_bytes(primary_url)
        except Exception:
            failed += 1
            continue

        downloaded_at = utc_now()
        filename_hint = seed.get("file_name") or _filename_from_url(primary_url, "asset")
        _register_download(
            source_id=config.source_id,
            rows=rows,
            item_type=(seed.get("item_type") or "catalog_seed"),
            title_hint=(seed.get("title") or filename_hint),
            item_url=item_url,
            file_url=file_url,
            parent_item_url=item_url if file_url and item_url and file_url != item_url else "",
            filename_hint=filename_hint,
            content=content,
            metadata=metadata,
            crawl_run_id=crawl_run_id,
            discovered_at=discovered_at,
            downloaded_at=downloaded_at,
        )
        downloaded += 1

    return {
        "source_id": config.source_id,
        "mode": config.mode,
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
    }


def _crawl_medlineplus(
    config: SourceConfig,
    *,
    rows: list[dict[str, str]],
    crawl_run_id: str,
    resume: bool,
) -> dict[str, int | str]:
    from pipelines.etl.medlineplus_scraper import MEDLINEPLUS_XML_FALLBACK, MEDLINEPLUS_XML_URL

    downloaded = 0
    skipped = 0
    failed = 0

    # Any complete XML row is enough to skip.
    if resume and any(is_complete_row(row) and row.get("content_class") == "xml" for row in rows):
        return {"source_id": config.source_id, "mode": config.mode, "downloaded": 0, "skipped": 1, "failed": 0}

    for url in [MEDLINEPLUS_XML_URL, MEDLINEPLUS_XML_FALLBACK]:
        discovered_at = utc_now()
        try:
            content, metadata = _download_bytes(url)
        except Exception:
            failed += 1
            continue
        downloaded_at = utc_now()
        _register_download(
            source_id=config.source_id,
            rows=rows,
            item_type=config.item_type,
            title_hint="MedlinePlus Health Topics XML",
            item_url="",
            file_url=url,
            parent_item_url="",
            filename_hint="mplus_topics.xml",
            content=content,
            metadata=metadata,
            crawl_run_id=crawl_run_id,
            discovered_at=discovered_at,
            downloaded_at=downloaded_at,
        )
        downloaded += 1
        break
    else:
        return {"source_id": config.source_id, "mode": config.mode, "downloaded": 0, "skipped": skipped, "failed": failed}

    return {"source_id": config.source_id, "mode": config.mode, "downloaded": downloaded, "skipped": skipped, "failed": failed}


def _crawl_who(
    config: SourceConfig,
    *,
    rows: list[dict[str, str]],
    crawl_run_id: str,
    max_items: int,
    resume: bool,
) -> dict[str, int | str]:
    topics = _discover_seeded_links(
        WHO_DISCOVERY_URLS,
        candidate_predicate=_is_who_candidate_url,
        follow_predicate=_is_who_follow_page,
        max_items=max_items,
        max_depth=1,
        max_pages=60,
    )
    downloaded = 0
    skipped = 0
    failed = 0

    for topic in topics:
        item_url = topic["url"]
        if resume and _should_skip(rows, item_url=item_url):
            skipped += 1
            continue
        discovered_at = utc_now()
        try:
            content, metadata = _download_bytes(item_url)
        except Exception:
            failed += 1
            continue
        downloaded_at = utc_now()
        filename_hint = _filename_from_url(item_url, _safe_filename(topic["title"].lower(), "topic"))
        if not Path(filename_hint).suffix:
            filename_hint = f"{Path(filename_hint).stem}.html"
        _register_download(
            source_id=config.source_id,
            rows=rows,
            item_type=_classify_who_item_type(item_url),
            title_hint=topic["title"],
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
        time.sleep(0.5)

    return {"source_id": config.source_id, "mode": config.mode, "downloaded": downloaded, "skipped": skipped, "failed": failed}


def _crawl_ncbi_bookshelf(
    config: SourceConfig,
    *,
    rows: list[dict[str, str]],
    crawl_run_id: str,
    max_items: int,
    resume: bool,
) -> dict[str, int | str]:
    from pipelines.etl.ncbi_bookshelf_scraper import NCBI_BOOKSHELF_BASE, discover_bookshelf_ids

    chapter_ids = discover_bookshelf_ids(
        max_items=max_items,
        topic_results=12,
        global_page_size=200,
    )

    downloaded = 0
    skipped = 0
    failed = 0
    for chapter_id in chapter_ids:
        item_url = f"{NCBI_BOOKSHELF_BASE}/{chapter_id}/"
        if resume and _should_skip(rows, item_url=item_url):
            skipped += 1
            continue
        discovered_at = utc_now()
        try:
            content, metadata = _download_bytes(item_url)
        except Exception:
            failed += 1
            continue
        downloaded_at = utc_now()
        _register_download(
            source_id=config.source_id,
            rows=rows,
            item_type=config.item_type,
            title_hint=f"NCBI Bookshelf chapter {chapter_id}",
            item_url=item_url,
            file_url="",
            parent_item_url="",
            filename_hint=f"{chapter_id}.html",
            content=content,
            metadata=metadata,
            crawl_run_id=crawl_run_id,
            discovered_at=discovered_at,
            downloaded_at=downloaded_at,
        )
        downloaded += 1
        time.sleep(0.25)

    return {"source_id": config.source_id, "mode": config.mode, "downloaded": downloaded, "skipped": skipped, "failed": failed}


def _crawl_who_vietnam(
    config: SourceConfig,
    *,
    rows: list[dict[str, str]],
    crawl_run_id: str,
    max_items: int,
    resume: bool,
) -> dict[str, int | str]:
    items = _discover_seeded_links(
        WHO_VIETNAM_DISCOVERY_URLS,
        candidate_predicate=_is_who_vietnam_candidate_url,
        follow_predicate=_is_who_vietnam_follow_page,
        max_items=max_items,
        max_depth=2,
        max_pages=80,
    )
    downloaded = 0
    skipped = 0
    failed = 0

    for item in items:
        item_url = item["url"]
        if resume and _should_skip(rows, item_url=item_url):
            skipped += 1
            continue
        discovered_at = utc_now()
        try:
            content, metadata = _download_bytes(item_url)
        except Exception:
            failed += 1
            continue
        downloaded_at = utc_now()
        filename_hint = _filename_from_url(item_url, _safe_filename(item["title"].lower(), "who_vietnam"))
        if not Path(filename_hint).suffix:
            filename_hint = f"{Path(filename_hint).stem}.html"
        _register_download(
            source_id=config.source_id,
            rows=rows,
            item_type="country_asset" if _looks_like_file_asset(item_url) else "country_page",
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
        time.sleep(0.35)

    return {"source_id": config.source_id, "mode": config.mode, "downloaded": downloaded, "skipped": skipped, "failed": failed}


def _crawl_vmj_ojs(
    config: SourceConfig,
    *,
    rows: list[dict[str, str]],
    crawl_run_id: str,
    max_items: int,
    resume: bool,
) -> dict[str, int | str]:
    from .sources import vmj_ojs as vmj

    return vmj.crawl(
        rows=rows,
        crawl_run_id=crawl_run_id,
        max_items=max_items,
        resume=resume,
        get_text=_get_text,
        download_bytes=_download_bytes,
        register_download=_register_download,
        should_skip=_should_skip,
        utc_now=utc_now,
        sleep_fn=time.sleep,
    )


def _crawl_basic_topic_site(
    config: SourceConfig,
    *,
    rows: list[dict[str, str]],
    crawl_run_id: str,
    max_items: int,
    resume: bool,
) -> dict[str, int | str]:
    from .sources import basic_topics

    return basic_topics.crawl(
        source_id=config.source_id,
        rows=rows,
        crawl_run_id=crawl_run_id,
        max_items=max_items,
        resume=resume,
        get_text=_get_text,
        download_bytes=_download_bytes,
        register_download=_register_download,
        should_skip=_should_skip,
        utc_now=utc_now,
        sleep_fn=time.sleep,
    )


def run_source(
    *,
    source_id: str,
    resume: bool = True,
    max_items: int = 0,
) -> dict[str, int | str]:
    if source_id not in KNOWN_SOURCE_IDS:
        raise ValueError(f"Unknown source_id: {source_id}")
    if source_id not in SOURCE_REGISTRY:
        raise ValueError(f"Source {source_id} is known but has no crawl config")

    ensure_rag_data_layout([source_id])
    bootstrap_source_manifest(source_id)
    rows = read_manifest(source_id)
    crawl_run_id = os.getenv("CRAWL_RUN_ID") or make_run_id("crawl", source_id)
    config = SOURCE_REGISTRY[source_id]

    if source_id == "vmj_ojs":
        repaired = _repair_vmj_ojs_rows(rows)
        if repaired:
            write_manifest(source_id, rows)
            build_corpus_catalog()

    if config.mode == "medlineplus_xml":
        report = _crawl_medlineplus(config, rows=rows, crawl_run_id=crawl_run_id, resume=resume)
    elif config.mode == "who_expanded":
        report = _crawl_who(config, rows=rows, crawl_run_id=crawl_run_id, max_items=max_items, resume=resume)
    elif config.mode == "ncbi_bookshelf":
        report = _crawl_ncbi_bookshelf(config, rows=rows, crawl_run_id=crawl_run_id, max_items=max_items, resume=resume)
    elif config.mode == "who_vietnam_site":
        report = _crawl_who_vietnam(config, rows=rows, crawl_run_id=crawl_run_id, max_items=max_items, resume=resume)
    elif config.mode == "vmj_ojs_site":
        report = _crawl_vmj_ojs(config, rows=rows, crawl_run_id=crawl_run_id, max_items=max_items, resume=resume)
    elif config.mode == "basic_topic_site":
        report = _crawl_basic_topic_site(config, rows=rows, crawl_run_id=crawl_run_id, max_items=max_items, resume=resume)
    elif config.mode == "seed_catalog_refresh":
        report = _crawl_seed_catalog_source(config, rows=rows, crawl_run_id=crawl_run_id, max_items=max_items, resume=resume)
    else:
        raise ValueError(f"Unsupported crawl mode: {config.mode}")

    write_manifest(source_id, rows)
    build_corpus_catalog()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run raw crawl for one supported source into per-source manifest.csv.")
    parser.add_argument("--source-id", required=True, choices=sorted(SOURCE_REGISTRY))
    parser.add_argument("--resume", action="store_true", default=False)
    parser.add_argument("--max-items", type=int, default=0)
    args = parser.parse_args()

    report = run_source(
        source_id=args.source_id,
        resume=args.resume,
        max_items=args.max_items,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
