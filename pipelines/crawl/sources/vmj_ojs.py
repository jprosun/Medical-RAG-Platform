from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from services.utils.data_paths import source_qa_dir


VMJ_ARCHIVE_URL = "https://tapchiyhocvietnam.vn/index.php/vmj/issue/archive"
FRONTIER_FILENAME = "vmj_ojs_frontier.json"


def normalize_href(base_url: str, href: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
        return ""
    return urljoin(base_url, href).split("#", 1)[0]


def extract_issue_urls_from_html(base_url: str, html: str) -> tuple[list[str], list[str]]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return [], []

    soup = BeautifulSoup(html, "html.parser")
    issues: list[str] = []
    archive_pages: list[str] = []
    seen_issues: set[str] = set()
    seen_pages: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        target_url = normalize_href(base_url, anchor.get("href", ""))
        if not target_url:
            continue
        path = urlparse(target_url).path.lower()
        if "/issue/view/" in path and target_url not in seen_issues:
            seen_issues.add(target_url)
            issues.append(target_url)
        elif ("/issue/archive/" in path or path.endswith("/issue/archive")) and target_url not in seen_pages:
            seen_pages.add(target_url)
            archive_pages.append(target_url)

    return issues, archive_pages


def extract_article_entries_from_html(issue_url: str, html: str) -> list[dict[str, str]]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    def clean_title(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "")).strip()

    soup = BeautifulSoup(html, "html.parser")
    articles: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        target_url = normalize_href(issue_url, anchor.get("href", ""))
        if not target_url:
            continue

        article_match = re.search(r"/article/view/(\d+)$", urlparse(target_url).path)
        pdf_match = re.search(r"/article/view/(\d+)/(\d+)$", urlparse(target_url).path)
        text = clean_title(anchor.get_text(" ", strip=True))

        if article_match and text and text.lower() != "pdf":
            article_id = article_match.group(1)
            articles.setdefault(
                article_id,
                {
                    "article_url": target_url,
                    "file_url": "",
                    "title": text,
                    "issue_url": issue_url,
                },
            )
            if not articles[article_id]["title"]:
                articles[article_id]["title"] = text
        elif pdf_match:
            article_id = pdf_match.group(1)
            articles.setdefault(
                article_id,
                {
                    "article_url": f"https://tapchiyhocvietnam.vn/index.php/vmj/article/view/{article_id}",
                    "file_url": "",
                    "title": "",
                    "issue_url": issue_url,
                },
            )
            articles[article_id]["file_url"] = target_url

    return [
        article
        for article in articles.values()
        if article.get("article_url") and article.get("file_url")
    ]


def extract_direct_download_url_from_html(base_url: str, html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    download_anchor = soup.select_one("a.download[href]")
    if download_anchor:
        href = normalize_href(base_url, download_anchor.get("href", ""))
        if href:
            return href

    iframe = soup.select_one("iframe[src]")
    if iframe:
        iframe_url = normalize_href(base_url, iframe.get("src", ""))
        if iframe_url:
            parsed = urlparse(iframe_url)
            file_param = parse_qs(parsed.query).get("file", [])
            if file_param:
                return unquote(file_param[0])

    return ""


def fallback_download_url_from_view_url(file_url: str) -> str:
    match = re.search(r"/article/view/(\d+)/(\d+)$", file_url)
    if not match:
        return file_url
    return f"https://tapchiyhocvietnam.vn/index.php/vmj/article/download/{match.group(1)}/{match.group(2)}"


def discover_issue_urls(get_text: Callable[[str], str | None], max_pages: int = 0) -> list[str]:
    issue_urls: list[str] = []
    issue_seen: set[str] = set()
    archive_seen: set[str] = set()
    queue: list[str] = [VMJ_ARCHIVE_URL]
    page_budget = max_pages if max_pages > 0 else 0

    while queue and (page_budget <= 0 or len(archive_seen) < page_budget):
        page_url = queue.pop(0)
        if page_url in archive_seen:
            continue
        archive_seen.add(page_url)
        html = get_text(page_url)
        if not html:
            continue
        issues, next_pages = extract_issue_urls_from_html(page_url, html)
        for issue_url in issues:
            if issue_url not in issue_seen:
                issue_seen.add(issue_url)
                issue_urls.append(issue_url)
        for next_page in next_pages:
            if next_page not in archive_seen:
                queue.append(next_page)

    return issue_urls


def resolve_download_url(wrapper_view_url: str, get_text: Callable[[str], str | None]) -> str:
    html = get_text(wrapper_view_url)
    if not html:
        return fallback_download_url_from_view_url(wrapper_view_url)
    return extract_direct_download_url_from_html(wrapper_view_url, html) or fallback_download_url_from_view_url(wrapper_view_url)


def _frontier_path() -> Path:
    qa_dir = source_qa_dir("vmj_ojs")
    qa_dir.mkdir(parents=True, exist_ok=True)
    return qa_dir / FRONTIER_FILENAME


def _load_frontier() -> dict[str, object]:
    path = _frontier_path()
    if not path.exists():
        return {
            "issue_urls": [],
            "issue_cursor": 0,
            "pending_articles": [],
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "issue_urls": [],
            "issue_cursor": 0,
            "pending_articles": [],
        }
    if not isinstance(payload, dict):
        return {
            "issue_urls": [],
            "issue_cursor": 0,
            "pending_articles": [],
        }
    payload.setdefault("issue_urls", [])
    payload.setdefault("issue_cursor", 0)
    payload.setdefault("pending_articles", [])
    return payload


def _save_frontier(frontier: dict[str, object]) -> None:
    path = _frontier_path()
    path.write_text(json.dumps(frontier, ensure_ascii=False, indent=2), encoding="utf-8")


def repair_rows(
    rows: list[dict[str, str]],
    *,
    rag_data_root: Path,
    processed_dir: Path,
) -> int:
    repaired = 0
    for row in rows:
        file_url = (row.get("file_url") or "").strip()
        if "/article/view/" not in file_url:
            continue

        rel_path = (row.get("relative_path") or "").strip()
        raw_path = rag_data_root / rel_path if rel_path else None
        corrected_url = file_url
        if raw_path and raw_path.exists():
            try:
                raw_html = raw_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                raw_html = ""
            corrected_url = extract_direct_download_url_from_html(file_url, raw_html) or fallback_download_url_from_view_url(file_url)
        else:
            corrected_url = fallback_download_url_from_view_url(file_url)

        if corrected_url == file_url:
            continue

        if raw_path and raw_path.exists():
            raw_path.unlink()

        if rel_path:
            processed_path = processed_dir / f"{Path(rel_path).stem}.txt"
            if processed_path.exists():
                processed_path.unlink()

        row["file_url"] = corrected_url
        row["mime_type"] = ""
        row["content_class"] = "pdf"
        row["http_status"] = ""
        row["content_length"] = ""
        row["etag"] = ""
        row["last_modified"] = ""
        row["sha256"] = ""
        row["extract_strategy"] = "classify_pdf"
        row["extract_status"] = "pending"
        row["notes"] = "repaired_vmj_pdf_link"
        repaired += 1

    return repaired


def _ensure_issue_urls(
    frontier: dict[str, object],
    *,
    get_text: Callable[[str], str | None],
    max_items: int,
) -> list[str]:
    issue_urls = [str(x) for x in frontier.get("issue_urls", []) if x]
    issue_cursor = int(frontier.get("issue_cursor", 0) or 0)
    pending_articles = list(frontier.get("pending_articles", []))

    frontier_exhausted = bool(issue_urls) and issue_cursor >= len(issue_urls) and not pending_articles
    if issue_urls and not frontier_exhausted:
        return issue_urls

    discovered_issue_urls = discover_issue_urls(get_text, max_pages=0)
    if issue_urls:
        seen = set(issue_urls)
        merged_issue_urls = issue_urls + [url for url in discovered_issue_urls if url not in seen]
        if len(merged_issue_urls) > len(issue_urls):
            print(
                f"[vmj_ojs] refreshed issue catalog: {len(issue_urls)} -> {len(merged_issue_urls)}",
                flush=True,
            )
        issue_urls = merged_issue_urls
        frontier["issue_cursor"] = min(issue_cursor, len(issue_urls))
    else:
        issue_urls = discovered_issue_urls
        frontier["issue_cursor"] = 0

    frontier["issue_urls"] = issue_urls
    _save_frontier(frontier)
    return issue_urls


def _pending_key(article: dict[str, str]) -> str:
    return f"{article.get('article_url','')}|{article.get('file_url','')}"


def _fill_frontier(
    frontier: dict[str, object],
    *,
    rows: list[dict[str, str]],
    get_text: Callable[[str], str | None],
    should_skip: Callable[..., bool],
    max_items: int,
) -> tuple[int, int]:
    issue_urls = _ensure_issue_urls(frontier, get_text=get_text, max_items=max_items)
    issue_cursor = int(frontier.get("issue_cursor", 0) or 0)
    pending_articles = list(frontier.get("pending_articles", []))
    pending_keys = {_pending_key(article) for article in pending_articles if isinstance(article, dict)}

    target_buffer = max(100, max_items * 4 if max_items else 200)
    discovered_issues = len(issue_urls)
    scanned_now = 0

    while issue_cursor < discovered_issues and len(pending_articles) < target_buffer:
        issue_url = issue_urls[issue_cursor]
        html = get_text(issue_url)
        issue_cursor += 1
        scanned_now += 1
        frontier["issue_cursor"] = issue_cursor
        if not html:
            continue

        for article in extract_article_entries_from_html(issue_url, html):
            key = _pending_key(article)
            if key in pending_keys:
                continue
            item_url = article["article_url"]
            file_url = article["file_url"]
            if should_skip(rows, item_url=item_url, file_url=file_url):
                continue
            pending_articles.append(article)
            pending_keys.add(key)

        frontier["pending_articles"] = pending_articles
        if scanned_now == 1 or scanned_now % 10 == 0:
            print(
                f"[vmj_ojs] frontier scan issue {issue_cursor}/{discovered_issues}; "
                f"pending_queue={len(pending_articles)}",
                flush=True,
            )
        _save_frontier(frontier)

    frontier["issue_cursor"] = issue_cursor
    frontier["pending_articles"] = pending_articles
    _save_frontier(frontier)
    return issue_cursor, discovered_issues


def crawl(
    *,
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
    frontier = _load_frontier()
    issue_cursor, discovered_issues = _fill_frontier(
        frontier,
        rows=rows,
        get_text=get_text,
        should_skip=should_skip,
        max_items=max_items,
    )
    downloaded = 0
    skipped = 0
    failed = 0
    seen_files: set[str] = set()
    limit = max_items or 0
    pending_articles = list(frontier.get("pending_articles", []))

    print(
        f"[vmj_ojs] frontier ready: issues={discovered_issues}, "
        f"issue_cursor={issue_cursor}, pending_queue={len(pending_articles)}, "
        f"max_items={max_items or 'all'}",
        flush=True,
    )

    while pending_articles:
        article = pending_articles.pop(0)
        frontier["pending_articles"] = pending_articles

        file_url = article["file_url"]
        if "/article/view/" in file_url:
            file_url = resolve_download_url(file_url, get_text)
        item_url = article["article_url"]
        if file_url in seen_files:
            continue
        seen_files.add(file_url)

        if resume and should_skip(rows, item_url=item_url, file_url=file_url):
            skipped += 1
            if skipped == 1 or skipped % 50 == 0:
                print(f"[vmj_ojs] skipped={skipped} pending_queue={len(pending_articles)}", flush=True)
            _save_frontier(frontier)
            continue

        discovered_at = utc_now()
        try:
            content, metadata = download_bytes(file_url)
        except Exception:
            failed += 1
            _save_frontier(frontier)
            continue

        downloaded_at = utc_now()
        match = re.search(r"/article/(?:view|download)/(\d+)/(\d+)(?:/(\d+))?$", urlparse(file_url).path)
        if match:
            suffix = f"_{match.group(3)}" if match.group(3) else ""
            filename_hint = f"{match.group(1)}_{match.group(2)}{suffix}.pdf"
        else:
            filename_hint = "vmj_article.pdf"

        register_download(
            source_id="vmj_ojs",
            rows=rows,
            item_type="journal_pdf",
            title_hint=article["title"] or "PDF",
            item_url=item_url,
            file_url=file_url,
            parent_item_url=article.get("issue_url", ""),
            filename_hint=filename_hint,
            content=content,
            metadata=metadata,
            crawl_run_id=crawl_run_id,
            discovered_at=discovered_at,
            downloaded_at=downloaded_at,
        )
        downloaded += 1
        if downloaded == 1 or downloaded % 25 == 0:
            print(
                f"[vmj_ojs] saved {downloaded} pdf(s); last={filename_hint}; "
                f"pending_queue={len(pending_articles)}",
                flush=True,
            )
        _save_frontier(frontier)
        if limit and downloaded >= limit:
            return {"source_id": "vmj_ojs", "mode": "vmj_ojs_site", "downloaded": downloaded, "skipped": skipped, "failed": failed}
        sleep_fn(0.2)

    return {"source_id": "vmj_ojs", "mode": "vmj_ojs_site", "downloaded": downloaded, "skipped": skipped, "failed": failed}
