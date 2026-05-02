from __future__ import annotations

import re
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, unquote, urljoin, urlparse


VMJ_ARCHIVE_URL = "https://tapchiyhocvietnam.vn/index.php/vmj/issue/archive"


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
    page_budget = max_pages or 200

    while queue and len(archive_seen) < page_budget:
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
    archive_page_budget = 200
    if max_items:
        archive_page_budget = max(8, min(200, (max_items // 20) + 8))

    issue_urls = discover_issue_urls(get_text, max_pages=archive_page_budget)
    downloaded = 0
    skipped = 0
    failed = 0
    seen_files: set[str] = set()
    limit = max_items or 0
    discovered_issues = len(issue_urls)

    print(
        f"[vmj_ojs] discovered {discovered_issues} issue pages "
        f"(archive_pages={archive_page_budget}, max_items={max_items or 'all'})",
        flush=True,
    )

    for issue_index, issue_url in enumerate(issue_urls, start=1):
        html = get_text(issue_url)
        if not html:
            failed += 1
            continue

        if issue_index == 1 or issue_index % 10 == 0:
            print(
                f"[vmj_ojs] issue {issue_index}/{discovered_issues} "
                f"downloaded={downloaded} skipped={skipped} failed={failed}",
                flush=True,
            )

        for article in extract_article_entries_from_html(issue_url, html):
            file_url = article["file_url"]
            if "/article/view/" in file_url:
                file_url = resolve_download_url(file_url, get_text)
            item_url = article["article_url"]
            if file_url in seen_files:
                continue
            seen_files.add(file_url)

            if resume and should_skip(rows, item_url=item_url, file_url=file_url):
                skipped += 1
                continue

            discovered_at = utc_now()
            try:
                content, metadata = download_bytes(file_url)
            except Exception:
                failed += 1
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
                parent_item_url=issue_url,
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
                    f"[vmj_ojs] saved {downloaded} pdf(s); "
                    f"last={filename_hint}",
                    flush=True,
                )
            if limit and downloaded >= limit:
                return {"source_id": "vmj_ojs", "mode": "vmj_ojs_site", "downloaded": downloaded, "skipped": skipped, "failed": failed}
            sleep_fn(0.2)

    return {"source_id": "vmj_ojs", "mode": "vmj_ojs_site", "downloaded": downloaded, "skipped": skipped, "failed": failed}
