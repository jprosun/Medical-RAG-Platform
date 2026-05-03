from __future__ import annotations

import argparse
import json
import re
from html import unescape
from pathlib import Path

from services.utils.crawl_manifest import build_corpus_catalog, read_manifest, write_manifest
from services.utils.data_paths import RAG_DATA_ROOT, source_processed_dir, source_qa_dir
from tools.classify_pdfs import classify_pdf
from tools.extract_digital_pdf import write_processed_pdf_text


def _clean_text(text: str) -> str:
    text = _fix_common_mojibake(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _postclean_text(text: str, *, source_id: str) -> str:
    replacements = {
        "â‰¥": "≥",
        "â‰¤": "≤",
        "â€“": "–",
        "â€”": "—",
        "â€™": "’",
        "â€œ": "“",
        "â€": "”",
        "â€˜": "‘",
        "Â©": "©",
        "Â®": "®",
        "Â±": "±",
        "Â°": "°",
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)

    lines = [_clean_text(line) for line in text.splitlines()]
    kept: list[str] = []
    for line in lines:
        if not line:
            continue
        lowered = line.lower()

        if source_id in {"msd_manual_consumer", "msd_manual_professional"}:
            if lowered in {
                "search",
                "language",
                "select language",
                "contact us",
                "health topics",
                "healthy living",
                "professional",
                "consumer",
                "consumer version",
                "professional edition",
                "professional version",
                "skip to main content",
                "settings",
            }:
                continue
            if lowered.startswith(("view our ", "follow us on ", "edition switcher", "newsletter")):
                continue
            if any(
                token in lowered
                for token in (
                    "skip to main content",
                    "contact us",
                    "edition switcher",
                    "view our facebook page",
                    "view our x page",
                    "follow us on",
                )
            ):
                continue
            if lowered.startswith("©"):
                continue
            if any(token in lowered for token in ("red arrow", "white arrow", "yellow arrow", "ctisus")):
                continue

        if source_id == "cdc_health_topics":
            if lowered in {"search", "contact us", "budget", "foia", "about cdc", "view all", "index"}:
                continue
            if any(
                token in lowered
                for token in ("budget", "foia", "contact us", "affirmative employment", "equal employment opportunity")
            ):
                continue

        kept.append(line)

    return _clean_text("\n".join(kept))


def _fix_common_mojibake(text: str) -> str:
    if not text or not any(token in text for token in ("Ã", "â", "Â")):
        return text
    replacements = {
        "Â°": "°",
        "â‰¥": "≥",
        "â‰¤": "≤",
        "â€”": "—",
        "â€“": "–",
        "â€™": "’",
        "â€œ": "“",
        "â€": "”",
        "â€˜": "‘",
        "Ã±": "ñ",
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    try:
        repaired = text.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
    except Exception:
        return text
    if not repaired:
        return text
    if sum(repaired.count(token) for token in ("Ã", "â", "Â")) < sum(text.count(token) for token in ("Ã", "â", "Â")):
        return repaired
    return text


def _dedupe_sentences(text: str) -> str:
    if not text or text.count(".") + text.count("!") + text.count("?") < 1:
        return text
    parts = re.split(r"(?<=[.!?])\s+", text)
    kept: list[str] = []
    normalized_seen: list[str] = []
    for part in parts:
        sentence = _clean_text(part)
        if not sentence:
            continue
        normalized = re.sub(r"\s+", " ", sentence).strip().lower()
        if normalized in normalized_seen:
            continue
        if len(normalized) >= 40 and any(normalized in existing or existing in normalized for existing in normalized_seen):
            continue
        normalized_seen.append(normalized)
        kept.append(sentence)
    return " ".join(kept).strip()


def _dedupe_blocks(blocks: list[str]) -> list[str]:
    cleaned: list[str] = []
    normalized_blocks: list[str] = []
    for block in blocks:
        text = _dedupe_sentences(_clean_text(block))
        if not text:
            continue
        normalized = re.sub(r"\s+", " ", text).strip().lower()
        if normalized in normalized_blocks:
            continue
        if len(normalized) >= 30 and len(normalized.split()) >= 4 and any(normalized in existing for existing in normalized_blocks):
            continue
        if len(normalized) >= 120 and any(normalized in existing or existing in normalized for existing in normalized_blocks):
            continue
        normalized_blocks.append(normalized)
        cleaned.append(text)
    return cleaned


def _processed_text_path(source_id: str, stem: str) -> Path:
    return source_processed_dir(source_id) / f"{stem}.txt"


def _delete_text_asset(source_id: str, stem: str) -> None:
    out_path = _processed_text_path(source_id, stem)
    if out_path.exists():
        out_path.unlink()


def _classify_pdf_extract_profile(source_id: str, asset_path: Path) -> dict[str, int | str]:
    category, pages, total_text = classify_pdf(asset_path)
    profile: dict[str, int | str] = {
        "category": category,
        "pages": pages,
        "total_text": total_text,
        "strategy": "classify_pdf",
        "action": "fail",
    }

    if source_id != "vien_dinh_duong":
        if category == "digital":
            profile["strategy"] = "digital_pdf_text"
            profile["action"] = "process"
        elif category == "scanned":
            profile["strategy"] = "ocr_backlog"
            profile["action"] = "defer"
        return profile

    avg_text_per_page = total_text / max(pages, 1)
    if category == "digital":
        if pages >= 24:
            profile["strategy"] = "long_pdf_book"
            profile["action"] = "defer"
        elif pages <= 4 and total_text < 600 and avg_text_per_page < 220:
            profile["strategy"] = "image_pdf_backlog"
            profile["action"] = "defer"
        else:
            profile["strategy"] = "digital_pdf_text"
            profile["action"] = "process"
        return profile

    if category == "scanned":
        if pages >= 24:
            profile["strategy"] = "long_pdf_book_ocr"
        else:
            profile["strategy"] = "image_pdf_backlog"
        profile["action"] = "defer"
        return profile

    return profile


def _generic_html_to_text(raw_html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _clean_text(re.sub(r"<[^>]+>", " ", raw_html))

    soup = BeautifulSoup(raw_html, "html.parser")
    for node in soup.select("script, style, noscript, svg, img, picture, source, button, form, header, footer, nav, aside"):
        node.decompose()
    main = (
        soup.select_one("main")
        or soup.select_one("article")
        or soup.select_one("[role='main']")
        or soup.select_one("#maincontent")
        or soup.select_one("#main-content")
        or soup.body
        or soup
    )
    return _clean_text(main.get_text(separator="\n", strip=True))


def _strip_html_fragment(fragment: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _clean_text(re.sub(r"<[^>]+>", " ", fragment))

    soup = BeautifulSoup(fragment or "", "html.parser")
    for node in soup.select("script, style, noscript, svg, img, picture, source"):
        node.decompose()
    return _clean_text(unescape(soup.get_text(separator="\n", strip=True)))


def _extract_nhs_html(raw_html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _generic_html_to_text(raw_html)

    soup = BeautifulSoup(raw_html, "html.parser")
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string or script.get_text() or ""
        if '"MedicalWebPage"' not in text or '"hasPart"' not in text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue
        parts = data.get("hasPart")
        if not isinstance(parts, list):
            continue

        blocks: list[str] = []
        title = _clean_text(str(data.get("name", "")))
        description = _clean_text(str(data.get("description", "")))
        if title:
            blocks.append(title)
        if description and description != title:
            blocks.append(description)

        for part in parts:
            if not isinstance(part, dict):
                continue
            heading = _clean_text(str(part.get("headline", "")))
            subparts = part.get("hasPart", [])
            texts: list[str] = []
            if isinstance(subparts, list):
                for subpart in subparts:
                    if not isinstance(subpart, dict):
                        continue
                    subheading = _clean_text(str(subpart.get("headline", "")))
                    subtext = _strip_html_fragment(str(subpart.get("text", "")))
                    if subheading and subheading not in {heading, title}:
                        texts.append(subheading)
                    if subtext:
                        texts.append(subtext)
            if texts:
                if heading:
                    blocks.append(heading)
                blocks.append("\n".join(texts))

        body = _clean_text("\n\n".join(blocks))
        if body:
            return body

    main = soup.select_one("main")
    if main:
        return _clean_text(main.get_text(separator="\n", strip=True))
    return _generic_html_to_text(raw_html)


def _extract_msd_html(raw_html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _generic_html_to_text(raw_html)

    soup = BeautifulSoup(raw_html, "html.parser")
    title_node = soup.select_one("#topicHeaderTitle") or soup.select_one("h1")
    definition_node = soup.select_one("[data-testid='topicDefinition']")
    main = soup.select_one("[data-testid='topic-main-content']") or soup.select_one("#mainContainer")
    if not main:
        return _generic_html_to_text(raw_html)

    for node in main.select(
        "script, style, noscript, svg, img, picture, source, button, nav, aside, "
        ".d-none, .tooltip-container, [class*='tooltip'], [data-testid='Topic-subnavigation'], "
        "[data-testid='topic-helper-icons-container'], [aria-hidden='true']"
    ):
        node.decompose()

    for heading in main.select("h2, h3"):
        heading_text = _clean_text(heading.get_text(" ", strip=True)).lower()
        if "reference" in heading_text:
            section = heading.find_parent("section")
            if section is not None:
                section.decompose()

    blocks: list[str] = []
    title = _clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
    definition = _clean_text(definition_node.get_text(" ", strip=True)) if definition_node else ""
    if title:
        blocks.append(title)
    if definition and definition != title:
        blocks.append(definition)

    content_root = soup.select_one("[data-testid='topic-main-content']") or main
    for node in content_root.select("h2, h3, p, li"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        lowered = text.lower()
        if lowered in {"general references", "references", "more information", "diagnosis & treatment"}:
            continue
        if re.fullmatch(r"[\d,().\s]+", text):
            continue
        blocks.append(text)

    cleaned_blocks = _dedupe_blocks(blocks)
    body = _clean_text("\n\n".join(cleaned_blocks))
    body = re.sub(r"\(\s*(?:,\s*)+\)", "", body)
    body = re.sub(r"\(\s*See also\s*\)", "", body, flags=re.IGNORECASE)
    body = re.sub(r"\s+\)\s*", ") ", body)
    body = re.sub(r"\(\s+", "(", body)
    return _postclean_text(body, source_id="msd_manual_professional")


def _extract_mayo_html(raw_html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _generic_html_to_text(raw_html)

    soup = BeautifulSoup(raw_html, "html.parser")
    for node in soup.select("script, style, noscript, svg, img, picture, source, header, footer, nav, aside, form, button"):
        node.decompose()
    main = (
        soup.select_one("article#main-content")
        or soup.select_one("article")
        or soup.select_one("main")
        or soup.select_one("#main-content")
        or soup.body
    )
    if not main:
        return _generic_html_to_text(raw_html)

    blocks: list[str] = []
    title_node = main.select_one("h1")
    if title_node:
        blocks.append(title_node.get_text(" ", strip=True))

    skip_exact = {
        "products & services",
        "by mayo clinic staff",
        "request an appointment",
        "more information",
        "diagnosis & treatment",
        "doctors & departments",
        "care at mayo clinic",
        "from mayo clinic to your inbox",
    }

    for node in main.select("h1, h2, h3, p, li"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        lowered = text.lower()
        if lowered in skip_exact:
            if lowered in {"request an appointment", "by mayo clinic staff", "more information", "diagnosis & treatment"}:
                break
            continue
        if lowered.startswith("products & services") or lowered.startswith("newsletter:") or lowered.startswith("a book:"):
            continue
        if lowered.startswith("request an appointment") or lowered.startswith("find a doctor"):
            continue
        if lowered == "diseases & conditions":
            break
        if "mayo clinic staff" in lowered:
            continue
        if "et al." in lowered or " doi:" in lowered or lowered.startswith("doi:") or "accessed " in lowered:
            break
        if re.fullmatch(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\.?\s+\d{1,2},\s+\d{4}", lowered):
            continue
        if re.fullmatch(r"con-\d+", lowered):
            continue
        if " - symptoms & causes - mayo clinic" in lowered:
            continue
        if " - diagnosis & treatment - mayo clinic" in lowered:
            continue
        if "doi:" in lowered or lowered.startswith("accessed "):
            continue
        blocks.append(text)

    cleaned_blocks = _dedupe_blocks(blocks)
    return _postclean_text("\n\n".join(cleaned_blocks), source_id="cdc_health_topics")


def _extract_cdc_html(raw_html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _generic_html_to_text(raw_html)

    soup = BeautifulSoup(raw_html, "html.parser")
    for node in soup.select("script, style, noscript, svg, img, picture, source, header, footer, nav, aside, form, button"):
        node.decompose()
    main = soup.select_one("main") or soup.select_one("#content") or soup.body
    if not main:
        return _generic_html_to_text(raw_html)

    blocks: list[str] = []
    title_node = soup.select_one("title")
    if title_node:
        title = _clean_text(title_node.get_text(" ", strip=True))
        title = re.sub(r"\s*\|\s*CDC.*$", "", title, flags=re.IGNORECASE)
        if "|" in title:
            title = title.split("|", 1)[0].strip()
        if title:
            blocks.append(title)

    for node in main.select("h1, h2, h3, p, li"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        lowered = text.lower()
        if lowered in {"español", "print", "share", "facebook", "linkedin", "twitter", "syndicate"}:
            continue
        if lowered in {"learn more", "view all"}:
            continue
        if lowered in {"more information", "sources"} or lowered.startswith("content source:"):
            break
        if re.fullmatch(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\.?\s+\d{1,2},\s+\d{4}", lowered):
            continue
        blocks.append(text)

    cleaned_blocks = _dedupe_blocks(blocks)
    return _clean_text("\n\n".join(cleaned_blocks))


def _source_specific_html_to_text(source_id: str, raw_html: str) -> str:
    if source_id == "nhs_health_a_z":
        return _extract_nhs_html(raw_html)
    if source_id in {"msd_manual_consumer", "msd_manual_professional"}:
        return _postclean_text(_extract_msd_html(raw_html), source_id=source_id)
    if source_id == "mayo_diseases_conditions":
        return _extract_mayo_html(raw_html)
    if source_id == "cdc_health_topics":
        return _extract_cdc_html(raw_html)
    return _generic_html_to_text(raw_html)


def _should_filter_html_page(source_id: str, row: dict[str, str]) -> bool:
    item_url = (row.get("item_url", "") or "").lower()
    title = (row.get("title_hint", "") or "").lower()

    if source_id in {"msd_manual_consumer", "msd_manual_professional"}:
        if "/pages-with-widgets/" in item_url or "/resource/" in item_url:
            return True
        if any(token in title for token in ("3d models", "resources", "news")):
            return True

    if source_id == "cdc_health_topics":
        utility_tokens = (
            "affirmative-employment",
            "alternative-dispute-resolution",
            "about-us",
            "about-cdc",
            "budget",
            "cdc-info",
            "contact-us",
            "data-research",
            "equal-employment-opportunity",
            "fellowships",
            "foia",
            "helpdesk",
            "index.html",
            "no-fear-act",
            "sams-user-faq",
            "site.html",
        )
        utility_title_tokens = (
            "contact us",
            "about our service",
            "view all",
            "español",
            "fellowships",
            "training opportunities",
            "no fear act",
            "faq",
            "help desk",
        )
        if any(token in item_url for token in utility_tokens) or any(token in title for token in utility_title_tokens):
            return True

    if source_id == "mayo_diseases_conditions":
        if "index?letter=" in item_url or item_url.rstrip("/").endswith("/diseases-conditions/index.aspx"):
            return True
        if not any(token in item_url for token in ("/symptoms-causes/", "/diagnosis-treatment/")):
            return True
        if title.startswith("find a condition that begins with the letter"):
            return True

    return False


def _write_text_asset(source_id: str, stem: str, header: dict[str, str], body: str) -> Path:
    out_dir = source_processed_dir(source_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = _processed_text_path(source_id, stem)
    frontmatter = "---\n" + "\n".join(f"{key}: {value}" for key, value in header.items()) + "\n---\n\n"
    out_path.write_text(frontmatter + body, encoding="utf-8")
    return out_path


def _extract_medlineplus_xml(raw_path: Path) -> int:
    from pipelines.etl.medlineplus_scraper import parse_xml_to_records

    records = parse_xml_to_records(str(raw_path))
    for index, record in enumerate(records, start=1):
        stem = f"{Path(raw_path).stem}_{index:04d}"
        _write_text_asset(
            "medlineplus",
            stem,
            {
                "source_id": "medlineplus",
                "title": record.title,
                "source_url": record.source_url,
                "language": record.language,
            },
            record.body,
        )
    return len(records)


def extract_source(source_id: str) -> dict[str, int | str]:
    rows = read_manifest(source_id)
    unique_rows: dict[str, dict[str, str]] = {}
    for row in rows:
        rel = row.get("relative_path", "").strip()
        if rel:
            unique_rows[rel] = row

    processed = 0
    failed = 0
    missing_assets = 0
    deferred = 0
    digital_pdfs = 0
    scanned_pdfs = 0
    long_pdf_books = 0
    image_like_pdfs = 0

    for rel_path, row in unique_rows.items():
        asset_path = RAG_DATA_ROOT / rel_path
        asset_stem = Path(rel_path).stem
        if not asset_path.exists():
            _delete_text_asset(source_id, asset_stem)
            failed += 1
            missing_assets += 1
            _set_extract_status(rows, rel_path, strategy=row.get("extract_strategy", "backlog"), status="missing_asset")
            continue

        content_class = row.get("content_class", "")
        if content_class in {"image", "doc", "docx", "xls", "xlsx", "binary"}:
            _delete_text_asset(source_id, asset_stem)
            deferred += 1
            _set_extract_status(rows, rel_path, strategy=row.get("extract_strategy", "backlog"), status="deferred")
            continue

        if content_class in {"html", "html_book"}:
            if _should_filter_html_page(source_id, row):
                _delete_text_asset(source_id, asset_stem)
                deferred += 1
                _set_extract_status(rows, rel_path, strategy="html_filtered", status="deferred")
                continue

            body = _source_specific_html_to_text(source_id, asset_path.read_text(encoding="utf-8", errors="ignore"))
            if not body or len(body) < 120:
                _delete_text_asset(source_id, asset_stem)
                failed += 1
                _set_extract_status(rows, rel_path, strategy="html_text", status="failed")
                continue
            _write_text_asset(
                source_id,
                asset_path.stem,
                {
                    "source_id": source_id,
                    "title": row.get("title_hint", asset_path.stem),
                    "item_url": row.get("item_url", ""),
                    "file_url": row.get("file_url", ""),
                },
                body,
            )
            processed += 1
            _set_extract_status(rows, rel_path, strategy="html_text", status="done")
            continue

        if content_class == "xml":
            count = _extract_medlineplus_xml(asset_path)
            processed += max(1, count)
            _set_extract_status(rows, rel_path, strategy="xml_text", status="done")
            continue

        if content_class == "pdf":
            profile = _classify_pdf_extract_profile(source_id, asset_path)
            category = str(profile["category"])
            strategy = str(profile["strategy"])
            action = str(profile["action"])
            if strategy in {"long_pdf_book", "long_pdf_book_ocr"}:
                long_pdf_books += 1
            if strategy == "image_pdf_backlog":
                image_like_pdfs += 1

            if action == "process":
                result = write_processed_pdf_text(
                    {
                        "relative_path": rel_path,
                        "source_id": source_id,
                        "institution_or_journal": "",
                        "title": row.get("title_hint", asset_path.stem),
                        "item_url": row.get("item_url", ""),
                        "file_url": row.get("file_url", ""),
                        "extension": row.get("extension", ".pdf"),
                    }
                )
                if result is None:
                    _delete_text_asset(source_id, asset_stem)
                    failed += 1
                    _set_extract_status(rows, rel_path, strategy="digital_pdf_text", status="failed")
                else:
                    processed += 1
                    digital_pdfs += 1
                    _set_extract_status(rows, rel_path, strategy=strategy, status="done")
            elif action == "defer":
                _delete_text_asset(source_id, asset_stem)
                deferred += 1
                if category == "scanned":
                    scanned_pdfs += 1
                _set_extract_status(rows, rel_path, strategy=strategy, status="deferred")
            else:
                _delete_text_asset(source_id, asset_stem)
                failed += 1
                _set_extract_status(rows, rel_path, strategy="classify_pdf", status="failed")
            continue

        if content_class in {"txt", "md", "csv", "jsonl", "json"}:
            body = asset_path.read_text(encoding="utf-8", errors="ignore")
            _write_text_asset(
                source_id,
                asset_path.stem,
                {
                    "source_id": source_id,
                    "title": row.get("title_hint", asset_path.stem),
                    "item_url": row.get("item_url", ""),
                    "file_url": row.get("file_url", ""),
                },
                body,
            )
            processed += 1
            _set_extract_status(rows, rel_path, strategy="universal_loader", status="done")
            continue

        _delete_text_asset(source_id, asset_stem)
        deferred += 1
        _set_extract_status(rows, rel_path, strategy=row.get("extract_strategy", "backlog"), status="deferred")

    write_manifest(source_id, rows)
    build_corpus_catalog()

    report = {
        "source_id": source_id,
        "processed": processed,
        "failed": failed,
        "missing_assets": missing_assets,
        "deferred": deferred,
        "digital_pdfs": digital_pdfs,
        "scanned_pdfs": scanned_pdfs,
        "long_pdf_books": long_pdf_books,
        "image_like_pdfs": image_like_pdfs,
    }
    qa_dir = source_qa_dir(source_id)
    qa_dir.mkdir(parents=True, exist_ok=True)
    (qa_dir / "extract_summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def _set_extract_status(rows: list[dict[str, str]], rel_path: str, *, strategy: str, status: str) -> None:
    for row in rows:
        if row.get("relative_path", "").strip() == rel_path:
            row["extract_strategy"] = strategy
            row["extract_status"] = status


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract processed text for one source using manifest.csv.")
    parser.add_argument("--source-id", required=True)
    args = parser.parse_args()

    report = extract_source(args.source_id)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
