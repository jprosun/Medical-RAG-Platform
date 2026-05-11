from __future__ import annotations

import argparse
import json
import re
from html import unescape
from pathlib import Path
from shutil import copy2
from urllib.parse import urlparse

from services.utils.crawl_manifest import build_corpus_catalog, read_manifest, resolve_asset_path, write_manifest
from services.utils.data_paths import RAG_DATA_ROOT, legacy_processed_dir, source_processed_dir, source_qa_dir
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


_CANONICAL_CONTENT_SUFFIXES = {
    ".html",
    ".htm",
    ".xml",
    ".pdf",
    ".txt",
    ".md",
    ".csv",
    ".json",
    ".jsonl",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
}


def _processed_asset_stem(rel_path: str, content_class: str = "") -> str:
    path = Path(rel_path)
    suffix = path.suffix.lower()
    if suffix and suffix not in _CANONICAL_CONTENT_SUFFIXES:
        return path.name
    if not suffix and content_class in {"html", "html_book", "xml", "pdf", "txt", "md", "csv", "jsonl", "json"}:
        return path.name
    return path.stem


def _delete_text_asset(source_id: str, stem: str) -> None:
    out_path = _processed_text_path(source_id, stem)
    if out_path.exists():
        out_path.unlink()


def _delete_text_asset_if_orphaned(source_id: str, stem: str, rows: list[dict[str, str]], rel_path: str) -> None:
    for sibling_row in rows:
        sibling_rel = (sibling_row.get("relative_path") or "").strip()
        if not sibling_rel or sibling_rel == rel_path:
            continue
        sibling_stem = _processed_asset_stem(sibling_rel, sibling_row.get("content_class", ""))
        if sibling_stem != stem:
            continue
        if (sibling_row.get("extract_status") or "").strip() == "done":
            return
    _delete_text_asset(source_id, stem)


def _legacy_processed_text_path(source_id: str, stem: str) -> Path:
    return legacy_processed_dir(source_id) / f"{stem}.txt"


def _restore_legacy_processed_text(source_id: str, stem: str) -> bool:
    out_path = _processed_text_path(source_id, stem)
    if out_path.exists():
        return True

    legacy_path = _legacy_processed_text_path(source_id, stem)
    if not legacy_path.exists():
        return False

    out_path.parent.mkdir(parents=True, exist_ok=True)
    copy2(legacy_path, out_path)
    return True


def _stale_processed_dir(source_id: str) -> Path:
    return source_qa_dir(source_id) / "stale_processed"


def _stale_processed_text_path(source_id: str, stem: str) -> Path:
    return _stale_processed_dir(source_id) / f"{stem}.txt"


def _restore_stale_processed_text(source_id: str, stem: str) -> bool:
    out_path = _processed_text_path(source_id, stem)
    if out_path.exists():
        return True

    stale_path = _stale_processed_text_path(source_id, stem)
    if not stale_path.exists():
        return False

    out_path.parent.mkdir(parents=True, exist_ok=True)
    copy2(stale_path, out_path)
    return True


def _restore_prior_processed_text(source_id: str, stem: str) -> bool:
    return _restore_legacy_processed_text(source_id, stem) or _restore_stale_processed_text(source_id, stem)


def _move_processed_to_stale_quarantine(source_id: str, processed_path: Path) -> None:
    stale_dir = _stale_processed_dir(source_id)
    stale_dir.mkdir(parents=True, exist_ok=True)
    target = stale_dir / processed_path.name
    if target.exists():
        index = 1
        while True:
            candidate = stale_dir / f"{processed_path.stem}__{index}{processed_path.suffix}"
            if not candidate.exists():
                target = candidate
                break
            index += 1
    processed_path.replace(target)


def _unique_manifest_rows(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    unique_rows: dict[str, dict[str, str]] = {}
    for row in rows:
        rel = row.get("relative_path", "").strip()
        if rel:
            unique_rows[rel] = row
    return unique_rows


def _group_unique_rows_by_stem(unique_rows: dict[str, dict[str, str]]) -> dict[str, list[tuple[str, dict[str, str]]]]:
    groups: dict[str, list[tuple[str, dict[str, str]]]] = {}
    for rel_path, row in unique_rows.items():
        groups.setdefault(_processed_asset_stem(rel_path, row.get("content_class", "")), []).append((rel_path, row))
    return groups


def _is_processable_content_class(content_class: str) -> bool:
    return content_class in {"html", "html_book", "xml", "pdf", "txt", "md", "csv", "jsonl", "json"}


_PROCESSABLE_EXTRACT_STRATEGIES = {
    "html_text",
    "xml_text",
    "digital_pdf_text",
    "universal_loader",
}


def _row_can_have_processed_output(row: dict[str, str]) -> bool:
    if _is_processable_content_class(row.get("content_class", "")):
        return True
    strategy = (row.get("extract_strategy") or "").strip()
    return strategy in _PROCESSABLE_EXTRACT_STRATEGIES


def _is_unrecoverable_bootstrapped_missing(row: dict[str, str]) -> bool:
    notes = (row.get("notes") or "").strip()
    has_url = bool((row.get("item_url") or "").strip() or (row.get("file_url") or "").strip())
    return "bootstrapped_from_existing_raw" in notes and not has_url


def _quarantine_stale_processed_outputs(source_id: str, rows: list[dict[str, str]]) -> int:
    if source_id == "medlineplus":
        return 0

    unique_rows = _unique_manifest_rows(rows)
    expected_stems = {
        _processed_asset_stem(rel_path, row.get("content_class", ""))
        for rel_path, row in unique_rows.items()
        if (row.get("extract_status") or "").strip() == "done"
    }

    moved = 0
    for processed_path in source_processed_dir(source_id).glob("*.txt"):
        if processed_path.stem in expected_stems:
            continue
        _move_processed_to_stale_quarantine(source_id, processed_path)
        moved += 1
    return moved


def _reconcile_extract_rows(source_id: str, rows: list[dict[str, str]]) -> None:
    unique_rows = _unique_manifest_rows(rows)
    stem_groups = _group_unique_rows_by_stem(unique_rows)

    for stem, items in stem_groups.items():
        if any((row.get("extract_status") or "").strip() == "done" for _, row in items):
            _restore_prior_processed_text(source_id, stem)

    for rel_path, row in unique_rows.items():
        content_class = row.get("content_class", "")
        stem = _processed_asset_stem(rel_path, content_class)
        asset_path = resolve_asset_path(rel_path, rag_root=RAG_DATA_ROOT)
        processed_path = _processed_text_path(source_id, stem)
        legacy_processed_path = _legacy_processed_text_path(source_id, stem)
        stem_group = stem_groups.get(stem, [])
        stem_has_done = any((sibling_row.get("extract_status") or "").strip() == "done" for _, sibling_row in stem_group)
        status = (row.get("extract_status") or "").strip()
        strategy = row.get("extract_strategy", "backlog")

        if not asset_path.exists():
            if stem_has_done:
                _set_extract_status(rows, rel_path, strategy="stale_sibling_backlog", status="deferred")
                continue
            if _is_unrecoverable_bootstrapped_missing(row):
                _delete_text_asset_if_orphaned(source_id, stem, rows, rel_path)
                _set_extract_status(rows, rel_path, strategy="legacy_missing_backlog", status="deferred")
                continue
            if not stem_has_done and not legacy_processed_path.exists():
                _delete_text_asset(source_id, stem)
            if status != "missing_asset":
                _set_extract_status(rows, rel_path, strategy=strategy, status="missing_asset")
            continue

        if status == "missing_asset":
            reset_status = "pending" if _row_can_have_processed_output(row) else "deferred"
            _set_extract_status(rows, rel_path, strategy=strategy, status=reset_status)
            status = reset_status

        if status == "done" and not processed_path.exists():
            if _restore_prior_processed_text(source_id, stem):
                continue
            _set_extract_status(rows, rel_path, strategy=strategy, status="pending")
            continue

        if status in {"failed", "deferred"} and processed_path.exists() and not stem_has_done:
            _delete_text_asset(source_id, stem)

    _quarantine_stale_processed_outputs(source_id, rows)


def _build_extract_report(source_id: str, rows: list[dict[str, str]]) -> dict[str, int | str]:
    unique_rows = _unique_manifest_rows(rows)

    processed = 0
    failed = 0
    missing_assets = 0
    deferred = 0
    pending = 0
    digital_pdfs = 0
    scanned_pdfs = 0
    long_pdf_books = 0
    image_like_pdfs = 0

    for rel_path, row in unique_rows.items():
        status = (row.get("extract_status") or "").strip()
        strategy = (row.get("extract_strategy") or "").strip()
        stem = _processed_asset_stem(rel_path, row.get("content_class", ""))
        processed_path = _processed_text_path(source_id, stem)

        if status == "done" and processed_path.exists():
            processed += 1
            if strategy == "digital_pdf_text":
                digital_pdfs += 1
        elif status == "done":
            pending += 1
        elif status == "failed":
            failed += 1
        elif status == "missing_asset":
            missing_assets += 1
        elif status == "deferred":
            deferred += 1
            if strategy in {"ocr_backlog", "long_pdf_book_ocr"}:
                scanned_pdfs += 1
        elif status == "pending":
            pending += 1

        if strategy in {"long_pdf_book", "long_pdf_book_ocr"}:
            long_pdf_books += 1
        if strategy == "image_pdf_backlog":
            image_like_pdfs += 1

    processed_files = len(list(source_processed_dir(source_id).glob("*.txt")))
    return {
        "source_id": source_id,
        "unique_assets": len(unique_rows),
        "processed": processed,
        "processed_files": processed_files,
        "failed": failed,
        "missing_assets": missing_assets,
        "deferred": deferred,
        "pending": pending,
        "digital_pdfs": digital_pdfs,
        "scanned_pdfs": scanned_pdfs,
        "long_pdf_books": long_pdf_books,
        "image_like_pdfs": image_like_pdfs,
    }


def _classify_pdf_extract_profile(source_id: str, asset_path: Path) -> dict[str, int | str]:
    category, pages, total_text = classify_pdf(asset_path)
    profile: dict[str, int | str] = {
        "category": category,
        "pages": pages,
        "total_text": total_text,
        "strategy": "classify_pdf",
        "action": "fail",
    }

    segmented_pdf_policies: dict[str, dict[str, int]] = {
        "vien_dinh_duong": {
            "long_pages": 24,
            "image_max_pages": 4,
            "image_max_text": 600,
            "image_max_avg_text": 220,
        },
        "who_vietnam": {
            "long_pages": 32,
            "image_max_pages": 4,
            "image_max_text": 700,
            "image_max_avg_text": 250,
        },
    }

    source_pdf_policy = segmented_pdf_policies.get(source_id)
    if source_pdf_policy is None:
        if category == "digital":
            profile["strategy"] = "digital_pdf_text"
            profile["action"] = "process"
        elif category == "scanned":
            profile["strategy"] = "ocr_backlog"
            profile["action"] = "defer"
        return profile

    avg_text_per_page = total_text / max(pages, 1)
    if category == "digital":
        if pages >= source_pdf_policy["long_pages"]:
            profile["strategy"] = "long_pdf_book"
            profile["action"] = "defer"
        elif (
            pages <= source_pdf_policy["image_max_pages"]
            and total_text < source_pdf_policy["image_max_text"]
            and avg_text_per_page < source_pdf_policy["image_max_avg_text"]
        ):
            profile["strategy"] = "image_pdf_backlog"
            profile["action"] = "defer"
        else:
            profile["strategy"] = "digital_pdf_text"
            profile["action"] = "process"
        return profile

    if category == "scanned":
        if pages >= source_pdf_policy["long_pages"]:
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
    main = (
        soup.select_one("article#main-content")
        or soup.select_one("article")
        or soup.select_one("main")
        or soup.select_one("#main-content")
        or soup.body
    )
    if not main:
        return _generic_html_to_text(raw_html)

    for node in main.select("script, style, noscript, svg, img, picture, source, footer, nav, aside, form, button"):
        node.decompose()

    blocks: list[str] = []
    title_node = soup.select_one("h1") or main.select_one("h1")
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
    body = _postclean_text("\n\n".join(cleaned_blocks), source_id="mayo_diseases_conditions")
    if body:
        return body

    meta_title = _clean_text(str((soup.find("meta", attrs={"property": "og:title"}) or {}).get("content", "")))
    meta_description = _clean_text(str((soup.find("meta", attrs={"name": "Description"}) or {}).get("content", "")))
    fallback_blocks = [part for part in (meta_title, meta_description) if part]
    return _clean_text("\n\n".join(fallback_blocks))


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


def _extract_uspstf_html(raw_html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _generic_html_to_text(raw_html)

    soup = BeautifulSoup(raw_html, "html.parser")
    for node in soup.select("script, style, noscript, svg, img, picture, source, header, footer, nav, aside, form, button"):
        node.decompose()
    main = (
        soup.select_one("main")
        or soup.select_one("[role='main']")
        or soup.select_one("#main-content")
        or soup.select_one("article")
        or soup.body
    )
    if not main:
        return _generic_html_to_text(raw_html)

    blocks: list[str] = []
    title_node = main.select_one("h1") or soup.select_one("h1")
    if title_node:
        blocks.append(_clean_text(title_node.get_text(" ", strip=True)))

    skip_exact = {
        "recommendation topics",
        "in progress",
        "published recommendations",
        "tools",
        "jump to",
        "related resources",
        "share",
        "print",
    }
    break_tokens = {"references", "more related information"}

    for node in main.select("h1, h2, h3, p, li"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        lowered = text.lower()
        if lowered in skip_exact:
            continue
        if lowered in break_tokens:
            break
        if lowered.startswith("read the full recommendation statement"):
            continue
        if lowered.startswith("the us preventive services task force"):
            continue
        if "last updated" in lowered or "archived recommendation" in lowered:
            continue
        blocks.append(text)

    return _clean_text("\n\n".join(_dedupe_blocks(blocks)))


def _extract_nccih_html(raw_html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _generic_html_to_text(raw_html)

    soup = BeautifulSoup(raw_html, "html.parser")
    for node in soup.select("script, style, noscript, svg, img, picture, source, header, footer, nav, aside, form, button"):
        node.decompose()
    main = (
        soup.select_one("main")
        or soup.select_one("[role='main']")
        or soup.select_one("#main-content")
        or soup.select_one("article")
        or soup.body
    )
    if not main:
        return _generic_html_to_text(raw_html)

    blocks: list[str] = []
    title = ""
    meta_title = soup.find("meta", attrs={"property": "og:title"})
    if meta_title and meta_title.get("content"):
        title = _clean_text(str(meta_title.get("content")))
    if not title:
        title_node = main.select_one("h1") or soup.select_one("h1")
        title = _clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
    description_node = soup.find("meta", attrs={"name": "description"})
    description = _clean_text(str(description_node.get("content"))) if description_node and description_node.get("content") else ""
    if title:
        blocks.append(title)
    if description and description != title:
        blocks.append(description)

    skip_exact = {
        "share this page",
        "print",
        "facebook",
        "x",
        "email",
        "more information",
    }
    break_tokens = {"for more information", "key references"}

    for node in main.select("h1, h2, h3, p, li"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        lowered = text.lower()
        if lowered in skip_exact:
            continue
        if lowered in break_tokens:
            break
        if lowered.startswith("nccih clinical digest") or lowered.startswith("image credit"):
            continue
        blocks.append(text)

    return _clean_text("\n\n".join(_dedupe_blocks(blocks)))


def _extract_nci_pdq_html(raw_html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return _generic_html_to_text(raw_html)

    soup = BeautifulSoup(raw_html, "html.parser")
    for node in soup.select("script, style, noscript, svg, img, picture, source, header, footer, nav, aside, form, button"):
        node.decompose()
    main = (
        soup.select_one("main")
        or soup.select_one("#main-content")
        or soup.select_one("article")
        or soup.body
    )
    if not main:
        return _generic_html_to_text(raw_html)

    blocks: list[str] = []
    title_node = main.select_one("h1") or soup.select_one("h1")
    if title_node:
        blocks.append(_clean_text(title_node.get_text(" ", strip=True)))

    description_node = soup.find("meta", attrs={"name": "description"})
    description = _clean_text(str(description_node.get("content"))) if description_node and description_node.get("content") else ""
    if description:
        blocks.append(description)

    skip_exact = {
        "on this page",
        "general information",
        "additional information",
        "more information",
        "related resources",
        "about this pdq summary",
        "about pdq",
        "health professional version",
        "patient version",
        "en español",
    }
    break_tokens = {"references", "referencias", "this summary is reviewed regularly and updated as necessary"}

    content_root = (
        main.select_one(".summary-sections")
        or main.select_one(".pdq-sections")
        or main.select_one("article")
        or main
    )
    for node in content_root.select("h1, h2, h3, p, li"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        lowered = text.lower()
        if lowered in skip_exact:
            continue
        if lowered in break_tokens or lowered.startswith("referencias"):
            break
        if lowered.startswith("this pdq cancer information summary") and "editorially independent" in lowered:
            continue
        blocks.append(text)

    return _clean_text("\n\n".join(_dedupe_blocks(blocks)))


def _html_frontmatter_overrides(source_id: str, raw_html: str, row: dict[str, str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    item_url = (row.get("item_url") or "").strip()

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        if source_id == "nci_pdq" and "/espanol/" in item_url.lower():
            overrides["language"] = "es"
        return overrides

    soup = BeautifulSoup(raw_html, "html.parser")
    html_tag = soup.find("html")
    page_lang = _clean_text(str((html_tag or {}).get("lang", ""))).lower() if html_tag else ""
    if page_lang:
        overrides["language"] = page_lang.split("-", 1)[0]

    if source_id == "uspstf_recommendations":
        overrides["doc_type"] = "guideline"
        overrides["audience"] = "clinician"
        overrides["trust_tier"] = "1"

    if source_id == "nccih_health":
        overrides["doc_type"] = "patient_education"
        overrides["audience"] = "patient"

    if source_id == "nci_pdq":
        overrides["doc_type"] = "reference"
        overrides["specialty"] = "oncology"
        overrides["trust_tier"] = "1"
        if "/espanol/" in item_url.lower():
            overrides["language"] = "es"
        audience_meta = soup.find("meta", attrs={"name": "dcterms.audience"})
        audience_text = _clean_text(str(audience_meta.get("content"))) if audience_meta and audience_meta.get("content") else ""
        if "health professional" in audience_text.lower():
            overrides["audience"] = "clinician"
        elif audience_text:
            overrides["audience"] = "patient"

    return {key: value for key, value in overrides.items() if value}


def _source_specific_html_to_text(source_id: str, raw_html: str) -> str:
    if source_id == "nhs_health_a_z":
        return _extract_nhs_html(raw_html)
    if source_id in {"msd_manual_consumer", "msd_manual_professional"}:
        return _postclean_text(_extract_msd_html(raw_html), source_id=source_id)
    if source_id == "uspstf_recommendations":
        return _extract_uspstf_html(raw_html)
    if source_id == "nccih_health":
        return _extract_nccih_html(raw_html)
    if source_id == "nci_pdq":
        return _extract_nci_pdq_html(raw_html)
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

    if source_id == "who_vietnam":
        if any(
            token in item_url
            for token in (
                "/westernpacific/emergencies/covid-19",
                "/about/how-we-work/pacific-support/",
                "/news/questions-and-answers",
            )
        ):
            return True
        if any(
            token in title
            for token in (
                "questions and answers",
                "pacific health ministers meeting",
                "who western pacific",
            )
        ):
            return True

    if source_id == "vien_dinh_duong":
        parsed = urlparse(item_url) if item_url else None
        path = parsed.path.lower() if parsed else ""
        title_variants = {title, title.replace("-", " ").strip()}
        if "/professional-activities/" in item_url:
            tail = path.split("/professional-activities/", 1)[1] if "/professional-activities/" in path else ""
            if tail and "/" not in tail and not re.search(r"[a-f0-9]{8,}$", tail):
                return True
        if path.rstrip("/") in {"/vi/professional-activities", "/vi/search"}:
            return True
        if any(
            token in item_url
            for token in (
                "/about/",
                "/cong-cu-va-tien-ich/",
                "/hop-tac-quoc-te",
                "/chuc-nang-nhiem-vu",
                "/gioi-thieu",
                "/mang-noi-bo",
                "/question/",
                "/site-map",
                "/sitemap",
                "/trang-chu",
            )
        ):
            return True
        if any(
            any(token in variant for variant in title_variants)
            for token in (
                "hợp tác quốc tế",
                "hop tac quoc te",
                "các văn bản chỉ đạo điều hành",
                "cac van ban chi dao dieu hanh",
                "chỉ đạo tuyến",
                "chi dao tuyen",
                "chức năng nhiệm vụ",
                "chuc nang nhiem vu",
                "dinh dưỡng học đường",
                "dinh duong hoc duong",
                "giao dục truyền thông dinh dưỡng",
                "giao duc truyen thong dinh duong",
                "mạng nội bộ",
                "mang noi bo",
                "ngày vi chất dinh dưỡng",
                "ngay vi chat dinh duong",
                "những lời khuyên dinh dưỡng",
                "nhung loi khuyen dinh duong",
                "phổ biến kiến thức dinh dưỡng",
                "pho bien kien thuc dinh duong",
                "site map",
                "tài liệu truyền thông",
                "tai lieu truyen thong",
                "tin tức sự kiện liên quan",
                "tin tuc su kien lien quan",
                "tin tức về đào tạo",
                "tin tuc ve dao tao",
                "tuần lễ dinh dưỡng và phát triển",
                "tuan le dinh duong va phat trien",
                "giới thiệu",
                "gioi thieu",
                "đội ngũ lãnh đạo",
                "doi ngu lanh dao",
                "sơ đồ tổ chức",
                "so do to chuc",
                "tra cứu",
                "tra cuu",
                "hỏi đáp",
                "hoi dap",
                "search",
            )
        ):
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


def extract_source(source_id: str, *, force: bool = False, reconcile_only: bool = False) -> dict[str, int | str]:
    rows = read_manifest(source_id)
    _reconcile_extract_rows(source_id, rows)
    unique_rows = _unique_manifest_rows(rows)

    if reconcile_only:
        write_manifest(source_id, rows)
        build_corpus_catalog()
        report = _build_extract_report(source_id, rows)
        qa_dir = source_qa_dir(source_id)
        qa_dir.mkdir(parents=True, exist_ok=True)
        (qa_dir / "extract_summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report

    for rel_path, row in unique_rows.items():
        asset_path = resolve_asset_path(rel_path, rag_root=RAG_DATA_ROOT)
        asset_stem = _processed_asset_stem(rel_path, row.get("content_class", ""))
        processed_path = _processed_text_path(source_id, asset_stem)
        current_status = (row.get("extract_status") or "").strip()
        if not force and current_status == "done" and processed_path.exists():
            continue
        if current_status == "deferred" and not asset_path.exists():
            continue
        if not asset_path.exists():
            _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
            _set_extract_status(rows, rel_path, strategy=row.get("extract_strategy", "backlog"), status="missing_asset")
            continue

        content_class = row.get("content_class", "")
        asset_extension = (row.get("extension") or asset_path.suffix or "").strip().lower()
        if asset_extension in {".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"}:
            _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
            _set_extract_status(rows, rel_path, strategy="office_backlog", status="deferred")
            continue

        if content_class in {"image", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "binary"}:
            _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
            _set_extract_status(rows, rel_path, strategy=row.get("extract_strategy", "backlog"), status="deferred")
            continue

        if content_class in {"html", "html_book"}:
            if _should_filter_html_page(source_id, row):
                _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
                _set_extract_status(rows, rel_path, strategy="html_filtered", status="deferred")
                continue

            raw_html = asset_path.read_text(encoding="utf-8", errors="ignore")
            body = _source_specific_html_to_text(source_id, raw_html)
            if not body or len(body) < 120:
                _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
                _set_extract_status(rows, rel_path, strategy="html_text", status="failed")
                continue
            header = {
                "source_id": source_id,
                "title": row.get("title_hint", asset_path.stem),
                "item_url": row.get("item_url", ""),
                "file_url": row.get("file_url", ""),
            }
            header.update(_html_frontmatter_overrides(source_id, raw_html, row))
            _write_text_asset(
                source_id,
                asset_stem,
                header,
                body,
            )
            _set_extract_status(rows, rel_path, strategy="html_text", status="done")
            continue

        if content_class == "xml":
            if asset_extension not in {"", ".xml"}:
                _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
                _set_extract_status(rows, rel_path, strategy="misclassified_non_xml", status="deferred")
                continue
            _extract_medlineplus_xml(asset_path)
            _set_extract_status(rows, rel_path, strategy="xml_text", status="done")
            continue

        if content_class == "pdf":
            profile = _classify_pdf_extract_profile(source_id, asset_path)
            category = str(profile["category"])
            strategy = str(profile["strategy"])
            action = str(profile["action"])

            if action == "process":
                result = write_processed_pdf_text(
                    {
                        "relative_path": rel_path,
                        "source_id": source_id,
                        "institution_or_journal": "",
                        "title": row.get("title_hint", asset_stem),
                        "item_url": row.get("item_url", ""),
                        "file_url": row.get("file_url", ""),
                        "extension": row.get("extension", ".pdf"),
                    }
                )
                if result is None:
                    _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
                    _set_extract_status(rows, rel_path, strategy="digital_pdf_text", status="failed")
                else:
                    _set_extract_status(rows, rel_path, strategy=strategy, status="done")
            elif action == "defer":
                _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
                _set_extract_status(rows, rel_path, strategy=strategy, status="deferred")
            else:
                _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
                _set_extract_status(rows, rel_path, strategy="classify_pdf", status="failed")
            continue

        if content_class in {"txt", "md", "csv", "jsonl", "json"}:
            body = asset_path.read_text(encoding="utf-8", errors="ignore")
            _write_text_asset(
                source_id,
                asset_stem,
                {
                    "source_id": source_id,
                    "title": row.get("title_hint", asset_stem),
                    "item_url": row.get("item_url", ""),
                    "file_url": row.get("file_url", ""),
                },
                body,
            )
            _set_extract_status(rows, rel_path, strategy="universal_loader", status="done")
            continue

        _delete_text_asset_if_orphaned(source_id, asset_stem, rows, rel_path)
        _set_extract_status(rows, rel_path, strategy=row.get("extract_strategy", "backlog"), status="deferred")

    write_manifest(source_id, rows)
    build_corpus_catalog()

    report = _build_extract_report(source_id, rows)
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
    parser.add_argument("--force", action="store_true", help="Re-extract assets already marked done.")
    parser.add_argument("--reconcile-only", action="store_true", help="Only reconcile manifest/processed state and refresh extract_summary.json.")
    args = parser.parse_args()

    report = extract_source(args.source_id, force=args.force, reconcile_only=args.reconcile_only)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
