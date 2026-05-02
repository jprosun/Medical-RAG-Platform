from __future__ import annotations

import csv
import hashlib
import mimetypes
import time
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

from services.utils.data_lineage import file_sha256
from services.utils.data_paths import KNOWN_SOURCE_IDS, RAG_DATA_ROOT, source_manifest_path, source_raw_dir


MANIFEST_FIELDS = [
    "source_id",
    "crawl_run_id",
    "item_id",
    "item_type",
    "title_hint",
    "item_url",
    "file_url",
    "parent_item_url",
    "relative_path",
    "extension",
    "mime_type",
    "content_class",
    "http_status",
    "content_length",
    "etag",
    "last_modified",
    "sha256",
    "discovered_at_utc",
    "downloaded_at_utc",
    "duplicate_status",
    "duplicate_of",
    "extract_strategy",
    "extract_status",
    "notes",
]

CATALOG_FIELDS = [
    "source_id",
    "institution_or_journal",
    "file_name",
    "relative_path",
    "extension",
    "file_size_kb",
    "title",
    "item_type",
    "item_url",
    "file_url",
    "sha256",
]

SOURCE_DISPLAY_NAMES = {
    "cantho_med_journal": "Cần Thơ Medical Journal",
    "dav_gov": "Drug Administration of Vietnam",
    "hue_jmp_ojs": "Hue Journal of Medicine and Pharmacy",
    "kcb_moh": "Ministry of Health - KCB",
    "medlineplus": "MedlinePlus",
    "mil_med_pharm_journal": "Military Medical and Pharmacy Journal",
    "ncbi_bookshelf": "NCBI Bookshelf",
    "trad_med_pharm_journal": "Traditional Medicine and Pharmacy Journal",
    "vmj_ojs": "Vietnam Medical Journal",
    "who": "World Health Organization",
    "who_vietnam": "WHO Vietnam",
}


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def read_manifest(source_id: str) -> list[dict[str, str]]:
    path = source_manifest_path(source_id)
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [_normalize_row(dict(row)) for row in reader]


def write_manifest(source_id: str, rows: Iterable[dict[str, str]]) -> Path:
    path = source_manifest_path(source_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(_normalize_row(row))
    return path


def make_resume_key(file_url: str = "", item_url: str = "") -> str:
    return (file_url or item_url or "").strip()


def make_item_id(source_id: str, primary_key: str, salt: str = "") -> str:
    raw = f"{source_id}::{primary_key}::{salt}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:20]


def infer_extension(
    *,
    filename: str = "",
    url: str = "",
    mime_type: str = "",
    fallback: str = "",
) -> str:
    for candidate in (filename, urlparse(url).path):
        suffix = Path(candidate).suffix.lower().strip()
        if suffix:
            return suffix if suffix.startswith(".") else f".{suffix}"
    if mime_type:
        guessed = mimetypes.guess_extension(mime_type.split(";")[0].strip())
        if guessed:
            return guessed.lower()
    if fallback:
        return fallback if fallback.startswith(".") else f".{fallback.lower()}"
    return ""


def infer_content_class(extension: str = "", mime_type: str = "") -> str:
    ext = (extension or "").lower()
    mime = (mime_type or "").lower()
    if ext in {".html", ".htm"} or "html" in mime:
        return "html"
    if ext == ".xml" or "xml" in mime:
        return "xml"
    if ext == ".pdf" or "pdf" in mime:
        return "pdf"
    if ext in {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"} or mime.startswith("image/"):
        return "image"
    if ext in {".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"}:
        return ext.lstrip(".")
    if ext == ".txt":
        return "txt"
    if ext in {".md", ".markdown"}:
        return "md"
    if ext == ".csv":
        return "csv"
    if ext == ".jsonl":
        return "jsonl"
    if ext == ".json":
        return "json"
    return "binary"


def default_extract_strategy(content_class: str) -> str:
    mapping = {
        "html": "html_text",
        "html_book": "html_text",
        "xml": "xml_text",
        "pdf": "classify_pdf",
        "image": "image_backlog",
        "doc": "office_backlog",
        "docx": "office_backlog",
        "xls": "office_backlog",
        "xlsx": "office_backlog",
        "txt": "universal_loader",
        "md": "universal_loader",
        "csv": "universal_loader",
        "jsonl": "universal_loader",
    }
    return mapping.get(content_class, "backlog")


def default_extract_status(content_class: str) -> str:
    if content_class in {"image", "doc", "docx", "xls", "xlsx"}:
        return "deferred"
    return "pending"


def resolve_asset_path(rel_path: str, *, rag_root: Path | None = None) -> Path:
    root = rag_root or RAG_DATA_ROOT
    rel = (rel_path or "").strip()
    if not rel:
        return root

    primary = root / rel
    if primary.exists():
        return primary

    legacy = root.parent / rel
    if legacy.exists():
        return legacy

    return primary


def is_complete_row(row: dict[str, str], *, rag_root: Path | None = None) -> bool:
    rel = (row.get("relative_path") or "").strip()
    if not rel:
        return False
    asset_path = resolve_asset_path(rel, rag_root=rag_root)
    return (
        row.get("http_status", "").strip() == "200"
        and bool(row.get("sha256", "").strip())
        and asset_path.exists()
    )


def latest_row_for_key(rows: list[dict[str, str]], key: str) -> dict[str, str] | None:
    if not key:
        return None
    for row in reversed(rows):
        if make_resume_key(row.get("file_url", ""), row.get("item_url", "")) == key:
            return row
    return None


def first_row_for_sha(rows: list[dict[str, str]], sha256: str, *, rag_root: Path | None = None) -> dict[str, str] | None:
    if not sha256:
        return None
    for row in rows:
        if row.get("sha256", "").strip() == sha256 and is_complete_row(row, rag_root=rag_root):
            return row
    return None


def manifest_row_from_catalog_row(row: dict[str, str]) -> dict[str, str]:
    source_id = (row.get("source_id") or "").strip()
    file_url = (row.get("file_url") or "").strip()
    item_url = (row.get("item_url") or "").strip()
    primary_key = file_url or item_url or (row.get("relative_path") or "").strip()
    extension = infer_extension(
        filename=row.get("file_name", ""),
        url=file_url or item_url,
        fallback=row.get("extension", ""),
    )
    content_class = infer_content_class(extension, "")
    sha = (row.get("sha256") or "").strip()
    return _normalize_row(
        {
            "source_id": source_id,
            "crawl_run_id": "",
            "item_id": make_item_id(source_id, primary_key or row.get("file_name", ""), "bootstrap"),
            "item_type": (row.get("item_type") or "").strip(),
            "title_hint": (row.get("title") or "").strip(),
            "item_url": item_url,
            "file_url": file_url,
            "parent_item_url": item_url if file_url and item_url and item_url != file_url else "",
            "relative_path": (row.get("relative_path") or "").strip(),
            "extension": extension,
            "mime_type": "",
            "content_class": content_class,
            "http_status": "200" if sha else "",
            "content_length": "",
            "etag": "",
            "last_modified": "",
            "sha256": sha,
            "discovered_at_utc": "",
            "downloaded_at_utc": "",
            "duplicate_status": "",
            "duplicate_of": "",
            "extract_strategy": default_extract_strategy(content_class),
            "extract_status": default_extract_status(content_class),
            "notes": "bootstrapped_from_corpus_catalog",
        }
    )


def manifest_row_from_existing_file(source_id: str, path: Path) -> dict[str, str]:
    rel = path.resolve().relative_to(RAG_DATA_ROOT.resolve()).as_posix()
    extension = infer_extension(filename=path.name)
    content_class = infer_content_class(extension, "")
    sha = file_sha256(path)
    return _normalize_row(
        {
            "source_id": source_id,
            "crawl_run_id": "",
            "item_id": make_item_id(source_id, rel, "raw_scan"),
            "item_type": "raw_asset",
            "title_hint": path.stem,
            "item_url": "",
            "file_url": "",
            "parent_item_url": "",
            "relative_path": rel,
            "extension": extension,
            "mime_type": mimetypes.guess_type(path.name)[0] or "",
            "content_class": content_class,
            "http_status": "200",
            "content_length": str(path.stat().st_size),
            "etag": "",
            "last_modified": "",
            "sha256": sha,
            "discovered_at_utc": "",
            "downloaded_at_utc": "",
            "duplicate_status": "",
            "duplicate_of": "",
            "extract_strategy": default_extract_strategy(content_class),
            "extract_status": default_extract_status(content_class),
            "notes": "bootstrapped_from_existing_raw",
        }
    )


def bootstrap_source_manifest(
    source_id: str,
    *,
    global_catalog_path: Path | None = None,
) -> dict[str, int | str]:
    if source_id not in KNOWN_SOURCE_IDS:
        raise ValueError(f"Unknown source_id: {source_id}")

    manifest_rows = read_manifest(source_id)
    added = 0

    raw_dir = source_raw_dir(source_id)
    if raw_dir.exists():
        for path in sorted(raw_dir.rglob("*")):
            if not path.is_file():
                continue
            row = manifest_row_from_existing_file(source_id, path)
            if _contains_equivalent_row(manifest_rows, row):
                continue
            manifest_rows.append(row)
            added += 1

    catalog_path = global_catalog_path or (RAG_DATA_ROOT / "corpus_catalog.csv")
    if catalog_path.exists():
        with open(catalog_path, "r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                if (row.get("source_id") or "").strip() != source_id:
                    continue
                manifest_row = manifest_row_from_catalog_row(row)
                if _contains_equivalent_row(manifest_rows, manifest_row):
                    continue
                manifest_rows.append(manifest_row)
                added += 1

    manifest_rows.sort(key=lambda row: ((row.get("relative_path") or ""), (row.get("item_id") or "")))
    write_manifest(source_id, manifest_rows)
    return {
        "source_id": source_id,
        "rows": len(manifest_rows),
        "added": added,
    }


def build_corpus_catalog(
    *,
    source_ids: Iterable[str] | None = None,
    output_path: Path | None = None,
) -> dict[str, int | str]:
    source_ids = list(source_ids or KNOWN_SOURCE_IDS)
    out_path = output_path or (RAG_DATA_ROOT / "corpus_catalog.csv")
    catalog_rows: list[dict[str, str]] = []
    seen_paths: set[str] = set()
    existing_catalog_rows = _load_existing_catalog_rows(out_path)

    for source_id in source_ids:
        manifest_rows = read_manifest(source_id)
        if not manifest_rows:
            for legacy_row in existing_catalog_rows.get(source_id, []):
                rel = (legacy_row.get("relative_path") or "").strip()
                if not rel or rel in seen_paths:
                    continue
                seen_paths.add(rel)
                catalog_rows.append({field: legacy_row.get(field, "") for field in CATALOG_FIELDS})
            continue

        for row in manifest_rows:
            rel = (row.get("relative_path") or "").strip()
            if not rel or rel in seen_paths:
                continue
            asset_path = RAG_DATA_ROOT / rel
            if not asset_path.exists():
                continue
            seen_paths.add(rel)
            catalog_rows.append(manifest_row_to_catalog_row(row, asset_path))

    catalog_rows.sort(key=lambda row: (row["source_id"], row["relative_path"], row["file_name"]))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CATALOG_FIELDS)
        writer.writeheader()
        for row in catalog_rows:
            writer.writerow(row)

    return {"rows": len(catalog_rows), "output_path": str(out_path)}


def manifest_row_to_catalog_row(row: dict[str, str], asset_path: Path) -> dict[str, str]:
    size_kb = ""
    if asset_path.exists():
        size_kb = str(int(round(asset_path.stat().st_size / 1024)))
    return {
        "source_id": row.get("source_id", ""),
        "institution_or_journal": SOURCE_DISPLAY_NAMES.get(row.get("source_id", ""), row.get("source_id", "")),
        "file_name": Path(row.get("relative_path", "")).name,
        "relative_path": row.get("relative_path", ""),
        "extension": row.get("extension", ""),
        "file_size_kb": size_kb,
        "title": row.get("title_hint", ""),
        "item_type": row.get("item_type", ""),
        "item_url": row.get("item_url", ""),
        "file_url": row.get("file_url", ""),
        "sha256": row.get("sha256", ""),
    }


def _normalize_row(row: dict[str, object]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for field in MANIFEST_FIELDS:
        value = row.get(field, "")
        normalized[field] = "" if value is None else str(value)
    return normalized


def _row_signature(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        make_resume_key(row.get("file_url", ""), row.get("item_url", "")),
        (row.get("relative_path") or "").strip(),
        (row.get("sha256") or "").strip(),
    )


def _contains_equivalent_row(rows: list[dict[str, str]], candidate: dict[str, str]) -> bool:
    candidate_key = make_resume_key(candidate.get("file_url", ""), candidate.get("item_url", ""))
    candidate_rel = (candidate.get("relative_path") or "").strip()
    candidate_sha = (candidate.get("sha256") or "").strip()
    for row in rows:
        row_key = make_resume_key(row.get("file_url", ""), row.get("item_url", ""))
        row_rel = (row.get("relative_path") or "").strip()
        row_sha = (row.get("sha256") or "").strip()
        if candidate_key and row_key and candidate_key == row_key:
            return True
        if candidate_rel and row_rel and candidate_rel == row_rel and (bool(candidate_key) == bool(row_key)):
            return True
        if candidate_sha and row_sha and candidate_sha == row_sha and candidate_rel and row_rel and candidate_rel == row_rel:
            return True
    return False


def _load_existing_catalog_rows(path: Path) -> dict[str, list[dict[str, str]]]:
    if not path.exists():
        return {}
    grouped: dict[str, list[dict[str, str]]] = {}
    with open(path, "r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            source_id = (row.get("source_id") or "").strip()
            if not source_id:
                continue
            grouped.setdefault(source_id, []).append(row)
    return grouped
