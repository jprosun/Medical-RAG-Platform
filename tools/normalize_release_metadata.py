from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from services.utils.data_paths import dataset_qa_dir, dataset_records_path
from pipelines.crawl.source_registry import SOURCE_REGISTRY


_MOJIBAKE_MARKERS = ("Ã", "â€", "â€“", "â€”", "â€™", "â€œ", "â€\u009d", "Â®", "Â°", "Â ")
_USPSTF_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)+$")
_MOJIBAKE_RE = re.compile(r"(?:Ã.)|(?:Â[®° ])|(?:â€)|(?:â€“)|(?:â€”)|(?:â€™)|(?:â€œ)|(?:â€\u009d)")
_RESIDUAL_REPLACEMENTS = {
    "Â®": "®",
    "Â°": "°",
    "Â ": " ",
    "â€“": "–",
    "â€”": "—",
    "â€™": "’",
    "â€œ": "“",
    "â€\u009d": "”",
}


def _fix_mojibake(text: str) -> str:
    if not text or not any(marker in text for marker in _MOJIBAKE_MARKERS):
        return text
    try:
        repaired = text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    return repaired


def _clean_scalar(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    text = _fix_mojibake(text)
    for old, new in _RESIDUAL_REPLACEMENTS.items():
        text = text.replace(old, new)
    return text


def _first_nonempty_line(text: str) -> str:
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def _looks_like_mojibake(text: str, *, language: str = "", source_id: str = "") -> bool:
    if not text:
        return False
    lowered_lang = (language or "").strip().lower()
    if lowered_lang == "vi":
        return any(token in text for token in ("â€", "â€“", "â€”", "â€™", "â€œ", "â€\u009d", "Â®", "Â°"))
    return bool(_MOJIBAKE_RE.search(text))


def _derive_source_name(source_id: str, current: str) -> str:
    if current:
        return current
    registry = SOURCE_REGISTRY.get(source_id)
    return registry.display_name if registry else source_id


def _derive_source_url(record: dict[str, Any]) -> str:
    source_url = _clean_scalar(record.get("source_url"))
    if source_url:
        return source_url

    source_id = _clean_scalar(record.get("source_id"))
    if source_id != "uspstf_recommendations":
        return source_url

    processed_path = _clean_scalar(record.get("processed_path") or record.get("source_file"))
    if not processed_path:
        return source_url
    stem = Path(processed_path).stem
    if not stem:
        return source_url
    return f"https://www.uspreventiveservicestaskforce.org/uspstf/recommendation/{stem}"


def normalize_record(record: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    rec = dict(record)
    changes: list[str] = []

    source_id = _clean_scalar(rec.get("source_id"))
    if source_id != rec.get("source_id", ""):
        rec["source_id"] = source_id
        changes.append("source_id")

    for key in (
        "title",
        "body",
        "source_name",
        "section_title",
        "source_url",
        "canonical_title",
        "heading_path",
        "published_at",
        "updated_at",
    ):
        new_value = _clean_scalar(rec.get(key))
        if new_value != rec.get(key, ""):
            rec[key] = new_value
            changes.append(key)

    if isinstance(rec.get("tags"), list):
        deduped_tags: list[str] = []
        seen_tags: set[str] = set()
        for tag in rec["tags"]:
            cleaned = _clean_scalar(tag)
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen_tags:
                continue
            seen_tags.add(lowered)
            deduped_tags.append(cleaned)
        if deduped_tags != rec.get("tags"):
            rec["tags"] = deduped_tags
            changes.append("tags")

    body = _clean_scalar(rec.get("body"))
    title = _clean_scalar(rec.get("title"))
    original_section_title = _clean_scalar(rec.get("section_title"))
    section_title = original_section_title or "Full text"
    canonical_title = _clean_scalar(rec.get("canonical_title"))

    if rec.get("source_id") == "uspstf_recommendations" and _USPSTF_SLUG_RE.match(title):
        first_line = _first_nonempty_line(body)
        if first_line:
            title = first_line
            canonical_title = first_line
            changes.append("uspstf_title_from_body")

    if not canonical_title:
        canonical_title = title
        changes.append("canonical_title")
    if section_title != original_section_title:
        changes.append("section_title")

    source_name = _derive_source_name(source_id, _clean_scalar(rec.get("source_name")))
    if source_name != rec.get("source_name"):
        rec["source_name"] = source_name
        changes.append("source_name")

    source_url = _derive_source_url(rec)
    if source_url != rec.get("source_url", ""):
        rec["source_url"] = source_url
        changes.append("source_url")

    heading_path = f"{canonical_title} > {section_title}" if canonical_title and section_title else _clean_scalar(rec.get("heading_path"))
    if heading_path != _clean_scalar(rec.get("heading_path")):
        changes.append("heading_path")

    rec["title"] = title
    rec["body"] = body
    rec["section_title"] = section_title
    rec["canonical_title"] = canonical_title
    rec["heading_path"] = heading_path

    if isinstance(rec.get("quality_flags"), list):
        flags = [_clean_scalar(flag) for flag in rec["quality_flags"] if _clean_scalar(flag)]
    else:
        flags = []

    if len(body) < 120 and "release_body_too_short" not in flags:
        flags.append("release_body_too_short")
    if not rec.get("source_url") and "release_missing_source_url" not in flags:
        flags.append("release_missing_source_url")

    if flags != rec.get("quality_flags"):
        rec["quality_flags"] = flags
        changes.append("quality_flags")

    quality_status = _clean_scalar(rec.get("quality_status"))
    if "release_body_too_short" in flags and quality_status in {"", "go"}:
        rec["quality_status"] = "review"
        changes.append("quality_status")

    return rec, changes


def _audit_record(record: dict[str, Any], counters: Counter[str]) -> None:
    title = _clean_scalar(record.get("title"))
    body = _clean_scalar(record.get("body"))
    canonical = _clean_scalar(record.get("canonical_title"))
    section = _clean_scalar(record.get("section_title"))
    heading = _clean_scalar(record.get("heading_path"))
    source_url = _clean_scalar(record.get("source_url"))
    language = _clean_scalar(record.get("language"))
    source_id = _clean_scalar(record.get("source_id"))

    if not canonical:
        counters["missing_canonical_title"] += 1
    if not section:
        counters["missing_section_title"] += 1
    if not source_url:
        counters["missing_source_url"] += 1
    if not heading or ">" not in heading or (canonical and section and heading != f"{canonical} > {section}"):
        counters["heading_mismatch"] += 1
    if _looks_like_mojibake(title, language=language, source_id=source_id) or _looks_like_mojibake(body[:500], language=language, source_id=source_id):
        counters["mojibake"] += 1
    if len(body) < 120:
        counters["body_too_short"] += 1


def normalize_dataset(dataset_id: str, *, dry_run: bool = False) -> dict[str, Any]:
    path = dataset_records_path(dataset_id)
    if not path.exists():
        raise FileNotFoundError(path)

    normalized_rows: list[str] = []
    changed_records = 0
    change_counter: Counter[str] = Counter()
    residual: Counter[str] = Counter()
    total = 0

    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            if not raw.strip():
                continue
            total += 1
            record = json.loads(raw)
            normalized, changes = normalize_record(record)
            if changes:
                changed_records += 1
                change_counter.update(changes)
            _audit_record(normalized, residual)
            normalized_rows.append(json.dumps(normalized, ensure_ascii=False))

    if not dry_run:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(normalized_rows) + "\n")

    report = {
        "dataset_id": dataset_id,
        "records": total,
        "changed_records": changed_records,
        "change_counts": dict(change_counter),
        "residual_issues": dict(residual),
        "dry_run": dry_run,
        "records_path": str(path),
    }

    if not dry_run:
        qa_dir = dataset_qa_dir(dataset_id)
        qa_dir.mkdir(parents=True, exist_ok=True)
        report_path = qa_dir / "metadata_normalization.json"
        with open(report_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)
        report["report_path"] = str(report_path)

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize dataset release metadata in-place.")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    report = normalize_dataset(args.dataset_id, dry_run=args.dry_run)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
