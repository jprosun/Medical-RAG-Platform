from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))

from app.document_schema import DocumentRecord
from pipelines.crawl.source_registry import SOURCE_REGISTRY
from services.utils.data_lineage import build_file_lineage, make_run_id
from services.utils.data_paths import preferred_processed_dir, source_records_path


_RE_FRONTMATTER = re.compile(r"^---\s*\r?\n(.*?)\r?\n---\s*\r?\n", re.DOTALL)
_MOJIBAKE_MARKERS = ("Ã", "Â", "â€", "â€“", "â€”", "â€™", "â€œ", "â€")
_GENERIC_NCI_TITLES = {"health professional", "patient", "español", "espanol"}
_RE_NCI_VERSION_SUFFIX = re.compile(
    r"\s*[–—-]\s*(Health Professional Version|Patient Version|Versión para profesionales de salud|Versión para pacientes)\s*$",
    re.IGNORECASE,
)


SOURCE_DEFAULTS: dict[str, dict[str, object]] = {
    "nhs_health_a_z": {
        "source_name": "NHS Health A-Z",
        "doc_type": "patient_education",
        "audience": "patient",
        "language": "en",
        "trust_tier": 3,
        "specialty": "general",
    },
    "msd_manual_consumer": {
        "source_name": "MSD Manual Consumer",
        "doc_type": "patient_education",
        "audience": "patient",
        "language": "en",
        "trust_tier": 3,
        "specialty": "general",
    },
    "msd_manual_professional": {
        "source_name": "MSD Manual Professional",
        "doc_type": "reference",
        "audience": "clinician",
        "language": "en",
        "trust_tier": 2,
        "specialty": "general",
    },
    "uspstf_recommendations": {
        "source_name": "USPSTF Recommendations",
        "doc_type": "guideline",
        "audience": "clinician",
        "language": "en",
        "trust_tier": 1,
        "specialty": "general",
    },
    "nccih_health": {
        "source_name": "NCCIH Health Topics",
        "doc_type": "patient_education",
        "audience": "patient",
        "language": "en",
        "trust_tier": 3,
        "specialty": "general",
    },
    "nci_pdq": {
        "source_name": "NCI PDQ",
        "doc_type": "reference",
        "audience": "clinician",
        "language": "en",
        "trust_tier": 1,
        "specialty": "oncology",
    },
    "mayo_diseases_conditions": {
        "source_name": "Mayo Clinic Diseases and Conditions",
        "doc_type": "patient_education",
        "audience": "patient",
        "language": "en",
        "trust_tier": 3,
        "specialty": "general",
    },
    "cdc_health_topics": {
        "source_name": "CDC Health Topics",
        "doc_type": "patient_education",
        "audience": "patient",
        "language": "en",
        "trust_tier": 1,
        "specialty": "general",
    },
}


def _parse_frontmatter(raw_text: str) -> tuple[dict[str, str], str]:
    match = _RE_FRONTMATTER.match(raw_text)
    if not match:
        return {}, raw_text

    meta: dict[str, str] = {}
    for line in match.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, _, value = line.partition(":")
        meta[key.strip()] = value.strip().strip("\"'")
    return meta, raw_text[match.end() :]


def _fix_mojibake(text: str) -> str:
    if not text or not any(marker in text for marker in _MOJIBAKE_MARKERS):
        return text
    try:
        repaired = text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    return repaired


def _first_nonempty_line(text: str) -> str:
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def _clean_body(text: str, title: str = "") -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if not text:
        return ""
    if title:
        lines = [line.strip() for line in text.splitlines()]
        while lines and not lines[0]:
            lines.pop(0)
        if lines and lines[0].strip().lower() == title.strip().lower():
            lines = lines[1:]
        text = "\n".join(lines).strip()
    return text


def _stable_doc_id(source_id: str, path: Path) -> str:
    raw = f"{source_id}:{path.stem}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()[:16]


def _source_defaults(source_id: str) -> dict[str, object]:
    defaults = dict(SOURCE_DEFAULTS.get(source_id, {}))
    if not defaults:
        registry = SOURCE_REGISTRY.get(source_id)
        defaults = {
            "source_name": registry.display_name if registry else source_id,
            "doc_type": "reference",
            "audience": "patient",
            "language": "en",
            "trust_tier": 3,
            "specialty": "general",
        }
    return defaults


def _frontmatter_overrides(meta: dict[str, str], defaults: dict[str, object]) -> dict[str, object]:
    merged = dict(defaults)
    for key in (
        "source_name",
        "doc_type",
        "audience",
        "language",
        "specialty",
        "section_title",
        "canonical_title",
        "published_at",
        "updated_at",
    ):
        value = (meta.get(key) or "").strip()
        if value:
            merged[key] = value

    trust_value = (meta.get("trust_tier") or "").strip()
    if trust_value.isdigit():
        merged["trust_tier"] = int(trust_value)
    return merged


def _apply_nci_pdq_overrides(
    *,
    meta: dict[str, str],
    body: str,
    defaults: dict[str, object],
    path: Path,
) -> tuple[str, str, dict[str, object], str]:
    first_line = _fix_mojibake(_first_nonempty_line(body))
    raw_title = _fix_mojibake((meta.get("title") or path.stem).strip())
    title = first_line if first_line and raw_title.lower() in _GENERIC_NCI_TITLES else raw_title
    title = title or raw_title or path.stem

    lower_title = title.lower()
    if "health professional version" in lower_title or "profesionales de salud" in lower_title:
        defaults["audience"] = "clinician"
    elif "patient version" in lower_title or "para pacientes" in lower_title:
        defaults["audience"] = "patient"

    if "versión para" in lower_title or "resumen de información" in _fix_mojibake(body[:400]).lower():
        defaults["language"] = "es"
    elif "health professional version" in lower_title or "patient version" in lower_title:
        defaults["language"] = "en"

    canonical_title = _RE_NCI_VERSION_SUFFIX.sub("", title).strip(" -–—")
    canonical_title = canonical_title or title
    body = _fix_mojibake(body)
    return title, canonical_title, defaults, body


def process_file(path: Path, *, source_id: str, etl_run_id: str) -> DocumentRecord | None:
    raw_text = path.read_text(encoding="utf-8", errors="ignore")
    meta, body = _parse_frontmatter(raw_text)
    defaults = _source_defaults(source_id)
    defaults = _frontmatter_overrides(meta, defaults)
    title = _fix_mojibake((meta.get("title") or path.stem).strip())
    canonical_title = str(defaults.get("canonical_title") or title)
    if source_id == "nci_pdq":
        title, canonical_title, defaults, body = _apply_nci_pdq_overrides(
            meta=meta,
            body=body,
            defaults=defaults,
            path=path,
        )
    else:
        body = _fix_mojibake(body)
    body = _clean_body(body, title=title)
    if len(body) < 120:
        return None

    lineage = build_file_lineage(path, source_id=source_id, etl_run_id=etl_run_id)
    source_url = (meta.get("item_url") or meta.get("source_url") or "").strip()
    section_title = str(defaults.get("section_title") or "Full text")
    canonical_title = str(defaults.get("canonical_title") or canonical_title or title)
    doc = DocumentRecord(
        doc_id=_stable_doc_id(source_id, path),
        title=title,
        body=body,
        source_name=str(defaults["source_name"]),
        section_title=section_title,
        source_url=source_url,
        canonical_title=canonical_title,
        doc_type=str(defaults["doc_type"]),
        specialty=str(defaults["specialty"]),
        audience=str(defaults["audience"]),
        language=str(defaults["language"]),
        trust_tier=int(defaults["trust_tier"]),
        heading_path=f"{canonical_title} > {section_title}",
        published_at=str(defaults.get("published_at") or ""),
        updated_at=str(defaults.get("updated_at") or ""),
        **lineage,
    )
    return doc


def process_directory(
    *,
    source_id: str,
    source_dir: Path,
    output_path: Path,
    max_files: int | None = None,
    dry_run: bool = False,
) -> dict[str, object]:
    etl_run_id = os.getenv("ETL_RUN_ID") or make_run_id("processed_frontmatter", source_id)
    files = sorted(source_dir.glob("*.txt"))
    if max_files:
        files = files[: max_files]

    records: list[DocumentRecord] = []
    skipped = 0
    for path in files:
        rec = process_file(path, source_id=source_id, etl_run_id=etl_run_id)
        if rec is None:
            skipped += 1
            continue
        records.append(rec)

    if not dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(rec.to_jsonl_line() + "\n")

    return {
        "source_id": source_id,
        "source_dir": str(source_dir),
        "output_path": str(output_path),
        "records": len(records),
        "skipped": skipped,
        "etl_run_id": etl_run_id,
        "dry_run": dry_run,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert processed text files with frontmatter to DocumentRecord JSONL.")
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--source-dir")
    parser.add_argument("--output")
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    source_dir = Path(args.source_dir) if args.source_dir else preferred_processed_dir(args.source_id)
    output_path = Path(args.output) if args.output else source_records_path(args.source_id)
    report = process_directory(
        source_id=args.source_id,
        source_dir=source_dir,
        output_path=output_path,
        max_files=args.max_files or None,
        dry_run=args.dry_run,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
