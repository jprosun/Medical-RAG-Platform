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


def process_file(path: Path, *, source_id: str, etl_run_id: str) -> DocumentRecord | None:
    raw_text = path.read_text(encoding="utf-8", errors="ignore")
    meta, body = _parse_frontmatter(raw_text)
    defaults = _source_defaults(source_id)
    title = (meta.get("title") or path.stem).strip()
    body = _clean_body(body, title=title)
    if len(body) < 120:
        return None

    lineage = build_file_lineage(path, source_id=source_id, etl_run_id=etl_run_id)
    source_url = (meta.get("item_url") or meta.get("source_url") or "").strip()
    doc = DocumentRecord(
        doc_id=_stable_doc_id(source_id, path),
        title=title,
        body=body,
        source_name=str(defaults["source_name"]),
        section_title="Full text",
        source_url=source_url,
        canonical_title=title,
        doc_type=str(defaults["doc_type"]),
        specialty=str(defaults["specialty"]),
        audience=str(defaults["audience"]),
        language=str(defaults["language"]),
        trust_tier=int(defaults["trust_tier"]),
        heading_path=f"{title} > Full text",
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
