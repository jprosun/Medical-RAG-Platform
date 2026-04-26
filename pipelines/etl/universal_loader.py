"""
Universal Data Loader — Benchmark RAG Pipeline
================================================

Converts various input formats (JSONL, CSV, TXT, Markdown) into
standardized DocumentRecord JSONL for the RAG ingest pipeline.

Supported formats:
  - JSONL: Each line is a JSON object mapped to DocumentRecord
  - CSV:   Columns mapped to DocumentRecord fields (title, body required)
  - TXT:   Each file = 1 record (filename → title, content → body)
  - MD:    Markdown split by headings → 1 record per section

Usage:
    cd services/qdrant-ingestor

    # From CSV
    python -m pipelines.etl.universal_loader --input data.csv --source-name "Custom DB" --output out.jsonl

    # From TXT directory
    python -m pipelines.etl.universal_loader --input ./txt_files/ --source-name "Notes" --output out.jsonl

    # From Markdown
    python -m pipelines.etl.universal_loader --input guide.md --source-name "Manual" --output out.jsonl

    # From JSONL (re-validate + normalize)
    
    python -m pipelines.etl.universal_loader --input raw.jsonl --output out.jsonl
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(INGESTOR_ROOT))
from app.document_schema import DocumentRecord, VALID_DOC_TYPES, VALID_AUDIENCES


# ── Helpers ─────────────────────────────────────────────────────────


def _generate_doc_id(source_name: str, title: str, index: int = 0) -> str:
    """Generate a stable doc_id from source + title."""
    raw = f"{source_name}::{title}::{index}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _detect_format(path: str) -> str:
    """Detect input format from file extension or directory."""
    p = Path(path)
    if p.is_dir():
        # Check what's inside
        exts = {f.suffix.lower() for f in p.iterdir() if f.is_file()}
        if ".txt" in exts:
            return "txt_dir"
        if ".md" in exts:
            return "md_dir"
        if ".jsonl" in exts:
            return "jsonl"
        return "txt_dir"  # default for directories

    ext = p.suffix.lower()
    if ext == ".jsonl":
        return "jsonl"
    elif ext == ".csv":
        return "csv"
    elif ext == ".txt":
        return "txt"
    elif ext in (".md", ".markdown"):
        return "md"
    else:
        raise ValueError(f"Unsupported file format: {ext} (supported: .jsonl, .csv, .txt, .md)")


# ── Format-specific loaders ─────────────────────────────────────────


def load_jsonl(path: str, source_name: str = "") -> List[DocumentRecord]:
    """Load JSONL, map to DocumentRecord, optionally override source_name."""
    records = []
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"  [WARN] Line {lineno}: JSON parse error — {e}")
                continue

            if source_name:
                obj.setdefault("source_name", source_name)
            if not obj.get("doc_id"):
                obj["doc_id"] = _generate_doc_id(
                    obj.get("source_name", "unknown"),
                    obj.get("title", f"record_{lineno}"),
                    lineno,
                )
            try:
                rec = DocumentRecord.from_dict(obj)
                records.append(rec)
            except Exception as e:
                print(f"  [WARN] Line {lineno}: Cannot build record — {e}")
    return records


def load_csv(path: str, source_name: str = "CSV Import") -> List[DocumentRecord]:
    """
    Load CSV. Required columns: title, body.
    Optional columns mapped directly to DocumentRecord fields.
    """
    records = []
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            print(f"  [ERROR] CSV has no headers")
            return records

        # Check required columns
        fields_lower = {f.lower().strip(): f for f in reader.fieldnames}
        if "title" not in fields_lower and "body" not in fields_lower:
            print(f"  [ERROR] CSV must have 'title' and 'body' columns")
            print(f"  Found columns: {list(reader.fieldnames)}")
            return records

        for i, row in enumerate(reader, start=1):
            # Normalize keys to lowercase
            norm_row = {k.lower().strip(): v.strip() if v else "" for k, v in row.items()}

            title = norm_row.get("title", "")
            body = norm_row.get("body", "") or norm_row.get("content", "") or norm_row.get("text", "")

            if not body:
                continue  # skip empty records

            if not title:
                title = body[:80].replace("\n", " ").strip()

            obj = {
                "doc_id": norm_row.get("doc_id", "") or _generate_doc_id(source_name, title, i),
                "title": title,
                "body": body,
                "source_name": norm_row.get("source_name", "") or source_name,
                "section_title": norm_row.get("section_title", ""),
                "source_url": norm_row.get("source_url", "") or norm_row.get("url", ""),
                "doc_type": norm_row.get("doc_type", "reference"),
                "specialty": norm_row.get("specialty", "general"),
                "audience": norm_row.get("audience", "patient"),
                "language": norm_row.get("language", "en"),
                "trust_tier": int(norm_row.get("trust_tier", "3")),
                "tags": [t.strip() for t in norm_row.get("tags", "").split(",") if t.strip()],
            }

            # Validate enums
            if obj["doc_type"] not in VALID_DOC_TYPES:
                obj["doc_type"] = "reference"
            if obj["audience"] not in VALID_AUDIENCES:
                obj["audience"] = "patient"

            records.append(DocumentRecord.from_dict(obj))

    return records


def load_txt(path: str, source_name: str = "TXT Import") -> List[DocumentRecord]:
    """Load a single TXT file as one record."""
    p = Path(path)
    body = p.read_text(encoding="utf-8").strip()
    if not body:
        return []

    title = p.stem.replace("_", " ").replace("-", " ").title()
    doc_id = _generate_doc_id(source_name, title, 0)

    return [DocumentRecord(
        doc_id=doc_id,
        title=title,
        body=body,
        source_name=source_name,
    )]


def load_txt_dir(path: str, source_name: str = "TXT Import") -> List[DocumentRecord]:
    """Load all .txt files from a directory."""
    records = []
    txt_files = sorted(Path(path).glob("*.txt"))
    for f in txt_files:
        records.extend(load_txt(str(f), source_name))
    return records


def _split_markdown_by_headings(text: str) -> List[Dict[str, str]]:
    """Split markdown into sections by headings."""
    sections = []
    current_title = ""
    current_body_lines = []

    for line in text.split("\n"):
        heading_match = re.match(r"^(#{1,3})\s+(.+)$", line)
        if heading_match:
            # Save previous section
            body = "\n".join(current_body_lines).strip()
            if current_title and body:
                sections.append({"title": current_title, "body": body})
            current_title = heading_match.group(2).strip()
            current_body_lines = []
        else:
            current_body_lines.append(line)

    # Last section
    body = "\n".join(current_body_lines).strip()
    if current_title and body:
        sections.append({"title": current_title, "body": body})
    elif not current_title and body:
        # No headings at all — treat entire file as one section
        sections.append({"title": Path("untitled").stem, "body": body})

    return sections


def load_markdown(path: str, source_name: str = "Markdown Import") -> List[DocumentRecord]:
    """Load a Markdown file, split by headings → 1 record per section."""
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    sections = _split_markdown_by_headings(text)

    records = []
    doc_title = p.stem.replace("_", " ").replace("-", " ").title()

    for i, sec in enumerate(sections):
        doc_id = _generate_doc_id(source_name, sec["title"], i)
        records.append(DocumentRecord(
            doc_id=doc_id,
            title=sec["title"],
            body=sec["body"],
            source_name=source_name,
            section_title=sec["title"],
            heading_path=f"{doc_title} > {sec['title']}",
        ))

    return records


def load_md_dir(path: str, source_name: str = "Markdown Import") -> List[DocumentRecord]:
    """Load all .md files from a directory."""
    records = []
    md_files = sorted(Path(path).glob("*.md"))
    for f in md_files:
        records.extend(load_markdown(str(f), source_name))
    return records


# ── Main loader dispatch ────────────────────────────────────────────


def universal_load(
    input_path: str,
    source_name: str = "",
    fmt: Optional[str] = None,
) -> List[DocumentRecord]:
    """
    Load data from any supported format and return DocumentRecords.

    Args:
        input_path: Path to file or directory
        source_name: Override source_name for all records
        fmt: Force format (auto-detected if None)

    Returns:
        List of DocumentRecord objects
    """
    if fmt is None:
        fmt = _detect_format(input_path)

    print(f"  [LOADER] Format: {fmt}")
    print(f"  [LOADER] Input:  {input_path}")
    print(f"  [LOADER] Source: {source_name or '(from data)'}")

    loader_map = {
        "jsonl": load_jsonl,
        "csv": load_csv,
        "txt": load_txt,
        "txt_dir": load_txt_dir,
        "md": load_markdown,
        "md_dir": load_md_dir,
    }

    loader = loader_map.get(fmt)
    if not loader:
        raise ValueError(f"Unknown format: {fmt}")

    if source_name:
        records = loader(input_path, source_name=source_name)
    else:
        records = loader(input_path)

    # Validate all records
    valid = []
    errors = 0
    for rec in records:
        errs = rec.validate()
        if errs:
            errors += 1
            if errors <= 5:
                print(f"  [WARN] Record '{rec.doc_id}': {errs}")
        else:
            valid.append(rec)

    print(f"  [LOADER] Loaded: {len(records)} records, {len(valid)} valid, {errors} errors")
    return valid


def save_jsonl(records: List[DocumentRecord], output_path: str):
    """Save records to JSONL file."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(rec.to_jsonl_line() + "\n")
    print(f"  [SAVED] {len(records)} records → {output_path}")


# ── CLI ─────────────────────────────────────────────────────────────


def main():
    ap = argparse.ArgumentParser(
        description="Universal Data Loader — Convert any format to DocumentRecord JSONL"
    )
    ap.add_argument("--input", "-i", required=True, help="Input file or directory")
    ap.add_argument("--output", "-o", required=True, help="Output JSONL path")
    ap.add_argument("--source-name", "-s", default="", help="Override source_name for all records")
    ap.add_argument("--format", "-f", default=None,
                     choices=["jsonl", "csv", "txt", "txt_dir", "md", "md_dir"],
                     help="Force input format (auto-detected if not specified)")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        print(f"  [ERROR] Input not found: {args.input}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Universal Data Loader")
    print(f"{'='*60}")

    records = universal_load(args.input, source_name=args.source_name, fmt=args.format)

    if not records:
        print(f"  [ERROR] No valid records loaded.")
        sys.exit(1)

    save_jsonl(records, args.output)

    # Quick stats
    sources = {}
    for r in records:
        sources[r.source_name] = sources.get(r.source_name, 0) + 1
    print(f"\n  Source distribution:")
    for src, cnt in sorted(sources.items()):
        print(f"    {src}: {cnt} records")

    body_lens = [len(r.body) for r in records]
    print(f"\n  Body length: min={min(body_lens)}, avg={sum(body_lens)//len(body_lens)}, max={max(body_lens)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
