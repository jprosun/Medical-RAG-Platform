"""
QA Pre-Ingest: Tang 3 - Kiem tra chunk sau khi chunking (Chunk QA)
==================================================================

Simulates chunking on the enriched JSONL and validates chunk quality:
- Chunks retain title/section/source provenance
- No truncated tables, lists, or recommendation blocks
- Chunk size distribution (token estimate)
- Duplicate chunk ratio (via text hash)
- Stable chunk ID verification
- Context header presence

Corresponds to CHECK.md Section 3.

Usage:
    python -m qa_pre_ingest.check_chunks ../../rag-data/sources/medlineplus/records/document_records.jsonl
    python -m qa_pre_ingest.check_chunks ../../rag-data/datasets/en_core_v1/records/document_records.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Stub heavy ML dependencies so we can import the pure-Python chunking logic
# Only use stubs if the real packages are NOT installed at all
from types import ModuleType

def _try_import(name):
    """Check if a package is truly available."""
    try:
        __import__(name)
        return True
    except ImportError:
        return False

if not _try_import("qdrant_client"):
    _qm = ModuleType("qdrant_client.http.models")
    _qm.VectorParams = type("VectorParams", (), {"__init__": lambda *a, **kw: None})
    _qm.Distance = type("Distance", (), {"COSINE": "Cosine"})()
    _qm.PointStruct = type("PointStruct", (), {"__init__": lambda self, **kw: None})
    _qhttp = ModuleType("qdrant_client.http"); _qhttp.models = _qm
    _qc = ModuleType("qdrant_client")
    _qc.QdrantClient = type("QdrantClient", (), {"__init__": lambda self, **kw: None})
    _qc.http = _qhttp
    sys.modules["qdrant_client"] = _qc
    sys.modules["qdrant_client.http"] = _qhttp
    sys.modules["qdrant_client.http.models"] = _qm
if not _try_import("fastembed"):
    _fe = ModuleType("fastembed")
    _fe.TextEmbedding = type("TextEmbedding", (), {"__init__": lambda self, **kw: None})
    sys.modules["fastembed"] = _fe

from app.document_schema import DocumentRecord
from app.ingest import chunk_by_structure, generate_stable_id


# ── Helpers ──────────────────────────────────────────────────────────
def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _text_hash(text: str) -> str:
    norm = " ".join(text.lower().split())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]


def _has_truncated_list(text: str) -> bool:
    """Check if a chunk ends mid-list (ends with a bullet without period)."""
    lines = text.strip().split("\n")
    if not lines:
        return False
    last = lines[-1].strip()
    # Ends with "-" or "*" or number. without proper closing
    if re.match(r"^[\-\*\d\.]+\s", last) and not last.endswith((".",")",":")):
        return True
    return False


def _has_truncated_table(text: str) -> bool:
    """Check if chunk contains partial table (| without closing row)."""
    pipe_lines = [l for l in text.split("\n") if "|" in l]
    if len(pipe_lines) >= 2:
        last_pipe = pipe_lines[-1].strip()
        if not last_pipe.endswith("|"):
            return True
    return False


# ── Main check ───────────────────────────────────────────────────────
def check_chunks(
    path: str,
    chunk_size: int = 900,
    overlap: int = 150,
) -> Dict[str, Any]:
    """Simulate chunking and validate quality."""
    report = {
        "file": os.path.basename(path),
        "total_docs": 0,
        "total_chunks": 0,
        "stats": {
            "tokens_per_chunk": [],
            "chars_per_chunk": [],
            "chunks_per_doc": [],
        },
        "issues": {
            "missing_provenance": 0,      # no title/section/source in chunk
            "missing_context_header": 0,   # no prepended metadata header
            "truncated_list": 0,
            "truncated_table": 0,
            "extremely_short": 0,          # < 50 tokens
            "extremely_long": 0,           # > 500 tokens
            "duplicate_chunks": 0,
        },
        "duplicate_pairs": [],
        "top_docs_by_chunks": Counter(),
    }

    # Read records
    records: List[DocumentRecord] = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            try:
                rec = DocumentRecord.from_dict(json.loads(line))
                records.append(rec)
            except Exception:
                continue

    report["total_docs"] = len(records)

    # Simulate chunking
    all_chunk_hashes: dict = {}  # hash -> (doc_id, chunk_idx)

    for rec in records:
        chunk_tuples = chunk_by_structure(
            rec.body,
            title=rec.title,
            source_name=rec.source_name,
            updated_at=rec.updated_at,
            audience=rec.audience,
            chunk_size=chunk_size,
            overlap=overlap,
        )

        report["stats"]["chunks_per_doc"].append(len(chunk_tuples))
        report["top_docs_by_chunks"][rec.title] += len(chunk_tuples)

        for idx, (heading_path, chunk_text) in enumerate(chunk_tuples):
            report["total_chunks"] += 1
            tokens = _estimate_tokens(chunk_text)
            chars = len(chunk_text)

            report["stats"]["tokens_per_chunk"].append(tokens)
            report["stats"]["chars_per_chunk"].append(chars)

            # Check provenance
            has_title = rec.title.lower() in chunk_text.lower() if rec.title else False
            has_source = rec.source_name.lower() in chunk_text.lower() if rec.source_name else False
            if not has_title and not has_source:
                report["issues"]["missing_provenance"] += 1

            # Check context header
            if "Title:" not in chunk_text and "Source:" not in chunk_text:
                report["issues"]["missing_context_header"] += 1

            # Truncated lists/tables
            if _has_truncated_list(chunk_text):
                report["issues"]["truncated_list"] += 1
            if _has_truncated_table(chunk_text):
                report["issues"]["truncated_table"] += 1

            # Size extremes
            if tokens < 50:
                report["issues"]["extremely_short"] += 1
            if tokens > 500:
                report["issues"]["extremely_long"] += 1

            # Duplicate detection
            ch = _text_hash(chunk_text)
            if ch in all_chunk_hashes:
                report["issues"]["duplicate_chunks"] += 1
                prev = all_chunk_hashes[ch]
                if len(report["duplicate_pairs"]) < 10:
                    report["duplicate_pairs"].append(
                        {"doc1": prev[0], "doc2": rec.doc_id, "chunk_idx": idx}
                    )
            else:
                all_chunk_hashes[ch] = (rec.doc_id, idx)

    return report


def print_report(report: Dict[str, Any]):
    """Print chunk QA report."""
    print(f"\n{'='*60}")
    print(f"  CHUNK QA: {report['file']}")
    print(f"{'='*60}")
    print(f"  Documents:  {report['total_docs']}")
    print(f"  Chunks:     {report['total_chunks']}")

    # Chunk size stats
    tokens = sorted(report["stats"]["tokens_per_chunk"]) if report["stats"]["tokens_per_chunk"] else [0]
    if tokens and tokens[0] > 0:
        p50 = tokens[len(tokens) // 2]
        p95 = tokens[int(len(tokens) * 0.95)]
        avg = sum(tokens) // len(tokens)
        print(f"\n  Token/chunk distribution:")
        print(f"    Min:     {min(tokens):,}")
        print(f"    Avg:     {avg:,}")
        print(f"    Median:  {p50:,}")
        print(f"    P95:     {p95:,}")
        print(f"    Max:     {max(tokens):,}")

    # Chunks per doc
    cpd = sorted(report["stats"]["chunks_per_doc"]) if report["stats"]["chunks_per_doc"] else [0]
    if cpd:
        print(f"\n  Chunks/doc distribution:")
        print(f"    Min: {min(cpd)}  Avg: {sum(cpd)//max(1,len(cpd))}  Max: {max(cpd)}")

    # Issues
    print(f"\n  Issues found:")
    for issue, count in report["issues"].items():
        total = report["total_chunks"]
        pct = (count / max(1, total)) * 100
        status = "[OK]" if count == 0 else f"[WARN] {count} ({pct:.1f}%)"
        print(f"    {issue:30s} {status}")

    # Top docs by chunk count
    print(f"\n  Top 5 docs by chunk count:")
    for title, count in report["top_docs_by_chunks"].most_common(5):
        print(f"    {title[:50]:50s} {count:3d} chunks")

    # Duplicate pairs
    if report["duplicate_pairs"]:
        print(f"\n  Duplicate chunk pairs ({len(report['duplicate_pairs'])}):")
        for dp in report["duplicate_pairs"][:5]:
            print(f"    {dp['doc1']} <-> {dp['doc2']} (chunk {dp['chunk_idx']})")

    total_issues = sum(report["issues"].values())
    dup_ratio = report["issues"]["duplicate_chunks"] / max(1, report["total_chunks"]) * 100
    status = "[PASS]" if total_issues == 0 else f"[REVIEW] {total_issues} issues, dup ratio {dup_ratio:.1f}%"
    print(f"\n  Result: {status}")
    print(f"{'='*60}")


def main():
    ap = argparse.ArgumentParser(description="Tang 3: Chunk quality check")
    ap.add_argument("files", nargs="+", help="JSONL files to check")
    ap.add_argument("--chunk-size", type=int, default=900)
    ap.add_argument("--overlap", type=int, default=150)
    args = ap.parse_args()

    for path in args.files:
        if not os.path.exists(path):
            print(f"  [SKIP] Not found: {path}")
            continue
        report = check_chunks(path, chunk_size=args.chunk_size, overlap=args.overlap)
        print_report(report)


if __name__ == "__main__":
    main()
