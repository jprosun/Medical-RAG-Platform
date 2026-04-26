"""
QA Pre-Ingest: Master Runner
==============================

Runs all pre-ingest QA checks (Tang 1-3) and produces a Go/No-Go summary.
Corresponds to CHECK.md Section 6.

Usage:
    python -m qa_pre_ingest.run_all_checks ../../rag-data/sources/medlineplus/records/document_records.jsonl
    python -m qa_pre_ingest.run_all_checks ../../rag-data/datasets/en_core_v1/records/document_records.jsonl
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from qa_pre_ingest.check_schema import validate_file as schema_validate, print_report as schema_print
from qa_pre_ingest.check_content import check_content, print_report as content_print
from qa_pre_ingest.check_chunks import check_chunks, print_report as chunks_print


def run_all(path: str, chunk_size: int = 900, overlap: int = 150) -> dict:
    """Run all 3 QA layers on a JSONL file and return Go/No-Go verdict."""
    results = {"file": os.path.basename(path), "layers": {}}

    # Layer 1: Schema
    r1 = schema_validate(path)
    schema_print(r1)
    results["layers"]["schema"] = {
        "pass": r1["error_records"] == 0,
        "errors": r1["error_records"],
        "total": r1["total_records"],
    }

    # Layer 2: Content
    r2 = check_content(path)
    content_print(r2)
    total_issues_2 = sum(r2["issues"].values())
    # Content issues are warnings, not hard failures
    issue_rate = total_issues_2 / max(1, r2["total_records"]) * 100
    results["layers"]["content"] = {
        "pass": issue_rate < 35,  # < 35% issue rate (heuristic warnings)
        "issues": total_issues_2,
        "issue_rate": round(issue_rate, 1),
    }

    # Layer 3: Chunks
    r3 = check_chunks(path, chunk_size=chunk_size, overlap=overlap)
    chunks_print(r3)
    dup_ratio = r3["issues"]["duplicate_chunks"] / max(1, r3["total_chunks"]) * 100
    provenance_ratio = r3["issues"]["missing_provenance"] / max(1, r3["total_chunks"]) * 100
    results["layers"]["chunks"] = {
        "pass": dup_ratio < 15 and provenance_ratio < 10,
        "duplicate_ratio": round(dup_ratio, 1),
        "provenance_missing_ratio": round(provenance_ratio, 1),
        "total_chunks": r3["total_chunks"],
    }

    return results


def compute_composite_score(results: dict) -> dict:
    """
    Compute a composite quality score (0-100) from 3 QA layers.

    Weights:
      - Schema (40%):  100 - (error_rate * 100)
      - Content (35%): 100 - issue_rate
      - Chunks (25%):  100 - (dup_ratio + provenance_ratio)

    Returns:
        {
            "schema_score": float,
            "content_score": float,
            "chunk_score": float,
            "total_score": float,
            "verdict": "GO" | "WARN" | "NO-GO"
        }
    """
    layers = results.get("layers", {})

    # Schema score (40%)
    schema = layers.get("schema", {})
    schema_total = max(1, schema.get("total", 1))
    schema_errors = schema.get("errors", 0)
    schema_score = max(0, 100 - (schema_errors / schema_total * 100))

    # Content score (35%)
    content = layers.get("content", {})
    content_issue_rate = content.get("issue_rate", 0)
    content_score = max(0, 100 - content_issue_rate)

    # Chunk score (25%)
    chunks = layers.get("chunks", {})
    dup_ratio = chunks.get("duplicate_ratio", 0)
    prov_ratio = chunks.get("provenance_missing_ratio", 0)
    chunk_score = max(0, 100 - (dup_ratio + prov_ratio))

    # Weighted total
    total = (schema_score * 0.40) + (content_score * 0.35) + (chunk_score * 0.25)
    total = round(total, 1)

    # Verdict
    if total >= 80:
        verdict = "GO"
    elif total >= 60:
        verdict = "WARN"
    else:
        verdict = "NO-GO"

    score = {
        "schema_score": round(schema_score, 1),
        "content_score": round(content_score, 1),
        "chunk_score": round(chunk_score, 1),
        "total_score": total,
        "verdict": verdict,
    }

    return score


def main():
    ap = argparse.ArgumentParser(description="Run all pre-ingest QA checks")
    ap.add_argument("files", nargs="+", help="JSONL files to check")
    ap.add_argument("--chunk-size", type=int, default=900)
    ap.add_argument("--overlap", type=int, default=150)
    args = ap.parse_args()

    all_pass = True

    for path in args.files:
        if not os.path.exists(path):
            print(f"  [SKIP] Not found: {path}")
            continue

        results = run_all(path, args.chunk_size, args.overlap)

        # Composite score
        score = compute_composite_score(results)

        # Go/No-Go verdict
        print(f"\n{'#'*60}")
        print(f"  GO / NO-GO: {results['file']}")
        print(f"{'#'*60}")
        for layer_name, layer_result in results["layers"].items():
            status = "[PASS]" if layer_result["pass"] else "[FAIL]"
            print(f"  {layer_name:15s} {status}  {layer_result}")

        # Quality Score
        print(f"\n  Quality Score:")
        print(f"    Schema:  {score['schema_score']:5.1f} / 100  (weight 40%)")
        print(f"    Content: {score['content_score']:5.1f} / 100  (weight 35%)")
        print(f"    Chunks:  {score['chunk_score']:5.1f} / 100  (weight 25%)")
        print(f"    ────────────────────────────")
        print(f"    TOTAL:   {score['total_score']:5.1f} / 100")

        verdict = score["verdict"]
        if verdict == "GO":
            print(f"\n  >>> VERDICT: GO — Score {score['total_score']} ≥ 80 <<<")
        elif verdict == "WARN":
            print(f"\n  >>> VERDICT: WARN — Score {score['total_score']} (60-79) <<<")
        else:
            print(f"\n  >>> VERDICT: NO-GO — Score {score['total_score']} < 60 <<<")
        print(f"{'#'*60}\n")

        if verdict == "NO-GO":
            all_pass = False

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
