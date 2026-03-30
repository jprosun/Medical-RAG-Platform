"""
Benchmark RAG Pipeline — Main Orchestrator
============================================

Automated 5-stage pipeline for validating and ingesting new data:

  Stage 1: Load & Normalize (universal_loader)
  Stage 2: Quality Score    (qa_pre_ingest)
  Stage 3: Ingest to Staging (ingest_staging)
  Stage 4: Retrieval Benchmark (fast_eval before/after)
  Stage 5: Report Generation

Usage:
    cd services/qdrant-ingestor

    python benchmark_pipeline.py \
      --input ../../data/custom/my_data.csv \
      --source-name "Custom Medical DB" \
      --threshold 80 \
      --baseline-collection staging_medqa \
      --target-collection staging_benchmark
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))

from etl.universal_loader import universal_load, save_jsonl
from qa_pre_ingest.run_all_checks import run_all, compute_composite_score


# ── Stage 1: Load & Normalize ──────────────────────────────────────


def stage_1_load(input_path: str, source_name: str, output_path: str) -> dict:
    """Load and normalize data to JSONL."""
    print(f"\n{'='*60}")
    print(f"  STAGE 1: Load & Normalize")
    print(f"{'='*60}")

    t0 = time.time()
    records = universal_load(input_path, source_name=source_name)

    if not records:
        return {"status": "FAIL", "reason": "No valid records loaded", "records": 0}

    save_jsonl(records, output_path)

    return {
        "status": "OK",
        "records": len(records),
        "output": output_path,
        "elapsed_s": round(time.time() - t0, 1),
    }


# ── Stage 2: Quality Score ─────────────────────────────────────────


def stage_2_quality(jsonl_path: str, threshold: float = 80.0) -> dict:
    """Run QA checks and compute composite score."""
    print(f"\n{'='*60}")
    print(f"  STAGE 2: Quality Score (threshold={threshold})")
    print(f"{'='*60}")

    t0 = time.time()
    results = run_all(jsonl_path)
    score = compute_composite_score(results)

    # Print score summary
    print(f"\n  Quality Score:")
    print(f"    Schema:  {score['schema_score']:5.1f} / 100")
    print(f"    Content: {score['content_score']:5.1f} / 100")
    print(f"    Chunks:  {score['chunk_score']:5.1f} / 100")
    print(f"    TOTAL:   {score['total_score']:5.1f} / 100")

    passed = score["total_score"] >= threshold
    score["passed"] = passed
    score["threshold"] = threshold
    score["elapsed_s"] = round(time.time() - t0, 1)

    if not passed:
        print(f"\n   QUALITY GATE FAILED: {score['total_score']} < {threshold}")
    else:
        print(f"\n   QUALITY GATE PASSED: {score['total_score']} ≥ {threshold}")

    return score


# ── Stage 3: Ingest to Staging ─────────────────────────────────────


def stage_3_ingest(
    jsonl_path: str,
    collection: str,
    qdrant_url: str = "http://localhost:6333",
    chunk_size: int = 900,
    overlap: int = 150,
    precomputed_file: str = None,
    remote_embed_url: str = None,
) -> dict:
    """Ingest JSONL into a Qdrant staging collection."""
    print(f"\n{'='*60}")
    print(f"  STAGE 3: Ingest to Staging -> {collection}")
    print(f"{'='*60}")

    t0 = time.time()

    try:
        import warnings
        warnings.filterwarnings("ignore")
        import traceback
        import tempfile
        import shutil
        import pickle

        from app.ingest import ingest_enriched_jsonl, ensure_collection, upsert_chunks
        from qdrant_client import QdrantClient
        from fastembed import TextEmbedding

        if precomputed_file and os.path.exists(precomputed_file):
            print(f"  [FAST PATH] Loading precomputed GPU chunks from: {precomputed_file}")
            with open(precomputed_file, "rb") as f:
                chunks = pickle.load(f)
        else:
            # Prepare temp dir with single file
            tmpdir = tempfile.mkdtemp(prefix="benchmark_ingest_")
            try:
                tmp_file = os.path.join(tmpdir, os.path.basename(jsonl_path))
                shutil.copy2(jsonl_path, tmp_file)

                chunks = ingest_enriched_jsonl(
                    input_path=tmpdir,
                    patterns=["*.jsonl"],
                    chunk_size=chunk_size,
                    overlap=overlap,
                )
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)

        if not chunks:
            return {"status": "FAIL", "reason": "No chunks generated", "chunks": 0}

        print(f"  Chunks generated: {len(chunks)}")

        # Connect and upsert
        embedding_model = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
        qclient = QdrantClient(url=qdrant_url, check_compatibility=False)
        embedder = TextEmbedding(model_name=embedding_model)
        vec_size = len(list(embedder.embed(["probe"]))[0].tolist())

        print(f"  Embedding model: {embedding_model} (dim={vec_size})")

        # Recreate collection for clean benchmark
        existing = {c.name for c in qclient.get_collections().collections}
        if collection in existing:
            qclient.delete_collection(collection)
            print(f"  Deleted existing collection: {collection}")
        ensure_collection(qclient, collection, vec_size)
        print(f"  Created collection: {collection}")

        print(f"  Upserting {len(chunks)} chunks (batch_size=64)...")
        upsert_chunks(
            client=qclient,
            collection=collection,
            embedder=embedder,
            chunks=chunks,
            batch_size=64,
            remote_embed_url=remote_embed_url,
        )

        info = qclient.get_collection(collection)
        print(f"  Done: {info.points_count} points in {collection}")
        return {
            "status": "OK",
            "chunks": len(chunks),
            "points": info.points_count,
            "collection": collection,
            "elapsed_s": round(time.time() - t0, 1),
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "FAIL", "reason": str(e), "chunks": 0}


# ── Stage 4: Retrieval Benchmark ───────────────────────────────────


def stage_4_benchmark(
    baseline_collection: str,
    candidate_collection: str,
    qdrant_url: str = "http://localhost:6333",
) -> dict:
    """Run retrieval eval on both collections and compare."""
    print(f"\n{'='*60}")
    print(f"  STAGE 4: Retrieval Benchmark")
    print(f"  Baseline:  {baseline_collection}")
    print(f"  Candidate: {candidate_collection}")
    print(f"{'='*60}")

    t0 = time.time()

    try:
        eval_dir = os.path.join(os.path.dirname(__file__), "..", "rag-orchestrator")
        eval_queries_path = os.path.join(eval_dir, "eval_queries.json")
        if not os.path.exists(eval_queries_path):
            return {"status": "SKIP", "reason": "eval_queries.json not found"}

        with open(eval_queries_path, "r", encoding="utf-8") as f:
            queries = json.load(f)

        import warnings
        warnings.filterwarnings("ignore")
        from fastembed import TextEmbedding
        from qdrant_client import QdrantClient
        from qdrant_client.models import SearchParams
        
        embedder = TextEmbedding(model_name=os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5"))
        qclient = QdrantClient(url=qdrant_url, check_compatibility=False)

        def eval_collection(collection_name: str) -> dict:
            """Run retrieval eval on a single collection."""
            total = len(queries)
            src_hit3 = 0
            title_hit3 = 0
            title_mrr = 0.0
            generic = 0

            for q in queries:
                query = q["query"]
                exp_source = q.get("expected_source", "")
                exp_title = q.get("expected_title", "")

                vec = list(embedder.embed([query]))[0].tolist()
                response = qclient.query_points(
                    collection_name=collection_name, 
                    query=vec, 
                    limit=3,
                    search_params=SearchParams(hnsw_ef=128, exact=False)
                )
                hits = response.points if hasattr(response, "points") else response

                # Source hit
                for rank, hit in enumerate(hits, 1):
                    src = str(hit.payload.get("source_name", "")).lower()
                    if exp_source.lower() in src:
                        src_hit3 += 1
                        break

                # Title hit
                for rank, hit in enumerate(hits, 1):
                    src = str(hit.payload.get("source_name", "")).lower()
                    title = str(hit.payload.get("title", "")).lower()
                    if exp_source.lower() in src:
                        if not exp_title or exp_title.lower() in title:
                            title_hit3 += 1
                            title_mrr += 1.0 / rank
                            break

                # Generic titles
                for hit in hits:
                    t = str(hit.payload.get("title", "")).lower()
                    if t in ("bookshelf", ""):
                        generic += 1

            return {
                "src_hit3": round(src_hit3 / max(1, total) * 100, 1),
                "title_hit3": round(title_hit3 / max(1, total) * 100, 1),
                "title_mrr": round(title_mrr / max(1, total), 3),
                "generic_titles_pct": round(generic / max(1, total * 3) * 100, 1),
                "total_queries": total,
            }

        baseline_metrics = eval_collection(baseline_collection)
        candidate_metrics = eval_collection(candidate_collection)

        # Compute delta
        delta = {
            "title_hit3": round(candidate_metrics["title_hit3"] - baseline_metrics["title_hit3"], 1),
            "title_mrr": round(candidate_metrics["title_mrr"] - baseline_metrics["title_mrr"], 3),
            "src_hit3": round(candidate_metrics["src_hit3"] - baseline_metrics["src_hit3"], 1),
        }

        # Decide verdict
        verdict = decide_verdict(baseline_metrics, candidate_metrics)

        return {
            "status": "OK",
            "baseline": baseline_metrics,
            "candidate": candidate_metrics,
            "delta": delta,
            "verdict": verdict,
            "elapsed_s": round(time.time() - t0, 1),
        }

    except Exception as e:
        return {"status": "FAIL", "reason": str(e)}


def decide_verdict(baseline: dict, candidate: dict) -> str:
    """
    Compare metrics between baseline and candidate.
    Returns: "PROMOTE" | "ROLLBACK" | "MANUAL_REVIEW"
    """
    # ROLLBACK conditions
    if candidate["title_hit3"] < 80:
        return "ROLLBACK"
    if candidate["generic_titles_pct"] > 0:
        return "ROLLBACK"
    if candidate["title_hit3"] < baseline["title_hit3"] - 5:
        return "ROLLBACK"

    # PROMOTE conditions
    if (candidate["title_hit3"] >= baseline["title_hit3"] - 3
        and candidate["title_mrr"] >= baseline["title_mrr"] - 0.03
        and candidate["title_hit3"] >= 85):
        return "PROMOTE"

    return "MANUAL_REVIEW"


# ── Stage 5: Report Generation ─────────────────────────────────────


def stage_5_report(
    all_results: dict,
    output_path: str = "benchmark_report.md",
) -> str:
    """Generate markdown benchmark report."""
    print(f"\n{'='*60}")
    print(f"  STAGE 5: Report Generation")
    print(f"{'='*60}")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"# Benchmark Report",
        f"",
        f"**Generated**: {now}",
        f"",
    ]

    # Stage 1
    s1 = all_results.get("stage_1", {})
    lines.append(f"## Stage 1: Data Loading")
    lines.append(f"- Records loaded: **{s1.get('records', 0)}**")
    lines.append(f"- Status: {s1.get('status', '?')}")
    lines.append(f"- Time: {s1.get('elapsed_s', '?')}s")
    lines.append("")

    # Stage 2
    s2 = all_results.get("stage_2", {})
    lines.append(f"## Stage 2: Quality Score")
    lines.append(f"| Metric | Score |")
    lines.append(f"|---|---|")
    lines.append(f"| Schema | {s2.get('schema_score', '?')} / 100 |")
    lines.append(f"| Content | {s2.get('content_score', '?')} / 100 |")
    lines.append(f"| Chunks | {s2.get('chunk_score', '?')} / 100 |")
    lines.append(f"| **Total** | **{s2.get('total_score', '?')} / 100** |")
    lines.append(f"| Verdict | {s2.get('verdict', '?')} |")
    lines.append("")

    # Stage 3
    s3 = all_results.get("stage_3", {})
    lines.append(f"## Stage 3: Ingestion")
    lines.append(f"- Chunks created: **{s3.get('chunks', 0)}**")
    lines.append(f"- Collection: `{s3.get('collection', '?')}`")
    lines.append(f"- Time: {s3.get('elapsed_s', '?')}s")
    lines.append("")

    # Stage 4
    s4 = all_results.get("stage_4", {})
    if s4.get("status") == "OK":
        b = s4.get("baseline", {})
        c = s4.get("candidate", {})
        d = s4.get("delta", {})
        lines.append(f"## Stage 4: Retrieval Benchmark")
        lines.append(f"| Metric | Baseline | Candidate | Delta |")
        lines.append(f"|---|---|---|---|")
        lines.append(f"| Title Hit@3 | {b.get('title_hit3')}% | {c.get('title_hit3')}% | {d.get('title_hit3'):+.1f}% |")
        lines.append(f"| Title MRR | {b.get('title_mrr')} | {c.get('title_mrr')} | {d.get('title_mrr'):+.3f} |")
        lines.append(f"| Src Hit@3 | {b.get('src_hit3')}% | {c.get('src_hit3')}% | {d.get('src_hit3'):+.1f}% |")
        lines.append(f"| Generic titles | {b.get('generic_titles_pct')}% | {c.get('generic_titles_pct')}% | - |")
        lines.append(f"")
        lines.append(f"**Verdict: {s4.get('verdict', '?')}**")
    elif s4.get("status") == "SKIP":
        lines.append(f"## Stage 4: Retrieval Benchmark")
        lines.append(f"- Skipped: {s4.get('reason', '?')}")
    lines.append("")

    # Final verdict
    final_verdict = s4.get("verdict", s2.get("verdict", "?"))
    lines.append(f"## Final Verdict: **{final_verdict}**")

    report = "\n".join(lines)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"  [SAVED] {output_path}")
    return output_path


# ── Main Pipeline ──────────────────────────────────────────────────


def run_pipeline(
    input_path: str,
    source_name: str,
    threshold: float = 80.0,
    baseline_collection: str = "staging_medqa",
    target_collection: str = "staging_benchmark",
    qdrant_url: str = "http://localhost:6333",
    skip_ingest: bool = False,
    skip_benchmark: bool = False,
    report_path: str = "benchmark_report.md",
    precomputed_file: str = None,
    remote_embed_url: str = None,
) -> dict:
    """Run the full benchmark pipeline."""
    print(f"\n{'#'*60}")
    print(f"  BENCHMARK RAG PIPELINE")
    print(f"{'#'*60}")
    print(f"  Input:     {input_path}")
    print(f"  Source:    {source_name}")
    print(f"  Threshold: {threshold}")
    print(f"  Baseline:  {baseline_collection}")
    print(f"  Target:    {target_collection}")
    print(f"{'#'*60}")

    all_results = {}
    normalized_path = os.path.join(
        os.path.dirname(input_path) or ".",
        f"_normalized_{os.path.basename(input_path).split('.')[0]}.jsonl"
    )

    # Stage 1
    s1 = stage_1_load(input_path, source_name, normalized_path)
    all_results["stage_1"] = s1
    if s1["status"] != "OK":
        print(f"\n  ❌ PIPELINE STOPPED at Stage 1: {s1.get('reason')}")
        stage_5_report(all_results, report_path)
        return all_results

    # Stage 2
    s2 = stage_2_quality(normalized_path, threshold)
    all_results["stage_2"] = s2
    if not s2.get("passed", False):
        print(f"\n   PIPELINE STOPPED at Stage 2: Score {s2['total_score']} < {threshold}")
        stage_5_report(all_results, report_path)
        return all_results

    # Stage 3
    if skip_ingest:
        all_results["stage_3"] = {"status": "SKIP", "reason": "skip_ingest=True"}
    else:
        s3 = stage_3_ingest(
            jsonl_path=normalized_path, 
            collection=target_collection, 
            qdrant_url=qdrant_url,
            precomputed_file=precomputed_file,
            remote_embed_url=remote_embed_url
        )
        all_results["stage_3"] = s3
        if s3.get("status") != "OK":
            print(f"\n   PIPELINE STOPPED at Stage 3: {s3.get('reason')}")
            stage_5_report(all_results, report_path)
            return all_results

    # Stage 4
    if skip_benchmark:
        all_results["stage_4"] = {"status": "SKIP", "reason": "skip_benchmark=True"}
    else:
        s4 = stage_4_benchmark(baseline_collection, target_collection, qdrant_url)
        all_results["stage_4"] = s4

    # Stage 5
    stage_5_report(all_results, report_path)

    # Final summary
    verdict = all_results.get("stage_4", {}).get("verdict",
              all_results.get("stage_2", {}).get("verdict", "?"))
    print(f"\n{'#'*60}")
    print(f"  PIPELINE COMPLETE — Verdict: {verdict}")
    print(f"{'#'*60}\n")

    return all_results


def main():
    ap = argparse.ArgumentParser(description="Benchmark RAG Pipeline")
    ap.add_argument("--input", "-i", required=True, help="Input data file or directory")
    ap.add_argument("--source-name", "-s", default="Benchmark Import", help="Source name")
    ap.add_argument("--threshold", "-t", type=float, default=80.0, help="Quality score threshold (0-100)")
    ap.add_argument("--baseline-collection", default="staging_medqa", help="Baseline Qdrant collection")
    ap.add_argument("--target-collection", default="staging_benchmark", help="Target Qdrant collection")
    ap.add_argument("--qdrant-url", default="http://localhost:6333", help="Qdrant URL")
    ap.add_argument("--skip-ingest", action="store_true", help="Skip Stage 3 (ingest)")
    ap.add_argument("--skip-benchmark", action="store_true", help="Skip Stage 4 (retrieval benchmark)")
    ap.add_argument("--report", default="benchmark_report.md", help="Report output path")
    ap.add_argument("--precomputed", default=None, help="Path to Kaggle-generated .pkl file for fast Stage 3 GPU ingestion, skipping CPU embedding.")
    ap.add_argument("--remote-embed-url", default=None, help="URL of the Remote GPU Embedding API (e.g., Kaggle/Colab via Cloudflare).")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        print(f"  [ERROR] Input not found: {args.input}")
        sys.exit(1)

    run_pipeline(
        input_path=args.input,
        source_name=args.source_name,
        threshold=args.threshold,
        baseline_collection=args.baseline_collection,
        target_collection=args.target_collection,
        qdrant_url=args.qdrant_url,
        skip_ingest=args.skip_ingest,
        skip_benchmark=args.skip_benchmark,
        report_path=args.report,
        precomputed_file=args.precomputed,
        remote_embed_url=args.remote_embed_url,
    )


if __name__ == "__main__":
    main()
