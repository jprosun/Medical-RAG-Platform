"""
Enhanced Fast Retrieval Evaluation v2 – Per-source + combined evaluation.

Changes from v1:
  - Two-tier scoring: source_hit AND title_hit
  - Per-collection evaluation support: run against individual source collections
  - Improved metrics: separate source_hit@3, title_hit@3

Usage:
  cd services/rag-orchestrator
  
  # Eval combined (default)
  python fast_eval.py
  
  # Eval per-source
  python fast_eval.py --collection staging_medlineplus --label MedlinePlus
  python fast_eval.py --collection staging_who --label WHO
  python fast_eval.py --collection staging_ncbi --label NCBI
  
  # Eval all (combined + per-source comparison)
  python fast_eval.py --all
"""

import json
import os
import re
import sys
import time
import argparse
from collections import defaultdict
from typing import Dict, List, Any, Optional

# Add path so local imports work
sys.path.insert(0, os.path.abspath("."))


# ── Helpers ─────────────────────────────────────────────────────────
def has_list_items(text: str) -> bool:
    patterns = [r"^\s*[\-\*\•]\s+", r"^\s*\d+[\.\)]\s+", r"^\s*[a-zA-Z][\.\)]\s+"]
    lines = text.split("\n")
    return sum(1 for line in lines if any(re.match(p, line) for p in patterns)) >= 2


def citation_quality(metadata: dict) -> Dict[str, bool]:
    return {
        "has_source_name": bool(metadata.get("source_name")),
        "has_title": bool(metadata.get("title")),
        "has_section_title": bool(metadata.get("section_title")),
        "has_source_url": bool(metadata.get("source_url")),
    }


def check_source_hit(chunk_metadata: dict, expected_source: str) -> bool:
    """Check if chunk source matches expected."""
    src = str(chunk_metadata.get("source_name", "")).lower()
    return expected_source.lower() in src


def check_title_hit(chunk_metadata: dict, expected_source: str, expected_title: str) -> bool:
    """Check if chunk matches expected source AND title/heading."""
    if not check_source_hit(chunk_metadata, expected_source):
        return False
    if not expected_title:
        return True  # no title requirement → source match is enough
    title = str(chunk_metadata.get("title", "")).lower()
    heading = str(chunk_metadata.get("heading_path", "")).lower()
    exp = expected_title.lower()
    return exp in title or exp in heading


# ── Single collection evaluation ────────────────────────────────────
def eval_collection(collection: str, label: str, queries: list) -> Dict:
    """Run evaluation against a single collection, return metrics dict."""
    # Set environment for retriever 
    os.environ["QDRANT_URL"] = os.environ.get("QDRANT_URL", "http://localhost:6333")
    os.environ["QDRANT_COLLECTION"] = collection
    os.environ["RAG_TOP_K"] = os.environ.get("RAG_TOP_K", "3")
    os.environ["EMBEDDING_MODEL"] = os.environ.get("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
    os.environ["RAG_MIN_SCORE"] = os.environ.get("RAG_MIN_SCORE", "0.0")
    os.environ["RAG_MAX_CONTEXT_TOKENS"] = os.environ.get("RAG_MAX_CONTEXT_TOKENS", "4096")

    from app.retriever import QdrantRetriever
    retriever = QdrantRetriever(
        qdrant_url=os.environ["QDRANT_URL"],
        collection=collection,
        embedding_model=os.environ["EMBEDDING_MODEL"],
        top_k=int(os.environ["RAG_TOP_K"]),
        score_threshold=0.0,
        max_context_tokens=int(os.environ["RAG_MAX_CONTEXT_TOKENS"]),
    )

    # Counters
    overall = {"src_hit1": 0, "src_hit3": 0, "title_hit1": 0, "title_hit3": 0,
               "total": 0, "src_mrr": 0.0, "title_mrr": 0.0}
    by_cat = defaultdict(lambda: {"src_hit1": 0, "src_hit3": 0, "title_hit1": 0, 
                                   "title_hit3": 0, "total": 0, "src_mrr": 0.0, "title_mrr": 0.0})
    by_src = defaultdict(lambda: {"src_hit1": 0, "src_hit3": 0, "title_hit1": 0,
                                   "title_hit3": 0, "total": 0, "src_mrr": 0.0, "title_mrr": 0.0})
    citation_stats = {"total": 0, "has_source_name": 0, "has_title": 0,
                      "has_section_title": 0, "has_source_url": 0, "title_generic": 0}
    trunc_list = {"total": 0, "has_list": 0}
    mixed_topic = {"total": 0, "confused": 0}
    results = []

    for i, q in enumerate(queries):
        query = q["query"]
        exp_source = q["expected_source"]
        exp_title = q.get("expected_title", "")
        category = q.get("category", "fact")

        # Multi-turn: rewrite query using mock history (rule-based, no LLM needed)
        search_query = query
        rewritten = False
        if category == "multi_turn" and q.get("mock_history"):
            from app.query_rewriter import rewrite_query as rw
            search_query = rw(query, q["mock_history"], llm_client=None)
            rewritten = search_query != query

        t0 = time.time()
        chunks = retriever.retrieve(search_query, auto_filter=False)
        latency = time.time() - t0

        # Source hit detection
        src_hit_at = 0
        for rank, c in enumerate(chunks, 1):
            if check_source_hit(c.metadata, exp_source):
                src_hit_at = rank
                break

        # Title hit detection (stricter)
        title_hit_at = 0
        for rank, c in enumerate(chunks, 1):
            if check_title_hit(c.metadata, exp_source, exp_title):
                title_hit_at = rank
                break

        src_h1 = 1 if src_hit_at == 1 else 0
        src_h3 = 1 if src_hit_at > 0 else 0
        src_rr = (1.0 / src_hit_at) if src_hit_at > 0 else 0.0
        tit_h1 = 1 if title_hit_at == 1 else 0
        tit_h3 = 1 if title_hit_at > 0 else 0
        tit_rr = (1.0 / title_hit_at) if title_hit_at > 0 else 0.0

        # Update counters
        for d in [overall, by_cat[category], by_src[exp_source]]:
            d["src_hit1"] += src_h1; d["src_hit3"] += src_h3
            d["title_hit1"] += tit_h1; d["title_hit3"] += tit_h3
            d["total"] += 1
            d["src_mrr"] += src_rr; d["title_mrr"] += tit_rr

        # Citation
        for c in chunks:
            cq = citation_quality(c.metadata)
            citation_stats["total"] += 1
            for k, v in cq.items():
                if v: citation_stats[k] += 1
            title_val = str(c.metadata.get("title", "")).lower()
            if title_val in ("bookshelf", ""):
                citation_stats["title_generic"] += 1

        # Category checks
        list_found = False
        if category == "truncated_list":
            trunc_list["total"] += 1
            list_found = any(has_list_items(c.text) for c in chunks)
            if list_found: trunc_list["has_list"] += 1

        confused = False
        if category == "mixed_topic":
            mixed_topic["total"] += 1
            srcs = set(c.metadata.get("source_name", "") for c in chunks)
            if len(srcs) > 1 and not src_h3:
                confused = True; mixed_topic["confused"] += 1

        # Status
        if tit_h3:
            icon = "✅"
        elif src_h3:
            icon = "🟡"
        else:
            icon = "❌"
        print(f"  [{i+1:2d}/{len(queries)}] {icon} {query[:55]}...")

        # Store
        chunk_details = []
        for rank, c in enumerate(chunks, 1):
            chunk_details.append({
                "rank": rank, "source": c.metadata.get("source_name", "?"),
                "title": c.metadata.get("title", "?")[:50],
                "section": c.metadata.get("section_title", "")[:30],
                "score": round(c.score, 4),
                "has_list": has_list_items(c.text),
            })
        results.append({
            "query": query, "expected_source": exp_source, "expected_title": exp_title,
            "category": category, "src_hit_at": src_hit_at, "title_hit_at": title_hit_at,
            "src_h1": src_h1, "src_h3": src_h3, "title_h1": tit_h1, "title_h3": tit_h3,
            "src_rr": src_rr, "title_rr": tit_rr, "latency": round(latency, 3),
            "chunks": chunk_details, "list_found": list_found if category == "truncated_list" else None,
            "confused": confused if category == "mixed_topic" else None,
        })

    return {
        "label": label, "collection": collection,
        "overall": overall, "by_cat": dict(by_cat), "by_src": dict(by_src),
        "citation": citation_stats, "trunc_list": trunc_list,
        "mixed_topic": mixed_topic, "results": results,
    }


# ── Report generation ───────────────────────────────────────────────
def _pct(n, d):
    return f"{n}/{d} ({n/max(1,d)*100:.1f}%)"


def print_summary(data: Dict):
    """Print console summary for one collection eval."""
    o = data["overall"]
    n = o["total"]
    label = data["label"]
    
    print(f"\n{'=' * 60}")
    print(f"  [{label}] collection: {data['collection']} (n={n})")
    print(f"{'=' * 60}")
    
    print(f"  {'Metric':<20s} {'Source Hit':>15s} {'Title Hit':>15s}")
    print(f"  {'─'*50}")
    print(f"  {'Hit@1':<20s} {_pct(o['src_hit1'],n):>15s} {_pct(o['title_hit1'],n):>15s}")
    print(f"  {'Hit@3':<20s} {_pct(o['src_hit3'],n):>15s} {_pct(o['title_hit3'],n):>15s}")
    print(f"  {'MRR':<20s} {o['src_mrr']/n:>15.3f} {o['title_mrr']/n:>15.3f}")

    print(f"\n  Theo nguồn:")
    for src in ["MedlinePlus", "WHO", "NCBI Bookshelf"]:
        s = data["by_src"].get(src)
        if s and s["total"] > 0:
            t = s["total"]
            print(f"    {src:20s}  SrcH3={_pct(s['src_hit3'],t):>12s}  TitH3={_pct(s['title_hit3'],t):>12s}")

    print(f"\n  Theo category:")
    for cat in ["fact", "truncated_list", "mixed_topic", "filter", "multi_turn"]:
        c = data["by_cat"].get(cat)
        if c and c["total"] > 0:
            t = c["total"]
            print(f"    {cat:18s}  SrcH3={_pct(c['src_hit3'],t):>12s}  TitH3={_pct(c['title_hit3'],t):>12s}")

    # Citation
    tc = data["citation"]["total"]
    if tc > 0:
        gen = data["citation"]["title_generic"]
        print(f"\n  Citation: {_pct(data['citation']['has_title'],tc)} has title, "
              f"{gen} generic titles ({gen/tc*100:.1f}%)")

    # Truncated list / mixed topic
    tl = data["trunc_list"]
    if tl["total"] > 0:
        print(f"  Truncated list: {_pct(tl['has_list'], tl['total'])} chunks with list items")
    mt = data["mixed_topic"]
    if mt["total"] > 0:
        print(f"  Mixed topic confused: {_pct(mt['confused'], mt['total'])}")


def write_report(all_data: List[Dict], output_path: str):
    """Write markdown report for all collection evaluations."""
    lines = []
    lines.append("# Báo cáo đánh giá Retrieval v2 – Per-Source + Combined\n\n")
    lines.append(f"**Thời gian**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    # Comparison table
    lines.append("## So sánh tổng quan\n\n")
    lines.append("| Collection | Src Hit@1 | Src Hit@3 | Title Hit@1 | Title Hit@3 | Src MRR | Title MRR | Generic Titles |\n")
    lines.append("|---|---|---|---|---|---|---|---|\n")
    for d in all_data:
        o = d["overall"]
        n = o["total"]
        tc = d["citation"]["total"]
        gen = d["citation"]["title_generic"]
        lines.append(f"| {d['label']} | {_pct(o['src_hit1'],n)} | {_pct(o['src_hit3'],n)} "
                     f"| {_pct(o['title_hit1'],n)} | {_pct(o['title_hit3'],n)} "
                     f"| {o['src_mrr']/n:.3f} | {o['title_mrr']/n:.3f} "
                     f"| {gen}/{tc} ({gen/max(1,tc)*100:.1f}%) |\n")

    # Per-collection details
    for d in all_data:
        o = d["overall"]
        n = o["total"]
        lines.append(f"\n---\n\n## {d['label']} (`{d['collection']}`)\n\n")

        # By source
        lines.append("### Theo nguồn\n\n")
        lines.append("| Source | Src Hit@3 | Title Hit@3 | Title MRR |\n|---|---|---|---|\n")
        for src in ["MedlinePlus", "WHO", "NCBI Bookshelf"]:
            s = d["by_src"].get(src)
            if s and s["total"] > 0:
                t = s["total"]
                lines.append(f"| {src} | {_pct(s['src_hit3'],t)} | {_pct(s['title_hit3'],t)} | {s['title_mrr']/t:.3f} |\n")

        # By category
        lines.append("\n### Theo category\n\n")
        lines.append("| Category | Src Hit@3 | Title Hit@3 | Title MRR |\n|---|---|---|---|\n")
        for cat in ["fact", "truncated_list", "mixed_topic", "filter", "multi_turn"]:
            c = d["by_cat"].get(cat)
            if c and c["total"] > 0:
                t = c["total"]
                lines.append(f"| {cat} | {_pct(c['src_hit3'],t)} | {_pct(c['title_hit3'],t)} | {c['title_mrr']/t:.3f} |\n")

        # Failed queries (title_hit miss)
        failed = [r for r in d["results"] if not r["title_h3"] and r.get("expected_title")]
        if failed:
            lines.append(f"\n### Câu hỏi miss (title_hit) — {len(failed)} queries\n\n")
            for r in failed:
                icon = "🟡" if r["src_h3"] else "❌"
                lines.append(f"#### {icon} {r['query']}\n")
                lines.append(f"- Expected: {r['expected_source']} ({r['expected_title']})\n")
                lines.append(f"- Category: {r['category']}\n")
                lines.append(f"- Src hit@: {r['src_hit_at']} | Title hit@: {r['title_hit_at']}\n\n")
                lines.append("| Rank | Source | Title | Section | Score |\n|---|---|---|---|---|\n")
                for ch in r["chunks"]:
                    lines.append(f"| {ch['rank']} | {ch['source']} | {ch['title']} | {ch['section']} | {ch['score']:.4f} |\n")
                lines.append("\n")

    with open(output_path, "w", encoding="utf-8") as f:
        f.writelines(lines)
    print(f"\n  📄 Report: {os.path.abspath(output_path)}")


# ── Main ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fast Retrieval Eval v2")
    parser.add_argument("--collection", default="staging_medqa")
    parser.add_argument("--label", default="Combined")
    parser.add_argument("--all", action="store_true", help="Eval all collections")
    parser.add_argument("--queries", default="eval_queries.json")
    parser.add_argument("--output", default="../../data/eval_report_staging.md")
    parser.add_argument("--compare", nargs=2, metavar=("BASELINE", "CANDIDATE"),
                        help="Compare two collections: --compare staging_medqa staging_benchmark")
    args = parser.parse_args()

    with open(args.queries, "r", encoding="utf-8") as f:
        queries = json.load(f)

    # Compare mode
    if args.compare:
        baseline_coll, candidate_coll = args.compare
        print("=" * 60)
        print("  Collection Comparison")
        print("=" * 60)

        print(f"\n  Evaluating BASELINE: {baseline_coll}")
        baseline_data = eval_collection(baseline_coll, "Baseline", queries)
        print_summary(baseline_data)

        print(f"\n  Evaluating CANDIDATE: {candidate_coll}")
        candidate_data = eval_collection(candidate_coll, "Candidate", queries)
        print_summary(candidate_data)

        # Comparison
        bo = baseline_data["overall"]
        co = candidate_data["overall"]
        bn, cn = bo["total"], co["total"]

        print(f"\n{'=' * 60}")
        print(f"  BEFORE / AFTER COMPARISON")
        print(f"{'=' * 60}")
        print(f"  {'Metric':<20s} {'Baseline':>12s} {'Candidate':>12s} {'Delta':>10s}")
        print(f"  {'─'*55}")

        metrics = [
            ("Src Hit@3", bo["src_hit3"]/bn*100, co["src_hit3"]/cn*100),
            ("Title Hit@3", bo["title_hit3"]/bn*100, co["title_hit3"]/cn*100),
            ("Title MRR", bo["title_mrr"]/bn, co["title_mrr"]/cn),
        ]
        for name, bval, cval in metrics:
            delta = cval - bval
            fmt = ".1f" if "Hit" in name else ".3f"
            print(f"  {name:<20s} {bval:>12{fmt}} {cval:>12{fmt}} {delta:>+10{fmt}}")

        # Verdict
        b_metrics = {
            "title_hit3": bo["title_hit3"]/bn*100,
            "title_mrr": bo["title_mrr"]/bn,
            "src_hit3": bo["src_hit3"]/bn*100,
            "generic_titles_pct": baseline_data["citation"]["title_generic"] / max(1, baseline_data["citation"]["total"]) * 100,
        }
        c_metrics = {
            "title_hit3": co["title_hit3"]/cn*100,
            "title_mrr": co["title_mrr"]/cn,
            "src_hit3": co["src_hit3"]/cn*100,
            "generic_titles_pct": candidate_data["citation"]["title_generic"] / max(1, candidate_data["citation"]["total"]) * 100,
        }

        # Verdict logic
        if c_metrics["title_hit3"] < 80:
            verdict = "ROLLBACK"
        elif c_metrics["generic_titles_pct"] > 0:
            verdict = "ROLLBACK"
        elif c_metrics["title_hit3"] < b_metrics["title_hit3"] - 5:
            verdict = "ROLLBACK"
        elif (c_metrics["title_hit3"] >= b_metrics["title_hit3"] - 3
              and c_metrics["title_mrr"] >= b_metrics["title_mrr"] - 0.03
              and c_metrics["title_hit3"] >= 85):
            verdict = "PROMOTE"
        else:
            verdict = "MANUAL_REVIEW"

        icon = {"PROMOTE": "✅", "ROLLBACK": "❌", "MANUAL_REVIEW": "⚠️"}.get(verdict, "?")
        print(f"\n  >>> VERDICT: {icon} {verdict} <<<")
        print(f"{'=' * 60}")

        # Write both reports
        write_report([baseline_data, candidate_data], args.output)
        return

    print("=" * 60)
    print("  Fast Retrieval Evaluation v2")
    print("=" * 60)

    all_results = []

    if args.all:
        configs = [
            ("staging_medqa", "Combined", queries),
            ("staging_medlineplus", "MedlinePlus", [q for q in queries if q["expected_source"] == "MedlinePlus"]),
            ("staging_who", "WHO", [q for q in queries if q["expected_source"] == "WHO"]),
            ("staging_ncbi", "NCBI", [q for q in queries if q["expected_source"] == "NCBI Bookshelf"]),
        ]
        for coll, label, qs in configs:
            if not qs:
                continue
            print(f"\n{'─' * 60}")
            print(f"  Evaluating: {label} ({coll}) — {len(qs)} queries")
            print(f"{'─' * 60}")
            data = eval_collection(coll, label, qs)
            print_summary(data)
            all_results.append(data)
    else:
        data = eval_collection(args.collection, args.label, queries)
        print_summary(data)
        all_results.append(data)

    write_report(all_results, args.output)

    # Print comparison table
    if len(all_results) > 1:
        print(f"\n{'=' * 60}")
        print(f"  SO SÁNH TỔNG QUAN")
        print(f"{'=' * 60}")
        print(f"  {'Collection':<20s} {'SrcH@3':>10s} {'TitH@3':>10s} {'TitMRR':>10s} {'GenTit':>10s}")
        print(f"  {'─'*60}")
        for d in all_results:
            o = d["overall"]
            n = o["total"]
            gen = d["citation"]["title_generic"]
            tc = d["citation"]["total"]
            print(f"  {d['label']:<20s} {_pct(o['src_hit3'],n):>10s} {_pct(o['title_hit3'],n):>10s} "
                  f"{o['title_mrr']/n:>10.3f} {gen:>5d}/{tc}")

    print(f"\n{'=' * 60}")
    print(f"  DONE!")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()

