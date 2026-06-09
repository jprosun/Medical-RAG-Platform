"""Warm the RAG backend before a demo.

Sends a fixed set of Vietnamese demo questions to the local /api/chat endpoint
so that the embedding model, the article lexical index, and the retrieval /
pipeline caches are all hot before anyone watches. Records per-query timings,
cache flags, and source counts to a JSON report.

This script never mutates data. It only issues normal chat requests.

Usage (PowerShell):
    python tools\\warm_demo_queries.py
    python tools\\warm_demo_queries.py --rounds 2 --include-thinking
    python tools\\warm_demo_queries.py --base-url http://localhost:8000

Acceptance: runs end-to-end, no request crashes, source-backed answers keep
their source list. Run a second round (or --rounds 2) to confirm retrieval_hit
becomes true and total_ms drops.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:  # pragma: no cover
    print("ERROR: the 'requests' package is required (pip install requests).")
    sys.exit(1)


DEMO_QUERIES = [
    "Ung thư là gì? Có chữa được không?",
    "Ung thư vú là gì?",
    "Hen phế quản là gì?",
    "Đái tháo đường type 2 là gì?",
    "Tăng huyết áp là gì?",
    "Viêm gan B là gì?",
    "Sốt xuất huyết là gì?",
    "Trầm cảm là gì?",
    "Kháng sinh là gì?",
    "Tác dụng phụ của thuốc là gì?",
]


def _ready(base_url: str, timeout: float = 5.0) -> bool:
    try:
        r = requests.get(f"{base_url}/ready", timeout=timeout)
        return r.status_code == 200
    except requests.RequestException:
        return False


def _ask(base_url: str, message: str, answer_mode: str, session_id: str, timeout: float) -> dict:
    """Send one chat request and return a compact metrics record."""
    started = time.time()
    try:
        r = requests.post(
            f"{base_url}/api/chat",
            json={"session_id": session_id, "message": message, "answer_mode": answer_mode},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        return {
            "query": message,
            "answer_mode": answer_mode,
            "ok": False,
            "error": f"request_failed: {exc}",
            "client_elapsed_s": round(time.time() - started, 2),
        }

    client_elapsed_s = round(time.time() - started, 2)
    if r.status_code != 200:
        return {
            "query": message,
            "answer_mode": answer_mode,
            "ok": False,
            "error": f"http_{r.status_code}",
            "client_elapsed_s": client_elapsed_s,
        }

    data = r.json()
    metadata = data.get("metadata") or {}
    timings = metadata.get("timings_ms") or {}
    cache = metadata.get("cache") or {}
    pipeline = metadata.get("pipeline") or {}
    answer = data.get("answer") or ""
    return {
        "query": message,
        "answer_mode": answer_mode,
        "ok": True,
        "client_elapsed_s": client_elapsed_s,
        "total_ms": timings.get("total_ms"),
        "retrieval_ms": timings.get("retrieval_ms"),
        "entity_fallback_ms": timings.get("entity_fallback_ms"),
        "evidence_extract_ms": timings.get("evidence_extract_ms"),
        "llm_answer_ms": timings.get("llm_answer_ms"),
        "verifier_ms": timings.get("verifier_ms"),
        "retrieval_hit": cache.get("retrieval_hit"),
        "entity_fallback_skipped": pipeline.get("entity_fallback_skipped"),
        "entity_fallback_used": pipeline.get("entity_fallback_used"),
        "entity_fallback_cache_hit": pipeline.get("entity_fallback_cache_hit"),
        "llm_verifier_used": pipeline.get("llm_verifier_used"),
        "query_type": metadata.get("query_type"),
        "coverage_mode": metadata.get("coverage_mode"),
        "source_count": len(data.get("retrieved_chunks") or []),
        "answer_chars": len(answer),
        "degraded_mode": data.get("degraded_mode"),
    }


def _fmt(value, suffix: str = "") -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.0f}{suffix}"
    return f"{value}{suffix}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Warm the RAG demo queries.")
    parser.add_argument(
        "--base-url",
        default=os.getenv("RAG_API_URL", "http://localhost:8000"),
        help="Backend base URL (default: http://localhost:8000).",
    )
    parser.add_argument("--rounds", type=int, default=1, help="How many passes over the query set.")
    parser.add_argument(
        "--include-thinking",
        action="store_true",
        help="Also warm Thinking mode (slower; usually skip for demo warmup).",
    )
    parser.add_argument("--timeout", type=float, default=600.0, help="Per-request timeout in seconds.")
    parser.add_argument(
        "--output-dir",
        default="benchmark/datasets/test_results",
        help="Directory for the JSON report.",
    )
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    modes = ["standard"] + (["thinking"] if args.include_thinking else [])

    print(f"[warm] backend: {base_url}")
    if not _ready(base_url):
        print("[warm] WARNING: /ready did not return 200. Backend may still be loading the embedding model.")
        print("[warm] Continuing anyway; the first cold request can take a few minutes.")

    records: list[dict] = []
    for round_idx in range(1, args.rounds + 1):
        for mode in modes:
            for i, query in enumerate(DEMO_QUERIES):
                session_id = f"probe_warm_r{round_idx}_{mode}_{i}"
                print(f"[warm] round {round_idx} | {mode:8s} | {query}")
                rec = _ask(base_url, query, mode, session_id, args.timeout)
                rec["round"] = round_idx
                records.append(rec)
                if rec["ok"]:
                    print(
                        f"        total={_fmt(rec['total_ms'],'ms')}"
                        f" retrieval={_fmt(rec['retrieval_ms'],'ms')}"
                        f" entity_fb={_fmt(rec['entity_fallback_ms'],'ms')}"
                        f" (skipped={_fmt(rec['entity_fallback_skipped'])})"
                        f" llm={_fmt(rec['llm_answer_ms'],'ms')}"
                        f" sources={rec['source_count']}"
                    )
                else:
                    print(f"        FAILED: {rec.get('error')}")

    ok = [r for r in records if r["ok"]]
    failed = [r for r in records if not r["ok"]]
    print("\n[warm] summary")
    print(f"  requests: {len(records)}  ok: {len(ok)}  failed: {len(failed)}")
    if ok:
        totals = [r["total_ms"] for r in ok if isinstance(r.get("total_ms"), (int, float))]
        if totals:
            totals_sorted = sorted(totals)
            print(f"  total_ms  min/median/max: "
                  f"{totals_sorted[0]:.0f} / {totals_sorted[len(totals_sorted)//2]:.0f} / {totals_sorted[-1]:.0f}")
        skipped = sum(1 for r in ok if r.get("entity_fallback_skipped"))
        print(f"  entity_fallback skipped: {skipped}/{len(ok)}")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    out_path = out_dir / f"demo_latency_warmup_{stamp}.json"
    out_path.write_text(
        json.dumps(
            {
                "base_url": base_url,
                "generated_at_utc": stamp,
                "rounds": args.rounds,
                "modes": modes,
                "records": records,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\n[warm] report written: {out_path}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
