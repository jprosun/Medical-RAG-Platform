"""End-to-end live smoke test for the demo improvements.

Run against a running backend. Verifies:
  1. /ready
  2. /api/chat (non-stream) still works; prints timings + pipeline flags
  3. entity-fallback guard: general Standard query -> entity_fallback_skipped=true
  4. guard keeps fallback for Thinking + for numeric/specific queries
  5. /api/chat/stream: SSE stage + token + final events; time-to-first-token
  6. /api/sessions does not 500

Usage:
    python tools\\live_smoke_test.py
    python tools\\live_smoke_test.py --base-url http://localhost:8000
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import requests


def _hr(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def check_ready(base: str) -> bool:
    try:
        r = requests.get(f"{base}/ready", timeout=10)
        print(f"/ready -> {r.status_code} {r.text[:120]}")
        return r.status_code == 200
    except requests.RequestException as exc:
        print(f"/ready failed: {exc}")
        return False


def chat(base: str, message: str, mode: str, sid: str, timeout: float = 600) -> dict:
    t0 = time.time()
    r = requests.post(
        f"{base}/api/chat",
        json={"session_id": sid, "message": message, "answer_mode": mode},
        timeout=timeout,
    )
    elapsed = time.time() - t0
    r.raise_for_status()
    data = r.json()
    md = data.get("metadata") or {}
    timings = md.get("timings_ms") or {}
    pipeline = md.get("pipeline") or {}
    print(f"[{mode}] '{message[:48]}'")
    print(f"  client_elapsed = {elapsed:.1f}s | total_ms={timings.get('total_ms')}")
    print(f"  retrieval_ms={timings.get('retrieval_ms')} entity_fallback_ms={timings.get('entity_fallback_ms')} "
          f"llm_answer_ms={timings.get('llm_answer_ms')} verifier_ms={timings.get('verifier_ms')}")
    print(f"  query_type={md.get('query_type')} coverage_mode={md.get('coverage_mode')}")
    print(f"  entity_fallback_skipped={pipeline.get('entity_fallback_skipped')} "
          f"used={pipeline.get('entity_fallback_used')} cache_hit={pipeline.get('entity_fallback_cache_hit')}")
    print(f"  sources={len(data.get('retrieved_chunks') or [])} answer_chars={len(data.get('answer') or '')} "
          f"degraded={data.get('degraded_mode')}")
    return data


def chat_stream(base: str, message: str, mode: str, sid: str, timeout: float = 600) -> dict:
    t0 = time.time()
    stages: list[str] = []
    token_count = 0
    first_token_at = None
    first_stage_at = None
    final = None
    chars = 0

    with requests.post(
        f"{base}/api/chat/stream",
        json={"session_id": sid, "message": message, "answer_mode": mode},
        headers={"Accept": "text/event-stream"},
        stream=True,
        timeout=timeout,
    ) as r:
        r.raise_for_status()
        buffer = ""
        for raw in r.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            if not raw:
                continue
            if not raw.startswith("data:"):
                continue
            payload = raw[5:].strip()
            if not payload:
                continue
            try:
                evt = json.loads(payload)
            except (ValueError, TypeError):
                continue
            etype = evt.get("type")
            if etype == "stage":
                if first_stage_at is None:
                    first_stage_at = time.time() - t0
                stages.append(evt.get("stage"))
            elif etype == "token":
                if first_token_at is None:
                    first_token_at = time.time() - t0
                token_count += 1
                chars += len(evt.get("delta") or "")
            elif etype == "final":
                final = evt
            elif etype == "error":
                print(f"  STREAM ERROR: {evt.get('detail')}")

    total = time.time() - t0
    print(f"[stream/{mode}] '{message[:48]}'")
    print(f"  stages={stages}")
    print(f"  first_stage_at={first_stage_at and round(first_stage_at,1)}s "
          f"first_token_at={first_token_at and round(first_token_at,1)}s total={total:.1f}s")
    print(f"  token_events={token_count} streamed_chars={chars} got_final={final is not None}")
    if final is not None:
        md = final.get("metadata") or {}
        pipeline = md.get("pipeline") or {}
        print(f"  final answer_chars={len(final.get('answer') or '')} sources={len(final.get('retrieved_chunks') or [])} "
              f"entity_fallback_skipped={pipeline.get('entity_fallback_skipped')}")
    return final or {}


def sessions(base: str) -> None:
    r = requests.get(f"{base}/api/sessions", timeout=30)
    print(f"/api/sessions -> {r.status_code}")
    r.raise_for_status()
    data = r.json()
    print(f"  session count = {len(data.get('sessions') or [])}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    ts = str(int(time.time()))

    _hr("0. Readiness")
    if not check_ready(base):
        print("Backend not ready; aborting.")
        return 1

    _hr("1. Standard general (expect entity_fallback_skipped=true, fast)")
    chat(base, "Ung thư là gì? Có chữa được không?", "standard", f"probe_std_general_{ts}")

    _hr("2. Standard general 2nd call (expect retrieval/cache effects)")
    chat(base, "Hen phế quản là gì?", "standard", f"probe_std_general2_{ts}")

    _hr("3. Standard specific/numeric (expect guard NOT skip -> fallback may run)")
    chat(base, "Theo nghiên cứu này, cỡ mẫu và kết quả chính (AUC, p) là gì?", "standard", f"probe_std_specific_{ts}")

    _hr("4. Thinking general (expect entity_fallback_skipped=false, deeper, more sources)")
    chat(base, "Ung thư là gì? Có chữa được không?", "thinking", f"probe_think_general_{ts}")

    _hr("5. Streaming Standard (expect stage events + tokens + final; first_token < total)")
    chat_stream(base, "Đái tháo đường type 2 là gì?", "standard", f"probe_stream_{ts}")

    _hr("6. /api/sessions (must not 500)")
    sessions(base)

    _hr("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
