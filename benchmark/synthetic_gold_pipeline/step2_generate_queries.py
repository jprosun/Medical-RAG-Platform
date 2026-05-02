# -*- coding: utf-8 -*-
"""
Step 2 - Query Evolution (Single-Seed, VMJ-only)
Usage:
  python step2_generate_queries.py                    # full 65 seed
  python step2_generate_queries.py --pilot             # pilot 10 seed
  python step2_generate_queries.py --pilot --limit 5   # pilot 5 seed
"""

import os
import re
import sys
import json
import time
import random
import argparse
import httpx
from typing import List, Dict, Optional

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ======================================================================
# CONFIG
# ======================================================================
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
SEED_FILE = os.path.join(OUTPUT_DIR, "seed_whitelist_v1.jsonl")
DRAFT_FILE = os.path.join(OUTPUT_DIR, "draft_queries_v1.jsonl")
PILOT_FILE = os.path.join(OUTPUT_DIR, "draft_queries_pilot.jsonl")

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL_ID = "google/gemini-2.5-flash"

RANDOM_SEED = 42

TAXONOMY_WEIGHTS = {
    "simple": 0.30,
    "reasoning": 0.30,
    "condition": 0.25,
    "bounded_partial": 0.15,
}

# ======================================================================
# PROMPT v3 - Ti\u1ebfng Vi\u1ec7t c\u00f3 d\u1ea5u \u0111\u1ea7y \u0111\u1ee7
# ======================================================================

# Load prompt from external file to avoid encoding issues in source
PROMPT_FILE = os.path.join(os.path.dirname(__file__), "prompt_step2.txt")


def _get_openrouter_api_key() -> str:
    key = (
        os.environ.get("OPENROUTER_API_KEY")
        or os.environ.get("OPEN_ROUTER_API_KEY")
    )
    if not key:
        raise RuntimeError(
            "Missing OpenRouter API key. Set OPENROUTER_API_KEY or OPEN_ROUTER_API_KEY in your environment/.env."
        )
    return key


def _load_prompt():
    if os.path.exists(PROMPT_FILE):
        with open(PROMPT_FILE, "r", encoding="utf-8") as f:
            return f.read()
    # fallback - should not happen
    return "You are a medical QA benchmark expert. Generate queries in Vietnamese."


# ======================================================================
# SEED ANALYSIS
# ======================================================================

RICH_MARKERS = [
    "m\u1ee5c ti\u00eau", "objective", "\u0111\u1ed1i t\u01b0\u1ee3ng v\u00e0 ph\u01b0\u01a1ng ph\u00e1p",
    "k\u1ebft qu\u1ea3", "results", "k\u1ebft lu\u1eadn", "conclusion",
    "y\u1ebfu t\u1ed1 nguy c\u01a1", "y\u1ebfu t\u1ed1 li\u00ean quan", "so s\u00e1nh",
    "OR", "HR", "p<", "p =", "KTC", "CI",
    "t\u1ef7 l\u1ec7", "trung b\u00ecnh", "ng\u01b0\u1ee1ng",
]

CASE_REPORT_MARKERS = [
    "ca l\u00e2m s\u00e0ng", "case report", "tr\u01b0\u1eddng h\u1ee3p",
    "b\u00e1o c\u00e1o ca", "nh\u00e2n m\u1ed9t tr\u01b0\u1eddng h\u1ee3p",
]


def classify_seed_richness(seed: Dict) -> int:
    ctx = seed.get("context", "").lower()
    title = seed.get("title", "").lower()
    quality = seed.get("seed_quality", "high")

    if quality == "borderline":
        return 1
    if any(m in title or m in ctx[:200] for m in CASE_REPORT_MARKERS):
        return 1

    rich_count = sum(1 for m in RICH_MARKERS if m in ctx)
    if rich_count >= 3:
        return 2

    if len(ctx) < 500:
        return 1

    return 1


def assign_taxonomy(num_seeds: int, rng: random.Random) -> list:
    type_pool = []
    for t, w in TAXONOMY_WEIGHTS.items():
        count = max(1, int(w * num_seeds * 1.8))
        type_pool.extend([t] * count)
    rng.shuffle(type_pool)
    return type_pool


# ======================================================================
# OPENROUTER API
# ======================================================================

def call_llm(http_client: httpx.Client, system_prompt: str,
             seed: Dict, assigned_types: List[str],
             num_queries: int) -> Optional[List[Dict]]:
    api_key = _get_openrouter_api_key()
    user_prompt = build_user_prompt(seed, assigned_types, num_queries)

    payload = {
        "model": MODEL_ID,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.7,
        "max_tokens": 2048,
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/medqa-benchmark",
        "X-Title": "MedQA Gold Pipeline",
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = http_client.post(
                OPENROUTER_BASE_URL,
                json=payload,
                headers=headers,
                timeout=60.0,
            )

            if resp.status_code != 200:
                err = resp.text[:300]
                print(f"    [!] API error {resp.status_code} (attempt {attempt+1}): {err}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                continue

            data = resp.json()
            raw_text = data["choices"][0]["message"]["content"].strip()

            # Strip markdown fences
            raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
            raw_text = re.sub(r"\s*```$", "", raw_text)

            parsed = json.loads(raw_text)

            if isinstance(parsed, dict):
                parsed = [parsed]

            if not isinstance(parsed, list):
                print(f"    [!] Unexpected response type: {type(parsed)}")
                return None

            return parsed

        except json.JSONDecodeError as e:
            print(f"    [!] JSON parse error (attempt {attempt+1}): {e}")
            print(f"    Raw: {raw_text[:200]}...")
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as e:
            print(f"    [!] API error (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)

    return None


def build_user_prompt(seed: Dict, assigned_types: List[str], num_queries: int) -> str:
    title = seed.get("title", "")
    context = seed.get("context", "")
    topic = seed.get("topic", "unknown")

    type_desc = ", ".join(assigned_types)

    multi_note = ""
    if num_queries > 1:
        multi_note = (
            "\n**QUAN TR\u1eccNG khi sinh 2 c\u00e2u h\u1ecfi:**\n"
            "- M\u1ed7i c\u00e2u ph\u1ea3i thu\u1ed9c M\u1ed8T lo\u1ea1i KH\u00c1C NHAU trong danh s\u00e1ch.\n"
            "- Hai c\u00e2u ph\u1ea3i h\u1ecfi v\u1ec1 2 KH\u00cdA C\u1ea0NH KH\u00c1C NHAU "
            "(v\u00ed d\u1ee5: 1 c\u00e2u v\u1ec1 y\u1ebfu t\u1ed1 nguy c\u01a1/b\u1ec7nh sinh, "
            "1 c\u00e2u v\u1ec1 ph\u01b0\u01a1ng ph\u00e1p/k\u1ebft qu\u1ea3/\u0111i\u1ec1u tr\u1ecb).\n"
            "- KH\u00d4NG \u0111\u01b0\u1ee3c 2 c\u00e2u c\u00f9ng h\u1ecfi v\u1ec1 c\u00f9ng m\u1ed9t \u00fd."
        )

    return (
        f"## Th\u00f4ng tin seed\n"
        f"- **Ti\u00eau \u0111\u1ec1 b\u00e0i b\u00e1o**: {title}\n"
        f"- **Chuy\u00ean ng\u00e0nh**: {topic}\n"
        f"- **Ng\u1eef c\u1ea3nh (context)**:\n{context}\n\n"
        f"## Y\u00eau c\u1ea7u\n"
        f"Sinh \u0111\u00fang {num_queries} c\u00e2u h\u1ecfi t\u1eeb context tr\u00ean.\n"
        f"C\u00e1c lo\u1ea1i c\u00e2u h\u1ecfi c\u1ea7n sinh: [{type_desc}]\n"
        f"{multi_note}\n\n"
        f"Nh\u1eafc l\u1ea1i: G\u00e1n nh\u00e3n \u0110\u00daNG b\u1ea3n ch\u1ea5t. "
        f"\u0110\u1ecdc l\u1ea1i \u0111\u1ecbnh ngh\u0129a taxonomy tr\u01b0\u1edbc khi g\u00e1n.\n"
        f"C\u00e2u h\u1ecfi ph\u1ea3i b\u1eb1ng ti\u1ebfng Vi\u1ec7t C\u00d3 D\u1ea4U \u0111\u1ea7y \u0111\u1ee7.\n"
        f"Tr\u1ea3 v\u1ec1 JSON array."
    )


# ======================================================================
# MAIN
# ======================================================================

def main():
    parser = argparse.ArgumentParser(description="Step 2: Generate queries from seed contexts")
    parser.add_argument("--pilot", action="store_true", help="Pilot mode: only process first 10 seeds")
    parser.add_argument("--limit", type=int, default=10, help="Number of seeds in pilot mode (default: 10)")
    args = parser.parse_args()

    rng = random.Random(RANDOM_SEED)

    # Load system prompt
    system_prompt = _load_prompt()
    print(f"[*] Loaded prompt from {PROMPT_FILE} ({len(system_prompt)} chars)")

    # Load seeds
    seeds = []
    with open(SEED_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                seeds.append(json.loads(line))

    print(f"[*] Loaded {len(seeds)} seeds from {SEED_FILE}")

    if args.pilot:
        seeds = seeds[:args.limit]
        output_file = PILOT_FILE
        print(f"[*] PILOT MODE: processing {len(seeds)} seeds")
    else:
        output_file = DRAFT_FILE
        print(f"[*] FULL MODE: processing {len(seeds)} seeds")

    # Classify richness
    seed_query_counts = []
    total_queries_planned = 0
    for s in seeds:
        n = classify_seed_richness(s)
        seed_query_counts.append(n)
        total_queries_planned += n

    print(f"[*] Planned: {total_queries_planned} queries from {len(seeds)} seeds")
    print(f"    1-query seeds: {seed_query_counts.count(1)}")
    print(f"    2-query seeds: {seed_query_counts.count(2)}")

    # Assign taxonomy types
    type_pool = assign_taxonomy(len(seeds), rng)
    type_idx = 0

    # Init HTTP client
    client = httpx.Client()

    all_queries = []
    query_counter = 0

    for i, (seed, num_q) in enumerate(zip(seeds, seed_query_counts)):
        title_short = seed.get("title", "")[:60]
        chunk_id = seed.get("chunk_id", f"seed_{i}")
        print(f"\n[{i+1}/{len(seeds)}] {title_short}... ({num_q} queries)")

        # Pick types from pool
        assigned_types = []
        for _ in range(num_q):
            if type_idx < len(type_pool):
                assigned_types.append(type_pool[type_idx])
                type_idx += 1
            else:
                assigned_types.append(rng.choice(list(TAXONOMY_WEIGHTS.keys())))

        # Call LLM
        results = call_llm(client, system_prompt, seed, assigned_types, num_q)

        if results is None:
            print(f"    [!] FAILED - skipping seed")
            continue

        for j, q in enumerate(results[:num_q]):
            query_counter += 1
            query_id = f"q_{query_counter:03d}"

            record = {
                "query_id": query_id,
                "seed_id": chunk_id,
                "question": q.get("question", ""),
                "query_type": q.get("query_type",
                                    assigned_types[j] if j < len(assigned_types) else "simple"),
                "difficulty": q.get("difficulty", "medium"),
                "expected_behavior": q.get("expected_behavior", "summary"),
                "answerability": q.get("answerability", "answerable"),
                "source": seed.get("source", "VMJ"),
                "topic": seed.get("topic", "unknown"),
                "title": seed.get("title", ""),
                "review_status": "draft",
            }
            all_queries.append(record)
            print(f"    > {query_id}: [{record['query_type']}] "
                  f"{record['question'][:80]}...")

        # Rate limiting
        time.sleep(4)

    # Summary
    print(f"\n{'='*60}")
    print(f"[+] Generated {len(all_queries)} queries total")

    type_counts = {}
    diff_counts = {}
    for q in all_queries:
        qt = q["query_type"]
        type_counts[qt] = type_counts.get(qt, 0) + 1
        d = q["difficulty"]
        diff_counts[d] = diff_counts.get(d, 0) + 1

    print(f"\nTaxonomy distribution:")
    for t, c in sorted(type_counts.items()):
        pct = c / len(all_queries) * 100 if all_queries else 0
        print(f"  {t}: {c} ({pct:.1f}%)")

    print(f"\nDifficulty distribution:")
    for d, c in sorted(diff_counts.items()):
        print(f"  {d}: {c}")

    # Save
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        for q in all_queries:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")

    print(f"\n[+] Saved to: {output_file}")


if __name__ == "__main__":
    main()
