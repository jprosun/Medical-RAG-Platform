# -*- coding: utf-8 -*-
"""
Export reviewed queries + seed context to a readable format
for human ground truth annotation.
"""
import json
import os

QUERIES_FILE = os.path.join(os.path.dirname(__file__), "output", "reviewed_queries_v1.jsonl")
SEED_FILE = os.path.join(os.path.dirname(__file__), "output", "seed_whitelist_v1.jsonl")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "output", "step3_annotation_template.jsonl")

def main():
    # Load seeds by chunk_id
    seeds = {}
    with open(SEED_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                s = json.loads(line)
                seeds[s["chunk_id"]] = s

    # Load queries
    queries = []
    with open(QUERIES_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                queries.append(json.loads(line))

    # Build annotation template
    items = []
    for q in queries:
        seed = seeds.get(q["seed_id"], {})
        item = {
            "query_id": q["query_id"],
            "question": q["question"],
            "query_type": q["query_type"],
            "difficulty": q["difficulty"],
            "expected_behavior": q["expected_behavior"],
            "answerability": q["answerability"],
            "topic": q["topic"],
            "title": seed.get("title", q.get("title", "")),
            "context": seed.get("context", ""),
            # === FIELDS TO FILL ===
            "ground_truth": "",
            "short_answer": "",
            "must_have_concepts": [],
            "must_not_claim": [],
        }
        items.append(item)

    # Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"Exported {len(items)} items to {OUTPUT_FILE}")
    print(f"Fields to fill: ground_truth, short_answer, must_have_concepts, must_not_claim")

if __name__ == "__main__":
    main()
