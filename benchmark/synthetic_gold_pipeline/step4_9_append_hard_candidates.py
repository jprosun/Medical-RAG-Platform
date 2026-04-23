# -*- coding: utf-8 -*-
"""Append reviewed hard candidates to create vmj_synthetic_gold_v1.1_102."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR.parent / "datasets"
OUTPUT_DIR = BASE_DIR / "output"

BASE_DATASET_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_90.json"
CANDIDATE_PACK_PATH = OUTPUT_DIR / "hard_record_candidates_v1.json"

JSONL_OUTPUT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_1_102.jsonl"
JSON_OUTPUT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_1_102.json"

DATASET_VERSION = "v1.1_102"
START_QUERY_INDEX = 91


def load_json(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def save_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_base_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["dataset_version"] = DATASET_VERSION
    normalized["seed_ids"] = [row["seed_id"]]
    normalized["derived_from_query_ids"] = [row["query_id"]]
    normalized["record_origin"] = "v1_90"
    normalized["candidate_id"] = None
    normalized["hard_type"] = None
    normalized["rationale"] = None
    return normalized


def normalize_candidate_row(candidate: dict[str, Any], query_index: int) -> dict[str, Any]:
    query_id = f"q_{query_index:03d}"
    seed_ids = list(dict.fromkeys(candidate["seed_ids"]))
    primary_seed_id = seed_ids[0] if seed_ids else None

    return {
        "query_id": query_id,
        "dataset_id": f"vmj_synth_gold_{query_id}",
        "dataset_version": DATASET_VERSION,
        "source": candidate["source"],
        "seed_id": primary_seed_id,
        "seed_ids": seed_ids,
        "review_status": "accepted",
        "language": candidate["language"],
        "split": "gold",
        "question": candidate["question"],
        "context": candidate["context"],
        "query_type": candidate["query_type"],
        "difficulty": candidate["difficulty"],
        "expected_behavior": candidate["expected_behavior"],
        "answerability": candidate["answerability"],
        "topic": candidate["topic"],
        "title": candidate["title"],
        "ground_truth": candidate["ground_truth"],
        "short_answer": candidate["short_answer"],
        "must_have_concepts": candidate["must_have_concepts"],
        "must_not_claim": candidate["must_not_claim"],
        "derived_from_query_ids": candidate["derived_from_query_ids"],
        "record_origin": "hard_candidate_v1",
        "candidate_id": candidate["candidate_id"],
        "hard_type": candidate["hard_type"],
        "rationale": candidate["rationale"],
    }


def main() -> None:
    base_rows = load_json(BASE_DATASET_PATH)
    candidate_rows = load_json(CANDIDATE_PACK_PATH)

    if len(base_rows) != 90:
        raise ValueError(f"Expected 90 base rows, found {len(base_rows)}")
    if len(candidate_rows) != 12:
        raise ValueError(f"Expected 12 candidate rows, found {len(candidate_rows)}")

    merged_rows = [normalize_base_row(row) for row in base_rows]

    next_index = START_QUERY_INDEX
    for candidate in candidate_rows:
        merged_rows.append(normalize_candidate_row(candidate, next_index))
        next_index += 1

    query_ids = [row["query_id"] for row in merged_rows]
    if len(query_ids) != len(set(query_ids)):
        raise ValueError("Duplicate query_id detected in merged dataset")

    save_jsonl(JSONL_OUTPUT_PATH, merged_rows)
    save_json(JSON_OUTPUT_PATH, merged_rows)

    print(f"Appended dataset JSONL: {JSONL_OUTPUT_PATH}")
    print(f"Appended dataset JSON: {JSON_OUTPUT_PATH}")
    print(f"Total records: {len(merged_rows)}")
    print(f"New query range: q_{START_QUERY_INDEX:03d} - q_{next_index - 1:03d}")


if __name__ == "__main__":
    main()
