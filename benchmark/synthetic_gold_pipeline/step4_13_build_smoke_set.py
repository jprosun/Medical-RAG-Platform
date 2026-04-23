# -*- coding: utf-8 -*-
"""Build a small representative smoke set from the dev split."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR.parent / "datasets"

DEV_JSON_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_2_102_split" / "dev" / "vmj_synthetic_gold_v1_2_102_dev.json"
SMOKE_DIR = DATASET_DIR / "vmj_synthetic_gold_v1_2_102_split" / "smoke"
SMOKE_JSON_PATH = SMOKE_DIR / "vmj_synthetic_gold_v1_2_102_smoke.json"
SMOKE_JSONL_PATH = SMOKE_DIR / "vmj_synthetic_gold_v1_2_102_smoke.jsonl"
SMOKE_MANIFEST_PATH = SMOKE_DIR / "manifest.json"

TARGET_QUERY_TYPES = {"simple": 3, "condition": 3, "reasoning": 3, "bounded_partial": 3}
TARGET_SIZE = 12


def load_json(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def select_smoke_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()

    for query_type, target in TARGET_QUERY_TYPES.items():
        candidates = [row for row in rows if row["query_type"] == query_type]
        candidates.sort(
            key=lambda row: (
                0 if row["difficulty"] == "hard" else 1 if row["difficulty"] == "medium" else 2,
                0 if row["answerability"] == "partial_only" else 1,
                row["query_id"],
            )
        )
        for row in candidates[:target]:
            selected.append(row)
            selected_ids.add(row["query_id"])

    if sum(1 for row in selected if row["difficulty"] == "hard") < 2:
        hard_candidates = [
            row for row in rows
            if row["difficulty"] == "hard" and row["query_id"] not in selected_ids
        ]
        for row in hard_candidates:
            replace_idx = next(
                (
                    idx for idx, item in enumerate(selected)
                    if item["difficulty"] != "hard" and item["query_type"] == row["query_type"]
                ),
                None,
            )
            if replace_idx is None:
                continue
            selected_ids.remove(selected[replace_idx]["query_id"])
            selected[replace_idx] = row
            selected_ids.add(row["query_id"])
            if sum(1 for item in selected if item["difficulty"] == "hard") >= 2:
                break

    if sum(1 for row in selected if row["answerability"] == "partial_only") < 3:
        partial_candidates = [
            row for row in rows
            if row["answerability"] == "partial_only" and row["query_id"] not in selected_ids
        ]
        for row in partial_candidates:
            replace_idx = next(
                (
                    idx for idx, item in enumerate(selected)
                    if item["answerability"] != "partial_only" and item["query_type"] == row["query_type"]
                ),
                None,
            )
            if replace_idx is None:
                continue
            selected_ids.remove(selected[replace_idx]["query_id"])
            selected[replace_idx] = row
            selected_ids.add(row["query_id"])
            if sum(1 for item in selected if item["answerability"] == "partial_only") >= 3:
                break

    selected.sort(key=lambda row: row["query_id"])
    if len(selected) != TARGET_SIZE:
        raise ValueError(f"Expected {TARGET_SIZE} smoke rows, found {len(selected)}")
    return selected


def build_manifest(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "split": "smoke",
        "count": len(rows),
        "query_ids": [row["query_id"] for row in rows],
        "query_type": dict(Counter(row["query_type"] for row in rows)),
        "answerability": dict(Counter(row["answerability"] for row in rows)),
        "difficulty": dict(Counter(row["difficulty"] for row in rows)),
        "topic": dict(Counter(row["topic"] for row in rows)),
    }


def main() -> None:
    dev_rows = load_json(DEV_JSON_PATH)
    smoke_rows = select_smoke_rows(dev_rows)
    save_json(SMOKE_JSON_PATH, smoke_rows)
    save_jsonl(SMOKE_JSONL_PATH, smoke_rows)
    save_json(SMOKE_MANIFEST_PATH, build_manifest(smoke_rows))
    print(f"Smoke JSON: {SMOKE_JSON_PATH}")
    print(f"Smoke JSONL: {SMOKE_JSONL_PATH}")
    print(f"Smoke manifest: {SMOKE_MANIFEST_PATH}")
    print(f"Smoke records: {len(smoke_rows)}")


if __name__ == "__main__":
    main()
