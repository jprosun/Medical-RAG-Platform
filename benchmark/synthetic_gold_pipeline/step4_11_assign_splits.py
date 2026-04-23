# -*- coding: utf-8 -*-
"""Assign balanced dev/test/holdout splits for vmj_synthetic_gold_v1.2_102."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR.parent / "datasets"

SOURCE_JSON_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_2_102.json"
JSONL_OUTPUT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_2_102_split.jsonl"
JSON_OUTPUT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_2_102_split.json"
REPORT_OUTPUT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_2_102_split_report.json"

DATASET_VERSION = "v1.2_102_split"
TARGET_SPLITS = {"dev": 64, "test": 20, "holdout": 18}
CATEGORY_EXTRACTORS = {
    "query_type": lambda row: row["query_type"],
    "answerability": lambda row: row["answerability"],
    "difficulty": lambda row: row["difficulty"],
    "topic": lambda row: row["topic"],
    "hard_bucket": lambda row: "hard" if row["difficulty"] == "hard" else "non_hard",
}
CATEGORY_WEIGHTS = {
    "query_type": 1.2,
    "answerability": 1.3,
    "difficulty": 1.1,
    "topic": 1.0,
    "hard_bucket": 2.2,
}
MIN_HARD_BY_SPLIT = {"test": 2, "holdout": 2}


def load_json(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def save_json(path: Path, rows: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def build_expected_counts(rows: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, float]]]:
    expected: dict[str, dict[str, dict[str, float]]] = {}
    total_rows = len(rows)
    for split, target_size in TARGET_SPLITS.items():
        expected[split] = {}
        ratio = target_size / total_rows
        for category in CATEGORY_WEIGHTS:
            counts = Counter(CATEGORY_EXTRACTORS[category](row) for row in rows)
            expected[split][category] = {
                value: count * ratio
                for value, count in counts.items()
            }
    return expected


def rarity_key(rows: list[dict[str, Any]]) -> dict[str, tuple[int, int, int, int]]:
    topic_counts = Counter(row["topic"] for row in rows)
    diff_counts = Counter(row["difficulty"] for row in rows)
    answer_counts = Counter(row["answerability"] for row in rows)
    hard_bucket_counts = Counter(CATEGORY_EXTRACTORS["hard_bucket"](row) for row in rows)
    combo_counts = Counter(
        (row["query_type"], row["answerability"], row["difficulty"], row["topic"])
        for row in rows
    )
    return {
        row["query_id"]: (
            combo_counts[(row["query_type"], row["answerability"], row["difficulty"], row["topic"])],
            topic_counts[row["topic"]],
            diff_counts[row["difficulty"]],
            answer_counts[row["answerability"]],
            hard_bucket_counts[CATEGORY_EXTRACTORS["hard_bucket"](row)],
        )
        for row in rows
    }


def incremental_cost(
    row: dict[str, Any],
    split: str,
    current_counts: dict[str, int],
    current_category_counts: dict[str, dict[str, Counter]],
    expected_counts: dict[str, dict[str, dict[str, float]]],
) -> float:
    if current_counts[split] >= TARGET_SPLITS[split]:
        return float("inf")

    cost = 0.0

    # Keep split sizes close to target while filling.
    current_fill = current_counts[split] / TARGET_SPLITS[split]
    cost += current_fill * 0.15

    for category, weight in CATEGORY_WEIGHTS.items():
        value = CATEGORY_EXTRACTORS[category](row)
        current = current_category_counts[split][category][value]
        expected = expected_counts[split][category].get(value, 0.0)
        before = (current - expected) ** 2
        after = (current + 1 - expected) ** 2
        cost += weight * (after - before)

    return cost


def assign_splits(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expected_counts = build_expected_counts(rows)
    order_key = rarity_key(rows)

    ordered_rows = sorted(
        rows,
        key=lambda row: (
            order_key[row["query_id"]],
            row["query_id"],
        ),
    )

    current_counts = {split: 0 for split in TARGET_SPLITS}
    current_category_counts: dict[str, dict[str, Counter]] = {
        split: {category: Counter() for category in CATEGORY_WEIGHTS}
        for split in TARGET_SPLITS
    }

    assigned_rows: list[dict[str, Any]] = []

    for row in ordered_rows:
        ranked_splits = sorted(
            TARGET_SPLITS,
            key=lambda split: (
                incremental_cost(
                    row,
                    split,
                    current_counts,
                    current_category_counts,
                    expected_counts,
                ),
                current_counts[split],
                split,
            ),
        )
        chosen_split = ranked_splits[0]

        updated = dict(row)
        updated["split"] = chosen_split
        updated["dataset_version"] = DATASET_VERSION
        assigned_rows.append(updated)

        current_counts[chosen_split] += 1
        for category in CATEGORY_WEIGHTS:
            value = CATEGORY_EXTRACTORS[category](row)
            current_category_counts[chosen_split][category][value] += 1

    assigned_rows.sort(key=lambda row: row["query_id"])
    return assigned_rows


def build_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    report: dict[str, Any] = {
        "dataset_version": DATASET_VERSION,
        "total_records": len(rows),
        "split_counts": dict(Counter(row["split"] for row in rows)),
        "by_split": {},
    }

    for split in TARGET_SPLITS:
        subset = [row for row in rows if row["split"] == split]
        split_report = {}
        for category in CATEGORY_WEIGHTS:
            split_report[category] = dict(Counter(CATEGORY_EXTRACTORS[category](row) for row in subset))
        report["by_split"][split] = split_report

    return report


def total_penalty(
    rows: list[dict[str, Any]],
    expected_counts: dict[str, dict[str, dict[str, float]]],
) -> float:
    cost = 0.0
    for split in TARGET_SPLITS:
        subset = [row for row in rows if row["split"] == split]
        for category, weight in CATEGORY_WEIGHTS.items():
            observed = Counter(CATEGORY_EXTRACTORS[category](row) for row in subset)
            expected = expected_counts[split][category]
            values = set(observed) | set(expected)
            for value in values:
                cost += weight * ((observed.get(value, 0) - expected.get(value, 0.0)) ** 2)
    return cost


def rebalance_min_hard(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expected_counts = build_expected_counts(rows)
    adjusted_rows = [dict(row) for row in rows]

    for target_split, minimum in MIN_HARD_BY_SPLIT.items():
        while sum(1 for row in adjusted_rows if row["split"] == target_split and row["difficulty"] == "hard") < minimum:
            donor_candidates = [
                row for row in adjusted_rows
                if row["split"] == "dev" and row["difficulty"] == "hard"
            ]
            receiver_candidates = [
                row for row in adjusted_rows
                if row["split"] == target_split and row["difficulty"] != "hard"
            ]
            if not donor_candidates or not receiver_candidates:
                break

            best_swap: tuple[float, str, str] | None = None
            for donor in donor_candidates:
                for receiver in receiver_candidates:
                    trial_rows = [dict(row) for row in adjusted_rows]
                    donor_trial = next(row for row in trial_rows if row["query_id"] == donor["query_id"])
                    receiver_trial = next(row for row in trial_rows if row["query_id"] == receiver["query_id"])
                    donor_trial["split"] = target_split
                    receiver_trial["split"] = "dev"
                    penalty = total_penalty(trial_rows, expected_counts)
                    swap_key = (penalty, donor["query_id"], receiver["query_id"])
                    if best_swap is None or swap_key < best_swap:
                        best_swap = swap_key

            if best_swap is None:
                break

            _, donor_id, receiver_id = best_swap
            donor_row = next(row for row in adjusted_rows if row["query_id"] == donor_id)
            receiver_row = next(row for row in adjusted_rows if row["query_id"] == receiver_id)
            donor_row["split"] = target_split
            receiver_row["split"] = "dev"

    adjusted_rows.sort(key=lambda row: row["query_id"])
    return adjusted_rows


def main() -> None:
    rows = load_json(SOURCE_JSON_PATH)
    if len(rows) != 102:
        raise ValueError(f"Expected 102 rows, found {len(rows)}")

    assigned_rows = assign_splits(rows)
    assigned_rows = rebalance_min_hard(assigned_rows)

    split_counts = Counter(row["split"] for row in assigned_rows)
    if dict(split_counts) != TARGET_SPLITS:
        raise ValueError(f"Unexpected split counts: {dict(split_counts)}")

    save_jsonl(JSONL_OUTPUT_PATH, assigned_rows)
    save_json(JSON_OUTPUT_PATH, assigned_rows)
    save_json(REPORT_OUTPUT_PATH, build_report(assigned_rows))

    print(f"Split dataset JSONL: {JSONL_OUTPUT_PATH}")
    print(f"Split dataset JSON: {JSON_OUTPUT_PATH}")
    print(f"Split report JSON: {REPORT_OUTPUT_PATH}")
    print(f"Split counts: {dict(split_counts)}")


if __name__ == "__main__":
    main()
