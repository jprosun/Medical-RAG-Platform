# -*- coding: utf-8 -*-
"""Materialize split dataset into separate dev/test/holdout folders."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR.parent / "datasets"

SOURCE_JSON_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_2_102_split.json"
SOURCE_REPORT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_2_102_split_report.json"
TARGET_ROOT = DATASET_DIR / "vmj_synthetic_gold_v1_2_102_split"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def build_manifest(rows: list[dict[str, Any]], split: str) -> dict[str, Any]:
    return {
        "split": split,
        "count": len(rows),
        "query_ids": [row["query_id"] for row in rows],
        "query_type": dict(Counter(row["query_type"] for row in rows)),
        "answerability": dict(Counter(row["answerability"] for row in rows)),
        "difficulty": dict(Counter(row["difficulty"] for row in rows)),
        "topic": dict(Counter(row["topic"] for row in rows)),
    }


def main() -> None:
    rows = load_json(SOURCE_JSON_PATH)
    report = load_json(SOURCE_REPORT_PATH)

    if len(rows) != 102:
        raise ValueError(f"Expected 102 split rows, found {len(rows)}")

    grouped: dict[str, list[dict[str, Any]]] = {"dev": [], "test": [], "holdout": []}
    for row in rows:
        split = row["split"]
        if split not in grouped:
            raise ValueError(f"Unexpected split value: {split}")
        grouped[split].append(row)

    for split, split_rows in grouped.items():
        split_rows.sort(key=lambda row: row["query_id"])
        split_dir = TARGET_ROOT / split
        base_name = f"vmj_synthetic_gold_v1_2_102_{split}"
        save_json(split_dir / f"{base_name}.json", split_rows)
        save_jsonl(split_dir / f"{base_name}.jsonl", split_rows)
        save_json(split_dir / "manifest.json", build_manifest(split_rows, split))

    save_json(TARGET_ROOT / "split_report.json", report)

    print(f"Materialized split folders under: {TARGET_ROOT}")
    for split, split_rows in grouped.items():
        print(f"{split}: {len(split_rows)} records")


if __name__ == "__main__":
    main()
