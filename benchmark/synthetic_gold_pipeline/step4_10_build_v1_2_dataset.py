# -*- coding: utf-8 -*-
"""Build vmj_synthetic_gold_v1.2_102 with low-risk deploy fixes."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR.parent / "datasets"

SOURCE_JSON_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_1_102.json"
JSONL_OUTPUT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_2_102.jsonl"
JSON_OUTPUT_PATH = DATASET_DIR / "vmj_synthetic_gold_v1_2_102.json"

DATASET_VERSION = "v1.2_102"


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


def normalize_number_spacing(text: str) -> str:
    text = re.sub(r"(?<=\d),\s+(?=\d)", ",", text)
    text = re.sub(r"(?<=\d)\.\s+(?=\d)", ".", text)
    return text


def normalize_value(value: Any) -> Any:
    if isinstance(value, str):
        return normalize_number_spacing(value)
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_value(item) for key, item in value.items()}
    return value


def apply_record_fixes(row: dict[str, Any]) -> dict[str, Any]:
    query_id = row["query_id"]

    if query_id == "q_003":
        row["ground_truth"] = (
            "Phẫu thuật cắt bỏ tổn thương là phương pháp điều trị được ưu tiên cho "
            "u tương bào ngoài tủy đơn độc ở phổi với tổn thương cô lập."
        )

    if query_id == "q_062":
        row["question"] = (
            "Những cytokine và yếu tố tăng trưởng nào được tìm thấy chủ yếu trong "
            "exosome từ huyết tương giàu tiểu cầu (PRP) theo nghiên cứu này?"
        )
        row["ground_truth"] = (
            "Theo nghiên cứu, các cytokine và yếu tố tăng trưởng chủ yếu trong exosome "
            "từ PRP gồm IL-6, TNF-α, PDGF, TGF-β và EGF. Các thành phần này được mô tả "
            "là hỗ trợ tái tạo mô và phục hồi da."
        )
        row["must_not_claim"] = [
            "IL-1β là thành phần chủ yếu của exosome từ PRP",
            "TNF-α là một interleukin",
            "Nghiên cứu khẳng định exosome từ PRP chỉ chứa yếu tố tăng trưởng mà không có cytokine",
        ]

    if query_id == "q_071":
        row["context"] = row["context"].replace("nôi dung", "nội dung")

    if query_id == "q_077":
        row["context"] = row["context"].replace("non- invasive", "non-invasive")

    if query_id == "q_079":
        row["ground_truth"] = (
            "Theo nghiên cứu này, Vancomycin, Linezolid và Co-trimoxazole vẫn còn hiệu quả "
            "đối với MRSA vì hầu như chưa ghi nhận kháng với các kháng sinh này. Nghiên cứu "
            "cũng ghi nhận MRSA kháng với nhiều kháng sinh khác, bao gồm cả carbapenem trong "
            "mẫu nghiên cứu này; đây là kết quả kháng sinh đồ của nghiên cứu cụ thể, không nên "
            "diễn giải thành đặc điểm chung của MRSA trên lâm sàng."
        )
        row["must_not_claim"] = [
            "Carbapenem vẫn còn hiệu quả cao với MRSA",
            "MRSA nhạy với hầu hết kháng sinh thông thường",
            "Đặc tính kháng carbapenem 87,5% là đặc điểm chung của MRSA",
        ]

    if query_id == "q_033":
        row["must_not_claim"] = [
            "Tuổi trẻ hơn là yếu tố làm tăng nguy cơ suy dinh dưỡng",
            "Nghiên cứu xác định mọi rối loạn điện giải đều làm tăng nguy cơ suy dinh dưỡng",
        ]

    return row


def main() -> None:
    rows = load_json(SOURCE_JSON_PATH)
    fixed_rows: list[dict[str, Any]] = []

    for row in rows:
        normalized_row = normalize_value(dict(row))
        normalized_row["dataset_version"] = DATASET_VERSION
        fixed_rows.append(apply_record_fixes(normalized_row))

    query_ids = [row["query_id"] for row in fixed_rows]
    if len(query_ids) != len(set(query_ids)):
        raise ValueError("Duplicate query_id detected after v1.2 transform")

    save_jsonl(JSONL_OUTPUT_PATH, fixed_rows)
    save_json(JSON_OUTPUT_PATH, fixed_rows)

    print(f"v1.2 JSONL: {JSONL_OUTPUT_PATH}")
    print(f"v1.2 JSON: {JSON_OUTPUT_PATH}")
    print(f"Total records: {len(fixed_rows)}")


if __name__ == "__main__":
    main()
