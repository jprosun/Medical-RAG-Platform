# -*- coding: utf-8 -*-
"""Produce a first-pass rewrite for the 42 flagged Step 3 records."""

from __future__ import annotations

import json
import re
import unicodedata
from copy import deepcopy
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
BATCH_DIR = BASE_DIR / "output" / "chatgpt_batches"

MERGED_PATH = BATCH_DIR / "final_merged.json"
FLAGGED_REVIEW_SET_PATH = BATCH_DIR / "final_flagged_42_review_set.json"

FIRST_PASS_MERGED_PATH = BATCH_DIR / "final_merged_first_pass.json"
FIRST_PASS_REVIEW_SET_PATH = BATCH_DIR / "final_flagged_42_first_pass_review_set.json"

TOKEN_RE = re.compile(r"\w+", re.UNICODE)

MANUAL_MUST_HAVE_OVERRIDES = {
    "q_084": [
        "Chỉ có mối liên quan với kết quả ở thời điểm 1 tháng",
        "p < 0,05",
        "Nhạy cảm nhẹ dần và bình thường ở 3 tháng",
        "Tỷ lệ thành công 6 tháng là 100%",
        "Không thể kết luận ảnh hưởng lâu dài từ đoạn trích",
    ]
}

MANUAL_SHORT_ANSWER_OVERRIDES = {
    "q_002": "Không; context chỉ nêu tỷ lệ mắc và các yếu tố nguy cơ, không có thông tin chi tiết về phòng ngừa hay quản lý bệnh.",
    "q_009": "Suy hô hấp là dấu hiệu lâm sàng phổ biến nhất và khởi phát trong giờ đầu sau sinh.",
    "q_039": "Nghiên cứu đánh giá tình trạng nhập viện, các khía cạnh lâm sàng, cận lâm sàng và kết quả điều trị, nhưng không mô tả cụ thể nội dung can thiệp.",
    "q_041": "Troponin quan trọng vì hs-cTn tăng liên quan mạnh, theo liều lượng với nguy cơ AF mới khởi phát, vẫn có ý nghĩa sau điều chỉnh yếu tố nguy cơ và có thể là dấu hiệu cảnh báo sớm tổn thương tâm nhĩ cận lâm sàng.",
    "q_057": "BASIC-DPS còn vận hành thủ công, xử lý chậm khoảng 12 phút cho 1x1 cm, lỗi quét khoảng 5% và cần tăng tự động hóa, độ tin cậy.",
    "q_084": "Không thể kết luận ảnh hưởng lâu dài; context chỉ cho thấy liên quan ở mốc 1 tháng, còn đến 3 tháng răng trở lại bình thường và 6 tháng tỷ lệ thành công là 100%; Chỉ có mối liên quan với kết quả ở thời điểm 1 tháng; p < 0,05; Nhạy cảm nhẹ dần và bình thường ở 3 tháng; Không đủ dữ liệu để kết luận ảnh hưởng lâu dài.",
}


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text).lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_for_matching(text: str) -> str:
    text = normalize_text(text)
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(text: str) -> list[str]:
    return [token for token in TOKEN_RE.findall(normalize_for_matching(text)) if len(token) > 1]


def phrase_present(phrase: str, text: str) -> bool:
    normalized_text = normalize_for_matching(text)
    normalized_phrase = normalize_for_matching(phrase)
    if not normalized_phrase:
        return True
    if normalized_phrase in normalized_text:
        return True

    phrase_tokens = [token for token in normalized_phrase.split() if len(token) > 1]
    if not phrase_tokens:
        return True

    text_tokens = set(normalized_text.split())
    overlap = sum(1 for token in phrase_tokens if token in text_tokens)
    return overlap / len(phrase_tokens) >= 0.8


def list_phrase_coverage(phrases: list[str], text: str) -> float:
    if not phrases:
        return 1.0
    matched = sum(1 for phrase in phrases if phrase_present(phrase, text))
    return matched / len(phrases)


def split_sentences(text: str) -> list[str]:
    pieces = re.split(r"(?<=[.!?])\s+", text.strip())
    return [piece.strip() for piece in pieces if piece.strip()]


def dedupe_preserve_order(items: list[str]) -> list[str]:
    seen = set()
    output = []
    for item in items:
        key = normalize_text(item)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def rewrite_must_have(record: dict[str, Any], item: dict[str, Any]) -> tuple[list[str], list[str]]:
    if record["query_id"] in MANUAL_MUST_HAVE_OVERRIDES:
        return MANUAL_MUST_HAVE_OVERRIDES[record["query_id"]], []

    must_have = list(record["must_have_concepts"])
    removed = []

    if "must_have_concepts_weakly_supported_by_ground_truth" in item["issues"]:
        unsupported = set(item["metrics"].get("missing_must_have_in_ground_truth", []) or [])
        new_must_have = []
        for concept in must_have:
            if concept in unsupported:
                removed.append(concept)
                continue
            new_must_have.append(concept)
        must_have = new_must_have

    must_have = dedupe_preserve_order(must_have)
    return must_have, removed


def append_missing_concepts(original_short: str, missing_concepts: list[str]) -> str:
    if not missing_concepts:
        return original_short.strip()
    base = original_short.strip().rstrip(" .;:")
    suffix = "; ".join(missing_concepts).strip()
    if not base:
        return suffix + "."
    return base + "; " + suffix + "."


def greedy_sentence_cover(ground_truth: str, concepts: list[str]) -> str:
    sentences = split_sentences(ground_truth)
    if not sentences:
        return ground_truth.strip()

    uncovered = list(concepts)
    selected_indexes: list[int] = []

    while uncovered:
        best_index = None
        best_gain = 0
        for idx, sentence in enumerate(sentences):
            if idx in selected_indexes:
                continue
            gain = sum(1 for concept in uncovered if phrase_present(concept, sentence))
            if gain > best_gain:
                best_gain = gain
                best_index = idx
        if best_index is None or best_gain == 0:
            break
        selected_indexes.append(best_index)
        uncovered = [concept for concept in uncovered if not phrase_present(concept, sentences[best_index])]

    if not selected_indexes:
        return ground_truth.strip()

    selected_indexes.sort()
    return " ".join(sentences[idx] for idx in selected_indexes).strip()


def choose_rewritten_short_answer(
    record: dict[str, Any],
    rewritten_must_have: list[str],
    item: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    if record["query_id"] in MANUAL_SHORT_ANSWER_OVERRIDES:
        chosen = MANUAL_SHORT_ANSWER_OVERRIDES[record["query_id"]]
        return chosen, {
            "strategy": "manual_first_pass_override",
            "coverage_before": round(list_phrase_coverage(rewritten_must_have, record["short_answer"]), 3),
            "coverage_after": round(list_phrase_coverage(rewritten_must_have, chosen), 3),
        }

    original_short = record["short_answer"].strip()
    ground_truth = record["ground_truth"].strip()

    if "short_answer_covers_too_few_must_have_concepts" not in item["issues"]:
        return original_short, {
            "strategy": "kept_original_short_answer",
            "coverage_before": round(list_phrase_coverage(rewritten_must_have, original_short), 3),
            "coverage_after": round(list_phrase_coverage(rewritten_must_have, original_short), 3),
        }

    missing_concepts = [concept for concept in rewritten_must_have if not phrase_present(concept, original_short)]

    appended = append_missing_concepts(original_short, missing_concepts)
    extracted = greedy_sentence_cover(ground_truth, rewritten_must_have)

    appended_coverage = list_phrase_coverage(rewritten_must_have, appended)
    extracted_coverage = list_phrase_coverage(rewritten_must_have, extracted)
    appended_tokens = len(tokenize(appended))
    extracted_tokens = len(tokenize(extracted))

    if extracted_coverage > appended_coverage:
        chosen = extracted
        strategy = "ground_truth_sentence_extraction"
    elif appended_coverage > extracted_coverage:
        chosen = appended
        strategy = "append_missing_concepts_to_original_short_answer"
    else:
        if extracted_tokens and extracted_tokens <= appended_tokens:
            chosen = extracted
            strategy = "ground_truth_sentence_extraction"
        else:
            chosen = appended
            strategy = "append_missing_concepts_to_original_short_answer"

    return chosen, {
        "strategy": strategy,
        "coverage_before": round(list_phrase_coverage(rewritten_must_have, original_short), 3),
        "coverage_after": round(list_phrase_coverage(rewritten_must_have, chosen), 3),
        "missing_concepts_added": missing_concepts,
    }


def main() -> None:
    merged_records = load_json(MERGED_PATH)
    flagged_review_set = load_json(FLAGGED_REVIEW_SET_PATH)

    record_map = {record["query_id"]: deepcopy(record) for record in merged_records}
    first_pass_review_entries = []

    for item in flagged_review_set:
        query_id = item["query_id"]
        current_record = deepcopy(record_map[query_id])
        original_record = deepcopy(current_record)

        rewritten_must_have, removed_must_have = rewrite_must_have(current_record, item)
        current_record["must_have_concepts"] = rewritten_must_have

        rewritten_short_answer, short_answer_meta = choose_rewritten_short_answer(
            current_record,
            rewritten_must_have,
            item,
        )
        current_record["short_answer"] = rewritten_short_answer

        record_map[query_id] = current_record
        first_pass_review_entries.append(
            {
                "query_id": query_id,
                "priority": item["priority"],
                "issues": item["issues"],
                "rewrite_notes": {
                    "removed_must_have_concepts": removed_must_have,
                    "short_answer_rewrite": short_answer_meta,
                },
                "original_record": original_record,
                "rewritten_record": current_record,
            }
        )

    rewritten_records = [record_map[record["query_id"]] for record in merged_records]
    save_json(FIRST_PASS_MERGED_PATH, rewritten_records)
    save_json(FIRST_PASS_REVIEW_SET_PATH, first_pass_review_entries)

    print(f"First-pass merged file: {FIRST_PASS_MERGED_PATH}")
    print(f"First-pass review set: {FIRST_PASS_REVIEW_SET_PATH}")
    print(f"Rewritten flagged records: {len(first_pass_review_entries)}")


if __name__ == "__main__":
    main()
