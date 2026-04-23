# -*- coding: utf-8 -*-
"""Export the flagged review subset and a repair plan.

This script isolates the records flagged by `final_quality_report.json`,
emits a compact JSON subset for editing, an enriched JSON review set with
record-level repair guidance, and a Markdown repair plan grouped by issue type.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
BATCH_DIR = BASE_DIR / "output" / "chatgpt_batches"

MERGED_PATH = BATCH_DIR / "final_merged.json"
REPORT_PATH = BATCH_DIR / "final_quality_report.json"

RAW_OUTPUT = BATCH_DIR / "final_flagged_42.json"
ENRICHED_OUTPUT = BATCH_DIR / "final_flagged_42_review_set.json"
PLAN_OUTPUT = BATCH_DIR / "final_flagged_42_repair_plan.md"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def save_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def build_suggested_actions(issue_list: list[str], metrics: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    missing_in_ground = metrics.get("missing_must_have_in_ground_truth", []) or []
    missing_in_short = metrics.get("missing_must_have_in_short_answer", []) or []

    if "must_have_concepts_weakly_supported_by_ground_truth" in issue_list:
        if missing_in_ground:
            actions.append(
                "Rà soát `must_have_concepts`; các ý sau đang chưa được support đủ mạnh bởi `ground_truth`: "
                + "; ".join(missing_in_ground[:3])
            )
        actions.append(
            "Ưu tiên sửa `must_have_concepts` trước: đổi phrasing về gần `ground_truth` hơn hoặc bỏ concept nếu chỉ là suy diễn."
        )

    if "short_answer_covers_too_few_must_have_concepts" in issue_list:
        if missing_in_short:
            actions.append(
                "Viết lại `short_answer` để cover các ý đang rơi: "
                + "; ".join(missing_in_short[:3])
            )
        actions.append(
            "Giữ `short_answer` ở dạng bản nén của `ground_truth`, không chỉ là một câu tổng quát hóa."
        )

    if not actions:
        actions.append("Record bị flag nhưng chưa có rule sửa riêng; cần review thủ công.")

    return actions


def priority_for_issues(issue_list: list[str]) -> str:
    if "must_have_concepts_weakly_supported_by_ground_truth" in issue_list:
        return "P1"
    return "P2"


def build_review_set(records: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    record_map = {record["query_id"]: record for record in records}
    flagged = report["content_quality"]["flagged_records"]

    raw_subset: list[dict[str, Any]] = []
    enriched_subset: list[dict[str, Any]] = []

    for item in flagged:
        query_id = item["query_id"]
        record = record_map[query_id]
        issues = item["issues"]
        metrics = item["metrics"]

        raw_subset.append(record)
        enriched_subset.append(
            {
                "query_id": query_id,
                "priority": priority_for_issues(issues),
                "issues": issues,
                "metrics": metrics,
                "suggested_actions": build_suggested_actions(issues, metrics),
                "record": record,
            }
        )

    return raw_subset, enriched_subset


def build_markdown_plan(enriched_subset: list[dict[str, Any]]) -> str:
    issue_counter: Counter[str] = Counter()
    must_have_batch: list[str] = []
    short_only_batch: list[str] = []
    dual_batch: list[str] = []

    for item in enriched_subset:
        for issue in item["issues"]:
            issue_counter[issue] += 1

        issue_set = set(item["issues"])
        query_id = item["query_id"]
        if "must_have_concepts_weakly_supported_by_ground_truth" in issue_set and "short_answer_covers_too_few_must_have_concepts" in issue_set:
            dual_batch.append(query_id)
        elif "must_have_concepts_weakly_supported_by_ground_truth" in issue_set:
            must_have_batch.append(query_id)
        else:
            short_only_batch.append(query_id)

    lines = [
        "# Repair Plan For Flagged 42 Records",
        "",
        "## Summary",
        f"- Total flagged records: `{len(enriched_subset)}`",
        f"- `short_answer_covers_too_few_must_have_concepts`: `{issue_counter['short_answer_covers_too_few_must_have_concepts']}`",
        f"- `must_have_concepts_weakly_supported_by_ground_truth`: `{issue_counter['must_have_concepts_weakly_supported_by_ground_truth']}`",
        f"- Dual-issue records: `{len(dual_batch)}`",
        "",
        "## Recommended Repair Order",
        "1. Batch A: sửa toàn bộ record có lỗi `must_have_concepts_weakly_supported_by_ground_truth` trước.",
        "2. Batch B: sau khi chốt lại `must_have_concepts`, mới viết lại `short_answer` cho record dual-issue.",
        "3. Batch C: cuối cùng xử lý các record chỉ lỗi `short_answer`.",
        "",
        "## Batch A — Fix `must_have_concepts` First",
        f"- Query IDs: `{must_have_batch + dual_batch}`",
        "- Rule: mọi `must_have_concepts` phải là paraphrase trực tiếp từ `ground_truth`, không được thêm kết luận mạnh hơn hoặc chi tiết chưa được nói rõ.",
        "- Với record dual-issue, không sửa `short_answer` trước khi chốt xong checklist.",
        "",
        "## Batch B — Fix Dual-Issue Records",
        f"- Query IDs: `{dual_batch}`",
        "- Rule: sửa `must_have_concepts` trước, rồi regenerate `short_answer` để cover 70-80% checklist đã sửa.",
        "",
        "## Batch C — Fix `short_answer` Only",
        f"- Query IDs: `{short_only_batch}`",
        "- Rule: giữ `short_answer` ngắn, nhưng phải giữ các boundary quan trọng như số liệu, thời gian, đối tượng, hoặc phần 'không đủ dữ liệu' nếu có.",
        "",
        "## Release Gate",
        "- `must_have_concepts_weakly_supported_by_ground_truth = 0`",
        "- `short_answer_covers_too_few_must_have_concepts <= 10`",
        "- Chỉ sau đó mới chuyển qua human approval toàn tập.",
        "",
        "## Per-Record Review Files",
        f"- Raw isolated records: `{RAW_OUTPUT.name}`",
        f"- Enriched review set with suggestions: `{ENRICHED_OUTPUT.name}`",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    records = load_json(MERGED_PATH)
    report = load_json(REPORT_PATH)

    raw_subset, enriched_subset = build_review_set(records, report)

    save_json(RAW_OUTPUT, raw_subset)
    save_json(ENRICHED_OUTPUT, enriched_subset)
    save_text(PLAN_OUTPUT, build_markdown_plan(enriched_subset))

    print(f"Flagged raw subset: {RAW_OUTPUT}")
    print(f"Flagged enriched review set: {ENRICHED_OUTPUT}")
    print(f"Repair plan: {PLAN_OUTPUT}")
    print(f"Total flagged records exported: {len(raw_subset)}")


if __name__ == "__main__":
    main()
