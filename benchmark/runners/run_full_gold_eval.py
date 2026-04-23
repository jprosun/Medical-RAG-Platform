import argparse
import json
import re
import time
from pathlib import Path
from typing import Any

import requests


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def load_split(split_name: str, path: Path) -> list[dict[str, Any]]:
    items = json.loads(path.read_text(encoding="utf-8"))
    for item in items:
        item["_split_name"] = split_name
    return items


def extract_top1_title(response_json: dict[str, Any]) -> str:
    chunks = response_json.get("retrieved_chunks") or []
    if not chunks:
        return ""
    metadata = (chunks[0] or {}).get("metadata") or {}
    return metadata.get("title") or ""


def title_hit(gold_title: str, predicted_title: str) -> bool:
    gold_norm = normalize_text(gold_title)
    pred_norm = normalize_text(predicted_title)
    if not gold_norm or not pred_norm:
        return False
    return gold_norm == pred_norm or gold_norm in pred_norm or pred_norm in gold_norm


def collect_phrase_hits(answer: str, phrases: list[str]) -> list[str]:
    answer_norm = normalize_text(answer)
    hits = []
    for phrase in phrases:
        phrase_norm = normalize_text(str(phrase))
        if phrase_norm and phrase_norm in answer_norm:
            hits.append(phrase)
    return hits


def evaluate_record(base_url: str, timeout: int, row: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "message": row.get("question", ""),
        "session_id": f"eval-{row.get('_split_name', 'unknown')}-{row.get('query_id', 'unknown')}",
    }
    start = time.time()
    try:
        response = requests.post(base_url, json=payload, timeout=timeout)
        latency_ms = round((time.time() - start) * 1000, 1)
        body_text = response.text
        try:
            data = response.json()
        except Exception:
            data = {}
        answer = data.get("answer", "") if isinstance(data, dict) else ""
        top1 = extract_top1_title(data) if isinstance(data, dict) else ""
        must_have = row.get("must_have_concepts") or []
        must_not = row.get("must_not_claim") or []
        must_have_hits = collect_phrase_hits(answer, must_have)
        must_not_hits = collect_phrase_hits(answer, must_not)
        return {
            "split": row.get("_split_name"),
            "query_id": row.get("query_id"),
            "question": row.get("question"),
            "expected_behavior": row.get("expected_behavior"),
            "answerability": row.get("answerability"),
            "status_code": response.status_code,
            "latency_ms": latency_ms,
            "degraded_mode": bool(data.get("degraded_mode")) if isinstance(data, dict) else False,
            "degraded_reason": data.get("degraded_reason") if isinstance(data, dict) else None,
            "title": row.get("title", ""),
            "ground_truth": row.get("ground_truth", ""),
            "short_answer": row.get("short_answer", ""),
            "top1_title": top1,
            "top1_title_hit": title_hit(row.get("title", ""), top1),
            "must_have_concepts": must_have,
            "must_have_hits": must_have_hits,
            "must_have_hit_rate": (len(must_have_hits) / len(must_have)) if must_have else None,
            "must_not_claim": must_not,
            "must_not_hits": must_not_hits,
            "must_not_violated": bool(must_not_hits),
            "answer": answer,
            "retrieved_chunks_count": len(data.get("retrieved_chunks") or []) if isinstance(data, dict) else 0,
            "error_body_excerpt": body_text[:500] if response.status_code != 200 else "",
        }
    except Exception as exc:
        latency_ms = round((time.time() - start) * 1000, 1)
        return {
            "split": row.get("_split_name"),
            "query_id": row.get("query_id"),
            "question": row.get("question"),
            "expected_behavior": row.get("expected_behavior"),
            "answerability": row.get("answerability"),
            "status_code": None,
            "latency_ms": latency_ms,
            "degraded_mode": False,
            "degraded_reason": None,
            "title": row.get("title", ""),
            "ground_truth": row.get("ground_truth", ""),
            "short_answer": row.get("short_answer", ""),
            "top1_title": "",
            "top1_title_hit": False,
            "must_have_concepts": row.get("must_have_concepts") or [],
            "must_have_hits": [],
            "must_have_hit_rate": 0.0,
            "must_not_claim": row.get("must_not_claim") or [],
            "must_not_hits": [],
            "must_not_violated": False,
            "answer": "",
            "retrieved_chunks_count": 0,
            "error": str(exc),
            "error_body_excerpt": "",
        }


def build_summary(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    count = len(rows)
    ok_rows = [row for row in rows if row.get("status_code") == 200]
    degraded_rows = [row for row in rows if row.get("degraded_mode")]
    latencies = [row["latency_ms"] for row in rows if isinstance(row.get("latency_ms"), (int, float))]
    title_hits = [1 for row in ok_rows if row.get("top1_title_hit")]
    must_have_rates = [
        row["must_have_hit_rate"]
        for row in ok_rows
        if row.get("must_have_hit_rate") is not None
    ]
    must_not_flags = [1 for row in ok_rows if row.get("must_not_violated")]
    failures = [row for row in rows if row.get("status_code") != 200]
    failure_ids = [f"{row.get('split')} {row.get('query_id')} {row.get('status_code')}" for row in failures[:10]]
    return (
        {
            "count": count,
            "http_200": len(ok_rows),
            "http_200_rate": (len(ok_rows) / count) if count else 0.0,
            "degraded": len(degraded_rows),
            "degraded_rate": (len(degraded_rows) / count) if count else 0.0,
            "avg_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else None,
            "top1_title_hit_rate": round(sum(title_hits) / len(ok_rows), 3) if ok_rows else None,
            "avg_must_have_hit_rate": round(sum(must_have_rates) / len(must_have_rates), 3) if must_have_rates else None,
            "must_not_violation_rate": round(sum(must_not_flags) / len(ok_rows), 3) if ok_rows else None,
            "first_failures": failure_ids,
        },
        failure_ids,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/api/chat")
    parser.add_argument("--dev-file", required=True)
    parser.add_argument("--test-file", required=True)
    parser.add_argument("--holdout-file", required=True)
    parser.add_argument("--raw-output", required=True)
    parser.add_argument("--summary-output", required=True)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    args = parser.parse_args()

    split_files = [
        ("dev", Path(args.dev_file)),
        ("test", Path(args.test_file)),
        ("holdout", Path(args.holdout_file)),
    ]

    all_rows: list[dict[str, Any]] = []
    for split_name, path in split_files:
        all_rows.extend(load_split(split_name, path))

    results: list[dict[str, Any]] = []
    for index, row in enumerate(all_rows, start=1):
        result = evaluate_record(args.base_url, args.timeout, row)
        results.append(result)
        print(
            f"{index}/{len(all_rows)}",
            result.get("split"),
            result.get("query_id"),
            result.get("status_code"),
            result.get("latency_ms"),
            flush=True,
        )
        time.sleep(args.sleep_seconds)

    raw_output = Path(args.raw_output)
    raw_output.write_text(
        "\n".join(json.dumps(item, ensure_ascii=False) for item in results) + "\n",
        encoding="utf-8",
    )

    grouped = {
        "dev": [row for row in results if row["split"] == "dev"],
        "test": [row for row in results if row["split"] == "test"],
        "holdout": [row for row in results if row["split"] == "holdout"],
        "full_gold": results,
    }
    summaries = {name: build_summary(rows)[0] for name, rows in grouped.items()}

    low_coverage = [
        row for row in results
        if row.get("status_code") == 200 and row.get("must_have_hit_rate") is not None and row["must_have_hit_rate"] < 0.5
    ]
    must_not_failures = [row for row in results if row.get("status_code") == 200 and row.get("must_not_violated")]
    retrieval_misses = [row for row in results if row.get("status_code") == 200 and not row.get("top1_title_hit")]
    non_200 = [row for row in results if row.get("status_code") != 200]
    slowest = sorted(results, key=lambda row: row.get("latency_ms") or 0, reverse=True)[:15]

    lines = [
        "# Full Gold vs Test Summary",
        "",
        f"- Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Base URL: {args.base_url}",
        "",
        "## Metrics",
        "",
        "| Split | Count | HTTP 200 | HTTP 200 Rate | Degraded | Degraded Rate | Avg Latency (ms) | Top1 Title Hit Rate (200 only) | Avg Must-Have Hit Rate (200 only) | Must-Not Violation Rate (200 only) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for split_name in ["dev", "test", "holdout", "full_gold"]:
        summary = summaries[split_name]
        lines.append(
            "| "
            + split_name
            + " | "
            + str(summary["count"])
            + " | "
            + str(summary["http_200"])
            + " | "
            + f"{summary['http_200_rate']:.1%}"
            + " | "
            + str(summary["degraded"])
            + " | "
            + f"{summary['degraded_rate']:.1%}"
            + " | "
            + str(summary["avg_latency_ms"])
            + " | "
            + str(summary["top1_title_hit_rate"])
            + " | "
            + str(summary["avg_must_have_hit_rate"])
            + " | "
            + str(summary["must_not_violation_rate"])
            + " |"
        )

    lines.extend(["", "## Non-200 Cases", ""])
    if non_200:
        for row in non_200[:30]:
            lines.append(
                "- "
                + str(row.get("split"))
                + " "
                + str(row.get("query_id"))
                + " status="
                + str(row.get("status_code"))
                + " error="
                + str(row.get("error_body_excerpt") or row.get("error", ""))
            )
    else:
        lines.append("- None")

    lines.extend(["", "## Retrieval Misses (Top1 Title, 200 only)", ""])
    if retrieval_misses:
        for row in retrieval_misses[:30]:
            lines.append(
                "- "
                + str(row.get("split"))
                + " "
                + str(row.get("query_id"))
                + " gold="
                + str(row.get("title"))
                + " top1="
                + str(row.get("top1_title"))
            )
    else:
        lines.append("- None")

    lines.extend(["", "## Low Must-Have Coverage (< 0.5, 200 only)", ""])
    if low_coverage:
        for row in low_coverage[:30]:
            lines.append(
                "- "
                + str(row.get("split"))
                + " "
                + str(row.get("query_id"))
                + " hit_rate="
                + str(row.get("must_have_hit_rate"))
                + " question="
                + str(row.get("question"))
            )
    else:
        lines.append("- None")

    lines.extend(["", "## Must-Not Violations (200 only)", ""])
    if must_not_failures:
        for row in must_not_failures[:30]:
            lines.append(
                "- "
                + str(row.get("split"))
                + " "
                + str(row.get("query_id"))
                + " hits="
                + ", ".join(str(item) for item in row.get("must_not_hits") or [])
            )
    else:
        lines.append("- None")

    lines.extend(["", "## Slowest 15", ""])
    for row in slowest:
        lines.append(
            "- "
            + str(row.get("split"))
            + " "
            + str(row.get("query_id"))
            + " latency_ms="
            + str(row.get("latency_ms"))
            + " status="
            + str(row.get("status_code"))
        )

    Path(args.summary_output).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"WROTE_RAW {raw_output}")
    print(f"WROTE_SUMMARY {args.summary_output}")


if __name__ == "__main__":
    main()
