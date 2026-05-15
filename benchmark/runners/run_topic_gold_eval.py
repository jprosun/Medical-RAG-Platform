import argparse
import json
import re
import time
import unicodedata
from pathlib import Path
from typing import Any

import requests


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text).lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def load_records(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("records") or data.get("items") or []
    return list(data)


def answer_word_count(answer: str) -> int:
    return len(re.findall(r"\w+", answer or "", flags=re.UNICODE))


def collect_phrase_hits(answer: str, phrases: list[str]) -> list[str]:
    answer_norm = normalize_text(answer)
    hits = []
    for phrase in phrases:
        phrase_norm = normalize_text(str(phrase))
        if phrase_norm and phrase_norm in answer_norm:
            hits.append(phrase)
    return hits


def retrieval_text(response_json: dict[str, Any]) -> str:
    chunks = response_json.get("retrieved_chunks") or []
    parts: list[str] = []
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        metadata = chunk.get("metadata") or {}
        for key in ("title", "canonical_title", "section_title", "source_name", "doc_type", "specialty"):
            value = metadata.get(key)
            if value:
                parts.append(str(value))
        text = chunk.get("text")
        if text:
            parts.append(str(text)[:800])
    return " ".join(parts)


def topic_source_hit_rate(response_json: dict[str, Any], keywords: list[str]) -> float | None:
    if not keywords:
        return None
    haystack = normalize_text(retrieval_text(response_json))
    if not haystack:
        return 0.0
    hits = 0
    for keyword in keywords:
        keyword_norm = normalize_text(str(keyword))
        if keyword_norm and keyword_norm in haystack:
            hits += 1
    return hits / len(keywords)


def top_titles(response_json: dict[str, Any], limit: int = 8) -> list[str]:
    titles: list[str] = []
    seen = set()
    for chunk in response_json.get("retrieved_chunks") or []:
        if not isinstance(chunk, dict):
            continue
        metadata = chunk.get("metadata") or {}
        title = str(metadata.get("title") or metadata.get("canonical_title") or "").strip()
        if title and title not in seen:
            seen.add(title)
            titles.append(title)
        if len(titles) >= limit:
            break
    return titles


def evaluate_record(base_url: str, timeout: int, row: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "message": row.get("question", ""),
        "session_id": f"topic-gold-v2-{row.get('split', 'unknown')}-{row.get('query_id', 'unknown')}",
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
        metadata = data.get("metadata") if isinstance(data, dict) else {}
        metadata = metadata if isinstance(metadata, dict) else {}
        must_have = row.get("must_have_concepts") or []
        must_not = row.get("must_not_claim") or []
        must_have_hits = collect_phrase_hits(answer, must_have)
        must_not_hits = collect_phrase_hits(answer, must_not)
        min_words = int(row.get("minimum_answer_words") or 250)
        words = answer_word_count(answer)

        return {
            **row,
            "status_code": response.status_code,
            "latency_ms": latency_ms,
            "answer": answer,
            "answer_words": words,
            "under_min_length": words < min_words,
            "degraded_mode": bool(data.get("degraded_mode")) if isinstance(data, dict) else False,
            "degraded_reason": data.get("degraded_reason") if isinstance(data, dict) else None,
            "metadata": metadata,
            "query_type": metadata.get("query_type"),
            "answer_policy": metadata.get("answer_policy"),
            "retrieval_mode": metadata.get("retrieval_mode"),
            "coverage_level": metadata.get("coverage_level"),
            "coverage_mode": metadata.get("coverage_mode"),
            "verification_status": metadata.get("verification_status"),
            "external_search_status": metadata.get("external_search_status"),
            "retrieved_chunks_count": len(data.get("retrieved_chunks") or []) if isinstance(data, dict) else 0,
            "topic_source_hit_rate": topic_source_hit_rate(data, row.get("retrieval_keywords") or []) if isinstance(data, dict) else 0.0,
            "top_titles": top_titles(data) if isinstance(data, dict) else [],
            "must_have_hits": must_have_hits,
            "must_have_hit_rate": (len(must_have_hits) / len(must_have)) if must_have else None,
            "must_not_hits": must_not_hits,
            "must_not_violated": bool(must_not_hits),
            "external_sources": data.get("external_sources") if isinstance(data, dict) else [],
            "error_body_excerpt": body_text[:500] if response.status_code != 200 else "",
        }
    except Exception as exc:
        latency_ms = round((time.time() - start) * 1000, 1)
        return {
            **row,
            "status_code": None,
            "latency_ms": latency_ms,
            "answer": "",
            "answer_words": 0,
            "under_min_length": True,
            "degraded_mode": False,
            "degraded_reason": None,
            "metadata": {},
            "query_type": None,
            "answer_policy": None,
            "retrieval_mode": None,
            "coverage_level": None,
            "coverage_mode": None,
            "verification_status": None,
            "external_search_status": None,
            "retrieved_chunks_count": 0,
            "topic_source_hit_rate": 0.0,
            "top_titles": [],
            "must_have_hits": [],
            "must_have_hit_rate": 0.0,
            "must_not_hits": [],
            "must_not_violated": False,
            "external_sources": [],
            "error": str(exc),
            "error_body_excerpt": "",
        }


def average(values: list[float | int]) -> float | None:
    if not values:
        return None
    return round(sum(float(value) for value in values) / len(values), 3)


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    ok_rows = [row for row in rows if row.get("status_code") == 200]
    if not count:
        return {}
    return {
        "count": count,
        "http_200": len(ok_rows),
        "http_200_rate": round(len(ok_rows) / count, 3),
        "degraded_rate": average([1 if row.get("degraded_mode") else 0 for row in ok_rows]),
        "avg_latency_ms": average([row["latency_ms"] for row in rows if isinstance(row.get("latency_ms"), (int, float))]),
        "avg_answer_words": average([row.get("answer_words", 0) for row in ok_rows]),
        "under_min_length_rate": average([1 if row.get("under_min_length") else 0 for row in ok_rows]),
        "avg_retrieved_chunks": average([row.get("retrieved_chunks_count", 0) for row in ok_rows]),
        "avg_topic_source_hit_rate": average([
            row["topic_source_hit_rate"]
            for row in ok_rows
            if row.get("topic_source_hit_rate") is not None
        ]),
        "avg_must_have_hit_rate": average([
            row["must_have_hit_rate"]
            for row in ok_rows
            if row.get("must_have_hit_rate") is not None
        ]),
        "must_not_violation_rate": average([1 if row.get("must_not_violated") else 0 for row in ok_rows]),
        "open_enriched_rate": average([1 if row.get("answer_policy") == "open_enriched" else 0 for row in ok_rows]),
        "article_centric_rate": average([1 if row.get("retrieval_mode") == "article_centric" else 0 for row in ok_rows]),
    }


def write_summary(path: Path, rows: list[dict[str, Any]], base_url: str) -> None:
    split_names = sorted({row.get("split", "unknown") for row in rows})
    grouped = {split: [row for row in rows if row.get("split", "unknown") == split] for split in split_names}
    grouped["full_gold"] = rows
    summaries = {name: build_summary(items) for name, items in grouped.items()}

    low_topic = [
        row for row in rows
        if row.get("status_code") == 200 and (row.get("topic_source_hit_rate") or 0.0) < 0.4
    ]
    low_length = [
        row for row in rows
        if row.get("status_code") == 200 and row.get("under_min_length")
    ]
    article_mode = [
        row for row in rows
        if row.get("status_code") == 200 and row.get("retrieval_mode") == "article_centric"
    ]
    non_200 = [row for row in rows if row.get("status_code") != 200]

    lines = [
        "# Topic Gold v2 Evaluation Summary",
        "",
        f"- Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Base URL: {base_url}",
        "- Intent: realistic topic/professional medical questions, not article-title guessing.",
        "",
        "## Metrics",
        "",
        "| Split | Count | HTTP 200 | HTTP 200 Rate | Open Enriched Rate | Article-Centric Rate | Avg Topic Source Hit | Avg Must-Have Hit | Avg Words | Under Min Length | Avg Latency ms |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name in [*split_names, "full_gold"]:
        summary = summaries[name]
        lines.append(
            f"| {name} | {summary.get('count')} | {summary.get('http_200')} | "
            f"{summary.get('http_200_rate')} | {summary.get('open_enriched_rate')} | "
            f"{summary.get('article_centric_rate')} | {summary.get('avg_topic_source_hit_rate')} | "
            f"{summary.get('avg_must_have_hit_rate')} | {summary.get('avg_answer_words')} | "
            f"{summary.get('under_min_length_rate')} | {summary.get('avg_latency_ms')} |"
        )

    lines.extend(["", "## Non-200 Cases", ""])
    if non_200:
        for row in non_200[:20]:
            lines.append(f"- {row.get('split')} {row.get('query_id')} status={row.get('status_code')} error={row.get('error_body_excerpt') or row.get('error')}")
    else:
        lines.append("- None")

    lines.extend(["", "## Low Topic Source Hit (< 0.4)", ""])
    if low_topic:
        for row in low_topic[:20]:
            titles = " | ".join(row.get("top_titles") or [])
            lines.append(f"- {row.get('split')} {row.get('query_id')} hit={row.get('topic_source_hit_rate')} titles={titles}")
    else:
        lines.append("- None")

    lines.extend(["", "## Under Minimum Length", ""])
    if low_length:
        for row in low_length[:20]:
            lines.append(f"- {row.get('split')} {row.get('query_id')} words={row.get('answer_words')} min={row.get('minimum_answer_words')}")
    else:
        lines.append("- None")

    lines.extend(["", "## Unexpected Article-Centric Routing", ""])
    if article_mode:
        for row in article_mode[:20]:
            lines.append(f"- {row.get('split')} {row.get('query_id')} query_type={row.get('query_type')} question={row.get('question')}")
    else:
        lines.append("- None")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/api/chat")
    parser.add_argument("--dataset-file", default="benchmark/datasets/medqa_topic_gold_v2.jsonl")
    parser.add_argument("--split", default="all", help="all, dev, test, or holdout")
    parser.add_argument("--raw-output", required=True)
    parser.add_argument("--summary-output", required=True)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    args = parser.parse_args()

    records = load_records(Path(args.dataset_file))
    if args.split != "all":
        records = [row for row in records if row.get("split") == args.split]

    results: list[dict[str, Any]] = []
    for index, row in enumerate(records, start=1):
        result = evaluate_record(args.base_url, args.timeout, row)
        results.append(result)
        print(
            f"{index}/{len(records)}",
            result.get("split"),
            result.get("query_id"),
            result.get("status_code"),
            result.get("latency_ms"),
            "policy=" + str(result.get("answer_policy")),
            "mode=" + str(result.get("retrieval_mode")),
            flush=True,
        )
        time.sleep(args.sleep_seconds)

    raw_output = Path(args.raw_output)
    raw_output.parent.mkdir(parents=True, exist_ok=True)
    raw_output.write_text(
        "\n".join(json.dumps(item, ensure_ascii=False) for item in results) + "\n",
        encoding="utf-8",
    )

    summary_output = Path(args.summary_output)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    write_summary(summary_output, results, args.base_url)

    print(f"WROTE_RAW {raw_output}")
    print(f"WROTE_SUMMARY {summary_output}")


if __name__ == "__main__":
    main()
