import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from score_semantic_lite import (  # noqa: E402
    best_sentence_similarity,
    concept_overlap_score,
    must_not_violation_score,
    negative_cue_present,
)


def answer_length_score(answer_words: int, minimum_words: int) -> float:
    if minimum_words <= 0:
        return 1.0
    return max(0.0, min(1.0, answer_words / minimum_words))


def score_row(row: dict[str, Any]) -> dict[str, Any]:
    answer = row.get("answer", "")
    must_have = row.get("must_have_concepts") or []
    must_not = row.get("must_not_claim") or []
    short_answer = row.get("short_answer", "")
    ground_truth = row.get("ground_truth", "")
    expected_policy = row.get("expected_policy") or "open_enriched"

    must_have_scores = [
        {
            "concept": concept,
            "score": round(concept_overlap_score(answer, concept), 3),
        }
        for concept in must_have
    ]
    must_have_soft_coverage = (
        sum(item["score"] for item in must_have_scores) / len(must_have_scores)
        if must_have_scores
        else None
    )

    must_not_scores = [
        {
            "concept": concept,
            "score": round(must_not_violation_score(answer, concept), 3),
        }
        for concept in must_not
    ]
    must_not_soft_hits = [item for item in must_not_scores if item["score"] >= 0.88]
    must_not_soft_violated = bool(must_not_soft_hits)

    short_similarity = best_sentence_similarity(answer, short_answer)
    truth_similarity = best_sentence_similarity(answer, ground_truth)
    reference_similarity = max(short_similarity, truth_similarity)
    topic_source_score = float(row.get("topic_source_hit_rate") or 0.0)
    length_score = answer_length_score(
        int(row.get("answer_words") or 0),
        int(row.get("minimum_answer_words") or 250),
    )

    components: list[tuple[float, float]] = []
    if must_have_soft_coverage is not None:
        components.append((0.50, must_have_soft_coverage))
    if short_answer or ground_truth:
        components.append((0.25, reference_similarity))
    components.append((0.15, topic_source_score))
    components.append((0.10, length_score))

    weight_sum = sum(weight for weight, _ in components) or 1.0
    base_score = sum(weight * value for weight, value in components) / weight_sum

    false_insufficiency = False
    if negative_cue_present(answer) and (must_have_soft_coverage or 0.0) >= 0.55:
        false_insufficiency = True
        base_score -= 0.10
    if expected_policy == "open_enriched" and row.get("answer_policy") != "open_enriched":
        base_score -= 0.08
    if expected_policy == "open_enriched" and row.get("retrieval_mode") == "article_centric":
        base_score -= 0.05
    if row.get("degraded_mode"):
        base_score -= 0.05
    if must_not_soft_violated:
        base_score -= 0.30

    topic_gold_score = max(0.0, min(1.0, base_score))
    topic_gold_pass = topic_gold_score >= 0.62 and not must_not_soft_violated

    return {
        **row,
        "must_have_soft_scores": must_have_scores,
        "must_have_soft_coverage": round(must_have_soft_coverage, 3) if must_have_soft_coverage is not None else None,
        "must_not_soft_scores": must_not_scores,
        "must_not_soft_hits": must_not_soft_hits,
        "must_not_soft_violated": must_not_soft_violated,
        "short_answer_similarity": round(short_similarity, 3),
        "ground_truth_similarity": round(truth_similarity, 3),
        "reference_similarity": round(reference_similarity, 3),
        "topic_source_score": round(topic_source_score, 3),
        "answer_length_score": round(length_score, 3),
        "negative_cue_present": negative_cue_present(answer),
        "false_insufficiency_flag": false_insufficiency,
        "topic_gold_score": round(topic_gold_score, 3),
        "topic_gold_pass": topic_gold_pass,
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    if not count:
        return {}
    success_rows = [row for row in rows if row.get("status_code") == 200]
    scores = [row["topic_gold_score"] for row in success_rows]
    coverage = [row["must_have_soft_coverage"] for row in success_rows if row.get("must_have_soft_coverage") is not None]
    references = [row["reference_similarity"] for row in success_rows]
    topic_hits = [row["topic_source_score"] for row in success_rows]
    length_scores = [row["answer_length_score"] for row in success_rows]
    passes = [1 for row in success_rows if row["topic_gold_pass"]]
    safe = [1 for row in success_rows if not row["must_not_soft_violated"]]
    open_policy = [1 for row in success_rows if row.get("answer_policy") == "open_enriched"]
    article_mode = [1 for row in success_rows if row.get("retrieval_mode") == "article_centric"]
    return {
        "count": count,
        "http_200": len(success_rows),
        "avg_topic_gold_score": round(sum(scores) / len(scores), 3) if scores else None,
        "topic_gold_pass_rate": round(sum(passes) / len(success_rows), 3) if success_rows else None,
        "avg_must_have_soft_coverage": round(sum(coverage) / len(coverage), 3) if coverage else None,
        "avg_reference_similarity": round(sum(references) / len(references), 3) if references else None,
        "avg_topic_source_score": round(sum(topic_hits) / len(topic_hits), 3) if topic_hits else None,
        "avg_answer_length_score": round(sum(length_scores) / len(length_scores), 3) if length_scores else None,
        "safe_rate": round(sum(safe) / len(success_rows), 3) if success_rows else None,
        "open_enriched_rate": round(sum(open_policy) / len(success_rows), 3) if success_rows else None,
        "article_centric_rate": round(sum(article_mode) / len(success_rows), 3) if success_rows else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-file", required=True)
    parser.add_argument("--detail-output", required=True)
    parser.add_argument("--summary-output", required=True)
    args = parser.parse_args()

    raw_rows = [
        json.loads(line)
        for line in Path(args.raw_file).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    scored = [score_row(row) for row in raw_rows]
    Path(args.detail_output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.detail_output).write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in scored) + "\n",
        encoding="utf-8",
    )

    split_names = sorted({row.get("split", "unknown") for row in scored})
    grouped = {split: [row for row in scored if row.get("split", "unknown") == split] for split in split_names}
    grouped["full_gold"] = scored
    summaries = {name: summarize(rows) for name, rows in grouped.items()}

    lowest = sorted(
        [row for row in scored if row.get("status_code") == 200],
        key=lambda row: row.get("topic_gold_score", math.inf),
    )[:20]
    must_not_fail = [row for row in scored if row.get("must_not_soft_violated")]
    article_mode = [
        row for row in scored
        if row.get("status_code") == 200 and row.get("retrieval_mode") == "article_centric"
    ]

    lines = [
        "# Topic Gold v2 Semantic Summary",
        "",
        "- Scoring method: soft concept coverage + reference similarity + topic-source hit + answer length + safety/policy penalties.",
        "- This benchmark is for realistic topic/professional UX, not article-title hit.",
        "",
        "## Metrics",
        "",
        "| Split | Count | HTTP 200 | Avg Topic Score | Pass Rate | Avg Must-Have Coverage | Avg Reference Similarity | Avg Topic Source | Avg Length | Safe Rate | Open Enriched | Article-Centric |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name in [*split_names, "full_gold"]:
        summary = summaries[name]
        lines.append(
            f"| {name} | {summary.get('count')} | {summary.get('http_200')} | "
            f"{summary.get('avg_topic_gold_score')} | {summary.get('topic_gold_pass_rate')} | "
            f"{summary.get('avg_must_have_soft_coverage')} | {summary.get('avg_reference_similarity')} | "
            f"{summary.get('avg_topic_source_score')} | {summary.get('avg_answer_length_score')} | "
            f"{summary.get('safe_rate')} | {summary.get('open_enriched_rate')} | {summary.get('article_centric_rate')} |"
        )

    lines.extend(["", "## Lowest 20 Topic Scores", ""])
    for row in lowest:
        lines.append(
            "- "
            + str(row.get("split"))
            + " "
            + str(row.get("query_id"))
            + " score="
            + str(row.get("topic_gold_score"))
            + " topic="
            + str(row.get("topic_source_score"))
            + " coverage="
            + str(row.get("must_have_soft_coverage"))
            + " length="
            + str(row.get("answer_length_score"))
        )

    lines.extend(["", "## Must-Not Soft Violations", ""])
    if must_not_fail:
        for row in must_not_fail[:20]:
            hits = ", ".join(item["concept"] for item in row.get("must_not_soft_hits") or [])
            lines.append("- " + str(row.get("split")) + " " + str(row.get("query_id")) + " hits=" + hits)
    else:
        lines.append("- None")

    lines.extend(["", "## Unexpected Article-Centric Routing", ""])
    if article_mode:
        for row in article_mode[:20]:
            lines.append("- " + str(row.get("split")) + " " + str(row.get("query_id")) + " query_type=" + str(row.get("query_type")))
    else:
        lines.append("- None")

    Path(args.summary_output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary_output).write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
