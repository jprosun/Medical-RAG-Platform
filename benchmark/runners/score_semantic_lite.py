import argparse
import json
import math
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


VI_STOPWORDS = {
    "và", "là", "của", "cho", "với", "trong", "được", "có", "không", "các", "những",
    "một", "này", "đó", "khi", "để", "theo", "tại", "ở", "về", "do", "từ", "đến",
    "thì", "lại", "cũng", "đã", "hay", "như", "trên", "sau", "trước", "giữa",
    "liệu", "rằng", "nên", "cần", "chỉ", "vì", "còn", "mà", "nếu", "đang", "được",
}
EN_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "for", "in", "on", "at", "by", "with",
    "is", "are", "was", "were", "be", "been", "being", "that", "this", "these", "those",
}
STOPWORDS = VI_STOPWORDS | EN_STOPWORDS

NEGATIVE_CUES = [
    "không có thông tin",
    "không đủ dữ liệu",
    "chưa thể",
    "không thể kết luận",
    "context chỉ",
    "bài không nêu",
    "đoạn trích không nêu",
]
NEGATION_MARKERS = [
    "không",
    "chưa",
    "không có",
    "không nêu",
    "không đủ",
    "không thể",
    "không cần",
]


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text).lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(text: str) -> list[str]:
    text = normalize_text(text)
    return re.findall(r"[\w%.,/-]+", text, flags=re.UNICODE)


def content_tokens(text: str) -> set[str]:
    tokens = tokenize(text)
    cleaned = {
        token
        for token in tokens
        if token not in STOPWORDS and len(token) > 1
    }
    return cleaned


def extract_numbers(text: str) -> list[str]:
    return re.findall(r"\d+(?:[.,]\d+)?%?", normalize_text(text))


def seq_ratio(a: str, b: str) -> float:
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)
    if not a_norm or not b_norm:
        return 0.0
    return SequenceMatcher(None, a_norm, b_norm).ratio()


def concept_overlap_score(answer: str, concept: str) -> float:
    answer_norm = normalize_text(answer)
    concept_norm = normalize_text(concept)
    if not answer_norm or not concept_norm:
        return 0.0
    if concept_norm in answer_norm:
        return 1.0

    concept_nums = extract_numbers(concept_norm)
    answer_nums = set(extract_numbers(answer_norm))
    if concept_nums:
        num_match = sum(1 for num in concept_nums if num in answer_nums) / len(concept_nums)
    else:
        num_match = 0.0

    concept_tokens = content_tokens(concept_norm)
    answer_tokens = content_tokens(answer_norm)
    if concept_tokens:
        token_coverage = len(concept_tokens & answer_tokens) / len(concept_tokens)
    else:
        token_coverage = 0.0

    similarity = seq_ratio(answer_norm, concept_norm)
    best_sentence = best_sentence_similarity(answer, concept)

    if concept_nums:
        return max(num_match, 0.55 * num_match + 0.25 * token_coverage + 0.20 * best_sentence)
    return max(token_coverage, best_sentence, similarity * 0.9)


def sentence_chunks(text: str) -> list[str]:
    if not text:
        return []
    pieces = re.split(r"[\n\r]+|(?<=[.!?])\s+", text)
    return [piece.strip(" -•\t") for piece in pieces if piece.strip()]


def best_sentence_similarity(answer: str, reference: str) -> float:
    reference_norm = normalize_text(reference)
    if not reference_norm:
        return 0.0
    best = 0.0
    for piece in sentence_chunks(answer):
        best = max(best, seq_ratio(piece, reference_norm))
    return best


def negative_cue_present(answer: str) -> bool:
    answer_norm = normalize_text(answer)
    return any(cue in answer_norm for cue in NEGATIVE_CUES)


def negation_polarity(text: str) -> bool:
    text_norm = normalize_text(text)
    return any(marker in text_norm for marker in NEGATION_MARKERS)


def sentence_concept_score(sentence: str, concept: str) -> float:
    sentence_norm = normalize_text(sentence)
    concept_norm = normalize_text(concept)
    if not sentence_norm or not concept_norm:
        return 0.0
    if concept_norm in sentence_norm:
        return 1.0
    sentence_tokens = content_tokens(sentence_norm)
    concept_tokens = content_tokens(concept_norm)
    token_coverage = (
        len(sentence_tokens & concept_tokens) / len(concept_tokens)
        if concept_tokens
        else 0.0
    )
    ratio = seq_ratio(sentence_norm, concept_norm)
    return max(token_coverage, ratio)


def must_not_violation_score(answer: str, concept: str) -> float:
    concept_norm = normalize_text(concept)
    answer_norm = normalize_text(answer)
    if not concept_norm or not answer_norm:
        return 0.0
    concept_negated = negation_polarity(concept_norm)
    best = 0.0
    for sentence in sentence_chunks(answer):
        score = sentence_concept_score(sentence, concept)
        if not score:
            continue
        sentence_negated = negation_polarity(sentence)
        if sentence_negated != concept_negated:
            score *= 0.15
        best = max(best, score)
    if concept_norm in answer_norm:
        return 1.0
    return best


def score_row(row: dict[str, Any]) -> dict[str, Any]:
    answer = row.get("answer", "")
    must_have = row.get("must_have_concepts") or []
    must_not = row.get("must_not_claim") or []
    short_answer = row.get("short_answer", "")
    ground_truth = row.get("ground_truth", "")
    answerability = row.get("answerability", "")

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
    retrieval_score = 1.0 if row.get("top1_title_hit") else 0.0

    components = []
    if must_have_soft_coverage is not None:
        components.append((0.45, must_have_soft_coverage))
    if short_answer or ground_truth:
        components.append((0.35, reference_similarity))
    components.append((0.20, retrieval_score))

    weight_sum = sum(weight for weight, _ in components) or 1.0
    base_score = sum(weight * value for weight, value in components) / weight_sum

    false_insufficiency = False
    if answerability == "partial_only":
        if negative_cue_present(answer):
            base_score += 0.08
    else:
        if negative_cue_present(answer) and (must_have_soft_coverage or 0.0) >= 0.55:
            false_insufficiency = True
            base_score -= 0.12

    if must_not_soft_violated:
        base_score -= 0.30

    semantic_lite_score = max(0.0, min(1.0, base_score))
    semantic_lite_pass = semantic_lite_score >= 0.60 and not must_not_soft_violated

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
        "negative_cue_present": negative_cue_present(answer),
        "false_insufficiency_flag": false_insufficiency,
        "semantic_lite_score": round(semantic_lite_score, 3),
        "semantic_lite_pass": semantic_lite_pass,
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    if not count:
        return {}
    success_rows = [row for row in rows if row.get("status_code") == 200]
    scores = [row["semantic_lite_score"] for row in success_rows]
    coverage = [row["must_have_soft_coverage"] for row in success_rows if row.get("must_have_soft_coverage") is not None]
    reference_similarity = [row["reference_similarity"] for row in success_rows]
    passes = [1 for row in success_rows if row["semantic_lite_pass"]]
    safe = [1 for row in success_rows if not row["must_not_soft_violated"]]
    false_insuff = [1 for row in success_rows if row["false_insufficiency_flag"]]
    return {
        "count": count,
        "http_200": len(success_rows),
        "avg_semantic_lite_score": round(sum(scores) / len(scores), 3) if scores else None,
        "semantic_lite_pass_rate": round(sum(passes) / len(success_rows), 3) if success_rows else None,
        "avg_must_have_soft_coverage": round(sum(coverage) / len(coverage), 3) if coverage else None,
        "avg_reference_similarity": round(sum(reference_similarity) / len(reference_similarity), 3) if reference_similarity else None,
        "safe_rate": round(sum(safe) / len(success_rows), 3) if success_rows else None,
        "false_insufficiency_rate": round(sum(false_insuff) / len(success_rows), 3) if success_rows else None,
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
    Path(args.detail_output).write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in scored) + "\n",
        encoding="utf-8",
    )

    grouped = {
        "dev": [row for row in scored if row.get("split") == "dev"],
        "test": [row for row in scored if row.get("split") == "test"],
        "holdout": [row for row in scored if row.get("split") == "holdout"],
        "full_gold": scored,
    }
    summaries = {name: summarize(rows) for name, rows in grouped.items()}

    lowest = sorted(
        [row for row in scored if row.get("status_code") == 200],
        key=lambda row: row.get("semantic_lite_score", math.inf),
    )[:20]
    false_insuff = [row for row in scored if row.get("false_insufficiency_flag")]
    must_not_fail = [row for row in scored if row.get("must_not_soft_violated")]

    lines = [
        "# Semantic-Lite Summary",
        "",
        "- Scoring method: soft concept match + short_answer/ground_truth similarity + retrieval hit + boundary penalties",
        "- This is stricter than raw substring match, but still lighter than an LLM judge.",
        "",
        "## Metrics",
        "",
        "| Split | Count | HTTP 200 | Avg Semantic-Lite Score | Semantic-Lite Pass Rate | Avg Must-Have Soft Coverage | Avg Reference Similarity | Safe Rate | False Insufficiency Rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for split_name in ["dev", "test", "holdout", "full_gold"]:
        summary = summaries[split_name]
        lines.append(
            "| "
            + split_name
            + " | "
            + str(summary.get("count"))
            + " | "
            + str(summary.get("http_200"))
            + " | "
            + str(summary.get("avg_semantic_lite_score"))
            + " | "
            + str(summary.get("semantic_lite_pass_rate"))
            + " | "
            + str(summary.get("avg_must_have_soft_coverage"))
            + " | "
            + str(summary.get("avg_reference_similarity"))
            + " | "
            + str(summary.get("safe_rate"))
            + " | "
            + str(summary.get("false_insufficiency_rate"))
            + " |"
        )

    lines.extend(["", "## Lowest 20 Semantic-Lite Scores", ""])
    for row in lowest:
        lines.append(
            "- "
            + str(row.get("split"))
            + " "
            + str(row.get("query_id"))
            + " score="
            + str(row.get("semantic_lite_score"))
            + " ref_sim="
            + str(row.get("reference_similarity"))
            + " coverage="
            + str(row.get("must_have_soft_coverage"))
        )

    lines.extend(["", "## False Insufficiency Flags", ""])
    if false_insuff:
        for row in false_insuff[:20]:
            lines.append(
                "- "
                + str(row.get("split"))
                + " "
                + str(row.get("query_id"))
                + " score="
                + str(row.get("semantic_lite_score"))
            )
    else:
        lines.append("- None")

    lines.extend(["", "## Must-Not Soft Violations", ""])
    if must_not_fail:
        for row in must_not_fail[:20]:
            hits = ", ".join(item["concept"] for item in row.get("must_not_soft_hits") or [])
            lines.append("- " + str(row.get("split")) + " " + str(row.get("query_id")) + " hits=" + hits)
    else:
        lines.append("- None")

    Path(args.summary_output).write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
