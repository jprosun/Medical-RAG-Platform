"""
Query Router — Rule-based classifier
======================================
Classifies user queries into 6 types per review.md §4.
Pure heuristic: no LLM call, zero latency cost.

Query types:
  1. fact_extraction          — tỷ lệ, định nghĩa, tiêu chuẩn
  2. study_result_extraction  — AUC, HR, OR, sensitivity, kết quả NC
  3. research_appraisal       — hạn chế, bias, khả năng áp dụng
  4. comparative_synthesis    — so sánh 2 phương pháp/guideline
  5. guideline_comparison     — theo guideline hiện hành
  6. teaching_explainer       — giải thích cơ chế, ý nghĩa lâm sàng
"""

from __future__ import annotations
from dataclasses import dataclass
import re
import unicodedata


@dataclass
class RouterOutput:
    query_type: str           # one of 6 types
    depth: str                # low | medium | high
    requires_numbers: bool
    requires_limitations: bool
    requires_comparison: bool
    answer_style: str         # exact | summary | bounded_partial | structured_study | appraisal
    retrieval_profile: str    # light | standard | deep
    needs_extractor: bool     # whether to run Evidence Extractor
    retrieval_mode: str       # article_centric | topic_summary | mechanistic_synthesis
    answer_policy: str = "strict_rag"  # strict_rag | open_enriched


def _strip_diacritics(text: str) -> str:
    """Remove Vietnamese diacritics for fuzzy matching."""
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).replace('đ', 'd').replace('Đ', 'D')


# ── Keyword pattern sets ─────────────────────────────────────────────
# Each set includes both diacritic and non-diacritic variants

_STUDY_RESULT_KW = {
    "auc", "hr", "or", "rr", "ci", "sensitivity", "specificity",
    "độ nhạy", "do nhay", "độ đặc hiệu", "do dac hieu",
    "p-value", "p value", "p <", "p=",
    "odds ratio", "hazard ratio", "relative risk",
    "kết quả", "ket qua", "hiệu quả", "hieu qua",
    "tỷ lệ", "ty le", "trung bình", "trung binh", "mean",
    "n=", "cỡ mẫu", "co mau", "sample size",
    "có liên quan", "co lien quan",
    "yếu tố liên quan", "yeu to lien quan",
    "yếu tố nguy cơ", "yeu to nguy co",
    "tiên lượng", "tien luong", "dự báo", "du bao", "dự đoán", "du doan",
    "kết cục", "ket cuc", "outcome", "endpoint",
    "tái phát", "tai phat", "tỷ lệ sống", "ty le song",
}

_RESEARCH_APPRAISAL_KW = {
    "hạn chế", "han che", "limitation", "bias", "nhiễu", "nhieu",
    "áp dụng", "ap dung", "external validity", "applicability",
    "thiết kế nghiên cứu", "thiet ke nghien cuu", "study design",
    "đánh giá", "danh gia", "phê bình", "phe binh",
    "appraisal", "critique",
    "điểm mạnh", "diem manh", "điểm yếu", "diem yeu",
    "strength", "weakness",
    "generalizability", "confound",
    "yếu tố gây nhiễu", "yeu to gay nhieu", "biến số", "bien so",
    "phân tích kỹ", "phan tich ky", "phân tích thật kỹ", "phan tich that ky",
    "bối cảnh lâm sàng", "boi canh lam sang",
    "cơ sở sinh học", "co so sinh hoc",
    "ý nghĩa ứng dụng", "y nghia ung dung",
}

_COMPARATIVE_KW = {
    "so sánh", "so sanh", "compare", "comparison",
    "khác nhau", "khac nhau", "giống nhau", "giong nhau", "difference",
    "versus", "vs", "hay là", "hay la", "hoặc", "hoac",
    "ưu điểm", "uu diem", "nhược điểm", "nhuoc diem",
    "nào tốt hơn", "nao tot hon", "nào hiệu quả hơn", "nao hieu qua hon",
    "đối chiếu", "doi chieu",
}

_GUIDELINE_KW = {
    "guideline", "hướng dẫn", "huong dan", "khuyến cáo", "khuyen cao",
    "consensus", "đồng thuận", "dong thuan",
    "theo who", "theo bộ y tế", "theo bo y te",
    "theo aha", "theo esc",
    "phác đồ", "phac do", "protocol",
    "tiêu chuẩn chẩn đoán", "tieu chuan chan doan",
    "tiêu chuẩn điều trị", "tieu chuan dieu tri",
    "theo hiệp hội", "theo hiep hoi",
}

_TEACHING_KW = {
    "giải thích", "giai thich", "explain",
    "cơ chế", "co che", "mechanism",
    "pathophysiology", "bệnh sinh", "benh sinh",
    "sinh lý bệnh", "sinh ly benh",
    "tại sao", "tai sao", "vì sao", "vi sao", "why",
    "như thế nào", "nhu the nao", "how does",
    "ý nghĩa lâm sàng", "y nghia lam sang", "clinical significance",
    "cho sinh viên", "cho sinh vien", "cho nội trú", "cho noi tru",
    "phân tích cơ chế", "phan tich co che", "bản chất", "ban chat",
}

_FACT_KW = {
    "là gì", "la gi", "what is", "định nghĩa", "dinh nghia", "definition",
    "bao nhiêu", "bao nhieu", "how many", "how much",
    "tên gì", "ten gi", "loại nào", "loai nao", "which",
    "tiêu chuẩn", "tieu chuan", "criteria",
    "phân loại", "phan loai", "classification",
    "dấu hiệu", "dau hieu", "triệu chứng", "trieu chung",
}

_BOUNDED_PARTIAL_KW = {
    "co the ket luan", "có thể kết luận",
    "co the khang dinh", "có thể khẳng định",
    "chua the khang dinh", "chưa thể khẳng định",
    "khong the khang dinh", "không thể khẳng định",
    "vuot troi tuyet doi", "vượt trội tuyệt đối",
    "o moi khia canh", "ở mọi khía cạnh",
    "dieu gi va chua the", "điều gì và chưa thể",
}


def _count_matches(text_lower: str, keyword_set: set) -> int:
    """Count how many keywords from the set appear in text.
    Checks both original text and diacritics-stripped version.
    """
    text_stripped = _strip_diacritics(text_lower)
    count = 0
    for kw in keyword_set:
        if kw in text_lower or kw in text_stripped:
            count += 1
    return count


def _has_explicit_comparison(text_lower: str) -> bool:
    """Detect direct X-vs-Y style questions that need a comparative answer."""
    text_stripped = _strip_diacritics(text_lower)
    padded = f" {text_lower} "
    padded_stripped = f" {text_stripped} "
    direct_markers = (
        " hay ",
        " hay ",
        " hoặc ",
        " hoac ",
        " so với ",
        " so voi ",
        " vs ",
        " versus ",
        " vượt trội ",
        " vuot troi ",
        " tốt hơn ",
        " tot hon ",
        " hiệu quả hơn ",
        " hieu qua hon ",
        " khác nhau ",
        " khac nhau ",
        " giống nhau ",
        " giong nhau ",
    )
    if any(marker in padded or marker in padded_stripped for marker in direct_markers):
        return True
    direct_patterns = (
        r"\b[\w/+.-]{2,}\s+(hay|hoac|vs|versus)\s+[\w/+.-]{2,}\b",
        r"\b[\w/+.-]{2,}\s+so voi\s+[\w/+.-]{2,}\b",
        r"\b[\w/+.-]{2,}\s+vuot troi(?: tuyet doi)?\s+so voi\s+[\w/+.-]{2,}\b",
        r"\bco the ket luan\s+[\w/+.-]{2,}.*\s+so voi\s+[\w/+.-]{2,}\b",
    )
    return any(re.search(pattern, text_stripped) for pattern in direct_patterns)


def _has_direct_fact_question(text_lower: str) -> bool:
    """Detect direct-value questions that should prefer concise fact answers."""
    text_stripped = _strip_diacritics(text_lower)
    padded = f" {text_lower} "
    padded_stripped = f" {text_stripped} "
    direct_markers = (
        " bao nhiêu ",
        " bao nhieu ",
        " là gì ",
        " la gi ",
        " tên gì ",
        " ten gi ",
        " loại nào ",
        " loai nao ",
        " như thế nào ",
        " nhu the nao ",
        " yếu tố nào ",
        " yeu to nao ",
        " những nhóm yếu tố nào ",
        " nhom yeu to nao ",
    )
    if any(marker in padded or marker in padded_stripped for marker in direct_markers):
        return True
    regex_markers = (
        r"bao\s+nhi",
        r"l[aà]\s+g[iì]",
        r"nh[uư]\s+th[ếe]\s+n[aà]o",
        r"y[ếe]u\s+t[ốo]\s+n[aà]o",
    )
    return any(
        re.search(pattern, text_lower) or re.search(pattern, text_stripped)
        for pattern in regex_markers
    )


def _asks_for_numeric_value(text_lower: str) -> bool:
    text_stripped = _strip_diacritics(text_lower)
    numeric_markers = (
        " bao nhieu ",
        " bao nhiêu ",
        " tỷ lệ ",
        " ty le ",
        " phần trăm ",
        " phan tram ",
        " hr ",
        " or ",
        " auc ",
        " karnofsky ",
        " điểm ",
        " diem ",
    )
    padded = f" {text_lower} "
    padded_stripped = f" {text_stripped} "
    if any(marker in padded or marker in padded_stripped for marker in numeric_markers):
        return True
    return bool(re.search(r"\b(hr|or|auc|rr|ci)\b", text_stripped))


def _has_bounded_partial_question(text_lower: str) -> bool:
    text_stripped = _strip_diacritics(text_lower)
    return any(kw in text_lower or kw in text_stripped for kw in _BOUNDED_PARTIAL_KW)


def _has_summary_request(text_lower: str) -> bool:
    text_stripped = _strip_diacritics(text_lower)
    padded = f" {text_lower} "
    padded_stripped = f" {text_stripped} "
    markers = (
        " những biện pháp ",
        " nhung bien phap ",
        " các biện pháp ",
        " cac bien phap ",
        " gồm những ",
        " gom nhung ",
        " bao gồm ",
        " vai trò ",
        " vai tro ",
        " khác nhau như thế nào ",
        " khac nhau nhu the nao ",
        " những nhóm yếu tố nào ",
        " nhung nhom yeu to nao ",
        " yếu tố nào ",
        " yeu to nao ",
    )
    return any(marker in padded or marker in padded_stripped for marker in markers)


def _has_professional_explainer_intent(text_lower: str) -> bool:
    text_stripped = _strip_diacritics(text_lower)
    padded = f" {text_lower} "
    padded_stripped = f" {text_stripped} "
    markers = (
        " là gì ", " la gi ", " định nghĩa ", " dinh nghia ",
        " giải thích ", " giai thich ", " vì sao ", " vi sao ",
        " tại sao ", " tai sao ", " cơ chế ", " co che ",
        " bệnh sinh ", " benh sinh ", " ý nghĩa lâm sàng ", " y nghia lam sang ",
        " tổng quan ", " tong quan ",
    )
    return any(marker in padded or marker in padded_stripped for marker in markers)


def _infer_answer_style(
    query_lower: str,
    best_type: str,
    direct_fact_question: bool,
    single_document_question: bool,
) -> str:
    if _has_bounded_partial_question(query_lower):
        return "bounded_partial"
    if best_type == "research_appraisal":
        return "appraisal"
    if best_type in {"fact_extraction", "study_result_extraction"}:
        if _has_summary_request(query_lower) and not _asks_for_numeric_value(query_lower):
            return "summary"
        if direct_fact_question or _asks_for_numeric_value(query_lower):
            return "exact"
        return "summary" if single_document_question else "exact"
    if best_type in {"comparative_synthesis", "guideline_comparison", "teaching_explainer", "professional_explainer"}:
        return "summary"
    return "summary"


def _looks_single_document_question(text_lower: str) -> bool:
    """Detect queries asking for a constrained answer from one article/context."""
    text_stripped = _strip_diacritics(text_lower)
    padded = f" {text_lower} "
    padded_stripped = f" {text_stripped} "
    hints = (
        "theo context",
        "theo context",
        "từ phần tổng quan này",
        "tu phan tong quan nay",
        "theo tổng quan này",
        "theo tong quan nay",
        "trong nghiên cứu này",
        "trong nghien cuu nay",
        "nghiên cứu này",
        "nghien cuu nay",
        "theo nghiên cứu này",
        "theo nghien cuu nay",
        "bài này",
        "bai nay",
    )
    return any(hint in padded or hint in padded_stripped for hint in hints)


_OPEN_ENRICHED_TOPIC_TYPES = {
    "fact_extraction",
    "study_result_extraction",
    "research_appraisal",
    "comparative_synthesis",
    "guideline_comparison",
    "teaching_explainer",
    "professional_explainer",
}


def _should_use_open_enriched_policy(
    query_lower: str,
    best_type: str,
    single_document_question: bool,
) -> bool:
    """Use open enrichment for real user topic questions, not article-bound asks."""
    if single_document_question:
        return False
    if best_type not in _OPEN_ENRICHED_TOPIC_TYPES:
        return False
    if best_type == "study_result_extraction" and _asks_for_numeric_value(query_lower):
        return False
    return True


def route_query(query: str) -> RouterOutput:
    """
    Classify a query into one of 6 types using rule-based heuristics.
    Returns RouterOutput with all downstream control signals.
    """
    q = query.lower().strip()

    # Score each category
    scores = {
        "study_result_extraction": _count_matches(q, _STUDY_RESULT_KW),
        "research_appraisal": _count_matches(q, _RESEARCH_APPRAISAL_KW),
        "comparative_synthesis": _count_matches(q, _COMPARATIVE_KW),
        "guideline_comparison": _count_matches(q, _GUIDELINE_KW),
        "teaching_explainer": _count_matches(q, _TEACHING_KW),
        "fact_extraction": _count_matches(q, _FACT_KW),
    }

    explicit_comparison = _has_explicit_comparison(q)
    single_document_question = _looks_single_document_question(q)
    direct_fact_question = _has_direct_fact_question(q)

    if (
        not single_document_question
        and not _asks_for_numeric_value(q)
        and _has_professional_explainer_intent(q)
        and scores["guideline_comparison"] == 0
        and scores["comparative_synthesis"] == 0
    ):
        best_type = "professional_explainer"
        best_score = max(scores["teaching_explainer"], scores["fact_extraction"], 1)
    elif explicit_comparison:
        best_type = (
            "guideline_comparison"
            if scores["guideline_comparison"] > 0
            else "comparative_synthesis"
        )
        best_score = max(max(scores.values()), 1)
    elif (
        direct_fact_question
        and scores["research_appraisal"] == 0
        and scores["comparative_synthesis"] == 0
        and scores["guideline_comparison"] == 0
        and scores["teaching_explainer"] == 0
    ):
        best_type = "fact_extraction"
        best_score = max(scores["fact_extraction"], 1)
    else:
        # Pick highest scoring type; default to fact_extraction
        best_type = max(scores, key=scores.get)
        best_score = scores[best_type]

    # If no clear signal, default based on query length
    if best_score == 0:
        if len(q.split()) > 25:
            best_type = "research_appraisal"
        elif len(q.split()) > 15:
            best_type = "study_result_extraction"
        else:
            best_type = "fact_extraction"

    answer_style = _infer_answer_style(
        q,
        best_type,
        direct_fact_question,
        single_document_question,
    )

    # Map type → downstream signals
    _TYPE_CONFIG = {
        "fact_extraction": {
            "depth": "low",
            "requires_numbers": False,
            "requires_limitations": False,
            "requires_comparison": False,
            "answer_style": "exact",
            "retrieval_profile": "light",
            "needs_extractor": False,
            "retrieval_mode": "mechanistic_synthesis",
        },
        "study_result_extraction": {
            "depth": "high",
            "requires_numbers": True,
            "requires_limitations": False,
            "requires_comparison": False,
            "answer_style": "exact",
            "retrieval_profile": "standard",
            "needs_extractor": True,
            "retrieval_mode": "article_centric",
        },
        "research_appraisal": {
            "depth": "high",
            "requires_numbers": True,
            "requires_limitations": True,
            "requires_comparison": False,
            "answer_style": "appraisal",
            "retrieval_profile": "deep",
            "needs_extractor": True,
            "retrieval_mode": "article_centric",
        },
        "comparative_synthesis": {
            "depth": "high",
            "requires_numbers": True,
            "requires_limitations": False,
            "requires_comparison": True,
            "answer_style": "summary",
            "retrieval_profile": "deep",
            "needs_extractor": True,
            "retrieval_mode": "topic_summary",
        },
        "guideline_comparison": {
            "depth": "medium",
            "requires_numbers": False,
            "requires_limitations": False,
            "requires_comparison": True,
            "answer_style": "summary",
            "retrieval_profile": "standard",
            "needs_extractor": False,
            "retrieval_mode": "topic_summary",
        },
        "teaching_explainer": {
            "depth": "medium",
            "requires_numbers": False,
            "requires_limitations": False,
            "requires_comparison": False,
            "answer_style": "summary",
            "retrieval_profile": "standard",
            "needs_extractor": False,
            "retrieval_mode": "mechanistic_synthesis",
        },
        "professional_explainer": {
            "depth": "medium",
            "requires_numbers": False,
            "requires_limitations": False,
            "requires_comparison": False,
            "answer_style": "summary",
            "retrieval_profile": "standard",
            "needs_extractor": True,
            "retrieval_mode": "mechanistic_synthesis",
        },
    }

    config = dict(_TYPE_CONFIG[best_type])
    config["answer_style"] = answer_style

    answer_policy = (
        "open_enriched"
        if _should_use_open_enriched_policy(q, best_type, single_document_question)
        else "strict_rag"
    )

    if best_type == "comparative_synthesis" and single_document_question:
        config["retrieval_mode"] = "article_centric"
        config["retrieval_profile"] = "standard"
    if answer_style in {"exact", "summary", "bounded_partial"} and best_type not in {"research_appraisal", "professional_explainer"}:
        config["needs_extractor"] = False
    if answer_style == "exact" and not _asks_for_numeric_value(q):
        config["requires_numbers"] = False
    if answer_style == "summary" and best_type in {"fact_extraction", "guideline_comparison"} and single_document_question:
        config["retrieval_mode"] = "article_centric"

    if answer_policy == "open_enriched":
        if best_type in {"teaching_explainer", "professional_explainer"}:
            config["retrieval_mode"] = "mechanistic_synthesis"
        else:
            config["retrieval_mode"] = "topic_summary"
        if best_type in {"study_result_extraction", "research_appraisal", "comparative_synthesis"}:
            config["retrieval_profile"] = "deep"
            config["needs_extractor"] = True
        elif best_type in {"fact_extraction", "guideline_comparison"}:
            config["retrieval_profile"] = "standard"
        if best_type == "study_result_extraction" and not _asks_for_numeric_value(q):
            config["requires_numbers"] = False

    if single_document_question:
        config["retrieval_mode"] = "article_centric"
        if config.get("retrieval_profile") == "light":
            config["retrieval_profile"] = "standard"
        if best_type in {"study_result_extraction", "research_appraisal", "comparative_synthesis"}:
            config["retrieval_profile"] = "standard"

    return RouterOutput(
        query_type=best_type,
        depth=config["depth"],
        requires_numbers=config["requires_numbers"],
        requires_limitations=config["requires_limitations"],
        requires_comparison=config["requires_comparison"],
        answer_style=config["answer_style"],
        retrieval_profile=config["retrieval_profile"],
        needs_extractor=config["needs_extractor"],
        retrieval_mode=config["retrieval_mode"],
        answer_policy=answer_policy,
    )
