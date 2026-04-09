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
import unicodedata


@dataclass
class RouterOutput:
    query_type: str           # one of 6 types
    depth: str                # low | medium | high
    requires_numbers: bool
    requires_limitations: bool
    requires_comparison: bool
    answer_style: str         # brief | structured_study | comparative | teaching
    retrieval_profile: str    # light | standard | deep
    needs_extractor: bool     # whether to run Evidence Extractor
    retrieval_mode: str       # article_centric | topic_summary | mechanistic_synthesis


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

    # Map type → downstream signals
    _TYPE_CONFIG = {
        "fact_extraction": {
            "depth": "low",
            "requires_numbers": False,
            "requires_limitations": False,
            "requires_comparison": False,
            "answer_style": "brief",
            "retrieval_profile": "light",
            "needs_extractor": False,
            "retrieval_mode": "topic_summary",
        },
        "study_result_extraction": {
            "depth": "high",
            "requires_numbers": True,
            "requires_limitations": False,
            "requires_comparison": False,
            "answer_style": "structured_study",
            "retrieval_profile": "standard",
            "needs_extractor": True,
            "retrieval_mode": "article_centric",
        },
        "research_appraisal": {
            "depth": "high",
            "requires_numbers": True,
            "requires_limitations": True,
            "requires_comparison": False,
            "answer_style": "structured_study",
            "retrieval_profile": "deep",
            "needs_extractor": True,
            "retrieval_mode": "article_centric",
        },
        "comparative_synthesis": {
            "depth": "high",
            "requires_numbers": True,
            "requires_limitations": False,
            "requires_comparison": True,
            "answer_style": "comparative",
            "retrieval_profile": "deep",
            "needs_extractor": True,
            "retrieval_mode": "topic_summary",
        },
        "guideline_comparison": {
            "depth": "medium",
            "requires_numbers": False,
            "requires_limitations": False,
            "requires_comparison": True,
            "answer_style": "comparative",
            "retrieval_profile": "standard",
            "needs_extractor": False,
            "retrieval_mode": "topic_summary",
        },
        "teaching_explainer": {
            "depth": "medium",
            "requires_numbers": False,
            "requires_limitations": False,
            "requires_comparison": False,
            "answer_style": "teaching",
            "retrieval_profile": "standard",
            "needs_extractor": False,
            "retrieval_mode": "mechanistic_synthesis",
        },
    }

    config = _TYPE_CONFIG[best_type]

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
    )
