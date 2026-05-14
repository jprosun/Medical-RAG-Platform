"""
Vietnamese Metadata Enricher
===============================
Infers missing metadata fields for Vietnamese medical documents:
  - doc_type (guideline, review, reference, patient_education)
  - specialty (from vn_specialty_dict)
  - audience (clinician, patient, student)
  - trust_tier (1=canonical, 2=reference, 3=patient)
  - language + language_confidence + is_mixed_language
"""

from __future__ import annotations

import re
from .vn_specialty_dict import detect_specialty


# ---------- Source-based defaults ----------

_SOURCE_DEFAULTS: dict[str, dict] = {
    "kcb_moh": {
        "doc_type": "guideline",
        "audience": "clinician",
        "trust_tier": 1,
    },
    "who_vietnam": {
        "doc_type": "reference",
        "audience": "clinician",
        "trust_tier": 1,
    },
    "dav_gov": {
        "doc_type": "reference",
        "audience": "clinician",
        "trust_tier": 1,
    },
    "vmj_ojs": {
        "doc_type": "research_article",
        "audience": "clinician",
        "trust_tier": 2,
    },
    "hue_jmp_ojs": {
        "doc_type": "research_article",
        "audience": "clinician",
        "trust_tier": 2,
    },
    "mil_med_pharm_journal": {
        "doc_type": "research_article",
        "audience": "clinician",
        "trust_tier": 2,
    },
    "cantho_med_journal": {
        "doc_type": "research_article",
        "audience": "clinician",
        "trust_tier": 2,
    },
    "trad_med_pharm_journal": {
        "doc_type": "research_article",
        "audience": "clinician",
        "trust_tier": 2,
    },
}

# Vietnamese diacritics (used for language detection)
_VN_DIACRITICS = set("àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡ"
                     "ùúụủũưừứựửữỳýỵỷỹđ"
                     "ÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ"
                     "ÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ")


def enrich(
    source_id: str,
    title: str,
    body: str,
    institution: str = "",
) -> dict:
    """Infer metadata for a Vietnamese medical document.

    Args:
        source_id: Source identifier (e.g. "vmj_ojs").
        title: Extracted document title.
        body: Cleaned body text.
        institution: Institution name from YAML frontmatter.

    Returns:
        Dict with keys: doc_type, specialty, audience, trust_tier,
        language, language_confidence, is_mixed_language, source_name.
    """
    defaults = _SOURCE_DEFAULTS.get(source_id, {
        "doc_type": "reference",
        "audience": "clinician",
        "trust_tier": 2,
    })

    # --- doc_type ---
    doc_type = defaults["doc_type"]
    title_lower = (title or "").lower()
    body_lower = body[:4000].lower() if body else ""
    if "hướng dẫn chẩn đoán và điều trị" in body_lower:
        doc_type = "guideline"
    elif "hướng dẫn quy trình kỹ thuật" in body_lower:
        doc_type = "guideline"
    elif any(marker in title_lower or marker in body_lower[:1200] for marker in (
        "báo cáo ca", "bao cao ca", "trường hợp lâm sàng", "truong hop lam sang",
        "case report", "ca lâm sàng", "ca lam sang",
    )):
        doc_type = "case_report"
    elif any(marker in title_lower for marker in (
        "tổng quan", "tong quan", "cập nhật", "cap nhat", "review",
        "meta-analysis", "phân tích gộp", "phan tich gop",
    )):
        if any(marker in title_lower for marker in ("meta-analysis", "phân tích gộp", "phan tich gop")):
            doc_type = "meta_analysis"
        else:
            doc_type = "review"

    # --- specialty ---
    body_preview = body[:2000] if body else ""
    specialty = detect_specialty(title, body_preview)

    # Override specialty for specific sources
    if source_id == "dav_gov" and specialty == "general":
        specialty = "pharmacology"
    if source_id == "trad_med_pharm_journal" and specialty == "general":
        specialty = "traditional_medicine"

    # --- audience ---
    audience = defaults["audience"]

    # --- trust_tier ---
    trust_tier = defaults["trust_tier"]

    # --- language detection ---
    lang, confidence, is_mixed = _detect_language(body)

    # --- source_name ---
    source_name = institution or source_id

    return {
        "doc_type": doc_type,
        "specialty": specialty,
        "audience": audience,
        "trust_tier": trust_tier,
        "language": lang,
        "language_confidence": round(confidence, 3),
        "is_mixed_language": is_mixed,
        "source_name": source_name,
    }


def _detect_language(text: str) -> tuple[str, float, bool]:
    """Detect language of text using Vietnamese diacritic ratio.

    Returns:
        Tuple of (language, confidence, is_mixed).
        language: "vi", "en", or "mixed"
        confidence: 0.0-1.0, ratio of VN diacritics to alpha chars
        is_mixed: True if confidence is between 0.1 and 0.3
    """
    if not text:
        return "vi", 0.0, False

    # Sample text (first 3000 chars for speed)
    sample = text[:3000]
    alpha_count = sum(1 for c in sample if c.isalpha())
    if alpha_count == 0:
        return "vi", 0.0, False

    vn_count = sum(1 for c in sample if c in _VN_DIACRITICS)
    ratio = vn_count / alpha_count

    if ratio > 0.03:  # even 3% diacritics strongly indicates Vietnamese
        language = "vi"
        confidence = min(1.0, ratio * 10)  # scale up
    elif ratio > 0.005:
        language = "mixed"
        confidence = ratio * 10
    else:
        language = "en"
        confidence = 1.0 - ratio * 100

    is_mixed = 0.005 < ratio < 0.03

    return language, confidence, is_mixed
