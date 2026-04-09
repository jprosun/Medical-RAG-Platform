"""
Mini Evidence Normalizer (Phase 2)
==================================
Standardizes common medical terminologies in extracted evidence.
Scoped narrowly to avoid complex NLP issues in Vietnamese.

Focuses on:
1. Metric names (e.g., "hazard ratio" -> "HR")
2. Polarity (e.g., "tăng nguy cơ" -> "POSITIVE_ASSOCIATION")
3. Recommendation Strength (strong/weak/conditional)
"""

import re
from app.evidence_extractor import EvidencePack

# Metric aliases mapping to canonical forms
_METRIC_MAP = {
    "hr": "HR", "hazard ratio": "HR", "tỉ số nguy cơ": "HR", "tỷ số nguy cơ": "HR",
    "or": "OR", "odds ratio": "OR", "tỉ số số chênh": "OR", "tỷ số bện": "OR",
    "rr": "RR", "relative risk": "RR", "nguy cơ tương đối": "RR",
    "auc": "AUC", "area under curve": "AUC", "diện tích dưới đường cong": "AUC",
    "p": "p-value", "p-value": "p-value", "giá trị p": "p-value",
    "độ nhạy": "sensitivity", "sensitivity": "sensitivity", "se": "sensitivity",
    "độ đặc hiệu": "specificity", "specificity": "specificity", "sp": "specificity",
    "ppv": "PPV", "giá trị tiên đoán dương": "PPV",
    "npv": "NPV", "giá trị tiên đoán âm": "NPV"
}

# Polarity patterns
_POLARITY_RULES = [
    (r"(tăng\s+nguy\s+cơ|tỷ\s+lệ\s+cao\s+hơn|yếu\s+tố\s+nguy\s+cơ|tác\s+dụng\s+phụ|biến\s+chứng|kết\s+cục\s+xấu|liên\s+quan\s+thuận|tỉ\s+lệ\s+thuận)", "NEGATIVE_OUTCOME"),
    (r"(giảm\s+nguy\s+cơ|cải\s+thiện|hiệu\s+quả|an\s+toàn|sống\s+còn|kết\s+cục\s+tốt|bảo\s+vệ|lợi\s+ích)", "POSITIVE_OUTCOME"),
    (r"(không\s+có\s+sự\s+khác\s+biệt|không\s+thấy\s+sự\s+khác\s+biệt|tương\s+đương|không\s+ảnh\s+hưởng|không\s+thay\s+đổi|không\s+liên\s+quan)", "NEUTRAL_OUTCOME"),
    (r"khuyến\s+cáo\s+mạnh", "STRONG_RECOMMENDATION"),
    (r"(khuyến\s+cáo\s+có\s+điều\s+kiện|khuyến\s+cáo\s+yếu)", "WEAK_RECOMMENDATION"),
    (r"(chống\s+chỉ\s+định|không\s+khuyến\s+cáo)", "NEGATIVE_RECOMMENDATION"),
]

def normalize_evidence(evidence_pack: EvidencePack) -> EvidencePack:
    """Normalize fields in the evidence pack in-place and return it."""
    
    # 1. Normalize numbers targeting primary source
    if evidence_pack.primary_source and evidence_pack.primary_source.numbers:
        for num_ev in evidence_pack.primary_source.numbers:
            metric_lower = num_ev.metric.lower().strip()
            # Direct map lookup
            if metric_lower in _METRIC_MAP:
                num_ev.metric = _METRIC_MAP[metric_lower]
            else:
                # Substring check for robust metric matching
                for alias, canonical in _METRIC_MAP.items():
                    if alias in metric_lower:
                        num_ev.metric = canonical
                        break

    # 2. Normalize polarity in finding claims for Conflict Detection
    if evidence_pack.primary_source and evidence_pack.primary_source.key_findings:
        for finding in evidence_pack.primary_source.key_findings:
            claim_text = finding.claim.lower()
            for pattern, tag in _POLARITY_RULES:
                if re.search(pattern, claim_text):
                    # Attach a normalized tag (claim_type) to help conflict detector
                    finding.claim_type = tag
                    break

    # Do the same for secondary sources
    for sec_source in evidence_pack.secondary_sources:
        if sec_source.key_findings:
            for finding in sec_source.key_findings:
                claim_text = finding.claim.lower()
                for pattern, tag in _POLARITY_RULES:
                    if re.search(pattern, claim_text):
                        finding.claim_type = tag
                        break

    return evidence_pack
