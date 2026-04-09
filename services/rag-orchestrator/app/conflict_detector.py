"""
Conflict Detector Heuristic (Phase 2)
=====================================
Detects potential conflicts between primary and secondary sources.
Works on normalized tags (from evidence_normalizer) and heuristically checks for mismatched intent.
"""

from app.evidence_extractor import EvidencePack


def detect_conflicts(evidence_pack: EvidencePack) -> EvidencePack:
    """Detect heuristic conflicts between primary and secondary sources."""
    
    # Initialize conflict notes list
    if not hasattr(evidence_pack, 'conflict_notes'):
        evidence_pack.conflict_notes = []

    primary = evidence_pack.primary_source
    if not primary or not evidence_pack.secondary_sources:
        return evidence_pack

    # Collect primary tags
    primary_tags = set()
    if primary.key_findings:
        for finding in primary.key_findings:
            tag = getattr(finding, "claim_type", "")
            if tag:
                primary_tags.add(tag)

    # If primary source is a guideline, track that
    primary_is_guideline = primary.source_type == "guideline"

    for idx, sec in enumerate(evidence_pack.secondary_sources):
        sec_tags = set()
        if sec.key_findings:
            for finding in sec.key_findings:
                tag = getattr(finding, "claim_type", "")
                if tag:
                    sec_tags.add(tag)
        
        # 1. Detect Polarity Mismatch: POSITIVE vs NEGATIVE vs NEUTRAL
        mismatch_found = False
        
        if ("POSITIVE_OUTCOME" in primary_tags and "NEGATIVE_OUTCOME" in sec_tags) or \
           ("NEGATIVE_OUTCOME" in primary_tags and "POSITIVE_OUTCOME" in sec_tags):
            mismatch_found = True
            evidence_pack.conflict_notes.append(
                f"Mâu thuẫn kết cục (Trái ngược) giữa tài liệu chính và phụ [{idx+2}]."
            )
        
        elif ("POSITIVE_OUTCOME" in primary_tags and "NEUTRAL_OUTCOME" in sec_tags) or \
             ("NEGATIVE_OUTCOME" in primary_tags and "NEUTRAL_OUTCOME" in sec_tags):
            mismatch_found = True
            evidence_pack.conflict_notes.append(
                f"Mâu thuẫn hiệu quả (Có/Không ý nghĩa) giữa tài liệu chính và phụ [{idx+2}]."
            )

        # 2. Detect Guideline vs Study Mismatch
        primary_recomendation = any("RECOMMENDATION" in tag for tag in primary_tags)
        sec_is_study = sec.source_type in ["original_study", "review"]
        
        if primary_is_guideline and primary_recomendation and sec_is_study:
            # If guideline has STRONG_RECOMMENDATION but study says NEGATIVE/NEUTRAL_OUTCOME
            if "STRONG_RECOMMENDATION" in primary_tags and ("NEGATIVE_OUTCOME" in sec_tags or "NEUTRAL_OUTCOME" in sec_tags):
                evidence_pack.conflict_notes.append(
                    f"Cảnh báo: Khuyến cáo mạnh từ hướng dẫn điều trị (chính) có vẻ mâu thuẫn với kết quả nghiên cứu trong tài liệu phụ [{idx+2}]."
                )
            # If guideline has NEGATIVE_RECOMMENDATION but study has POSITIVE_OUTCOME
            elif "NEGATIVE_RECOMMENDATION" in primary_tags and "POSITIVE_OUTCOME" in sec_tags:
                evidence_pack.conflict_notes.append(
                    f"Cảnh báo: Chống chỉ định/Không khuyến cáo (chính) mâu thuẫn với hiệu quả báo cáo trong nghiên cứu phụ [{idx+2}]."
                )

    return evidence_pack
