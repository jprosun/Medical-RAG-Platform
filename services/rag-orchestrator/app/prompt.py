"""
Answer Composer — Structured Prompt Builder
=============================================
Builds LLM prompts per review.md §9.
Writer receives evidence pack + router signals → composes grounded answer.

5 output templates by query type:
  - fact_extraction      → brief answer
  - study_result_extraction → structured study report
  - research_appraisal   → deep analysis with limitations
  - comparative_synthesis → side-by-side comparison (uses structured study)
  - guideline_comparison → guideline-focused
  - teaching_explainer   → educational explanation
"""

from __future__ import annotations
from typing import List, Dict, Optional

from .query_router import RouterOutput
from .evidence_extractor import EvidencePack, PrimaryEvidence
from .coverage_scorer import CoverageOutput
from .retriever import RetrievedChunk


OPEN_ENRICHED_SYSTEM_RULES = """Bạn là chuyên gia y khoa và giảng viên lâm sàng. Trả lời bằng tiếng Việt cho người có chuyên môn, đầy đủ, có cấu trúc và có độ sâu.

CHÍNH SÁCH OPEN-KNOWLEDGE ENRICHED RAG:
1. Ưu tiên evidence RAG. Claim nào lấy từ tài liệu RAG phải gắn citation [n].
2. Được dùng kiến thức nền/chuyên sâu của LLM để giải thích, mở rộng bối cảnh, cơ chế, ý nghĩa lâm sàng khi RAG yếu hoặc thiếu.
3. Kiến thức nền không cần citation, nhưng tuyệt đối không được gắn citation [n] nếu claim đó không nằm trong evidence.
4. Không tự bịa số liệu, guideline mới, liều thuốc, phác đồ điều trị cụ thể, chỉ định cá nhân hóa hoặc khuyến cáo thay bác sĩ.
5. Nếu cần số liệu/guideline/liều/phác đồ cụ thể mà evidence không có, chỉ nêu khi có external source [E]. Nếu không có nguồn, viết ở mức tổng quát.
6. Citation phải nằm ngay sau claim được support. Citation sai evidence là lỗi nghiêm trọng.
7. Nếu retrieval chỉ đúng title/chủ đề nhưng thiếu đoạn evidence, có thể dùng title làm anchor để giải thích nền, nhưng không xem title là bằng chứng cho số liệu hoặc kết luận cụ thể.
8. Câu trả lời không được sơ sài. Với câu hỏi lý thuyết hoặc giải thích, hãy trả lời theo hướng bài giảng ngắn: kết luận, giải thích nền, phân tích chuyên sâu, bằng chứng có trong RAG, ý nghĩa thực hành, giới hạn/an toàn.
"""

_OPEN_ENRICHED_GUIDANCE = """CHẾ ĐỘ TRẢ LỜI OPEN_ENRICHED:
- Hãy enrich câu trả lời bằng kiến thức nền/chuyên sâu hợp lý nếu RAG không đủ bao phủ.
- Phần có evidence RAG phải citation [1], [2]. Phần kiến thức nền không có evidence thì không citation.
- Không bịa số liệu, guideline, liều, phác đồ. Nếu không có nguồn cho claim cụ thể, chuyển claim đó thành mô tả khái quát.
- Target độ dài: 800-1200 từ cho câu hỏi lý thuyết/professional explainer; 250-500 từ cho exact article answer.
"""


# ── System prompt with 10 composer rules (review.md §9.1) ───────────

SYSTEM_RULES_V2 = """Bạn là chuyên gia y khoa và giảng viên lâm sàng (clinical educator). Trả lời các câu hỏi y học bằng tiếng Việt với văn phong học thuật, chính xác, có tổ chức logic và luôn dẫn nguồn.

14 QUY TẮC BẮT BUỘC:
1. Ưu tiên tuyệt đối TÀI LIỆU CHÍNH (đánh dấu ★). Kết luận chính phải dựa trên nguồn này.
2. Dùng SỐ LIỆU trước diễn giải. Nếu có OR, HR, AUC, sensitivity, specificity → phải trích.
3. KHÔNG bịa số. Mọi con số phải lấy từ evidence. Nếu không có số, KHÔNG đoán.
4. KHÔNG suy ra causal claim nếu evidence chỉ là association.
5. Tách rõ thông tin từ TÀI LIỆU CHÍNH vs TÀI LIỆU PHỤ.
6. Luôn nêu GIỚI HẠN khi evidence không mạnh (cỡ mẫu nhỏ, đơn trung tâm, hồi cứu).
7. Không thêm kiến thức ngoài evidence pack. Chỉ phân tích những gì có trong tài liệu.
8. Nếu evidence không đủ → NÓI RÕ phần nào không có dữ liệu, KHÔNG answer chung chung.
9. Thuật ngữ y khoa tiếng Anh để trong ngoặc đơn khi cần (VD: clearance, half-life).
10. Mỗi claim quan trọng hoặc con số phải đi kèm citation [n] ngay trong câu.
11. CẤM NGOẠI SUY: Nếu tài liệu truy hồi KHÔNG chứa bằng chứng trực tiếp cho khái niệm mà người dùng hỏi, phải nói rõ: "Tài liệu [n] chỉ cung cấp bằng chứng về [X], không chứa dữ kiện về [Y]". KHÔNG được tự nối hai khái niệm nếu source không chứng minh mối liên hệ đó.
12. CẤM DÙNG TRI THỨC NỀN: Không được lấp khoảng trống evidence bằng kiến thức pre-training. Nếu evidence chỉ hỗ trợ một phần câu hỏi, phải TÁCH RÕ phần nào có evidence và phần nào không.
13. CẤM CHIỀU NGƯỜI HỎI: Nếu không đủ evidence để trả lời một phần câu hỏi, dùng mẫu từ chối cứng thay vì cố tạo câu trả lời "có ích". Groundedness quan trọng hơn helpfulness.
14. PHÂN BIỆT SCOPE: Với mỗi claim trong câu trả lời, phải chỉ rõ claim đó được support bởi tài liệu nào [n]. Claim không có citation = claim không được phép tồn tại.

QUY TẮC DẪN NGUỒN:
- Citation trong câu: "Tỷ lệ tái phát là 23.5% [1]"
- Cuối câu trả lời: liệt kê đầy đủ nguồn theo format [n] Tên bài - Tạp chí
"""


# ── Output templates per query type ─────────────────────────────────

_TEMPLATE_FACT = """Trả lời theo cấu trúc:

## Kết luận ngắn
1-3 câu trả lời trực tiếp câu hỏi. Dẫn nguồn [1].

## Theo dữ liệu nghiên cứu
- Tóm tắt nội dung liên quan từ tài liệu chính.
- Trích dẫn số liệu nếu có.

## Nguồn tham khảo
[1] Tên bài viết - Tạp chí"""

_TEMPLATE_STUDY = """Trả lời theo cấu trúc:

## Kết luận ngắn
1-3 câu trả lời trực tiếp câu hỏi. Dẫn nguồn [1].

## Theo dữ liệu nghiên cứu
### Tài liệu chính [1]
- **Thiết kế**: [loại nghiên cứu]
- **Quần thể**: [đối tượng, tiêu chuẩn chọn]
- **Cỡ mẫu**: [n=...]
- **Kết quả chính**: [mô tả kết quả với số liệu cụ thể, trích OR/HR/AUC/p nếu có] [1]
- **Số liệu quan trọng**: liệt kê các chỉ số chính [1]

{secondary_section}

## Giới hạn & Mức chắc chắn
- Hạn chế chính của nghiên cứu (nếu có trong tài liệu)
- Điểm cần thận trọng khi diễn giải

## Nguồn tham khảo
[1] Tên bài viết chính - Tạp chí"""

_TEMPLATE_APPRAISAL = """Trả lời theo cấu trúc:

## Kết luận ngắn
1-3 câu trả lời trực tiếp câu hỏi. Dẫn nguồn [1].

## Theo dữ liệu nghiên cứu
### Bối cảnh và thiết kế [1]
- **Bối cảnh lâm sàng**: [tại sao nghiên cứu này cần thiết]
- **Thiết kế**: [loại nghiên cứu, đơn/đa trung tâm]
- **Quần thể và cỡ mẫu**: [n=..., tiêu chuẩn]

### Biến số và phương pháp phân tích [1]
- Biến số chính được phân tích
- Phương pháp thống kê

### Kết quả chính [1]
- Kết quả quan trọng nhất với số liệu cụ thể
- Phân tích đa biến nếu có (adjusted OR/HR, p-value)

### Yếu tố gây nhiễu có thể có
- Các confounders đã/chưa kiểm soát

{secondary_section}

## Giới hạn & Mức chắc chắn
- Hạn chế chính (cỡ mẫu, thiết kế, selection bias)
- Khả năng áp dụng (generalizability)
- Những điểm còn chưa rõ

## Ý nghĩa ứng dụng
- Ý nghĩa lâm sàng thực tiễn

## Nguồn tham khảo
[1] Tên bài viết - Tạp chí"""

_TEMPLATE_COMPARATIVE = """Trả lời theo cấu trúc:

## Kết luận ngắn
1-3 câu so sánh tổng quan. Dẫn nguồn.

## So sánh chi tiết
### Nguồn 1 [1]
- Thiết kế và quần thể
- Kết quả chính

### Nguồn 2 [2]
- Thiết kế và quần thể
- Kết quả chính

### Điểm giống và khác
- Tương đồng
- Khác biệt

## Giới hạn & Mức chắc chắn
- Hạn chế khi so sánh
- Điểm cần thận trọng

## Nguồn tham khảo"""

_TEMPLATE_TEACHING = """Trả lời theo cấu trúc:

## Kết luận ngắn
1-3 câu trả lời trực tiếp.

## Giải thích theo dữ liệu nghiên cứu
- Giải thích cơ chế, bệnh sinh, hoặc ý nghĩa lâm sàng dựa trên tài liệu [1]
- Phân tích sâu: cấp độ tế bào/phân tử → biểu hiện lâm sàng (nếu phù hợp)
- Ý nghĩa thực hành lâm sàng

## Giới hạn & Mức chắc chắn
- Mức chắc chắn của thông tin
- Những điểm cần nghiên cứu thêm

## Nguồn tham khảo
[1] Tên bài viết - Tạp chí"""

_TEMPLATE_GUIDELINE = """Trả lời theo cấu trúc:

## Kết luận ngắn
1-3 câu tóm tắt theo guideline/khuyến cáo.

## Theo dữ liệu nghiên cứu
- Nội dung từ tài liệu ingest liên quan đến câu hỏi [1]

## Giới hạn & Mức chắc chắn
- Mức chắc chắn
- Lưu ý khi áp dụng

## Nguồn tham khảo
[1] Tên bài viết - Tạp chí"""

_TEMPLATE_EXACT = """Trả lời theo cấu trúc:

## Câu trả lời trực tiếp
1-2 câu trả lời đúng ngay fact/span/số liệu mà câu hỏi yêu cầu. Nếu có số liệu, nêu số trước. Không kể mục tiêu/phương pháp nghiên cứu nếu câu hỏi không hỏi.

## Căn cứ trong tài liệu
- Chỉ nêu 1-3 chi tiết trực tiếp chứng minh câu trả lời từ tài liệu chính [1].
- Không biến phần này thành báo cáo nghiên cứu đầy đủ.

## Nguồn tham khảo
[1] Tên bài viết - Tạp chí"""

_TEMPLATE_SUMMARY = """Trả lời theo cấu trúc:

## Kết luận ngắn
1-2 câu tóm tắt trực tiếp ý chính của câu hỏi. Không mở đầu bằng background chung chung.

## Ý chính theo dữ liệu
- Liệt kê 2-5 ý chính thật sự được support bởi tài liệu [1].
- Ưu tiên trả lời đúng các ý người dùng hỏi; không lan sang methods/background nếu không cần.

## Nguồn tham khảo
[1] Tên bài viết - Tạp chí"""

_TEMPLATE_BOUNDED = """Trả lời theo cấu trúc:

## Có thể khẳng định
- Liệt kê ngắn các điểm được tài liệu hỗ trợ trực tiếp [1].

## Chưa thể khẳng định
- Liệt kê ngắn các điểm tài liệu chưa đủ để kết luận.
- Không dùng ngôn ngữ tuyệt đối cho phần chưa đủ evidence.

## Căn cứ trong tài liệu
- Tóm tắt ngắn câu/ý chính trong tài liệu làm căn cứ cho phần trên [1].

## Nguồn tham khảo
[1] Tên bài viết - Tạp chí"""


_TEMPLATES = {
    "fact_extraction": _TEMPLATE_FACT,
    "study_result_extraction": _TEMPLATE_STUDY,
    "research_appraisal": _TEMPLATE_APPRAISAL,
    "comparative_synthesis": _TEMPLATE_COMPARATIVE,
    "guideline_comparison": _TEMPLATE_GUIDELINE,
    "teaching_explainer": _TEMPLATE_TEACHING,
    "professional_explainer": """Trả lời theo cấu trúc:

## Kết luận trực tiếp
2-4 câu trả lời thẳng vào câu hỏi.

## Giải thích nền tảng
Giải thích khái niệm, cơ chế hoặc bối cảnh y khoa cần thiết cho người có chuyên môn.

## Phân tích chuyên sâu
Triển khai các ý chính theo logic lâm sàng/sinh học bệnh. Có thể dùng kiến thức nền nếu không gắn citation giả.

## Bằng chứng từ tài liệu truy hồi
Nêu rõ tài liệu RAG hỗ trợ phần nào. Chỉ gắn [n] vào claim có trong evidence.

## Ý nghĩa thực hành và lưu ý an toàn
Nêu ý nghĩa ứng dụng, giới hạn bằng chứng, và tránh khuyến cáo cá nhân hóa.

## Nguồn tham khảo
Liệt kê tài liệu RAG [n] và external source [E] nếu có.""",
}


# ── Context formatting ──────────────────────────────────────────────

def _format_evidence_context(
    evidence_pack: EvidencePack,
    include_secondary_sources: bool = True,
    question: str = "",
) -> str:
    """Format evidence pack into context string for LLM."""
    parts = []

    # Primary source
    ev = evidence_pack.primary_source
    query_norm = (question or "").lower()
    criteria_only_scope = (
        any(marker in query_norm for marker in ("chỉ số", "chi so", "tiêu chí", "tieu chi"))
        and bool(getattr(ev, "key_findings", []))
        and not bool(getattr(ev, "direct_answer_spans", []))
    )
    primary_block = f"★ TÀI LIỆU CHÍNH [1]\nTitle: {ev.title}\n"

    if ev.direct_answer_spans:
        primary_block += "TRÍCH ĐOẠN TRẢ LỜI TRỰC TIẾP:\n"
        for span in ev.direct_answer_spans:
            section = f" ({span.section_title})" if span.section_title else ""
            primary_block += f"  - {span.supporting_span or span.claim}{section}\n"
        primary_block += "\n"

    if evidence_pack.extractor_used:
        # Structured evidence available
        if ev.design:
            primary_block += f"Thiết kế: {ev.design.text}\n"
        if ev.population:
            primary_block += f"Quần thể: {ev.population.text}\n"
        if ev.sample_size:
            primary_block += f"Cỡ mẫu: {ev.sample_size.text}\n"
        if ev.setting:
            primary_block += f"Nơi thực hiện: {ev.setting.text}\n"
        if ev.intervention_or_exposure:
            primary_block += f"Can thiệp/Phơi nhiễm: {ev.intervention_or_exposure.text}\n"
        if ev.comparator:
            primary_block += f"Nhóm so sánh: {ev.comparator.text}\n"
        if ev.outcomes:
            outcomes_str = "; ".join(o.text for o in ev.outcomes)
            primary_block += f"Kết cục: {outcomes_str}\n"
        if ev.numbers:
            primary_block += "Số liệu:\n"
            for n in ev.numbers:
                unit_str = f" {n.unit}" if n.unit else ""
                primary_block += f"  - {n.metric}: {n.value}{unit_str}\n"
        if ev.limitations:
            primary_block += "Hạn chế:\n"
            for lim in ev.limitations:
                primary_block += f"  - {lim.claim}\n"
        if ev.authors_conclusion:
            primary_block += f"Kết luận tác giả: {ev.authors_conclusion.text}\n"

        # Also include raw text for full context
        primary_block += f"\nNội dung đầy đủ:\n{ev.raw_text}"
    else:
        # Raw text only
        primary_block += f"\n{ev.raw_text}"

    if criteria_only_scope and ev.raw_text:
        truncated_raw = (ev.raw_text or "")[:1200]
        primary_block = primary_block.replace(ev.raw_text, truncated_raw, 1)
        primary_block = primary_block.replace("Nội dung đầy đủ:", "Nội dung truy hồi liên quan:", 1)

    if ev.key_findings:
        findings_block = "Phát hiện chính gần nhất với câu hỏi:\n"
        for finding in ev.key_findings:
            section = f" ({finding.section_title})" if finding.section_title else ""
            findings_block += f"  - {finding.claim}{section}\n"
        if criteria_only_scope and ev.raw_text:
            truncated_raw = (ev.raw_text or "")[:1200]
            primary_block = primary_block.replace(f"\n{truncated_raw}", f"\n{findings_block}\n{truncated_raw}", 1)
        else:
            primary_block = primary_block.replace(f"\n{ev.raw_text}", f"\n{findings_block}\n{ev.raw_text}", 1)

    parts.append(primary_block)

    # Secondary sources
    if include_secondary_sources:
        for i, sec in enumerate(evidence_pack.secondary_sources):
            sec_block = f"\n📄 TÀI LIỆU PHỤ [{i+2}]\nTitle: {sec.title}\n{sec.raw_text}"
            parts.append(sec_block)

    # Inject conflict notes if any
    notes = getattr(evidence_pack, 'conflict_notes', [])
    if notes:
        conflict_block = "\n⚠️ GHI CHÚ MÂU THUẪN (Heuristic Detect):\n"
        for note in notes:
            conflict_block += f"- {note}\n"
        parts.append(conflict_block)

    return "\n\n" + "═" * 60 + "\n\n".join(parts)


def _get_coverage_instruction(coverage: CoverageOutput) -> str:
    """Generate instruction based on coverage level and confidence ceiling (v2)."""
    parts = []

    # Base coverage instruction
    if coverage.coverage_level == "high":
        parts.append("Dữ liệu ĐẦY ĐỦ. Trả lời chi tiết dựa hoàn toàn trên evidence.")
    elif coverage.coverage_level == "medium":
        instr = "Dữ liệu CÓ nhưng THIẾU một số phần."
        if coverage.force_abstain_parts:
            abstain = ", ".join(coverage.force_abstain_parts)
            instr += f" Nói rõ: '{abstain}' không có trong tài liệu."
        parts.append(instr)
    else:
        instr = "Dữ liệu KHÔNG ĐỦ. Phải nói rõ giới hạn."
        if coverage.force_abstain_parts:
            abstain = ", ".join(coverage.force_abstain_parts)
            instr += f" Đặc biệt thiếu: {abstain}."
        parts.append(instr)

    # v2: Confidence ceiling instruction
    ceiling = getattr(coverage, "confidence_ceiling", "high")
    if ceiling == "moderate":
        parts.append("MỨC CHẮC CHẮN: TRUNG BÌNH. Dùng ngôn ngữ thận trọng (\"có thể\", \"gợi ý\", \"cần thêm nghiên cứu\").")
    elif ceiling == "low":
        parts.append("MỨC CHẮC CHẮN: THẤP. Dùng ngôn ngữ rất dè dặt. Nêu rõ bằng chứng yếu.")

    # v2: Missing requirements
    missing = getattr(coverage, "missing_requirements", [])
    if missing:
        missing_str = ", ".join(missing)
        parts.append(f"Thiếu: {missing_str}. Nói rõ phần nào chưa có trong tài liệu.")

    return " ".join(parts)


def _should_force_opening_disclaimer(coverage: CoverageOutput) -> bool:
    """Only force a leading disclaimer when evidence is truly inadequate."""
    unsupported = getattr(coverage, "unsupported_concepts", []) or []
    allowed_scope = getattr(coverage, "allowed_answer_scope", "") or ""
    if coverage.coverage_level == "low":
        return True
    if coverage.force_abstain_parts and coverage.coverage_level != "high":
        return True
    if unsupported and not allowed_scope:
        return True
    return False


def _should_include_secondary_sources(router_output: RouterOutput) -> bool:
    """Only expose secondary sources when the answer genuinely needs synthesis."""
    retrieval_mode = getattr(router_output, "retrieval_mode", "")
    if retrieval_mode == "article_centric":
        return False
    return router_output.query_type in {"comparative_synthesis", "guideline_comparison", "teaching_explainer", "professional_explainer"}


def _select_template(router_output: RouterOutput) -> str:
    if getattr(router_output, "query_type", "") == "professional_explainer":
        return _TEMPLATES["professional_explainer"]
    answer_style = getattr(router_output, "answer_style", "")
    if answer_style == "exact":
        return _TEMPLATE_EXACT
    if answer_style == "summary":
        return _TEMPLATE_SUMMARY
    if answer_style == "bounded_partial":
        return _TEMPLATE_BOUNDED
    return _TEMPLATES.get(router_output.query_type, _TEMPLATE_FACT)


def _answer_style_instruction(router_output: RouterOutput) -> str:
    answer_style = getattr(router_output, "answer_style", "")
    if answer_style == "exact":
        return (
            "KIỂU TRẢ LỜI EXACT: Trả lời đúng fact/span người dùng hỏi ở câu đầu tiên. "
            "Nếu EVIDENCE có mục 'TRÍCH ĐOẠN TRẢ LỜI TRỰC TIẾP', phải ưu tiên tuyệt đối các span này trước danh sách số liệu hoặc raw text. "
            "Không chuyển sang kiểu 'mục tiêu-phương pháp-kết quả' nếu câu hỏi không yêu cầu.\n\n"
        )
    if answer_style == "summary":
        return (
            "KIỂU TRẢ LỜI SUMMARY: Ưu tiên đủ các ý chính người dùng hỏi. "
            "Nếu EVIDENCE có mục 'Phát hiện chính gần nhất với câu hỏi', phải ưu tiên các câu này trước raw text. "
            "Không biến câu trả lời thành báo cáo nghiên cứu chung chung. Nếu tài liệu chỉ nêu nhóm ý hoặc tiêu chí chung, giữ nguyên ở mức nhóm chung; không tự mở rộng thành chỉ số, thuốc hoặc ví dụ cụ thể mà nguồn không nêu.\n\n"
        )
    if answer_style == "bounded_partial":
        return (
            "KIỂU TRẢ LỜI BOUNDED_PARTIAL: Bắt buộc tách rõ phần 'Có thể khẳng định' và 'Chưa thể khẳng định'. "
            "Chỉ nêu giới hạn đúng tại claim còn thiếu evidence.\n\n"
        )
    return ""


# ── Main builder ─────────────────────────────────────────────────────

def _query_scope_instruction(
    question: str,
    evidence_pack: EvidencePack,
    router_output: RouterOutput,
) -> str:
    query_norm = (question or "").lower()
    answer_style = getattr(router_output, "answer_style", "")
    if answer_style not in {"summary", "bounded_partial", "exact"}:
        return ""

    ev = evidence_pack.primary_source
    key_findings = getattr(ev, "key_findings", []) or []
    has_direct_spans = bool(getattr(ev, "direct_answer_spans", []))
    instructions: list[str] = []

    if any(marker in query_norm for marker in ("chỉ số", "chi so", "tiêu chí", "tieu chi")) and key_findings and not has_direct_spans:
        instructions.append(
            "Với câu hỏi hỏi về chỉ số/tiêu chí đánh giá: nếu phần 'Phát hiện chính gần nhất với câu hỏi' "
            "chỉ nêu nhóm đánh giá ở mức lâm sàng, cận lâm sàng, tình trạng nhập viện hoặc mục tiêu theo dõi, "
            "phải giữ câu trả lời ở đúng mức đó và nói rõ evidence hiện có chưa nêu bộ chỉ số cụ thể. "
            "Không được đào sang raw text để tự ráp thêm EF, huyết áp, thuốc hay thước đo cụ thể nếu chúng không nằm trong findings/direct spans."
        )

    if any(marker in query_norm for marker in ("vì sao", "vi sao", "tại sao", "tai sao")):
        instructions.append(
            "Với câu hỏi 'vì sao', phải giữ đủ các nhóm lý do độc lập có trong nguồn chính. "
            "Nếu evidence có cả khó khăn chẩn đoán và nguy cơ/tương tác điều trị, phải nêu đủ cả hai; không được chỉ trả lời một vế."
        )

    if any(marker in query_norm for marker in ("dựa trên", "dua tren", "nhóm yếu tố", "nhom yeu to", "quyết định", "quyet dinh")):
        instructions.append(
            "Với câu hỏi hỏi 'dựa trên những nhóm yếu tố nào' hoặc 'quyết định ... và theo dõi ...', phải liệt kê đủ các nhóm tiêu chí mà nguồn chính nêu. "
            "Nếu evidence có cả tiêu chí chọn can thiệp và yêu cầu theo dõi sau can thiệp, phải nêu cả hai; không thay bằng diễn giải sức khỏe chung chung."
        )
        instructions.append(
            "Nếu nguồn chính có nêu điều trị nội khoa hỗ trợ, kiểm soát yếu tố nguy cơ hoặc theo dõi hình ảnh sau can thiệp, phải ưu tiên nêu các nhóm management đó; "
            "không được thu hẹp phần follow-up chỉ còn lịch tái khám đơn thuần."
        )

    return "\n".join(instructions) + ("\n\n" if instructions else "")


def build_prompt_v2(
    question: str,
    evidence_pack: EvidencePack,
    router_output: RouterOutput,
    coverage: CoverageOutput,
    chat_history: list | None = None,
    answer_plan_text: str = "",
    external_sources_text: str = "",
) -> str:
    """
    Build structured LLM prompt with evidence pack, router signals, and templates.

    This is the main entry point for the Answer Composer (review.md §9).
    """
    answer_policy = getattr(router_output, "answer_policy", "strict_rag")
    coverage_mode = getattr(coverage, "coverage_mode", "")
    open_enriched = answer_policy == "open_enriched" or coverage_mode in {"open_knowledge", "title_anchored"}
    system_rules = OPEN_ENRICHED_SYSTEM_RULES if open_enriched else SYSTEM_RULES_V2
    messages = [{"role": "system", "content": system_rules}]

    # Chat history (last 6 messages)
    if chat_history:
        history_lines: list[str] = []
        for m in chat_history[-6:]:
            role = str(m.get("role", "user") or "user").upper()
            content = m.get("content", "")
            if content.strip():
                history_lines.append(f"{role}: {content}")
        if history_lines:
            sections.append("CHAT_HISTORY:\n" + "\n".join(history_lines))

    # Build context from evidence
    context_str = _format_evidence_context(evidence_pack, question=question)

    # Get template for this query type
    template = _select_template(router_output)

    # Handle secondary section placeholder
    secondary_section = ""
    if evidence_pack.secondary_sources:
        sec_parts = []
        for i, sec in enumerate(evidence_pack.secondary_sources):
            sec_parts.append(f"### Tài liệu phụ [{i+2}]\n- Tóm tắt nội dung liên quan [{i+2}]")
        secondary_section = "\n".join(sec_parts)
    template = template.replace("{secondary_section}", secondary_section)

    # Coverage instruction
    coverage_instr = _get_coverage_instruction(coverage)

    bounded_prefix = ""
    missing = getattr(coverage, "missing_requirements", [])
    ceiling = getattr(coverage, "confidence_ceiling", "high")
    unsupported = getattr(coverage, "unsupported_concepts", []) or []
    force_disclaimer = _should_force_opening_disclaimer(coverage)

    if force_disclaimer:
        missing_str = ", ".join(missing) if missing else "một số khía cạnh"
        bounded_prefix = (
            f"BẮT BUỘC MỞ ĐẦU CÂU TRẢ LỜI BẰNG ĐÚNG CÂU SAU (với tư cách là Disclaimer):\n"
            f"> \"Trong phạm vi dữ liệu nội bộ hiện có, tôi mới tìm thấy bằng chứng liên quan một phần. "
            f"Chưa có đủ dữ liệu nội bộ để kết luận đầy đủ về {missing_str}, nên phần trả lời dưới đây chỉ phản ánh những gì có thể kiểm chứng từ các tài liệu đã truy hồi.\"\n\n"
        )

    # Phase 4: Bounded Execution — unsupported concept refusal guard
    # Balance: refuse unsupported parts, but USE what evidence covers
    unsupported = getattr(coverage, "unsupported_concepts", []) or []
    allowed_scope = getattr(coverage, "allowed_answer_scope", "") or ""
    
    if unsupported and allowed_scope:
        # Partial coverage: corpus has SOME useful data
        unsupported_str = ", ".join(unsupported[:5])
        bounded_prefix += (
            f"HƯỚNG DẪN SCOPE:\n"
            f"- Dữ liệu nội bộ CÓ HỖ TRỢ các khái niệm: {allowed_scope}\n"
            f"  → Hãy phân tích SÂU các phần này dựa trên evidence.\n"
            f"- Dữ liệu nội bộ KHÔNG CÓ evidence trực tiếp về: {unsupported_str}\n"
            f"  → Với những phần này, ghi ngắn gọn: 'Dữ liệu nội bộ chưa có bằng chứng về [khái niệm].'\n"
            f"  → KHÔNG được tự bổ sung kiến thức nền để lấp khoảng trống.\n\n"
        )
    elif unsupported:
        # No coverage at all
        unsupported_str = ", ".join(unsupported[:5])
        bounded_prefix += (
            f"CÁC KHÁI NIỆM KHÔNG CÓ EVIDENCE: {unsupported_str}\n"
            f"BẮT BUỘC: Ghi rõ dữ liệu nội bộ chưa có bằng chứng về các khái niệm trên.\n"
            f"KHÔNG được tự bổ sung kiến thức nền.\n\n"
        )

    # Compose user message
    user_content = (
        f"EVIDENCE:\n{context_str}\n\n"
        f"{answer_plan_text + chr(10) + chr(10) if answer_plan_text else ''}"
        f"{external_sources_text + chr(10) + chr(10) if external_sources_text else ''}"
        f"{'═' * 60}\n"
        f"QUESTION: {question}\n\n"
        f"QUERY TYPE: {router_output.query_type}\n"
        f"COVERAGE: {coverage.coverage_level} — {coverage_instr}\n\n"
        f"FORMAT YÊU CẦU:\n{template}\n\n"
        f"{bounded_prefix}"
        f"Hãy đọc evidence kỹ lưỡng và đưa ra bài phân tích học thuật chi tiết, sâu sắc. "
        f"Dẫn nguồn [n] đầy đủ trong câu. Không viết quá ngắn."
    )

    messages.append({"role": "user", "content": user_content})
    return messages


# ── Legacy builder (kept as fallback) ────────────────────────────────

SYSTEM_RULES_LEGACY = """Bạn là chuyên gia y khoa xuất sắc và là giảng viên lâm sàng (clinical educator), chuyên trả lời các câu hỏi y học bằng tiếng Việt với văn phong học thuật, chính xác, có tổ chức logic và luôn dẫn nguồn rõ ràng.

QUY TẮC BẮT BUỘC:
1) LUÔN trả lời bằng tiếng Việt. Thuật ngữ y khoa tiếng Anh có thể để trong ngoặc đơn.
2) CHỈ sử dụng thông tin có trong CONTEXT được cung cấp. Tuyệt đối không tự suy diễn.
3) Trả lời phải có CHIỀU SÂU HỌC THUẬT.
4) Nếu CONTEXT không đủ thông tin, hãy nói rõ phần nào không có trong tài liệu.
5) Cấu trúc câu trả lời phải rõ ràng, mạch lạc.
6) Không chẩn đoán cá thể hóa, không kê đơn cụ thể.

QUY TẮC DẪN NGUỒN:
- Cuối câu trả lời bắt buộc phải có mục "Nguồn tham khảo:" liệt kê các nguồn.
"""


def _format_chunk_legacy(chunk: RetrievedChunk) -> str:
    """Format a single chunk for legacy prompt."""
    md = chunk.metadata
    source_name = md.get("source_name", "")
    title = md.get("title", "")
    if source_name and title:
        citation = f"[Source: {source_name} - {title}]"
    else:
        citation = f"[source:{chunk.id}]"
    return f"{citation}\n{chunk.text}"


def build_prompt(
    question: str,
    chunks: List[RetrievedChunk],
    chat_history: list | None = None,
) -> str:
    """Legacy prompt builder — kept as fallback."""
    sections: list[str] = []

    if chat_history:
        history_lines: list[str] = []
        for m in chat_history[-6:]:
            role = str(m.get("role", "user") or "user").upper()
            content = m.get("content", "")
            if content.strip():
                history_lines.append(f"{role}: {content}")
        if history_lines:
            sections.append("CHAT_HISTORY:\n" + "\n".join(history_lines))

    if not chunks:
        context_str = "NO_CONTEXT"
    else:
        context_str = "\n\n---\n\n".join(
            _format_chunk_legacy(c) for c in chunks
        )

    lang_hint = "Hãy đọc ngữ cảnh kỹ lưỡng và đưa ra bài phân tích học thuật chi tiết, sâu sắc. Dẫn nguồn đầy đủ ở cuối. Không viết quá ngắn."
    sections.append(f"CONTEXT:\n{context_str}")
    sections.append(f"QUESTION: {question}")
    sections.append(lang_hint)

    return "\n\n".join(sections)


def build_prompt_v2(
    question: str,
    evidence_pack: EvidencePack,
    router_output: RouterOutput,
    coverage: CoverageOutput,
    chat_history: list | None = None,
    answer_plan_text: str = "",
    external_sources_text: str = "",
) -> List[Dict[str, str]]:
    """
    Overridden v2 builder with tighter boundary control.

    The original implementation above is kept for context, but this version is
    the active one at import time and avoids forcing a generic insufficiency
    disclaimer for every medium-confidence answer.
    """
    answer_policy = getattr(router_output, "answer_policy", "strict_rag")
    coverage_mode = getattr(coverage, "coverage_mode", "")
    open_enriched = answer_policy == "open_enriched" or coverage_mode in {"open_knowledge", "title_anchored"}
    system_rules = OPEN_ENRICHED_SYSTEM_RULES if open_enriched else SYSTEM_RULES_V2
    messages = [{"role": "system", "content": system_rules}]

    if chat_history:
        for m in chat_history[-6:]:
            role = m.get("role", "user")
            content = m.get("content", "")
            if content.strip():
                messages.append({"role": role, "content": content})

    include_secondary_sources = _should_include_secondary_sources(router_output)
    context_str = _format_evidence_context(
        evidence_pack,
        include_secondary_sources=include_secondary_sources,
        question=question,
    )
    template = _select_template(router_output)

    secondary_section = ""
    if include_secondary_sources and evidence_pack.secondary_sources:
        sec_parts = []
        for i, _sec in enumerate(evidence_pack.secondary_sources):
            sec_parts.append(f"### Tài liệu phụ [{i+2}]\n- Tóm tắt nội dung liên quan [{i+2}]")
        secondary_section = "\n".join(sec_parts)
    template = template.replace("{secondary_section}", secondary_section)

    coverage_instr = _get_coverage_instruction(coverage)
    missing = getattr(coverage, "missing_requirements", [])
    unsupported = getattr(coverage, "unsupported_concepts", []) or []
    allowed_scope = getattr(coverage, "allowed_answer_scope", "") or ""

    prompt_prefix = ""
    if open_enriched:
        prompt_prefix += _OPEN_ENRICHED_GUIDANCE + "\n"
    if _should_force_opening_disclaimer(coverage):
        missing_str = ", ".join(missing) if missing else "một số khía cạnh"
        prompt_prefix += (
            "BẮT BUỘC mở đầu câu trả lời bằng đúng câu sau:\n"
            f"> \"Trong phạm vi dữ liệu nội bộ hiện có, tôi mới tìm thấy bằng chứng liên quan một phần. "
            f"Chưa có đủ dữ liệu nội bộ để kết luận đầy đủ về {missing_str}, nên phần trả lời dưới đây chỉ phản ánh những gì có thể kiểm chứng từ các tài liệu đã truy hồi.\"\n\n"
        )
    else:
        prompt_prefix += (
            "KHÔNG được mở đầu bằng disclaimer chung chung nếu ý chính đã có evidence trực tiếp. "
            "Trả lời thẳng ở phần 'Kết luận ngắn', chỉ nêu giới hạn đúng tại claim còn thiếu evidence.\n\n"
        )

    if open_enriched:
        prompt_prefix += (
            "PHẠM VI NGUỒN: Tài liệu RAG là evidence ưu tiên. Được dùng kiến thức nền ngoài RAG để giải thích, "
            "nhưng chỉ gắn citation [n] hoặc [E] vào claim thật sự được nguồn đó hỗ trợ.\n\n"
        )
    elif not include_secondary_sources:
        prompt_prefix += (
            "PHẠM VI NGUỒN: Với câu hỏi này, chỉ dùng TÀI LIỆU CHÍNH [1] để hình thành kết luận. "
            "Không được đưa chi tiết từ tài liệu khác vào phần kết luận chính.\n\n"
        )
    else:
        prompt_prefix += (
            "NGUYÊN TẮC NGUỒN PHỤ: Chỉ dùng tài liệu phụ khi nó bổ sung một điểm liên quan trực tiếp mà "
            "TÀI LIỆU CHÍNH [1] chưa bao phủ rõ. Nếu tài liệu phụ chỉ lặp lại hoặc yếu hơn [1], bỏ qua nó "
            "trong kết luận chính.\n\n"
        )

    if open_enriched and unsupported:
        unsupported_str = ", ".join(unsupported[:5])
        prompt_prefix += (
            "PHẠM VI ENRICHMENT:\n"
            f"- RAG chưa có evidence trực tiếp cho: {unsupported_str}.\n"
            "- Được giải thích kiến thức nền liên quan nếu hợp lý, nhưng không gắn citation RAG cho phần đó.\n"
            "- Nếu các phần này cần số liệu/guideline/liều/phác đồ cụ thể, chỉ nêu khi có external source [E].\n\n"
        )
    elif unsupported and allowed_scope:
        unsupported_str = ", ".join(unsupported[:5])
        prompt_prefix += (
            "HƯỚNG DẪN SCOPE:\n"
            f"- Dữ liệu nội bộ CÓ HỖ TRỢ các khái niệm: {allowed_scope}\n"
            "  -> Hãy phân tích sâu các phần này dựa trên evidence.\n"
            f"- Dữ liệu nội bộ KHÔNG CÓ evidence trực tiếp về: {unsupported_str}\n"
            "  -> Với những phần này, chỉ được ghi ngắn gọn rằng dữ liệu nội bộ chưa có bằng chứng.\n"
            "  -> KHÔNG được tự bổ sung kiến thức nền để lấp khoảng trống.\n\n"
        )
    elif unsupported:
        unsupported_str = ", ".join(unsupported[:5])
        prompt_prefix += (
            f"CÁC KHÁI NIỆM KHÔNG CÓ EVIDENCE: {unsupported_str}\n"
            "BẮT BUỘC: Ghi rõ dữ liệu nội bộ chưa có bằng chứng về các khái niệm trên.\n"
            "KHÔNG được tự bổ sung kiến thức nền.\n\n"
        )

    prompt_prefix += _answer_style_instruction(router_output)
    prompt_prefix += _query_scope_instruction(question, evidence_pack, router_output)
    if answer_plan_text or external_sources_text:
        context_str = "\n\n".join(
            part for part in (context_str, answer_plan_text, external_sources_text) if part
        )

    user_content = (
        f"EVIDENCE:\n{context_str}\n\n"
        f"{'═' * 60}\n"
        f"QUESTION: {question}\n\n"
        f"QUERY TYPE: {router_output.query_type}\n"
        f"COVERAGE: {coverage.coverage_level} — {coverage_instr}\n\n"
        f"FORMAT YÊU CẦU:\n{template}\n\n"
        f"{prompt_prefix}"
        "Hãy đọc evidence kỹ lưỡng và trả lời trực tiếp phần nào đã được support rõ. "
        "Nếu còn phần thiếu evidence, nêu ngắn gọn đúng phần thiếu thay vì viết một đoạn từ chối dài. "
        "Dẫn nguồn [n] đầy đủ trong câu."
    )

    messages.append({"role": "user", "content": user_content})
    return messages
