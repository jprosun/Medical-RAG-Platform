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


_TEMPLATES = {
    "fact_extraction": _TEMPLATE_FACT,
    "study_result_extraction": _TEMPLATE_STUDY,
    "research_appraisal": _TEMPLATE_APPRAISAL,
    "comparative_synthesis": _TEMPLATE_COMPARATIVE,
    "guideline_comparison": _TEMPLATE_GUIDELINE,
    "teaching_explainer": _TEMPLATE_TEACHING,
}


# ── Context formatting ──────────────────────────────────────────────

def _format_evidence_context(
    evidence_pack: EvidencePack,
) -> str:
    """Format evidence pack into context string for LLM."""
    parts = []

    # Primary source
    ev = evidence_pack.primary_source
    primary_block = f"★ TÀI LIỆU CHÍNH [1]\nTitle: {ev.title}\n"

    if evidence_pack.extractor_used and ev.key_findings:
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
        if ev.key_findings:
            primary_block += "Phát hiện chính:\n"
            for f in ev.key_findings:
                primary_block += f"  - {f.claim}\n"
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

    parts.append(primary_block)

    # Secondary sources
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


# ── Main builder ─────────────────────────────────────────────────────

def build_prompt_v2(
    question: str,
    evidence_pack: EvidencePack,
    router_output: RouterOutput,
    coverage: CoverageOutput,
    chat_history: list | None = None,
) -> List[Dict[str, str]]:
    """
    Build structured LLM prompt with evidence pack, router signals, and templates.

    This is the main entry point for the Answer Composer (review.md §9).
    """
    messages = [{"role": "system", "content": SYSTEM_RULES_V2}]

    # Chat history (last 6 messages)
    if chat_history:
        for m in chat_history[-6:]:
            role = m.get("role", "user")
            content = m.get("content", "")
            if content.strip():
                messages.append({"role": role, "content": content})

    # Build context from evidence
    context_str = _format_evidence_context(evidence_pack)

    # Get template for this query type
    template = _TEMPLATES.get(router_output.query_type, _TEMPLATE_FACT)

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
    
    if missing or coverage.coverage_level != "high" or ceiling != "high":
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
) -> List[Dict[str, str]]:
    """Legacy prompt builder — kept as fallback."""
    messages = [{"role": "system", "content": SYSTEM_RULES_LEGACY}]

    if chat_history:
        for m in chat_history[-6:]:
            role = m.get("role", "user")
            content = m.get("content", "")
            if content.strip():
                messages.append({"role": role, "content": content})

    if not chunks:
        context_str = "No medical context provided."
    else:
        context_str = "\n\n---\n\n".join(
            _format_chunk_legacy(c) for c in chunks
        )

    lang_hint = "Hãy đọc ngữ cảnh kỹ lưỡng và đưa ra bài phân tích học thuật chi tiết, sâu sắc. Dẫn nguồn đầy đủ ở cuối. Không viết quá ngắn."
    user_content = f"CONTEXT:\n{context_str}\n\nQUESTION: {question}\n\n{lang_hint}"
    messages.append({"role": "user", "content": user_content})

    return messages
