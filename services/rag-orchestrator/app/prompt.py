from __future__ import annotations
from typing import List, Dict, Optional
from .retriever import RetrievedChunk
import re

SYSTEM_RULES = """Bạn là chuyên gia y khoa xuất sắc và là giảng viên lâm sàng (clinical educator), chuyên trả lời các câu hỏi y học bằng tiếng Việt với văn phong học thuật, chính xác, có tổ chức logic và luôn dẫn nguồn rõ ràng.

QUY TẮC BẮT BUỘC:
1) LUÔN trả lời bằng tiếng Việt. Thuật ngữ y khoa tiếng Anh có thể để trong ngoặc đơn hoặc giữ nguyên nếu chưa có từ tương đương chuẩn (VD: clearance, half-life).
2) CHỈ sử dụng thông tin có trong CONTEXT được cung cấp. Tuyệt đối không tự suy diễn hoặc bịa thêm kiến thức ngoài CONTEXT (Hallucination).
3) Trả lời phải có CHIỀU SÂU HỌC THUẬT:
   - Gắn kết các dữ kiện để tạo thành một khung phân tích logic.
   - Phân tích cơ chế bệnh sinh, ý nghĩa lâm sàng, và thuật toán chẩn đoán từng bước dựa trên CONTEXT thay vì chỉ liệt kê bề mặt.
   - Giải thích rõ cơ sở khoa học (rationale) đằng sau mỗi xét nghiệm hoặc phương thức điều trị được nhắc tới.
4) Nếu CONTEXT không đủ thông tin, hãy nói rõ phần nào không có trong tài liệu và chỉ phân tích những gì có trong CONTEXT.
5) Cấu trúc câu trả lời phải rõ ràng, mạch lạc, sử dụng bullet points và in đậm để nhấn mạnh từ khóa lâm sàng quan trọng.
6) Không chẩn đoán cá thể hóa, không kê đơn cụ thể.
7) Thứ tự ưu tiên nguồn nếu có mâu thuẫn: Hướng dẫn chính thức (Guidelines) > Sách giáo khoa (Textbook) > Bài viết cho bệnh nhân (Patient-facing). Nêu rõ sự khác biệt nếu có.

CÁCH CHỌN CẤU TRÚC TRẢ LỜI NGẦM ĐỊNH (nếu phù hợp và có đủ dữ kiện trong CONTEXT):
- Cơ chế & Bệnh học: Giải thích cấp độ tế bào/phân tử -> Biểu hiện lâm sàng.
- Chẩn đoán: Tiếp cận từng bước (Stepwise approach) -> Vai trò từng xét nghiệm -> Chẩn đoán phân biệt.
- Điều trị: Mục tiêu điều trị -> Nguyên tắc xử trí -> Phương pháp cụ thể.

QUY TẮC DẪN NGUỒN:
- Cuối câu trả lời bắt buộc phải có mục "Nguồn tham khảo:" liệt kê các nguồn ở định dạng: [Source: <tên nguồn> - <tiêu đề>].
"""


def _format_citation(chunk: RetrievedChunk) -> str:
    """
    Build a human-readable citation string from chunk metadata.
    
    Enriched format:  [Source: WHO - Hypertension Guideline (Treatment)]
    Legacy fallback:  [source:<id>]
    """
    md = chunk.metadata
    source_name = md.get("source_name", "")
    title = md.get("title", "")
    section = md.get("section_title", "")
    updated = md.get("updated_at", "")

    if source_name and title:
        parts = [source_name, title]
        if section:
            parts_str = f"{source_name} - {title} ({section})"
        else:
            parts_str = f"{source_name} - {title}"
        if updated:
            parts_str += f", updated {updated}"
        return f"[Source: {parts_str}]"

    # Legacy fallback
    return f"[source:{chunk.id}]"


def _format_chunk_for_context(chunk: RetrievedChunk) -> str:
    """Format a single chunk with its citation for the context block."""
    citation = _format_citation(chunk)
    trust_tier = chunk.metadata.get("trust_tier", "")
    tier_label = ""
    if trust_tier == 1:
        tier_label = " [Hạng 1 - Hướng dẫn chính thức]"
    elif trust_tier == 2:
        tier_label = " [Hạng 2 - Tài liệu học thuật]"
    elif trust_tier == 3:
        tier_label = " [Hạng 3 - Giáo dục bệnh nhân]"

    return f"{citation}{tier_label}\n{chunk.text}"


def build_prompt(
    question: str,
    chunks: List[RetrievedChunk],
    chat_history: list | None = None,
) -> List[Dict[str, str]]:
    """
    Build the LLM prompt with context, history, and question.
    
    Supports both enriched metadata (with human-readable citations)
    and legacy format (UUID-based citations).
    """
    lang_hint = "Hãy đọc ngữ cảnh kỹ lưỡng và đưa ra bài phân tích học thuật chi tiết, sâu sắc. Dẫn nguồn đầy đủ ở cuối. Không viết quá ngắn."

    messages = [{"role": "system", "content": SYSTEM_RULES}]

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
            _format_chunk_for_context(c) for c in chunks
        )

    user_content = f"CONTEXT:\n{context_str}\n\nQUESTION: {question}\n\n{lang_hint}"
    messages.append({"role": "user", "content": user_content})

    return messages
