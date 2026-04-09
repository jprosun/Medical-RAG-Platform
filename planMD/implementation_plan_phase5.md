# Yêu cầu

Nâng cấp logic Answer Generation (Grounding Policy) để triệt tiêu hiện tượng Hallucination và RAG Over-reliance (Model trả lời dựa trên kiến thức nền tự do thay vì context retrieved). Cụ thể:
1. Chuyển từ việc ném Top K chunks ngẫu nhiên sang **Article Bundle Context** (chỉ lấy chunks thuộc về Top 1-2 bài được rank cao nhất).
2. Xây dựng **Evidence-First System Prompt** cấm LLM chèn tài liệu tham khảo ngẫu nhiên, nhắc nhở từ chối overclaim nếu thiếu dữ liệu, và bắt buộc chỉ trích xuất từ CONTEXT.
3. Chỉnh LLM **Temperature về 0.1** để đảm bảo tính deterministic.

## User Review Required

> [!WARNING]
> Mặc định, tôi sẽ lấy **Top 15 chunks** từ Qdrant, sau đó gộp tất cả chunks thuộc về **1 bài báo có điểm chunk cao nhất (Top 1 Article)**, hoặc có thể nới lỏng thành **Top 2 Articles** nếu muốn so sánh. Bạn muốn Answer Generation tập trung đọc **duy nhất 1 bài (được đánh giá cao nhất)** để trả lời thật sâu, hay muốn đọc tối đa **2 bài**?

> [!IMPORTANT]
> Tăng `RAG_TOP_K` trong file env lên 15-20 chunks có thể làm chậm quá trình Retrieval một vài mili-giây, bù lại chúng ta gom bài chuẩn xác hơn. Hãy xác nhận!

## Proposed Changes

---

### Rag-Orchestrator Logic

Tạo cơ chế Article Bundle cho Retriever và thay đổi cách thiết kế Prompt.

#### [MODIFY] `services/rag-orchestrator/app/retriever.py`
- Tăng giá trị default của tham số `top_k` lên 15.
- Cập nhật hàm `retrieve()`: Sau khi client query ra 15 chunks từ Qdrant, thực hiện hàm `collapse_by_article()` giống như script `gate_g3_eval.py`.
- Tách list chunks theo `title_norm` (hoặc title metadata) và chỉ trả về chunks nằm trong **Top 1 (hoặc Top 2)** Article cao điểm nhất để làm bối cảnh thống nhất. 

#### [MODIFY] `services/rag-orchestrator/app/prompt.py`
- Cập nhật biến `SYSTEM_RULES`:
  - Thêm quy tắc CẤM tạo reference ngoài CONTEXT.
  - Quy tắc CẤM suy luận số hạng nếu không có trong dữ liệu.
  - Sửa lại schema output: Bắt đầu bằng Evidence (Cỡ mẫu, thiết kế nghiên cứu), theo sau là Kết quả lõi có số liệu, Hạn chế (nếu có).
- Cập nhật `_format_chunk_for_context()`: gộp chunk theo Article Title trước khi stringify, tạo format đẹp hơn: `[Bài báo: <Title>]\n - Dữ kiện 1: ... \n - Dữ kiện 2: ...`.

#### [MODIFY] `docker-compose.local.yml`
- Mục `rag-orchestrator` environment:
  - Cập nhật `LLM_TEMPERATURE: "0.1"`.
  - Đổi biến `RAG_TOP_K` từ `7` lên `15`. Định nghĩa thêm biến `RAG_TOP_ARTICLES=1`.

## Open Questions

1. Có nên thiết lập để Model trả về `"Chưa đủ dữ liệu trong bài báo retrieved"` ở ngay đầu dòng (như một cảnh báo cho User) trong trường hợp thông tin câu hỏi bị lệch hoàn toàn so với Context?
2. Có cần thiết phải cung cấp Prompt phụ cho Llama-3 dưới định dạng `<system> ... </system>` không, hoặc Llama-3 trên Groq chạy tốt với list dictionaries `[{"role": "system"...}]`? (Tôi sẽ giữ cấu trúc dict như cũ nếu bạn ko có ghi chú khác).

## Verification Plan

### Automated Tests
- Chạy lại các query ví dụ bị lỗi trước đó (như câu hỏi viêm lệ quản, Propess, Khorana) thông qua API `POST /v1/chat/completions` cục bộ và so sánh text trước - sau.
- Kỳ vọng: Model hoàn toàn không còn nhắc tới "WHO, NHS" hay "Diabetic ketoacidosis".
- Kỳ vọng: Model trả lời theo khung định dạng rõ ràng, tập trung phân tích bài gốc hoặc kết luận là không có dữ liệu nếu cần.

### Manual Verification
- Tải lại trang web `http://localhost:8501`, chat với các câu hỏi Hard trong BMJ eval. Đọc trực tiếp câu trả lời trên màn hình.
