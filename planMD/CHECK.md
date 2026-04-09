Cách kiểm tra “dữ liệu sau cùng” trước khi đưa vào RAG nên làm theo một **pipeline kiểm định cuối**, không phải chỉ nhìn file có đẹp hay không. Với repo bạn đang bám, vì hệ thống dùng **Qdrant để retrieve** và về bản chất dựa rất nhiều vào **payload/metadata để lọc và giải thích kết quả**, bước kiểm định cuối nên tập trung vào 3 câu hỏi:
**(1)** tài liệu có sạch và đúng cấu trúc không,
**(2)** retrieve thử có ra đúng đoạn đúng nguồn không,
**(3)** câu trả lời sinh ra có bám nguồn hay vẫn bịa thêm. Repo hiện là một RAG stack với Streamlit, FastAPI orchestrator, Qdrant search, external LLM và Redis-backed history, nên bạn hoàn toàn có thể chèn một “pre-ingest QA gate” trước khi cho dữ liệu vào collection chính. ([GitHub][1])

Tôi khuyên bạn kiểm định theo **5 tầng**, và chỉ khi qua đủ 5 tầng mới cho ingest.

## 1) Kiểm tra cấu trúc dữ liệu

Ở tầng này, mục tiêu là xác nhận mỗi record đủ thông tin để sau này retrieve, filter và cite được.

Mỗi record cuối cùng nên có tối thiểu:

* `doc_id`
* `title`
* `section_title`
* `body`
* `source_name`
* `source_url`
* `doc_type`
* `specialty`
* `audience`
* `language`
* `updated_at`
* `version`
* `trust_tier`

Nếu bạn chưa dùng hết các field đó trong app hiện tại thì vẫn nên giữ trong source-of-truth, vì Qdrant hỗ trợ gắn **payload JSON** vào vector points và dùng chúng để lọc khi search. ([Qdrant][2])

Cách kiểm tra cụ thể:

* không có field bắt buộc nào bị thiếu
* `doc_id` là duy nhất
* `source_url` hợp lệ
* `updated_at` parse được
* `audience`, `doc_type`, `specialty` nằm trong enum bạn định nghĩa
* `body` không rỗng, không quá ngắn, không chỉ là tiêu đề
* không còn HTML thô, CSS, menu, footer, boilerplate

Tôi thường đặt một rule rất thực dụng: record nào **không thể tạo citation tử tế** thì chưa được vào RAG.

## 2) Kiểm tra chất lượng nội dung trước chunking

Đây là bước rất hay bị bỏ qua. Một corpus “đúng schema” vẫn có thể rất tệ nếu nội dung bẩn.

Bạn nên chạy kiểm tra tự động và kiểm tra thủ công theo lô:

**Kiểm tra tự động**

* tỷ lệ ký tự lạ, OCR lỗi
* tỷ lệ câu lặp
* tỷ lệ boilerplate lặp giữa các tài liệu
* tỷ lệ đoạn quá ngắn hoặc quá dài
* tài liệu có lẫn nhiều chủ đề không liên quan
* có bản cũ và bản mới cùng tồn tại không

**Kiểm tra thủ công**

* lấy ngẫu nhiên 50–100 record mỗi nguồn
* đọc xem có thực sự là tri thức y khoa hay chỉ là navigation text
* xem source có đáng tin không
* xem phần mở đầu có nêu rõ tài liệu gì, dành cho ai, cập nhật khi nào không

Một nguyên tắc rất quan trọng: **đừng đánh giá corpus bằng mắt trên file gốc, hãy đánh giá trên record sau khi đã làm sạch**.

## 3) Kiểm tra chunk sau khi chunking

Nhiều hệ thống hỏng không phải vì tài liệu gốc kém, mà vì chunk xấu.

Sau khi chunk, bạn cần kiểm tra từng chunk có còn giữ được ngữ cảnh không. Với RAG tri thức, chunk tốt phải trả lời được ít nhất câu hỏi:
“Đây là đoạn gì, của nguồn nào, nói về chủ đề nào?”

Tôi khuyên bạn kiểm tra 6 thứ:

**Một là, chunk có mất tiêu đề không.**
Nếu chunk chỉ còn thân bài mà không còn `title` hoặc `section_title`, retrieval sẽ yếu hơn và citation cũng khó đọc hơn.

**Hai là, chunk có bị cắt ngang bảng, bullet list, recommendation block không.**
Đây là lỗi phổ biến nếu chunk theo ký tự quá cơ học.

**Ba là, chunk có đủ provenance không.**
Ít nhất phải truy ngược được về `source_name`, `doc_id`, `section_title`, `updated_at`.

**Bốn là, chunk có quá nhiều trùng lặp không.**
Nếu một guideline bị cắt thành nhiều chunk chồng lấp quá mạnh, search sẽ trả về 4 đoạn gần như giống nhau.

**Năm là, chunk có quá “chatty” không.**
Nếu bạn lỡ ingest dữ liệu hội thoại hoặc FAQ kiểu nói chuyện quá dài, chunk sẽ nhiễu.

**Sáu là, chunk id có ổn định không.**
Nếu id thay đổi mỗi lần re-index, citation và debugging sẽ rất mệt.

Ở bước này, tôi luôn yêu cầu một báo cáo thống kê:

* số tài liệu
* số chunk
* trung bình token/chunk
* p50/p95 token/chunk
* top nguồn theo số chunk
* top tài liệu tạo nhiều chunk nhất
* top tỷ lệ duplicate theo hash/similarity

Nếu p95 quá lớn hoặc duplicate quá cao, dừng lại và chỉnh pipeline.

## 4) Kiểm tra retrieval thật sự

Đây là bước quyết định nhất. Dữ liệu chỉ được coi là “sẵn sàng cho RAG” khi retrieve thử ra đúng.

Bạn nên làm một **retrieval validation set** khoảng 100–300 câu hỏi trước. Bộ này không cần quá lớn, nhưng phải có đủ loại:

* câu định nghĩa
* câu giải thích cơ chế
* câu guideline
* câu patient education
* câu so sánh
* câu follow-up ngắn
* câu đáng lẽ phải từ chối

Với mỗi câu, bạn gán:

* tài liệu nguồn đúng
* section đúng
* câu trả lời chuẩn ngắn
* nhãn: trả lời được / không đủ dữ liệu

Rồi chạy search thử trên Qdrant và đo:

* **Hit@1**: top 1 có đúng nguồn/section không
* **Hit@3**
* **MRR** hoặc vị trí chunk đúng
* tỷ lệ retrieve nhầm nguồn cũ
* tỷ lệ retrieve đúng topic nhưng sai audience
* tỷ lệ retrieve ra duplicate

Qdrant hỗ trợ **payload** và **filtering**, nên bạn nên test thêm retrieval có filter:

* chỉ guideline
* chỉ patient-friendly
* chỉ chuyên khoa tim mạch
* chỉ tài liệu cập nhật sau một mốc nhất định
  Nếu filter làm kết quả tốt hơn rõ rệt, đó là dấu hiệu metadata của bạn đang có giá trị và nên được dùng trong production. ([Qdrant][3])

Tôi khuyên một ngưỡng vận hành thực dụng như sau, đây là **target nội bộ do tôi đề xuất**, không phải chuẩn chính thức:

* Hit@3 cho câu hỏi fact/guideline nên đạt khoảng **80%+**
* duplicate trong top 5 nên thấp
* câu follow-up sau query rewrite phải tốt hơn câu gốc
* câu “không đủ dữ liệu” không được kéo về các chunk yếu chỉ vì semantic gần gần

## 5) Kiểm tra câu trả lời end-to-end

Sau khi retrieval ổn, mới kiểm tra generation. Bước này nhằm xác nhận model không “nói hay hơn dữ liệu”.

Bạn lấy cùng bộ câu hỏi đó, chạy full pipeline rồi chấm 5 tiêu chí:

**Groundedness**
Câu trả lời có bám hoàn toàn vào chunk đã retrieve không.

**Citation correctness**
Citation có trỏ đúng tài liệu/section không.

**Unsupported claim rate**
Có ý nào xuất hiện trong câu trả lời nhưng không thấy trong nguồn không.

**Abstention quality**
Khi không có dữ liệu đủ mạnh, hệ thống có từ chối đúng không.

**Freshness correctness**
Nếu có nhiều version, hệ thống có dùng bản mới đúng không.

Vì mục tiêu của bạn là **cung cấp kiến thức**, tôi sẽ đặt tiêu chí chặt hơn:

* thà trả lời ngắn nhưng đúng nguồn
* còn hơn trả lời sâu nhưng thêm thắt ngoài nguồn

## 6) Quy trình kiểm tra cuối cùng trước khi ingest production

Tôi đề xuất bạn làm hẳn một **Go/No-Go checklist**. Mỗi batch dữ liệu mới chỉ được vào collection production nếu qua hết:

### A. Data QA

* schema pass 100%
* URL/source hợp lệ
* metadata enum hợp lệ
* không có tài liệu rỗng
* không có duplicate exact-match

### B. Chunk QA

* chunk size nằm trong khoảng hợp lý
* có title/section/source đi kèm
* duplicate ratio dưới ngưỡng nội bộ
* không có chunk cực ngắn/cực dài bất thường

### C. Retrieval QA

* retrieval set đạt target Hit@k
* filter test pass
* không lẫn nhiều version cũ
* follow-up query test ổn

### D. Answer QA

* citation đọc được
* unsupported claim rate thấp
* abstention ổn
* không trả lời bằng giọng chẩn đoán nếu không có nguồn phù hợp

### E. Release QA

* index staging chạy ổn
* smoke test query pass
* backup snapshot có sẵn
* có thể rollback collection cũ

Nếu thiếu một trong các bước này, chưa nên cho vào production.
