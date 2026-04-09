Dưới đây là **bản Phase 4 hoàn chỉnh** theo đúng format proposal của bạn, nhưng đã được chỉnh lý để:

* nối tiếp hợp lý sau **Phase 3**
* giữ scope đúng cho đồ án
* tránh biến Phase 4 thành một pha “chữa mọi thứ bằng prompt”
* tập trung vào 3 mục tiêu thật sự cần thiết:

  * **lọc rác trước evidence layer**
  * **bounded execution / anti-sycophancy**
  * **decomposed retrieval cho query đa trục** 

---

# Phase 4: Garbage Filtration, Bounded Execution, and Decomposed Multi-Axis Retrieval

Mục tiêu cốt lõi của **Phase 4** là xử lý ba hạn chế còn tồn tại sau Phase 3:

1. **Garbage Chunk Pollution**
   Một số chunk gần nghĩa nhưng thực chất là rác học thuật, bibliography fragment, DOI line, tiêu đề cụt, hoặc đoạn tham khảo bị kéo lên do vector similarity.

2. **Sycophantic / Unsupported Synthesis**
   Hệ thống đã biết “mở đầu có giới hạn” nhưng vẫn có xu hướng cố chiều người dùng bằng cách tạo ra liên hệ không được evidence support.

3. **Average-Vector Failure ở Query Đa Trục**
   Với những câu hỏi cơ chế đa hệ, đa yếu tố, hoặc dạng “liên hệ nhiều lớp”, một embedding duy nhất thường chỉ tạo ra một vector trung bình, khiến retriever bỏ lỡ các tài liệu mạnh nhưng chuyên sâu theo từng trục nhỏ.

Phase 4 **không nhằm triển khai consensus-lite hoàn chỉnh**. Pha này chỉ nhằm làm cho evidence pipeline:

* sạch hơn
* trung thực hơn
* phù hợp hơn với các query đa trục khó 

---

## User Review Required

> [!CAUTION] Phase 4 có thay đổi hành vi truy hồi và sinh câu trả lời
> Pha này sẽ:
>
> * lọc bỏ chunk rác trước khi aggregate
> * chặn answer cố suy diễn vượt ra ngoài evidence
> * với query `mechanistic_synthesis`, không còn search một lần duy nhất mà có thể phân rã query thành 2–3 sub-query
>
> Đổi lại:
>
> * retrieval path sẽ phức tạp hơn
> * có thêm logic merge/rerank candidate pool
> * bounded execution có thể làm answer “thẳng tay từ chối” hơn trước
>
> Đây là đánh đổi chủ ý để tăng **evidence fidelity** và giảm **unsupported synthesis**.

---

# Proposed Changes

## 1. Garbage Chunk Filtration (Tăng Extraction Fidelity)

### Vấn đề

Qdrant hiện có thể trả về các chunk gần nghĩa nhưng **không có giá trị bằng chứng**, ví dụ:

* citation fragment
* bibliography line
* DOI / journal metadata line
* tiêu đề cụt
* đoạn text quá ngắn nhưng không chứa nội dung chuyên môn thực chất

Những chunk này nếu lọt vào evidence pipeline sẽ:

* làm aggregator chấm sai article quality
* làm extractor lấy nhầm số
* làm answer layer bị nhiễu hoặc viện dẫn sai nguồn

### Giải pháp kiến trúc

Chèn một lớp **Chunk Quality Filter** giữa:

* đầu ra raw chunks của retriever
* và đầu vào của article aggregation

### [MODIFY] `services/rag-orchestrator/app/retriever.py`

hoặc tạo utility riêng:

* `services/rag-orchestrator/app/chunk_quality_filter.py`

> Khuyến nghị: tách riêng utility thay vì nhét thẳng logic vào `article_aggregator.py`, vì đây là **chunk-level quality control**, không phải article scoring.

### Cơ chế hoạt động

Áp dụng hàm `is_junk_chunk()` hoặc `chunk_quality_score()`.

### Luật lọc đề xuất

#### Hard Reject

Loại bỏ ngay nếu:

* chứa `Tài liệu tham khảo`, `References`, `Bibliography`
* chứa pattern kiểu citation line:

  * `[0-9]{4};[0-9]+(?:\([0-9]+\))?:`
  * DOI line
  * volume(issue):page format quá đậm đặc
* quá ngắn và không có câu hoàn chỉnh
* chỉ là title/header rời rạc
* tỷ lệ ký hiệu / số / punctuation bất thường cao

#### Soft Penalty

Không loại ngay nhưng giảm ưu tiên nếu:

* digit ratio quá cao
* punctuation density quá cao
* thiếu verb / domain term meaningful
* information density thấp
* có dấu hiệu reference fragment nhưng chưa chắc chắn

### Hàm gợi ý

```python id="vajb6n"
def is_junk_chunk(text: str) -> bool:
    ...
```

hoặc tốt hơn:

```python id="7lsuqs"
def chunk_quality_score(text: str) -> float:
    ...
```

### Luồng mới

```text id="6x26do"
Raw Qdrant Results
   ↓
Chunk Quality Filter
   ↓
Clean Candidate Chunks
   ↓
Article Aggregator
```

### Kết quả mong đợi

* fragment reference không còn lên làm primary evidence
* extractor ít bị nhầm số hơn
* aggregator ít bị bias bởi garbage chunks

---

## 2. Bounded Execution & Anti-Sycophancy Guard

### Vấn đề

Sau Phase 3, hệ thống đã biết **mở đầu có giới hạn** (`bounded opening`), nhưng vẫn còn nguy cơ:

* cố chiều người hỏi
* tự bắc cầu giữa evidence A và khái niệm B không được support
* dùng tri thức nền để “lấp khoảng trống” trong bài retrieved

Ví dụ:

* tài liệu chỉ nói về tỷ lệ, hiện trạng, hoặc burden
* nhưng answer lại suy diễn sang cơ chế hoặc quan hệ nhân quả mà source không hề hỗ trợ

### Giải pháp kiến trúc

Không chỉ thêm prompt penalty, mà thêm **Bounded Execution Policy** ở 3 lớp:

#### Lớp 1 — Evidence Gap Signaling

Evidence layer phải truyền rõ xuống answer layer:

* `missing_requirements`
* `unsupported_concepts`
* `concept_evidence_gap`
* `allowed_answer_scope`

#### Lớp 2 — Prompt Rule cứng

System prompt phải cấm:

* ngoại suy không có supporting evidence
* nối khái niệm chưa được source support
* dùng helpfulness để trả lời thay bằng groundedness

#### Lớp 3 — Output Refusal Template

Nếu một phần câu hỏi không có evidence support trực tiếp, model phải trả lời theo mẫu từ chối cứng, thay vì “cố nói cho có ích”.

---

### [MODIFY] `services/rag-orchestrator/app/prompt.py`

### Rule mới đề xuất

Bổ sung vào System Prompt:

* Nếu tài liệu retrieved **không chứa bằng chứng trực tiếp** cho khái niệm mà người dùng hỏi, phải nói rõ điều đó.
* Không được tự nối giữa “bài có nhắc X” và “khái niệm Y” nếu source không chứng minh mối liên hệ đó.
* Không được dùng tri thức ngoài evidence pack để lấp chỗ trống.
* Nếu evidence chỉ hỗ trợ một phần câu hỏi, phải chia rõ:

  * phần nào có evidence
  * phần nào không có evidence

### Mẫu bounded refusal đề xuất

> **Tài liệu [1] và [2] chỉ cung cấp bằng chứng về [X]. Chúng không cung cấp dữ kiện trực tiếp để kết luận về [Y]. Vì vậy, tôi không thể suy ra mối liên hệ giữa [X] và [Y] chỉ từ dữ liệu nội bộ hiện có.**

### Khi nào kích hoạt?

Khi:

* `unsupported_concepts` không rỗng
* hoặc `concept_evidence_gap == True`
* hoặc `missing_requirements` có phần trùng với khái niệm user đang hỏi

### Kết quả mong đợi

* giảm kiểu answer “chiều người hỏi”
* giảm unsupported synthesis
* answer trung thực hơn, đặc biệt với query cơ chế hoặc liên hệ đa tầng

> Ghi chú: phần này nên được mô tả là **bounded execution** hoặc **unsupported-claim guard**, thay vì chỉ gọi là “anti-sycophancy prompt”, vì như vậy nghe kỹ thuật và dễ bảo vệ hơn.

---

## 3. Decomposed Multi-Axis Retrieval cho Mechanistic Queries

### Vấn đề

Với các query kiểu:

* cơ chế đa hệ thống
* liên kết nhiều yếu tố
* câu hỏi dài, nhiều vế
* “tại sao / bằng cách nào / qua những trục nào”

nếu chỉ embedding toàn bộ câu một lần, vector thường bị “trung bình hóa”, dẫn đến:

* retrieve các bài rất chung chung
* bỏ lỡ bài mạnh nhưng chuyên sâu cho từng trục nhỏ

### Giải pháp kiến trúc

Khi `query_router` trả về:

```text id="7lmr4w"
retrieval_mode == "mechanistic_synthesis"
```

thì không search một lần duy nhất nữa, mà gọi:

* **Mechanistic Query Decomposer**
* rồi chạy **Multi-Query Retrieval**

### [MODIFY] `services/rag-orchestrator/app/retriever.py`

Có thể thêm module phụ:

* `mechanistic_query_decomposer.py`

### Cơ chế hoạt động

#### Bước 1 — Query Decomposition

Từ query gốc, sinh ra 2–3 sub-queries ngắn hơn, mỗi query đại diện cho một trục ý nghĩa.

Ví dụ:

* Query gốc:

  * “nhiễm khuẩn bệnh viện, thời gian chờ, áp lực nhân viên liên hệ thế nào với sụp đổ chất lượng hệ thống”
* Sub-query:

  1. `kiểm soát nhiễm khuẩn bệnh viện`
  2. `thời gian chờ khám và chất lượng dịch vụ`
  3. `áp lực nhân viên y tế burnout`

#### Bước 2 — Multi-Retrieval

Mỗi sub-query gọi Qdrant riêng.

#### Bước 3 — Merge Candidate Pool

Toàn bộ candidate chunks được gộp vào chung một pool.

#### Bước 4 — Dedup + Junk Filter

Áp chunk quality filter từ mục 1.

#### Bước 5 — Regroup + Rerank

Nhóm lại theo article, rồi rerank theo:

* semantic relevance
* authority
* cross-subquery coverage

---

### Query Decomposer: cách triển khai

Nên có **2 lớp fallback**:

#### Ưu tiên

LLM decomposition:

* ngắn
* tối đa 3 subqueries
* mỗi subquery chỉ giữ 1 trục ý chính

#### Fallback

Heuristic decomposition:

* tách theo conjunctions
* hoặc tách theo concept keywords/domain terms

### Ràng buộc để tránh scope creep

* chỉ bật cho `mechanistic_synthesis`
* tối đa 3 subqueries
* mỗi subquery không quá dài
* nếu decomposition thất bại → fallback về single-query retrieval

### Luồng mới

```text id="cgnbw2"
Mechanistic Query
   ↓
Query Decomposer
   ↓
2–3 Subqueries
   ↓
Qdrant Search x N
   ↓
Merged Candidate Pool
   ↓
Junk Filter
   ↓
Article Regroup + Rerank
   ↓
Evidence Extraction
```

### Kết quả mong đợi

* tăng breadth cho query đa trục
* giảm việc chỉ lấy các bài “chung chung”
* dễ kéo được các source chuyên sâu cho từng nhánh

---

# Open Questions

> [!IMPORTANT] Thống nhất ngưỡng lọc rác
> Bạn có muốn bắt đầu bằng bộ rule bảo thủ hơn không?
>
> * hard reject với bibliography/reference chunks thật rõ
> * soft penalty với các chunk nghi ngờ
>
> Đây là cách an toàn hơn so với loại mạnh tay ngay từ đầu.

> [!IMPORTANT] Thống nhất bounded refusal wording
> Bạn có muốn dùng đúng giọng điệu này không?
>
> **"Tài liệu [1] và [2] chỉ cung cấp bằng chứng về [X]. Chúng không cung cấp dữ kiện trực tiếp để kết luận về [Y]. Vì vậy, tôi không thể suy ra mối liên hệ giữa [X] và [Y] chỉ từ dữ liệu nội bộ hiện có."**
>
> Hoặc bạn muốn một giọng điệu mềm hơn / học thuật hơn?

> [!IMPORTANT] Thống nhất số lượng subqueries
> Khuyến nghị Phase 4 chỉ dùng tối đa **2–3 subqueries** cho `mechanistic_synthesis`.
> Đây là giới hạn tốt để:
>
> * tránh noise
> * tránh latency cao
> * tránh pool quá lớn

---

# Verification Plan

## Automated Tests

### 1. Garbage Filter Regression Test

* tạo tập chunks mẫu gồm:

  * bibliography fragments
  * DOI lines
  * headers
  * good study chunks
* đảm bảo:

  * junk chunks bị reject
  * useful chunks không bị loại oan quá mức

### 2. Retrieval Regression

* chạy lại Gate G3
* kiểm tra junk filter + decomposed retrieval không làm tụt retrieval metrics ở các query bình thường
* đặc biệt:

  * fact queries
  * study_result queries
  * multi-turn queries

### 3. Multi-Axis Retrieval Unit Test

* với `mechanistic_synthesis`, verify:

  * query được decomposed thành 2–3 subqueries
  * merged candidate pool có article diversity tốt hơn
  * vẫn có rerank sau merge
* nếu decomposition fail → fallback single-query hoạt động đúng

### 4. Bounded Execution Unit Test

* khi `unsupported_concepts` có dữ liệu → prompt phải inject refusal guard
* khi evidence chỉ support một phần → answer template phải phân biệt rõ phần supported và unsupported
* khi evidence đủ → không từ chối oan

---

## Manual Verification

### 1. Chạy lại các câu hỏi đa hệ / mechanistic khó

Đối chiếu:

* trước Phase 4: answer có bị “vẽ cầu” giữa các khái niệm không
* sau Phase 4: answer có biết từ chối đúng phần unsupported không

### 2. Kiểm tra garbage chunks

Với một số query trước đây từng retrieve ra fragment rác:

* xem fragment có còn vào pool không
* xem aggregator có còn bị lệch không

### 3. Kiểm tra multi-axis retrieval

Với query dài nhiều vế:

* số article distinct có tăng không
* có kéo được article chuyên sâu cho từng vế không
* primary article có còn hợp lý không

---

# Acceptance Criteria

Phase 4 được coi là đạt khi:

## Garbage Filtration

* chunk rác không còn lọt thường xuyên vào evidence layer
* extractor ít nhầm số từ bibliography/reference fragments hơn

## Bounded Execution

* giảm rõ kiểu answer “chiều người hỏi”
* giảm unsupported synthesis
* answer biết tách:

  * phần có evidence
  * phần không có evidence

## Decomposed Retrieval

* query `mechanistic_synthesis` có article diversity tốt hơn
* query bình thường không bị chậm hoặc nhiễu quá mức
* fallback single-query vẫn an toàn

---

# Implementation Order

## Bước 1

`chunk_quality_filter.py` hoặc logic tương đương

* `is_junk_chunk`
* `chunk_quality_score`

## Bước 2

`retriever.py`

* tích hợp junk filter
* chỉ giữ clean candidate pool

## Bước 3

`prompt.py`

* thêm bounded execution rules
* thêm refusal template theo `unsupported_concepts`

## Bước 4

`query_router.py`

* xác nhận `mechanistic_synthesis` routing ổn
* nếu chưa đủ, tinh chỉnh rule detect

## Bước 5

`mechanistic_query_decomposer.py` + `retriever.py`

* subquery generation
* multi-query search
* merge + rerank

## Bước 6

tests + regression + manual verification

---

# Out of Scope for Phase 4

Các phần sau **không thuộc Phase 4**:

* full conflict detector
* consensus-lite hoàn chỉnh
* claim verifier hậu answer
* NLI contradiction model
* expert review diện rộng
* full natural-language evidence normalization

---

# Kết luận

Phase 4 này có nhiệm vụ rất rõ:

> **làm sạch đầu vào của evidence layer, chặn suy diễn không được hỗ trợ, và mở rộng truy hồi cho các query đa trục khó.**

Nó là pha nối tiếp hợp lý sau Phase 3 vì:

* Phase 3 khóa evidence structure
* Phase 4 khóa evidence cleanliness và bounded execution
* sau đó mới nên nghĩ đến consensus-lite
