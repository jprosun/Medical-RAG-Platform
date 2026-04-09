# Trả lời chi tiết từng tiêu chí — Dựa hoàn toàn vào dự án

> Mọi thông tin dưới đây đều trích dẫn trực tiếp từ source code, config, benchmark reports, và dữ liệu thực tế trong dự án LLM-MedQA-Assistant.

---

## 1) Đúng nội dung chuyên môn

### Phạm vi chuyên môn

**Đa chuyên khoa y khoa**, bao phủ 10 nhóm chuyên khoa (theo gold benchmark):

| # | Chuyên khoa | Nguồn VN | Nguồn EN |
|---|---|---|---|
| 1 | Chẩn đoán - xét nghiệm - hình ảnh | VMJ OJS | NCBI Bookshelf |
| 2 | Dược - điều dưỡng - quản trị lâm sàng | VMJ OJS, KCB MOH | MedlinePlus |
| 3 | Ngoại khoa - chấn thương | VMJ OJS | NCBI Bookshelf |
| 4 | Nhi khoa - sơ sinh | VMJ OJS | MedlinePlus |
| 5 | Nhiễm trùng - hô hấp | VMJ OJS | WHO |
| 6 | Sản phụ khoa - sinh sản | VMJ OJS | — |
| 7 | Thần kinh - tâm thần | VMJ OJS | NCBI Bookshelf |
| 8 | Tim mạch - chuyển hóa - thận tiết niệu | VMJ OJS | WHO, NCBI |
| 9 | Ung bướu - huyết học | VMJ OJS | — |
| 10 | Y tế công cộng - phục hồi - chất lượng sống | WHO Vietnam | WHO |

Top specialties trong corpus VN (VMJ): gastroenterology (2,386), oncology (2,010), surgery (1,496), hematology (1,127), general (1,030), infectious_disease (736), cardiology (716), radiology (694).

### Đối tượng trả lời chính

Được định nghĩa trong system prompt ([prompt.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/prompt.py) L27):

> *"Bạn là **chuyên gia y khoa và giảng viên lâm sàng** (clinical educator). Trả lời các câu hỏi y học bằng tiếng Việt với **văn phong học thuật**, chính xác, có tổ chức logic và luôn dẫn nguồn."*

Audience metadata trong corpus:

| Audience | Nguồn | Records |
|---|---|---|
| `clinician` | WHO, KCB MOH, WHO Vietnam | 898 |
| `student` | NCBI Bookshelf | 1,066 |
| `patient` | MedlinePlus | 199 |

→ **Đối tượng chính: sinh viên y khoa và bác sĩ** (clinician + student chiếm 92% corpus EN). Phần VN (VMJ 14,384 records) mặc định audience = `clinician`.

### Bộ nguồn tri thức đang dùng

| # | Nguồn | Loại | Records | Trust Tier | Ngôn ngữ |
|---|---|---|---|---|---|
| 1 | **Tạp chí Y học Việt Nam (VMJ)** | Peer-reviewed journal | 14,384 | Tier 2 | VI |
| 2 | **Cục Quản lý KCB - Bộ Y tế** | Government guideline | 1,848 | Tier 1 | VI |
| 3 | **NCBI Bookshelf** | Medical textbook | 1,066 | Tier 2 | EN |
| 4 | **WHO Vietnam** | Int'l organization | 475 | Tier 1 | VI |
| 5 | **WHO (global)** | Int'l organization | 423 | Tier 1 | EN |
| 6 | **Tạp chí Y dược cổ truyền** | Peer-reviewed journal | 263 | Tier 2 | VI |
| 7 | **Tạp chí Y dược quân sự** | Peer-reviewed journal | 248 | Tier 2 | VI |
| 8 | **MedlinePlus** | Patient education | 199 | Tier 3 | EN |
| 9 | **Tạp chí ĐH Y Dược Huế** | Peer-reviewed journal | 135 | Tier 2 | VI |
| 10 | **Tạp chí ĐH Y Dược Cần Thơ** | Peer-reviewed journal | 117 | Tier 2 | VI |
| 11 | **Cục Quản lý Dược (DAV)** | Government reference | 24 | Tier 1 | VI |

**Tổng: ~19,182 records** từ 11 nguồn.

### Nguyên tắc ưu tiên nguồn

Định nghĩa trong [vn_metadata_enricher.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_metadata_enricher.py):

| Trust Tier | Nguồn | Ưu tiên |
|---|---|---|
| **Tier 1** (Canonical) | WHO, BYT (kcb_moh, dav_gov) | Cao nhất — cơ quan có thẩm quyền |
| **Tier 2** (Reference) | VMJ, Huế, Cần Thơ, Quân sự, Cổ truyền, NCBI | Trung bình — peer-reviewed |
| **Tier 3** (Patient) | MedlinePlus | Thấp nhất — patient education |

Tuy nhiên, article ranking hiện tại ([article_aggregator.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/article_aggregator.py) L134) **chưa dùng trust_tier** trong scoring formula:

```python
score = 0.35 * max_chunk_score + 0.20 * avg_chunk_score
     + 0.15 * section_diversity + 0.15 * numeric_density + 0.15 * kw_score
```

→ Ưu tiên nguồn hiện tại dựa trên **semantic relevance** chứ chưa dựa trên **evidence strength**.

### 10–20 câu hỏi mẫu khó

Dự án có **2 bộ gold benchmark** đã soạn sẵn:

**Bộ VN (100 câu)** — [vmj_retrieval_gold_g3_v2.jsonl](file:///d:/CODE/DATN/LLM-MedQA-Assistant/benchmark/datasets/vmj_retrieval_gold_g3_v2.jsonl):
- 20 easy, 50 medium, 30 hard
- 10 chuyên khoa × 10 câu
- 21 loại câu hỏi (intervention_outcome, diagnostic_value, risk_factor...)
- Query được paraphrase (không copy title)

**Bộ EN (68 câu)** — [eval_queries.json](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/eval_queries.json):
- 25 fact, 13 truncated_list, 12 mixed_topic, 10 filter, 8 multi_turn

Ví dụ 10 câu khó (hard) từ gold VN:

| # | Query | Type |
|---|---|---|
| 1 | U mô đệm dạ dày được phẫu thuật tại Việt Đức có đặc điểm lâm sàng cận lâm sàng gì? | intervention_outcome |
| 2 | COPD quản lý ngoại trú có những yếu tố nào làm tăng nguy cơ đợt cấp? | risk_factor_association |
| 3 | Viêm phổi liên quan thở máy ở người bệnh hồi sức lớn tuổi có căn nguyên vi sinh gì? | microbiology_pattern |
| 4 | Trẻ động kinh dưới 6 tuổi có kiểu gen gì, điện não đồ ra sao? | genomics_biomarker |
| 5 | Bướu thận nằm hoàn toàn trong xoang thận có thể vẫn cắt bán phần nhờ robot? | rare_case |
| 6 | Một thuốc kháng đông đường uống trực tiếp có chi phí hiệu quả ra sao cho phòng ngừa? | health_economics |
| 7 | Phân tích mạng lưới giữa sức khỏe tâm thần và chất lượng cuộc sống ở nam giới? | evidence_synthesis |
| 8 | Tái tạo vú tức thì sau đoạn nhũ bằng túi độn tác động thế nào lên hình thể? | intervention_outcome |
| 9 | Hội chứng cổ rùa ở sinh viên dùng điện thoại có tần suất như thế nào? | health_behavior |
| 10 | Một thang sàng lọc suy yếu rút gọn có đủ hữu ích cho người già đa bệnh không? | diagnostic_value |

### Đáp án chuẩn tham chiếu

Mỗi gold query có `expected_title` — tiêu đề bài báo đúng cần tìm. Ví dụ:

```json
{
  "query": "CK19 có phải là yếu tố tiên lượng độc lập của tái phát sớm ở carcinôm tế bào gan",
  "expected_title": "MÔ BỆNH HỌC CÓ Ý NGHĨA TIÊN LƯỢNG Ở CARCINÔM TẾ BÀO GAN",
  "rationale": "Query nhấn vào ý nghĩa tiên lượng thay vì chép lại tiêu đề"
}
```

Đáp án do **người phát triển tự soạn** dựa trên nội dung corpus. Chưa có đáp án từ giảng viên/bác sĩ hướng dẫn.

---

## 2) Đúng nguồn trích dẫn / bám bằng chứng

### Cấu trúc metadata của từng tài liệu

Mỗi document record có **18 fields** (định nghĩa trong [document_schema.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/app/document_schema.py)):

```
doc_id, title, body, source_name, source_url, section_title,
doc_type, specialty, audience, language, trust_tier,
heading_path, published_at, updated_at, authors,
quality_score, quality_status, quality_flags
```

Metadata được gắn vào mỗi chunk khi ingest vào Qdrant (payload).

### Quy tắc gán citation [1], [2], [3]

Trong [prompt.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/prompt.py) L42-43:

```
QUY TẮC DẪN NGUỒN:
- Citation trong câu: "Tỷ lệ tái phát là 23.5% [1]"
- Cuối câu trả lời: liệt kê đầy đủ nguồn theo format [n] Tên bài - Tạp chí
```

Primary source luôn được gán `[1]` (đánh dấu `★ TÀI LIỆU CHÍNH [1]` trong context). Secondary sources được gán `[2]`, `[3]` theo thứ tự article_score.

### Có bắt buộc mọi claim quan trọng phải đi kèm nguồn không?

**Có.** Rule #10 trong SYSTEM_RULES_V2:

> *"Mỗi claim quan trọng hoặc con số phải đi kèm citation [n] ngay trong câu."*

### Có câu trả lời nào từng cite sai chưa?

**Chưa có systematic test** cho citation accuracy. Dự án chỉ test retrieval accuracy (tìm đúng bài) chứ chưa test citation fidelity (cite đúng claim). Đây là **khoảng trống** cần lấp.

### Ví dụ câu trả lời thực tế có citation / raw retrieved chunks

Dự án **chưa có log output thực tế** lưu sẵn (chưa chạy production). Tuy nhiên, response API trả về:

```json
{
  "session_id": "uuid",
  "answer": "...[câu trả lời có citation [1], [2]]...",
  "context_used": 12,
  "retrieved_chunks": [
    {"id": "chunk_id", "text": "...", "metadata": {"title": "...", "source_name": "..."}}
  ]
}
```

→ Chunks được trả kèm response, cho phép audit. Nhưng chưa có kho sample answers.

---

## 3) Không bịa khi thiếu dữ liệu

### Cách chatbot đang fallback khi thiếu context

**3 tầng fallback** trong [main.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/main.py):

| Tình huống | Code | Hành vi |
|---|---|---|
| **Chunks = 0** (không tìm được gì) | L280-281, L356-358 | `"I don't have enough context. Ingest documents into Qdrant first."` |
| **Coverage = low** | coverage_scorer.py L125-126 | Prompt instruction: `"Dữ liệu KHÔNG ĐỦ. Phải nói rõ giới hạn."` + `force_abstain_parts` |
| **Coverage = medium** | coverage_scorer.py L251-254 | Prompt instruction: `"Dữ liệu CÓ nhưng THIẾU một số phần. Nói rõ: '{parts}' không có trong tài liệu."` |

### Ngưỡng coverage

Từ [coverage_scorer.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/coverage_scorer.py) L121-126:

```python
if avg_score >= 0.7 and scores.direct_answerability >= 0.5:
    level = "high"      # → ingest-only, không cần external
elif avg_score >= 0.4:
    level = "medium"    # → trả lời được nhưng thiếu 1-2 phần
else:
    level = "low"       # → phải cảnh báo thiếu dữ liệu
```

`avg_score` tính từ 4 tiêu chí: direct_answerability, numeric_coverage, methods_coverage, limitations_coverage.

### Prompt rules chống bịa

- Rule #3: *"KHÔNG bịa số. Mọi con số phải lấy từ evidence. Nếu không có số, KHÔNG đoán."*
- Rule #4: *"KHÔNG suy ra causal claim nếu evidence chỉ là association."*
- Rule #7: *"Không thêm kiến thức ngoài evidence pack."*
- Rule #8: *"Nếu evidence không đủ → NÓI RÕ phần nào không có dữ liệu, KHÔNG answer chung chung."*

### Câu hỏi ngoài phạm vi / thiếu dữ liệu — Output thật

**Chưa có kho output thật** cho các case out-of-scope. Dự án chưa chạy production test với các câu hỏi ngoài phạm vi. Đây là **khoảng trống cần test**.

Tuy nhiên, bộ eval EN có 8 câu multi_turn (follow-up ngắn như "Can it be cured?", "What about children?") — 7/8 (87.5%) tìm đúng source.

---

## 4) Biết nêu mức độ chắc chắn / giới hạn

### Quy tắc xác định certainty

Hệ thống dùng **Coverage Scorer** (heuristic, không dùng LLM) gồm 5 tiêu chí:

| Tiêu chí | Cách tính | Ảnh hưởng |
|---|---|---|
| **direct_answerability** | Keyword overlap giữa query và key_findings | 0.0 - 1.0 |
| **numeric_coverage** | Có numbers trong evidence? (OR, HR, AUC...) | Only if router says `requires_numbers` |
| **methods_coverage** | Có design, population, sample_size, setting? | 0 - 4 fields / 4 |
| **limitations_coverage** | Có limitation keywords trong raw text? | Only if router says `requires_limitations` |
| **conflict_risk** | Có secondary sources? | 0.0 or 0.2 |

### Phân biệt guideline mạnh với nghiên cứu đơn lẻ

**Một phần.** Hệ thống có `trust_tier` metadata (Tier 1 = guideline/WHO, Tier 2 = journal), và prompt phân biệt "★ TÀI LIỆU CHÍNH" vs "📄 TÀI LIỆU PHỤ". Tuy nhiên:

- Article ranking **chưa dùng trust_tier** trong scoring
- Coverage scorer **chưa phân biệt** strength-of-evidence giữa guideline và original study
- Prompt rule #6 yêu cầu nêu giới hạn khi "cỡ mẫu nhỏ, đơn trung tâm, hồi cứu" — nhưng đây là **instruction cho LLM**, không phải automated check

### Section "Giới hạn & Mức chắc chắn"

**Mọi answer template** đều có section này. Ví dụ `_TEMPLATE_STUDY` (L76-78):

```
## Giới hạn & Mức chắc chắn
- Hạn chế chính của nghiên cứu (nếu có trong tài liệu)
- Điểm cần thận trọng khi diễn giải
```

Khi coverage = low, prompt thêm instruction: *"Dữ liệu KHÔNG ĐỦ. Phải nói rõ giới hạn."*

---

## 5) Ưu tiên nguồn mạnh hơn nguồn yếu

### Danh sách loại nguồn trong corpus

| doc_type | Mô tả | Nguồn | Records |
|---|---|---|---|
| `guideline` | Hướng dẫn chẩn đoán/điều trị | WHO, KCB MOH | ~2,271 |
| `textbook` | Sách giáo khoa | NCBI Bookshelf | 1,066 |
| `review` | Bài tổng quan/nghiên cứu gốc | VMJ, Huế, Cần Thơ, Quân sự, Cổ truyền | ~15,147 |
| `patient_education` | Giáo dục sức khỏe | MedlinePlus | 199 |
| `reference` | Tra cứu thuốc | DAV | 24 |

> Lưu ý: Corpus hiện **không có meta-analysis** hoặc **systematic review** riêng biệt — tất cả peer-reviewed articles đều gán doc_type = `review`.

### Cách gán trust_tier

Trong [vn_metadata_enricher.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_metadata_enricher.py), trust_tier gán theo source_id:

```python
_TRUST_TIERS = {
    "kcb_moh": 1, "dav_gov": 1, "who_vietnam": 1,   # Tier 1: Government/WHO
    "vmj_ojs": 2, "hue_jmp_ojs": 2, ...              # Tier 2: Journals
}
```

EN sources: WHO=1, NCBI=2, MedlinePlus=3.

### Article ranking có dùng trust_tier không?

**KHÔNG.** Formula trong `_compute_article_score()`:

```python
score = 0.35 * max_chunk_score       # cosine similarity cao nhất
     + 0.20 * avg_chunk_score        # cosine similarity trung bình
     + 0.15 * section_diversity      # chunk_count / 6
     + 0.15 * numeric_density        # % chunks có số liệu
     + 0.15 * kw_score               # keyword overlap query↔title
```

**trust_tier hoàn toàn vắng mặt.** Ranking hiện dựa 100% vào semantic relevance + keyword overlap, không phân biệt guideline vs original study.

### Trường hợp xung đột guideline vs bài lẻ

Hệ thống **chưa có conflict detection** thực sự. `conflict_risk` trong coverage_scorer chỉ đơn giản = 0.2 nếu có secondary sources, 0.0 nếu không — **không phân tích nội dung** có mâu thuẫn hay không.

---

## 6) Trả lời đủ ý cho người có chuyên môn

### Kỳ vọng mỗi loại câu hỏi phải có những phần nào

Định nghĩa trong 6 answer templates ([prompt.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/prompt.py)):

| Query Type | Các phần bắt buộc |
|---|---|
| **fact_extraction** | Kết luận ngắn → Dữ liệu nghiên cứu → Nguồn |
| **study_result_extraction** | Kết luận → Thiết kế → Quần thể → Cỡ mẫu → Kết quả chính → Số liệu quan trọng → Giới hạn → Nguồn |
| **research_appraisal** | Kết luận → Bối cảnh → Thiết kế → Quần thể/Cỡ mẫu → Biến số → Phương pháp thống kê → Kết quả → Confounders → Giới hạn → Ý nghĩa ứng dụng → Nguồn |
| **comparative_synthesis** | Kết luận → Nguồn 1 (thiết kế + kết quả) → Nguồn 2 → Điểm giống/khác → Giới hạn → Nguồn |
| **guideline_comparison** | Kết luận → Dữ liệu ingest → Giới hạn → Nguồn |
| **teaching_explainer** | Kết luận → Giải thích cơ chế → Ý nghĩa lâm sàng → Giới hạn → Nguồn |

### Rubric "đủ ý"

Chưa có rubric formal từ giảng viên. Template hiện tại là rubric nội bộ do developer thiết kế. Tuy nhiên, prompt buộc:
- Rule #2: Phải trích số liệu trước diễn giải
- Rule #6: Phải nêu giới hạn
- Rule #10: Mỗi claim phải có citation

### Câu trả lời thực tế

**Chưa có kho câu trả lời thực tế.** Hệ thống chưa được chạy production để thu thập sample answers.

---

## 7) Xử lý câu hỏi follow-up

### Cơ chế hiện tại

[query_rewriter.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/query_rewriter.py):

**Bước 1 — Heuristic check** (`_needs_rewriting()`):
- Nếu ≤ 5 words → cần rewrite
- Nếu chứa referent words ("it", "this", "what about", "also"...) → cần rewrite
- Nếu không có history → không rewrite

**Bước 2 — LLM rewrite** (nếu có LLM client):
- System prompt: *"You are a query rewriter for a medical knowledge RAG system. Take a follow-up question and rewrite it as a STANDALONE search query."*
- Dùng last 2 turns (4 messages) làm context
- Max 150 tokens, temperature 0.1

**Bước 3 — Fallback rule-based** (nếu LLM fail):
- Lấy 8 words đầu của câu hỏi user trước → prepend "Regarding {topic}: {message}"

### Session history

[session.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/session.py) — Redis-backed. Prompt builder dùng **last 6 messages** làm chat history khi build prompt.

### Bộ test multi_turn

Dự án có **8 câu multi_turn** trong eval_queries.json. Ví dụ:

```json
{
  "query": "Can it be cured?",
  "mock_history": [
    {"role": "user", "content": "What is asthma?"},
    {"role": "assistant", "content": "Asthma is a chronic respiratory condition..."}
  ]
}
```

Kết quả retrieval: 7/8 (87.5%) tìm đúng source, 7/8 title Hit@3.

### Case rewrite sai

1 miss trong multi_turn: "Can it be cured?" (asthma) → tìm ra WHO thay vì MedlinePlus (đúng topic nhưng sai source expected).

### Các case chưa test

- Đổi chủ thể giữa chừng
- Follow-up phủ định / so sánh
- Hội thoại > 3 lượt

---

## 8) Ổn định giữa nhiều lần hỏi

### Cấu hình hiện tại

Từ [main.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/main.py) L337-338:

```python
max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
temperature = float(os.getenv("LLM_TEMPERATURE", "0.2"))
```

| Tham số | Giá trị | Ảnh hưởng |
|---|---|---|
| **temperature** | 0.2 | Thấp → ít biến động giữa các lần |
| **max_tokens** | 1024 | Giới hạn output length |
| **top_k retriever** | 8/12/20 theo profile | Cố định theo query type |
| **Cache** | Không (mỗi lần search Qdrant mới) | Retrieval luôn deterministic (cosine) |

### Yếu tố ổn định

- **Retrieval**: Deterministic (cosine similarity) → cùng query → cùng chunks
- **Router**: Rule-based (keyword match) → deterministic
- **Evidence Extractor**: LLM-based → có variance
- **Answer Composer**: LLM (temp=0.2) → variance thấp nhưng không zero

### Consistency test

**Chưa có.** Chưa chạy cùng câu hỏi nhiều lần để đo variance. Đây là **khoảng trống** cần test.

---

## 9) Thời gian phản hồi

### Monitoring hiện tại

Prometheus metrics trong [metrics.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/metrics.py):

```python
RAG_RETRIEVAL_LATENCY_SECONDS  # retrieval step
RAG_GENERATION_LATENCY_SECONDS # LLM inference step
RAG_CONTEXT_TOKENS             # estimated context tokens
```

Tách được:
- `retrieval_ms` → vector search
- `llm_ms` → LLM inference
- rewrite, router, aggregator, extractor, coverage → traced via OpenTelemetry spans

### Latency data thực tế

**Chưa có production logs.** Tuy nhiên:

- **Retrieval benchmark** (Gate G3): 68s cho 100 queries = **0.68s/query** (bao gồm embed + Qdrant search + collapse + rerank)
- **LLM inference**: Phụ thuộc vào backend (KServe + Mistral-7B hoặc tương đương). Config timeout = 180s.

### Tốc độ theo query type

| Profile | top_k | Predicted latency |
|---|---|---|
| `light` (fact) | 8 chunks | Nhanh nhất (ít context) |
| `standard` (study) | 12 chunks | Trung bình |
| `deep` (appraisal) | 20 chunks | Chậm nhất (nhiều context + extractor) |

### Context tokens

`est_tokens = sum(max(1, len(c.text) // 4) for c in chunks)` — ước lượng 1 token ≈ 4 chars.

---

## 10) Dễ kiểm chứng lại / audit được

### Response sample hoàn chỉnh

API [ChatResponse](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/rag-orchestrator/app/schemas.py) trả về:

```json
{
  "session_id": "uuid-123",
  "answer": "## Kết luận ngắn\n... [1]\n## Nguồn tham khảo\n[1] ...",
  "history": [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}],
  "context_used": 12,
  "retrieved_chunks": [
    {"id": "chunk_abc", "text": "Kết quả cho thấy...", "metadata": {"title": "...", "source_name": "VMJ"}}
  ]
}
```

### Full trace cho một phiên

**Có — OpenTelemetry**. Mỗi request tạo spans:

```
rag.chat (root)
  ├── session.load_history
  ├── session.append_user
  ├── query.rewrite          ← query.original, query.rewritten
  ├── query.route            ← router.query_type, router.depth
  ├── retriever.build
  ├── retrieval.vector_search ← vector.top_k, retrieval.chunks
  ├── article.aggregate      ← article.primary, article.secondary_count
  ├── evidence.extract       ← evidence.extractor_used, evidence.numbers_found
  ├── coverage.score         ← coverage.level, coverage.allow_external
  ├── prompt.build           ← prompt.version
  └── llm.inference          ← llm.model, llm.provider
```

### UI hiển thị nguồn

Trong [app.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/streamlit-ui/app.py) L216-217:

```python
if context_used > 0:
    st.caption(f"Context chunks used: {context_used}")
st.markdown(answer)
```

**Hiện tại UI chỉ hiện số chunks dùng** (ví dụ "Context chunks used: 12"), không hiện chunk details. Người dùng **không thể** click xem chunk gốc hoặc source_url từ UI.

Tuy nhiên, API response có `retrieved_chunks` → developer/auditor có thể truy ngược. UI cần cải thiện để end-user cũng xem được.

---

## 11) Scope phù hợp cho đồ án

### 1. Scope chuyên khoa

**Đa chuyên khoa y khoa Việt Nam**, tập trung vào:
- Nghiên cứu lâm sàng tiếng Việt (VMJ — 14,384 bài)
- Hướng dẫn chẩn đoán/điều trị BYT (1,848 quy trình)
- Tài liệu y khoa quốc tế EN (WHO, NCBI, MedlinePlus — 1,688 bài)

### 2. Loại câu hỏi chatbot nhận

6 loại (qua Query Router):

| Loại | Ví dụ |
|---|---|
| `fact_extraction` | "Định nghĩa X là gì?" |
| `study_result_extraction` | "AUC/OR/HR của nghiên cứu Y?" |
| `research_appraisal` | "Hạn chế, bias của nghiên cứu Z?" |
| `comparative_synthesis` | "So sánh phương pháp A vs B?" |
| `guideline_comparison` | "Theo guideline BYT, nên làm gì?" |
| `teaching_explainer` | "Giải thích cơ chế bệnh sinh X?" |

### 3. Loại câu hỏi chatbot KHÔNG xử lý

- ❌ Chẩn đoán cá thể hóa (Rule #6 legacy: "Không chẩn đoán cá thể hóa, không kê đơn cụ thể")
- ❌ Kê đơn thuốc cụ thể
- ❌ Câu hỏi phi y khoa (Guardrails integrated nhưng **chưa configure flows**)
- ❌ Câu hỏi về chuyên khoa chưa crawl (da liễu, mắt, tai mũi họng — thiếu dữ liệu chuyên sâu)

### 4. Nguồn dữ liệu giới hạn

Chỉ từ **11 nguồn đã crawl** (liệt kê ở mục 1). Không dùng web search, không dùng external augmentation (gate có implement nhưng chưa connect external source).

### 5. Mục tiêu sử dụng

**Hỗ trợ học tập và tra cứu** (research & study aid), **KHÔNG** hỗ trợ ra quyết định lâm sàng trực tiếp.

---

## 12) Gói thông tin tối thiểu

### Gói A — Mô tả hệ thống ✅

Đã trả lời đầy đủ ở mục 1 và 11.

### Gói B — Dữ liệu kiểm thử ⚠️

| Yêu cầu | Trạng thái |
|---|---|
| 15–20 câu hỏi mẫu | ✅ 100 VN + 68 EN gold queries |
| Đáp án chuẩn | ✅ expected_title cho mỗi query |
| 5 câu follow-up | ✅ 8 multi_turn trong eval_queries.json |
| 5 câu ngoài phạm vi | ❌ **Chưa có** |
| 5 câu thiếu dữ liệu/mâu thuẫn | ❌ **Chưa có** |

### Gói C — Output hệ thống ❌

| Yêu cầu | Trạng thái |
|---|---|
| Rewritten query | ❌ Chưa có sample outputs |
| Retrieved titles/chunks | ✅ Có trong G3 eval report (top_articles per query) |
| Answer cuối | ❌ Chưa có production answers |
| Citation | ❌ Chưa có |
| Coverage level | ❌ Chưa có |

### Gói D — Kết quả hiện có ✅

| Yêu cầu | Trạng thái | Kết quả |
|---|---|---|
| Retrieval metrics | ✅ | **VN**: Hit@3 = 97% (100 queries)<br>**EN Combined**: Src Hit@3 = 91.2%, Title Hit@3 = 89.7% (68 queries)<br>**EN WHO**: 100%, **NCBI**: 100%, **MedlinePlus**: 88.5% |
| Benchmark | ✅ | Gate G3 pass (≥75% threshold), eval_report_staging.md |
| Unit/integration tests | ✅ | test_prompt.py, test_retriever.py, test_schemas.py, test_session.py |
| Lỗi thực tế | ✅ | 3 miss VN (title lỗi ký tự, semantic gap), 5 miss EN (glaucoma not in corpus, sickle cell title mismatch) |

---

## 13) Đánh giá sơ bộ theo format hội đồng

### Tiêu chí ĐẠT

| Tiêu chí | Lý do |
|---|---|
| **Clinically structured** | 6 answer templates chuyên biệt, đúng chuẩn y khoa |
| **Traceable** | Citation [n], retrieved_chunks trả kèm, OTel tracing |
| **Auditable** | Full trace pipeline, Prometheus metrics, session storage |
| **Follow-up handling** | Query rewriter (LLM + rule fallback), 87.5% multi_turn accuracy |
| **Retrieval quality** | Hit@3 = 97% VN, 89.7% EN — vượt gate 75% |

### Tiêu chí ĐẠT MỘT PHẦN

| Tiêu chí | Đạt gì | Thiếu gì |
|---|---|---|
| **Đúng nội dung chuyên môn** | Nguồn uy tín, evidence pack, 10 prompt rules | Chưa có content verification, chưa có expert review |
| **Không bịa khi thiếu dữ liệu** | Coverage scorer, abstain instructions, 4 anti-hallucination rules | Chưa test output thật cho out-of-scope cases |
| **Mức chắc chắn** | Coverage levels, "Giới hạn" section, force_abstain | Chưa phân biệt guideline strength vs single study |
| **Ổn định** | temperature=0.2, deterministic retrieval | Chưa có consistency test |
| **Thời gian phản hồi** | Monitoring sẵn (Prometheus, OTel) | Chưa có production latency data |

### Tiêu chí CHƯA ĐẠT

| Tiêu chí | Lý do |
|---|---|
| **Ưu tiên nguồn mạnh** | trust_tier **không** nằm trong article ranking formula |
| **Scope-limited** | Guardrails integrated nhưng **Colang flows rỗng** — chưa chặn phi y khoa |

### Rủi ro lớn nhất

1. **Không có Claim Verifier** — answer có thể drift khỏi evidence mà không bị detect
2. **trust_tier không dùng trong ranking** — bài lẻ có thể đánh bại guideline nếu cosine similarity cao hơn
3. **Chưa có production output** — mọi đánh giá đều dựa trên thiết kế, chưa dựa trên hành vi thực tế

### Cải tiến ưu tiên cao

| # | Cải tiến | Effort |
|---|---|---|
| 1 | Thêm trust_tier vào article ranking formula | Thấp |
| 2 | Viết Colang guardrail flows (input + output rails) | Thấp |
| 3 | Chạy 20 câu end-to-end, lưu full output → Gói C | Trung bình |
| 4 | Soạn 5 câu out-of-scope + 5 câu mâu thuẫn → Gói B | Trung bình |
| 5 | Thêm Claim Verifier post-generation | Cao |

### Kết luận

Hệ thống hiện ở mức **đồ án tốt nghiệp mạnh** (strong thesis project) — kiến trúc production-grade, retrieval quality cao, nhưng cần bổ sung **output testing thực tế** và **trust-tier ranking** để đạt mức **sản phẩm thử nghiệm** (research prototype).
