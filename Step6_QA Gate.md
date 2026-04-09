Edited WORKFLOW.md

Let me read the QA check modules and check if there are actual QA reports.

Viewed check_schema.py:1-278
Viewed check_content.py:1-238
Viewed check_chunks.py:1-275

Now let me check if there are actual QA reports and also check the benchmark/reports folder:

Listed directory benchmark
Viewed gate_g3_eval.py:1-420

Now let me check the actual QA reports:

Listed directory reports
Viewed run_all_checks.py:1-178

Đã có đầy đủ. Đây là giải thích chi tiết Step 6:

---

## Step 6: QA Pre-Ingest (Quality Assurance)

### Mục đích

Kiểm tra **chất lượng cuối cùng** trước khi đưa dữ liệu vào Qdrant. Đây là **cổng chặn cuối** — nếu không pass, dữ liệu không được ingest.


  enriched JSONL  →  3 tầng kiểm tra  →  GO / WARN / NO-GO
                     (schema → content → chunks)


### Phương pháp — 3 tầng (layers)

Script: [run_all_checks.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/qa_pre_ingest/run_all_checks.py)

---

#### Tầng 1: Schema Validation — `check_schema.py`

**Kiểm tra cấu trúc dữ liệu có đúng format không:**

| Kiểm tra | Logic | Action |
|---|---|---|
| Required fields | `doc_id`, `title`, `body`, `source_name` phải có | FAIL nếu thiếu |
| doc_id trùng lặp | Không được có 2 records cùng doc_id | FAIL |
| Enum validation | `doc_type` ∈ {guideline, textbook, review...} | FAIL |
| | `audience` ∈ {patient, student, clinician} | FAIL |
| | `trust_tier` ∈ {1, 2, 3} | FAIL |
| URL hợp lệ | `source_url` phải parse được http/https | FAIL |
| Date format | `published_at` phải match YYYY-MM-DD | FAIL |
| Body quality | Body ≥ 50 chars, body ≠ title | FAIL |
| HTML remnants | Không có `<div>`, `class=""` trong body | FAIL |
| Citation check | Title + source_name phải đủ để tạo citation | FAIL |

**Pass/Fail:** `error_records == 0` → PASS

---

#### Tầng 2: Content Quality — `check_content.py`

**Kiểm tra nội dung có "sạch" không:**

| Kiểm tra | Ngưỡng | Phát hiện gì |
|---|---|---|
| **Character noise** | ratio > 0.15 | OCR artifacts, ký tự lạ |
| **Sentence repetition** | ratio > 0.3 | Câu lặp lại trong cùng doc |
| **Cross-doc duplicate** | MD5 hash 500 chars đầu | 2 docs có body giống nhau |
| **Navigation text** | ≥ 2 patterns | Menu, cookie, policy scraped lẫn |
| **Mixed topics** | < 15% word overlap title↔body | Body không liên quan đến title |
| **Too short** | < 100 chars | Body quá ngắn |
| **Too long** | > 10,000 chars | Body quá dài (chưa sectionize) |

**Pass/Fail:** `issue_rate < 35%` → PASS (heuristic warnings, mềm hơn tầng 1)

---

#### Tầng 3: Chunk QA — `check_chunks.py`

**Giả lập chunking rồi kiểm tra chất lượng chunk:**

| Kiểm tra | Logic | Phát hiện gì |
|---|---|---|
| **Missing provenance** | Title/source không xuất hiện trong chunk | Chunk mất nguồn gốc |
| **Missing context header** | Không có `Title:` hoặc `Source:` | Chunk thiếu header metadata |
| **Truncated list** | Chunk kết thúc giữa list (bullet không có `.`) | List bị cắt ngang |
| **Truncated table** | `\|` không đóng | Bảng bị cắt |
| **Extremely short** | < 50 tokens | Chunk quá nhỏ, ít thông tin |
| **Extremely long** | > 500 tokens | Chunk quá lớn, tốn token embedding |
| **Duplicate chunks** | SHA256 hash normalized text | 2 chunk trùng nhau |

**Pass/Fail:** `dup_ratio < 15%` AND `provenance_missing < 10%` → PASS

---

### Composite Score — Điểm tổng hợp GO/WARN/NO-GO

python
Schema score  = 100 - (error_rate × 100)     # weight 40%
Content score = 100 - issue_rate              # weight 35%
Chunk score   = 100 - (dup_ratio + prov_ratio) # weight 25%

TOTAL = Schema×0.40 + Content×0.35 + Chunk×0.25


| Score | Verdict | Ý nghĩa |
|---|---|---|
| ≥ 80 | **GO** ✅ | Sẵn sàng ingest |
| 60-79 | **WARN** ⚠️ | Ingest được nhưng cần review |
| < 60 | **NO-GO** ❌ | Không được ingest, phải fix |

---

### Có chia luồng theo nguồn không?

**Có — nhưng implicit**, không phải explicit pipeline branching:

| Nguồn | Cách QA | Chi tiết |
|---|---|---|
| **vmj_ojs** | VN Quality Scorer (Step 4) + `vmj_pre_ingest_validate.py` riêng | Đã có `quality_score` trong JSONL, validate duplicate IDs riêng |
| **combined.jsonl** (EN) | `run_all_checks.py` — 3 tầng đầy đủ | Chạy trên file combined.jsonl |
| **Các VN khác** | `run_all_checks.py` trên từng `{source}.jsonl` | Chạy riêng cho kcb_moh.jsonl, hue_jmp_ojs.jsonl, v.v. |


               run_all_checks.py
                      │
        Nhận INPUT là bất kỳ .jsonl nào
                      │
    ┌─────────────────┼──────────────────┐
    │                 │                  │
combined.jsonl   vmj_ojs.jsonl    kcb_moh.jsonl  ...
 (EN sources)    (riêng)         (riêng)
    │                 │                  │
 Schema check    Schema check     Schema check
 Content check   Content check    Content check
 Chunk check     Chunk check      Chunk check
    │                 │                  │
 GO/WARN/NO-GO   GO/WARN/NO-GO   GO/WARN/NO-GO


**Không merge tất cả rồi QA 1 lần** — mà QA từng file riêng lẻ. Mỗi source nhận verdict độc lập.

---

### Cách chạy

bash
cd services/qdrant-ingestor

# QA 1 file cụ thể
python -m qa_pre_ingest.run_all_checks ../../data/data_final/vmj_ojs.jsonl

# QA tất cả files
python -m qa_pre_ingest.run_all_checks ../../data/data_final/*.jsonl

# QA chỉ schema
python -m qa_pre_ingest.check_schema ../../data/data_final/combined.jsonl


---

### Ngoài ra: Gate G3 — Retrieval Evaluation

Dự án còn có **gate_g3_eval.py** — kiểm tra **sau khi ingest** (Step 7) bằng cách:
- Chạy 100 gold queries → search Qdrant → kiểm tra Hit@1, Hit@3, Hit@5
- **Ngưỡng: Hit@3 ≥ 75%** → PASS
- Dùng title_norm matching + article collapse + heuristic reranking
- Đây là **end-to-end test** — nếu pass = toàn bộ pipeline đáng tin cậy

| Gate | Kiểm tra gì | Ngưỡng | Thời điểm |
|---|---|---|---|
| **QA Pre-Ingest** (Step 6) | Schema + Content + Chunks | Score ≥ 80 | **Trước** ingest |
| **Gate G3** (Step 7+) | Retrieval accuracy trên queries thật | Hit@3 ≥ 75% | **Sau** ingest |

Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md
Edited WORKFLOW.md

Câu hỏi rất hay — đi thẳng vào điểm yếu của pipeline. Để trả lời trung thực:

---

## Chất lượng nội dung y khoa — đang kiểm soát thế nào?

### Thực tế: Hệ thống QA hiện tại **chủ yếu kiểm tra cấu trúc**, KHÔNG kiểm tra ngữ nghĩa y khoa


  CÁI ĐANG KIỂM TRA                      CÁI CHƯA KIỂM TRA
  ─────────────────                      ────────────────────
  ✅ Schema đúng format?                 ❌ Nội dung y khoa có chính xác?
  ✅ Body có đủ dài?                     ❌ Thông tin có cập nhật?
  ✅ Title có hợp lệ?                    ❌ Thuốc/liều lượng có đúng?
  ✅ Có HTML remnants?                   ❌ Khuyến cáo có mâu thuẫn?
  ✅ Có duplicate?                       ❌ Bài retracted/thu hồi?
  ✅ Chunk có bị cắt ngang?              ❌ Evidence level đáng tin?


### Hệ thống đang dựa vào 3 lớp phòng thủ GIÁN TIẾP cho content quality:

#### Lớp 1: Trust-by-Source (tin tưởng theo nguồn)

| Trust Tier | Nguồn | Logic |
|---|---|---|
| **Tier 1** | WHO, Bộ Y tế (kcb_moh, dav_gov) | Cơ quan có thẩm quyền → nội dung đáng tin |
| **Tier 2** | Tạp chí y khoa (vmj, hue, cantho...) | Peer-reviewed → đã qua phản biện |
| **Tier 3** | MedlinePlus | Patient education → đã qua biên tập |

→ **Giả định:** Nguồn uy tín = nội dung đáng tin. Đây là proxy, không phải verification.

#### Lớp 2: Heuristic Quality Flags (cờ cảnh báo)

`vn_quality_scorer.py` phát hiện **triệu chứng** của content kém:

| Flag | Gián tiếp phát hiện gì |
|---|---|
| `title_looks_like_reference` | Record có thể là danh mục tham khảo, không phải bài viết |
| `title_looks_like_table_header` | Record chỉ là header bảng thuốc, không có nội dung |
| `body_too_short` | Có thể trích xuất lỗi, mất nội dung |
| `body_noisy` | OCR artifacts, text extraction lỗi |
| `mixed_language` | Có thể trích sai vùng (EN lẫn VN) |
| `mixed_topics` | Body không liên quan đến title |

→ Phát hiện nội dung "bất thường" nhưng **không đánh giá đúng sai về y khoa**.

#### Lớp 3: Gate G3 — Retrieval Evaluation (sau ingest)

- Chạy 100 câu hỏi y khoa thực tế → search Qdrant → kiểm tra có tìm đúng bài không
- **Ngưỡng: Hit@3 ≥ 75%**
- Đây là **end-to-end test** — nhưng chỉ kiểm tra **retrieval accuracy** (có tìm đúng bài), không kiểm tra **content accuracy** (bài đó có đúng về y khoa)

### Những gì CHƯA có trong pipeline

| Cơ chế | Làm gì | Trạng thái |
|---|---|---|
| **LLM-based fact checking** | Dùng GPT/Gemini verify nội dung y khoa | ❌ Chưa có |
| **Cross-reference validation** | So sánh claims giữa nhiều nguồn | ❌ Chưa có |
| **Expert review sampling** | Bác sĩ review random sample | ❌ Chưa có |
| **Retraction checking** | Kiểm tra bài bị thu hồi | ❌ Chưa có |
| **Currency checking** | Kiểm tra thông tin đã lỗi thời chưa | ❌ Chưa có |
| **Contraindication conflict detection** | Phát hiện chống chỉ định mâu thuẫn | ❌ Chưa có |

### Tóm lại — Content quality hiện tại phụ thuộc vào:


  Chất lượng nội dung = Trust(nguồn gốc) + Structural(cấu trúc đúng) 
                        + Retrieval(tìm đúng bài)
  
  THIẾU:               Semantic(ngữ nghĩa đúng) + Expert(chuyên gia xác nhận)


> **Đánh giá:** Với một hệ thống RAG y khoa, đây là mức độ chấp nhận được cho MVP/research. Các nguồn đều là **peer-reviewed journals** hoặc **cơ quan nhà nước**, nên nội dung có cơ sở tin cậy ban đầu. Tuy nhiên, để production thực sự thì cần thêm **LLM-based validation** hoặc **expert sampling**.

---

## Sau bước này → Ingest flow

**Đúng.** Sau QA, flow là:


  enriched JSONL  →  ingest_staging.py  →  Qdrant Vector DB  →  RAG queries
       │                    │                     │
       │              3 bước:                     │
       │              1. chunk_by_structure()     │
       │              2. embed (bge-m3/fastembed)  │
       │              3. upsert to Qdrant         │
       │                                          │
       └──────────────────────────────────────────┘
                     Sẵn sàng truy vấn


| Bước | Xảy ra gì |
|---|---|
| **Chunk** | Body được tách theo heading_path, ~900 chars/chunk, overlap 150 |
| **Embed** | Mỗi chunk → vector 1024-dim bằng `BAAI/bge-m3` |
| **Upsert** | Vector + metadata payload (title, source, specialty...) → Qdrant |
| **Query** | User question → embed → cosine similarity search → top-K chunks → LLM generate answer |

Let me check the actual retrieval evaluation results and benchmark setup.

Listed directory vmj
Listed directory datasets
Listed directory core
Viewed G3_retrieval_eval_v2.md:1-193
Ran command: `python -X utf8 -c "import json; f=open(r'benchmark\datasets\vmj_retrieval_gold_g3_v2.jsonl','r',encoding='utf-8'); lines=[json.loads(l) for l in f if l.strip()]; f.close(); print(f'Total: {len(lines)} gold queries'); print(f'\nSample:'); print(json.dumps(lines[0], indent=2, ensure_ascii=False)[:500]); from collections import Counter; diffs=Counter(q['difficulty'] for q in lines); groups=Counter(q['group'] for q in lines); types=Counter(q['type'] for q in lines); print(f'\nDifficulty: {dict(diffs)}'); print(f'Groups: {dict(groups)}'); print(f'Types: {dict(types)}')"`

Đây là câu trả lời chi tiết:

---

## Kiểm soát chất lượng truy hồi (Retrieval Quality) — Dự án đang làm như thế nào?

### Cơ chế chính: Gate G3 — Retrieval Evaluation

Dự án có hệ thống **đánh giá retrieval chuyên biệt** với [gate_g3_eval.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/gate_g3_eval.py):


  100 gold queries (do người viết)  →  embed query  →  search Qdrant (top-20 chunks)
        │                                                       │
        │                                              collapse → top-5 articles
        │                                                       │
        └─── So sánh: expected_title ↔ retrieved_title ─────────┘
                                │
                         Hit@1, Hit@3, Hit@5


### Gold Dataset — 100 câu hỏi y khoa thủ công

| Metric | Giá trị |
|---|---|
| Tổng queries | **100** |
| Phân bố độ khó | Easy: 20, Medium: 50, Hard: 30 |
| Phân bố nhóm | **10 chuyên khoa** × 10 câu mỗi nhóm |
| Loại câu hỏi | 21 types (intervention_outcome, diagnostic_value, risk_factor...) |

**Mỗi gold query gồm:**
json
{
  "query": "CK19 có phải là yếu tố tiên lượng...",      // câu hỏi tự nhiên
  "expected_title": "BIỂU HIỆN CK19 VÀ MỐI LIÊN QUAN...", // bài đúng
  "difficulty": "medium",
  "group": "Ung bướu - huyết học",
  "type": "prognostic_marker",
  "rationale": "Query nhấn vào ý nghĩa tiên lượng..."   // lý giải
}


> **Đặc biệt:** Query được viết **KHÔNG COPY title** — mà paraphrase thành câu hỏi lâm sàng tự nhiên. Đây là cách test sát thực tế nhất.

### Kết quả thực tế — Hit@3 = 97%

| Metric | v1 (exact match) | v2 (norm+collapse) | Cải thiện |
|---|---|---|---|
| Hit@1 | 40% | **93%** | +53 queries |
| **Hit@3** | **42%** | **97%** | **+55 queries** |
| Hit@5 | 42% | **97%** | +55 queries |

**Gate: Hit@3 ≥ 75% → PASS ✅ (đạt 97%)**

### Breakdown theo chuyên khoa

| Chuyên khoa | Hit@3 |
|---|---|
| Nhi khoa - sơ sinh | 10/10 (100%) |
| Nhiễm trùng - hô hấp | 10/10 (100%) |
| Thần kinh - tâm thần | 10/10 (100%) |
| Tim mạch - chuyển hóa | 10/10 (100%) |
| Ngoại khoa - chấn thương | 10/10 (100%) |
| Dược - điều dưỡng | 10/10 (100%) |
| Ung bướu - huyết học | 10/10 (100%) |
| Y tế công cộng | 10/10 (100%) |
| Chẩn đoán - xét nghiệm | 9/10 (90%) |
| **Sản phụ khoa** | **8/10 (80%)** ← Yếu nhất |

### Chỉ 3 / 100 queries MISS hoàn toàn

| # | Query | Nguyên nhân |
|---|---|---|
| 1 | Viêm gan B mạn chưa điều trị... | Title trong DB bị lỗi ký tự `V[` |
| 2 | PPOS ở phụ nữ vô sinh... | Bài retrieval gần nhưng khác topic |
| 3 | Khối phần phụ có cấu trúc nang... | Semantic gap quá lớn giữa query và title |

### Kỹ thuật cải thiện retrieval

Pipeline dùng **3 kỹ thuật** để tăng chất lượng từ 42% → 97%:

| Kỹ thuật | Tác dụng | Chi tiết |
|---|---|---|
| **title_norm matching** | Fuzzy match thay vì exact | Unicode NFC, lowercase, bỏ số, collapse whitespace |
| **Article collapse** | Top-20 chunks → top-5 articles | Group chunks by title_norm, max score per article |
| **Heuristic reranker** | Rerank theo keyword overlap | query↔title keyword +0.06, chunk coverage +0.02 |

---

### 1 query truy hồi nhiều nội dung — Kiểm soát thế nào?

Đúng, 1 query trả về **top-20 chunks**, collapse thành **top-5 articles**. Kiểm soát bằng:

| Cơ chế | Làm gì |
|---|---|
| **Cosine similarity score** | Chỉ giữ chunks có score đủ cao (bge-m3, threshold ~0.6+) |
| **Article collapse** | 20 chunks có thể từ nhiều bài → group lại, mỗi bài lấy max score |
| **Heuristic reranker** | Ưu tiên bài có keyword overlap cao với query |
| **Trust tier trong payload** | Khi RAG generate, LLM biết trust_tier để ưu tiên nguồn tin cậy hơn |
| **Context header** | Mỗi chunk có `Title: ... Source: ...` → LLM biết nguồn gốc |

### Tóm lại — Retrieval quality hiện tại

| Khía cạnh | Trạng thái | Đánh giá |
|---|---|---|
| **Gold benchmark** | 100 queries, 10 chuyên khoa | ✅ Comprehensive |
| **Hit@3** | 97% | ✅ Rất tốt |
| **Đa dạng câu hỏi** | 21 types, 3 mức khó | ✅ |
| **Paraphrased queries** | Không copy title | ✅ Sát thực tế |
| **Cross-specialty coverage** | 10/10 nhóm | ✅ |
| **Chỉ test trên VMJ** | Chưa test EN sources, chưa test cross-source | ⚠️ |
| **Chỉ test retrieval, không test generation** | Chưa đánh giá câu trả lời LLM sinh ra | ⚠️ |

> **Kết luận:** Retrieval quality được kiểm soát tốt bằng gold benchmark 100 queries, đạt 97% Hit@3. Tuy nhiên, hiện chỉ đo **tìm đúng bài** (retrieval accuracy), chưa đo **trả lời đúng** (generation accuracy/faithfulness).