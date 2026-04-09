Dưới đây là **checklist rollout** tôi khuyên dùng cho `vmj_ojs`, theo đúng trạng thái hiện tại: pilot Phase D đã rất đẹp trên 54 file split với **GO 98.3%**, **HOLD 0%**, **title accuracy 98.3%**, **ref leak 0%**, **section purity 100%**, và retrieval sanity pilot cho thấy **12/15 query đúng bài ở top-1 khi soi tay**. Vì vậy, bước tiếp theo hợp lý là **full staging ingest riêng**, chưa merge ngay vào corpus chung. 

---

# Checklist Rollout `vmj_ojs`

## Giai đoạn 0 — Freeze trước khi chạy

### 0.1. Đóng băng artifact

* [ ] Freeze thư mục `rag-data/data_intermediate/vmj_ojs_split_articles/`
* [ ] Freeze `vmj_split_manifest.jsonl`
* [ ] Ghi version:

  * [ ] `vmj_issue_splitter.py`
  * [ ] `vn_title_extractor.py`
  * [ ] `vn_sectionizer.py`
  * [ ] `vn_quality_scorer.py`
* [ ] Tạo `ROLLUP.md` hoặc `run_config.json` ghi:

  * [ ] ngày chạy
  * [ ] source = `vmj_ojs`
  * [ ] số file raw
  * [ ] số article split
  * [ ] commit hash / tag

### 0.2. Chốt collection rollout

* [ ] Tạo collection staging riêng:

  * [ ] `staging_medqa_vi_vmj_v1`
* [ ] Không ingest vào collection core hiện tại
* [ ] Không merge với các source khác ở bước này

---

## Giai đoạn 1 — Full ingest vào staging riêng

### 1.1. Chạy full convert

* [ ] Chạy full pipeline từ `vmj_ojs_split_articles` → `vmj_ojs.jsonl`
* [ ] Lưu log đầy đủ:

  * [ ] tổng số article files đầu vào
  * [ ] tổng số records đầu ra
  * [ ] tổng số chunks
  * [ ] số file fail
  * [ ] lý do fail

### 1.2. Chạy ingest vào Qdrant staging

* [ ] Ingest toàn bộ `vmj_ojs.jsonl` vào `staging_medqa_vi_vmj_v1`
* [ ] Lưu ingest report:

  * [ ] số docs
  * [ ] số chunks
  * [ ] embedding model
  * [ ] chunk params
  * [ ] thời gian ingest
  * [ ] lỗi/skip count

### Gate G1

* [ ] Không có lỗi ingest hàng loạt
* [ ] Tỷ lệ file fail trong convert **≤ 2%**
* [ ] Số article ingest được không lệch bất thường so với split output

---

## Giai đoạn 2 — Post-ingest quality report trên full corpus

### 2.1. Xuất quality report full-corpus

* [ ] GO / REVIEW / HOLD
* [ ] title semantic accuracy
* [ ] reference leak rate
* [ ] section purity rate
* [ ] metadata completeness
* [ ] duplicate suspect rate
* [ ] chunk/article distribution

### 2.2. Kiểm tra phân bố chunks

* [ ] p10 chunks/article
* [ ] median chunks/article
* [ ] p90 chunks/article
* [ ] % article chỉ có 1 chunk
* [ ] % article có số chunks quá cao
* [ ] % article quá ngắn / quá dài

### Gate G2

* [ ] `GO ≥ 85%`
* [ ] `HOLD ≤ 5%`
* [ ] `title_semantic_accuracy ≥ 95%`
* [ ] `reference_leak_rate ≤ 2%`
* [ ] `section_purity ≥ 90%`

---

## Giai đoạn 3 — Semantic audit hậu tích hợp

### 3.1. Lấy mẫu audit

* [ ] 30 article ngẫu nhiên
* [ ] 10 article score thấp/cận ngưỡng
* [ ] 10 article rất ngắn
* [ ] 10 article rất dài

### 3.2. Audit tay từng article

Mỗi article chấm:

* [ ] title đúng bài
* [ ] author line đúng bài
* [ ] không dính tail bài trước
* [ ] không dính head bài sau
* [ ] sectionizer tạo section usable
* [ ] article usable cho retrieval

### Gate G3

* [ ] `title_semantic_accuracy ≥ 90%`
* [ ] `cross_article_contamination ≤ 5%`
* [ ] `retrieval_usable ≥ 85%`

---

## Giai đoạn 4 — Retrieval evaluation (article-level)

### 4.1. Sửa evaluator

* [ ] Chấm theo **article/doc-level**, không chỉ chunk-level
* [ ] Mỗi query có:

  * [ ] `gold_article_title`
  * [ ] `acceptable_titles` nếu có
  * [ ] `gold_passage_hint`

### 4.2. Chuẩn bị query pack

* [ ] 30 query theo title
* [ ] 30 query theo disease/topic
* [ ] 20 query theo excerpt / phrase
* [ ] 10 query tiếng Anh hoặc mixed nếu có
* [ ] 10 query khó / gần trùng

Tổng: **100 query** tối thiểu

### 4.3. Chạy retrieval test trên `staging_medqa_vi_vmj_v1`

* [ ] top-1
* [ ] top-3
* [ ] top-5
* [ ] log retrieved titles
* [ ] log retrieved contexts
* [ ] log score / distance

### Gate G4

* [ ] `Article Hit@1 ≥ 80%`
* [ ] `Article Hit@3 ≥ 90%`
* [ ] `Semantic Support Pass ≥ 85%`
* [ ] `Noise Rate ≤ 5%`

---

## Giai đoạn 5 — Mini E2E RAG evaluation

### 5.1. Chuẩn bị mini benchmark

* [ ] 20–30 câu hỏi
* [ ] chia theo:

  * [ ] factual
  * [ ] explanation
  * [ ] title-aware retrieval
  * [ ] should-abstain
  * [ ] near-duplicate topic

### 5.2. Chạy answer generation

* [ ] dùng `staging_medqa_vi_vmj_v1`
* [ ] log:

  * [ ] question
  * [ ] answer
  * [ ] citations
  * [ ] retrieved articles
  * [ ] prompt version

### 5.3. Chấm điểm

* [ ] accuracy
* [ ] fidelity
* [ ] citation usefulness
* [ ] unsupported claim
* [ ] abstain success

### Gate G5

* [ ] `accuracy ≥ 3.5/4`
* [ ] `fidelity ≥ 3.5/4`
* [ ] `unsupported_claim_rate ≤ 5%`
* [ ] `abstain_success ≥ 90%`

---

## Giai đoạn 6 — A/B test trước khi merge

### 6.1. Chuẩn bị 3 collection

* [ ] `staging_medqa_vi_core_v1`
* [ ] `staging_medqa_vi_vmj_v1`
* [ ] `staging_medqa_vi_core_plus_vmj_v1`

### 6.2. Chạy cùng một query pack trên 3 collection

* [ ] core only
* [ ] vmj only
* [ ] core + vmj

### 6.3. So sánh

* [ ] coverage tăng không
* [ ] top-1/title hit tăng hay giảm
* [ ] noise tăng hay không
* [ ] answer depth tăng không
* [ ] citation fidelity có giảm không

### Gate G6

Chỉ merge nếu:

* [ ] `core + vmj` **không làm xấu** retrieval trên các query core
* [ ] `core + vmj` **cải thiện hoặc giữ nguyên** answer quality ở slice journal/research
* [ ] không xuất hiện noise/systematic drift rõ rệt

---

## Giai đoạn 7 — Quyết định rollout

### Nếu PASS tất cả G1 → G6

* [ ] Promote `vmj_ojs` sang collection merged
* [ ] Tạo tag release:

  * [ ] `vmj_ojs_v1_ready`
* [ ] Ghi decision log:

  * [ ] merge date
  * [ ] collection names
  * [ ] metrics final

### Nếu FAIL một trong các gate

* [ ] Không merge
* [ ] Giữ `vmj_ojs` ở collection riêng
* [ ] Mở issue fix theo đúng failure mode:

  * [ ] boundary
  * [ ] title
  * [ ] section
  * [ ] retrieval ranking
  * [ ] evaluator bug

---

# Artifact cần xuất ở mỗi giai đoạn

## Bắt buộc

* [ ] `vmj_split_manifest.jsonl`
* [ ] `vmj_full_ingest_report.json`
* [ ] `vmj_full_quality_report.json`
* [ ] `vmj_semantic_audit.csv`
* [ ] `vmj_retrieval_eval.jsonl`
* [ ] `vmj_rag_eval.jsonl`
* [ ] `vmj_ab_test_report.json`
* [ ] `vmj_rollout_decision.md`

---

# Quy tắc rollback

Rollback ngay nếu có một trong các dấu hiệu sau:

* [ ] `HOLD > 10%` sau full run
* [ ] `cross_article_contamination > 5%`
* [ ] `Article Hit@3 < 85%`
* [ ] `Noise Rate > 10%`
* [ ] `unsupported_claim_rate` tăng rõ khi merge
* [ ] query core bị xấu đi khi thêm `vmj_ojs`

---

# Khuyến nghị chốt

## Bạn nên làm ngay

* [ ] Full ingest `vmj_ojs` vào **staging riêng**
* [ ] Chạy full quality report
* [ ] Chạy retrieval eval 100 query
* [ ] Chạy mini E2E 20–30 câu
* [ ] Chưa merge vào collection chung

## Bạn chưa nên làm ngay

* [ ] Promote thẳng vào merged production corpus
* [ ] Dùng pilot 15 query làm bằng chứng cuối cùng
* [ ] Bỏ qua A/B test

---
