# Triển khai Đầu Ingest cho VMJ OJS vào Staging — v1.1

## Mục tiêu

Đưa corpus `vmj_ojs` đã được split thành article files vào một môi trường staging riêng, với đầy đủ gate kiểm soát chất lượng trước khi cân nhắc merge vào core corpus.

Mục tiêu của v1.1 không phải là merge ngay vào hệ thống chung, mà là:

1. Ingest toàn bộ `vmj_ojs` vào collection staging riêng.
2. Xác nhận full-corpus quality sau ingest.
3. Xác nhận retrieval article-level đủ mạnh.
4. Chỉ sau khi pass toàn bộ gate mới cân nhắc A/B test và merge.

---

## Quyết định kiến trúc đã chốt

### 1. Collection riêng

Sử dụng collection riêng cho VMJ:

* `staging_medqa_vi_vmj_v1`

Không đưa vào collection core hiện tại và không dùng payload filtering trong cùng một collection ở giai đoạn này.

### 2. Giữ cùng embedding model với core

VMJ staging phải dùng đúng embedding model, vector dimension, distance metric, và payload schema đang dùng cho core corpus.

Mục tiêu là để các so sánh `core vs vmj vs core+vmj` có ý nghĩa thực tế.

### 3. Split-to-disk là input chuẩn

Đầu vào ingest là thư mục article files đã split:

* `rag-data/data_intermediate/vmj_ojs_split_articles/`

Không ingest trực tiếp từ raw issue files.

---

## Giai đoạn 0 — Freeze & Snapshot

### 0.1. Artifact cần freeze

* Thư mục `rag-data/data_intermediate/vmj_ojs_split_articles/`
* File `vmj_split_manifest.jsonl`
* File `data/data_final/vmj_ojs.jsonl` nếu đã được sinh
* Version của:

  * `vmj_issue_splitter.py`
  * `vn_title_extractor.py`
  * `vn_sectionizer.py`
  * `vn_quality_scorer.py`
  * `vn_txt_to_jsonl.py`

### 0.2. Manifest run config

Tạo file `benchmark/reports/vmj_run_config.json` gồm:

* `run_date`
* `source_id = vmj_ojs`
* `raw_issue_files_count`
* `split_article_files_count`
* `jsonl_records_count`
* `embedding_model`
* `qdrant_url`
* `collection_name`
* `git_commit_or_version_tag`

### Gate G0

* Snapshot và config được lưu đầy đủ
* Có thể tái lập lại pipeline từ splitter đến ingest

---

## Giai đoạn 1 — Pre-Ingest Validation

### Mục tiêu

Xác nhận file `vmj_ojs.jsonl` và article files đủ sạch trước khi push vào Qdrant.

### Script đề xuất

* `services/qdrant-ingestor/vmj_pre_ingest_validate.py`

### Kiểm tra bắt buộc

* `chunk_id` hoặc `point_id` deterministic
* `doc_id` / `article_id` tồn tại và không rỗng
* `title` không rỗng, không phải placeholder
* `source_id = vmj_ojs`
* `source_url` hoặc `file_url` có mặt
* `article_index` có mặt
* `body/text` không rỗng
* không có duplicate nghiêm trọng theo `point_id`
* không có chunk cực ngắn bất thường vượt ngưỡng cảnh báo

### Output

* `benchmark/reports/vmj_pre_ingest_validate.json`

### Gate G1

* Tỷ lệ lỗi validation ≤ 1%
* Không có lỗi hệ thống kiểu duplicate point IDs hàng loạt
* Không có metadata lineage bị mất

---

## Giai đoạn 1.2 — Full Ingest vào Qdrant Staging

### Script đề xuất

* `services/qdrant-ingestor/staging_ingest.py`

### Yêu cầu triển khai

* Đọc config từ env hoặc config file:

  * `QDRANT_URL`
  * `QDRANT_COLLECTION`
  * `EMBED_MODEL`
  * `BATCH_SIZE`
  * `UPSERT_MODE`
* Tạo collection mới:

  * `staging_medqa_vi_vmj_v1`
* Batch upsert toàn bộ records/chunks từ `vmj_ojs.jsonl`
* Sử dụng point IDs deterministic
* Hỗ trợ:

  * `dry-run`
  * `resume`
  * `skip-existing`
  * `overwrite`

### Metadata payload tối thiểu

* `source_id`
* `doc_id`
* `article_id`
* `title`
* `source_url`
* `file_url`
* `article_index`
* `section_title` nếu có
* `doc_type`
* `language`
* `trust_tier`

### Output

* `benchmark/reports/vmj_full_ingest_report.json`

### Nội dung report

* tổng số records ingest
* tổng số vectors
* thời gian chạy
* throughput
* số lượng fail/skip
* embedding model
* vector dimension
* collection name

### Gate G1.2

* Ingest hoàn tất không lỗi hàng loạt
* Tỷ lệ fail/skip ≤ 2%
* Tổng records/vectors không lệch bất thường so với pre-ingest validation

---

## Giai đoạn 2 — Full-Corpus Post-Ingest Quality Report

### Mục tiêu

Xác nhận rằng sau full run, corpus VMJ vẫn giữ được chất lượng như pilot.

### Script đề xuất

* `services/qdrant-ingestor/_sprint2_phaseG2_report.py`

### Metric bắt buộc

* `GO%`
* `REVIEW%`
* `HOLD%`
* `title_semantic_accuracy`
* `reference_leak_rate`
* `section_purity_rate`
* `metadata_completeness`
* `duplicate_suspect_rate`
* `chunk/article distribution`

### Thống kê phân bố

* p10 / p50 / p90 số chunks mỗi article
* p10 / p50 / p90 độ dài chunk
* tỷ lệ article chỉ có 1 chunk
* tỷ lệ article quá ngắn
* tỷ lệ article quá dài

### Output

* `benchmark/reports/vmj_full_quality_report.json`
* `benchmark/reports/vmj_full_quality_summary.md`

### Gate G2

* `GO ≥ 85%`
* `HOLD ≤ 5%`
* `title_semantic_accuracy ≥ 95%`
* `reference_leak_rate ≤ 2%`
* `section_purity ≥ 90%`

---

## Giai đoạn 2.1 — Manual Semantic Audit Sample

### Mục tiêu

Không phụ thuộc hoàn toàn vào heuristic/script report.

### Lấy mẫu

* 50 article random
* 20 article score thấp hoặc cận ngưỡng
* 20 article rất ngắn hoặc rất dài

### Audit fields

* `title_semantic_correct`
* `tail_or_head_contamination`
* `section_usable`
* `metadata_lineage_correct`
* `retrieval_usable`
* `notes`

### Output

* `benchmark/reports/vmj_semantic_audit.csv`

### Gate G2.1

* `title_semantic_accuracy ≥ 90%`
* `cross_article_contamination ≤ 5%`
* `retrieval_usable ≥ 85%`

---

## Giai đoạn 3 — Retrieval Evaluation (Article-Level)

### Mục tiêu

Xác nhận corpus mới cải thiện retrieval thực sự ở mức article/doc, không chỉ đẹp về chunk quality.

### Điều chỉnh evaluator

Phải chấm theo **article-level** hoặc **doc-level**, không chỉ chunk-level.

### Query pack tối thiểu

Tạo `benchmark/datasets/vmj_retrieval_gold_v1.jsonl` gồm 100 query:

* 30 title-based queries
* 30 topic/disease queries
* 20 excerpt-based queries
* 10 bilingual/mixed queries nếu có
* 10 hard/near-duplicate queries

### Metrics

* `Article Hit@1`
* `Article Hit@3`
* `Semantic Support Pass`
* `Noise Rate`

### Output

* `benchmark/reports/vmj_retrieval_eval.jsonl`
* `benchmark/reports/vmj_retrieval_summary.json`

### Gate G3

* `Article Hit@1 ≥ 80%`
* `Article Hit@3 ≥ 90%`
* `Semantic Support Pass ≥ 85%`
* `Noise Rate ≤ 5%`

---

## Giai đoạn 4 — Mini End-to-End RAG Evaluation

### Mục tiêu

Xác nhận dữ liệu VMJ không chỉ retrieve tốt mà còn giúp answer tốt.

### Query set

20–30 câu hỏi:

* factual
* explanation
* title-aware retrieval
* should-abstain
* near-duplicate topic

### Log bắt buộc

* question
* answer
* retrieved titles
* retrieved contexts
* citations
* prompt version

### Metrics

* `accuracy`
* `fidelity`
* `citation_usefulness`
* `unsupported_claim_rate`
* `abstain_success`

### Output

* `benchmark/reports/vmj_rag_eval.jsonl`
* `benchmark/reports/vmj_rag_eval_summary.json`

### Gate G4

* `accuracy ≥ 3.5/4`
* `fidelity ≥ 3.5/4`
* `unsupported_claim_rate ≤ 5%`
* `abstain_success ≥ 90%`

---

## Giai đoạn 5 — A/B Test Trước Khi Merge

### Collection dùng để so sánh

* `staging_medqa_vi_core_v1`
* `staging_medqa_vi_vmj_v1`
* `staging_medqa_vi_core_plus_vmj_v1`

### Mục tiêu

Đo xem thêm VMJ vào có:

* tăng coverage không
* tăng answer depth không
* hay làm tăng noise / lệch retrieval khỏi core sources

### Output

* `benchmark/reports/vmj_ab_test_report.json`
* `benchmark/reports/vmj_rollout_decision.md`

### Gate G5

Chỉ merge nếu:

* `core + vmj` không làm xấu retrieval trên query core
* `core + vmj` cải thiện hoặc giữ nguyên answer quality ở slice journal/research
* không xuất hiện drift/noise rõ rệt

---

## Rollback Conditions

Rollback ngay nếu có một trong các dấu hiệu sau:

* `HOLD > 10%` sau full run
* `cross_article_contamination > 5%`
* `Article Hit@3 < 85%`
* `Noise Rate > 10%`
* `unsupported_claim_rate` tăng rõ khi merge
* query core bị xấu đi khi thêm `vmj_ojs`

---

## Quyết định rollout hiện tại

### Được phép làm ngay

* Full ingest vào `staging_medqa_vi_vmj_v1`
* Chạy G2 quality report
* Chạy G3 retrieval evaluation
* Chạy G4 mini E2E

### Chưa được làm ngay

* Merge trực tiếp vào collection core
* Dùng pilot nhỏ làm bằng chứng cuối cùng để productionize

---

## Deliverables bắt buộc

* `vmj_run_config.json`
* `vmj_pre_ingest_validate.json`
* `vmj_full_ingest_report.json`
* `vmj_full_quality_report.json`
* `vmj_full_quality_summary.md`
* `vmj_semantic_audit.csv`
* `vmj_retrieval_eval.jsonl`
* `vmj_retrieval_summary.json`
* `vmj_rag_eval.jsonl`
* `vmj_rag_eval_summary.json`
* `vmj_ab_test_report.json`
* `vmj_rollout_decision.md`

---

## Kết luận

VMJ OJS được xử lý theo hướng staging-first, evidence-first. Chỉ sau khi pass đủ G1 → G5 mới được cân nhắc merge vào corpus chung.
