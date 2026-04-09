# 1) Bộ test này nhằm kiểm tra cái gì

Bộ test phải trả lời được 5 câu hỏi theo đúng 5 tầng:

1. **Module có chạy đúng logic không?**
2. **Record sau chuẩn hóa có đúng nghĩa không?**
3. **Cả source có đủ sạch để đi tiếp không?**
4. **Dữ liệu đã đủ tốt cho retrieval chưa?**
5. **Khi đi qua RAG, câu trả lời có đúng, bám nguồn, và biết abstain không?**

Đây cũng khớp với pipeline benchmark hiện có của bạn: normalize → quality gate → ingest staging → retrieval compare → promote/rollback. 

---

# 2) Bộ test chuẩn đề xuất: cấu trúc file

Tôi khuyên bạn tạo hẳn một gói test với cấu trúc như sau:

```text
tests/
  fixtures/
    vn/
      raw/
        cleaner/
        title/
        metadata/
        sectionizer/
        scorer/
      expected/
        cleaner_expected.json
        title_expected.json
        metadata_expected.json
        section_expected.json
        scorer_expected.json

  unit/
    test_vn_cleaner.py
    test_vn_title.py
    test_vn_metadata.py
    test_vn_sectionizer.py
    test_vn_quality_scorer.py

benchmark/
  datasets/
    vn_tier2_document_audit_v1.csv
    vn_tier4_retrieval_gold_v1.jsonl
    vn_tier5_rag_gold_v1.jsonl
  queries/
    vn_retrieval_queries_v1.jsonl
  reports/
    tier1_unit_report.json
    tier2_document_audit_report.csv
    tier3_source_gate_report.json
    tier4_retrieval_gate_report.json
    tier5_rag_eval_report.json
```

Tách như vậy để bạn không lẫn giữa:

* fixture kỹ thuật,
* audit dữ liệu,
* gold queries cho retrieval,
* gold QA cho E2E.

---

# 3) Tầng 1 — Unit Tests chuẩn

## Mục tiêu

Khóa chặt logic của 5 module chính:

* `vn_text_cleaner.py`
* `vn_title_extractor.py`
* `vn_metadata_enricher.py`
* `vn_sectionizer.py`
* `vn_quality_scorer.py` 

## Quy mô chuẩn

Tôi khuyên tối thiểu:

* `cleaner`: **10 test cases**
* `title_extractor`: **16 test cases**
* `metadata_enricher`: **12 test cases**
* `sectionizer`: **12 test cases**
* `quality_scorer`: **10 test cases**

Tổng: **60 unit fixtures**

## 3.1. `vn_text_cleaner.py` — 10 case

### Các case bắt buộc

1. Unicode tổ hợp → NFC
2. merge line giữa câu
3. không merge heading
4. bỏ chữ ký số
5. bỏ số trang đơn
6. bỏ header journal lặp
7. giữ `TÓM TẮT` / `ABSTRACT`
8. giữ bullet list thuốc / chỉ định
9. giữ dòng có `1. ĐẶT VẤN ĐỀ`
10. không làm mất dấu tiếng Việt

### Pass criteria

* 10/10 pass
* không được fail ở 3 nhóm:

  * Unicode
  * heading preservation
  * repeated header removal

## 3.2. `vn_title_extractor.py` — 16 case

Đây là module cần test nặng nhất, vì hiện title là lỗ hổng lớn nhất của dữ liệu VN. 

### Phân bổ theo source

* `hue_jmp_ojs`: 3 case
* `mil_med_pharm_journal`: 3 case
* `trad_med_pharm_journal`: 2 case
* `vmj_ojs`: 3 case
* `kcb_moh`: 2 case
* `dav_gov`: 2 case
* `who_vietnam`: 1 case

### Case vàng bắt buộc

Từ source fixtures hiện có, tôi sẽ khóa ít nhất các case sau:

**Case T1 — Huế**

* input: `hue_jmp_ojs/140_135.txt`
* expected title:
  `Nghiên cứu giá trị chẩn đoán và mối liên quan với các yếu tố tiên lượng của nồng độ EBV-DNA huyết thanh trong ung thư vòm`
* must fail if extractor trả title dạng citation tiếng Anh Pan Afr Med J.

**Case T2 — Quân y**

* input: `mil_med_pharm_journal/1000_558.txt`
* expected title:
  `NGHIÊN CỨU MỨC ĐỘ KHÁNG KHÁNG SINH CỦA KLEBSIELLA PNEUMONIAE PHÂN LẬP ĐƯỢC TỪ BỆNH NHÂN ĐIỀU TRỊ TẠI BỆNH VIỆN QUÂN Y 103 (2020 - 2022)`
* must fail if extractor trả title dạng:
  `Eugenol eliminates carbapenem-resistant...`

**Case T3 — YHCT**

* input: `trad_med_pharm_journal/10_8.txt`
* expected title:
  `Đặc điểm nguồn nhân lực y học cổ truyền tại các cơ sở y tế xã huyện trà cú, tỉnh Trà Vinh`
* must fail if extractor trả title dạng references bắt đầu bằng `2. WHO, ...`.

**Case T4 — KCB**

* input: guideline hô hấp
* expected title:
  `Hướng dẫn quy trình kỹ thuật Hô hấp - Tập 1.1`
* accepted fallback:
  `Về việc ban hành tài liệu chuyên môn “Hướng dẫn quy trình kỹ thuật Hô hấp - Tập 1.1”`
* must flag `title_contains_admin_wrapper=true` nếu lấy cả phần “Về việc ban hành…” thay vì title lõi.

**Case T5 — DAV**

* input: `2025_1-dm-thuoc-dieu-tri-benh-hiem...`
* expected title không được là:
  `Đường dùng, dạng bào chế,`
* test này phải xác nhận extractor nhận diện được đây là header bảng chứ không phải title.

### Gate

* `EXACT + CLOSE ≥ 90%`
* `reference_leak = 0`
* `table_header_leak = 0`
* `blank/PDF = 0`

## 3.3. `vn_metadata_enricher.py` — 12 case

### Các case bắt buộc

* guideline → `doc_type=guideline`, `audience=clinician`, `trust_tier=1`
* DAV → `doc_type=reference`, `specialty=pharmacology`
* WHO Việt Nam publication page / health topic → `trust_tier=1`, `source_name=WHO`
* journal → `doc_type=review`, `trust_tier=2`
* article về kháng kháng sinh → `specialty=infectious_disease`
* article về EBV-DNA/ung thư vòm → `specialty=oncology`
* article YHCT → `specialty=traditional_medicine` hoặc `general` nhưng không được lệch sang `pharmacology` trừ khi có bằng chứng mạnh

### Gate

* `doc_type accuracy ≥ 95%`
* `trust_tier accuracy = 100%`
* `specialty accuracy ≥ 85%`
* `language accuracy ≥ 95%`

## 3.4. `vn_sectionizer.py` — 12 case

### Các case bắt buộc

* bài báo chuẩn có `TÓM TẮT`, `ABSTRACT`, `ĐẶT VẤN ĐỀ`, `KẾT QUẢ`
* guideline dài có mục lục
* DAV bảng/list
* WHO VN publication page
* mixed EN/VI
* section `TÀI LIỆU THAM KHẢO` phải bị bỏ

### Case vàng

**S1 — KCB TOC leak**
Input KCB dài; expected:

* các dòng mục lục kiểu `10. CHỌC HÚT... .........`
  không được thành section nội dung hợp lệ. Pilot hiện đang để lọt các section kiểu này dù score 95 và GO, nên test này là bắt buộc.

### Gate

* `reference_drop success = 100%`
* `toc_leak = 0` trên fixture chuẩn
* `short_section_ratio ≤ 10%`

## 3.5. `vn_quality_scorer.py` — 10 case

### Case bắt buộc

Scorer phải bịt đúng các lỗi mà pilot đã bỏ lọt:

* title từ references
* title từ table header
* too_many_sections
* title “Về việc ban hành…” (cho điểm vừa, không max)
* body nhiễu
* metadata thiếu
* mixed language

### Gate

* mọi case “semantic bad title” phải ra `REVIEW` hoặc `HOLD`
* không có case title từ table/reference được `GO`

---

# 4) Tầng 2 — Document Audit Gold Set

## Mục tiêu

Đánh giá semantic correctness ở cấp record sau khi convert.

## Quy mô chuẩn

Tôi khuyên:

* **8 record/source × 8 sources = 64 record**
* riêng `vmj_ojs`: tăng lên **16 record**
  Tổng nên là **72 record audit**

## Cách chọn mẫu

Mỗi source lấy:

* 4 record ngẫu nhiên
* 2 record có `title_extracted`
* 1 record có `too_many_sections`
* 1 record có `body_too_short`
  Riêng `vmj_ojs` thêm 8 record multi-article nghi ngờ.

## Audit sheet bắt buộc

Mỗi record chấm 6 trường:

* `title_semantic_correct` : yes/no
* `title_from_wrong_region` : none/reference/table/footer/prev_article
* `metadata_correct` : pass/minor/major
* `section_purity` : pass/minor/major
* `retrieval_usable` : yes/review/no
* `notes`

## Gate

Một source chỉ qua Tầng 2 nếu:

* `title_semantic_accuracy ≥ 90%`
* `reference_or_table_leak ≤ 5%`
* `section_purity ≥ 85%`
* `retrieval_usable ≥ 85%`

---

# 5) Tầng 3 — Source Batch Gate

## Mục tiêu

Đánh giá cả source sau full-batch, không chỉ từng document.

## Input

* file JSONL theo source
* report từ `run_all_checks.py`
* report semantic audit ở Tầng 2

## Metrics chuẩn

### Từ pipeline

* total records
* avg score
* GO/REVIEW/HOLD rate
* title valid rate
* metadata completeness

### Từ semantic audit

* title semantic accuracy
* reference leak rate
* table-header leak rate
* section purity rate
* retrieval usability rate
* duplicate suspect rate

## Gate

### GO

* avg score ≥ 85
* GO rate ≥ 80%
* title semantic accuracy ≥ 90%
* section purity ≥ 85%
* leak rate ≤ 5%

### REVIEW

* avg score 75–84
* hoặc semantic accuracy 80–89
* hoặc section purity 70–84

### HOLD

* semantic accuracy < 80
* hoặc leak > 10
* hoặc retrieval usability < 70

### Với kết quả hiện tại

Tôi sẽ dùng chính pilot để khởi tạo “expected provisional verdict”:

* `kcb_moh` → REVIEW cho đến khi fix TOC leak
* `dav_gov` → REVIEW cho đến khi parser title bảng ổn
* `hue_jmp_ojs` → REVIEW
* `mil_med_pharm_journal` → REVIEW
* `trad_med_pharm_journal` → REVIEW
* `vmj_ojs` → HOLD cho đến khi có article boundary split

---

# 6) Tầng 4 — Retrieval Gold Set

Đây là phần bạn muốn “thật chuẩn”. Tôi khuyên retrieval gold set **không cố ôm tất cả 8 source ngay**. V1 nên ưu tiên nguồn đáng tin hơn:

* `kcb_moh`
* `dav_gov`
* `who_vietnam`
* 1–2 journal source đại diện
  vì corpus hiện tại cũng đang coi title là lỗ hổng lớn nhất và `vmj_ojs` còn đa bài trong một file.

## Quy mô chuẩn

**72 query retrieval**

* `kcb_moh`: 20
* `dav_gov`: 16
* `who_vietnam`: 16
* `hue_jmp_ojs`: 8
* `mil_med_pharm_journal`: 8
* `trad_med_pharm_journal`: 4

## Schema chuẩn

```json
{
  "query_id": "ret_kcb_001",
  "query": "quy trình kỹ thuật hô hấp tập 1.1",
  "language": "vi",
  "source_focus": "kcb_moh",
  "specialty": "respiratory",
  "gold_titles": ["Hướng dẫn quy trình kỹ thuật Hô hấp - Tập 1.1"],
  "gold_section_hints": ["Nguyên tắc áp dụng Hướng dẫn quy trình kỹ thuật"],
  "must_retrieve_semantic": [
    "tài liệu chuyên môn về hô hấp",
    "hướng dẫn quy trình kỹ thuật"
  ],
  "should_abstain": false
}
```

## 6.1. Query templates theo source

### KCB/Bộ Y tế

Dựa trên pilot KCB hô hấp và bản chất guideline/quy trình, dùng 4 loại query:

* theo title: `quy trình kỹ thuật hô hấp tập 1.1`
* theo procedure: `chọc dò dịch màng phổi`
* theo section logic: `nguyên tắc áp dụng hướng dẫn quy trình kỹ thuật`
* theo specialty paraphrase: `quy trình hô hấp nội khoa`

### DAV

Dựa trên tài liệu orphan drugs/list:

* `thuốc điều trị bệnh hiếm`
* `glucagon-like peptide thuốc điều trị bệnh hiếm`
* `đường dùng dạng bào chế thuốc bệnh hiếm`
* `danh mục thuốc điều trị bệnh hiếm`

### WHO Việt Nam

WHO Viet Nam có health topics và publications pages tiếng Việt/Anh, gồm các chủ đề y tế và ấn phẩm công khai. Tôi sẽ dùng WHO cho retrieval test ở các mảng:

* `tiêm chủng tại Việt Nam`
* `an toàn người bệnh`
* `nhân lực y tế Việt Nam`
* `sức khỏe người cao tuổi`
  vì WHO Vietnam có health topics page và publications page cho các chủ đề này. ([World Health Organization][1])

### Journal articles

Dùng query theo bài báo đã thấy trong pilot:

* `EBV-DNA huyết thanh trong ung thư vòm`
* `Klebsiella pneumoniae kháng kháng sinh bệnh viện 103`
* `nguồn nhân lực y học cổ truyền huyện Trà Cú`

## Metrics

* `Title Hit@3`
* `Section Hint Hit@3`
* `Semantic Support Pass`
* `Noise Rate`

## Gate

* `Title Hit@3 ≥ 85%`
* `Section Hint Hit@3 ≥ 75%`
* `Semantic Support Pass ≥ 85%`
* `Noise Rate ≤ 10%`

---

# 7) Tầng 5 — Mini End-to-End RAG Gold Set

Để “chuẩn” ở V1, tôi không khuyên dùng journal nhiều ở tầng này. Tầng E2E nên dùng chủ yếu nguồn:

* `kcb_moh`
* `who_vietnam`
* `dav_gov`
  vì chúng là nguồn trust cao hơn, dễ kiểm chứng hơn, và phù hợp với tiêu chí “đúng, sâu, đúng nguồn”. WHO Viet Nam công bố health topics và publications pages công khai; KCB/BYT là guideline/procedure; DAV là tài liệu thuốc/dược chính thức. ([World Health Organization][2])

## Quy mô chuẩn

**30 câu**

* KCB: 12
* WHO VN: 8
* DAV: 6
* Journal: 4

## Schema chuẩn

```json
{
  "query_id": "rag_kcb_001",
  "question": "Tài liệu này nói gì về nguyên tắc áp dụng Hướng dẫn quy trình kỹ thuật hô hấp?",
  "language": "vi",
  "specialty": "respiratory",
  "question_type": "open-ended",
  "difficulty_level": "medium",
  "gold_sources": ["Cục Quản lý Khám, chữa bệnh - Bộ Y tế"],
  "gold_titles": ["Hướng dẫn quy trình kỹ thuật Hô hấp - Tập 1.1"],
  "gold_passages": [
    "Cơ sở khám bệnh, chữa bệnh được phép áp dụng toàn bộ Hướng dẫn quy trình kỹ thuật..."
  ],
  "must_mention_points": [
    "cơ sở khám bệnh chữa bệnh được phép áp dụng",
    "đây là hướng dẫn quy trình kỹ thuật hô hấp",
    "cần nêu đây là tài liệu chuyên môn/bộ quy trình"
  ],
  "must_not_claim": [
    "đây là phác đồ điều trị bệnh cụ thể",
    "đây là danh mục thuốc"
  ],
  "should_abstain": false,
  "abstain_reason": null
}
```

## 7.1. Các câu gold E2E mẫu đầu tiên

### KCB/BYT

1. Tài liệu hô hấp tập 1.1 là loại tài liệu gì?
2. Tài liệu nói gì về nguyên tắc áp dụng hướng dẫn quy trình kỹ thuật?
3. Vì sao các mục lục procedure không nên được xem là section nội dung?
4. `should_abstain=true`: hỏi về một quy trình không có trong tài liệu đang index.

### DAV

5. Danh mục thuốc điều trị bệnh hiếm đang mô tả nhóm thông tin nào?
6. Tài liệu này có phải guideline điều trị không?
7. `should_abstain=true`: hỏi liều điều trị cụ thể của một thuốc khi file chỉ là danh mục/list.
8. Cần phân biệt title tài liệu với header bảng như thế nào?

### WHO VN

9. WHO Việt Nam hiện công khai loại tài liệu nào trên mục Publications?
10. Trang health topics Việt ngữ của WHO bao phủ các chủ đề nào liên quan hệ thống y tế và an toàn người bệnh?
11. WHO nói gì về patient safety/quality care?
12. `should_abstain=true`: hỏi chi tiết lâm sàng mà trang chủ đề chỉ nêu tổng quan.

### Journal

13. Bài EBV-DNA tập trung vào giá trị chẩn đoán và tiên lượng trong bệnh gì?
14. Bài Quân y về K. pneumoniae nghiên cứu điều gì và trong bối cảnh nào?
15. Bài YHCT về Trà Cú nghiên cứu đối tượng nào?
16. `should_abstain=true`: hỏi khuyến cáo điều trị cụ thể khi bài chỉ là nghiên cứu quan sát.

## Scoring

Dùng bộ judge hiện có của bạn:

* `accuracy`
* `depth`
* `fidelity`
* `citation`
* `has_severe_factual_error`
* `has_unsupported_claim`
* `abstain_success`

## Gate

* mean `accuracy ≥ 3.5/4`
* mean `fidelity ≥ 3.5/4`
* `unsupported_claim_rate ≤ 5%`
* `abstain_success ≥ 90%`

---

# 8) Cách sinh gold “chuẩn” từ web và từ source hiện có

Tôi khuyên nguyên tắc như sau:

## Với Tier 1–3

Nguồn chân lý chính là **chính raw files hiện có** của bạn, vì mục tiêu là test pipeline transform từ raw → JSONL.

## Với Tier 4–5

Nguồn chân lý nên là:

* **official pages / publications** nếu có web công khai
* hoặc **chính raw file gốc + expected passage đã audit tay**

Ví dụ:

* WHO VN: dùng official pages/publications từ WHO website để viết query/gold points. WHO Viet Nam có cả health topics tiếng Việt và trang publications. ([World Health Organization][1])
* Quân y: dùng official article/download page trên `jmpm.vn` để xác nhận title thật và abstract. ([Journal of Military Pharmaco-medicine][3])
* KCB/DAV/journal VN còn lại: dùng **manual gold từ raw file** đã audit, vì đó là thứ bạn thực sự đang ingest.

Đây là điểm rất quan trọng:
**đừng cố dùng web làm chân lý cho mọi source nếu source thực tế bạn ingest là file raw nội bộ của bạn.** Web chỉ dùng để tăng độ chắc cho những nguồn có trang chính thức truy được.

---

# 9) Bộ test chuẩn cuối cùng: quy mô và thứ tự chạy

## Quy mô tôi khuyên chốt cho V1

* **Tier 1 Unit fixtures:** 60
* **Tier 2 Document audit:** 72 records
* **Tier 3 Source batch gates:** 8 nguồn
* **Tier 4 Retrieval gold queries:** 72
* **Tier 5 Mini end-to-end gold QA:** 30

Tổng thể đây là một bộ đủ mạnh để dùng như:

* kiểm thử kỹ thuật
* gate dữ liệu
* gate retrieval
* gate answer

mà vẫn không quá nặng để vận hành.

## Thứ tự chạy

1. chạy Tier 1 trên mỗi lần sửa module
2. pilot 40–60 file
3. chạy Tier 2 audit
4. nếu pass → full batch source đó
5. chạy Tier 3 source gate
6. index staging
7. chạy Tier 4 retrieval
8. nếu pass → chạy Tier 5 E2E
9. chỉ source pass cả 5 tầng mới được vào production collection

---

# 10) Kết luận

Đây là **bộ test chuẩn đủ mạnh để bắt đầu dùng ngay** cho pipeline hiện tại, vì nó đã được thiết kế bám sát đúng:

* 8 nguồn tiếng Việt đang có,
* lỗi pilot thực tế ở title extraction và sectionization,
* kiến trúc benchmark pipeline hiện có của bạn,
* và mức độ trust khác nhau giữa KCB/WHO/DAV/journal.   

Nếu bạn triển khai đúng bộ này, bạn sẽ có một chuỗi kiểm soát chất lượng rõ ràng:

**module đúng → record đúng → source đúng → retrieval đúng → answer đúng**

và đó là mức “chuẩn” đủ tốt để biến pipeline Việt hiện tại từ pilot thành hệ thống có thể kiểm soát chất lượng thật.

