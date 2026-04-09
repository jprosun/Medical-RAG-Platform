Dưới đây là **bản kế hoạch cải thiện evidence layer** cho hệ RAG của bạn, theo hướng **không phá kiến trúc đang có**, mà tận dụng đúng những gì đang làm tốt rồi mở rộng dần. Mục tiêu là nâng từ mức:

> **retrieve đúng tài liệu**
> lên
> **chọn đúng bằng chứng, trích đúng claim, phản ánh đúng độ mạnh của bằng chứng**.

Kế hoạch này bám sát pipeline hiện tại của bạn: Query Rewriter → Router → Retriever → Article Aggregator → Evidence Extractor → Coverage Scorer → Prompt Builder → LLM.  

---

# 1) Mục tiêu cải thiện

## Mục tiêu tổng

Biến evidence layer hiện tại từ dạng:

* chọn bài liên quan
* trích findings/numbers/limitations
* chấm coverage

thành dạng:

* chọn **nguồn mạnh và phù hợp loại câu hỏi**
* trích **claim có neo vào span gốc**
* phát hiện **xung đột giữa các nguồn**
* đánh giá **evidence sufficiency** theo từng loại câu hỏi
* chỉ chuyển cho answer layer những evidence đủ tin cậy

## Mục tiêu cụ thể

Sau cải tiến, evidence layer cần trả lời được 5 câu:

1. **Nguồn nào là nguồn nên tin nhất cho câu hỏi này?**
2. **Claim nào trong nguồn đó thực sự được support?**
3. **Claim đó được support bởi câu/đoạn nào?**
4. **Có nguồn nào mâu thuẫn hoặc làm yếu kết luận không?**
5. **Với loại câu hỏi này, bằng chứng hiện có đã đủ để kết luận chưa?**

---

# 2) Nguyên tắc thiết kế

Kế hoạch này giữ lại các phần đang tốt của hệ thống:

* **Query Rewriter**: giữ nguyên, vì đã xử lý follow-up khá tốt. 
* **Query Router**: giữ nguyên vai trò phân loại query, nhưng dùng thêm để điều khiển evidence policy. 
* **Retriever + Qdrant + filters + post-processing**: giữ nguyên làm tầng recall ban đầu. 
* **Article Aggregator**: không bỏ, mà nâng từ semantic ranking thành authority-aware ranking. 
* **Evidence Extractor**: không bỏ, mà đổi từ article summary sang claim-grounded extraction. 
* **Coverage Scorer**: không bỏ, mà mở rộng thành evidence sufficiency scorer. 

Tức là bạn **không xây lại từ đầu**, mà nâng cấp từng lớp.

---

# 3) Kiến trúc evidence layer mới

## Luồng mới đề xuất

```text id="5650m3"
Retrieved Chunks
   ↓
Article Aggregator v2
   ↓
Primary / Secondary Articles
   ↓
Claim-grounded Evidence Extractor
   ↓
Evidence Normalizer
   ↓
Conflict Detector
   ↓
Evidence Sufficiency Scorer
   ↓
Claim Verifier (optional before answer / after draft)
   ↓
Evidence Pack v2 cho Prompt Builder
```

## Khác biệt chính so với hiện tại

### Hiện tại

* chọn bài theo relevance heuristic
* trích summary-level evidence
* coverage scorer hơi tổng quát

### Sau cải tiến

* chọn bài theo **relevance + authority + query-fit**
* trích **claim-level evidence**
* có **conflict detection**
* coverage chấm theo **đúng loại bằng chứng cần cho câu hỏi**
* có thể thêm **claim verifier**

---

# 4) Các module cần cải thiện

## Module 1 — Article Aggregator v2

Đây là chỗ nên sửa đầu tiên.

### Hiện tại đang tốt ở đâu

* đã group chunk theo title
* đã chấm article_score bằng nhiều tín hiệu
* đã chọn primary + secondary hợp lý 

### Vấn đề hiện tại

* chưa dùng `trust_tier`
* chưa phân biệt query hỏi guideline hay study
* chưa ưu tiên nguồn mạnh theo loại câu hỏi 

### Mục tiêu mới

Tính **article_score_v2** gồm 3 nhóm tín hiệu:

#### 1. Relevance signals

Giữ lại:

* max_chunk_score
* avg_chunk_score
* keyword_overlap
* section_diversity

#### 2. Authority signals

Thêm:

* trust_tier boost
* doc_type boost
* source priority boost

Ví dụ:

* guideline / WHO / Bộ Y tế: boost cao
* textbook/reference: boost vừa
* journal đơn lẻ: boost thấp hơn
* patient education: chỉ boost nếu audience/query phù hợp

#### 3. Query-fit signals

Thêm logic theo query type:

* `guideline_comparison` → ưu tiên guideline
* `teaching_explainer` → ưu tiên textbook/reference
* `study_result_extraction` → ưu tiên original study/review có số liệu
* `research_appraisal` → ưu tiên bài có methods + limitations rõ

### Đầu ra mới

Mỗi article không chỉ có:

* article_score

mà còn có:

* relevance_score
* authority_score
* query_fit_score
* evidence_class
* selected_reason

### Lợi ích

Khi bảo vệ đồ án, bạn có thể nói:

> hệ thống không chọn nguồn chính chỉ theo semantic similarity, mà còn theo độ mạnh và độ phù hợp của bằng chứng.

---

## Module 2 — Claim-Grounded Evidence Extractor

Đây là phần quan trọng nhất của evidence correctness.

### Hiện tại đang tốt ở đâu

* đã trích được population, sample size, design, numbers, limitations
* có 2 chế độ LLM/regex fallback 

### Vấn đề hiện tại

* trích ở mức article summary
* key findings chưa gắn chặt với supporting span
* chưa chuẩn hóa loại claim

### Mục tiêu mới

Extractor phải xuất ra `claims[]`, mỗi claim có đủ neo nguồn.

### Schema đề xuất

```json id="niwqv3"
{
  "article_id": "vmj_xxx",
  "title": "...",
  "source_name": "...",
  "trust_tier": 2,
  "doc_type": "review",
  "evidence_class": "original_study",
  "claims": [
    {
      "claim_id": "c1",
      "claim_type": "prognostic_association",
      "claim_text": "CK19 liên quan độc lập với tái phát sớm",
      "supporting_span": "Phân tích đa biến cho thấy...",
      "chunk_id": "chunk_12",
      "section_title": "Kết quả",
      "numbers": [
        {"metric": "HR", "value": "2.1"},
        {"metric": "p-value", "value": "0.03"}
      ],
      "certainty_signal": "moderate",
      "limitations_linked": ["đơn trung tâm", "cỡ mẫu nhỏ"]
    }
  ],
  "study_profile": {
    "design": "...",
    "population": "...",
    "sample_size": "n=146",
    "setting": "...",
    "outcomes": ["..."]
  }
}
```

### Các loại claim nên chuẩn hóa

* definition
* mechanism
* diagnostic_value
* prognostic_association
* treatment_effect
* safety_signal
* guideline_recommendation
* epidemiology_frequency
* comparison

### Lợi ích

* claim nào cũng truy được về chunk và span
* dễ verify
* dễ debug
* dễ benchmark

---

## Module 3 — Evidence Normalizer

Module này nhỏ nhưng rất đáng làm.

### Mục tiêu

Chuẩn hóa các evidence item từ nhiều nguồn khác nhau về cùng format.

Ví dụ:

* “HR 2.1”
* “hazard ratio = 2.1”
* “nguy cơ tăng gấp 2.1 lần”

đều chuẩn hóa thành cùng một object.

### Việc module này làm

* normalize metric names
* normalize effect direction
* normalize study design labels
* normalize limitation vocabulary
* normalize recommendation strength phrases

### Lợi ích

Conflict detection và sufficiency scoring sẽ chính xác hơn.

---

## Module 4 — Conflict Detector

Đây là module bạn hiện gần như chưa có thật sự. 

### Mục tiêu

Phát hiện khi nhiều nguồn không đồng nhất.

### Mức triển khai khuyến nghị

Bắt đầu bằng heuristic trước, chưa cần NLI nặng.

### Quy tắc phát hiện conflict

* cùng claim target nhưng polarity khác
* một nguồn kết luận mạnh, nguồn khác nói insufficient evidence
* guideline không khuyến cáo nhưng original study gợi ý hiệu quả
* nguồn mới hơn phủ định hoặc làm yếu nguồn cũ hơn

### Đầu ra đề xuất

```json id="q0f3mz"
{
  "has_conflict": true,
  "conflict_type": "recommendation_conflict",
  "involved_claims": ["c1", "c7"],
  "preferred_resolution": "guideline_over_single_study",
  "conflict_note": "Nguồn guideline chưa khuyến cáo mạnh, trong khi một nghiên cứu đơn lẻ cho kết quả tích cực"
}
```

### Lợi ích

Answer layer sẽ không tự “hòa giải” sai giữa các nguồn.

---

## Module 5 — Evidence Sufficiency Scorer v2

Đây là bản nâng cấp của coverage scorer hiện tại.

### Hiện tại đang tốt ở đâu

* đã có high / medium / low
* đã có direct_answerability, numeric, methods, limitations 

### Vấn đề hiện tại

* còn quá heuristic
* chưa gắn chặt với query type
* chưa xét source quality
* conflict mới tính rất sơ sài

### Mục tiêu mới

Với từng loại query, scorer phải biết **cần loại bằng chứng nào mới được coi là đủ**.

### Ví dụ policy theo query type

#### Nếu query là `guideline_comparison`

Muốn `high` thì cần:

* ít nhất 1 guideline-quality source
* recommendation rõ
* không thiếu target population quá nhiều

#### Nếu query là `study_result_extraction`

Muốn `high` thì cần:

* có design
* có population
* có sample size
* có key numeric findings
* claim gắn được vào span

#### Nếu query là `research_appraisal`

Muốn `high` thì cần:

* methods đủ
* limitations đủ
* confounders hoặc design weakness được nêu
* không chỉ có background text

#### Nếu query là `teaching_explainer`

Muốn `high` thì cần:

* definition/mechanism từ textbook hoặc reference source
* không cần số liệu mạnh như original study

### Chỉ số mới nên thêm

* source_quality_score
* claim_support_rate
* contradiction_penalty
* evidence_type_fit
* span_grounding_rate

### Đầu ra mới

Không chỉ `high/medium/low`, mà còn có:

* `missing_requirements`
* `confidence_ceiling`
* `abstain_reason`
* `recommended_answer_mode`

Ví dụ:

* `confidence_ceiling = "moderate"`
* `recommended_answer_mode = "guarded_summary"`

---

## Module 6 — Claim Verifier

Nếu đủ thời gian, đây là module giá trị cao nhất sau article ranking.

### Mục tiêu

Kiểm tra từng claim có thật sự được evidence support không.

### Cách triển khai phù hợp đồ án

Không cần mô hình verifier riêng quá lớn. Có thể làm 2 bước:

#### Bước 1

Tách answer draft hoặc claims draft thành atomic claims

#### Bước 2

Với từng claim:

* tìm supporting spans trong evidence pack
* chấm:

  * supported
  * partially_supported
  * unsupported
  * contradicted

### Ứng dụng

* nếu unsupported → bỏ claim
* nếu partially supported → ép giảm certainty language
* nếu contradicted → chuyển sang answer conflict mode

### Lợi ích

Đây là cầu nối trực tiếp từ evidence correctness sang answer correctness.

---

# 5) Kế hoạch triển khai theo pha

## Pha 1 — Nâng chất lượng nhanh, ít phá hệ thống

Thời gian: 1–2 tuần

### Việc làm

1. thêm `trust_tier` và `doc_type` vào `article_score`
2. thêm `query_type_fit` vào ranking
3. sửa extractor để output `claims[]` + `supporting_span`
4. viết unit test cho:

   * article_aggregator
   * evidence_extractor
   * coverage_scorer

### Kết quả mong đợi

* nguồn chính đáng tin hơn
* evidence có neo nguồn rõ hơn
* dễ benchmark hơn

### Vì sao nên làm trước

Đây là phần leverage cao nhất mà effort không quá lớn.

---

## Pha 2 — Làm evidence layer “thực sự thông minh hơn”

Thời gian: 2–3 tuần

### Việc làm

1. thêm Evidence Normalizer
2. thêm Conflict Detector heuristic
3. nâng Coverage Scorer thành Evidence Sufficiency Scorer v2
4. thêm policy theo query type

### Kết quả mong đợi

* hệ thống biết khi nào bằng chứng đủ / chưa đủ theo đúng loại câu hỏi
* biết khi nào có mâu thuẫn

---

## Pha 3 — Siết độ tin cậy ở mức gần sản phẩm thử nghiệm

Thời gian: 2–4 tuần

### Việc làm

1. thêm Claim Verifier
2. thêm citation fidelity check
3. thêm benchmark riêng cho evidence correctness
4. expert review 20–30 case

### Kết quả mong đợi

Bạn có thể nói mạnh hơn về độ tin cậy evidence layer.

---

# 6) Benchmark cần có cho evidence layer

## Bộ benchmark tối thiểu

Nên tạo 30–50 câu, chia 5 nhóm:

* 10 câu guideline/recommendation
* 10 câu study result
* 10 câu appraisal
* 5 câu mechanism/teaching
* 5 câu conflict/insufficient evidence

## Với mỗi câu nên có

* expected primary source type
* expected evidence class
* must-have claims
* must-not-claim
* required supporting span
* expected confidence ceiling

## Metrics nên đo

* primary-source correctness
* claim grounding rate
* supporting-span accuracy
* unsupported claim rate
* conflict detection recall
* sufficiency classification accuracy

---

# 7) Những gì giữ nguyên, không nên bỏ

Bạn lưu ý là hệ thống đang có một số điểm rất tốt, không nên “đập đi làm lại”:

### Giữ nguyên

* Query Rewriter
* Query Router
* Qdrant semantic retrieval
* score threshold
* dedup
* token budget
* primary/secondary article structure
* citation-aware prompt templates
* tracing + metrics  

### Chỉ cần nâng cấp

* article score
* extractor schema
* coverage logic
* post-extraction verification

---

# 8) Ưu tiên triển khai nếu thời gian đồ án có hạn

Nếu bạn chỉ làm được một phần, mình khuyên ưu tiên đúng thứ tự này:

## Ưu tiên 1

**Article Aggregator v2**

* thêm trust_tier
* thêm doc_type fit
* thêm query-type fit

## Ưu tiên 2

**Claim-grounded Evidence Extractor**

* mỗi finding phải có supporting span + chunk_id

## Ưu tiên 3

**Evidence Sufficiency Scorer v2**

* chấm theo loại câu hỏi
* thêm contradiction penalty

## Ưu tiên 4

**Conflict Detector**

* heuristic là đủ cho đồ án

## Ưu tiên 5

**Claim Verifier**

* nếu còn thời gian

---

# 9) Đầu ra mong muốn sau khi hoàn thành

Sau khi triển khai, evidence pack của bạn nên giống thế này:

```text id="ghqb9i"
Primary Source:
- title
- source
- trust_tier
- evidence_class
- reason_selected

Claims:
- claim_text
- claim_type
- supporting_span
- chunk_id
- numbers
- limitations
- support_status

Secondary Sources:
- same structure

Evidence Summary:
- sufficiency_level
- missing_requirements
- conflict_note
- confidence_ceiling
```

Khi đó answer layer sẽ làm việc trên một nền tốt hơn rất nhiều.

---

# 10) Câu chốt để bạn dùng khi triển khai

Bạn có thể xem toàn bộ kế hoạch này theo một nguyên tắc:

> **Từ “article retrieval” chuyển sang “claim-grounded evidence selection”.**

Đó là bản chất của cải thiện evidence correctness.
