Tôi review và **đồng ý với hướng chỉnh của bạn**. Nếu chốt để triển khai ngay, tôi sẽ xác nhận như sau:

## Kết luận ngắn

* **Đúng, plan cũ cần sửa**: không thể giả định chỉ `large issue files` mới cần split. Nếu 71.3% file nhỏ cũng chứa 2 bài, thì `vmj_ojs` phải được xem là **nguồn “issue-or-compound-file by default”**, và splitter phải chạy trên **toàn bộ 1,336 file**, không chỉ subset lớn. Kế hoạch cũ của chúng ta thực sự đang ngầm giả định file issue lớn nhiều bài là ca chính, nên phần này bạn sửa là đúng. 
* **Đúng, nên chuyển sang `TÓM TẮT-first, look-back for title` làm chiến lược chính**. Tôi xem đây là cải tiến quan trọng nhất của Sprint 2.
* **Đúng, vẫn giữ kiến trúc split-to-disk**. Điều này tôi giữ nguyên, vì nó vẫn là lựa chọn tốt nhất cho debug, audit và hồi quy. 

Nói cách khác:
**Tôi confirm triển khai theo hướng mới của bạn.**

---

# Đánh giá chi tiết nhận xét của bạn

## 1) “Plan gốc sai giả định lớn” — tôi đồng ý

Đây là một correction rất quan trọng.

Plan cũ mô tả `vmj_ojs` như nguồn mà “mỗi file = 1 số tạp chí chứa nhiều bài”, và ví dụ trọng tâm là các file lớn 965K–1.35M chars, 10–40 bài/file. Điều đó không sai, nhưng nó tạo ra một thiên kiến: tưởng như chỉ cần giải quyết lớp file issue lớn là đủ. 

Nếu dữ liệu thực tế cho thấy:

* **71.3% small files (27KB) cũng chứa 2 bài**
* tổng 1,336 file có thể ra khoảng **4,958 bài**

thì bài toán không còn là “xử lý ngoại lệ file lớn”, mà là:
**thiết kế một splitter tổng quát cho mọi file `vmj_ojs`**.

Tôi đồng ý với kết luận của bạn:
splitter phải được xem là **preprocessing mặc định của cả source**, không phải chỉ là patch cho một số file lớn.

---

## 2) `TÓM TẮT-first, look-back for title` — tôi đồng ý và xem đây là hướng đúng nhất

Tôi đánh giá đây là một thay đổi **rất đúng về mặt signal engineering**.

Trong plan cũ, boundary pattern đi theo hướng:

* scan top-down
* tìm title ALL CAPS
* rồi xác minh bằng author + affiliation + `TÓM TẮT/ĐẶT VẤN ĐỀ` sau đó. 

Cách đó dùng được khi:

* title đứng khá sạch ở đầu block
* boundary ít bị nhiễu
* file không có nhiều đoạn reference / tail text chen trước

Nhưng với `vmj_ojs`, nếu cả file nhỏ cũng có thể chứa 2 bài, thì top-down title scan sẽ rất dễ:

* bắt nhầm title của bài trước còn sót,
* bắt nhầm dòng in hoa không phải title,
* hoặc bị “trôi” do article boundary không sạch.

### Vì sao `TÓM TẮT-first` tốt hơn

Tôi đồng ý với lý do bạn nêu:

* `TÓM TẮT` là tín hiệu mạnh
* xuất hiện đều
* ít false positive hơn title ALL CAPS
* từ `TÓM TẮT` look-back lên để tìm title + author hợp lý là logic ổn hơn nhiều

Nếu triển khai chuẩn, quy trình nên là:

1. scan toàn file tìm anchor:

   * `TÓM TẮT`
   * fallback: `ABSTRACT`
   * fallback tiếp: `ĐẶT VẤN ĐỀ`
2. với mỗi anchor, look-back khoảng **3–15 dòng**
3. trong vùng look-back đó, tìm:

   * title block
   * author block
   * affiliation nếu có
4. boundary được chốt theo score

Tôi đánh giá cách này:

* **ít false positive hơn**
* **scale tốt hơn**
* và phù hợp với cả file nhỏ lẫn file issue lớn.

### Một chỉnh nhỏ tôi khuyên thêm

Đừng dùng `TÓM TẮT-first` như một **luật duy nhất**.
Hãy dùng nó như **primary anchor**, nhưng vẫn có fallback:

* `ABSTRACT-first`
* `ĐẶT VẤN ĐỀ-first`
* hoặc trường hợp hiếm không có abstract thì dùng:

  * title block + author line + issue separator

Tức là:
**primary = TÓM TẮT**
**fallback = ABSTRACT / ĐẶT VẤN ĐỀ / article heading block**

Nếu làm vậy, splitter sẽ bền hơn.

---

## 3) Kiến trúc split-to-disk — tôi giữ nguyên

Điểm này tôi không đổi.

Ngay cả khi bạn chuyển sang `TÓM TẮT-first`, tôi vẫn khuyên:

* split thành file article riêng trên disk
* không split in-memory ở giai đoạn này

Lý do vẫn như cũ:

* dễ debug boundary
* dễ audit tay
* dễ so raw issue ↔ article output
* dễ hồi quy khi rule thay đổi
* dễ dùng lại article files cho toàn bộ pipeline journal

Nên quyết định ở đây là:

**confirm: split-to-disk**

---

# 3 open questions — tôi confirm thế nào?

Bạn nói implementation plan mới đã có **3 open questions**. Tôi chưa thấy nguyên văn 3 câu đó trong phần bạn paste, nhưng dựa trên hướng bạn vừa chốt, đây là **3 quyết định mà tôi xác nhận** để bạn triển khai:

## Open Question 1 — Splitter có chạy trên toàn bộ 1,336 file không?

**Confirm: Có.**

Không chia “small files bỏ qua, large files mới split”.
Toàn bộ `vmj_ojs` nên đi qua cùng một preprocessor:

* detect number of article anchors
* nếu chỉ có 1 anchor → xuất 1 article file
* nếu >1 anchor → split nhiều article

Tức là cùng một engine, không phải hai logic hoàn toàn khác nhau.

## Open Question 2 — Dùng `TÓM TẮT-first` làm primary strategy hay không?

**Confirm: Có.**

Primary boundary strategy của Sprint 2 nên là:

* `TÓM TẮT-first, look-back for title`

Nhưng phải kèm fallback:

* `ABSTRACT`
* `ĐẶT VẤN ĐỀ`
* title block + author line

## Open Question 3 — Split ra file riêng hay giữ in-memory?

**Confirm: Split ra file riêng trên disk.**

Đây vẫn là quyết định tốt nhất cho V1/V2.

---

# Kế hoạch Sprint 2 tôi chỉnh lại theo nhận xét của bạn

## Pha A — Re-scope source model

Cập nhật giả định nguồn:

* **mọi file `vmj_ojs` đều có khả năng multi-article**
* preprocessor bắt buộc chạy cho toàn bộ source

Deliverable:

* `vmj_source_model.md` hoặc note ngắn mô tả:

  * % file 1 bài
  * % file 2 bài
  * % file >2 bài
  * ước tính tổng bài

## Pha B — Xây anchor-driven splitter

Thay boundary logic cũ bằng:

* `TÓM TẮT-first`
* look-back for title
* score boundary
* split to disk

Deliverable:

* `vmj_issue_splitter.py`
* `vmj_split_manifest.jsonl`

## Pha C — Boundary audit

Lấy mẫu:

* 50–80 article boundaries
* có cả file nhỏ 2 bài
* có cả file issue lớn

KPI:

* `boundary_precision ≥ 90%`
* `boundary_recall ≥ 85%`
* `cross_article_contamination ≤ 5%`

## Pha D — Re-run journal pipeline

Article files sau split mới đi vào:

* cleaner
* title extractor
* metadata enricher
* sectionizer article_mode
* scorer

KPI:

* `title_semantic_accuracy ≥ 90%`
* `GO ≥ 50–60%`
* `HOLD ≤ 10%`

---

# Một lưu ý rất quan trọng

Vì bạn đã phát hiện “small files cũng có 2 bài”, nên **đừng dùng `file size` làm proxy cho article count nữa**.

File size chỉ nên dùng cho:

* ưu tiên chọn sample audit
* estimate compute cost

Không nên dùng cho:

* quyết định split hay không
* quyết định boundary logic

---

# Verdict cuối

Tôi **đồng ý với phản biện của bạn** và **xác nhận triển khai theo hướng mới**.

### Chốt 3 quyết định

1. **Splitter chạy trên toàn bộ 1,336 file**
2. **Primary strategy = `TÓM TẮT-first, look-back for title`**
3. **Split article ra file riêng trên disk**

Đây là hướng tốt hơn plan cũ và phù hợp hơn với thực trạng dữ liệu bạn vừa khảo sát. Plan cũ vẫn đúng ở kiến trúc tổng thể và KPI ưu tiên boundary hơn GO, nhưng giả định “chỉ issue lớn mới là vấn đề” thì giờ nên bỏ. 

