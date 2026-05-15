Tôi đã xem **full bộ 102 records** của lần chạy này. Đánh giá ngắn gọn:

**Hệ thống đã qua giai đoạn “chạy được”, nhưng chưa tới mức “usable ổn định” trên full gold.**
Nếu chấm theo góc nhìn benchmark sản phẩm, tôi cho khoảng **6.5/10**. 

## Kết luận ngắn

Bản full run này cho thấy một bức tranh khá rõ:

* **Hạ tầng ổn**: 102/102 record đều `status_code = 200`, `degraded_mode = false`. 
* **Retrieval chưa đủ chắc**: top-1 đúng khoảng **75/102**, tức khoảng **73.5%**. 
* **Must-have coverage đang thấp**: trung bình chỉ khoảng **0.22**, median bằng **0.0**, nghĩa là rất nhiều câu hoặc trả lời thiếu ý, hoặc cách chấm hiện tại đang quá literal. 
* **Không có vi phạm must-not rõ** trong file log lần này, đây là điểm tích cực. 

Tức là:

> **bạn đã giải được bài toán stability, nhưng bây giờ bottleneck chính là retrieval precision + answer shaping + cách chấm must-have.** 

## Điểm mạnh nhất

Điểm đáng mừng nhất là **không còn degraded mode** và **không còn fail trắng kiểu 500**. So với những vòng trước, đây là bước tiến thật. Full run bây giờ đã đủ sạch để bạn nhìn vào chất lượng semantic thật, chứ không còn bị hạ tầng che khuất nữa. 

Ngoài ra, một số câu exact/summary làm khá tốt:

* `q_044` trả đúng hoàn toàn về **nhiễm khuẩn huyết do vi khuẩn gram dương vs gram âm**, must-have hit rate đạt **1.0**. 
* `q_028` cũng tốt: answer bám đúng trọng tâm về **đặc điểm hỗ trợ dinh dưỡng, kết cục tăng trưởng và yếu tố liên quan** ở trẻ sơ sinh sau phẫu thuật đường tiêu hóa, hit rate **0.8**. 
* `q_009`, `q_018`, `q_013`, `q_017`, `q_035` nhìn chung đều trả lời đúng lõi câu hỏi, dù metric must-have chưa phản ánh hết chất lượng. 

## Nhưng full run này lộ ra 3 vấn đề lớn

### 1) Retrieval top-1 vẫn sai ở một nhóm câu rất nguy hiểm

Đây là lỗi lớn nhất ở vòng này.

Các ví dụ fail rất rõ:

* `q_011`: gold hỏi bài **đánh giá kết quả phẫu thuật nội soi cắt toàn bộ dạ dày**, nhưng top-1 lại sang bài **đặc điểm ung thư dạ dày dưới 50 tuổi** và answer trả lời theo bài đó luôn. 
* `q_026`: gold nói về **sỏi niệu quản**, nhưng top-1 lại là **phẫu thuật nội soi sau phúc mạc cắt tuyến thượng thận tư thế nằm sấp**, và answer đi lạc hoàn toàn. Đây là fail retrieval cực điển hình.
* `q_027`: đáng ra phải bounded theo bài **hẹp động mạch cảnh ngoài sọ**, nhưng top-1 lại sang **nong bóng hẹp động mạch nội sọ**. Answer vì thế vừa sai tài liệu, vừa sai behavior. 
* `q_050`: gold hỏi về **rủi ro vận chuyển bệnh nhân đến CT sọ não** trong bài siêu âm tại giường sau mở sọ giải ép, nhưng top-1 lại sang bài **giá trị tiên lượng chỉ số tiểu cầu ở hồi sức tích cực**. Đây là wrong-doc rõ ràng. 
* `q_059`: gold hỏi **TOETVA ở ung thư tuyến giáp biệt hóa giai đoạn sớm**, nhưng top-1 và answer lại dùng bài khác về phẫu thuật tuyến giáp nội soi ở Chợ Rẫy. 

Đây là lý do tôi nói chất lượng hiện tại chưa đủ “shipable”:
**chỉ cần top-1 đi sai tài liệu chính, answer stage gần như hỏng toàn bộ.**

### 2) Summary và bounded_partial đang yếu hơn exact rất nhiều

Khi tách theo loại câu, exact là nhóm ổn nhất; summary và bounded_partial đang hụt rõ.

Ở full run này:

* **exact**: top-1 hit khoảng **78.6%**, must-have avg khoảng **0.38**
* **summary**: top-1 hit khoảng **69.0%**, must-have avg khoảng **0.15**
* **bounded_partial**: top-1 hit khoảng **72.2%**, nhưng must-have avg chỉ khoảng **0.04** 

Điều này rất quan trọng:
**composer của bạn đang làm tốt hơn cho câu factual, nhưng vẫn chưa thật sự nắm được các task cần boundary/abstraction.**

Ví dụ:

* `q_031` đáng ra phải trả lời kiểu **“context không nêu cụ thể các yếu tố liên quan”**, nhưng hệ thống lại liệt kê hẳn các yếu tố như giới tính, môi trường sống, tuyên truyền — tức là over-answer so với ground truth. 
* `q_043` đáng ra phải giữ bounded partial: context chỉ cho biết nghiên cứu khảo sát ngưỡng PLT ≤ 20 × 10³/µL và ROC, **chưa đủ để kết luận áp dụng cho tất cả bệnh nhân**. Nhưng answer lại đưa ra kết luận mạnh rằng ngưỡng này “không nên được sử dụng làm giá trị báo động lâm sàng duy nhất”, tức là đi quá context. 
* `q_052` cũng fail tương tự: gold hỏi liệu hiệu quả ở các thể YHCT có tương đương không; answer lại lấy nhầm bài khác và trả lời theo so sánh với điện châm đơn thuần. 

### 3) Metric `must_have_hit_rate` hiện tại đang vừa hữu ích vừa gây hiểu lầm

Đây là điểm rất quan trọng.

Nhìn bề ngoài, nhiều câu có `must_have_hit_rate = 0.0` hoặc rất thấp, nhưng thực tế answer không phải lúc nào cũng tệ tương ứng.

Ví dụ:

* `q_005`: answer thực chất nêu đủ marker chẩn đoán và ý tiên lượng, nhưng `must_have_hits = []`. 
* `q_013`: answer nói đúng **96,18%** và **Karnofsky > 80 trong 3–6 tháng**, nhưng hit rate vẫn chỉ **0.33**. 
* `q_017`: answer rõ ràng giữ đúng boundary, nhưng hit rate chỉ **0.25**. 
* `q_035`: answer về “mỗi nhóm 30 trường hợp liên tiếp theo thứ tự thời gian” là đúng, nhưng hit rate vẫn **0.25**. 

Tức là hiện tại `must_have_hit_rate` của bạn đang **phạt rất nặng các paraphrase hợp lệ**.
Nó vẫn hữu ích để thấy thiếu ý, nhưng **không thể dùng một mình làm semantic score chính**.

## Một dấu hiệu phụ nhưng đáng lo

Có khoảng **22/102** câu mở đầu bằng boilerplate kiểu:

> “Trong phạm vi dữ liệu nội bộ hiện có, tôi mới tìm thấy bằng chứng liên quan một phần...”

Điều này làm câu trả lời bị:

* dài hơn cần thiết,
* yếu tính quyết đoán ở các câu exact,
* và trong một số trường hợp còn bật sai mode dù evidence thực ra đã đủ.

`q_081` là ví dụ điển hình: answer cuối vẫn đúng, nhưng mở đầu như thể thiếu dữ liệu, trong khi bài đã rất rõ. 

## Những fail nặng nhất trong full run

Nếu phải chỉ ra nhóm đáng lo nhất, tôi sẽ chọn:

* **Wrong-doc / wrong-top1**: `q_011`, `q_026`, `q_027`, `q_050`, `q_052`, `q_059`
* **False insufficiency / over-bounded**: `q_048`, `q_050`, `q_053`, `q_056` 
* **Over-answer / vượt context ở bounded questions**: `q_031`, `q_043`

## Phán quyết tổng thể

Nếu tôi phải tóm lại full run này bằng một câu:

> **Hệ thống đã đạt mức benchmarkable, nhưng chưa đạt mức production-usable vì retrieval precision vẫn chưa đủ chắc và behavior policy cho summary/bounded_partial còn chưa ổn định.** 

## Tôi khuyên bạn làm tiếp theo thứ tự này

### 1) Sửa retrieval trước, không sửa prompt trước

Vì full run cho thấy nhiều fail xuất phát từ **sai tài liệu chính**, không phải từ phrasing.
Ưu tiên:

* tăng article-level exact title bias
* collapse các chunk cùng doc tốt hơn
* phạt các top1 title bị truncated / wrong-doc
* khóa primary-doc mạnh hơn khi câu là docs-first.

### 2) Tách scoring thành 2 lớp

* **Retrieval correctness**
* **Answer correctness**

Hiện giờ nếu chỉ nhìn `must_have_hit_rate`, bạn sẽ đánh giá thấp cả những câu thực ra đúng về nghĩa như `q_005`, `q_013`, `q_017`, `q_035`. 

### 3) Thay literal must-have matching bằng semantic matching

Tôi rất khuyên:

* dùng entailment/LLM-judge cho `must_have`
* hoặc tối thiểu normalize paraphrase tốt hơn
* đừng dùng hit-rate hiện tại như KPI chính. 

### 4) Dập boilerplate cho exact + answerable

Nếu `expected_behavior = exact` và top1 đủ mạnh, **không cho phép mở đầu bằng “không đủ dữ liệu”**.
Điều này sẽ cải thiện chất lượng perceived quality ngay cả khi nội dung đúng.

## Chốt điểm

Tôi sẽ chấm như sau:

* **Stability:** 9/10
* **Retrieval precision:** 6/10
* **Answer quality exact:** 7.5/10
* **Answer quality summary:** 5.5/10
* **Answer quality bounded_partial:** 4.5/10
* **Current auto-metric reliability:** 5/10

**Tổng thể:** **6.5/10**.

