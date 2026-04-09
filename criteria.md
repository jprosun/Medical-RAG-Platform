Để mình **đánh giá kỹ từng tiêu chí một cách có căn cứ**, mình cần bạn cung cấp thêm thông tin theo đúng từng nhóm tiêu chí, thay vì chỉ mô tả kiến trúc. Vì hiện tại mình mới thấy khá rõ phần pipeline và một phần cơ chế kiểm soát, nhưng để **chấm được độ tin cậy thật sự**, còn thiếu dữ liệu về corpus, benchmark, hành vi thực tế và kết quả test.  

## 1) Với tiêu chí **đúng nội dung chuyên môn**

Mình cần:

* **Phạm vi chuyên môn chính xác** của chatbot
  Ví dụ: nội tiết, tim mạch, hô hấp, hay đa chuyên khoa.
* **Đối tượng trả lời chính**
  Sinh viên năm nào, bác sĩ nội trú, bác sĩ đa khoa, hay bác sĩ chuyên khoa.
* **Bộ nguồn tri thức đang dùng**
  Sách, guideline, review, original study, website y khoa nào.
* **Nguyên tắc ưu tiên nguồn**
  Ví dụ: guideline > systematic review > textbook > original study.
* **10–20 câu hỏi mẫu khó** trong phạm vi thật của bạn
* **Đáp án chuẩn tham chiếu** cho các câu đó
  Do bạn tự soạn, giảng viên, bác sĩ hướng dẫn, hay từ guideline.

### Vì sao cần?

Vì muốn chấm “đúng chuyên môn” thì phải biết:

1. chatbot đang được kỳ vọng trả lời cái gì
2. và “đúng” đang đối chiếu theo chuẩn nào

---

## 2) Với tiêu chí **đúng nguồn trích dẫn / bám bằng chứng**

Mình cần:

* **Cấu trúc metadata của từng tài liệu**

  * title
  * source_name
  * year
  * doc_type
  * trust_tier
  * source_url
* **Ví dụ 5–10 câu trả lời thực tế** có citation
* **Raw retrieved chunks** tương ứng với mỗi câu trả lời
* **Quy tắc hiện tại để gán citation [1], [2], [3]**
* **Có bắt buộc mọi claim quan trọng phải đi kèm nguồn không**
* **Có câu trả lời nào từng cite sai chưa**

### Vì sao cần?

Hiện hệ thống có cơ chế citation và trả kèm retrieved chunks, nhưng muốn đánh giá thật thì mình phải đối chiếu:

* claim nào
* đến từ chunk nào
* chunk đó có thật sự nói đúng điều claim đó không 

---

## 3) Với tiêu chí **không bịa khi thiếu dữ liệu**

Mình cần:

* **5–10 câu hỏi ngoài phạm vi**
  Ví dụ hỏi ngoài chuyên khoa, hỏi thuốc rất hiếm, hỏi vấn đề không có trong corpus
* **5–10 câu hỏi mơ hồ / thiếu ngữ cảnh**
* **5–10 câu hỏi có dữ liệu trong corpus nhưng không đủ để kết luận**
* **Output thật của hệ thống** cho các case đó
* **Cách chatbot đang fallback khi thiếu context**
* **Ngưỡng nào thì coverage bị coi là low / medium / high trong chạy thực tế**

### Vì sao cần?

Mình không thể đánh giá “không bịa” chỉ bằng thiết kế. Phải nhìn các ca mà hệ thống **nên từ chối hoặc trả lời dè dặt** xem nó có làm thật không. Hiện kiến trúc có coverage scorer và rule buộc nêu giới hạn, nhưng cần case thực tế để kiểm chứng.  

---

## 4) Với tiêu chí **biết nêu mức độ chắc chắn / mức độ giới hạn**

Mình cần:

* **Ví dụ các câu hỏi có bằng chứng mạnh**
* **Ví dụ các câu hỏi có bằng chứng yếu hoặc mâu thuẫn**
* **Ví dụ câu trả lời mà chatbot có section “Giới hạn & Mức chắc chắn”**
* **Quy tắc hiện tại để xác định certainty**

  * dựa coverage
  * dựa trust tier
  * dựa số lượng nguồn
  * hay do LLM tự diễn đạt
* **Có phân biệt được guideline mạnh với nghiên cứu đơn lẻ không**

### Vì sao cần?

Vì đây là điểm rất quan trọng với chatbot y khoa. Một hệ thống có thể trả lời đúng một phần, nhưng vẫn **nguy hiểm nếu nói quá chắc**. Hiện hệ thống đã có section giới hạn/mức chắc chắn, nhưng mình cần xem nó dùng có nhất quán không. 

---

## 5) Với tiêu chí **ưu tiên nguồn mạnh hơn nguồn yếu**

Mình cần:

* **Danh sách các loại nguồn trong corpus**

  * guideline
  * textbook
  * review
  * meta-analysis
  * original study
  * local journal
* **Cách gán trust_tier**
* **Article ranking formula hiện tại có dùng trust_tier thực sự không**
* **Ví dụ một câu hỏi có nhiều nguồn khác nhau cùng trả lời**
* **Top retrieved articles + article scores** cho case đó
* **Trường hợp có xung đột giữa guideline và bài nghiên cứu lẻ**

### Vì sao cần?

Vì đây có thể là điểm yếu lớn của pipeline. Mình muốn biết hệ thống đang chọn “nguồn liên quan nhất” hay “nguồn mạnh nhất về bằng chứng”. Hai cái này không giống nhau. Trong file bạn gửi cũng có lưu ý rằng trust-tier chưa được dùng đầy đủ trong ranking. 

---

## 6) Với tiêu chí **trả lời đủ ý cho người có chuyên môn**

Mình cần:

* **Bạn kỳ vọng mỗi loại câu hỏi phải có những phần nào**
  Ví dụ:

  * định nghĩa
  * cơ chế
  * chỉ định
  * chống chỉ định
  * mức chứng cứ
  * số liệu
  * giới hạn nghiên cứu
* **3–5 ví dụ cho mỗi loại query**

  * fact
  * mechanism
  * study result
  * guideline comparison
  * appraisal
* **Rubric “đủ ý”** của bạn hoặc giảng viên hướng dẫn
* **Câu trả lời thực tế của chatbot** cho từng loại

### Vì sao cần?

“Đủ ý” với sinh viên y khác “đủ ý” với bác sĩ lâm sàng. Muốn chấm đúng phải biết chuẩn mong đợi cho từng nhóm người dùng.

---

## 7) Với tiêu chí **xử lý câu hỏi follow-up**

Mình cần:

* **5–10 đoạn hội thoại nhiều lượt**
* Trong đó có:

  * câu hỏi tham chiếu đại từ
  * đổi chủ thể giữa chừng
  * hỏi tiếp một ý hẹp
  * follow-up phủ định / so sánh
* **Query gốc**, **query rewritten**, **retrieved chunks**, **answer cuối**
* **Các case rewrite sai mà bạn từng gặp**

### Vì sao cần?

Hiện kiến trúc có query rewriter và session history, nhưng chưa thể kết luận tốt hay không nếu chưa có log hội thoại thật.  

---

## 8) Với tiêu chí **ổn định giữa nhiều lần hỏi**

Mình cần:

* Chạy **cùng một câu hỏi ít nhất 3–5 lần**
* Ghi lại:

  * query rewritten
  * retrieved docs
  * answer
  * citations
* **Cấu hình temperature** và các biến ảnh hưởng sampling
* Có dùng cache hay không
* Có thay đổi top_k / retriever profile giữa các lần không

### Vì sao cần?

Một chatbot y khoa không chỉ cần đúng, mà còn phải **ổn định**. Hiện có temperature thấp giúp ổn định hơn, nhưng chưa có consistency test thì chưa đánh giá chắc được. 

---

## 9) Với tiêu chí **thời gian phản hồi / tính dùng được**

Mình cần:

* **Latency trung bình**
* **P95 / P99 latency**
* Tách theo từng bước nếu có:

  * rewrite
  * retrieve
  * extract
  * generate
* **Tốc độ khi query đơn giản vs query sâu**
* **Số lượng token context trung bình**
* **Tốc độ trên máy demo / môi trường thật**

### Vì sao cần?

Vì đồ án có thể đúng nhưng quá chậm thì vẫn chưa ra sản phẩm tốt. Trong file bạn gửi có nói hệ thống có monitoring latency, nên nếu có log số liệu thật thì mình sẽ đánh giá sát hơn. 

---

## 10) Với tiêu chí **dễ kiểm chứng lại / audit được**

Mình cần:

* Một **response sample hoàn chỉnh** gồm:

  * user question
  * rewritten query
  * router output
  * retrieved chunks
  * evidence pack
  * coverage level
  * final answer
* **Cách UI đang hiển thị nguồn**
* **Người dùng có xem được chunk gốc không**
* **Có log lại full trace cho một phiên không**

### Vì sao cần?

Với chatbot y khoa, auditability là điểm rất thuyết phục trước hội đồng. Nếu người dùng hoặc giảng viên có thể lần ngược được từ answer về source thì độ tin cậy tăng rất mạnh.

---

# 11) Với tiêu chí **scope phù hợp cho đồ án**

Mình cần bạn nói rất rõ 5 điều:

1. **Scope chuyên khoa**
2. **Loại câu hỏi chatbot nhận**
3. **Loại câu hỏi chatbot không xử lý**
4. **Nguồn dữ liệu giới hạn trong đâu**
5. **Mục tiêu dùng để học tập / tra cứu / hỗ trợ ra quyết định ở mức nào**

### Vì sao cần?

Vì một hệ thống nhỏ nhưng scope rõ sẽ được đánh giá cao hơn một hệ thống ôm rộng nhưng không kiểm soát nổi.

---

# 12) Thông tin tối thiểu mình cần bạn gửi trước để mình chấm thật

Nếu bạn chưa muốn gửi quá nhiều, chỉ cần gửi trước **gói tối thiểu** này:

### Gói A — mô tả hệ thống

* scope chuyên khoa
* đối tượng người dùng
* danh sách nguồn tri thức
* nguyên tắc ưu tiên nguồn

### Gói B — dữ liệu kiểm thử

* 15–20 câu hỏi mẫu
* đáp án chuẩn hoặc ý chính bắt buộc
* 5 câu follow-up
* 5 câu ngoài phạm vi
* 5 câu có bằng chứng mâu thuẫn/thiếu

### Gói C — output hệ thống

Với mỗi câu hỏi, gửi:

* rewritten query
* retrieved titles/chunks
* answer cuối
* citation
* coverage level

### Gói D — kết quả hiện có

* retrieval metrics nếu có
* benchmark nếu có
* unit/integration tests nếu có
* ví dụ lỗi thực tế từng gặp

---

# 13) Mình sẽ đánh giá lại cho bạn theo format nào sau khi bạn cung cấp

Sau khi bạn gửi các thông tin trên, mình có thể trả lại cho bạn một bản đánh giá theo đúng kiểu hội đồng:

* **tiêu chí nào đạt**
* **tiêu chí nào đạt một phần**
* **tiêu chí nào chưa đạt**
* **rủi ro lớn nhất của hệ thống**
* **các cải tiến ưu tiên cao**
* **kết luận hệ thống hiện phù hợp mức đồ án hay mức sản phẩm thử nghiệm**

Nếu bạn muốn làm nhanh nhất, ở tin nhắn tiếp theo bạn chỉ cần gửi theo mẫu này:

```text
1. Scope:
2. Đối tượng người dùng:
3. Nguồn tri thức:
4. Nguyên tắc ưu tiên nguồn:
5. 10 câu hỏi mẫu + đáp án chuẩn:
6. 5 câu follow-up:
7. 5 câu ngoài phạm vi:
8. 5 câu thiếu dữ liệu / mâu thuẫn:
9. Với mỗi câu: rewritten query + retrieved chunks + answer + citation + coverage:
10. Metrics hiện có:
11. Lỗi từng gặp:
```

Sau đó mình sẽ chấm chi tiết từng tiêu chí cho bạn.
