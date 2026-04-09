Có — để mình tạo **100 câu hỏi article-level** thật chắc tay và dùng tốt cho **Gate G3**, mình cần chốt vài điểm trước. Không nhiều đâu, nhưng rất quan trọng:

1. **Format output bạn muốn**
   Hiện file `vmj_retrieval_gold_v1.jsonl` của bạn đang có dạng:
   `{"query": ..., "expected_title": ..., "type": ...}`

   Mình cần biết bạn muốn mình giữ đúng format này, hay nâng lên thành:
   `query / expected_title / expected_doc_id / type / difficulty / rationale`

2. **Định nghĩa “article-level” của bạn**
   Mình đang hiểu là:

   * mỗi câu hỏi phải truy hồi ra **đúng 1 bài báo đích**
   * không hỏi kiểu “tên bài là gì” quá lộ
   * ưu tiên tín hiệu ở mức **toàn bài**: bệnh, đối tượng, mục tiêu, bối cảnh, phương pháp, nơi nghiên cứu, biến số chính

   Nếu đúng vậy mình sẽ bám theo. Nếu bạn muốn “article-level” theo nghĩa khác thì cần chốt.

3. **Mức độ khó mong muốn**
   Mình đề xuất chia 100 câu thành:

   * 20 câu dễ: bám chủ đề rõ
   * 50 câu trung bình: paraphrase, đổi trật tự, rút gọn
   * 30 câu khó: nhiều thực thể, dễ nhầm với bài gần giống, có hard negative pressure

   Nếu bạn muốn thiên về benchmark khó hơn, mình sẽ tăng tỉ lệ câu khó.

4. **Có dùng kiến thức ngoài ở mức nào**
   Bạn nói “dựa vào file vmjojs, và cả kiến thức bên ngoài”.
   Mình cần chốt là:

   * **A.** chỉ dùng kiến thức ngoài để **paraphrase tự nhiên hơn** và sinh câu hỏi theo cách người dùng thật sẽ hỏi
   * hay **B.** được phép thêm **alias/synonym/chuẩn thuật ngữ y khoa ngoài dữ liệu**
   * hay **C.** được phép thêm cả **suy luận ngữ cảnh** không xuất hiện trực tiếp trong article

   Mình khuyên dùng **A + B**, tránh C để gold set vẫn sạch.

5. **Có cần tránh leakage không**
   Mình khuyên:

   * hạn chế copy nguyên văn title
   * không lặp quá nhiều cụm hiếm nguyên xi từ article
   * tránh query chứa full hospital name + full disease + full period nếu như vậy quá dễ

   Nếu bạn đồng ý, mình sẽ cố tình làm query giống người dùng thật hơn.

6. **Tiêu chí chấm retrieval**
   Ví dụ:

   * pass nếu **Top-1** đúng bài
   * hay chấp nhận **Top-3 / Top-5**
   * nếu có bài rất gần nghĩa thì xử lý thế nào

   Cái này ảnh hưởng cách mình viết câu hỏi và độ “đơn trị” của mỗi query.

7. **Phạm vi nguồn**
   File `vmj_ojs.jsonl` mình thấy là dữ liệu bài báo VMJ/OJS đã được chunk; một bài có nhiều dòng/chunk.
   Mình cần biết:

   * gold set sẽ map về **title** như file hiện tại
   * hay map về **doc_id/article_id canonical**

   Với eval retrieval, mình khuyên map về **canonical article key**, không chỉ title.

Nếu bạn muốn, cách nhanh nhất là bạn chỉ cần trả lời cho mình theo mẫu này:

```text
1) Format: giữ format cũ / format mới
2) Article-level: đúng như bạn hiểu / chỉnh lại...
3) Difficulty: easy-medium-hard = ?
4) External knowledge: A / A+B / A+B+C
5) Leakage: có tránh
6) Metric: Top-1 hay Top-k
7) Mapping key: expected_title hay expected_doc_id
```

Nếu bạn chưa muốn quyết từng mục, mình vẫn có thể tự chốt một default khá an toàn:

* giữ JSONL
* article-level = 1 query -> 1 bài đích
* 20/50/30 easy-medium-hard
* dùng external knowledge ở mức **A+B**
* tránh leakage
* target cho **Top-1**
* output gồm cả `expected_title` và `expected_doc_id` nếu suy ra được

Chỉ cần bạn trả lời 7 điểm trên, mình sẽ bắt đầu dựng bộ **100 câu hỏi gold** cẩn thận.
