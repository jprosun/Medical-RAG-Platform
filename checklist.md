Dưới đây là checklist thực dụng để bạn và team **truy nguyên nguyên nhân G3 fail** một cách có hệ thống, thay vì đoán mò. Nó bám đúng các dấu hiệu trong report: **Hit@3 = 55%**, easy chỉ **65%**, nhiều `expected_title` bị cắt/méo, và top results có cả fragment rác/tham khảo.  

## 1) Mục tiêu của vòng audit

Mỗi query fail cần được gán vào **1 nguyên nhân chính**:

* **EVAL_KEY_ERROR**: bài đúng đã xuất hiện nhưng bị chấm sai vì `expected_title` hỏng hoặc so khớp title quá thô
* **METADATA_NOISY**: payload/title/chunk bị cắt, lẫn reference, bibliographic text, fragment rác
* **EMBEDDING_MISS**: query đúng, dữ liệu sạch, nhưng model không kéo được bài đúng lên top-k
* **NEAR_DUPLICATE_CONFUSION**: retrieve ra bài rất gần nghĩa/cùng bệnh/cùng kỹ thuật nhưng sai article
* **QUERY_TOO_ABSTRACT**: query vượt quá mức article-level có thể truy hồi tốt với dense retrieval hiện tại
* **INGESTION_ERROR**: bài đúng có trong nguồn gốc nhưng index sai, chunk sai, thiếu chunk, hoặc gán title sai

## 2) Phạm vi audit trước mắt

Audit trước **toàn bộ 45 query fail Hit@3**. Report hiện cho thấy:

* tổng 100 query
* **Hit@1 = 50%**
* **Hit@3 = 55%**
* **Hit@5 = 59%**
* easy chỉ **13/20 ở Hit@3**, medium **27/50**, hard **15/30**.  

Đây là đủ lớn để thấy pattern, nhưng vẫn đủ nhỏ để review thủ công trong 1–2 buổi.

## 3) File audit nên có các cột này

Tạo một sheet hoặc CSV với các cột:

* `query_id`
* `difficulty`
* `group`
* `query`
* `expected_title_raw`
* `expected_title_should_be`
* `top1_title`
* `top3_titles`
* `is_true_hit_at_3_manual` (yes/no)
* `failure_bucket`
* `primary_root_cause`
* `secondary_root_cause`
* `notes`
* `fix_action`

## 4) Quy trình review cho từng query fail

### Bước A: kiểm tra “bị chấm oan” trước

Đọc query, rồi nhìn `expected_title_raw` và top-3.

Đánh dấu **EVAL_KEY_ERROR** nếu xảy ra một trong các tình huống:

* top result là cùng bài nhưng title ở index đầy đủ hơn còn `expected_title` chỉ là mảnh bị cắt
* `expected_title` chỉ là đoạn đuôi/đoạn giữa, ví dụ:

  * `TỪ THÁNG 4 NĂM 2023 ĐẾN THÁNG 4 NĂM 2`
  * `KỸ THUẬT VÀ KẾT QUẢ BƯỚC ĐẦU`
  * `BỆNH VIỆN TRUNG ƯƠNG THÁI NGUYÊN`
    Các title kiểu này đã xuất hiện trong report và không đủ tin cậy để làm canonical key. 
* top result là full title đúng nghĩa, nhưng expected key chỉ là đoạn truncated

Nếu đúng, sửa `expected_title_should_be` bằng title canonical sạch.

### Bước B: kiểm tra metadata/title có bẩn không

Đánh dấu **METADATA_NOISY** nếu thấy:

* title chứa ký tự lỗi như `[` `\` hoặc encoding lạ
  Ví dụ: `VIÊM GAN B MẠN CHƯA ĐIỀU TRỊ V[ MỘT SỐ YẾU TỐ LIÊN QUAN`, `THAI ĐẾN KH\\M THAI...` 
* title chỉ là fragment mơ hồ:

  * `NHÂN CÁC TRƯỜNG HỢP LÂM SÀNG`
  * `KỸ THUẬT VÀ KẾT QUẢ BƯỚC ĐẦU`
  * `COVID-19 TẠI BỆNH VIỆN...` nhưng thiếu phần đầu
* top result là text kiểu reference / luận văn / câu trích tài liệu
  Report đang có nhiều kết quả dạng này. 

Nếu đúng, ghi `fix_action = reparse metadata / clean title / remove references / rebuild payload`.

### Bước C: kiểm tra có phải retrieve gần đúng nhưng sai bài không

Đánh dấu **NEAR_DUPLICATE_CONFUSION** nếu top-1 hoặc top-3:

* cùng bệnh
* cùng thủ thuật
* cùng nhóm bệnh nhân
* nhưng khác bệnh viện / khác mục tiêu nghiên cứu / khác article

Ví dụ rất rõ:

* query về *cắt một phần thận nội soi cho ung thư tế bào thận cT1bN0M0* lại lên bài *tán sỏi thận qua da* ở top-1, còn bài đúng chỉ ở top-4/top-5. Đây là dấu hiệu semantic overlap ở ngành tiết niệu nhưng phân biệt article yếu. 
* query về *u tương bào ngoài tủy hiếm gặp* lại lên một bài *báo cáo một trường hợp hiếm gặp và hồi cứu y văn* khác. 

Nếu đúng, ghi `fix_action = article-level rerank / hybrid / collapse-by-article`.

### Bước D: kiểm tra model semantic miss thật sự

Đánh dấu **EMBEDDING_MISS** nếu:

* query hợp lệ, rõ article-level
* expected title sạch
* top-k không có bài đúng
* top-k cũng không có bài thật sự gần đúng, hoặc toàn bài lệch ngữ nghĩa

Ví dụ các query kiểu:

* *Viêm lệ quản thường do tác nhân vi sinh nào*
* *Viêm loét giác mạc trẻ em có căn nguyên vi sinh nào hay gặp*
* *Mốc PLT nào đủ nhạy để báo động sớm...*
  mà top results lại lệch rất xa. Đây thường là chỗ dense English model hụt ngữ nghĩa tiếng Việt chuyên ngành. 

### Bước E: kiểm tra query có quá trừu tượng không

Đánh dấu **QUERY_TOO_ABSTRACT** nếu query:

* không còn lexical anchor đủ mạnh
* dùng khái niệm quá khái quát như “cơ chế bệnh sinh”, “gợi ý ác tính”, “mức độ bệnh”, “hữu ích không”
* mà trong corpus lại có nhiều bài gần nhau

Điều này đặc biệt hay xảy ra ở hard set. Ví dụ các query về:

* bằng chứng tổng hợp,
* phân tích mạng lưới,
* chi phí hiệu quả,
* mô hình dự báo,
* thang sàng lọc rút gọn. 

Nếu đúng, chưa chắc là lỗi hệ thống; có thể gán `secondary_root_cause = benchmark hard by design`.

### Bước F: kiểm tra ingestion/index có lỗi không

Đánh dấu **INGESTION_ERROR** nếu:

* bạn tìm trong nguồn gốc thấy bài tồn tại đầy đủ, nhưng index chỉ có chunk rời / title sai
* một bài bị split thành nhiều chunk nhưng payload `title` không thống nhất
* expected article không xuất hiện với payload nhất quán trong Qdrant

Đây là lỗi hạ tầng, không phải lỗi model.

## 5) Checklist pass/fail cho từng query

Khi review từng query, hỏi tuần tự:

1. `expected_title_raw` có đủ làm canonical key không?
2. Top-3 có bài đúng nhưng bị chấm sai do title mismatch không?
3. Top-3 có fragment/rác/reference không?
4. Top-3 có bài gần đúng nhưng sai article không?
5. Nếu không, đây có phải semantic miss thật không?
6. Query có quá trừu tượng so với dense retrieval hiện tại không?
7. Có dấu hiệu ingest sai / payload sai không?

Chỉ cần trả lời tuần tự như vậy là bucket sẽ hiện ra khá rõ.

## 6) Quy tắc gắn nhãn nhanh

Bạn có thể dùng luật ngắn này để không bị tranh cãi:

* **Nếu top-k chứa cùng bài nhưng khác title** → `EVAL_KEY_ERROR`
* **Nếu top-k chứa fragment/bibliography/rác** → `METADATA_NOISY`
* **Nếu top-k chứa bài gần giống nhưng sai article** → `NEAR_DUPLICATE_CONFUSION`
* **Nếu top-k lệch hẳn, query sạch, title sạch** → `EMBEDDING_MISS`
* **Nếu query quá khái quát/ẩn dụ/đa bước** → `QUERY_TOO_ABSTRACT`
* **Nếu source có bài đúng nhưng index không phản ánh đúng** → `INGESTION_ERROR`

## 7) Những pattern cần đếm ngay sau audit

Sau khi gắn nhãn 45 query fail, đếm:

* bao nhiêu query là **bị chấm oan**
* bao nhiêu query fail vì **title/payload bẩn**
* bao nhiêu query là **near duplicate**
* bao nhiêu query là **semantic miss thật**
* bao nhiêu query là **benchmark quá abstract**

Kết quả này sẽ quyết định roadmap.

### Cách diễn giải:

* nếu `EVAL_KEY_ERROR + METADATA_NOISY` > 30% fail
  → sửa data/eval trước, chưa cần kết luận model
* nếu `EMBEDDING_MISS` chiếm lớn
  → đổi model multilingual là ưu tiên cao
* nếu `NEAR_DUPLICATE_CONFUSION` nhiều
  → cần hybrid + reranker + article collapse
* nếu `QUERY_TOO_ABSTRACT` nhiều
  → tách benchmark thành 2 mức: production realistic và stress test

## 8) Hành động sửa tương ứng

### Nếu lỗi chính là key/title

* tạo `article_id`
* chuẩn hóa `title_clean`, `title_norm`
* evaluator match theo `article_id` hoặc `title_norm`, không match raw title

### Nếu lỗi chính là metadata noisy

* parse lại title từ nguồn
* loại reference chunks
* chặn chunk mở đầu/kết thúc bị dính tài liệu tham khảo
* thống nhất payload theo article

### Nếu lỗi chính là model

* thay `bge-small-en-v1.5`
* benchmark lại bằng model multilingual
* giữ nguyên gold set để so công bằng

### Nếu lỗi chính là near duplicate

* retrieve top 20–50 chunks
* collapse theo `article_id`
* rerank ở mức article
* cân nhắc hybrid dense + sparse

## 9) Tiêu chí hoàn tất vòng audit

Vòng audit coi như xong khi bạn có:

* 1 bảng 45 fail đã gắn bucket
* 1 bảng tổng hợp số lượng theo bucket
* 10 ví dụ đại diện cho từng bucket
* 1 quyết định rõ: sửa **eval**, **data**, **model**, hay **retrieval stack**

## 10) Thứ tự sửa khuyến nghị

Mình khuyên làm theo thứ tự này:

1. **Sửa evaluator và canonical key**
2. **Sửa title/payload/chunk cleanliness**
3. **Chạy lại G3 với cùng model hiện tại**
4. **Mới bắt đầu ablation model multilingual**
5. **Sau đó mới hybrid + rerank**

Làm ngược lại sẽ rất dễ tốn công tuning model trên một benchmark đang bị nhiễu.
