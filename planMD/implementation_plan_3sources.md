# Kế hoạch sửa 3 source còn lại

---

## 1. `kcb_moh` — 8.4% GO, 67.5% HOLD

### Đặc điểm nguồn
- **26 file** trên disk, mỗi file là 1 **quyết định bộ Y tế** chứa 10-100 quy trình kỹ thuật
- File lớn nhất: **958,746 chars** (524 trang, 97 quy trình hô hấp)
- Cấu trúc: Admin preamble → MỤC LỤC → Procedure 1 → Procedure 2 → ...
- Mỗi procedure có: **Tiêu đề ALL CAPS** → ĐẠI CƯƠNG → CHỈ ĐỊNH → CHỐNG CHỈ ĐỊNH → CHUẨN BỊ → CÁC BƯỚC TIẾN HÀNH → TÀI LIỆU THAM KHẢO

### Khó khăn — tại sao 67.5% HOLD

Phân tích JSONL cho thấy **3 lỗi gốc** rất rõ:

#### Lỗi 1: Sectionizer cắt tại sub-heading thay vì procedure boundary
Top 5 titles hiện tại:
```
304x  'Tài Liệu Tham Khảo'      ← mỗi procedure đều có TÀI LIỆU THAM KHẢO
243x  '1. Đại Cương'              ← sub-heading, không phải procedure name
170x  '2. Chuẩn Bị'               ← sub-heading
165x  '5. Những Sai Sót Và Xử Trí'  ← sub-heading
164x  '4. Các Bước Tiến Hành'     ← sub-heading
```
→ Sectionizer đang coi **mỗi sub-heading** là 1 procedure mới

#### Lỗi 2: Reference leak nghiêm trọng
- 1,246/1,248 HOLD records bị flag `title_looks_like_reference`
- Sectionizer tạo section từ block "TÀI LIỆU THAM KHẢO" → title = reference text

#### Lỗi 3: Titles GO "giả" — admin text
```
GO titles ví dụ:
  'Cộng Hoà Xã Hội Chủ Nghĩa Việt Nam'     ← document header
  'CONG HOA xX HOI CHU NGHiA VIET NAM'      ← OCR artifact  
  'Danh Sách Nhận Công Văn'                   ← admin metadata
```

### Kế hoạch sửa

#### Fix A: Rewrite `procedure_mode` sectionizer
```
Hiện tại: cắt tại mọi ALL CAPS line ≥10 chars
Sửa thành: chỉ cắt khi gặp ALL CAPS line MÀ line tiếp theo là "ĐẠI CƯƠNG"
```
Logic mới:
1. Scan toàn bộ file, tìm tất cả vị trí có pattern: `[ALL CAPS line] + [ĐẠI CƯƠNG]`
2. Đó mới là procedure boundary thực
3. Mỗi procedure = tất cả text từ boundary này đến boundary tiếp, **gộp thành 1 body**
4. Sub-headings (ĐẠI CƯƠNG, CHỈ ĐỊNH, etc.) giữ trong body, không cắt thành section riêng

#### Fix B: Strip references aggressively
- Cắt bỏ mọi block bắt đầu bằng `TÀI LIỆU THAM KHẢO` cho đến hết procedure
- Cắt bỏ dòng kiểu `1. Author et al (202X)...`

#### Fix C: Strip admin preamble hoàn toàn
- Skip tất cả text trước procedure đầu tiên (QĐ, "Nơi nhận:", "BỘ TRƯỞNG", etc.)

#### Kết quả kỳ vọng
| Metric | Hiện tại | Kỳ vọng |
|--------|:--------:|:-------:|
| Records/file | 71 | **10-30** (= số procedure thực) |
| Unique titles | 262 (nhiều trùng sub-heading) | **~200+** (tên procedure thực) |
| GO% | 8.4% | **≥70%** |
| HOLD% | 67.5% | **≤5%** |
| ref_leak | 1,248 | **0** |

---

## 2. `cantho_med_journal` — 59.8% GO, 2.6% HOLD

### Đặc điểm nguồn
- **2,135 file** — source lớn thứ 2 (sau vmj_ojs)
- Mỗi file = **1 bài báo** (không phải multi-article như vmj_ojs)
- Cấu trúc tương tự hue_jmp_ojs: header tạp chí → title → authors → abstract → nội dung → TÀI LIỆU THAM KHẢO
- File mẫu: `TẠP CHÍ Y DƯỢC HỌC CẦN THƠ – SỐ 45/2022` → Title → Authors → Body

### Khó khăn — tại sao 59.8% GO thay vì ~88% như hue_jmp

#### Lỗi 1: Reference text lọt vào title (35 records, 30%)
```
Ví dụ title bị lấy sai:
  'COVID-19 among Chinese residents during the rapid rise period...'  ← reference EN
  'cholecystectomy during the same session: Feasibility and safety, World J Gastroe...'  ← citation
  'Phacoemulsification in posterior polar cataract: Experience from a tertiary eye...'  ← citation
```
→ Title extractor vẫn bắt nhầm dòng citation tiếng Anh đầu file

#### Lỗi 2: Một số file bắt đầu bằng phần cuối bài trước
```
File 1001_853.txt:
  L13: 9. Zhong, Bao-Liang, Luo, W., et al. (2020), Knowledge, attitudes...
  L14: COVID-19 among Chinese residents during the rapid rise period...
```
→ File bắt đầu bằng **reference #9 của bài trước**, title extractor lấy nhầm reference line

### Kế hoạch sửa

#### Fix A: Reference line filter mạnh hơn cho cantho
Thêm patterns vào `_RE_REFERENCE_LINE`:
- Dòng bắt đầu bằng `\d+\.\s+[A-Z].*et al` (numbered citation)
- Dòng chứa journal abbreviations: `World J`, `Am J`, `Prev Chronic Dis`, `ISSN`
- Dòng tiếng Anh thuần (>80% ASCII) ở đầu file

#### Fix B: Skip lines trước title thực
Nếu file bắt đầu bằng references/citations (từ bài trước), skip cho đến khi gặp:
- Dòng ALL CAPS tiếng Việt ≥30 chars (= title thật)
- Hoặc dòng header tạp chí `TẠP CHÍ Y DƯỢC HỌC CẦN THƠ`

#### Kết quả kỳ vọng
| Metric | Hiện tại | Kỳ vọng |
|--------|:--------:|:-------:|
| GO% | 59.8% | **≥80%** |
| ref_leak | 35 | **≤3** |
| HOLD% | 2.6% | **≤2%** |

---

## 3. `vmj_ojs` — 7.4% GO, 87.6% REVIEW

### Đặc điểm nguồn
- **1,336 file** — source lớn nhất
- Mỗi file = **1 số tạp chí** chứa **nhiều bài báo** (10-40 bài/file)
- File lớn: **965K-1.35M chars** (30K-42K dòng)
- Header: `TẠP CHÍ Y HỌC VIỆT NAM TẬP 538 - THÁNG 5 - SỐ CHUYÊN ĐỀ - 2024`

### Khó khăn — nguồn khó nhất

#### Lỗi 1: Mỗi file = 1 số tạp chí, nhưng pipeline chỉ lấy 1 title
```
File 10201_8922.txt: 965,679 chars
  → 1 title: 'THỰC TRẠNG CÔNG TÁC KHÁM...' (bài đầu tiên)
  → 186 records đều dùng title này
  → Nhưng trong file có 20+ bài khác nhau!
```

#### Lỗi 2: Article_mode sectionizer cắt vô nghĩa
Vì body chứa CẢ SỐ tạp chí, sectionizer tách theo heading "KẾT QUẢ", "BÀN LUẬN" → nhưng mỗi bài đều có "KẾT QUẢ" → sections pha trộn text từ nhiều bài

#### Lỗi 3: Body rất lớn, sections bị trộn bài
```
Record GO title: 'hernia recurrence after microdiscectomy. Revista Española...'
→ Đây là text reference/abstract từ 1 bài khác, không phải title
```

### Kế hoạch sửa

#### Fix: Article Boundary Splitter (pre-processing step)

**Ý tưởng**: Trước khi vào pipeline chuẩn, split file thành nhiều article riêng biệt.

**Pattern ranh giới bài** (quan sát từ dữ liệu thực):
```
[Dòng trống]
[TITLE: ALL CAPS tiếng Việt, ≥20 chars, ≤300 chars, có thể 2-3 dòng]
[AUTHORS: "Nguyễn Văn A1*, Trần B2" hoặc tên + số superscript]
[AFFILIATION: "1Trường Đại học...", "2Bệnh viện..."]
[Dòng trống]
[TÓM TẮT hoặc ĐẶT VẤN ĐỀ]
```

**Logic cụ thể**:
1. Scan file line-by-line
2. Khi gặp block: `ALL CAPS title + author line + "TÓM TẮT"` → đánh dấu = article boundary
3. Split file tại mỗi boundary → N article files tạm
4. Mỗi article đi qua `article_mode` sectionizer như bình thường

> [!WARNING]
> vmj_ojs là nguồn khó nhất. Không nên block pipeline V1 vì nó. Ưu tiên sửa kcb_moh và cantho trước.

#### Kết quả kỳ vọng
| Metric | Hiện tại | Kỳ vọng |
|--------|:--------:|:-------:|
| Records/file | 34.9 | **phụ thuộc số bài/file** |
| Unique titles | 30 (= 30 files) | **300-600** (= 10-20 bài × 30 files) |
| GO% | 7.4% | **≥60%** (sau split) |
| title accuracy | ~100% (nhưng chỉ bài đầu) | **~90%** (tất cả bài) |

---

## Thứ tự triển khai đề xuất

| Ưu tiên | Source | Complexity | Thời gian | Impact |
|:-------:|--------|:---------:|:---------:|--------|
| **1** | `kcb_moh` | Medium | 2-3h | ROI cao nhất: 26 file → ~500 GO records |
| **2** | `cantho_med` | Low | 1h | 2,135 files, fix nhỏ → ~80% GO |
| **3** | `vmj_ojs` | High | 4-6h | 1,336 files, nhưng article boundary khó |

## Open Questions

> [!IMPORTANT]
> 1. **kcb_moh**: Nếu procedure quá ngắn (<2000 chars), có nên merge với procedure kề không hay giữ nguyên?
> 2. **vmj_ojs**: Nên split ra file riêng (pre-processing disk) hay split in-memory khi process?
> 3. **cantho_med**: Có muốn loại hoàn toàn các file bắt đầu bằng reference (= file bị cắt sai từ crawl) không?
