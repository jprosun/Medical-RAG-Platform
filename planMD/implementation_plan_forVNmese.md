# Chuẩn hóa dữ liệu tiếng Việt → DocumentRecord JSONL

Biến 1,350+ file [.txt](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/requirements.txt) thô từ 8 nguồn y khoa VN thành JSONL chuẩn [DocumentRecord](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/app/document_schema.py#27-87), metadata đúng, text sạch, sẵn sàng cho sectionize → chunk → ingest.

## Phát hiện quan trọng từ khảo sát dữ liệu thực tế

Tất cả 8 nguồn **đều đã có YAML frontmatter** với các trường: `source_id`, `institution`, `source_url`, `file_url`, `pages`, `chars`. Tuy nhiên:

| Nguồn | Files | Title hiện tại | Cấu trúc text | Nhóm |
|-------|------:|---------------|---------------|------|
| `cantho_med_journal` | ~100 | Blank/`PDF` | Bài báo khoa học (TÓM TẮT, ĐẶT VẤN ĐỀ, KẾT QUẢ...) | A – Journal |
| `vmj_ojs` | 262 | `"pdf"` | Bài báo khoa học, đôi khi chứa 2+ bài trong 1 file | A – Journal |
| `hue_jmp_ojs` | 342 | `"PDF"` | Bài báo khoa học, có DOI, abstract EN/VI | A – Journal |
| `mil_med_pharm_journal` | 317 | `"PDF"` | Bài báo khoa học, header lặp mỗi trang | A – Journal |
| `trad_med_pharm_journal` | 230 | `"PDF"` | Bài báo YHCT, heading "BÀI NGHIÊN CỨU" | A – Journal |
| `kcb_moh` | 26 | Blank | Quyết định BYT, quy trình kỹ thuật (10k–20k dòng) | B – Guideline |
| `dav_gov` | 79 | Blank | Bảng thuốc, thông tư, phụ lục dược | C – Pharma |
| `who_vietnam` | 97 | Blank? | Tài liệu WHO (EN/mixed), nghị quyết WHA | D – WHO |

> [!IMPORTANT]
> **Title đang là lỗ hổng lớn nhất**: 5/8 nguồn có `title: PDF` hoặc blank. Nếu không sửa, benchmark `gold_titles` match sẽ thất bại 100% cho dữ liệu VN.

---

## Proposed Changes

### Module Architecture

Tất cả modules mới nằm trong `services/qdrant-ingestor/etl/vn/`:

```
services/qdrant-ingestor/etl/vn/
├── __init__.py
├── vn_text_cleaner.py      # Unicode NFC, line-merge, noise strip
├── vn_title_extractor.py   # Source-specific title extraction
├── vn_metadata_enricher.py # specialty, audience, trust_tier, doc_type
├── vn_sectionizer.py       # Section split, heading_path
├── vn_specialty_dict.py    # Từ điển chuyên khoa VN→EN
└── vn_txt_to_jsonl.py      # Master converter: TXT → DocumentRecord JSONL
```

---

### [NEW] `vn/vn_text_cleaner.py`

Xử lý text thô trước khi trích xuất metadata/title.

**Chức năng:**
1. **Unicode NFC normalize** - chuẩn hóa dấu tiếng Việt tổ hợp
2. **Line merge** - nối dòng bị ngắt giữa câu (dòng trước không kết thúc `.?!:;`)
3. **Noise removal:**
   - Dòng chữ ký số: regex `\w+\.\w+_.*_\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{2}` (ví dụ: `ngoctlv.kcb_Truong Le Van Ngoc_29/10/2025 17:15:41`)
   - Số trang đơn (dòng chỉ chứa 1 số + khoảng trắng)
   - Header lặp: `TẠP CHÍ Y.*SỐ \d+`, `HUE JOURNAL OF MEDICINE AND PHARMACY ISSN`, `BÀI NGHIÊN CỨU`
   - CỘNG HÒA XÃ HỘI CHỦ NGHĨA VIỆT NAM (chỉ khi ở dòng đầu)
4. **Normalize whitespace** - collapse multiple blank lines → max 1

**Input:** raw text (sau YAML frontmatter)
**Output:** cleaned text string

---

### [NEW] `vn/vn_title_extractor.py`

Trích xuất title thật từ text body khi YAML `title` là blank/`PDF`/vô nghĩa.

**Logic theo nhóm nguồn:**

#### Nhóm A – Journal (`vmj_ojs`, `hue_jmp_ojs`, `mil_med_pharm_journal`, `cantho_med_journal`, `trad_med_pharm_journal`)

Quan sát thực tế từ sample:
- [vmj_ojs/11340_9905.txt](file:///d:/CODE/DATN/LLM-MedQA-Assistant/rag-data/data_processed/vmj_ojs/11340_9905.txt): Title thật = `"ĐẶC ĐIỂM LÂM SÀNG VÀ KIỂU GEN CỦA BIẾN THỂ rs9290927 TRÊN GEN CLDN-1 Ở NGƯỜI BỆNH VIÊM DA CƠ ĐỊA"` (dòng 90)
- [hue_jmp_ojs/140_135.txt](file:///d:/CODE/DATN/LLM-MedQA-Assistant/rag-data/data_processed/hue_jmp_ojs/140_135.txt): Title thật = `"Nghiên cứu giá trị chẩn đoán và mối liên quan với các yếu tố tiên lượng của nồng độ EBV-DNA huyết thanh trong ung thư vòm"` (dòng 13)
- [mil_med_pharm_journal/1000_558.txt](file:///d:/CODE/DATN/LLM-MedQA-Assistant/rag-data/data_processed/mil_med_pharm_journal/1000_558.txt): Title thật = `"NGHIÊN CỨU MỨC ĐỘ KHÁNG KHÁNG SINH CỦA KLEBSIELLA PNEUMONIAE..."` (dòng 13-15)
- [trad_med_pharm_journal/10_8.txt](file:///d:/CODE/DATN/LLM-MedQA-Assistant/rag-data/data_processed/trad_med_pharm_journal/10_8.txt): Title thật = `"Đặc điểm nguồn nhân lực y học cổ truyền tại các cơ sở y tế xã huyện trà cú, tỉnh Trà Vinh"` (dòng 15-16)

**Thuật toán journal title extraction:**
1. Bỏ qua: header lặp tạp chí, dòng "BÀI NGHIÊN CỨU", dòng số/ISSN, blank
2. Bỏ qua: dòng phần cuối bài trước (TÀI LIỆU THAM KHẢO, KẾT LUẬN cuối cùng) — vì một số file chứa >1 bài
3. Tìm dòng đầu tiên thỏa: `30 ≤ len(stripped) ≤ 300`, không phải tên tác giả (regex author pattern `^[A-ZÀ-Ỹa-zà-ỹ\s,]+\d$`), không phải heading chuẩn section
4. Nếu dòng tiếp theo cũng không phải tên tác giả và có vẻ tiếp nối → nối lại
5. **Fallback**: nếu không tìm được → dùng dòng đầu hợp lệ dài nhất

#### Nhóm B – Guideline (`kcb_moh`)

Quan sát: Title thật nằm trong dòng dạng `"Hướng dẫn quy trình kỹ thuật..."` hoặc `Về việc ban hành tài liệu chuyên môn "..."`.

**Thuật toán:**
1. Regex: `"([^"]+)"` trong 100 dòng đầu → trích nội dung trong dấu ngoặc kép
2. Nếu match → đó là title (ví dụ: `Hướng dẫn quy trình kỹ thuật Hô hấp - Tập 1.1`)
3. Fallback: tìm dòng chứa `QUYẾT ĐỊNH` rồi đọc dòng tiếp theo `Về việc...`

#### Nhóm C – Pharma (`dav_gov`)

Quan sát: Tên file thường mô tả nội dung (ví dụ: `1-dm-thuoc-dieu-tri-benh-hiem`).

**Thuật toán:**
1. Nếu `source_url` chứa path mô tả → humanize slug
2. Tìm dòng đầu tiên dài 20-200 ký tự, không phải header biểu mẫu (`TT\tTên hoạt chất`)
3. Fallback: humanize filename

#### Nhóm D – WHO (`who_vietnam`)

**Thuật toán:**
1. Tìm dòng đầu tiên dài ≥ 30 ký tự, lọc bỏ dòng metadata kỹ thuật
2. WHO docs thường có title rõ ràng trong text

---

### [NEW] `vn/vn_specialty_dict.py`

Từ điển keyword → specialty, dùng cho `vn_metadata_enricher.py`.

```python
SPECIALTY_KEYWORDS = {
    "cardiology": ["tăng huyết áp", "suy tim", "nhồi máu cơ tim", "rung nhĩ",
                   "van tim", "động mạch vành", "stent", "holter"],
    "respiratory": ["hen", "COPD", "viêm phổi", "lao phổi", "hô hấp",
                   "phế quản", "màng phổi", "khí phế thũng"],
    "endocrinology": ["đái tháo đường", "tuyến giáp", "insulin", "cushing"],
    "oncology": ["ung thư", "u ác tính", "hóa trị", "xạ trị", "di căn",
                "lymphoma", "carcinoma"],
    "neurology": ["đột quỵ", "động kinh", "alzheimer", "parkinson",
                 "thần kinh", "co giật"],
    "gastroenterology": ["dạ dày", "gan", "viêm gan", "đại tràng",
                        "tiêu hóa", "xơ gan"],
    "nephrology": ["thận", "chạy thận", "lọc máu", "suy thận"],
    "dermatology": ["da liễu", "viêm da", "vảy nến", "nấm da"],
    "infectious_disease": ["HIV", "nhiễm khuẩn", "kháng sinh", "vi khuẩn",
                          "sốt rét", "lao"],
    "pharmacology": ["thuốc", "dược", "liều dùng", "tương tác",
                    "chống chỉ định", "GMP", "GSP"],
    "traditional_medicine": ["y học cổ truyền", "YHCT", "châm cứu",
                            "thuốc cổ truyền", "dược liệu"],
    "obstetrics_gynecology": ["thai", "sản khoa", "phụ khoa", "tử cung"],
    "pediatrics": ["trẻ em", "nhi khoa", "sơ sinh", "nhũ nhi"],
    "surgery": ["phẫu thuật", "ngoại khoa", "nội soi"],
    "ophthalmology": ["mắt", "võng mạc", "nhãn khoa", "thủy tinh thể"],
    "hematology": ["máu", "đông máu", "hemophilia", "tiểu cầu", "bạch cầu"],
}
```

---

### [NEW] `vn/vn_metadata_enricher.py`

Suy diễn metadata còn thiếu, **đặc biệt 5 trường quan trọng nhất**.

**Logic:**

| Trường | Cách suy diễn |
|--------|--------------|
| `doc_type` | **Source mapping first**: `kcb_moh` → `guideline`, `dav_gov` → `reference`, journal → `review`. **Body fallback**: chứa `Hướng dẫn chẩn đoán và điều trị` → `guideline` |
| [specialty](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/medlineplus_scraper.py#86-96) | Dùng `vn_specialty_dict.py`: quét title+body 500 ký tự đầu, đếm matches, chọn specialty có nhiều hit nhất. Default `"general"` |
| `audience` | **Source mapping**: guideline/journal → `clinician`, WHO patient education → `patient`, textbook → `student` |
| `trust_tier` | `kcb_moh`/`who_vietnam` → `1`, journals → `2`, patient-facing → `3` |
| `language` | Phát hiện tỷ lệ ký tự tiếng Việt (dấu) vs ASCII-only. >20% dấu → `vi`, else [en](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/app/document_schema.py#27-87) |

**Input:** dict có `title`, [body](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/normalize_all.py#92-97), `source_id`, `institution`
**Output:** dict bổ sung `doc_type`, [specialty](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/medlineplus_scraper.py#86-96), `audience`, `trust_tier`, `language`

---

### [NEW] `vn/vn_sectionizer.py`

Tách một record dài thành nhiều section records, tạo `section_title` và [heading_path](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/app/ingest_utils.py#85-88).

**Heading detection regex** (case-insensitive, stripped):

```python
VN_HEADING_PATTERNS = [
    # Academic paper sections
    r"^(TÓM TẮT|ABSTRACT|SUMMARY)$",
    r"^(ĐẶT VẤN ĐỀ|GIỚI THIỆU|MỞ ĐẦU)$",
    r"^(ĐỐI TƯỢNG VÀ PHƯƠNG PHÁP.*|VẬT LIỆU VÀ PHƯƠNG PHÁP)$",
    r"^(KẾT QUẢ.*|KẾT QUẢ VÀ BÀN LUẬN)$",
    r"^(BÀN LUẬN)$",
    r"^(KẾT LUẬN|KIẾN NGHỊ)$",
    r"^(TÀI LIỆU THAM KHẢO)$",
    # Numbered sections: I., II., 1., 2., Chương 1, Mục 2
    r"^(I{1,3}V?|VI{0,3}|IX|X)\.\s+.+",
    r"^\d+\.\s+[A-ZÀ-Ỹ].{10,}",
    r"^Chương\s+\d+",
    # Drug/pharma sections
    r"^(Chỉ định|Liều dùng|Chống chỉ định|Tác dụng không mong muốn|Tương tác thuốc)$",
    # KCB procedure sections
    r"^(ĐẠI CƯƠNG|CHỈ ĐỊNH|CHỐNG CHỈ ĐỊNH|CHUẨN BỊ|CÁC BƯỚC TIẾN HÀNH|THEO DÕI|TAI BIẾN)$",
]
```

**Logic:**
1. Split text theo detected headings
2. Mỗi section tạo `heading_path = f"{title} > {section_title}"`
3. Section quá ngắn (< 50 chars) → merge vào section trước
4. Section quá dài (> 5000 chars) → giữ nguyên (để chunker xử lý sau)
5. Bỏ section `TÀI LIỆU THAM KHẢO` vì không hữu ích cho retrieval

**Input:** [(title: str, body: str)](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/universal_loader.py#334-373) 
**Output:** `list[dict]` với mỗi dict = `{"section_title": ..., "heading_path": ..., "body": ...}`

---

### [NEW] `vn/vn_txt_to_jsonl.py`

**Master converter** kết hợp tất cả modules trên.

**Pipeline cho mỗi file TXT:**
```
1. Parse YAML frontmatter → extract source_id, institution, source_url, file_url, pages, chars
2. Extract body text (after ---)
3. vn_text_cleaner.clean(body) → cleaned_body
4. vn_title_extractor.extract(source_id, cleaned_body, yaml_title) → real_title
5. vn_metadata_enricher.enrich(source_id, real_title, cleaned_body) → metadata dict
6. vn_sectionizer.sectionize(real_title, cleaned_body) → sections[]
7. For each section → emit 1 DocumentRecord as JSON line
```

**Validation gates** (skip file nếu):
- `len(cleaned_body) < 200` → body quá ngắn
- `real_title == "PDF"` hoặc len < 10 → title extraction thất bại
- `pages == 0` hoặc `chars == 0` → file rỗng

**CLI:**
```bash
python -m etl.vn.vn_txt_to_jsonl \
    --source-dir ../../rag-data/data_processed/vmj_ojs \
    --output ../../data/data_final/vmj_ojs.jsonl \
    [--dry-run] [--verbose]
```

Chạy theo batch (Lô 1 trước):
```bash
# Lô 1 — nguồn chất lượng cao
python -m etl.vn.vn_txt_to_jsonl --source-dir ../../rag-data/data_processed/kcb_moh    --output ../../data/data_final/kcb_moh.jsonl
python -m etl.vn.vn_txt_to_jsonl --source-dir ../../rag-data/data_processed/who_vietnam --output ../../data/data_final/who_vietnam.jsonl
python -m etl.vn.vn_txt_to_jsonl --source-dir ../../rag-data/data_processed/dav_gov    --output ../../data/data_final/dav_gov.jsonl

# Lô 2 — tạp chí (sau khi Lô 1 đã ổn)
python -m etl.vn.vn_txt_to_jsonl --source-dir ../../rag-data/data_processed/vmj_ojs              --output ../../data/data_final/vmj_ojs.jsonl
python -m etl.vn.vn_txt_to_jsonl --source-dir ../../rag-data/data_processed/hue_jmp_ojs           --output ../../data/data_final/hue_jmp_ojs.jsonl
python -m etl.vn.vn_txt_to_jsonl --source-dir ../../rag-data/data_processed/cantho_med_journal     --output ../../data/data_final/cantho_med_journal.jsonl
python -m etl.vn.vn_txt_to_jsonl --source-dir ../../rag-data/data_processed/mil_med_pharm_journal  --output ../../data/data_final/mil_med_pharm_journal.jsonl
python -m etl.vn.vn_txt_to_jsonl --source-dir ../../rag-data/data_processed/trad_med_pharm_journal --output ../../data/data_final/trad_med_pharm_journal.jsonl
```

---

## User Review Required

> [!IMPORTANT]
> **Về file `vmj_ojs` chứa 2+ bài báo trong 1 file**: Sample [11340_9905.txt](file:///d:/CODE/DATN/LLM-MedQA-Assistant/rag-data/data_processed/vmj_ojs/11340_9905.txt) bắt đầu từ phần "KẾT LUẬN" của bài 1, rồi chuyển sang bài 2 hoàn chỉnh. Tôi đề xuất trong v1 **chỉ trích xuất bài cuối cùng** (bài hoàn chỉnh nhất) từ mỗi file, thay vì cố split multi-article. Bạn đồng ý không?

> [!IMPORTANT]
> **Xử lý file `kcb_moh` 20k+ dòng**: Mỗi file KCB là tập hợp 97 quy trình kỹ thuật. Tôi đề xuất tách thành **nhiều DocumentRecord** — mỗi quy trình = 1 record, title = tên quy trình (ví dụ: "CHỌC DÒ DỊCH MÀNG PHỔI"). Bạn đồng ý?

> [!IMPORTANT]
> **Bỏ section "TÀI LIỆU THAM KHẢO"**: Phần references không hữu ích cho retrieval và sẽ tạo noise. Tôi đề xuất bỏ khi sectionize. Bạn đồng ý?

> [!WARNING]
> **Nguồn `who_vietnam` chủ yếu là tiếng Anh**: Từ filename pattern (WHA, EB, Appeals), phần lớn là nghị quyết WHO Assembly/Executive Board bằng tiếng Anh. Cần xác nhận: có nên vẫn nạp những file này vào corpus VN, hay chỉ lấy file thực sự bằng tiếng Việt?

---

## Verification Plan

### Automated Tests

#### Test 1: Unit test cho `vn_text_cleaner`
```bash
cd d:\CODE\DATN\LLM-MedQA-Assistant\services\qdrant-ingestor
d:\CODE\.venv\Scripts\python.exe -m pytest tests/test_vn_cleaner.py -v
```
Viết test mới `tests/test_vn_cleaner.py` kiểm tra:
- Unicode NFC normalize (dấu tổ hợp → dấu precomposed)
- Line merge (dòng bị ngắt giữa câu)
- Noise removal (header lặp, chữ ký số, số trang)

#### Test 2: Unit test cho `vn_title_extractor` 
```bash
d:\CODE\.venv\Scripts\python.exe -m pytest tests/test_vn_title.py -v
```
Viết test mới với **5 sample thật** (1 từ mỗi nhóm nguồn), assert title extracted đúng:
- `vmj_ojs` → `"ĐẶC ĐIỂM LÂM SÀNG VÀ KIỂU GEN CỦA BIẾN THỂ rs9290927..."`
- `hue_jmp_ojs` → `"Nghiên cứu giá trị chẩn đoán...EBV-DNA...ung thư vòm"`
- `mil_med_pharm_journal` → `"NGHIÊN CỨU MỨC ĐỘ KHÁNG KHÁNG SINH..."`
- `kcb_moh` → `"Hướng dẫn quy trình kỹ thuật Hô hấp - Tập 1.1"`
- `trad_med_pharm_journal` → `"Đặc điểm nguồn nhân lực y học cổ truyền..."`

#### Test 3: Integration test — end-to-end converter
```bash
d:\CODE\.venv\Scripts\python.exe -m pytest tests/test_vn_converter.py -v
```
- Chạy `vn_txt_to_jsonl` trên 3 sample files
- Assert output JSONL valid, mỗi record có `title`, [body](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/normalize_all.py#92-97), `source_name`, [specialty](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/medlineplus_scraper.py#86-96), `trust_tier`
- Assert `title != "PDF"` và `title != ""`

#### Test 4: QA checks trên output JSONL
```bash
cd d:\CODE\DATN\LLM-MedQA-Assistant\services\qdrant-ingestor
d:\CODE\.venv\Scripts\python.exe -m qa_pre_ingest.run_all_checks ../../data/data_final/kcb_moh.jsonl
```
Sử dụng [run_all_checks.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/qa_pre_ingest/run_all_checks.py) có sẵn → **target: composite score ≥ 80 (GO)** trên mỗi file JSONL VN output

### Manual Verification
- Spot-check 5 records ngẫu nhiên từ output JSONL của mỗi nguồn
- Xác nhận: title có nghĩa, specialty phù hợp, body sạch
