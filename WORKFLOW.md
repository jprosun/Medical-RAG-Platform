# Step 1: Crawl Data — Chi tiết

## Tổng quan

| Metric | Giá trị |
|||
| Seed sources định nghĩa | **19** nguồn |
| Đã crawl | **8 / 19** VN + **3** EN = **11 nguồn** |
| Chưa crawl | **11** nguồn |
| Tổng files trên disk | **5,810** files (VN) + **152** files (EN) |
| Tổng dung lượng raw | **5.45 GB** (VN) + **46 MB** (EN) ≈ **5.5 GB** |



## Vietnamese Sources (đã crawl)

| # | Source | Loại | Manifest | Downloaded | Files trên disk | Size (MB) | Định dạng |
|||||||||
| 1 | **cantho_med_journal** | OJS Journal | 2,135 | 2,135 | 2,135 | **1,590.9** | .pdf (2,135) |
| 2 | **vmj_ojs** | OJS Journal | 1,475 | 1,337 | 1,343 | **1,477.2** | .pdf (1,343) |
| 3 | **dav_gov** | PDF Site | 541 | 533 | 533 | **994.7** | .pdf (461), .docx (45), .xlsx (19), .doc (8) |
| 4 | **trad_med_pharm_journal** | OJS Journal | 328 | 328 | 330 | **544.4** | .pdf (326), .docx (3), .html (1) |
| 5 | **mil_med_pharm_journal** | OJS Journal | 1,300 | 1,300 | 891 | **479.6** | .pdf (890), .html (1) |
| 6 | **who_vietnam** | PDF Site | 189 | 182 | 181 | **220.8** | .pdf (167), .html (8), .xlsx (6) |
| 7 | **hue_jmp_ojs** | OJS Journal | 370 | 362 | 362 | **178.8** | .pdf (362) |
| 8 | **kcb_moh** | PDF Site | 42 | 35 | 35 | **95.5** | .pdf (26), .docx (6), .doc (2), .xlsx (1) |
| | **TỔNG** | | **6,380** | **6,212** | **5,810** | **5,581.8** | |

> [!NOTE]
> **Gap analysis:**
> - `mil_med_pharm_journal`: 1,300 downloaded nhưng chỉ 891 files trên disk (409 files thiếu — có thể là lỗi download hoặc đã filter)
> - `vmj_ojs`: 1,337 downloaded nhưng có 1,343 files trên disk (6 files dư — có thể là file phụ hoặc retry)

### File type breakdown

| Extension | Files | % |
||||
| `.pdf` | 5,710 | 98.3% |
| `.docx` | 54 | 0.9% |
| `.xlsx` | 26 | 0.4% |
| `.doc` | 10 | 0.2% |
| `.html` | 10 | 0.2% |



## English Sources (đã scrape)

| # | Source | Files | Size (MB) | Phương pháp |
||||||
| 1 | **medlineplus** | 1 (bundled) | 28.6 | HTML scrape → JSONL |
| 2 | **ncbi_bookshelf** | 100 chapters | 11.9 | HTML scrape → JSONL |
| 3 | **who** | 51 topics | 5.6 | HTML scrape → JSONL |
| | **TỔNG** | **152** | **46.1** | |



## Sources CHƯA crawl (11/19)

| # | Source ID | Tên | Loại | Ghi chú |
||||||
| 1 | `hmu_dspace_home` | Kho dữ liệu số ĐH Y Hà Nội | DSpace | Luận án, luận văn, ebook |
| 2 | `nihe_uploads` | Viện VSDT Trung ương (NIHE) | PDF Site | Luận án, dịch tễ |
| 3 | `moh_ydct` | Bộ Y tế - Y học cổ truyền | PDF Site | Hướng dẫn, quy trình |
| 4 | `vncdc_documents` | VNCDC | PDF Site | Y tế dự phòng |
| 5 | `hue_library_opac` | Thư viện ĐH Y Dược Huế | Library Catalog | Metadata only |
| 6 | `ctump_library` | Thư viện ĐH Y Dược Cần Thơ | Library Catalog | Metadata + tài liệu online |
| 7 | `tbump_library` | Thư viện ĐH Y Dược Thái Bình | Library Catalog | Metadata + tài liệu số |
| 8 | `bv103_pdfs` | Bệnh viện Quân y 103 | PDF Site | Bài giảng, cập nhật điều trị |
| 9 | `ump_uploads` | ĐH Y Dược TP.HCM | PDF Site | Học thuật, bài báo, luận án |
| 10 | `tphcm_med_journal` | Tạp chí Y học TP.HCM | OJS Journal | Bài báo khoa học |
| 11 | `hmu_med_research_journal` | Tạp chí Nghiên cứu Y học (HMU) | OJS Journal | Bài báo khoa học |

> [!WARNING]
> **11 nguồn chưa crawl** chiếm phần lớn các nguồn seed. Trong đó:
> - 3 nguồn **Library Catalog** (Huế, Cần Thơ, Thái Bình) chủ yếu để discovery metadata, không có fulltext
> - 5 nguồn **PDF Site** và **2 nguồn OJS Journal** có tiềm năng yielded thêm hàng nghìn PDF



## Crawlers có sẵn

| Script | Dùng cho | Config |
||||
| [crawl_ojs.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/rag-data/medical_crawl_seed/crawl_ojs.py) | OJS Journals (archive → issue → article → galley PDF) | `--source-id`, `--max-issues` |
| [crawl_dspace.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/rag-data/medical_crawl_seed/crawl_dspace.py) | DSpace Repos (collection → item → bitstream) | `--source-id`, `--max-pages` |
| [crawl_pdf_site.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/rag-data/medical_crawl_seed/crawl_pdf_site.py) | Generic sitewide PDF discovery | `--source-id`, `--max-pages` |
| [crawler_common.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/rag-data/medical_crawl_seed/crawler_common.py) | Shared utilities (download, dedup, manifest) | — |

### Output format của mỗi crawler

data_raw/{source_id}/
├── manifest.csv        ← metadata: source_id, item_url, file_url, title, sha256, status
└── files/
    ├── file1.pdf
    ├── file2.pdf
    └── ...


## Step2 : Tạo corpus catalog 

### Tất cả 8 manifest có CÙNG schema (14 cột):

source_id, item_url, file_url, title, item_type, institution, 
year, doc_type, access, mime_type, local_path, sha256, status, note


Vì dùng chung `crawler_common.py` nên mọi manifest đều đồng nhất — việc gộp chỉ là **union + transform**, không cần xử lý schema khác nhau.

### Field Mapping: 14 cột manifest → 11 cột catalog

| Manifest (input) | Catalog (output) | Logic |
||||
| `source_id` | `source_id` | Giữ nguyên |
| `institution` | `institution_or_journal` | **Rename** |
| `local_path` | `file_name` | **Derive**: `os.path.basename()` |
| *(constructed)* | `relative_path` | **Tạo mới**: `medical_crawl_seed/data_raw/{source}/files/{name}` |
| *(derived)* | `extension` | **Tạo mới**: `.pdf`, `.docx`... |
| *(stat from disk)* | `file_size_kb` | **Tạo mới**: đọc size thực tế trên disk |
| `title` | `title` | Giữ nguyên |
| `item_type` | `item_type` | Giữ nguyên |
| `item_url` | `item_url` | Giữ nguyên (URL trang gốc) |
| `file_url` | `file_url` | Giữ nguyên (URL download) |
| `sha256` | `sha256` | Giữ nguyên |
| `status` | ❌ **Bỏ** | Dùng để filter (`== "downloaded"`) |
| `year, doc_type, access` | ❌ **Bỏ** | Không đưa vào catalog |
| `mime_type, note` | ❌ **Bỏ** | Không đưa vào catalog |

### 3 bộ lọc đảm bảo chất lượng

| Filter | Loại gì | Tại sao |
||||
| `status != "downloaded"` | File chưa tải xong | Tránh file bị lỗi giữa chừng |
| File không tồn tại trên disk | Manifest ghi nhưng file mất | Tránh broken reference |
| `file_size < 10KB` | File junk | HTML error pages, file rỗng do server trả 404 |

### Đảm bảo dữ liệu gốc không bị mất

> **Manifest gốc không bị sửa đổi.** Script chỉ **đọc** manifest + **tạo ra** file catalog mới. Cấu trúc gốc vẫn nguyên:


data_raw/
├── cantho_med_journal/
│   ├── manifest.csv          ← GIỮ NGUYÊN (14 cột, toàn bộ rows)
│   └── files/                ← GIỮ NGUYÊN
├── vmj_ojs/
│   ├── manifest.csv          ← GIỮ NGUYÊN
│   └── files/                ← GIỮ NGUYÊN
└── ...

corpus_catalog.csv  ←── FILE MỚI ĐƯỢC TẠO RA (chỉ 5,796 rows hợp lệ)


### Metadata cuối cùng (11 trường)

Ví dụ 1 row trong `corpus_catalog.csv`:

csv
source_id:              cantho_med_journal
institution_or_journal: Tạp chí Y Dược học Cần Thơ
file_name:              1000_852.pdf
relative_path:          medical_crawl_seed/data_raw/cantho_med_journal/files/1000_852.pdf
extension:              .pdf
file_size_kb:           1066
title:                  PDF
item_type:              journal_pdf
item_url:               https://tapchi.ctump.edu.vn/index.php/ctump/issue/view/18
file_url:               https://tapchi.ctump.edu.vn/.../download/1000/852
sha256:                 11cb3a88ffacf6e6...


> **Nhận xét:** Trường `title` hầu hết chỉ có giá trị `"PDF"` hoặc rỗng — đây là hạn chế từ crawler (OJS galley thường không có title riêng). Title thực sự sẽ được trích xuất ở **Step 4** (VN title extractor) khi phân tích nội dung PDF.

## Step 3:  Phân loại PDF

Đúng, tại bước xử lý dữ liệu thô thì có **2 nhánh riêng biệt** cho dữ liệu tiếng Việt (và thêm 1 nhánh cho EN):


                    Raw Data (5,810 files VN + 152 EN)
                              │
              ┌───────────────┼───────────────────┐
              │               │                   │
         Nhánh 1         Nhánh 2             Nhánh 3
      PDF Pipeline      VN OJS ETL        EN Scrapers
              │               │                   │
      7 nguồn (4,459)   vmj_ojs (1,337)    3 nguồn (152)
              │               │                   │
   classify_pdfs.py    vmj_issue_splitter    HTML → JSONL
         │             vn_title_extractor     (trực tiếp)
   extract_digital     vn_text_cleaner            │
         │             vn_sectionizer             │
      .txt files       vn_metadata_enricher       │
         │             vn_quality_scorer          │
         │             vn_dedup                   │
         │                   │                    │
         ▼                   ▼                    ▼
   cần qua thêm        enriched JSONL       enriched JSONL
   VN ETL modules    (vmj_ojs.jsonl 84MB)  (medlineplus, who,
   để thành JSONL                            ncbi_bookshelf)


### Tóm tắt 2 nhánh chính (VN)

| | Nhánh 1: PDF Pipeline | Nhánh 2: VN OJS ETL |
||||
| **Sources** | cantho, dav_gov, hue_jmp, kcb_moh, mil_med, trad_med, who_vn | vmj_ojs |
| **Files** | 4,459 | 1,337 |
| **Bước 3** | ✅ classify_pdfs.py | ❌ Không cần (biết trước là PDF digital) |
| **Bước 4** | extract_digital_pdf → .txt | vmj_issue_splitter → title extract → clean → enrich |
| **Output** | `.txt` files (cần xử lý tiếp) | Thẳng ra enriched JSONL (sẵn sàng) |
| **Trạng thái** | Đã extract → đã có JSONL riêng lẻ trong `data_final/` | ✅ Hoàn thành (`vmj_ojs.jsonl` 84MB) |

**Nhánh 2 (vmj_ojs) bỏ qua Step 3** vì đây là bài báo từ OJS journal — crawler đã biết chắc toàn bộ là PDF digital, không cần classify lại. Thay vào đó nó đi thẳng vào pipeline chuyên biệt hơn: tách issue → trích title → clean → sectionize → enrich metadata.

# step 4 : Extract 
Edited WORKFLOW.md

Let me read the key VN ETL modules to give you a complete picture.

Viewed vn_txt_to_jsonl.py:1-329
Viewed vmj_issue_splitter.py:1-248
Viewed vn_text_cleaner.py:1-149
Viewed vn_sectionizer.py:1-513
Viewed vn_metadata_enricher.py:1-170
Viewed vn_quality_scorer.py:1-228

Giờ tôi có đầy đủ thông tin. Đây là giải thích chi tiết Step 4:



## Step 4: Trích xuất văn bản — 3 nhánh xử lý

### Tổng quan pipeline


                    Raw Files
                       │
         ┌─────────────┼──────────────┐
         │             │              │
    NHÁNH 1        NHÁNH 2       NHÁNH 3
  PDF Pipeline   VN OJS ETL    EN Scrapers
  (7 nguồn VN)   (vmj_ojs)    (3 nguồn EN)
         │             │              │
    6 bước         7 bước       1 bước
         │             │              │
         ▼             ▼              ▼
   enriched JSONL  enriched JSONL  enriched JSONL




### NHÁNH 1: PDF Pipeline (7 nguồn VN)

**6 bước tuần tự:**

| # | Module | Làm gì | Đảm bảo chất lượng |
|||||
| 1 | **extract_digital_pdf.py** | PyMuPDF extract text từ PDF → `.txt` + YAML header | Bỏ page numbers, normalize whitespace |
| 2 | **vn_text_cleaner.py** | Clean text VN chuyên sâu | Unicode NFC, merge dòng bị ngắt, bỏ noise |
| 3 | **vn_title_extractor.py** | Trích tiêu đề từ nội dung | Gate: skip nếu title < 10 chars hoặc = "PDF" |
| 4 | **vn_metadata_enricher.py** | Suy luận doc_type, specialty, audience, language | Source-based defaults + rule-based detection |
| 5 | **vn_sectionizer.py** | Tách thành sections theo source mode | 5 modes khác nhau theo nguồn |
| 6 | **vn_quality_scorer.py** | Chấm điểm 0-100, quyết định go/review/hold | 6 tiêu chí, semantic penalties |

**Orchestrator**: [vn_txt_to_jsonl.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_txt_to_jsonl.py) — gọi 6 bước trên cho từng file `.txt`



### NHÁNH 2: VMJ OJS ETL (vmj_ojs — nguồn lớn nhất)

Có **bước đặc biệt** do VMJ là tạp chí gộp nhiều bài trong 1 PDF:

| # | Module | Làm gì |
||||
| **0** | **extract_digital_pdf.py** | PDF → `.txt` (giống Nhánh 1) |
| **1** | **vmj_issue_splitter.py** | Tách 1 file issue → nhiều file article |
| **2-6** | **vn_txt_to_jsonl.py** pipeline | Clean → title → enrich → sectionize → score |

**VMJ Issue Splitter** hoạt động thế nào:
- Dùng **mỏ neo "TÓM TẮT"** (abstract) để tìm ranh giới bài
- **Look-back 15 dòng** từ mỏ neo để tìm tiêu đề (ALL-CAPS) + tác giả
- **Scoring**: TÓM TẮT (3đ) + Author (2đ) + Title (2đ) = 7đ → chỉ chấp nhận ≥ 5đ
- Output: `data_intermediate/vmj_ojs_split_articles/{issue}__art_001.txt`



### NHÁNH 3: EN Scrapers (3 nguồn EN)

Scrape trực tiếp HTML → enriched JSONL (không qua PDF):
- `medlineplus_scraper.py` → parse topic pages
- `who_scraper.py` → parse fact sheets
- `ncbi_bookshelf_scraper.py` → parse book chapters



## Chi tiết từng module chất lượng

### 1. Text Cleaner — Bỏ gì?

| Loại noise | Regex/Logic | Ví dụ bỏ |
||||
| Chữ ký số | `ngoctlv.kcb_Truong Le Van Ngoc_29/10/2025 17:15:41` | Chữ ký BYT |
| Page numbers | `^\d{1,5}$` | Số trang đơn lẻ |
| Journal headers | `TẠP CHÍ Y... SỐ... TẬP...` | Header lặp lại |
| Gov headers | `CỘNG HÒA XÃ HỘI CHỦ NGHĨA...` | Quốc hiệu |
| **Line merge** | Nếu dòng trước không kết thúc `.?!;:` → nối | Fix dòng bị ngắt từ PDF |

### 2. Sectionizer — 5 modes theo nguồn

| Mode | Nguồn | Cách tách | Logic |
|||||
| **A. Publication** | who_vietnam | 1 doc = 1 record, split nếu > 6000 chars | Paragraph boundary |
| **B. Article** | 5 tạp chí (vmj, hue, cantho, mil, trad) | Tách theo major headings | TÓM TẮT → ĐẶT VẤN ĐỀ → KẾT QUẢ → BÀN LUẬN → KẾT LUẬN |
| **C. Procedure** | kcb_moh | 1 quy trình = 1 record | Detect "N. TÊN QUY TRÌNH" + ≥2 anchor headings |
| **D. Table Entry** | dav_gov | 1 entry bảng = 1 record | Parse drug/entry table rows |
| **E. Generic** | fallback | Giữ nguyên, split nếu > 8000 chars | Paragraph boundary |

### 3. Quality Scorer — Thang điểm 100

| Tiêu chí | Điểm tối đa | Ví dụ trừ điểm |
||||
| **Title Quality** | 25 | -10 nếu < 20 chars, -20 nếu title = reference/table header |
| **Metadata** | 20 | -3 per missing field (doc_type, specialty, source_url...) |
| **Body Cleanliness** | 20 | -10 nếu < 500 chars, -5 nếu > 30% short lines |
| **Sectionization** | 15 | -5 nếu 1 section, -10 nếu quá nhiều sections/quá ngắn |
| **Source Trust** | 10 | Tier 1 = 10đ, Tier 2 = 7đ, Tier 3 = 4đ |
| **Language Confidence** | 10 | Trừ 3 nếu mixed language |

| Score | Status | Ý nghĩa |
||||
| ≥ 85 | **"go"** ✅ | Sẵn sàng ingest |
| 70-84 | **"review"** ⚠️ | Cần review thủ công |
| < 70 | **"hold"** ❌ | Chưa đủ chất lượng |

### 4. Validation Gates (cửa chặn)

`vn_txt_to_jsonl.py` có **2 gate cứng** trước khi output:

python
# Gate 1: Body quá ngắn → SKIP
if len(cleaned_body) < 200:
    return []

# Gate 2: Title không hợp lệ → SKIP  
if not title or len(title) < 10 or title in {"pdf", "document"}:
    return []


→ File bị skip sẽ được đếm vào `skipped_files` trong summary.


## Output cuối cùng — enriched JSONL

Mỗi record có **~18 fields**:

json
{
  "doc_id": "a1b2c3d4e5f6g7h8",
  "title": "ĐÁNH GIÁ KẾT QUẢ PHẪU THUẬT NỘI SOI...",
  "body": "Đặt vấn đề: Phẫu thuật nội soi...",
  "source_name": "Tạp chí Y học Việt Nam",
  "section_title": "KẾT QUẢ",
  "source_url": "https://tapchiyhocvietnam.vn/...",
  "doc_type": "review",
  "specialty": "surgery",
  "audience": "clinician",
  "language": "vi",
  "trust_tier": 2,
  "heading_path": "ĐÁNH GIÁ KẾT QUẢ... > KẾT QUẢ",
  "quality_score": 87,
  "quality_status": "go",
  "quality_flags": ["title_extracted"]
}

# step 5 : Normalize & Merge
Edited WORKFLOW.md

Let me analyze the actual output data to give you concrete quality stats.

Edited tmp_step5.py
Ran command: `python -X utf8 tmp_step5.py`
Ran command: `del tmp_step5.py`

Giờ tôi có full data. Đây là Step 5 chi tiết:



## Step 5: Normalize & Merge

### Mục đích

Thu gom tất cả JSONL riêng lẻ từ nhiều nguồn → **1 file combined.jsonl duy nhất** đã qua validate, dedup, và kiểm tra metadata — sẵn sàng cho QA gate (Step 6) và ingest (Step 7).

### Phương pháp — 4 giai đoạn

Script: [normalize_all.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/normalize_all.py)


  Giai đoạn 1          Giai đoạn 2          Giai đoạn 3         Giai đoạn 4
  ──────────          ──────────          ──────────         ──────────
  Scrape EN     →     Validate      →     Dedup + Merge  →   Statistics
  (nếu cần)           Schema               combined.jsonl     Report


**Giai đoạn 1 — Scrape EN sources** (bỏ qua nếu `--skip-scrape`):
- Chạy 3 scrapers: MedlinePlus, WHO, NCBI Bookshelf
- Output: `medlineplus.jsonl`, `who.jsonl`, `ncbi_bookshelf.jsonl`

**Giai đoạn 2 — Validate schema**:
- Mỗi record phải match `DocumentRecord` schema (doc_id, title, body, source_name bắt buộc)
- doc_type phải thuộc enum: `guideline | textbook | faq | patient_education | review | reference`
- audience phải thuộc: `patient | student | clinician`
- trust_tier phải là 1, 2, hoặc 3

**Giai đoạn 3 — Dedup + Merge**:

| Bước | Logic | Chi tiết |
||||
| **Within-source dedup** | Body hash (MD5, 500 chars đầu) | Nếu trùng → giữ record có metadata tốt hơn |
| **Cross-source dedup** | Chỉ loại exact `(doc_id, source_name)` pairs | Khác nguồn cùng topic → GIỮ CẢ HAI |
| **Metadata preservation test** | Kiểm tra title/URL quality | Cảnh báo nếu title generic/empty |
| **Write output** | Ghi `combined.jsonl` | 1 record/line |

**Giai đoạn 4 — Statistics**: In specialty distribution, trust tier, quality stats.

### Kiểm soát chất lượng

`normalize_all.py` có **3 tầng kiểm soát** ngay trong quá trình merge:

| Tầng | Kiểm tra | Action |
||||
| **Schema validation** | doc_id, title, body required; enum values | Skip invalid records |
| **Dedup** | Within-source body hash, cross-source key match | Remove duplicates, keep best |
| **Metadata preservation test** | Title generic? Missing URL? Missing section? | Print WARNING, không block |



### Đầu ra thực tế

#### combined.jsonl hiện tại: **CHỈ CÓ 3 nguồn EN**

| Source | Records | Doc Type | Trust Tier |
|||||
| NCBI Bookshelf | 1,066 | textbook | Tier 2 |
| WHO | 423 | guideline | Tier 1 |
| MedlinePlus | 199 | patient_education | Tier 3 |
| **TOTAL** | **1,688** | | |

> ⚠️ **VẤN ĐỀ LỚN:** `combined.jsonl` hiện chỉ merge 3 EN sources (1,688 records, 3.9 MB). Các nguồn VN (vmj_ojs 14,384 records, kcb_moh 1,848 records, v.v.) **chưa được merge vào combined**.

#### Toàn bộ JSONL files trong data_final — overview:

| File | Records | Size | Trạng thái |
|||||
| **vmj_ojs.jsonl** | 14,384 | 79.9 MB | ✅ Production |
| vmj_ojs_v2.jsonl | 14,471 | 81.9 MB | Phiên bản mới |
| vmj_ojs_patched.jsonl | 14,384 | 82.2 MB | Patched titles |
| kcb_moh.jsonl | 1,848 | 5.3 MB | ✅ Production |
| **combined.jsonl** | 1,688 | 3.9 MB | ✅ EN only |
| ncbi_bookshelf.jsonl | 1,066 | 2.7 MB | Included in combined |
| who_vietnam.jsonl | 475 | 2.2 MB | ❌ Chưa merge |
| who.jsonl | 423 | 0.7 MB | Included in combined |
| trad_med_pharm_journal.jsonl | 263 | 0.8 MB | ❌ Chưa merge |
| mil_med_pharm_journal.jsonl | 248 | 0.7 MB | ❌ Chưa merge |
| medlineplus.jsonl | 200 | 0.5 MB | Included in combined |
| hue_jmp_ojs.jsonl | 135 | 1.0 MB | ❌ Chưa merge |
| cantho_med_journal.jsonl | 117 | 0.7 MB | ❌ Chưa merge |
| dav_gov.jsonl | 24 | 1.4 MB | ❌ Chưa merge |
| + 5 pilot files | ~3,445 | ~9.6 MB | Pilot/test |



### Chất lượng đầu ra thực tế

#### combined.jsonl (EN) — Chất lượng rất tốt:

| Metric | Giá trị | Đánh giá |
||||
| Missing title | 0 / 1,688 (0%) | ✅ Hoàn hảo |
| Bad title | 0 / 1,688 (0%) | ✅ Hoàn hảo |
| Missing source_url | 0 / 1,688 (0%) | ✅ Hoàn hảo |
| Missing body | 0 / 1,688 (0%) | ✅ Hoàn hảo |
| Short body (<200ch) | 88 / 1,688 (5.2%) | ⚠️ Acceptable |
| Body median length | 1,118 chars | ✅ Tốt |
| Language | EN 95.1%, ES 4.9% | ⚠️ 83 records tiếng Tây Ban Nha |

#### vmj_ojs.jsonl (VN) — Nguồn lớn nhất, chất lượng cao:

| Metric | Giá trị | Đánh giá |
||||
| Quality Score avg | **94.3 / 100** | ✅ Rất tốt |
| **go** (≥85) | 13,493 (93.8%) | ✅ |
| **review** (70-84) | 830 (5.8%) | ⚠️ |
| **hold** (<70) | 61 (0.4%) | ✅ Rất ít |
| Title extracted OK | 13,811 / 14,384 (96.0%) | ✅ |
| Body too short | 1,189 (8.3%) | ⚠️ Acceptable |
| Title looks like reference | 122 (0.8%) | ⚠️ Cần review |
| Title looks like table header | 203 (1.4%) | ⚠️ Cần review |

**Top specialties (VMJ):** gastroenterology (2,386), oncology (2,010), surgery (1,496), hematology (1,127), general (1,030)...



### Tóm tắt vấn đề Step 5

| # | Vấn đề | Impact |
||||
| 1 | **combined.jsonl chỉ có EN** (1,688 records) | Thiếu >17,000 records VN |
| 2 | **Chưa merge VN sources** vào combined | vmj_ojs, kcb_moh, who_vn... đứng riêng |
| 3 | **83 records tiếng Tây Ban Nha** lẫn trong combined | WHO scraper crawl nhầm |
| 4 | **Nhiều file backup/pilot** trong data_final | Gây nhầm lẫn (3 bản vmj_ojs) |

> Nếu merge tất cả VN + EN vào combined, tổng sẽ là khoảng **~19,000+ records** thay vì chỉ 1,688.
Câu hỏi rất thực tế. Nhìn vào thực trạng dự án:

## Thực tế flow hiện tại


VMJ (14,384 records)  ──→  vmj_ojs.jsonl  ──→  ingest_staging.py  ──→  Qdrant  ✅ Đã xong
KCB (1,848 records)   ──→  kcb_moh.jsonl  ──→  ingest riêng?
Các VN khác           ──→  {source}.jsonl  ──→  ?

EN (1,688 records)    ──→  combined.jsonl  ──→  ingest_staging.py  ──→  Qdrant


**VMJ bỏ qua hoàn toàn Step 5** — đi thẳng từ enriched JSONL → ingest. Và nó vẫn hoạt động tốt.

## Bước này có cần thiết không?

**Câu trả lời ngắn: Không bắt buộc.** Mỗi source có thể ingest riêng lẻ.

Nhưng nó **có giá trị** ở 3 điểm:

| Giá trị | Khi ingest riêng lẻ | Khi merge qua Step 5 |
||||
| **Cross-source dedup** | ❌ Không kiểm tra trùng giữa sources | ✅ Phát hiện nếu 2 source có cùng bài |
| **Metadata consistency** | Mỗi source tự validate riêng | ✅ Validate đồng nhất schema trước khi ingest |
| **Quản lý đơn giản** | Nhiều lệnh ingest, nhiều file | ✅ 1 file → 1 lệnh ingest |

Tuy nhiên, **với dự án này cụ thể:**
- Các nguồn rất khác nhau (tạp chí, hướng dẫn BYT, WHO…) → **ít khả năng trùng lặp cross-source**
- Mỗi source đã có quality scorer riêng (Step 4)
- VMJ chiếm **75%+ tổng dữ liệu** và đã ingest rồi

## Kết luận

| Tình huống | Cần Step 5? |
|||
| Chỉ ingest 1 source tại 1 thời điểm | **Không cần** — ingest thẳng từ `{source}.jsonl` |
| Muốn gộp tất cả sources vào Qdrant cùng lúc | **Nên có** — đảm bảo không trùng, schema đồng nhất |
| Cần báo cáo cross-source stats cho luận văn | **Nên có** — normalize_all.py in metadata preservation report |

Với project hiện tại, **bước này đang đóng vai trò merge EN sources** (MedlinePlus + WHO + NCBI → combined.jsonl). Các VN sources đều đi thẳng. Bạn có thể giữ nó như optional step hoặc bỏ qua — không ảnh hưởng chất lượng cuối cùng.


