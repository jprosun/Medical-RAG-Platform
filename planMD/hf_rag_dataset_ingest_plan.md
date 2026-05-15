# Ke hoach ingest Hugging Face datasets cho RAG evidence-rich

## Muc tieu

Repo nay da co corpus lon, nhung mix nguon hien tai van thien ve journal article. Neu muc tieu la lam cau tra loi cuoi cung dai hon, giau kien thuc hon, va co evidence ro rang hon, thi phan can bo sung khong phai la "them that nhieu text", ma la "them dung loai tai lieu":

- guideline / recommendation de nang chat luong grounding
- reference / drug label de bo sung tra cuu thuc hanh
- research abstract chi them co chon loc de lap cho specialty gap

Theo code hien tai:

- `services/rag-orchestrator/app/prompt.py` da co `open_enriched`
- `services/rag-orchestrator/app/answer_verifier.py` da phat answer qua ngan
- `retrieval_deep_dive.md` cho thay chunk metadata duoc dung nhieu trong retrieval va evidence extraction

Nen nut that lon nhat o day la `evidence quality`, khong phai chi la `response length`.

## Ket luan nhanh

| Dataset | Vai tro | Fit voi `DocumentRecord` | Hanh dong |
|---|---|---:|---|
| `epfl-llm/guidelines` | guideline, recommendation, evidence cap cao | High | ingest truoc |
| `MedRAG/pubmed` | abstract nghien cuu de lap specialty gap | Medium | ingest co chon loc |
| `MedRAG/textbooks` | explainer, teaching-style evidence | Medium-Low | chi nen dung o phase 2 |
| `um-ids/dailymed-annotations` | drug label / indications | Medium-Low | tach thanh drug sub-index |
| `VietAI/vi_pubmed` | bilingual support / query expansion | Low | khong dua thang vao main corpus |

## Chu de nen uu tien

### 1. Authoritative guidance

Day la nhom co ROI cao nhat cho answer dai va chac:

- chan doan va xu tri theo guideline
- screening / prevention
- infection / antimicrobial use
- oncology / cardio / endocrine / respiratory guideline
- patient-facing recommendation neu muon answer than thien hon

### 2. Selective research evidence

Khong do full PubMed vao index. Chi them khi:

- specialty do dang thieu nguon manh trong corpus hien tai
- can bo sung bang chung moi hon so voi journal VN
- query type hay hoi ve ket qua nghien cuu, so sanh can thiep, prognosis

### 3. Drug reference

Tach rieng khoi main evidence corpus. Thuoc thuong can:

- chi dinh
- canh bao
- contraindication
- practical dosing context

### 4. Bilingual support

Khong nham thay the tai lieu goc. Chi dung de:

- mo rong truy van song ngu
- dictionary thuat ngu EN-VI
- tim abstract EN tu query VI

## Mapping cu the vao `DocumentRecord`

## 1. `epfl-llm/guidelines`

Nguon nay fit nhat voi repo vi dataset card da co cac field can thiet:

- `id`
- `source`
- `title`
- `url`
- `raw_text`
- `clean_text`
- `overview`

### Ly do nen ingest truoc

- dung loai tai lieu ma answer verifier va open-enriched mode can
- giau guideline thuc hanh hon corpus journal hien tai
- metadata tuong doi sach, co URL va source ro
- de map vao `doc_type=guideline`

### Cach map field

| HF field | `DocumentRecord` | Quy uoc |
|---|---|---|
| `id` | `doc_id` | `hf_guidelines:{source}:{id}` |
| `title` | `title` | dung truc tiep; neu rong thi rut tu heading dau trong `clean_text` |
| `title` | `canonical_title` | ban title da strip khoang trang / ky tu loi |
| `clean_text` | `body` | nguon body chinh |
| `url` | `source_url` | giu nguyen neu co |
| `source` | `source_id` | khong nen de 1 source_id chung; nen no thanh `hf_guidelines_who`, `hf_guidelines_nice`, `hf_guidelines_cdc`, `hf_guidelines_pubmed`, ... |
| `source` | `source_name` | map sang ten human-readable: `WHO Guidelines`, `NICE Guidelines`, `CDC Guidance`, ... |
| `id` | `article_id` | luu lai `id` goc de trace |
| inferred | `doc_type` | `guideline` |
| inferred | `audience` | mac dinh `clinician`; doi sang `patient` neu title/body chua cum nhu `patient information`, `for patients`, `what you need to know` |
| inferred | `language` | `en` |
| inferred | `trust_tier` | `1` cho `who`, `cdc`, `nice`, `cma`, `cco`, `spor`, `icrc`; `2` cho `pubmed`, `wikidoc` |
| inferred | `specialty` | dua qua specialty detector EN; fallback bang keyword tu title |
| `url` + local export | `source_file` | `hf://datasets/epfl-llm/guidelines/{source}/{id}` hoac local export row pointer |
| local export | `raw_path` | shard parquet/jsonl ban luu trong `rag-data/sources/.../raw/` |
| generated text | `processed_path` | file txt/jsonl da normalize |
| inferred | `tags` | `["hf", "guideline", source]` |

### Rule bo sung quan trong

- Dung `clean_text` lam `body`, khong dung `raw_text`.
- `overview` khong co slot rieng trong schema hien tai. Neu `overview` khac rong va khong lap lai o 800 ky tu dau, co the prepend:

```text
Overview:
<overview>

<clean_text>
```

- Khong merge thang vao source `who` hay `nice_guidance` dang san co. Nen tao source_id moi de giu provenance ro, sau do dedup cap release bang body-hash + title similarity.
- Release nen dat ten rieng: `guideline_en_v1`.

### Muc QA toi thieu

- `title` khong rong
- `body` sau clean phai > 1200 ky tu
- `source_url` co mat voi cac source co URL
- `doc_type=guideline`
- `trust_tier` duoc gan day du

### Mau record de xuat

```json
{
  "doc_id": "hf_guidelines:nice:7a73f9287841533eeb11c025026322a23d519f2c",
  "title": "Hypertension in adults: diagnosis and management",
  "canonical_title": "Hypertension in adults: diagnosis and management",
  "body": "Overview:\n...\n\n# Recommendations\n...",
  "source_name": "NICE Guidelines",
  "source_id": "hf_guidelines_nice",
  "source_url": "https://...",
  "article_id": "7a73f9287841533eeb11c025026322a23d519f2c",
  "doc_type": "guideline",
  "audience": "clinician",
  "language": "en",
  "trust_tier": 1,
  "tags": ["hf", "guideline", "nice"]
}
```

## 2. `MedRAG/pubmed`

Dataset nay co cac field ro rang:

- `id`
- `PMID`
- `title`
- `content`
- `contents`

Nhung no la abstract-level evidence, khong phai guideline.

### Khi nao nen dung

- bo sung bang chung theo specialty dang thieu
- query hay hoi ve nghien cuu, hieu qua can thiep, risk factor, prognosis
- muon co source PubMed de dan evidence ro hon

### Khi nao khong nen dung

- khong dung lam corpus chinh
- khong dump full 23M+ rows vao index
- khong nen cho journal abstract ap dao guideline source

### Cach map field

| HF field | `DocumentRecord` | Quy uoc |
|---|---|---|
| `PMID` neu co, fallback `id` | `doc_id` | `hf_pubmed:{PMID_or_id}` |
| `title` | `title` | dung truc tiep |
| `title` | `canonical_title` | clean title |
| `content` | `body` | body chinh; khong dung `contents` de tranh lap title |
| `PMID` | `article_id` | giu lai PMID |
| inferred | `source_name` | `PubMed (MedRAG)` |
| inferred | `source_id` | `hf_medrag_pubmed_selected` |
| inferred | `source_url` | `https://pubmed.ncbi.nlm.nih.gov/{PMID}/` neu co PMID |
| inferred | `doc_type` | `research_article` |
| inferred | `audience` | `clinician` |
| inferred | `language` | `en` |
| inferred | `trust_tier` | `2` |
| inferred | `tags` | `["hf", "pubmed", "abstract_only"]` + specialty |

### Rule bo sung quan trong

- Chi lay subset, vi du:
  - cardiology
  - endocrinology
  - oncology
  - infectious disease
  - critical care
- Bo loc ingest nen dua theo:
  - keyword specialty
  - abstract length
  - PMID co mat
  - loai cau hoi benchmark ma repo dang yeu
- Vi dataset card khong cho thay `published_at`, truong nay nen de rong thay vi suy doan.
- Gan them `quality_flags=["abstract_only", "no_pub_date"]` neu khong co metadata phu.

### Cach build release

- tao release rieng: `pubmed_gap_v1`
- khong merge vao `guideline_en_v1`
- ve retrieval, chi route den release nay khi query mang tinh nghien cuu / appraisal / comparative evidence

### QA gate toi thieu

- co `PMID` hoac `id`
- `body` > 600 ky tu
- `title` khong rong
- khong duplicate title-body voi corpus dang co

## 3. `MedRAG/textbooks`

Dataset nay co:

- `id`
- `title`
- `content`
- `contents`

Van de la row da chunk san thanh snippet nho, trong khi pipeline cua repo dang nghi theo `DocumentRecord` muc document ro rang hon.

### Vi sao fit kem hon guideline / pubmed

- thieu URL
- thieu chapter metadata
- thieu page / section provenance
- title thuong chi la ten sach, khong phai ten muc hay chapter
- neu ingest 1 row = 1 record thi article aggregation se rat yeu

### Cach dung phu hop hon

Chi nen dung neu ban chap nhan mot trong hai huong:

1. Tao index phu cho textbook snippets, route rieng cho query kieu giang giai / teaching.
2. Reconstruct lai chapter-level docs tu upstream source khac, roi moi dua vao `DocumentRecord`.

### Neu van muon map thang

| HF field | `DocumentRecord` | Quy uoc |
|---|---|---|
| `id` | `doc_id` | `hf_textbook_chunk:{id}` |
| `title` | `title` | ten textbook |
| `content` | `body` | snippet text |
| inferred | `source_name` | `Medical Textbooks (MedRAG)` |
| inferred | `source_id` | `hf_medrag_textbooks` |
| inferred | `doc_type` | `textbook` |
| inferred | `audience` | `student` |
| inferred | `language` | `en` |
| inferred | `trust_tier` | `2` |
| inferred | `tags` | `["hf", "textbook", "prechunked"]` |

### Khuyen nghi thuc te

- Khong ingest vao main collection o phase 1.
- Neu can answer dai kieu textbook, uu tien sau khi `guideline_en_v1` on dinh.

## 4. `um-ids/dailymed-annotations`

Nguon nay co gia tri cho thuoc, nhung viewer hien tai bao loi schema khong dong nhat. Preview cho thay it nhat hai dang row:

- dang co `set_id`, `xml_id`, `version_number`, `indication_cleaned`, `length`
- dang co field `text`

### He qua doi voi repo

- phai co pre-normalization step truoc khi vao `DocumentRecord`
- khong nen xem day la dataset "drop-in"

### Cach map neu chon dung

| HF field | `DocumentRecord` | Quy uoc |
|---|---|---|
| `set_id` + `xml_id` | `doc_id` | `hf_dailymed:{set_id}:{xml_id}` |
| inferred tu text dau | `title` | rut ten thuoc tu dong dau / cum truoc `is indicated` |
| `indication_cleaned` fallback `text` | `body` | body chinh |
| `set_id` | `article_id` | luu lai label set id |
| inferred | `source_name` | `DailyMed` |
| inferred | `source_id` | `hf_dailymed_indications` |
| inferred | `source_url` | `https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={set_id}` |
| inferred | `doc_type` | `reference` |
| inferred | `audience` | `clinician` |
| inferred | `language` | `en` |
| inferred | `trust_tier` | `1` |
| inferred | `tags` | `["hf", "drug", "dailymed"]` |

### Rule bo sung quan trong

- Luon gan `quality_flags=["schema_mixed", "title_inferred"]` neu title phai suy ra.
- Tach thanh release rieng: `drug_reference_en_v1`.
- Chi route khi query xoay quanh thuoc, chi dinh, label, caution.

## 5. `VietAI/vi_pubmed`

Dataset viewer hien tai cho thay moi hai cot:

- `en`
- `vi`

No rat lon, nhung metadata qua ngheo de dua thang vao RAG main corpus.

### Vi sao khong fit voi schema hien tai

- khong thay `title`
- khong thay `PMID`
- khong thay `source_url`
- khong thay specialty / publication metadata
- kha nang cao la abstract dich, phu hop cho bilingual support hon la provenance-heavy evidence

### Cach dung dung

Khong ingest vao `DocumentRecord` main collection. Nen dung cho:

- query expansion EN-VI
- synonym / terminology mining
- dich truy van VI sang medical English
- bilingual rerank support

### San pham nen tao tu no

- `rag-data/aux/vi_pubmed_terms.tsv`
- `rag-data/aux/vi_pubmed_query_pairs.jsonl`

Neu ep map thang vao `DocumentRecord`, record se rat ngheo metadata va kho tao citation sach.

## Thu tu ingest de xuat

1. Hoan thien nguon noi bo con thieu truoc:
   - `nice_guidance`
   - `vaac_hiv_aids`
   - `vncdc_documents`
2. Ingest `epfl-llm/guidelines` thanh `guideline_en_v1`.
3. Ingest `MedRAG/pubmed` co chon loc thanh `pubmed_gap_v1`.
4. Neu can support thuoc, ingest `um-ids/dailymed-annotations` thanh `drug_reference_en_v1`.
5. Giu `VietAI/vi_pubmed` cho nhom query expansion, khong cho vao main release.
6. Chi can nhac `MedRAG/textbooks` sau khi da co routing / index rieng cho teaching-style answers.

## Goi y source_id / dataset_id

| Muc dich | Source/Dataset ID de xuat |
|---|---|
| guideline HF | `hf_guidelines_*` va release `guideline_en_v1` |
| PubMed bo loc | `hf_medrag_pubmed_selected` va release `pubmed_gap_v1` |
| drug labels | `hf_dailymed_indications` va release `drug_reference_en_v1` |
| bilingual aux | `vi_pubmed_aux_v1` |

## Ranh gioi quan trong

### Dataset dung cho RAG corpus

- `epfl-llm/guidelines`
- `MedRAG/pubmed` subset
- `um-ids/dailymed-annotations` sau clean schema

### Dataset dung cho eval / benchmark, khong dua vao retrieval corpus

- `lavita/MedQuAD`
- `II-Vietnam/Medical-VN-Benchmark`
- `aisc-team-d2/healthsearchqa`

Ly do:

- day la QA / benchmark data, khong phai tai lieu goc
- neu ingest thang vao RAG, retriever de lay "dap an san" thay vi evidence document

## Ghi chu ngoai pham vi dataset nhung anh huong lon den answer quality

Ngay ca khi ingest dung dataset, answer cuoi cung van co the chua toi uu neu ranking khong uu tien evidence manh. Trong repo hien tai:

- metadata co `trust_tier`
- nhung article ranking chua dung `trust_tier` nhu mot feature rank chinh

Nen sau khi co `guideline_en_v1`, buoc ky thuat co ROI cao tiep theo la:

- uu tien guideline / trust_tier=1 trong article aggregation
- route `guideline_comparison` / `professional_explainer` vao collection phu hop

## Cong viec ETL nen lam tiep

Neu muon bien plan nay thanh code, thu tu hop ly la:

1. Tao `pipelines/etl/hf_guidelines_to_jsonl.py`
2. Tao `pipelines/etl/hf_pubmed_selected_to_jsonl.py`
3. Tao `tools/build_guideline_en_v1.py` hoac dung lai `tools/build_dataset_release.py`
4. Them test schema cho hai nguon moi trong `services/qdrant-ingestor/tests/`

## Nguon tham khao

- `epfl-llm/guidelines`: https://huggingface.co/datasets/epfl-llm/guidelines
- `MedRAG/pubmed`: https://huggingface.co/datasets/MedRAG/pubmed
- `MedRAG/textbooks`: https://huggingface.co/datasets/MedRAG/textbooks
- `um-ids/dailymed-annotations`: https://huggingface.co/datasets/um-ids/dailymed-annotations
- `VietAI/vi_pubmed`: https://huggingface.co/datasets/VietAI/vi_pubmed
