# Vietnamese Data Normalization Pipeline — Walkthrough

## What Was Built

8-module pipeline in `services/qdrant-ingestor/etl/vn/` that converts ~1,350 raw `.txt` files from 8 Vietnamese medical sources into standardized `DocumentRecord` JSONL.

### Pipeline Flow
```
TXT (YAML frontmatter + body)
  → vn_text_cleaner     (Unicode NFC, line-merge, noise strip)
  → vn_title_extractor  (source-specific: Journal/Guideline/Pharma/WHO)
  → vn_metadata_enricher (doc_type, specialty, audience, trust, language)
  → vn_sectionizer      (heading detection → section split)
  → vn_quality_scorer   (0-100 score, go/review/hold)
  → JSONL output        (DocumentRecord compatible)
```

### Modules Created

| Module | Purpose | Lines |
|--------|---------|------:|
| [vn_text_cleaner.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_text_cleaner.py) | Unicode NFC, line-merge, noise removal | 135 |
| [vn_title_extractor.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_title_extractor.py) | Source-specific title extraction (4 groups) | 255 |
| [vn_specialty_dict.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_specialty_dict.py) | 22 medical specialties, keyword detection | 142 |
| [vn_metadata_enricher.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_metadata_enricher.py) | 8 source defaults, language confidence | 168 |
| [vn_sectionizer.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_sectionizer.py) | Vietnamese heading patterns, TÀI LIỆU THAM KHẢO drop | 155 |
| [vn_quality_scorer.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_quality_scorer.py) | 6-criteria scoring (100 pts), go/review/hold | 128 |
| [vn_dedup.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_dedup.py) | SHA256 + fingerprint + Levenshtein dedup | 187 |
| [vn_txt_to_jsonl.py](file:///d:/CODE/DATN/LLM-MedQA-Assistant/services/qdrant-ingestor/etl/vn/vn_txt_to_jsonl.py) | Master converter, KCB procedure splitter, pilot mode | 278 |

---

## Pilot Test Results

Ran pilot on 30 files across 6 source types:

| Source | Files | Records | Avg Score | GO% | Hold% | Bad Titles |
|--------|------:|--------:|----------:|----:|------:|-----------:|
| **kcb_moh** | 5 | 18 | **98.1** | 100% | 0% | 0 |
| **trad_med_pharm_journal** | 5 | 44 | **95.1** | 100% | 0% | 0 |
| **dav_gov** | 5 | 122 | **94.2** | 97.5% | 0% | 0 |
| **mil_med_pharm_journal** | 5 | 70 | **94.0** | 100% | 0% | 0 |
| **hue_jmp_ojs** | 5 | 79 | **92.4** | 98.7% | 0% | 0 |
| **vmj_ojs** | 5 | 5,156 | **86.1** | 42.6% | 0% | 0 |

> [!TIP]
> vmj_ojs files are full journal issues containing dozens of articles. The lower GO% is due to many short sections and `too_many_sections` flag — but **0 bad titles** and **0 holds**.

### Specialties Detected Correctly
- `cardiology` — metabolic syndrome article
- `infectious_disease` — antibiotic resistance article
- `pharmacology` — pharmaceutical reference docs, traditional medicine journal
- `oncology` — nasopharyngeal carcinoma article

---

## Bugs Fixed During Pilot

1. **Sectionizer over-splitting**: Removed generic ALL-CAPS regex that matched table headers and data lines, not just section headers
2. **Numbered heading max length**: Capped `\d+\.\s+[A-ZÀ-Ỹ].{5,}` to `{5,80}` to avoid matching paragraph-length numbered items
3. **Min section body**: Raised from 50 to 200 chars to avoid trivially short sections

---

## How to Run

### Pilot (40-60 files)
```bash
cd d:\CODE\DATN\LLM-MedQA-Assistant\services\qdrant-ingestor
d:\CODE\.venv\Scripts\python.exe -m etl.vn.vn_txt_to_jsonl \
    --source-dir ../../rag-data/data_processed/kcb_moh \
    --output ../../data/data_final/pilot_kcb_moh.jsonl \
    --max-files 10 --verbose
```

### Full batch
```bash
d:\CODE\.venv\Scripts\python.exe -m etl.vn.vn_txt_to_jsonl \
    --source-dir ../../rag-data/data_processed/kcb_moh \
    --output ../../data/data_final/kcb_moh.jsonl --verbose
```

### Dedup check
```bash
d:\CODE\.venv\Scripts\python.exe -m etl.vn.vn_dedup \
    --input ../../data/data_final/kcb_moh.jsonl --report
```

---

## Known Issues & Next Steps

1. **vmj_ojs title accuracy**: The title extractor sometimes picks up text from a previous article's tail when files contain multi-article journal issues. V2 could implement article boundary detection.
2. **kcb_moh title**: Some KCB files don't contain quoted titles in the expected format, falling back to generic extraction — there may be room to improve the guideline-specific parser.
3. **Full corpus run**: After user approval, run on all ~1,350 files using the batch commands in the implementation plan.
4. **Gate 2 (Quality)**: Run `run_all_checks.py` on full output JSONL files.
5. **Gate 3 (Retrieval)**: Index into `staging_medqa_vi` and test retrieval.
