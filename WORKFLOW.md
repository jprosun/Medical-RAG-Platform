# Data Workflow

Single source of truth:

- Data lives under `rag-data/`
- ETL code lives under `pipelines/etl/`
- Ingest and retrieval code lives under `services/qdrant-ingestor/` and `services/rag-orchestrator/`

## Canonical layout

```text
rag-data/
  qa/
  sources/
    <source_id>/
      raw/
      intermediate/
      processed/
      records/
        document_records.jsonl
      qa/
  datasets/
    <dataset_id>/
      records/
        document_records.jsonl
      qa/
      manifest.json
  embeddings/
    exports/
      <dataset_id>/<profile>/
    staging/
      <dataset_id>/<profile>/
    runs/
```

## Naming contract

- `source_id`: stable source identity such as `vmj_ojs`, `who`, `medlineplus`
- `release_id`: source snapshot such as `v2`, `2026-04-25`
- `dataset_id`: app-facing ingest artifact such as `en_core_v1`, `all_corpus_v1`, `vmj_ojs_release_v2`
- `profile`: embedding profile such as `multilingual`

Do not treat a release suffix as a new source. `vmj_ojs` is the source. `v2` is a release.

## Standard pipeline

### 1. Crawl raw data

- VN crawlers store files under `rag-data/sources/<source_id>/raw/`
- EN scrapers also keep their downloaded raw assets under `rag-data/sources/<source_id>/raw/`

Examples:

```powershell
python -m pipelines.etl.medlineplus_scraper
python -m pipelines.etl.who_scraper
python -m pipelines.etl.ncbi_bookshelf_scraper
```

### 2. Prepare processed text for VN PDF sources

- Digital PDF extraction writes normalized text to `rag-data/sources/<source_id>/processed/`
- VMJ issue splitting writes article-level text to `rag-data/sources/vmj_ojs/intermediate/split_articles/`

Examples:

```powershell
python tools/extract_digital_pdf.py
python -m pipelines.etl.vn.vmj_issue_splitter
```

### 3. Run ETL to source records

- VN ETL reads `processed/` or `intermediate/` and writes `records/document_records.jsonl`
- EN normalize step writes per-source records and can also build a dataset release

Examples:

```powershell
python -m pipelines.etl.vn.vn_txt_to_jsonl --source-id vmj_ojs
python -m pipelines.etl.vn.vn_txt_to_jsonl --source-id kcb_moh
python -m pipelines.etl.normalize_all --dataset-id en_core_v1
```

Outputs:

- `rag-data/sources/vmj_ojs/records/document_records.jsonl`
- `rag-data/sources/kcb_moh/records/document_records.jsonl`
- `rag-data/sources/medlineplus/records/document_records.jsonl`
- `rag-data/datasets/en_core_v1/records/document_records.jsonl`

### 4. Build dataset releases

App-facing ingest should prefer dataset releases over individual source files.

Examples:

- `rag-data/datasets/en_core_v1/records/document_records.jsonl`
- `rag-data/datasets/all_corpus_v1/records/document_records.jsonl`
- `rag-data/datasets/vmj_ojs_release_v2/records/document_records.jsonl`

Related files:

- `rag-data/datasets/<dataset_id>/manifest.json`
- `rag-data/datasets/<dataset_id>/qa/validation_summary.json`

### 5. QA before ingest

Run QA checks on a source record file or on a dataset release.

Examples:

```powershell
cd services/qdrant-ingestor
python -m qa_pre_ingest.run_all_checks ../../rag-data/sources/medlineplus/records/document_records.jsonl
python -m qa_pre_ingest.run_all_checks ../../rag-data/datasets/en_core_v1/records/document_records.jsonl
```

### 6. Migration audit before deleting legacy data

Copy legacy artifacts into canonical paths with hash checks:

```powershell
python tools/migrate_legacy_data_to_rag_data.py --data-final --execute
python tools/migrate_legacy_data_to_rag_data.py --kaggle --dataset-id vmj_ojs_release_v2 --profile multilingual --execute
python tools/migrate_legacy_data_to_rag_data.py --raw --execute
python tools/migrate_legacy_data_to_rag_data.py --processed --execute
python tools/migrate_legacy_data_to_rag_data.py --intermediate --execute
python tools/migrate_legacy_data_to_rag_data.py --root-chunks --execute --overwrite
```

`--root-chunks` copies the root `data/chunk_*` exports into canonical embedding export folders. The root
`data/chunk_texts_for_embed.jsonl` is the VMJ text export that aligns with `chunk_ids.json` and
`embeddings.npy`; use the embedding audit below before importing vectors.

Build curated dataset releases from canonical source records:

```powershell
python tools/build_dataset_release.py --dataset-id vi_core_v1 --source-group vi
python tools/build_dataset_release.py --dataset-id all_corpus_v1 --source-group all
```

Run this before archiving or deleting any old top-level `data/` artifact:

```powershell
python tools/audit_rag_data_migration.py --all-known
python tools/audit_embedding_artifacts.py --dataset-id vmj_ojs_release_v2 --profile multilingual
```

Output:

- `rag-data/qa/migration_audit.json`
- `rag-data/qa/legacy_copy_data_final_manifest.json`
- `rag-data/qa/legacy_copy_kaggle_multilingual_manifest.json`
- `rag-data/qa/legacy_copy_raw_manifest.json`
- `rag-data/qa/legacy_copy_processed_manifest.json`
- `rag-data/qa/legacy_copy_intermediate_manifest.json`
- `rag-data/qa/legacy_copy_root_chunks_manifest.json`
- `rag-data/qa/embedding_alignment_<dataset_id>_<profile>.json`

The audit compares canonical records against legacy records by record count and `doc_id` set. A source or dataset should be `match` or intentionally `canonical_only` before fallback paths are removed.

### 7. Export chunks for offline embedding

Chunk exports are keyed by `dataset_id` and `profile`.

Example:

```powershell
python tools/kaggle/export_chunks_for_kaggleV2.py --dataset-id en_core_v1 --profile multilingual
python tools/kaggle/export_chunks_for_kaggleV2.py --source-id vmj_ojs --profile multilingual
```

The export command refuses to overwrite existing chunk exports by default. Use `--overwrite` only when
you intentionally want to regenerate an export and then rerun the embedding artifact audit.

Outputs:

- `rag-data/embeddings/exports/<dataset_id>/<profile>/chunk_texts_for_embed.jsonl`
- `rag-data/embeddings/exports/<dataset_id>/<profile>/chunk_metadata.jsonl`

### 8. Return embeddings from Kaggle or another offline GPU job

Returned vector artifacts should be placed under:

- `rag-data/embeddings/staging/<dataset_id>/<profile>/embeddings.npy`
- `rag-data/embeddings/staging/<dataset_id>/<profile>/chunk_ids.json`

Then import them into Qdrant:

```powershell
$env:EMBED_DATASET_ID='en_core_v1'
$env:KAGGLE_PROFILE='multilingual'
python tools/audit_embedding_artifacts.py --dataset-id $env:EMBED_DATASET_ID --profile $env:KAGGLE_PROFILE
python tools/kaggle/import_kaggle_embeddings.py
```

### 9. Ingest into Qdrant

For online chunking and embedding:

```powershell
cd services/qdrant-ingestor
$env:DATASET_ID='en_core_v1'
python ingest_staging.py
```

For local Docker ingest, `qdrant-ingestor` mounts `./rag-data:/rag-data:ro` and ingests the canonical
dataset release configured in `docker-compose.local.yml`.

For Docker ingest with precomputed Kaggle vectors (`chunk_ids.json` + `embeddings.npy`), use the
dedicated precomputed profile:

```powershell
docker compose -f docker-compose.local.yml --profile precomputed-ingest run --rm --build qdrant-precomputed-ingestor
```

For a clean collection rebuild, explicitly recreate the target collection:

```powershell
docker compose -f docker-compose.local.yml --profile precomputed-ingest run --rm --build -e QDRANT_RECREATE_COLLECTION=true qdrant-precomputed-ingestor
```

For a single source file:

```powershell
cd services/qdrant-ingestor
$env:DATA_SOURCE_ID='vmj_ojs'
python ingest_staging.py
```

## Retrieval storage

- Chunk vectors used for retrieval are stored in Qdrant collections
- `rag-data/` stores raw data, ETL artifacts, dataset releases, and offline embedding artifacts
- `rag-data/` is not the live retrieval index

## Lineage fields

New ETL records include optional lineage fields:

- `source_file`
- `raw_path`
- `processed_path`
- `intermediate_path`
- `parent_file`
- `source_sha256`
- `crawl_run_id`
- `etl_run_id`

These fields are carried into Qdrant chunk metadata. They are optional so old JSONL artifacts remain readable.

## Migration policy

- New runs must write to `rag-data/`
- Run `python tools/audit_rag_data_migration.py --all-known` before removing any legacy artifact
- Legacy fallback readers for top-level `data/` are still available during migration
- Keep the working tree lean after audit: do not retain top-level `data/`, `rag-data/data_*`, crawl-seed mirrors, or old embedding releases when their canonical artifacts already exist
- Keep only current app-facing dataset and embedding artifacts unless an older release is needed for a specific benchmark
- Historical benchmark reports may still mention old paths; treat them as snapshots, not as the current contract
