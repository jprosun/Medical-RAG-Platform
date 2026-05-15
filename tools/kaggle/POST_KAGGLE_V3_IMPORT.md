# Post-Kaggle Import Flow for `medqa_release_v3_all_open_enriched`

After the Kaggle notebook finishes, download these files from Kaggle Output:

- `embeddings.npy`
- `chunk_ids.json`
- `embedding_manifest.json`

Put them in one local folder, for example:

```powershell
D:\CODE\DATN\kaggle-output\medqa_release_v3_all_open_enriched
```

Finalize and audit the artifacts:

```powershell
python tools\kaggle\finalize_kaggle_embedding_artifacts.py `
  --input-dir D:\CODE\DATN\kaggle-output\medqa_release_v3_all_open_enriched `
  --overwrite
```

The script copies files into:

```text
rag-data/embeddings/staging/medqa_release_v3_all_open_enriched/multilingual/
```

It then runs the full artifact audit. Do not import into Qdrant unless the audit status is `pass`.

Import into Qdrant locally:

```powershell
$env:EMBED_DATASET_ID='medqa_release_v3_all_open_enriched'
$env:KAGGLE_PROFILE='multilingual'
$env:QDRANT_COLLECTION='medqa_release_v3_all_bge_m3'
python services\qdrant-ingestor\ingest_kaggle_precomputed.py
```

Or via Docker Compose:

```powershell
docker compose -f docker-compose.local.yml --profile precomputed-ingest run --rm qdrant-precomputed-ingestor
```

Final verification:

```powershell
python tools\audit_embedding_artifacts.py --dataset-id medqa_release_v3_all_open_enriched --profile multilingual
```
