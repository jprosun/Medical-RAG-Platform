"""
Import pre-computed GPU embeddings into Qdrant.

Canonical layout:
  - rag-data/embeddings/staging/<dataset_id>/<profile>/embeddings.npy
  - rag-data/embeddings/staging/<dataset_id>/<profile>/chunk_ids.json
  - rag-data/embeddings/exports/<dataset_id>/<profile>/chunk_metadata.jsonl
  - rag-data/embeddings/exports/<dataset_id>/<profile>/chunk_texts_for_embed.jsonl
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_paths import (  # noqa: E402
    preferred_embedding_ids_path,
    preferred_embedding_vectors_path,
    preferred_chunk_metadata_export_path,
    preferred_chunk_texts_export_path,
)


QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "medqa_release_v3_all_bge_m3")
BATCH_SIZE = 256
EMBED_DATASET_ID = os.getenv("EMBED_DATASET_ID", "medqa_release_v3_all_open_enriched")
KAGGLE_PROFILE = os.getenv("KAGGLE_PROFILE", "multilingual")

EMBEDDINGS_FILE = preferred_embedding_vectors_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)
IDS_FILE = preferred_embedding_ids_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)
METADATA_FILE = preferred_chunk_metadata_export_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)
TEXTS_FILE = preferred_chunk_texts_export_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)


def main() -> None:
    print("=" * 60)
    print("  Import Kaggle GPU Embeddings -> Qdrant")
    print("=" * 60)
    print(f"  Dataset: {EMBED_DATASET_ID or '(legacy/global export)'}")
    print(f"  Profile: {KAGGLE_PROFILE}")

    print(f"\n[1/4] Loading embeddings from {EMBEDDINGS_FILE}...")
    embeddings = np.load(EMBEDDINGS_FILE, mmap_mode="r")
    print(f"  Shape: {embeddings.shape}")

    print("[2/4] Loading chunk IDs...")
    with open(IDS_FILE, "r", encoding="utf-8") as fh:
        chunk_ids = json.load(fh)
    print(f"  {len(chunk_ids)} IDs")

    print("[3/4] Loading metadata + texts...")
    metadata_map = {}
    with open(METADATA_FILE, "r", encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            metadata_map[rec["id"]] = rec["metadata"]

    text_map = {}
    if TEXTS_FILE.exists():
        with open(TEXTS_FILE, "r", encoding="utf-8") as fh:
            for line in fh:
                rec = json.loads(line)
                text_map[rec["id"]] = rec["text"]
        print(f"  {len(text_map)} texts loaded")
    else:
        print(f"  WARNING: {TEXTS_FILE} not found, text will be empty")
    print(f"  {len(metadata_map)} metadata records")

    assert len(chunk_ids) == embeddings.shape[0], (
        f"Mismatch: {len(chunk_ids)} IDs vs {embeddings.shape[0]} embeddings"
    )
    missing_metadata = [cid for cid in chunk_ids if cid not in metadata_map]
    missing_text = [cid for cid in chunk_ids if cid not in text_map]
    if missing_metadata or missing_text:
        raise RuntimeError(
            "Embedding artifact alignment failed: "
            f"missing_metadata={len(missing_metadata)}, missing_text={len(missing_text)}. "
            "Run tools/audit_embedding_artifacts.py before import."
        )

    print(f"\n[4/4] Connecting to Qdrant at {QDRANT_URL}...")
    client = QdrantClient(url=QDRANT_URL)

    vec_size = embeddings.shape[1]
    existing = {c.name for c in client.get_collections().collections}
    if COLLECTION not in existing:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=qm.VectorParams(size=vec_size, distance=qm.Distance.COSINE),
        )
        print(f"  Created collection '{COLLECTION}'")
    else:
        info = client.get_collection(COLLECTION)
        print(f"  Collection exists: {info.points_count} points")

    total = len(chunk_ids)
    print(f"\n  Upserting {total} vectors (batch_size={BATCH_SIZE})...")
    t0 = time.time()

    for i in range(0, total, BATCH_SIZE):
        batch_ids = chunk_ids[i:i + BATCH_SIZE]
        batch_vecs = embeddings[i:i + BATCH_SIZE]

        points = []
        for cid, vec in zip(batch_ids, batch_vecs):
            payload = {"text": text_map.get(cid, ""), "human_id": cid}
            if cid in metadata_map:
                payload.update(metadata_map[cid])
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, cid))
            points.append(
                qm.PointStruct(
                    id=point_id,
                    vector=vec.tolist(),
                    payload=payload,
                )
            )

        client.upsert(collection_name=COLLECTION, points=points)

        if (i // BATCH_SIZE) % 50 == 0:
            elapsed = time.time() - t0
            print(f"    {i + len(batch_ids)}/{total} ({(i + len(batch_ids)) / total * 100:.1f}%) - {elapsed:.0f}s")

    elapsed = time.time() - t0
    info = client.get_collection(COLLECTION)
    print(f"\n{'=' * 60}")
    print(f"  DONE! {info.points_count} points in '{COLLECTION}' ({elapsed:.1f}s)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
