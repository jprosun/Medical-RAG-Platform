"""
Ingest pre-computed embeddings from Kaggle into Qdrant.

Reads:
  - embeddings.npy: (N, 1024) float32 array
  - chunk_ids.json: list of N chunk IDs
  - chunk_metadata.jsonl: {id, metadata} per chunk

Upserts directly to Qdrant — zero embedding computation needed.
"""

import json
import os
import sys
import time
import uuid

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

# ── Config ───────────────────────────────────────────────────────────
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "staging_medqa_vi_vmj_v2")
VECTOR_DIM = 1024
BATCH_SIZE = 256

# Paths
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
EMBED_DIR = os.path.join(ROOT, "data", "kaggle_staging", "multilingual")
META_FILE = os.path.join(ROOT, "data", "kaggle_staging", "chunk_metadata.jsonl")
TEXTS_FILE = os.path.join(ROOT, "data", "kaggle_staging", "chunk_texts_for_embed.jsonl")


def load_metadata(meta_path: str) -> dict:
    """Load chunk metadata keyed by chunk ID."""
    meta = {}
    with open(meta_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            meta[rec["id"]] = rec.get("metadata", {})
    return meta


def load_texts(texts_path: str) -> dict:
    """Load chunk texts keyed by chunk ID."""
    texts = {}
    if not os.path.exists(texts_path):
        return texts
    with open(texts_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            texts[rec["id"]] = rec.get("text", "")
    return texts


def main():
    print("=" * 60)
    print("  Kaggle Pre-computed Embedding Ingestion")
    print("=" * 60)

    # Load embeddings
    embed_path = os.path.join(EMBED_DIR, "embeddings.npy")
    ids_path = os.path.join(EMBED_DIR, "chunk_ids.json")

    print(f"\n[1/5] Loading embeddings from {embed_path}...")
    embeddings = np.load(embed_path)
    print(f"  Shape: {embeddings.shape}, dtype: {embeddings.dtype}")

    print(f"\n[2/5] Loading chunk IDs from {ids_path}...")
    with open(ids_path, "r", encoding="utf-8") as f:
        chunk_ids = json.load(f)
    print(f"  Total: {len(chunk_ids)} IDs")

    assert len(chunk_ids) == embeddings.shape[0], (
        f"Mismatch: {len(chunk_ids)} IDs vs {embeddings.shape[0]} embeddings"
    )
    assert embeddings.shape[1] == VECTOR_DIM, (
        f"Expected {VECTOR_DIM}-dim, got {embeddings.shape[1]}"
    )

    # Load metadata
    print(f"\n[3/5] Loading metadata from {META_FILE}...")
    metadata = load_metadata(META_FILE)
    print(f"  Loaded metadata for {len(metadata)} chunks")

    # Load texts
    print(f"  Loading texts from {TEXTS_FILE}...")
    texts = load_texts(TEXTS_FILE)
    print(f"  Loaded texts for {len(texts)} chunks")

    # Stats
    matched = sum(1 for cid in chunk_ids if cid in metadata)
    print(f"  Matched IDs: {matched}/{len(chunk_ids)}")
    if matched < len(chunk_ids):
        missing = [cid for cid in chunk_ids[:5] if cid not in metadata]
        print(f"  Sample missing: {missing}")

    # Connect to Qdrant
    print(f"\n[4/5] Connecting to Qdrant at {QDRANT_URL}...")
    client = QdrantClient(url=QDRANT_URL, check_compatibility=False)

    # Create/ensure collection
    existing = {c.name for c in client.get_collections().collections}
    if COLLECTION in existing:
        info = client.get_collection(COLLECTION)
        print(f"  Collection '{COLLECTION}' exists: {info.points_count} points")
        choice = input("  Delete and recreate? [y/N]: ").strip().lower()
        if choice == "y":
            client.delete_collection(COLLECTION)
            print(f"  Deleted.")
        else:
            print(f"  Keeping existing. Will upsert (overwrite duplicates).")

    if COLLECTION not in {c.name for c in client.get_collections().collections}:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=qm.VectorParams(
                size=VECTOR_DIM,
                distance=qm.Distance.COSINE,
            ),
        )
        print(f"  Created collection '{COLLECTION}' (dim={VECTOR_DIM}, cosine)")

    # Upsert in batches
    print(f"\n[5/5] Upserting {len(chunk_ids)} chunks (batch={BATCH_SIZE})...")
    t0 = time.time()
    total_upserted = 0
    skipped = 0

    for i in range(0, len(chunk_ids), BATCH_SIZE):
        batch_ids = chunk_ids[i:i + BATCH_SIZE]
        batch_vecs = embeddings[i:i + BATCH_SIZE]

        points = []
        for cid, vec in zip(batch_ids, batch_vecs):
            md = metadata.get(cid, {})
            text = texts.get(cid, "")

            if not text and not md:
                skipped += 1
                continue

            payload = {"text": text, "human_id": cid}
            payload.update(md)

            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, cid))
            points.append(
                qm.PointStruct(
                    id=point_id,
                    vector=vec.tolist(),
                    payload=payload,
                )
            )

        if points:
            client.upsert(collection_name=COLLECTION, points=points)
            total_upserted += len(points)

        if (i // BATCH_SIZE) % 20 == 0:
            elapsed = time.time() - t0
            pct = min(100, (i + BATCH_SIZE) / len(chunk_ids) * 100)
            print(f"  [{pct:5.1f}%] {total_upserted} upserted, {elapsed:.1f}s elapsed")

    elapsed = time.time() - t0
    print(f"\n  Done: {total_upserted} upserted, {skipped} skipped, {elapsed:.1f}s")

    # Verify
    info = client.get_collection(COLLECTION)
    print(f"\n{'=' * 60}")
    print(f"  Collection '{COLLECTION}': {info.points_count} points")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
