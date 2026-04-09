"""
Step 3: Import pre-computed GPU embeddings into Qdrant.
Downloads from Kaggle: embeddings.npy + chunk_ids.json
Uses chunk_metadata.jsonl (created by export script) for payload.

Usage:
  1. Place embeddings.npy and chunk_ids.json in data/ folder
  2. Run: python import_kaggle_embeddings.py
"""
import json, os, sys, time
import numpy as np

sys.path.insert(0, os.path.abspath("."))

from qdrant_client import QdrantClient
from qdrant_client.http import models as qm
import uuid

# --- Config ---
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "staging_medqa_vi_vmj_v2")
BATCH_SIZE = 64

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "kaggle_staging", "multilingual"))
EMBEDDINGS_FILE = os.path.join(BASE, "embeddings.npy")
IDS_FILE = os.path.join(BASE, "chunk_ids.json")
METADATA_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "kaggle_staging", "chunk_metadata.jsonl"))
TEXTS_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "chunk_texts_for_embed.jsonl"))

def main():
    print("=" * 60)
    print("  Import Kaggle GPU Embeddings → Qdrant")
    print("=" * 60)

    # Load embeddings
    print(f"\n[1/4] Loading embeddings from {EMBEDDINGS_FILE}...")
    embeddings = np.load(EMBEDDINGS_FILE)
    print(f"  Shape: {embeddings.shape}")

    # Load IDs
    print(f"[2/4] Loading chunk IDs...")
    with open(IDS_FILE, 'r') as f:
        chunk_ids = json.load(f)
    print(f"  {len(chunk_ids)} IDs")

    # Load metadata
    print(f"[3/4] Loading metadata + texts...")
    metadata_map = {}
    with open(METADATA_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            rec = json.loads(line)
            metadata_map[rec['id']] = rec['metadata']
    
    # Load chunk texts
    text_map = {}
    if os.path.exists(TEXTS_FILE):
        with open(TEXTS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                rec = json.loads(line)
                text_map[rec['id']] = rec['text']
        print(f"  {len(text_map)} texts loaded")
    else:
        print(f"  WARNING: {TEXTS_FILE} not found, text will be empty")
    print(f"  {len(metadata_map)} metadata records")

    assert len(chunk_ids) == embeddings.shape[0], f"Mismatch: {len(chunk_ids)} IDs vs {embeddings.shape[0]} embeddings"

    # Connect Qdrant
    print(f"\n[4/4] Connecting to Qdrant at {QDRANT_URL}...")
    client = QdrantClient(url=QDRANT_URL, check_compatibility=False)

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

    # Upsert in batches
    total = len(chunk_ids)
    print(f"\n  Upserting {total} vectors (batch_size={BATCH_SIZE})...")
    t0 = time.time()

    for i in range(0, total, BATCH_SIZE):
        batch_ids = chunk_ids[i:i+BATCH_SIZE]
        batch_vecs = embeddings[i:i+BATCH_SIZE]

        points = []
        for cid, vec in zip(batch_ids, batch_vecs):
            payload = {"text": text_map.get(cid, ""), "human_id": cid}
            if cid in metadata_map:
                payload.update(metadata_map[cid])
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, cid))
            points.append(qm.PointStruct(
                id=point_id,
                vector=vec.tolist(),
                payload=payload,
            ))

        client.upsert(collection_name=COLLECTION, points=points)

        if (i // BATCH_SIZE) % 50 == 0:
            elapsed = time.time() - t0
            print(f"    {i+len(batch_ids)}/{total} ({(i+len(batch_ids))/total*100:.1f}%) - {elapsed:.0f}s")

    elapsed = time.time() - t0
    info = client.get_collection(COLLECTION)
    print(f"\n{'=' * 60}")
    print(f"  DONE! {info.points_count} points in '{COLLECTION}' ({elapsed:.1f}s)")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
