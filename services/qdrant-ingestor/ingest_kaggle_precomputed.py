"""
Ingest pre-computed embeddings from Kaggle into Qdrant.

Canonical layout:
  - rag-data/embeddings/staging/<dataset_id>/<profile>/{embeddings.npy, chunk_ids.json}
  - rag-data/embeddings/exports/<dataset_id>/<profile>/{chunk_metadata.jsonl, chunk_texts_for_embed.jsonl}
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
VECTOR_DIM = 1024
BATCH_SIZE = int(os.getenv("QDRANT_BATCH_SIZE", "128"))
QDRANT_TIMEOUT = int(os.getenv("QDRANT_TIMEOUT", "120"))
EMBED_DATASET_ID = os.getenv("EMBED_DATASET_ID", "medqa_release_v3_all_open_enriched")
KAGGLE_PROFILE = os.getenv("KAGGLE_PROFILE", "multilingual")
QDRANT_RECREATE_COLLECTION = os.getenv("QDRANT_RECREATE_COLLECTION", "")
QDRANT_CONNECT_RETRIES = int(os.getenv("QDRANT_CONNECT_RETRIES", "30"))
QDRANT_CONNECT_SLEEP_S = float(os.getenv("QDRANT_CONNECT_SLEEP_S", "2"))

EMBEDDINGS_FILE = preferred_embedding_vectors_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)
IDS_FILE = preferred_embedding_ids_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)
META_FILE = preferred_chunk_metadata_export_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)
TEXTS_FILE = preferred_chunk_texts_export_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def connect_qdrant() -> QdrantClient:
    last_error: Exception | None = None
    for attempt in range(1, QDRANT_CONNECT_RETRIES + 1):
        try:
            client = QdrantClient(url=QDRANT_URL, timeout=QDRANT_TIMEOUT)
            client.get_collections()
            return client
        except Exception as exc:
            last_error = exc
            print(
                f"  Qdrant not ready ({attempt}/{QDRANT_CONNECT_RETRIES}): {exc}. "
                f"Retrying in {QDRANT_CONNECT_SLEEP_S:g}s..."
            )
            time.sleep(QDRANT_CONNECT_SLEEP_S)
    raise RuntimeError(f"Qdrant not reachable at {QDRANT_URL}") from last_error


def maybe_recreate_collection(client: QdrantClient) -> None:
    existing = {c.name for c in client.get_collections().collections}
    if COLLECTION not in existing:
        return

    info = client.get_collection(COLLECTION)
    print(f"  Collection '{COLLECTION}' exists: {info.points_count} points")

    if QDRANT_RECREATE_COLLECTION:
        if _truthy(QDRANT_RECREATE_COLLECTION):
            client.delete_collection(COLLECTION)
            print("  Deleted existing collection due to QDRANT_RECREATE_COLLECTION=true.")
        else:
            print("  Keeping existing collection due to QDRANT_RECREATE_COLLECTION=false.")
        return

    if sys.stdin.isatty():
        choice = input("  Delete and recreate? [y/N]: ").strip().lower()
        if choice == "y":
            client.delete_collection(COLLECTION)
            print("  Deleted.")
        else:
            print("  Keeping existing. Will upsert (overwrite duplicates).")
        return

    print("  Keeping existing collection in non-interactive mode. Will upsert duplicates.")


def _upsert_with_retry(client: QdrantClient, points: list) -> QdrantClient:
    for attempt in range(1, 6):
        try:
            client.upsert(collection_name=COLLECTION, points=points)
            return client
        except Exception as exc:
            if attempt == 5:
                raise
            wait = attempt * 5
            print(f"  [retry {attempt}/5] upsert error: {exc}. Reconnecting in {wait}s...")
            time.sleep(wait)
            client = connect_qdrant()
    return client


def main() -> None:
    print("=" * 60)
    print("  Kaggle Pre-computed Embedding Ingestion")
    print("=" * 60)
    print(f"  Dataset: {EMBED_DATASET_ID or '(legacy/global export)'}")
    print(f"  Profile: {KAGGLE_PROFILE}")

    print(f"\n[1/4] Loading embeddings (mmap) from {EMBEDDINGS_FILE}...")
    embeddings = np.load(EMBEDDINGS_FILE, mmap_mode="r")
    print(f"  Shape: {embeddings.shape}, dtype: {embeddings.dtype}")

    print(f"\n[2/4] Loading chunk IDs from {IDS_FILE}...")
    with open(IDS_FILE, "r", encoding="utf-8") as fh:
        chunk_ids = json.load(fh)
    print(f"  Total: {len(chunk_ids)} IDs")

    assert len(chunk_ids) == embeddings.shape[0], (
        f"Mismatch: {len(chunk_ids)} IDs vs {embeddings.shape[0]} embeddings"
    )
    assert embeddings.shape[1] == VECTOR_DIM, (
        f"Expected {VECTOR_DIM}-dim, got {embeddings.shape[1]}"
    )

    print(f"\n[3/4] Connecting to Qdrant at {QDRANT_URL}...")
    client = connect_qdrant()
    maybe_recreate_collection(client)

    if COLLECTION not in {c.name for c in client.get_collections().collections}:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=qm.VectorParams(size=VECTOR_DIM, distance=qm.Distance.COSINE),
        )
        print(f"  Created collection '{COLLECTION}' (dim={VECTOR_DIM}, cosine)")

    total = len(chunk_ids)
    print(f"\n[4/4] Upserting {total} chunks (batch={BATCH_SIZE}, streaming metadata)...")
    t0 = time.time()
    total_upserted = 0
    skipped = 0

    # Stream metadata and texts line-by-line to avoid loading ~800MB into RAM.
    # Files are order-aligned with chunk_ids so we iterate them in lockstep.
    with open(META_FILE, "r", encoding="utf-8") as meta_fh, \
         open(TEXTS_FILE, "r", encoding="utf-8") as texts_fh:

        meta_iter = (ln for ln in meta_fh if ln.strip())
        texts_iter = (ln for ln in texts_fh if ln.strip())

        for i in range(0, total, BATCH_SIZE):
            batch_end = min(i + BATCH_SIZE, total)
            points = []

            for j in range(i, batch_end):
                cid = chunk_ids[j]
                vec = embeddings[j]

                meta_line = next(meta_iter, None)
                text_line = next(texts_iter, None)

                if meta_line is None or text_line is None:
                    skipped += 1
                    continue

                meta_rec = json.loads(meta_line)
                text_rec = json.loads(text_line)

                if str(meta_rec.get("id", "")) != cid or str(text_rec.get("id", "")) != cid:
                    raise RuntimeError(
                        f"Alignment mismatch at index {j}: "
                        f"expected={cid} meta_id={meta_rec.get('id')} text_id={text_rec.get('id')}"
                    )

                md = meta_rec.get("metadata", {})
                text = text_rec.get("text", "")

                if not text and not md:
                    skipped += 1
                    continue

                payload = {"text": text, "human_id": cid}
                payload.update(md)

                point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, cid))
                points.append(qm.PointStruct(id=point_id, vector=vec.tolist(), payload=payload))

            if points:
                client = _upsert_with_retry(client, points)
                total_upserted += len(points)

            if (i // BATCH_SIZE) % 20 == 0:
                elapsed = time.time() - t0
                pct = min(100.0, batch_end / total * 100)
                print(f"  [{pct:5.1f}%] {total_upserted} upserted, {elapsed:.1f}s elapsed")

    elapsed = time.time() - t0
    info = client.get_collection(COLLECTION)
    print(f"\n  Done: {total_upserted} upserted, {skipped} skipped, {elapsed:.1f}s")
    print(f"\n{'=' * 60}")
    print(f"  Collection '{COLLECTION}': {info.points_count} points")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
