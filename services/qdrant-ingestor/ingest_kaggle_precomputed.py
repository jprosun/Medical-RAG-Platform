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
BATCH_SIZE = 256
EMBED_DATASET_ID = os.getenv("EMBED_DATASET_ID", "medqa_release_v3_all_open_enriched")
KAGGLE_PROFILE = os.getenv("KAGGLE_PROFILE", "multilingual")
QDRANT_RECREATE_COLLECTION = os.getenv("QDRANT_RECREATE_COLLECTION", "")
QDRANT_CONNECT_RETRIES = int(os.getenv("QDRANT_CONNECT_RETRIES", "30"))
QDRANT_CONNECT_SLEEP_S = float(os.getenv("QDRANT_CONNECT_SLEEP_S", "2"))

EMBEDDINGS_FILE = preferred_embedding_vectors_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)
IDS_FILE = preferred_embedding_ids_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)
META_FILE = preferred_chunk_metadata_export_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)
TEXTS_FILE = preferred_chunk_texts_export_path(dataset_id=EMBED_DATASET_ID or None, profile=KAGGLE_PROFILE)


def load_metadata(meta_path: Path) -> dict:
    meta = {}
    with open(meta_path, "r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            rec = json.loads(line)
            meta[rec["id"]] = rec.get("metadata", {})
    return meta


def load_texts(texts_path: Path) -> dict:
    texts = {}
    if not texts_path.exists():
        return texts
    with open(texts_path, "r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            rec = json.loads(line)
            texts[rec["id"]] = rec.get("text", "")
    return texts


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def connect_qdrant() -> QdrantClient:
    last_error: Exception | None = None
    for attempt in range(1, QDRANT_CONNECT_RETRIES + 1):
        try:
            client = QdrantClient(url=QDRANT_URL)
            client.get_collections()
            return client
        except Exception as exc:  # pragma: no cover - depends on Docker/Qdrant timing
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


def main() -> None:
    print("=" * 60)
    print("  Kaggle Pre-computed Embedding Ingestion")
    print("=" * 60)
    print(f"  Dataset: {EMBED_DATASET_ID or '(legacy/global export)'}")
    print(f"  Profile: {KAGGLE_PROFILE}")

    embed_path = EMBEDDINGS_FILE
    ids_path = IDS_FILE

    print(f"\n[1/5] Loading embeddings from {embed_path}...")
    embeddings = np.load(embed_path, mmap_mode="r")
    print(f"  Shape: {embeddings.shape}, dtype: {embeddings.dtype}")

    print(f"\n[2/5] Loading chunk IDs from {ids_path}...")
    with open(ids_path, "r", encoding="utf-8") as fh:
        chunk_ids = json.load(fh)
    print(f"  Total: {len(chunk_ids)} IDs")

    assert len(chunk_ids) == embeddings.shape[0], (
        f"Mismatch: {len(chunk_ids)} IDs vs {embeddings.shape[0]} embeddings"
    )
    assert embeddings.shape[1] == VECTOR_DIM, (
        f"Expected {VECTOR_DIM}-dim, got {embeddings.shape[1]}"
    )

    print(f"\n[3/5] Loading metadata from {META_FILE}...")
    metadata = load_metadata(META_FILE)
    print(f"  Loaded metadata for {len(metadata)} chunks")

    print(f"  Loading texts from {TEXTS_FILE}...")
    texts = load_texts(TEXTS_FILE)
    print(f"  Loaded texts for {len(texts)} chunks")

    matched = sum(1 for cid in chunk_ids if cid in metadata)
    print(f"  Matched IDs: {matched}/{len(chunk_ids)}")
    missing_texts = sum(1 for cid in chunk_ids if cid not in texts)
    if matched != len(chunk_ids) or missing_texts:
        raise RuntimeError(
            "Embedding artifact alignment failed: "
            f"missing_metadata={len(chunk_ids) - matched}, missing_texts={missing_texts}. "
            "Run tools/audit_embedding_artifacts.py before ingest."
        )

    print(f"\n[4/5] Connecting to Qdrant at {QDRANT_URL}...")
    client = connect_qdrant()
    maybe_recreate_collection(client)

    if COLLECTION not in {c.name for c in client.get_collections().collections}:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=qm.VectorParams(size=VECTOR_DIM, distance=qm.Distance.COSINE),
        )
        print(f"  Created collection '{COLLECTION}' (dim={VECTOR_DIM}, cosine)")

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
    info = client.get_collection(COLLECTION)
    print(f"\n  Done: {total_upserted} upserted, {skipped} skipped, {elapsed:.1f}s")
    print(f"\n{'=' * 60}")
    print(f"  Collection '{COLLECTION}': {info.points_count} points")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
