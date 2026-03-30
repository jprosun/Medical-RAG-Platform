"""
Standalone script to ingest enriched JSONL data into Qdrant staging collection.
No Docker required – just Qdrant running on localhost:6333.

Usage:
  cd services/qdrant-ingestor
  python ingest_staging.py
"""

import json
import os
import sys
import time
import tempfile
import shutil

# Add parent paths so imports work
sys.path.insert(0, os.path.abspath("."))

from app.ingest import (
    ingest_enriched_jsonl,
    ensure_collection,
    upsert_chunks,
)
from qdrant_client import QdrantClient
from fastembed import TextEmbedding

# ── Configuration ───────────────────────────────────────────────────
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "staging_medqa")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
DATA_PATH = os.getenv("DATA_PATH", "../../data/data_final")
DATA_FILE = os.getenv("DATA_FILE", "combined.jsonl")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "900"))
OVERLAP = int(os.getenv("OVERLAP", "150"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "64"))


def main():
    print("=" * 60)
    print("  RAG Staging Ingestion")
    print("=" * 60)
    print(f"  Qdrant URL   : {QDRANT_URL}")
    print(f"  Collection   : {COLLECTION}")
    print(f"  Embedding    : {EMBEDDING_MODEL}")
    print(f"  Data file    : {DATA_FILE}")
    print(f"  Chunk size   : {CHUNK_SIZE} / overlap {OVERLAP}")
    print("=" * 60)

    # Resolve source file
    abs_data_path = os.path.abspath(DATA_PATH)
    source_file = os.path.join(abs_data_path, DATA_FILE)
    if not os.path.isfile(source_file):
        print(f"[ERROR] Data file not found: {source_file}")
        sys.exit(1)

    # Use a temp directory with only the single JSONL file
    # to avoid ingesting duplicate individual source files
    tmpdir = tempfile.mkdtemp(prefix="staging_ingest_")
    try:
        # Create a symlink (or copy on Windows) to the data file
        tmp_file = os.path.join(tmpdir, DATA_FILE)
        try:
            os.symlink(source_file, tmp_file)
        except (OSError, NotImplementedError):
            shutil.copy2(source_file, tmp_file)

        print(f"\n[1/4] Chunking documents (structure-aware)...")
        t0 = time.time()
        chunks = ingest_enriched_jsonl(
            input_path=tmpdir,
            patterns=["*.jsonl"],
            chunk_size=CHUNK_SIZE,
            overlap=OVERLAP,
        )
        chunk_time = time.time() - t0
        print(f"  → {len(chunks)} chunks created in {chunk_time:.1f}s")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    if not chunks:
        print("[ERROR] No chunks generated. Check data files.")
        sys.exit(1)

    # Show sample
    sample = chunks[0]
    print(f"\n[Sample chunk]")
    print(f"  ID:       {sample.id}")
    print(f"  Metadata: {json.dumps(sample.metadata, indent=4, ensure_ascii=False)[:500]}")
    print(f"  Text:     {sample.text[:200]}...")

    # Source distribution
    source_counts = {}
    for c in chunks:
        src = c.metadata.get("source_name", "unknown")
        source_counts[src] = source_counts.get(src, 0) + 1
    print(f"\n[Chunk distribution by source]")
    for src, cnt in sorted(source_counts.items()):
        print(f"  {src}: {cnt} chunks")

    # Connect to Qdrant
    print(f"\n[2/4] Connecting to Qdrant at {QDRANT_URL}...")
    qclient = QdrantClient(url=QDRANT_URL)

    # Build embedder and detect vector size
    print(f"\n[3/4] Loading embedding model: {EMBEDDING_MODEL}...")
    embedder = TextEmbedding(model_name=EMBEDDING_MODEL)
    vec_size = len(next(embedder.embed(["vector size probe"])).tolist())
    print(f"  → Vector dimension: {vec_size}")

    # Ensure collection exists (recreate if needed for clean staging)
    existing = {c.name for c in qclient.get_collections().collections}
    if COLLECTION in existing:
        print(f"  → Deleting existing collection '{COLLECTION}' for clean staging...")
        qclient.delete_collection(COLLECTION)
    ensure_collection(qclient, COLLECTION, vec_size)
    print(f"  → Collection '{COLLECTION}' ready")

    # Upsert
    print(f"\n[4/4] Upserting {len(chunks)} chunks (batch size={BATCH_SIZE})...")
    t0 = time.time()
    upsert_chunks(
        client=qclient,
        collection=COLLECTION,
        embedder=embedder,
        chunks=chunks,
        batch_size=BATCH_SIZE,
    )
    upsert_time = time.time() - t0
    print(f"  → Upserted in {upsert_time:.1f}s")

    # Verify
    info = qclient.get_collection(COLLECTION)
    print(f"\n{'=' * 60}")
    print(f"  DONE! Collection '{COLLECTION}' now has {info.points_count} points")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
