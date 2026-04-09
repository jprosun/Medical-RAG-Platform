"""
Step 1: Export chunk texts from vmj_ojs.jsonl for offline GPU embedding.
Output: chunk_texts.jsonl (~40MB) containing {id, text} per line.
Upload this file to Kaggle as a dataset.
"""
import json, os, sys, time
sys.path.insert(0, os.path.abspath("."))

from app.ingest import ingest_enriched_jsonl

DATA_PATH = os.path.abspath("../../data/data_final")
DATA_FILE = "vmj_ojs_v2.jsonl"
OUTPUT = os.path.abspath("../../data/chunk_texts_for_embed.jsonl")

import tempfile, shutil

print("Chunking vmj_ojs.jsonl...")
t0 = time.time()

source_file = os.path.join(DATA_PATH, DATA_FILE)
tmpdir = tempfile.mkdtemp(prefix="export_")
try:
    tmp_file = os.path.join(tmpdir, DATA_FILE)
    try:
        os.symlink(source_file, tmp_file)
    except (OSError, NotImplementedError):
        shutil.copy2(source_file, tmp_file)

    chunks = ingest_enriched_jsonl(
        input_path=tmpdir,
        patterns=["*.jsonl"],
        chunk_size=900,
        overlap=150,
    )
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

print(f"  {len(chunks)} chunks in {time.time()-t0:.1f}s")

# Export id + text only (small file for upload)
print(f"Writing {OUTPUT}...")
with open(OUTPUT, 'w', encoding='utf-8') as f:
    for i, chunk in enumerate(chunks):
        f.write(json.dumps({"id": chunk.id, "text": chunk.text}, ensure_ascii=False) + "\n")

# Also save full chunk metadata for later Qdrant push
META_OUTPUT = os.path.abspath("../../data/chunk_metadata.jsonl")
with open(META_OUTPUT, 'w', encoding='utf-8') as f:
    for chunk in chunks:
        f.write(json.dumps({"id": chunk.id, "metadata": chunk.metadata}, ensure_ascii=False) + "\n")

size_mb = os.path.getsize(OUTPUT) / 1024 / 1024
print(f"Done! {OUTPUT} ({size_mb:.1f} MB)")
print(f"Metadata: {META_OUTPUT}")
print(f"\nUpload chunk_texts_for_embed.jsonl to Kaggle as a dataset.")
