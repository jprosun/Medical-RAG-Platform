from __future__ import annotations

import argparse
import gc
import json
import os
import time
from pathlib import Path

import numpy as np
import torch
from sentence_transformers import SentenceTransformer


DEFAULT_MODEL = "BAAI/bge-m3"
DEFAULT_BATCH_SIZE = 64
DEFAULT_INPUT_NAME = "chunk_texts_for_embed.jsonl"


def _find_input_jsonl(preferred_path: str, preferred_name: str) -> Path:
    if preferred_path:
        path = Path(preferred_path)
        if not path.exists():
            raise FileNotFoundError(f"Input path not found: {path}")
        return path

    candidates: list[Path] = []
    for root, _, files in os.walk("/kaggle/input"):
        for name in files:
            if name == preferred_name:
                candidates.append(Path(root) / name)

    if not candidates:
        for root, _, files in os.walk("/kaggle/input"):
            for name in files:
                if name.endswith(".jsonl"):
                    candidates.append(Path(root) / name)

    if not candidates:
        raise FileNotFoundError("No JSONL input found under /kaggle/input")

    candidates.sort()
    return candidates[0]


def _load_rows(input_path: Path, max_rows: int = 0) -> tuple[list[str], list[str]]:
    ids: list[str] = []
    texts: list[str] = []
    seen: set[str] = set()

    with open(input_path, "r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            if not raw.strip():
                continue
            record = json.loads(raw)
            chunk_id = str(record["id"])
            if chunk_id in seen:
                raise ValueError(f"Duplicate chunk id at line {line_no}: {chunk_id}")
            seen.add(chunk_id)
            ids.append(chunk_id)
            texts.append(str(record["text"]))
            if max_rows and len(ids) >= max_rows:
                break

    if not ids:
        raise ValueError("Input JSONL contained no usable rows")
    return ids, texts


def _gpu_mem_gb() -> str:
    if not torch.cuda.is_available():
        return "cpu"
    used = torch.cuda.memory_allocated(0) / 1024 / 1024 / 1024
    reserved = torch.cuda.memory_reserved(0) / 1024 / 1024 / 1024
    total = torch.cuda.get_device_properties(0).total_memory / 1024 / 1024 / 1024
    return f"{used:.1f}/{reserved:.1f}/{total:.1f} GB"


def _log(msg: str) -> None:
    now = time.strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline multilingual embedding on Kaggle with terminal logs.")
    parser.add_argument("--input-path", default="", help="Absolute path to chunk_texts_for_embed.jsonl")
    parser.add_argument("--input-name", default=DEFAULT_INPUT_NAME, help="Preferred file name to auto-discover")
    parser.add_argument("--model-name", default=DEFAULT_MODEL)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-rows", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=50, help="Print a progress line every N batches")
    parser.add_argument("--no-normalize", action="store_true", help="Disable embedding normalization")
    parser.add_argument("--output-dir", default="/kaggle/working")
    args = parser.parse_args()

    normalize_embeddings = not args.no_normalize
    input_path = _find_input_jsonl(args.input_path, args.input_name)
    _log(f"Input: {input_path}")

    ids, texts = _load_rows(input_path, max_rows=args.max_rows)
    avg_chars = sum(len(t) for t in texts) / max(1, len(texts))
    _log(f"Loaded {len(texts)} chunks | avg_chars={avg_chars:.0f}")
    _log(f"Sample ID: {ids[0]}")
    _log(f"Sample text[:160]: {texts[0][:160].replace(chr(10), ' ')}")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    _log(f"Device: {device}")
    if device == "cuda":
        _log(f"GPU: {torch.cuda.get_device_name(0)} | mem={_gpu_mem_gb()}")

    _log(f"Loading model: {args.model_name}")
    model = SentenceTransformer(args.model_name, device=device)
    dim = model.get_sentence_embedding_dimension()
    _log(f"Model loaded | embedding_dim={dim}")

    _log("Warmup...")
    _ = model.encode(["warmup"], normalize_embeddings=normalize_embeddings, show_progress_bar=False)
    gc.collect()
    if device == "cuda":
        torch.cuda.empty_cache()
        _log(f"Warmup done | gpu_mem={_gpu_mem_gb()}")
    else:
        _log("Warmup done")

    t0 = time.time()
    all_emb: list[np.ndarray] = []
    total = len(texts)
    total_batches = (total + args.batch_size - 1) // args.batch_size
    _log(f"Start embedding | total_chunks={total} | batch_size={args.batch_size} | total_batches={total_batches}")

    for batch_index, start in enumerate(range(0, total, args.batch_size), start=1):
        batch = texts[start : start + args.batch_size]
        vecs = model.encode(
            batch,
            batch_size=args.batch_size,
            normalize_embeddings=normalize_embeddings,
            show_progress_bar=False,
            convert_to_numpy=True,
        )
        all_emb.append(vecs)

        should_log = (
            batch_index == 1
            or batch_index == total_batches
            or batch_index % max(1, args.progress_every) == 0
        )
        if should_log:
            done = min(start + len(batch), total)
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0.0
            remaining = total - done
            eta_s = remaining / rate if rate > 0 else 0.0
            eta_m = eta_s / 60
            pct = done / total * 100
            msg = (
                f"Progress {done}/{total} ({pct:.1f}%) | "
                f"batch={batch_index}/{total_batches} | "
                f"elapsed={elapsed/60:.1f}m | rate={rate:.0f} vec/s | ETA={eta_m:.1f}m"
            )
            if device == "cuda":
                msg += f" | gpu_mem={_gpu_mem_gb()}"
            _log(msg)

    elapsed = time.time() - t0
    emb_array = np.vstack(all_emb).astype(np.float32)
    norms = np.linalg.norm(emb_array[: min(5, len(emb_array))], axis=1)
    _log(
        f"Embedding complete | shape={emb_array.shape} | dtype={emb_array.dtype} | "
        f"time={elapsed/60:.1f}m | avg_rate={emb_array.shape[0]/elapsed:.0f} vec/s"
    )
    _log(f"Sample norms: {norms}")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    emb_path = out_dir / "embeddings.npy"
    ids_path = out_dir / "chunk_ids.json"
    manifest_path = out_dir / "embedding_manifest.json"

    _log(f"Saving embeddings -> {emb_path}")
    np.save(emb_path, emb_array)
    _log(f"Saving chunk ids -> {ids_path}")
    with open(ids_path, "w", encoding="utf-8") as fh:
        json.dump(ids, fh, ensure_ascii=False)

    manifest = {
        "model_name": args.model_name,
        "batch_size": args.batch_size,
        "normalize_embeddings": normalize_embeddings,
        "input_path": str(input_path),
        "chunk_count": len(ids),
        "vector_shape": list(emb_array.shape),
        "dtype": str(emb_array.dtype),
        "elapsed_seconds": round(elapsed, 2),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"Saved manifest -> {manifest_path}")
    _log(f"Done | embeddings_mb={emb_array.nbytes/1024/1024:.1f}")


if __name__ == "__main__":
    main()
