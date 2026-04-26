# from __future__ import annotations

import argparse
import glob
import json
import os
import re
from dataclasses import dataclass
from typing import Iterable, List, Dict, Any, Optional, Tuple

from qdrant_client import QdrantClient
from qdrant_client.http import models as qm
from fastembed import TextEmbedding
import uuid

from .ingest_utils import (
    read_file,
    normalize_whitespace,
    split_by_headings,
    sanitize_for_id,
    Section,
)
from .document_schema import DocumentRecord, iter_jsonl
from .ingest_quality import (
    classify_section_title,
    evaluate_document_quality,
    infer_chunk_role,
    passes_quality_gate,
    reference_line_ratio,
    should_skip_chunk,
    table_line_ratio,
)

# Optional GCS support (kept optional to avoid hard dependency if you don't need it)
try:
    from google.cloud import storage  # type: ignore
except Exception:  # pragma: no cover
    storage = None


# ── Chunk dataclass ──────────────────────────────────────────────────
@dataclass
class Chunk:
    id: str
    text: str
    metadata: Dict[str, Any]


# ── Character-based chunking (legacy) ────────────────────────────────
def chunk_text(text: str, chunk_size: int = 900, overlap: int = 150) -> List[str]:
    """
    Simple character-based chunker with overlap.
    - chunk_size: target size in characters
    - overlap: overlap between consecutive chunks
    """
    text = normalize_whitespace(text)
    if not text:
        return []
    if overlap >= chunk_size:
        overlap = max(0, chunk_size // 4)

    chunks: List[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(n, start + chunk_size)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= n:
            break
        start = max(0, end - overlap)
    return chunks


# ── Structure-aware chunking ─────────────────────────────────────────
def _render_context_header(
    title: str = "",
    section_title: str = "",
    source_name: str = "",
    updated_at: str = "",
    audience: str = "",
) -> str:
    """
    Prepend metadata context into the chunk text.
    This helps the embedding model capture topic/source semantics.
    """
    parts: List[str] = []
    if title:
        parts.append(f"Title: {title}")
    if section_title:
        parts.append(f"Section: {section_title}")
    if source_name:
        parts.append(f"Source: {source_name}")
    if updated_at:
        parts.append(f"Updated: {updated_at}")
    if audience:
        parts.append(f"Audience: {audience}")
    if parts:
        return "\n".join(parts) + "\nBody:\n"
    return ""


def _strip_context_header(text: str) -> str:
    if "\nBody:\n" not in text:
        return text
    return text.split("\nBody:\n", 1)[1]


def chunk_by_structure(
    body: str,
    *,
    title: str = "",
    source_name: str = "",
    updated_at: str = "",
    audience: str = "",
    chunk_size: int = 900,
    overlap: int = 150,
    sections: Optional[List[Section]] = None,
) -> List[Tuple[str, str]]:
    """
    Chunk a document body using heading structure when available.

    Returns a list of (section_title_or_heading_path, chunk_text) tuples.
    Falls back to character chunking when no headings are found.
    """
    sections = sections if sections is not None else split_by_headings(body)

    if not sections:
        # Fallback: character-based chunking
        header = _render_context_header(
            title=title,
            source_name=source_name,
            updated_at=updated_at,
            audience=audience,
        )
        raw_chunks = chunk_text(body, chunk_size=chunk_size, overlap=overlap)
        return [("", header + ch) for ch in raw_chunks]

    result: List[Tuple[str, str]] = []
    for sec in sections:
        if not sec.body.strip():
            continue

        header = _render_context_header(
            title=title,
            section_title=sec.title,
            source_name=source_name,
            updated_at=updated_at,
            audience=audience,
        )

        # If this section is small enough, keep it as one chunk
        full_text = header + sec.body
        if len(full_text) <= chunk_size:
            result.append((sec.heading_path, full_text))
        else:
            # Sub-chunk large sections
            sub_chunks = chunk_text(sec.body, chunk_size=chunk_size, overlap=overlap)
            for sub in sub_chunks:
                result.append((sec.heading_path, header + sub))

    return result


# ── Stable chunk ID generation ───────────────────────────────────────
def generate_stable_id(
    source_name: str,
    doc_id: str,
    section_slug: str,
    chunk_idx: int,
) -> str:
    """
    Create a human-readable, deterministic chunk ID.

    Example: "medlineplus_hypertension_diagnosis_chunk02"
    """
    src = sanitize_for_id(source_name, max_len=20)
    did = sanitize_for_id(doc_id, max_len=40)
    sec = sanitize_for_id(section_slug, max_len=30) if section_slug else "main"
    return f"{src}_{did}_{sec}_chunk{chunk_idx:02d}"


# ── Qdrant helpers ───────────────────────────────────────────────────
def ensure_collection(client: QdrantClient, collection: str, vector_size: int):
    existing = {c.name for c in client.get_collections().collections}
    if collection in existing:
        return
    client.create_collection(
        collection_name=collection,
        vectors_config=qm.VectorParams(size=vector_size, distance=qm.Distance.COSINE),
    )


def upsert_chunks(
    client: QdrantClient,
    collection: str,
    embedder: TextEmbedding,
    chunks: List[Chunk],
    batch_size: int = 64,
    remote_embed_url: str = None,
):
    # embed + upsert in batches
    for i in range(0, len(chunks), batch_size):
        batch = chunks[i : i + batch_size]
        
        # Check if batch already has precomputed embeddings (e.g. from Kaggle GPU)
        has_precomputed = all(hasattr(c, "embedding") and c.embedding is not None for c in batch)
        
        if has_precomputed:
            vectors = [c.embedding for c in batch]
        elif remote_embed_url:
            import requests
            texts = [c.text for c in batch]
            resp = requests.post(
                f"{remote_embed_url.rstrip('/')}/embed", 
                json={"texts": texts},
                timeout=120
            )
            resp.raise_for_status()
            vectors = resp.json().get("embeddings", [])
        else:
            texts = [c.text for c in batch]
            vectors = [v.tolist() for v in embedder.embed(texts)]
            
        points = []
        for ch, vec in zip(batch, vectors):
            # Build payload: metadata fields as top-level keys for Qdrant filtering
            payload = {"text": ch.text}
            payload.update(ch.metadata)
            payload["human_id"] = ch.id  # Keep original human-readable string ID
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, ch.id))
            points.append(
                qm.PointStruct(
                    id=point_id,
                    vector=vec,
                    payload=payload,
                )
            )
        client.upsert(collection_name=collection, points=points)


# ── Legacy local file ingestion ──────────────────────────────────────
def iter_local_files(path: str, patterns: List[str]) -> Iterable[str]:
    for pat in patterns:
        yield from sorted(glob.glob(os.path.join(path, pat)))


def ingest_local_path(
    input_path: str,
    collection: str,
    source_name: str,
    patterns: List[str],
    chunk_size: int,
    overlap: int,
) -> List[Chunk]:
    files = list(iter_local_files(input_path, patterns))
    if not files:
        raise SystemExit(f"No matching files in {input_path} for patterns {patterns}")

    chunks: List[Chunk] = []
    for fp in files:
        raw = read_file(fp)
        txt = normalize_whitespace(raw)
        rel = os.path.relpath(fp, input_path)
        doc_id = rel.replace(os.sep, "/")
        for idx, ch in enumerate(chunk_text(txt, chunk_size=chunk_size, overlap=overlap)):
            # cid = f"{source_name}:{doc_id}#{idx}"
            cid = str(uuid.uuid4())
            chunks.append(
                Chunk(
                    id=cid,
                    text=ch,
                    metadata={
                        "source": source_name,
                        "document": doc_id,
                        "chunk_index": idx,
                    },
                )
            )
    return chunks


# ── Enriched JSONL ingestion ─────────────────────────────────────────
def ingest_enriched_jsonl(
    input_path: str,
    patterns: List[str],
    chunk_size: int,
    overlap: int,
    min_quality_status: str = "hold",
) -> List[Chunk]:
    """
    Ingest documents from enriched JSONL files conforming to DocumentRecord schema.

    Each record is chunked using structure-aware splitting, and every chunk
    carries full metadata for downstream Qdrant filtering and citation.
    """
    jsonl_patterns = [p for p in patterns if p.lower().endswith(".jsonl")] or ["*.jsonl"]
    jsonl_files = list(iter_local_files(input_path, jsonl_patterns))
    if not jsonl_files:
        raise SystemExit(f"No .jsonl files found in {input_path}")

    chunks: List[Chunk] = []
    seen_ids: set = set()
    errors: List[str] = []
    skipped_quality_records = 0
    skipped_noise_chunks = 0

    for fp in jsonl_files:
        for record in iter_jsonl(fp):
            # Validate
            validation_errors = record.validate()
            if validation_errors:
                errors.append(f"{fp}: doc_id={record.doc_id!r} – {'; '.join(validation_errors)}")
                continue

            effective_title = (record.canonical_title or record.title).strip() or record.title
            sections = split_by_headings(record.body)
            quality_input = record.to_dict()
            quality_input["title"] = effective_title
            quality_input["canonical_title"] = effective_title
            quality_input["_section_count"] = len(sections)
            quality = evaluate_document_quality(quality_input)
            if not passes_quality_gate(quality, min_quality_status=min_quality_status):
                skipped_quality_records += 1
                continue

            section_chunks = chunk_by_structure(
                record.body,
                title=effective_title,
                source_name=record.source_name,
                updated_at=record.updated_at,
                audience=record.audience,
                chunk_size=chunk_size,
                overlap=overlap,
                sections=sections,
            )

            for idx, (heading_path, chunk_text_content) in enumerate(section_chunks):
                section_title = heading_path.split(" > ")[-1].strip() if heading_path else record.section_title.strip()
                section_type = classify_section_title(section_title)
                body_text = _strip_context_header(chunk_text_content)
                if should_skip_chunk(section_type, body_text):
                    skipped_noise_chunks += 1
                    continue

                chunk_ref_ratio = reference_line_ratio(body_text)
                chunk_table_ratio = table_line_ratio(body_text)
                section_slug = section_title or record.section_title or "main"
                cid = generate_stable_id(
                    source_name=record.source_name,
                    doc_id=record.doc_id,
                    section_slug=section_slug,
                    chunk_idx=idx,
                )

                # Ensure uniqueness
                original_cid = cid
                counter = 1
                while cid in seen_ids:
                    cid = f"{original_cid}_{counter}"
                    counter += 1
                seen_ids.add(cid)

                metadata = {
                    "doc_id": record.doc_id,
                    "title": effective_title,
                    "raw_title": record.title,
                    "canonical_title": effective_title,
                    "section_title": section_title,
                    "section_type": section_type,
                    "chunk_role": infer_chunk_role(section_type),
                    "source_name": record.source_name,
                    "source_id": record.source_id or record.source_url or record.source_name,
                    "source_url": record.source_url,
                    "source_file": record.source_file,
                    "raw_path": record.raw_path,
                    "processed_path": record.processed_path,
                    "intermediate_path": record.intermediate_path,
                    "parent_file": record.parent_file,
                    "source_sha256": record.source_sha256,
                    "crawl_run_id": record.crawl_run_id,
                    "etl_run_id": record.etl_run_id,
                    "doc_type": record.doc_type,
                    "specialty": record.specialty,
                    "audience": record.audience,
                    "language": record.language,
                    "language_confidence": record.language_confidence,
                    "is_mixed_language": record.is_mixed_language,
                    "trust_tier": record.trust_tier,
                    "published_at": record.published_at,
                    "updated_at": record.updated_at,
                    "tags": record.tags,
                    "heading_path": heading_path or record.heading_path,
                    "chunk_index": idx,
                    "quality_score": quality["quality_score"],
                    "quality_status": quality["quality_status"],
                    "quality_flags": quality["quality_flags"],
                    "document_reference_line_ratio": quality["reference_line_ratio"],
                    "document_table_line_ratio": quality["table_line_ratio"],
                    "reference_line_ratio": chunk_ref_ratio,
                    "table_line_ratio": chunk_table_ratio,
                }

                chunks.append(Chunk(id=cid, text=chunk_text_content, metadata=metadata))

    if errors:
        print(f"[WARN] {len(errors)} validation error(s) during enriched ingest:")
        for e in errors[:10]:
            print(f"  - {e}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")
    if skipped_quality_records:
        print(f"[INFO] Skipped {skipped_quality_records} record(s) below quality gate '{min_quality_status}'.")
    if skipped_noise_chunks:
        print(f"[INFO] Skipped {skipped_noise_chunks} noisy/reference chunk(s).")

    return chunks


# ── GCS helpers (unchanged) ──────────────────────────────────────────
def list_gcs_blobs(gcs_uri: str):
    if storage is None:
        raise SystemExit(
            "google-cloud-storage is not installed. "
            "Add it to requirements.txt to use --gcs-uri."
        )

    if not gcs_uri.startswith("gs://"):
        raise SystemExit("--gcs-uri must start with gs://")

    _, _, rest = gcs_uri.partition("gs://")
    bucket_name, _, prefix = rest.partition("/")
    prefix = prefix.strip("/")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = list(client.list_blobs(bucket, prefix=prefix))
    if not blobs:
        raise SystemExit(f"No blobs found under {gcs_uri}")

    return bucket_name, blobs


def resolve_allowed_suffixes(patterns: List[str]) -> List[str]:
    allowed_suffixes = []
    for p in patterns:
        if p.startswith("*."):
            allowed_suffixes.append(p[1:])  # ".txt", ".md", ".jsonl"

    if not allowed_suffixes:
        allowed_suffixes = [".txt", ".md", ".jsonl"]

    return allowed_suffixes

def blob_to_chunks(
    blob,
    bucket_name: str,
    source_name: str,
    chunk_size: int,
    overlap: int,
) -> List[Chunk]:
    raw = blob.download_as_text(encoding="utf-8", errors="ignore")
    txt = normalize_whitespace(raw)
    doc_id = blob.name

    chunks: List[Chunk] = []
    for idx, ch in enumerate(chunk_text(txt, chunk_size=chunk_size, overlap=overlap)):
        cid = f"{source_name}:{doc_id}#{idx}"
        chunks.append(
            Chunk(
                id=cid,
                text=ch,
                metadata={
                    "source": source_name,
                    "document": doc_id,
                    "chunk_index": idx,
                    "gcs_uri": f"gs://{bucket_name}/{blob.name}",
                },
            )
        )
    return chunks

def ingest_gcs_prefix(
    gcs_uri: str,
    collection: str,
    source_name: str,
    patterns: List[str],
    chunk_size: int,
    overlap: int,
) -> List[Chunk]:

    bucket_name, blobs = list_gcs_blobs(gcs_uri)
    allowed_suffixes = resolve_allowed_suffixes(patterns)

    chunks: List[Chunk] = []

    for blob in blobs:
        if not any(blob.name.endswith(suf) for suf in allowed_suffixes):
            continue
        chunks.extend(
            blob_to_chunks(
                blob,
                bucket_name=bucket_name,
                source_name=source_name,
                chunk_size=chunk_size,
                overlap=overlap,
            )
        )

    if not chunks:
        raise SystemExit(f"No matching .txt/.md/.jsonl blobs under {gcs_uri}")

    return chunks


# ── CLI entrypoint ───────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Ingest documents into Qdrant for RAG.")
    ap.add_argument("--qdrant-url", required=True, help="e.g. http://qdrant:6333")
    ap.add_argument("--collection", default=os.getenv("QDRANT_COLLECTION", "medical_docs"))
    ap.add_argument("--embedding-model", default=os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5"))
    ap.add_argument("--top-level-path", default="/data", help="Local mount path for docs (used with --input-path)")
    ap.add_argument("--input-path", default=".", help="Relative to --top-level-path when running in cluster")
    ap.add_argument("--gcs-uri", default="", help="gs://bucket/prefix (optional alternative to local input)")
    ap.add_argument("--source-name", default="medical_corpus")
    ap.add_argument("--patterns", default="*.txt,*.md,*.jsonl", help="Comma-separated glob patterns")
    ap.add_argument("--chunk-size", type=int, default=900)
    ap.add_argument("--overlap", type=int, default=150)
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument(
        "--min-quality-status",
        choices=["hold", "review", "go"],
        default=os.getenv("INGEST_MIN_QUALITY_STATUS", "hold"),
        help="Minimum document quality status allowed for enriched JSONL ingest.",
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--mode",
        choices=["legacy", "enriched"],
        default="legacy",
        help=(
            "'legacy' = original char-based chunking (backward compatible). "
            "'enriched' = structure-aware chunking from JSONL with rich metadata."
        ),
    )
    args = ap.parse_args()

    patterns = [p.strip() for p in args.patterns.split(",") if p.strip()]

    qclient = QdrantClient(url=args.qdrant_url)
    embedder = TextEmbedding(model_name=args.embedding_model)

    # discover embedding vector size
    vec_size = len(next(embedder.embed(["vector size probe"])).tolist())
    ensure_collection(qclient, args.collection, vec_size)

    if args.mode == "enriched":
        base = os.path.join(args.top_level_path, args.input_path)
        chunks = ingest_enriched_jsonl(
            input_path=base,
            patterns=patterns,
            chunk_size=args.chunk_size,
            overlap=args.overlap,
            min_quality_status=args.min_quality_status,
        )
    elif args.gcs_uri.strip():
        chunks = ingest_gcs_prefix(
            gcs_uri=args.gcs_uri.strip(),
            collection=args.collection,
            source_name=args.source_name,
            patterns=patterns,
            chunk_size=args.chunk_size,
            overlap=args.overlap,
        )
    else:
        base = os.path.join(args.top_level_path, args.input_path)
        chunks = ingest_local_path(
            input_path=base,
            collection=args.collection,
            source_name=args.source_name,
            patterns=patterns,
            chunk_size=args.chunk_size,
            overlap=args.overlap,
        )

    print(json.dumps({"collection": args.collection, "chunks": len(chunks), "mode": args.mode}, indent=2))

    if args.dry_run:
        # Print sample chunk for verification
        if chunks:
            sample = chunks[0]
            print("\n--- Sample chunk ---")
            print(f"ID:   {sample.id}")
            print(f"Meta: {json.dumps(sample.metadata, indent=2, ensure_ascii=False)}")
            print(f"Text: {sample.text[:300]}...")
        return

    upsert_chunks(
        client=qclient,
        collection=args.collection,
        embedder=embedder,
        chunks=chunks,
        batch_size=args.batch_size,
    )
    print(f" Upserted {len(chunks)} chunks into '{args.collection}' at {args.qdrant_url}.")


if __name__ == "__main__":
    main()
