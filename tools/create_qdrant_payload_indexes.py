"""Create Qdrant payload indexes used by the RAG runtime.

This is a runtime-only optimization. It does not modify vectors, embeddings,
payload values, or source data.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qm
except Exception as exc:  # pragma: no cover - dependency/runtime guard
    print(f"[ERROR] qdrant-client is required: {exc}", file=sys.stderr)
    sys.exit(2)


KEYWORD_FIELDS = [
    "source_id",
    "source_name",
    "doc_type",
    "audience",
    "language",
    "specialty",
    "article_id",
    "doc_id",
    "title",
    "canonical_title",
    "section_title",
    "section_type",
    "chunk_role",
    "institution",
    "quality_status",
]

INTEGER_FIELDS = [
    "trust_tier",
    "chunk_index",
]


def _schema_name(schema: Any) -> str:
    return getattr(schema, "value", str(schema))


def _create_index(
    client: QdrantClient,
    *,
    collection: str,
    field_name: str,
    field_schema: Any,
    wait: bool,
) -> str:
    try:
        client.create_payload_index(
            collection_name=collection,
            field_name=field_name,
            field_schema=field_schema,
            wait=wait,
        )
        return "created"
    except Exception as exc:
        message = str(exc).lower()
        if "already exists" in message or "exists" in message:
            return "exists"
        return f"error: {exc}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create idempotent Qdrant payload indexes for RAG filters.",
    )
    parser.add_argument(
        "--qdrant-url",
        default=os.getenv("QDRANT_URL", "http://localhost:6333"),
        help="Qdrant URL. Defaults to QDRANT_URL or http://localhost:6333.",
    )
    parser.add_argument(
        "--collection",
        default=os.getenv("QDRANT_COLLECTION", "medqa_release_v4_all_bge_m3"),
        help="Qdrant collection name. Defaults to QDRANT_COLLECTION.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("QDRANT_TIMEOUT", "60")),
        help="Client timeout in seconds.",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Do not wait for each index operation to complete.",
    )
    parser.add_argument(
        "--verify-count",
        action="store_true",
        help="Print exact collection count after creating indexes.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = QdrantClient(url=args.qdrant_url, timeout=args.timeout)

    collection_info = client.get_collection(args.collection)
    approx_count = getattr(collection_info, "points_count", None)
    print(f"[Qdrant] collection={args.collection} url={args.qdrant_url}")
    if approx_count is not None:
        print(f"[Qdrant] points_count={approx_count}")

    schemas: list[tuple[str, Any]] = [
        (field, qm.PayloadSchemaType.KEYWORD) for field in KEYWORD_FIELDS
    ]
    schemas.extend((field, qm.PayloadSchemaType.INTEGER) for field in INTEGER_FIELDS)

    wait = not args.no_wait
    failures = 0
    for field_name, schema in schemas:
        status = _create_index(
            client,
            collection=args.collection,
            field_name=field_name,
            field_schema=schema,
            wait=wait,
        )
        print(f"[index] {field_name:<20} {_schema_name(schema):<8} {status}")
        if status.startswith("error:"):
            failures += 1

    if args.verify_count:
        exact_count = client.count(
            collection_name=args.collection,
            exact=True,
        ).count
        print(f"[Qdrant] exact_count={exact_count}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
