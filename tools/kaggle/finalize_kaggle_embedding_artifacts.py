"""Finalize Kaggle embedding outputs into the canonical staging layout.

Expected Kaggle output files:
  - embeddings.npy
  - chunk_ids.json
  - embedding_manifest.json

The script validates vector/id alignment, copies artifacts into
rag-data/embeddings/staging/<dataset_id>/<profile>/, then runs the repository
embedding artifact audit against exports + staging.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from services.utils import data_paths  # noqa: E402
from tools.audit_embedding_artifacts import audit_embedding_artifacts, write_report  # noqa: E402


DEFAULT_DATASET_ID = "medqa_release_v3_all_open_enriched"
DEFAULT_COLLECTION = "medqa_release_v3_all_bge_m3"
REQUIRED_FILES = ("embeddings.npy", "chunk_ids.json", "embedding_manifest.json")


def _load_chunk_ids(path: Path) -> list[str]:
    with open(path, "r", encoding="utf-8") as fh:
        ids = json.load(fh)
    if not isinstance(ids, list) or not all(isinstance(item, str) for item in ids):
        raise ValueError(f"{path} must be a JSON list of string chunk ids")
    if not ids:
        raise ValueError(f"{path} is empty")
    duplicate_count = len(ids) - len(set(ids))
    if duplicate_count:
        raise ValueError(f"{path} contains {duplicate_count} duplicate chunk ids")
    return ids


def _validate_input_dir(input_dir: Path, expected_dim: int) -> dict[str, Any]:
    missing = [name for name in REQUIRED_FILES if not (input_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing Kaggle output file(s): {', '.join(missing)}")

    ids_path = input_dir / "chunk_ids.json"
    vectors_path = input_dir / "embeddings.npy"
    manifest_path = input_dir / "embedding_manifest.json"

    ids = _load_chunk_ids(ids_path)
    vectors = np.load(vectors_path, mmap_mode="r")
    if len(vectors.shape) != 2:
        raise ValueError(f"{vectors_path} must be a 2D array, got shape={vectors.shape}")
    if vectors.shape[0] != len(ids):
        raise ValueError(
            f"Vector/id mismatch: embeddings rows={vectors.shape[0]}, chunk_ids={len(ids)}"
        )
    if expected_dim and vectors.shape[1] != expected_dim:
        raise ValueError(f"Expected vector dim={expected_dim}, got dim={vectors.shape[1]}")

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON manifest: {manifest_path}") from exc

    manifest_count = manifest.get("chunk_count")
    if isinstance(manifest_count, int) and manifest_count != len(ids):
        raise ValueError(f"Manifest chunk_count={manifest_count}, chunk_ids={len(ids)}")

    return {
        "chunk_count": len(ids),
        "vector_shape": list(vectors.shape),
        "vector_dtype": str(vectors.dtype),
        "manifest_model": manifest.get("model_name", ""),
    }


def _copy_required_files(input_dir: Path, staging_dir: Path, *, overwrite: bool, move: bool) -> dict[str, str]:
    staging_dir.mkdir(parents=True, exist_ok=True)
    copied: dict[str, str] = {}
    for name in REQUIRED_FILES:
        src = input_dir / name
        dst = staging_dir / name
        if dst.exists() and not overwrite:
            raise FileExistsError(f"Refusing to overwrite existing file: {dst}")
        if move:
            shutil.move(str(src), str(dst))
        else:
            shutil.copy2(src, dst)
        copied[name] = str(dst)
    return copied


def finalize_artifacts(
    *,
    input_dir: Path,
    dataset_id: str = DEFAULT_DATASET_ID,
    profile: str = data_paths.DEFAULT_EMBEDDING_PROFILE,
    collection: str = DEFAULT_COLLECTION,
    expected_dim: int = 1024,
    overwrite: bool = False,
    move: bool = False,
    audit_output: Path | None = None,
) -> dict[str, Any]:
    input_dir = input_dir.resolve()
    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(input_dir)

    validation = _validate_input_dir(input_dir, expected_dim=expected_dim)
    data_paths.ensure_rag_data_layout(dataset_ids=[dataset_id], embedding_profiles=[profile])
    staging_dir = data_paths.embeddings_staging_dir(profile, dataset_id=dataset_id)
    copied = _copy_required_files(input_dir, staging_dir, overwrite=overwrite, move=move)

    report = audit_embedding_artifacts(dataset_id, profile)
    if audit_output is None:
        audit_output = data_paths.dataset_qa_dir(dataset_id) / "post_kaggle_embedding_artifact_audit.json"
    report_path = write_report(report, audit_output)
    if report["status"] != "pass":
        raise RuntimeError(f"Post-Kaggle artifact audit failed. Report: {report_path}")

    return {
        "status": "pass",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset_id": dataset_id,
        "profile": profile,
        "collection": collection,
        "input_dir": str(input_dir),
        "staging_dir": str(staging_dir),
        "copied": copied,
        "validation": validation,
        "audit_report": str(report_path),
        "import_commands": {
            "powershell": (
                f"$env:EMBED_DATASET_ID='{dataset_id}'; "
                f"$env:KAGGLE_PROFILE='{profile}'; "
                f"$env:QDRANT_COLLECTION='{collection}'; "
                "python services\\qdrant-ingestor\\ingest_kaggle_precomputed.py"
            ),
            "docker_compose": (
                "docker compose -f docker-compose.local.yml "
                "--profile precomputed-ingest run --rm qdrant-precomputed-ingestor"
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Finalize Kaggle embedding output artifacts.")
    parser.add_argument("--input-dir", required=True, help="Directory containing Kaggle output files.")
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID)
    parser.add_argument("--profile", default=data_paths.DEFAULT_EMBEDDING_PROFILE)
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--expected-dim", type=int, default=1024)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--move", action="store_true", help="Move files instead of copying them.")
    parser.add_argument("--audit-output", default="")
    args = parser.parse_args()

    result = finalize_artifacts(
        input_dir=Path(args.input_dir),
        dataset_id=args.dataset_id,
        profile=args.profile,
        collection=args.collection,
        expected_dim=args.expected_dim,
        overwrite=args.overwrite,
        move=args.move,
        audit_output=Path(args.audit_output) if args.audit_output else None,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
