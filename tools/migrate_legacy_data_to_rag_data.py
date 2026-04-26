"""Copy legacy data artifacts into the canonical `rag-data/` layout.

The script is non-destructive:
  - default mode is dry-run
  - source files are never moved or deleted
  - existing destinations with different hashes are reported as conflicts
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from services.utils.data_lineage import file_sha256
from services.utils.data_paths import (
    DEFAULT_EMBEDDING_PROFILE,
    KNOWN_SOURCE_IDS,
    LEGACY_DATA_ROOT,
    RAG_DATA_ROOT,
    chunk_metadata_export_path,
    chunk_texts_export_path,
    dataset_records_path,
    embedding_ids_path,
    embedding_vectors_path,
    legacy_intermediate_dir,
    legacy_processed_dir,
    legacy_raw_dir,
    migration_audit_path,
    source_intermediate_dir,
    source_processed_dir,
    source_raw_dir,
    source_records_path,
    source_release_records_path,
)


VMJ_RELEASE_DATASET_ID = "vmj_ojs_release_v2"
VMJ_RELEASE_ID = "v2"
ALL_CORPUS_DATASET_ID = "all_corpus_v1"


@dataclass(frozen=True)
class CopyTask:
    kind: str
    source: Path
    destination: Path
    note: str = ""


def _iter_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*") if path.is_file())


def _mirror_tree_tasks(kind: str, source_root: Path, destination_root: Path, note: str = "") -> list[CopyTask]:
    tasks: list[CopyTask] = []
    for src in _iter_files(source_root):
        tasks.append(CopyTask(kind, src, destination_root / src.relative_to(source_root), note))
    return tasks


def _legacy_data_final_tasks() -> list[CopyTask]:
    data_final = LEGACY_DATA_ROOT / "data_final"
    tasks: list[CopyTask] = []
    for source_id in KNOWN_SOURCE_IDS:
        src = data_final / f"{source_id}.jsonl"
        if src.exists():
            tasks.append(CopyTask("source_records", src, source_records_path(source_id)))

    combined = data_final / "combined.jsonl"
    if combined.exists():
        tasks.append(CopyTask("dataset_records", combined, dataset_records_path("en_core_v1"), "legacy combined alias"))

    vmj_v2 = data_final / "vmj_ojs_v2.jsonl"
    if vmj_v2.exists():
        tasks.extend(
            [
                CopyTask("source_records", vmj_v2, source_records_path("vmj_ojs"), "current VMJ source release"),
                CopyTask(
                    "source_release_records",
                    vmj_v2,
                    source_release_records_path("vmj_ojs", VMJ_RELEASE_ID),
                ),
                CopyTask("dataset_records", vmj_v2, dataset_records_path(VMJ_RELEASE_DATASET_ID)),
            ]
        )

    return tasks


def _legacy_kaggle_tasks(dataset_id: str, profile: str, *, include_exports: bool = True) -> list[CopyTask]:
    staging = LEGACY_DATA_ROOT / "kaggle_staging"
    profile_dir = staging / profile
    candidates = []
    if include_exports:
        candidates.extend(
            [
                CopyTask(
                    "embedding_export",
                    staging / "chunk_texts_for_embed.jsonl",
                    chunk_texts_export_path(dataset_id=dataset_id, profile=profile),
                ),
                CopyTask(
                    "embedding_export",
                    staging / "chunk_metadata.jsonl",
                    chunk_metadata_export_path(dataset_id=dataset_id, profile=profile),
                ),
            ]
        )

    candidates.extend(
        [
        CopyTask(
            "embedding_staging",
            profile_dir / "chunk_ids.json",
            embedding_ids_path(dataset_id=dataset_id, profile=profile),
        ),
        CopyTask(
            "embedding_staging",
            profile_dir / "embeddings.npy",
            embedding_vectors_path(dataset_id=dataset_id, profile=profile),
        ),
        ]
    )
    return [task for task in candidates if task.source.exists()]


def _legacy_raw_tasks() -> list[CopyTask]:
    tasks: list[CopyTask] = []
    for source_id in KNOWN_SOURCE_IDS:
        tasks.extend(
            _mirror_tree_tasks(
                "source_raw",
                legacy_raw_dir(source_id),
                source_raw_dir(source_id),
            )
        )
    return tasks


def _legacy_processed_tasks() -> list[CopyTask]:
    tasks: list[CopyTask] = []
    for source_id in KNOWN_SOURCE_IDS:
        tasks.extend(
            _mirror_tree_tasks(
                "source_processed",
                legacy_processed_dir(source_id),
                source_processed_dir(source_id),
            )
        )
    return tasks


def _split_legacy_intermediate_name(name: str) -> tuple[str, str] | None:
    for source_id in sorted(KNOWN_SOURCE_IDS, key=len, reverse=True):
        if name == source_id:
            return source_id, ""
        prefix = f"{source_id}_"
        if name.startswith(prefix):
            return source_id, name[len(prefix):]
    return None


def _legacy_intermediate_tasks() -> list[CopyTask]:
    legacy_root = RAG_DATA_ROOT / "data_intermediate"
    if not legacy_root.exists():
        return []

    tasks: list[CopyTask] = []
    for child in sorted(path for path in legacy_root.iterdir() if path.is_dir()):
        parsed = _split_legacy_intermediate_name(child.name)
        if parsed is None:
            continue
        source_id, name = parsed
        destination = source_intermediate_dir(source_id, name or None)
        tasks.extend(
            _mirror_tree_tasks(
                "source_intermediate",
                legacy_intermediate_dir(source_id, name or None),
                destination,
                "legacy rag-data/data_intermediate mirror",
            )
        )
    return tasks


def _legacy_root_chunk_tasks(profile: str) -> list[CopyTask]:
    candidates = [
        CopyTask(
            "embedding_export",
            LEGACY_DATA_ROOT / "chunk_texts_for_embed.jsonl",
            chunk_texts_export_path(dataset_id=VMJ_RELEASE_DATASET_ID, profile=profile),
            "root VMJ chunk text export aligned with returned vector IDs",
        ),
        CopyTask(
            "embedding_export",
            LEGACY_DATA_ROOT / "chunk_metadata.jsonl",
            chunk_metadata_export_path(dataset_id=VMJ_RELEASE_DATASET_ID, profile=profile),
            "root VMJ chunk metadata export",
        ),
        CopyTask(
            "embedding_export",
            LEGACY_DATA_ROOT / "chunk_texts_all.jsonl",
            chunk_texts_export_path(dataset_id=ALL_CORPUS_DATASET_ID, profile=profile),
            "root all-corpus chunk text export",
        ),
        CopyTask(
            "embedding_export",
            LEGACY_DATA_ROOT / "chunk_metadata_all.jsonl",
            chunk_metadata_export_path(dataset_id=ALL_CORPUS_DATASET_ID, profile=profile),
            "root all-corpus chunk metadata export",
        ),
    ]
    return [task for task in candidates if task.source.exists()]


def build_tasks(
    *,
    include_data_final: bool,
    include_kaggle: bool,
    include_raw: bool = False,
    include_processed: bool = False,
    include_intermediate: bool = False,
    include_root_chunks: bool = False,
    dataset_id: str,
    profile: str,
) -> list[CopyTask]:
    tasks: list[CopyTask] = []
    if include_data_final:
        tasks.extend(_legacy_data_final_tasks())
    if include_kaggle:
        tasks.extend(_legacy_kaggle_tasks(dataset_id, profile, include_exports=not include_root_chunks))
    if include_raw:
        tasks.extend(_legacy_raw_tasks())
    if include_processed:
        tasks.extend(_legacy_processed_tasks())
    if include_intermediate:
        tasks.extend(_legacy_intermediate_tasks())
    if include_root_chunks:
        tasks.extend(_legacy_root_chunk_tasks(profile))
    return tasks


def _copy_task(task: CopyTask, *, execute: bool, overwrite: bool) -> dict:
    result = {
        **asdict(task),
        "source": str(task.source),
        "destination": str(task.destination),
        "source_exists": task.source.exists(),
        "destination_exists": task.destination.exists(),
        "action": "dry_run",
        "source_sha256": "",
        "destination_sha256": "",
        "bytes": 0,
    }

    if not task.source.exists():
        result["action"] = "missing_source"
        return result

    result["bytes"] = task.source.stat().st_size
    source_hash = file_sha256(task.source)
    result["source_sha256"] = source_hash

    if task.destination.exists():
        dest_hash = file_sha256(task.destination)
        result["destination_sha256"] = dest_hash
        if dest_hash == source_hash:
            result["action"] = "skip_same_hash"
            return result
        if not overwrite:
            result["action"] = "conflict"
            return result
        result["action"] = "overwrite" if execute else "would_overwrite"
    else:
        result["action"] = "copy" if execute else "would_copy"

    if execute:
        task.destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(task.source, task.destination)
        result["destination_exists"] = True
        result["destination_sha256"] = file_sha256(task.destination)

    return result


def run_migration(tasks: Iterable[CopyTask], *, execute: bool, overwrite: bool) -> dict:
    task_list = list(tasks)
    results = [_copy_task(task, execute=execute, overwrite=overwrite) for task in task_list]
    action_counts: dict[str, int] = {}
    for item in results:
        action = item["action"]
        action_counts[action] = action_counts.get(action, 0) + 1

    return {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "execute": execute,
        "overwrite": overwrite,
        "task_count": len(results),
        "action_counts": action_counts,
        "tasks": results,
    }


def _write_manifest(report: dict, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely copy legacy data artifacts into rag-data.")
    parser.add_argument("--data-final", action="store_true", help="Copy legacy data/data_final JSONL records.")
    parser.add_argument("--kaggle", action="store_true", help="Copy legacy data/kaggle_staging artifacts.")
    parser.add_argument("--raw", action="store_true", help="Copy legacy data/data_raw files into source raw folders.")
    parser.add_argument(
        "--processed",
        action="store_true",
        help="Copy legacy rag-data/data_processed files into source processed folders.",
    )
    parser.add_argument(
        "--intermediate",
        action="store_true",
        help="Copy legacy rag-data/data_intermediate files into source intermediate folders.",
    )
    parser.add_argument(
        "--root-chunks",
        action="store_true",
        help="Copy root data/chunk_* exports into canonical embedding export folders.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Include data-final, kaggle, raw, processed, intermediate, and root chunk artifacts.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually copy files. Omit for dry-run.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite destination files when hashes differ.")
    parser.add_argument("--dataset-id", default=VMJ_RELEASE_DATASET_ID, help="Dataset ID for Kaggle artifacts.")
    parser.add_argument("--profile", default=DEFAULT_EMBEDDING_PROFILE, help="Embedding profile for Kaggle artifacts.")
    parser.add_argument(
        "--manifest",
        default=str(migration_audit_path("legacy_copy_manifest.json")),
        help="Copy manifest output path.",
    )
    args = parser.parse_args()

    if args.all:
        args.data_final = True
        args.kaggle = True
        args.raw = True
        args.processed = True
        args.intermediate = True
        args.root_chunks = True

    if not any((args.data_final, args.kaggle, args.raw, args.processed, args.intermediate, args.root_chunks)):
        parser.error("Choose at least one artifact group or --all")

    tasks = build_tasks(
        include_data_final=args.data_final,
        include_kaggle=args.kaggle,
        include_raw=args.raw,
        include_processed=args.processed,
        include_intermediate=args.intermediate,
        include_root_chunks=args.root_chunks,
        dataset_id=args.dataset_id,
        profile=args.profile,
    )
    report = run_migration(tasks, execute=args.execute, overwrite=args.overwrite)
    _write_manifest(report, Path(args.manifest))
    print(json.dumps({k: report[k] for k in ("execute", "task_count", "action_counts")}, indent=2))
    print(f"Manifest: {args.manifest}")

    if any(item["action"] == "conflict" for item in report["tasks"]):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
