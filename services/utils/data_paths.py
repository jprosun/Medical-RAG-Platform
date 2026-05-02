"""
Canonical data layout helpers for the RAG corpus.

This module centralizes repository-relative data paths and provides a
non-destructive migration path from the legacy `data/` + `rag-data/`
split layout to a single canonical `rag-data/` hierarchy.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
RAG_DATA_ROOT = Path(os.getenv("RAG_DATA_ROOT", REPO_ROOT / "rag-data"))
LEGACY_DATA_ROOT = Path(os.getenv("LEGACY_DATA_ROOT", REPO_ROOT / "data"))
DEFAULT_EMBEDDING_PROFILE = "multilingual"

KNOWN_SOURCE_IDS = (
    "cantho_med_journal",
    "cdc_health_topics",
    "dav_gov",
    "hue_jmp_ojs",
    "kcb_moh",
    "mayo_diseases_conditions",
    "medlineplus",
    "mil_med_pharm_journal",
    "msd_manual_consumer",
    "msd_manual_professional",
    "ncbi_bookshelf",
    "nhs_health_a_z",
    "trad_med_pharm_journal",
    "vmj_ojs",
    "who",
    "who_vietnam",
)
KNOWN_DATASET_IDS = (
    "en_core_v1",
    "vi_core_v1",
    "all_corpus_v1",
    "vmj_ojs_release_v2",
)
LEGACY_DATASET_ALIASES: dict[str, tuple[str, ...]] = {
    "combined": ("combined",),
    "en_core": ("combined",),
    "en_core_v1": ("combined",),
}


def _has_children(path: Path) -> bool:
    return path.exists() and any(path.iterdir())


def _first_existing(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def _prefer_data_dir(canonical: Path, legacy: Path) -> Path:
    if _has_children(canonical):
        return canonical
    if legacy.exists():
        return legacy
    return canonical


def registry_dir() -> Path:
    return RAG_DATA_ROOT / "registry"


def qa_root_dir() -> Path:
    return RAG_DATA_ROOT / "qa"


def migration_audit_path(filename: str = "migration_audit.json") -> Path:
    return qa_root_dir() / filename


def datasets_root() -> Path:
    return RAG_DATA_ROOT / "datasets"


def dataset_root(dataset_id: str) -> Path:
    return datasets_root() / dataset_id


def dataset_records_dir(dataset_id: str) -> Path:
    return dataset_root(dataset_id) / "records"


def dataset_records_path(dataset_id: str, filename: str = "document_records.jsonl") -> Path:
    return dataset_records_dir(dataset_id) / filename


def dataset_qa_dir(dataset_id: str) -> Path:
    return dataset_root(dataset_id) / "qa"


def dataset_manifest_path(dataset_id: str, filename: str = "manifest.json") -> Path:
    return dataset_root(dataset_id) / filename


def embeddings_root() -> Path:
    return RAG_DATA_ROOT / "embeddings"


def embeddings_exports_dir() -> Path:
    return embeddings_root() / "exports"


def embeddings_export_dir(dataset_id: str | None = None, profile: str | None = None) -> Path:
    path = embeddings_exports_dir()
    if dataset_id:
        path = path / dataset_id
        if profile:
            path = path / profile
    return path


def embeddings_staging_root() -> Path:
    return embeddings_root() / "staging"


def embeddings_staging_dir(
    name: str = DEFAULT_EMBEDDING_PROFILE,
    *,
    dataset_id: str | None = None,
) -> Path:
    path = embeddings_staging_root()
    if dataset_id:
        path = path / dataset_id
    return path / name


def embeddings_runs_dir() -> Path:
    return embeddings_root() / "runs"


def source_root(source_id: str) -> Path:
    return RAG_DATA_ROOT / "sources" / source_id


def source_raw_dir(source_id: str) -> Path:
    return source_root(source_id) / "raw"


def source_manifest_path(source_id: str, filename: str = "manifest.csv") -> Path:
    return source_root(source_id) / filename


def source_processed_dir(source_id: str) -> Path:
    return source_root(source_id) / "processed"


def source_intermediate_dir(source_id: str, name: str | None = None) -> Path:
    base = source_root(source_id) / "intermediate"
    return base / name if name else base


def source_records_dir(source_id: str) -> Path:
    return source_root(source_id) / "records"


def source_records_path(source_id: str, filename: str = "document_records.jsonl") -> Path:
    return source_records_dir(source_id) / filename


def source_release_records_path(
    source_id: str,
    release_id: str,
    filename: str = "document_records.jsonl",
) -> Path:
    return source_records_dir(source_id) / "releases" / release_id / filename


def source_qa_dir(source_id: str) -> Path:
    return source_root(source_id) / "qa"


def legacy_raw_dir(source_id: str) -> Path:
    return LEGACY_DATA_ROOT / "data_raw" / source_id


def legacy_final_dir() -> Path:
    return LEGACY_DATA_ROOT / "data_final"


def legacy_processed_dir(source_id: str) -> Path:
    return RAG_DATA_ROOT / "data_processed" / source_id


def legacy_intermediate_dir(source_id: str, name: str | None = None) -> Path:
    base = RAG_DATA_ROOT / "data_intermediate"
    if name:
        return base / f"{source_id}_{name}"
    return base / source_id


def legacy_records_path(source_id: str) -> Path:
    return legacy_final_dir() / f"{source_id}.jsonl"


def legacy_dataset_records_candidates(dataset_id: str) -> tuple[Path, ...]:
    candidates = [legacy_final_dir() / f"{dataset_id}.jsonl"]
    for alias in LEGACY_DATASET_ALIASES.get(dataset_id, ()):
        candidates.append(legacy_final_dir() / f"{alias}.jsonl")
    return tuple(candidates)


def legacy_chunk_texts_export_path() -> Path:
    return LEGACY_DATA_ROOT / "chunk_texts_for_embed.jsonl"


def legacy_chunk_metadata_export_path() -> Path:
    return LEGACY_DATA_ROOT / "chunk_metadata.jsonl"


def legacy_kaggle_staging_root() -> Path:
    return LEGACY_DATA_ROOT / "kaggle_staging"


def legacy_kaggle_export_path(filename: str) -> Path:
    return legacy_kaggle_staging_root() / filename


def legacy_kaggle_profile_dir(name: str = DEFAULT_EMBEDDING_PROFILE) -> Path:
    return legacy_kaggle_staging_root() / name


def legacy_kaggle_metadata_path() -> Path:
    return legacy_kaggle_staging_root() / "chunk_metadata.jsonl"


def preferred_raw_dir(source_id: str) -> Path:
    return _prefer_data_dir(source_raw_dir(source_id), legacy_raw_dir(source_id))


def preferred_processed_dir(source_id: str) -> Path:
    return _prefer_data_dir(source_processed_dir(source_id), legacy_processed_dir(source_id))


def preferred_intermediate_dir(source_id: str, name: str | None = None) -> Path:
    canonical = source_intermediate_dir(source_id, name)
    legacy = legacy_intermediate_dir(source_id, name)
    return _prefer_data_dir(canonical, legacy)


def preferred_records_path(source_id: str, filename: str = "document_records.jsonl") -> Path:
    canonical = source_records_path(source_id, filename)
    legacy = legacy_records_path(source_id)
    return _first_existing(canonical, legacy)


def preferred_dataset_records_path(dataset_id: str, filename: str = "document_records.jsonl") -> Path:
    canonical = dataset_records_path(dataset_id, filename)
    return _first_existing(canonical, *legacy_dataset_records_candidates(dataset_id))


def chunk_texts_export_path(
    filename: str = "chunk_texts_for_embed.jsonl",
    *,
    dataset_id: str | None = None,
    profile: str | None = None,
) -> Path:
    return embeddings_export_dir(dataset_id=dataset_id, profile=profile) / filename


def chunk_metadata_export_path(
    filename: str = "chunk_metadata.jsonl",
    *,
    dataset_id: str | None = None,
    profile: str | None = None,
) -> Path:
    return embeddings_export_dir(dataset_id=dataset_id, profile=profile) / filename


def embedding_ids_path(
    filename: str = "chunk_ids.json",
    *,
    dataset_id: str | None = None,
    profile: str = DEFAULT_EMBEDDING_PROFILE,
) -> Path:
    return embeddings_staging_dir(profile, dataset_id=dataset_id) / filename


def embedding_vectors_path(
    filename: str = "embeddings.npy",
    *,
    dataset_id: str | None = None,
    profile: str = DEFAULT_EMBEDDING_PROFILE,
) -> Path:
    return embeddings_staging_dir(profile, dataset_id=dataset_id) / filename


def preferred_chunk_texts_export_path(
    filename: str = "chunk_texts_for_embed.jsonl",
    *,
    dataset_id: str | None = None,
    profile: str | None = None,
) -> Path:
    return _first_existing(
        chunk_texts_export_path(filename, dataset_id=dataset_id, profile=profile),
        legacy_chunk_texts_export_path(),
        legacy_kaggle_export_path(filename),
    )


def preferred_chunk_metadata_export_path(
    filename: str = "chunk_metadata.jsonl",
    *,
    dataset_id: str | None = None,
    profile: str | None = None,
) -> Path:
    return _first_existing(
        chunk_metadata_export_path(filename, dataset_id=dataset_id, profile=profile),
        legacy_kaggle_export_path(filename),
        legacy_chunk_metadata_export_path(),
        legacy_kaggle_metadata_path(),
    )


def preferred_embedding_ids_path(
    filename: str = "chunk_ids.json",
    *,
    dataset_id: str | None = None,
    profile: str = DEFAULT_EMBEDDING_PROFILE,
) -> Path:
    return _first_existing(
        embedding_ids_path(filename, dataset_id=dataset_id, profile=profile),
        legacy_kaggle_profile_dir(profile) / filename,
    )


def preferred_embedding_vectors_path(
    filename: str = "embeddings.npy",
    *,
    dataset_id: str | None = None,
    profile: str = DEFAULT_EMBEDDING_PROFILE,
) -> Path:
    return _first_existing(
        embedding_vectors_path(filename, dataset_id=dataset_id, profile=profile),
        legacy_kaggle_profile_dir(profile) / filename,
    )


def preferred_kaggle_staging_dir(
    name: str = DEFAULT_EMBEDDING_PROFILE,
    *,
    dataset_id: str | None = None,
) -> Path:
    canonical = embeddings_staging_dir(name, dataset_id=dataset_id)
    legacy = legacy_kaggle_profile_dir(name)
    return _prefer_data_dir(canonical, legacy)


def ensure_rag_data_layout(
    source_ids: Iterable[str] | None = None,
    dataset_ids: Iterable[str] | None = None,
    embedding_profiles: Iterable[str] | None = None,
) -> list[Path]:
    """Create the canonical non-destructive directory scaffold."""
    created: list[Path] = []
    base_dirs = [
        registry_dir(),
        qa_root_dir(),
        datasets_root(),
        embeddings_exports_dir(),
        embeddings_staging_dir(),
        embeddings_runs_dir(),
    ]
    for path in base_dirs:
        path.mkdir(parents=True, exist_ok=True)
        created.append(path)

    for source_id in source_ids or []:
        for path in (
            source_raw_dir(source_id),
            source_intermediate_dir(source_id),
            source_processed_dir(source_id),
            source_records_dir(source_id),
            source_qa_dir(source_id),
        ):
            path.mkdir(parents=True, exist_ok=True)
            created.append(path)

    profiles = list(embedding_profiles or [DEFAULT_EMBEDDING_PROFILE])
    for dataset_id in dataset_ids or []:
        for path in (
            dataset_records_dir(dataset_id),
            dataset_qa_dir(dataset_id),
        ):
            path.mkdir(parents=True, exist_ok=True)
            created.append(path)
        for profile in profiles:
            for path in (
                embeddings_export_dir(dataset_id=dataset_id, profile=profile),
                embeddings_staging_dir(profile, dataset_id=dataset_id),
            ):
                path.mkdir(parents=True, exist_ok=True)
                created.append(path)

    return created
