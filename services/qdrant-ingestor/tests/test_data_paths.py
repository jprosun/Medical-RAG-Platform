from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_data_paths():
    module = importlib.import_module("services.utils.data_paths")
    return importlib.reload(module)


def test_preferred_records_path_falls_back_to_legacy(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    legacy_records = legacy_root / "data_final" / "vmj_ojs.jsonl"
    legacy_records.parent.mkdir(parents=True, exist_ok=True)
    legacy_records.write_text("{}", encoding="utf-8")

    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    assert module.preferred_records_path("vmj_ojs") == legacy_records
    assert module.source_records_path("vmj_ojs") == rag_root / "sources" / "vmj_ojs" / "records" / "document_records.jsonl"


def test_source_release_records_path(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    assert module.source_release_records_path("vmj_ojs", "v2") == (
        rag_root / "sources" / "vmj_ojs" / "records" / "releases" / "v2" / "document_records.jsonl"
    )


def test_source_manifest_path(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    assert module.source_manifest_path("who") == rag_root / "sources" / "who" / "manifest.csv"


def test_ensure_rag_data_layout_creates_canonical_scaffold(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    created = module.ensure_rag_data_layout(
        ["who_vietnam"],
        ["en_core_v1"],
        ["multilingual"],
    )

    expected_dirs = [
        rag_root / "registry",
        rag_root / "qa",
        rag_root / "datasets",
        rag_root / "embeddings" / "exports",
        rag_root / "embeddings" / "staging" / "multilingual",
        rag_root / "embeddings" / "runs",
        rag_root / "sources" / "who_vietnam" / "raw",
        rag_root / "sources" / "who_vietnam" / "intermediate",
        rag_root / "sources" / "who_vietnam" / "processed",
        rag_root / "sources" / "who_vietnam" / "records",
        rag_root / "sources" / "who_vietnam" / "qa",
        rag_root / "datasets" / "en_core_v1" / "records",
        rag_root / "datasets" / "en_core_v1" / "processed",
        rag_root / "datasets" / "en_core_v1" / "qa",
        rag_root / "embeddings" / "exports" / "en_core_v1" / "multilingual",
        rag_root / "embeddings" / "staging" / "en_core_v1" / "multilingual",
    ]

    for path in expected_dirs:
        assert path in created
        assert path.exists()


def test_preferred_processed_dir_uses_legacy_when_canonical_is_empty(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_processed = rag_root / "data_processed" / "who_vietnam"
    legacy_processed.mkdir(parents=True, exist_ok=True)
    (legacy_processed / "sample.txt").write_text("content", encoding="utf-8")
    legacy_root = tmp_path / "data"

    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()
    module.ensure_rag_data_layout(["who_vietnam"])

    assert module.preferred_processed_dir("who_vietnam") == legacy_processed


def test_preferred_dataset_records_path_falls_back_to_legacy_combined_alias(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    legacy_combined = legacy_root / "data_final" / "combined.jsonl"
    legacy_combined.parent.mkdir(parents=True, exist_ok=True)
    legacy_combined.write_text("{}", encoding="utf-8")

    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    assert module.preferred_dataset_records_path("en_core_v1") == legacy_combined
    assert module.dataset_records_path("en_core_v1") == rag_root / "datasets" / "en_core_v1" / "records" / "document_records.jsonl"


def test_preferred_embedding_vectors_path_falls_back_to_legacy_kaggle_layout(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    legacy_embeddings = legacy_root / "kaggle_staging" / "multilingual" / "embeddings.npy"
    legacy_embeddings.parent.mkdir(parents=True, exist_ok=True)
    legacy_embeddings.write_bytes(b"123")

    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    assert module.preferred_embedding_vectors_path(dataset_id="vmj_ojs_v2", profile="multilingual") == legacy_embeddings


def test_preferred_chunk_texts_uses_root_export_before_kaggle_staging(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    root_export = legacy_root / "chunk_texts_for_embed.jsonl"
    kaggle_export = legacy_root / "kaggle_staging" / "chunk_texts_for_embed.jsonl"
    root_export.parent.mkdir(parents=True, exist_ok=True)
    kaggle_export.parent.mkdir(parents=True, exist_ok=True)
    root_export.write_text("root", encoding="utf-8")
    kaggle_export.write_text("kaggle", encoding="utf-8")

    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    assert module.preferred_chunk_texts_export_path(dataset_id="vmj_ojs_v2", profile="multilingual") == root_export


def test_dataset_processed_paths(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    assert module.dataset_processed_dir("en_core_v1") == rag_root / "datasets" / "en_core_v1" / "processed"
    assert module.dataset_processed_manifest_path("en_core_v1") == (
        rag_root / "datasets" / "en_core_v1" / "processed" / "processed_manifest.jsonl"
    )


def test_data_proceed_paths(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    assert module.data_proceed_root() == rag_root / "data_proceed"
    assert module.data_proceed_processed_dir("nhs_health_a_z") == (
        rag_root / "data_proceed" / "nhs_health_a_z" / "processed"
    )
    assert module.data_proceed_records_path("nhs_health_a_z") == (
        rag_root / "data_proceed" / "nhs_health_a_z" / "records" / "document_records.jsonl"
    )


def test_source_partition_records_paths(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    module = _reload_data_paths()

    assert module.source_partition_records_dir("vien_dinh_duong", "article_only") == (
        rag_root / "sources" / "vien_dinh_duong" / "records" / "article_only"
    )
    assert module.source_partition_records_path("vien_dinh_duong", "long_pdf_book_ocr", "ocr_jobs.jsonl") == (
        rag_root / "sources" / "vien_dinh_duong" / "records" / "long_pdf_book_ocr" / "ocr_jobs.jsonl"
    )
