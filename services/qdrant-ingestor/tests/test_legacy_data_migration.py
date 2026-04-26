from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_migration_modules(monkeypatch, rag_root: Path, legacy_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    migration = importlib.import_module("tools.migrate_legacy_data_to_rag_data")
    return importlib.reload(migration)


def test_build_data_final_tasks_maps_source_and_vmj_release(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    data_final = legacy_root / "data_final"
    data_final.mkdir(parents=True)
    (data_final / "medlineplus.jsonl").write_text('{"doc_id":"m"}\n', encoding="utf-8")
    (data_final / "vmj_ojs_v2.jsonl").write_text('{"doc_id":"v"}\n', encoding="utf-8")

    migration = _reload_migration_modules(monkeypatch, rag_root, legacy_root)

    tasks = migration.build_tasks(
        include_data_final=True,
        include_kaggle=False,
        dataset_id=migration.VMJ_RELEASE_DATASET_ID,
        profile="multilingual",
    )
    destinations = {task.destination for task in tasks}

    assert rag_root / "sources" / "medlineplus" / "records" / "document_records.jsonl" in destinations
    assert rag_root / "sources" / "vmj_ojs" / "records" / "document_records.jsonl" in destinations
    assert rag_root / "sources" / "vmj_ojs" / "records" / "releases" / "v2" / "document_records.jsonl" in destinations
    assert rag_root / "datasets" / "vmj_ojs_release_v2" / "records" / "document_records.jsonl" in destinations


def test_build_tasks_maps_raw_processed_intermediate_and_root_chunks(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    raw = legacy_root / "data_raw" / "medlineplus" / "mplus_topics.xml"
    processed = rag_root / "data_processed" / "vmj_ojs" / "article.txt"
    intermediate = rag_root / "data_intermediate" / "vmj_ojs_split_articles" / "split.txt"
    root_chunk_texts = legacy_root / "chunk_texts_for_embed.jsonl"
    root_chunk_metadata = legacy_root / "chunk_metadata_all.jsonl"
    for path in (raw, processed, intermediate, root_chunk_texts, root_chunk_metadata):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    migration = _reload_migration_modules(monkeypatch, rag_root, legacy_root)

    tasks = migration.build_tasks(
        include_data_final=False,
        include_kaggle=False,
        include_raw=True,
        include_processed=True,
        include_intermediate=True,
        include_root_chunks=True,
        dataset_id=migration.VMJ_RELEASE_DATASET_ID,
        profile="multilingual",
    )
    destinations = {task.destination for task in tasks}

    assert rag_root / "sources" / "medlineplus" / "raw" / "mplus_topics.xml" in destinations
    assert rag_root / "sources" / "vmj_ojs" / "processed" / "article.txt" in destinations
    assert rag_root / "sources" / "vmj_ojs" / "intermediate" / "split_articles" / "split.txt" in destinations
    assert (
        rag_root
        / "embeddings"
        / "exports"
        / "vmj_ojs_release_v2"
        / "multilingual"
        / "chunk_texts_for_embed.jsonl"
        in destinations
    )
    assert (
        rag_root
        / "embeddings"
        / "exports"
        / "all_corpus_v1"
        / "multilingual"
        / "chunk_metadata.jsonl"
        in destinations
    )


def test_run_migration_copy_then_skip_same_hash(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    source = legacy_root / "data_final" / "medlineplus.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text('{"doc_id":"m"}\n', encoding="utf-8")

    migration = _reload_migration_modules(monkeypatch, rag_root, legacy_root)
    tasks = migration.build_tasks(
        include_data_final=True,
        include_kaggle=False,
        dataset_id=migration.VMJ_RELEASE_DATASET_ID,
        profile="multilingual",
    )

    dry_run = migration.run_migration(tasks, execute=False, overwrite=False)
    assert dry_run["action_counts"] == {"would_copy": 1}

    executed = migration.run_migration(tasks, execute=True, overwrite=False)
    assert executed["action_counts"] == {"copy": 1}

    destination = rag_root / "sources" / "medlineplus" / "records" / "document_records.jsonl"
    assert destination.read_text(encoding="utf-8") == '{"doc_id":"m"}\n'

    repeated = migration.run_migration(tasks, execute=True, overwrite=False)
    assert repeated["action_counts"] == {"skip_same_hash": 1}


def test_run_migration_reports_conflict_without_overwrite(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    source = legacy_root / "data_final" / "medlineplus.jsonl"
    destination = rag_root / "sources" / "medlineplus" / "records" / "document_records.jsonl"
    source.parent.mkdir(parents=True)
    destination.parent.mkdir(parents=True)
    source.write_text('{"doc_id":"legacy"}\n', encoding="utf-8")
    destination.write_text('{"doc_id":"canonical"}\n', encoding="utf-8")

    migration = _reload_migration_modules(monkeypatch, rag_root, legacy_root)
    tasks = migration.build_tasks(
        include_data_final=True,
        include_kaggle=False,
        dataset_id=migration.VMJ_RELEASE_DATASET_ID,
        profile="multilingual",
    )

    report = migration.run_migration(tasks, execute=True, overwrite=False)

    assert report["action_counts"] == {"conflict": 1}
    assert destination.read_text(encoding="utf-8") == '{"doc_id":"canonical"}\n'
