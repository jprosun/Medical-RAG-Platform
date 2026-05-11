from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
if str(INGESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INGESTOR_ROOT))


def _reload_builder(monkeypatch, rag_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    builder = importlib.import_module("tools.build_dataset_release")
    return importlib.reload(builder)


def _record(doc_id: str, source_id: str) -> dict:
    return {
        "doc_id": doc_id,
        "title": f"Title {doc_id}",
        "body": f"Body {doc_id}",
        "source_name": source_id,
        "source_id": source_id,
    }


def test_build_dataset_release_concatenates_sources_and_writes_manifest(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    source_a = rag_root / "sources" / "medlineplus" / "records" / "document_records.jsonl"
    source_b = rag_root / "sources" / "who" / "records" / "document_records.jsonl"
    source_a.parent.mkdir(parents=True)
    source_b.parent.mkdir(parents=True)
    source_a.write_text(json.dumps(_record("a", "medlineplus")) + "\n", encoding="utf-8")
    source_b.write_text(json.dumps(_record("b", "who")) + "\n", encoding="utf-8")

    builder = _reload_builder(monkeypatch, rag_root)

    report = builder.build_dataset_release(
        dataset_id="en_core_v1",
        source_ids=("medlineplus", "who"),
    )

    output = rag_root / "datasets" / "en_core_v1" / "records" / "document_records.jsonl"
    manifest = rag_root / "datasets" / "en_core_v1" / "manifest.json"
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]

    assert report["record_count"] == 2
    assert [row["doc_id"] for row in rows] == ["a", "b"]
    assert manifest.exists()


def test_build_dataset_release_dedups_by_source_doc_id(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    source = rag_root / "sources" / "medlineplus" / "records" / "document_records.jsonl"
    source.parent.mkdir(parents=True)
    records = [
        _record("same", "medlineplus"),
        _record("same", "medlineplus"),
    ]
    source.write_text("\n".join(json.dumps(item) for item in records) + "\n", encoding="utf-8")

    builder = _reload_builder(monkeypatch, rag_root)

    report = builder.build_dataset_release(
        dataset_id="en_core_v1",
        source_ids=("medlineplus",),
    )

    assert report["record_count"] == 1
    assert report["duplicates_skipped"] == 1


def test_build_dataset_release_collects_processed_files(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    source = rag_root / "sources" / "nhs_health_a_z" / "records" / "document_records.jsonl"
    processed = rag_root / "sources" / "nhs_health_a_z" / "processed" / "acute-pancreatitis.txt"
    source.parent.mkdir(parents=True, exist_ok=True)
    processed.parent.mkdir(parents=True, exist_ok=True)
    processed.write_text("body", encoding="utf-8")

    record = _record("nhs1", "nhs_health_a_z")
    record["processed_path"] = "rag-data/sources/nhs_health_a_z/processed/acute-pancreatitis.txt"
    source.write_text(json.dumps(record) + "\n", encoding="utf-8")

    builder = _reload_builder(monkeypatch, rag_root)

    report = builder.build_dataset_release(
        dataset_id="en_core_v1",
        source_ids=("nhs_health_a_z",),
    )

    processed_dir = rag_root / "datasets" / "en_core_v1" / "processed"
    promoted = list(processed_dir.glob("*.txt"))
    manifest = processed_dir / "processed_manifest.jsonl"

    assert report["processed_files_copied"] == 1
    assert len(promoted) == 1
    assert manifest.exists()
    manifest_rows = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
    assert manifest_rows[0]["source_id"] == "nhs_health_a_z"
    assert manifest_rows[0]["source_processed_path"] == record["processed_path"]


def test_safe_dataset_processed_name_truncates_long_stems(tmp_path, monkeypatch):
    builder = _reload_builder(monkeypatch, tmp_path / "rag-data")
    long_name = "rag-data/sources/who_vietnam/processed/" + ("x" * 220) + ".txt"

    safe = builder._safe_dataset_processed_name("who_vietnam", long_name)

    assert safe.startswith("who_vietnam__")
    assert safe.endswith(".txt")
    assert len(Path(safe).stem) < 120


def test_build_dataset_release_backfills_missing_source_id(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    source = rag_root / "sources" / "who" / "records" / "document_records.jsonl"
    source.parent.mkdir(parents=True)

    record = _record("who1", "who")
    record["source_id"] = ""
    source.write_text(json.dumps(record) + "\n", encoding="utf-8")

    builder = _reload_builder(monkeypatch, rag_root)

    builder.build_dataset_release(
        dataset_id="en_core_v1",
        source_ids=("who",),
    )

    output = rag_root / "datasets" / "en_core_v1" / "records" / "document_records.jsonl"
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]

    assert rows[0]["source_id"] == "who"


def test_resolve_source_ids_none_group_only_uses_explicit_ids(tmp_path, monkeypatch):
    builder = _reload_builder(monkeypatch, tmp_path / "rag-data")

    assert builder._resolve_source_ids(["who", "medlineplus"], "none") == ("who", "medlineplus")
