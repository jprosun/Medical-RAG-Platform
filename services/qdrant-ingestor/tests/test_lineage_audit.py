from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
INGESTOR_ROOT = REPO_ROOT / "services" / "qdrant-ingestor"
for path in (REPO_ROOT, INGESTOR_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.document_schema import DocumentRecord
from services.utils.data_audit import compare_jsonl_records, write_migration_audit
from services.utils.data_lineage import build_file_lineage, file_sha256


def test_build_file_lineage_sets_hash_and_processed_path(tmp_path):
    source_file = tmp_path / "rag-data" / "sources" / "vmj_ojs" / "processed" / "article.txt"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("medical text", encoding="utf-8")

    lineage = build_file_lineage(source_file, source_id="vmj_ojs", etl_run_id="run_1")

    assert lineage["source_id"] == "vmj_ojs"
    assert lineage["source_file"].endswith("article.txt")
    assert lineage["processed_path"].endswith("article.txt")
    assert lineage["raw_path"] == ""
    assert lineage["source_sha256"] == file_sha256(source_file)
    assert lineage["etl_run_id"] == "run_1"


def test_document_record_lineage_roundtrip():
    rec = DocumentRecord(
        doc_id="d1",
        title="Title",
        body="Body text",
        source_name="Source",
        source_id="vmj_ojs",
        source_file="rag-data/sources/vmj_ojs/processed/a.txt",
        processed_path="rag-data/sources/vmj_ojs/processed/a.txt",
        source_sha256="abc",
        etl_run_id="run_1",
    )

    restored = DocumentRecord.from_dict(json.loads(rec.to_jsonl_line()))

    assert restored.source_id == "vmj_ojs"
    assert restored.source_file.endswith("a.txt")
    assert restored.source_sha256 == "abc"
    assert restored.etl_run_id == "run_1"


def test_compare_jsonl_records_reports_match_and_mismatch(tmp_path):
    canonical = tmp_path / "canonical.jsonl"
    legacy = tmp_path / "legacy.jsonl"
    canonical.write_text(
        json.dumps({"doc_id": "a", "title": "A", "body": "B", "source_name": "S"}) + "\n",
        encoding="utf-8",
    )
    legacy.write_text(
        json.dumps({"doc_id": "b", "title": "A", "body": "B", "source_name": "S"}) + "\n",
        encoding="utf-8",
    )

    report = compare_jsonl_records(canonical, legacy)

    assert report["status"] == "mismatch"
    assert report["missing_in_canonical"] == ["b"]
    assert report["missing_in_legacy"] == ["a"]


def test_write_migration_audit_creates_parent_dirs(tmp_path):
    target = tmp_path / "qa" / "migration_audit.json"

    written = write_migration_audit({"sources": {}, "datasets": {}}, target)

    assert written == target
    assert json.loads(target.read_text(encoding="utf-8")) == {"sources": {}, "datasets": {}}
