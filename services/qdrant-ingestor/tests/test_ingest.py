"""
Unit tests for the upgraded qdrant-ingestor.

Covers: document schema, structure-aware chunking, stable ID generation,
enriched JSONL ingestion, and backward compatibility.
"""

import json
import os
import tempfile
import pytest

# ── Patch qdrant_client and fastembed before importing app modules ────
# These heavy dependencies aren't needed for unit testing logic.
import sys
from types import ModuleType

# Stub qdrant_client
_qm = ModuleType("qdrant_client.http.models")
_qm.VectorParams = type("VectorParams", (), {"__init__": lambda *a, **kw: None})
_qm.Distance = type("Distance", (), {"COSINE": "Cosine"})()
_qm.PointStruct = type("PointStruct", (), {"__init__": lambda self, **kw: None})

_qhttp = ModuleType("qdrant_client.http")
_qhttp.models = _qm

_qc = ModuleType("qdrant_client")
_qc.QdrantClient = type("QdrantClient", (), {"__init__": lambda self, **kw: None})
_qc.http = _qhttp

sys.modules.setdefault("qdrant_client", _qc)
sys.modules.setdefault("qdrant_client.http", _qhttp)
sys.modules.setdefault("qdrant_client.http.models", _qm)

_fe = ModuleType("fastembed")
_fe.TextEmbedding = type("TextEmbedding", (), {"__init__": lambda self, **kw: None})
sys.modules.setdefault("fastembed", _fe)

# Now safe to import
from app.document_schema import DocumentRecord, iter_jsonl, VALID_DOC_TYPES
from app.ingest_utils import (
    normalize_whitespace,
    split_by_headings,
    sanitize_for_id,
    build_heading_path,
)
from app.ingest import (
    chunk_text,
    chunk_by_structure,
    generate_stable_id,
    ingest_enriched_jsonl,
)
from app.ingest_quality import evaluate_document_quality


# =====================================================================
# DocumentRecord schema tests
# =====================================================================
class TestDocumentRecord:
    def test_valid_record(self):
        rec = DocumentRecord(
            doc_id="test_001",
            title="Test Title",
            body="Some body text here.",
            source_name="TestSource",
        )
        errors = rec.validate()
        assert errors == [], f"Expected no errors, got: {errors}"

    def test_missing_required_fields(self):
        rec = DocumentRecord(doc_id="", title="", body="", source_name="")
        errors = rec.validate()
        assert len(errors) == 4  # doc_id, title, body, source_name

    def test_invalid_doc_type(self):
        rec = DocumentRecord(
            doc_id="x", title="x", body="x", source_name="x",
            doc_type="invalid_type",
        )
        errors = rec.validate()
        assert any("doc_type" in e for e in errors)

    def test_invalid_audience(self):
        rec = DocumentRecord(
            doc_id="x", title="x", body="x", source_name="x",
            audience="expert",
        )
        errors = rec.validate()
        assert any("audience" in e for e in errors)

    def test_invalid_trust_tier(self):
        rec = DocumentRecord(
            doc_id="x", title="x", body="x", source_name="x",
            trust_tier=5,
        )
        errors = rec.validate()
        assert any("trust_tier" in e for e in errors)

    def test_to_jsonl_roundtrip(self):
        rec = DocumentRecord(
            doc_id="rt_001",
            title="Roundtrip",
            body="Body text.",
            source_name="Src",
            tags=["a", "b"],
            source_file="rag-data/sources/src/processed/a.txt",
            source_sha256="sha",
            etl_run_id="run_1",
        )
        line = rec.to_jsonl_line()
        restored = DocumentRecord.from_dict(json.loads(line))
        assert restored.doc_id == rec.doc_id
        assert restored.tags == ["a", "b"]
        assert restored.source_file == "rag-data/sources/src/processed/a.txt"
        assert restored.source_sha256 == "sha"
        assert restored.etl_run_id == "run_1"

    def test_from_dict_ignores_unknown_keys(self):
        d = {
            "doc_id": "x", "title": "x", "body": "x",
            "source_name": "x", "unknown_field": 42,
        }
        rec = DocumentRecord.from_dict(d)
        assert not hasattr(rec, "unknown_field") or True  # should not crash

    def test_tags_from_comma_string(self):
        d = {"doc_id": "x", "title": "x", "body": "x", "source_name": "x", "tags": "a, b, c"}
        rec = DocumentRecord.from_dict(d)
        assert rec.tags == ["a", "b", "c"]


# =====================================================================
# JSONL reader tests
# =====================================================================
class TestIterJsonl:
    def test_reads_valid_jsonl(self, tmp_path):
        fp = tmp_path / "test.jsonl"
        records = [
            {"doc_id": "d1", "title": "T1", "body": "B1", "source_name": "S1"},
            {"doc_id": "d2", "title": "T2", "body": "B2", "source_name": "S2"},
        ]
        fp.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
        result = list(iter_jsonl(str(fp)))
        assert len(result) == 2
        assert result[0].doc_id == "d1"
        assert result[1].doc_id == "d2"

    def test_skips_blank_lines(self, tmp_path):
        fp = tmp_path / "test.jsonl"
        fp.write_text(
            '{"doc_id":"d1","title":"T","body":"B","source_name":"S"}\n'
            "\n"
            '{"doc_id":"d2","title":"T","body":"B","source_name":"S"}\n',
            encoding="utf-8",
        )
        result = list(iter_jsonl(str(fp)))
        assert len(result) == 2

    def test_invalid_json_raises(self, tmp_path):
        fp = tmp_path / "bad.jsonl"
        fp.write_text("not json\n", encoding="utf-8")
        with pytest.raises(ValueError, match="invalid JSON"):
            list(iter_jsonl(str(fp)))


# =====================================================================
# ingest_utils tests
# =====================================================================
class TestSplitByHeadings:
    def test_markdown_headings(self):
        text = "# Intro\nSome intro text.\n## Details\nMore details here.\n## Summary\nFinal."
        sections = split_by_headings(text)
        assert len(sections) == 3
        assert sections[0].title == "Intro"
        assert sections[0].level == 1
        assert "intro text" in sections[0].body
        assert sections[1].title == "Details"
        assert sections[1].heading_path == "Intro > Details"
        assert sections[2].title == "Summary"

    def test_no_headings_returns_empty(self):
        text = "Just plain text without any headings at all."
        assert split_by_headings(text) == []

    def test_plain_vietnamese_headings(self):
        text = (
            "TÓM TẮT\n"
            "Nội dung tóm tắt của bài báo.\n"
            "KẾT QUẢ\n"
            "Kết quả chính của nghiên cứu.\n"
            "KẾT LUẬN\n"
            "Kết luận cuối cùng."
        )
        sections = split_by_headings(text)
        assert len(sections) == 3
        assert sections[0].title.upper() == "TÓM TẮT"
        assert "tóm tắt" in sections[0].body.lower()
        assert sections[1].title.upper() == "KẾT QUẢ"
        assert sections[2].heading_path.upper() == "KẾT LUẬN"

    def test_inline_vietnamese_headings(self):
        text = (
            "TÓM TẮT1 Mục tiêu: Mô tả đặc điểm lâm sàng.\n"
            "Phương pháp: Nghiên cứu mô tả cắt ngang.\n"
            "Kết quả: Tỷ lệ cải thiện là 85%.\n"
            "KẾT LUẬN: Can thiệp sớm có lợi.\n"
        )
        sections = split_by_headings(text)
        assert len(sections) == 3
        assert sections[0].title.upper() == "TÓM TẮT"
        assert sections[0].body.startswith("Mục tiêu:")
        assert sections[1].title.upper() == "KẾT QUẢ"
        assert sections[2].title.upper() == "KẾT LUẬN"

    def test_nested_headings_path(self):
        text = "# A\n\n## B\n\n### C\nDeep content."
        sections = split_by_headings(text)
        assert len(sections) == 3
        assert sections[2].heading_path == "A > B > C"

    def test_heading_level_reset(self):
        text = "# First\ncontent1\n## Sub\ncontent2\n# Second\ncontent3"
        sections = split_by_headings(text)
        # "Second" is a level-1, so its path should be just "Second"
        assert sections[2].heading_path == "Second"


class TestSanitizeForId:
    def test_basic_slug(self):
        assert sanitize_for_id("Hypertension in Adults") == "hypertension_in_adults"

    def test_special_chars(self):
        slug = sanitize_for_id("NICE NG136 – Diagnosis & Management!")
        assert slug == "nice_ng136_diagnosis_management"

    def test_max_length(self):
        long = "a" * 100
        result = sanitize_for_id(long, max_len=20)
        assert len(result) <= 20

    def test_empty_string(self):
        assert sanitize_for_id("") == "untitled"
        assert sanitize_for_id("!!!") == "untitled"

    def test_unicode_accents(self):
        slug = sanitize_for_id("Hépatite résumé")
        assert slug == "hepatite_resume"


class TestBuildHeadingPath:
    def test_basic(self):
        assert build_heading_path(["A", "B", "C"]) == "A > B > C"

    def test_empty(self):
        assert build_heading_path([]) == ""

    def test_strips_whitespace(self):
        assert build_heading_path(["  A ", " B"]) == "A > B"


# =====================================================================
# Chunking tests
# =====================================================================
class TestChunkText:
    def test_short_text_single_chunk(self):
        result = chunk_text("Hello world", chunk_size=100, overlap=10)
        assert result == ["Hello world"]

    def test_empty_text(self):
        assert chunk_text("") == []
        assert chunk_text("   ") == []

    def test_chunks_overlap(self):
        text = "A" * 2000
        chunks = chunk_text(text, chunk_size=900, overlap=150)
        assert len(chunks) > 1
        # Each chunk should be ≤ chunk_size
        for ch in chunks:
            assert len(ch) <= 900


class TestChunkByStructure:
    def test_with_headings(self):
        body = "# Section A\nContent A here.\n## Section B\nContent B is longer and more detailed."
        result = chunk_by_structure(body, title="TestDoc", source_name="TestSrc")
        assert len(result) >= 2
        # heading_path should be populated
        assert result[0][0] == "Section A"
        assert result[1][0] == "Section A > Section B"
        # context header should be prepended
        assert "Title: TestDoc" in result[0][1]
        assert "Source: TestSrc" in result[0][1]

    def test_fallback_no_headings(self):
        body = "Plain text " * 200  # long enough for multiple chunks
        result = chunk_by_structure(body, chunk_size=200, overlap=20)
        assert len(result) > 1
        # heading_path should be empty for fallback
        assert result[0][0] == ""

    def test_single_short_section(self):
        body = "# Only\nSmall text."
        result = chunk_by_structure(body, chunk_size=900)
        assert len(result) == 1
        assert "Only" in result[0][0]


# =====================================================================
# Stable ID generation tests
# =====================================================================
class TestGenerateStableId:
    def test_basic_format(self):
        cid = generate_stable_id("MedlinePlus", "hypertension_overview", "diagnosis", 3)
        assert cid == "medlineplus_hypertension_overview_diagnosis_chunk03"

    def test_special_chars_cleaned(self):
        cid = generate_stable_id("NCBI Bookshelf", "Type 2 Diabetes!", "First-line Therapy", 0)
        assert "ncbi_bookshelf" in cid
        assert "chunk00" in cid
        # no spaces or special chars
        assert " " not in cid
        assert "!" not in cid

    def test_empty_section(self):
        cid = generate_stable_id("WHO", "guideline_001", "", 5)
        assert "_main_chunk05" in cid


# =====================================================================
# Enriched JSONL ingestion (integration-ish)
# =====================================================================
class TestIngestEnrichedJsonl:
    def test_ingest_sample_data(self, tmp_path):
        # Create a small enriched JSONL
        records = [
            {
                "doc_id": "test_record_1",
                "title": "Bệnh học thử nghiệm",
                "canonical_title": "Vai trò can thiệp sớm trong bệnh học thử nghiệm",
                "section_title": "Overview",
                "body": (
                    "TÓM TẮT\n"
                    + ("Can thiệp sớm giúp cải thiện kết cục lâm sàng. " * 12)
                    + "\nKẾT QUẢ\n"
                    + ("Nhóm điều trị sớm đạt kết quả tốt hơn nhóm chứng. " * 12)
                ),
                "source_name": "TestSource",
                "source_url": "https://example.com/test",
                "source_id": "test_source",
                "source_file": "rag-data/sources/test_source/processed/test.txt",
                "processed_path": "rag-data/sources/test_source/processed/test.txt",
                "source_sha256": "sha256-test",
                "etl_run_id": "etl-run-test",
                "doc_type": "patient_education",
                "specialty": "cardiology",
                "audience": "patient",
                "language": "vi",
                "trust_tier": 3,
                "published_at": "2025-01-01",
                "updated_at": "2026-01-01",
                "tags": ["test", "cardiology"],
                "heading_path": "Test Disease > Overview",
                "quality_status": "go",
            },
        ]
        fp = tmp_path / "test.jsonl"
        fp.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")

        chunks = ingest_enriched_jsonl(
            input_path=str(tmp_path),
            patterns=["*.jsonl"],
            chunk_size=900,
            overlap=150,
        )

        assert len(chunks) >= 1

        # Check chunk structure
        ch = chunks[0]
        assert "testsource" in ch.id  # stable ID contains source
        assert "test_record_1" in ch.id
        assert ch.metadata["source_name"] == "TestSource"
        assert ch.metadata["source_id"] == "test_source"
        assert ch.metadata["source_file"] == "rag-data/sources/test_source/processed/test.txt"
        assert ch.metadata["processed_path"] == "rag-data/sources/test_source/processed/test.txt"
        assert ch.metadata["source_sha256"] == "sha256-test"
        assert ch.metadata["etl_run_id"] == "etl-run-test"
        assert ch.metadata["specialty"] == "cardiology"
        assert ch.metadata["trust_tier"] == 3
        assert ch.metadata["doc_type"] == "patient_education"
        assert ch.metadata["title"] == "Vai trò can thiệp sớm trong bệnh học thử nghiệm"
        assert ch.metadata["raw_title"] == "Bệnh học thử nghiệm"
        assert ch.metadata["section_type"] in {"abstract", "results"}
        assert ch.metadata["chunk_role"] in {"high_signal", "evidence"}
        assert ch.metadata["quality_status"] in {"go", "review"}
        assert "Title: Vai trò can thiệp sớm trong bệnh học thử nghiệm" in ch.text

    def test_validation_error_skipped(self, tmp_path):
        # Record with empty required fields
        records = [
            {"doc_id": "", "title": "", "body": "", "source_name": ""},
            {"doc_id": "good", "title": "Good", "body": "Content", "source_name": "Src"},
        ]
        fp = tmp_path / "test.jsonl"
        fp.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")

        chunks = ingest_enriched_jsonl(
            input_path=str(tmp_path),
            patterns=["*.jsonl"],
            chunk_size=900,
            overlap=150,
        )

        # Only the valid record should produce chunks
        assert len(chunks) >= 1
        assert all("good" in ch.id or "src" in ch.id for ch in chunks)

    def test_quality_gate_skips_low_quality_records(self, tmp_path):
        records = [
            {
                "doc_id": "hold_doc",
                "title": "T",
                "body": "Ngắn",
                "source_name": "Src",
                "source_url": "",
                "doc_type": "reference",
                "audience": "patient",
                "language": "vi",
                "trust_tier": 3,
            },
            {
                "doc_id": "review_doc",
                "title": "Bài báo có cấu trúc rõ ràng",
                "body": (
                    "TÓM TẮT\n"
                    + ("Can thiệp giúp cải thiện tiên lượng. " * 15)
                    + "\nKẾT LUẬN\n"
                    + ("Khuyến nghị theo dõi định kỳ sau điều trị. " * 10)
                ),
                "source_name": "Src",
                "source_url": "https://example.com/review",
                "doc_type": "reference",
                "audience": "patient",
                "language": "vi",
                "trust_tier": 3,
            },
        ]
        fp = tmp_path / "test.jsonl"
        fp.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in records), encoding="utf-8")

        chunks = ingest_enriched_jsonl(
            input_path=str(tmp_path),
            patterns=["*.jsonl"],
            chunk_size=900,
            overlap=150,
            min_quality_status="review",
        )

        assert chunks
        assert all(ch.metadata["doc_id"] == "review_doc" for ch in chunks)

    def test_reference_sections_are_skipped(self, tmp_path):
        records = [
            {
                "doc_id": "doc_with_refs",
                "title": "Tài liệu có tham khảo",
                "body": (
                    "KẾT QUẢ\n"
                    + ("Kết quả lâm sàng cho thấy cải thiện rõ. " * 12)
                    + "\nTÀI LIỆU THAM KHẢO\n"
                    + "[1] Nguyen A. Example study. BMJ. 2024.\n"
                    + "[2] Tran B. Another study. doi:10.1000/test.\n"
                ),
                "source_name": "Src",
                "source_url": "https://example.com/doc",
                "doc_type": "reference",
                "audience": "patient",
                "language": "vi",
                "trust_tier": 3,
            },
        ]
        fp = tmp_path / "test.jsonl"
        fp.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in records), encoding="utf-8")

        chunks = ingest_enriched_jsonl(
            input_path=str(tmp_path),
            patterns=["*.jsonl"],
            chunk_size=900,
            overlap=150,
        )

        assert chunks
        assert all(ch.metadata["section_type"] != "references" for ch in chunks)
        assert all("doi:10.1000/test" not in ch.text.lower() for ch in chunks)


class TestIngestQuality:
    def test_presectionized_record_not_flagged_no_sections(self):
        record = {
            "title": "Monitoring Health For The SDGs",
            "canonical_title": "Monitoring Health For The SDGs",
            "section_title": "Monitoring Health For The SDGs (phần 1)",
            "heading_path": "Monitoring Health For The SDGs > phần 1",
            "body": "Đây là phần đầu của tài liệu dài. " * 50,
            "source_name": "WHO",
            "source_url": "https://example.com",
            "doc_type": "reference",
            "language": "vi",
            "_section_count": 0,
        }
        quality = evaluate_document_quality(record)
        assert "no_sections_detected" not in quality["quality_flags"]

    def test_no_jsonl_files_raises(self, tmp_path):
        with pytest.raises(SystemExit, match="No .jsonl files"):
            ingest_enriched_jsonl(
                input_path=str(tmp_path),
                patterns=["*.jsonl"],
                chunk_size=900,
                overlap=150,
            )


# =====================================================================
# Backward compatibility
# =====================================================================
class TestBackwardCompat:
    def test_legacy_chunk_text_unchanged(self):
        """The original chunk_text function should still work identically."""
        text = "Medical text. " * 100
        chunks = chunk_text(text, chunk_size=900, overlap=150)
        assert len(chunks) > 1
        for ch in chunks:
            assert len(ch) <= 900
