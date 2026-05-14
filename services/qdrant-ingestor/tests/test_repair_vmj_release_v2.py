from __future__ import annotations

import json
from pathlib import Path

from tools.repair_vmj_release_v2 import build_v4_title_index, repair_row, repair_dataset


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_repair_row_backfills_source_url_from_unique_v4_title() -> None:
    title_index = {
        "abc study about hospital treatment outcomes": {
            "title": "ABC Study about hospital treatment outcomes",
            "url_values": ["https://site/article/view/1"],
            "best_source_url": "https://site/article/view/1",
            "best_source_file": "processed/1.txt",
            "best_processed_path": "processed/1.txt",
            "best_source_sha256": "sha1",
            "best_specialty": "cardiology",
            "best_doc_type": "review",
        }
    }
    row = {
        "doc_id": "x1",
        "title": "ABC Study about hospital treatment outcomes",
        "body": "A" * 900,
        "source_url": "https://site/issue/view/99",
        "quality_status": "go",
        "quality_flags": [],
        "tags": [],
    }

    decision, repaired = repair_row(row, title_index)

    assert decision == "keep"
    assert repaired["source_url"] == "https://site/article/view/1"
    assert repaired["source_file"] == "processed/1.txt"
    assert "source_url_backfilled_from_v4" in repaired["quality_flags"]
    assert repaired["quality_status"] == "go"


def test_repair_row_quarantines_generic_or_conference_records() -> None:
    title_index = {}
    row = {
        "doc_id": "x2",
        "title": "Kỷ yếu các công trình nghiên cứu",
        "body": "B" * 1200,
        "source_url": "https://site/issue/view/12",
        "quality_status": "go",
        "quality_flags": [],
        "tags": [],
    }

    decision, payload = repair_row(row, title_index)

    assert decision == "quarantine"
    assert "conference_title" in payload["reasons"]


def test_repair_dataset_marks_unmapped_issue_url_records_review(tmp_path: Path) -> None:
    legacy_rows = [
        {
            "doc_id": "x3",
            "title": "Unique legacy article title that is definitely long enough for filtering",
            "body": "C" * 1000,
            "source_url": "https://site/issue/view/77",
            "quality_status": "go",
            "quality_score": 95,
            "quality_flags": [],
            "tags": [],
        }
    ]
    v4_rows = [
        {
            "doc_id": "y1",
            "title": "Different v4 title",
            "body": "D" * 1000,
            "source_url": "https://site/article/view/9",
            "source_file": "processed/9.txt",
            "processed_path": "processed/9.txt",
            "source_sha256": "sha9",
            "quality_status": "go",
        }
    ]

    input_path = tmp_path / "legacy.jsonl"
    v4_path = tmp_path / "v4.jsonl"
    output_dir = tmp_path / "out"
    _write_jsonl(input_path, legacy_rows)
    _write_jsonl(v4_path, v4_rows)

    summary = repair_dataset(input_path, v4_path, output_dir)

    assert summary["kept_records"] == 1
    assert summary["quarantined_records"] == 0
    assert summary["legacy_issue_url_only_records"] == 1
    assert summary["legacy_issue_url_only_high_confidence_records"] == 1

    repaired_row = json.loads((output_dir / "records" / "document_records.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert repaired_row["quality_status"] == "review"
    assert "legacy_issue_url_only" in repaired_row["quality_flags"]


def test_repair_row_salvages_article_embedded_under_conference_banner() -> None:
    title_index = {}
    row = {
        "doc_id": "x4",
        "title": "Ở BỆNH NHÂN LOÉT DẠ DÀY TÁ TRÀNG TẠI BỆNH VIỆN TRƯỜNG ĐẠI HỌC Y KHOA VINH",
        "section_title": "SUMMARY",
        "body": (
            "TREATMENT RESULTS AND SIDE EFFECTS OF 4-DRUG REGIMEN WITH BISMUTH PTMB "
            "IN ERATING HELICOBACTER PYLORI IN PATIENTS WITH GASTRIC AND DUODENAL ULCERS "
            "AT VINH MEDICAL UNIVERSITY HOSPITAL Objective: To evaluate efficacy and side effects. "
            "Subjects and methods: Case-series study on 34 patients from 04/2023 to 04/2024. "
            "HỘI NGHỊ KHOA HỌC CÔNG NGHỆ MỞ RỘNG NĂM 2024 - TRƯỜNG ĐẠI HỌC Y KHOA VINH "
            "Results: eradication rate 91.2%, side effects 44.1%, all mild. " + ("X" * 500)
        ),
        "source_url": "https://site/issue/view/311",
        "quality_status": "review",
        "quality_score": 95,
        "quality_flags": [],
        "tags": [],
    }

    decision, repaired = repair_row(row, title_index)

    assert decision == "keep"
    assert repaired["source_url"] == "https://site/issue/view/311"
    assert "legacy_title_salvaged_from_body" in repaired["quality_flags"]
    assert "conference_banner_stripped" in repaired["quality_flags"]
    assert "VINH MEDICAL UNIVERSITY HOSPITAL" in repaired["title"]
