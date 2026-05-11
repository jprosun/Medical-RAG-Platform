from __future__ import annotations

import json
from pathlib import Path

from tools.prepare_embed_ready_release import build_embed_ready_release, filter_record


def test_filter_record_drops_missing_provenance_and_bad_title() -> None:
    record = {
        "title": "Skip to main content",
        "body": "A" * 200,
        "source_url": "",
        "quality_status": "go",
        "quality_flags": [],
    }
    reasons = filter_record(record)
    assert "bad_title" in reasons
    assert "missing_source_url" in reasons


def test_build_embed_ready_release_writes_only_kept_records(tmp_path: Path) -> None:
    input_path = tmp_path / "records.jsonl"
    output_path = tmp_path / "embed_ready.jsonl"
    report_path = tmp_path / "report.json"
    records = [
        {
            "doc_id": "ok",
            "title": "Useful Article",
            "body": "A" * 200,
            "source_url": "https://example.org/a",
            "source_id": "src_a",
            "quality_status": "go",
            "quality_flags": [],
        },
        {
            "doc_id": "bad",
            "title": "Document",
            "body": "short",
            "source_url": "",
            "source_id": "src_b",
            "quality_status": "hold",
            "quality_flags": ["release_body_too_short"],
        },
    ]
    with open(input_path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    report = build_embed_ready_release(
        input_path=input_path,
        output_path=output_path,
        report_path=report_path,
    )

    kept_rows = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(kept_rows) == 1
    assert json.loads(kept_rows[0])["doc_id"] == "ok"
    assert report["input_records"] == 2
    assert report["output_records"] == 1
    assert report["drop_reasons"]["missing_source_url"] == 1
    assert report["drop_reasons"]["quality_hold"] == 1
