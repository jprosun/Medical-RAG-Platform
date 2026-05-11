from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules():
    data_paths = importlib.import_module("services.utils.data_paths")
    data_paths = importlib.reload(data_paths)
    crawl_manifest = importlib.import_module("services.utils.crawl_manifest")
    crawl_manifest = importlib.reload(crawl_manifest)
    module = importlib.import_module("pipelines.etl.vmj_ojs_etl")
    module = importlib.reload(module)
    return data_paths, crawl_manifest, module


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_vmj_partitioned_etl_excludes_issue_bundles(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    data_paths, crawl_manifest, module = _reload_modules()

    crawl_manifest.write_manifest(
        "vmj_ojs",
        [
            {
                "source_id": "vmj_ojs",
                "relative_path": "sources/vmj_ojs/raw/bai-viet.pdf",
                "content_class": "pdf",
                "extract_status": "done",
                "extract_strategy": "digital_pdf_text",
                "item_url": "https://example.org/bai-viet",
                "file_url": "https://example.org/bai-viet.pdf",
            },
            {
                "source_id": "vmj_ojs",
                "relative_path": "sources/vmj_ojs/raw/ky-yeu.pdf",
                "content_class": "pdf",
                "extract_status": "done",
                "extract_strategy": "digital_pdf_text",
                "item_url": "https://example.org/ky-yeu",
                "file_url": "https://example.org/ky-yeu.pdf",
            },
        ],
    )

    processed_dir = data_paths.source_processed_dir("vmj_ojs")
    _write_text(
        processed_dir / "bai-viet.txt",
        """---
source_id: vmj_ojs
title: Nghiên cứu dinh dưỡng lâm sàng ở người bệnh
source_url: https://example.org/bai-viet
file_url: https://example.org/bai-viet.pdf
pages: 8
chars: 5400
---

TÓM TẮT
Nghiên cứu này mô tả đặc điểm lâm sàng và đánh giá hiệu quả can thiệp dinh dưỡng ở nhóm người bệnh nội trú.
Nội dung bao gồm phương pháp, kết quả và bàn luận đủ dài để vượt qua ETL gate.
Kết luận nhấn mạnh vai trò sàng lọc nguy cơ dinh dưỡng và theo dõi định kỳ.
""",
    )
    _write_text(
        processed_dir / "ky-yeu.txt",
        """---
source_id: vmj_ojs
title: HỘI NGHỊ KHOA HỌC THƯỜNG NIÊN 2024
source_url: https://example.org/ky-yeu
file_url: https://example.org/ky-yeu.pdf
pages: 320
chars: 820000
---

TÓM TẮT
Bài thứ nhất trong kỷ yếu.

TÓM TẮT
Bài thứ hai trong kỷ yếu.

TÓM TẮT
Bài thứ ba trong kỷ yếu.
""",
    )

    report = module.run_partitioned_etl()

    assert report["article_only"]["total_files"] == 1
    assert report["issue_bundle_backlog"]["backlog_assets"] == 1

    canonical_records = data_paths.source_records_path("vmj_ojs")
    backlog_path = data_paths.source_partition_records_path("vmj_ojs", "issue_bundle_backlog", "backlog.jsonl")
    assert canonical_records.exists()
    assert backlog_path.exists()

    with open(canonical_records, "r", encoding="utf-8") as fh:
        records = [json.loads(line) for line in fh if line.strip()]
    assert len(records) >= 1
    assert all("HỘI NGHỊ KHOA HỌC THƯỜNG NIÊN 2024" not in rec["title"] for rec in records)

    with open(backlog_path, "r", encoding="utf-8") as fh:
        backlog = [json.loads(line) for line in fh if line.strip()]
    assert backlog[0]["reason"] in {"oversized_issue_bundle", "large_multi_article_pdf", "conference_or_proceedings_title"}
