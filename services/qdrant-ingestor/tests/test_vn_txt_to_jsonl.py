from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module(monkeypatch, rag_root: Path):
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(rag_root.parent / "data"))
    data_paths = importlib.import_module("services.utils.data_paths")
    importlib.reload(data_paths)
    module = importlib.import_module("pipelines.etl.vn.vn_txt_to_jsonl")
    return importlib.reload(module)


def test_process_file_skips_admin_signature_stub(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module = _reload_module(monkeypatch, rag_root)
    path = rag_root / "sources" / "kcb_moh" / "processed" / "2492-cvbyt-2025_signed_c2c3a.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """---
source_id: kcb_moh
title:
source_url: https://kcb.vn/example
file_url: https://kcb.vn/example.pdf
---

sonht.kcb_Ha Thai Son_25/04/2025 11:01:12
2492

Ký bởi: Bộ Y Tế
Cơ quan: Bộ Y Tế
Ngày ký: 25-04-2025 10:40:43 +07:00
sonht.kcb_Ha Thai Son_25/04/2025 11:01:12
""",
        encoding="utf-8",
    )

    records = module.process_file(str(path), source_id="kcb_moh", etl_run_id="etl-test")

    assert records == []


def test_process_file_skips_too_short_sections(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module = _reload_module(monkeypatch, rag_root)
    path = rag_root / "sources" / "dav_gov" / "processed" / "appendix.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """---
source_id: dav_gov
title:
source_url: https://dav.gov.vn/example
file_url: https://dav.gov.vn/example.pdf
---

Đơn đề nghị thu hồi giấy đăng ký lưu hành thuốc và tài liệu giải thích chi tiết kèm theo.
Nội dung mô tả bổ sung để vượt qua ngưỡng tối thiểu của toàn văn trước khi sectionize.
Nội dung mô tả bổ sung để vượt qua ngưỡng tối thiểu của toàn văn trước khi sectionize.
Nội dung mô tả bổ sung để vượt qua ngưỡng tối thiểu của toàn văn trước khi sectionize.
""",
        encoding="utf-8",
    )

    class FakeSection:
        def __init__(self, section_title: str, body: str, heading_path: str):
            self.section_title = section_title
            self.body = body
            self.heading_path = heading_path

    monkeypatch.setattr(
        module.vn_sectionizer,
        "sectionize",
        lambda title, body, source_id=None: [
            FakeSection("Short", "Hoạt chất: (*)", f"{title} > Short"),
            FakeSection("Long", "Nội dung hợp lệ " * 10, f"{title} > Long"),
        ],
    )

    records = module.process_file(str(path), source_id="dav_gov", etl_run_id="etl-test")

    assert len(records) == 1
    assert "Nội dung hợp lệ" in records[0]["body"]


def test_process_file_falls_back_to_item_url_for_source_url(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module = _reload_module(monkeypatch, rag_root)
    path = rag_root / "sources" / "who_vietnam" / "processed" / "news.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """---
source_id: who_vietnam
title: WHO Viet Nam update
item_url: https://example.org/who-vn-news
file_url:
---

WHO Viet Nam update describes public health activities in the country with enough text
to pass the ETL gate and keep a single rich record for downstream ingestion and retrieval.
WHO Viet Nam update describes public health activities in the country with enough text
to pass the ETL gate and keep a single rich record for downstream ingestion and retrieval.
WHO Viet Nam update describes public health activities in the country with enough text
to pass the ETL gate and keep a single rich record for downstream ingestion and retrieval.
""",
        encoding="utf-8",
    )

    records = module.process_file(str(path), source_id="who_vietnam", etl_run_id="etl-test")

    assert len(records) >= 1
    assert records[0]["source_url"] == "https://example.org/who-vn-news"


def test_process_file_repairs_who_vietnam_title_and_source_url_from_manifest(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module = _reload_module(monkeypatch, rag_root)
    crawl_manifest = importlib.import_module("services.utils.crawl_manifest")
    crawl_manifest = importlib.reload(crawl_manifest)

    path = rag_root / "sources" / "who_vietnam" / "processed" / "building-climate-resilient-health-systems.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """---
source_id: who_vietnam
title: Skip to main content
source_url:
file_url:
---

Skip to main content
Building climate-resilient health systems

Building climate-resilient health systems describes practical public health adaptation work,
health-system resilience, and climate-sensitive planning with enough detail for a valid ETL record.
Building climate-resilient health systems describes practical public health adaptation work,
health-system resilience, and climate-sensitive planning with enough detail for a valid ETL record.
""",
        encoding="utf-8",
    )
    crawl_manifest.write_manifest(
        "who_vietnam",
        [
            {
                "source_id": "who_vietnam",
                "relative_path": "sources/who_vietnam/raw/building-climate-resilient-health-systems.html",
                "content_class": "html",
                "extract_status": "done",
                "title_hint": "building-climate-resilient-health-systems",
                "notes": "bootstrapped_from_existing_raw",
            },
            {
                "source_id": "who_vietnam",
                "relative_path": "sources/who_vietnam/raw/building-climate-resilient-health-systems.html",
                "content_class": "html",
                "extract_status": "done",
                "title_hint": "Building climate-resilient health systems",
                "item_url": "https://www.who.int/westernpacific/activities/building-climate-resilient-health-systems",
            },
        ],
    )

    records = module.process_file(str(path), source_id="who_vietnam", etl_run_id="etl-test")

    assert len(records) >= 1
    assert records[0]["title"] == "Building climate-resilient health systems"
    assert records[0]["source_url"] == "https://www.who.int/westernpacific/activities/building-climate-resilient-health-systems"


def test_process_file_backfills_vien_dinh_duong_from_hash_sibling_and_quarantines_about(monkeypatch, tmp_path):
    rag_root = tmp_path / "rag-data"
    module = _reload_module(monkeypatch, rag_root)
    crawl_manifest = importlib.import_module("services.utils.crawl_manifest")
    crawl_manifest = importlib.reload(crawl_manifest)

    path = rag_root / "sources" / "vien_dinh_duong" / "processed" / "67e4d788cd449ce98907cfee.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """---
source_id: vien_dinh_duong
title: 67e4d788cd449ce98907cfee
source_url:
file_url:
---

Trung tâm Dịch vụ Khoa học kỹ thuật Dinh dưỡng – Thực phẩm

Nội dung giới thiệu tổ chức và cơ cấu đơn vị trong viện với đủ độ dài để nếu không có
quarantine thì ETL vẫn tạo record, nhưng đây không phải bài article phù hợp cho batch retrieval.
Nội dung giới thiệu tổ chức và cơ cấu đơn vị trong viện với đủ độ dài để nếu không có
quarantine thì ETL vẫn tạo record, nhưng đây không phải bài article phù hợp cho batch retrieval.
""",
        encoding="utf-8",
    )
    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "relative_path": "sources/vien_dinh_duong/raw/67e4d788cd449ce98907cfee.html",
                "content_class": "html",
                "extract_status": "done",
                "title_hint": "Xem chi tiết →",
                "item_url": "https://viendinhduong.vn/vi/about/don-vi-trong-vien/67e4d788cd449ce98907cfee",
            },
            {
                "source_id": "vien_dinh_duong",
                "relative_path": "sources/vien_dinh_duong/raw/trung-tam-dich-vu-khoa-hoc-ky-thuat-dinh-duong---thuc-pham-67e4d788cd449ce98907cfee.html",
                "content_class": "html",
                "extract_status": "done",
                "title_hint": "Trung tâm Dịch vụ Khoa học kỹ thuật Dinh dưỡng – Thực phẩm",
                "item_url": "https://viendinhduong.vn/vi/about/don-vi-trong-vien/trung-tam-dich-vu-khoa-hoc-ky-thuat-dinh-duong---thuc-pham-67e4d788cd449ce98907cfee",
            },
        ],
    )

    records = module.process_file(str(path), source_id="vien_dinh_duong", etl_run_id="etl-test")

    assert records == []
