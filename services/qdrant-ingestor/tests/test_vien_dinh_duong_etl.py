from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import fitz


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules():
    data_paths = importlib.import_module("services.utils.data_paths")
    data_paths = importlib.reload(data_paths)
    crawl_manifest = importlib.import_module("services.utils.crawl_manifest")
    crawl_manifest = importlib.reload(crawl_manifest)
    module = importlib.import_module("pipelines.etl.vien_dinh_duong_etl")
    module = importlib.reload(module)
    return data_paths, crawl_manifest, module


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _create_pdf(path: Path, page_texts: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = fitz.open()
    try:
        for text in page_texts:
            page = doc.new_page()
            page.insert_text((72, 72), text)
        doc.save(path)
    finally:
        doc.close()


def test_vien_dinh_duong_article_only_filters_booklet_like_done(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    data_paths, crawl_manifest, module = _reload_modules()

    processed_dir = data_paths.source_processed_dir("vien_dinh_duong")
    manifest_rows = [
        {
            "source_id": "vien_dinh_duong",
            "relative_path": "sources/vien_dinh_duong/raw/bai-viet.pdf",
            "content_class": "pdf",
            "extract_status": "done",
            "extract_strategy": "digital_pdf_text",
            "item_url": "https://example.org/bai-viet.pdf",
        },
        {
            "source_id": "vien_dinh_duong",
            "relative_path": "sources/vien_dinh_duong/raw/so-tay.pdf",
            "content_class": "pdf",
            "extract_status": "done",
            "extract_strategy": "digital_pdf_text",
            "item_url": "https://example.org/so-tay.pdf",
        },
    ]
    crawl_manifest.write_manifest("vien_dinh_duong", manifest_rows)

    article_text = """---
source_id: vien_dinh_duong
title: Bài viết dinh dưỡng cộng đồng
source_url: https://example.org/bai-viet.pdf
pages: 2
---

Bài viết dinh dưỡng cộng đồng này trình bày các nguyên tắc ăn uống lành mạnh cho người trưởng thành.
Nội dung giải thích cách phối hợp nhóm thực phẩm, kiểm soát khẩu phần, và theo dõi tình trạng dinh dưỡng.
Đây là đoạn văn đủ dài để vượt qua gate chất lượng của ETL tiếng Việt.
"""
    booklet_text = """---
source_id: vien_dinh_duong
title: Mục lục
source_url: https://example.org/so-tay.pdf
pages: 4
---

MỤC LỤC
1 Dinh dưỡng lâm sàng ............................................. 8
2 Đánh giá tình trạng dinh dưỡng ................................ 18
3 Xây dựng khẩu phần ............................................. 40
4 Quy trình chăm sóc dinh dưỡng ................................ 52
5 Dinh dưỡng điều trị bệnh tim mạch ............................. 65
6 Dinh dưỡng điều trị đái tháo đường ............................ 77
"""
    _write_text(processed_dir / "bai-viet.txt", article_text)
    _write_text(processed_dir / "so-tay.txt", booklet_text)

    summary = module.run_article_only_etl()

    assert summary["excluded_book_like_assets"] == 1

    canonical_records = data_paths.source_records_path("vien_dinh_duong")
    partition_records = data_paths.source_partition_records_path("vien_dinh_duong", "article_only")
    assert canonical_records.exists()
    assert partition_records.exists()

    with open(canonical_records, "r", encoding="utf-8") as fh:
        records = [json.loads(line) for line in fh if line.strip()]

    assert len(records) == 1
    assert "Mục lục" not in records[0]["title"]


def test_vien_dinh_duong_long_pdf_book_pipeline_emits_records(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    data_paths, crawl_manifest, module = _reload_modules()

    raw_path = data_paths.source_raw_dir("vien_dinh_duong") / "cam-nang.pdf"
    _create_pdf(
        raw_path,
        [
            "Cam nang dinh duong lam sang\nNoi dung trang mot ve danh gia tinh trang dinh duong va can thiep.",
            "Trang hai mo ta xay dung khau phan va theo doi nguoi benh trong qua trinh dieu tri.",
            "Trang ba trinh bay huong dan nuoi duong duong ruot va cac luu y khi thuc hien.",
            "Trang bon bao gom cac bang tham khao va muc tieu can nang cho tung nhom doi tuong.",
            "Trang nam tong hop quy trinh cham soc dinh duong va danh sach tai lieu can doc them.",
        ],
    )

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "relative_path": "sources/vien_dinh_duong/raw/cam-nang.pdf",
                "content_class": "pdf",
                "extract_status": "deferred",
                "extract_strategy": "long_pdf_book",
                "item_url": "https://example.org/cam-nang.pdf",
            }
        ],
    )

    summary = module.run_long_pdf_book_etl()
    output_path = data_paths.source_partition_records_path("vien_dinh_duong", "long_pdf_book")

    assert summary["record_count"] >= 1
    assert output_path.exists()

    with open(output_path, "r", encoding="utf-8") as fh:
        first = json.loads(next(line for line in fh if line.strip()))

    assert first["source_id"] == "vien_dinh_duong"
    assert first["doc_type"] == "textbook"
    assert first["section_title"].startswith("Pages ")


def test_vien_dinh_duong_long_pdf_book_rejects_generic_download_title(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    data_paths, crawl_manifest, module = _reload_modules()

    raw_path = data_paths.source_raw_dir("vien_dinh_duong") / "tai-xuong.pdf"
    _create_pdf(
        raw_path,
        [
            "DINH DUONG LAM SANG\nNoi dung trang mot gioi thieu sach dinh duong lam sang, bao gom cac muc tieu danh gia tinh trang dinh duong, xac dinh nguy co va lua chon chien luoc can thiep phu hop cho nguoi benh trong moi boi canh lam sang.",
            "Trang hai tiep tuc noi dung va huong dan danh gia tinh trang dinh duong, trinh bay cach khai thac tien su, do nhan trac, tong hop dau hieu lam sang va dien giai cac chi so can theo doi trong qua trinh dieu tri.",
            "Trang ba noi ve xay dung khau phan va theo doi nguoi benh, giai thich cach tinh nhu cau nang luong, protein, vi chat, dieu chinh theo benh ly va giam sat dap ung can thiep trong tung giai doan.",
            "Trang bon tong hop cac nguyen tac can thuc hien trong thuc hanh, nhan manh vai tro phoi hop lien chuyen khoa, giao duc nguoi benh va danh gia ket qua de cap nhat ke hoach cham soc dinh duong.",
        ],
    )
    processed_stub = data_paths.source_processed_dir("vien_dinh_duong") / "tai-xuong.txt"
    _write_text(
        processed_stub,
        """---
source_id: vien_dinh_duong
title: Tải xuống
source_url: https://example.org/tai-xuong.pdf
pages: 4
---

Mục lục
1 Dinh dưỡng lâm sàng
2 Đánh giá tình trạng dinh dưỡng
""",
    )

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "relative_path": "sources/vien_dinh_duong/raw/tai-xuong.pdf",
                "content_class": "pdf",
                "extract_status": "deferred",
                "extract_strategy": "long_pdf_book",
                "item_url": "https://example.org/tai-xuong.pdf",
            }
        ],
    )

    summary = module.run_long_pdf_book_etl()
    assert summary["record_count"] >= 1

    output_path = data_paths.source_partition_records_path("vien_dinh_duong", "long_pdf_book")
    with open(output_path, "r", encoding="utf-8") as fh:
        first = json.loads(next(line for line in fh if line.strip()))

    assert first["title"] != "Tải xuống"


def test_vien_dinh_duong_long_pdf_book_ocr_pipeline_emits_backlog_jobs(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    data_paths, crawl_manifest, module = _reload_modules()

    raw_path = data_paths.source_raw_dir("vien_dinh_duong") / "so-tay-scan.pdf"
    _create_pdf(raw_path, ["Trang scan 1", "Trang scan 2"])

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "relative_path": "sources/vien_dinh_duong/raw/so-tay-scan.pdf",
                "content_class": "pdf",
                "extract_status": "deferred",
                "extract_strategy": "long_pdf_book_ocr",
                "item_url": "https://example.org/so-tay-scan.pdf",
            }
        ],
    )

    summary = module.run_long_pdf_book_ocr_pipeline()
    output_path = data_paths.source_partition_records_path("vien_dinh_duong", "long_pdf_book_ocr", "ocr_jobs.jsonl")

    assert summary["ocr_jobs"] == 1
    assert output_path.exists()

    with open(output_path, "r", encoding="utf-8") as fh:
        job = json.loads(next(line for line in fh if line.strip()))

    assert job["extract_strategy"] == "long_pdf_book_ocr"
    assert job["ocr_required"] is True


def test_vien_dinh_duong_article_only_filters_slide_download_listing(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    data_paths, crawl_manifest, module = _reload_modules()

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "relative_path": "sources/vien_dinh_duong/raw/slide-trinh-chieu.html",
                "content_class": "html",
                "extract_status": "done",
                "extract_strategy": "html_text",
                "item_url": "https://example.org/slide-trinh-chieu.html",
            }
        ],
    )

    processed_dir = data_paths.source_processed_dir("vien_dinh_duong")
    _write_text(
        processed_dir / "slide-trinh-chieu.txt",
        """---
source_id: vien_dinh_duong
title: Slide trình chiếu
source_url: https://example.org/slide-trinh-chieu.html
pages: 1
---

Bài 5- Chế độ ăn uống lành mạnh và không lành mạnh
24/07/2025 09:49:42
Tải xuống
Bài 4- Chúng ta cần làm gì khi bị thừa cân, béo phì
24/07/2025 09:48:23
Tải xuống
Bài 3- Phòng chống thừa cân béo phì
24/07/2025 09:47:00
Tải xuống
""",
    )

    summary = module.run_article_only_etl()
    assert summary["total_records"] == 0
    assert summary["excluded_book_like_assets"] == 1


def test_vien_dinh_duong_article_only_filters_about_pages(tmp_path, monkeypatch):
    rag_root = tmp_path / "rag-data"
    legacy_root = tmp_path / "data"
    monkeypatch.setenv("RAG_DATA_ROOT", str(rag_root))
    monkeypatch.setenv("LEGACY_DATA_ROOT", str(legacy_root))

    data_paths, crawl_manifest, module = _reload_modules()

    crawl_manifest.write_manifest(
        "vien_dinh_duong",
        [
            {
                "source_id": "vien_dinh_duong",
                "relative_path": "sources/vien_dinh_duong/raw/67e4d788cd449ce98907cfee.html",
                "content_class": "html",
                "extract_status": "done",
                "extract_strategy": "html_text",
                "item_url": "https://viendinhduong.vn/vi/about/don-vi-trong-vien/67e4d788cd449ce98907cfee",
            }
        ],
    )

    processed_dir = data_paths.source_processed_dir("vien_dinh_duong")
    _write_text(
        processed_dir / "67e4d788cd449ce98907cfee.txt",
        """---
source_id: vien_dinh_duong
title: 67e4d788cd449ce98907cfee
source_url:
---

Trung tâm Dịch vụ Khoa học kỹ thuật Dinh dưỡng – Thực phẩm
Nội dung giới thiệu tổ chức có đủ độ dài để ETL bình thường nếu không bị lọc khỏi article-only.
Nội dung giới thiệu tổ chức có đủ độ dài để ETL bình thường nếu không bị lọc khỏi article-only.
""",
    )

    summary = module.run_article_only_etl()
    assert summary["total_records"] == 0
    assert summary["excluded_book_like_assets"] == 1
