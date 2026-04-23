import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.chunk_quality_filter import _is_junk_title, filter_chunks
from app.retriever import RetrievedChunk


def test_dense_citation_title_rejected():
    assert _is_junk_title("2024;12:1098765. associated with nursing evidence")


def test_normal_title_not_rejected():
    assert not _is_junk_title("HIỆU QUẢ GIẢM ĐAU Ở THAI PHỤ CHUYỂN DẠ BẰNG PIEB")


def test_reference_section_metadata_is_rejected():
    chunks = [
        RetrievedChunk(
            id="ref-1",
            text="Nội dung nghiên cứu hợp lệ nhưng nằm trong mục tài liệu tham khảo.",
            score=0.8,
            metadata={
                "title": "NGHIÊN CỨU THỬ NGHIỆM LÂM SÀNG",
                "section_title": "Tài liệu tham khảo",
            },
        )
    ]

    assert filter_chunks(chunks) == []
