import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from types import SimpleNamespace

from app.article_aggregator import ArticleGroup
from app.retriever import (
    RetrievedChunk,
    QdrantRetriever,
    _expand_query_for_retrieval,
    _chunk_query_bonus,
)


def test_expands_cea_cas_query_with_carotid_hints():
    query = "Theo context, quyết định chọn CEA hay CAS và theo dõi sau can thiệp cần dựa trên yếu tố nào?"
    expanded = _expand_query_for_retrieval(query)
    assert "carotid endarterectomy" in expanded
    assert "dong mach canh" in expanded


def test_expands_pieb_cei_query_with_epidural_hints():
    query = "Có thể kết luận PIEB vượt trội so với CEI ở mọi khía cạnh không?"
    expanded = _expand_query_for_retrieval(query)
    assert "ngoai mang cung" in expanded
    assert "epidural labor analgesia" in expanded


def test_leaves_normal_query_unchanged():
    query = "Ở người cao tuổi bị thoái hóa khớp, nên can thiệp sớm thế nào?"
    expanded = _expand_query_for_retrieval(query)
    assert "osteoarthritis" in expanded
    assert "early intervention" in expanded


def test_leaves_non_alias_query_unchanged():
    query = "Bệnh viêm phổi cộng đồng điều trị ra sao?"
    assert _expand_query_for_retrieval(query) == query


def test_chunk_query_bonus_prefers_title_and_acronym_alignment_for_article_centric_queries():
    query = "Theo context, chọn CEA hay CAS dựa trên yếu tố nào?"
    aligned_payload = {
        "title": "ĐIỀU TRỊ HẸP ĐỘNG MẠCH CẢNH BẰNG CEA HAY CAS",
        "section_title": "Lựa chọn kỹ thuật",
    }
    distractor_payload = {
        "title": "GIÁ TRỊ CỦA XÉT NGHIỆM CEA TRONG UNG THƯ ĐẠI TRỰC TRÀNG",
        "section_title": "Kết quả nghiên cứu",
    }

    assert _chunk_query_bonus(query, aligned_payload, "article_centric") > _chunk_query_bonus(
        query,
        distractor_payload,
        "article_centric",
    )


def test_expand_primary_article_chunks_surfaces_exact_answer_chunk_from_same_article():
    class FakeClient:
        def scroll(self, **kwargs):
            points = [
                SimpleNamespace(
                    id="extra-1",
                    score=0.0,
                    payload={
                        "text": (
                            "Kết quả cho thấy tỷ lệ hồi phục chức năng vận động đạt 96,18%. "
                            "Điểm Karnofsky trên 80 được duy trì trong 3-6 tháng theo dõi."
                        ),
                        "title": "ỨNG DỤNG KỸ THUẬT ĐỊNH VỊ THẦN KINH...",
                        "section_title": "Kết quả",
                        "doc_id": "vmj-q013",
                        "chunk_index": 8,
                    },
                ),
                SimpleNamespace(
                    id="extra-2",
                    score=0.0,
                    payload={
                        "text": "Title: ỨNG DỤNG KỸ THUẬT ĐỊNH VỊ THẦN KINH... Source: Tạp chí Y học Việt Nam.",
                        "title": "ỨNG DỤNG KỸ THUẬT ĐỊNH VỊ THẦN KINH...",
                        "section_title": "SUMMARY",
                        "doc_id": "vmj-q013",
                        "chunk_index": 1,
                    },
                ),
            ]
            return (points, None)

    retriever = QdrantRetriever.__new__(QdrantRetriever)
    retriever.client = FakeClient()
    retriever.collection = "medical_docs"
    retriever.max_context_tokens = 4096

    article = ArticleGroup(
        title="ỨNG DỤNG KỸ THUẬT ĐỊNH VỊ THẦN KINH...",
        title_norm="ung dung ky thuat dinh vi than kinh",
        chunks=[
            RetrievedChunk(
                id="seed-1",
                text="Tỷ lệ nhóm 1 chiếm 88,14% sau 1-3 tháng và 92,16% sau 3-6 tháng.",
                score=0.72,
                metadata={
                    "title": "ỨNG DỤNG KỸ THUẬT ĐỊNH VỊ THẦN KINH...",
                    "section_title": "Bảng 3",
                    "doc_id": "vmj-q013",
                    "chunk_index": 4,
                },
            )
        ],
        chunk_count=1,
    )

    expanded = retriever.expand_primary_article_chunks(
        article,
        "Sau phẫu thuật u tế bào hình sao bậc thấp có sử dụng hệ thống định vị thần kinh, tỷ lệ bệnh nhân hồi phục chức năng vận động là bao nhiêu và chất lượng sống được duy trì như thế nào?",
        max_chunks=3,
    )

    assert any("96,18%" in chunk.text for chunk in expanded), expanded
    assert expanded[0].metadata.get("section_title") == "Kết quả"
