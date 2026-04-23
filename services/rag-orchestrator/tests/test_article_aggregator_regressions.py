import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataclasses import dataclass

from app.retriever import RetrievedChunk
from app.article_aggregator import aggregate_articles


@dataclass
class MockRouterOutput:
    query_type: str = "comparative_synthesis"
    depth: str = "high"
    requires_numbers: bool = True
    requires_limitations: bool = False
    requires_comparison: bool = True
    answer_style: str = "comparative"
    retrieval_profile: str = "deep"
    needs_extractor: bool = True
    retrieval_mode: str = "topic_summary"


def test_acronym_disambiguation_prefers_full_query_match_for_q094_style_case():
    """Article mentioning both CEA and CAS should beat a single-acronym distractor."""
    chunks = [
        RetrievedChunk(
            id="wrong-1",
            text="CEA trong ung thư đại trực tràng được khảo sát với giá trị tiên lượng.",
            score=0.56,
            metadata={
                "title": "NHẬN XÉT GIÁ TRỊ CỦA XÉT NGHIỆM CEA TRONG UNG THƯ ĐẠI TRỰC TRÀNG",
                "doc_id": "doc-wrong",
                "source_name": "VMJ",
            },
        ),
        RetrievedChunk(
            id="right-1",
            text="Lựa chọn CEA hay CAS cần cá thể hóa theo mức độ hẹp, triệu chứng và nguy cơ quanh thủ thuật.",
            score=0.55,
            metadata={
                "title": "CHẨN ĐOÁN VÀ ĐIỀU TRỊ HẸP ĐỘNG MẠCH CẢNH NGOÀI SỌ",
                "section_title": "Điều trị hẹp động mạch cảnh bằng CEA hay CAS",
                "doc_id": "doc-right",
                "source_name": "VMJ",
            },
        ),
    ]

    result = aggregate_articles(
        chunks,
        "Theo context, quyết định chọn CEA hay CAS và việc theo dõi sau can thiệp đều phải cá thể hóa dựa trên những nhóm yếu tố nào?",
        MockRouterOutput(),
    )

    assert result.primary.title == "CHẨN ĐOÁN VÀ ĐIỀU TRỊ HẸP ĐỘNG MẠCH CẢNH NGOÀI SỌ"


def test_secondary_filter_rejects_polluted_support_for_q095_style_case():
    """Secondary sources without PIEB/CEI alignment should not survive topic-summary selection."""
    chunks = [
        RetrievedChunk(
            id="primary-1",
            text="PIEB giúp giảm lượng thuốc tê và không tăng tác dụng phụ khác so với CEI.",
            score=0.51,
            metadata={
                "title": "HIỆU QUẢ GIẢM ĐAU Ở THAI PHỤ CHUYỂN DẠ BẰNG GÂY TÊ NGOÀI MÀNG CỨNG NGẮT QUÃNG TỰ ĐỘNG",
                "section_title": "So sánh PIEB với CEI",
                "doc_id": "doc-pieb",
                "source_name": "VMJ",
            },
        ),
        RetrievedChunk(
            id="noise-1",
            text="Thực hành điều dưỡng dựa trên bằng chứng cải thiện kỹ năng lâm sàng.",
            score=0.50,
            metadata={
                "title": "THỰC HÀNH ĐIỀU DƯỠNG DỰA TRÊN BẰNG CHỨNG",
                "doc_id": "doc-ebp",
                "source_name": "VMJ",
            },
        ),
    ]

    result = aggregate_articles(
        chunks,
        "Từ phần tổng quan này, có thể kết luận PIEB vượt trội tuyệt đối so với CEI ở mọi khía cạnh không?",
        MockRouterOutput(),
    )

    assert result.primary.title.startswith("HIỆU QUẢ GIẢM ĐAU")
    assert result.secondary == []
def test_article_centric_primary_stays_with_top_retrieval_anchor():
    """Article-centric selection should not flip primary away from the strongest retrieved article."""

    @dataclass
    class ArticleCentricRouter(MockRouterOutput):
        retrieval_mode: str = "article_centric"

    chunks = [
        RetrievedChunk(
            id="anchor-1",
            text="Quáº£n lÃ½ sá»›m báº±ng giÃ¡o dá»¥c sá»©c khá»e, dinh dÆ°á»¡ng, táº­p luyá»‡n vÃ kiá»ƒm soÃ¡t cÃ¢n náº·ng giÃºp giáº£m triá»‡u chá»©ng vÃ lÃ m cháº­m tiáº¿n triá»ƒn thoÃ¡i hÃ³a khá»›p.",
            score=0.66,
            metadata={
                "title": "THOÃI HÃ“A KHá»šP, THÃCH THá»¨C Vá»šI Sá»¨C KHá»ŽE NGÆ¯á»œI CAO TUá»”I VÃ€ Cá»¬A Sá»” CÆ Há»˜I CHO ÄIá»€U TRá»Š",
                "section_title": "Can thiá»‡p sá»›m",
                "doc_id": "doc-oa-primary",
                "source_name": "VMJ",
            },
        ),
        RetrievedChunk(
            id="anchor-2",
            text="Can thiá»‡p toÃ n diá»‡n trong cá»­a sá»• cÆ¡ há»™i cÃ³ thá»ƒ giáº£m Ä‘au vÃ cải thiện cháº¥t lÆ°á»£ng sá»‘ng á»Ÿ ngÆ°á»i bá»‡nh thoÃ¡i hÃ³a khá»›p.",
            score=0.58,
            metadata={
                "title": "THOÃI HÃ“A KHá»šP, THÃCH THá»¨C Vá»šI Sá»¨C KHá»ŽE NGÆ¯á»œI CAO TUá»”I VÃ€ Cá»¬A Sá»” CÆ Há»˜I CHO ÄIá»€U TRá»Š",
                "doc_id": "doc-oa-primary",
                "source_name": "VMJ",
            },
        ),
        RetrievedChunk(
            id="distractor-1",
            text="HÆ°á»›ng dáº«n tá»•ng quÃ¡t vá» thoÃ¡i hÃ³a khá»›p vÃ quản lÃ½ triá»‡u chá»©ng.",
            score=0.62,
            metadata={
                "title": "HÆ¯á»›NG DáºªN TÁ»”NG QUÃT VÃ€ Cáº¬P NHáº¬T ÄIá»€U TRá»Š THOÃI HÃ“A KHá»šP",
                "section_title": "Tá»•ng quan",
                "doc_id": "doc-guideline",
                "doc_type": "guideline",
                "trust_tier": 1,
                "source_name": "VMJ",
            },
        ),
        RetrievedChunk(
            id="distractor-2",
            text="ThoÃ¡i hÃ³a khá»›p cÃ³ thá»ƒ Ä‘Æ°á»£c quản lÃ½ báº±ng cÃ¡c biá»‡n phÃ¡p dÆ°á»£c lÃ½ vÃ không dÆ°á»£c lÃ½.",
            score=0.61,
            metadata={
                "title": "HÆ¯á»›NG DáºªN TÁ»”NG QUÃT VÃ€ Cáº¬P NHáº¬T ÄIá»€U TRá»Š THOÃI HÃ“A KHá»šP",
                "section_title": "Äiá»u trá»‹",
                "doc_id": "doc-guideline",
                "doc_type": "guideline",
                "trust_tier": 1,
                "source_name": "VMJ",
            },
        ),
        RetrievedChunk(
            id="distractor-3",
            text="Bá»‡nh thoÃ¡i hÃ³a khá»›p á»Ÿ ngÆ°á»i cao tuá»•i cáº§n can thiá»‡p sá»›m Ä‘á»ƒ hạn cháº¿ gÃ¡nh nặng bá»‡nh táº­t.",
            score=0.60,
            metadata={
                "title": "HÆ¯á»›NG DáºªN TÁ»”NG QUÃT VÃ€ Cáº¬P NHáº¬T ÄIá»€U TRá»Š THOÃI HÃ“A KHá»šP",
                "section_title": "NgÆ°á»i cao tuá»•i",
                "doc_id": "doc-guideline",
                "doc_type": "guideline",
                "trust_tier": 1,
                "source_name": "VMJ",
            },
        ),
    ]

    result = aggregate_articles(
        chunks,
        "Theo context, can thiá»‡p sá»›m nÃ o giÃºp giáº£m triá»‡u chá»©ng vÃ lÃ m cháº­m tiáº¿n triá»ƒn thoÃ¡i hÃ³a khá»›p?",
        ArticleCentricRouter(),
    )

    assert result.primary.title.startswith("THOÃI HÃ“A KHá»šP")
    assert "article-centric retrieval anchor" in result.primary.selected_reason


def test_article_centric_drops_secondary_articles_entirely():
    @dataclass
    class ArticleCentricRouter(MockRouterOutput):
        retrieval_mode: str = "article_centric"
        query_type: str = "comparative_synthesis"
        answer_style: str = "summary"

    chunks = [
        RetrievedChunk(
            id="primary-1",
            text="Nghiên cứu này đánh giá tình trạng nhập viện trên lâm sàng và cận lâm sàng cùng kết quả điều trị ở bệnh nhân suy tim mạn tính.",
            score=0.61,
            metadata={
                "title": "ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ BỆNH NHÂN SUY TIM MÃN TÍNH",
                "section_title": "SUMMARY",
                "doc_id": "doc-hf-primary",
                "source_name": "VMJ",
            },
        ),
        RetrievedChunk(
            id="secondary-1",
            text="Phân suất tống máu thất trái, huyết áp tâm thu và thuốc điều trị liên quan đến tiên lượng.",
            score=0.60,
            metadata={
                "title": "THỰC TRẠNG TUÂN THỦ ĐIỀU TRỊ Ở BỆNH NHÂN SUY TIM MẠN TÍNH",
                "section_title": "SUMMARY",
                "doc_id": "doc-hf-secondary",
                "source_name": "VMJ",
            },
        ),
    ]

    result = aggregate_articles(
        chunks,
        "Theo nghiên cứu này, kết quả điều trị ở bệnh nhân suy tim mạn tính được đánh giá qua những chỉ số hoặc tiêu chí nào?",
        ArticleCentricRouter(),
    )

    assert result.primary.title.startswith("ĐÁNH GIÁ KẾT QUẢ ĐIỀU TRỊ")
    assert result.secondary == []


def test_topic_summary_secondary_must_add_distinct_query_support():
    chunks = [
        RetrievedChunk(
            id="primary-1",
            text="Can thiệp sớm bằng giáo dục, dinh dưỡng và tập luyện giúp giảm triệu chứng thoái hóa khớp.",
            score=0.67,
            metadata={
                "title": "THOÁI HÓA KHỚP Ở NGƯỜI CAO TUỔI VÀ CỬA SỔ CƠ HỘI ĐIỀU TRỊ",
                "section_title": "Can thiệp sớm",
                "doc_id": "doc-oa-1",
                "source_name": "VMJ",
            },
        ),
        RetrievedChunk(
            id="secondary-repeat",
            text="Giáo dục sức khỏe và tập luyện cũng là trọng tâm quản lý triệu chứng thoái hóa khớp.",
            score=0.66,
            metadata={
                "title": "CẬP NHẬT QUẢN LÝ THOÁI HÓA KHỚP",
                "section_title": "Can thiệp sớm",
                "doc_id": "doc-oa-2",
                "source_name": "VMJ",
            },
        ),
    ]

    @dataclass
    class TopicSummaryRouter(MockRouterOutput):
        query_type: str = "teaching_explainer"
        retrieval_mode: str = "topic_summary"

    result = aggregate_articles(
        chunks,
        "Ở người cao tuổi bị thoái hóa khớp, nên can thiệp sớm thế nào để giảm triệu chứng?",
        TopicSummaryRouter(),
    )

    assert result.primary.title.startswith("THOÁI HÓA KHỚP")
    assert result.secondary == []
