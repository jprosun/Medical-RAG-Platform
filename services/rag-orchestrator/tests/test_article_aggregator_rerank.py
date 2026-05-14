import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataclasses import dataclass

from app.article_aggregator import aggregate_articles
from app.retriever import RetrievedChunk


@dataclass
class ArticleCentricRouter:
    query_type: str = "study_result_extraction"
    depth: str = "high"
    requires_numbers: bool = True
    requires_limitations: bool = False
    requires_comparison: bool = False
    answer_style: str = "exact"
    retrieval_profile: str = "standard"
    needs_extractor: bool = False
    retrieval_mode: str = "article_centric"


def test_article_centric_anchor_without_query_support_does_not_override_composite_primary():
    chunks = [
        RetrievedChunk(
            id="wrong-mental-1",
            text="Roi loan tram cam o thanh thieu nien co lien quan voi nguy co tu sat va can can thiep som.",
            score=0.69,
            metadata={
                "title": "TRAM CAM O THANH THIEU NIEN",
                "doc_id": "doc-mental",
                "source_name": "mil_med_pharm_journal",
            },
        ),
        RetrievedChunk(
            id="right-stroke-1",
            text="Nhoi mau nao cap co ty le tai thong mach cao hon khi can thiep trong cua so thoi gian phu hop.",
            score=0.64,
            metadata={
                "title": "DANH GIA KET QUA DIEU TRI NHOI MAU NAO CAP",
                "section_title": "Ket qua",
                "doc_id": "doc-stroke",
                "source_name": "VMJ",
            },
        ),
        RetrievedChunk(
            id="right-stroke-2",
            text="Benh nhan nhoi mau nao duoc theo doi NIHSS va muc do tu chu sau dieu tri.",
            score=0.62,
            metadata={
                "title": "DANH GIA KET QUA DIEU TRI NHOI MAU NAO CAP",
                "section_title": "Doi tuong va phuong phap",
                "doc_id": "doc-stroke",
                "source_name": "VMJ",
            },
        ),
    ]

    result = aggregate_articles(
        chunks,
        "Theo nghien cuu nay, ket qua dieu tri nhoi mau nao cap duoc danh gia qua nhung chi so nao?",
        ArticleCentricRouter(),
    )

    assert result.primary.title == "DANH GIA KET QUA DIEU TRI NHOI MAU NAO CAP"
    assert "highest composite score" in result.primary.selected_reason


def test_article_identity_bonus_prefers_matching_procedure_study_over_same_hospital_neighbor_article():
    chunks = [
        RetrievedChunk(
            id="wrong-gastric-1",
            text="Nghien cuu dac diem lam sang, noi soi va mo benh hoc cua benh nhan ung thu da day duoi 50 tuoi.",
            score=0.74,
            metadata={
                "title": "NGHIEN CUU DAC DIEM LAM SANG, HINH ANH NOI SOI VA MO BENH HOC CUA BENH NHAN UNG THU DA DAY DUOI 50 TUOI TAI BENH VIEN UNG BUOU DA NANG",
                "doc_id": "doc-wrong-gastric",
                "source_name": "vmj_ojs",
            },
        ),
        RetrievedChunk(
            id="right-surgery-1",
            text="Muc tieu nghien cuu mo ta dac diem lam sang, can lam sang va danh gia ket qua phau thuat noi soi cat toan bo da day do ung thu.",
            score=0.68,
            metadata={
                "title": "DANH GIA KET QUA PHAU THUAT NOI SOI CAT TOAN BO DA DAY DO UNG THU TAI BENH VIEN UNG BUOU DA NANG",
                "section_title": "Tom tat",
                "doc_id": "doc-right-surgery",
                "source_name": "vmj_ojs",
            },
        ),
    ]

    result = aggregate_articles(
        chunks,
        "Nghien cuu tai Benh vien Ung buou Da Nang danh gia ket qua phau thuat noi soi cat toan bo da day do ung thu nhu the nao?",
        ArticleCentricRouter(),
    )

    assert result.primary.title == "DANH GIA KET QUA PHAU THUAT NOI SOI CAT TOAN BO DA DAY DO UNG THU TAI BENH VIEN UNG BUOU DA NANG"


def test_article_identity_bonus_prefers_matching_hospital_for_bismuth_regimen_query():
    chunks = [
        RetrievedChunk(
            id="wrong-cantho-1",
            text="Nghien cuu mo ta cat ngang tren 75 benh nhan dieu tri phac do 4 thuoc co bismuth tai Benh vien Truong Dai hoc Y Duoc Can Tho.",
            score=0.75,
            metadata={
                "title": "DANH GIA HIEU QUA DIEU TRI CUA PHAC DO 4 THUOC CO BISMUTH TAI BENH VIEN TRUONG DAI HOC Y DUOC CAN THO",
                "doc_id": "doc-cantho",
                "source_name": "cantho_med_journal",
            },
        ),
        RetrievedChunk(
            id="right-vinh-1",
            text="Nghien cuu mo ta loat ca benh tren 34 benh nhan tai Benh vien Truong Dai hoc Y khoa Vinh tu 04/2023 den 04/2024.",
            score=0.64,
            metadata={
                "title": "DANH GIA KET QUA DIEU TRI VA TAC DUNG PHU CUA PHAC DO 4 THUOC CO BISMUTH PTMB TAI BENH VIEN TRUONG DAI HOC Y KHOA VINH",
                "section_title": "Doi tuong va phuong phap",
                "doc_id": "doc-vinh",
                "source_name": "vmj_ojs",
            },
        ),
    ]

    result = aggregate_articles(
        chunks,
        "Nghien cuu nay su dung thiet ke nao de danh gia hieu qua va tac dung phu cua phac do 4 thuoc co Bismuth PTMB tai Benh vien Truong Dai hoc Y khoa Vinh?",
        ArticleCentricRouter(),
    )

    assert result.primary.title == "DANH GIA KET QUA DIEU TRI VA TAC DUNG PHU CUA PHAC DO 4 THUOC CO BISMUTH PTMB TAI BENH VIEN TRUONG DAI HOC Y KHOA VINH"
