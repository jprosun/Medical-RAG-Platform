import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from types import SimpleNamespace

from app.retriever import QdrantRetriever


class FakePointClient:
    def __init__(self, points):
        self._points = points

    def query_points(self, **kwargs):
        return SimpleNamespace(points=list(self._points))


def _build_retriever(points, max_context_tokens=100):
    retriever = object.__new__(QdrantRetriever)
    retriever.client = FakePointClient(points)
    retriever.collection = "test"
    retriever.top_k = 2
    retriever.score_threshold = 0.0
    retriever.max_context_tokens = max_context_tokens
    retriever.deduplicate = True
    retriever._model_name = "fake"
    retriever._embed_query = lambda text: [0.1, 0.2, 0.3]
    return retriever


def test_retriever_prefers_chunk_with_title_and_text_overlap():
    points = [
        SimpleNamespace(
            id="wrong",
            score=0.55,
            payload={
                "text": "Noi dung ve tram cam va can thiep tam ly cho thanh thieu nien.",
                "title": "TRAM CAM O THANH THIEU NIEN",
                "doc_id": "doc-mental",
                "source_name": "journal",
            },
        ),
        SimpleNamespace(
            id="right",
            score=0.55,
            payload={
                "text": "Nhoi mau nao cap can duoc danh gia NIHSS va theo doi tai thong mach.",
                "title": "DANH GIA KET QUA DIEU TRI NHOI MAU NAO CAP",
                "doc_id": "doc-stroke",
                "source_name": "journal",
            },
        ),
    ]
    retriever = _build_retriever(points)

    chunks = retriever.retrieve("ket qua dieu tri nhoi mau nao cap")

    assert [chunk.id for chunk in chunks[:1]] == ["right"]


def test_retriever_penalizes_admin_reference_source_for_study_query():
    points = [
        SimpleNamespace(
            id="admin",
            score=0.60,
            payload={
                "text": "Chi tiet ve phu luc quy dinh dang ky luu hanh thuoc va cap phep nhap khau.",
                "title": "PHU LUC DIEU CHINH CAP PHEP NHAP KHAU",
                "doc_type": "reference",
                "source_name": "dav_gov",
                "doc_id": "doc-admin",
            },
        ),
        SimpleNamespace(
            id="journal",
            score=0.56,
            payload={
                "text": "Nhoi mau nao cap duoc danh gia bang NIHSS, mRS va tai thong mach sau dieu tri.",
                "title": "DANH GIA KET QUA DIEU TRI NHOI MAU NAO CAP",
                "source_name": "VMJ",
                "doc_id": "doc-journal",
            },
        ),
    ]
    retriever = _build_retriever(points)

    chunks = retriever.retrieve(
        "Theo nghien cuu nay, ket qua dieu tri nhoi mau nao cap duoc danh gia qua nhung chi so nao?",
        retrieval_mode="article_centric",
        query_type="study_result_extraction",
        answer_style="exact",
    )

    assert [chunk.id for chunk in chunks[:1]] == ["journal"]


def test_retriever_penalizes_issue_bundle_titles_for_study_query():
    points = [
        SimpleNamespace(
            id="issue",
            score=0.61,
            payload={
                "text": "Abstract ve ung thu gan va dap ung TACE.",
                "title": "TAP CHI Y DUOC HOC QUAN SU - SO DAC BIET 5/2",
                "doc_type": "review",
                "source_name": "mil_med_pharm_journal",
                "doc_id": "doc-issue",
            },
        ),
        SimpleNamespace(
            id="article",
            score=0.57,
            payload={
                "text": "Nhoi mau nao cap duoc danh gia bang NIHSS, mRS va muc do tai thong mach.",
                "title": "DANH GIA KET QUA DIEU TRI NHOI MAU NAO CAP",
                "source_name": "VMJ",
                "doc_id": "doc-article",
            },
        ),
    ]
    retriever = _build_retriever(points)

    chunks = retriever.retrieve(
        "Theo nghien cuu nay, ket qua dieu tri nhoi mau nao cap duoc danh gia qua nhung chi so nao?",
        retrieval_mode="article_centric",
        query_type="study_result_extraction",
        answer_style="exact",
    )

    assert [chunk.id for chunk in chunks[:1]] == ["article"]


def test_retriever_candidate_pool_is_larger_than_prompt_budget():
    points = [
        SimpleNamespace(
            id="c1",
            score=0.60,
            payload={"text": " ".join(["nhoi", "mau", "nao", "cap", "dieu", "tri", "cohort1"] * 15), "title": "A", "doc_id": "a"},
        ),
        SimpleNamespace(
            id="c2",
            score=0.59,
            payload={"text": " ".join(["nhoi", "mau", "nao", "cap", "dieu", "tri", "cohort2"] * 15), "title": "B", "doc_id": "b"},
        ),
        SimpleNamespace(
            id="c3",
            score=0.58,
            payload={"text": " ".join(["nhoi", "mau", "nao", "cap", "dieu", "tri", "cohort3"] * 15), "title": "C", "doc_id": "c"},
        ),
    ]
    retriever = _build_retriever(points, max_context_tokens=100)

    chunks = retriever.retrieve("nhoi mau nao")

    assert len(chunks) == 3
