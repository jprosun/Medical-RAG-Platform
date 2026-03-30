import numpy as np
from types import SimpleNamespace
from app.retriever import QdrantRetriever, RetrievedChunk


class FakeEmbedder:
    def embed(self, texts):
        yield np.array([0.1, 0.2, 0.3])


class FakeClient:
    def search(self, **kwargs):
        return [
            SimpleNamespace(
                id="1",
                score=0.9,
                payload={"text": "Medical text", "metadata": {}},
            ),
            SimpleNamespace(
                id="2",
                score=0.1,  # below threshold
                payload={"text": "Ignore me", "metadata": {}},
            ),
        ]

class EmptyClient:
    def search(self, **kwargs):
        return []

class EdgeClient:
    def search(self, **kwargs):
        return [
            SimpleNamespace(
                id="1",
                score=0.5,
                payload={"text": "Edge case text", "metadata": {}},
            ),
        ]

def test_retriever_filters_by_score(monkeypatch):
    r = QdrantRetriever(
        qdrant_url="http://fake",
        collection="test",
        score_threshold=0.5,
    )

    monkeypatch.setattr(r, "client", FakeClient())
    monkeypatch.setattr(r, "embedder", FakeEmbedder())

    chunks = r.retrieve("query")

    assert len(chunks) == 1
    assert isinstance(chunks[0], RetrievedChunk)
    assert chunks[0].text == "Medical text"

def test_retriever_empty_result(monkeypatch):
    r = QdrantRetriever(
        qdrant_url="http://fake",
        collection="test",
    )

    monkeypatch.setattr(r, "client", EmptyClient())
    monkeypatch.setattr(r, "embedder", FakeEmbedder())

    chunks = r.retrieve("query")
    assert chunks == []

def test_retriever_score_equals_threshold(monkeypatch):
    r = QdrantRetriever(
        qdrant_url="http://fake",
        collection="test",
        score_threshold=0.5,
    )

    monkeypatch.setattr(r, "client", EdgeClient())
    monkeypatch.setattr(r, "embedder", FakeEmbedder())

    chunks = r.retrieve("query")

    assert len(chunks) == 1
    assert chunks[0].text == "Edge case text"