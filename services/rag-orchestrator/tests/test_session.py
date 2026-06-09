import json
from app.session import SessionStore
from app import session


class FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value

    def setex(self, key, ttl, value):
        # ignore TTL for unit test; just store the value
        self.store[key] = value

    def keys(self, pattern):
        return list(self.store.keys())

    def delete(self, key):
        self.store.pop(key, None)

def test_session_store_memory_fallback(monkeypatch):
    monkeypatch.delenv("REDIS_HOST", raising=False)

    store = SessionStore()
    assert store.redis_enabled is False

    store.append("s1", "user", "hello")
    store.append("s1", "assistant", "hi")

    history = store.get_history("s1")
    assert len(history) == 2
    assert history[0]["role"] == "user"

def test_session_store_separate_sessions(monkeypatch):
    monkeypatch.delenv("REDIS_HOST", raising=False)

    store = SessionStore()

    store.append("s1", "user", "hello")
    store.append("s2", "user", "hi")

    assert len(store.get_history("s1")) == 1
    assert len(store.get_history("s2")) == 1

def test_session_store_empty_history(monkeypatch):
    monkeypatch.delenv("REDIS_HOST", raising=False)

    store = SessionStore()

    history = store.get_history("nonexistent")
    assert history == []

def test_session_store_preserves_content(monkeypatch):
    monkeypatch.delenv("REDIS_HOST", raising=False)

    store = SessionStore()
    store.append("s1", "user", "hello")
    store.append("s1", "assistant", "hi")

    history = store.get_history("s1")

    assert history[0]["content"] == "hello"
    assert history[1]["content"] == "hi"


def test_module_level_session_empty(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(session, "r", fake)

    s = session.get_session("unknown")
    assert s == {"messages": []}
    assert fake.store == {}  # nothing stored yet


def test_module_level_append_and_get_messages(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(session, "r", fake)

    session.append_message("s1", "user", "hello")
    session.append_message("s1", "assistant", "hi")

    msgs = session.get_messages("s1")
    assert len(msgs) == 2
    assert msgs[0]["role"] == "user"
    assert msgs[0]["content"] == "hello"
    assert msgs[1]["role"] == "assistant"
    assert msgs[1]["content"] == "hi"


def test_get_all_sessions_ignores_pipeline_cache_keys(monkeypatch):
    fake = FakeRedis()
    fake.store = {
        "session-a": json.dumps([{"role": "user", "content": "hello"}]),
        "title:session-a": "Session A",
        "rag:pipeline:v1:evidence_extract:abc": "cached",
        "session:legacy": json.dumps({"messages": []}),
    }
    monkeypatch.setenv("REDIS_URL", "redis://test")
    monkeypatch.setattr(session.redis, "from_url", lambda *_args, **_kwargs: fake)

    store = SessionStore()
    sessions = store.get_all_sessions()
    assert len(sessions) == 1
    assert sessions[0]["id"] == "session-a"
    assert sessions[0]["title"] == "Session A"
    assert "updated_at" in sessions[0]


def test_get_all_sessions_ignores_topic_and_non_json_keys(monkeypatch):
    fake = FakeRedis()
    fake.store = {
        "session-a": json.dumps([{"role": "user", "content": "hello", "created_at": 10.0}]),
        "title:session-a": "Session A",
        # The bug: topic keys and stray non-JSON values used to be treated as
        # sessions and crashed get_all_sessions with a JSONDecodeError (500).
        "topic:session-a": "ung thư",
        "updated_at:session-a": "10.0",
        "rag:pipeline:v1:retrieval:xyz": "cached-blob",
        "session:legacy": json.dumps({"messages": []}),
        "stray-non-json-key": "not json at all",
    }
    monkeypatch.setenv("REDIS_URL", "redis://test")
    monkeypatch.setattr(session.redis, "from_url", lambda *_args, **_kwargs: fake)

    store = SessionStore()
    sessions = store.get_all_sessions()  # must not raise

    assert [s["id"] for s in sessions] == ["session-a"]
    assert sessions[0]["title"] == "Session A"


def test_get_history_survives_corrupt_payload(monkeypatch):
    fake = FakeRedis()
    fake.store = {"s1": "not-json"}
    monkeypatch.setenv("REDIS_URL", "redis://test")
    monkeypatch.setattr(session.redis, "from_url", lambda *_args, **_kwargs: fake)

    store = SessionStore()
    assert store.get_history("s1") == []


def test_session_store_persists_assistant_metadata(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)

    store = SessionStore()
    store.append(
        "s1",
        "assistant",
        "answer",
        metadata={"answer_mode": "thinking"},
        retrieved_chunks=[{"id": "c1", "text": "evidence", "metadata": {}}],
        context_used=1,
    )

    history = store.get_history("s1")
    assert history[0]["metadata"]["answer_mode"] == "thinking"
    assert history[0]["retrieved_chunks"][0]["id"] == "c1"
    assert history[0]["context_used"] == 1
    assert "created_at" in history[0]
