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