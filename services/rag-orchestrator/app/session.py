import json
import os
import time
import redis
from typing import List, Dict, Any

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
SESSION_TTL_S = int(os.getenv("SESSION_TTL_S", "0"))

r = redis.from_url(REDIS_URL, decode_responses=True)

def _key(session_id: str) -> str:
    return f"session:{session_id}"

def get_session(session_id: str) -> Dict:
    raw = r.get(_key(session_id))
    return json.loads(raw) if raw else {"messages": []}

def save_session(session_id: str, data: Dict):
    payload = json.dumps(data)
    if SESSION_TTL_S > 0:
        r.setex(_key(session_id), SESSION_TTL_S, payload)
    else:
        r.set(_key(session_id), payload)

def append_message(session_id: str, role: str, content: str):
    s = get_session(session_id)
    s["messages"].append({"role": role, "content": content})
    save_session(session_id, s)

def get_messages(session_id: str) -> List[Dict]:
    return get_session(session_id)["messages"]

class SessionStore:
    def __init__(self):
        self.redis_enabled = False
        self._client = None
        self._memory_store: Dict[str, List[Dict]] = {}
        self._memory_titles: Dict[str, str] = {}

        redis_url = os.getenv("REDIS_URL")
        if redis_url and redis:
            self._client = redis.from_url(redis_url, decode_responses=True)
            self.redis_enabled = True

        self.ttl = int(os.getenv("REDIS_TTL_SECONDS", "86400"))

    def get_history(self, session_id: str) -> List[Dict]:
        if self.redis_enabled:
            try:
                data = self._client.get(session_id)
            except Exception:
                # Key holds a non-string Redis type, or transient error: treat as empty.
                return []
            if not data:
                return []
            try:
                parsed = json.loads(data)
            except (ValueError, TypeError):
                # Corrupt / non-session payload under this key: never crash the caller.
                return []
            return parsed if isinstance(parsed, list) else []
        return self._memory_store.get(session_id, [])

    def _is_session_history_key(self, key: str) -> bool:
        """True only if ``key`` holds a chat history (a JSON list).

        Redis is shared between session histories and helper keys (titles,
        topics, updated_at markers, pipeline cache). Listing sessions must
        ignore everything that is not an actual history, and must never raise
        if a key holds an unexpected value/type.
        """
        try:
            raw = self._client.get(key)
        except Exception:
            return False
        if not raw:
            return False
        try:
            return isinstance(json.loads(raw), list)
        except (ValueError, TypeError):
            return False

    def _session_updated_at(self, session_id: str, history: List[Dict] | None = None) -> float:
        if self.redis_enabled:
            raw = self._client.get(f"updated_at:{session_id}")
            if raw:
                try:
                    return float(raw)
                except (TypeError, ValueError):
                    pass
        history = history if history is not None else self.get_history(session_id)
        for message in reversed(history or []):
            raw = message.get("created_at")
            if raw:
                try:
                    return float(raw)
                except (TypeError, ValueError):
                    continue
        return 0.0

    def get_all_sessions(self) -> List[Dict[str, Any]]:
        if self.redis_enabled:
            all_keys = self._client.keys("*")
            cache_prefix = os.getenv("PIPELINE_CACHE_PREFIX", "rag:pipeline")
            ignored_prefixes = (
                "title:",
                "updated_at:",
                "topic:",
                f"{cache_prefix}:",
                "session:",
            )

            result = []
            for sid in all_keys:
                if any(sid.startswith(prefix) for prefix in ignored_prefixes):
                    continue
                # Only treat keys that actually hold a chat history as sessions.
                if not self._is_session_history_key(sid):
                    continue
                try:
                    title = self._client.get(f"title:{sid}") or "Cuộc trò chuyện"
                    updated_at = self._session_updated_at(sid)
                except Exception:
                    # Defensive: skip any key that misbehaves rather than 500 the sidebar.
                    continue
                result.append({"id": sid, "title": title, "updated_at": updated_at})
            result.sort(key=lambda item: item.get("updated_at", 0), reverse=True)
            return result
            
        result = []
        for sid, history in self._memory_store.items():
            title = self._memory_titles.get(sid, "Cuộc trò chuyện")
            result.append({"id": sid, "title": title, "updated_at": self._session_updated_at(sid, history)})
        result.sort(key=lambda item: item.get("updated_at", 0), reverse=True)
        return result

    def get_title(self, session_id: str) -> str:
        if self.redis_enabled:
            t = self._client.get(f"title:{session_id}")
            return t if t else "Cuộc trò chuyện"
        return self._memory_titles.get(session_id, "Cuộc trò chuyện")

    def set_title(self, session_id: str, title: str):
        if self.redis_enabled:
            self._client.setex(f"title:{session_id}", self.ttl, title)
        else:
            self._memory_titles[session_id] = title

    def append(self, session_id: str, role: str, content: str, **extra: Any):
        history = self.get_history(session_id)
        now = time.time()
        message = {"role": role, "content": content, "created_at": now}
        message.update({key: value for key, value in extra.items() if value is not None})
        history.append(message)

        if self.redis_enabled:
            self._client.setex(
                session_id,
                self.ttl,
                json.dumps(history),
            )
            self._client.setex(f"updated_at:{session_id}", self.ttl, str(now))
        else:
            self._memory_store[session_id] = history

    def get_topic(self, session_id: str) -> str:
        """Return the stored topic entity phrase for this session, or empty string."""
        if self.redis_enabled:
            return self._client.get(f"topic:{session_id}") or ""
        return self._memory_titles.get(f"topic:{session_id}", "")

    def set_topic(self, session_id: str, topic: str):
        """Persist the primary topic entity phrase for this session."""
        if not topic:
            return
        if self.redis_enabled:
            self._client.setex(f"topic:{session_id}", self.ttl, topic)
        else:
            self._memory_titles[f"topic:{session_id}"] = topic

    def delete_session(self, session_id: str):
        """Delete a session's history and title."""
        if self.redis_enabled:
            self._client.delete(session_id)
            self._client.delete(f"title:{session_id}")
            self._client.delete(f"updated_at:{session_id}")
            self._client.delete(f"topic:{session_id}")
        else:
            self._memory_store.pop(session_id, None)
            self._memory_titles.pop(session_id, None)
            self._memory_titles.pop(f"topic:{session_id}", None)

