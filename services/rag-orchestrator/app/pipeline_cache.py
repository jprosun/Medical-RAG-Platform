from __future__ import annotations

import base64
import hashlib
import json
import os
import pickle
import time
from typing import Any

try:
    import redis
except Exception:  # pragma: no cover - optional at runtime
    redis = None


def stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class PipelineCache:
    def __init__(self) -> None:
        self.enabled = _env_flag("PIPELINE_CACHE_ENABLED", default=True)
        self.ttl_s = int(os.getenv("PIPELINE_CACHE_TTL_SECONDS", "86400"))
        self.prefix = os.getenv("PIPELINE_CACHE_PREFIX", "rag:pipeline")
        self._memory: dict[str, tuple[float, Any]] = {}
        self._client = None
        redis_url = os.getenv("REDIS_URL")
        if self.enabled and redis_url and redis is not None:
            try:
                self._client = redis.from_url(redis_url, decode_responses=True)
            except Exception:
                self._client = None

    def key(self, namespace: str, payload: Any) -> str:
        version = os.getenv("PIPELINE_CACHE_VERSION", "v1")
        return f"{self.prefix}:{version}:{namespace}:{stable_hash(payload)}"

    def get(self, key: str) -> Any | None:
        if not self.enabled:
            return None
        now = time.time()
        item = self._memory.get(key)
        if item:
            expires_at, value = item
            if expires_at <= 0 or expires_at > now:
                return value
            self._memory.pop(key, None)

        if self._client is None:
            return None
        try:
            raw = self._client.get(key)
            if not raw:
                return None
            return pickle.loads(base64.b64decode(raw.encode("ascii")))
        except Exception:
            return None

    def set(self, key: str, value: Any) -> None:
        if not self.enabled:
            return
        expires_at = time.time() + self.ttl_s if self.ttl_s > 0 else 0
        self._memory[key] = (expires_at, value)
        if self._client is None:
            return
        try:
            raw = base64.b64encode(pickle.dumps(value)).decode("ascii")
            if self.ttl_s > 0:
                self._client.setex(key, self.ttl_s, raw)
            else:
                self._client.set(key, raw)
        except Exception:
            return


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


pipeline_cache = PipelineCache()
