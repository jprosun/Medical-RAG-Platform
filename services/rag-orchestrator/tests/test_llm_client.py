import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.llm_client import KServeClient, UpstreamRateLimitError


class MockResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400 and self.status_code != 429:
            raise RuntimeError(f"http {self.status_code}")

    def json(self):
        return self._payload


def _client():
    return KServeClient(
        base_url="http://example.test",
        completions_path="/v1/chat/completions",
        model_id="demo",
        api_key=None,
        timeout_s=5,
        retries=1,
        retry_backoff_s=0,
    )


def test_rate_limit_retry_budget_exceeds_env_retry_setting(monkeypatch):
    client = _client()
    calls = {"count": 0}

    def fake_post(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] < 4:
            return MockResponse(429)
        return MockResponse(
            200,
            payload={"choices": [{"message": {"content": "ok"}}], "usage": {}},
        )

    monkeypatch.setattr("app.llm_client.requests.post", fake_post)
    monkeypatch.setattr("app.llm_client.time.sleep", lambda *_args, **_kwargs: None)

    result = client.generate("hello")
    assert result == "ok"
    assert calls["count"] == 4


def test_rate_limit_raises_custom_error_after_budget_exhausted(monkeypatch):
    client = _client()

    monkeypatch.setattr("app.llm_client.requests.post", lambda *args, **kwargs: MockResponse(429))
    monkeypatch.setattr("app.llm_client.time.sleep", lambda *_args, **_kwargs: None)

    try:
        client.generate("hello")
    except UpstreamRateLimitError as exc:
        assert "Rate limit exceeded" in str(exc)
    else:
        raise AssertionError("Expected UpstreamRateLimitError")


def test_attempt_budget_can_shorten_noncritical_calls(monkeypatch):
    client = _client()
    calls = {"count": 0}

    def fake_post(*args, **kwargs):
        calls["count"] += 1
        return MockResponse(429)

    monkeypatch.setattr("app.llm_client.requests.post", fake_post)
    monkeypatch.setattr("app.llm_client.time.sleep", lambda *_args, **_kwargs: None)

    try:
        client.generate("hello", attempt_budget=1)
    except UpstreamRateLimitError:
        pass
    else:
        raise AssertionError("Expected UpstreamRateLimitError")

    assert calls["count"] == 1


def test_deepseek_payload_disables_thinking_mode(monkeypatch):
    client = KServeClient(
        base_url="https://api.deepseek.com",
        completions_path="/chat/completions",
        model_id="deepseek-v4-flash",
        api_key="demo",
        timeout_s=5,
        retries=0,
        retry_backoff_s=0,
    )
    captured = {}

    def fake_post(*args, **kwargs):
        captured["json"] = kwargs.get("json")
        return MockResponse(
            200,
            payload={"choices": [{"message": {"content": "ok"}}], "usage": {}},
        )

    monkeypatch.setenv("LLM_PROVIDER_HINT", "deepseek")
    monkeypatch.setenv("LLM_THINKING_MODE", "disabled")
    monkeypatch.setattr("app.llm_client.requests.post", fake_post)

    result = client.generate("hello")
    assert result == "ok"
    assert captured["json"]["thinking"] == {"type": "disabled"}
    assert "reasoning_effort" not in captured["json"]


def test_generate_raises_when_response_has_reasoning_only_without_content(monkeypatch):
    client = _client()

    def fake_post(*args, **kwargs):
        return MockResponse(
            200,
            payload={
                "choices": [
                    {
                        "message": {
                            "content": "",
                            "reasoning_content": "internal only",
                        }
                    }
                ],
                "usage": {},
            },
        )

    monkeypatch.setattr("app.llm_client.requests.post", fake_post)
    monkeypatch.setattr("app.llm_client.time.sleep", lambda *_args, **_kwargs: None)

    try:
        client.generate("hello", attempt_budget=1)
    except RuntimeError as exc:
        assert "no assistant content" in str(exc).lower()
    else:
        raise AssertionError("Expected RuntimeError for reasoning-only response")
