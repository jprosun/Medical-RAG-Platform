import json
import os
import time
from typing import Optional

import requests

from .metrics_llm import (
    LLM_REQUESTS_TOTAL,
    LLM_INFERENCE_LATENCY_SECONDS,
    LLM_PROMPT_TOKENS_TOTAL,
    LLM_COMPLETION_TOKENS_TOTAL,
)


class UpstreamRateLimitError(RuntimeError):
    """Raised when the upstream LLM keeps throttling after bounded retries."""

    def __init__(self, message: str, wait_s: float = 0.0):
        super().__init__(message)
        self.wait_s = wait_s


class KServeClient:
    """
    OpenAI-compatible *COMPLETIONS* client.

    Designed for:
    - External vLLM servers (e.g. Vast.ai)
    - KServe-hosted vLLM exposing /v1/completions

    This client intentionally uses:
    - prompt (string), NOT chat messages
    - choices[0].text for output
    """

    def __init__(
        self,
        base_url: str,
        completions_path: str,
        model_id: str,
        api_key: Optional[str],
        timeout_s: int,
        retries: int,
        retry_backoff_s: int,
    ):
        self.base_url = base_url.rstrip("/")
        self.completions_path = completions_path
        self.model_id = model_id
        self.api_key = api_key
        self.timeout_s = timeout_s
        self.retries = retries
        self.retry_backoff_s = retry_backoff_s

    def generate(
        self,
        prompt_or_messages: str | list,
        max_tokens: int = 512,
        temperature: float = 0.2,
        attempt_budget: Optional[int] = None,
    ) -> str:
        """
        Generate text using OpenAI *Chat Completions* contract.
        """

        url = f"{self.base_url}{self.completions_path}"

        messages = prompt_or_messages if isinstance(prompt_or_messages, list) else [{"role": "user", "content": prompt_or_messages}]

        payload = {
            "model": self.model_id,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "top_p": float(os.getenv("LLM_TOP_P", "0.9")),
            "frequency_penalty": float(os.getenv("LLM_FREQUENCY_PENALTY", "0.2")),
            "presence_penalty": float(os.getenv("LLM_PRESENCE_PENALTY", "0.0")),
        }

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        referer = (os.getenv("LLM_HTTP_REFERER") or "").strip()
        if referer:
            headers["HTTP-Referer"] = referer
        app_name = (os.getenv("LLM_APP_NAME") or "").strip()
        if app_name:
            headers["X-Title"] = app_name

        last_err = None
        # Provider throttling is common during benchmark batches.
        # Keep a small minimum retry budget even if env sets retries too low.
        min_attempts = max(1, int(os.getenv("LLM_MIN_ATTEMPTS", "4")))
        max_attempts = attempt_budget or max(self.retries + 1, min_attempts)

        for attempt in range(max_attempts):
            try:
                start = time.time()
                r = requests.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout_s,
                )
                latency = time.time() - start
                LLM_INFERENCE_LATENCY_SECONDS.labels(model=self.model_id).observe(latency)

                if r.status_code in (503, 504):
                    raise RuntimeError(f"Upstream transient error {r.status_code}")

                # Phase 4: Handle 429 rate limit with longer backoff
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After", "")
                    wait_s = float(retry_after) if retry_after.replace(".", "").isdigit() else 0.0
                    backoff_s = self.retry_backoff_s * (2 ** attempt)
                    max_wait_s = float(os.getenv("LLM_MAX_RATE_LIMIT_WAIT_S", "30"))
                    wait_s = min(max(wait_s, backoff_s, 10.0), max_wait_s)
                    print(f"[LLM] 429 rate limit hit, waiting {wait_s}s (attempt {attempt + 1}/{max_attempts})")
                    if attempt < max_attempts - 1:
                        time.sleep(wait_s)
                        continue
                    raise UpstreamRateLimitError(
                        f"Rate limit exceeded after {max_attempts} attempts",
                        wait_s=wait_s,
                    )

                r.raise_for_status()
                data = r.json()

                choices = data.get("choices")
                if isinstance(choices, list) and choices:
                    msg = choices[0].get("message")
                    if msg and msg.get("content"):

                        # Token usage
                        usage = data.get("usage") or {}
                        prompt_tokens = int(usage.get("prompt_tokens", 0))
                        completion_tokens = int(usage.get("completion_tokens", 0))

                        LLM_REQUESTS_TOTAL.labels(
                            model=self.model_id,
                            status="success",
                        ).inc()

                        if prompt_tokens:
                            LLM_PROMPT_TOKENS_TOTAL.labels(model=self.model_id).inc(prompt_tokens)
                        if completion_tokens:
                            LLM_COMPLETION_TOKENS_TOTAL.labels(model=self.model_id).inc(completion_tokens)

                        return msg["content"].strip()

                # Defensive fallback
                return json.dumps(data)

            except Exception as e:
                LLM_REQUESTS_TOTAL.labels(
                    model=self.model_id,
                    status="error",
                ).inc()
                last_err = e
                if attempt < max_attempts - 1:
                    # Use longer backoff for subsequent retries
                    backoff = self.retry_backoff_s * (attempt + 1)
                    time.sleep(backoff)
                    continue
                raise last_err


def build_kserve_client_from_env() -> Optional[KServeClient]:
    """
    Factory for inference client.

    Switching between:
    - in-cluster KServe
    - external vLLM

    is done purely via environment variables.
    """

    enabled = os.getenv("KSERVE_ENABLED", "false").lower() == "true"
    if not enabled:
        return None

    base_url = (os.getenv("KSERVE_BASE_URL") or "").strip()
    if not base_url:
        return None

    completions_path = (
        os.getenv("KSERVE_COMPLETIONS_PATH") or "/v1/completions"
    ).strip()

    model_id = (os.getenv("LLM_MODEL_ID") or "").strip()
    if not model_id:
        raise RuntimeError("LLM_MODEL_ID is required when KSERVE_ENABLED=true")

    api_key = (os.getenv("LLM_API_KEY") or "").strip() or None

    return KServeClient(
        base_url=base_url,
        completions_path=completions_path,
        model_id=model_id,
        api_key=api_key,
        timeout_s=int(os.getenv("LLM_TIMEOUT_S", "300")),
        retries=int(os.getenv("LLM_RETRIES", "3")),
        retry_backoff_s=int(os.getenv("LLM_RETRY_BACKOFF_S", "3")),
    )
