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

        last_err = None

        for attempt in range(self.retries + 1):
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
                if attempt < self.retries:
                    time.sleep(self.retry_backoff_s)
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
