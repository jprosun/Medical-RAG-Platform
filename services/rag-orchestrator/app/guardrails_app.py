import os
from functools import lru_cache
from typing import Any

from nemoguardrails import RailsConfig, LLMRails
from nemoguardrails.llm.providers import register_llm_provider

from typing import Any, List, Optional
from langchain_core.language_models import BaseLanguageModel
from langchain_core.outputs import Generation, LLMResult
from langchain_core.runnables import RunnableConfig


class ExternalInferenceLLM(BaseLanguageModel):
    """
    LangChain 0.2.x compatible LLM for NeMo Guardrails 0.20.0
    backed by external inference (KServe).
    Note: LangChain and NeMo Guardrails do not call inference directly.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()

    @property
    def _llm_type(self) -> str:
        return "external-kserve"

    # ---- Core implementation ----

    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> str:
        """
        Note: LangChain requires `_call` for sync execution paths.
        """
        from .llm_client import build_kserve_client_from_env

        client = build_kserve_client_from_env()
        if not client:
            raise RuntimeError("External inference client not configured")

        return client.generate(
            prompt,
            max_tokens=kwargs.get("max_tokens", 512),
            temperature=kwargs.get("temperature", 0.2),
        )

    async def _acall(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> str:
        """
        LangChain + NeMo Guardrails expect an async version of `_call`. 
        However, our backend is not async, we can only delegate to `_call`
        """
        return self._call(prompt, stop=stop, **kwargs)

    # ---- Required abstract methods (LangChain 0.2.x) ----

    def predict(self, text: str, **kwargs: Any) -> str:
        return self._call(text, **kwargs)

    async def apredict(self, text: str, **kwargs: Any) -> str:
        return await self._acall(text, **kwargs)

    def predict_messages(self, messages: List[Any], **kwargs: Any) -> str:
        prompt = self._messages_to_prompt(messages)
        return self._call(prompt, **kwargs)

    async def apredict_messages(self, messages: List[Any], **kwargs: Any) -> str:
        prompt = self._messages_to_prompt(messages)
        return await self._acall(prompt, **kwargs)

    def generate_prompt(
        self,
        prompts: List[str],
        stop: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> LLMResult:
        generations = [
            [Generation(text=self._call(p, stop=stop, **kwargs))]
            for p in prompts
        ]
        return LLMResult(generations=generations)

    async def agenerate_prompt(
        self,
        prompts: List[str],
        stop: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> LLMResult:
        generations = [
            [Generation(text=await self._acall(p, stop=stop, **kwargs))]
            for p in prompts
        ]
        return LLMResult(generations=generations)

    def invoke(
        self,
        input: str,
        config: Optional[RunnableConfig] = None,
        **kwargs: Any,
    ) -> str:
        """
        LangChain never calls `_call` directly.
        It always calls `invoke()` in synchronous execution paths.
        This approach adapts LangChain's Runnable interface to `_call`.
        """
        return self._call(input, **kwargs)


    async def ainvoke(
        self,
        input: str,
        config: Optional[RunnableConfig] = None,
        **kwargs: Any,
    ) -> str:
        """
        Guardrails and async chains use `ainvoke()`.
        This method bridges async execution to `_acall`.
        Required for compatibility with LangChain's async engine.
        """
        return await self._acall(input, **kwargs)

    @staticmethod
    def _messages_to_prompt(messages: List[Any]) -> str:
        parts = []
        for m in messages:
            role = getattr(m, "role", None) or m.get("role", "user")
            content = getattr(m, "content", None) or m.get("content", "")
            parts.append(f"[{role.upper()}]\n{content}")
        return "\n\n".join(parts)


register_llm_provider("external", ExternalInferenceLLM)


GUARDRAILS_ENABLED = os.getenv("GUARDRAILS_ENABLED", "false").lower() == "true"


@lru_cache()
def get_rails_app() -> LLMRails:
    config = RailsConfig.from_path("guardrails")
    return LLMRails(config)


def generate_with_guardrails(user_message: str, messages_payload: list) -> str:
    rails = get_rails_app()

    response: Any = rails.generate(messages=messages_payload)

    # Normalize output safely
    if isinstance(response, str):
        return response
    if isinstance(response, dict):
        return response.get("content") or response.get("output") or str(response)
    return str(response)
