from __future__ import annotations

import os


class ConfigError(Exception):
    pass


class MockLLMClient:
    def chat(
        self,
        system_prompt: str,
        messages: list[dict],
        max_tokens: int = 2000,
    ) -> str:
        return (
            "⚠️ Demo-läge: LLM ej konfigurerad.\n\n"
            f"Antal RAG-källor: {system_prompt.count('[Källa')}\n"
            f"Fråga: {messages[-1]['content'][:120]}..."
        )


class LLMClient:
    def __init__(self):
        self.api_key = os.environ.get("ANTHROPIC_API_KEY")
        self.model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
        if not self.api_key:
            raise ConfigError("ANTHROPIC_API_KEY saknas.")
        from anthropic import Anthropic

        self.client = Anthropic(api_key=self.api_key)

    def chat(
        self,
        system_prompt: str,
        messages: list[dict],
        max_tokens: int = 2000,
    ) -> str:
        response = self.client.messages.create(
            model=self.model,
            system=system_prompt,
            messages=messages,
            max_tokens=max_tokens,
        )
        return response.content[0].text


def get_llm_client() -> MockLLMClient | LLMClient:
    """Returnera LLMClient om API-nyckel finns, annars MockLLMClient."""
    if os.environ.get("ANTHROPIC_API_KEY"):
        return LLMClient()
    return MockLLMClient()

