from rag.llm_client import ConfigError, LLMClient, MockLLMClient, get_llm_client


def test_get_llm_client_returns_mock_without_key(monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    client = get_llm_client()

    assert isinstance(client, MockLLMClient)


def test_mock_client_returns_string() -> None:
    client = MockLLMClient()

    response = client.chat(
        system_prompt="[Källa 1]\n[Källa 2]",
        messages=[{"role": "user", "content": "Vad säger avtalslagen?"}],
    )

    assert isinstance(response, str)
    assert "Antal RAG-källor: 2" in response


def test_llm_client_raises_without_key(monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    try:
        LLMClient()
    except ConfigError:
        return

    raise AssertionError("LLMClient() skulle ha kastat ConfigError utan API-nyckel")
