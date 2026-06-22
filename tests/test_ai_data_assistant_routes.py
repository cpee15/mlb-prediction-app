from mlb_app import ai_data_assistant_routes as routes


class DummySession:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyFactory:
    def __call__(self):
        return DummySession()


def test_resolve_use_llm_defaults_on_when_configured(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("AI_DATA_ASSISTANT_DEFAULT_USE_LLM", "true")
    assert routes._resolve_use_llm(None) is True


def test_resolve_use_llm_forces_off_without_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert routes._resolve_use_llm(True) is False
    assert routes._resolve_use_llm(None) is False


def test_query_uses_route_level_llm_polish_and_preserves_backend_contract(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(routes, "session_factory", lambda: DummyFactory())

    captured = {}

    def fake_build(**kwargs):
        captured.update(kwargs)
        return {
            "answer": "Deterministic answer",
            "intent": "best_model_edges",
            "primary_recommendations": [{"label": "Cubs ML"}],
            "watchlist": [],
            "warnings": [],
            "missing_data": [],
            "confidence_note": "Deterministic base.",
            "data_used": ["model_projections"],
            "sources_used": ["model_projections"],
            "context_preview": {"intent": "best_model_edges"},
            "data_quality": {},
        }

    def fake_polish(message, result, conversation):
        assert message == "What stands out today?"
        assert conversation == [{"role": "user", "content": "Earlier question"}]
        result = dict(result)
        result["answer"] = "Here’s what stands out today."
        result["llm_answer"] = result["answer"]
        return result

    monkeypatch.setattr(routes, "build_ai_data_assistant_response", fake_build)
    monkeypatch.setattr(routes, "_apply_conversational_llm_polish", fake_polish)

    payload = routes.AIDataAssistantRequest(
        message="What stands out today?",
        use_llm=True,
        conversation=[routes.ConversationTurn(role="user", content="Earlier question")],
    )
    result = routes.ai_data_assistant_query(payload)

    assert captured["use_llm"] is False
    assert result["answer"] == "Here’s what stands out today."
    assert result["primary_recommendations"] == [{"label": "Cubs ML"}]
    assert result["llm_mode"]["active"] is True
    assert result["llm_mode"]["conversation_turns"] == 1


def test_query_warns_when_llm_requested_but_not_configured(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(routes, "session_factory", lambda: DummyFactory())
    monkeypatch.setattr(
        routes,
        "build_ai_data_assistant_response",
        lambda **kwargs: {
            "answer": "Deterministic answer",
            "intent": "daily_slate_summary",
            "primary_recommendations": [],
            "watchlist": [],
            "warnings": [],
            "missing_data": [],
            "confidence_note": "Base note.",
            "data_used": [],
            "sources_used": [],
            "context_preview": {},
            "data_quality": {},
        },
    )

    payload = routes.AIDataAssistantRequest(message="Summarize today", use_llm=True)
    result = routes.ai_data_assistant_query(payload)

    assert result["llm_mode"]["active"] is False
    assert result["llm_mode"]["configured"] is False
    assert any("OPENAI_API_KEY" in warning for warning in result["warnings"])
    assert "Deterministic fallback" in result["confidence_note"]
