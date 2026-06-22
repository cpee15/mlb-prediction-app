from mlb_app import ai_data_assistant_pipeline as pipeline
from mlb_app import ai_data_assistant as core


class DummySession:
    pass


def test_build_best_model_edges_packet_normalizes_fields(monkeypatch):
    monkeypatch.setattr(
        core,
        "build_model_projection_intelligence_context",
        lambda session, date: {
            "intent": "best_model_edges",
            "date": date,
            "sources_used": ["model_projections", "daily_odds_models"],
            "projection_edges": [
                {
                    "game_pk": 1,
                    "label": "Cubs @ Brewers",
                    "model_favorite": "Brewers",
                    "confidence_tier": "high",
                    "confidence": 0.76,
                    "score": 1.24,
                    "win_probability_edge": 0.11,
                    "expected_run_differential": 1.2,
                    "total_projection": {"total_expected_runs": 8.7},
                    "why": ["edge exists"],
                    "missing_inputs": [],
                }
            ],
            "prop_watchlist": [
                {
                    "type": "pitcher_prop_watchlist",
                    "player_name": "Pitcher A",
                    "team": "Brewers",
                    "score": 0.52,
                    "angle": "strikeout lean",
                    "reasons": ["K% edge"],
                    "market_price_available": False,
                }
            ],
            "data_quality": {"projection_games": 1},
            "missing_data": [],
            "projection_errors": [],
        },
    )

    packet = pipeline.build_best_model_edges_packet(DummySession(), message="best edges", date="2026-06-22")
    assert packet["intent"] == "best_model_edges"
    assert packet["date"] == "2026-06-22"
    assert packet["primary_recommendations"]
    assert packet["watchlist"]
    assert packet["sources_used"] == ["model_projections", "daily_odds_models"]
    assert "context_preview" in packet
    assert isinstance(packet["warnings"], list)


def test_build_assistant_packet_routes_to_game_explanation(monkeypatch):
    calls = {"builder": None}

    monkeypatch.setattr(core, "classify_assistant_intent", lambda message: "best_model_edges")
    monkeypatch.setattr(core, "date_from_message", lambda message, date: "2026-06-22")

    def fake_builder(session, **kwargs):
        calls["builder"] = "game"
        return {
            "intent": "game_explanation",
            "date": kwargs["date"],
            "game_pk": kwargs["game_pk"],
            "sources_used": ["model_projections"],
            "primary_recommendations": [],
            "watchlist": [],
            "missing_data": [],
            "warnings": [],
        }

    monkeypatch.setattr(pipeline, "build_game_explanation_packet", fake_builder)

    packet = pipeline.build_assistant_packet(DummySession(), message="explain", date="2026-06-22", game_pk=123)
    assert calls["builder"] == "game"
    assert packet["intent"] == "game_explanation"
    assert packet["game_pk"] == 123


def test_render_structured_response_shape():
    packet = {
        "intent": "odds_and_props",
        "date": "2026-06-22",
        "game_pk": None,
        "player_id": None,
        "team_id": None,
        "sources_used": ["daily_odds_models", "model_projections"],
        "primary_recommendations": [{"label": "Player A", "market": "hits", "score": 1.2}],
        "watchlist": [{"label": "Player B", "score": 0.7}],
        "data_quality": {"projection_games": 3},
        "missing_data": ["missing_weather"],
        "warnings": ["thin market"],
    }

    response = pipeline.render_structured_response("What does Daily Odds tell us?", packet)
    for key in [
        "answer",
        "intent",
        "primary_recommendations",
        "watchlist",
        "data_used",
        "missing_data",
        "warnings",
        "confidence_note",
        "date",
        "context_preview",
    ]:
        assert key in response
    assert response["intent"] == "odds_and_props"
    assert response["data_used"] == ["daily_odds_models", "model_projections"]


def test_answer_with_optional_llm_structured_without_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    packet = {
        "intent": "pitcher_analysis",
        "date": "2026-06-22",
        "sources_used": ["model_projections"],
        "primary_recommendations": [],
        "watchlist": [],
        "data_quality": {},
        "missing_data": [],
        "warnings": [],
    }
    response = pipeline.answer_with_optional_llm_structured("Top pitcher leans", packet, use_llm=True)
    assert response["answer"]
    assert "confidence_note" in response
    assert "LLM summarized" not in response["confidence_note"]


def test_game_explanation_without_game_pk_falls_back_to_daily_slate(monkeypatch):
    monkeypatch.setattr(core, "classify_assistant_intent", lambda message: "game_explanation")
    monkeypatch.setattr(core, "date_from_message", lambda message, date: "2026-06-22")

    def fake_daily(session, **kwargs):
        return {
            "intent": "daily_slate_summary",
            "date": kwargs["date"],
            "sources_used": ["matchups", "model_projections"],
            "games": [],
            "primary_recommendations": [],
            "watchlist": [],
            "missing_data": [],
            "warnings": [],
        }

    monkeypatch.setattr(pipeline, "build_daily_slate_packet", fake_daily)

    packet = pipeline.build_assistant_packet(DummySession(), message="Explain this matchup", date="2026-06-22")
    assert packet["intent"] == "daily_slate_summary"
