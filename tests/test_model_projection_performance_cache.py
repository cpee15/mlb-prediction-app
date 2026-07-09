from __future__ import annotations

from typing import Any, Dict

from mlb_app import model_projection_performance_cache as cache
from mlb_app import model_projections as raw_model_projections
from mlb_app.shared_payload_cache import clear_shared_payload_cache


class FakeResult:
    def __init__(self, row: Dict[str, Any] | None):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class FakeInspector:
    table_name_calls = 0
    column_calls = 0

    def __init__(self, _bind):
        pass

    def get_table_names(self):
        FakeInspector.table_name_calls += 1
        return ["bullpen_stats"]

    def get_columns(self, table):
        FakeInspector.column_calls += 1
        assert table == "bullpen_stats"
        return [
            {"name": "team_id"},
            {"name": "era"},
            {"name": "bb_per_9"},
            {"name": "whip"},
        ]


class FakeBind:
    url = "sqlite://unit-test"


class FakeSession:
    bind = FakeBind()

    def __init__(self):
        self.queries = []

    def execute(self, query, params):
        self.queries.append((str(query), params))
        return FakeResult({"era": 3.55, "bb_per_9": 3.1, "whip": 1.21})


def test_bullpen_schema_discovery_is_cached(monkeypatch) -> None:
    cache.clear_projection_performance_caches()
    FakeInspector.table_name_calls = 0
    FakeInspector.column_calls = 0
    monkeypatch.setattr(cache, "inspect", FakeInspector)
    session = FakeSession()

    first = cache.cached_bullpen_inputs(session, team_id=1, team_name="Home Club")
    second = cache.cached_bullpen_inputs(session, team_id=2, team_name="Away Club")

    assert first["source_table"] == "bullpen_stats"
    assert second["source_table"] == "bullpen_stats"
    assert first["era"] == 3.55
    assert FakeInspector.table_name_calls == 1
    assert FakeInspector.column_calls == 1
    assert len(session.queries) == 2


def test_install_model_projection_performance_cache_patches_raw_builder_functions() -> None:
    assert cache.install_model_projection_performance_cache() is True
    assert raw_model_projections._bullpen_inputs is cache.cached_bullpen_inputs
    assert raw_model_projections._build_projection_simulation_cards is cache.cached_projection_simulation_cards


def test_cached_projection_simulation_cards_reuses_cached_result(monkeypatch) -> None:
    clear_shared_payload_cache("artifact")
    calls = []

    def fake_raw(matchup, away, home):
        calls.append(matchup["game_pk"])
        return {
            "away": [{"model_name": "away"}],
            "home": [{"model_name": "home"}],
            "workspace": {"built": True},
        }

    monkeypatch.setattr(cache, "_RAW_BUILD_PROJECTION_SIMULATION_CARDS", fake_raw)
    matchup = {"game_pk": 123, "game_date": "2026-07-09", "venue": "Test Park"}
    away = {"team_id": 1, "team_name": "Away", "offense_inputs": {"pa": 500}, "bullpen_inputs": {"era": 3.5}}
    home = {"team_id": 2, "team_name": "Home", "offense_inputs": {"pa": 505}, "bullpen_inputs": {"era": 3.9}}

    first = cache.cached_projection_simulation_cards(matchup, away, home)
    second = cache.cached_projection_simulation_cards(matchup, away, home)

    assert calls == [123]
    assert first["workspace"]["built"] is True
    assert second["workspace"]["built"] is True
    assert second["cache_hit"] is True
    assert first["cache_key"] == second["cache_key"]
    assert first["cache_key"].startswith("artifact:shared_artifact_v1:simulation")


def test_build_model_projection_payload_installs_cache_before_delegating(monkeypatch) -> None:
    called = {}

    def fake_build(session, target_date):
        called["session"] = session
        called["date"] = target_date
        return {"date": target_date, "games": []}

    monkeypatch.setattr(cache.raw_model_projections, "build_model_projection_payload", fake_build)

    result = cache.build_model_projection_payload(object(), "2026-07-09")

    assert result == {"date": "2026-07-09", "games": []}
    assert called["date"] == "2026-07-09"
    assert raw_model_projections._bullpen_inputs is cache.cached_bullpen_inputs
