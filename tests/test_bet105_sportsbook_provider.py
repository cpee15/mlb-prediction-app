from mlb_app import kibl_bet105_provider as base
from mlb_app import kibl_bet105_sportsbook_provider as sportsbook


def _fixture_row():
    return {
        "fixture_id": "fx-1",
        "away_team": {"name": "New York Yankees"},
        "home_team": {"name": "Boston Red Sox"},
        "start_time": "2026-06-12 19:10:00",
    }


def _market_row(selection="New York Yankees", side="away", price=-118):
    return {
        "fixture_id": "fx-1",
        "away_team": {"name": "New York Yankees"},
        "home_team": {"name": "Boston Red Sox"},
        "start_time": "2026-06-12 19:10:00",
        "market": "moneyline",
        "selection": selection,
        "side": side,
        "price": price,
    }


def _normalize(rows):
    return base._normalize_payload_items(rows, is_live=False)


def _common_monkeypatch(monkeypatch):
    monkeypatch.setattr(base, "_configured", lambda: True)
    monkeypatch.setattr(base, "_cache_get", lambda key: None)
    monkeypatch.setattr(base, "_cache_set", lambda key, value: None)
    monkeypatch.setattr(
        base,
        "build_kibl_bet105_request_params",
        lambda *args, **kwargs: {
            **{
                "feed_source_id": 171,
                "betting_type_id": 1,
                "league_id": "20,643",
                "from_cache": False,
                "start_date": "2026-06-12 00:00:00",
                "end_date": "2026-06-13 00:00:00",
                "from": "2026-06-12 00:00:00",
                "to": "2026-06-13 00:00:00",
            },
            **({"markets": "h2h,spreads,totals"} if kwargs.get("include_markets", True) else {}),
        },
    )


def test_retries_without_market_filter_when_filtered_market_response_contains_fixture_only_rows(monkeypatch):
    _common_monkeypatch(monkeypatch)

    def fake_fetch_items(scope, params, game_pk, is_live, kind):
        if kind == "fixtures":
            rows = [_fixture_row()]
            return {}, "info/fixtures", rows, _normalize(rows)

        if params.get("markets"):
            rows = [_fixture_row()]  # nonempty events, but zero flattenable markets
            return {}, "info/markets", rows, _normalize(rows)

        if params.get("fixture_id") == "fx-1":
            rows = [_market_row()]
            return {}, "info/markets", rows, _normalize(rows)

        rows = [_fixture_row()]  # generic unfiltered body must not stop the loop
        return {}, "info/markets", rows, _normalize(rows)

    monkeypatch.setattr(base, "_fetch_items", fake_fetch_items)

    payload = sportsbook.fetch_kibl_bet105_events(date="2026-06-12", raw=True, live_only=False)

    assert payload["status"] == "ok"
    assert payload["event_count"] == 1
    assert payload["market_count"] == 1
    assert any(note.startswith("markets_no_filter_core:") for note in payload["normalization_notes"])


def test_continues_id_scoped_unfiltered_retries_until_markets_are_found(monkeypatch):
    _common_monkeypatch(monkeypatch)

    def fake_fetch_items(scope, params, game_pk, is_live, kind):
        if kind == "fixtures":
            rows = [_fixture_row()]
            return {}, "info/fixtures", rows, _normalize(rows)

        if params.get("markets"):
            return {}, "info/markets", [], []

        if "fixture_id" not in params:
            rows = [_fixture_row()]  # generic unfiltered response still has zero markets
            return {}, "info/markets", rows, _normalize(rows)

        rows = [_market_row(selection="Boston Red Sox", side="home", price=108)]
        return {}, "info/markets", rows, _normalize(rows)

    monkeypatch.setattr(base, "_fetch_items", fake_fetch_items)

    payload = sportsbook.fetch_kibl_bet105_events(date="2026-06-12", raw=True, live_only=False)

    assert payload["status"] == "ok"
    assert payload["market_count"] == 1
    assert payload["events"][0]["home_team"]["name"] == "Boston Red Sox"
    assert payload["events"][0]["away_team"]["name"] == "New York Yankees"


def test_returns_fixtures_only_when_no_market_request_shape_returns_odds(monkeypatch):
    _common_monkeypatch(monkeypatch)

    def fake_fetch_items(scope, params, game_pk, is_live, kind):
        rows = [_fixture_row()]
        return {}, "info/fixtures" if kind == "fixtures" else "info/markets", rows, _normalize(rows)

    monkeypatch.setattr(base, "_fetch_items", fake_fetch_items)

    payload = sportsbook.fetch_kibl_bet105_events(date="2026-06-12", raw=True, live_only=False)

    assert payload["status"] == "fixtures_only"
    assert payload["event_count"] == 1
    assert payload["market_count"] == 0


def test_recursive_nested_name_extracts_team_objects():
    assert base._nested_name({"team": {"name": "Boston Red Sox"}}) == "Boston Red Sox"
    assert base._nested_name({"participant": {"displayName": "New York Yankees"}}) == "New York Yankees"
