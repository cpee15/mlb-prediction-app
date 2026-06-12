from mlb_app import kibl_bet105_provider as base
from mlb_app import kibl_bet105_sportsbook_runtime as runtime


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


def _kibl_fixture_row():
    return {
        "fixture_id": 636736,
        "league": "MLB",
        "start_date": "2026-06-12 19:10:00",
        "participants": [
            {"participant_id": 52888, "participant_side_id": 1, "participant": {"name": "New York Yankees"}},
            {"participant_id": 52889, "participant_side_id": 2, "participant": {"name": "Boston Red Sox"}},
        ],
    }


def _kibl_market_row():
    return {
        "participant_id": 52889,
        "participant_side_id": 2,
        "participant_rotation": 0,
        "market_id": 2465698420,
        "feed_source_id": 171,
        "fixture_id": 636736,
        "fixture_participant_id": 945155,
        "market_type_id": 1,
        "segment_id": 1,
        "point": 0,
        "price_american": 115,
        "price_decimal": 2.15,
        "price_fraction": "23/20",
        "is_live": False,
        "is_opener": False,
        "is_previous": False,
        "is_current": True,
        "inserted_on": "2026-06-11T17:09:23.982Z",
        "is_main": True,
        "uuid": None,
        "market_status_id": 1,
        "side_id": 2,
        "betting_type_id": 1,
        "alt_id": 0,
        "routing_key": "get.info.markets.0.5.643.0.2.636736.171.1.1.1.0",
        "inserted_on_epoch": 1781197763982479,
        "max_limit": 1750,
        "info": {"parser_name": "kibl-parser-bet105-tennis-prematch-game"},
    }


def test_runtime_enriches_fixture_metadata_and_moneyline_display(monkeypatch):
    _common_monkeypatch(monkeypatch)

    def fake_fetch_items(scope, params, game_pk, is_live, kind):
        if kind == "fixtures":
            rows = [_kibl_fixture_row()]
            return {}, "info/fixtures", rows, _normalize(rows)
        rows = [_kibl_market_row()]
        return {}, "info/markets", rows, _normalize(rows)

    monkeypatch.setattr(base, "_fetch_items", fake_fetch_items)

    payload = runtime.fetch_kibl_bet105_events(date="2026-06-12", raw=True, live_only=False)

    assert payload["status"] == "ok"
    assert payload["request_params"]["path"] == "info/markets"
    assert payload["market_count"] == 1
    event = payload["events"][0]
    assert event["name"] == "New York Yankees @ Boston Red Sox"
    assert event["away_team"]["name"] == "New York Yankees"
    assert event["home_team"]["name"] == "Boston Red Sox"
    assert event["start_time"] == "2026-06-12T23:10:00Z"

    market = event["markets"][0]
    assert market["market_id"] == 2465698420
    assert market["market_key"] == "h2h"
    assert market["market_type"] == "h2h"
    assert market["market_name"] == "Moneyline"
    assert market["line"] == 0

    selection = market["selections"][0]
    assert selection["selection_id"] == 945155
    assert selection["name"] == "Boston Red Sox"
    assert selection["description"] == "Boston Red Sox"
    assert selection["team"] == "Boston Red Sox"
    assert selection["side"] == "home"
    assert selection["line"] == 0
    assert selection["price"] == 115
    assert selection["odds"]["american"] == 115
    assert selection["odds"]["decimal"] == 2.15
    assert selection["odds"]["implied_probability"] == 0.4651
