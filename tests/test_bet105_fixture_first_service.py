from mlb_app import kibl_bet105_provider as base
from mlb_app import sportsbook_bet105_service as service


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


def _kibl_fixture_payload():
    return {
        "data": [
            {
                "fixture_id": 636736,
                "league": "MLB",
                "start_date": "2026-06-12 19:10:00",
                "participants": [
                    {"participant_id": 52888, "participant_side_id": 1, "participant": {"name": "New York Yankees"}},
                    {"participant_id": 52889, "participant_side_id": 2, "participant": {"name": "Boston Red Sox"}},
                ],
            }
        ]
    }


def _kibl_market_rows():
    return [
        {
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
    ]


def test_fixture_first_service_enriches_real_matchup_labels(monkeypatch):
    _common_monkeypatch(monkeypatch)

    def fake_fetch_kibl_payload(scope, params, event_id=None, kind="markets"):
        if kind == "fixtures":
            return _kibl_fixture_payload(), "info/fixtures"
        return {"data": _kibl_market_rows()}, "info/markets"

    def fake_fetch_items(scope, params, game_pk, is_live, kind):
        rows = _kibl_market_rows()
        return {}, "info/markets", rows, base._normalize_payload_items(rows, is_live=is_live)

    monkeypatch.setattr(base, "_fetch_kibl_payload", fake_fetch_kibl_payload)
    monkeypatch.setattr(base, "_fetch_items", fake_fetch_items)

    payload = service.fetch_board(date="2026-06-12", raw=True, live_only=False)

    assert payload["event_count"] == 1
    assert payload["market_count"] == 1
    assert payload["events"][0]["name"] == "New York Yankees @ Boston Red Sox"
    assert payload["events"][0]["away_team"]["name"] == "New York Yankees"
    assert payload["events"][0]["home_team"]["name"] == "Boston Red Sox"
    assert payload["events"][0]["start_time"] == "2026-06-12T23:10:00Z"

    market = payload["events"][0]["markets"][0]
    assert market["market_name"] == "Moneyline"
    assert market["market_key"] == "h2h"

    selection = market["selections"][0]
    assert selection["name"] == "Boston Red Sox"
    assert selection["description"] == "Boston Red Sox"
    assert selection["team"] == "Boston Red Sox"
    assert selection["price"] == 115


def test_fixture_first_service_exposes_debug_diagnostics(monkeypatch):
    _common_monkeypatch(monkeypatch)

    def fake_fetch_kibl_payload(scope, params, event_id=None, kind="markets"):
        if kind == "fixtures":
            return _kibl_fixture_payload(), "info/fixtures"
        return {"data": _kibl_market_rows()}, "info/markets"

    def fake_fetch_items(scope, params, game_pk, is_live, kind):
        rows = _kibl_market_rows()
        return {}, "info/markets", rows, base._normalize_payload_items(rows, is_live=is_live)

    monkeypatch.setattr(base, "_fetch_kibl_payload", fake_fetch_kibl_payload)
    monkeypatch.setattr(base, "_fetch_items", fake_fetch_items)

    payload = service.fetch_board(date="2026-06-12", raw=True, live_only=False)

    assert payload["fixtures"]["count"] == 1
    assert payload["fixtures"]["fixture_ids"] == ["636736"]
    assert payload["markets_meta"]["row_count"] == 1
    assert payload["markets_meta"]["market_type_ids"] == [1]
    assert payload["diagnostics"]["placeholder_event_names"] == 0
    assert payload["diagnostics"]["placeholder_market_names"] == 0
    assert payload["diagnostics"]["placeholder_selection_names"] == 0
