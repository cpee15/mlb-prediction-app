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


def _fixtures_payload():
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
            },
            {
                "fixture_id": 636737,
                "league": "MLB",
                "start_date": "2026-06-12 20:10:00",
                "participants": [
                    {"participant_id": 52900, "participant_side_id": 1, "participant": {"name": "Chicago Cubs"}},
                    {"participant_id": 52901, "participant_side_id": 2, "participant": {"name": "St. Louis Cardinals"}},
                ],
            },
        ]
    }


def _market_row(fixture_id, participant_id, side_id, market_id, market_type_id, point, price):
    return {
        "fixture_id": fixture_id,
        "participant_id": participant_id,
        "participant_side_id": side_id,
        "market_id": market_id,
        "market_type_id": market_type_id,
        "segment_id": 1,
        "point": point,
        "price_american": price,
        "price_decimal": 1.91,
        "is_current": True,
        "side_id": side_id,
        "betting_type_id": 1,
        "feed_source_id": 171,
    }


def _rows_for_fixture(fixture_id):
    if str(fixture_id) == "636736":
        return [
            _market_row(636736, 52888, 1, 1001, 1, 0, -125),
            _market_row(636736, 52889, 2, 1002, 1, 0, 115),
        ]
    if str(fixture_id) == "636737":
        return [
            _market_row(636737, 52900, 1, 2001, 1, 0, 105),
            _market_row(636737, 52901, 2, 2002, 1, 0, -115),
        ]
    return []


def test_service_unions_partial_fixture_market_candidates(monkeypatch):
    _common_monkeypatch(monkeypatch)

    def fake_fetch_kibl_payload(scope, params, event_id=None, kind="markets"):
        if kind == "fixtures":
            return _fixtures_payload(), "info/fixtures"
        return {"data": []}, "info/markets"

    def fake_fetch_items(scope, params, game_pk, is_live, kind):
        # Simulate production KIBL behavior where broad market requests are empty,
        # but fixture-specific request bodies each return one real event.
        fixture_value = params.get("fixture_id") or params.get("event_id") or params.get("id")
        rows = _rows_for_fixture(fixture_value) if fixture_value else []
        return {}, "info/markets", rows, base._normalize_payload_items(rows, is_live=is_live)

    monkeypatch.setattr(base, "_fetch_kibl_payload", fake_fetch_kibl_payload)
    monkeypatch.setattr(base, "_fetch_items", fake_fetch_items)

    payload = service.fetch_board(date="2026-06-12", raw=True, live_only=False)

    assert payload["event_count"] == 2
    assert payload["market_count"] == 2
    assert payload["markets_meta"]["candidate_event_set_count"] >= 2
    assert payload["markets_meta"]["best_flattened_market_count"] == 2
    assert any(note.startswith("markets_union_selected") for note in payload["normalization_notes"])

    games = {event["name"] for event in payload["events"]}
    assert "New York Yankees @ Boston Red Sox" in games
    assert "Chicago Cubs @ St. Louis Cardinals" in games

    selections = [
        selection["name"]
        for event in payload["events"]
        for market in event["markets"]
        for selection in market["selections"]
    ]
    assert {"New York Yankees", "Boston Red Sox", "Chicago Cubs", "St. Louis Cardinals"}.issubset(set(selections))
    assert "Away @ Home" not in games
    assert "Selection" not in selections
