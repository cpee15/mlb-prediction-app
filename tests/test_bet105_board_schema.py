from mlb_app import kibl_bet105_provider as base
from mlb_app import sportsbook_bet105_service as service


def _patch_common(monkeypatch):
    monkeypatch.setattr(base, "_configured", lambda: True)
    monkeypatch.setattr(base, "_cache_get", lambda key: None)
    monkeypatch.setattr(base, "_cache_set", lambda key, value: None)
    monkeypatch.setattr(
        base,
        "build_kibl_bet105_request_params",
        lambda *args, **kwargs: {
            "feed_source_id": 171,
            "betting_type_id": 1,
            "league_id": "20,643",
            "from_cache": False,
            "start_date": "2026-06-12 00:00:00",
            "end_date": "2026-06-13 00:00:00",
            **({"markets": "h2h,spreads,totals"} if kwargs.get("include_markets", True) else {}),
        },
    )


def _fixture_payload():
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


def _row(market_type_id, side_id, participant_id, point, price, decimal, fraction, market_id):
    return {
        "fixture_id": 636736,
        "market_id": market_id,
        "market_type_id": market_type_id,
        "participant_side_id": side_id,
        "side_id": side_id,
        "participant_id": participant_id,
        "fixture_participant_id": 900000 + market_id,
        "segment_id": 1,
        "point": point,
        "price_american": price,
        "price_decimal": decimal,
        "price_fraction": fraction,
        "is_current": True,
        "is_main": True,
    }


def _partial_rows():
    return [_row(1, 2, 52889, 0, 115, 2.15, "23/20", 1)]


def _full_rows():
    return [
        _row(1, 2, 52889, 0, 115, 2.15, "23/20", 1),
        _row(2, 1, 52888, -1.5, -105, 1.9524, "20/21", 2),
        _row(2, 2, 52889, 1.5, -115, 1.8696, "20/23", 3),
        _row(3, 3, None, 8.5, -110, 1.9091, "10/11", 4),
        _row(3, 4, None, 8.5, -110, 1.9091, "10/11", 5),
    ]


def test_fixture_first_board_schema_and_grouping(monkeypatch):
    _patch_common(monkeypatch)
    calls = []

    def fake_fetch_kibl_payload(scope, params, event_id=None, kind="markets"):
        if kind == "fixtures":
            return _fixture_payload(), "info/fixtures"
        return {"data": _partial_rows()}, "info/markets"

    def fake_fetch_items(scope, params, game_pk, is_live, kind):
        calls.append(dict(params))
        rows = _partial_rows() if len(calls) == 1 else _full_rows()
        return {}, "info/markets", rows, base._normalize_payload_items(rows, is_live=is_live)

    monkeypatch.setattr(base, "_fetch_kibl_payload", fake_fetch_kibl_payload)
    monkeypatch.setattr(base, "_fetch_items", fake_fetch_items)

    payload = service.fetch_board(date="2026-06-12", raw=True, live_only=False)

    assert len(calls) > 1
    event = payload["events"][0]
    assert event["name"] == "New York Yankees @ Boston Red Sox"
    assert event["start_time"] == "2026-06-12T23:10:00Z"
    assert event["commence_time"] == "2026-06-12T23:10:00Z"
    assert payload["market_count"] == 3
    assert payload["markets_meta"]["best_flattened_market_count"] == 5

    markets = {market["market_name"]: market for market in event["markets"]}
    assert set(markets) == {"Moneyline", "Spread", "Total"}
    assert markets["Moneyline"]["market_type_id"] == 1
    assert markets["Spread"]["market_type_id"] == 2
    assert markets["Total"]["market_type_id"] == 3

    assert len(markets["Spread"]["selections"]) == 2
    assert {selection["name"] for selection in markets["Spread"]["selections"]} == {"New York Yankees", "Boston Red Sox"}
    assert {selection["line"] for selection in markets["Spread"]["selections"]} == {-1.5, 1.5}

    assert len(markets["Total"]["selections"]) == 2
    assert {selection["name"] for selection in markets["Total"]["selections"]} == {"Over", "Under"}
    assert {selection["line"] for selection in markets["Total"]["selections"]} == {8.5}

    selections = [selection for market in event["markets"] for selection in market["selections"]]
    assert all(selection.get("selection_id") for selection in selections)
    assert all("side_id" in selection for selection in selections)
    assert all("participant_id" in selection for selection in selections)
    assert all("price_american" in selection for selection in selections)
    assert all("price_decimal" in selection for selection in selections)
    assert all("price_fraction" in selection for selection in selections)
    assert all("implied_probability" in selection for selection in selections)
    assert all(selection["is_current"] is True for selection in selections)
    assert all(selection["active"] is True for selection in selections)
    assert "Away @ Home" not in event["name"]
    assert all(market["market_name"] != "market" for market in event["markets"])
    assert all(selection["name"] != "Selection" for selection in selections)
