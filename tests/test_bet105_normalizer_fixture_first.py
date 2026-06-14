from mlb_app.bet105_normalizer import normalize_board
from mlb_app.kibl_bet105_types import Bet105RawBoard


FIXTURE = {
    "fixture_id": "fx-mlb-1",
    "sport_id": 2,
    "league_id": 7,
    "fixture_type_id": 1,
    "name": "New York Yankees vs Toronto Blue Jays",
    "start_time": "2026-06-14T17:37:00.000Z",
    "participants": [
        {"fixture_participant_id": "fp-away", "participant_id": "p-away", "name": "New York Yankees"},
        {"fixture_participant_id": "fp-home", "participant_id": "p-home", "name": "Toronto Blue Jays"},
    ],
}


def test_normalizer_renders_fixture_events_when_markets_are_empty():
    board = Bet105RawBoard(
        filters={"date": "2026-06-14"},
        fixture_rows=[
            FIXTURE,
            {
                "fixture_id": "fx-mlb-2",
                "sport_id": 2,
                "league_id": 7,
                "fixture_type_id": 1,
                "name": "Los Angeles Dodgers vs Chicago White Sox",
                "start_time": "2026-06-14T18:10:00.000Z",
                "participants": [
                    {"name": "Los Angeles Dodgers"},
                    {"name": "Chicago White Sox"},
                ],
            },
        ],
        market_rows=[],
        participant_rows=[],
        notes=[],
        ids={},
    )

    payload = normalize_board(board, live_only=False, raw=True)

    assert payload["status"] == "fixtures_only"
    assert payload["event_count"] == 2
    assert payload["market_count"] == 0
    assert [event["fixture_id"] for event in payload["events"]] == ["fx-mlb-1", "fx-mlb-2"]
    assert payload["events"][0]["name"] == "New York Yankees @ Toronto Blue Jays"
    assert payload["events"][1]["name"] == "Los Angeles Dodgers @ Chicago White Sox"
    assert payload["diagnostics"]["missing_start_times"] == 0


def test_normalizer_shapes_moneyline_market_rows_from_fixture_participants():
    board = Bet105RawBoard(
        filters={"date": "2026-06-14"},
        fixture_rows=[FIXTURE],
        market_rows=[
            {
                "fixture_id": "fx-mlb-1",
                "market_id": 101,
                "market_type_id": 1,
                "fixture_participant_id": "fp-away",
                "participant_id": "p-away",
                "price_american": -135,
                "price_decimal": 1.74,
                "price_fraction": "20/27",
                "is_current": True,
            },
            {
                "fixture_id": "fx-mlb-1",
                "market_id": 101,
                "market_type_id": 1,
                "fixture_participant_id": "fp-home",
                "participant_id": "p-home",
                "price_american": 115,
                "price_decimal": 2.15,
                "price_fraction": "23/20",
                "is_current": True,
            },
        ],
        participant_rows=[],
        notes=[],
        ids={},
    )

    payload = normalize_board(board, live_only=False, raw=True)

    assert payload["status"] == "ok"
    assert payload["event_count"] == 1
    assert payload["market_count"] == 1
    market = payload["events"][0]["markets"][0]
    assert market["market_key"] == "h2h"
    assert market["market_name"] == "Moneyline"
    selections = market["selections"]
    assert [selection["name"] for selection in selections] == ["New York Yankees", "Toronto Blue Jays"]
    assert [selection["price"] for selection in selections] == [-135, 115]
    assert selections[0]["odds"]["decimal"] == 1.74
    assert selections[1]["odds"]["american"] == 115


def test_normalizer_shapes_other_binary_market_without_generic_selection_label():
    board = Bet105RawBoard(
        filters={"date": "2026-06-14"},
        fixture_rows=[FIXTURE],
        market_rows=[
            {
                "fixture_id": "fx-mlb-1",
                "market_id": 202,
                "market_type_id": 0,
                "price_american": 252,
                "price_decimal": 3.52,
                "is_current": True,
                "info": {"side": "yes", "line_id": "3481684", "contestant_id": "3481684"},
            }
        ],
        participant_rows=[],
        notes=[],
        ids={},
    )

    payload = normalize_board(board, live_only=False, raw=True)

    market = payload["events"][0]["markets"][0]
    assert market["market_key"] == "other"
    assert market["market_name"] == "Other Market"
    assert market["selections"][0]["name"] == "Yes"
    assert market["selections"][0]["price"] == 252
    assert payload["diagnostics"]["placeholder_selection_names"] == 0
