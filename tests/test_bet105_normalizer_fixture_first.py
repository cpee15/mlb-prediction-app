from mlb_app.bet105_normalizer import normalize_board
from mlb_app.kibl_bet105_types import Bet105RawBoard


def test_normalizer_renders_fixture_events_when_markets_are_empty():
    board = Bet105RawBoard(
        filters={"date": "2026-06-14"},
        fixture_rows=[
            {
                "fixture_id": "fx-mlb-1",
                "sport_id": 2,
                "league_id": 7,
                "fixture_type_id": 1,
                "name": "New York Yankees vs Toronto Blue Jays",
                "start_time": "2026-06-14T17:37:00.000Z",
                "participants": [
                    {"name": "New York Yankees"},
                    {"name": "Toronto Blue Jays"},
                ],
            },
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
