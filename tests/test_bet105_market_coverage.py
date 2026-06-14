from mlb_app.bet105_normalizer import normalize_board
from mlb_app.kibl_bet105_types import Bet105RawBoard


FIXTURE = {
    "fixture_id": "fx-1",
    "sport_id": 2,
    "league_id": 7,
    "fixture_type_id": 1,
    "name": "Arizona Diamondbacks vs Cincinnati Reds",
    "start_time": "2026-06-14T17:40:00.000Z",
    "participants": [
        {"fixture_participant_id": "fp-away", "participant_id": "p-away", "name": "Arizona Diamondbacks"},
        {"fixture_participant_id": "fp-home", "participant_id": "p-home", "name": "Cincinnati Reds"},
    ],
}


def row(**kwargs):
    base = {"fixture_id": "fx-1", "is_current": True, "price_decimal": 1.91}
    base.update(kwargs)
    return base


def test_normalizer_preserves_three_game_line_families():
    board = Bet105RawBoard(
        filters={"date": "2026-06-14"},
        fixture_rows=[FIXTURE],
        market_rows=[
            row(market_id=1, market_type_id=1, fixture_participant_id="fp-away", participant_id="p-away", price_american=-114),
            row(market_id=1, market_type_id=1, fixture_participant_id="fp-home", participant_id="p-home", price_american=104),
            row(market_id=2, market_type_id=2, fixture_participant_id="fp-away", participant_id="p-away", point=-1.5, price_american=145),
            row(market_id=2, market_type_id=2, fixture_participant_id="fp-home", participant_id="p-home", point=1.5, price_american=-165),
            row(market_id=3, market_type_id=3, side_id=3, point=8.5, price_american=-110),
            row(market_id=3, market_type_id=3, side_id=4, point=8.5, price_american=-110),
        ],
        participant_rows=[],
        notes=[],
        ids={},
    )

    payload = normalize_board(board, live_only=False, raw=True)
    markets = {market["market_key"]: market for market in payload["events"][0]["markets"]}

    assert set(markets) == {"h2h", "spreads", "totals"}
    assert [selection["name"] for selection in markets["h2h"]["selections"]] == ["Arizona Diamondbacks", "Cincinnati Reds"]
    assert [selection["line"] for selection in markets["spreads"]["selections"]] == [-1.5, 1.5]
    assert [selection["name"] for selection in markets["totals"]["selections"]] == ["Over", "Under"]


def test_normalizer_marks_moneyline_only_coverage_truthfully():
    board = Bet105RawBoard(
        filters={"date": "2026-06-14"},
        fixture_rows=[FIXTURE],
        market_rows=[row(market_id=1, market_type_id=1, fixture_participant_id="fp-away", participant_id="p-away", price_american=-114)],
        participant_rows=[],
        notes=[],
        ids={},
    )

    payload = normalize_board(board, live_only=False, raw=True)

    assert payload["events"][0]["coverage_notes"] == ["Only Moneyline returned by Bet105/KIBL for this fixture request."]


def test_normalizer_labels_participantless_total_rows_from_kibl():
    board = Bet105RawBoard(
        filters={"date": "2026-06-14"},
        fixture_rows=[FIXTURE],
        market_rows=[row(market_id=3, market_type_id=3, participant_id=0, fixture_participant_id=0, price_american=-110)],
        participant_rows=[],
        notes=[],
        ids={},
    )

    payload = normalize_board(board, live_only=False, raw=True)
    total = payload["events"][0]["markets"][0]

    assert payload["status"] == "ok"
    assert payload["diagnostics"]["placeholder_selection_names"] == 0
    assert total["market_key"] == "totals"
    assert total["selections"][0]["name"] == "Total Runs"
    assert total["selections"][0]["price"] == -110
