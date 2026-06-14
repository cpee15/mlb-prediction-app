from mlb_app.bet105_normalizer import normalize_board
from mlb_app.kibl_bet105_fast_repository import KiblBet105Repository
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


def request_filters():
    return {
        "feed_source_id": 171,
        "betting_type_id": 1,
        "sport_id": "2",
        "league_id": "7",
        "start_date": "2026-06-14 00:00:00",
        "end_date": "2026-06-15 00:00:00",
    }


def row(**kwargs):
    base = {"fixture_id": "fx-1", "is_current": True, "price_decimal": 1.91}
    base.update(kwargs)
    return base


def test_fast_mode_builds_only_three_market_type_requests_per_fixture():
    repo = KiblBet105Repository(client=None, discovery_probes=False)

    bodies = repo.market_request_bodies(request_filters(), ["fixture-a", "fixture-b"])
    labels = [label for label, _body in bodies]

    assert len(bodies) == 6
    assert all("market_type_id:" in label for label in labels)
    assert not any("market_type_ids:list" in label for label in labels)
    assert not any("market_type_ids:csv" in label for label in labels)
    assert [body["market_type_id"] for _label, body in bodies[:3]] == [1, 2, 3]


def test_discovery_mode_keeps_extra_probe_variants():
    repo = KiblBet105Repository(client=None, discovery_probes=True)

    bodies = repo.market_request_bodies(request_filters(), ["fixture-a"])
    labels = [label for label, _body in bodies]

    assert len(bodies) == 6
    assert "dated:fixture_id:fixture-a" in labels
    assert "dated:fixture_id:fixture-a:market_type_id:1" in labels
    assert "dated:fixture_id:fixture-a:market_type_id:2" in labels
    assert "dated:fixture_id:fixture-a:market_type_id:3" in labels
    assert "dated:fixture_id:fixture-a:market_type_ids:list" in labels
    assert "dated:fixture_id:fixture-a:market_type_ids:csv" in labels


def test_worker_count_is_bounded(monkeypatch):
    monkeypatch.setenv("KIBL_BET105_MARKET_WORKERS", "999")
    assert KiblBet105Repository._worker_count() == 12

    monkeypatch.setenv("KIBL_BET105_MARKET_WORKERS", "0")
    assert KiblBet105Repository._worker_count() == 1


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


def test_normalizer_extracts_nested_kibl_price_object():
    board = Bet105RawBoard(
        filters={"date": "2026-06-14"},
        fixture_rows=[FIXTURE],
        market_rows=[
            {
                "fixture_id": "fx-1",
                "market_id": 1,
                "market_type_id": 1,
                "fixture_participant_id": "fp-away",
                "participant_id": "p-away",
                "price": {"american": -128, "decimal": 1.78125},
                "is_current": True,
            }
        ],
        participant_rows=[],
        notes=[],
        ids={},
    )

    payload = normalize_board(board, live_only=False, raw=True)
    selection = payload["events"][0]["markets"][0]["selections"][0]

    assert selection["name"] == "Arizona Diamondbacks"
    assert selection["price"] == -128
    assert selection["odds"]["american"] == -128
    assert selection["odds"]["implied_probability"] == 0.5614
