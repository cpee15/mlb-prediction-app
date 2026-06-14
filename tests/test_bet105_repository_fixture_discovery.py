from mlb_app.kibl_bet105_repository import KiblBet105Repository


class FakeClient:
    def __init__(self):
        self.calls = []

    def post(self, path, body):
        self.calls.append((path, dict(body)))
        if path == "info/fixtures":
            league_id = str(body.get("league_id"))
            assert "feed_source_id" not in body
            assert "betting_type_id" not in body
            if league_id == "20":
                return {"data": [{"fixture_id": "fx-20", "league_id": 20, "start_time": "2026-06-14T12:00:00.000Z"}]}
            if league_id == "643":
                return {"data": [{"fixture_id": "fx-643", "league_id": 643, "start_time": "2026-06-14T13:00:00.000Z"}]}
            return {"data": []}
        if path == "info/markets":
            assert body.get("feed_source_id") == 171
            assert body.get("betting_type_id") == 1
            return {"data": []}
        return {"data": []}


def test_fixture_discovery_splits_baseball_leagues_without_book_filters():
    client = FakeClient()
    repo = KiblBet105Repository(client=client)
    notes = []
    filters = {
        "feed_source_id": 171,
        "betting_type_id": 1,
        "league_id": "20,643",
        "start_date": "2026-06-14 00:00:00",
        "end_date": "2026-06-15 00:00:00",
        "from": "2026-06-14 00:00:00",
        "to": "2026-06-15 00:00:00",
    }

    rows = repo.fetch_fixture_summary(filters, notes)

    assert [row["fixture_id"] for row in rows] == ["fx-20", "fx-643"]
    fixture_calls = [body for path, body in client.calls if path == "info/fixtures"]
    assert [body["league_id"] for body in fixture_calls] == ["20", "643"]
    assert all("feed_source_id" not in body for body in fixture_calls)
    assert all("betting_type_id" not in body for body in fixture_calls)
    assert any("fixture_candidate:league20" in note for note in notes)
    assert any("fixture_candidate:league643" in note for note in notes)
    assert any("fixture_summary:info/fixtures:raw=2:deduped=2" in note for note in notes)


def test_fetch_board_uses_fixture_ids_before_book_scoped_market_calls(monkeypatch):
    client = FakeClient()
    repo = KiblBet105Repository(client=client)
    monkeypatch.setattr(
        repo,
        "build_filters",
        lambda date=None, live_only=None, event_id=None: {
            "feed_source_id": 171,
            "betting_type_id": 1,
            "league_id": "20,643",
            "start_date": "2026-06-14 00:00:00",
            "end_date": "2026-06-15 00:00:00",
            "from": "2026-06-14 00:00:00",
            "to": "2026-06-15 00:00:00",
        },
    )

    board = repo.fetch_board(date="2026-06-14", live_only=False)

    assert [row["fixture_id"] for row in board.fixture_rows] == ["fx-20", "fx-643"]
    market_calls = [body for path, body in client.calls if path == "info/markets"]
    assert market_calls
    assert all(body.get("feed_source_id") == 171 for body in market_calls)
    assert all(body.get("betting_type_id") == 1 for body in market_calls)
    assert any(body.get("fixture_id") in {"fx-20", "fx-643"} for body in market_calls)
    assert any("fixture_ids_from_summary:2" in note for note in board.notes)
