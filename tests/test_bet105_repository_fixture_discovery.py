from mlb_app.kibl_bet105_repository import KiblBet105Repository


class FakeClient:
    def __init__(self):
        self.calls = []

    def post(self, path, body):
        self.calls.append((path, dict(body)))
        if path == "info/fixtures":
            assert "feed_source_id" not in body
            assert "betting_type_id" not in body
            assert str(body.get("sport_id")) == "2"
            assert str(body.get("league_id")) == "7"
            return {
                "data": [
                    {
                        "fixture_id": "fx-mlb-1",
                        "sport_id": 2,
                        "league_id": 7,
                        "name": "New York Yankees vs Toronto Blue Jays",
                        "start_time": "2026-06-14T17:37:00.000Z",
                    },
                    {
                        "fixture_id": "fx-mlb-2",
                        "sport_id": 2,
                        "league_id": 7,
                        "name": "Los Angeles Dodgers vs Chicago White Sox",
                        "start_time": "2026-06-14T18:10:00.000Z",
                    },
                ]
            }
        if path == "info/markets":
            assert body.get("feed_source_id") == 171
            assert body.get("betting_type_id") == 1
            assert str(body.get("sport_id")) == "2"
            assert str(body.get("league_id")) == "7"
            return {"data": []}
        return {"data": []}


def _filters():
    # Legacy provider may still produce the old tennis league filter; repository must override it.
    return {
        "feed_source_id": 171,
        "betting_type_id": 1,
        "league_id": "20,643",
        "start_date": "2026-06-14 00:00:00",
        "end_date": "2026-06-15 00:00:00",
        "from": "2026-06-14 00:00:00",
        "to": "2026-06-15 00:00:00",
    }


def test_fixture_discovery_uses_mlb_sport_2_league_7_without_book_filters():
    client = FakeClient()
    repo = KiblBet105Repository(client=client)
    notes = []

    rows = repo.fetch_fixture_summary(_filters(), notes)

    assert [row["fixture_id"] for row in rows] == ["fx-mlb-1", "fx-mlb-2"]
    fixture_calls = [body for path, body in client.calls if path == "info/fixtures"]
    assert len(fixture_calls) == 1
    assert fixture_calls[0]["sport_id"] == "2"
    assert fixture_calls[0]["league_id"] == "7"
    assert "feed_source_id" not in fixture_calls[0]
    assert "betting_type_id" not in fixture_calls[0]
    assert any("fixture_candidate:mlb_sport2_league7" in note for note in notes)
    assert any("fixture_summary:info/fixtures:raw=2:deduped=2:scope=sport_id=2,league_id=7" in note for note in notes)


def test_fetch_board_keeps_fixture_seeded_market_calls_disabled_by_default(monkeypatch):
    client = FakeClient()
    repo = KiblBet105Repository(client=client)
    monkeypatch.delenv("KIBL_ENABLE_FIXTURE_SEEDED_MARKETS", raising=False)
    monkeypatch.setattr(repo, "build_filters", lambda date=None, live_only=None, event_id=None: _filters())

    board = repo.fetch_board(date="2026-06-14", live_only=False)

    assert [row["fixture_id"] for row in board.fixture_rows] == ["fx-mlb-1", "fx-mlb-2"]
    market_calls = [body for path, body in client.calls if path == "info/markets"]
    assert market_calls
    assert all(body.get("feed_source_id") == 171 for body in market_calls)
    assert all(body.get("betting_type_id") == 1 for body in market_calls)
    assert all(str(body.get("sport_id")) == "2" for body in market_calls)
    assert all(str(body.get("league_id")) == "7" for body in market_calls)
    assert all("fixture_id" not in body for body in market_calls)
    assert any("fixture_ids_from_summary:2" in note for note in board.notes)
    assert any("fixture_seeded_market_requests_disabled" in note for note in board.notes)


def test_fixture_seeded_market_calls_are_explicit_opt_in(monkeypatch):
    client = FakeClient()
    repo = KiblBet105Repository(client=client)
    monkeypatch.setenv("KIBL_ENABLE_FIXTURE_SEEDED_MARKETS", "true")
    monkeypatch.setenv("KIBL_MARKET_FIXTURE_ID_LIMIT", "1")
    monkeypatch.setenv("KIBL_MARKET_FIXTURE_BATCH_LIMIT", "0")
    monkeypatch.setattr(repo, "build_filters", lambda date=None, live_only=None, event_id=None: _filters())

    board = repo.fetch_board(date="2026-06-14", live_only=False)

    assert [row["fixture_id"] for row in board.fixture_rows] == ["fx-mlb-1", "fx-mlb-2"]
    market_calls = [body for path, body in client.calls if path == "info/markets"]
    assert any(body.get("fixture_id") == "fx-mlb-1" for body in market_calls)
