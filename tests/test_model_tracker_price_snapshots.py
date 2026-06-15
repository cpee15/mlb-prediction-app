import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.database import Base
from mlb_app.model_tracker_price_snapshots import (
    ModelTrackerPriceSnapshot,
    list_price_snapshots,
    normalize_price_rows_from_board,
    upsert_price_rows,
)


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session()


def _board(price=-120):
    return {
        "provider": "kibl_bet105",
        "book": "bet105",
        "status": "ok",
        "events": [
            {
                "event_id": "640812",
                "game_pk": 777,
                "fixture_id": "640812",
                "away_team": {"name": "Cubs"},
                "home_team": {"name": "Cardinals"},
                "start_time": "2026-06-15T20:15:00Z",
                "markets": [
                    {
                        "market_key": "h2h",
                        "market_type": "h2h",
                        "market_name": "Moneyline",
                        "selections": [
                            {
                                "selection_id": "home-ml",
                                "name": "Cardinals",
                                "description": "Cardinals",
                                "team": "Cardinals",
                                "price": price,
                                "line": None,
                            }
                        ],
                    }
                ],
            }
        ],
    }


def test_price_snapshot_upsert_is_idempotent_for_same_hour_bucket():
    session = _session()
    captured = dt.datetime(2026, 6, 15, 14, 37, 10)
    rows = normalize_price_rows_from_board(_board(-120), "2026-06-15", "bet105", captured_at=captured)

    first = upsert_price_rows(session, rows)
    second = upsert_price_rows(session, rows)

    assert first["inserted"] == 1
    assert second["inserted"] == 0
    assert second["updated"] == 1
    assert session.query(ModelTrackerPriceSnapshot).count() == 1


def test_price_snapshot_preserves_different_hourly_buckets():
    session = _session()
    row_one = normalize_price_rows_from_board(_board(-120), "2026-06-15", "bet105", captured_at=dt.datetime(2026, 6, 15, 14, 37))[0]
    row_two = normalize_price_rows_from_board(_board(-105), "2026-06-15", "bet105", captured_at=dt.datetime(2026, 6, 15, 15, 2))[0]

    upsert_price_rows(session, [row_one])
    upsert_price_rows(session, [row_two])
    payload = list_price_snapshots(session, "2026-06-15", provider="bet105")

    assert payload["snapshot_count"] == 2
    assert len(payload["summaries"]) == 1
    summary = payload["summaries"][0]
    assert summary["first_seen_price"] == -120
    assert summary["latest_price"] == -105
    assert summary["snapshot_count"] == 2


def test_price_snapshot_rows_store_provider_market_selection_and_implied_probability():
    rows = normalize_price_rows_from_board(_board(140), "2026-06-15", "bet105", captured_at=dt.datetime(2026, 6, 15, 14, 0))

    assert len(rows) == 1
    row = rows[0]
    assert row["provider"] == "bet105"
    assert row["event_id"] == "640812"
    assert row["game_pk"] == 777
    assert row["market_key"] == "h2h"
    assert row["selection_label"] == "Cardinals"
    assert row["price"] == 140
    assert row["decimal_price"] is not None
    assert row["implied_probability"] is not None
    assert row["snapshot_key"].endswith("2026-06-15 14:00:00")
