import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.database import Base
from mlb_app.my_dashboard_dataset import (
    DATASET_MODE_ACTIVE_LINEUPS,
    DATASET_MODE_STANDARD,
    MyDashboardRecord,
    dashboard_dataset_status,
    hydrate_dashboard_dataset,
)


@pytest.fixture()
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, future=True)
    with factory() as db:
        yield db


def payload(name="One", score=0.8, **extra):
    value = {
        "items": [
            {
                "entity_id": "101",
                "entity_name": name,
                "entity_type": "hitter",
                "player_type": "hitter",
                "team": "CHC",
                "opponent": "STL",
                "game_pk": 9001,
                "score": score,
                "base_score": score,
                "confidence": "high",
                "source": "test_solver",
                "metrics": {"xwOBA": 0.401, "EV": 92.3},
                "reasoning": ["test reason"],
                "missing_data": [],
            }
        ],
        "data_quality": {"source_rows": 1},
    }
    value.update(extra)
    return value


def test_hydration_persists_normalized_current_rows(session):
    result = hydrate_dashboard_dataset(
        session=session,
        date="2026-07-15",
        component="hitters",
        payload_builder=lambda: payload(),
        ttl_seconds=300,
        solver_version="solver-test-v1",
        now=dt.datetime(2026, 7, 15, 12, 0, 0),
    )

    row = session.query(MyDashboardRecord).one()
    assert result["dataset_source"] == "my_dashboard_records"
    assert result["dataset_row_count"] == 1
    assert row.dataset_mode == DATASET_MODE_STANDARD
    assert row.is_current is True
    assert row.entity_key.startswith("hitters:101:9001")
    assert row.metrics_json["xwOBA"] == pytest.approx(0.401)
    assert row.record_json["entity_name"] == "One"
    assert row.solver_version == "solver-test-v1"


def test_rehydration_promotes_new_version_without_duplicate_current_rows(session):
    first = hydrate_dashboard_dataset(
        session=session,
        date="2026-07-15",
        component="hitters",
        payload_builder=lambda: payload(name="Old", score=0.5),
    )
    second = hydrate_dashboard_dataset(
        session=session,
        date="2026-07-15",
        component="hitters",
        payload_builder=lambda: payload(name="New", score=0.9),
    )

    rows = session.query(MyDashboardRecord).order_by(MyDashboardRecord.id.asc()).all()
    current = [row for row in rows if row.is_current]
    assert len(rows) == 2
    assert len(current) == 1
    assert current[0].entity_name == "New"
    assert first["dataset_version"] != second["dataset_version"]


def test_standard_and_active_lineup_datasets_do_not_collide(session):
    hydrate_dashboard_dataset(
        session=session,
        date="2026-07-15",
        component="hitters",
        payload_builder=lambda: payload(name="Standard"),
    )
    hydrate_dashboard_dataset(
        session=session,
        date="2026-07-15",
        component="hitters",
        active_lineups=True,
        payload_builder=lambda: payload(name="Confirmed", lineup_revision="rev-7", model_state="confirmed"),
    )

    standard = dashboard_dataset_status(session=session, date="2026-07-15", component="hitters")
    active = dashboard_dataset_status(session=session, date="2026-07-15", component="hitters", active_lineups=True)
    assert standard["dataset_mode"] == DATASET_MODE_STANDARD
    assert active["dataset_mode"] == DATASET_MODE_ACTIVE_LINEUPS
    assert standard["dataset_version"] != active["dataset_version"]
    assert active["lineup_revision"] == "rev-7"
    assert active["model_state"] == "confirmed"


def test_failed_builder_keeps_previous_current_dataset(session):
    first = hydrate_dashboard_dataset(
        session=session,
        date="2026-07-15",
        component="hitters",
        payload_builder=lambda: payload(name="Safe"),
    )

    def fail():
        raise RuntimeError("solver failed")

    with pytest.raises(RuntimeError, match="solver failed"):
        hydrate_dashboard_dataset(
            session=session,
            date="2026-07-15",
            component="hitters",
            payload_builder=fail,
        )

    status = dashboard_dataset_status(session=session, date="2026-07-15", component="hitters")
    assert status["ready"] is True
    assert status["dataset_version"] == first["dataset_version"]
    assert status["dataset_row_count"] == 1


def test_status_reports_expiration_without_deleting_rows(session):
    hydrate_dashboard_dataset(
        session=session,
        date="2026-07-15",
        component="hitters",
        payload_builder=lambda: payload(),
        ttl_seconds=60,
        now=dt.datetime(2026, 7, 15, 12, 0, 0),
    )

    status = dashboard_dataset_status(
        session=session,
        date="2026-07-15",
        component="hitters",
        now=dt.datetime(2026, 7, 15, 12, 2, 0),
    )
    assert status["ready"] is True
    assert status["stale"] is True
    assert status["dataset_row_count"] == 1
