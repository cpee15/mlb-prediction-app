import datetime as dt

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_object_models import DashboardPlayer, DashboardPlayerCurrent, DashboardPlayerSnapshot
from mlb_app.dashboard_report_types import REPORT_TYPES, describe_report_type
from mlb_app.database import Base, create_tables


def test_object_tables_and_query_indexes_are_sqlite_compatible():
    engine = create_engine("sqlite:///:memory:", future=True)
    create_tables(engine)
    inspector = inspect(engine)
    assert {"dashboard_players", "dashboard_player_snapshots", "dashboard_player_current"}.issubset(inspector.get_table_names())
    assert "ix_dashboard_current_active_type" in {row["name"] for row in inspector.get_indexes("dashboard_player_current")}


def test_snapshot_lineup_status_accepts_all_canonical_activity_reasons():
    longest_reason = "today_confirmed_or_projected_lineup"
    assert DashboardPlayerSnapshot.__table__.c.lineup_status.type.length >= len(longest_reason)


def test_canonical_player_id_is_unique():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        values = dict(mlb_player_id=123, full_name="One Player", player_type="hitter", is_active=True, first_tracked_date=dt.date(2026, 7, 1), last_tracked_date=dt.date(2026, 7, 15), identity_resolution_status="resolved")
        session.add(DashboardPlayer(**values))
        session.commit()
        session.add(DashboardPlayer(**values))
        with pytest.raises(IntegrityError):
            session.commit()


def test_snapshot_business_key_is_versioned_and_unique():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        values = dict(mlb_player_id=123, snapshot_date=dt.date(2026, 7, 15), analytical_context="current_metrics", snapshot_version="v1", generated_at=dt.datetime(2026, 7, 15, 10), refreshed_at=dt.datetime(2026, 7, 15, 10))
        session.add(DashboardPlayerSnapshot(**values))
        session.commit()
        session.add(DashboardPlayerSnapshot(**values))
        with pytest.raises(IntegrityError):
            session.commit()


def test_current_projection_has_one_row_per_canonical_player():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        values = dict(mlb_player_id=123, snapshot_id=1, player_type="hitter", full_name="One Player", is_active=True, projection_version="v1", promoted_at=dt.datetime(2026, 7, 15, 10), updated_at=dt.datetime(2026, 7, 15, 10))
        session.add(DashboardPlayerCurrent(**values))
        session.commit()
        session.add(DashboardPlayerCurrent(**values))
        with pytest.raises(IntegrityError):
            session.commit()


def test_report_type_registry_is_explicit_and_describable():
    assert set(REPORT_TYPES) == {"all_active_hitters", "all_active_pitchers", "hitters_current_matchup", "hitters_arsenal_splits", "players_lineup_history", "teams_daily_analysis", "games_totals_analysis"}
    hitters = describe_report_type("all_active_hitters")
    assert hitters["population"] == {"is_active": True, "player_type": "hitter"}
    assert hitters["fields"][0]["name"] == "mlb_player_id"
    assert hitters["fields"][0]["supported_operators"] == ["eq", "in"]
