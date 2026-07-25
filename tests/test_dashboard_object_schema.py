import datetime as dt

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_object_models import DashboardPlayer, DashboardPlayerCurrent, DashboardPlayerSnapshot
from mlb_app.dashboard_report_types import FIELD_CATALOG, REPORT_TYPES, describe_report_type
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
    assert set(REPORT_TYPES) == {
        "all_active_hitters", "all_active_pitchers", "hitters_current_matchup",
        "hitters_arsenal_splits", "players_lineup_history", "teams_daily_analysis",
        "games_totals_analysis", "overall_players_daily_analysis",
        "model_projection_games", "model_projection_players",
        "model_tracker_snapshots", "competitive_batter_arsenal",
    }
    hitters = describe_report_type("all_active_hitters")
    assert hitters["population"] == {"is_active": True, "player_type": "hitter"}
    assert hitters["fields"][0]["name"] == "mlb_player_id"
    assert hitters["fields"][0]["supported_operators"] == ["eq", "in"]


def test_report_field_api_names_are_unique_normalized_and_filterable():
    for report_type, fields in FIELD_CATALOG.items():
        names = [field["name"] for field in fields]
        assert len(names) == len(set(names)), report_type
        assert all(name == name.strip() and " " not in name for name in names), report_type
        selectable = [field for field in fields if field.get("selectable", True)]
        assert all(field.get("filterable") is True for field in selectable), report_type
        assert all(field.get("supported_operators") for field in selectable), report_type


def test_daily_dataset_catalogs_expose_only_object_related_base_fields():
    teams = {field["name"] for field in FIELD_CATALOG["teams_daily_analysis"]}
    totals = {field["name"] for field in FIELD_CATALOG["games_totals_analysis"]}
    overall = {
        field["name"]
        for field in FIELD_CATALOG["overall_players_daily_analysis"]
    }
    assert "player_type" not in teams
    assert "player_type" not in totals
    assert "player_type" in overall
    assert {
        "edge_score",
        "win_edge",
        "run_differential",
        "iso",
        "obp",
        "slg",
    }.issubset(teams)
    assert {"projected_total", "raw_total", "run_index"}.issubset(totals)
    assert {
        "xwoba",
        "exit_velocity",
        "strikeout_rate",
        "xwoba_allowed",
        "pitch_type",
        "pitch_name",
        "lineup_verified",
        "lineup_source",
        "confirmed_lineup_date",
        "lineup_revision",
        "model_state",
    }.issubset(overall)
