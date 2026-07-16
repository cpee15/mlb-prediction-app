import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_canonical_status import canonical_dashboard_status
from mlb_app.dashboard_object_models import DashboardPlayer, DashboardPlayerCurrent, DashboardProjectionRun
from mlb_app.database import Base, BatterPitchTypeMatchup


NOW = dt.datetime(2026, 7, 16, 12, 0, 0)
DATE = NOW.date()


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, future=True)()


def player(player_id, player_type):
    return DashboardPlayer(
        mlb_player_id=player_id, full_name=f"Player {player_id}", player_type=player_type,
        is_active=True, active_status_reason="recent_confirmed_lineup",
        first_tracked_date=DATE, last_tracked_date=DATE, most_recent_lineup_date=DATE,
        lineup_appearance_count=2, tracked_game_count=1, identity_resolution_status="resolved",
    )


def current(player_id, player_type, **values):
    return DashboardPlayerCurrent(
        mlb_player_id=player_id, snapshot_id=player_id, player_type=player_type,
        full_name=f"Player {player_id}", is_active=True, projection_version="v1",
        source_freshness_json={"snapshot_date": DATE.isoformat()}, provenance_json={},
        promoted_at=NOW, updated_at=NOW, metrics_json={}, **values,
    )


def test_status_exposes_counts_coverage_related_rows_and_run_evidence():
    session = make_session()
    session.add_all([
        player(1, "hitter"), player(2, "pitcher"),
        current(1, "hitter", xwoba=0.4, exit_velocity=92.0),
        current(2, "pitcher", xwoba=0.3, strikeout_rate=0.28),
        BatterPitchTypeMatchup(batter_id=1, batter_name="Player 1", opposing_pitcher_id=9, pitch_type="FF", target_date=DATE, pitches_seen=50),
        DashboardProjectionRun(run_type="canonical_refresh", target_date=DATE, status="success", started_at=NOW, completed_at=NOW, canonical_count=2, active_count=2, current_count=2, snapshot_count=2, result_json={}),
        DashboardProjectionRun(run_type="canonical_refresh", target_date=DATE, status="failed", started_at=NOW, completed_at=NOW, canonical_count=2, active_count=2, current_count=2, snapshot_count=2, error_type="RuntimeError", error_message="source failed", result_json={}),
    ])
    session.commit()
    result = canonical_dashboard_status(session, now=NOW + dt.timedelta(hours=1))
    assert result["status"] == "ready"
    assert result["population"]["active_count"] == 2
    assert result["current_projection"]["row_count"] == 2
    assert result["field_coverage"]["hitters"]["fields"]["xwoba"]["coverage"] == 1.0
    assert result["related_reports"]["confirmed_lineup_appearance_count"] == 4
    assert result["related_reports"]["active_hitter_arsenal_split_rows"] == 1
    assert result["refresh_runs"]["latest_success"]["status"] == "success"
    assert result["refresh_runs"]["latest_failure"]["error_type"] == "RuntimeError"


def test_status_is_explicitly_not_ready_for_empty_or_mismatched_projection():
    session = make_session()
    empty = canonical_dashboard_status(session, now=NOW)
    assert empty["status"] == "not_ready"
    assert "canonical_population_empty" in empty["issues"]
    session.add_all([player(1, "hitter"), player(2, "pitcher"), current(1, "hitter")])
    session.commit()
    mismatch = canonical_dashboard_status(session, now=NOW)
    assert "current_projection_population_mismatch" in mismatch["issues"]
