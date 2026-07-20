import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_object_models import DashboardPlayerCurrent, DashboardProjectionRun
from mlb_app.dashboard_projection_operator import (
    ensure_canonical_projection,
    fetch_verified_active_rosters,
    run_canonical_projection_refresh,
)
from mlb_app.database import Base, BatterAggregate


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, future=True)()


def request_get(url, **kwargs):
    if url.endswith("/teams"):
        return Response({"teams": [{"id": index, "name": f"Team {index}"} for index in range(1, 31)]})
    return Response({"roster": []})


def test_operator_refresh_populates_from_verified_sources_and_records_success():
    session = make_session()
    today = dt.date.today()
    result = run_canonical_projection_refresh(
        session,
        target_date=today,
        request_get=request_get,
        matchup_builder=lambda *_: [{"game_pk": 10, "away_team_id": 1, "away_team_name": "Team 1", "home_team_id": 2, "home_team_name": "Team 2"}],
        lineup_fetcher=lambda _: {"away": [{"id": 101, "fullName": "Verified Hitter"}], "home": []},
    )
    assert result["status"] == "success"
    assert result["team_count"] == 30
    assert result["population"]["active_hitter_count"] == 1
    assert session.query(DashboardPlayerCurrent).count() == 1
    assert session.query(DashboardProjectionRun).one().status == "success"


def test_operator_failure_is_audited_without_erasing_prior_current_projection():
    session = make_session()
    today = dt.date.today()
    run_canonical_projection_refresh(
        session,
        target_date=today,
        request_get=request_get,
        matchup_builder=lambda *_: [{"game_pk": 10, "away_team_id": 1, "away_team_name": "Team 1", "home_team_id": 2, "home_team_name": "Team 2"}],
        lineup_fetcher=lambda _: {"away": [{"id": 101, "fullName": "Verified Hitter"}], "home": []},
    )
    version = session.query(DashboardPlayerCurrent).one().projection_version

    def incomplete_teams(url, **kwargs):
        return Response({"teams": [{"id": 1, "name": "Only Team"}]})

    with pytest.raises(RuntimeError, match="only 1 active teams"):
        run_canonical_projection_refresh(session, target_date=today, request_get=incomplete_teams)
    assert session.query(DashboardPlayerCurrent).one().projection_version == version
    assert session.query(DashboardProjectionRun).filter(DashboardProjectionRun.status == "failed").count() == 1



def test_empty_projection_auto_bootstraps_once_and_reuses_current_rows(monkeypatch):
    monkeypatch.setenv("DASHBOARD_CANONICAL_AUTO_BOOTSTRAP", "true")
    session = make_session()
    calls = []

    def refresh(current_session, *, target_date):
        calls.append(target_date)
        now = dt.datetime(2026, 7, 20, 12, 0, 0)
        current_session.add(DashboardPlayerCurrent(
            mlb_player_id=101,
            snapshot_id=1,
            player_type="hitter",
            full_name="Bootstrap Hitter",
            is_active=True,
            metrics_json={},
            projection_version="bootstrap-v1",
            source_freshness_json={"snapshot_date": target_date.isoformat()},
            provenance_json={},
            promoted_at=now,
            updated_at=now,
        ))
        current_session.commit()
        return {"run_id": 7, "projection_version": "bootstrap-v1"}

    target_date = dt.date(2026, 7, 20)
    first = ensure_canonical_projection(session, target_date=target_date, refresh=refresh)
    second = ensure_canonical_projection(session, target_date=target_date, refresh=refresh)

    assert first == {
        "status": "populated",
        "current_count": 1,
        "run_id": 7,
        "projection_version": "bootstrap-v1",
    }
    assert second == {"status": "already_available", "current_count": 1}
    assert calls == [target_date]


def test_auto_bootstrap_can_be_disabled_and_redacts_refresh_failure(monkeypatch):
    session = make_session()
    target_date = dt.date(2026, 7, 20)
    monkeypatch.setenv("DASHBOARD_CANONICAL_AUTO_BOOTSTRAP", "false")
    assert ensure_canonical_projection(session, target_date=target_date) == {
        "status": "disabled",
        "current_count": 0,
    }

    monkeypatch.setenv("DASHBOARD_CANONICAL_AUTO_BOOTSTRAP", "true")

    def fail(*_args, **_kwargs):
        raise RuntimeError("sensitive upstream detail")

    result = ensure_canonical_projection(session, target_date=target_date, refresh=fail)
    assert result == {
        "status": "failed",
        "current_count": 0,
        "error_type": "RuntimeError",
    }
    assert "sensitive" not in str(result)



def test_confirmed_lineup_failure_does_not_block_roster_aggregate_baseline():
    session = make_session()
    target_date = dt.date(2026, 7, 20)
    session.add(BatterAggregate(
        batter_id=201,
        window="season",
        end_date=target_date,
        avg_exit_velocity=91.0,
        avg_launch_angle=14.0,
        hard_hit_pct=0.44,
        barrel_pct=0.11,
        k_pct=0.19,
        bb_pct=0.09,
        batting_avg=0.275,
    ))
    session.commit()

    def roster_request_get(url, **kwargs):
        if url.endswith("/teams"):
            return Response({"teams": [
                {"id": index, "name": f"Team {index}"}
                for index in range(1, 31)
            ]})
        if url.endswith("/teams/1/roster"):
            return Response({"roster": [{
                "person": {"id": 201, "fullName": "Roster Hitter"},
                "position": {"abbreviation": "OF", "type": "Outfielder"},
            }]})
        return Response({"roster": []})

    result = run_canonical_projection_refresh(
        session,
        target_date=target_date,
        request_get=roster_request_get,
        matchup_builder=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("matchup source unavailable")
        ),
    )

    assert result["status"] == "success"
    assert result["lineup_player_count"] == 0
    assert result["lineup_error_count"] == 1
    assert result["population"]["active_hitter_count"] == 1
    current = session.query(DashboardPlayerCurrent).one()
    assert current.full_name == "Roster Hitter"
    assert current.exit_velocity == 91.0



def test_verified_rosters_are_collected_for_all_teams_and_fail_as_a_set():
    teams = [{"team_id": index, "team_name": f"Team {index}"} for index in range(1, 31)]
    calls = []

    def successful_get(url, **kwargs):
        calls.append(url)
        return Response({"roster": []})

    assert fetch_verified_active_rosters(
        teams,
        2026,
        request_get=successful_get,
        max_workers=8,
    ) == []
    assert len(calls) == 30

    def one_failure(url, **kwargs):
        if "/teams/15/roster" in url:
            raise TimeoutError("upstream timeout")
        return Response({"roster": []})

    with pytest.raises(
        RuntimeError,
        match=r"failed for 1 of 30 teams .*TimeoutError",
    ):
        fetch_verified_active_rosters(
            teams,
            2026,
            request_get=one_failure,
            max_workers=8,
        )


def test_abandoned_running_refresh_is_retired_before_bootstrap(monkeypatch):
    monkeypatch.setenv("DASHBOARD_CANONICAL_RUNNING_TIMEOUT_MINUTES", "5")
    session = make_session()
    checked_at = dt.datetime(2026, 7, 20, 14, 0, 0)
    abandoned = DashboardProjectionRun(
        run_type="canonical_refresh",
        target_date=checked_at.date(),
        status="running",
        started_at=checked_at - dt.timedelta(minutes=6),
        canonical_count=0,
        active_count=0,
        current_count=0,
        snapshot_count=0,
    )
    session.add(abandoned)
    session.commit()

    def refresh(current_session, *, target_date):
        current_session.add(DashboardPlayerCurrent(
            mlb_player_id=301,
            snapshot_id=1,
            player_type="pitcher",
            full_name="Recovered Pitcher",
            is_active=True,
            metrics_json={},
            projection_version="recovered-v1",
            source_freshness_json={"snapshot_date": target_date.isoformat()},
            provenance_json={},
            promoted_at=checked_at,
            updated_at=checked_at,
        ))
        current_session.commit()
        return {"run_id": 9, "projection_version": "recovered-v1"}

    result = ensure_canonical_projection(
        session,
        target_date=checked_at.date(),
        refresh=refresh,
        now=checked_at,
    )

    session.refresh(abandoned)
    assert abandoned.status == "failed"
    assert abandoned.error_type == "AbandonedProjectionRun"
    assert result["status"] == "populated"
    assert result["current_count"] == 1


def test_recent_running_refresh_still_prevents_overlap(monkeypatch):
    monkeypatch.setenv("DASHBOARD_CANONICAL_RUNNING_TIMEOUT_MINUTES", "5")
    session = make_session()
    checked_at = dt.datetime(2026, 7, 20, 14, 0, 0)
    running = DashboardProjectionRun(
        run_type="canonical_refresh",
        target_date=checked_at.date(),
        status="running",
        started_at=checked_at - dt.timedelta(minutes=1),
        canonical_count=0,
        active_count=0,
        current_count=0,
        snapshot_count=0,
    )
    session.add(running)
    session.commit()

    result = ensure_canonical_projection(
        session,
        target_date=checked_at.date(),
        refresh=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("overlapping refresh")
        ),
        now=checked_at,
    )

    assert result == {
        "status": "in_progress",
        "current_count": 0,
        "run_id": running.id,
    }
