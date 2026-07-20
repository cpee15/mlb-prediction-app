import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_object_models import DashboardPlayerCurrent, DashboardProjectionRun
from mlb_app.dashboard_projection_operator import ensure_canonical_projection, run_canonical_projection_refresh
from mlb_app.database import Base


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
