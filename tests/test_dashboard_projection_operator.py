import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_object_models import DashboardPlayerCurrent, DashboardProjectionRun
from mlb_app.dashboard_projection_operator import run_canonical_projection_refresh
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
