import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_object_models import DashboardPlayer
from mlb_app.dashboard_related_report_query import query_related_report
from mlb_app.dashboard_report_types import describe_report_type
from mlb_app.database import Base, BatterPitchTypeMatchup


DATE = dt.date(2026, 7, 16)


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, future=True)()


def player(player_id, name, appearances, player_type="hitter", active=True):
    return DashboardPlayer(
        mlb_player_id=player_id, full_name=name, player_type=player_type,
        is_active=active, active_status_reason="recent_confirmed_lineup",
        first_tracked_date=DATE - dt.timedelta(days=30), last_tracked_date=DATE,
        most_recent_lineup_date=DATE, most_recent_game_date=DATE,
        lineup_appearance_count=appearances, tracked_game_count=10,
        identity_resolution_status="resolved",
    )


def test_lineup_history_is_a_complete_validated_related_report():
    session = make_session()
    session.add_all([player(1, "Alpha", 3), player(2, "Beta", 1), player(3, "None", 0)])
    session.commit()
    result = query_related_report(session, "players_lineup_history", page_size=1)
    assert result["totalSize"] == 2
    assert result["records"][0]["full_name"] == "Alpha"
    assert result["query_source"] == "dashboard_players"
    assert result["page_info"]["has_next"] is True
    fields = {field["name"] for field in describe_report_type("players_lineup_history")["fields"]}
    assert {"most_recent_lineup_date", "lineup_appearance_count", "tracked_game_count"}.issubset(fields)


def test_arsenal_splits_only_include_active_canonical_hitters_and_filter_in_sql():
    session = make_session()
    session.add_all([player(1, "Alpha", 3), player(2, "Inactive", 3, active=False)])
    session.add_all([
        BatterPitchTypeMatchup(batter_id=1, batter_name="Alpha", opposing_pitcher_id=9, pitch_type="FF", target_date=DATE, pitches_seen=80, xwoba=0.41),
        BatterPitchTypeMatchup(batter_id=1, batter_name="Alpha", opposing_pitcher_id=9, pitch_type="SL", target_date=DATE, pitches_seen=20, xwoba=0.31),
        BatterPitchTypeMatchup(batter_id=2, batter_name="Inactive", opposing_pitcher_id=9, pitch_type="FF", target_date=DATE, pitches_seen=100, xwoba=0.5),
    ])
    session.commit()
    result = query_related_report(
        session, "hitters_arsenal_splits",
        filters={"pitch_type": "FF", "min_pitches_seen": 50},
    )
    assert result["totalSize"] == 1
    assert result["records"][0]["batter_id"] == 1
    assert result["records"][0]["pitches_seen"] == 80
    assert result["query_source"] == "batter_pitch_type_matchups"


def test_related_reports_reject_unsupported_fields_weights_and_filters():
    session = make_session()
    with pytest.raises(ValueError, match="Weights are not supported"):
        query_related_report(session, "players_lineup_history", weights={"Score": 2})
    with pytest.raises(ValueError, match="Unsupported selected field"):
        query_related_report(session, "players_lineup_history", selected_fields=["secret"])
    with pytest.raises(ValueError, match="Unsupported filter field"):
        query_related_report(session, "hitters_arsenal_splits", filters=[{"field": "secret", "value": 1}])
