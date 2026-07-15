import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_object_models import DashboardPlayer
from mlb_app.dashboard_player_population import (
    active_player_window_days,
    canonical_player_id,
    evaluate_active_player,
    fetch_active_roster,
    fetch_confirmed_lineup_players,
    normalize_source_player,
    populate_dashboard_players,
    tracked_game_players,
)
from mlb_app.database import Base, BatterAggregate, StatcastEvent


AS_OF = dt.date(2026, 7, 15)


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, future=True)()


def test_identity_requires_explicit_mlb_id_and_never_derives_from_name():
    assert canonical_player_id({"fullName": "Same Name"}) is None
    assert canonical_player_id({"person": {"id": 123, "fullName": "Same Name"}}) == 123


def test_active_window_is_configurable_and_invalid_values_use_default(monkeypatch):
    monkeypatch.setenv("DASHBOARD_ACTIVE_PLAYER_WINDOW_DAYS", "45")
    assert active_player_window_days() == 45
    monkeypatch.setenv("DASHBOARD_ACTIVE_PLAYER_WINDOW_DAYS", "bad")
    assert active_player_window_days() == 30


def test_active_policy_has_four_explicit_eligibility_paths():
    assert evaluate_active_player({"appears_today_lineup": True}, as_of=AS_OF, window_days=30) == (True, "today_confirmed_or_projected_lineup")
    assert evaluate_active_player({"most_recent_lineup_date": AS_OF - dt.timedelta(days=29)}, as_of=AS_OF, window_days=30) == (True, "recent_confirmed_lineup")
    assert evaluate_active_player({"most_recent_game_date": AS_OF}, as_of=AS_OF, window_days=30) == (True, "recent_tracked_game")
    assert evaluate_active_player({"on_active_roster": True, "has_usable_analytics": True}, as_of=AS_OF, window_days=30) == (True, "active_roster_with_analytics")
    assert evaluate_active_player({"on_active_roster": True, "has_usable_analytics": False}, as_of=AS_OF, window_days=30)[0] is False


def test_verified_roster_adapter_preserves_mlb_identity_position_and_team():
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"roster": [{"person": {"id": 123, "fullName": "Roster Player"}, "position": {"type": "Outfielder", "abbreviation": "RF"}}]}

    rows = fetch_active_roster(112, 2026, team_name="Cubs", request_get=lambda *args, **kwargs: Response())
    assert rows[0]["mlb_player_id"] == 123
    assert rows[0]["team_id"] == 112
    assert rows[0]["primary_position"] == "RF"
    assert rows[0]["on_active_roster"] is True


def test_confirmed_lineup_ingestion_reports_unresolved_rows_without_name_matching():
    session = make_session()
    result = fetch_confirmed_lineup_players(
        session,
        AS_OF,
        matchup_builder=lambda *_: [{"game_pk": 9, "away_team_id": 112, "away_team_name": "Cubs", "home_team_id": 138, "home_team_name": "Cardinals"}],
        lineup_fetcher=lambda _: {"away": [{"id": 123, "fullName": "Resolved"}, {"fullName": "No ID"}], "home": []},
    )
    assert [row["mlb_player_id"] for row in result["players"]] == [123]
    assert result["unresolved_identities"][0]["identity_resolution_status"] == "missing_mlb_id"


def test_tracked_game_ingestion_groups_distinct_games_by_canonical_id():
    session = make_session()
    session.add_all([
        StatcastEvent(game_date=AS_OF, game_pk=1, at_bat_number=1, pitch_number=1, pitcher_id=900, batter_id=100, pitch_type="FF"),
        StatcastEvent(game_date=AS_OF, game_pk=1, at_bat_number=1, pitch_number=2, pitcher_id=900, batter_id=100, pitch_type="SL"),
        StatcastEvent(game_date=AS_OF - dt.timedelta(days=1), game_pk=2, at_bat_number=1, pitch_number=1, pitcher_id=901, batter_id=100, pitch_type="FF"),
    ])
    session.commit()
    hitter = next(row for row in tracked_game_players(session, as_of=AS_OF, window_days=30) if row["player_type"] == "hitter")
    assert hitter["mlb_player_id"] == 100
    assert hitter["tracked_game_count"] == 2


def test_population_deduplicates_by_id_and_combines_roster_lineup_and_analytics():
    session = make_session()
    session.add(BatterAggregate(batter_id=123, window="90d", end_date=AS_OF, avg_exit_velocity=91.0))
    session.commit()
    lineup = normalize_source_player({"id": 123, "fullName": "Player One"}, source="mlb_boxscore_confirmed_lineup", player_type="hitter", observed_date=AS_OF)
    lineup.update({"confirmed_lineup": True, "appears_today_lineup": True})
    roster = normalize_source_player({"person": {"id": 123, "fullName": "Player One"}, "position": {"abbreviation": "CF"}}, source="mlb_stats_active_roster", team_id=112, team_name="Cubs")
    roster["on_active_roster"] = True
    result = populate_dashboard_players(session, as_of=AS_OF, lineup_rows=[lineup], roster_rows=[roster])
    player = session.query(DashboardPlayer).one()
    assert result["resolved_candidate_count"] == 1
    assert player.mlb_player_id == 123
    assert player.current_team_id == 112
    assert player.is_active is True
    assert player.active_status_reason == "today_confirmed_or_projected_lineup"
    assert player.source_provenance_json["sources"] == ["mlb_boxscore_confirmed_lineup", "mlb_stats_active_roster"]


def test_population_reports_unresolved_and_does_not_persist_it():
    session = make_session()
    result = populate_dashboard_players(session, as_of=AS_OF, roster_rows=[{"source": "roster", "fullName": "No ID", "on_active_roster": True}])
    assert result["unresolved_identity_count"] == 1
    assert result["unresolved_identities"][0]["reason"] == "missing_mlb_id"
    assert session.query(DashboardPlayer).count() == 0


def test_existing_identity_allows_tracked_activity_upsert_without_guessing_name():
    session = make_session()
    session.add(DashboardPlayer(mlb_player_id=100, full_name="Known Player", player_type="hitter", is_active=False, active_status_reason="old", first_tracked_date=AS_OF - dt.timedelta(days=60), last_tracked_date=AS_OF - dt.timedelta(days=60), identity_resolution_status="resolved"))
    session.add(StatcastEvent(game_date=AS_OF, game_pk=1, at_bat_number=1, pitch_number=1, pitcher_id=900, batter_id=100, pitch_type="FF"))
    session.commit()
    result = populate_dashboard_players(session, as_of=AS_OF)
    player = session.get(DashboardPlayer, 100)
    assert result["activated_count"] == 1
    assert player.full_name == "Known Player"
    assert player.active_status_reason == "recent_tracked_game"


def test_complete_refresh_can_transition_unobserved_active_player_to_inactive():
    session = make_session()
    session.add(DashboardPlayer(mlb_player_id=123, full_name="Old Player", player_type="hitter", is_active=True, active_status_reason="recent_tracked_game", first_tracked_date=AS_OF - dt.timedelta(days=90), last_tracked_date=AS_OF - dt.timedelta(days=60), identity_resolution_status="resolved"))
    session.commit()
    result = populate_dashboard_players(session, as_of=AS_OF, transition_missing_players=True)
    player = session.get(DashboardPlayer, 123)
    assert result["deactivated_count"] == 1
    assert player.is_active is False
    assert player.active_status_reason == "not_observed_in_complete_refresh"


def test_repeated_population_is_idempotent_for_identity_and_activity_counts():
    session = make_session()
    lineup = normalize_source_player({"id": 123, "fullName": "Player One"}, source="lineup", player_type="hitter", observed_date=AS_OF)
    lineup.update({"confirmed_lineup": True, "appears_today_lineup": True})
    populate_dashboard_players(session, as_of=AS_OF, lineup_rows=[lineup])
    populate_dashboard_players(session, as_of=AS_OF, lineup_rows=[lineup])
    player = session.query(DashboardPlayer).one()
    assert player.lineup_appearance_count == 1
    assert player.most_recent_lineup_date == AS_OF
