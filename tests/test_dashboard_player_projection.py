import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_object_models import DashboardPlayer, DashboardPlayerCurrent, DashboardPlayerSnapshot
from mlb_app.dashboard_player_projection import backfill_player_projection, build_player_snapshot_rows, refresh_player_projection
from mlb_app.database import Base, BatterAggregate, PitcherAggregate
from mlb_app.my_dashboard_dataset import hydrate_dashboard_dataset


DATE = dt.date(2026, 7, 15)
NOW = dt.datetime(2026, 7, 15, 12, 0, 0)


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, future=True)()


def player(player_id, name, player_type="hitter", active=True):
    return DashboardPlayer(
        mlb_player_id=player_id,
        full_name=name,
        player_type=player_type,
        is_active=active,
        active_status_reason="recent_tracked_game",
        first_tracked_date=DATE - dt.timedelta(days=20),
        last_tracked_date=DATE,
        most_recent_game_date=DATE,
        identity_resolution_status="resolved",
    )


def row(player_id, name, player_type="hitter", **metrics):
    value = {"mlb_player_id": player_id, "full_name": name, "player_type": player_type, "metrics": {}, "provenance": {"sources": ["test"]}}
    value.update(metrics)
    return value


def test_default_builder_returns_every_active_resolved_player_and_aggregate_metrics():
    session = make_session()
    session.add_all([
        player(1, "Hitter"),
        player(2, "Pitcher", "pitcher"),
        player(3, "Inactive", active=False),
        BatterAggregate(batter_id=1, window="90d", end_date=DATE, avg_exit_velocity=92.1, avg_launch_angle=14.2, hard_hit_pct=0.46),
        PitcherAggregate(pitcher_id=2, window="90d", end_date=DATE, xwoba=0.301, xba=0.229, k_pct=0.28),
    ])
    session.commit()
    rows = build_player_snapshot_rows(session, DATE)
    assert [item["mlb_player_id"] for item in rows] == [1, 2]
    assert rows[0]["exit_velocity"] == pytest.approx(92.1)
    assert rows[1]["xwoba"] == pytest.approx(0.301)


def test_date_partitioned_records_overlay_metrics_but_do_not_define_population():
    session = make_session()
    session.add_all([player(1, "With Overlay"), player(2, "Without Overlay")])
    session.commit()
    hydrate_dashboard_dataset(
        session=session,
        date=DATE.isoformat(),
        component="hitters",
        payload_builder=lambda: {"items": [{"entity_id": "1", "entity_name": "With Overlay", "entity_type": "hitter", "player_type": "hitter", "score": 0.8, "metrics": {"xwOBA": 0.401}}]},
        now=NOW,
    )
    rows = build_player_snapshot_rows(session, DATE)
    assert len(rows) == 2
    assert rows[0]["xwoba"] == pytest.approx(0.401)
    assert rows[1]["xwoba"] is None


def test_refresh_creates_historical_snapshots_and_current_projection_atomically():
    session = make_session()
    session.add_all([player(1, "One"), player(2, "Two", "pitcher")])
    session.commit()
    result = refresh_player_projection(
        session,
        snapshot_date=DATE,
        row_builder=lambda: [row(1, "One", xwoba=0.4), row(2, "Two", "pitcher", xwoba=0.3)],
        now=NOW,
    )
    assert result["row_count"] == 2
    assert result["snapshots_created"] == 2
    assert session.query(DashboardPlayerSnapshot).count() == 2
    assert session.query(DashboardPlayerCurrent).count() == 2
    assert session.get(DashboardPlayerCurrent, 1).xwoba == pytest.approx(0.4)


def test_identical_refresh_is_idempotent():
    session = make_session()
    session.add(player(1, "One"))
    session.commit()
    builder = lambda: [row(1, "One", xwoba=0.4)]
    first = refresh_player_projection(session, snapshot_date=DATE, row_builder=builder, now=NOW)
    second = refresh_player_projection(session, snapshot_date=DATE, row_builder=builder, now=NOW)
    assert first["snapshots_created"] == 1
    assert second["snapshots_created"] == 0
    assert second["snapshots_reused"] == 1
    assert second["current_created"] == second["current_updated"] == 0
    assert session.query(DashboardPlayerSnapshot).count() == 1


def test_incremental_change_creates_only_changed_player_snapshot():
    session = make_session()
    session.add_all([player(1, "One"), player(2, "Two")])
    session.commit()
    refresh_player_projection(session, snapshot_date=DATE, row_builder=lambda: [row(1, "One", xwoba=0.4), row(2, "Two", xwoba=0.3)], now=NOW)
    changed = refresh_player_projection(session, snapshot_date=DATE, row_builder=lambda: [row(1, "One", xwoba=0.41), row(2, "Two", xwoba=0.3)], now=NOW + dt.timedelta(minutes=5))
    assert changed["snapshots_created"] == 1
    assert changed["snapshots_reused"] == 1
    assert session.query(DashboardPlayerSnapshot).count() == 3


def test_builder_failure_preserves_prior_current_projection():
    session = make_session()
    session.add(player(1, "One"))
    session.commit()
    refresh_player_projection(session, snapshot_date=DATE, row_builder=lambda: [row(1, "One", xwoba=0.4)], now=NOW)
    previous = session.get(DashboardPlayerCurrent, 1).projection_version

    def fail():
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError, match="source failed"):
        refresh_player_projection(session, snapshot_date=DATE, row_builder=fail)
    assert session.get(DashboardPlayerCurrent, 1).projection_version == previous
    assert session.get(DashboardPlayerCurrent, 1).xwoba == pytest.approx(0.4)


def test_failure_after_staging_rolls_back_snapshots_and_current_changes():
    session = make_session()
    session.add(player(1, "One"))
    session.commit()
    refresh_player_projection(session, snapshot_date=DATE, row_builder=lambda: [row(1, "One", xwoba=0.4)], now=NOW)

    def reject(_):
        raise RuntimeError("promotion rejected")

    with pytest.raises(RuntimeError, match="promotion rejected"):
        refresh_player_projection(
            session,
            snapshot_date=DATE,
            row_builder=lambda: [row(1, "One", xwoba=0.5)],
            promotion_guard=reject,
            now=NOW + dt.timedelta(minutes=5),
        )
    assert session.query(DashboardPlayerSnapshot).count() == 1
    assert session.get(DashboardPlayerCurrent, 1).xwoba == pytest.approx(0.4)


def test_empty_or_incomplete_full_refresh_is_rejected_before_promotion():
    session = make_session()
    session.add_all([player(1, "One"), player(2, "Two")])
    session.commit()
    with pytest.raises(ValueError, match="empty"):
        refresh_player_projection(session, snapshot_date=DATE, row_builder=lambda: [])
    with pytest.raises(ValueError, match="coverage mismatch"):
        refresh_player_projection(session, snapshot_date=DATE, row_builder=lambda: [row(1, "One")])
    assert session.query(DashboardPlayerCurrent).count() == 0


def test_partial_refresh_updates_requested_player_without_removing_other_current_rows():
    session = make_session()
    session.add_all([player(1, "One"), player(2, "Two")])
    session.commit()
    refresh_player_projection(session, snapshot_date=DATE, row_builder=lambda: [row(1, "One", xwoba=0.4), row(2, "Two", xwoba=0.3)], now=NOW)
    refresh_player_projection(
        session,
        snapshot_date=DATE,
        row_builder=lambda: [row(1, "One", xwoba=0.5)],
        full_refresh=False,
        now=NOW + dt.timedelta(minutes=5),
    )
    assert session.query(DashboardPlayerCurrent).count() == 2
    assert session.get(DashboardPlayerCurrent, 1).xwoba == pytest.approx(0.5)
    assert session.get(DashboardPlayerCurrent, 2).xwoba == pytest.approx(0.3)


def test_successful_full_refresh_removes_projection_for_now_inactive_player():
    session = make_session()
    first, second = player(1, "One"), player(2, "Two")
    session.add_all([first, second])
    session.commit()
    refresh_player_projection(session, snapshot_date=DATE, row_builder=lambda: [row(1, "One"), row(2, "Two")], now=NOW)
    second.is_active = False
    session.commit()
    result = refresh_player_projection(session, snapshot_date=DATE + dt.timedelta(days=1), row_builder=lambda: [row(1, "One")], now=NOW + dt.timedelta(days=1))
    assert result["current_removed"] == 1
    assert session.get(DashboardPlayerCurrent, 2) is None


def test_projection_exposes_freshness_provenance_and_versions():
    session = make_session()
    session.add(player(1, "One"))
    session.commit()
    result = refresh_player_projection(
        session,
        snapshot_date=DATE,
        row_builder=lambda: [row(
            1,
            "One",
            model_score=0.72,
            source_versions={"solver": "v7"},
            provenance={"sources": ["solver"]},
        )],
        now=NOW,
    )
    current = session.get(DashboardPlayerCurrent, 1)
    assert current.source_freshness_json["snapshot_date"] == DATE.isoformat()
    assert current.source_freshness_json["source_versions"] == {"solver": "v7"}
    assert current.provenance_json["sources"] == ["solver"]
    assert result["field_coverage"]["hitter"]["row_count"] == 1
    assert result["field_coverage"]["hitter"]["fields"]["model_score"]["coverage"] == 1.0


def test_backfill_retains_each_date_and_promotes_only_final_successful_date():
    session = make_session()
    session.add(player(1, "One"))
    session.commit()
    result = backfill_player_projection(
        session,
        dates=[DATE - dt.timedelta(days=1), DATE],
        row_builder=lambda target: [row(1, "One", xwoba=0.39 if target < DATE else 0.4)],
    )
    assert result["successful_date_count"] == 2
    assert result["snapshot_rows_created"] == 2
    assert result["historical_snapshot_count"] == 2
    assert result["final_current_row_count"] == 1
    assert session.get(DashboardPlayerCurrent, 1).xwoba == pytest.approx(0.4)
