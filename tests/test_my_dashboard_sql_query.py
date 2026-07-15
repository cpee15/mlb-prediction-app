import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.database import Base
from mlb_app.my_dashboard_dataset import hydrate_dashboard_dataset
from mlb_app.my_dashboard_sql_query import query_dashboard_dataset


DATE = "2026-07-15"


def session_factory():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, future=True)


def rows():
    return [
        {
            "entity_id": "1",
            "entity_name": "Alpha Hitter",
            "entity_type": "hitter",
            "player_type": "hitter",
            "team": "CHC",
            "opponent": "STL",
            "game_pk": 10,
            "score": 0.91,
            "base_score": 0.91,
            "confidence": "high",
            "source": "dashboard_fixture",
            "metrics": {"EV": 95.0, "xwOBA": 0.410},
        },
        {
            "entity_id": "2",
            "entity_name": "Bravo Hitter",
            "entity_type": "hitter",
            "player_type": "hitter",
            "team": "CHC",
            "opponent": "STL",
            "game_pk": 10,
            "score": 0.82,
            "base_score": 0.82,
            "confidence": "medium",
            "source": "dashboard_fixture",
            "metrics": {"EV": 91.0, "xwOBA": 0.370},
        },
        {
            "entity_id": "3",
            "entity_name": "Charlie Hitter",
            "entity_type": "hitter",
            "player_type": "hitter",
            "team": "MIL",
            "opponent": "CIN",
            "game_pk": 11,
            "score": 0.73,
            "base_score": 0.73,
            "confidence": "medium",
            "source": "dashboard_fixture",
            "metrics": {"EV": 89.0, "xwOBA": 0.350},
        },
        {
            "entity_id": "4",
            "entity_name": "Delta Hitter",
            "entity_type": "hitter",
            "player_type": "hitter",
            "team": "MIL",
            "opponent": "CIN",
            "game_pk": 11,
            "score": 0.64,
            "base_score": 0.64,
            "confidence": "low",
            "source": "dashboard_fixture",
            "metrics": {"EV": 87.0, "xwOBA": 0.320},
        },
    ]


def hydrate(session, payload_rows=None):
    return hydrate_dashboard_dataset(
        session=session,
        date=DATE,
        component="hitters",
        payload_builder=lambda: {"items": payload_rows or rows(), "model_state": "projected"},
        now=dt.datetime(2026, 7, 15, 12, 0, 0),
    )


def test_sql_filters_before_count_and_pagination():
    Session = session_factory()
    with Session() as session:
        hydrate(session)
        result = query_dashboard_dataset(
            session=session,
            date=DATE,
            component="hitters",
            filters={"team": "CHC"},
            page_size=1,
            page_number=1,
        )
        assert result["dataset_source"] == "my_dashboard_records"
        assert result["totalSize"] == 2
        assert result["page_info"]["record_count"] == 1
        assert result["items"][0]["entity_name"] == "Alpha Hitter"
        assert result["page_info"]["has_next"] is True


def test_page_two_does_not_repeat_page_one():
    Session = session_factory()
    with Session() as session:
        hydrate(session)
        first = query_dashboard_dataset(session=session, date=DATE, component="hitters", page_size=2, page_number=1)
        second = query_dashboard_dataset(session=session, date=DATE, component="hitters", page_size=2, page_number=2)
        assert {row["entity_id"] for row in first["items"]}.isdisjoint({row["entity_id"] for row in second["items"]})
        assert first["totalSize"] == second["totalSize"] == 4


def test_metric_filter_and_metric_sort_are_sql_backed():
    Session = session_factory()
    with Session() as session:
        hydrate(session)
        result = query_dashboard_dataset(
            session=session,
            date=DATE,
            component="hitters",
            filters={"metrics": {"EV": {"min": 90}}},
            sort_by="metrics.EV",
            sort_direction="asc",
        )
        assert result["totalSize"] == 2
        assert [row["entity_name"] for row in result["items"]] == ["Bravo Hitter", "Alpha Hitter"]


def test_confidence_and_score_filters_work_together():
    Session = session_factory()
    with Session() as session:
        hydrate(session)
        result = query_dashboard_dataset(
            session=session,
            date=DATE,
            component="hitters",
            filters={"min_confidence": "medium", "min_score": 0.8},
        )
        assert [row["entity_id"] for row in result["items"]] == ["1", "2"]


def test_sql_weight_formula_runs_before_sort_and_pagination():
    Session = session_factory()
    weighted_rows = [
        {
            "entity_id": "slow",
            "entity_name": "Slow EV",
            "entity_type": "hitter",
            "player_type": "hitter",
            "score": 0.80,
            "base_score": 0.80,
            "confidence": "high",
            "metrics": {"EV": 76.0},
        },
        {
            "entity_id": "fast",
            "entity_name": "Fast EV",
            "entity_type": "hitter",
            "player_type": "hitter",
            "score": 0.70,
            "base_score": 0.70,
            "confidence": "high",
            "metrics": {"EV": 100.0},
        },
    ]
    with Session() as session:
        hydrate(session, weighted_rows)
        result = query_dashboard_dataset(
            session=session,
            date=DATE,
            component="hitters",
            filters={"weights": {"EV": 2.0}},
            page_size=1,
            page_number=1,
            sort_by="score",
            sort_direction="desc",
        )
        assert result["totalSize"] == 2
        assert result["items"][0]["entity_id"] == "fast"
        assert result["items"][0]["base_score"] == pytest.approx(0.70)
        assert result["items"][0]["adjusted_score"] == pytest.approx(0.95)
        assert result["items"][0]["score"] == pytest.approx(0.95)
        assert result["items"][0]["weight_explanation"] == ["EV emphasized at 2.0"]
        assert result["weight_ranking"]["enabled"] is True
        assert result["weight_ranking"]["persisted"] is False


def test_unsupported_weight_metric_warns_without_changing_score():
    Session = session_factory()
    with Session() as session:
        hydrate(session)
        result = query_dashboard_dataset(
            session=session,
            date=DATE,
            component="hitters",
            filters={"team": "CHC", "weights": {"Not A Metric": 2.0}},
        )
        assert result["items"][0]["score"] == pytest.approx(0.91)
        assert "Unsupported weight metric: Not A Metric" in result["filter_warnings"]
        assert result["weight_ranking"]["enabled"] is False


def test_invalid_sort_field_is_rejected():
    Session = session_factory()
    with Session() as session:
        hydrate(session)
        with pytest.raises(ValueError, match="Unsupported sort field"):
            query_dashboard_dataset(
                session=session,
                date=DATE,
                component="hitters",
                sort_by="drop table my_dashboard_records",
            )


def test_standard_and_active_lineup_modes_are_isolated():
    Session = session_factory()
    with Session() as session:
        hydrate(session)
        hydrate_dashboard_dataset(
            session=session,
            date=DATE,
            component="hitters",
            active_lineups=True,
            payload_builder=lambda: {"items": [dict(rows()[0], lineup_verified=True)], "lineup_revision": "rev-1", "model_state": "confirmed"},
        )
        standard = query_dashboard_dataset(session=session, date=DATE, component="hitters")
        active = query_dashboard_dataset(session=session, date=DATE, component="hitters", active_lineups=True)
        assert standard["totalSize"] == 4
        assert active["totalSize"] == 1
        assert active["lineup_revision"] == "rev-1"
        assert active["model_state"] == "confirmed"
