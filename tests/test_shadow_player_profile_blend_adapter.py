import datetime as dt

from sqlalchemy import create_engine

from mlb_app.database import (
    Base,
    BatterAggregate,
    PlayerSplit,
    get_session,
)
from mlb_app.simulation.shadow.player_profile_blend import (
    load_shadow_hitter_profile_blend,
)


def split_row(season, split="vsR"):
    return PlayerSplit(
        player_id=7,
        season=season,
        split=split,
        pa=100,
        hits=25,
        doubles=5,
        triples=1,
        home_runs=4,
        walks=10,
        strikeouts=20,
        batting_avg=0.278,
        on_base_pct=0.350,
        slugging_pct=0.450,
        iso=0.172,
        k_pct=0.200,
        bb_pct=0.100,
    )


def session_factory():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return get_session(engine)


def test_adapter_loads_matching_disjoint_windows_and_latest_past_aggregate():
    Session = session_factory()
    with Session() as session:
        session.add_all(
            [
                split_row(2026),
                split_row(2025),
                split_row(2024),
                split_row(2023),
                split_row(2026, split="vsL"),
                BatterAggregate(
                    batter_id=7,
                    window="90d",
                    end_date=dt.date(2026, 7, 20),
                    hard_hit_pct=0.410,
                    barrel_pct=0.090,
                ),
                BatterAggregate(
                    batter_id=7,
                    window="90d",
                    end_date=dt.date(2026, 8, 1),
                    hard_hit_pct=0.999,
                ),
            ]
        )
        session.commit()

        result = load_shadow_hitter_profile_blend(
            session,
            player_id=7,
            season=2026,
            split="vsR",
            as_of_date=dt.date(2026, 7, 27),
        )

    assert result["status"] == "ready"
    assert result["storage_evidence"] == {
        "player_split_row_count": 4,
        "player_split_seasons": [2023, 2024, 2025, 2026],
        "batter_aggregate_found": True,
        "batter_aggregate_window": "90d",
    }
    assert result["contact_quality_context"]["end_date"] == "2026-07-20"
    assert result["contact_quality_context"]["hard_hit_pct"] == 0.410
    assert result["windows"]["career_pre_prior"]["seasons"] == [2023, 2024]


def test_adapter_reports_missing_persisted_evidence_without_fabrication():
    Session = session_factory()
    with Session() as session:
        result = load_shadow_hitter_profile_blend(
            session,
            player_id=99,
            season=2026,
            split="vsR",
            as_of_date=dt.date(2026, 7, 27),
        )

    assert result["status"] == "blocked"
    assert result["storage_evidence"]["player_split_row_count"] == 0
    assert result["storage_evidence"]["batter_aggregate_found"] is False
    assert "missing_current_season_split" in result["blockers"]
    assert "missing_batter_contact_aggregate" in result["warnings"]
