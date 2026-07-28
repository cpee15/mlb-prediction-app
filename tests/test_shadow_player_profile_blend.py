import datetime as dt

from mlb_app.simulation.shadow.player_profile_blend import (
    SHADOW_PLAYER_PROFILE_BLEND_VERSION,
    build_shadow_hitter_profile_blend,
)


def row(season, pa, hits, doubles, triples, home_runs, walks, strikeouts, split="vsR"):
    return {
        "player_id": 7,
        "season": season,
        "split": split,
        "pa": pa,
        "hits": hits,
        "doubles": doubles,
        "triples": triples,
        "home_runs": home_runs,
        "walks": walks,
        "strikeouts": strikeouts,
        "batting_avg": hits / max(pa - walks, 1),
        "slugging_pct": 0.450,
    }


def test_builds_disjoint_sample_aware_actual_blend():
    result = build_shadow_hitter_profile_blend(
        player_id=7,
        season=2026,
        split="vsR",
        player_splits=[
            row(2026, 100, 25, 5, 1, 4, 10, 20),
            row(2025, 300, 80, 18, 2, 15, 30, 60),
            row(2024, 250, 65, 15, 1, 12, 25, 55),
            row(2023, 150, 38, 8, 1, 7, 15, 35),
            row(2026, 999, 999, 0, 0, 0, 0, 0, split="vsL"),
        ],
        batter_aggregate={
            "end_date": "2026-07-25",
            "hard_hit_pct": 0.41,
            "barrel_pct": 0.09,
        },
        as_of_date=dt.date(2026, 7, 27),
    )

    assert result["schema_version"] == SHADOW_PLAYER_PROFILE_BLEND_VERSION
    assert result["status"] == "ready"
    assert result["shadow_only"] is True
    assert result["production_authority_changed"] is False
    assert result["windows"]["current_season"]["seasons"] == [2026]
    assert result["windows"]["prior_season"]["seasons"] == [2025]
    assert result["windows"]["career_pre_prior"]["seasons"] == [2023, 2024]
    assert result["windows"]["career_pre_prior"]["pa"] == 400
    assert round(sum(
        window["normalized_weight"]
        for window in result["windows"].values()
    ), 5) == 1.0
    assert result["blended_actual_metrics"]["k_pct"] is not None
    assert result["expected_component_adjustment"]["applied"] is False
    assert (
        result["expected_component_adjustment"]["status"]
        == "unsupported_source_schema"
    )
    assert "unsupported_expected_components" in result["warnings"]


def test_blocks_when_current_or_multiple_windows_are_missing():
    result = build_shadow_hitter_profile_blend(
        player_id=7,
        season=2026,
        split="vsR",
        player_splits=[row(2025, 100, 25, 5, 1, 4, 10, 20)],
        as_of_date=dt.date(2026, 7, 27),
    )

    assert result["status"] == "blocked"
    assert "missing_current_season_split" in result["blockers"]
    assert "insufficient_disjoint_windows" in result["blockers"]


def test_reports_stale_contact_context_without_using_it_as_expected_data():
    result = build_shadow_hitter_profile_blend(
        player_id=7,
        season=2026,
        split="vsR",
        player_splits=[
            row(2026, 100, 25, 5, 1, 4, 10, 20),
            row(2025, 100, 25, 5, 1, 4, 10, 20),
        ],
        batter_aggregate={
            "end_date": "2026-05-03",
            "hard_hit_pct": 0.40,
        },
        as_of_date=dt.date(2026, 7, 27),
    )

    assert result["contact_quality_context"]["age_days"] == 85
    assert "stale_batter_contact_aggregate" in result["warnings"]
    assert result["expected_component_adjustment"]["applied"] is False


def test_expected_fields_are_reported_but_never_applied_in_v1():
    result = build_shadow_hitter_profile_blend(
        player_id=7,
        season=2026,
        split="vsR",
        player_splits=[
            row(2026, 100, 25, 5, 1, 4, 10, 20),
            row(2025, 100, 25, 5, 1, 4, 10, 20),
        ],
        batter_aggregate={
            "end_date": "2026-07-27",
            "xwoba": 0.350,
            "xba": 0.270,
        },
        as_of_date=dt.date(2026, 7, 27),
    )

    expected = result["expected_component_adjustment"]
    assert expected["status"] == "available"
    assert expected["fields"] == {"xwoba": 0.350, "xba": 0.270}
    assert expected["applied"] is False
