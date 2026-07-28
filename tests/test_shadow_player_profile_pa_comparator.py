import copy
import datetime as dt

from mlb_app.simulation.shadow.player_profile_blend import (
    build_shadow_hitter_profile_blend,
    compare_shadow_hitter_pa_outcomes,
)


def ready_blend():
    return build_shadow_hitter_profile_blend(
        player_id=7,
        season=2026,
        split="vsR",
        player_splits=[
            {
                "player_id": 7,
                "season": 2026,
                "split": "vsR",
                "pa": 100,
                "hits": 30,
                "doubles": 7,
                "triples": 1,
                "home_runs": 5,
                "walks": 12,
                "strikeouts": 18,
                "batting_avg": 0.341,
                "slugging_pct": 0.550,
            },
            {
                "player_id": 7,
                "season": 2025,
                "split": "vsR",
                "pa": 300,
                "hits": 75,
                "doubles": 15,
                "triples": 2,
                "home_runs": 12,
                "walks": 24,
                "strikeouts": 66,
                "batting_avg": 0.272,
                "slugging_pct": 0.430,
            },
        ],
        batter_aggregate={
            "end_date": "2026-07-25",
            "hard_hit_pct": 0.44,
            "barrel_pct": 0.11,
        },
        as_of_date=dt.date(2026, 7, 27),
    )


def test_paired_comparison_preserves_production_and_normalizes_both_sides():
    production_profile = {
        "contact_skill": {"k_rate": 0.25, "contact_rate": 0.72},
        "plate_discipline": {"bb_rate": 0.08},
        "power": {
            "iso": 0.150,
            "barrel_rate": 0.07,
            "hard_hit_rate": 0.36,
        },
    }
    original = copy.deepcopy(production_profile)

    result = compare_shadow_hitter_pa_outcomes(
        blend=ready_blend(),
        production_batter_profile=production_profile,
        pitcher_profile={
            "bat_missing": {"k_rate": 0.22},
            "command_control": {"bb_rate": 0.08},
            "contact_management": {
                "barrel_rate_allowed": 0.08,
                "hard_hit_rate_allowed": 0.38,
                "xba_allowed": 0.250,
            },
        },
        environment_profile={
            "run_environment": {
                "hr_boost_index": 1.0,
                "hit_boost_index": 1.0,
                "run_scoring_index": 1.0,
            },
        },
    )

    assert result["status"] == "ready"
    assert result["shadow_only"] is True
    assert result["production_authority_changed"] is False
    assert production_profile == original
    assert round(sum(result["production_probabilities"].values()), 4) == 1.0
    assert abs(sum(result["shadow_probabilities"].values()) - 1.0) <= 0.0005
    assert result["maximum_absolute_probability_delta"] > 0
    assert set(result["probability_deltas"]) == set(
        result["production_probabilities"]
    )


def test_stale_contact_metrics_are_not_inserted_into_candidate_profile():
    blend = ready_blend()
    blend["warnings"].append("stale_batter_contact_aggregate")

    result = compare_shadow_hitter_pa_outcomes(
        blend=blend,
        production_batter_profile={},
        pitcher_profile={},
        environment_profile={},
    )

    power = result["candidate_profile"]["power"]
    assert power["barrel_rate"] is None
    assert power["hard_hit_rate"] is None
    assert power["iso"] is not None


def test_blocked_blend_does_not_run_a_shadow_comparison():
    result = compare_shadow_hitter_pa_outcomes(
        blend={"status": "blocked"},
        production_batter_profile={},
        pitcher_profile={},
        environment_profile={},
    )

    assert result["status"] == "blocked"
    assert result["production_authority_changed"] is False
    assert "production_probabilities" not in result
