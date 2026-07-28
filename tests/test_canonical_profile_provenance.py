from copy import deepcopy

from mlb_app.matchup_workspace_builder import (
    build_lineup_pa_outcome_model,
)
from mlb_app.simulation.pa_outcome_model import (
    build_pa_outcome_probabilities,
)
from mlb_app.simulation.profile_provenance import (
    CANONICAL_PROFILE_PROVENANCE_VERSION,
    build_canonical_profile_provenance,
)


def batter_profile():
    return {
        "metadata": {
            "source_type": "player_split",
            "profile_granularity": "individual_hitter",
            "sample_window": "2026:vsR",
            "sample_size": 120,
        },
        "contact_skill": {
            "k_rate": 0.20,
            "contact_rate": 0.78,
        },
        "plate_discipline": {"bb_rate": 0.09},
        "power": {
            "iso": 0.19,
            "barrel_rate": 0.10,
            "hard_hit_rate": 0.42,
        },
    }


def pitcher_profile():
    return {
        "metadata": {
            "source_type": "pitcher_aggregate",
            "profile_granularity": "individual_pitcher",
            "sample_window": "90d",
        },
        "bat_missing": {"k_rate": 0.25},
        "command_control": {"bb_rate": 0.07},
        "contact_management": {
            "barrel_rate_allowed": 0.08,
            "hard_hit_rate_allowed": 0.38,
            "xba_allowed": 0.245,
        },
    }


def environment_profile():
    return {
        "metadata": {
            "source_type": "game_environment",
            "profile_granularity": "game",
        },
        "run_environment": {
            "hr_boost_index": 1.02,
            "hit_boost_index": 0.99,
            "run_scoring_index": 1.01,
        },
    }


def test_version_is_explicit():
    assert CANONICAL_PROFILE_PROVENANCE_VERSION == (
        "canonical_profile_provenance_v1"
    )


def test_normalizer_exposes_missing_metadata():
    result = build_canonical_profile_provenance(
        {"source": "team_splits"},
        role="batter",
        input_values={
            "k_rate": 0.22,
            "xwoba": None,
        },
    )

    assert result["source_type"] == "team_splits"
    assert result["sample_window"] is None
    assert result["sample_size"] is None
    assert result["available_input_fields"] == [
        "k_rate"
    ]
    assert result["missing_input_fields"] == [
        "xwoba"
    ]


def test_provenance_does_not_change_probabilities():
    batter = batter_profile()
    pitcher = pitcher_profile()
    environment = environment_profile()

    baseline = build_pa_outcome_probabilities(
        deepcopy(batter),
        deepcopy(pitcher),
        deepcopy(environment),
    )
    enriched_batter = deepcopy(batter)
    enriched_batter["profile_provenance"] = {
        "source_type": "player_split",
        "sample_window": "2026:vsR",
        "sample_size": 120,
    }
    enriched = build_pa_outcome_probabilities(
        enriched_batter,
        deepcopy(pitcher),
        deepcopy(environment),
    )

    assert enriched["probabilities"] == (
        baseline["probabilities"]
    )
    provenance = enriched["profile_provenance"]
    assert provenance["schema_version"] == (
        CANONICAL_PROFILE_PROVENANCE_VERSION
    )
    assert provenance["batter"]["source_type"] == (
        "player_split"
    )
    assert provenance["pitcher"]["sample_window"] == (
        "90d"
    )


def test_lineup_rows_admit_shared_profile_reuse():
    lineup_profile = batter_profile()
    lineup_profile["profile_granularity"] = (
        "lineup_average"
    )

    result = build_lineup_pa_outcome_model(
        lineup=[
            {"id": 1, "name": "One"},
            {"id": 2, "name": "Two"},
        ],
        lineup_profile=lineup_profile,
        opposing_pitcher_profile=pitcher_profile(),
        environment_profile=environment_profile(),
        side_label="away_offense",
    )

    assert result["profile_granularity"] == (
        "lineup_average"
    )
    assert result["shared_profile_reused"] is True
    assert all(
        row["shared_profile_reused"] is True
        for row in result["player_outcomes"]
    )
    assert all(
        row["profile_provenance"]["batter"][
            "profile_granularity"
        ]
        == "lineup_average"
        for row in result["player_outcomes"]
    )


def test_authoritative_engine_pa_wrapper_exposes_provenance():
    from mlb_app.simulation.game_engine_v2 import _build_pa_model

    result = _build_pa_model(
        offense_profile={
            "rates": {
                "k_rate": 0.22,
                "bb_rate": 0.09,
                "batting_avg": 0.255,
                "iso": 0.175,
                "slugging_pct": 0.430,
            },
            "metadata": {
                "source_type": "team_splits",
                "profile_granularity": "team_offense",
                "team_id": 140,
                "lineup_source": (
                    "team_splits_fallback_not_confirmed_lineup"
                ),
                "sample_window": "season=2026;split=vsR",
                "sample_size": 925,
                "sample_blend_policy": "team_split",
            },
        },
        opposing_pitcher_profile={
            "rates": {
                "k_rate": 0.2133,
                "bb_rate": 0.0872,
                "xba_allowed": 0.250,
                "xwoba_allowed": 0.320,
                "hard_hit_rate_allowed": 0.38,
                "hr_rate": 0.03,
            },
            "metadata": {
                "source_type": (
                    "model_projection_pitcher_features"
                ),
                "profile_granularity": "probable_pitcher",
                "pitcher_id": 677958,
                "sample_window": "season=2026",
                "sample_size": 420,
                "sample_blend_policy": "selected_source_window",
            },
        },
        environment_profile={
            "indices": {
                "run_scoring_index": 1.01,
                "hr_boost_index": 1.02,
                "hit_boost_index": 1.00,
            },
            "metadata": {
                "source_type": "matchup_detail_context",
                "profile_granularity": "game_environment",
            },
        },
        side="away_offense_vs_home_starter",
    )

    assert result["lineup_average_probabilities"]
    provenance = result["profile_provenance"]
    assert provenance["schema_version"] == (
        CANONICAL_PROFILE_PROVENANCE_VERSION
    )
    assert provenance["batter"]["source_type"] == "team_splits"
    assert (
        provenance["batter"]["profile_granularity"]
        == "team_offense"
    )
    assert provenance["batter"]["sample_size"] == 925
    assert provenance["batter"]["shared_profile_reused"] is True
    assert provenance["batter"]["fallback_used"] is True
    assert provenance["pitcher"]["player_id"] == 677958
    assert (
        provenance["pitcher"]["sample_window"]
        == "season=2026"
    )
    assert (
        provenance["environment"]["source_type"]
        == "matchup_detail_context"
    )
