from __future__ import annotations

from mlb_app.simulation.game import (
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.shadow import (
    build_canonical_shadow_bootstrap_readiness,
    discover_canonical_shadow_exact_artifact,
)


PROVIDER = CanonicalProbabilityProviderIdentity(
    provider_name="model_projections_pa_outcome",
    provider_version="pa_outcome_v1",
)


def side_context(prefix, pitcher_id):
    return {
        "pitcher_id": pitcher_id,
        "offense_inputs": {
            "lineup": [
                {
                    "batter_id": f"{prefix}{index}",
                    "has_player_split": True,
                    "has_batter_aggregate": False,
                    "simulation_inputs": {
                        "k_pct": 0.22,
                        "bb_pct": 0.09,
                        "batting_avg": 0.255,
                        "on_base_pct": 0.330,
                        "slugging_pct": 0.430,
                        "iso": 0.175,
                        "hard_hit_pct": 0.40,
                        "barrel_pct": 0.08,
                    },
                }
                for index in range(1, 10)
            ]
        },
    }


def probability_workspace():
    pitcher_profile = {
        "bat_missing": {"k_rate": 0.24},
        "command_control": {"bb_rate": 0.08},
        "contact_management": {
            "barrel_rate_allowed": 0.07,
            "hard_hit_rate_allowed": 0.38,
            "xba_allowed": 0.245,
        },
    }

    return {
        "awayPitcherProfile": pitcher_profile,
        "homePitcherProfile": pitcher_profile,
        "environmentProfile": {
            "run_environment": {
                "hr_boost_index": 1.0,
                "hit_boost_index": 1.0,
                "run_scoring_index": 1.0,
            }
        },
    }


def ready_matchup():
    return {
        "game_pk": 123,
        "away_pitcher_id": 100,
        "home_pitcher_id": 200,
        "away_lineup": [
            {"player_id": f"1{index}"}
            for index in range(1, 10)
        ],
        "home_lineup": [
            {"player_id": f"2{index}"}
            for index in range(1, 10)
        ],
        "away_bullpen_pitcher_ids": [
            {"pitcher_id": 101},
        ],
        "home_bullpen_pitcher_ids": [
            {"pitcher_id": 201},
        ],
    }


def base_readiness_workspace():
    return {
        "canonicalProbabilityProvider": {
            "identity": PROVIDER.identity,
        },
        "canonicalProbabilityFallbackCatalog": {
            "digest": "a" * 64,
        },
    }


def test_exact_artifact_advances_readiness_to_ten():
    away = side_context("1", 100)
    home = side_context("2", 200)
    workspace = probability_workspace()

    discovery = discover_canonical_shadow_exact_artifact(
        away_context=away,
        home_context=home,
        workspace=workspace,
        provider=PROVIDER,
    )

    readiness_workspace = (
        base_readiness_workspace()
    )
    readiness_workspace.update(
        discovery.readiness_workspace_fields()
    )

    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=ready_matchup(),
        away_context=away,
        home_context=home,
        workspace=readiness_workspace,
    )

    assert report["requirements"][
        "exact_probability_artifact"
    ]["ready"] is True
    assert report["missing_requirements"] == []
    assert report["ready"] is True
    assert report["status"] == "ready"

    # Full input readiness still does not authorize execution.
    assert report["activation_permitted"] is False
    assert report["activation_status"] == (
        "diagnostic_only"
    )
    assert report["authoritative_source"] == (
        "legacy"
    )


def test_incomplete_exact_coverage_keeps_final_blocker():
    away = side_context("1", 100)

    for index in range(3):
        away["offense_inputs"]["lineup"][index][
            "has_player_split"
        ] = False

    home = side_context("2", 200)
    workspace = probability_workspace()

    discovery = discover_canonical_shadow_exact_artifact(
        away_context=away,
        home_context=home,
        workspace=workspace,
        provider=PROVIDER,
    )

    readiness_workspace = (
        base_readiness_workspace()
    )
    readiness_workspace.update(
        discovery.readiness_workspace_fields()
    )

    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=ready_matchup(),
        away_context=away,
        home_context=home,
        workspace=readiness_workspace,
    )

    assert report["requirements"][
        "exact_probability_artifact"
    ]["ready"] is False
    assert report["missing_requirements"] == [
        "exact_probability_artifact",
    ]
