from __future__ import annotations

from mlb_app.simulation.game import (
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.shadow import (
    build_canonical_shadow_bootstrap_readiness,
    discover_canonical_shadow_fallback_catalog,
)


PROVIDER = CanonicalProbabilityProviderIdentity(
    provider_name="model_projections_pa_outcome",
    provider_version="pa_outcome_v1",
)


def probabilities():
    return {
        "k": 0.225,
        "bb": 0.085,
        "hbp": 0.011,
        "single": 0.145,
        "double": 0.045,
        "triple": 0.004,
        "hr": 0.030,
        "reached_on_error": 0.007,
        "out": 0.448,
    }


def workspace_models():
    return {
        key: {
            "model_version": "pa_outcome_v1",
            "probabilities": probabilities(),
        }
        for key in (
            "awayPAOutcomeModel",
            "homePAOutcomeModel",
            "awayVsHomeBullpenPAOutcomeModel",
            "homeVsAwayBullpenPAOutcomeModel",
        )
    }


def ready_matchup():
    return {
        "game_pk": 123,
        "away_pitcher_id": 100,
        "home_pitcher_id": 200,
        "away_lineup": [
            {"player_id": f"a{index}"}
            for index in range(9)
        ],
        "home_lineup": [
            {"player_id": f"h{index}"}
            for index in range(9)
        ],
        "away_bullpen_pitcher_ids": [
            {"pitcher_id": 101},
        ],
        "home_bullpen_pitcher_ids": [
            {"pitcher_id": 201},
        ],
    }


def test_fallback_catalog_advances_readiness_to_nine():
    workspace = workspace_models()

    discovery = (
        discover_canonical_shadow_fallback_catalog(
            workspace=workspace,
            provider=PROVIDER,
        )
    )

    readiness_workspace = dict(workspace)
    readiness_workspace.update({
        "canonicalProbabilityProvider": {
            "identity": PROVIDER.identity,
        }
    })
    readiness_workspace.update(
        discovery.readiness_workspace_fields()
    )

    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=ready_matchup(),
        away_context={},
        home_context={},
        workspace=readiness_workspace,
    )

    assert report["requirements"][
        "fallback_probability_catalog"
    ]["ready"] is True

    assert report["missing_requirements"] == [
        "exact_probability_artifact",
    ]


def test_invalid_catalog_source_keeps_requirement_blocked():
    workspace = workspace_models()
    workspace[
        "homePAOutcomeModel"
    ]["probabilities"] = {}

    discovery = (
        discover_canonical_shadow_fallback_catalog(
            workspace=workspace,
            provider=PROVIDER,
        )
    )

    readiness_workspace = dict(workspace)
    readiness_workspace.update({
        "canonicalProbabilityProvider": {
            "identity": PROVIDER.identity,
        }
    })
    readiness_workspace.update(
        discovery.readiness_workspace_fields()
    )

    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=ready_matchup(),
        away_context={},
        home_context={},
        workspace=readiness_workspace,
    )

    assert report["requirements"][
        "fallback_probability_catalog"
    ]["ready"] is False
