from __future__ import annotations

from mlb_app.simulation.shadow import (
    build_canonical_shadow_bootstrap_readiness,
    discover_canonical_shadow_probability_provider,
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
    model = {
        "model_version": "pa_outcome_v1",
        "probabilities": probabilities(),
    }

    return {
        "awayPAOutcomeModel": dict(model),
        "homePAOutcomeModel": dict(model),
        "awayVsHomeBullpenPAOutcomeModel": dict(model),
        "homeVsAwayBullpenPAOutcomeModel": dict(model),
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


def test_discovered_provider_advances_readiness_to_eight():
    workspace = workspace_models()

    discovery = (
        discover_canonical_shadow_probability_provider(
            workspace=workspace,
        )
    )

    readiness_workspace = dict(workspace)
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
        "probability_provider"
    ]["ready"] is True

    assert report["missing_requirements"] == [
        "exact_probability_artifact",
        "fallback_probability_catalog",
    ]


def test_invalid_provider_models_keep_requirement_blocked():
    workspace = workspace_models()
    workspace[
        "awayPAOutcomeModel"
    ]["probabilities"] = {}

    discovery = (
        discover_canonical_shadow_probability_provider(
            workspace=workspace,
        )
    )

    readiness_workspace = dict(workspace)
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
        "probability_provider"
    ]["ready"] is False
