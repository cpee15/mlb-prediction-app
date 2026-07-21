from __future__ import annotations

from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_BOOTSTRAP_READINESS_VERSION,
    build_canonical_shadow_bootstrap_readiness,
)


def complete_matchup():
    return {
        "game_pk": 123,
        "away_lineup": [
            {"player_id": f"away_{index}"}
            for index in range(9)
        ],
        "home_lineup": [
            {"player_id": f"home_{index}"}
            for index in range(9)
        ],
        "away_pitcher_id": 101,
        "home_pitcher_id": 201,
        "away_bullpen_pitcher_ids": [
            102,
            103,
        ],
        "home_bullpen_pitcher_ids": [
            202,
            203,
        ],
    }


def complete_workspace():
    return {
        "canonicalProbabilityProvider": {
            "provider_name": "test",
        },
        "canonicalExactProbabilityArtifact": {
            "artifact_version": "v1",
        },
        "canonicalProbabilityFallbackCatalog": {
            "catalog_version": "v1",
        },
    }


def test_complete_inputs_report_ready_without_activation():
    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=complete_matchup(),
        away_context={},
        home_context={},
        workspace=complete_workspace(),
    )

    assert report["schema_version"] == (
        CANONICAL_SHADOW_BOOTSTRAP_READINESS_VERSION
    )
    assert report["status"] == "ready"
    assert report["ready"] is True
    assert report["missing_requirements"] == []
    assert report["activation_permitted"] is False
    assert report["authoritative_source"] == "legacy"
    assert report["probability_records_exposed"] is False


def test_current_team_prior_shape_reports_real_blockers():
    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=824410,
        matchup={
            "game_pk": 824410,
            "away_pitcher_id": 101,
            "home_pitcher_id": 201,
        },
        away_context={
            "pitcher_id": 101,
            "offense_inputs": {
                "source": "team_splits",
            },
        },
        home_context={
            "pitcher_id": 201,
            "offense_inputs": {
                "source": "team_splits",
            },
        },
        workspace={
            "awayPAOutcomeModel": {
                "probabilities": {
                    "strikeout": 0.2,
                },
            },
            "homePAOutcomeModel": {
                "probabilities": {
                    "strikeout": 0.2,
                },
            },
        },
    )

    assert report["status"] == "blocked"
    assert report["ready"] is False

    assert report["requirements"][
        "away_starter"
    ]["ready"] is True

    assert report["requirements"][
        "home_starter"
    ]["ready"] is True

    assert report["requirements"][
        "away_lineup"
    ]["player_count"] == 0

    assert report["requirements"][
        "home_lineup"
    ]["player_count"] == 0

    assert report["missing_requirements"] == [
        "away_lineup",
        "home_lineup",
        "away_bullpen",
        "home_bullpen",
        "probability_provider",
        "exact_probability_artifact",
        "fallback_probability_catalog",
    ]


def test_lineup_requires_exactly_nine_unique_ids():
    matchup = complete_matchup()
    matchup["away_lineup"] = [
        {"player_id": "duplicate"}
        for _ in range(9)
    ]

    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=matchup,
        away_context={},
        home_context={},
        workspace=complete_workspace(),
    )

    assert report["requirements"][
        "away_lineup"
    ]["ready"] is False

    assert report["requirements"][
        "away_lineup"
    ]["player_count"] == 1


def test_context_starter_fallback_is_observed():
    matchup = complete_matchup()
    matchup.pop("away_pitcher_id")

    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=matchup,
        away_context={
            "pitcher_id": 999,
        },
        home_context={},
        workspace=complete_workspace(),
    )

    away_starter = report[
        "requirements"
    ]["away_starter"]

    assert away_starter["ready"] is True
    assert away_starter["source"] == (
        "away_context.pitcher_id"
    )


def test_missing_game_identity_is_reported():
    report = build_canonical_shadow_bootstrap_readiness(
        game_pk=None,
        matchup={},
        away_context={},
        home_context={},
        workspace={},
    )

    assert report["status"] == "blocked"
    assert report["requirements"][
        "game_identity"
    ]["ready"] is False

    assert report["missing_requirements"][0] == (
        "game_identity"
    )


def test_input_objects_are_not_mutated():
    matchup = complete_matchup()
    away_context = {
        "pitcher_id": 101,
    }
    home_context = {
        "pitcher_id": 201,
    }
    workspace = complete_workspace()

    matchup_snapshot = repr(matchup)
    away_snapshot = repr(away_context)
    home_snapshot = repr(home_context)
    workspace_snapshot = repr(workspace)

    build_canonical_shadow_bootstrap_readiness(
        game_pk=123,
        matchup=matchup,
        away_context=away_context,
        home_context=home_context,
        workspace=workspace,
    )

    assert repr(matchup) == matchup_snapshot
    assert repr(away_context) == away_snapshot
    assert repr(home_context) == home_snapshot
    assert repr(workspace) == workspace_snapshot
