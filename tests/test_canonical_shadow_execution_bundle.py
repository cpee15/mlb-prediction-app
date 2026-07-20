from __future__ import annotations

from copy import deepcopy

import pytest

import mlb_app.simulation.game_simulation_builder as builder
from mlb_app.simulation.box_score import (
    ReducedBoxScore,
    TeamBoxScore,
)
from mlb_app.simulation.game import (
    CanonicalGameOutcomeProjection,
    CanonicalProbabilityResolutionDiagnosticsCollector,
    CanonicalTrialBatch,
    CanonicalTrialDiagnostics,
    DistributionPoint,
    ProbabilityMetric,
)
from mlb_app.simulation.projections import (
    aggregate_projection_payload,
)
from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_EXECUTION_BUNDLE_VERSION,
    CanonicalShadowExecutionBundle,
    canonical_shadow_execution_bundle_to_material,
)


def trial_batch():
    box_score = ReducedBoxScore(
        away=TeamBoxScore(
            team_side="away",
            runs=3,
            hits=7,
        ),
        home=TeamBoxScore(
            team_side="home",
            runs=4,
            hits=8,
        ),
        pitcher_attribution_complete=True,
    )

    projections = aggregate_projection_payload(
        box_scores=(box_score,),
        model_version="execution-bundle-test-v1",
        replay_validation_passes=(True,),
    )

    outcomes = CanonicalGameOutcomeProjection(
        simulation_count=1,
        away_win_probability=0.0,
        home_win_probability=1.0,
        tie_probability=0.0,
        extra_innings_probability=0.0,
        walk_off_probability=0.0,
        away_run_distribution=(
            DistributionPoint(
                value=3,
                probability=1.0,
            ),
        ),
        home_run_distribution=(
            DistributionPoint(
                value=4,
                probability=1.0,
            ),
        ),
        total_run_distribution=(
            DistributionPoint(
                value=7,
                probability=1.0,
            ),
        ),
        team_total_probabilities=(
            ProbabilityMetric(
                name="away_3_plus",
                probability=1.0,
            ),
            ProbabilityMetric(
                name="home_3_plus",
                probability=1.0,
            ),
        ),
        total_probabilities=(
            ProbabilityMetric(
                name="over_6.5",
                probability=1.0,
            ),
            ProbabilityMetric(
                name="under_6.5",
                probability=0.0,
            ),
        ),
    )

    return CanonicalTrialBatch(
        games=(object(),),
        box_scores=(box_score,),
        reconciliations=(object(),),
        outcomes=outcomes,
        projections=projections,
        diagnostics=CanonicalTrialDiagnostics(
            game_validation_pass_rate=1.0,
            box_score_reconciliation_pass_rate=1.0,
        ),
    )


def diagnostics_snapshot():
    return (
        CanonicalProbabilityResolutionDiagnosticsCollector()
        .snapshot()
    )


def execution_bundle():
    return CanonicalShadowExecutionBundle(
        trial_batch=trial_batch(),
        probability_resolution_diagnostics=(
            diagnostics_snapshot()
        ),
    )


def legacy_result():
    return {
        "model_version": "legacy-test-v1",
        "simulations": 1,
        "away_expected_runs": 3.0,
        "home_expected_runs": 4.0,
        "total_expected_runs": 7.0,
        "home_win_probability": 1.0,
        "away_run_distribution": {"3": 1.0},
        "home_run_distribution": {"4": 1.0},
        "total_run_distribution": {"7": 1.0},
        "metadata": {
            "simulation_count": 1,
        },
    }


def install_engine(monkeypatch, observed):
    def engine(game_pk, config):
        observed["game_pk"] = game_pk
        observed["config"] = deepcopy(config)
        return legacy_result()

    monkeypatch.setattr(
        builder,
        "_load_sandbox_engine",
        lambda: engine,
    )


def test_bundle_validates_component_types():
    with pytest.raises(
        TypeError,
        match="trial_batch",
    ):
        CanonicalShadowExecutionBundle(
            trial_batch="invalid",
            probability_resolution_diagnostics=(
                diagnostics_snapshot()
            ),
        )

    with pytest.raises(
        TypeError,
        match="probability_resolution_diagnostics",
    ):
        CanonicalShadowExecutionBundle(
            trial_batch=trial_batch(),
            probability_resolution_diagnostics="invalid",
        )


def test_bundle_version_is_explicit():
    value = execution_bundle()

    assert value.bundle_version == (
        CANONICAL_SHADOW_EXECUTION_BUNDLE_VERSION
    )


def test_bundle_adapts_payload_and_diagnostics_atomically():
    value = execution_bundle()

    material = (
        canonical_shadow_execution_bundle_to_material(
            value
        )
    )

    assert material.canonical_payload[
        "simulation_count"
    ] == 1
    assert (
        material.probability_resolution_diagnostics
        is value.probability_resolution_diagnostics
    )


def test_explicit_payload_precedes_execution_bundle():
    explicit_payload = {
        "simulation_count": 999,
    }

    payload, diagnostics = (
        builder._canonical_shadow_material(
            {
                "canonical_shadow_payload": (
                    explicit_payload
                ),
                "canonical_shadow_execution_bundle": (
                    execution_bundle()
                ),
            }
        )
    )

    assert payload is explicit_payload
    assert diagnostics is None


def test_execution_bundle_precedes_explicit_trial_batch():
    payload, diagnostics = (
        builder._canonical_shadow_material(
            {
                "canonical_shadow_execution_bundle": (
                    execution_bundle()
                ),
                "canonical_shadow_trial_batch": (
                    trial_batch()
                ),
            }
        )
    )

    assert payload["simulation_count"] == 1
    assert diagnostics == diagnostics_snapshot()


def test_builder_attaches_bundle_atomically(monkeypatch):
    observed = {}
    install_engine(monkeypatch, observed)

    result = builder.build_game_simulation(
        123,
        {
            "canonical_shadow_enabled": True,
            "canonical_shadow_execution_bundle": (
                execution_bundle()
            ),
        },
    )

    shadow = result["diagnostics"]["canonical_shadow"]

    assert shadow["status"] == "complete"
    assert shadow["authoritative_source"] == "legacy"
    assert shadow[
        "probability_resolution"
    ]["summary"]["total_resolutions"] == 0

    assert (
        "canonical_shadow_execution_bundle"
        not in observed["config"]
    )


def test_injected_factory_may_return_execution_bundle(
    monkeypatch,
):
    observed = {}
    install_engine(monkeypatch, observed)
    factory_calls = []

    def factory(*, factory_input):
        factory_calls.append(factory_input)
        return execution_bundle()

    result = builder.build_game_simulation(
        123,
        {
            "canonical_shadow_enabled": True,
            "simulation_count": 1,
            "seed": 12345,
        },
        canonical_shadow_trial_batch_factory=factory,
    )

    shadow = result["diagnostics"]["canonical_shadow"]

    assert len(factory_calls) == 1
    assert shadow["status"] == "complete"
    assert (
        shadow["probability_resolution"]
        ["summary"]["total_resolutions"]
        == 0
    )


def test_invalid_explicit_bundle_fails_open(monkeypatch):
    observed = {}
    install_engine(monkeypatch, observed)

    result = builder.build_game_simulation(
        123,
        {
            "canonical_shadow_enabled": True,
            "canonical_shadow_execution_bundle": (
                "invalid"
            ),
        },
    )

    shadow = result["diagnostics"]["canonical_shadow"]

    assert shadow["status"] == "error"
    assert shadow["error_type"] == "TypeError"
    assert shadow["authoritative_source"] == "legacy"
    assert result["away_expected_runs"] == 3.0
