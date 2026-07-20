import json

from mlb_app.simulation import (
    game_simulation_builder as builder,
)
from mlb_app.simulation.box_score import (
    ReducedBoxScore,
    TeamBoxScore,
)
from mlb_app.simulation.game import (
    CanonicalGameOutcomeProjection,
    CanonicalTrialBatch,
    CanonicalTrialDiagnostics,
    DistributionPoint,
    ProbabilityMetric,
)
from mlb_app.simulation.projections import (
    aggregate_projection_payload,
)
from mlb_app.simulation.shadow import (
    canonical_trial_batch_to_shadow_payload,
    compare_shadow_payloads,
)


def reduced_box_score():
    return ReducedBoxScore(
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


def trial_batch():
    box_score = reduced_box_score()

    projections = aggregate_projection_payload(
        box_scores=(box_score,),
        model_version="canonical_trial_shadow_v1",
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
            box_score_reconciliation_pass_rate=(
                1.0
            ),
        ),
    )


def legacy_result():
    return {
        "model_version": "legacy_test_v1",
        "simulations": 1,
        "away_expected_runs": 3.0,
        "home_expected_runs": 4.0,
        "total_expected_runs": 7.0,
        "home_win_probability": 1.0,
        "away_run_distribution": {
            "3": 1.0,
        },
        "home_run_distribution": {
            "4": 1.0,
        },
        "total_run_distribution": {
            "7": 1.0,
        },
        "metadata": {
            "simulation_count": 1,
        },
    }


def comparison(diagnostics, name):
    return next(
        item
        for item in diagnostics.comparisons
        if item.name == name
    )


def test_trial_batch_adapts_to_shadow_payload():
    payload = (
        canonical_trial_batch_to_shadow_payload(
            trial_batch()
        )
    )

    assert payload["simulation_count"] == 1
    assert (
        payload["outcomes"]
        ["home_win_probability"]
        == 1.0
    )
    assert (
        payload["outcomes"]
        ["away_run_distribution"]
        == {"3": 1.0}
    )
    assert (
        payload["trial_diagnostics"]
        ["game_validation_pass_rate"]
        == 1.0
    )
    assert (
        payload["shadow_metadata"]
        ["authoritative_source"]
        == "legacy"
    )


def test_trial_batch_enables_home_win_comparison():
    payload = (
        canonical_trial_batch_to_shadow_payload(
            trial_batch()
        )
    )

    diagnostics = compare_shadow_payloads(
        legacy_result=legacy_result(),
        canonical_payload=payload,
    )

    home_win = comparison(
        diagnostics,
        "home_win_probability",
    )

    assert home_win.available is True
    assert home_win.canonical_value == 1.0
    assert home_win.absolute_difference == 0.0
    assert diagnostics.status == "complete"


def test_trial_batch_shadow_payload_is_json_safe():
    payload = (
        canonical_trial_batch_to_shadow_payload(
            trial_batch()
        )
    )

    encoded = json.dumps(
        payload,
        sort_keys=True,
    )

    assert "canonical_trial_batch" in encoded
    assert "home_win_probability" in encoded


def test_builder_accepts_trial_batch_without_changing_legacy(
    monkeypatch,
):
    observed = {}

    def engine(game_pk, config):
        observed["game_pk"] = game_pk
        observed["config"] = config
        return legacy_result()

    monkeypatch.setattr(
        builder,
        "_load_sandbox_engine",
        lambda: engine,
    )

    result = builder.build_game_simulation(
        123,
        {
            "canonical_shadow_enabled": True,
            "canonical_shadow_trial_batch": (
                trial_batch()
            ),
        },
    )

    assert observed["game_pk"] == 123
    assert (
        "canonical_shadow_trial_batch"
        not in observed["config"]
    )
    assert result["away_expected_runs"] == 3.0
    assert result["home_expected_runs"] == 4.0
    assert (
        result["diagnostics"]
        ["canonical_shadow"]["status"]
        == "complete"
    )
    assert (
        result["diagnostics"]
        ["canonical_shadow"]
        ["authoritative_source"]
        == "legacy"
    )


def test_builder_runs_injected_trial_factory(monkeypatch):
    observed = {
        "factory_calls": 0,
    }

    def engine(game_pk, config):
        observed["engine_game_pk"] = game_pk
        observed["engine_config"] = config
        return legacy_result()

    def factory(*, game_pk, config):
        observed["factory_calls"] += 1
        observed["factory_game_pk"] = game_pk
        observed["factory_config"] = config
        return trial_batch()

    monkeypatch.setattr(
        builder,
        "_load_sandbox_engine",
        lambda: engine,
    )

    result = builder.build_game_simulation(
        456,
        {
            "simulation_count": 1,
            "seed": 12345,
            "canonical_shadow_enabled": True,
        },
        canonical_shadow_trial_batch_factory=factory,
    )

    assert observed["factory_calls"] == 1
    assert observed["factory_game_pk"] == 456
    assert observed["factory_config"] == {
        "simulation_count": 1,
        "seed": 12345,
    }
    assert observed["engine_config"] == {
        "simulation_count": 1,
        "seed": 12345,
    }
    assert result["away_expected_runs"] == 3.0
    assert result["home_expected_runs"] == 4.0
    assert (
        result["diagnostics"]
        ["canonical_shadow"]["status"]
        == "complete"
    )
    assert (
        result["diagnostics"]
        ["canonical_shadow"]
        ["authoritative_source"]
        == "legacy"
    )


def test_disabled_shadow_does_not_run_injected_factory(
    monkeypatch,
):
    observed = {
        "factory_calls": 0,
    }

    def engine(game_pk, config):
        return legacy_result()

    def factory(*, game_pk, config):
        observed["factory_calls"] += 1
        return trial_batch()

    monkeypatch.setattr(
        builder,
        "_load_sandbox_engine",
        lambda: engine,
    )

    result = builder.build_game_simulation(
        456,
        {
            "canonical_shadow_enabled": False,
        },
        canonical_shadow_trial_batch_factory=factory,
    )

    assert observed["factory_calls"] == 0
    assert (
        result["diagnostics"]
        ["canonical_shadow"]["status"]
        == "disabled"
    )
    assert result["away_expected_runs"] == 3.0


def test_explicit_payload_precedes_injected_factory(
    monkeypatch,
):
    observed = {
        "factory_calls": 0,
    }

    def engine(game_pk, config):
        return legacy_result()

    def factory(*, game_pk, config):
        observed["factory_calls"] += 1
        raise AssertionError(
            "factory must not run when payload is explicit"
        )

    monkeypatch.setattr(
        builder,
        "_load_sandbox_engine",
        lambda: engine,
    )

    explicit_payload = (
        canonical_trial_batch_to_shadow_payload(
            trial_batch()
        )
    )

    result = builder.build_game_simulation(
        456,
        {
            "canonical_shadow_enabled": True,
            "canonical_shadow_payload": explicit_payload,
        },
        canonical_shadow_trial_batch_factory=factory,
    )

    assert observed["factory_calls"] == 0
    assert (
        result["diagnostics"]
        ["canonical_shadow"]["status"]
        == "complete"
    )


def test_explicit_trial_batch_precedes_injected_factory(
    monkeypatch,
):
    observed = {
        "factory_calls": 0,
    }

    def engine(game_pk, config):
        observed["engine_config"] = config
        return legacy_result()

    def factory(*, game_pk, config):
        observed["factory_calls"] += 1
        raise AssertionError(
            "factory must not run when batch is explicit"
        )

    monkeypatch.setattr(
        builder,
        "_load_sandbox_engine",
        lambda: engine,
    )

    result = builder.build_game_simulation(
        456,
        {
            "simulation_count": 1,
            "canonical_shadow_enabled": True,
            "canonical_shadow_trial_batch": (
                trial_batch()
            ),
        },
        canonical_shadow_trial_batch_factory=factory,
    )

    assert observed["factory_calls"] == 0
    assert observed["engine_config"] == {
        "simulation_count": 1,
    }
    assert (
        result["diagnostics"]
        ["canonical_shadow"]["status"]
        == "complete"
    )


def test_injected_factory_failure_is_fail_open(
    monkeypatch,
):
    def engine(game_pk, config):
        return legacy_result()

    def factory(*, game_pk, config):
        raise RuntimeError(
            "canonical factory test failure"
        )

    monkeypatch.setattr(
        builder,
        "_load_sandbox_engine",
        lambda: engine,
    )

    result = builder.build_game_simulation(
        456,
        {
            "canonical_shadow_enabled": True,
        },
        canonical_shadow_trial_batch_factory=factory,
    )

    shadow = result["diagnostics"][
        "canonical_shadow"
    ]

    assert shadow["status"] == "error"
    assert shadow["authoritative_source"] == "legacy"
    assert shadow["error_type"] == "RuntimeError"
    assert (
        "canonical factory test failure"
        in shadow["error_message"]
    )
    assert result["away_expected_runs"] == 3.0
    assert result["home_expected_runs"] == 4.0
