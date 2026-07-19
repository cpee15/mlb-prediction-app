from copy import deepcopy
import json

from mlb_app.simulation.box_score import (
    ReducedBoxScore,
    TeamBoxScore,
)
from mlb_app.simulation.projections import (
    aggregate_projection_payload,
)
from mlb_app.simulation.shadow import (
    attach_canonical_shadow,
    compare_shadow_payloads,
)


def canonical_payload():
    return aggregate_projection_payload(
        box_scores=(
            ReducedBoxScore(
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
            ),
            ReducedBoxScore(
                away=TeamBoxScore(
                    team_side="away",
                    runs=5,
                    hits=9,
                ),
                home=TeamBoxScore(
                    team_side="home",
                    runs=2,
                    hits=6,
                ),
                pitcher_attribution_complete=False,
            ),
        ),
        model_version="canonical_event_model_v1",
        replay_validation_passes=(True, True),
    )


def legacy_result():
    return {
        "model_version": "full_game_sim_v1",
        "simulations": 2,
        "away_expected_runs": 3.5,
        "home_expected_runs": 3.5,
        "total_expected_runs": 7.0,
        "home_win_probability": 0.55,
        "away_run_distribution": {
            "2": 0.5,
            "5": 0.5,
        },
        "home_run_distribution": {
            "1": 0.5,
            "4": 0.5,
        },
        "total_run_distribution": {
            "6": 0.5,
            "8": 0.5,
        },
        "metadata": {
            "simulation_count": 2,
        },
    }


def comparison(diagnostics, name):
    return next(
        item
        for item in diagnostics.comparisons
        if item.name == name
    )


def range_comparison(diagnostics, name):
    return next(
        item
        for item in diagnostics.ranges
        if item.name == name
    )


def test_comparator_reports_run_mean_differences():
    diagnostics = compare_shadow_payloads(
        legacy_result=legacy_result(),
        canonical_payload=canonical_payload(),
    )

    away = comparison(
        diagnostics,
        "away_runs_mean",
    )
    home = comparison(
        diagnostics,
        "home_runs_mean",
    )
    total = comparison(
        diagnostics,
        "total_runs_mean",
    )

    assert diagnostics.status == "partial"
    assert away.canonical_value == 4.0
    assert away.absolute_difference == 0.5
    assert home.canonical_value == 3.0
    assert home.absolute_difference == 0.5
    assert total.canonical_value == 7.0
    assert total.absolute_difference == 0.0


def test_comparator_reports_distribution_ranges():
    diagnostics = compare_shadow_payloads(
        legacy_result=legacy_result(),
        canonical_payload=canonical_payload(),
    )

    away = range_comparison(
        diagnostics,
        "away_runs_range",
    )
    total = range_comparison(
        diagnostics,
        "total_runs_range",
    )

    assert away.legacy_minimum == 2.0
    assert away.legacy_maximum == 5.0
    assert away.canonical_minimum == 3.0
    assert away.canonical_maximum == 5.0
    assert total.canonical_minimum == 5.0
    assert total.canonical_maximum == 9.0


def test_missing_canonical_home_win_is_explicit():
    diagnostics = compare_shadow_payloads(
        legacy_result=legacy_result(),
        canonical_payload=canonical_payload(),
    )

    home_win = comparison(
        diagnostics,
        "home_win_probability",
    )

    assert home_win.available is False
    assert (
        "comparison_unavailable:"
        "home_win_probability"
        in diagnostics.warnings
    )


def test_quality_diagnostics_are_preserved():
    diagnostics = compare_shadow_payloads(
        legacy_result=legacy_result(),
        canonical_payload=canonical_payload(),
    )

    assert (
        diagnostics
        .pitcher_attribution_complete_rate
        == 0.5
    )
    assert (
        diagnostics.replay_validation_pass_rate
        == 1.0
    )
    assert (
        diagnostics.earned_run_status
        == "not_reconstructed"
    )


def test_disabled_shadow_preserves_authoritative_result():
    legacy = legacy_result()
    original = deepcopy(legacy)

    attached = attach_canonical_shadow(
        legacy_result=legacy,
        enabled=False,
    )

    assert legacy == original
    assert attached is not legacy
    assert attached["model_version"] == (
        original["model_version"]
    )
    assert attached["away_expected_runs"] == (
        original["away_expected_runs"]
    )
    assert (
        attached["diagnostics"]
        ["canonical_shadow"]["status"]
        == "disabled"
    )


def test_missing_payload_is_fail_open():
    attached = attach_canonical_shadow(
        legacy_result=legacy_result(),
        enabled=True,
        canonical_payload=None,
    )

    shadow = attached["diagnostics"][
        "canonical_shadow"
    ]

    assert shadow["status"] == "unavailable"
    assert shadow["authoritative_source"] == "legacy"


def test_invalid_payload_is_fail_open_error():
    legacy = legacy_result()

    attached = attach_canonical_shadow(
        legacy_result=legacy,
        enabled=True,
        canonical_payload={"invalid": True},
    )

    shadow = attached["diagnostics"][
        "canonical_shadow"
    ]

    assert shadow["status"] == "error"
    assert shadow["error_type"] is not None
    assert attached["away_expected_runs"] == (
        legacy["away_expected_runs"]
    )


def test_nested_shared_simulation_is_supported():
    nested = {
        "derived_outputs": {
            "bullpen_adjusted_game_simulation": (
                legacy_result()
            ),
        },
        "diagnostics": {
            "sources": ["legacy"],
        },
    }

    attached = attach_canonical_shadow(
        legacy_result=nested,
        enabled=True,
        canonical_payload=canonical_payload(),
    )

    assert (
        attached["diagnostics"]["sources"]
        == ["legacy"]
    )
    assert (
        attached["diagnostics"]
        ["canonical_shadow"]["status"]
        == "partial"
    )


def test_output_is_json_serializable():
    attached = attach_canonical_shadow(
        legacy_result=legacy_result(),
        enabled=True,
        canonical_payload=canonical_payload(),
    )

    encoded = json.dumps(
        attached,
        sort_keys=True,
    )

    assert "canonical_shadow_v1" in encoded
