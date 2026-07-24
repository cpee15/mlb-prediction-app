import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_CALIBRATION_COMPARISON_VERSION,
    CanonicalBaserunningShadowSummary,
    CanonicalObservedBaserunningTotals,
    compare_baserunning_shadow_to_observed,
)


def summary(
    *,
    status="ready",
    ready_count=2,
    stolen_bases=3.5,
    caught_stealing=1.0,
    error_message=None,
):
    return CanonicalBaserunningShadowSummary(
        status=status,
        validation_count=ready_count,
        ready_count=ready_count,
        stolen_base_mean_total=stolen_bases,
        caught_stealing_mean_total=caught_stealing,
        error_message=error_message,
    )


def observed(
    *,
    game_count=2,
    stolen_bases=4,
    caught_stealing=1,
):
    return CanonicalObservedBaserunningTotals(
        game_count=game_count,
        stolen_bases=stolen_bases,
        caught_stealing=caught_stealing,
        source_version="statcast_observed_v1",
    )


def test_compares_aligned_shadow_and_observed_totals():
    result = compare_baserunning_shadow_to_observed(
        summary(),
        observed(),
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.game_count == 2
    assert result.projected_stolen_bases == 3.5
    assert result.observed_stolen_bases == 4
    assert result.stolen_base_absolute_error == 0.5
    assert result.projected_caught_stealing == 1.0
    assert result.observed_caught_stealing == 1
    assert result.caught_stealing_absolute_error == 0.0
    assert result.projected_attempts == 4.5
    assert result.observed_attempts == 5
    assert result.attempt_absolute_error == 0.5
    assert result.projected_success_rate == 0.777778
    assert result.observed_success_rate == 0.8
    assert result.success_rate_absolute_error == 0.022222


def test_zero_attempts_have_no_success_rate():
    result = compare_baserunning_shadow_to_observed(
        summary(
            stolen_bases=0.0,
            caught_stealing=0.0,
        ),
        observed(
            stolen_bases=0,
            caught_stealing=0,
        ),
    )

    assert result.status == "ready"
    assert result.projected_success_rate is None
    assert result.observed_success_rate is None
    assert result.success_rate_absolute_error is None


def test_unaligned_game_counts_are_unavailable():
    result = compare_baserunning_shadow_to_observed(
        summary(ready_count=2),
        observed(game_count=3),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.error_message == (
        "observed game_count must match "
        "ready shadow validation count"
    )


def test_unavailable_summary_fails_open():
    result = compare_baserunning_shadow_to_observed(
        CanonicalBaserunningShadowSummary(
            status="unavailable",
            error_message="summary unavailable",
        ),
        observed(),
    )

    assert result.status == "unavailable"
    assert result.error_message == "summary unavailable"


def test_invalid_summary_contract_fails_open():
    result = compare_baserunning_shadow_to_observed(
        object(),
        observed(),
    )

    assert result.status == "error"
    assert result.error_message == (
        "summary must be CanonicalBaserunningShadowSummary"
    )


def test_invalid_observed_contract_fails_open():
    result = compare_baserunning_shadow_to_observed(
        summary(),
        object(),
    )

    assert result.status == "error"
    assert result.error_message == (
        "observed must be "
        "CanonicalObservedBaserunningTotals"
    )


def test_observed_totals_require_positive_game_count():
    with pytest.raises(
        ValueError,
        match="game_count must be positive",
    ):
        observed(game_count=0)


def test_observed_totals_reject_negative_counts():
    with pytest.raises(
        ValueError,
        match="stolen_bases must be nonnegative",
    ):
        observed(stolen_bases=-1)

    with pytest.raises(
        ValueError,
        match="caught_stealing must be nonnegative",
    ):
        observed(caught_stealing=-1)


def test_observed_source_must_be_available():
    with pytest.raises(
        ValueError,
        match=(
            "source_version must identify "
            "an available source"
        ),
    ):
        CanonicalObservedBaserunningTotals(
            game_count=1,
            stolen_bases=1,
            caught_stealing=0,
            source_version="unavailable",
        )


def test_diagnostics_preserve_legacy_authority():
    diagnostics = (
        compare_baserunning_shadow_to_observed(
            summary(),
            observed(),
        ).to_diagnostics()
    )

    assert diagnostics["calibration_approved"] is False
    assert diagnostics["activation_permitted"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_comparison_is_deterministic():
    first = compare_baserunning_shadow_to_observed(
        summary(),
        observed(),
    )
    second = compare_baserunning_shadow_to_observed(
        summary(),
        observed(),
    )

    assert first == second


def test_comparison_version_is_explicit():
    result = compare_baserunning_shadow_to_observed(
        summary(),
        observed(),
    )

    assert result.comparison_version == (
        CANONICAL_BASERUNNING_CALIBRATION_COMPARISON_VERSION
    )
