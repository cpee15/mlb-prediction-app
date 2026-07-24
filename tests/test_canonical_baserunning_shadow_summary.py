from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_SHADOW_SUMMARY_VERSION,
    CanonicalBaserunningOutputValidation,
    summarize_canonical_baserunning_shadow_validations,
)


def validation(
    *,
    status="ready",
    digest="digest-a",
    simulation_count=100,
    runner_count=9,
    stolen_bases=1.25,
    caught_stealing=0.25,
    warnings=(),
):
    return CanonicalBaserunningOutputValidation(
        status=status,
        catalog_digest=digest,
        simulation_count=simulation_count,
        runner_projection_count=runner_count,
        stolen_base_mean_total=stolen_bases,
        caught_stealing_mean_total=caught_stealing,
        warnings=warnings,
    )


def test_summarizes_ready_validations():
    result = (
        summarize_canonical_baserunning_shadow_validations(
            (
                validation(),
                validation(
                    digest="digest-b",
                    simulation_count=200,
                    stolen_bases=2.0,
                    caught_stealing=0.5,
                ),
            )
        )
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.validation_count == 2
    assert result.ready_count == 2
    assert result.simulation_count_total == 300
    assert result.runner_projection_count_total == 18
    assert result.stolen_base_mean_total == 3.25
    assert result.caught_stealing_mean_total == 0.75
    assert result.active_validation_count == 2
    assert result.observed_activity is True
    assert result.catalog_digests == (
        "digest-a",
        "digest-b",
    )


def test_partial_coverage_is_reported():
    result = (
        summarize_canonical_baserunning_shadow_validations(
            (
                validation(),
                validation(
                    status="unavailable",
                    digest=None,
                    simulation_count=0,
                    runner_count=0,
                    stolen_bases=0.0,
                    caught_stealing=0.0,
                ),
            )
        )
    )

    assert result.status == "ready"
    assert result.ready_count == 1
    assert result.unavailable_count == 1
    assert result.warnings == (
        "incomplete_baserunning_shadow_coverage",
    )


def test_validation_errors_are_reported():
    result = (
        summarize_canonical_baserunning_shadow_validations(
            (
                validation(),
                validation(
                    status="error",
                    digest=None,
                    simulation_count=0,
                    runner_count=0,
                    stolen_bases=0.0,
                    caught_stealing=0.0,
                ),
            )
        )
    )

    assert result.status == "ready"
    assert result.error_count == 1
    assert result.warnings == (
        "baserunning_shadow_validation_errors",
    )


def test_empty_input_is_unavailable():
    result = (
        summarize_canonical_baserunning_shadow_validations(
            ()
        )
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.error_message == (
        "no baserunning shadow validations were supplied"
    )


def test_non_tuple_input_fails_open():
    result = (
        summarize_canonical_baserunning_shadow_validations(
            []
        )
    )

    assert result.status == "error"
    assert result.error_message == (
        "validations must be a tuple"
    )


def test_invalid_member_fails_open():
    result = (
        summarize_canonical_baserunning_shadow_validations(
            (object(),)
        )
    )

    assert result.status == "error"
    assert result.error_message == (
        "validations must contain "
        "CanonicalBaserunningOutputValidation"
    )


def test_no_ready_validation_is_unavailable():
    result = (
        summarize_canonical_baserunning_shadow_validations(
            (
                validation(
                    status="unavailable",
                    digest=None,
                    simulation_count=0,
                    runner_count=0,
                    stolen_bases=0.0,
                    caught_stealing=0.0,
                ),
            )
        )
    )

    assert result.status == "unavailable"
    assert result.ready_count == 0
    assert result.unavailable_count == 1


def test_zero_activity_is_preserved():
    result = (
        summarize_canonical_baserunning_shadow_validations(
            (
                validation(
                    stolen_bases=0.0,
                    caught_stealing=0.0,
                    warnings=(
                        "zero_baserunning_activity_observed",
                    ),
                ),
            )
        )
    )

    assert result.observed_activity is False
    assert result.active_validation_count == 0
    assert result.warnings == (
        "zero_baserunning_activity_observed",
    )


def test_diagnostics_preserve_shadow_authority():
    diagnostics = (
        summarize_canonical_baserunning_shadow_validations(
            (validation(),)
        ).to_diagnostics()
    )

    assert diagnostics["activation_permitted"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_summary_is_deterministic():
    values = (
        validation(digest="digest-b"),
        validation(digest="digest-a"),
    )

    first = (
        summarize_canonical_baserunning_shadow_validations(
            values
        )
    )
    second = (
        summarize_canonical_baserunning_shadow_validations(
            values
        )
    )

    assert first == second
    assert first.catalog_digests == (
        "digest-a",
        "digest-b",
    )


def test_summary_version_is_explicit():
    result = (
        summarize_canonical_baserunning_shadow_validations(
            (validation(),)
        )
    )

    assert result.summary_version == (
        CANONICAL_BASERUNNING_SHADOW_SUMMARY_VERSION
    )
