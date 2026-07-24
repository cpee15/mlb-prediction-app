import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_PITCHER_BASERUNNING_MATERIALIZATION_VERSION,
    CANONICAL_STATCAST_PICKOFF_SOURCE_VERSION,
    CanonicalPitcherBaserunningContext,
    CanonicalStatcastPitcherPickoffCounts,
    materialize_statcast_pitcher_observations,
)


def count(
    pitcher_id="pitcher",
    *,
    opportunities=40,
    attempts=8,
    successes=2,
):
    return CanonicalStatcastPitcherPickoffCounts(
        pitcher_id=pitcher_id,
        eligible_opportunities=opportunities,
        pickoff_attempts=attempts,
        successful_pickoffs=successes,
    )


def context(
    pitcher_id="pitcher",
    *,
    hold_score=0.72,
    delivery_time_score=0.38,
):
    return CanonicalPitcherBaserunningContext(
        pitcher_id=pitcher_id,
        hold_score=hold_score,
        delivery_time_score=delivery_time_score,
        context_source_version=(
            "explicit_pitcher_hold_context_v1"
        ),
    )


def test_materializes_complete_pitcher_observation():
    observations = (
        materialize_statcast_pitcher_observations(
            counts=(count(),),
            contexts=(context(),),
        )
    )

    assert len(observations) == 1

    value = observations[0]

    assert value.pitcher_id == "pitcher"
    assert value.eligible_pickoff_opportunities == 40
    assert value.pickoff_attempts == 8
    assert value.successful_pickoffs == 2
    assert value.hold_score == 0.72
    assert value.delivery_time_score == 0.38
    assert value.pickoff_attempt_rate == 0.2
    assert value.pickoff_success_rate == 0.25
    assert value.source_version == (
        f"{CANONICAL_STATCAST_PICKOFF_SOURCE_VERSION}+"
        "explicit_pitcher_hold_context_v1"
    )


def test_missing_context_does_not_fabricate_observation():
    observations = (
        materialize_statcast_pitcher_observations(
            counts=(count(),),
            contexts=(),
        )
    )

    assert observations == ()


def test_unmatched_context_is_ignored():
    observations = (
        materialize_statcast_pitcher_observations(
            counts=(count("pitcher-1"),),
            contexts=(context("pitcher-2"),),
        )
    )

    assert observations == ()


def test_count_order_is_preserved():
    observations = (
        materialize_statcast_pitcher_observations(
            counts=(
                count("pitcher-2"),
                count("pitcher-1"),
            ),
            contexts=(
                context("pitcher-1"),
                context("pitcher-2"),
            ),
        )
    )

    assert tuple(
        value.pitcher_id
        for value in observations
    ) == ("pitcher-2", "pitcher-1")


def test_duplicate_count_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "pitcher pickoff count identifiers "
            "must be unique"
        ),
    ):
        materialize_statcast_pitcher_observations(
            counts=(
                count(),
                count(attempts=7),
            ),
            contexts=(context(),),
        )


def test_duplicate_context_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "pitcher context identifiers must be unique"
        ),
    ):
        materialize_statcast_pitcher_observations(
            counts=(count(),),
            contexts=(
                context(),
                context(hold_score=0.60),
            ),
        )


def test_invalid_count_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "counts must contain "
            "CanonicalStatcastPitcherPickoffCounts"
        ),
    ):
        materialize_statcast_pitcher_observations(
            counts=(object(),),
            contexts=(),
        )


def test_invalid_context_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "contexts must contain "
            "CanonicalPitcherBaserunningContext"
        ),
    ):
        materialize_statcast_pitcher_observations(
            counts=(),
            contexts=(object(),),
        )


def test_context_requires_explicit_source():
    with pytest.raises(
        ValueError,
        match=(
            "context_source_version must identify "
            "an available source"
        ),
    ):
        CanonicalPitcherBaserunningContext(
            pitcher_id="pitcher",
            hold_score=0.50,
            delivery_time_score=0.50,
        )


def test_context_rejects_invalid_scores():
    with pytest.raises(
        ValueError,
        match=(
            "delivery_time_score must be between 0 and 1"
        ),
    ):
        context(
            delivery_time_score=1.01,
        )


def test_materialization_is_deterministic():
    first = materialize_statcast_pitcher_observations(
        counts=(count(),),
        contexts=(context(),),
    )
    second = materialize_statcast_pitcher_observations(
        counts=(count(),),
        contexts=(context(),),
    )

    assert first == second
    assert first[0].digest == second[0].digest


def test_materialization_version_is_explicit():
    assert context().materialization_version == (
        CANONICAL_PITCHER_BASERUNNING_MATERIALIZATION_VERSION
    )
