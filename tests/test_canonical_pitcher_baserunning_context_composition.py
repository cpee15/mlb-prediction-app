import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_PITCHER_CONTEXT_COMPOSITION_VERSION,
    CanonicalPitcherDeliveryTimeObservation,
    CanonicalPitcherHoldObservation,
    compose_pitcher_baserunning_contexts,
)


def hold(
    *,
    pitcher_id="pitcher",
    opportunities=10,
    attempts=2,
):
    return CanonicalPitcherHoldObservation(
        pitcher_id=pitcher_id,
        eligible_opportunities=opportunities,
        steal_attempts_against=attempts,
        source_version="statcast_counts_v1",
    )


def delivery(
    *,
    pitcher_id="pitcher",
    seconds_to_plate=1.20,
):
    return CanonicalPitcherDeliveryTimeObservation(
        pitcher_id=pitcher_id,
        seconds_to_plate=seconds_to_plate,
        source_version="delivery_measurements_v1",
    )


def compose(
    *,
    holds=None,
    deliveries=None,
):
    return compose_pitcher_baserunning_contexts(
        hold_observations=(
            (hold(),)
            if holds is None
            else holds
        ),
        delivery_time_observations=(
            (delivery(),)
            if deliveries is None
            else deliveries
        ),
    )


def test_composes_complete_pitcher_context():
    context = compose()[0]

    assert context.pitcher_id == "pitcher"
    assert context.hold_score == 0.8
    assert context.delivery_time_score == 1.0


def test_composition_preserves_source_provenance():
    context = compose()[0]

    assert context.context_source_version == (
        "statcast_counts_v1+"
        "canonical_pitcher_hold_evidence_v1+"
        "canonical_pitcher_hold_normalization_v1+"
        "delivery_measurements_v1+"
        "canonical_pitcher_delivery_time_normalization_v1+"
        "canonical_pitcher_context_composition_v1"
    )


def test_missing_delivery_time_is_omitted():
    assert compose(
        deliveries=(),
    ) == ()


def test_missing_hold_evidence_is_omitted():
    assert compose(
        holds=(),
    ) == ()


def test_unmatched_pitcher_is_omitted():
    assert compose(
        holds=(
            hold(pitcher_id="hold_pitcher"),
        ),
        deliveries=(
            delivery(
                pitcher_id="delivery_pitcher"
            ),
        ),
    ) == ()


def test_output_order_follows_hold_observations():
    contexts = compose(
        holds=(
            hold(pitcher_id="second"),
            hold(pitcher_id="first"),
        ),
        deliveries=(
            delivery(pitcher_id="first"),
            delivery(pitcher_id="second"),
        ),
    )

    assert tuple(
        value.pitcher_id
        for value in contexts
    ) == ("second", "first")


def test_duplicate_hold_identifiers_are_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "pitcher hold observation identifiers "
            "must be unique"
        ),
    ):
        compose(
            holds=(
                hold(),
                hold(),
            ),
        )


def test_duplicate_delivery_identifiers_are_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "pitcher delivery-time observation "
            "identifiers must be unique"
        ),
    ):
        compose(
            deliveries=(
                delivery(),
                delivery(),
            ),
        )


def test_non_tuple_hold_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match="hold_observations must be a tuple",
    ):
        compose_pitcher_baserunning_contexts(
            hold_observations=[],
            delivery_time_observations=(),
        )


def test_non_tuple_delivery_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "delivery_time_observations must be a tuple"
        ),
    ):
        compose_pitcher_baserunning_contexts(
            hold_observations=(),
            delivery_time_observations=[],
        )


def test_invalid_hold_observation_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "hold_observations must contain "
            "CanonicalPitcherHoldObservation"
        ),
    ):
        compose_pitcher_baserunning_contexts(
            hold_observations=(object(),),
            delivery_time_observations=(),
        )


def test_invalid_delivery_observation_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "delivery_time_observations must contain "
            "CanonicalPitcherDeliveryTimeObservation"
        ),
    ):
        compose_pitcher_baserunning_contexts(
            hold_observations=(),
            delivery_time_observations=(object(),),
        )


def test_composition_is_deterministic():
    assert compose() == compose()


def test_composition_version_is_explicit():
    assert (
        CANONICAL_PITCHER_CONTEXT_COMPOSITION_VERSION
        == "canonical_pitcher_context_composition_v1"
    )
