import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_PITCHER_HOLD_EVIDENCE_VERSION,
    CANONICAL_PITCHER_HOLD_NORMALIZATION_VERSION,
    CanonicalPitcherHoldObservation,
    CanonicalStatcastPitcherBaserunningCounts,
    adapt_statcast_pitcher_hold_evidence,
)


def counts(
    *,
    pitcher_id="pitcher",
    opportunities=10,
    stolen_bases=2,
    caught_stealing=1,
):
    return CanonicalStatcastPitcherBaserunningCounts(
        pitcher_id=pitcher_id,
        eligible_opportunities=opportunities,
        stolen_bases_allowed=stolen_bases,
        caught_stealing=caught_stealing,
    )


def test_adapts_exact_attempt_exposure():
    observation = adapt_statcast_pitcher_hold_evidence(
        (counts(),)
    )[0]

    assert observation.pitcher_id == "pitcher"
    assert observation.eligible_opportunities == 10
    assert observation.steal_attempts_against == 3
    assert observation.attempt_rate == 0.3
    assert observation.hold_score == 0.7


def test_caught_stealing_counts_only_as_an_attempt():
    stolen = adapt_statcast_pitcher_hold_evidence(
        (
            counts(
                pitcher_id="stolen",
                stolen_bases=3,
                caught_stealing=0,
            ),
        )
    )[0]
    caught = adapt_statcast_pitcher_hold_evidence(
        (
            counts(
                pitcher_id="caught",
                stolen_bases=0,
                caught_stealing=3,
            ),
        )
    )[0]

    assert stolen.steal_attempts_against == 3
    assert caught.steal_attempts_against == 3
    assert stolen.hold_score == caught.hold_score


def test_zero_attempts_produce_maximum_observed_hold():
    observation = adapt_statcast_pitcher_hold_evidence(
        (
            counts(
                stolen_bases=0,
                caught_stealing=0,
            ),
        )
    )[0]

    assert observation.attempt_rate == 0.0
    assert observation.hold_score == 1.0


def test_zero_opportunity_counts_are_omitted():
    value = counts(
        opportunities=0,
        stolen_bases=0,
        caught_stealing=0,
    )

    assert adapt_statcast_pitcher_hold_evidence(
        (value,)
    ) == ()


def test_input_order_is_preserved():
    observations = adapt_statcast_pitcher_hold_evidence(
        (
            counts(pitcher_id="first"),
            counts(pitcher_id="second"),
        )
    )

    assert tuple(
        value.pitcher_id
        for value in observations
    ) == ("first", "second")


def test_duplicate_pitcher_identifiers_are_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "pitcher baserunning count identifiers "
            "must be unique"
        ),
    ):
        adapt_statcast_pitcher_hold_evidence(
            (
                counts(),
                counts(),
            )
        )


def test_non_tuple_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match="counts must be a tuple",
    ):
        adapt_statcast_pitcher_hold_evidence([])


def test_invalid_count_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "counts must contain "
            "CanonicalStatcastPitcherBaserunningCounts"
        ),
    ):
        adapt_statcast_pitcher_hold_evidence(
            (object(),)
        )


def test_observation_versions_are_explicit():
    observation = adapt_statcast_pitcher_hold_evidence(
        (counts(),)
    )[0]

    assert observation.evidence_version == (
        CANONICAL_PITCHER_HOLD_EVIDENCE_VERSION
    )
    assert observation.normalization_version == (
        CANONICAL_PITCHER_HOLD_NORMALIZATION_VERSION
    )
    assert observation.source_version == counts().source_version


def test_observation_is_deterministic():
    assert adapt_statcast_pitcher_hold_evidence(
        (counts(),)
    ) == adapt_statcast_pitcher_hold_evidence(
        (counts(),)
    )


def test_direct_observation_requires_positive_exposure():
    with pytest.raises(
        ValueError,
        match="eligible_opportunities must be positive",
    ):
        CanonicalPitcherHoldObservation(
            pitcher_id="pitcher",
            eligible_opportunities=0,
            steal_attempts_against=0,
            source_version="source_v1",
        )
