import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_RUNNER_CONTEXT_COMPOSITION_VERSION,
    CanonicalRunnerAvailabilityObservation,
    CanonicalRunnerLeadQualityObservation,
    CanonicalRunnerSprintSpeedObservation,
    compose_runner_baserunning_contexts,
)


def sprint(
    runner_id="runner",
    speed=28.0,
):
    return CanonicalRunnerSprintSpeedObservation(
        runner_id=runner_id,
        sprint_speed_ft_per_second=speed,
    )


def lead(
    runner_id="runner",
    score=0.60,
):
    return CanonicalRunnerLeadQualityObservation(
        runner_id=runner_id,
        lead_quality=score,
        source_version="lead_source_v1",
    )


def availability(
    runner_id="runner",
    fatigue=0.20,
    injury=False,
):
    return CanonicalRunnerAvailabilityObservation(
        runner_id=runner_id,
        fatigue_index=fatigue,
        injury_limit_flag=injury,
        source_version="availability_source_v1",
    )


def compose(
    *,
    speeds=None,
    leads=None,
    availabilities=None,
):
    return compose_runner_baserunning_contexts(
        sprint_speed_observations=(
            (sprint(),)
            if speeds is None
            else speeds
        ),
        lead_quality_observations=(
            (lead(),)
            if leads is None
            else leads
        ),
        availability_observations=(
            (availability(),)
            if availabilities is None
            else availabilities
        ),
    )


def test_composes_complete_runner_context():
    value = compose()[0]

    assert value.runner_id == "runner"
    assert value.speed_score == sprint().speed_score
    assert value.lead_quality == 0.60
    assert value.fatigue_index == 0.20
    assert value.injury_limit_flag is False
    assert (
        CANONICAL_RUNNER_CONTEXT_COMPOSITION_VERSION
        in value.context_source_version
    )


def test_preserves_injury_limitation():
    value = compose(
        availabilities=(
            availability(injury=True),
        ),
    )[0]

    assert value.injury_limit_flag is True


@pytest.mark.parametrize(
    (
        "speeds",
        "leads",
        "availabilities",
    ),
    (
        ((), (lead(),), (availability(),)),
        ((sprint(),), (), (availability(),)),
        ((sprint(),), (lead(),), ()),
    ),
)
def test_incomplete_runner_is_omitted(
    speeds,
    leads,
    availabilities,
):
    assert compose(
        speeds=speeds,
        leads=leads,
        availabilities=availabilities,
    ) == ()


def test_unmatched_runner_is_omitted():
    assert compose(
        leads=(lead(runner_id="other"),),
    ) == ()


def test_output_order_follows_sprint_observations():
    values = compose(
        speeds=(
            sprint("runner_b", 29.0),
            sprint("runner_a", 27.0),
        ),
        leads=(
            lead("runner_a"),
            lead("runner_b"),
        ),
        availabilities=(
            availability("runner_a"),
            availability("runner_b"),
        ),
    )

    assert tuple(
        value.runner_id
        for value in values
    ) == ("runner_b", "runner_a")


@pytest.mark.parametrize(
    (
        "keyword",
        "message",
    ),
    (
        (
            "sprint_speed_observations",
            "sprint_speed_observations must be a tuple",
        ),
        (
            "lead_quality_observations",
            "lead_quality_observations must be a tuple",
        ),
        (
            "availability_observations",
            "availability_observations must be a tuple",
        ),
    ),
)
def test_non_tuple_input_is_rejected(
    keyword,
    message,
):
    arguments = {
        "sprint_speed_observations": (sprint(),),
        "lead_quality_observations": (lead(),),
        "availability_observations": (
            availability(),
        ),
    }
    arguments[keyword] = []

    with pytest.raises(TypeError, match=message):
        compose_runner_baserunning_contexts(
            **arguments
        )


@pytest.mark.parametrize(
    (
        "keyword",
        "message",
    ),
    (
        (
            "sprint_speed_observations",
            "must contain "
            "CanonicalRunnerSprintSpeedObservation",
        ),
        (
            "lead_quality_observations",
            "must contain "
            "CanonicalRunnerLeadQualityObservation",
        ),
        (
            "availability_observations",
            "must contain "
            "CanonicalRunnerAvailabilityObservation",
        ),
    ),
)
def test_invalid_observation_is_rejected(
    keyword,
    message,
):
    arguments = {
        "sprint_speed_observations": (sprint(),),
        "lead_quality_observations": (lead(),),
        "availability_observations": (
            availability(),
        ),
    }
    arguments[keyword] = (object(),)

    with pytest.raises(TypeError, match=message):
        compose_runner_baserunning_contexts(
            **arguments
        )


@pytest.mark.parametrize(
    "keyword",
    (
        "sprint_speed_observations",
        "lead_quality_observations",
        "availability_observations",
    ),
)
def test_duplicate_runner_identifiers_are_rejected(
    keyword,
):
    arguments = {
        "sprint_speed_observations": (sprint(),),
        "lead_quality_observations": (lead(),),
        "availability_observations": (
            availability(),
        ),
    }
    duplicate = arguments[keyword][0]
    arguments[keyword] = (
        duplicate,
        duplicate,
    )

    with pytest.raises(
        ValueError,
        match="runner identifiers must be unique",
    ):
        compose_runner_baserunning_contexts(
            **arguments
        )


def test_composition_is_deterministic():
    assert compose() == compose()


def test_composition_version_is_explicit():
    assert (
        CANONICAL_RUNNER_CONTEXT_COMPOSITION_VERSION
        == "canonical_runner_context_composition_v1"
    )
