from mlb_app.simulation.shadow import (
    CANONICAL_CATCHER_OBSERVATION_COMPOSITION_VERSION,
    CONFIRMED_CATCHER_ASSIGNMENT_SOURCE_VERSION,
    CanonicalCatcherAssignmentDiscovery,
    CanonicalCatcherPopTimeObservation,
    CanonicalCatcherTeamAssignment,
    CanonicalStatcastCatcherBaserunningCounts,
    compose_confirmed_catcher_observations,
)


def assignment(
    catcher_id,
    team_side,
):
    return CanonicalCatcherTeamAssignment(
        catcher_id=catcher_id,
        team_side=team_side,
        assignment_source_version=(
            CONFIRMED_CATCHER_ASSIGNMENT_SOURCE_VERSION
        ),
    )


def assignment_discovery(
    *,
    status="ready",
    assignments=None,
):
    if assignments is None:
        assignments = (
            assignment("away-catcher", "away"),
            assignment("home-catcher", "home"),
        )

    return CanonicalCatcherAssignmentDiscovery(
        assignments=assignments,
        away_candidate_count=1,
        home_candidate_count=1,
        status=status,
    )


def count(
    catcher_id,
    *,
    opportunities=20,
    stolen_bases_allowed=5,
    caught_stealing=2,
):
    return CanonicalStatcastCatcherBaserunningCounts(
        catcher_id=catcher_id,
        eligible_opportunities=opportunities,
        stolen_bases_allowed=stolen_bases_allowed,
        caught_stealing=caught_stealing,
    )


def pop_time(
    catcher_id,
    seconds,
):
    return CanonicalCatcherPopTimeObservation(
        catcher_id=catcher_id,
        pop_time_seconds=seconds,
    )


def compose(**overrides):
    values = {
        "assignment_discovery": (
            assignment_discovery()
        ),
        "counts": (
            count("home-catcher"),
            count(
                "away-catcher",
                stolen_bases_allowed=4,
                caught_stealing=3,
            ),
        ),
        "pop_times": (
            pop_time("home-catcher", 1.95),
            pop_time("away-catcher", 1.89),
        ),
    }
    values.update(overrides)

    return compose_confirmed_catcher_observations(
        **values
    )


def test_composes_complete_confirmed_observations():
    result = compose()

    assert result.status == "ready"
    assert result.ready is True
    assert result.assignment_count == 2
    assert result.count_record_count == 2
    assert result.pop_time_count == 2
    assert result.context_count == 2

    assert result.away_observation is not None
    assert result.home_observation is not None

    assert result.away_observation.catcher_id == (
        "away-catcher"
    )
    assert (
        result.away_observation.steal_attempts_against
        == 7
    )
    assert result.away_observation.caught_stealing == 3
    assert result.away_observation.pop_time_score == 0.7

    assert result.home_observation.catcher_id == (
        "home-catcher"
    )
    assert (
        result.home_observation.steal_attempts_against
        == 7
    )
    assert result.home_observation.caught_stealing == 2
    assert result.home_observation.pop_time_score == 0.5


def test_observations_are_ordered_away_then_home():
    result = compose()

    assert tuple(
        value.team_side
        for value in result.observations
    ) == ("away", "home")


def test_incomplete_assignment_discovery_fails_open():
    result = compose(
        assignment_discovery=(
            CanonicalCatcherAssignmentDiscovery(
                assignments=(
                    assignment(
                        "away-catcher",
                        "away",
                    ),
                ),
                away_candidate_count=1,
                home_candidate_count=0,
                status="partial",
            )
        ),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.observations == ()
    assert result.assignment_count == 1


def test_missing_pop_time_fails_open():
    result = compose(
        pop_times=(
            pop_time("away-catcher", 1.89),
        ),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.observations == ()
    assert result.context_count == 1


def test_missing_count_record_fails_open():
    result = compose(
        counts=(
            count("away-catcher"),
        ),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.observations == ()


def test_unassigned_evidence_does_not_complete_matchup():
    result = compose(
        counts=(
            count("away-catcher"),
            count("other-catcher"),
        ),
        pop_times=(
            pop_time("away-catcher", 1.89),
            pop_time("other-catcher", 1.95),
        ),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.observations == ()


def test_invalid_assignment_contract_fails_open():
    result = compose_confirmed_catcher_observations(
        assignment_discovery=object(),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.observations == ()
    assert result.error_type == "TypeError"
    assert result.error_message == (
        "assignment_discovery must be "
        "CanonicalCatcherAssignmentDiscovery"
    )


def test_invalid_count_contract_fails_open():
    result = compose(
        counts=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.observations == ()
    assert result.error_type == "AttributeError"


def test_invalid_pop_time_contract_fails_open():
    result = compose(
        pop_times=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.observations == ()
    assert result.error_type == "TypeError"
    assert result.error_message == (
        "pop_times must contain "
        "CanonicalCatcherPopTimeObservation"
    )


def test_non_tuple_evidence_fails_open():
    result = compose(
        counts=[],
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.error_type == "TypeError"
    assert result.error_message == (
        "counts must be a tuple"
    )


def test_diagnostics_preserve_shadow_authority():
    diagnostics = compose().to_diagnostics()

    assert diagnostics["ready"] is True
    assert diagnostics["observation_count"] == 2
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_composition_is_deterministic():
    first = compose()
    second = compose()

    assert first == second


def test_composition_version_is_explicit():
    assert compose().composition_version == (
        CANONICAL_CATCHER_OBSERVATION_COMPOSITION_VERSION
    )
