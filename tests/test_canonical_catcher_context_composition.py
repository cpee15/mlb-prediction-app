import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_CATCHER_CONTEXT_COMPOSITION_VERSION,
    CanonicalCatcherPopTimeObservation,
    CanonicalCatcherTeamAssignment,
    compose_catcher_baserunning_contexts,
)


def assignment(
    catcher_id="800",
    team_side="home",
):
    return CanonicalCatcherTeamAssignment(
        catcher_id=catcher_id,
        team_side=team_side,
        assignment_source_version=(
            "confirmed_lineup_catcher_v1"
        ),
    )


def pop_time(
    catcher_id="800",
    seconds=1.89,
):
    return CanonicalCatcherPopTimeObservation(
        catcher_id=catcher_id,
        pop_time_seconds=seconds,
    )


def test_composes_complete_catcher_context():
    contexts = compose_catcher_baserunning_contexts(
        assignments=(assignment(),),
        pop_times=(pop_time(),),
    )

    assert len(contexts) == 1

    context = contexts[0]

    assert context.catcher_id == "800"
    assert context.team_side == "home"
    assert context.pop_time_score == 0.7
    assert context.context_source_version == (
        "confirmed_lineup_catcher_v1+"
        "baseball_savant_catcher_pop_time_v1+"
        "canonical_catcher_pop_time_normalization_v1"
    )


def test_missing_pop_time_does_not_fabricate_context():
    assert (
        compose_catcher_baserunning_contexts(
            assignments=(assignment(),),
            pop_times=(),
        )
        == ()
    )


def test_unassigned_pop_time_is_ignored():
    assert (
        compose_catcher_baserunning_contexts(
            assignments=(),
            pop_times=(pop_time(),),
        )
        == ()
    )


def test_assignment_order_is_preserved():
    contexts = compose_catcher_baserunning_contexts(
        assignments=(
            assignment(
                catcher_id="catcher-2",
                team_side="away",
            ),
            assignment(
                catcher_id="catcher-1",
                team_side="home",
            ),
        ),
        pop_times=(
            pop_time("catcher-1"),
            pop_time("catcher-2"),
        ),
    )

    assert tuple(
        value.catcher_id
        for value in contexts
    ) == (
        "catcher-2",
        "catcher-1",
    )


def test_duplicate_assignment_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "catcher assignment identifiers must be unique"
        ),
    ):
        compose_catcher_baserunning_contexts(
            assignments=(
                assignment(),
                assignment(
                    team_side="away",
                ),
            ),
            pop_times=(pop_time(),),
        )


def test_duplicate_assignment_side_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "catcher assignment team sides must be unique"
        ),
    ):
        compose_catcher_baserunning_contexts(
            assignments=(
                assignment("catcher-1", "home"),
                assignment("catcher-2", "home"),
            ),
            pop_times=(
                pop_time("catcher-1"),
                pop_time("catcher-2"),
            ),
        )


def test_duplicate_pop_time_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "catcher pop-time identifiers must be unique"
        ),
    ):
        compose_catcher_baserunning_contexts(
            assignments=(assignment(),),
            pop_times=(
                pop_time(),
                pop_time(seconds=1.91),
            ),
        )


def test_non_assignment_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "assignments must contain "
            "CanonicalCatcherTeamAssignment"
        ),
    ):
        compose_catcher_baserunning_contexts(
            assignments=(object(),),
            pop_times=(),
        )


def test_non_pop_time_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "pop_times must contain "
            "CanonicalCatcherPopTimeObservation"
        ),
    ):
        compose_catcher_baserunning_contexts(
            assignments=(),
            pop_times=(object(),),
        )


def test_unavailable_assignment_source_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "assignment_source_version must identify "
            "an available source"
        ),
    ):
        CanonicalCatcherTeamAssignment(
            catcher_id="catcher",
            team_side="away",
        )


def test_composition_version_is_explicit():
    assert assignment().composition_version == (
        CANONICAL_CATCHER_CONTEXT_COMPOSITION_VERSION
    )
