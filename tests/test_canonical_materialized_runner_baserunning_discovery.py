from mlb_app.simulation.shadow import (
    CanonicalCatcherBaserunningObservation,
    CanonicalCatcherObservationComposition,
    CanonicalPitcherBaserunningContext,
    CanonicalRunnerBaserunningContext,
    CanonicalStatcastPitcherPickoffCounts,
    CanonicalStatcastRunnerBaserunningCounts,
    discover_materialized_runner_baserunning_evidence,
)


def catcher(
    catcher_id,
    team_side,
):
    return CanonicalCatcherBaserunningObservation(
        catcher_id=catcher_id,
        team_side=team_side,
        steal_attempts_against=10,
        caught_stealing=3,
        pop_time_score=0.85,
    )


def catcher_composition():
    return CanonicalCatcherObservationComposition(
        observations=(
            catcher("away-catcher", "away"),
            catcher("home-catcher", "home"),
        ),
        assignment_count=2,
        count_record_count=2,
        pop_time_count=2,
        context_count=2,
        status="ready",
    )


def runner_counts(
    runner_id="runner",
):
    return CanonicalStatcastRunnerBaserunningCounts(
        runner_id=runner_id,
        eligible_opportunities=20,
        stolen_bases=6,
        caught_stealing=2,
    )


def runner_context(
    runner_id="runner",
):
    return CanonicalRunnerBaserunningContext(
        runner_id=runner_id,
        speed_score=0.90,
        lead_quality=0.80,
        fatigue_index=0.10,
        context_source_version=(
            "composed_runner_context_v1"
        ),
    )


def pitcher_counts(
    pitcher_id="pitcher",
):
    return CanonicalStatcastPitcherPickoffCounts(
        pitcher_id=pitcher_id,
        eligible_opportunities=40,
        pickoff_attempts=8,
        successful_pickoffs=2,
    )


def pitcher_context(
    pitcher_id="pitcher",
):
    return CanonicalPitcherBaserunningContext(
        pitcher_id=pitcher_id,
        hold_score=0.80,
        delivery_time_score=0.75,
        context_source_version=(
            "composed_pitcher_context_v1"
        ),
    )


def discover(
    *,
    counts=None,
    contexts=None,
    pitcher_pickoffs=None,
    pitcher_context_values=None,
):
    return (
        discover_materialized_runner_baserunning_evidence(
            required_runner_ids=("runner",),
            required_pitcher_ids=("pitcher",),
            runner_counts=(
                (runner_counts(),)
                if counts is None
                else counts
            ),
            runner_contexts=(
                (runner_context(),)
                if contexts is None
                else contexts
            ),
            pitcher_pickoff_counts=(
                (pitcher_counts(),)
                if pitcher_pickoffs is None
                else pitcher_pickoffs
            ),
            pitcher_contexts=(
                (pitcher_context(),)
                if pitcher_context_values is None
                else pitcher_context_values
            ),
            catcher_composition=(
                catcher_composition()
            ),
        )
    )


def test_materialized_runner_builds_catalog():
    result = discover()

    assert result.status == "ready"
    assert result.ready is True
    assert result.catalog is not None
    assert len(result.catalog.runners) == 1

    runner = result.catalog.runners[0]

    assert runner.runner_id == "runner"
    assert runner.speed_score == 0.90
    assert runner.lead_quality == 0.80
    assert runner.fatigue_index == 0.10
    assert runner.attempt_rate == 0.40
    assert runner.success_rate == 0.75


def test_materialized_runner_preserves_digest():
    result = discover()

    assert result.observation_digest is not None
    assert len(result.observation_digest) == 64
    assert (
        result.to_diagnostics()["observation_digest"]
        == result.observation_digest
    )


def test_missing_runner_context_remains_unavailable():
    result = discover(
        contexts=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_runner_count == 0


def test_unmatched_runner_context_remains_unavailable():
    result = discover(
        contexts=(
            runner_context("other-runner"),
        ),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_runner_count == 0


def test_missing_runner_counts_remains_unavailable():
    result = discover(
        counts=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_runner_count == 0


def test_invalid_runner_count_fails_open():
    result = discover(
        counts=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "counts must contain "
        "CanonicalStatcastRunnerBaserunningCounts"
    )


def test_invalid_runner_context_fails_open():
    result = discover(
        contexts=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "contexts must contain "
        "CanonicalRunnerBaserunningContext"
    )


def test_duplicate_runner_counts_fail_open():
    result = discover(
        counts=(
            runner_counts(),
            runner_counts(),
        ),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "runner count identifiers must be unique"
    )


def test_duplicate_runner_contexts_fail_open():
    result = discover(
        contexts=(
            runner_context(),
            runner_context(),
        ),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "runner context identifiers must be unique"
    )


def test_invalid_pitcher_materialization_still_fails_open():
    result = discover(
        pitcher_pickoffs=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "counts must contain "
        "CanonicalStatcastPitcherPickoffCounts"
    )


def test_discovery_is_deterministic():
    first = discover()
    second = discover()

    assert first.catalog is not None
    assert second.catalog is not None
    assert first.catalog.digest == second.catalog.digest
    assert (
        first.observation_digest
        == second.observation_digest
    )


def test_diagnostics_preserve_shadow_authority():
    diagnostics = discover().to_diagnostics()

    assert diagnostics["ready"] is True
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"
