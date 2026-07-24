from mlb_app.simulation.shadow import (
    CanonicalCatcherBaserunningObservation,
    CanonicalCatcherObservationComposition,
    CanonicalPitcherBaserunningContext,
    CanonicalRunnerBaserunningObservation,
    CanonicalStatcastPitcherPickoffCounts,
    discover_materialized_pitcher_baserunning_evidence,
)


def runner():
    return CanonicalRunnerBaserunningObservation(
        runner_id="runner",
        eligible_opportunities=20,
        stolen_bases=6,
        caught_stealing=2,
        speed_score=0.90,
        lead_quality=0.80,
        fatigue_index=0.10,
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


def pickoff_counts(
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
):
    return (
        discover_materialized_pitcher_baserunning_evidence(
            required_runner_ids=("runner",),
            required_pitcher_ids=("pitcher",),
            runner_observations=(runner(),),
            pitcher_pickoff_counts=(
                (pickoff_counts(),)
                if counts is None
                else counts
            ),
            pitcher_contexts=(
                (pitcher_context(),)
                if contexts is None
                else contexts
            ),
            catcher_composition=(
                catcher_composition()
            ),
        )
    )


def test_materialized_pitcher_builds_catalog():
    result = discover()

    assert result.status == "ready"
    assert result.ready is True
    assert result.catalog is not None
    assert len(result.catalog.pitchers) == 1

    pitcher = result.catalog.pitchers[0]

    assert pitcher.pitcher_id == "pitcher"
    assert pitcher.hold_score == 0.80
    assert pitcher.delivery_time_score == 0.75
    assert pitcher.pickoff_attempt_rate == 0.20
    assert pitcher.pickoff_success_rate == 0.25


def test_materialized_pitcher_preserves_digest():
    result = discover()

    assert result.observation_digest is not None
    assert len(result.observation_digest) == 64
    assert (
        result.to_diagnostics()["observation_digest"]
        == result.observation_digest
    )


def test_missing_pitcher_context_remains_unavailable():
    result = discover(
        contexts=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_pitcher_count == 0


def test_unmatched_pitcher_context_remains_unavailable():
    result = discover(
        contexts=(
            pitcher_context("other-pitcher"),
        ),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_pitcher_count == 0


def test_missing_pickoff_counts_remains_unavailable():
    result = discover(
        counts=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_pitcher_count == 0


def test_invalid_pickoff_count_fails_open():
    result = discover(
        counts=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "counts must contain "
        "CanonicalStatcastPitcherPickoffCounts"
    )


def test_invalid_pitcher_context_fails_open():
    result = discover(
        contexts=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "contexts must contain "
        "CanonicalPitcherBaserunningContext"
    )


def test_duplicate_pickoff_counts_fail_open():
    result = discover(
        counts=(
            pickoff_counts(),
            pickoff_counts(),
        ),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "pitcher pickoff count identifiers "
        "must be unique"
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
