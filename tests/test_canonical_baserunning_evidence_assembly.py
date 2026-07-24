from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_EVIDENCE_ASSEMBLY_VERSION,
    CanonicalCatcherBaserunningObservation,
    CanonicalCatcherObservationComposition,
    CanonicalPitcherDeliveryTimeObservation,
    CanonicalRunnerAvailabilityObservation,
    CanonicalRunnerLeadQualityObservation,
    CanonicalRunnerSprintSpeedObservation,
    CanonicalStatcastPitcherBaserunningCounts,
    CanonicalStatcastPitcherPickoffCounts,
    CanonicalStatcastRunnerBaserunningCounts,
    assemble_complete_canonical_baserunning_evidence,
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


def catchers():
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


def runner_counts():
    return CanonicalStatcastRunnerBaserunningCounts(
        runner_id="runner",
        eligible_opportunities=20,
        stolen_bases=6,
        caught_stealing=2,
    )


def sprint():
    return CanonicalRunnerSprintSpeedObservation(
        runner_id="runner",
        sprint_speed_ft_per_second=28.0,
    )


def lead():
    return CanonicalRunnerLeadQualityObservation(
        runner_id="runner",
        lead_quality=0.80,
        source_version="lead_source_v1",
    )


def availability():
    return CanonicalRunnerAvailabilityObservation(
        runner_id="runner",
        fatigue_index=0.10,
        injury_limit_flag=False,
        source_version="availability_source_v1",
    )


def pitcher_counts():
    return CanonicalStatcastPitcherBaserunningCounts(
        pitcher_id="pitcher",
        eligible_opportunities=40,
        stolen_bases_allowed=6,
        caught_stealing=2,
    )


def pickoff_counts():
    return CanonicalStatcastPitcherPickoffCounts(
        pitcher_id="pitcher",
        eligible_opportunities=40,
        pickoff_attempts=8,
        successful_pickoffs=2,
    )


def delivery():
    return CanonicalPitcherDeliveryTimeObservation(
        pitcher_id="pitcher",
        seconds_to_plate=1.20,
        source_version="delivery_source_v1",
    )


def assemble(
    *,
    runner_count_values=None,
    sprint_values=None,
    lead_values=None,
    availability_values=None,
    pitcher_count_values=None,
    pickoff_values=None,
    delivery_values=None,
):
    return (
        assemble_complete_canonical_baserunning_evidence(
            required_runner_ids=("runner",),
            required_pitcher_ids=("pitcher",),
            catcher_composition=catchers(),
            runner_counts=(
                (runner_counts(),)
                if runner_count_values is None
                else runner_count_values
            ),
            runner_sprint_speed_observations=(
                (sprint(),)
                if sprint_values is None
                else sprint_values
            ),
            runner_lead_quality_observations=(
                (lead(),)
                if lead_values is None
                else lead_values
            ),
            runner_availability_observations=(
                (availability(),)
                if availability_values is None
                else availability_values
            ),
            pitcher_baserunning_counts=(
                (pitcher_counts(),)
                if pitcher_count_values is None
                else pitcher_count_values
            ),
            pitcher_pickoff_counts=(
                (pickoff_counts(),)
                if pickoff_values is None
                else pickoff_values
            ),
            pitcher_delivery_time_observations=(
                (delivery(),)
                if delivery_values is None
                else delivery_values
            ),
        )
    )


def test_complete_evidence_builds_catalog():
    result = assemble()

    assert result.status == "ready"
    assert result.ready is True
    assert result.catalog is not None
    assert len(result.catalog.runners) == 1
    assert len(result.catalog.pitchers) == 1

    runner = result.catalog.runners[0]
    pitcher = result.catalog.pitchers[0]

    assert runner.runner_id == "runner"
    assert runner.attempt_rate == 0.40
    assert runner.success_rate == 0.75
    assert runner.lead_quality == 0.80
    assert runner.fatigue_index == 0.10

    assert pitcher.pitcher_id == "pitcher"
    assert pitcher.hold_score == 0.80
    assert pitcher.delivery_time_score == 1.0
    assert pitcher.pickoff_attempt_rate == 0.20
    assert pitcher.pickoff_success_rate == 0.25


def test_missing_runner_sprint_speed_is_unavailable():
    result = assemble(
        sprint_values=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_runner_count == 0


def test_missing_runner_lead_quality_is_unavailable():
    result = assemble(
        lead_values=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_runner_count == 0


def test_missing_runner_availability_is_unavailable():
    result = assemble(
        availability_values=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_runner_count == 0


def test_missing_pitcher_hold_evidence_is_unavailable():
    result = assemble(
        pitcher_count_values=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_pitcher_count == 0


def test_missing_pitcher_delivery_time_is_unavailable():
    result = assemble(
        delivery_values=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_pitcher_count == 0


def test_invalid_runner_evidence_fails_open():
    result = assemble(
        sprint_values=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "sprint_speed_observations must contain "
        "CanonicalRunnerSprintSpeedObservation"
    )


def test_invalid_pitcher_evidence_fails_open():
    result = assemble(
        pitcher_count_values=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "counts must contain "
        "CanonicalStatcastPitcherBaserunningCounts"
    )


def test_assembly_is_deterministic():
    first = assemble()
    second = assemble()

    assert first.catalog is not None
    assert second.catalog is not None
    assert first.catalog.digest == second.catalog.digest
    assert (
        first.observation_digest
        == second.observation_digest
    )


def test_diagnostics_preserve_shadow_authority():
    diagnostics = assemble().to_diagnostics()

    assert diagnostics["ready"] is True
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_assembly_version_is_explicit():
    assert (
        CANONICAL_BASERUNNING_EVIDENCE_ASSEMBLY_VERSION
        == "canonical_baserunning_evidence_assembly_v1"
    )
