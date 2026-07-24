from mlb_app.simulation.shadow import (
    CanonicalCatcherBaserunningObservation,
    CanonicalCatcherObservationComposition,
    CanonicalPitcherBaserunningObservation,
    CanonicalRunnerBaserunningObservation,
    discover_composed_canonical_baserunning_evidence,
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


def pitcher():
    return CanonicalPitcherBaserunningObservation(
        pitcher_id="pitcher",
        eligible_pickoff_opportunities=25,
        pickoff_attempts=5,
        successful_pickoffs=1,
        hold_score=0.75,
        delivery_time_score=0.70,
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


def ready_composition():
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


def discover(
    *,
    catcher_composition=None,
):
    if catcher_composition is None:
        catcher_composition = ready_composition()

    return discover_composed_canonical_baserunning_evidence(
        required_runner_ids=("runner",),
        required_pitcher_ids=("pitcher",),
        runner_observations=(runner(),),
        pitcher_observations=(pitcher(),),
        catcher_composition=catcher_composition,
    )


def test_ready_composition_builds_catalog():
    result = discover()

    assert result.status == "ready"
    assert result.ready is True
    assert result.catalog is not None

    assert result.catalog.away_catcher.catcher_id == (
        "away-catcher"
    )
    assert result.catalog.away_catcher.team_side == "away"

    assert result.catalog.home_catcher.catcher_id == (
        "home-catcher"
    )
    assert result.catalog.home_catcher.team_side == "home"


def test_ready_composition_preserves_observation_digest():
    result = discover()

    assert result.observation_digest is not None
    assert len(result.observation_digest) == 64
    assert (
        result.to_diagnostics()["observation_digest"]
        == result.observation_digest
    )


def test_unavailable_composition_blocks_catalog():
    result = discover(
        catcher_composition=(
            CanonicalCatcherObservationComposition(
                assignment_count=1,
                count_record_count=1,
                pop_time_count=1,
                context_count=1,
                status="unavailable",
            )
        ),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.observation_digest is not None


def test_error_composition_fails_open():
    result = discover(
        catcher_composition=(
            CanonicalCatcherObservationComposition(
                status="error",
                error_type="ValueError",
                error_message=(
                    "catcher evidence is incomplete"
                ),
            )
        ),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.observation_digest is None
    assert result.error_message == (
        "catcher evidence is incomplete"
    )


def test_error_without_message_uses_explicit_fallback():
    result = discover(
        catcher_composition=(
            CanonicalCatcherObservationComposition(
                status="error",
                error_type="RuntimeError",
            )
        ),
    )

    assert result.status == "error"
    assert result.error_message == (
        "catcher observation composition failed"
    )


def test_invalid_composition_contract_fails_open():
    result = (
        discover_composed_canonical_baserunning_evidence(
            required_runner_ids=("runner",),
            required_pitcher_ids=("pitcher",),
            runner_observations=(runner(),),
            pitcher_observations=(pitcher(),),
            catcher_composition=object(),
        )
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.requested_runner_count == 1
    assert result.requested_pitcher_count == 1
    assert result.error_message == (
        "catcher_composition must be "
        "CanonicalCatcherObservationComposition"
    )


def test_missing_runner_evidence_remains_unavailable():
    result = (
        discover_composed_canonical_baserunning_evidence(
            required_runner_ids=("runner",),
            required_pitcher_ids=("pitcher",),
            runner_observations=(),
            pitcher_observations=(pitcher(),),
            catcher_composition=ready_composition(),
        )
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.available_runner_count == 0


def test_same_composition_produces_same_catalog():
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
