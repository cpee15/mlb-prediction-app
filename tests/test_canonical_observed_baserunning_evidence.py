from mlb_app.simulation.shadow import (
    CANONICAL_OBSERVED_BASERUNNING_DIGEST_VERSION,
    CanonicalCatcherBaserunningObservation,
    CanonicalPitcherBaserunningObservation,
    CanonicalRunnerBaserunningObservation,
    discover_observed_canonical_baserunning_evidence,
)


def runner(
    runner_id="runner",
):
    return CanonicalRunnerBaserunningObservation(
        runner_id=runner_id,
        eligible_opportunities=20,
        stolen_bases=6,
        caught_stealing=2,
        speed_score=0.90,
        lead_quality=0.80,
        fatigue_index=0.10,
    )


def pitcher(
    pitcher_id="pitcher",
):
    return CanonicalPitcherBaserunningObservation(
        pitcher_id=pitcher_id,
        eligible_pickoff_opportunities=25,
        pickoff_attempts=5,
        successful_pickoffs=1,
        hold_score=0.75,
        delivery_time_score=0.70,
    )


def catcher(
    *,
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


def discover(**overrides):
    values = {
        "required_runner_ids": ("runner",),
        "required_pitcher_ids": ("pitcher",),
        "runner_observations": (runner(),),
        "pitcher_observations": (pitcher(),),
        "away_catcher_observation": catcher(
            catcher_id="away-catcher",
            team_side="away",
        ),
        "home_catcher_observation": catcher(
            catcher_id="home-catcher",
            team_side="home",
        ),
    }
    values.update(overrides)

    return discover_observed_canonical_baserunning_evidence(
        **values
    )


def test_complete_observations_build_ready_catalog():
    result = discover()

    assert result.status == "ready"
    assert result.ready is True
    assert result.catalog is not None

    runner_profile = result.catalog.runners[0]
    pitcher_profile = result.catalog.pitchers[0]

    assert runner_profile.runner_id == "runner"
    assert runner_profile.attempt_rate == 0.4
    assert runner_profile.success_rate == 0.75

    assert pitcher_profile.pitcher_id == "pitcher"
    assert pitcher_profile.pickoff_attempt_rate == 0.2
    assert pitcher_profile.pickoff_success_rate == 0.2

    assert result.catalog.away_catcher.catcher_id == (
        "away-catcher"
    )
    assert (
        result.catalog.away_catcher.throwing_score
        == 0.3
    )
    assert result.catalog.home_catcher.catcher_id == (
        "home-catcher"
    )


def test_required_identity_order_is_preserved():
    result = discover(
        required_runner_ids=(
            "runner-2",
            "runner-1",
        ),
        runner_observations=(
            runner("runner-1"),
            runner("runner-2"),
        ),
    )

    assert result.status == "ready"
    assert result.catalog is not None
    assert tuple(
        profile.runner_id
        for profile in result.catalog.runners
    ) == (
        "runner-2",
        "runner-1",
    )


def test_missing_observation_remains_unavailable():
    result = discover(
        runner_observations=(),
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None
    assert result.requested_runner_count == 1
    assert result.available_runner_count == 0
    assert result.error_message is None


def test_missing_catcher_remains_unavailable():
    result = discover(
        away_catcher_observation=None,
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.catalog is None


def test_invalid_observation_contract_fails_open():
    result = discover(
        runner_observations=(object(),),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.requested_runner_count == 1
    assert result.requested_pitcher_count == 1
    assert result.error_message == (
        "observation must be a "
        "CanonicalRunnerBaserunningObservation"
    )


def test_invalid_catcher_side_fails_open_during_discovery():
    result = discover(
        away_catcher_observation=catcher(
            catcher_id="home-catcher-as-away",
            team_side="home",
        ),
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.catalog is None
    assert result.error_message == (
        "away_catcher must use away team side"
    )


def test_same_observations_produce_same_catalog_digest():
    first = discover()
    second = discover()

    assert first.catalog is not None
    assert second.catalog is not None
    assert first.catalog.digest == second.catalog.digest



def test_complete_observations_expose_digest_provenance():
    result = discover()

    assert result.observation_digest is not None
    assert len(result.observation_digest) == 64
    assert (
        result.to_diagnostics()["observation_digest"]
        == result.observation_digest
    )


def test_same_observations_produce_same_observation_digest():
    first = discover()
    second = discover()

    assert first.observation_digest is not None
    assert (
        first.observation_digest
        == second.observation_digest
    )


def test_changed_observation_changes_observation_digest():
    first = discover()
    changed = discover(
        runner_observations=(
            CanonicalRunnerBaserunningObservation(
                runner_id="runner",
                eligible_opportunities=20,
                stolen_bases=5,
                caught_stealing=2,
                speed_score=0.90,
                lead_quality=0.80,
                fatigue_index=0.10,
            ),
        ),
    )

    assert first.observation_digest is not None
    assert changed.observation_digest is not None
    assert (
        first.observation_digest
        != changed.observation_digest
    )


def test_incomplete_observations_preserve_digest():
    result = discover(
        runner_observations=(),
    )

    assert result.status == "unavailable"
    assert result.observation_digest is not None


def test_invalid_observations_do_not_claim_digest():
    result = discover(
        runner_observations=(object(),),
    )

    assert result.status == "error"
    assert result.observation_digest is None


def test_observation_digest_version_is_explicit():
    assert (
        CANONICAL_OBSERVED_BASERUNNING_DIGEST_VERSION
        == "canonical_observed_baserunning_digest_v1"
    )
