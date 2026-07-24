from mlb_app.simulation.game import (
    CanonicalCatcherBaserunningProfile,
    CanonicalPitcherBaserunningProfile,
    CanonicalRunnerBaserunningProfile,
)
from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_BASERUNNING_DISCOVERY_VERSION,
    discover_canonical_shadow_baserunning_evidence,
)


def runner(player_id):
    return CanonicalRunnerBaserunningProfile(
        runner_id=player_id,
        speed_score=0.70,
        attempt_rate=0.20,
        success_rate=0.80,
        lead_quality=0.65,
        fatigue_index=0.10,
    )


def pitcher(player_id):
    return CanonicalPitcherBaserunningProfile(
        pitcher_id=player_id,
        hold_score=0.55,
        delivery_time_score=0.45,
        pickoff_attempt_rate=0.08,
        pickoff_success_rate=0.02,
    )


def catcher(player_id, side):
    return CanonicalCatcherBaserunningProfile(
        catcher_id=player_id,
        team_side=side,
        throwing_score=0.60,
        pop_time_score=0.55,
    )


def test_complete_evidence_builds_ready_catalog():
    discovery = (
        discover_canonical_shadow_baserunning_evidence(
            required_runner_ids=(
                "away-runner",
                "home-runner",
            ),
            required_pitcher_ids=(
                "away-pitcher",
                "home-pitcher",
            ),
            runner_profiles=(
                runner("home-runner"),
                runner("away-runner"),
            ),
            pitcher_profiles=(
                pitcher("home-pitcher"),
                pitcher("away-pitcher"),
            ),
            away_catcher=catcher(
                "away-catcher",
                "away",
            ),
            home_catcher=catcher(
                "home-catcher",
                "home",
            ),
        )
    )

    assert discovery.ready is True
    assert discovery.status == "ready"
    assert discovery.catalog is not None
    assert tuple(
        profile.runner_id
        for profile in discovery.catalog.runners
    ) == (
        "away-runner",
        "home-runner",
    )
    assert len(discovery.catalog.digest) == 64

    diagnostics = discovery.to_diagnostics()

    assert diagnostics["ready"] is True
    assert diagnostics["catalog_digest"] == (
        discovery.catalog.digest
    )
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_missing_evidence_fails_open_without_catalog():
    discovery = (
        discover_canonical_shadow_baserunning_evidence(
            required_runner_ids=(
                "away-runner",
                "home-runner",
            ),
            required_pitcher_ids=(
                "away-pitcher",
            ),
            runner_profiles=(
                runner("away-runner"),
            ),
            pitcher_profiles=(
                pitcher("away-pitcher"),
            ),
        )
    )

    assert discovery.ready is False
    assert discovery.status == "unavailable"
    assert discovery.catalog is None
    assert discovery.requested_runner_count == 2
    assert discovery.available_runner_count == 1
    assert discovery.requested_pitcher_count == 1
    assert discovery.available_pitcher_count == 1


def test_invalid_identity_contract_fails_open():
    discovery = (
        discover_canonical_shadow_baserunning_evidence(
            required_runner_ids=(
                "duplicate",
                "duplicate",
            ),
            required_pitcher_ids=(
                "pitcher",
            ),
        )
    )

    assert discovery.ready is False
    assert discovery.status == "error"
    assert discovery.catalog is None
    assert discovery.error_message == (
        "runner identifiers must be unique"
    )


def test_wrong_catcher_side_fails_open():
    discovery = (
        discover_canonical_shadow_baserunning_evidence(
            required_runner_ids=("runner",),
            required_pitcher_ids=("pitcher",),
            runner_profiles=(runner("runner"),),
            pitcher_profiles=(pitcher("pitcher"),),
            away_catcher=catcher(
                "away-catcher",
                "home",
            ),
            home_catcher=catcher(
                "home-catcher",
                "home",
            ),
        )
    )

    assert discovery.ready is False
    assert discovery.status == "error"
    assert discovery.catalog is None
    assert discovery.error_message == (
        "away_catcher must use away team side"
    )


def test_discovery_version_is_explicit():
    discovery = (
        discover_canonical_shadow_baserunning_evidence(
            required_runner_ids=(),
            required_pitcher_ids=(),
        )
    )

    assert discovery.discovery_version == (
        CANONICAL_SHADOW_BASERUNNING_DISCOVERY_VERSION
    )



def test_discovery_diagnostics_default_observation_digest():
    discovery = (
        discover_canonical_shadow_baserunning_evidence(
            required_runner_ids=(),
            required_pitcher_ids=(),
        )
    )

    assert discovery.observation_digest is None
    assert (
        discovery.to_diagnostics()["observation_digest"]
        is None
    )
