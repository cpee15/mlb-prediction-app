import copy

import pytest

from mlb_app.simulation.game import (
    CanonicalBaserunningEvidenceCatalog,
    CanonicalCatcherBaserunningProfile,
    CanonicalPitcherBaserunningProfile,
    CanonicalRunnerBaserunningProfile,
)
from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_PRODUCTION_PRIOR_VERSION,
    CanonicalBaserunningProductionPrior,
    CanonicalBaserunningProductionPriorCatcher,
    CanonicalHistoricalBaserunningReplayEvidenceGame,
    CanonicalHistoricalBaserunningReplayEvidenceWindow,
    build_baserunning_production_prior,
    decode_baserunning_production_prior,
    load_baserunning_production_prior,
)


def prior():
    return CanonicalBaserunningProductionPrior(
        source_through_date="2026-07-25",
        runners=(
            CanonicalRunnerBaserunningProfile(
                runner_id="1",
                speed_score=0.6,
                attempt_rate=0.08,
                success_rate=0.78,
                lead_quality=0.7,
                fatigue_index=0.0,
            ),
            CanonicalRunnerBaserunningProfile(
                runner_id="2",
                speed_score=0.4,
                attempt_rate=0.04,
                success_rate=0.72,
                lead_quality=0.6,
                fatigue_index=0.0,
            ),
        ),
        pitchers=(
            CanonicalPitcherBaserunningProfile(
                pitcher_id="10",
                hold_score=0.55,
                delivery_time_score=0.55,
                pickoff_attempt_rate=0.02,
                pickoff_success_rate=0.10,
            ),
        ),
        catchers=(
            CanonicalBaserunningProductionPriorCatcher(
                catcher_id="20",
                throwing_score=0.30,
                pop_time_score=0.30,
            ),
            CanonicalBaserunningProductionPriorCatcher(
                catcher_id="21",
                throwing_score=0.25,
                pop_time_score=0.25,
            ),
        ),
        direct_evidence_count=5,
        proxy_evidence_count=8,
        fallback_evidence_count=0,
    )


def test_prior_round_trips_with_digest():
    source = prior()
    decoded = decode_baserunning_production_prior(
        source.to_payload()
    )

    assert decoded == source
    assert decoded.digest == source.digest


def test_prior_discovers_complete_matchup():
    source = prior()
    discovery = source.discover_matchup(
        required_runner_ids=("1", "2"),
        required_pitcher_ids=("10",),
        away_catcher_id="20",
        home_catcher_id="21",
    )

    assert discovery.ready is True
    assert discovery.catalog is not None
    assert discovery.catalog.away_catcher.team_side == (
        "away"
    )
    assert discovery.catalog.home_catcher.team_side == (
        "home"
    )
    assert discovery.observation_digest == source.digest


def test_missing_identity_is_unavailable():
    discovery = prior().discover_matchup(
        required_runner_ids=("1", "999"),
        required_pitcher_ids=("10",),
        away_catcher_id="20",
        home_catcher_id="21",
    )

    assert discovery.ready is False
    assert discovery.status == "unavailable"
    assert discovery.requested_runner_count == 2
    assert discovery.available_runner_count == 1


def test_tampered_artifact_is_rejected():
    payload = copy.deepcopy(prior().to_payload())
    payload["runners"][0]["attempt_rate"] = 0.99

    with pytest.raises(
        ValueError,
        match="digest mismatch",
    ):
        decode_baserunning_production_prior(
            payload
        )


def test_prior_is_deterministic():
    first = prior()
    second = prior()

    assert first == second
    assert first.digest == second.digest
    assert first.to_payload() == second.to_payload()


def test_version_is_explicit():
    assert (
        CANONICAL_BASERUNNING_PRODUCTION_PRIOR_VERSION
        == "canonical_baserunning_production_prior_v1"
    )


DIGEST = "a" * 64


def evidence_catalog(
    *,
    runner_attempt_rate,
    pitcher_hold_score,
    away_throwing_score,
):
    return CanonicalBaserunningEvidenceCatalog(
        runners=(
            CanonicalRunnerBaserunningProfile(
                runner_id="2",
                speed_score=0.5,
                attempt_rate=runner_attempt_rate,
                success_rate=0.75,
                lead_quality=0.65,
                fatigue_index=0.0,
            ),
            CanonicalRunnerBaserunningProfile(
                runner_id="1",
                speed_score=0.6,
                attempt_rate=0.08,
                success_rate=0.78,
                lead_quality=0.70,
                fatigue_index=0.0,
            ),
        ),
        pitchers=(
            CanonicalPitcherBaserunningProfile(
                pitcher_id="10",
                hold_score=pitcher_hold_score,
                delivery_time_score=0.55,
                pickoff_attempt_rate=0.02,
                pickoff_success_rate=0.10,
            ),
        ),
        away_catcher=(
            CanonicalCatcherBaserunningProfile(
                catcher_id="20",
                team_side="away",
                throwing_score=away_throwing_score,
                pop_time_score=away_throwing_score,
            )
        ),
        home_catcher=(
            CanonicalCatcherBaserunningProfile(
                catcher_id="21",
                team_side="home",
                throwing_score=0.25,
                pop_time_score=0.25,
            )
        ),
    )


def evidence_game(
    *,
    game_pk,
    game_date,
    cutoff,
    catalog,
    counts,
):
    return CanonicalHistoricalBaserunningReplayEvidenceGame(
        game_pk=game_pk,
        game_date=game_date,
        statistics_through_date=cutoff,
        catalog=catalog,
        evidence_digest=DIGEST,
        direct_evidence_count=counts[0],
        proxy_evidence_count=counts[1],
        fallback_evidence_count=counts[2],
    )


def evidence_window(*games):
    return CanonicalHistoricalBaserunningReplayEvidenceWindow(
        observed_window_digest=DIGEST,
        lineup_bullpen_window_digest=DIGEST,
        games=tuple(games),
        digest=DIGEST,
    )


def test_build_prior_uses_latest_cutoff_safe_profiles():
    earlier = evidence_game(
        game_pk=1,
        game_date="2026-04-20",
        cutoff="2026-04-19",
        catalog=evidence_catalog(
            runner_attempt_rate=0.04,
            pitcher_hold_score=0.40,
            away_throwing_score=0.20,
        ),
        counts=(3, 2, 1),
    )
    later = evidence_game(
        game_pk=2,
        game_date="2026-04-21",
        cutoff="2026-04-20",
        catalog=evidence_catalog(
            runner_attempt_rate=0.09,
            pitcher_hold_score=0.70,
            away_throwing_score=0.35,
        ),
        counts=(5, 4, 2),
    )

    result = build_baserunning_production_prior(
        evidence_window(later, earlier)
    )

    assert result.source_through_date == "2026-04-20"
    assert tuple(
        value.runner_id
        for value in result.runners
    ) == ("1", "2")
    assert result.runners[1].attempt_rate == 0.09
    assert result.pitchers[0].hold_score == 0.70
    assert result.catchers[0].catcher_id == "20"
    assert result.catchers[0].throwing_score == 0.35
    assert result.direct_evidence_count == 8
    assert result.proxy_evidence_count == 6
    assert result.fallback_evidence_count == 3


def test_build_prior_is_independent_of_window_order():
    earlier = evidence_game(
        game_pk=1,
        game_date="2026-04-20",
        cutoff="2026-04-19",
        catalog=evidence_catalog(
            runner_attempt_rate=0.04,
            pitcher_hold_score=0.40,
            away_throwing_score=0.20,
        ),
        counts=(3, 2, 1),
    )
    later = evidence_game(
        game_pk=2,
        game_date="2026-04-21",
        cutoff="2026-04-20",
        catalog=evidence_catalog(
            runner_attempt_rate=0.09,
            pitcher_hold_score=0.70,
            away_throwing_score=0.35,
        ),
        counts=(5, 4, 2),
    )

    first = build_baserunning_production_prior(
        evidence_window(earlier, later)
    )
    second = build_baserunning_production_prior(
        evidence_window(later, earlier)
    )

    assert first == second
    assert first.digest == second.digest
    assert first.to_payload() == second.to_payload()


def test_build_prior_rejects_noncanonical_evidence():
    with pytest.raises(
        TypeError,
        match="evidence must be",
    ):
        build_baserunning_production_prior(
            object()
        )



def test_prior_loader_verifies_checked_payload(
    tmp_path,
):
    artifact_path = tmp_path / "prior.json"
    artifact_path.write_text(
        __import__("json").dumps(
            prior().to_payload()
        ),
        encoding="utf-8",
    )

    load_baserunning_production_prior.cache_clear()
    loaded = load_baserunning_production_prior(
        artifact_path
    )

    assert loaded == prior()
    assert loaded.digest == prior().digest


def test_prior_loader_rejects_tampered_payload(
    tmp_path,
):
    payload = prior().to_payload()
    payload["runners"][0]["attempt_rate"] = 0.99
    artifact_path = tmp_path / "prior.json"
    artifact_path.write_text(
        __import__("json").dumps(payload),
        encoding="utf-8",
    )

    load_baserunning_production_prior.cache_clear()

    with pytest.raises(
        ValueError,
        match="digest mismatch",
    ):
        load_baserunning_production_prior(
            artifact_path
        )



def test_missing_identity_can_use_explicit_fallback():
    source = prior()

    first = source.discover_matchup(
        required_runner_ids=("1", "999"),
        required_pitcher_ids=("10", "998"),
        away_catcher_id="20",
        home_catcher_id="997",
        allow_fallback_profiles=True,
    )
    second = source.discover_matchup(
        required_runner_ids=("1", "999"),
        required_pitcher_ids=("10", "998"),
        away_catcher_id="20",
        home_catcher_id="997",
        allow_fallback_profiles=True,
    )

    assert first.ready is True
    assert first == second
    assert first.catalog is not None
    assert first.catalog.digest == second.catalog.digest
    assert first.observation_digest == (
        second.observation_digest
    )
    assert first.observation_digest != source.digest
    assert first.fallback_runner_count == 1
    assert first.fallback_pitcher_count == 1
    assert first.fallback_catcher_count == 1
    assert first.fallback_policy_version == (
        "baserunning_production_fallback_policy_v1"
    )

    runners = {
        value.runner_id: value
        for value in first.catalog.runners
    }
    pitchers = {
        value.pitcher_id: value
        for value in first.catalog.pitchers
    }

    assert runners["999"].attempt_rate == 0.05
    assert runners["999"].success_rate == 0.75
    assert runners["999"].lead_quality == 0.75
    assert pitchers["998"].hold_score == 0.5
    assert pitchers["998"].delivery_time_score == 0.5
    assert pitchers["998"].pickoff_attempt_rate == 0.0
    assert pitchers["998"].pickoff_success_rate == 0.10
    assert (
        first.catalog.home_catcher.throwing_score
        == 0.25
    )
    assert (
        first.catalog.home_catcher.pop_time_score
        == 0.25
    )

    diagnostics = first.to_diagnostics()
    assert diagnostics["fallback_evidence_count"] == 3
    assert diagnostics["fallback_runner_count"] == 1
    assert diagnostics["fallback_pitcher_count"] == 1
    assert diagnostics["fallback_catcher_count"] == 1


def test_fallback_profiles_must_be_explicitly_enabled():
    discovery = prior().discover_matchup(
        required_runner_ids=("1", "999"),
        required_pitcher_ids=("10",),
        away_catcher_id="20",
        home_catcher_id="21",
    )

    assert discovery.ready is False
    assert discovery.fallback_evidence_count == 0
