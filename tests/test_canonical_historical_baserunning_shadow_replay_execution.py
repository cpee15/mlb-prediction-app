import hashlib

import pytest

from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalBaserunningEvidenceCatalog,
    CanonicalCatcherBaserunningProfile,
    CanonicalOutcomeProbability,
    CanonicalPitcherBaserunningProfile,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityArtifactRecord,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
    CanonicalRunnerBaserunningProfile,
)
from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVALUATION_VERSION,
    CANONICAL_HISTORICAL_BASERUNNING_SHADOW_REPLAY_VERSION,
    HISTORICAL_BASERUNNING_REPLAY_REVIEW_POLICY_VERSION,
    CanonicalBaserunningCalibrationPolicy,
    CanonicalMlbPlayByPlayBaserunningGame,
    CanonicalMlbPlayByPlayBaserunningSnapshot,
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
    CanonicalHistoricalProbabilityArtifactGame,
    CanonicalHistoricalProbabilityArtifactWindow,
    build_historical_baserunning_replay_review_policy,
    evaluate_historical_baserunning_shadow_replays,
    execute_historical_baserunning_shadow_replays,
    source_historical_baserunning_replay_evidence,
)


DIGEST = hashlib.sha256(b"fixture").hexdigest()

PROVIDER = CanonicalProbabilityProviderIdentity(
    provider_name="model_projections_pa_outcome",
    provider_version="pa_outcome_v1",
)


def probability_points():
    values = {
        "out": 0.43,
        "single": 0.15,
        "double": 0.05,
        "triple": 0.005,
        "hr": 0.03,
        "bb": 0.085,
        "hbp": 0.01,
        "k": 0.24,
    }

    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=values[outcome.value],
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


def lineup_window():
    return CanonicalHistoricalLineupBullpenWindow(
        observed_window_digest=DIGEST,
        games=(
            CanonicalHistoricalLineupBullpenGameSnapshot(
                game_pk=123,
                game_date="2026-04-20",
                away_lineup_ids=tuple(
                    str(value)
                    for value in range(1, 10)
                ),
                home_lineup_ids=tuple(
                    str(value)
                    for value in range(11, 20)
                ),
                away_bullpen_ids=("101",),
                home_bullpen_ids=("201",),
                lineup_digest=DIGEST,
                bullpen_digest=DIGEST,
            ),
        ),
        digest=DIGEST,
    )


def probability_window(
    *,
    observed_window_digest=DIGEST,
):
    records = tuple(
        CanonicalProbabilityArtifactRecord(
            batter_id=batter_id,
            pitcher_id=pitcher_id,
            probabilities=probability_points(),
        )
        for batter_id, pitcher_id in (
            *(
                (str(value), "200")
                for value in range(1, 10)
            ),
            *(
                (str(value), "100")
                for value in range(11, 20)
            ),
        )
    )
    artifact = CanonicalProbabilityArtifact(
        provider=PROVIDER,
        records=records,
    )
    fallback = CanonicalProbabilityFallbackCatalog(
        provider=PROVIDER,
        records=(
            CanonicalProbabilityFallbackRecord(
                tier=(
                    CanonicalProbabilityFallbackTier
                    .GLOBAL
                ),
                identity=None,
                probabilities=probability_points(),
            ),
        ),
    )

    return CanonicalHistoricalProbabilityArtifactWindow(
        observed_window_digest=(
            observed_window_digest
        ),
        statistics_window_digest=DIGEST,
        workspace_window_digest=DIGEST,
        games=(
            CanonicalHistoricalProbabilityArtifactGame(
                game_pk=123,
                game_date="2026-04-20",
                statistics_snapshot_digest=DIGEST,
                workspace_digest=DIGEST,
                exact_artifact=artifact,
                fallback_catalog=fallback,
                possible_matchup_count=18,
                zero_sample_matchup_count=0,
                digest=DIGEST,
            ),
        ),
        digest=DIGEST,
    )


def baserunning_catalog():
    return CanonicalBaserunningEvidenceCatalog(
        runners=tuple(
            CanonicalRunnerBaserunningProfile(
                runner_id=str(value),
                speed_score=0.50,
                attempt_rate=0.05,
                success_rate=0.75,
                lead_quality=0.50,
                fatigue_index=0.0,
            )
            for value in (
                *range(1, 10),
                *range(11, 20),
            )
        ),
        pitchers=tuple(
            CanonicalPitcherBaserunningProfile(
                pitcher_id=str(value),
                hold_score=0.50,
                delivery_time_score=0.50,
                pickoff_attempt_rate=0.02,
                pickoff_success_rate=0.10,
            )
            for value in (
                100,
                101,
                200,
                201,
            )
        ),
        away_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="3",
            team_side="away",
            throwing_score=0.50,
            pop_time_score=0.50,
        ),
        home_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="13",
            team_side="home",
            throwing_score=0.50,
            pop_time_score=0.50,
        ),
    )


def evidence_window():
    lineups = lineup_window()

    return source_historical_baserunning_replay_evidence(
        lineup_bullpen=lineups,
        catalogs={
            123: baserunning_catalog(),
        },
        statistics_through_dates={
            123: "2026-04-19",
        },
        evidence_counts={
            123: (10, 5, 1),
        },
    )


def execute(
    *,
    lineups=None,
    probabilities=None,
    evidence=None,
    starters=None,
):
    return execute_historical_baserunning_shadow_replays(
        lineup_bullpen=(
            lineups or lineup_window()
        ),
        probability_artifacts=(
            probabilities or probability_window()
        ),
        baserunning_evidence=(
            evidence or evidence_window()
        ),
        starting_pitcher_ids=(
            {123: ("100", "200")}
            if starters is None
            else starters
        ),
        simulation_count=2,
    )


def test_executes_real_historical_shadow_replay():
    result = execute()
    game = result.games[0]
    diagnostics = result.to_diagnostics()

    assert result.ready is True
    assert result.game_count == 1
    assert result.executed_game_count == 1
    assert result.blocked_game_count == 0
    assert result.error_game_count == 0

    assert game.executed is True
    assert game.execution.material is not None
    assert game.execution.execution_inputs is not None
    assert game.execution.simulation_count == 2

    assert diagnostics[
        "historical_replay_executed"
    ] is True
    assert diagnostics[
        "external_fetch_performed"
    ] is False
    assert diagnostics[
        "persistence_performed"
    ] is False
    assert diagnostics["production_activation"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics[
        "authoritative_source"
    ] == "legacy"


def test_baserunning_catalog_is_attached():
    result = execute()
    game = result.games[0]

    assert (
        game.execution.execution_inputs
        .baserunning_evidence_catalog_digest
        == baserunning_catalog().digest
    )
    assert len(game.output_digest) == 64
    assert len(game.replay_digest) == 64
    assert len(result.digest) == 64


def test_inputs_must_exactly_cover_games():
    with pytest.raises(
        ValueError,
        match="starting pitchers must exactly cover",
    ):
        execute(starters={})


def test_observed_window_digests_must_match():
    with pytest.raises(
        ValueError,
        match="observed window digests must match",
    ):
        execute(
            probabilities=probability_window(
                observed_window_digest="b" * 64,
            )
        )


def test_replay_is_deterministic():
    first = execute()
    second = execute()

    assert first.digest == second.digest
    assert (
        first.to_diagnostics()
        == second.to_diagnostics()
    )


def test_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_SHADOW_REPLAY_VERSION
        == "canonical_historical_baserunning_shadow_replay_v1"
    )


def observed_snapshot(
    *,
    game_pk=123,
    game_date="2026-04-20",
):
    return CanonicalMlbPlayByPlayBaserunningSnapshot(
        window_start="2026-04-20",
        window_end="2026-04-20",
        games=(
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=game_pk,
                game_date=game_date,
                stolen_bases=1,
                caught_stealing=1,
            ),
        ),
        event_count=2,
        stolen_bases=1,
        caught_stealing=1,
        duplicate_event_record_count=0,
        digest=DIGEST,
    )


def evaluation_policy():
    return CanonicalBaserunningCalibrationPolicy(
        minimum_game_count=1,
        maximum_stolen_base_error_per_game=10.0,
        maximum_caught_stealing_error_per_game=10.0,
        maximum_attempt_error_per_game=20.0,
        maximum_success_rate_absolute_error=1.0,
        policy_version=(
            "historical_baserunning_replay_test_policy_v1"
        ),
    )


def evaluate():
    return evaluate_historical_baserunning_shadow_replays(
        replays=execute(),
        observed=observed_snapshot(),
        policy=evaluation_policy(),
    )


def test_evaluates_replay_against_observed_outcomes():
    result = evaluate()
    diagnostics = result.to_diagnostics()

    assert result.ready is True
    assert result.game_count == 1
    assert result.observed_stolen_bases == 1
    assert result.observed_caught_stealing == 1
    assert result.artifact.ready is True

    assert diagnostics["observed_attempts"] == 2
    assert diagnostics["report"]["ready"] is True
    assert diagnostics["activation_permitted"] is False
    assert diagnostics["production_activation"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics[
        "authoritative_source"
    ] == "legacy"


def test_evaluation_requires_exact_observed_coverage():
    with pytest.raises(
        ValueError,
        match=(
            "replay games must exactly match "
            "observed games"
        ),
    ):
        evaluate_historical_baserunning_shadow_replays(
            replays=execute(),
            observed=observed_snapshot(
                game_pk=999,
            ),
            policy=evaluation_policy(),
        )


def test_evaluation_is_deterministic():
    first = evaluate()
    second = evaluate()

    assert first.digest == second.digest
    assert (
        first.to_diagnostics()
        == second.to_diagnostics()
    )


def test_evaluation_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVALUATION_VERSION
        == (
            "canonical_historical_baserunning_"
            "replay_evaluation_v1"
        )
    )


def test_review_policy_is_explicit_and_non_authoritative():
    policy = (
        build_historical_baserunning_replay_review_policy()
    )

    assert policy.minimum_game_count == 150
    assert (
        policy.policy_version
        == HISTORICAL_BASERUNNING_REPLAY_REVIEW_POLICY_VERSION
    )
    assert (
        HISTORICAL_BASERUNNING_REPLAY_REVIEW_POLICY_VERSION
        == "historical_baserunning_replay_review_policy_v1"
    )
