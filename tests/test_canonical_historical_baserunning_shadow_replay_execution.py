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
    CANONICAL_HISTORICAL_BASERUNNING_SHADOW_REPLAY_VERSION,
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
    CanonicalHistoricalProbabilityArtifactGame,
    CanonicalHistoricalProbabilityArtifactWindow,
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
