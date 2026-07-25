import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_PA_WORKSPACE_RECONSTRUCTION_VERSION,
    REQUIRED_WORKSPACE_MODELS,
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
    CanonicalHistoricalProbabilityGameStatistics,
    CanonicalHistoricalProbabilityPlayerStatistics,
    CanonicalHistoricalProbabilityStatisticsWindow,
    discover_canonical_shadow_probability_provider,
    reconstruct_historical_pa_probability_workspaces,
)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64
DIGEST_E = "e" * 64
DIGEST_F = "f" * 64


def roster_game():
    return CanonicalHistoricalLineupBullpenGameSnapshot(
        game_pk=1,
        game_date="2026-04-20",
        away_lineup_ids=tuple(
            str(value)
            for value in range(1, 10)
        ),
        home_lineup_ids=tuple(
            str(value)
            for value in range(11, 20)
        ),
        away_bullpen_ids=("22", "23"),
        home_bullpen_ids=("32", "33"),
        lineup_digest=DIGEST_A,
        bullpen_digest=DIGEST_B,
    )


def roster_window():
    return CanonicalHistoricalLineupBullpenWindow(
        observed_window_digest=DIGEST_C,
        games=(roster_game(),),
        digest=DIGEST_D,
    )


def hitting(player_id, sample=True):
    value = 10 if sample else 0

    return CanonicalHistoricalProbabilityPlayerStatistics(
        player_id=player_id,
        role="hitting",
        counts=(
            ("pa", value),
            ("ab", value),
            ("hits", 3 if sample else 0),
            ("double", 1 if sample else 0),
            ("triple", 0),
            ("hr", 1 if sample else 0),
            ("bb", 1 if sample else 0),
            ("k", 2 if sample else 0),
            ("hbp", 0),
        ),
        sample_available=sample,
    )


def pitching(player_id, sample=True):
    value = 10 if sample else 0

    return CanonicalHistoricalProbabilityPlayerStatistics(
        player_id=player_id,
        role="pitching",
        counts=(
            ("batters_faced", value),
            ("ab", value),
            ("hits", 2 if sample else 0),
            ("double", 1 if sample else 0),
            ("triple", 0),
            ("hr", 1 if sample else 0),
            ("bb", 1 if sample else 0),
            ("k", 3 if sample else 0),
            ("hbp", 0),
        ),
        sample_available=sample,
    )


def statistics_window(*, zero_sample=False):
    players = tuple(
        hitting(
            str(value),
            sample=not (
                zero_sample
                and value == 1
            ),
        )
        for value in range(1, 20)
    ) + tuple(
        pitching(str(value))
        for value in (
            20,
            22,
            23,
            30,
            32,
            33,
        )
    )

    game = CanonicalHistoricalProbabilityGameStatistics(
        game_pk=1,
        game_date="2026-04-20",
        statistics_through_date="2026-04-19",
        players=players,
        snapshot_digest=DIGEST_E,
    )

    return CanonicalHistoricalProbabilityStatisticsWindow(
        observed_window_digest=DIGEST_C,
        lineup_bullpen_window_digest=DIGEST_D,
        games=(game,),
        digest=DIGEST_F,
    )


def starters():
    return {1: ("20", "30")}


def reconstruct(*, zero_sample=False):
    return reconstruct_historical_pa_probability_workspaces(
        lineup_bullpen=roster_window(),
        statistics=statistics_window(
            zero_sample=zero_sample
        ),
        starting_pitcher_ids=starters(),
    )


def test_reconstructs_all_four_workspace_models():
    result = reconstruct()
    workspace = result.games[0].workspace

    assert tuple(workspace) == (
        REQUIRED_WORKSPACE_MODELS
    )
    assert all(
        tuple(model["probabilities"]) == (
            "k",
            "bb",
            "hbp",
            "single",
            "double",
            "triple",
            "hr",
            "reached_on_error",
            "out",
        )
        for model in workspace.values()
    )


def test_reconstructed_provider_is_ready():
    result = reconstruct()
    discovery = (
        discover_canonical_shadow_probability_provider(
            workspace=result.games[0].workspace
        )
    )

    assert discovery.ready is True
    assert discovery.provider is not None
    assert result.provider_identity == (
        discovery.provider.identity
    )
    assert result.model_versions == (
        "pa_outcome_v1",
    )


def test_zero_sample_player_uses_model_fallbacks():
    result = reconstruct(zero_sample=True)

    assert result.games[0].workspace[
        "awayPAOutcomeModel"
    ]["probabilities"]
    assert result.games[0].provider_identity


def test_environment_policy_is_explicitly_neutral():
    result = reconstruct()

    for model in result.games[0].workspace.values():
        assert model[
            "historical_environment_policy"
        ] == (
            "neutral_environment_"
            "no_archived_forecast_v1"
        )
        assert model["inputs_used"][
            "hr_boost_index"
        ] == 1.0


def test_statistics_window_must_match_rosters():
    statistics = statistics_window()

    mismatched = (
        CanonicalHistoricalProbabilityStatisticsWindow(
            observed_window_digest=DIGEST_A,
            lineup_bullpen_window_digest=DIGEST_D,
            games=statistics.games,
            digest=DIGEST_F,
        )
    )

    with pytest.raises(
        ValueError,
        match=(
            "statistics observed window "
            "must match rosters"
        ),
    ):
        reconstruct_historical_pa_probability_workspaces(
            lineup_bullpen=roster_window(),
            statistics=mismatched,
            starting_pitcher_ids=starters(),
        )


def test_starters_must_exactly_cover_games():
    with pytest.raises(
        ValueError,
        match=(
            "starters must exactly cover historical games"
        ),
    ):
        reconstruct_historical_pa_probability_workspaces(
            lineup_bullpen=roster_window(),
            statistics=statistics_window(),
            starting_pitcher_ids={},
        )


def test_missing_required_player_statistics_fail():
    statistics = statistics_window()
    game = statistics.games[0]

    missing = CanonicalHistoricalProbabilityGameStatistics(
        game_pk=game.game_pk,
        game_date=game.game_date,
        statistics_through_date=(
            game.statistics_through_date
        ),
        players=tuple(
            value
            for value in game.players
            if value.player_id != "1"
        ),
        snapshot_digest=game.snapshot_digest,
    )
    window = (
        CanonicalHistoricalProbabilityStatisticsWindow(
            observed_window_digest=(
                statistics.observed_window_digest
            ),
            lineup_bullpen_window_digest=(
                statistics.lineup_bullpen_window_digest
            ),
            games=(missing,),
            digest=statistics.digest,
        )
    )

    with pytest.raises(
        ValueError,
        match=(
            "historical statistics missing required "
            "hitting players"
        ),
    ):
        reconstruct_historical_pa_probability_workspaces(
            lineup_bullpen=roster_window(),
            statistics=window,
            starting_pitcher_ids=starters(),
        )


def test_reconstruction_is_deterministic():
    first = reconstruct()
    second = reconstruct()

    assert first == second
    assert first.digest == second.digest


def test_diagnostics_preserve_shadow_authority():
    diagnostics = reconstruct().to_diagnostics()

    assert diagnostics["ready"] is True
    assert diagnostics["game_count"] == 1
    assert diagnostics[
        "exact_artifacts_built"
    ] is False
    assert diagnostics[
        "fallback_catalogs_built"
    ] is False
    assert diagnostics[
        "historical_replay_executed"
    ] is False
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"
    assert diagnostics[
        "probability_records_exposed"
    ] is False


def test_reconstruction_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_PA_WORKSPACE_RECONSTRUCTION_VERSION
        == (
            "canonical_historical_pa_workspace_"
            "reconstruction_v1"
        )
    )
