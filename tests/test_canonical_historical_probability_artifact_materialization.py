import pytest

from mlb_app.simulation.pa_outcome_model import (
    build_pa_outcome_probabilities,
)
from mlb_app.simulation.game import (
    CanonicalProbabilityFallbackTier,
)
from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_PROBABILITY_ARTIFACT_MATERIALIZATION_VERSION,
    HISTORICAL_PROBABILITY_ARTIFACT_SOURCE,
    REQUIRED_WORKSPACE_MODELS,
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
    CanonicalHistoricalPaProbabilityWorkspaceGame,
    CanonicalHistoricalPaProbabilityWorkspaceWindow,
    CanonicalHistoricalProbabilityGameStatistics,
    CanonicalHistoricalProbabilityPlayerStatistics,
    CanonicalHistoricalProbabilityStatisticsWindow,
    materialize_historical_probability_artifacts,
)
from mlb_app.simulation.shadow.historical_probability_statistics_source import (
    HITTING_STAT_KEYS,
    PITCHING_STAT_KEYS,
)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64


def _counts(role, sample=True):
    keys = (
        HITTING_STAT_KEYS
        if role == "hitting"
        else PITCHING_STAT_KEYS
    )

    if not sample:
        return tuple(
            (key, 0)
            for key, _ in keys
        )

    values = {
        "pa": 100,
        "ab": 90,
        "hits": 24,
        "double": 5,
        "triple": 1,
        "hr": 4,
        "bb": 8,
        "k": 20,
        "hbp": 2,
        "batters_faced": 100,
    }

    return tuple(
        (key, values.get(key, 0))
        for key, _ in keys
    )


def _player(player_id, role, sample=True):
    return CanonicalHistoricalProbabilityPlayerStatistics(
        player_id=str(player_id),
        role=role,
        counts=_counts(role, sample),
        sample_available=sample,
    )


def _lineups():
    return CanonicalHistoricalLineupBullpenWindow(
        observed_window_digest=DIGEST_A,
        games=(
            CanonicalHistoricalLineupBullpenGameSnapshot(
                game_pk=123,
                game_date="2026-04-20",
                away_lineup_ids=tuple(
                    str(value)
                    for value in range(101, 110)
                ),
                home_lineup_ids=tuple(
                    str(value)
                    for value in range(201, 210)
                ),
                away_bullpen_ids=("302",),
                home_bullpen_ids=("402",),
                lineup_digest=DIGEST_B,
                bullpen_digest=DIGEST_C,
            ),
        ),
        digest=DIGEST_D,
    )


def _statistics(
    *,
    zero_sample=(),
    omit=(),
):
    players = []

    for player_id in (
        *range(101, 110),
        *range(201, 210),
    ):
        key = ("hitting", str(player_id))
        if key not in omit:
            players.append(
                _player(
                    player_id,
                    "hitting",
                    key not in zero_sample,
                )
            )

    for player_id in (301, 302, 401, 402):
        key = ("pitching", str(player_id))
        if key not in omit:
            players.append(
                _player(
                    player_id,
                    "pitching",
                    key not in zero_sample,
                )
            )

    return CanonicalHistoricalProbabilityStatisticsWindow(
        observed_window_digest=DIGEST_A,
        lineup_bullpen_window_digest=DIGEST_D,
        games=(
            CanonicalHistoricalProbabilityGameStatistics(
                game_pk=123,
                game_date="2026-04-20",
                statistics_through_date="2026-04-19",
                players=tuple(players),
                snapshot_digest=DIGEST_B,
            ),
        ),
        digest=DIGEST_C,
    )


def _workspaces():
    model = {
        **build_pa_outcome_probabilities(
            batter_profile=None,
            pitcher_profile=None,
            environment_profile=None,
        ),
        "historical_environment_policy": (
            "neutral_environment_no_archived_forecast_v1"
        ),
        "historical_reconstruction": True,
    }
    workspace = {
        key: dict(model)
        for key in REQUIRED_WORKSPACE_MODELS
    }

    return CanonicalHistoricalPaProbabilityWorkspaceWindow(
        observed_window_digest=DIGEST_A,
        statistics_window_digest=DIGEST_C,
        games=(
            CanonicalHistoricalPaProbabilityWorkspaceGame(
                game_pk=123,
                game_date="2026-04-20",
                statistics_through_date="2026-04-19",
                statistics_snapshot_digest=DIGEST_B,
                workspace=workspace,
                provider_identity=(
                    "model_projections_pa_outcome:"
                    "pa_outcome_v1"
                ),
                digest=DIGEST_D,
            ),
        ),
        digest=DIGEST_B,
    )


def _materialize(
    *,
    statistics=None,
    workspaces=None,
    starters=None,
):
    return materialize_historical_probability_artifacts(
        lineup_bullpen=_lineups(),
        statistics=statistics or _statistics(),
        workspaces=workspaces or _workspaces(),
        starting_pitcher_ids=(
            {123: ("301", "401")}
            if starters is None
            else starters
        ),
    )


def test_materializes_all_exact_matchups_and_global_fallback():
    result = _materialize()
    game = result.games[0]

    assert result.game_count == 1
    assert result.possible_matchup_count == 36
    assert result.exact_record_count == 36
    assert result.zero_sample_matchup_count == 0

    assert len(game.fallback_catalog.records) == 1
    fallback = game.fallback_catalog.records[0]
    assert fallback.tier is (
        CanonicalProbabilityFallbackTier.GLOBAL
    )
    assert fallback.identity is None

    game.exact_artifact.record_for(
        batter_id="101",
        pitcher_id="401",
    )
    game.exact_artifact.record_for(
        batter_id="101",
        pitcher_id="402",
    )
    game.exact_artifact.record_for(
        batter_id="201",
        pitcher_id="301",
    )
    game.exact_artifact.record_for(
        batter_id="201",
        pitcher_id="302",
    )


def test_zero_sample_matchups_are_not_labeled_exact():
    result = _materialize(
        statistics=_statistics(
            zero_sample=(("hitting", "101"),),
        )
    )
    game = result.games[0]

    assert game.possible_matchup_count == 36
    assert game.exact_record_count == 34
    assert game.zero_sample_matchup_count == 2

    with pytest.raises(KeyError):
        game.exact_artifact.record_for(
            batter_id="101",
            pitcher_id="401",
        )

    diagnostics = game.to_diagnostics()
    assert diagnostics[
        "zero_sample_rows_labeled_exact"
    ] is False


def test_zero_sample_pitcher_removes_its_matchups():
    result = _materialize(
        statistics=_statistics(
            zero_sample=(("pitching", "402"),),
        )
    )

    assert result.exact_record_count == 27
    assert result.zero_sample_matchup_count == 9


def test_inventory_records_are_ready_and_date_bounded():
    result = _materialize()
    records = result.to_inventory_records()

    assert len(records) == 1
    record = records[0]

    assert record.ready is True
    assert record.source == (
        HISTORICAL_PROBABILITY_ARTIFACT_SOURCE
    )
    assert record.artifact_as_of_date == record.game_date
    assert record.future_data_rejected is False
    assert record.provider_ready is True
    assert record.exact_artifact_ready is True
    assert record.fallback_catalog_ready is True


def test_materialization_is_deterministic():
    first = _materialize()
    second = _materialize()

    assert first == second
    assert first.digest == second.digest
    assert (
        first.games[0].exact_artifact.digest
        == second.games[0].exact_artifact.digest
    )
    assert (
        first.games[0].fallback_catalog.digest
        == second.games[0].fallback_catalog.digest
    )


def test_missing_required_statistics_fail_closed():
    with pytest.raises(
        ValueError,
        match="required pitcher",
    ):
        _materialize(
            statistics=_statistics(
                omit=(("pitching", "402"),),
            )
        )


def test_starters_must_exactly_cover_games():
    with pytest.raises(
        ValueError,
        match="exactly cover",
    ):
        _materialize(starters={})


def test_diagnostics_preserve_shadow_authority():
    diagnostics = _materialize().to_diagnostics()

    assert diagnostics["ready"] is True
    assert diagnostics["exact_artifacts_built"] is True
    assert diagnostics["fallback_catalogs_built"] is True
    assert diagnostics["historical_replay_executed"] is False
    assert diagnostics["historical_replay_permitted"] is False
    assert diagnostics["production_activation"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_materialization_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_PROBABILITY_ARTIFACT_MATERIALIZATION_VERSION
        == "canonical_historical_probability_artifact_materialization_v1"
    )
