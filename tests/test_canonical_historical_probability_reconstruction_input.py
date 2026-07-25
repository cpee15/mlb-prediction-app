import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_PROBABILITY_RECONSTRUCTION_INPUT_VERSION,
    HISTORICAL_PROBABILITY_STATISTICS_SOURCE,
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
    CanonicalHistoricalProbabilityStatisticsSnapshot,
    define_historical_probability_reconstruction_inputs,
)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64
DIGEST_E = "e" * 64


def roster_game(
    *,
    game_pk=1,
    game_date="2026-04-20",
    lineup_digest=DIGEST_A,
    bullpen_digest=DIGEST_B,
):
    return CanonicalHistoricalLineupBullpenGameSnapshot(
        game_pk=game_pk,
        game_date=game_date,
        away_lineup_ids=tuple(
            str(value)
            for value in range(1, 10)
        ),
        home_lineup_ids=tuple(
            str(value)
            for value in range(11, 20)
        ),
        away_bullpen_ids=("21", "22"),
        home_bullpen_ids=("31", "32"),
        lineup_digest=lineup_digest,
        bullpen_digest=bullpen_digest,
    )


def roster_window(*games):
    values = games or (roster_game(),)

    return CanonicalHistoricalLineupBullpenWindow(
        observed_window_digest=DIGEST_C,
        games=tuple(values),
        digest=DIGEST_D,
    )


def statistics(
    *,
    game_pk=1,
    game_date="2026-04-20",
    statistics_through_date="2026-04-19",
    snapshot_digest=DIGEST_E,
):
    return CanonicalHistoricalProbabilityStatisticsSnapshot(
        game_pk=game_pk,
        game_date=game_date,
        statistics_through_date=(
            statistics_through_date
        ),
        source_version=(
            HISTORICAL_PROBABILITY_STATISTICS_SOURCE
        ),
        snapshot_digest=snapshot_digest,
    )


def test_complete_inputs_are_ready():
    result = (
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(),
            statistics_snapshots=(statistics(),),
        )
    )

    assert result.ready is True
    assert result.game_count == 1
    assert result.ready_game_count == 1
    assert result.blocked_game_count == 0
    assert result.inputs[0].leakage_safe is True
    assert result.inputs[0].missing_requirements == ()


def test_missing_statistics_remain_explicit():
    result = (
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(),
        )
    )

    assert result.ready is False
    assert result.ready_game_count == 0
    assert result.blocked_game_count == 1
    assert result.inputs[0].missing_requirements == (
        "missing_historical_statistics_snapshot",
    )


def test_sparse_statistics_preserve_exact_game_window():
    result = (
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(
                roster_game(),
                roster_game(
                    game_pk=2,
                    game_date="2026-04-21",
                ),
            ),
            statistics_snapshots=(statistics(),),
        )
    )

    assert tuple(
        value.game_pk
        for value in result.inputs
    ) == (1, 2)
    assert result.game_count == 2
    assert result.ready_game_count == 1
    assert result.blocked_game_count == 1


@pytest.mark.parametrize(
    "cutoff",
    (
        "2026-04-20",
        "2026-04-21",
    ),
)
def test_same_day_or_future_statistics_are_rejected(
    cutoff,
):
    with pytest.raises(
        ValueError,
        match=(
            "statistics_through_date must be "
            "before game_date"
        ),
    ):
        statistics(
            statistics_through_date=cutoff,
        )


def test_invalid_statistics_digest_is_rejected():
    with pytest.raises(
        ValueError,
        match="snapshot_digest must be a SHA256 digest",
    ):
        statistics(
            snapshot_digest="not-a-digest",
        )


def test_duplicate_statistics_games_are_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "statistics snapshot game identifiers "
            "must be unique"
        ),
    ):
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(),
            statistics_snapshots=(
                statistics(),
                statistics(),
            ),
        )


def test_unknown_statistics_game_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "statistics snapshots contain unknown games"
        ),
    ):
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(),
            statistics_snapshots=(
                statistics(game_pk=99),
            ),
        )


def test_statistics_game_date_must_match_roster():
    with pytest.raises(
        ValueError,
        match=(
            "statistics snapshot game_date must match"
        ),
    ):
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(),
            statistics_snapshots=(
                statistics(
                    game_date="2026-04-21",
                    statistics_through_date=(
                        "2026-04-19"
                    ),
                ),
            ),
        )


def test_order_and_digest_are_deterministic():
    first = (
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(
                roster_game(
                    game_pk=2,
                    game_date="2026-04-21",
                ),
                roster_game(),
            ),
            statistics_snapshots=(
                statistics(
                    game_pk=2,
                    game_date="2026-04-21",
                ),
                statistics(),
            ),
        )
    )
    second = (
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(
                roster_game(),
                roster_game(
                    game_pk=2,
                    game_date="2026-04-21",
                ),
            ),
            statistics_snapshots=(
                statistics(),
                statistics(
                    game_pk=2,
                    game_date="2026-04-21",
                ),
            ),
        )
    )

    assert first == second
    assert first.digest == second.digest
    assert tuple(
        value.game_pk
        for value in first.inputs
    ) == (1, 2)


def test_diagnostics_preserve_shadow_authority():
    diagnostics = (
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(),
        ).to_diagnostics()
    )

    assert diagnostics["ready"] is False
    assert diagnostics[
        "probability_workspace_reconstructed"
    ] is False
    assert diagnostics[
        "historical_replay_permitted"
    ] is False
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"
    assert diagnostics[
        "player_identifiers_exposed"
    ] is False
    assert diagnostics[
        "probability_records_exposed"
    ] is False


def test_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_PROBABILITY_RECONSTRUCTION_INPUT_VERSION
        == (
            "canonical_historical_probability_"
            "reconstruction_input_v1"
        )
    )
