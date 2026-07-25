import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_PROBABILITY_STATISTICS_SOURCE_VERSION,
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
    define_historical_probability_reconstruction_inputs,
    source_historical_probability_statistics,
)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64


def roster_game(
    *,
    game_pk=1,
    game_date="2026-04-20",
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
        away_bullpen_ids=("22", "23"),
        home_bullpen_ids=("32", "33"),
        lineup_digest=DIGEST_A,
        bullpen_digest=DIGEST_B,
    )


def roster_window(*games):
    return CanonicalHistoricalLineupBullpenWindow(
        observed_window_digest=DIGEST_C,
        games=tuple(games or (roster_game(),)),
        digest=DIGEST_D,
    )


def hitting_stat(value=1):
    return {
        "plateAppearances": value,
        "atBats": value,
        "hits": value,
        "doubles": 0,
        "triples": 0,
        "homeRuns": 0,
        "baseOnBalls": 0,
        "strikeOuts": 0,
        "hitByPitch": 0,
    }


def pitching_stat(value=1):
    return {
        "battersFaced": value,
        "atBats": value,
        "hits": 0,
        "doubles": 0,
        "triples": 0,
        "homeRuns": 0,
        "baseOnBalls": 0,
        "strikeOuts": value,
        "hitBatsmen": 0,
    }


def payload(role, player_ids):
    builder = (
        hitting_stat
        if role == "hitting"
        else pitching_stat
    )

    return {
        "stats": [
            {
                "splits": [
                    {
                        "player": {
                            "id": int(player_id)
                        },
                        "stat": builder(),
                    }
                    for player_id in player_ids
                ]
            }
        ]
    }


def payloads(
    *,
    cutoff="2026-04-19",
    hitter_ids=tuple(
        str(value)
        for value in range(1, 20)
    ),
    pitcher_ids=(
        "20",
        "21",
        "22",
        "23",
        "30",
        "31",
        "32",
        "33",
    ),
):
    return {
        cutoff: {
            "hitting": payload(
                "hitting",
                hitter_ids,
            ),
            "pitching": payload(
                "pitching",
                pitcher_ids,
            ),
        }
    }


def starters():
    return {1: ("20", "30")}


def test_complete_source_is_ready():
    result = source_historical_probability_statistics(
        lineup_bullpen=roster_window(),
        starting_pitcher_ids=starters(),
        statistics_payloads=payloads(),
    )

    assert result.game_count == 1
    assert result.zero_sample_count == 0
    assert result.games[0].observed_sample_count == 24
    assert result.games[0].zero_sample_count == 0


def test_absent_player_is_explicit_zero_sample():
    result = source_historical_probability_statistics(
        lineup_bullpen=roster_window(),
        starting_pitcher_ids=starters(),
        statistics_payloads=payloads(
            hitter_ids=tuple(
                str(value)
                for value in range(1, 19)
            ),
        ),
    )

    missing = [
        value
        for value in result.games[0].players
        if value.player_id == "19"
    ]

    assert len(missing) == 1
    assert missing[0].sample_available is False
    assert all(
        value == 0
        for _, value in missing[0].counts
    )
    assert result.zero_sample_count == 1


def test_starters_and_bullpens_are_required():
    result = source_historical_probability_statistics(
        lineup_bullpen=roster_window(),
        starting_pitcher_ids=starters(),
        statistics_payloads=payloads(),
    )

    pitcher_ids = {
        value.player_id
        for value in result.games[0].players
        if value.role == "pitching"
    }

    assert pitcher_ids == {
        "20",
        "22",
        "23",
        "30",
        "32",
        "33",
    }


def test_snapshot_makes_reconstruction_ready():
    statistics = (
        source_historical_probability_statistics(
            lineup_bullpen=roster_window(),
            starting_pitcher_ids=starters(),
            statistics_payloads=payloads(),
        )
    )

    reconstruction = (
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window(),
            statistics_snapshots=(
                statistics.to_reconstruction_snapshots()
            ),
        )
    )

    assert reconstruction.ready is True
    assert reconstruction.ready_game_count == 1


def test_doubleheader_games_share_prior_day_cutoff():
    window = roster_window(
        roster_game(),
        roster_game(
            game_pk=2,
            game_date="2026-04-20",
        ),
    )

    result = source_historical_probability_statistics(
        lineup_bullpen=window,
        starting_pitcher_ids={
            1: ("20", "30"),
            2: ("20", "30"),
        },
        statistics_payloads=payloads(),
    )

    assert tuple(
        value.statistics_through_date
        for value in result.games
    ) == (
        "2026-04-19",
        "2026-04-19",
    )


def test_missing_cutoff_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "statistics payload dates must exactly match"
        ),
    ):
        source_historical_probability_statistics(
            lineup_bullpen=roster_window(),
            starting_pitcher_ids=starters(),
            statistics_payloads={},
        )


def test_missing_group_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "each cutoff requires hitting and pitching"
        ),
    ):
        source_historical_probability_statistics(
            lineup_bullpen=roster_window(),
            starting_pitcher_ids=starters(),
            statistics_payloads={
                "2026-04-19": {
                    "hitting": payload(
                        "hitting",
                        ("1",),
                    ),
                }
            },
        )


def test_unknown_starter_game_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "starting pitchers contain unknown games"
        ),
    ):
        source_historical_probability_statistics(
            lineup_bullpen=roster_window(),
            starting_pitcher_ids={
                1: ("20", "30"),
                99: ("20", "30"),
            },
            statistics_payloads=payloads(),
        )


def test_missing_required_stat_key_is_rejected():
    invalid = payloads()
    del invalid["2026-04-19"]["hitting"][
        "stats"
    ][0]["splits"][0]["stat"][
        "plateAppearances"
    ]

    with pytest.raises(
        ValueError,
        match=(
            "hitting statistics missing "
            "plateAppearances"
        ),
    ):
        source_historical_probability_statistics(
            lineup_bullpen=roster_window(),
            starting_pitcher_ids=starters(),
            statistics_payloads=invalid,
        )


def test_order_and_digest_are_deterministic():
    first = source_historical_probability_statistics(
        lineup_bullpen=roster_window(),
        starting_pitcher_ids=starters(),
        statistics_payloads=payloads(),
    )
    second = source_historical_probability_statistics(
        lineup_bullpen=roster_window(),
        starting_pitcher_ids=starters(),
        statistics_payloads=payloads(
            hitter_ids=tuple(
                reversed(
                    tuple(
                        str(value)
                        for value in range(1, 20)
                    )
                )
            ),
        ),
    )

    assert first == second
    assert first.digest == second.digest


def test_diagnostics_preserve_shadow_authority():
    diagnostics = (
        source_historical_probability_statistics(
            lineup_bullpen=roster_window(),
            starting_pitcher_ids=starters(),
            statistics_payloads=payloads(),
        ).to_diagnostics()
    )

    assert diagnostics["ready"] is True
    assert diagnostics["future_data_permitted"] is False
    assert diagnostics[
        "doubleheader_same_cutoff"
    ] is True
    assert diagnostics[
        "probability_workspace_reconstructed"
    ] is False
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"
    assert diagnostics[
        "player_identifiers_exposed"
    ] is False


def test_source_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_PROBABILITY_STATISTICS_SOURCE_VERSION
        == (
            "canonical_historical_probability_"
            "statistics_source_v1"
        )
    )
