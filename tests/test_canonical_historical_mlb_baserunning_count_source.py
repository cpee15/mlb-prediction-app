import hashlib

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_MLB_BASERUNNING_COUNT_SOURCE_VERSION,
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
    source_historical_mlb_baserunning_counts,
)


DIGEST = hashlib.sha256(b"fixture").hexdigest()


def lineup_window():
    return CanonicalHistoricalLineupBullpenWindow(
        observed_window_digest=DIGEST,
        games=(
            CanonicalHistoricalLineupBullpenGameSnapshot(
                game_pk=11,
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
                home_bullpen_ids=("102",),
                lineup_digest=DIGEST,
                bullpen_digest=DIGEST,
            ),
        ),
        digest=DIGEST,
    )


def split(player_id, role):
    if role == "hitting":
        stats = {
            "hits": 10,
            "baseOnBalls": 4,
            "hitByPitch": 1,
            "homeRuns": 2,
            "stolenBases": (
                3 if player_id == 1 else 0
            ),
            "caughtStealing": (
                1 if player_id == 1 else 0
            ),
        }
    else:
        stats = {
            "battersFaced": 100,
            "stolenBases": 4,
            "caughtStealing": 1,
        }

    return {
        "player": {"id": player_id},
        "stat": stats,
    }


def payload(role, player_ids):
    return {
        "stats": [
            {
                "splits": [
                    split(player_id, role)
                    for player_id in player_ids
                ]
            }
        ]
    }


def source():
    hitter_ids = tuple(range(1, 10)) + tuple(
        range(11, 20)
    )
    pitcher_ids = (100, 103, 101, 102)

    return source_historical_mlb_baserunning_counts(
        lineup_bullpen=lineup_window(),
        starting_pitcher_ids={
            11: ("100", "103"),
        },
        starting_catcher_ids={
            11: ("2", "12"),
        },
        statistics_payloads={
            "2026-04-19": {
                "hitting": payload(
                    "hitting",
                    hitter_ids,
                ),
                "pitching": payload(
                    "pitching",
                    pitcher_ids,
                ),
            }
        },
        pitcher_pickoffs_by_cutoff={
            "2026-04-19": {
                str(value): 1
                for value in pitcher_ids
            }
        },
        catcher_outcomes_by_cutoff={
            "2026-04-19": {
                "2": (8, 2),
                "12": (6, 3),
            }
        },
    )


def test_sources_complete_guarded_window():
    result = source()
    diagnostics = result.to_diagnostics()
    game = result.games[0]

    assert result.ready is True
    assert result.game_count == 1
    assert len(game.catalog.runners) == 18
    assert len(game.catalog.pitchers) == 4
    assert game.catalog.away_catcher.catcher_id == "2"
    assert game.catalog.home_catcher.catcher_id == "12"
    assert game.statistics_through_date == "2026-04-19"
    assert diagnostics["target_game_outcomes_used"] is False
    assert diagnostics["future_data_permitted"] is False
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_runner_counts_use_prior_statistics():
    result = source()
    runner = result.games[0].catalog.runners[0]

    assert runner.runner_id == "1"
    assert runner.attempt_rate == pytest.approx(
        4 / 13
    )
    assert runner.success_rate == pytest.approx(
        3 / 4
    )


def test_inputs_require_exact_cutoff_coverage():
    with pytest.raises(
        ValueError,
        match="exactly match required prior-day cutoffs",
    ):
        source_historical_mlb_baserunning_counts(
            lineup_bullpen=lineup_window(),
            starting_pitcher_ids={
                11: ("100", "103"),
            },
            starting_catcher_ids={
                11: ("2", "12"),
            },
            statistics_payloads={},
            pitcher_pickoffs_by_cutoff={},
            catcher_outcomes_by_cutoff={},
        )


def test_pickoffs_require_all_pitchers():
    hitter_ids = tuple(range(1, 10)) + tuple(
        range(11, 20)
    )
    pitcher_ids = (100, 103, 101, 102)

    with pytest.raises(
        ValueError,
        match="every required pitcher",
    ):
        source_historical_mlb_baserunning_counts(
            lineup_bullpen=lineup_window(),
            starting_pitcher_ids={
                11: ("100", "103"),
            },
            starting_catcher_ids={
                11: ("2", "12"),
            },
            statistics_payloads={
                "2026-04-19": {
                    "hitting": payload(
                        "hitting",
                        hitter_ids,
                    ),
                    "pitching": payload(
                        "pitching",
                        pitcher_ids,
                    ),
                }
            },
            pitcher_pickoffs_by_cutoff={
                "2026-04-19": {
                    "100": 1,
                }
            },
            catcher_outcomes_by_cutoff={
                "2026-04-19": {
                    "2": (8, 2),
                    "12": (6, 3),
                }
            },
        )


def test_source_is_deterministic():
    first = source()
    second = source()

    assert first == second
    assert first.digest == second.digest
    assert (
        first.to_diagnostics()
        == second.to_diagnostics()
    )


def test_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_MLB_BASERUNNING_COUNT_SOURCE_VERSION
        == "canonical_historical_mlb_baserunning_count_source_v1"
    )
