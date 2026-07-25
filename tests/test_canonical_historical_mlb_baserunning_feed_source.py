import hashlib

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_MLB_BASERUNNING_FEED_SOURCE_VERSION,
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
    source_historical_mlb_baserunning_feed_evidence,
)


DIGEST = hashlib.sha256(b"fixture").hexdigest()


def player(
    player_id,
    position,
    batting_order=None,
    *,
    pickoffs=0,
    stolen_bases=0,
    caught_stealing=0,
    all_positions=None,
):
    value = {
        "person": {"id": player_id},
        "position": {
            "abbreviation": position,
        },
        "stats": {
            "pitching": (
                {"pickoffs": pickoffs}
                if position == "P"
                else {}
            ),
            "fielding": (
                {
                    "stolenBases": stolen_bases,
                    "caughtStealing": caught_stealing,
                }
                if position == "C"
                else {}
            ),
        },
    }

    if batting_order is not None:
        value["battingOrder"] = batting_order

    if all_positions is not None:
        value["allPositions"] = [
            {"abbreviation": abbreviation}
            for abbreviation in all_positions
        ]

    return value


def feed(
    official_date,
    away_players,
    home_players,
):
    return {
        "gameData": {
            "datetime": {
                "officialDate": official_date,
            }
        },
        "liveData": {
            "boxscore": {
                "teams": {
                    "away": {
                        "players": away_players,
                    },
                    "home": {
                        "players": home_players,
                    },
                }
            }
        },
    }


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


def target_feed():
    return feed(
        "2026-04-20",
        {
            "ID2": player(
                2,
                "C",
                "200",
            ),
        },
        {
            "ID12": player(
                12,
                "C",
                "200",
            ),
        },
    )


def prior_feed(
    official_date="2026-04-19",
):
    return feed(
        official_date,
        {
            "ID100": player(
                100,
                "P",
                pickoffs=2,
            ),
            "ID2": player(
                2,
                "C",
                stolen_bases=4,
                caught_stealing=1,
            ),
        },
        {
            "ID103": player(
                103,
                "P",
                pickoffs=1,
            ),
            "ID12": player(
                12,
                "C",
                stolen_bases=3,
                caught_stealing=2,
            ),
        },
    )


def source():
    return source_historical_mlb_baserunning_feed_evidence(
        lineup_bullpen=lineup_window(),
        starting_pitcher_ids={
            11: ("100", "103"),
        },
        target_game_feeds={
            11: target_feed(),
        },
        prior_game_feeds_by_cutoff={
            "2026-04-19": {
                10: prior_feed(),
            }
        },
    )


def test_sources_identity_and_prior_outcomes():
    result = source()
    diagnostics = result.to_diagnostics()

    assert result.starting_catcher_ids == {
        11: ("2", "12"),
    }
    assert (
        result.pitcher_pickoffs_by_cutoff[
            "2026-04-19"
        ]["100"]
        == 2
    )
    assert (
        result.pitcher_pickoffs_by_cutoff[
            "2026-04-19"
        ]["101"]
        == 0
    )
    assert (
        result.catcher_outcomes_by_cutoff[
            "2026-04-19"
        ]["2"]
        == (4, 1)
    )
    assert diagnostics[
        "starting_catcher_identity_from_target_feed"
    ] is True
    assert diagnostics["target_game_outcomes_used"] is False
    assert diagnostics["prior_feed_outcomes_only"] is True
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_future_feed_is_rejected():
    with pytest.raises(
        ValueError,
        match="cannot exceed cutoff",
    ):
        source_historical_mlb_baserunning_feed_evidence(
            lineup_bullpen=lineup_window(),
            starting_pitcher_ids={
                11: ("100", "103"),
            },
            target_game_feeds={
                11: target_feed(),
            },
            prior_game_feeds_by_cutoff={
                "2026-04-19": {
                    10: prior_feed(
                        "2026-04-20"
                    ),
                }
            },
        )


def test_starting_catcher_is_required():
    invalid = target_feed()
    invalid["liveData"]["boxscore"]["teams"][
        "away"
    ]["players"]["ID2"]["battingOrder"] = "201"

    with pytest.raises(
        ValueError,
        match="exactly one starting catcher",
    ):
        source_historical_mlb_baserunning_feed_evidence(
            lineup_bullpen=lineup_window(),
            starting_pitcher_ids={
                11: ("100", "103"),
            },
            target_game_feeds={
                11: invalid,
            },
            prior_game_feeds_by_cutoff={
                "2026-04-19": {
                    10: prior_feed(),
                }
            },
        )


def test_starting_catcher_uses_first_recorded_position():
    target = target_feed()
    target["liveData"]["boxscore"]["teams"][
        "home"
    ]["players"]["ID20"] = player(
        20,
        "C",
        "100",
        all_positions=("DH", "C"),
    )

    result = (
        source_historical_mlb_baserunning_feed_evidence(
            lineup_bullpen=lineup_window(),
            starting_pitcher_ids={
                11: ("100", "103"),
            },
            target_game_feeds={
                11: target,
            },
            prior_game_feeds_by_cutoff={
                "2026-04-19": {
                    10: prior_feed(),
                }
            },
        )
    )

    assert result.starting_catcher_ids == {
        11: ("2", "12"),
    }


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
        CANONICAL_HISTORICAL_MLB_BASERUNNING_FEED_SOURCE_VERSION
        == "canonical_historical_mlb_baserunning_feed_source_v1"
    )
