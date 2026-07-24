"""Compare Statcast descriptions with MLB play-by-play outcomes."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import requests
from pybaseball import statcast

from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_SMOKE_WINDOW_END,
    CANONICAL_BASERUNNING_SMOKE_WINDOW_START,
    source_mlb_play_by_play_baserunning_window,
    source_statcast_baserunning_window,
)


_SCHEDULE_URL = (
    "https://statsapi.mlb.com/api/v1/schedule"
)
_FEED_URL = (
    "https://statsapi.mlb.com/api/v1.1/game/"
    "{game_pk}/feed/live"
)


def _json(url, *, params=None):
    response = requests.get(
        url,
        params=params,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def _completed_game_ids(schedule):
    return tuple(
        sorted(
            {
                int(game["gamePk"])
                for date_row in schedule["dates"]
                for game in date_row["games"]
                if game["status"][
                    "abstractGameState"
                ]
                == "Final"
            }
        )
    )


def _feed(game_pk):
    return (
        game_pk,
        _json(
            _FEED_URL.format(
                game_pk=game_pk
            )
        ),
    )


def main() -> None:
    frame = statcast(
        start_dt=(
            CANONICAL_BASERUNNING_SMOKE_WINDOW_START
        ),
        end_dt=(
            CANONICAL_BASERUNNING_SMOKE_WINDOW_END
        ),
    )
    statcast_source = (
        source_statcast_baserunning_window(
            rows=frame.to_dict(
                orient="records"
            ),
            window_start=(
                CANONICAL_BASERUNNING_SMOKE_WINDOW_START
            ),
            window_end=(
                CANONICAL_BASERUNNING_SMOKE_WINDOW_END
            ),
        )
    )

    schedule = _json(
        _SCHEDULE_URL,
        params={
            "sportId": 1,
            "startDate": (
                CANONICAL_BASERUNNING_SMOKE_WINDOW_START
            ),
            "endDate": (
                CANONICAL_BASERUNNING_SMOKE_WINDOW_END
            ),
        },
    )
    game_ids = _completed_game_ids(
        schedule
    )

    with ThreadPoolExecutor(
        max_workers=8
    ) as executor:
        feeds = dict(
            executor.map(
                _feed,
                game_ids,
            )
        )

    play_by_play = (
        source_mlb_play_by_play_baserunning_window(
            schedule=schedule,
            game_feeds=feeds,
            window_start=(
                CANONICAL_BASERUNNING_SMOKE_WINDOW_START
            ),
            window_end=(
                CANONICAL_BASERUNNING_SMOKE_WINDOW_END
            ),
        )
    )

    statcast_diagnostics = (
        statcast_source.snapshot.to_diagnostics()
    )
    observed = play_by_play.to_diagnostics()

    comparison = {
        "window_start": (
            CANONICAL_BASERUNNING_SMOKE_WINDOW_START
        ),
        "window_end": (
            CANONICAL_BASERUNNING_SMOKE_WINDOW_END
        ),
        "statcast_description": (
            statcast_diagnostics
        ),
        "mlb_play_by_play": observed,
        "statcast_event_coverage_rate": round(
            statcast_diagnostics["outcome_count"]
            / observed["event_count"],
            6,
        ),
        "statcast_stolen_base_coverage_rate": round(
            statcast_diagnostics["stolen_bases"]
            / observed["stolen_bases"],
            6,
        ),
        "statcast_caught_stealing_coverage_rate": (
            round(
                statcast_diagnostics[
                    "caught_stealing"
                ]
                / observed["caught_stealing"],
                6,
            )
        ),
        "statcast_description_coverage_complete": (
            False
        ),
        "calibration_observed_source": (
            observed["schema_version"]
        ),
        "production_activation": False,
        "production_authority_changed": False,
        "authoritative_source": "legacy",
    }

    print(
        json.dumps(
            comparison,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
