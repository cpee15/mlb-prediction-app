"""Report real historical lineup and bullpen smoke-window coverage."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json

import requests

from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_SMOKE_WINDOW_END,
    CANONICAL_BASERUNNING_SMOKE_WINDOW_START,
    report_historical_lineup_bullpen_coverage,
    source_historical_lineup_bullpen_snapshots,
    source_mlb_play_by_play_baserunning_window,
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
    game_ids = _completed_game_ids(schedule)

    with ThreadPoolExecutor(
        max_workers=8
    ) as executor:
        feeds = dict(
            executor.map(
                _feed,
                game_ids,
            )
        )

    observed = (
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
    source_window = (
        source_historical_lineup_bullpen_snapshots(
            observed=observed,
            game_feeds=feeds,
        )
    )
    report = (
        report_historical_lineup_bullpen_coverage(
            source_window
        )
    )

    print(
        json.dumps(
            report.to_diagnostics(),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
