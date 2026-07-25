"""Run the real historical probability-statistics source."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import json
import sys
import time
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from mlb_app.simulation.shadow import (
    define_historical_probability_reconstruction_inputs,
    materialize_historical_probability_artifacts,
    reconstruct_historical_pa_probability_workspaces,
    source_historical_mlb_baserunning_counts,
    source_historical_mlb_baserunning_feed_evidence,
    source_historical_lineup_bullpen_snapshots,
    source_historical_probability_statistics,
    source_mlb_play_by_play_baserunning_window,
)
from mlb_app.simulation.shadow.statcast_baserunning_window_source import (
    CANONICAL_BASERUNNING_SMOKE_WINDOW_END,
    CANONICAL_BASERUNNING_SMOKE_WINDOW_START,
)


_SCHEDULE_URL = (
    "https://statsapi.mlb.com/api/v1/schedule"
)
_FEED_URL = (
    "https://statsapi.mlb.com/api/v1.1/game/"
    "{game_pk}/feed/live"
)
_STATS_URL = (
    "https://statsapi.mlb.com/api/v1/stats"
)
_FINAL_STATES = {
    "Final",
    "Game Over",
    "Completed Early",
}


def _json(url, params=None):
    if params:
        url = f"{url}?{urlencode(params)}"

    request = Request(
        url,
        headers={
            "User-Agent": (
                "mlb-prediction-app/"
                "historical-statistics-source"
            )
        },
    )

    maximum_attempts = 6

    for attempt in range(
        1,
        maximum_attempts + 1,
    ):
        try:
            with urlopen(
                request,
                timeout=120,
            ) as response:
                return json.load(response)
        except (
            URLError,
            TimeoutError,
            ConnectionError,
            json.JSONDecodeError,
        ) as error:
            if attempt == maximum_attempts:
                raise

            delay_seconds = min(
                2 ** (attempt - 1),
                16,
            )
            print(
                (
                    "historical_source_retry "
                    f"attempt={attempt} "
                    f"delay_seconds={delay_seconds} "
                    f"error={type(error).__name__}"
                ),
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay_seconds)

    raise RuntimeError(
        "historical source retry loop exhausted"
    )


def _completed_game_ids(schedule):
    games = {}

    for entry in schedule.get("dates") or []:
        for game in entry.get("games") or []:
            status = (
                (game.get("status") or {})
                .get("detailedState")
            )
            game_pk = game.get("gamePk")

            if (
                status in _FINAL_STATES
                and game_pk is not None
            ):
                games[int(game_pk)] = game

    return tuple(sorted(games))


def _feed(game_pk):
    return (
        game_pk,
        _json(
            _FEED_URL.format(
                game_pk=game_pk
            )
        ),
    )


def _feed_official_date(feed):
    return (
        (
            (feed.get("gameData") or {})
            .get("datetime")
            or {}
        ).get("officialDate")
    )


def _starter_id(team):
    pitchers = team.get("pitchers") or []
    if not pitchers:
        raise ValueError(
            "historical team has no starting pitcher"
        )

    return str(int(pitchers[0]))


def _starters(feeds):
    result = {}

    for game_pk, feed in feeds.items():
        boxscore = (
            (feed.get("liveData") or {})
            .get("boxscore")
            or {}
        )
        teams = boxscore.get("teams") or {}

        result[game_pk] = (
            _starter_id(teams.get("away") or {}),
            _starter_id(teams.get("home") or {}),
        )

    return result


def _stats(request):
    cutoff, group = request

    payload = _json(
        _STATS_URL,
        {
            "stats": "byDateRange",
            "group": group,
            "gameType": "R",
            "sportIds": 1,
            "playerPool": "ALL",
            "season": 2026,
            "startDate": "2026-03-01",
            "endDate": cutoff,
            "limit": 5000,
            "hydrate": "person",
        },
    )

    return cutoff, group, payload


def main():
    schedule = _json(
        _SCHEDULE_URL,
        {
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
    roster_window = (
        source_historical_lineup_bullpen_snapshots(
            observed=observed,
            game_feeds=feeds,
        )
    )

    cutoffs = sorted(
        {
            (
                date.fromisoformat(value.game_date)
                - timedelta(days=1)
            ).isoformat()
            for value in roster_window.games
        }
    )
    requests = tuple(
        (cutoff, group)
        for cutoff in cutoffs
        for group in ("hitting", "pitching")
    )

    payloads = {
        cutoff: {}
        for cutoff in cutoffs
    }

    with ThreadPoolExecutor(
        max_workers=6
    ) as executor:
        for cutoff, group, payload in executor.map(
            _stats,
            requests,
        ):
            payloads[cutoff][group] = payload

    history_schedule = _json(
        _SCHEDULE_URL,
        {
            "sportId": 1,
            "gameType": "R",
            "startDate": "2026-03-01",
            "endDate": max(cutoffs),
        },
    )
    history_game_ids = _completed_game_ids(
        history_schedule
    )

    with ThreadPoolExecutor(
        max_workers=8
    ) as executor:
        history_feeds = dict(
            executor.map(
                _feed,
                history_game_ids,
            )
        )

    prior_game_feeds_by_cutoff = {
        cutoff: {
            game_pk: feed
            for game_pk, feed in history_feeds.items()
            if (
                _feed_official_date(feed)
                and _feed_official_date(feed)
                <= cutoff
            )
        }
        for cutoff in cutoffs
    }

    starting_pitchers = _starters(feeds)

    feed_evidence = (
        source_historical_mlb_baserunning_feed_evidence(
            lineup_bullpen=roster_window,
            starting_pitcher_ids=starting_pitchers,
            target_game_feeds=feeds,
            prior_game_feeds_by_cutoff=(
                prior_game_feeds_by_cutoff
            ),
        )
    )

    baserunning_evidence = (
        source_historical_mlb_baserunning_counts(
            lineup_bullpen=roster_window,
            starting_pitcher_ids=starting_pitchers,
            starting_catcher_ids=(
                feed_evidence.starting_catcher_ids
            ),
            statistics_payloads=payloads,
            pitcher_pickoffs_by_cutoff=(
                feed_evidence
                .pitcher_pickoffs_by_cutoff
            ),
            catcher_outcomes_by_cutoff=(
                feed_evidence
                .catcher_outcomes_by_cutoff
            ),
        )
    )

    statistics = (
        source_historical_probability_statistics(
            lineup_bullpen=roster_window,
            starting_pitcher_ids=starting_pitchers,
            statistics_payloads=payloads,
        )
    )
    reconstruction = (
        define_historical_probability_reconstruction_inputs(
            lineup_bullpen=roster_window,
            statistics_snapshots=(
                statistics.to_reconstruction_snapshots()
            ),
        )
    )
    workspaces = (
        reconstruct_historical_pa_probability_workspaces(
            lineup_bullpen=roster_window,
            statistics=statistics,
            starting_pitcher_ids=_starters(feeds),
        )
    )

    artifacts = (
        materialize_historical_probability_artifacts(
            lineup_bullpen=roster_window,
            statistics=statistics,
            workspaces=workspaces,
            starting_pitcher_ids=_starters(feeds),
        )
    )

    diagnostics = {
        "statistics": statistics.to_diagnostics(),
        "reconstruction": (
            reconstruction.to_diagnostics()
        ),
        "workspaces": workspaces.to_diagnostics(),
        "artifacts": artifacts.to_diagnostics(),
        "baserunning_feed_evidence": (
            feed_evidence.to_diagnostics()
        ),
        "baserunning_replay_evidence": (
            baserunning_evidence.to_diagnostics()
        ),
        "history_feed_count": len(history_feeds),
        "cutoff_count": len(cutoffs),
        "request_count": len(requests),
        "historical_replay_executed": False,
        "production_activation": False,
        "production_authority_changed": False,
        "authoritative_source": "legacy",
    }

    print(
        json.dumps(
            diagnostics,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
