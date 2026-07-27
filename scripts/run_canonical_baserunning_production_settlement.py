#!/usr/bin/env python3
"""Settle completed canonical baserunning production observations."""

from __future__ import annotations

import json
import os
import sys
import time
from copy import deepcopy
from typing import Any, Callable, Dict, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from mlb_app.database import (
    create_tables,
    get_engine,
    get_session,
)
from mlb_app.simulation.shadow import (
    load_canonical_baserunning_production_settlements,
    load_pending_canonical_baserunning_production_observations,
    materialize_canonical_baserunning_production_settlements,
    source_mlb_play_by_play_baserunning_window,
    summarize_canonical_baserunning_production_settlements,
)


SCHEDULE_URL = (
    "https://statsapi.mlb.com/api/v1/schedule"
)
GAME_FEED_URL = (
    "https://statsapi.mlb.com/api/v1.1/game/"
    "{game_pk}/feed/live"
)
USER_AGENT = (
    "mlb-prediction-app/"
    "canonical-baserunning-settlement-v1"
)
MAX_ATTEMPTS = 4
TIMEOUT_SECONDS = 30


def fetch_json(
    url: str,
    *,
    max_attempts: int = MAX_ATTEMPTS,
    timeout_seconds: int = TIMEOUT_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> Mapping[str, Any]:
    """Fetch one JSON payload with bounded exponential retry."""

    if max_attempts <= 0:
        raise ValueError(
            "max_attempts must be positive"
        )
    if timeout_seconds <= 0:
        raise ValueError(
            "timeout_seconds must be positive"
        )

    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            request = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": USER_AGENT,
                },
            )
            with urlopen(
                request,
                timeout=timeout_seconds,
            ) as response:
                payload = json.load(response)

            if not isinstance(payload, Mapping):
                raise TypeError(
                    "MLB response must be a mapping"
                )
            return payload
        except (
            HTTPError,
            URLError,
            TimeoutError,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc

            if attempt == max_attempts:
                break

            delay = 2 ** (attempt - 1)
            print(
                (
                    "canonical_settlement_source_retry "
                    f"attempt={attempt} "
                    f"delay_seconds={delay} "
                    f"error={type(exc).__name__}"
                ),
                file=sys.stderr,
            )
            sleep(delay)

    raise RuntimeError(
        "canonical settlement source unavailable"
    ) from last_error


def _schedule_url(
    start_date: str,
    end_date: str,
) -> str:
    return (
        SCHEDULE_URL
        + "?"
        + urlencode(
            {
                "sportId": 1,
                "startDate": start_date,
                "endDate": end_date,
            }
        )
    )


def filter_schedule_to_pending_final_games(
    schedule: Mapping[str, Any],
    *,
    pending_game_ids: set[int],
) -> Dict[str, Any]:
    """Keep only final games represented by pending observations."""

    if not isinstance(schedule, Mapping):
        raise TypeError(
            "schedule must be a mapping"
        )

    filtered_dates = []

    for raw_date in schedule.get("dates") or []:
        if not isinstance(raw_date, Mapping):
            raise TypeError(
                "schedule date rows must be mappings"
            )

        games = []
        for raw_game in raw_date.get("games") or []:
            if not isinstance(raw_game, Mapping):
                raise TypeError(
                    "schedule games must be mappings"
                )

            try:
                game_pk = int(
                    raw_game.get("gamePk")
                )
            except (TypeError, ValueError):
                continue

            status = raw_game.get("status") or {}
            abstract_state = str(
                status.get(
                    "abstractGameState",
                    "",
                )
            ).strip()

            if (
                game_pk in pending_game_ids
                and abstract_state == "Final"
            ):
                games.append(
                    deepcopy(dict(raw_game))
                )

        if games:
            date_row = deepcopy(dict(raw_date))
            date_row["games"] = games
            filtered_dates.append(date_row)

    return {"dates": filtered_dates}


def final_game_ids(
    schedule: Mapping[str, Any],
) -> tuple[int, ...]:
    return tuple(
        sorted(
            int(game["gamePk"])
            for date_row in (
                schedule.get("dates") or []
            )
            for game in (
                date_row.get("games") or []
            )
        )
    )


def run_canonical_baserunning_production_settlement(
    session,
    *,
    fetcher: Callable[
        [str],
        Mapping[str, Any],
    ] = fetch_json,
) -> Dict[str, Any]:
    """Fetch and settle all currently final pending games."""

    pending = (
        load_pending_canonical_baserunning_production_observations(
            session
        )
    )

    if not pending:
        rows = (
            load_canonical_baserunning_production_settlements(
                session
            )
        )
        return {
            "status": "complete",
            "pending_observation_count": 0,
            "final_game_count": 0,
            "created_game_ids": (),
            "reused_game_ids": (),
            "pending_game_ids": (),
            "summary": (
                summarize_canonical_baserunning_production_settlements(
                    rows
                )
            ),
        }

    start_date = min(
        row.game_date.isoformat()
        for row in pending
    )
    end_date = max(
        row.game_date.isoformat()
        for row in pending
    )
    pending_game_ids = {
        row.game_pk
        for row in pending
    }

    schedule = fetcher(
        _schedule_url(
            start_date,
            end_date,
        )
    )
    filtered_schedule = (
        filter_schedule_to_pending_final_games(
            schedule,
            pending_game_ids=pending_game_ids,
        )
    )
    game_ids = final_game_ids(
        filtered_schedule
    )

    if not game_ids:
        rows = (
            load_canonical_baserunning_production_settlements(
                session
            )
        )
        return {
            "status": "waiting_for_final_games",
            "window_start": start_date,
            "window_end": end_date,
            "pending_observation_count": len(
                pending
            ),
            "final_game_count": 0,
            "created_game_ids": (),
            "reused_game_ids": (),
            "pending_game_ids": tuple(
                sorted(pending_game_ids)
            ),
            "summary": (
                summarize_canonical_baserunning_production_settlements(
                    rows
                )
            ),
        }

    game_feeds = {
        game_pk: fetcher(
            GAME_FEED_URL.format(
                game_pk=game_pk
            )
        )
        for game_pk in game_ids
    }

    observed = (
        source_mlb_play_by_play_baserunning_window(
            schedule=filtered_schedule,
            game_feeds=game_feeds,
            window_start=start_date,
            window_end=end_date,
        )
    )
    result = (
        materialize_canonical_baserunning_production_settlements(
            session,
            observed=observed,
        )
    )
    result["status"] = "settled"
    result["window_start"] = start_date
    result["window_end"] = end_date
    result["pending_observation_count"] = len(
        pending
    )
    result["final_game_count"] = len(
        game_ids
    )

    return result


def _database_url() -> str:
    value = os.getenv("DATABASE_URL", "").strip()

    if not value:
        raise RuntimeError(
            "DATABASE_URL is required for canonical "
            "production settlement"
        )

    if (
        value.startswith("sqlite")
        and os.getenv(
            "MLB_CANONICAL_SETTLEMENT_ALLOW_SQLITE",
            "",
        ).strip()
        != "1"
    ):
        raise RuntimeError(
            "canonical production settlement refuses "
            "SQLite unless "
            "MLB_CANONICAL_SETTLEMENT_ALLOW_SQLITE=1"
        )

    return value


def main() -> int:
    engine = get_engine(_database_url())
    create_tables(engine)
    session_factory = get_session(engine)

    with session_factory() as session:
        result = (
            run_canonical_baserunning_production_settlement(
                session
            )
        )
        session.commit()

    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
