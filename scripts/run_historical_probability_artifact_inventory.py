"""Inventory historical probability artifacts for the smoke window."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path

import requests

from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_SMOKE_WINDOW_END,
    CANONICAL_BASERUNNING_SMOKE_WINDOW_START,
    CanonicalHistoricalProbabilityArtifactRecord,
    inventory_historical_probability_artifacts,
    source_mlb_play_by_play_baserunning_window,
)


_SCHEDULE_URL = (
    "https://statsapi.mlb.com/api/v1/schedule"
)
_FEED_URL = (
    "https://statsapi.mlb.com/api/v1.1/game/"
    "{game_pk}/feed/live"
)
_DEFAULT_MANIFEST = Path(
    "data/historical_probability_artifacts.json"
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


def _manifest_records(path):
    if not path.exists():
        return ()

    payload = json.loads(
        path.read_text(encoding="utf-8")
    )
    rows = (
        payload.get("records", ())
        if isinstance(payload, dict)
        else payload
    )

    if not isinstance(rows, list):
        raise ValueError(
            "historical probability artifact manifest "
            "records must be a list"
        )

    return tuple(
        CanonicalHistoricalProbabilityArtifactRecord(
            game_pk=int(row["game_pk"]),
            game_date=str(row["game_date"]),
            source=str(
                row.get("source") or "unavailable"
            ),
            artifact_as_of_date=(
                row.get("artifact_as_of_date")
            ),
            provider_identity=(
                row.get("provider_identity")
            ),
            exact_artifact_digest=(
                row.get("exact_artifact_digest")
            ),
            fallback_catalog_digest=(
                row.get("fallback_catalog_digest")
            ),
        )
        for row in rows
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

    manifest_path = Path(
        os.getenv(
            "HISTORICAL_PROBABILITY_ARTIFACT_MANIFEST",
            str(_DEFAULT_MANIFEST),
        )
    )
    records = _manifest_records(manifest_path)

    inventory = (
        inventory_historical_probability_artifacts(
            observed=observed,
            artifacts=records,
        )
    )
    diagnostics = inventory.to_diagnostics()
    diagnostics["manifest_present"] = (
        manifest_path.exists()
    )
    diagnostics["manifest_record_count"] = len(
        records
    )
    diagnostics["database_artifact_row_count"] = 0
    diagnostics["in_memory_cache_historical_eligible"] = (
        False
    )

    print(
        json.dumps(
            diagnostics,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
