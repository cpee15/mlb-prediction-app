#!/usr/bin/env python3
"""Audit the official hitter speed snapshot acquisition contract."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import urllib.parse
import urllib.request
from typing import Any, Callable, Iterable

from mlb_app.simulation.shadow.hitter_speed_snapshot_acquisition_validation import (
    evaluate_hitter_speed_snapshot_acquisition_contract,
)


BASE_URL = (
    "https://baseballsavant.mlb.com/"
    "leaderboard/sprint_speed"
)
DEFAULT_SEASONS = (2024, 2025, 2026)


def build_acquisition_url(
    season: int,
    *,
    minimum_competitive_runs: int = 10,
) -> str:
    query = urllib.parse.urlencode({
        "year": season,
        "position": "",
        "team": "",
        "min": minimum_competitive_runs,
        "csv": "true",
    })
    return f"{BASE_URL}?{query}"


def fetch_response(
    url: str,
    *,
    timeout: float,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "mlb-prediction-app/"
                "hitter-speed-acquisition-audit"
            ),
            "Accept": "text/csv",
        },
        method="GET",
    )

    with urllib.request.urlopen(
        request,
        timeout=timeout,
    ) as response:
        body = response.read()
        headers = {
            key.lower(): value
            for key, value
            in response.headers.items()
        }
        return {
            "http_status": response.status,
            "headers": headers,
            "body": body,
            "final_url": response.geturl(),
        }


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _normalize_response(
    response: dict[str, Any],
    *,
    season: int,
) -> dict[str, Any]:
    body = response.get("body")

    if not isinstance(body, bytes):
        raise TypeError(
            "acquisition response body must be bytes"
        )

    text = body.decode(
        "utf-8-sig"
    )
    reader = csv.DictReader(
        io.StringIO(text)
    )
    rows = list(reader)
    fieldnames = list(
        reader.fieldnames or []
    )

    normalized = []
    invalid_player_ids = 0
    duplicate_player_ids = 0
    underqualified_rows = 0
    invalid_sprint_speeds = 0
    player_ids = set()

    for row in rows:
        try:
            player_id = int(
                row["player_id"]
            )
        except (
            KeyError,
            TypeError,
            ValueError,
        ):
            invalid_player_ids += 1
            continue

        if player_id in player_ids:
            duplicate_player_ids += 1
        player_ids.add(player_id)

        try:
            competitive_runs = int(
                row["competitive_runs"]
            )
        except (
            KeyError,
            TypeError,
            ValueError,
        ):
            competitive_runs = None

        try:
            sprint_speed = float(
                row["sprint_speed"]
            )
        except (
            KeyError,
            TypeError,
            ValueError,
        ):
            sprint_speed = None

        if (
            competitive_runs is None
            or competitive_runs < 10
        ):
            underqualified_rows += 1

        if (
            sprint_speed is None
            or not 15.0 <= sprint_speed <= 40.0
        ):
            invalid_sprint_speeds += 1

        normalized.append({
            "player_id": player_id,
            "competitive_runs":
                competitive_runs,
            "sprint_speed": sprint_speed,
            "team_id": row.get("team_id"),
            "position": row.get("position"),
        })

    normalized.sort(
        key=lambda row: row["player_id"]
    )
    headers = {
        str(key).lower(): value
        for key, value in (
            response.get("headers") or {}
        ).items()
    }

    return {
        "season_requested": season,
        "http_status":
            response.get("http_status"),
        "content_type":
            headers.get("content-type"),
        "content_disposition":
            headers.get(
                "content-disposition"
            ),
        "final_url":
            response.get("final_url"),
        "fieldnames": fieldnames,
        "row_count": len(rows),
        "unique_player_count":
            len(player_ids),
        "invalid_player_id_count":
            invalid_player_ids,
        "duplicate_player_id_count":
            duplicate_player_ids,
        "underqualified_row_count":
            underqualified_rows,
        "invalid_sprint_speed_count":
            invalid_sprint_speeds,
        "raw_byte_count": len(body),
        "raw_sha256": hashlib.sha256(
            body
        ).hexdigest(),
        "semantic_sha256": hashlib.sha256(
            _canonical_json(
                normalized
            ).encode("utf-8")
        ).hexdigest(),
        "season_field_present": any(
            str(field).strip().lower()
            in {"season", "year", "timeframe"}
            for field in fieldnames
        ),
        "freshness_field_present": any(
            str(field).strip().lower()
            in {
                "as_of_date",
                "source_updated_at",
                "updated_at",
                "timestamp",
            }
            for field in fieldnames
        ),
    }


def observe_season(
    season: int,
    *,
    fetcher: Callable[..., dict[str, Any]],
    timeout: float,
) -> dict[str, Any]:
    url = build_acquisition_url(season)

    first_response = fetcher(
        url,
        timeout=timeout,
    )
    second_response = fetcher(
        url,
        timeout=timeout,
    )

    first = _normalize_response(
        first_response,
        season=season,
    )
    second = _normalize_response(
        second_response,
        season=season,
    )

    first["raw_replay_identical"] = (
        first["raw_sha256"]
        == second["raw_sha256"]
    )
    first["semantic_replay_identical"] = (
        first["semantic_sha256"]
        == second["semantic_sha256"]
    )
    first["repeat_raw_sha256"] = (
        second["raw_sha256"]
    )
    first["repeat_semantic_sha256"] = (
        second["semantic_sha256"]
    )
    return first


def run_acquisition_audit(
    seasons: Iterable[int] = DEFAULT_SEASONS,
    *,
    fetcher: Callable[..., dict[str, Any]] = (
        fetch_response
    ),
    timeout: float = 30.0,
) -> dict[str, Any]:
    ordered_seasons = sorted(
        set(seasons)
    )
    observations = [
        observe_season(
            season,
            fetcher=fetcher,
            timeout=timeout,
        )
        for season in ordered_seasons
    ]

    evaluation = (
        evaluate_hitter_speed_snapshot_acquisition_contract(
            observations,
            historical_as_of_query_supported=False,
        )
    )

    return {
        "status": evaluation["status"],
        "observations": observations,
        "evaluation": evaluation,
        "external_fetch_performed": True,
        "database_writes_performed": False,
        "artifact_file_write_performed": False,
        "production_model_modified": False,
        "simulation_authority_changed": False,
        "parameter_selected": False,
        "production_authority_changed": False,
        "shadow_only": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
    )
    parser.add_argument(
        "--season",
        action="append",
        dest="seasons",
        type=int,
    )
    parser.add_argument(
        "--timeout",
        default=30.0,
        type=float,
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    seasons = (
        tuple(args.seasons)
        if args.seasons
        else DEFAULT_SEASONS
    )

    try:
        result = run_acquisition_audit(
            seasons,
            timeout=args.timeout,
        )
    except Exception as exc:
        result = {
            "status": "error",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "external_fetch_performed": True,
            "database_writes_performed": False,
            "artifact_file_write_performed": False,
            "production_authority_changed": False,
            "shadow_only": True,
        }
        print(
            json.dumps(
                result,
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )
    return (
        0
        if result["status"] == "ready"
        else 2
    )


if __name__ == "__main__":
    raise SystemExit(main())
