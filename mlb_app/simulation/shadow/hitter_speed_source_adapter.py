"""Cutoff-safe adapter for supplied Savant Sprint Speed CSV snapshots."""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import math
from collections.abc import Mapping
from typing import Any


SCHEMA_VERSION = (
    "shadow_hitter_speed_source_adapter_v1"
)
SOURCE_PROVIDER = "MLB Baseball Savant"
SOURCE_DATASET = "Statcast Sprint Speed Leaderboard"
DEFAULT_SOURCE_URL = (
    "https://baseballsavant.mlb.com/"
    "leaderboard/sprint_speed"
)
MINIMUM_COMPETITIVE_RUNS = 10

REQUIRED_ALIASES = {
    "player_id": (
        "player_id",
        "playerid",
        "mlb_id",
        "mlbam_id",
    ),
    "competitive_runs": (
        "competitive_runs",
        "competitive run",
        "competitive_runs_count",
        "runs",
    ),
    "sprint_speed": (
        "sprint_speed",
        "sprint speed",
        "sprint_speed_ft_sec",
    ),
}
OPTIONAL_ALIASES = {
    "last_name": (
        "last_name",
        "last name",
    ),
    "first_name": (
        "first_name",
        "first name",
    ),
    "team_id": (
        "team_id",
        "teamid",
    ),
    "team": (
        "team",
        "team_name",
    ),
    "position": (
        "position",
        "position_name",
        "pos",
    ),
    "bolt_count": (
        "bolts",
        "bolt_count",
        "bolt count",
    ),
    "home_to_first": (
        "hp_to_1b",
        "home_to_first",
        "home to first",
    ),
    "row_season": (
        "season",
        "year",
    ),
}


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_payload(value: Any) -> str:
    return _sha256_bytes(
        _canonical_json(value).encode("utf-8")
    )


def _normalized_header(value: Any) -> str:
    return " ".join(
        str(value or "")
        .strip()
        .lower()
        .replace("-", " ")
        .replace("_", " ")
        .split()
    )


def _header_lookup(
    fieldnames: list[str],
) -> dict[str, str]:
    return {
        _normalized_header(field): field
        for field in fieldnames
        if field is not None
    }


def _resolve_header(
    lookup: Mapping[str, str],
    aliases,
):
    for alias in aliases:
        resolved = lookup.get(
            _normalized_header(alias)
        )
        if resolved is not None:
            return resolved
    return None


def _integer(value: Any):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        number = float(
            text.replace(",", "")
        )
    except ValueError:
        return None
    if not math.isfinite(number):
        return None
    integer = int(number)
    return integer if number == integer else None


def _number(value: Any):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        number = float(
            text.replace(",", "")
        )
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _date(value: Any):
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(
            str(value).strip()[:10]
        )
    except (TypeError, ValueError):
        return None


def _timestamp(value: Any):
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = dt.datetime.fromisoformat(
                text
            )
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        return None
    return parsed


def parse_savant_sprint_speed_csv(
    csv_text: str,
    *,
    season: int,
    as_of_date,
    source_updated_at,
    source_url: str = DEFAULT_SOURCE_URL,
    source_version: str = "savant_sprint_speed_csv_v1",
    minimum_competitive_runs: int = (
        MINIMUM_COMPETITIVE_RUNS
    ),
) -> dict[str, Any]:
    """Normalize a supplied immutable Savant CSV snapshot."""

    raw_bytes = csv_text.encode("utf-8")
    raw_sha256 = _sha256_bytes(raw_bytes)
    parsed_as_of = _date(as_of_date)
    parsed_updated_at = _timestamp(
        source_updated_at
    )
    blockers = []

    if not isinstance(season, int) or season < 2015:
        blockers.append("invalid_season")
    if parsed_as_of is None:
        blockers.append("invalid_as_of_date")
    if parsed_updated_at is None:
        blockers.append(
            "invalid_source_updated_at"
        )
    if (
        parsed_as_of is not None
        and parsed_updated_at is not None
        and parsed_updated_at.date()
        != parsed_as_of
    ):
        blockers.append(
            "snapshot_date_does_not_match_capture_date"
        )
    if minimum_competitive_runs < 1:
        blockers.append(
            "invalid_minimum_competitive_runs"
        )
    if not str(source_version).strip():
        blockers.append("missing_source_version")
    if not str(source_url).strip():
        blockers.append("missing_source_url")

    reader = csv.DictReader(
        io.StringIO(csv_text)
    )
    fieldnames = list(
        reader.fieldnames or []
    )
    lookup = _header_lookup(fieldnames)
    resolved = {}
    missing_headers = []

    for field, aliases in REQUIRED_ALIASES.items():
        header = _resolve_header(
            lookup,
            aliases,
        )
        resolved[field] = header
        if header is None:
            missing_headers.append(field)

    for field, aliases in OPTIONAL_ALIASES.items():
        resolved[field] = _resolve_header(
            lookup,
            aliases,
        )

    if missing_headers:
        blockers.append(
            "missing_required_csv_headers"
        )

    records = []
    rejected_rows = []
    seen_player_ids = set()

    if not missing_headers:
        for row_number, row in enumerate(
            reader,
            start=2,
        ):
            player_id = _integer(
                row.get(resolved["player_id"])
            )
            competitive_runs = _integer(
                row.get(
                    resolved["competitive_runs"]
                )
            )
            sprint_speed = _number(
                row.get(
                    resolved["sprint_speed"]
                )
            )
            row_season = (
                _integer(
                    row.get(
                        resolved["row_season"]
                    )
                )
                if resolved["row_season"]
                else None
            )

            reasons = []
            if player_id is None or player_id <= 0:
                reasons.append(
                    "invalid_player_id"
                )
            if (
                competitive_runs is None
                or competitive_runs < 0
            ):
                reasons.append(
                    "invalid_competitive_runs"
                )
            elif (
                competitive_runs
                < minimum_competitive_runs
            ):
                reasons.append(
                    "insufficient_competitive_runs"
                )
            if (
                sprint_speed is None
                or not 15.0 <= sprint_speed <= 40.0
            ):
                reasons.append(
                    "invalid_sprint_speed"
                )
            if (
                row_season is not None
                and row_season != season
            ):
                reasons.append(
                    "row_season_mismatch"
                )
            if player_id in seen_player_ids:
                reasons.append(
                    "duplicate_player_id"
                )

            if reasons:
                rejected_rows.append({
                    "row_number": row_number,
                    "player_id": player_id,
                    "reasons": sorted(
                        set(reasons)
                    ),
                })
                continue

            seen_player_ids.add(player_id)
            record = {
                "player_id": player_id,
                "season": season,
                "as_of_date":
                    (
                        parsed_as_of.isoformat()
                        if parsed_as_of is not None
                        else None
                    ),
                "sprint_speed": sprint_speed,
                "competitive_runs":
                    competitive_runs,
                "bolt_count": (
                    _integer(
                        row.get(
                            resolved["bolt_count"]
                        )
                    )
                    if resolved["bolt_count"]
                    else None
                ),
                "home_to_first": (
                    _number(
                        row.get(
                            resolved["home_to_first"]
                        )
                    )
                    if resolved["home_to_first"]
                    else None
                ),
                "first_name": (
                    str(
                        row.get(
                            resolved["first_name"]
                        )
                        or ""
                    ).strip()
                    if resolved["first_name"]
                    else None
                ),
                "last_name": (
                    str(
                        row.get(
                            resolved["last_name"]
                        )
                        or ""
                    ).strip()
                    if resolved["last_name"]
                    else None
                ),
                "team_id": (
                    _integer(
                        row.get(
                            resolved["team_id"]
                        )
                    )
                    if resolved["team_id"]
                    else None
                ),
                "team": (
                    str(
                        row.get(
                            resolved["team"]
                        )
                        or ""
                    ).strip()
                    if resolved["team"]
                    else None
                ),
                "position": (
                    str(
                        row.get(
                            resolved["position"]
                        )
                        or ""
                    ).strip()
                    if resolved["position"]
                    else None
                ),
                "source_updated_at":
                    (
                        parsed_updated_at.isoformat()
                        if parsed_updated_at is not None
                        else None
                    ),
                "source_version":
                    str(source_version).strip(),
                "source_provider":
                    SOURCE_PROVIDER,
                "source_dataset":
                    SOURCE_DATASET,
                "source_url":
                    str(source_url).strip(),
                "raw_source_sha256":
                    raw_sha256,
            }
            record["record_sha256"] = (
                _sha256_payload(record)
            )
            records.append(record)

    records.sort(
        key=lambda record: record["player_id"]
    )
    rejected_rows.sort(
        key=lambda row: (
            row["row_number"],
            row["player_id"] or -1,
        )
    )

    if rejected_rows:
        blockers.append(
            "csv_rows_rejected"
        )
    if not records:
        blockers.append(
            "no_eligible_speed_records"
        )

    snapshot_identity = {
        "season": season,
        "as_of_date": (
            parsed_as_of.isoformat()
            if parsed_as_of is not None
            else None
        ),
        "source_updated_at": (
            parsed_updated_at.isoformat()
            if parsed_updated_at is not None
            else None
        ),
        "source_version":
            str(source_version).strip(),
        "raw_source_sha256": raw_sha256,
        "player_ids": [
            record["player_id"]
            for record in records
        ],
    }
    snapshot_sha256 = _sha256_payload(
        snapshot_identity
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "ready"
            if not blockers
            else "blocked"
        ),
        "shadow_only": True,
        "source_adapter_only": True,
        "external_fetch_performed": False,
        "database_writes_performed": False,
        "parameter_selected": False,
        "production_authority_changed": False,
        "season": season,
        "as_of_date": (
            parsed_as_of.isoformat()
            if parsed_as_of is not None
            else None
        ),
        "source_updated_at": (
            parsed_updated_at.isoformat()
            if parsed_updated_at is not None
            else None
        ),
        "source_version":
            str(source_version).strip(),
        "source_provider": SOURCE_PROVIDER,
        "source_dataset": SOURCE_DATASET,
        "source_url": str(source_url).strip(),
        "raw_source_byte_length":
            len(raw_bytes),
        "raw_source_sha256": raw_sha256,
        "snapshot_sha256": snapshot_sha256,
        "minimum_competitive_runs":
            minimum_competitive_runs,
        "csv_headers": fieldnames,
        "resolved_headers": resolved,
        "missing_required_headers":
            missing_headers,
        "record_count": len(records),
        "rejected_row_count":
            len(rejected_rows),
        "records": records,
        "rejected_rows": rejected_rows,
        "blockers": sorted(set(blockers)),
    }
