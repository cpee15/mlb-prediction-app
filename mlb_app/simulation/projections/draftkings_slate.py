"""Deterministic DraftKings MLB salary-slate ingestion."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from hashlib import sha256
import io
from pathlib import Path
import re
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple


DRAFTKINGS_SLATE_SCHEMA_VERSION = (
    "draftkings_mlb_slate_v1"
)

_REQUIRED_COLUMNS = (
    "Position",
    "Name + ID",
    "Name",
    "ID",
    "Roster Position",
    "Salary",
    "Game Info",
    "TeamAbbrev",
    "AvgPointsPerGame",
)


@dataclass(frozen=True)
class DraftKingsSlatePlayer:
    dk_player_id: str
    player_name: str
    position: str
    roster_positions: Tuple[str, ...]
    salary: int
    game_info: str
    away_team: str
    home_team: str
    team_abbrev: str
    average_points_per_game: float
    status: Optional[str] = None
    starting: Optional[bool] = None


@dataclass(frozen=True)
class DraftKingsSlate:
    schema_version: str
    source: str
    source_filename: Optional[str]
    slate_id: str
    player_count: int
    players: Tuple[DraftKingsSlatePlayer, ...]
    warnings: Tuple[str, ...]


def ingest_draftkings_salary_csv(
    source: Any,
    *,
    source_filename: Optional[str] = None,
) -> DraftKingsSlate:
    """
    Parse one DraftKings MLB salary CSV into a normalized slate.

    The parser accepts a path, text stream, bytes, or raw CSV text.
    """

    text, inferred_filename = _read_source(source)

    filename = (
        source_filename
        or inferred_filename
    )

    reader = csv.DictReader(
        io.StringIO(text)
    )

    headers = tuple(
        reader.fieldnames or ()
    )

    missing = [
        column
        for column in _REQUIRED_COLUMNS
        if column not in headers
    ]

    if missing:
        raise ValueError(
            "missing required DraftKings columns: "
            + ", ".join(missing)
        )

    players = []
    seen_ids = set()
    warnings = []

    for row_number, row in enumerate(
        reader,
        start=2,
    ):
        player = _normalize_player(
            row,
            row_number=row_number,
        )

        if player.dk_player_id in seen_ids:
            raise ValueError(
                "duplicate DraftKings player ID: "
                f"{player.dk_player_id}"
            )

        seen_ids.add(player.dk_player_id)
        players.append(player)

    if not players:
        raise ValueError(
            "DraftKings slate contains no players"
        )

    ordered_players = tuple(
        sorted(
            players,
            key=lambda player: (
                player.game_info,
                player.team_abbrev,
                player.position,
                player.player_name.casefold(),
                player.dk_player_id,
            ),
        )
    )

    if any(
        player.starting is None
        for player in ordered_players
    ):
        warnings.append(
            "starting_status_partially_unavailable"
        )

    digest = sha256(
        text.encode("utf-8")
    ).hexdigest()

    return DraftKingsSlate(
        schema_version=(
            DRAFTKINGS_SLATE_SCHEMA_VERSION
        ),
        source="draftkings_salary_csv",
        source_filename=filename,
        slate_id=digest,
        player_count=len(ordered_players),
        players=ordered_players,
        warnings=tuple(warnings),
    )


def draftkings_slate_to_dict(
    slate: DraftKingsSlate,
) -> Dict[str, Any]:
    if not isinstance(
        slate,
        DraftKingsSlate,
    ):
        raise TypeError(
            "slate must be DraftKingsSlate"
        )

    payload = asdict(slate)
    payload["players"] = [
        {
            **asdict(player),
            "roster_positions": list(
                player.roster_positions
            ),
        }
        for player in slate.players
    ]
    payload["warnings"] = list(
        slate.warnings
    )

    return payload


def _read_source(
    source: Any,
) -> Tuple[str, Optional[str]]:
    if isinstance(source, Path):
        return (
            source.read_text(
                encoding="utf-8-sig"
            ),
            source.name,
        )

    if isinstance(source, bytes):
        return (
            source.decode("utf-8-sig"),
            None,
        )

    if hasattr(source, "read"):
        value = source.read()

        if isinstance(value, bytes):
            value = value.decode(
                "utf-8-sig"
            )

        if not isinstance(value, str):
            raise TypeError(
                "CSV stream must return str or bytes"
            )

        name = getattr(
            source,
            "name",
            None,
        )

        return value, (
            Path(name).name
            if isinstance(name, str)
            else None
        )

    if isinstance(source, str):
        candidate = Path(source)

        if (
            "\n" not in source
            and "\r" not in source
            and candidate.exists()
        ):
            return (
                candidate.read_text(
                    encoding="utf-8-sig"
                ),
                candidate.name,
            )

        return source, None

    raise TypeError(
        "source must be a path, stream, bytes, or CSV text"
    )


def _normalize_player(
    row: Mapping[str, Any],
    *,
    row_number: int,
) -> DraftKingsSlatePlayer:
    dk_player_id = _required_text(
        row.get("ID"),
        field_name="ID",
        row_number=row_number,
    )
    player_name = _required_text(
        row.get("Name"),
        field_name="Name",
        row_number=row_number,
    )
    position = _required_text(
        row.get("Position"),
        field_name="Position",
        row_number=row_number,
    )
    roster_positions = _roster_positions(
        row.get("Roster Position"),
        row_number=row_number,
    )
    salary = _positive_int(
        row.get("Salary"),
        field_name="Salary",
        row_number=row_number,
    )
    game_info = _required_text(
        row.get("Game Info"),
        field_name="Game Info",
        row_number=row_number,
    )
    team_abbrev = _required_text(
        row.get("TeamAbbrev"),
        field_name="TeamAbbrev",
        row_number=row_number,
    ).upper()

    away_team, home_team = _teams_from_game_info(
        game_info,
        row_number=row_number,
    )

    average_points = _float_value(
        row.get("AvgPointsPerGame"),
        field_name="AvgPointsPerGame",
        row_number=row_number,
    )

    status = _optional_text(
        row.get("Status")
    )
    starting = _starting_value(
        row.get("Starting")
    )

    return DraftKingsSlatePlayer(
        dk_player_id=dk_player_id,
        player_name=player_name,
        position=position,
        roster_positions=roster_positions,
        salary=salary,
        game_info=game_info,
        away_team=away_team,
        home_team=home_team,
        team_abbrev=team_abbrev,
        average_points_per_game=(
            average_points
        ),
        status=status,
        starting=starting,
    )


def _teams_from_game_info(
    game_info: str,
    *,
    row_number: int,
) -> Tuple[str, str]:
    matchup = game_info.split()[0]

    match = re.fullmatch(
        r"([A-Za-z0-9]+)@([A-Za-z0-9]+)",
        matchup,
    )

    if match is None:
        raise ValueError(
            "invalid Game Info matchup "
            f"at row {row_number}: {game_info}"
        )

    return (
        match.group(1).upper(),
        match.group(2).upper(),
    )


def _roster_positions(
    value: Any,
    *,
    row_number: int,
) -> Tuple[str, ...]:
    text = _required_text(
        value,
        field_name="Roster Position",
        row_number=row_number,
    )

    positions = tuple(
        dict.fromkeys(
            item.strip()
            for item in text.split("/")
            if item.strip()
        )
    )

    if not positions:
        raise ValueError(
            "Roster Position is empty "
            f"at row {row_number}"
        )

    return positions


def _required_text(
    value: Any,
    *,
    field_name: str,
    row_number: int,
) -> str:
    text = str(
        value or ""
    ).strip()

    if not text:
        raise ValueError(
            f"{field_name} is required "
            f"at row {row_number}"
        )

    return text


def _optional_text(
    value: Any,
) -> Optional[str]:
    text = str(
        value or ""
    ).strip()

    return text or None


def _positive_int(
    value: Any,
    *,
    field_name: str,
    row_number: int,
) -> int:
    try:
        normalized = int(
            str(value).strip()
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{field_name} must be an integer "
            f"at row {row_number}"
        ) from exc

    if normalized <= 0:
        raise ValueError(
            f"{field_name} must be positive "
            f"at row {row_number}"
        )

    return normalized


def _float_value(
    value: Any,
    *,
    field_name: str,
    row_number: int,
) -> float:
    try:
        return float(
            str(value).strip()
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{field_name} must be numeric "
            f"at row {row_number}"
        ) from exc


def _starting_value(
    value: Any,
) -> Optional[bool]:
    text = str(
        value or ""
    ).strip().casefold()

    if text in {
        "yes",
        "y",
        "true",
        "1",
        "confirmed",
    }:
        return True

    if text in {
        "no",
        "n",
        "false",
        "0",
    }:
        return False

    return None
