"""Conservative matching of canonical projections to DraftKings slates."""

from __future__ import annotations

from copy import deepcopy
import re
import unicodedata
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from .draftkings_slate import (
    DraftKingsSlate,
    DraftKingsSlatePlayer,
)


DRAFTKINGS_PROJECTION_MATCH_SCHEMA_VERSION = (
    "draftkings_projection_match_v1"
)


_TEAM_ALIASES = {
    "AZ": "ARI",
    "CWS": "CHW",
    "KC": "KCR",
    "SD": "SDP",
    "SF": "SFG",
    "TB": "TBR",
    "WSH": "WSN",
}


def match_canonical_projections_to_draftkings(
    *,
    projection_payload: Mapping[str, Any],
    slate: DraftKingsSlate,
) -> Dict[str, Any]:
    """
    Match identity-enriched canonical rows to one DraftKings slate.

    Matching is exact after deterministic name normalization and requires
    compatible player type plus compatible team when a team abbreviation
    can be resolved from the canonical row.
    """

    if not isinstance(projection_payload, Mapping):
        raise TypeError(
            "projection_payload must be a mapping"
        )

    if not isinstance(slate, DraftKingsSlate):
        raise TypeError(
            "slate must be DraftKingsSlate"
        )

    canonical_players = projection_payload.get(
        "players"
    )

    if not isinstance(
        canonical_players,
        (list, tuple),
    ):
        raise TypeError(
            "projection players must be a list or tuple"
        )

    canonical_rows = tuple(
        _validated_projection_row(row)
        for row in canonical_players
    )

    indexes = _canonical_indexes(
        canonical_rows
    )

    output_rows = []
    matched_projection_keys = set()
    matched_count = 0
    ambiguous_count = 0
    unmatched_count = 0

    for dk_player in slate.players:
        candidates = _match_candidates(
            dk_player=dk_player,
            indexes=indexes,
        )

        if len(candidates) == 1:
            projection = candidates[0]
            matched_count += 1
            matched_projection_keys.add(
                _projection_key(projection)
            )
            output_rows.append(
                _matched_row(
                    dk_player=dk_player,
                    projection=projection,
                )
            )
        elif len(candidates) > 1:
            ambiguous_count += 1
            output_rows.append(
                _unmatched_row(
                    dk_player=dk_player,
                    status="ambiguous",
                    candidate_count=len(candidates),
                )
            )
        else:
            unmatched_count += 1
            output_rows.append(
                _unmatched_row(
                    dk_player=dk_player,
                    status="unmatched",
                    candidate_count=0,
                )
            )

    unmatched_canonical = [
        {
            "player_id": row.get("player_id"),
            "mlb_player_id": row.get(
                "mlb_player_id"
            ),
            "full_name": row.get("full_name"),
            "player_type": row.get(
                "player_type"
            ),
            "team_name": row.get("team_name"),
        }
        for row in canonical_rows
        if _projection_key(row)
        not in matched_projection_keys
    ]

    return {
        "schema_version": (
            DRAFTKINGS_PROJECTION_MATCH_SCHEMA_VERSION
        ),
        "source_projection_schema_version": (
            projection_payload.get(
                "schema_version"
            )
        ),
        "slate_schema_version": (
            slate.schema_version
        ),
        "slate_id": slate.slate_id,
        "simulation_count": (
            projection_payload.get(
                "simulation_count"
            )
        ),
        "players": output_rows,
        "diagnostics": {
            "draftkings_player_count": (
                slate.player_count
            ),
            "canonical_player_count": len(
                canonical_rows
            ),
            "matched_player_count": matched_count,
            "ambiguous_player_count": (
                ambiguous_count
            ),
            "unmatched_draftkings_player_count": (
                unmatched_count
            ),
            "unmatched_canonical_player_count": (
                len(unmatched_canonical)
            ),
            "unmatched_canonical_players": (
                unmatched_canonical
            ),
            "match_policy": (
                "exact_normalized_name_team_type"
            ),
            "fuzzy_matching_used": False,
        },
        "authoritative": False,
        "authoritative_source": "legacy",
    }


def _validated_projection_row(
    row: Any,
) -> Mapping[str, Any]:
    if not isinstance(row, Mapping):
        raise TypeError(
            "projection player entries must be mappings"
        )

    return row


def _canonical_indexes(
    rows: Sequence[Mapping[str, Any]],
) -> Dict[
    Tuple[str, str],
    Tuple[Mapping[str, Any], ...],
]:
    grouped: Dict[
        Tuple[str, str],
        list,
    ] = {}

    for row in rows:
        name = _normalize_name(
            row.get("full_name")
        )
        player_type = _canonical_type(
            row.get("player_type")
        )

        if not name or player_type is None:
            continue

        grouped.setdefault(
            (name, player_type),
            [],
        ).append(row)

    return {
        key: tuple(values)
        for key, values in grouped.items()
    }


def _match_candidates(
    *,
    dk_player: DraftKingsSlatePlayer,
    indexes: Mapping[
        Tuple[str, str],
        Tuple[Mapping[str, Any], ...],
    ],
) -> Tuple[Mapping[str, Any], ...]:
    name = _normalize_name(
        dk_player.player_name
    )
    player_type = _dk_player_type(
        dk_player
    )

    candidates = indexes.get(
        (name, player_type),
        (),
    )

    compatible = tuple(
        row
        for row in candidates
        if _team_compatible(
            canonical_row=row,
            dk_team=dk_player.team_abbrev,
        )
    )

    return compatible


def _matched_row(
    *,
    dk_player: DraftKingsSlatePlayer,
    projection: Mapping[str, Any],
) -> Dict[str, Any]:
    projected = _optional_float(
        projection.get(
            "projected_dfs_points"
        )
    )
    floor = _optional_float(
        projection.get("dfs_floor")
    )
    median = _optional_float(
        projection.get("dfs_median")
    )
    ceiling = _optional_float(
        projection.get("dfs_ceiling")
    )

    return {
        "dk_player_id": dk_player.dk_player_id,
        "mlb_player_id": projection.get(
            "mlb_player_id"
        ),
        "player_id": projection.get(
            "player_id"
        ),
        "full_name": projection.get(
            "full_name"
        ),
        "team_name": projection.get(
            "team_name"
        ),
        "team_abbrev": dk_player.team_abbrev,
        "position": dk_player.position,
        "roster_positions": list(
            dk_player.roster_positions
        ),
        "salary": dk_player.salary,
        "game_info": dk_player.game_info,
        "average_points_per_game": (
            dk_player.average_points_per_game
        ),
        "status": dk_player.status,
        "starting": dk_player.starting,
        "player_type": projection.get(
            "player_type"
        ),
        "projected_dfs_points": projected,
        "dfs_floor": floor,
        "dfs_median": median,
        "dfs_ceiling": ceiling,
        "value_per_1000": _value_metric(
            projected,
            dk_player.salary,
        ),
        "floor_value_per_1000": _value_metric(
            floor,
            dk_player.salary,
        ),
        "median_value_per_1000": _value_metric(
            median,
            dk_player.salary,
        ),
        "ceiling_value_per_1000": _value_metric(
            ceiling,
            dk_player.salary,
        ),
        "metrics": deepcopy(
            projection.get("metrics") or {}
        ),
        "match_method": (
            "exact_normalized_name_team_type"
        ),
        "match_status": "matched",
        "match_candidate_count": 1,
    }


def _unmatched_row(
    *,
    dk_player: DraftKingsSlatePlayer,
    status: str,
    candidate_count: int,
) -> Dict[str, Any]:
    return {
        "dk_player_id": dk_player.dk_player_id,
        "mlb_player_id": None,
        "player_id": None,
        "full_name": dk_player.player_name,
        "team_name": None,
        "team_abbrev": dk_player.team_abbrev,
        "position": dk_player.position,
        "roster_positions": list(
            dk_player.roster_positions
        ),
        "salary": dk_player.salary,
        "game_info": dk_player.game_info,
        "average_points_per_game": (
            dk_player.average_points_per_game
        ),
        "status": dk_player.status,
        "starting": dk_player.starting,
        "player_type": _dk_player_type(
            dk_player
        ),
        "projected_dfs_points": None,
        "dfs_floor": None,
        "dfs_median": None,
        "dfs_ceiling": None,
        "value_per_1000": None,
        "floor_value_per_1000": None,
        "median_value_per_1000": None,
        "ceiling_value_per_1000": None,
        "metrics": {},
        "match_method": None,
        "match_status": status,
        "match_candidate_count": (
            candidate_count
        ),
    }


def _projection_key(
    row: Mapping[str, Any],
) -> Tuple[Any, Any, Any]:
    return (
        row.get("player_id"),
        row.get("mlb_player_id"),
        row.get("player_type"),
    )


def _canonical_type(
    value: Any,
) -> Optional[str]:
    text = str(
        value or ""
    ).strip().casefold()

    if text in {"pitcher", "p"}:
        return "pitcher"

    if text in {
        "batter",
        "hitter",
        "position_player",
    }:
        return "batter"

    return None


def _dk_player_type(
    player: DraftKingsSlatePlayer,
) -> str:
    if (
        player.position.strip().upper() == "SP"
        or "P" in {
            value.strip().upper()
            for value in player.roster_positions
        }
    ):
        return "pitcher"

    return "batter"


def _team_compatible(
    *,
    canonical_row: Mapping[str, Any],
    dk_team: str,
) -> bool:
    canonical_team = (
        canonical_row.get("team_abbrev")
        or canonical_row.get(
            "team_abbreviation"
        )
    )

    if canonical_team:
        return (
            _normalize_team(canonical_team)
            == _normalize_team(dk_team)
        )

    team_name = canonical_row.get("team_name")

    if not team_name:
        return True

    normalized_name = _normalize_team(
        team_name
    )

    normalized_dk = _normalize_team(
        dk_team
    )

    return (
        normalized_name == normalized_dk
        or normalized_dk in normalized_name
    )


def _normalize_name(
    value: Any,
) -> str:
    text = unicodedata.normalize(
        "NFKD",
        str(value or ""),
    )
    text = "".join(
        character
        for character in text
        if not unicodedata.combining(
            character
        )
    )
    text = text.casefold()
    text = re.sub(
        r"[^a-z0-9]+",
        " ",
        text,
    )

    return " ".join(
        text.split()
    )


def _normalize_team(
    value: Any,
) -> str:
    text = re.sub(
        r"[^A-Za-z0-9]+",
        "",
        str(value or ""),
    ).upper()

    return _TEAM_ALIASES.get(
        text,
        text,
    )


def _optional_float(
    value: Any,
) -> Optional[float]:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _value_metric(
    points: Optional[float],
    salary: int,
) -> Optional[float]:
    if points is None:
        return None

    return round(
        points / (salary / 1000.0),
        6,
    )
