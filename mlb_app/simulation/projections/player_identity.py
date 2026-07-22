"""Dashboard-backed identity enrichment for canonical player rows."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Mapping, Optional, Tuple

from mlb_app.dashboard_object_models import (
    DashboardPlayerCurrent,
)


CANONICAL_PLAYER_IDENTITY_ENRICHMENT_VERSION = (
    "canonical_player_identity_enrichment_v1"
)


def enrich_canonical_player_projection_rows(
    *,
    session: Any,
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    Enrich canonical player rows from DashboardPlayerCurrent.

    Unresolved rows remain present. This function does not mutate the
    supplied payload or alter simulation values or production authority.
    """

    if session is None:
        raise TypeError(
            "session is required"
        )

    if not isinstance(payload, Mapping):
        raise TypeError(
            "payload must be a mapping"
        )

    players = payload.get("players")

    if not isinstance(players, (list, tuple)):
        raise TypeError(
            "players must be a list or tuple"
        )

    requested_ids = {
        player_id
        for player in players
        if isinstance(player, Mapping)
        for player_id in (
            _positive_player_id(
                player.get("player_id")
            ),
        )
        if player_id is not None
    }

    identities = _identity_map(
        session=session,
        player_ids=tuple(
            sorted(requested_ids)
        ),
    )

    enriched = []
    resolved_count = 0
    inactive_count = 0
    unresolved_ids = []

    for raw_player in players:
        if not isinstance(raw_player, Mapping):
            raise TypeError(
                "player entries must be mappings"
            )

        player = deepcopy(dict(raw_player))
        numeric_id = _positive_player_id(
            player.get("player_id")
        )
        identity = (
            identities.get(numeric_id)
            if numeric_id is not None
            else None
        )

        if identity is None:
            player.update(
                {
                    "mlb_player_id": numeric_id,
                    "full_name": None,
                    "team_id": None,
                    "team_name": None,
                    "primary_position": None,
                    "identity_player_type": None,
                    "is_active": None,
                    "identity_resolution_status": (
                        "unresolved"
                    ),
                }
            )
            unresolved_ids.append(
                str(player.get("player_id"))
            )
        else:
            resolved_count += 1

            if not identity.is_active:
                inactive_count += 1

            player.update(
                {
                    "mlb_player_id": (
                        identity.mlb_player_id
                    ),
                    "full_name": identity.full_name,
                    "team_id": identity.team_id,
                    "team_name": identity.team_name,
                    "primary_position": (
                        identity.primary_position
                    ),
                    "identity_player_type": (
                        identity.player_type
                    ),
                    "is_active": bool(
                        identity.is_active
                    ),
                    "identity_resolution_status": (
                        "resolved"
                    ),
                }
            )

        enriched.append(player)

    result = deepcopy(dict(payload))
    result["players"] = enriched
    result["identity_enrichment"] = {
        "schema_version": (
            CANONICAL_PLAYER_IDENTITY_ENRICHMENT_VERSION
        ),
        "source": "dashboard_player_current",
        "requested_player_count": len(players),
        "numeric_player_id_count": len(
            requested_ids
        ),
        "resolved_player_count": resolved_count,
        "unresolved_player_count": (
            len(players) - resolved_count
        ),
        "inactive_player_count": inactive_count,
        "unresolved_player_ids": sorted(
            unresolved_ids
        ),
    }
    result["identity_enrichment_applied"] = True

    return result


def _identity_map(
    *,
    session: Any,
    player_ids: Tuple[int, ...],
) -> Dict[int, DashboardPlayerCurrent]:
    if not player_ids:
        return {}

    rows = (
        session.query(DashboardPlayerCurrent)
        .filter(
            DashboardPlayerCurrent.mlb_player_id.in_(
                player_ids
            )
        )
        .order_by(
            DashboardPlayerCurrent.mlb_player_id.asc()
        )
        .all()
    )

    return {
        int(row.mlb_player_id): row
        for row in rows
    }


def _positive_player_id(
    value: Any,
) -> Optional[int]:
    if isinstance(value, bool):
        return None

    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return None

    return (
        normalized
        if normalized > 0
        else None
    )
