"""Confirmed-lineup discovery for canonical shadow bootstrap inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple


CANONICAL_SHADOW_LINEUP_DISCOVERY_VERSION = (
    "canonical_shadow_lineup_discovery_v1"
)


def _normalize_identifier(value: Any) -> Optional[str]:
    if value in (None, "") or isinstance(value, bool):
        return None

    try:
        return str(int(value))
    except (TypeError, ValueError):
        text = str(value).strip()
        return text or None


def _lineup_identifiers(
    records: Any,
) -> Tuple[str, ...]:
    if not isinstance(records, Sequence) or isinstance(
        records,
        (str, bytes),
    ):
        return ()

    ordered = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        identifier = _normalize_identifier(
            record.get("batter_id")
            or record.get("player_id")
            or record.get("id")
            or record.get("person_id")
        )

        if (
            identifier is not None
            and identifier not in ordered
        ):
            ordered.append(identifier)

    return tuple(ordered)


@dataclass(frozen=True)
class CanonicalShadowLineupDiscovery:
    """
    Confirmed lineup discovery result.

    Player identifiers are retained internally for canonical input assembly.
    Public diagnostics intentionally expose counts and status only.
    """

    away_player_ids: Tuple[str, ...] = ()
    home_player_ids: Tuple[str, ...] = ()
    away_source_count: int = 0
    home_source_count: int = 0
    status: str = "unavailable"
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    discovery_version: str = (
        CANONICAL_SHADOW_LINEUP_DISCOVERY_VERSION
    )

    def __post_init__(self) -> None:
        if self.discovery_version != (
            CANONICAL_SHADOW_LINEUP_DISCOVERY_VERSION
        ):
            raise ValueError(
                "unsupported canonical shadow lineup "
                "discovery version"
            )

        if len(set(self.away_player_ids)) != len(
            self.away_player_ids
        ):
            raise ValueError(
                "away_player_ids must be unique"
            )

        if len(set(self.home_player_ids)) != len(
            self.home_player_ids
        ):
            raise ValueError(
                "home_player_ids must be unique"
            )

    @property
    def away_ready(self) -> bool:
        return len(self.away_player_ids) == 9

    @property
    def home_ready(self) -> bool:
        return len(self.home_player_ids) == 9

    @property
    def ready(self) -> bool:
        return self.away_ready and self.home_ready

    def readiness_matchup_fields(
        self,
    ) -> Dict[str, Any]:
        fields: Dict[str, Any] = {}

        if self.away_ready:
            fields["away_lineup"] = [
                {"player_id": player_id}
                for player_id in self.away_player_ids
            ]

        if self.home_ready:
            fields["home_lineup"] = [
                {"player_id": player_id}
                for player_id in self.home_player_ids
            ]

        return fields

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.discovery_version,
            "status": self.status,
            "ready": self.ready,
            "source": "mlb_stats_boxscore",
            "away": {
                "ready": self.away_ready,
                "source_record_count": (
                    self.away_source_count
                ),
                "validated_player_count": len(
                    self.away_player_ids
                ),
                "required_player_count": 9,
            },
            "home": {
                "ready": self.home_ready,
                "source_record_count": (
                    self.home_source_count
                ),
                "validated_player_count": len(
                    self.home_player_ids
                ),
                "required_player_count": 9,
            },
            "error_type": self.error_type,
            "error_message": self.error_message,
            "player_identifiers_exposed": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


def discover_canonical_shadow_lineups(
    *,
    game_pk: Any,
    lineup_fetcher: Optional[
        Callable[[int], Mapping[str, Any]]
    ] = None,
) -> CanonicalShadowLineupDiscovery:
    """
    Discover confirmed lineups without activating canonical execution.

    Missing, incomplete, malformed, or failed boxscore responses return a
    fail-open diagnostic result. No projected lineup is manufactured.
    """

    identifier = _normalize_identifier(game_pk)

    if identifier is None:
        return CanonicalShadowLineupDiscovery(
            status="blocked",
            error_type="missing_game_pk",
            error_message=(
                "game_pk is required for confirmed "
                "lineup discovery"
            ),
        )

    if lineup_fetcher is None:
        from mlb_app.lineup_profile import (
            fetch_boxscore_lineup,
        )

        lineup_fetcher = fetch_boxscore_lineup

    try:
        payload = lineup_fetcher(int(identifier))
    except Exception as exc:
        return CanonicalShadowLineupDiscovery(
            status="error",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )

    if not isinstance(payload, Mapping):
        return CanonicalShadowLineupDiscovery(
            status="blocked",
            error_type="invalid_payload",
            error_message=(
                "lineup fetcher must return a mapping"
            ),
        )

    away_records = payload.get("away") or []
    home_records = payload.get("home") or []

    away_ids = _lineup_identifiers(away_records)
    home_ids = _lineup_identifiers(home_records)

    ready = (
        len(away_ids) == 9
        and len(home_ids) == 9
    )

    any_records = bool(
        away_records or home_records
    )

    status = (
        "ready"
        if ready
        else "partial"
        if any_records
        else "unavailable"
    )

    return CanonicalShadowLineupDiscovery(
        away_player_ids=away_ids,
        home_player_ids=home_ids,
        away_source_count=(
            len(away_records)
            if isinstance(away_records, Sequence)
            else 0
        ),
        home_source_count=(
            len(home_records)
            if isinstance(home_records, Sequence)
            else 0
        ),
        status=status,
    )
