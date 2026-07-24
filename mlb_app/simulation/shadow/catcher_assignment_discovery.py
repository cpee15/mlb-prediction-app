"""Discover exact active catchers from confirmed boxscore lineups."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

from .catcher_context_composition import (
    CanonicalCatcherTeamAssignment,
)


CANONICAL_CATCHER_ASSIGNMENT_DISCOVERY_VERSION = (
    "canonical_catcher_assignment_discovery_v1"
)
CONFIRMED_CATCHER_ASSIGNMENT_SOURCE_VERSION = (
    "mlb_stats_confirmed_lineup_catcher_v1"
)


def _normalize_identifier(
    value: Any,
) -> Optional[str]:
    if value in (None, "") or isinstance(value, bool):
        return None

    try:
        return str(int(value))
    except (TypeError, ValueError):
        text = str(value).strip()
        return text or None


def _is_explicit_catcher(
    record: Mapping[str, Any],
) -> bool:
    position = str(
        record.get("position") or ""
    ).strip().lower()
    position_code = str(
        record.get("position_code") or ""
    ).strip()

    return (
        position in {"c", "catcher"}
        or position_code == "2"
    )


def _catcher_identifiers(
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

        if not _is_explicit_catcher(record):
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
class CanonicalCatcherAssignmentDiscovery:
    """Exact catcher assignments discovered for one matchup."""

    assignments: Tuple[
        CanonicalCatcherTeamAssignment,
        ...,
    ] = ()
    away_candidate_count: int = 0
    home_candidate_count: int = 0
    status: str = "unavailable"
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    discovery_version: str = (
        CANONICAL_CATCHER_ASSIGNMENT_DISCOVERY_VERSION
    )

    def __post_init__(self) -> None:
        if self.discovery_version != (
            CANONICAL_CATCHER_ASSIGNMENT_DISCOVERY_VERSION
        ):
            raise ValueError(
                "unsupported catcher assignment "
                "discovery version"
            )

        for value in self.assignments:
            if not isinstance(
                value,
                CanonicalCatcherTeamAssignment,
            ):
                raise TypeError(
                    "assignments must contain "
                    "CanonicalCatcherTeamAssignment"
                )

    @property
    def ready(self) -> bool:
        return (
            self.status == "ready"
            and len(self.assignments) == 2
            and {
                value.team_side
                for value in self.assignments
            }
            == {"away", "home"}
        )


def discover_confirmed_catcher_assignments(
    *,
    game_pk: Any,
    lineup_fetcher: Optional[
        Callable[[int], Mapping[str, Any]]
    ] = None,
) -> CanonicalCatcherAssignmentDiscovery:
    """
    Discover catchers only from explicit confirmed positions.

    Missing, malformed, or ambiguous position evidence fails open.
    Batting order and roster membership are never used to infer a catcher.
    """

    identifier = _normalize_identifier(game_pk)

    if identifier is None:
        return CanonicalCatcherAssignmentDiscovery(
            status="blocked",
            error_type="missing_game_pk",
            error_message=(
                "game_pk is required for confirmed "
                "catcher assignment discovery"
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
        return CanonicalCatcherAssignmentDiscovery(
            status="error",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )

    if not isinstance(payload, Mapping):
        return CanonicalCatcherAssignmentDiscovery(
            status="blocked",
            error_type="invalid_payload",
            error_message=(
                "lineup fetcher must return a mapping"
            ),
        )

    away_ids = _catcher_identifiers(
        payload.get("away") or []
    )
    home_ids = _catcher_identifiers(
        payload.get("home") or []
    )

    if len(away_ids) > 1 or len(home_ids) > 1:
        return CanonicalCatcherAssignmentDiscovery(
            away_candidate_count=len(away_ids),
            home_candidate_count=len(home_ids),
            status="blocked",
            error_type="ambiguous_catcher_assignment",
            error_message=(
                "confirmed lineup must identify exactly "
                "one catcher per team"
            ),
        )

    assignments = []

    if len(away_ids) == 1:
        assignments.append(
            CanonicalCatcherTeamAssignment(
                catcher_id=away_ids[0],
                team_side="away",
                assignment_source_version=(
                    CONFIRMED_CATCHER_ASSIGNMENT_SOURCE_VERSION
                ),
            )
        )

    if len(home_ids) == 1:
        assignments.append(
            CanonicalCatcherTeamAssignment(
                catcher_id=home_ids[0],
                team_side="home",
                assignment_source_version=(
                    CONFIRMED_CATCHER_ASSIGNMENT_SOURCE_VERSION
                ),
            )
        )

    ready = len(assignments) == 2

    return CanonicalCatcherAssignmentDiscovery(
        assignments=tuple(assignments),
        away_candidate_count=len(away_ids),
        home_candidate_count=len(home_ids),
        status=(
            "ready"
            if ready
            else "partial"
            if assignments
            else "unavailable"
        ),
    )
