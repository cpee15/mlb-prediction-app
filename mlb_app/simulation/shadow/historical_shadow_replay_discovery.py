"""Discover reproducible historical shadow replay inputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional, Tuple

from .mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)


CANONICAL_HISTORICAL_SHADOW_REPLAY_DISCOVERY_VERSION = (
    "canonical_historical_shadow_replay_discovery_v1"
)

_INPUT_REQUIREMENTS = (
    ("lineups_ready", "missing_lineups"),
    ("bullpens_ready", "missing_bullpens"),
    (
        "probability_provider_ready",
        "missing_probability_provider",
    ),
    (
        "exact_artifact_ready",
        "missing_exact_artifact",
    ),
    (
        "fallback_catalog_ready",
        "missing_fallback_catalog",
    ),
    (
        "baserunning_catalog_ready",
        "missing_baserunning_catalog",
    ),
)

_PROVENANCE_REQUIREMENTS = (
    (
        "probability_provider_identity",
        "missing_probability_provider_identity",
    ),
    (
        "exact_artifact_digest",
        "missing_exact_artifact_digest",
    ),
    (
        "fallback_catalog_digest",
        "missing_fallback_catalog_digest",
    ),
    (
        "baserunning_catalog_digest",
        "missing_baserunning_catalog_digest",
    ),
)


def _available_text(value: Optional[str]) -> bool:
    return bool(
        isinstance(value, str)
        and value.strip()
        and value != "unavailable"
    )


@dataclass(frozen=True)
class CanonicalHistoricalShadowReplayInputGame:
    game_pk: int
    game_date: str
    lineups_ready: bool
    bullpens_ready: bool
    probability_provider_ready: bool
    exact_artifact_ready: bool
    fallback_catalog_ready: bool
    baserunning_catalog_ready: bool
    probability_provider_identity: Optional[str] = None
    exact_artifact_digest: Optional[str] = None
    fallback_catalog_digest: Optional[str] = None
    baserunning_catalog_digest: Optional[str] = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.game_pk, int)
            or isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be a positive integer"
            )

        if not isinstance(self.game_date, str):
            raise TypeError(
                "game_date must be a string"
            )

        parsed_date = date.fromisoformat(
            self.game_date
        )
        if parsed_date.isoformat() != self.game_date:
            raise ValueError(
                "game_date must use ISO format"
            )

        for field_name, _ in _INPUT_REQUIREMENTS:
            if not isinstance(
                getattr(self, field_name),
                bool,
            ):
                raise TypeError(
                    f"{field_name} must be boolean"
                )

    @property
    def missing_requirements(self) -> Tuple[str, ...]:
        missing = [
            reason
            for field_name, reason
            in _INPUT_REQUIREMENTS
            if not getattr(self, field_name)
        ]

        if not missing:
            missing.extend(
                reason
                for field_name, reason
                in _PROVENANCE_REQUIREMENTS
                if not _available_text(
                    getattr(self, field_name)
                )
            )

        return tuple(missing)

    @property
    def ready(self) -> bool:
        return not self.missing_requirements

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "ready": self.ready,
            "missing_requirements": (
                self.missing_requirements
            ),
            "lineups_ready": self.lineups_ready,
            "bullpens_ready": self.bullpens_ready,
            "probability_provider_ready": (
                self.probability_provider_ready
            ),
            "exact_artifact_ready": (
                self.exact_artifact_ready
            ),
            "fallback_catalog_ready": (
                self.fallback_catalog_ready
            ),
            "baserunning_catalog_ready": (
                self.baserunning_catalog_ready
            ),
            "probability_provider_identity": (
                self.probability_provider_identity
            ),
            "exact_artifact_digest": (
                self.exact_artifact_digest
            ),
            "fallback_catalog_digest": (
                self.fallback_catalog_digest
            ),
            "baserunning_catalog_digest": (
                self.baserunning_catalog_digest
            ),
            "lineup_identifiers_exposed": False,
            "probability_records_exposed": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalHistoricalShadowReplayDiscovery:
    games: Tuple[
        CanonicalHistoricalShadowReplayInputGame,
        ...,
    ]
    missing_requirement_counts: Tuple[
        Tuple[str, int],
        ...,
    ]
    discovery_version: str = (
        CANONICAL_HISTORICAL_SHADOW_REPLAY_DISCOVERY_VERSION
    )

    def __post_init__(self) -> None:
        if not self.games:
            raise ValueError(
                "games must contain replay inputs"
            )

        if self.discovery_version != (
            CANONICAL_HISTORICAL_SHADOW_REPLAY_DISCOVERY_VERSION
        ):
            raise ValueError(
                "unsupported historical shadow "
                "replay discovery version"
            )

    @property
    def game_count(self) -> int:
        return len(self.games)

    @property
    def ready_game_count(self) -> int:
        return sum(
            value.ready
            for value in self.games
        )

    @property
    def blocked_game_count(self) -> int:
        return (
            self.game_count
            - self.ready_game_count
        )

    @property
    def ready(self) -> bool:
        return self.blocked_game_count == 0

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.discovery_version,
            "ready": self.ready,
            "game_count": self.game_count,
            "ready_game_count": (
                self.ready_game_count
            ),
            "blocked_game_count": (
                self.blocked_game_count
            ),
            "missing_requirement_counts": dict(
                self.missing_requirement_counts
            ),
            "games": tuple(
                value.to_diagnostics()
                for value in self.games
            ),
            "historical_replay_permitted": (
                self.ready
            ),
            "calibration_execution_permitted": (
                False
            ),
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def discover_historical_shadow_replay_inputs(
    *,
    observed: CanonicalMlbPlayByPlayBaserunningSnapshot,
    games: Tuple[
        CanonicalHistoricalShadowReplayInputGame,
        ...,
    ],
) -> CanonicalHistoricalShadowReplayDiscovery:
    """
    Audit exact historical replay readiness for a complete observed window.

    This does not fetch inputs, manufacture fallbacks, execute simulations,
    approve calibration, or alter production authority.
    """

    if not isinstance(
        observed,
        CanonicalMlbPlayByPlayBaserunningSnapshot,
    ):
        raise TypeError(
            "observed must be "
            "CanonicalMlbPlayByPlayBaserunningSnapshot"
        )

    if not isinstance(games, tuple):
        raise TypeError(
            "games must be a tuple"
        )
    if not games:
        raise ValueError(
            "games must contain replay inputs"
        )

    for value in games:
        if not isinstance(
            value,
            CanonicalHistoricalShadowReplayInputGame,
        ):
            raise TypeError(
                "games must contain "
                "CanonicalHistoricalShadowReplayInputGame"
            )

    games_by_id = {}
    for value in games:
        if value.game_pk in games_by_id:
            raise ValueError(
                "historical replay game identifiers "
                "must be unique"
            )
        games_by_id[value.game_pk] = value

    observed_by_id = {
        value.game_pk: value
        for value in observed.games
    }

    if set(games_by_id) != set(observed_by_id):
        raise ValueError(
            "historical replay games must exactly match "
            "observed play-by-play games"
        )

    ordered = []
    for game_pk in sorted(
        games_by_id,
        key=lambda value: (
            games_by_id[value].game_date,
            value,
        ),
    ):
        replay_game = games_by_id[game_pk]
        observed_game = observed_by_id[game_pk]

        if (
            replay_game.game_date
            != observed_game.game_date
        ):
            raise ValueError(
                "historical replay game_date must "
                "match observed official game_date"
            )

        ordered.append(replay_game)

    reasons = sorted(
        {
            reason
            for value in ordered
            for reason in value.missing_requirements
        }
    )
    counts = tuple(
        (
            reason,
            sum(
                reason
                in value.missing_requirements
                for value in ordered
            ),
        )
        for reason in reasons
    )

    return CanonicalHistoricalShadowReplayDiscovery(
        games=tuple(ordered),
        missing_requirement_counts=counts,
    )
