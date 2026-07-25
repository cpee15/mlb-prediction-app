"""Inventory immutable historical probability artifacts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
import hashlib
import json
from typing import Any, Dict, Optional, Tuple

from .mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)


CANONICAL_HISTORICAL_PROBABILITY_ARTIFACT_INVENTORY_VERSION = (
    "canonical_historical_probability_artifact_inventory_v1"
)
HISTORICAL_PROBABILITY_ARTIFACT_SOURCE = (
    "historical_probability_artifact_archive_v1"
)


def _available(value: Optional[str]) -> bool:
    return bool(
        isinstance(value, str)
        and value.strip()
        and value != "unavailable"
    )


def _validate_digest(
    value: Optional[str],
    *,
    field_name: str,
) -> None:
    if value is None:
        return

    if not isinstance(value, str):
        raise TypeError(
            f"{field_name} must be a string or None"
        )

    if len(value) != 64:
        raise ValueError(
            f"{field_name} must be a SHA-256 hex digest"
        )

    try:
        int(value, 16)
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must be a SHA-256 hex digest"
        ) from exc


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class CanonicalHistoricalProbabilityArtifactRecord:
    game_pk: int
    game_date: str
    source: str = "unavailable"
    artifact_as_of_date: Optional[str] = None
    provider_identity: Optional[str] = None
    exact_artifact_digest: Optional[str] = None
    fallback_catalog_digest: Optional[str] = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.game_pk, int)
            or isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be a positive integer"
            )

        parsed_game_date = date.fromisoformat(
            self.game_date
        )
        if parsed_game_date.isoformat() != self.game_date:
            raise ValueError(
                "game_date must use ISO format"
            )

        if self.artifact_as_of_date is not None:
            parsed_artifact_date = date.fromisoformat(
                self.artifact_as_of_date
            )
            if (
                parsed_artifact_date.isoformat()
                != self.artifact_as_of_date
            ):
                raise ValueError(
                    "artifact_as_of_date must use ISO format"
                )

        for field_name in (
            "exact_artifact_digest",
            "fallback_catalog_digest",
        ):
            _validate_digest(
                getattr(self, field_name),
                field_name=field_name,
            )

    @property
    def source_ready(self) -> bool:
        return (
            self.source
            == HISTORICAL_PROBABILITY_ARTIFACT_SOURCE
        )

    @property
    def as_of_date_ready(self) -> bool:
        return (
            self.source_ready
            and self.artifact_as_of_date
            == self.game_date
        )

    @property
    def future_data_rejected(self) -> bool:
        return bool(
            self.artifact_as_of_date
            and self.artifact_as_of_date
            > self.game_date
        )

    @property
    def provider_ready(self) -> bool:
        return (
            self.as_of_date_ready
            and _available(self.provider_identity)
        )

    @property
    def exact_artifact_ready(self) -> bool:
        return (
            self.as_of_date_ready
            and _available(
                self.exact_artifact_digest
            )
        )

    @property
    def fallback_catalog_ready(self) -> bool:
        return (
            self.as_of_date_ready
            and _available(
                self.fallback_catalog_digest
            )
        )

    @property
    def ready(self) -> bool:
        return (
            self.provider_ready
            and self.exact_artifact_ready
            and self.fallback_catalog_ready
        )

    @property
    def missing_requirements(self) -> Tuple[str, ...]:
        missing = []

        if not self.source_ready:
            missing.append(
                "missing_historical_artifact_source"
            )
        elif not self.as_of_date_ready:
            missing.append(
                "artifact_as_of_date_mismatch"
            )

        if not self.provider_ready:
            missing.append(
                "missing_probability_provider"
            )
        if not self.exact_artifact_ready:
            missing.append(
                "missing_exact_artifact"
            )
        if not self.fallback_catalog_ready:
            missing.append(
                "missing_fallback_catalog"
            )

        return tuple(missing)

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "source": self.source,
            "artifact_as_of_date": (
                self.artifact_as_of_date
            ),
            "ready": self.ready,
            "provider_ready": self.provider_ready,
            "exact_artifact_ready": (
                self.exact_artifact_ready
            ),
            "fallback_catalog_ready": (
                self.fallback_catalog_ready
            ),
            "missing_requirements": (
                self.missing_requirements
            ),
            "future_data_rejected": (
                self.future_data_rejected
            ),
            "provider_identity": (
                self.provider_identity
            ),
            "exact_artifact_digest": (
                self.exact_artifact_digest
            ),
            "fallback_catalog_digest": (
                self.fallback_catalog_digest
            ),
            "probability_records_exposed": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalHistoricalProbabilityArtifactInventory:
    observed_window_digest: str
    games: Tuple[
        CanonicalHistoricalProbabilityArtifactRecord,
        ...,
    ]
    missing_requirement_counts: Tuple[
        Tuple[str, int],
        ...,
    ]
    inventory_digest: str
    inventory_version: str = (
        CANONICAL_HISTORICAL_PROBABILITY_ARTIFACT_INVENTORY_VERSION
    )

    def __post_init__(self) -> None:
        if not self.games:
            raise ValueError(
                "games must contain inventory records"
            )

        for field_name in (
            "observed_window_digest",
            "inventory_digest",
        ):
            _validate_digest(
                getattr(self, field_name),
                field_name=field_name,
            )

        if self.inventory_version != (
            CANONICAL_HISTORICAL_PROBABILITY_ARTIFACT_INVENTORY_VERSION
        ):
            raise ValueError(
                "unsupported historical probability "
                "artifact inventory version"
            )

    @property
    def game_count(self) -> int:
        return len(self.games)

    @property
    def provider_ready_game_count(self) -> int:
        return sum(
            value.provider_ready
            for value in self.games
        )

    @property
    def exact_artifact_ready_game_count(
        self,
    ) -> int:
        return sum(
            value.exact_artifact_ready
            for value in self.games
        )

    @property
    def fallback_catalog_ready_game_count(
        self,
    ) -> int:
        return sum(
            value.fallback_catalog_ready
            for value in self.games
        )

    @property
    def ready_game_count(self) -> int:
        return sum(
            value.ready
            for value in self.games
        )

    @property
    def ready(self) -> bool:
        return self.ready_game_count == self.game_count

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.inventory_version,
            "ready": self.ready,
            "game_count": self.game_count,
            "provider_ready_game_count": (
                self.provider_ready_game_count
            ),
            "exact_artifact_ready_game_count": (
                self.exact_artifact_ready_game_count
            ),
            "fallback_catalog_ready_game_count": (
                self.fallback_catalog_ready_game_count
            ),
            "ready_game_count": self.ready_game_count,
            "blocked_game_count": (
                self.game_count
                - self.ready_game_count
            ),
            "missing_requirement_counts": dict(
                self.missing_requirement_counts
            ),
            "observed_window_digest": (
                self.observed_window_digest
            ),
            "inventory_digest": (
                self.inventory_digest
            ),
            "games": tuple(
                value.to_diagnostics()
                for value in self.games
            ),
            "probability_records_exposed": False,
            "historical_reconstruction_required": (
                not self.ready
            ),
            "historical_replay_permitted": self.ready,
            "historical_replay_executed": False,
            "calibration_execution_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def inventory_historical_probability_artifacts(
    *,
    observed: CanonicalMlbPlayByPlayBaserunningSnapshot,
    artifacts: Tuple[
        CanonicalHistoricalProbabilityArtifactRecord,
        ...,
    ] = (),
) -> CanonicalHistoricalProbabilityArtifactInventory:
    """
    Inventory archived artifacts without constructing replacements.

    Missing games are materialized as unavailable inventory rows. Current
    workspaces, in-memory caches, and future-dated data are not accepted.
    """

    if not isinstance(
        observed,
        CanonicalMlbPlayByPlayBaserunningSnapshot,
    ):
        raise TypeError(
            "observed must be "
            "CanonicalMlbPlayByPlayBaserunningSnapshot"
        )

    if not isinstance(artifacts, tuple):
        raise TypeError("artifacts must be a tuple")

    by_id = {}

    for value in artifacts:
        if not isinstance(
            value,
            CanonicalHistoricalProbabilityArtifactRecord,
        ):
            raise TypeError(
                "artifacts must contain "
                "CanonicalHistoricalProbabilityArtifactRecord"
            )

        if value.game_pk in by_id:
            raise ValueError(
                "historical probability artifact "
                "game identifiers must be unique"
            )

        by_id[value.game_pk] = value

    observed_by_id = {
        value.game_pk: value
        for value in observed.games
    }

    unknown_ids = set(by_id) - set(observed_by_id)
    if unknown_ids:
        raise ValueError(
            "historical probability artifacts must "
            "belong to the observed window"
        )

    ordered = []

    for game_pk, observed_game in sorted(
        observed_by_id.items(),
        key=lambda item: (
            item[1].game_date,
            item[0],
        ),
    ):
        value = by_id.get(game_pk)

        if value is None:
            value = (
                CanonicalHistoricalProbabilityArtifactRecord(
                    game_pk=game_pk,
                    game_date=observed_game.game_date,
                )
            )
        elif value.game_date != observed_game.game_date:
            raise ValueError(
                "historical probability artifact "
                "game_date must match observed game_date"
            )

        ordered.append(value)

    reasons = tuple(
        sorted(
            {
                reason
                for value in ordered
                for reason in value.missing_requirements
            }
        )
    )
    missing_counts = tuple(
        (
            reason,
            sum(
                reason in value.missing_requirements
                for value in ordered
            ),
        )
        for reason in reasons
    )

    inventory_digest = _sha256(
        {
            "observed_window_digest": observed.digest,
            "games": [
                asdict(value)
                for value in ordered
            ],
            "missing_requirement_counts": (
                missing_counts
            ),
        }
    )

    return CanonicalHistoricalProbabilityArtifactInventory(
        observed_window_digest=observed.digest,
        games=tuple(ordered),
        missing_requirement_counts=missing_counts,
        inventory_digest=inventory_digest,
    )
