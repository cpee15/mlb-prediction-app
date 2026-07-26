"""Monitor paired legacy and calibrated live baserunning shadows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from typing import Any, Dict, Tuple

from .baserunning_output_validation import (
    CanonicalBaserunningOutputValidation,
)


CANONICAL_LIVE_BASERUNNING_SHADOW_OBSERVATION_VERSION = (
    "canonical_live_baserunning_shadow_observation_v1"
)
CANONICAL_LIVE_BASERUNNING_SHADOW_MONITOR_VERSION = (
    "canonical_live_baserunning_shadow_monitor_v1"
)
LIVE_BASERUNNING_SHADOW_MINIMUM_GAME_COUNT = 100
LIVE_BASERUNNING_SHADOW_MINIMUM_DAY_SPAN = 7
LIVE_BASERUNNING_SHADOW_MAXIMUM_DAY_SPAN = 14

_PAIRED_SEED_POLICY = (
    "canonical_trial_seed_same_game_config_v1"
)


def _sha256(value: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _digest(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(
            character not in "0123456789abcdef"
            for character in value
        )
    ):
        raise ValueError(
            f"{field_name} must be a SHA-256 digest"
        )


@dataclass(frozen=True)
class CanonicalLiveBaserunningShadowObservation:
    game_pk: int
    game_date: str
    paired_context_digest: str
    calibrated_transform_digest: str
    legacy_validation: (
        CanonicalBaserunningOutputValidation
    )
    calibrated_validation: (
        CanonicalBaserunningOutputValidation
    )
    input_parity_verified: bool
    seed_parity_verified: bool
    observation_version: str = (
        CANONICAL_LIVE_BASERUNNING_SHADOW_OBSERVATION_VERSION
    )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.game_pk, int)
            or isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be a positive integer"
            )

        try:
            parsed_date = date.fromisoformat(
                self.game_date
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "game_date must be an ISO date"
            ) from exc

        if parsed_date.isoformat() != self.game_date:
            raise ValueError(
                "game_date must use ISO format"
            )

        _digest(
            self.paired_context_digest,
            "paired_context_digest",
        )
        _digest(
            self.calibrated_transform_digest,
            "calibrated_transform_digest",
        )

        for field_name in (
            "legacy_validation",
            "calibrated_validation",
        ):
            if not isinstance(
                getattr(self, field_name),
                CanonicalBaserunningOutputValidation,
            ):
                raise TypeError(
                    f"{field_name} must be canonical"
                )

        if (
            self.legacy_validation.simulation_count
            != self.calibrated_validation.simulation_count
        ):
            raise ValueError(
                "paired validations must use the same "
                "simulation count"
            )

        if (
            self.legacy_validation.catalog_digest
            != self.calibrated_validation.catalog_digest
        ):
            raise ValueError(
                "paired validations must use the same "
                "baserunning catalog"
            )

        if self.ready and (
            not self.input_parity_verified
            or not self.seed_parity_verified
        ):
            raise ValueError(
                "ready paired observation requires input "
                "and seed parity"
            )

        if self.observation_version != (
            CANONICAL_LIVE_BASERUNNING_SHADOW_OBSERVATION_VERSION
        ):
            raise ValueError(
                "unsupported live baserunning shadow "
                "observation version"
            )

    @property
    def ready(self) -> bool:
        return (
            self.legacy_validation.ready
            and self.calibrated_validation.ready
        )

    @property
    def status(self) -> str:
        if self.ready:
            return "ready"
        if (
            self.legacy_validation.status == "error"
            or self.calibrated_validation.status == "error"
        ):
            return "error"
        return "unavailable"

    @property
    def simulation_count(self) -> int:
        return self.legacy_validation.simulation_count

    @property
    def stolen_base_delta(self) -> float:
        return round(
            self.calibrated_validation
            .stolen_base_mean_total
            - self.legacy_validation
            .stolen_base_mean_total,
            6,
        )

    @property
    def caught_stealing_delta(self) -> float:
        return round(
            self.calibrated_validation
            .caught_stealing_mean_total
            - self.legacy_validation
            .caught_stealing_mean_total,
            6,
        )

    @property
    def digest(self) -> str:
        return _sha256(
            {
                "observation_version": (
                    self.observation_version
                ),
                "game_pk": self.game_pk,
                "game_date": self.game_date,
                "paired_context_digest": (
                    self.paired_context_digest
                ),
                "calibrated_transform_digest": (
                    self.calibrated_transform_digest
                ),
                "legacy_validation": (
                    self.legacy_validation.to_diagnostics()
                ),
                "calibrated_validation": (
                    self.calibrated_validation.to_diagnostics()
                ),
                "input_parity_verified": (
                    self.input_parity_verified
                ),
                "seed_parity_verified": (
                    self.seed_parity_verified
                ),
                "paired_seed_policy": (
                    _PAIRED_SEED_POLICY
                ),
            }
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.observation_version,
            "status": self.status,
            "ready": self.ready,
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "simulation_count": self.simulation_count,
            "paired_context_digest": (
                self.paired_context_digest
            ),
            "calibrated_transform_digest": (
                self.calibrated_transform_digest
            ),
            "input_parity_verified": (
                self.input_parity_verified
            ),
            "seed_parity_verified": (
                self.seed_parity_verified
            ),
            "paired_seed_policy": _PAIRED_SEED_POLICY,
            "legacy_validation": (
                self.legacy_validation.to_diagnostics()
            ),
            "calibrated_validation": (
                self.calibrated_validation.to_diagnostics()
            ),
            "stolen_base_delta": self.stolen_base_delta,
            "caught_stealing_delta": (
                self.caught_stealing_delta
            ),
            "observation_digest": self.digest,
            "activation_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalLiveBaserunningShadowMonitor:
    observations: Tuple[
        CanonicalLiveBaserunningShadowObservation,
        ...
    ] = ()
    minimum_game_count: int = (
        LIVE_BASERUNNING_SHADOW_MINIMUM_GAME_COUNT
    )
    minimum_day_span: int = (
        LIVE_BASERUNNING_SHADOW_MINIMUM_DAY_SPAN
    )
    maximum_day_span: int = (
        LIVE_BASERUNNING_SHADOW_MAXIMUM_DAY_SPAN
    )
    monitor_version: str = (
        CANONICAL_LIVE_BASERUNNING_SHADOW_MONITOR_VERSION
    )

    def __post_init__(self) -> None:
        if not all(
            isinstance(
                value,
                CanonicalLiveBaserunningShadowObservation,
            )
            for value in self.observations
        ):
            raise TypeError(
                "observations must be canonical"
            )

        game_ids = tuple(
            value.game_pk
            for value in self.observations
        )
        if len(set(game_ids)) != len(game_ids):
            raise ValueError(
                "live shadow game identifiers must be unique"
            )

        ordered = tuple(
            sorted(
                self.observations,
                key=lambda value: (
                    value.game_date,
                    value.game_pk,
                ),
            )
        )
        if ordered != self.observations:
            raise ValueError(
                "live shadow observations must be ordered"
            )

        if self.minimum_game_count < 1:
            raise ValueError(
                "minimum_game_count must be positive"
            )
        if self.minimum_day_span < 1:
            raise ValueError(
                "minimum_day_span must be positive"
            )
        if self.maximum_day_span < self.minimum_day_span:
            raise ValueError(
                "maximum_day_span cannot be below minimum"
            )
        if self.monitor_version != (
            CANONICAL_LIVE_BASERUNNING_SHADOW_MONITOR_VERSION
        ):
            raise ValueError(
                "unsupported live baserunning shadow "
                "monitor version"
            )

    @property
    def game_count(self) -> int:
        return len(self.observations)

    @property
    def ready_count(self) -> int:
        return sum(
            value.ready
            for value in self.observations
        )

    @property
    def unavailable_count(self) -> int:
        return sum(
            value.status == "unavailable"
            for value in self.observations
        )

    @property
    def error_count(self) -> int:
        return sum(
            value.status == "error"
            for value in self.observations
        )

    @property
    def day_span(self) -> int:
        if not self.observations:
            return 0

        start = date.fromisoformat(
            self.observations[0].game_date
        )
        end = date.fromisoformat(
            self.observations[-1].game_date
        )
        return (end - start).days + 1

    @property
    def live_shadow_complete(self) -> bool:
        return (
            self.game_count >= self.minimum_game_count
            and self.day_span >= self.minimum_day_span
            and self.day_span <= self.maximum_day_span
            and self.ready_count == self.game_count
            and self.error_count == 0
            and self.unavailable_count == 0
        )

    @property
    def digest(self) -> str:
        return _sha256(
            {
                "monitor_version": self.monitor_version,
                "minimum_game_count": (
                    self.minimum_game_count
                ),
                "minimum_day_span": (
                    self.minimum_day_span
                ),
                "maximum_day_span": (
                    self.maximum_day_span
                ),
                "observation_digests": [
                    value.digest
                    for value in self.observations
                ],
            }
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.monitor_version,
            "game_count": self.game_count,
            "ready_count": self.ready_count,
            "unavailable_count": (
                self.unavailable_count
            ),
            "error_count": self.error_count,
            "day_span": self.day_span,
            "minimum_game_count": (
                self.minimum_game_count
            ),
            "minimum_day_span": self.minimum_day_span,
            "maximum_day_span": self.maximum_day_span,
            "live_shadow_complete": (
                self.live_shadow_complete
            ),
            "stolen_base_delta_total": round(
                sum(
                    value.stolen_base_delta
                    for value in self.observations
                ),
                6,
            ),
            "caught_stealing_delta_total": round(
                sum(
                    value.caught_stealing_delta
                    for value in self.observations
                ),
                6,
            ),
            "observation_digests": tuple(
                value.digest
                for value in self.observations
            ),
            "monitor_digest": self.digest,
            "eligible_for_activation_review": (
                self.live_shadow_complete
            ),
            "activation_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def summarize_live_baserunning_shadow(
    observations: Tuple[
        CanonicalLiveBaserunningShadowObservation,
        ...
    ],
) -> CanonicalLiveBaserunningShadowMonitor:
    ordered = tuple(
        sorted(
            observations,
            key=lambda value: (
                value.game_date,
                value.game_pk,
            ),
        )
    )
    return CanonicalLiveBaserunningShadowMonitor(
        observations=ordered,
    )
