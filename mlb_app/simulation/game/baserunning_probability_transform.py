"""Explicit non-default transforms for baserunning probabilities."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
from typing import Any, Dict

from .baserunning_resolver import (
    CanonicalBaserunningEvidence,
)


CANONICAL_BASERUNNING_PROBABILITY_TRANSFORM_VERSION = (
    "canonical_baserunning_probability_transform_v1"
)


def _finite_between(
    value: float,
    name: str,
    lower: float,
    upper: float,
) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be finite"
        ) from exc

    if (
        not math.isfinite(normalized)
        or normalized < lower
        or normalized > upper
    ):
        raise ValueError(
            f"{name} must be between "
            f"{lower} and {upper}"
        )

    return normalized


@dataclass(frozen=True)
class CanonicalBaserunningProbabilityTransform:
    """
    Apply one explicit candidate transform to evaluator probabilities.

    The identity defaults preserve existing behavior. Construction alone
    does not activate the transform in production or shadow execution.
    """

    attempt_probability_multiplier: float = 1.0
    success_rate_adjustment: float = 0.0
    transform_version: str = (
        CANONICAL_BASERUNNING_PROBABILITY_TRANSFORM_VERSION
    )

    def __post_init__(self) -> None:
        _finite_between(
            self.attempt_probability_multiplier,
            "attempt_probability_multiplier",
            0.0,
            1.0,
        )
        _finite_between(
            self.success_rate_adjustment,
            "success_rate_adjustment",
            -1.0,
            1.0,
        )

        if self.transform_version != (
            CANONICAL_BASERUNNING_PROBABILITY_TRANSFORM_VERSION
        ):
            raise ValueError(
                "unsupported baserunning probability "
                "transform version"
            )

    @property
    def is_identity(self) -> bool:
        return (
            self.attempt_probability_multiplier == 1.0
            and self.success_rate_adjustment == 0.0
        )

    @property
    def digest(self) -> str:
        parts = (
            self.transform_version,
            repr(self.attempt_probability_multiplier),
            repr(self.success_rate_adjustment),
        )

        return hashlib.sha256(
            "\x1f".join(parts).encode("utf-8")
        ).hexdigest()

    def apply(
        self,
        evidence: CanonicalBaserunningEvidence,
    ) -> CanonicalBaserunningEvidence:
        if not isinstance(
            evidence,
            CanonicalBaserunningEvidence,
        ):
            raise TypeError(
                "evidence must be CanonicalBaserunningEvidence"
            )

        attempt_probability = round(
            min(
                max(
                    evidence.attempt_probability
                    * self.attempt_probability_multiplier,
                    0.0,
                ),
                1.0,
            ),
            6,
        )
        success_probability = round(
            min(
                max(
                    evidence.success_probability
                    + self.success_rate_adjustment,
                    0.0,
                ),
                1.0,
            ),
            6,
        )

        return CanonicalBaserunningEvidence(
            pitcher_id=evidence.pitcher_id,
            attempt_probability=attempt_probability,
            success_probability=success_probability,
            probability_provenance=(
                evidence.probability_provenance
                + "|"
                + self.transform_version
                + ":"
                + self.digest
            ),
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.transform_version,
            "attempt_probability_multiplier": (
                self.attempt_probability_multiplier
            ),
            "success_rate_adjustment": (
                self.success_rate_adjustment
            ),
            "identity_transform": self.is_identity,
            "transform_digest": self.digest,
            "activation_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }
