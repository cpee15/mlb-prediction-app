"""Evaluate shadow-only historical baserunning calibration candidates."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Dict, Tuple

from .baserunning_calibration_comparison import (
    CanonicalBaserunningCalibrationComparison,
)
from .baserunning_calibration_gate import (
    CanonicalBaserunningCalibrationPolicy,
)


CANONICAL_HISTORICAL_BASERUNNING_CALIBRATION_CANDIDATE_VERSION = (
    "canonical_historical_baserunning_calibration_candidate_v1"
)
CANONICAL_HISTORICAL_BASERUNNING_CALIBRATION_GRID_VERSION = (
    "canonical_historical_baserunning_calibration_grid_v1"
)


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _validate_digest(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(
            character not in "0123456789abcdef"
            for character in value
        )
    ):
        raise ValueError(
            f"{name} must be a SHA256 digest"
        )


def _finite_rate(
    value: float,
    name: str,
    *,
    upper: float = 1.0,
) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be finite"
        ) from exc

    if (
        not math.isfinite(normalized)
        or normalized < 0.0
        or normalized > upper
    ):
        raise ValueError(
            f"{name} must be between zero and {upper}"
        )

    return normalized


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningCalibrationCandidate:
    candidate_name: str
    attempt_probability_multiplier: float
    success_rate_adjustment: float
    candidate_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_CALIBRATION_CANDIDATE_VERSION
    )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.candidate_name, str)
            or not self.candidate_name.strip()
        ):
            raise ValueError(
                "candidate_name is required"
            )

        _finite_rate(
            self.attempt_probability_multiplier,
            "attempt_probability_multiplier",
        )
        _finite_rate(
            self.success_rate_adjustment,
            "success_rate_adjustment",
        )

        if self.candidate_version != (
            CANONICAL_HISTORICAL_BASERUNNING_CALIBRATION_CANDIDATE_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "calibration candidate version"
            )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "candidate_name": self.candidate_name,
            "attempt_probability_multiplier": (
                self.attempt_probability_multiplier
            ),
            "success_rate_adjustment": (
                self.success_rate_adjustment
            ),
            "candidate_version": self.candidate_version,
        }


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningCalibrationCandidateResult:
    candidate: CanonicalHistoricalBaserunningCalibrationCandidate
    game_count: int
    projected_stolen_bases: float
    projected_caught_stealing: float
    projected_attempts: float
    projected_success_rate: float
    stolen_base_error_per_game: float
    caught_stealing_error_per_game: float
    attempt_error_per_game: float
    success_rate_absolute_error: float
    failures: Tuple[str, ...]
    objective_score: float

    def __post_init__(self) -> None:
        if not isinstance(
            self.candidate,
            CanonicalHistoricalBaserunningCalibrationCandidate,
        ):
            raise TypeError(
                "candidate must be a canonical candidate"
            )
        if self.game_count <= 0:
            raise ValueError(
                "game_count must be positive"
            )
        if not math.isfinite(self.objective_score):
            raise ValueError(
                "objective_score must be finite"
            )

    @property
    def calibration_gate_passed(self) -> bool:
        return not self.failures

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            **self.candidate.to_diagnostics(),
            "game_count": self.game_count,
            "projected_stolen_bases": (
                self.projected_stolen_bases
            ),
            "projected_caught_stealing": (
                self.projected_caught_stealing
            ),
            "projected_attempts": self.projected_attempts,
            "projected_success_rate": (
                self.projected_success_rate
            ),
            "stolen_base_error_per_game": (
                self.stolen_base_error_per_game
            ),
            "caught_stealing_error_per_game": (
                self.caught_stealing_error_per_game
            ),
            "attempt_error_per_game": (
                self.attempt_error_per_game
            ),
            "success_rate_absolute_error": (
                self.success_rate_absolute_error
            ),
            "calibration_gate_passed": (
                self.calibration_gate_passed
            ),
            "failures": self.failures,
            "objective_score": self.objective_score,
            "activation_permitted": False,
            "production_activation": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningCalibrationGrid:
    baseline_evaluation_digest: str
    policy_version: str
    results: Tuple[
        CanonicalHistoricalBaserunningCalibrationCandidateResult,
        ...,
    ]
    selected_candidate_name: str
    digest: str
    grid_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_CALIBRATION_GRID_VERSION
    )

    def __post_init__(self) -> None:
        _validate_digest(
            self.baseline_evaluation_digest,
            "baseline_evaluation_digest",
        )
        _validate_digest(self.digest, "digest")

        if not self.results:
            raise ValueError(
                "results must contain candidates"
            )

        names = tuple(
            result.candidate.candidate_name
            for result in self.results
        )
        if len(names) != len(set(names)):
            raise ValueError(
                "candidate names must be unique"
            )
        if self.selected_candidate_name not in names:
            raise ValueError(
                "selected candidate must exist in results"
            )
        if self.grid_version != (
            CANONICAL_HISTORICAL_BASERUNNING_CALIBRATION_GRID_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "calibration grid version"
            )

    @property
    def selected_result(
        self,
    ) -> CanonicalHistoricalBaserunningCalibrationCandidateResult:
        return next(
            result
            for result in self.results
            if (
                result.candidate.candidate_name
                == self.selected_candidate_name
            )
        )

    @property
    def passing_candidate_count(self) -> int:
        return sum(
            result.calibration_gate_passed
            for result in self.results
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.grid_version,
            "ready": True,
            "baseline_evaluation_digest": (
                self.baseline_evaluation_digest
            ),
            "policy_version": self.policy_version,
            "candidate_count": len(self.results),
            "passing_candidate_count": (
                self.passing_candidate_count
            ),
            "selected_candidate_name": (
                self.selected_candidate_name
            ),
            "selected_candidate": (
                self.selected_result.to_diagnostics()
            ),
            "candidates": tuple(
                result.to_diagnostics()
                for result in self.results
            ),
            "grid_digest": self.digest,
            "candidate_requires_replay_validation": True,
            "eligible_for_activation_review": False,
            "activation_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def build_historical_baserunning_calibration_candidates(
) -> Tuple[
    CanonicalHistoricalBaserunningCalibrationCandidate,
    ...,
]:
    candidates = [
        CanonicalHistoricalBaserunningCalibrationCandidate(
            candidate_name="baseline_unmodified",
            attempt_probability_multiplier=1.0,
            success_rate_adjustment=0.0,
        )
    ]

    for attempt_multiplier in (
        0.45,
        0.50,
        0.52,
        0.55,
        0.60,
    ):
        for success_adjustment in (
            0.05,
            0.075,
            0.09,
            0.10,
        ):
            candidates.append(
                CanonicalHistoricalBaserunningCalibrationCandidate(
                    candidate_name=(
                        "attempt_"
                        f"{attempt_multiplier:.3f}_"
                        "success_plus_"
                        f"{success_adjustment:.3f}"
                    ),
                    attempt_probability_multiplier=(
                        attempt_multiplier
                    ),
                    success_rate_adjustment=(
                        success_adjustment
                    ),
                )
            )

    return tuple(candidates)


def evaluate_historical_baserunning_calibration_candidates(
    *,
    baseline: CanonicalBaserunningCalibrationComparison,
    baseline_evaluation_digest: str,
    policy: CanonicalBaserunningCalibrationPolicy,
    candidates: Tuple[
        CanonicalHistoricalBaserunningCalibrationCandidate,
        ...,
    ] | None = None,
) -> CanonicalHistoricalBaserunningCalibrationGrid:
    if not isinstance(
        baseline,
        CanonicalBaserunningCalibrationComparison,
    ):
        raise TypeError(
            "baseline must be a canonical calibration comparison"
        )
    if not baseline.ready:
        raise ValueError(
            "baseline comparison must be ready"
        )
    _validate_digest(
        baseline_evaluation_digest,
        "baseline_evaluation_digest",
    )
    if not isinstance(
        policy,
        CanonicalBaserunningCalibrationPolicy,
    ):
        raise TypeError(
            "policy must be a canonical calibration policy"
        )
    if (
        baseline.projected_success_rate is None
        or baseline.observed_success_rate is None
    ):
        raise ValueError(
            "baseline success rates must be available"
        )

    selected_candidates = (
        candidates
        if candidates is not None
        else build_historical_baserunning_calibration_candidates()
    )
    if not selected_candidates:
        raise ValueError(
            "candidates must not be empty"
        )

    results = []

    for candidate in selected_candidates:
        if not isinstance(
            candidate,
            CanonicalHistoricalBaserunningCalibrationCandidate,
        ):
            raise TypeError(
                "candidates must contain canonical candidates"
            )

        projected_attempts = round(
            baseline.projected_attempts
            * candidate.attempt_probability_multiplier,
            6,
        )
        projected_success_rate = round(
            min(
                max(
                    baseline.projected_success_rate
                    + candidate.success_rate_adjustment,
                    0.0,
                ),
                1.0,
            ),
            6,
        )
        projected_stolen_bases = round(
            projected_attempts
            * projected_success_rate,
            6,
        )
        projected_caught_stealing = round(
            projected_attempts
            - projected_stolen_bases,
            6,
        )

        stolen_base_error_per_game = round(
            abs(
                projected_stolen_bases
                - baseline.observed_stolen_bases
            )
            / baseline.game_count,
            6,
        )
        caught_stealing_error_per_game = round(
            abs(
                projected_caught_stealing
                - baseline.observed_caught_stealing
            )
            / baseline.game_count,
            6,
        )
        attempt_error_per_game = round(
            abs(
                projected_attempts
                - baseline.observed_attempts
            )
            / baseline.game_count,
            6,
        )
        success_rate_absolute_error = round(
            abs(
                projected_success_rate
                - baseline.observed_success_rate
            ),
            6,
        )

        failures = []
        if baseline.game_count < policy.minimum_game_count:
            failures.append("minimum_game_count_not_met")
        if stolen_base_error_per_game > (
            policy.maximum_stolen_base_error_per_game
        ):
            failures.append(
                "stolen_base_error_per_game_exceeded"
            )
        if caught_stealing_error_per_game > (
            policy.maximum_caught_stealing_error_per_game
        ):
            failures.append(
                "caught_stealing_error_per_game_exceeded"
            )
        if attempt_error_per_game > (
            policy.maximum_attempt_error_per_game
        ):
            failures.append(
                "attempt_error_per_game_exceeded"
            )
        if success_rate_absolute_error > (
            policy.maximum_success_rate_absolute_error
        ):
            failures.append(
                "success_rate_absolute_error_exceeded"
            )

        objective_score = round(
            stolen_base_error_per_game
            + caught_stealing_error_per_game
            + attempt_error_per_game
            + success_rate_absolute_error,
            6,
        )

        results.append(
            CanonicalHistoricalBaserunningCalibrationCandidateResult(
                candidate=candidate,
                game_count=baseline.game_count,
                projected_stolen_bases=projected_stolen_bases,
                projected_caught_stealing=(
                    projected_caught_stealing
                ),
                projected_attempts=projected_attempts,
                projected_success_rate=(
                    projected_success_rate
                ),
                stolen_base_error_per_game=(
                    stolen_base_error_per_game
                ),
                caught_stealing_error_per_game=(
                    caught_stealing_error_per_game
                ),
                attempt_error_per_game=(
                    attempt_error_per_game
                ),
                success_rate_absolute_error=(
                    success_rate_absolute_error
                ),
                failures=tuple(failures),
                objective_score=objective_score,
            )
        )

    ranked = tuple(
        sorted(
            results,
            key=lambda result: (
                not result.calibration_gate_passed,
                result.objective_score,
                result.candidate.candidate_name,
            ),
        )
    )
    selected = ranked[0]

    grid_digest = _sha256(
        {
            "schema_version": (
                CANONICAL_HISTORICAL_BASERUNNING_CALIBRATION_GRID_VERSION
            ),
            "baseline_evaluation_digest": (
                baseline_evaluation_digest
            ),
            "policy_version": policy.policy_version,
            "results": [
                result.to_diagnostics()
                for result in ranked
            ],
            "selected_candidate_name": (
                selected.candidate.candidate_name
            ),
        }
    )

    return CanonicalHistoricalBaserunningCalibrationGrid(
        baseline_evaluation_digest=(
            baseline_evaluation_digest
        ),
        policy_version=policy.policy_version,
        results=ranked,
        selected_candidate_name=(
            selected.candidate.candidate_name
        ),
        digest=grid_digest,
    )
