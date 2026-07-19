"""Versioned baseline probabilities for runner advancement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping

from .advancement_version import (
    BASELINE_RUNNER_ADVANCEMENT_MODEL_VERSION,
)
from .batted_ball import (
    BattedBallContext,
    BattedBallDepth,
    BattedBallType,
    ContactQuality,
)


@dataclass(frozen=True)
class AdvancementProbability:
    """One named probability used during transition resolution."""

    name: str
    probability: float

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("probability name is required")
        if not 0.0 <= self.probability <= 1.0:
            raise ValueError(
                "probability must be between 0.0 and 1.0"
            )


BASELINE_ADVANCEMENT_RATES: Mapping[str, float] = {
    "single_runner_second_scores": 0.62,
    "single_runner_first_to_third": 0.28,
    "double_runner_first_scores": 0.42,
    "groundout_runner_third_scores": 0.25,
    "groundout_runner_second_to_third": 0.35,
    "flyout_runner_third_scores_shallow": 0.08,
    "flyout_runner_third_scores_medium": 0.38,
    "flyout_runner_third_scores_deep": 0.65,
    "flyout_runner_second_to_third_shallow": 0.05,
    "flyout_runner_second_to_third_medium": 0.22,
    "flyout_runner_second_to_third_deep": 0.35,
    "line_drive_runner_third_scores": 0.08,
    "line_drive_runner_second_to_third": 0.08,
    "popup_runner_third_scores": 0.01,
    "popup_runner_second_to_third": 0.01,
}


def validate_baseline_advancement_rates(
    rates: Mapping[str, float] = BASELINE_ADVANCEMENT_RATES,
) -> None:
    if not rates:
        raise ValueError("advancement rates cannot be empty")

    for name, value in rates.items():
        if not name:
            raise ValueError("advancement rate name is required")
        if not isinstance(value, (int, float)):
            raise TypeError(
                f"advancement rate must be numeric: {name}"
            )
        if not 0.0 <= float(value) <= 1.0:
            raise ValueError(
                f"advancement rate out of bounds: {name}"
            )


class BaselineRunnerAdvancementModel:
    """Resolve context-aware baseline advancement probabilities.

    These values are explicit initial assumptions. They are not
    represented as calibrated historical MLB rates.
    """

    model_version = BASELINE_RUNNER_ADVANCEMENT_MODEL_VERSION

    def __init__(
        self,
        *,
        rates: Mapping[str, float] | None = None,
    ) -> None:
        resolved = dict(BASELINE_ADVANCEMENT_RATES)

        if rates is not None:
            unknown = set(rates) - set(resolved)
            if unknown:
                raise ValueError(
                    "unknown advancement rates: "
                    + ", ".join(sorted(unknown))
                )
            resolved.update(rates)

        validate_baseline_advancement_rates(resolved)
        self._rates: Dict[str, float] = resolved

    def probability(
        self,
        name: str,
        *,
        context: BattedBallContext,
    ) -> AdvancementProbability:
        resolved_name = self._resolve_context_key(
            name=name,
            context=context,
        )

        if resolved_name not in self._rates:
            raise ValueError(
                f"unsupported advancement probability: {name}"
            )

        probability = self._rates[resolved_name]
        probability = self._apply_contact_quality_modifier(
            probability=probability,
            context=context,
        )

        return AdvancementProbability(
            name=resolved_name,
            probability=max(0.0, min(1.0, probability)),
        )

    def _resolve_context_key(
        self,
        *,
        name: str,
        context: BattedBallContext,
    ) -> str:
        if name == "caught_ball_runner_third_scores":
            if context.batted_ball_type is BattedBallType.FLY_BALL:
                return (
                    "flyout_runner_third_scores_"
                    f"{context.depth.value}"
                )
            if context.batted_ball_type is BattedBallType.LINE_DRIVE:
                return "line_drive_runner_third_scores"
            if context.batted_ball_type is BattedBallType.POPUP:
                return "popup_runner_third_scores"

        if name == "caught_ball_runner_second_to_third":
            if context.batted_ball_type is BattedBallType.FLY_BALL:
                return (
                    "flyout_runner_second_to_third_"
                    f"{context.depth.value}"
                )
            if context.batted_ball_type is BattedBallType.LINE_DRIVE:
                return "line_drive_runner_second_to_third"
            if context.batted_ball_type is BattedBallType.POPUP:
                return "popup_runner_second_to_third"

        return name

    @staticmethod
    def _apply_contact_quality_modifier(
        *,
        probability: float,
        context: BattedBallContext,
    ) -> float:
        if context.contact_quality is ContactQuality.HARD:
            return probability + 0.05
        if context.contact_quality is ContactQuality.SOFT:
            return probability - 0.05
        return probability


validate_baseline_advancement_rates()
