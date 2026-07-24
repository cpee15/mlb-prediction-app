"""Adapt diagnostic baserunning evaluations into typed evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from mlb_app.simulation.events import Base
from mlb_app.simulation.stolen_base_pickoff_evaluator import (
    evaluate_stolen_base_and_pickoff_state,
    validate_stolen_base_and_pickoff_evaluation,
)

from .baserunning_resolver import (
    CanonicalBaserunningEvidence,
    CanonicalBaserunningEvidenceProvider,
    CanonicalBaserunningEvidenceQuery,
)


CANONICAL_BASERUNNING_EVIDENCE_ADAPTER_VERSION = (
    "canonical_baserunning_evidence_adapter_v1"
)

_BASE_NAMES = {
    Base.FIRST: "first",
    Base.SECOND: "second",
    Base.THIRD: "third",
}


CanonicalBaserunningStateProvider = Callable[
    [CanonicalBaserunningEvidenceQuery],
    Optional[Mapping[str, Any]],
]


@dataclass(frozen=True)
class CanonicalBaserunningEvaluatorEvidenceAdapter:
    """Fail-open adapter around the existing deterministic evaluator."""

    state_provider: CanonicalBaserunningStateProvider
    adapter_version: str = (
        CANONICAL_BASERUNNING_EVIDENCE_ADAPTER_VERSION
    )

    def __post_init__(self) -> None:
        if not callable(self.state_provider):
            raise TypeError(
                "state_provider must be callable"
            )
        if self.adapter_version != (
            CANONICAL_BASERUNNING_EVIDENCE_ADAPTER_VERSION
        ):
            raise ValueError(
                "unsupported baserunning evidence adapter version"
            )

    def __call__(
        self,
        query: CanonicalBaserunningEvidenceQuery,
    ) -> Optional[CanonicalBaserunningEvidence]:
        if not isinstance(
            query,
            CanonicalBaserunningEvidenceQuery,
        ):
            return None

        try:
            provided_state = self.state_provider(query)

            if not isinstance(
                provided_state,
                Mapping,
            ):
                return None

            evaluator_state = dict(provided_state)

            if not _state_matches_query(
                evaluator_state,
                query,
            ):
                return None

            evaluation = (
                evaluate_stolen_base_and_pickoff_state(
                    evaluator_state
                )
            )
            validation = (
                validate_stolen_base_and_pickoff_evaluation(
                    evaluation
                )
            )

            if validation.get("valid") is not True:
                return None
            if (
                evaluation.get("state_completeness")
                != "complete"
            ):
                return None
            if evaluation.get("fallback_used") is not False:
                return None
            if evaluation.get("steal_eligible") is not True:
                return None

            pitcher = evaluator_state.get("pitcher")
            if not isinstance(pitcher, Mapping):
                return None

            pitcher_id = pitcher.get("pitcher_id")
            if not pitcher_id:
                return None

            return CanonicalBaserunningEvidence(
                pitcher_id=str(pitcher_id),
                attempt_probability=float(
                    evaluation["attempt_probability"]
                ),
                success_probability=float(
                    evaluation["success_probability"]
                ),
                probability_provenance=(
                    self.adapter_version
                ),
            )
        except Exception:
            return None


def build_canonical_baserunning_evidence_provider(
    *,
    state_provider: CanonicalBaserunningStateProvider,
) -> CanonicalBaserunningEvidenceProvider:
    """Build the explicit fail-open evaluator adapter."""

    return CanonicalBaserunningEvaluatorEvidenceAdapter(
        state_provider=state_provider,
    )


def _state_matches_query(
    evaluator_state: Mapping[str, Any],
    query: CanonicalBaserunningEvidenceQuery,
) -> bool:
    state = query.state

    if evaluator_state.get("inning") != state.inning:
        return False
    if evaluator_state.get("half") != state.half:
        return False
    if evaluator_state.get("outs") != state.outs:
        return False
    if evaluator_state.get("origin_base") != (
        _BASE_NAMES[query.origin_base]
    ):
        return False
    if evaluator_state.get("target_base") != (
        _BASE_NAMES[query.target_base]
    ):
        return False

    runner = evaluator_state.get("runner")
    if not isinstance(runner, Mapping):
        return False
    if runner.get("runner_id") != query.runner_id:
        return False

    base_state = evaluator_state.get("base_state")
    if not isinstance(base_state, Mapping):
        return False

    expected_base_state = {
        "first": state.first is not None,
        "second": state.second is not None,
        "third": state.third is not None,
    }

    return dict(base_state) == expected_base_state
