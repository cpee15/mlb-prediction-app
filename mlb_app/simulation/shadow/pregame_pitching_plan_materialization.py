"""Materialize canonical pregame pitching plans safely."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Tuple

from mlb_app.simulation.game.matchup_input import (
    CanonicalPitchingPlan,
)


SUPPORTED_PLAN_TYPES = frozenset({
    "traditional_starter",
    "opener_bulk",
    "tandem",
    "bullpen_game",
    "workload_capped_starter",
    "unknown_fallback",
})

FOLLOWER_PLAN_TYPES = frozenset({
    "opener_bulk",
    "tandem",
    "bullpen_game",
})


@dataclass(frozen=True)
class CanonicalPregamePitchingPlanMaterialization:
    """Read-only result of one team's plan materialization."""

    status: str
    pitching_plan: CanonicalPitchingPlan
    requested_plan_type: str
    fallback_used: bool
    blockers: Tuple[str, ...] = ()
    diagnostics: Mapping[str, Any] = field(
        default_factory=dict
    )
    database_writes_performed: bool = False
    production_authority_changed: bool = False

    @property
    def ready(self) -> bool:
        return self.status == "ready"


def _identifier(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip()


def _pitcher_ids(values: Any) -> Tuple[str, ...]:
    if not isinstance(values, (list, tuple)):
        return ()

    result = []
    seen = set()

    for value in values:
        pitcher_id = _identifier(value)

        if not pitcher_id or pitcher_id in seen:
            continue

        seen.add(pitcher_id)
        result.append(pitcher_id)

    return tuple(result)


def _requested_plan_type(
    classification: Mapping[str, Any] | None,
) -> str:
    if not isinstance(classification, Mapping):
        return "traditional_starter"

    return (
        _identifier(
            classification.get("plan_type")
        )
        or "traditional_starter"
    )


def _preferred_replacements(
    *,
    classification: Mapping[str, Any],
    starter_id: str,
    bullpen_pitcher_ids: Tuple[str, ...],
) -> tuple[Tuple[str, ...], int, int]:
    planned_sequence = classification.get(
        "planned_sequence"
    )

    if not isinstance(planned_sequence, (list, tuple)):
        return (), 0, 0

    bullpen_ids = set(bullpen_pitcher_ids)
    preferred = []
    seen = set()
    invalid_record_count = 0
    unavailable_pitcher_count = 0

    for row in planned_sequence:
        if not isinstance(row, Mapping):
            invalid_record_count += 1
            continue

        pitcher_id = _identifier(
            row.get("pitcher_id")
        )

        if not pitcher_id:
            invalid_record_count += 1
            continue

        if (
            pitcher_id == starter_id
            or pitcher_id in seen
        ):
            continue

        seen.add(pitcher_id)

        if pitcher_id not in bullpen_ids:
            unavailable_pitcher_count += 1
            continue

        preferred.append(pitcher_id)

    return (
        tuple(preferred),
        invalid_record_count,
        unavailable_pitcher_count,
    )


def materialize_canonical_pregame_pitching_plan(
    *,
    team_side: str,
    starter_id: Any,
    bullpen_pitcher_ids: Any,
    classification: Mapping[str, Any] | None = None,
) -> CanonicalPregamePitchingPlanMaterialization:
    """
    Convert classification evidence into one canonical plan.

    Invalid, unsupported, or explicitly fallback classification
    evidence safely produces a traditional-starter plan. Preferred
    followers are retained only when they belong to the eligible
    bullpen supplied to this boundary.
    """

    normalized_starter_id = _identifier(starter_id)

    if not normalized_starter_id:
        raise ValueError("starter_id is required")

    normalized_bullpen_ids = _pitcher_ids(
        bullpen_pitcher_ids
    )
    requested_plan_type = _requested_plan_type(
        classification
    )

    blockers = []
    fallback_used = False
    fallback_reason = None
    materialized_plan_type = requested_plan_type
    preferred_replacements: Tuple[str, ...] = ()
    invalid_sequence_record_count = 0
    unavailable_planned_pitcher_count = 0

    if not isinstance(classification, Mapping):
        fallback_used = True
        fallback_reason = (
            "classification_unavailable"
        )
        blockers.append(fallback_reason)
        materialized_plan_type = (
            "traditional_starter"
        )
    elif requested_plan_type not in (
        SUPPORTED_PLAN_TYPES
    ):
        fallback_used = True
        fallback_reason = (
            "unsupported_plan_type"
        )
        blockers.append(fallback_reason)
        materialized_plan_type = (
            "traditional_starter"
        )
    elif (
        classification.get("fallback_used")
        is True
        or requested_plan_type
        == "unknown_fallback"
    ):
        fallback_used = True
        fallback_reason = (
            "classification_fallback_used"
        )
        blockers.append(fallback_reason)
        materialized_plan_type = (
            "traditional_starter"
        )
    elif requested_plan_type in FOLLOWER_PLAN_TYPES:
        (
            preferred_replacements,
            invalid_sequence_record_count,
            unavailable_planned_pitcher_count,
        ) = _preferred_replacements(
            classification=classification,
            starter_id=normalized_starter_id,
            bullpen_pitcher_ids=(
                normalized_bullpen_ids
            ),
        )

        if invalid_sequence_record_count:
            blockers.append(
                "invalid_planned_sequence_record"
            )

        if unavailable_planned_pitcher_count:
            blockers.append(
                "planned_pitcher_not_in_bullpen"
            )

    pitching_plan = CanonicalPitchingPlan(
        team_side=team_side,
        starter_id=normalized_starter_id,
        bullpen_pitcher_ids=(
            normalized_bullpen_ids
        ),
        plan_type=materialized_plan_type,
        preferred_replacement_pitcher_ids=(
            preferred_replacements
        ),
    )

    diagnostics = {
        "schema_version": (
            "canonical_pregame_pitching_plan_"
            "materialization_v1"
        ),
        "status": "ready",
        "requested_plan_type": (
            requested_plan_type
        ),
        "materialized_plan_type": (
            materialized_plan_type
        ),
        "fallback_used": fallback_used,
        "fallback_reason": fallback_reason,
        "bullpen_pitcher_count": len(
            normalized_bullpen_ids
        ),
        "preferred_replacement_count": len(
            preferred_replacements
        ),
        "invalid_sequence_record_count": (
            invalid_sequence_record_count
        ),
        "unavailable_planned_pitcher_count": (
            unavailable_planned_pitcher_count
        ),
        "classification_available": isinstance(
            classification,
            Mapping,
        ),
        "database_writes_performed": False,
        "production_authority_changed": False,
    }

    return (
        CanonicalPregamePitchingPlanMaterialization(
            status="ready",
            pitching_plan=pitching_plan,
            requested_plan_type=(
                requested_plan_type
            ),
            fallback_used=fallback_used,
            blockers=tuple(
                sorted(set(blockers))
            ),
            diagnostics=diagnostics,
        )
    )
