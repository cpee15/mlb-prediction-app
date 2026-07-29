"""Combine actual and expected hitter evidence for shadow PA comparisons."""

from __future__ import annotations

import copy
from typing import Any, Mapping, Optional

from mlb_app.simulation.shadow.player_profile_blend import (
    build_shadow_candidate_batter_profile,
)


COMBINED_SHADOW_HITTER_PROFILE_VERSION = "combined_shadow_hitter_profile_v1"
COMBINED_HIT_SKILL_POLICY_VERSION = "actual_ba_xba_equal_weight_v1"


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_combined_shadow_hitter_profile(
    *,
    actual_blend: Mapping[str, Any],
    expected_components: Mapping[str, Any],
    expected_weight: float = 0.50,
) -> dict[str, Any]:
    """Build a shadow candidate without changing production authority."""

    blockers = []
    if actual_blend.get("status") != "ready":
        blockers.append("actual_profile_blend_not_ready")
    if expected_components.get("status") != "ready":
        blockers.append("expected_components_not_ready")

    identity_fields = ("player_id", "season", "split")
    identity_mismatches = [
        field
        for field in identity_fields
        if actual_blend.get(field) != expected_components.get(field)
    ]
    if identity_mismatches:
        blockers.append("profile_evidence_identity_mismatch")

    weight = _number(expected_weight)
    if weight is None or not 0.0 <= weight <= 1.0:
        blockers.append("invalid_expected_weight")

    actual = dict(actual_blend.get("blended_actual_metrics") or {})
    expected = dict(
        expected_components.get("blended_expected_metrics") or {}
    )
    batting_avg = _number(actual.get("batting_avg"))
    xba = _number(expected.get("xba"))
    xwoba = _number(expected.get("xwoba"))
    if batting_avg is None:
        blockers.append("missing_blended_batting_average")
    if xba is None:
        blockers.append("missing_blended_xba")

    base_candidate = build_shadow_candidate_batter_profile(actual_blend)
    if base_candidate.get("status") != "ready":
        blockers.append("actual_candidate_profile_not_ready")

    blockers = list(dict.fromkeys(blockers))
    if blockers:
        return {
            "schema_version": COMBINED_SHADOW_HITTER_PROFILE_VERSION,
            "status": "blocked",
            "shadow_only": True,
            "production_authority_changed": False,
            "player_id": actual_blend.get("player_id"),
            "season": actual_blend.get("season"),
            "split": actual_blend.get("split"),
            "blockers": blockers,
            "candidate_profile": None,
        }

    actual_weight = 1.0 - weight
    hit_skill = (batting_avg * actual_weight) + (xba * weight)
    profile = copy.deepcopy(base_candidate["profile"])
    profile.setdefault("contact_skill", {})["contact_rate"] = None
    profile["contact_skill"]["hit_skill"] = round(hit_skill, 6)
    profile.setdefault("metadata", {}).update(
        {
            "source_type": "combined_shadow_hitter_profile_evidence",
            "combined_profile_schema_version": (
                COMBINED_SHADOW_HITTER_PROFILE_VERSION
            ),
            "hit_skill_policy_version": COMBINED_HIT_SKILL_POLICY_VERSION,
            "actual_blend_schema_version": actual_blend.get(
                "schema_version"
            ),
            "expected_components_schema_version": expected_components.get(
                "schema_version"
            ),
            "expected_weight": weight,
            "actual_weight": actual_weight,
            "xwoba_applied": False,
            "shadow_only": True,
            "production_authority_changed": False,
        }
    )

    return {
        "schema_version": COMBINED_SHADOW_HITTER_PROFILE_VERSION,
        "status": "ready",
        "shadow_only": True,
        "production_authority_changed": False,
        "player_id": actual_blend.get("player_id"),
        "season": actual_blend.get("season"),
        "split": actual_blend.get("split"),
        "blockers": [],
        "hit_skill_policy": {
            "version": COMBINED_HIT_SKILL_POLICY_VERSION,
            "actual_metric": "batting_avg",
            "expected_metric": "xba",
            "actual_weight": actual_weight,
            "expected_weight": weight,
            "actual_value": batting_avg,
            "expected_value": xba,
            "combined_value": round(hit_skill, 6),
            "parameter_selected": False,
        },
        "expected_evidence": {
            "xwoba": xwoba,
            "xba": xba,
            "xwoba_applied": False,
            "xwoba_role": "evaluation_context_only",
            "source_latest_date": expected_components.get(
                "source_latest_date"
            ),
            "source_age_days": expected_components.get("source_age_days"),
        },
        "actual_warnings": list(actual_blend.get("warnings") or ()),
        "expected_warnings": list(
            expected_components.get("warnings") or ()
        ),
        "candidate_profile": profile,
    }


def load_combined_shadow_hitter_profile(
    session: Any,
    *,
    player_id: int,
    season: int,
    split: str,
    as_of_date: Any,
    expected_weight: float = 0.50,
    career_start_season: Optional[int] = None,
) -> dict[str, Any]:
    """Load player-ID evidence and build a cutoff-safe combined profile."""

    from mlb_app.simulation.shadow.hitter_expected_components import (
        load_shadow_hitter_expected_components,
    )
    from mlb_app.simulation.shadow.hitter_actual_components import (
        load_shadow_hitter_actual_components,
    )

    actual_blend = load_shadow_hitter_actual_components(
        session,
        player_id=player_id,
        season=season,
        split=split,
        as_of_date=as_of_date,
        career_start_season=career_start_season,
    )
    expected_components = load_shadow_hitter_expected_components(
        session,
        player_id=player_id,
        season=season,
        split=split,
        as_of_date=as_of_date,
        career_start_season=career_start_season,
    )
    result = build_combined_shadow_hitter_profile(
        actual_blend=actual_blend,
        expected_components=expected_components,
        expected_weight=expected_weight,
    )
    result["evidence_status"] = {
        "actual": actual_blend.get("status"),
        "expected": expected_components.get("status"),
    }
    result["evidence_blockers"] = {
        "actual": list(actual_blend.get("blockers") or ()),
        "expected": list(expected_components.get("blockers") or ()),
    }
    result["storage_evidence"] = {
        "actual": dict(actual_blend.get("storage_evidence") or {}),
        "expected": dict(
            expected_components.get("storage_evidence") or {}
        ),
    }
    return result


def compare_combined_shadow_hitter_pa_outcomes(
    *,
    actual_blend: Mapping[str, Any],
    expected_components: Mapping[str, Any],
    production_batter_profile: Optional[Mapping[str, Any]],
    pitcher_profile: Optional[Mapping[str, Any]],
    environment_profile: Optional[Mapping[str, Any]],
    expected_weight: float = 0.50,
) -> dict[str, Any]:
    """Compare combined shadow evidence with unchanged production inputs."""

    from mlb_app.simulation.pa_outcome_model import (
        build_pa_outcome_probabilities,
    )

    combined = build_combined_shadow_hitter_profile(
        actual_blend=actual_blend,
        expected_components=expected_components,
        expected_weight=expected_weight,
    )
    if combined["status"] != "ready":
        return {
            "schema_version": "combined_shadow_hitter_pa_comparison_v1",
            "status": "blocked",
            "shadow_only": True,
            "production_authority_changed": False,
            "blockers": list(combined.get("blockers") or ()),
        }

    production_input = copy.deepcopy(
        dict(production_batter_profile or {})
    )
    production = build_pa_outcome_probabilities(
        production_input,
        dict(pitcher_profile or {}),
        dict(environment_profile or {}),
    )
    shadow = build_pa_outcome_probabilities(
        combined["candidate_profile"],
        dict(pitcher_profile or {}),
        dict(environment_profile or {}),
    )
    production_probabilities = dict(production.get("probabilities") or {})
    shadow_probabilities = dict(shadow.get("probabilities") or {})
    keys = sorted(set(production_probabilities) | set(shadow_probabilities))
    deltas = {
        key: round(
            float(shadow_probabilities.get(key) or 0.0)
            - float(production_probabilities.get(key) or 0.0),
            6,
        )
        for key in keys
    }

    return {
        "schema_version": "combined_shadow_hitter_pa_comparison_v1",
        "status": "ready",
        "shadow_only": True,
        "production_authority_changed": False,
        "production_model_version": production.get("model_version"),
        "shadow_model_version": shadow.get("model_version"),
        "production_probabilities": production_probabilities,
        "shadow_probabilities": shadow_probabilities,
        "probability_deltas": deltas,
        "maximum_absolute_probability_delta": max(
            (abs(value) for value in deltas.values()),
            default=0.0,
        ),
        "combined_profile": combined,
        "production_inputs_used": production.get("inputs_used"),
        "shadow_inputs_used": shadow.get("inputs_used"),
    }
