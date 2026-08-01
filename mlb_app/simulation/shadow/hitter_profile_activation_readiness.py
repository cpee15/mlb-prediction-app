"""Synthesize hitter-profile activation readiness."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping


ACTIVATION_ELIGIBLE = "activation_eligible"
PARAMETERIZATION_PENDING = (
    "signal_supported_parameterization_pending"
)
EVIDENCE_BLOCKED = "evidence_blocked"
CONTEXT_ONLY = "evaluation_context_only"

RECOMMENDED_NEXT_SLICE = (
    "select_hitter_profile_candidate_parameterizations"
)


DEFAULT_SIGNAL_EVIDENCE = {
    "hit_skill": {
        "production_input":
            "contact_skill.hit_skill",
        "candidate":
            "actual_batting_average_plus_xba",
        "evidence_source_prs": [
            1300,
            1305,
        ],
        "evidence": {
            "pooled_expected_weight": 0.35,
            "current_expected_weight": 0.50,
            "global_expected_weight_spread":
                0.35,
            "vsR_expected_weight_spread": 0.45,
            "vsL_expected_weight_spread": 0.90,
            "holdout_ab": 84883,
        },
        "state": EVIDENCE_BLOCKED,
        "blockers": [
            "unstable_global_expected_weight",
            "unstable_vsR_expected_weight",
            "unstable_vsL_expected_weight",
            "production_weight_not_selected",
        ],
        "excluded_features": [
            "xwoba_as_direct_hit_probability",
        ],
    },
    "strikeout_skill": {
        "production_input":
            "contact_skill.k_rate",
        "candidate":
            "actual_strikeout_rate_plus_whiff_rate",
        "evidence_source_prs": [
            1307,
        ],
        "evidence": {
            "relative_mse_improvement": 0.0316,
            "bootstrap_probability_of_improvement":
                0.9975,
            "bootstrap_interval_fully_positive":
                True,
        },
        "state": ACTIVATION_ELIGIBLE,
        "blockers": [],
        "selected_parameterization": {
            "schema_version":
                "shadow_hitter_strikeout_skill_parameterization_v1",
            "intercept":
                0.04501320822630234,
            "actual_k_coefficient":
                0.4339959775511906,
            "whiff_coefficient":
                0.34344736989394414,
            "outside_range_policy":
                "fallback_to_actual_strikeout_rate",
            "production_enabled": False,
        },
        "excluded_features": [
            "called_strike_rate",
            "swinging_strike_rate_increment",
            "full_auxiliary_model",
            "contact_rate_redundant_with_whiff_rate",
        ],
    },
    "walk_skill": {
        "production_input":
            "plate_discipline.bb_rate",
        "candidate": "called_ball_rate",
        "evidence_source_prs": [
            1308,
        ],
        "evidence": {
            "relative_mse_improvement_over_actual_bb":
                0.0279,
            "bootstrap_probability_of_improvement":
                0.995,
            "bootstrap_interval_fully_positive":
                True,
            "source_denominator": "pitches",
            "production_denominator":
                "plate_appearances",
        },
        "state": ACTIVATION_ELIGIBLE,
        "blockers": [],
        "selected_parameterization": {
            "schema_version":
                "shadow_hitter_walk_skill_parameterization_v1",
            "intercept":
                -0.12043625608007737,
            "slope":
                0.5605462949774747,
            "minimum_called_ball_rate":
                0.2383177570093458,
            "maximum_called_ball_rate":
                0.5,
            "outside_range_policy":
                "fallback_to_actual_walk_rate",
            "production_enabled": False,
        },
        "fallback": "actual_walk_rate",
        "excluded_features": [
            "actual_walk_rate_blend_increment",
            "take_rate",
            "called_strike_rate",
            "full_auxiliary_model",
            "chase_rate_unavailable",
        ],
    },
    "power_skill": {
        "production_input": "power.iso",
        "candidate": "expected_damage",
        "evidence_source_prs": [
            1306,
        ],
        "evidence": {
            "relative_mse_improvement_over_actual_iso":
                0.0584,
            "bootstrap_probability_of_improvement":
                1.0,
            "bootstrap_interval_fully_positive":
                True,
        },
        "state": ACTIVATION_ELIGIBLE,
        "blockers": [],
        "selected_parameterization": {
            "schema_version":
                "shadow_hitter_power_skill_parameterization_v1",
            "intercept":
                0.08025334564396619,
            "expected_damage_coefficient":
                1.5145365016897803,
            "minimum_expected_damage_per_ab":
                0.005903692307692304,
            "maximum_expected_damage_per_ab":
                0.1880161714254247,
            "outside_range_policy":
                "fallback_to_actual_iso",
            "production_enabled": False,
        },
        "excluded_features": [
            "hard_hit_increment",
            "barrel_proxy_increment",
            "full_auxiliary_model",
        ],
    },
    "hit_type_allocation": {
        "production_input":
            "single_double_triple_hr_allocation",
        "candidate":
            "single_supported_model_only",
        "supported_models": [
            "actual_allocation",
            "expected_damage",
        ],
        "evidence_source_prs": [
            1310,
        ],
        "evidence": {
            "actual_vs_league_bootstrap_probability":
                1.0,
            "expected_vs_league_bootstrap_probability":
                1.0,
            "blend_increment_probability": 0.905,
            "geometry_increment_probability": 0.455,
        },
        "state": ACTIVATION_ELIGIBLE,
        "blockers": [],
        "selected_parameterization": {
            "schema_version":
                "shadow_hitter_hit_type_allocation_parameterization_v1",
            "selected_model":
                "expected_damage",
            "selected_outcomes": [
                "single",
                "double",
                "home_run",
            ],
            "triple_policy":
                "retain_current_conservative_triple_probability",
            "outside_range_policy":
                "fallback_to_actual_allocation",
            "production_enabled": False,
        },
        "excluded_features": [
            "actual_expected_blend_increment",
            "contact_geometry_increment",
        ],
    },
    "speed_and_triples": {
        "production_input": "triple_probability",
        "candidate":
            "prospective_sprint_speed_evidence",
        "evidence_source_prs": [
            1311,
            1312,
            1313,
            1314,
        ],
        "evidence": {
            "source_endpoint_confirmed": True,
            "prospective_collection_allowed": True,
            "historical_as_of_query_supported":
                False,
            "retrospective_evaluation_allowed":
                False,
        },
        "state": EVIDENCE_BLOCKED,
        "blockers": [
            "historical_as_of_query_unsupported",
            "historical_capture_precedes_outcomes_unverified",
            "prospective_outcomes_not_yet_observed",
        ],
        "fallback":
            "retain_conservative_triple_policy",
        "excluded_features": [
            "launch_angle_as_speed_proxy",
            "exit_velocity_as_speed_proxy",
            "stolen_base_rate_as_speed_proxy",
            "triple_rate_as_speed_proxy",
        ],
    },
    "xwoba_context": {
        "production_input": None,
        "candidate": "xwoba",
        "evidence_source_prs": [
            1296,
        ],
        "state": CONTEXT_ONLY,
        "blockers": [
            "direct_pa_outcome_mapping_not_selected",
        ],
        "excluded_features": [],
    },
}


def synthesize_hitter_profile_activation_readiness(
    signal_evidence: Mapping[
        str,
        Mapping[str, Any],
    ]
    | None = None,
) -> dict[str, Any]:
    """Build one deterministic activation decision matrix."""

    signals = deepcopy(
        dict(
            signal_evidence
            or DEFAULT_SIGNAL_EVIDENCE
        )
    )
    allowed_states = {
        ACTIVATION_ELIGIBLE,
        PARAMETERIZATION_PENDING,
        EVIDENCE_BLOCKED,
        CONTEXT_ONLY,
    }
    contract_blockers = []

    for name, signal in sorted(
        signals.items()
    ):
        state = signal.get("state")
        if state not in allowed_states:
            contract_blockers.append(
                f"invalid_signal_state:{name}"
            )

        if not signal.get("candidate"):
            contract_blockers.append(
                f"missing_candidate:{name}"
            )

        if (
            state
            in {
                PARAMETERIZATION_PENDING,
                EVIDENCE_BLOCKED,
            }
            and not signal.get("blockers")
        ):
            contract_blockers.append(
                f"missing_signal_blockers:{name}"
            )

    state_counts = {
        state: sum(
            signal.get("state") == state
            for signal in signals.values()
        )
        for state in sorted(allowed_states)
    }
    activation_eligible_signals = sorted(
        name
        for name, signal in signals.items()
        if signal.get("state")
        == ACTIVATION_ELIGIBLE
    )
    pending_signals = sorted(
        name
        for name, signal in signals.items()
        if signal.get("state")
        == PARAMETERIZATION_PENDING
    )
    blocked_signals = sorted(
        name
        for name, signal in signals.items()
        if signal.get("state")
        == EVIDENCE_BLOCKED
    )
    context_only_signals = sorted(
        name
        for name, signal in signals.items()
        if signal.get("state")
        == CONTEXT_ONLY
    )

    first_activation_ready = (
        not contract_blockers
        and bool(activation_eligible_signals)
        and not pending_signals
    )

    activation_blockers = list(
        contract_blockers
    )
    if pending_signals:
        activation_blockers.append(
            "supported_signals_require_parameter_selection"
        )
    if not activation_eligible_signals:
        activation_blockers.append(
            "no_new_signal_is_activation_eligible"
        )

    activation_blockers = sorted(
        set(activation_blockers)
    )

    return {
        "schema_version":
            "hitter_profile_activation_readiness_v1",
        "status": (
            "ready_for_activation"
            if first_activation_ready
            else "not_ready_for_activation"
        ),
        "first_activation_ready":
            first_activation_ready,
        "signals": {
            name: signals[name]
            for name in sorted(signals)
        },
        "state_counts": state_counts,
        "activation_eligible_signals":
            activation_eligible_signals,
        "parameterization_pending_signals":
            pending_signals,
        "evidence_blocked_signals":
            blocked_signals,
        "context_only_signals":
            context_only_signals,
        "activation_blockers":
            activation_blockers,
        "recommended_next_slice": (
            "run_hitter_profile_shadow_canary"
            if first_activation_ready
            else RECOMMENDED_NEXT_SLICE
        ),
        "first_activation_scope": {
            "include_only_after_selection": [
                "strikeout_skill",
                "walk_skill",
                "power_skill",
                "hit_type_allocation",
            ],
            "retain_current_policy": [
                "hit_skill",
            ],
            "exclude": [
                "speed_and_triples",
                "xwoba_context",
            ],
            "fallback_required": True,
            "feature_flag_required": True,
            "shadow_canary_required": True,
        },
        "walk_policy": {
            "supported_signal":
                "called_ball_rate",
            "actual_walk_rate_role":
                "production_fallback",
            "blend_supported": False,
            "per_pitch_to_per_pa_mapping_required":
                False,
            "parameterization_selected":
                True,
            "activation_eligible":
                True,
            "production_enabled":
                False,
        },
        "parameter_selected":
            first_activation_ready,
        "production_authority_changed": False,
        "shadow_only": True,
    }
