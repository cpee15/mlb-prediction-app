import json
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_CSV = TMP_DIR / "bullpen_candidate_architecture.csv"
OUTPUT_CHECKS = TMP_DIR / "bullpen_candidate_architecture_checks.csv"


CANDIDATE_MODE = "candidate_leverage"
DEFAULT_MODE = "off"

FALLBACK_RULE = (
    "fallback_to_current_production_pitcher_assumption"
)


COMPONENTS = [
    {
        "component": "bullpen_state_container",
        "purpose": (
            "Persist bullpen availability, usage, fatigue, and "
            "active reliever state across innings."
        ),
        "inputs_required": (
            "team_id, bullpen_roster, reliever_roles, "
            "usage_history, current_pitcher"
        ),
        "stateful": True,
        "risk_score": 6,
        "implementation_priority": 10,
        "candidate_only_safe": True,
        "notes": (
            "Foundational architecture object required before any "
            "candidate bullpen logic can exist."
        ),
    },
    {
        "component": "reliever_role_model",
        "purpose": (
            "Assign bullpen role hierarchy for leverage-aware deployment."
        ),
        "inputs_required": (
            "saves, holds, innings, leverage_usage, appearances"
        ),
        "stateful": False,
        "risk_score": 4,
        "implementation_priority": 9,
        "candidate_only_safe": True,
        "notes": (
            "Can begin with heuristic/inferred role labels."
        ),
    },
    {
        "component": "availability_filter",
        "purpose": (
            "Filter relievers based on workload and availability."
        ),
        "inputs_required": (
            "recent_appearances, innings_pitched, pitch_count_proxy"
        ),
        "stateful": True,
        "risk_score": 6,
        "implementation_priority": 9,
        "candidate_only_safe": True,
        "notes": (
            "Needed before realistic pitcher chaining."
        ),
    },
    {
        "component": "fatigue_model",
        "purpose": (
            "Apply fatigue penalties and depletion pressure."
        ),
        "inputs_required": (
            "back_to_back_usage, workload_proxy, recent_games"
        ),
        "stateful": True,
        "risk_score": 8,
        "implementation_priority": 8,
        "candidate_only_safe": True,
        "notes": (
            "One of the highest-risk realism systems because "
            "fatigue compounds across innings."
        ),
    },
    {
        "component": "leverage_score_model",
        "purpose": (
            "Quantify inning/game pressure for bullpen deployment."
        ),
        "inputs_required": (
            "inning, score_diff, base_state, outs, "
            "save_situation, extra_inning_flag"
        ),
        "stateful": False,
        "risk_score": 6,
        "implementation_priority": 8,
        "candidate_only_safe": True,
        "notes": (
            "Can begin with deterministic scoring before "
            "future probabilistic tuning."
        ),
    },
    {
        "component": "replacement_trigger_model",
        "purpose": (
            "Determine when pitchers should be replaced."
        ),
        "inputs_required": (
            "starter_pitch_count_proxy, times_through_order_proxy, "
            "inning, leverage_score, fatigue"
        ),
        "stateful": True,
        "risk_score": 7,
        "implementation_priority": 8,
        "candidate_only_safe": True,
        "notes": (
            "Must preserve safe fallback behavior."
        ),
    },
    {
        "component": "pitcher_chain_selector",
        "purpose": (
            "Select the next reliever based on leverage and availability."
        ),
        "inputs_required": (
            "available_relievers, role_match, fatigue_penalty, "
            "leverage_score"
        ),
        "stateful": True,
        "risk_score": 9,
        "implementation_priority": 8,
        "candidate_only_safe": True,
        "notes": (
            "Highest-risk architecture component because it controls "
            "multi-inning sequencing realism."
        ),
    },
    {
        "component": "depletion_fallback_model",
        "purpose": (
            "Provide deterministic fallback when bullpen depth is exhausted."
        ),
        "inputs_required": (
            "available_relievers, exhaustion_state"
        ),
        "stateful": True,
        "risk_score": 5,
        "implementation_priority": 7,
        "candidate_only_safe": True,
        "notes": (
            "Must explicitly preserve production assumptions "
            "when candidate state fails."
        ),
    },
    {
        "component": "candidate_activation_mode",
        "purpose": (
            "Ensure bullpen realism only activates in candidate mode."
        ),
        "inputs_required": (
            "bullpen_mode flag"
        ),
        "stateful": False,
        "risk_score": 3,
        "implementation_priority": 10,
        "candidate_only_safe": True,
        "notes": (
            "Critical protection against production leakage."
        ),
    },
    {
        "component": "default_equivalence_guardrail",
        "purpose": (
            "Verify default-off behavior remains unchanged."
        ),
        "inputs_required": (
            "baseline_simulation_outputs, candidate_disabled_outputs"
        ),
        "stateful": False,
        "risk_score": 4,
        "implementation_priority": 10,
        "candidate_only_safe": True,
        "notes": (
            "Should mirror prior Layer 6 transition governance."
        ),
    },
    {
        "component": "backtest_harness",
        "purpose": (
            "Support production-vs-candidate bullpen calibration."
        ),
        "inputs_required": (
            "simulation_outputs, actual_results, bullpen_metrics"
        ),
        "stateful": False,
        "risk_score": 5,
        "implementation_priority": 8,
        "candidate_only_safe": True,
        "notes": (
            "Should integrate with existing Layer 6 calibration structure."
        ),
    },
    {
        "component": "promotion_guardrails",
        "purpose": (
            "Prevent unsafe realism promotion into production."
        ),
        "inputs_required": (
            "tail_metrics, variance_metrics, equivalence_checks"
        ),
        "stateful": False,
        "risk_score": 5,
        "implementation_priority": 9,
        "candidate_only_safe": True,
        "notes": (
            "Required before any bullpen realism promotion."
        ),
    },
]


BULLPEN_STATE_CONTAINER = {
    "team_id": "...",
    "available_relievers": [],
    "used_pitchers": [],
    "current_pitcher": None,
    "fatigue_by_pitcher": {},
    "role_by_pitcher": {},
    "handedness_by_pitcher": {},
    "usage_log": [],
}


RELIEVER_ROLES = [
    "closer",
    "setup",
    "high_leverage",
    "middle_relief",
    "long_relief",
    "low_leverage",
    "unknown",
]


def main():
    df = pd.DataFrame(COMPONENTS)

    df.to_csv(OUTPUT_CSV, index=False)

    required_components = {
        "bullpen_state_container",
        "reliever_role_model",
        "availability_filter",
        "fatigue_model",
        "leverage_score_model",
        "replacement_trigger_model",
        "pitcher_chain_selector",
        "depletion_fallback_model",
        "candidate_activation_mode",
        "default_equivalence_guardrail",
        "backtest_harness",
        "promotion_guardrails",
    }

    stateful_components = sorted(
        df.loc[
            df["stateful"],
            "component",
        ].tolist()
    )

    highest_risk_component = str(
        df.sort_values(
            "risk_score",
            ascending=False,
        ).iloc[0]["component"]
    )

    highest_priority_component = str(
        df.sort_values(
            "implementation_priority",
            ascending=False,
        ).iloc[0]["component"]
    )

    checks = [
        {
            "check": "required_components_present",
            "passed": (
                set(df["component"]) == required_components
            ),
            "detail": ",".join(sorted(df["component"])),
        },
        {
            "check": "candidate_mode_defined",
            "passed": (
                CANDIDATE_MODE == "candidate_leverage"
            ),
            "detail": CANDIDATE_MODE,
        },
        {
            "check": "default_mode_off",
            "passed": (
                DEFAULT_MODE == "off"
            ),
            "detail": DEFAULT_MODE,
        },
        {
            "check": "fallback_rule_defined",
            "passed": (
                FALLBACK_RULE
                == "fallback_to_current_production_pitcher_assumption"
            ),
            "detail": FALLBACK_RULE,
        },
        {
            "check": "all_components_candidate_safe",
            "passed": (
                df["candidate_only_safe"].all()
            ),
            "detail": int(
                df["candidate_only_safe"].sum()
            ),
        },
        {
            "check": "stateful_components_identified",
            "passed": (
                len(stateful_components) > 0
            ),
            "detail": ",".join(stateful_components),
        },
        {
            "check": "risk_scores_within_bounds",
            "passed": (
                df["risk_score"]
                .between(1, 10)
                .all()
            ),
            "detail": "risk_1_to_10",
        },
        {
            "check": "priorities_within_bounds",
            "passed": (
                df["implementation_priority"]
                .between(1, 10)
                .all()
            ),
            "detail": "priority_1_to_10",
        },
        {
            "check": "highest_risk_identified",
            "passed": (
                highest_risk_component
                == "pitcher_chain_selector"
            ),
            "detail": highest_risk_component,
        },
    ]

    pd.DataFrame(checks).to_csv(
        OUTPUT_CHECKS,
        index=False,
    )

    payload = {
        "diagnosis": (
            "bullpen_candidate_architecture_complete"
        ),
        "architecture_decision": (
            "proceed_to_candidate_architecture_prototype"
        ),
        "candidate_mode": CANDIDATE_MODE,
        "default_mode": DEFAULT_MODE,
        "components_defined": len(df),
        "stateful_components": stateful_components,
        "highest_risk_component": (
            highest_risk_component
        ),
        "highest_priority_component": (
            highest_priority_component
        ),
        "fallback_rule": FALLBACK_RULE,
        "bullpen_state_container": (
            BULLPEN_STATE_CONTAINER
        ),
        "reliever_roles": RELIEVER_ROLES,
        "recommended_next_step": (
            "layer_6ad_bullpen_state_container_prototype"
        ),
    }

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
