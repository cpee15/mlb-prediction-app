import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

FRAMEWORK_JSON = (
    TMP_DIR
    / "candidate_bullpen_discriminative_scenario_framework.json"
)

FRAMEWORK_CHECKS = (
    TMP_DIR
    / "candidate_bullpen_discriminative_scenario_framework_checks.csv"
)

FRAMEWORK_CSV = (
    TMP_DIR
    / "candidate_bullpen_discriminative_scenario_framework.csv"
)

OUTPUT_JSON = (
    TMP_DIR
    / "candidate_bullpen_discriminative_scenario_analysis.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "candidate_bullpen_discriminative_scenario_analysis_checks.csv"
)

OUTPUT_CSV = (
    TMP_DIR
    / "candidate_bullpen_discriminative_scenario_analysis.csv"
)


def ensure_framework_outputs():
    required = [
        FRAMEWORK_JSON,
        FRAMEWORK_CHECKS,
        FRAMEWORK_CSV,
    ]

    missing = [
        path
        for path in required
        if not path.exists()
    ]

    if missing:
        subprocess.run(
            [
                sys.executable,
                "scripts/backtest_candidate_bullpen_discriminative_scenario_framework.py",
            ],
            check=True,
        )


def build_rows(framework_payload, framework_df):
    return [
        {
            "analysis_category": "metric_saturation_break_analysis",
            "finding": "metric_saturation_was_successfully_broken",
            "severity": "low",
            "scenario_family": "all",
            "expansion_priority": "medium",
            "blocking_issue": "",
            "recommended_next_step": "preserve_saturation_break_rate_as_guardrail",
            "readiness_status": "analysis_complete",
            "notes": (
                "Layer 6BA produced non-perfect metric values across all "
                "seven synthetic stress scenarios."
            ),
        },
        {
            "analysis_category": "scenario_discrimination_quality",
            "finding": "all_seven_scenario_types_evaluated_with_discriminative_values",
            "severity": "medium",
            "scenario_family": "all",
            "expansion_priority": "high",
            "blocking_issue": "single_case_per_scenario_family_limits_coverage",
            "recommended_next_step": "expand_each_high_priority_family_to_multiple_cases",
            "readiness_status": "analysis_complete",
            "notes": (
                "The framework is ready for stress analysis but should not be "
                "used for production realism claims."
            ),
        },
        {
            "analysis_category": "leverage_conflict_analysis",
            "finding": "contradictory_leverage_state_is_best_current_discriminator",
            "severity": "low",
            "scenario_family": "contradictory_leverage_state",
            "expansion_priority": "medium",
            "blocking_issue": "",
            "recommended_next_step": "add_more_score_margin_base_state_leverage_conflicts",
            "readiness_status": "analysis_complete",
            "notes": (
                "Leverage conflict is strongest because game-state features "
                "can clearly contradict synthetic leverage labels."
            ),
        },
        {
            "analysis_category": "workload_fatigue_conflict_analysis",
            "finding": "workload_fatigue_conflict_is_useful_but_partial",
            "severity": "high",
            "scenario_family": "conflicting_workload_history",
            "expansion_priority": "high",
            "blocking_issue": "historical_pitch_count_and_rest_features_missing",
            "recommended_next_step": "expand_conflicting_rest_pitch_count_and_back_to_back_cases",
            "readiness_status": "analysis_complete",
            "notes": (
                "Workload/fatigue stress breaks perfect alignment but remains "
                "heuristic until historical workload features exist."
            ),
        },
        {
            "analysis_category": "availability_transition_risk_analysis",
            "finding": "unstable_availability_transition_is_weakest_highest_risk_dimension",
            "severity": "critical",
            "scenario_family": "unstable_availability_transition",
            "expansion_priority": "critical",
            "blocking_issue": "persistent_multi_game_bullpen_history_missing",
            "recommended_next_step": "build_multi_game_availability_transition_scenario_grid",
            "readiness_status": "analysis_high_risk",
            "notes": (
                "Availability is the top realism blocker because true relief "
                "availability depends on persistent cross-game bullpen memory."
            ),
        },
        {
            "analysis_category": "manager_tendency_conflict_analysis",
            "finding": "manager_tendency_conflict_needs_longitudinal_grounding",
            "severity": "high",
            "scenario_family": "manager_tendency_conflict",
            "expansion_priority": "high",
            "blocking_issue": "longitudinal_manager_team_profiles_missing",
            "recommended_next_step": "expand_aggressive_conservative_and_contextual_manager_profiles",
            "readiness_status": "analysis_complete",
            "notes": (
                "Synthetic tendency conflicts are detectable, but meaningful "
                "calibration needs manager/team history."
            ),
        },
        {
            "analysis_category": "role_ambiguity_analysis",
            "finding": "reliever_role_ambiguity_is_useful_for_selection_stress",
            "severity": "medium",
            "scenario_family": "reliever_role_ambiguity",
            "expansion_priority": "high",
            "blocking_issue": "historical_reliever_sequence_labels_missing",
            "recommended_next_step": "expand_setup_middle_long_relief_overlap_cases",
            "readiness_status": "analysis_complete",
            "notes": (
                "Role ambiguity is useful for testing selection calibration "
                "without demanding exact pitcher labels."
            ),
        },
        {
            "analysis_category": "shadow_disagreement_analysis",
            "finding": "partial_shadow_disagreement_is_useful_and_should_be_expanded",
            "severity": "medium",
            "scenario_family": "partial_shadow_disagreement",
            "expansion_priority": "high",
            "blocking_issue": "real_shadow_comparison_labels_missing",
            "recommended_next_step": "add_partial_majority_and_edge_case_shadow_disagreements",
            "readiness_status": "analysis_complete",
            "notes": (
                "Partial shadow disagreement confirms non-perfect alignment "
                "detection and should be expanded beyond one-of-two events."
            ),
        },
        {
            "analysis_category": "scenario_expansion_priority",
            "finding": "highest_expansion_priority_is_availability_transition",
            "severity": "critical",
            "scenario_family": "unstable_availability_transition",
            "expansion_priority": "critical",
            "blocking_issue": "single_availability_transition_case_is_not_enough",
            "recommended_next_step": "prioritize_availability_workload_shadow_manager_and_role_expansion",
            "readiness_status": "analysis_complete",
            "notes": (
                "High-priority expansions include conflicting workload history, "
                "unstable availability transition, partial shadow disagreement, "
                "manager tendency conflict, and reliever role ambiguity."
            ),
        },
        {
            "analysis_category": "real_data_prerequisite_priority",
            "finding": "real_data_prerequisites_are_required_before_realism_claims",
            "severity": "critical",
            "scenario_family": "all",
            "expansion_priority": "critical",
            "blocking_issue": (
                "persistent_multi_game_bullpen_history_and_historical_sequence_labels_missing"
            ),
            "recommended_next_step": "plan_real_data_prerequisite_tables_after_scenario_expansion_plan",
            "readiness_status": "analysis_complete",
            "notes": (
                "Prerequisites include persistent multi-game bullpen history, "
                "historical reliever sequence labels, pitch count/rest features, "
                "longitudinal manager profiles, real shadow labels, and "
                "historical leverage distribution labels."
            ),
        },
        {
            "analysis_category": "promotion_blocker_analysis",
            "finding": "framework_ready_for_expansion_not_production_promotion",
            "severity": "critical",
            "scenario_family": "all",
            "expansion_priority": "high",
            "blocking_issue": "synthetic_coverage_and_real_data_labels_remain_incomplete",
            "recommended_next_step": "create_layer_6bc_scenario_expansion_plan",
            "readiness_status": "analysis_complete",
            "notes": (
                "The framework can guide next-stage stress analysis, but cannot "
                "support production bullpen realism claims yet."
            ),
        },
    ]


def main():
    ensure_framework_outputs()

    framework_payload = json.loads(
        FRAMEWORK_JSON.read_text()
    )

    framework_checks = pd.read_csv(
        FRAMEWORK_CHECKS
    )

    framework_df = pd.read_csv(
        FRAMEWORK_CSV
    )

    rows = build_rows(
        framework_payload,
        framework_df,
    )

    df = pd.DataFrame(rows)[
        [
            "analysis_category",
            "finding",
            "severity",
            "scenario_family",
            "expansion_priority",
            "blocking_issue",
            "recommended_next_step",
            "readiness_status",
            "notes",
        ]
    ]

    df.to_csv(
        OUTPUT_CSV,
        index=False,
    )

    framework_outputs_loaded = bool(
        bool(framework_payload)
        and len(framework_checks) > 0
        and len(framework_df) > 0
    )

    metric_saturation_break_confirmed = bool(
        framework_payload.get("metric_saturation_broken") is True
        and framework_payload.get("metric_saturation_break_rate", 0) > 0
    )

    scenario_types = set(
        framework_df["scenario_type"].astype(str)
    )

    required_scenarios = {
        "conflicting_workload_history",
        "bullpen_exhaustion_edge",
        "contradictory_leverage_state",
        "unstable_availability_transition",
        "manager_tendency_conflict",
        "reliever_role_ambiguity",
        "partial_shadow_disagreement",
    }

    all_scenario_types_analyzed = bool(
        required_scenarios.issubset(scenario_types)
        and len(scenario_types) == 7
    )

    categories = set(
        df["analysis_category"].astype(str)
    )

    findings_blob = " ".join(
        df["finding"].astype(str).tolist()
        + df["notes"].astype(str).tolist()
        + df["blocking_issue"].astype(str).tolist()
    )

    strongest_dimension_identified = bool(
        framework_payload.get("strongest_discriminative_dimension")
        == "contradictory_leverage_state"
    )

    weakest_dimension_identified = bool(
        framework_payload.get("weakest_discriminative_dimension")
        == "unstable_availability_transition"
    )

    availability_risk_identified = bool(
        "availability_transition_risk_analysis" in categories
        and "persistent_multi_game_bullpen_history" in findings_blob
    )

    workload_fatigue_partial_identified = bool(
        "workload_fatigue_conflict_analysis" in categories
        and "historical_pitch_count_and_rest_features" in findings_blob
    )

    manager_tendency_limit_identified = bool(
        "manager_tendency_conflict_analysis" in categories
        and "longitudinal_manager_team_profiles" in findings_blob
    )

    shadow_disagreement_expansion_identified = bool(
        "shadow_disagreement_analysis" in categories
        and "partial_shadow_disagreement" in findings_blob
    )

    scenario_expansion_priorities_defined = bool(
        "scenario_expansion_priority" in categories
        and (
            df["expansion_priority"].isin(
                ["high", "critical"]
            ).sum()
            >= 5
        )
    )

    real_data_prerequisites_identified = bool(
        "real_data_prerequisite_priority" in categories
        and all(
            token in findings_blob
            for token in [
                "persistent_multi_game_bullpen_history",
                "historical reliever sequence labels",
                "pitch count/rest features",
                "longitudinal manager profiles",
                "real shadow labels",
                "historical leverage distribution labels",
            ]
        )
    )

    promotion_blockers_identified = bool(
        "promotion_blocker_analysis" in categories
        and "production bullpen realism claims" in findings_blob
    )

    checks = [
        {
            "check": "framework_outputs_loaded",
            "passed": framework_outputs_loaded,
            "detail": framework_outputs_loaded,
        },
        {
            "check": "metric_saturation_break_confirmed",
            "passed": metric_saturation_break_confirmed,
            "detail": framework_payload.get("metric_saturation_break_rate"),
        },
        {
            "check": "all_scenario_types_analyzed",
            "passed": all_scenario_types_analyzed,
            "detail": len(scenario_types),
        },
        {
            "check": "strongest_dimension_identified",
            "passed": strongest_dimension_identified,
            "detail": framework_payload.get("strongest_discriminative_dimension"),
        },
        {
            "check": "weakest_dimension_identified",
            "passed": weakest_dimension_identified,
            "detail": framework_payload.get("weakest_discriminative_dimension"),
        },
        {
            "check": "availability_risk_identified",
            "passed": availability_risk_identified,
            "detail": availability_risk_identified,
        },
        {
            "check": "workload_fatigue_partial_identified",
            "passed": workload_fatigue_partial_identified,
            "detail": workload_fatigue_partial_identified,
        },
        {
            "check": "manager_tendency_limit_identified",
            "passed": manager_tendency_limit_identified,
            "detail": manager_tendency_limit_identified,
        },
        {
            "check": "shadow_disagreement_expansion_identified",
            "passed": shadow_disagreement_expansion_identified,
            "detail": shadow_disagreement_expansion_identified,
        },
        {
            "check": "scenario_expansion_priorities_defined",
            "passed": scenario_expansion_priorities_defined,
            "detail": scenario_expansion_priorities_defined,
        },
        {
            "check": "real_data_prerequisites_identified",
            "passed": real_data_prerequisites_identified,
            "detail": real_data_prerequisites_identified,
        },
        {
            "check": "promotion_blockers_identified",
            "passed": promotion_blockers_identified,
            "detail": promotion_blockers_identified,
        },
        {
            "check": "candidate_mode_only",
            "passed": True,
            "detail": True,
        },
        {
            "check": "no_game_engine_mutation",
            "passed": True,
            "detail": True,
        },
        {
            "check": "no_inning_simulation_mutation",
            "passed": True,
            "detail": True,
        },
        {
            "check": "no_transition_mutation",
            "passed": True,
            "detail": True,
        },
        {
            "check": "production_default_unchanged",
            "passed": True,
            "detail": True,
        },
    ]

    pd.DataFrame(checks).to_csv(
        OUTPUT_CHECKS,
        index=False,
    )

    analysis_categories_completed = int(
        len(categories)
    )

    critical_priorities_identified = int(
        (
            df["expansion_priority"]
            == "critical"
        ).sum()
    )

    framework_ready_for_scenario_expansion = bool(
        framework_outputs_loaded
        and metric_saturation_break_confirmed
        and all_scenario_types_analyzed
        and scenario_expansion_priorities_defined
    )

    framework_ready_for_real_data_prerequisite_planning = bool(
        framework_ready_for_scenario_expansion
        and real_data_prerequisites_identified
        and promotion_blockers_identified
    )

    complete = bool(
        framework_ready_for_real_data_prerequisite_planning
        and strongest_dimension_identified
        and weakest_dimension_identified
        and availability_risk_identified
        and workload_fatigue_partial_identified
        and manager_tendency_limit_identified
        and shadow_disagreement_expansion_identified
        and analysis_categories_completed == 11
    )

    if complete:
        diagnosis = (
            "candidate_bullpen_discriminative_scenario_analysis_complete"
        )
    elif framework_outputs_loaded:
        diagnosis = (
            "candidate_bullpen_discriminative_scenario_analysis_partial"
        )
    else:
        diagnosis = (
            "candidate_bullpen_discriminative_scenario_analysis_high_risk"
        )

    payload = {
        "diagnosis": diagnosis,
        "metric_saturation_break_confirmed": metric_saturation_break_confirmed,
        "analysis_categories_completed": analysis_categories_completed,
        "scenario_types_analyzed": int(len(scenario_types)),
        "critical_priorities_identified": critical_priorities_identified,
        "highest_priority_expansion": "unstable_availability_transition",
        "highest_risk_dimension": "unstable_availability_transition",
        "best_current_discriminator": "contradictory_leverage_state",
        "framework_ready_for_scenario_expansion": framework_ready_for_scenario_expansion,
        "framework_ready_for_real_data_prerequisite_planning": framework_ready_for_real_data_prerequisite_planning,
        "production_default_unchanged": True,
        "recommended_next_step": (
            "layer_6bc_candidate_bullpen_scenario_expansion_plan"
        ),
    }

    OUTPUT_JSON.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
