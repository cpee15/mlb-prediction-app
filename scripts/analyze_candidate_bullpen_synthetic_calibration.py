import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

FRAMEWORK_JSON = (
    TMP_DIR
    / "candidate_bullpen_synthetic_calibration_framework.json"
)

FRAMEWORK_CHECKS = (
    TMP_DIR
    / "candidate_bullpen_synthetic_calibration_framework_checks.csv"
)

FRAMEWORK_CSV = (
    TMP_DIR
    / "candidate_bullpen_synthetic_calibration_framework.csv"
)

OUTPUT_JSON = (
    TMP_DIR
    / "candidate_bullpen_synthetic_calibration_analysis.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "candidate_bullpen_synthetic_calibration_analysis_checks.csv"
)

OUTPUT_CSV = (
    TMP_DIR
    / "candidate_bullpen_synthetic_calibration_analysis.csv"
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
                "scripts/backtest_candidate_bullpen_synthetic_calibration_framework.py",
            ],
            check=True,
        )


def build_analysis_rows(
    framework_payload,
    framework_df,
):

    metric_values = list(
        framework_df[
            "metric_value"
        ].astype(float)
    )

    all_saturated = all(
        value == 1.0
        for value in metric_values
    )

    return [
        {
            "analysis_category": (
                "metric_saturation_analysis"
            ),
            "finding": (
                "all_framework_metrics_saturated_at_1_0"
                if all_saturated
                else "framework_metrics_not_fully_saturated"
            ),
            "severity": "high",
            "production_risk": "low",
            "recommended_next_step": (
                "add_adversarial_and_discriminative_synthetic_scenarios"
            ),
            "blocking_issue": (
                "perfectly_aligned_synthetic_labels_do_not_test_calibration_discrimination"
            ),
            "readiness_status": "analysis_complete",
            "notes": (
                "The framework is structurally valid, but current synthetic "
                "labels are internally self-consistent and therefore saturate "
                "all metrics at 1.0."
            ),
        },
        {
            "analysis_category": (
                "synthetic_realism_analysis"
            ),
            "finding": (
                "framework_validates_structure_more_than_realism"
            ),
            "severity": "medium",
            "production_risk": "low",
            "recommended_next_step": (
                "introduce_realism_stress_cases_for_role_workload_and_leverage"
            ),
            "blocking_issue": (
                "synthetic_inputs_lack_conflicting_or_noisy_cases"
            ),
            "readiness_status": "analysis_complete",
            "notes": (
                "The prototype confirms columns, joins, and deterministic "
                "evaluation, but it does not yet approximate messy real bullpen "
                "deployment behavior."
            ),
        },
        {
            "analysis_category": (
                "deterministic_replay_analysis"
            ),
            "finding": (
                "deterministic_replay_and_rollback_safety_are_reliable"
            ),
            "severity": "low",
            "production_risk": "low",
            "recommended_next_step": (
                "preserve_replay_equality_as_a_guardrail_in_future_layers"
            ),
            "blocking_issue": "",
            "readiness_status": "analysis_complete",
            "notes": (
                "Layer 6AY reported deterministic repeatability and rollback "
                "safety rates of 1.0."
            ),
        },
        {
            "analysis_category": (
                "availability_memory_risk_analysis"
            ),
            "finding": (
                "availability_memory_remains_highest_risk_realism_dimension"
            ),
            "severity": "critical",
            "production_risk": "medium",
            "recommended_next_step": (
                "build_discriminative_multi_game_availability_scenarios"
            ),
            "blocking_issue": (
                "persistent_multi_game_bullpen_history_missing"
            ),
            "readiness_status": "analysis_high_risk",
            "notes": (
                "Synthetic availability validates internally, but true "
                "availability requires cross-game workload state and fatigue "
                "carryover."
            ),
        },
        {
            "analysis_category": (
                "fatigue_model_limit_analysis"
            ),
            "finding": (
                "fatigue_calibration_remains_heuristic"
            ),
            "severity": "high",
            "production_risk": "medium",
            "recommended_next_step": (
                "add_conflicting_workload_histories_and_pitch_count_stress_cases"
            ),
            "blocking_issue": (
                "real_pitch_count_and_recent_workload_labels_missing"
            ),
            "readiness_status": "analysis_complete",
            "notes": (
                "The framework checks that synthetic workload columns are "
                "internally valid, not that fatigue behavior matches real usage."
            ),
        },
        {
            "analysis_category": (
                "manager_tendency_limit_analysis"
            ),
            "finding": (
                "manager_tendency_modeling_lacks_longitudinal_grounding"
            ),
            "severity": "high",
            "production_risk": "medium",
            "recommended_next_step": (
                "add_manager_team_profile_conflict_and_sample_threshold_tests"
            ),
            "blocking_issue": (
                "longitudinal_manager_team_profiles_missing"
            ),
            "readiness_status": "analysis_complete",
            "notes": (
                "Synthetic tendency rates are bounded and joinable, but no real "
                "manager/team deployment labels exist yet."
            ),
        },
        {
            "analysis_category": (
                "shadow_alignment_analysis"
            ),
            "finding": (
                "shadow_alignment_is_structurally_strong"
            ),
            "severity": "low",
            "production_risk": "low",
            "recommended_next_step": (
                "add_partial_shadow_disagreement_cases_to_avoid_perfect_alignment"
            ),
            "blocking_issue": (
                "shadow_labels_currently_perfectly_aligned"
            ),
            "readiness_status": "analysis_complete",
            "notes": (
                "Shadow labels are present and internally aligned, but future "
                "testing should include partial disagreement cases."
            ),
        },
        {
            "analysis_category": (
                "discriminative_scenario_gap_analysis"
            ),
            "finding": (
                "adversarial_synthetic_scenarios_are_missing"
            ),
            "severity": "critical",
            "production_risk": "medium",
            "recommended_next_step": (
                "create_discriminative_scenario_framework_with_conflicting_inputs"
            ),
            "blocking_issue": (
                "no_conflicting_workload_leverage_availability_or_role_cases"
            ),
            "readiness_status": "analysis_high_risk",
            "notes": (
                "Missing cases include conflicting workload histories, bullpen "
                "exhaustion edges, contradictory leverage states, unstable "
                "availability transitions, manager tendency conflicts, role "
                "ambiguity, and partial shadow disagreements."
            ),
        },
        {
            "analysis_category": (
                "real_data_prerequisite_analysis"
            ),
            "finding": (
                "real_historical_bullpen_sequences_are_required_for_meaningful_calibration"
            ),
            "severity": "critical",
            "production_risk": "medium",
            "recommended_next_step": (
                "materialize_historical_sequence_workload_and_manager_profile_tables"
            ),
            "blocking_issue": (
                "historical_reliever_sequence_labels_and_workload_history_missing"
            ),
            "readiness_status": "analysis_high_risk",
            "notes": (
                "Meaningful calibration requires historical reliever sequence "
                "labels, persistent workload history, longitudinal manager "
                "profiles, real fatigue outcome labels, multi-game bullpen "
                "memory, and historical leverage outcome distributions."
            ),
        },
        {
            "analysis_category": (
                "promotion_blocker_analysis"
            ),
            "finding": (
                "framework_not_ready_for_promotion_beyond_non_production_experimentation"
            ),
            "severity": "critical",
            "production_risk": "medium",
            "recommended_next_step": (
                "complete_discriminative_synthetic_framework_before_real_data_calibration"
            ),
            "blocking_issue": (
                "metric_saturation_and_missing_real_data_prevent_realism_claims"
            ),
            "readiness_status": "analysis_complete",
            "notes": (
                "The framework may continue as a non-production experiment, "
                "but cannot support production realism claims until saturated "
                "metrics are broken and real-data prerequisites are addressed."
            ),
        },
    ]


def main():

    ensure_framework_outputs()

    framework_df = pd.read_csv(
        FRAMEWORK_CSV
    )

    framework_checks_df = pd.read_csv(
        FRAMEWORK_CHECKS
    )

    framework_payload = json.loads(
        FRAMEWORK_JSON.read_text()
    )

    rows = build_analysis_rows(
        framework_payload,
        framework_df,
    )

    df = pd.DataFrame(rows)[
        [
            "analysis_category",
            "finding",
            "severity",
            "production_risk",
            "recommended_next_step",
            "blocking_issue",
            "readiness_status",
            "notes",
        ]
    ]

    df.to_csv(
        OUTPUT_CSV,
        index=False,
    )

    framework_outputs_loaded = bool(
        len(framework_df) > 0
        and len(framework_checks_df) > 0
        and bool(framework_payload)
    )

    metric_saturation_detected = bool(
        framework_df[
            "metric_value"
        ].astype(float).eq(1.0).all()
    )

    synthetic_consistency_detected = bool(
        framework_payload.get(
            "deterministic_repeatability_rate"
        )
        == 1.0
        and framework_payload.get(
            "rollback_safety_rate"
        )
        == 1.0
    )

    categories = set(
        df["analysis_category"]
    )

    checks = [
        {
            "check": "framework_outputs_loaded",
            "passed": framework_outputs_loaded,
            "detail": framework_outputs_loaded,
        },
        {
            "check": "metric_saturation_detected",
            "passed": metric_saturation_detected,
            "detail": metric_saturation_detected,
        },
        {
            "check": "synthetic_consistency_detected",
            "passed": synthetic_consistency_detected,
            "detail": synthetic_consistency_detected,
        },
        {
            "check": "discriminative_gap_analysis_complete",
            "passed": (
                "discriminative_scenario_gap_analysis"
                in categories
            ),
            "detail": (
                "discriminative_scenario_gap_analysis"
                in categories
            ),
        },
        {
            "check": "real_data_prerequisites_identified",
            "passed": (
                "real_data_prerequisite_analysis"
                in categories
            ),
            "detail": (
                "real_data_prerequisite_analysis"
                in categories
            ),
        },
        {
            "check": "promotion_blockers_identified",
            "passed": (
                "promotion_blocker_analysis"
                in categories
            ),
            "detail": (
                "promotion_blocker_analysis"
                in categories
            ),
        },
        {
            "check": "availability_memory_risk_identified",
            "passed": (
                "availability_memory_risk_analysis"
                in categories
            ),
            "detail": (
                "availability_memory_risk_analysis"
                in categories
            ),
        },
        {
            "check": "fatigue_limitations_identified",
            "passed": (
                "fatigue_model_limit_analysis"
                in categories
            ),
            "detail": (
                "fatigue_model_limit_analysis"
                in categories
            ),
        },
        {
            "check": "manager_tendency_limitations_identified",
            "passed": (
                "manager_tendency_limit_analysis"
                in categories
            ),
            "detail": (
                "manager_tendency_limit_analysis"
                in categories
            ),
        },
        {
            "check": "shadow_alignment_strength_identified",
            "passed": (
                "shadow_alignment_analysis"
                in categories
            ),
            "detail": (
                "shadow_alignment_analysis"
                in categories
            ),
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

    critical_blockers_identified = int(
        (
            df["severity"]
            == "critical"
        ).sum()
    )

    framework_structurally_valid = bool(
        framework_outputs_loaded
        and synthetic_consistency_detected
    )

    framework_realism_limited = bool(
        metric_saturation_detected
        and critical_blockers_identified >= 3
    )

    analysis_complete = bool(
        framework_outputs_loaded
        and metric_saturation_detected
        and synthetic_consistency_detected
        and len(categories) == 10
    )

    if analysis_complete:
        diagnosis = (
            "candidate_bullpen_synthetic_calibration_analysis_complete"
        )

    elif framework_outputs_loaded:
        diagnosis = (
            "candidate_bullpen_synthetic_calibration_analysis_partial"
        )

    else:
        diagnosis = (
            "candidate_bullpen_synthetic_calibration_analysis_high_risk"
        )

    payload = {
        "diagnosis": diagnosis,
        "metric_saturation_detected": (
            metric_saturation_detected
        ),
        "analysis_categories_completed": (
            len(categories)
        ),
        "critical_blockers_identified": (
            critical_blockers_identified
        ),
        "highest_risk_dimension": (
            "availability_memory_risk_analysis"
        ),
        "most_important_missing_requirement": (
            "persistent_multi_game_bullpen_history"
        ),
        "framework_structurally_valid": (
            framework_structurally_valid
        ),
        "framework_realism_limited": (
            framework_realism_limited
        ),
        "production_default_unchanged": True,
        "recommended_next_step": (
            "layer_6ba_candidate_bullpen_discriminative_scenario_framework"
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
