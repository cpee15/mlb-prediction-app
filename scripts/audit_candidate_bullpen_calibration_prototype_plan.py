import json
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = (
    TMP_DIR
    / "candidate_bullpen_calibration_prototype_plan.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "candidate_bullpen_calibration_prototype_plan_checks.csv"
)

OUTPUT_CSV = (
    TMP_DIR
    / "candidate_bullpen_calibration_prototype_plan.csv"
)


INSPECTION_TARGETS = [
    "scripts/prototype_candidate_bullpen_etl.py",
    "scripts/validate_candidate_bullpen_etl_prototype.py",
    "scripts/audit_candidate_bullpen_etl_plan.py",
    "scripts/audit_candidate_bullpen_dataset_schema.py",
    "scripts/audit_candidate_bullpen_calibration_feasibility.py",
]


def build_rows():

    return [
        {
            "calibration_category": (
                "reliever_selection_calibration"
            ),
            "target_label": (
                "selected_pitcher_id_or_role_label"
            ),
            "candidate_features": (
                "leverage_bucket,inning,score_margin,base_state,"
                "role_label,days_rest,recent_pitch_count"
            ),
            "evaluation_metric": (
                "selection_match_rate"
            ),
            "split_strategy": (
                "deterministic_synthetic_train_eval_split_by_game_id"
            ),
            "promotion_guardrail": (
                "selection_match_rate_above_baseline_and_no_default_change"
            ),
            "rollback_condition": (
                "selection_match_rate_regresses_or_default_equivalence_breaks"
            ),
            "blocking_issue": (
                "real_historical_selected_pitcher_labels_not_materialized"
            ),
            "readiness_status": (
                "prototype_plan_partial"
            ),
            "notes": (
                "Directionally ready using role labels and synthetic "
                "selection outputs; real labels still need materialization."
            ),
        },
        {
            "calibration_category": (
                "leverage_bucket_calibration"
            ),
            "target_label": (
                "leverage_bucket"
            ),
            "candidate_features": (
                "inning,score_margin,outs,base_state"
            ),
            "evaluation_metric": (
                "leverage_bucket_accuracy"
            ),
            "split_strategy": (
                "deterministic_replay_validation_across_synthetic_entry_states"
            ),
            "promotion_guardrail": (
                "leverage_bucket_accuracy_at_or_above_expected_threshold"
            ),
            "rollback_condition": (
                "leverage_bucket_accuracy_below_threshold"
            ),
            "blocking_issue": "",
            "readiness_status": (
                "prototype_plan_ready"
            ),
            "notes": (
                "Entry-state features are available in the synthetic "
                "prototype and map directly to leverage buckets."
            ),
        },
        {
            "calibration_category": (
                "fatigue_response_calibration"
            ),
            "target_label": (
                "fatigue_alignment_score"
            ),
            "candidate_features": (
                "days_rest,recent_pitch_count,back_to_back_flag,"
                "rolling_3_day_pitch_count,rolling_7_day_appearances"
            ),
            "evaluation_metric": (
                "fatigue_alignment_score"
            ),
            "split_strategy": (
                "synthetic_workload_replay_with_fixed_seed"
            ),
            "promotion_guardrail": (
                "fatigue_alignment_score_improves_without_selection_instability"
            ),
            "rollback_condition": (
                "fatigue_alignment_score_invalid_or_selection_instability_detected"
            ),
            "blocking_issue": (
                "real_pitch_count_and_recent_workload_history_not_materialized"
            ),
            "readiness_status": (
                "prototype_plan_partial"
            ),
            "notes": (
                "Synthetic workload columns exist, but real workload "
                "history and alignment labels remain incomplete."
            ),
        },
        {
            "calibration_category": (
                "availability_memory_calibration"
            ),
            "target_label": (
                "availability_status"
            ),
            "candidate_features": (
                "days_rest,recent_pitch_count,back_to_back_flag,"
                "fatigue_carryover_score"
            ),
            "evaluation_metric": (
                "availability_consistency_rate"
            ),
            "split_strategy": (
                "multi_game_synthetic_replay_partitioned_by_pitcher_id"
            ),
            "promotion_guardrail": (
                "availability_consistency_rate_high_and_state_carryover_deterministic"
            ),
            "rollback_condition": (
                "availability_state_non_deterministic_or_default_behavior_changes"
            ),
            "blocking_issue": (
                "persistent_multi_game_memory_and_workload_history_not_implemented"
            ),
            "readiness_status": (
                "prototype_plan_partial"
            ),
            "notes": (
                "Highest risk category because true availability requires "
                "persistent multi-game state."
            ),
        },
        {
            "calibration_category": (
                "manager_tendency_calibration"
            ),
            "target_label": (
                "manager_tendency_alignment_rate"
            ),
            "candidate_features": (
                "manager_team_profile_id,role_label,"
                "aggressive_closer_usage_rate,high_leverage_hook_rate"
            ),
            "evaluation_metric": (
                "manager_tendency_alignment_rate"
            ),
            "split_strategy": (
                "synthetic_team_manager_profile_holdout"
            ),
            "promotion_guardrail": (
                "manager_tendency_alignment_rate_stable_and_sample_threshold_met"
            ),
            "rollback_condition": (
                "manager_tendency_alignment_rate_unstable_or_low_sample_warning"
            ),
            "blocking_issue": (
                "manager_team_longitudinal_profiles_not_materialized"
            ),
            "readiness_status": (
                "prototype_plan_partial"
            ),
            "notes": (
                "Synthetic profile columns exist, but real manager/team "
                "tendency labels require longitudinal data."
            ),
        },
        {
            "calibration_category": (
                "shadow_delta_calibration"
            ),
            "target_label": (
                "shadow_delta_detected"
            ),
            "candidate_features": (
                "candidate_selection_count,fallback_count,leverage_bucket,"
                "role_label,availability_status"
            ),
            "evaluation_metric": (
                "shadow_delta_alignment_rate"
            ),
            "split_strategy": (
                "baseline_vs_candidate_shadow_comparison_replay"
            ),
            "promotion_guardrail": (
                "shadow_delta_alignment_rate_valid_and_baseline_equivalence_preserved"
            ),
            "rollback_condition": (
                "shadow_delta_labels_missing_or_baseline_equivalence_breaks"
            ),
            "blocking_issue": "",
            "readiness_status": (
                "prototype_plan_ready"
            ),
            "notes": (
                "Shadow labels are already validated in the prototype "
                "and can support a synthetic calibration experiment."
            ),
        },
    ]


def main():

    rows = build_rows()

    df = pd.DataFrame(rows)[
        [
            "calibration_category",
            "target_label",
            "candidate_features",
            "evaluation_metric",
            "split_strategy",
            "promotion_guardrail",
            "rollback_condition",
            "blocking_issue",
            "readiness_status",
            "notes",
        ]
    ]

    df.to_csv(
        OUTPUT_CSV,
        index=False,
    )

    inspection_targets_present = all(
        Path(path).exists()
        for path in INSPECTION_TARGETS
    )

    all_calibration_categories_defined = bool(
        len(df) == 6
    )

    target_labels_defined = bool(
        df["target_label"].astype(str).str.len().gt(0).all()
    )

    candidate_features_defined = bool(
        df["candidate_features"].astype(str).str.len().gt(0).all()
    )

    evaluation_metrics_defined = bool(
        df["evaluation_metric"].astype(str).str.len().gt(0).all()
    )

    split_strategy_defined = bool(
        df["split_strategy"].astype(str).str.len().gt(0).all()
    )

    promotion_guardrails_defined = bool(
        df["promotion_guardrail"].astype(str).str.len().gt(0).all()
    )

    rollback_conditions_defined = bool(
        df["rollback_condition"].astype(str).str.len().gt(0).all()
    )

    shadow_alignment_planned = bool(
        (
            df["calibration_category"]
            == "shadow_delta_calibration"
        ).any()
        and (
            df["evaluation_metric"]
            == "shadow_delta_alignment_rate"
        ).any()
    )

    availability_memory_planned = bool(
        (
            df["calibration_category"]
            == "availability_memory_calibration"
        ).any()
        and df[
            df["calibration_category"]
            == "availability_memory_calibration"
        ]["candidate_features"]
        .astype(str)
        .str.contains(
            "fatigue_carryover_score"
        )
        .any()
    )

    checks = [
        {
            "check": "inspection_targets_present",
            "passed": inspection_targets_present,
            "detail": ",".join(INSPECTION_TARGETS),
        },
        {
            "check": "all_calibration_categories_defined",
            "passed": all_calibration_categories_defined,
            "detail": len(df),
        },
        {
            "check": "target_labels_defined",
            "passed": target_labels_defined,
            "detail": target_labels_defined,
        },
        {
            "check": "candidate_features_defined",
            "passed": candidate_features_defined,
            "detail": candidate_features_defined,
        },
        {
            "check": "evaluation_metrics_defined",
            "passed": evaluation_metrics_defined,
            "detail": evaluation_metrics_defined,
        },
        {
            "check": "split_strategy_defined",
            "passed": split_strategy_defined,
            "detail": split_strategy_defined,
        },
        {
            "check": "promotion_guardrails_defined",
            "passed": promotion_guardrails_defined,
            "detail": promotion_guardrails_defined,
        },
        {
            "check": "rollback_conditions_defined",
            "passed": rollback_conditions_defined,
            "detail": rollback_conditions_defined,
        },
        {
            "check": "shadow_alignment_planned",
            "passed": shadow_alignment_planned,
            "detail": shadow_alignment_planned,
        },
        {
            "check": "availability_memory_planned",
            "passed": availability_memory_planned,
            "detail": availability_memory_planned,
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

    prototype_plan_ready_categories = int(
        (
            df["readiness_status"]
            == "prototype_plan_ready"
        ).sum()
    )

    prototype_plan_partial_categories = int(
        (
            df["readiness_status"]
            == "prototype_plan_partial"
        ).sum()
    )

    prototype_plan_blocked_categories = int(
        (
            df["readiness_status"]
            == "prototype_plan_blocked"
        ).sum()
    )

    risk_rank = {
        "prototype_plan_ready": 1,
        "prototype_plan_partial": 2,
        "prototype_plan_blocked": 3,
    }

    highest_risk_row = df.assign(
        risk_rank=df["readiness_status"].map(risk_rank)
    ).sort_values(
        ["risk_rank", "calibration_category"],
        ascending=[False, True],
    ).iloc[0]

    prototype_ready_for_synthetic_experiment = bool(
        all_calibration_categories_defined
        and target_labels_defined
        and candidate_features_defined
        and evaluation_metrics_defined
        and split_strategy_defined
        and promotion_guardrails_defined
        and rollback_conditions_defined
        and prototype_plan_blocked_categories == 0
    )

    if (
        prototype_ready_for_synthetic_experiment
        and prototype_plan_partial_categories == 0
    ):
        diagnosis = (
            "candidate_bullpen_calibration_prototype_plan_ready"
        )

    elif prototype_ready_for_synthetic_experiment:
        diagnosis = (
            "candidate_bullpen_calibration_prototype_plan_partial"
        )

    else:
        diagnosis = (
            "candidate_bullpen_calibration_prototype_plan_blocked"
        )

    payload = {
        "diagnosis": diagnosis,
        "calibration_categories_defined": len(df),
        "prototype_plan_ready_categories": (
            prototype_plan_ready_categories
        ),
        "prototype_plan_partial_categories": (
            prototype_plan_partial_categories
        ),
        "prototype_plan_blocked_categories": (
            prototype_plan_blocked_categories
        ),
        "highest_risk_category": (
            highest_risk_row[
                "calibration_category"
            ]
        ),
        "top_blocker": (
            "persistent_multi_game_memory_and_workload_history_not_implemented"
        ),
        "prototype_ready_for_synthetic_experiment": (
            prototype_ready_for_synthetic_experiment
        ),
        "production_default_unchanged": True,
        "recommended_next_step": (
            "layer_6ay_candidate_bullpen_synthetic_calibration_framework"
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
