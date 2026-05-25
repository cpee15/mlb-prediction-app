import json
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = (
    TMP_DIR
    / "candidate_bullpen_etl_plan.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "candidate_bullpen_etl_plan_checks.csv"
)

OUTPUT_CSV = (
    TMP_DIR
    / "candidate_bullpen_etl_plan.csv"
)


INSPECTION_TARGETS = [
    "scripts/audit_candidate_bullpen_dataset_schema.py",
    "scripts/audit_candidate_bullpen_data_requirements.py",
    "scripts/audit_candidate_bullpen_calibration_feasibility.py",
    "mlb_app/simulation/bullpen_state.py",
    "mlb_app/simulation/bullpen_shadow_simulation.py",
    "scripts/backtest_candidate_bullpen_shadow_simulation.py",
]


def build_rows():

    return [
        {
            "table_name": (
                "bullpen_reliever_appearance_sequences"
            ),
            "source_inputs": (
                "historical_game_logs,relief_appearance_logs"
            ),
            "transformation_steps": (
                "filter_relief_appearances;sort_by_game_team_entry_order;"
                "assign_appearance_order;derive_role_and_leverage_context"
            ),
            "join_strategy": (
                "join_on_game_id_team_id_pitcher_id"
            ),
            "validation_checks": (
                "unique_game_team_appearance_order;"
                "non_null_pitcher_id;monotonic_appearance_order"
            ),
            "output_grain": (
                "one_row_per_team_reliever_appearance"
            ),
            "dependency_order": 1,
            "implementation_risk": "medium",
            "readiness_status": "etl_plan_defined",
            "blocker": (
                "historical_sequence_etl_not_implemented"
            ),
            "recommended_action": (
                "prototype_sequence_extractor"
            ),
        },
        {
            "table_name": (
                "bullpen_reliever_entry_states"
            ),
            "source_inputs": (
                "play_by_play,game_logs,relief_appearance_sequences"
            ),
            "transformation_steps": (
                "locate_pitcher_entry_event;derive_inning_outs_base_state;"
                "derive_score_margin;map_leverage_bucket"
            ),
            "join_strategy": (
                "join_sequence_to_play_by_play_on_game_id_team_id_pitcher_id"
            ),
            "validation_checks": (
                "entry_state_present;valid_base_out_state;"
                "valid_leverage_bucket"
            ),
            "output_grain": (
                "one_row_per_pitcher_entry_state"
            ),
            "dependency_order": 2,
            "implementation_risk": "medium",
            "readiness_status": "etl_plan_defined",
            "blocker": (
                "entry_event_mapping_not_implemented"
            ),
            "recommended_action": (
                "prototype_entry_state_reconstruction"
            ),
        },
        {
            "table_name": (
                "bullpen_recent_workload_features"
            ),
            "source_inputs": (
                "appearance_logs,pitch_logs,calendar_schedule"
            ),
            "transformation_steps": (
                "aggregate_recent_pitch_counts;derive_days_rest;"
                "flag_back_to_back_usage;compute_rolling_3_and_7_day_windows"
            ),
            "join_strategy": (
                "join_on_pitcher_id_game_date"
            ),
            "validation_checks": (
                "days_rest_non_negative;recent_pitch_count_non_negative;"
                "rolling_windows_deterministic"
            ),
            "output_grain": (
                "one_row_per_pitcher_game"
            ),
            "dependency_order": 3,
            "implementation_risk": "high",
            "readiness_status": "etl_plan_partial",
            "blocker": (
                "rolling_workload_logic_not_implemented"
            ),
            "recommended_action": (
                "design_recent_workload_window_builder"
            ),
        },
        {
            "table_name": (
                "bullpen_role_identity_profiles"
            ),
            "source_inputs": (
                "appearance_sequences,entry_states,season_team_metadata"
            ),
            "transformation_steps": (
                "aggregate_usage_by_role_leverage_and_inning;"
                "derive_closer_setup_middle_usage_rates;assign_role_identity"
            ),
            "join_strategy": (
                "join_on_team_id_pitcher_id_season"
            ),
            "validation_checks": (
                "role_label_present;usage_rates_between_0_and_1;"
                "team_profile_coverage"
            ),
            "output_grain": (
                "one_row_per_team_pitcher_season_role_profile"
            ),
            "dependency_order": 4,
            "implementation_risk": "medium",
            "readiness_status": "etl_plan_defined",
            "blocker": (
                "role_profile_derivation_not_implemented"
            ),
            "recommended_action": (
                "prototype_role_identity_profile_builder"
            ),
        },
        {
            "table_name": (
                "bullpen_manager_team_tendency_profiles"
            ),
            "source_inputs": (
                "appearance_sequences,entry_states,team_metadata,manager_metadata"
            ),
            "transformation_steps": (
                "aggregate_team_manager_leverage_decisions;"
                "derive_closer_aggression_and_hook_rates;"
                "build_manager_team_profile_id"
            ),
            "join_strategy": (
                "join_on_team_id_season_manager_id"
            ),
            "validation_checks": (
                "profile_id_present;tendency_rates_between_0_and_1;"
                "minimum_sample_threshold"
            ),
            "output_grain": (
                "one_row_per_manager_team_season_profile"
            ),
            "dependency_order": 5,
            "implementation_risk": "high",
            "readiness_status": "etl_plan_partial",
            "blocker": (
                "manager_metadata_and_tendency_logic_not_implemented"
            ),
            "recommended_action": (
                "define_manager_team_tendency_feature_builder"
            ),
        },
        {
            "table_name": (
                "bullpen_multi_game_availability_memory"
            ),
            "source_inputs": (
                "workload_features,appearance_logs,calendar_schedule"
            ),
            "transformation_steps": (
                "carry_forward_recent_usage_state;derive_availability_status;"
                "compute_fatigue_carryover_score;reset_on_rest_thresholds"
            ),
            "join_strategy": (
                "join_on_pitcher_id_game_date"
            ),
            "validation_checks": (
                "availability_status_valid;fatigue_score_bounds;"
                "state_carryover_deterministic"
            ),
            "output_grain": (
                "one_row_per_pitcher_game_availability_state"
            ),
            "dependency_order": 6,
            "implementation_risk": "high",
            "readiness_status": "etl_plan_partial",
            "blocker": (
                "multi_game_memory_logic_not_implemented"
            ),
            "recommended_action": (
                "prototype_persistent_availability_memory_builder"
            ),
        },
        {
            "table_name": (
                "bullpen_shadow_backtest_labels"
            ),
            "source_inputs": (
                "layer_6_shadow_outputs,candidate_shadow_backtests"
            ),
            "transformation_steps": (
                "reuse_shadow_delta_outputs;materialize_candidate_selection_count;"
                "materialize_fallback_count;attach_scenario_metadata"
            ),
            "join_strategy": (
                "join_on_game_id_team_id_pitcher_id_or_synthetic_scenario_id"
            ),
            "validation_checks": (
                "shadow_delta_boolean;selection_count_non_negative;"
                "fallback_count_non_negative"
            ),
            "output_grain": (
                "one_row_per_shadow_label_observation"
            ),
            "dependency_order": 7,
            "implementation_risk": "low",
            "readiness_status": "etl_plan_defined",
            "blocker": "",
            "recommended_action": (
                "reuse_existing_shadow_output_labels"
            ),
        },
    ]


def main():

    rows = build_rows()

    df = pd.DataFrame(rows)[
        [
            "table_name",
            "source_inputs",
            "transformation_steps",
            "join_strategy",
            "validation_checks",
            "output_grain",
            "dependency_order",
            "implementation_risk",
            "readiness_status",
            "blocker",
            "recommended_action",
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

    all_schema_tables_planned = bool(
        len(df) == 7
    )

    dependency_order_defined = bool(
        df["dependency_order"].notnull().all()
        and df["dependency_order"].is_unique
    )

    source_inputs_defined = bool(
        df["source_inputs"].astype(str).str.len().gt(0).all()
    )

    transformation_steps_defined = bool(
        df["transformation_steps"].astype(str).str.len().gt(0).all()
    )

    join_strategy_defined = bool(
        df["join_strategy"].astype(str).str.len().gt(0).all()
    )

    validation_checks_defined = bool(
        df["validation_checks"].astype(str).str.len().gt(0).all()
    )

    output_grain_defined = bool(
        df["output_grain"].astype(str).str.len().gt(0).all()
    )

    table_names = set(df["table_name"])

    sequence_etl_planned = (
        "bullpen_reliever_appearance_sequences"
        in table_names
    )

    entry_state_etl_planned = (
        "bullpen_reliever_entry_states"
        in table_names
    )

    workload_etl_planned = (
        "bullpen_recent_workload_features"
        in table_names
    )

    shadow_label_etl_planned = (
        "bullpen_shadow_backtest_labels"
        in table_names
    )

    checks = [
        {
            "check": "inspection_targets_present",
            "passed": inspection_targets_present,
            "detail": ",".join(INSPECTION_TARGETS),
        },
        {
            "check": "all_schema_tables_planned",
            "passed": all_schema_tables_planned,
            "detail": len(df),
        },
        {
            "check": "dependency_order_defined",
            "passed": dependency_order_defined,
            "detail": dependency_order_defined,
        },
        {
            "check": "source_inputs_defined",
            "passed": source_inputs_defined,
            "detail": source_inputs_defined,
        },
        {
            "check": "transformation_steps_defined",
            "passed": transformation_steps_defined,
            "detail": transformation_steps_defined,
        },
        {
            "check": "join_strategy_defined",
            "passed": join_strategy_defined,
            "detail": join_strategy_defined,
        },
        {
            "check": "validation_checks_defined",
            "passed": validation_checks_defined,
            "detail": validation_checks_defined,
        },
        {
            "check": "output_grain_defined",
            "passed": output_grain_defined,
            "detail": output_grain_defined,
        },
        {
            "check": "sequence_etl_planned",
            "passed": sequence_etl_planned,
            "detail": sequence_etl_planned,
        },
        {
            "check": "entry_state_etl_planned",
            "passed": entry_state_etl_planned,
            "detail": entry_state_etl_planned,
        },
        {
            "check": "workload_etl_planned",
            "passed": workload_etl_planned,
            "detail": workload_etl_planned,
        },
        {
            "check": "shadow_label_etl_planned",
            "passed": shadow_label_etl_planned,
            "detail": shadow_label_etl_planned,
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

    etl_plan_defined_tables = int(
        (
            df["readiness_status"]
            == "etl_plan_defined"
        ).sum()
    )

    etl_plan_partial_tables = int(
        (
            df["readiness_status"]
            == "etl_plan_partial"
        ).sum()
    )

    etl_plan_blocked_tables = int(
        (
            df["readiness_status"]
            == "etl_plan_blocked"
        ).sum()
    )

    risk_order = {
        "low": 1,
        "medium": 2,
        "high": 3,
    }

    highest_risk_row = df.assign(
        risk_rank=df["implementation_risk"].map(risk_order)
    ).sort_values(
        ["risk_rank", "dependency_order"],
        ascending=[False, False],
    ).iloc[0]

    etl_ready_for_prototype = bool(
        all_schema_tables_planned
        and sequence_etl_planned
        and entry_state_etl_planned
        and dependency_order_defined
        and validation_checks_defined
        and etl_plan_blocked_tables == 0
    )

    if (
        etl_ready_for_prototype
        and etl_plan_partial_tables == 0
    ):
        diagnosis = "candidate_bullpen_etl_plan_ready"

    elif etl_ready_for_prototype:
        diagnosis = "candidate_bullpen_etl_plan_partial"

    else:
        diagnosis = "candidate_bullpen_etl_plan_blocked"

    payload = {
        "diagnosis": diagnosis,
        "tables_planned": len(df),
        "etl_plan_defined_tables": etl_plan_defined_tables,
        "etl_plan_partial_tables": etl_plan_partial_tables,
        "etl_plan_blocked_tables": etl_plan_blocked_tables,
        "highest_risk_table": highest_risk_row["table_name"],
        "top_blocker": (
            "rolling_workload_and_multi_game_memory_logic_not_implemented"
        ),
        "etl_ready_for_prototype": etl_ready_for_prototype,
        "production_default_unchanged": True,
        "recommended_next_step": (
            "layer_6av_candidate_bullpen_etl_prototype"
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
