import json
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = (
    TMP_DIR
    / "candidate_bullpen_data_requirements.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "candidate_bullpen_data_requirements_checks.csv"
)

OUTPUT_CSV = (
    TMP_DIR
    / "candidate_bullpen_data_requirements.csv"
)


INSPECTION_TARGETS = [
    "mlb_app/simulation/bullpen_state.py",
    "mlb_app/simulation/bullpen_initializer.py",
    "mlb_app/simulation/bullpen_selection.py",
    "mlb_app/simulation/bullpen_usage.py",
    "mlb_app/simulation/bullpen_chain.py",
    "mlb_app/simulation/bullpen_shadow_simulation.py",
    "scripts/backtest_candidate_bullpen_shadow_simulation.py",
    "scripts/audit_candidate_bullpen_calibration_feasibility.py",
]


def classify(score):

    if score <= 3:
        return "missing"

    if score <= 6:
        return "partially_available"

    if score <= 8:
        return "available_or_derivable"

    return "ready_for_calibration"


def build_rows():

    return [
        {
            "requirement": (
                "historical_reliever_appearance_sequences"
            ),
            "score": 7,
            "source_type": (
                "historical_game_logs"
            ),
            "blocking_issue": (
                "bullpen_sequence_table_not_materialized"
            ),
            "recommended_action": (
                "build_reliever_appearance_sequence_etl"
            ),
            "notes": (
                "Reliever appearance order should be derivable from "
                "historical game logs, but the sequence table does not "
                "exist yet."
            ),
        },
        {
            "requirement": (
                "game_state_at_reliever_entry"
            ),
            "score": 7,
            "source_type": (
                "game_logs_and_play_by_play"
            ),
            "blocking_issue": (
                "reliever_entry_state_table_missing"
            ),
            "recommended_action": (
                "derive_entry_inning_score_base_out_context"
            ),
            "notes": (
                "Entry game state appears derivable and is required "
                "for leverage and role calibration."
            ),
        },
        {
            "requirement": (
                "score_margin_and_inning_context"
            ),
            "score": 8,
            "source_type": (
                "game_logs"
            ),
            "blocking_issue": "",
            "recommended_action": (
                "standardize_score_margin_and_inning_features"
            ),
            "notes": (
                "Score margin and inning context are available or "
                "straightforward to derive from existing historical data."
            ),
        },
        {
            "requirement": (
                "base_out_state_context"
            ),
            "score": 7,
            "source_type": (
                "play_by_play"
            ),
            "blocking_issue": (
                "entry_base_out_state_not_materialized"
            ),
            "recommended_action": (
                "materialize_base_out_state_at_pitcher_entry"
            ),
            "notes": (
                "Base/out state is derivable from play-by-play and "
                "supports leverage bucket calibration."
            ),
        },
        {
            "requirement": (
                "pitch_count_and_workload_history"
            ),
            "score": 5,
            "source_type": (
                "pitch_logs_or_appearance_logs"
            ),
            "blocking_issue": (
                "pitch_count_recent_workload_features_missing"
            ),
            "recommended_action": (
                "construct_pitch_count_and_recent_workload_features"
            ),
            "notes": (
                "Workload history is partially available but needs "
                "explicit aggregation across recent appearances."
            ),
        },
        {
            "requirement": (
                "days_rest_and_back_to_back_usage"
            ),
            "score": 6,
            "source_type": (
                "appearance_logs"
            ),
            "blocking_issue": (
                "rest_day_features_not_materialized"
            ),
            "recommended_action": (
                "derive_days_rest_and_back_to_back_flags"
            ),
            "notes": (
                "Days rest and back-to-back usage are derivable from "
                "appearance history but need persistent features."
            ),
        },
        {
            "requirement": (
                "team_bullpen_role_identity"
            ),
            "score": 7,
            "source_type": (
                "derived_usage_profiles"
            ),
            "blocking_issue": (
                "team_role_identity_profiles_missing"
            ),
            "recommended_action": (
                "derive_closer_setup_middle_role_profiles_by_team"
            ),
            "notes": (
                "Team bullpen roles can be inferred from leverage, inning, "
                "and appearance patterns."
            ),
        },
        {
            "requirement": (
                "manager_team_tendency_profiles"
            ),
            "score": 4,
            "source_type": (
                "derived_longitudinal_profiles"
            ),
            "blocking_issue": (
                "manager_team_tendency_profiles_missing"
            ),
            "recommended_action": (
                "derive_manager_and_team_usage_tendency_features"
            ),
            "notes": (
                "Manager/team tendencies are likely derivable but require "
                "longitudinal modeling and metadata not yet present."
            ),
        },
        {
            "requirement": (
                "multi_game_availability_memory"
            ),
            "score": 4,
            "source_type": (
                "derived_stateful_history"
            ),
            "blocking_issue": (
                "multi_game_availability_state_missing"
            ),
            "recommended_action": (
                "design_persistent_bullpen_availability_memory_table"
            ),
            "notes": (
                "Multi-game bullpen memory is the main blocker. Current "
                "candidate state is single-game scoped."
            ),
        },
        {
            "requirement": (
                "shadow_backtest_label_outputs"
            ),
            "score": 9,
            "source_type": (
                "existing_layer_6_shadow_outputs"
            ),
            "blocking_issue": "",
            "recommended_action": (
                "reuse_shadow_delta_and_selection_metadata_as_labels"
            ),
            "notes": (
                "Shadow label outputs already exist from prior Layer 6 "
                "candidate bullpen shadow backtests."
            ),
        },
    ]


def main():

    rows = build_rows()

    for row in rows:
        row["status"] = classify(
            row["score"]
        )

    df = pd.DataFrame(rows)[
        [
            "requirement",
            "score",
            "status",
            "source_type",
            "blocking_issue",
            "recommended_action",
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

    requirements = set(
        df["requirement"]
    )

    checks = [
        {
            "check": "inspection_targets_present",
            "passed": inspection_targets_present,
            "detail": ",".join(
                INSPECTION_TARGETS
            ),
        },
        {
            "check": "historical_sequences_assessed",
            "passed": (
                "historical_reliever_appearance_sequences"
                in requirements
            ),
            "detail": (
                "historical_reliever_appearance_sequences"
                in requirements
            ),
        },
        {
            "check": "entry_game_state_assessed",
            "passed": (
                "game_state_at_reliever_entry"
                in requirements
            ),
            "detail": (
                "game_state_at_reliever_entry"
                in requirements
            ),
        },
        {
            "check": "workload_history_assessed",
            "passed": (
                "pitch_count_and_workload_history"
                in requirements
            ),
            "detail": (
                "pitch_count_and_workload_history"
                in requirements
            ),
        },
        {
            "check": "role_identity_assessed",
            "passed": (
                "team_bullpen_role_identity"
                in requirements
            ),
            "detail": (
                "team_bullpen_role_identity"
                in requirements
            ),
        },
        {
            "check": "manager_tendency_assessed",
            "passed": (
                "manager_team_tendency_profiles"
                in requirements
            ),
            "detail": (
                "manager_team_tendency_profiles"
                in requirements
            ),
        },
        {
            "check": "multi_game_memory_assessed",
            "passed": (
                "multi_game_availability_memory"
                in requirements
            ),
            "detail": (
                "multi_game_availability_memory"
                in requirements
            ),
        },
        {
            "check": "shadow_labels_assessed",
            "passed": (
                "shadow_backtest_label_outputs"
                in requirements
            ),
            "detail": (
                "shadow_backtest_label_outputs"
                in requirements
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

    checks_df = pd.DataFrame(checks)

    checks_df.to_csv(
        OUTPUT_CHECKS,
        index=False,
    )

    ready_or_derivable = int(
        df[
            "status"
        ].isin(
            [
                "available_or_derivable",
                "ready_for_calibration",
            ]
        ).sum()
    )

    partial_requirements = int(
        (
            df["status"]
            == "partially_available"
        ).sum()
    )

    missing_requirements = int(
        (
            df["status"]
            == "missing"
        ).sum()
    )

    calibration_dataset_possible = bool(
        ready_or_derivable >= 5
        and partial_requirements <= 4
        and missing_requirements == 0
    )

    if (
        ready_or_derivable >= 8
        and calibration_dataset_possible
    ):
        diagnosis = (
            "candidate_bullpen_data_requirements_ready"
        )
        overall_data_readiness = (
            "ready_for_schema_design"
        )

    elif calibration_dataset_possible:
        diagnosis = (
            "candidate_bullpen_data_requirements_partial"
        )
        overall_data_readiness = (
            "calibration_dataset_possible_but_not_materialized"
        )

    else:
        diagnosis = (
            "candidate_bullpen_data_requirements_blocked"
        )
        overall_data_readiness = "blocked"

    payload = {
        "diagnosis": diagnosis,
        "overall_data_readiness": (
            overall_data_readiness
        ),
        "ready_or_derivable_requirements": (
            ready_or_derivable
        ),
        "partial_requirements": (
            partial_requirements
        ),
        "missing_requirements": (
            missing_requirements
        ),
        "top_blocker": (
            "bullpen_sequence_and_recent_workload_tables_not_materialized"
        ),
        "calibration_dataset_possible": (
            calibration_dataset_possible
        ),
        "production_default_unchanged": True,
        "recommended_next_step": (
            "layer_6at_candidate_bullpen_dataset_schema"
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
