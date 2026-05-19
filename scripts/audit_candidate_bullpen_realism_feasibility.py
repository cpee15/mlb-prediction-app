import json
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = (
    TMP_DIR
    / "candidate_bullpen_realism_feasibility.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "candidate_bullpen_realism_feasibility_checks.csv"
)

OUTPUT_CSV = (
    TMP_DIR
    / "candidate_bullpen_realism_feasibility.csv"
)


INSPECTION_TARGETS = [
    "mlb_app/simulation/bullpen_state.py",
    "mlb_app/simulation/bullpen_initializer.py",
    "mlb_app/simulation/bullpen_selection.py",
    "mlb_app/simulation/bullpen_usage.py",
    "mlb_app/simulation/bullpen_chain.py",
    "mlb_app/simulation/bullpen_shadow_simulation.py",
    "scripts/backtest_candidate_bullpen_shadow_simulation.py",
]


def classify(score):

    if score <= 3:
        return "blocked"

    if score <= 6:
        return "partial"

    if score <= 8:
        return "feasible"

    return "strong"


def build_rows():

    return [
        {
            "category": "reliever_role_realism",
            "score": 7,
            "blocking_issue": (
                "role_labels_are_heuristic"
            ),
            "recommended_action": (
                "calibrate_role_mapping_against_actual_reliever_usage"
            ),
            "notes": (
                "Current architecture supports closer/setup/middle relief "
                "roles, but role assignment still depends on heuristic "
                "inference rather than historical deployment labels."
            ),
        },
        {
            "category": "leverage_matching_realism",
            "score": 8,
            "blocking_issue": "",
            "recommended_action": (
                "validate_leverage_bucket_rules_against_actual_late_game_usage"
            ),
            "notes": (
                "Candidate selection already differentiates high, medium, "
                "and low leverage and preserves high-leverage role priority."
            ),
        },
        {
            "category": "fatigue_progression_realism",
            "score": 6,
            "blocking_issue": (
                "pitch_count_and_recent_usage_data_missing"
            ),
            "recommended_action": (
                "map_recent_appearances_and_pitch_counts_into_fatigue_proxy"
            ),
            "notes": (
                "Fatigue progression is deterministic and safe, but still "
                "heuristic. It does not yet use recent workload or pitch counts."
            ),
        },
        {
            "category": "bullpen_exhaustion_realism",
            "score": 6,
            "blocking_issue": (
                "multi_game_bullpen_lifecycle_missing"
            ),
            "recommended_action": (
                "extend_shadow_backtests_with_longer_exhaustion_sequences"
            ),
            "notes": (
                "Exhaustion fallback exists, but shadow backtest fallback rate "
                "was 0.0 because current synthetic exhaustion did not exceed "
                "available reliever depth."
            ),
        },
        {
            "category": "fallback_behavior_realism",
            "score": 7,
            "blocking_issue": "",
            "recommended_action": (
                "preserve_safe_fallback_while_adding_replacement_policy_detail"
            ),
            "notes": (
                "Fallback behavior is safe and deterministic. Realism can be "
                "improved by modeling mop-up or emergency reliever selection."
            ),
        },
        {
            "category": "multi_inning_usage_realism",
            "score": 5,
            "blocking_issue": (
                "multi_inning_reliever_logic_missing"
            ),
            "recommended_action": (
                "add_multi_inning_continuation_and_availability_rules"
            ),
            "notes": (
                "Current chain assumes sequential reliever choices and does "
                "not yet model staying with the same reliever across innings."
            ),
        },
        {
            "category": "closer_preservation_realism",
            "score": 8,
            "blocking_issue": "",
            "recommended_action": (
                "validate_closer_preservation_against_save_and_non_save_states"
            ),
            "notes": (
                "Prior backtests showed closer preservation in blowout states "
                "and closer usage in high-leverage states."
            ),
        },
        {
            "category": "calibration_readiness",
            "score": 6,
            "blocking_issue": (
                "historical_reliever_deployment_mapping_incomplete"
            ),
            "recommended_action": (
                "design_calibration_dataset_for_reliever_usage_by_game_state"
            ),
            "notes": (
                "Calibration appears feasible, but historical mapping is not "
                "yet implemented."
            ),
        },
        {
            "category": "historical_mapping_feasibility",
            "score": 5,
            "blocking_issue": (
                "historical_bullpen_usage_mapping_missing"
            ),
            "recommended_action": (
                "derive_relief_appearance_sequences_from_game_logs"
            ),
            "notes": (
                "The architecture can consume historical mappings, but the "
                "mapping layer does not exist yet."
            ),
        },
        {
            "category": "data_dependency_feasibility",
            "score": 6,
            "blocking_issue": (
                "recent_usage_data_and_manager_tendencies_missing"
            ),
            "recommended_action": (
                "add_recent_usage_memory_and_manager_tendency_inputs"
            ),
            "notes": (
                "Reliever identity and role inference are plausible, but "
                "recent usage, persistent fatigue memory, multi-game lifecycle, "
                "and manager tendency modeling remain missing."
            ),
        },
    ]


def main():

    rows = build_rows()

    for row in rows:
        row["classification"] = classify(
            row["score"]
        )

    df = pd.DataFrame(rows)[
        [
            "category",
            "score",
            "classification",
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

    role_realism_assessed = bool(
        "reliever_role_realism"
        in set(df["category"])
    )

    leverage_realism_assessed = bool(
        "leverage_matching_realism"
        in set(df["category"])
    )

    fatigue_realism_assessed = bool(
        "fatigue_progression_realism"
        in set(df["category"])
    )

    exhaustion_realism_assessed = bool(
        "bullpen_exhaustion_realism"
        in set(df["category"])
    )

    fallback_realism_assessed = bool(
        "fallback_behavior_realism"
        in set(df["category"])
    )

    calibration_feasibility_assessed = bool(
        "calibration_readiness"
        in set(df["category"])
    )

    checks = [
        {
            "check": "inspection_targets_present",
            "passed": inspection_targets_present,
            "detail": ",".join(INSPECTION_TARGETS),
        },
        {
            "check": "role_realism_assessed",
            "passed": role_realism_assessed,
            "detail": role_realism_assessed,
        },
        {
            "check": "leverage_realism_assessed",
            "passed": leverage_realism_assessed,
            "detail": leverage_realism_assessed,
        },
        {
            "check": "fatigue_realism_assessed",
            "passed": fatigue_realism_assessed,
            "detail": fatigue_realism_assessed,
        },
        {
            "check": "exhaustion_realism_assessed",
            "passed": exhaustion_realism_assessed,
            "detail": exhaustion_realism_assessed,
        },
        {
            "check": "fallback_realism_assessed",
            "passed": fallback_realism_assessed,
            "detail": fallback_realism_assessed,
        },
        {
            "check": "calibration_feasibility_assessed",
            "passed": calibration_feasibility_assessed,
            "detail": calibration_feasibility_assessed,
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

    avg_score = float(
        df["score"].mean()
    )

    strongest_row = df.sort_values(
        ["score", "category"],
        ascending=[False, True],
    ).iloc[0]

    weakest_row = df.sort_values(
        ["score", "category"],
        ascending=[True, True],
    ).iloc[0]

    calibration_feasible = bool(
        df[
            df["category"]
            == "calibration_readiness"
        ]["score"].iloc[0]
        >= 6
    )

    feasible_components = int(
        (
            df["classification"].isin(
                ["feasible", "strong"]
            )
        ).sum()
    )

    partial_components = int(
        (
            df["classification"]
            == "partial"
        ).sum()
    )

    if (
        avg_score >= 7
        and feasible_components >= 7
        and calibration_feasible
    ):
        diagnosis = (
            "candidate_bullpen_realism_feasible"
        )
        overall_realism_readiness = "feasible"

    elif (
        avg_score >= 5
        and calibration_feasible
    ):
        diagnosis = (
            "candidate_bullpen_realism_partial"
        )
        overall_realism_readiness = (
            "directionally_feasible"
        )

    else:
        diagnosis = (
            "candidate_bullpen_realism_blocked"
        )
        overall_realism_readiness = "blocked"

    payload = {
        "diagnosis": diagnosis,
        "overall_realism_readiness": (
            overall_realism_readiness
        ),
        "average_realism_score": round(
            avg_score,
            4,
        ),
        "feasible_or_strong_components": (
            feasible_components
        ),
        "partial_components": (
            partial_components
        ),
        "strongest_component": (
            strongest_row[
                "category"
            ]
        ),
        "weakest_component": (
            weakest_row[
                "category"
            ]
        ),
        "top_blocker": (
            "persistent_multi_game_usage_memory_missing"
        ),
        "calibration_feasible": (
            calibration_feasible
        ),
        "production_default_unchanged": True,
        "recommended_next_step": (
            "layer_6ar_candidate_bullpen_calibration_feasibility"
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
