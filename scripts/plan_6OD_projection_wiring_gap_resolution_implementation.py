#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path

LAYER_ID = "6OD"
LAYER_NAME = "layer6_projection_wiring_gap_resolution_implementation_plan"
SLUG = "layer_6OD_projection_wiring_gap_resolution_implementation_plan"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

RECOMMENDED_NEXT_LAYER = "6OE_layer6_projection_wiring_gap_resolution_implementation_plan_audit"

IMPLEMENTATION_PLAN = [
    {
        "feature_name": "base_out_state",
        "implementation_sequence": 1,
        "intended_adapter_path": "create projection-safe adapter that invokes or summarizes base/out-aware simulation without replacing final side probability",
        "model_projection_integration_point": "mlb_app/model_projections.py::_build_projection_simulation_cards",
        "payload_fields_to_expose": "base_out_state_enabled;base_out_transition_model_status;base_out_simulation_summary",
        "ui_visibility_contract": "Model Projections diagnostic simulation panel exposes base/out enabled/status/summary fields",
        "future_validation_strategy": "assert payload keys exist and diagnostic values are populated for sample projection rows",
        "rollback_boundary": "adapter output may be omitted without changing final projection probability",
        "priority": "critical",
    },
    {
        "feature_name": "base_advancement_transitions",
        "implementation_sequence": 2,
        "intended_adapter_path": "surface probabilistic runner advancement metadata from inning/subtype transition simulator",
        "model_projection_integration_point": "mlb_app/model_projections.py::_build_projection_simulation_cards",
        "payload_fields_to_expose": "runner_advancement_enabled;runner_advancement_model_status;runner_advancement_summary",
        "ui_visibility_contract": "Model Projections diagnostic simulation panel exposes advancement model status and summary",
        "future_validation_strategy": "assert single/double/groundout/flyout transition status fields are present",
        "rollback_boundary": "diagnostic-only payload removal must not alter final side probability",
        "priority": "critical",
    },
    {
        "feature_name": "ghost_runner_extra_innings",
        "implementation_sequence": 3,
        "intended_adapter_path": "wrap extras/walkoff simulator summary into projection diagnostics without replacing current final probability",
        "model_projection_integration_point": "mlb_app/model_projections.py::_build_projection_simulation_cards",
        "payload_fields_to_expose": "extras_enabled;ghost_runner_enabled;walkoff_shortening_enabled;extras_walkoff_model_status",
        "ui_visibility_contract": "Model Projections diagnostic simulation panel exposes extras/ghost-runner/walkoff status",
        "future_validation_strategy": "assert extras status appears and tie-after-regulation remains available",
        "rollback_boundary": "extras diagnostics can be disabled independently from projection cards",
        "priority": "critical",
    },
    {
        "feature_name": "double_play_logic",
        "implementation_sequence": 4,
        "intended_adapter_path": "include double-play transition metadata in base/out diagnostic summary",
        "model_projection_integration_point": "mlb_app/model_projections.py::_build_projection_simulation_cards",
        "payload_fields_to_expose": "double_play_enabled;double_play_rate_source;double_play_transition_summary",
        "ui_visibility_contract": "Model Projections diagnostics show double-play logic status",
        "future_validation_strategy": "assert double-play field exists when base/out diagnostics are enabled",
        "rollback_boundary": "diagnostic-only field can be removed without changing sim core",
        "priority": "high",
    },
    {
        "feature_name": "sac_fly_logic",
        "implementation_sequence": 5,
        "intended_adapter_path": "include sac-fly transition metadata in base/out diagnostic summary",
        "model_projection_integration_point": "mlb_app/model_projections.py::_build_projection_simulation_cards",
        "payload_fields_to_expose": "sac_fly_enabled;sac_fly_rate_source;sac_fly_transition_summary",
        "ui_visibility_contract": "Model Projections diagnostics show sac-fly logic status",
        "future_validation_strategy": "assert sac-fly field exists when base/out diagnostics are enabled",
        "rollback_boundary": "diagnostic-only field can be removed without changing sim core",
        "priority": "high",
    },
    {
        "feature_name": "steals_caught_stealing",
        "implementation_sequence": 6,
        "intended_adapter_path": "do not wire placeholder steal logic until audited as real; expose deferred status only",
        "model_projection_integration_point": "mlb_app/model_projections.py::_build_projection_simulation_cards",
        "payload_fields_to_expose": "steals_model_status;steals_projection_wiring_status",
        "ui_visibility_contract": "Model Projections diagnostics indicate steal model deferred/not active",
        "future_validation_strategy": "assert deferred status is explicit and not misrepresented as active",
        "rollback_boundary": "deferred status only; no behavioral effect allowed",
        "priority": "high",
    },
]

PAYLOAD_CONTRACT = [
    {"payload_group": "game_state_realism", "field": "base_out_state_enabled", "type": "boolean_or_null", "required_future": True},
    {"payload_group": "game_state_realism", "field": "runner_advancement_enabled", "type": "boolean_or_null", "required_future": True},
    {"payload_group": "game_state_realism", "field": "extras_enabled", "type": "boolean_or_null", "required_future": True},
    {"payload_group": "game_state_realism", "field": "ghost_runner_enabled", "type": "boolean_or_null", "required_future": True},
    {"payload_group": "game_state_realism", "field": "walkoff_shortening_enabled", "type": "boolean_or_null", "required_future": True},
    {"payload_group": "game_state_realism", "field": "double_play_enabled", "type": "boolean_or_null", "required_future": True},
    {"payload_group": "game_state_realism", "field": "sac_fly_enabled", "type": "boolean_or_null", "required_future": True},
    {"payload_group": "game_state_realism", "field": "steals_model_status", "type": "string", "required_future": True},
]

VALIDATION_PLAN = [
    {"validation": "compile_all_without_pyc", "purpose": "syntax validation without pycache filename issues"},
    {"validation": "run_future_implementation_script", "purpose": "generate implementation evidence artifacts"},
    {"validation": "payload_key_presence_check", "purpose": "confirm Model Projections payload includes diagnostics"},
    {"validation": "ui_field_reference_check", "purpose": "confirm frontend references diagnostic fields"},
    {"validation": "no_final_probability_replacement_check", "purpose": "confirm final side probability source is unchanged"},
    {"validation": "safety_boundary_check", "purpose": "confirm no tuning, backtest, pricing, or edge detection"},
]

SAFETY = [
    {"boundary": "planning_only", "passed": True},
    {"boundary": "no_feature_wiring", "passed": True},
    {"boundary": "no_runtime_behavior_change", "passed": True},
    {"boundary": "no_final_probability_replacement", "passed": True},
    {"boundary": "no_tuning", "passed": True},
    {"boundary": "no_historical_validation", "passed": True},
    {"boundary": "no_prediction_join", "passed": True},
    {"boundary": "no_accuracy_metrics", "passed": True},
    {"boundary": "no_backtests", "passed": True},
    {"boundary": "no_pricing", "passed": True},
    {"boundary": "no_edge_detection", "passed": True},
    {"boundary": "layer6_exit_not_recommended", "passed": True},
]


def write_csv(path: Path, rows):
    rows = list(rows) or [{"empty": True}]
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    features = {row["feature_name"] for row in IMPLEMENTATION_PLAN}
    required = {
        "base_out_state",
        "base_advancement_transitions",
        "ghost_runner_extra_innings",
        "double_play_logic",
        "sac_fly_logic",
        "steals_caught_stealing",
    }

    checks = [
        {"check": "implementation_targets_defined", "passed": len(IMPLEMENTATION_PLAN) == 6},
        {"check": "required_targets_present", "passed": required.issubset(features)},
        {"check": "payload_contract_defined", "passed": len(PAYLOAD_CONTRACT) >= 8},
        {"check": "validation_plan_defined", "passed": len(VALIDATION_PLAN) >= 6},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in SAFETY)},
    ]

    recommended = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": "audit_projection_wiring_gap_resolution_implementation_plan_before_wiring",
            "reason": "Implementation plan should be audited before any Model Projections or UI wiring changes.",
            "passed": True,
        }
    ]

    all_passed = all(row["passed"] for row in checks)

    files = {
        "checks": OUTDIR / "checks.csv",
        "implementation_plan": OUTDIR / "implementation_plan.csv",
        "payload_contract": OUTDIR / "payload_contract.csv",
        "validation_plan": OUTDIR / "validation_plan.csv",
        "safety": OUTDIR / "safety_boundaries.csv",
        "recommended": OUTDIR / "recommended_path.csv",
        "diagnosis": OUTDIR / "diagnosis.json",
    }

    write_csv(files["checks"], checks)
    write_csv(files["implementation_plan"], IMPLEMENTATION_PLAN)
    write_csv(files["payload_contract"], PAYLOAD_CONTRACT)
    write_csv(files["validation_plan"], VALIDATION_PLAN)
    write_csv(files["safety"], SAFETY)
    write_csv(files["recommended"], recommended)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_projection_wiring_gap_resolution_implementation_plan_complete" if all_passed else "layer_6_projection_wiring_gap_resolution_implementation_plan_failed",
        "all_checks_passed": all_passed,
        "implementation_targets_count": len(IMPLEMENTATION_PLAN),
        "payload_contract_fields": len(PAYLOAD_CONTRACT),
        "feature_wiring_allowed_next": False,
        "runtime_behavior_change_allowed_next": False,
        "final_probability_replacement_allowed_next": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "accuracy_metrics_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "layer6_exit_recommended": False,
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "generated_csv_artifacts": [
            str(files["checks"]),
            str(files["implementation_plan"]),
            str(files["payload_contract"]),
            str(files["validation_plan"]),
            str(files["safety"]),
            str(files["recommended"]),
        ],
        "generated_json_artifacts": [str(files["diagnosis"])],
    }

    files["diagnosis"].write_text(json.dumps(diagnosis, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
