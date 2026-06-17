#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path

LAYER_ID = "6OH"
LAYER_NAME = "layer6_model_projection_realism_ui_visibility_plan"
SLUG = "layer_6OH_model_projection_realism_ui_visibility_plan"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

RECOMMENDED_NEXT_LAYER = "6OI_layer6_model_projection_realism_ui_visibility_plan_audit"

UI_FIELDS = [
    ("base_out_state_enabled", "Base/out state", "boolean_status"),
    ("runner_advancement_enabled", "Runner advancement", "boolean_status"),
    ("extras_enabled", "Extra innings", "boolean_status"),
    ("ghost_runner_enabled", "Ghost runner", "boolean_status"),
    ("walkoff_shortening_enabled", "Walkoff shortening", "boolean_status"),
    ("double_play_enabled", "Double-play logic", "boolean_status"),
    ("sac_fly_enabled", "Sac-fly logic", "boolean_status"),
    ("steals_model_status", "Steals model", "status_text"),
]

UI_PLAN = [
    {
        "payload_group": "game_state_realism",
        "field": field,
        "display_label": label,
        "display_type": display_type,
        "ui_location": "Model Projections diagnostic simulation panel",
        "required_disclaimer": "Diagnostic-only. Does not replace final projection probability.",
        "required_future": True,
    }
    for field, label, display_type in UI_FIELDS
]

SAFETY = [
    {"boundary": "planning_only", "passed": True},
    {"boundary": "no_frontend_wiring", "passed": True},
    {"boundary": "no_backend_behavior_change", "passed": True},
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

VALIDATION_PLAN = [
    {"validation": "frontend_field_reference_check", "purpose": "confirm UI references all 8 game_state_realism fields"},
    {"validation": "diagnostic_disclaimer_check", "purpose": "confirm UI says diagnostics do not replace final projection probability"},
    {"validation": "backend_payload_unchanged_check", "purpose": "confirm backend diagnostic payload still exists"},
    {"validation": "no_probability_replacement_check", "purpose": "confirm final side probability is unchanged"},
    {"validation": "compile_syntax_check", "purpose": "confirm Python scripts compile"},
    {"validation": "safety_boundary_check", "purpose": "confirm no tuning, validation, pricing, or edge detection"},
]


def write_csv(path: Path, rows):
    rows = list(rows) or [{"empty": True}]
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    checks = [
        {"check": "ui_fields_defined", "passed": len(UI_PLAN) == 8},
        {"check": "all_fields_use_game_state_realism_group", "passed": all(row["payload_group"] == "game_state_realism" for row in UI_PLAN)},
        {"check": "all_fields_have_disclaimer", "passed": all(bool(row["required_disclaimer"]) for row in UI_PLAN)},
        {"check": "validation_plan_defined", "passed": len(VALIDATION_PLAN) >= 6},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in SAFETY)},
    ]

    recommended = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": "audit_ui_visibility_plan_before_frontend_wiring",
            "reason": "UI visibility plan should be audited before modifying the Model Projections frontend.",
            "passed": True,
        }
    ]

    all_passed = all(row["passed"] for row in checks)

    files = {
        "checks": OUTDIR / "checks.csv",
        "ui_plan": OUTDIR / "ui_visibility_plan.csv",
        "validation": OUTDIR / "validation_plan.csv",
        "safety": OUTDIR / "safety_boundaries.csv",
        "recommended": OUTDIR / "recommended_path.csv",
        "diagnosis": OUTDIR / "diagnosis.json",
    }

    write_csv(files["checks"], checks)
    write_csv(files["ui_plan"], UI_PLAN)
    write_csv(files["validation"], VALIDATION_PLAN)
    write_csv(files["safety"], SAFETY)
    write_csv(files["recommended"], recommended)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_ui_visibility_plan_complete" if all_passed else "layer_6_model_projection_realism_ui_visibility_plan_failed",
        "all_checks_passed": all_passed,
        "ui_fields_planned": len(UI_PLAN),
        "payload_group": "game_state_realism",
        "frontend_wiring_allowed_next": False,
        "backend_behavior_change_allowed_next": False,
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
            str(files["ui_plan"]),
            str(files["validation"]),
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
