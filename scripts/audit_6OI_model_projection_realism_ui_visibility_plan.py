#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

LAYER_ID = "6OI"
LAYER_NAME = "layer6_model_projection_realism_ui_visibility_plan_audit"
SLUG = "layer_6OI_model_projection_realism_ui_visibility_plan_audit"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/plan_6OH_model_projection_realism_ui_visibility.py")
PREDECESSOR_DIR = Path("tmp/layer_6OH_model_projection_realism_ui_visibility_plan")

UI_PLAN = PREDECESSOR_DIR / "ui_visibility_plan.csv"
VALIDATION_PLAN = PREDECESSOR_DIR / "validation_plan.csv"
SAFETY_IN = PREDECESSOR_DIR / "safety_boundaries.csv"
DIAGNOSIS_IN = PREDECESSOR_DIR / "diagnosis.json"

RECOMMENDED_NEXT_LAYER = "6OJ_layer6_model_projection_realism_ui_visibility_implementation"
REQUIRED_DISCLAIMER = "Diagnostic-only. Does not replace final projection probability."

REQUIRED_FIELDS = {
    "base_out_state_enabled",
    "runner_advancement_enabled",
    "extras_enabled",
    "ghost_runner_enabled",
    "walkoff_shortening_enabled",
    "double_play_enabled",
    "sac_fly_enabled",
    "steals_model_status",
}

REQUIRED_SAFETY = {
    "planning_only",
    "no_frontend_wiring",
    "no_backend_behavior_change",
    "no_final_probability_replacement",
    "no_tuning",
    "no_historical_validation",
    "no_prediction_join",
    "no_accuracy_metrics",
    "no_backtests",
    "no_pricing",
    "no_edge_detection",
    "layer6_exit_not_recommended",
}


def read_csv(path: Path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


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


def load_json(path: Path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def run_predecessor() -> int:
    if not PREDECESSOR_SCRIPT.exists():
        return 1
    return subprocess.run([sys.executable, str(PREDECESSOR_SCRIPT)], check=False).returncode


def main() -> int:
    predecessor_rc = run_predecessor()
    predecessor_json = load_json(DIAGNOSIS_IN)

    ui_rows = read_csv(UI_PLAN)
    validation_rows = read_csv(VALIDATION_PLAN)
    safety_rows = read_csv(SAFETY_IN)

    ui_fields = {row.get("field") for row in ui_rows}
    safety_boundaries = {row.get("boundary") for row in safety_rows}

    plan_audit = [
        {"check": "6oh_script_exists", "passed": PREDECESSOR_SCRIPT.exists()},
        {"check": "6oh_regenerates_artifacts", "passed": predecessor_rc == 0},
        {"check": "6oh_diagnosis_complete", "passed": predecessor_json.get("diagnosis") == "layer_6_model_projection_realism_ui_visibility_plan_complete"},
        {"check": "ui_visibility_plan_exists", "passed": UI_PLAN.exists()},
        {"check": "validation_plan_exists", "passed": VALIDATION_PLAN.exists()},
        {"check": "safety_boundaries_exists", "passed": SAFETY_IN.exists()},
    ]

    ui_field_audit = [
        {
            "field": field,
            "present": field in ui_fields,
            "payload_group_game_state_realism": any(
                row.get("field") == field and row.get("payload_group") == "game_state_realism"
                for row in ui_rows
            ),
            "has_required_disclaimer": any(
                row.get("field") == field and row.get("required_disclaimer") == REQUIRED_DISCLAIMER
                for row in ui_rows
            ),
            "passed": (
                field in ui_fields
                and any(row.get("field") == field and row.get("payload_group") == "game_state_realism" for row in ui_rows)
                and any(row.get("field") == field and row.get("required_disclaimer") == REQUIRED_DISCLAIMER for row in ui_rows)
            ),
        }
        for field in sorted(REQUIRED_FIELDS)
    ]

    validation_audit = [
        {"check": "validation_plan_count", "expected": ">=6", "actual": len(validation_rows), "passed": len(validation_rows) >= 6},
        {"check": "frontend_field_reference_check_planned", "passed": any(row.get("validation") == "frontend_field_reference_check" for row in validation_rows)},
        {"check": "diagnostic_disclaimer_check_planned", "passed": any(row.get("validation") == "diagnostic_disclaimer_check" for row in validation_rows)},
        {"check": "no_probability_replacement_check_planned", "passed": any(row.get("validation") == "no_probability_replacement_check" for row in validation_rows)},
    ]

    safety_audit = [
        {"required_boundary": boundary, "present": boundary in safety_boundaries, "passed": boundary in safety_boundaries}
        for boundary in sorted(REQUIRED_SAFETY)
    ]

    recommended = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": "implement_model_projection_realism_ui_visibility_after_plan_audit",
            "reason": "UI visibility plan is valid; next layer may wire frontend diagnostics without backend behavior changes or probability replacement.",
            "passed": True,
        }
    ]

    checks = [
        {"check": "plan_audit", "passed": all(row["passed"] for row in plan_audit)},
        {"check": "ui_field_audit", "passed": len(ui_rows) == 8 and all(row["passed"] for row in ui_field_audit)},
        {"check": "validation_plan_audit", "passed": all(row["passed"] for row in validation_audit)},
        {"check": "safety_audit", "passed": all(row["passed"] for row in safety_audit)},
        {"check": "recommended_path", "passed": True},
    ]

    all_passed = all(row["passed"] for row in checks)

    files = {
        "checks": OUTDIR / "checks.csv",
        "plan_audit": OUTDIR / "plan_audit.csv",
        "ui_field_audit": OUTDIR / "ui_field_audit.csv",
        "validation_audit": OUTDIR / "validation_plan_audit.csv",
        "safety_audit": OUTDIR / "safety_audit.csv",
        "recommended": OUTDIR / "recommended_path.csv",
        "diagnosis": OUTDIR / "diagnosis.json",
    }

    write_csv(files["checks"], checks)
    write_csv(files["plan_audit"], plan_audit)
    write_csv(files["ui_field_audit"], ui_field_audit)
    write_csv(files["validation_audit"], validation_audit)
    write_csv(files["safety_audit"], safety_audit)
    write_csv(files["recommended"], recommended)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_ui_visibility_plan_audit_complete" if all_passed else "layer_6_model_projection_realism_ui_visibility_plan_audit_failed",
        "all_checks_passed": all_passed,
        "ui_fields_audited": len(ui_field_audit),
        "ui_fields_planned": len(ui_rows),
        "payload_group": "game_state_realism",
        "frontend_wiring_allowed_next": True if all_passed else False,
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
            str(files["plan_audit"]),
            str(files["ui_field_audit"]),
            str(files["validation_audit"]),
            str(files["safety_audit"]),
            str(files["recommended"]),
        ],
        "generated_json_artifacts": [str(files["diagnosis"])],
    }

    files["diagnosis"].write_text(json.dumps(diagnosis, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
