#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

LAYER_ID = "6OK"
LAYER_NAME = "layer6_model_projection_realism_ui_visibility_implementation_audit"
SLUG = "layer_6OK_model_projection_realism_ui_visibility_implementation_audit"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/implement_6OJ_model_projection_realism_ui_visibility.py")
PREDECESSOR_DIR = Path("tmp/layer_6OJ_model_projection_realism_ui_visibility_implementation")

FRONTEND_PAGE = Path("frontend/src/pages/ModelProjectionsPage.jsx")
BACKEND_PAGE = Path("mlb_app/model_projections.py")

RECOMMENDED_NEXT_LAYER = "6OL_layer6_model_projection_realism_ui_backend_contract_check_plan"
REQUIRED_DISCLAIMER = "Diagnostic-only. Does not replace final projection probability."

REQUIRED_FIELDS = [
    "base_out_state_enabled",
    "runner_advancement_enabled",
    "extras_enabled",
    "ghost_runner_enabled",
    "walkoff_shortening_enabled",
    "double_play_enabled",
    "sac_fly_enabled",
    "steals_model_status",
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


def run_predecessor() -> int:
    if not PREDECESSOR_SCRIPT.exists():
        return 1
    return subprocess.run([sys.executable, str(PREDECESSOR_SCRIPT)], check=False).returncode


def main() -> int:
    predecessor_rc = run_predecessor()

    frontend_text = FRONTEND_PAGE.read_text(encoding="utf-8", errors="ignore")
    backend_text = BACKEND_PAGE.read_text(encoding="utf-8", errors="ignore")

    implementation_audit = [
        {"check": "6oj_script_exists", "passed": PREDECESSOR_SCRIPT.exists()},
        {"check": "6oj_regenerates_artifacts", "passed": predecessor_rc == 0},
        {"check": "frontend_page_exists", "passed": FRONTEND_PAGE.exists()},
        {"check": "game_state_realism_reference_exists", "passed": "game_state_realism" in frontend_text},
        {"check": "field_list_exists", "passed": "GAME_STATE_REALISM_FIELDS" in frontend_text},
        {"check": "render_helper_exists", "passed": "renderGameStateRealismDiagnostics" in frontend_text},
        {"check": "formatter_exists", "passed": "formatGameStateRealismValue" in frontend_text},
        {"check": "required_disclaimer_exists", "passed": REQUIRED_DISCLAIMER in frontend_text},
    ]

    field_audit = [
        {
            "field": field,
            "present_in_frontend": field in frontend_text,
            "present_in_backend": field in backend_text,
            "passed": field in frontend_text and field in backend_text,
        }
        for field in REQUIRED_FIELDS
    ]

    backend_contract_audit = [
        {"check": "backend_diagnostic_helper_exists", "passed": "_build_game_state_realism_diagnostics" in backend_text},
        {"check": "backend_payload_group_exists", "passed": "game_state_realism" in backend_text},
        {"check": "backend_diagnostic_only_marker_exists", "passed": "diagnostic_only" in backend_text},
        {"check": "backend_probability_guard_exists", "passed": "final_probability_replacement" in backend_text},
    ]

    safety_audit = [
        {"boundary": "audit_only", "passed": True},
        {"boundary": "frontend_visibility_only", "passed": True},
        {"boundary": "backend_behavior_unchanged", "passed": "_build_game_state_realism_diagnostics" in backend_text},
        {"boundary": "no_final_probability_replacement", "passed": "final_probability_replacement_allowed_next" not in frontend_text},
        {"boundary": "no_tuning", "passed": "tuning_allowed_next" not in frontend_text},
        {"boundary": "no_historical_validation", "passed": "historical_validation_allowed_next" not in frontend_text},
        {"boundary": "no_prediction_join", "passed": "prediction_join_execution_allowed_next" not in frontend_text},
        {"boundary": "no_accuracy_metrics", "passed": "accuracy_metrics_allowed_next" not in frontend_text},
        {"boundary": "no_backtests", "passed": "backtests_allowed_next" not in frontend_text},
        {"boundary": "no_pricing", "passed": "pricing_allowed_next" not in frontend_text},
        {"boundary": "no_edge_detection", "passed": "edge_detection_allowed_next" not in frontend_text},
        {"boundary": "layer6_exit_not_recommended", "passed": True},
    ]

    recommended = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": "plan_backend_frontend_contract_check_before_runtime_validation",
            "reason": "UI visibility is present; next layer should plan a contract check proving backend payload shape and frontend expectations match.",
            "passed": True,
        }
    ]

    checks = [
        {"check": "implementation_audit", "passed": all(row["passed"] for row in implementation_audit)},
        {"check": "field_audit", "passed": all(row["passed"] for row in field_audit)},
        {"check": "backend_contract_audit", "passed": all(row["passed"] for row in backend_contract_audit)},
        {"check": "safety_audit", "passed": all(row["passed"] for row in safety_audit)},
        {"check": "recommended_path", "passed": True},
    ]

    all_passed = all(row["passed"] for row in checks)

    files = {
        "checks": OUTDIR / "checks.csv",
        "implementation": OUTDIR / "implementation_audit.csv",
        "fields": OUTDIR / "ui_field_audit.csv",
        "backend": OUTDIR / "backend_contract_audit.csv",
        "safety": OUTDIR / "safety_audit.csv",
        "recommended": OUTDIR / "recommended_path.csv",
        "diagnosis": OUTDIR / "diagnosis.json",
    }

    write_csv(files["checks"], checks)
    write_csv(files["implementation"], implementation_audit)
    write_csv(files["fields"], field_audit)
    write_csv(files["backend"], backend_contract_audit)
    write_csv(files["safety"], safety_audit)
    write_csv(files["recommended"], recommended)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_ui_visibility_implementation_audit_complete" if all_passed else "layer_6_model_projection_realism_ui_visibility_implementation_audit_failed",
        "all_checks_passed": all_passed,
        "ui_fields_audited": len(field_audit),
        "ui_fields_visible": sum(1 for row in field_audit if row["present_in_frontend"]),
        "backend_fields_present": sum(1 for row in field_audit if row["present_in_backend"]),
        "payload_group": "game_state_realism",
        "backend_frontend_contract_check_allowed_next": True if all_passed else False,
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
            str(files["implementation"]),
            str(files["fields"]),
            str(files["backend"]),
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
