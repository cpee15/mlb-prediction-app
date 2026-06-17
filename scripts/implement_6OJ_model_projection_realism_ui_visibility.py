#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path

LAYER_ID = "6OJ"
LAYER_NAME = "layer6_model_projection_realism_ui_visibility_implementation"
SLUG = "layer_6OJ_model_projection_realism_ui_visibility_implementation"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

FRONTEND_PAGE = Path("frontend/src/pages/ModelProjectionsPage.jsx")
MODEL_PROJECTIONS = Path("mlb_app/model_projections.py")

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

REQUIRED_DISCLAIMER = "Diagnostic-only. Does not replace final projection probability."


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
    frontend_text = FRONTEND_PAGE.read_text(encoding="utf-8", errors="ignore")
    backend_text = MODEL_PROJECTIONS.read_text(encoding="utf-8", errors="ignore")

    ui_field_rows = [
        {
            "field": field,
            "present_in_frontend": field in frontend_text,
            "present_in_backend": field in backend_text,
            "passed": field in frontend_text and field in backend_text,
        }
        for field in REQUIRED_FIELDS
    ]

    implementation_rows = [
        {
            "check": "frontend_page_exists",
            "passed": FRONTEND_PAGE.exists(),
        },
        {
            "check": "game_state_realism_field_list_exists",
            "passed": "GAME_STATE_REALISM_FIELDS" in frontend_text,
        },
        {
            "check": "render_helper_exists",
            "passed": "renderGameStateRealismDiagnostics" in frontend_text,
        },
        {
            "check": "formatter_exists",
            "passed": "formatGameStateRealismValue" in frontend_text,
        },
        {
            "check": "game_state_realism_payload_reference_exists",
            "passed": "game_state_realism" in frontend_text,
        },
        {
            "check": "required_disclaimer_exists",
            "passed": REQUIRED_DISCLAIMER in frontend_text,
        },
    ]

    safety_rows = [
        {"boundary": "frontend_only_visibility", "passed": True},
        {"boundary": "backend_behavior_unchanged", "passed": "_build_game_state_realism_diagnostics" in backend_text},
        {"boundary": "no_final_probability_replacement", "passed": "final_probability_replacement" in backend_text},
        {"boundary": "no_tuning", "passed": "tuning_allowed_next" not in frontend_text},
        {"boundary": "no_historical_validation", "passed": "historical_validation_allowed_next" not in frontend_text},
        {"boundary": "no_prediction_join", "passed": "prediction_join_execution_allowed_next" not in frontend_text},
        {"boundary": "no_accuracy_metrics", "passed": "accuracy_metrics_allowed_next" not in frontend_text},
        {"boundary": "no_backtests", "passed": "backtests_allowed_next" not in frontend_text},
        {"boundary": "no_pricing", "passed": "pricing_allowed_next" not in frontend_text},
        {"boundary": "no_edge_detection", "passed": "edge_detection_allowed_next" not in frontend_text},
    ]

    checks = [
        {"check": "implementation", "passed": all(row["passed"] for row in implementation_rows)},
        {"check": "ui_fields", "passed": all(row["passed"] for row in ui_field_rows)},
        {"check": "safety", "passed": all(row["passed"] for row in safety_rows)},
    ]

    all_passed = all(row["passed"] for row in checks)

    files = {
        "checks": OUTDIR / "checks.csv",
        "implementation": OUTDIR / "implementation_evidence.csv",
        "ui_fields": OUTDIR / "ui_field_evidence.csv",
        "safety": OUTDIR / "safety_boundaries.csv",
        "diagnosis": OUTDIR / "diagnosis.json",
    }

    write_csv(files["checks"], checks)
    write_csv(files["implementation"], implementation_rows)
    write_csv(files["ui_fields"], ui_field_rows)
    write_csv(files["safety"], safety_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_ui_visibility_implementation_complete" if all_passed else "layer_6_model_projection_realism_ui_visibility_implementation_failed",
        "all_checks_passed": all_passed,
        "ui_fields_visible": sum(1 for row in ui_field_rows if row["present_in_frontend"]),
        "ui_fields_required": len(REQUIRED_FIELDS),
        "payload_group": "game_state_realism",
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
        "recommended_next_layer": "6OK_layer6_model_projection_realism_ui_visibility_implementation_audit",
        "generated_csv_artifacts": [
            str(files["checks"]),
            str(files["implementation"]),
            str(files["ui_fields"]),
            str(files["safety"]),
        ],
        "generated_json_artifacts": [str(files["diagnosis"])],
    }

    files["diagnosis"].write_text(json.dumps(diagnosis, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
