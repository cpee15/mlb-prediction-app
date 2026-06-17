#!/usr/bin/env python3
from __future__ import annotations

import ast
import csv
import json
from pathlib import Path

LAYER_ID = "6OF"
LAYER_NAME = "layer6_projection_wiring_gap_resolution_implementation"
SLUG = "layer_6OF_projection_wiring_gap_resolution_implementation"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

MODEL_PROJECTIONS = Path("mlb_app/model_projections.py")
FRONTEND_PAGE = Path("frontend/src/pages/ModelProjectionsPage.jsx")

REQUIRED_PAYLOAD_FIELDS = [
    "base_out_state_enabled",
    "runner_advancement_enabled",
    "extras_enabled",
    "ghost_runner_enabled",
    "walkoff_shortening_enabled",
    "double_play_enabled",
    "sac_fly_enabled",
    "steals_model_status",
]

PROHIBITED_TOKENS = [
    "pricing",
    "edge_detection",
    "backtest",
    "calibration",
    "tuning",
]


def write_csv(path: Path, rows):
    rows = list(rows) or [{"empty": True}]
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    model_text = MODEL_PROJECTIONS.read_text(encoding="utf-8")
    frontend_text = FRONTEND_PAGE.read_text(encoding="utf-8", errors="ignore") if FRONTEND_PAGE.exists() else ""

    ast.parse(model_text)

    payload_rows = [
        {
            "field": field,
            "present_in_model_projections": field in model_text,
            "present_in_frontend": field in frontend_text,
            "required": True,
            "passed": field in model_text,
        }
        for field in REQUIRED_PAYLOAD_FIELDS
    ]

    implementation_rows = [
        {
            "target": "diagnostic_helper",
            "path": str(MODEL_PROJECTIONS),
            "evidence": "_build_game_state_realism_diagnostics",
            "present": "_build_game_state_realism_diagnostics" in model_text,
            "passed": "_build_game_state_realism_diagnostics" in model_text,
        },
        {
            "target": "payload_group",
            "path": str(MODEL_PROJECTIONS),
            "evidence": "game_state_realism",
            "present": "game_state_realism" in model_text,
            "passed": "game_state_realism" in model_text,
        },
        {
            "target": "final_probability_replacement_guard",
            "path": str(MODEL_PROJECTIONS),
            "evidence": "final_probability_replacement",
            "present": "final_probability_replacement" in model_text,
            "passed": "final_probability_replacement" in model_text,
        },
        {
            "target": "steals_deferred_guard",
            "path": str(MODEL_PROJECTIONS),
            "evidence": "deferred_not_active",
            "present": "deferred_not_active" in model_text,
            "passed": "deferred_not_active" in model_text,
        },
    ]

    safety_rows = [
        {"boundary": "diagnostic_only", "passed": "diagnostic_only" in model_text},
        {"boundary": "no_final_probability_replacement", "passed": "final_probability_replacement_allowed_next" not in model_text},
        {"boundary": "no_pricing", "passed": "pricing_allowed_next" not in model_text},
        {"boundary": "no_edge_detection", "passed": "edge_detection_allowed_next" not in model_text},
        {"boundary": "no_backtests", "passed": "backtests_allowed_next" not in model_text},
        {"boundary": "no_tuning", "passed": "tuning_allowed_next" not in model_text},
    ]

    checks = [
        {"check": "model_projections_syntax_valid", "passed": True},
        {"check": "diagnostic_helper_present", "passed": any(row["target"] == "diagnostic_helper" and row["passed"] for row in implementation_rows)},
        {"check": "payload_group_present", "passed": any(row["target"] == "payload_group" and row["passed"] for row in implementation_rows)},
        {"check": "required_payload_fields_present", "passed": all(row["passed"] for row in payload_rows)},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows)},
    ]

    all_passed = all(row["passed"] for row in checks)

    files = {
        "checks": OUTDIR / "checks.csv",
        "implementation": OUTDIR / "implementation_evidence.csv",
        "payload": OUTDIR / "payload_field_evidence.csv",
        "safety": OUTDIR / "safety_boundaries.csv",
        "diagnosis": OUTDIR / "diagnosis.json",
    }

    write_csv(files["checks"], checks)
    write_csv(files["implementation"], implementation_rows)
    write_csv(files["payload"], payload_rows)
    write_csv(files["safety"], safety_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_projection_wiring_gap_resolution_implementation_complete" if all_passed else "layer_6_projection_wiring_gap_resolution_implementation_failed",
        "all_checks_passed": all_passed,
        "diagnostic_payload_group": "game_state_realism",
        "payload_fields_present": sum(1 for row in payload_rows if row["passed"]),
        "payload_fields_required": len(payload_rows),
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
        "recommended_next_layer": "6OG_layer6_projection_wiring_gap_resolution_implementation_audit",
        "generated_csv_artifacts": [
            str(files["checks"]),
            str(files["implementation"]),
            str(files["payload"]),
            str(files["safety"]),
        ],
        "generated_json_artifacts": [str(files["diagnosis"])],
    }

    files["diagnosis"].write_text(json.dumps(diagnosis, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
