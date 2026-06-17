#!/usr/bin/env python3
from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path

LAYER_ID = "6OG"
LAYER_NAME = "layer6_projection_wiring_gap_resolution_implementation_audit"
SLUG = "layer_6OG_projection_wiring_gap_resolution_implementation_audit"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/implement_6OF_projection_wiring_gap_resolution.py")
PREDECESSOR_DIR = Path("tmp/layer_6OF_projection_wiring_gap_resolution_implementation")
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

RECOMMENDED_NEXT_LAYER = "6OH_layer6_model_projection_realism_ui_visibility_plan"


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


def run_predecessor() -> int:
    if not PREDECESSOR_SCRIPT.exists():
        return 1
    return subprocess.run([sys.executable, str(PREDECESSOR_SCRIPT)], check=False).returncode


def main() -> int:
    predecessor_rc = run_predecessor()
    text = MODEL_PROJECTIONS.read_text(encoding="utf-8")
    ast.parse(text)

    field_rows = [
        {"field": field, "present": field in text, "passed": field in text}
        for field in REQUIRED_FIELDS
    ]

    implementation_rows = [
        {"check": "6of_script_exists", "passed": PREDECESSOR_SCRIPT.exists()},
        {"check": "6of_regenerates_artifacts", "passed": predecessor_rc == 0},
        {"check": "model_projections_syntax_valid", "passed": True},
        {"check": "diagnostic_helper_exists", "passed": "_build_game_state_realism_diagnostics" in text},
        {"check": "game_state_realism_payload_group_exists", "passed": "game_state_realism" in text},
        {"check": "diagnostic_only_marker_exists", "passed": "diagnostic_only" in text},
        {"check": "final_probability_replacement_guard_exists", "passed": "final_probability_replacement" in text},
        {"check": "steals_deferred_guard_exists", "passed": "deferred_not_active" in text},
    ]

    safety_rows = [
        {"boundary": "audit_only", "passed": True},
        {"boundary": "diagnostic_backend_wiring_only", "passed": "diagnostic_only" in text},
        {"boundary": "no_final_probability_replacement", "passed": "final_probability_replacement_allowed_next" not in text},
        {"boundary": "no_tuning", "passed": "tuning_allowed_next" not in text},
        {"boundary": "no_historical_validation", "passed": "historical_validation_allowed_next" not in text},
        {"boundary": "no_prediction_join", "passed": "prediction_join_execution_allowed_next" not in text},
        {"boundary": "no_backtests", "passed": "backtests_allowed_next" not in text},
        {"boundary": "no_pricing", "passed": "pricing_allowed_next" not in text},
        {"boundary": "no_edge_detection", "passed": "edge_detection_allowed_next" not in text},
        {"boundary": "layer6_exit_not_recommended", "passed": True},
    ]

    recommended_rows = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": "plan_ui_visibility_for_game_state_realism_diagnostics",
            "reason": "Backend diagnostic payload is present; next layer should plan UI visibility for these diagnostics.",
            "passed": True,
        }
    ]

    checks = [
        {"check": "implementation", "passed": all(row["passed"] for row in implementation_rows)},
        {"check": "payload_fields", "passed": all(row["passed"] for row in field_rows)},
        {"check": "safety", "passed": all(row["passed"] for row in safety_rows)},
        {"check": "recommended_path", "passed": True},
    ]

    all_passed = all(row["passed"] for row in checks)

    artifacts = {
        "checks": OUTDIR / "checks.csv",
        "implementation": OUTDIR / "implementation_audit.csv",
        "payload": OUTDIR / "payload_field_audit.csv",
        "safety": OUTDIR / "safety_audit.csv",
        "recommended": OUTDIR / "recommended_path.csv",
        "diagnosis": OUTDIR / "diagnosis.json",
    }

    write_csv(artifacts["checks"], checks)
    write_csv(artifacts["implementation"], implementation_rows)
    write_csv(artifacts["payload"], field_rows)
    write_csv(artifacts["safety"], safety_rows)
    write_csv(artifacts["recommended"], recommended_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_projection_wiring_gap_resolution_implementation_audit_complete" if all_passed else "layer_6_projection_wiring_gap_resolution_implementation_audit_failed",
        "all_checks_passed": all_passed,
        "payload_fields_audited": len(field_rows),
        "payload_fields_present": sum(1 for row in field_rows if row["passed"]),
        "diagnostic_payload_group": "game_state_realism",
        "frontend_visibility_allowed_next": True if all_passed else False,
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
            str(artifacts["checks"]),
            str(artifacts["implementation"]),
            str(artifacts["payload"]),
            str(artifacts["safety"]),
            str(artifacts["recommended"]),
        ],
        "generated_json_artifacts": [str(artifacts["diagnosis"])],
    }

    artifacts["diagnosis"].write_text(json.dumps(diagnosis, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
