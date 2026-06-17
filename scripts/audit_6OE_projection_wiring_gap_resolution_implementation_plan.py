#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

LAYER_ID = "6OE"
LAYER_NAME = "layer6_projection_wiring_gap_resolution_implementation_plan_audit"
SLUG = "layer_6OE_projection_wiring_gap_resolution_implementation_plan_audit"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/plan_6OD_projection_wiring_gap_resolution_implementation.py")
PREDECESSOR_DIR = Path("tmp/layer_6OD_projection_wiring_gap_resolution_implementation_plan")

IMPLEMENTATION_PLAN = PREDECESSOR_DIR / "implementation_plan.csv"
PAYLOAD_CONTRACT = PREDECESSOR_DIR / "payload_contract.csv"
VALIDATION_PLAN = PREDECESSOR_DIR / "validation_plan.csv"
SAFETY_IN = PREDECESSOR_DIR / "safety_boundaries.csv"
DIAGNOSIS_IN = PREDECESSOR_DIR / "diagnosis.json"

CHECKS_CSV = OUTDIR / "checks.csv"
PREDECESSOR_CSV = OUTDIR / "predecessor.csv"
IMPLEMENTATION_AUDIT_CSV = OUTDIR / "implementation_plan_audit.csv"
PAYLOAD_AUDIT_CSV = OUTDIR / "payload_contract_audit.csv"
VALIDATION_AUDIT_CSV = OUTDIR / "validation_plan_audit.csv"
SAFETY_AUDIT_CSV = OUTDIR / "safety_audit.csv"
RECOMMENDED_CSV = OUTDIR / "recommended_path.csv"
DIAGNOSIS_JSON = OUTDIR / "diagnosis.json"

EXPECTED_DIAGNOSIS = "layer_6_projection_wiring_gap_resolution_implementation_plan_complete"
RECOMMENDED_NEXT_LAYER = "6OF_layer6_projection_wiring_gap_resolution_implementation"

REQUIRED_FEATURES = {
    "base_out_state",
    "base_advancement_transitions",
    "ghost_runner_extra_innings",
    "double_play_logic",
    "sac_fly_logic",
    "steals_caught_stealing",
}

REQUIRED_PAYLOAD_FIELDS = {
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
    "no_feature_wiring",
    "no_runtime_behavior_change",
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
        for k in row:
            if k not in fields:
                fields.append(k)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_json(path: Path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def boolish(value) -> bool:
    return value is True or str(value).strip().lower() == "true"


def run_predecessor() -> int:
    if not PREDECESSOR_SCRIPT.exists():
        return 1
    return subprocess.run([sys.executable, str(PREDECESSOR_SCRIPT)], check=False).returncode


def main() -> int:
    predecessor_rc = run_predecessor()
    predecessor_json = load_json(DIAGNOSIS_IN)

    implementation_rows = read_csv(IMPLEMENTATION_PLAN)
    payload_rows = read_csv(PAYLOAD_CONTRACT)
    validation_rows = read_csv(VALIDATION_PLAN)
    safety_rows = read_csv(SAFETY_IN)

    implementation_features = {row.get("feature_name") for row in implementation_rows}
    payload_fields = {row.get("field") for row in payload_rows}
    safety_boundaries = {row.get("boundary") for row in safety_rows}

    predecessor_audit = [
        {"check": "6od_script_exists", "expected": True, "actual": PREDECESSOR_SCRIPT.exists(), "passed": PREDECESSOR_SCRIPT.exists()},
        {"check": "6od_regenerates_artifacts", "expected": 0, "actual": predecessor_rc, "passed": predecessor_rc == 0},
        {"check": "6od_diagnosis", "expected": EXPECTED_DIAGNOSIS, "actual": predecessor_json.get("diagnosis"), "passed": predecessor_json.get("diagnosis") == EXPECTED_DIAGNOSIS},
        {"check": "6od_all_checks_passed", "expected": True, "actual": predecessor_json.get("all_checks_passed"), "passed": predecessor_json.get("all_checks_passed") is True},
    ]

    implementation_audit = [
        {"check": "implementation_plan_exists", "expected": True, "actual": IMPLEMENTATION_PLAN.exists(), "passed": IMPLEMENTATION_PLAN.exists()},
        {"check": "implementation_targets_count", "expected": 6, "actual": len(implementation_rows), "passed": len(implementation_rows) == 6},
        {"check": "required_features_present", "expected": True, "actual": REQUIRED_FEATURES.issubset(implementation_features), "passed": REQUIRED_FEATURES.issubset(implementation_features)},
        {"check": "all_targets_have_model_projection_integration_point", "expected": True, "actual": all(bool(row.get("model_projection_integration_point")) for row in implementation_rows), "passed": all(bool(row.get("model_projection_integration_point")) for row in implementation_rows)},
        {"check": "all_targets_have_ui_visibility_contract", "expected": True, "actual": all(bool(row.get("ui_visibility_contract")) for row in implementation_rows), "passed": all(bool(row.get("ui_visibility_contract")) for row in implementation_rows)},
        {"check": "all_targets_have_rollback_boundary", "expected": True, "actual": all(bool(row.get("rollback_boundary")) for row in implementation_rows), "passed": all(bool(row.get("rollback_boundary")) for row in implementation_rows)},
    ]

    payload_audit = [
        {"check": "payload_contract_exists", "expected": True, "actual": PAYLOAD_CONTRACT.exists(), "passed": PAYLOAD_CONTRACT.exists()},
        {"check": "payload_contract_field_count", "expected": 8, "actual": len(payload_rows), "passed": len(payload_rows) >= 8},
        {"check": "required_payload_fields_present", "expected": True, "actual": REQUIRED_PAYLOAD_FIELDS.issubset(payload_fields), "passed": REQUIRED_PAYLOAD_FIELDS.issubset(payload_fields)},
    ]

    validation_audit = [
        {"check": "validation_plan_exists", "expected": True, "actual": VALIDATION_PLAN.exists(), "passed": VALIDATION_PLAN.exists()},
        {"check": "validation_plan_count", "expected": ">=6", "actual": len(validation_rows), "passed": len(validation_rows) >= 6},
    ]

    safety_audit = [
        {"required_boundary": boundary, "present": boundary in safety_boundaries, "passed": boundary in safety_boundaries}
        for boundary in sorted(REQUIRED_SAFETY)
    ]

    recommended = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": "implement_projection_wiring_gap_resolution_after_plan_audit",
            "reason": "6OD implementation plan is valid; next layer may implement guarded diagnostic wiring without tuning or final probability replacement.",
            "passed": True,
        }
    ]

    checks = [
        {"check": "predecessor", "passed": all(boolish(row["passed"]) for row in predecessor_audit)},
        {"check": "implementation_plan", "passed": all(boolish(row["passed"]) for row in implementation_audit)},
        {"check": "payload_contract", "passed": all(boolish(row["passed"]) for row in payload_audit)},
        {"check": "validation_plan", "passed": all(boolish(row["passed"]) for row in validation_audit)},
        {"check": "safety_boundaries", "passed": all(boolish(row["passed"]) for row in safety_audit)},
        {"check": "recommended_path", "passed": True},
    ]

    all_passed = all(boolish(row["passed"]) for row in checks)

    write_csv(CHECKS_CSV, checks)
    write_csv(PREDECESSOR_CSV, predecessor_audit)
    write_csv(IMPLEMENTATION_AUDIT_CSV, implementation_audit)
    write_csv(PAYLOAD_AUDIT_CSV, payload_audit)
    write_csv(VALIDATION_AUDIT_CSV, validation_audit)
    write_csv(SAFETY_AUDIT_CSV, safety_audit)
    write_csv(RECOMMENDED_CSV, recommended)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_projection_wiring_gap_resolution_implementation_plan_audit_complete" if all_passed else "layer_6_projection_wiring_gap_resolution_implementation_plan_audit_failed",
        "all_checks_passed": all_passed,
        "predecessor_layer": "6OD",
        "predecessor_diagnosis": predecessor_json.get("diagnosis"),
        "implementation_targets_audited": len(implementation_rows),
        "payload_contract_fields_audited": len(payload_rows),
        "feature_wiring_allowed_next": True if all_passed else False,
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
            str(CHECKS_CSV),
            str(PREDECESSOR_CSV),
            str(IMPLEMENTATION_AUDIT_CSV),
            str(PAYLOAD_AUDIT_CSV),
            str(VALIDATION_AUDIT_CSV),
            str(SAFETY_AUDIT_CSV),
            str(RECOMMENDED_CSV),
        ],
        "generated_json_artifacts": [str(DIAGNOSIS_JSON)],
    }

    DIAGNOSIS_JSON.write_text(json.dumps(diagnosis, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
