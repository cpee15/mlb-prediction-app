#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

LAYER_ID = "6OC"
LAYER_NAME = "layer6_projection_wiring_gap_resolution_audit"
SLUG = "layer_6OC_projection_wiring_gap_resolution_audit"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/plan_6OB_projection_wiring_gap_resolution.py")
PREDECESSOR_DIR = Path("tmp/layer_6OB_projection_wiring_gap_resolution_plan")

WIRING_TARGETS_CSV = PREDECESSOR_DIR / "projection_wiring_targets.csv"
DEFERRED_CSV = PREDECESSOR_DIR / "deferred_features.csv"
SAFETY_CSV_IN = PREDECESSOR_DIR / "safety_boundaries.csv"
RECOMMENDED_CSV_IN = PREDECESSOR_DIR / "recommended_path.csv"
DIAGNOSIS_JSON_IN = PREDECESSOR_DIR / "diagnosis.json"

CHECKS_CSV = OUTDIR / "checks.csv"
PREDECESSOR_CSV = OUTDIR / "predecessor.csv"
WIRING_AUDIT_CSV = OUTDIR / "wiring_target_audit.csv"
DEFERRED_AUDIT_CSV = OUTDIR / "deferred_feature_audit.csv"
SAFETY_AUDIT_CSV = OUTDIR / "safety_audit.csv"
RECOMMENDED_CSV = OUTDIR / "recommended_path.csv"
DIAGNOSIS_JSON = OUTDIR / "diagnosis.json"

EXPECTED_PREDECESSOR_DIAGNOSIS = "layer_6_projection_wiring_gap_resolution_plan_complete"
RECOMMENDED_NEXT_LAYER = "6OD_layer6_projection_wiring_gap_resolution_implementation_plan"

REQUIRED_WIRING_TARGETS = {
    "base_out_state",
    "base_advancement_transitions",
    "ghost_runner_extra_innings",
    "double_play_logic",
    "sac_fly_logic",
    "steals_caught_stealing",
}

CRITICAL_TARGETS = {
    "base_out_state",
    "base_advancement_transitions",
    "ghost_runner_extra_innings",
}

REQUIRED_SAFETY = {
    "planning_only",
    "no_feature_wiring",
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
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
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
    predecessor_json = load_json(DIAGNOSIS_JSON_IN)

    wiring_rows = read_csv(WIRING_TARGETS_CSV)
    deferred_rows = read_csv(DEFERRED_CSV)
    safety_rows = read_csv(SAFETY_CSV_IN)
    recommended_rows_in = read_csv(RECOMMENDED_CSV_IN)

    wiring_features = {row.get("feature_name") for row in wiring_rows}
    deferred_features = {row.get("feature_name") for row in deferred_rows}
    safety_boundaries = {row.get("boundary") for row in safety_rows}

    predecessor_audit = [
        {"check": "6ob_script_exists", "expected": True, "actual": PREDECESSOR_SCRIPT.exists(), "passed": PREDECESSOR_SCRIPT.exists()},
        {"check": "6ob_regenerates_artifacts", "expected": 0, "actual": predecessor_rc, "passed": predecessor_rc == 0},
        {"check": "6ob_diagnosis", "expected": EXPECTED_PREDECESSOR_DIAGNOSIS, "actual": predecessor_json.get("diagnosis"), "passed": predecessor_json.get("diagnosis") == EXPECTED_PREDECESSOR_DIAGNOSIS},
        {"check": "6ob_all_checks_passed", "expected": True, "actual": predecessor_json.get("all_checks_passed"), "passed": predecessor_json.get("all_checks_passed") is True},
    ]

    wiring_audit = [
        {"check": "wiring_targets_file_exists", "expected": True, "actual": WIRING_TARGETS_CSV.exists(), "passed": WIRING_TARGETS_CSV.exists()},
        {"check": "wiring_targets_count", "expected": 6, "actual": len(wiring_rows), "passed": len(wiring_rows) == 6},
        {"check": "required_wiring_targets_present", "expected": True, "actual": REQUIRED_WIRING_TARGETS.issubset(wiring_features), "passed": REQUIRED_WIRING_TARGETS.issubset(wiring_features)},
        {"check": "critical_wiring_targets_present", "expected": True, "actual": CRITICAL_TARGETS.issubset(wiring_features), "passed": CRITICAL_TARGETS.issubset(wiring_features)},
        {"check": "all_targets_have_projection_target", "expected": True, "actual": all(bool(row.get("projection_target")) for row in wiring_rows), "passed": all(bool(row.get("projection_target")) for row in wiring_rows)},
        {"check": "all_targets_have_ui_target", "expected": True, "actual": all(bool(row.get("ui_target")) for row in wiring_rows), "passed": all(bool(row.get("ui_target")) for row in wiring_rows)},
    ]

    deferred_audit = [
        {"check": "deferred_features_file_exists", "expected": True, "actual": DEFERRED_CSV.exists(), "passed": DEFERRED_CSV.exists()},
        {"check": "opener_bulk_pitcher_deferred", "expected": True, "actual": "opener_bulk_pitcher" in deferred_features, "passed": "opener_bulk_pitcher" in deferred_features},
        {"check": "balks_deferred", "expected": True, "actual": "balks" in deferred_features, "passed": "balks" in deferred_features},
        {"check": "wild_pitch_passed_ball_deferred", "expected": True, "actual": "wild_pitch_passed_ball" in deferred_features, "passed": "wild_pitch_passed_ball" in deferred_features},
    ]

    safety_audit = [
        {"required_boundary": boundary, "present": boundary in safety_boundaries, "passed": boundary in safety_boundaries}
        for boundary in sorted(REQUIRED_SAFETY)
    ]

    recommended_rows = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": "plan_projection_wiring_gap_resolution_implementation_after_audit",
            "reason": "6OB wiring plan is valid; next layer may plan implementation, still without wiring.",
            "passed": True,
        }
    ]

    checks = [
        {"check": "predecessor", "passed": all(boolish(row["passed"]) for row in predecessor_audit)},
        {"check": "wiring_targets", "passed": all(boolish(row["passed"]) for row in wiring_audit)},
        {"check": "deferred_features", "passed": all(boolish(row["passed"]) for row in deferred_audit)},
        {"check": "safety_boundaries", "passed": all(boolish(row["passed"]) for row in safety_audit)},
        {"check": "recommended_path", "passed": True},
    ]

    all_passed = all(boolish(row["passed"]) for row in checks)

    write_csv(CHECKS_CSV, checks)
    write_csv(PREDECESSOR_CSV, predecessor_audit)
    write_csv(WIRING_AUDIT_CSV, wiring_audit)
    write_csv(DEFERRED_AUDIT_CSV, deferred_audit)
    write_csv(SAFETY_AUDIT_CSV, safety_audit)
    write_csv(RECOMMENDED_CSV, recommended_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_projection_wiring_gap_resolution_audit_complete" if all_passed else "layer_6_projection_wiring_gap_resolution_audit_failed",
        "all_checks_passed": all_passed,
        "predecessor_layer": "6OB",
        "predecessor_diagnosis": predecessor_json.get("diagnosis"),
        "wiring_targets_audited": len(wiring_rows),
        "critical_targets_present": CRITICAL_TARGETS.issubset(wiring_features),
        "opener_bulk_pitcher_deferred": "opener_bulk_pitcher" in deferred_features,
        "feature_wiring_allowed_next": False,
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
            str(WIRING_AUDIT_CSV),
            str(DEFERRED_AUDIT_CSV),
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
