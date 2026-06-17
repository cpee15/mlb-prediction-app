#!/usr/bin/env python3
"""Layer 6NY — Model Projection Realism Injection Inventory Implementation Audit."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


LAYER_ID = "6NY"
LAYER_NAME = "layer6_model_projection_realism_injection_inventory_implementation_audit"
SLUG = "layer_6NY_model_projection_realism_injection_inventory_implementation_audit"

TMP_DIR = Path("tmp") / SLUG
TMP_DIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/implement_6NX_model_projection_realism_injection_inventory.py")
PREDECESSOR_DIR = Path("tmp/layer_6NX_model_projection_realism_injection_inventory_implementation")

PREDECESSOR_JSON = PREDECESSOR_DIR / "diagnosis.json"
FEATURE_INVENTORY_CSV_IN = PREDECESSOR_DIR / "feature_injection_inventory.csv"
CLASSIFICATION_SUMMARY_CSV_IN = PREDECESSOR_DIR / "classification_summary.csv"
SOURCE_TARGETS_CSV_IN = PREDECESSOR_DIR / "source_targets_observed.csv"
BLOCKERS_CSV_IN = PREDECESSOR_DIR / "blockers.csv"
SAFETY_CSV_IN = PREDECESSOR_DIR / "safety_boundaries.csv"

CHECKS_CSV = TMP_DIR / "checks.csv"
PREDECESSOR_CSV = TMP_DIR / "predecessor.csv"
FEATURE_AUDIT_CSV = TMP_DIR / "feature_inventory_audit.csv"
CLASSIFICATION_AUDIT_CSV = TMP_DIR / "classification_summary_audit.csv"
SOURCE_TARGET_AUDIT_CSV = TMP_DIR / "source_target_audit.csv"
BLOCKER_AUDIT_CSV = TMP_DIR / "blocker_audit.csv"
SAFETY_AUDIT_CSV = TMP_DIR / "safety_audit.csv"
DECISION_CSV = TMP_DIR / "decision.csv"
RECOMMENDED_CSV = TMP_DIR / "recommended_path.csv"
DIAGNOSIS_JSON = TMP_DIR / "diagnosis.json"

EXPECTED_PREDECESSOR_DIAGNOSIS = "layer_6_model_projection_realism_injection_inventory_implementation_complete"
RECOMMENDED_NEXT_LAYER = "6NZ_layer6_model_projection_realism_injection_gap_analysis_plan"
RECOMMENDED_PATH = "plan_gap_analysis_from_inventory_before_wiring_or_tuning"

REQUIRED_FEATURES = [
    "bullpen_transition",
    "dynamic_starter_exit",
    "opener_bulk_pitcher",
    "individual_reliever_selection",
    "leverage_bullpen_usage",
    "base_out_state",
    "base_advancement_transitions",
    "double_play_logic",
    "sac_fly_logic",
    "extra_innings_ghost_runner",
    "walkoff_shortening",
    "steals_caught_stealing",
    "balks",
    "wild_pitch_passed_ball",
    "lineup_order_state",
]

ALLOWED_CLASSIFICATIONS = {
    "absent",
    "present_not_reachable",
    "sim_reachable_not_projection_reachable",
    "projection_reachable_not_ui_visible",
    "ui_visible_diagnostic_only",
    "active_in_model_projection_output",
}

REQUIRED_SAFETY_BOUNDARIES = [
    "source_inspection_only",
    "no_feature_wiring",
    "no_tuning",
    "no_historical_validation",
    "no_prediction_join",
    "no_accuracy_metrics",
    "no_backtests",
    "no_pricing",
    "no_edge_detection",
    "no_live_fetches_or_remote_apis",
    "no_production_writes",
    "layer6_exit_not_recommended",
]


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True}]
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, dict) else {}


def boolish(value: Any) -> bool:
    return value is True or str(value).strip().lower() == "true"


def run_predecessor() -> Dict[str, Any]:
    if not PREDECESSOR_SCRIPT.exists():
        return {"returncode": 1, "stdout": "", "stderr": "missing predecessor script"}
    proc = subprocess.run(
        [sys.executable, str(PREDECESSOR_SCRIPT)],
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout[-3000:],
        "stderr": proc.stderr[-3000:],
    }


def main() -> int:
    predecessor_run = run_predecessor()

    predecessor_json = load_json(PREDECESSOR_JSON)
    feature_rows = read_csv(FEATURE_INVENTORY_CSV_IN)
    summary_rows = read_csv(CLASSIFICATION_SUMMARY_CSV_IN)
    source_rows = read_csv(SOURCE_TARGETS_CSV_IN)
    blocker_rows = read_csv(BLOCKERS_CSV_IN)
    safety_rows = read_csv(SAFETY_CSV_IN)

    feature_names = {row.get("feature_family") for row in feature_rows}
    safety_names = {row.get("boundary") for row in safety_rows}
    classifications = [row.get("classification") for row in feature_rows]

    predecessor_audit = [
        {
            "check": "6nx_script_exists",
            "expected": True,
            "actual": PREDECESSOR_SCRIPT.exists(),
            "passed": PREDECESSOR_SCRIPT.exists(),
        },
        {
            "check": "6nx_script_regenerates_artifacts",
            "expected": 0,
            "actual": predecessor_run["returncode"],
            "passed": predecessor_run["returncode"] == 0,
        },
        {
            "check": "6nx_diagnosis",
            "expected": EXPECTED_PREDECESSOR_DIAGNOSIS,
            "actual": predecessor_json.get("diagnosis"),
            "passed": predecessor_json.get("diagnosis") == EXPECTED_PREDECESSOR_DIAGNOSIS,
        },
        {
            "check": "6nx_all_checks_passed",
            "expected": True,
            "actual": predecessor_json.get("all_checks_passed"),
            "passed": predecessor_json.get("all_checks_passed") is True,
        },
    ]

    feature_audit = [
        {
            "required_feature": feature,
            "present": feature in feature_names,
            "classification": next((row.get("classification") for row in feature_rows if row.get("feature_family") == feature), ""),
            "passed": feature in feature_names,
        }
        for feature in REQUIRED_FEATURES
    ]

    classification_audit = [
        {
            "check": "all_classifications_allowed",
            "expected": True,
            "actual": all(classification in ALLOWED_CLASSIFICATIONS for classification in classifications),
            "passed": all(classification in ALLOWED_CLASSIFICATIONS for classification in classifications),
        },
        {
            "check": "classification_summary_present",
            "expected": True,
            "actual": len(summary_rows) > 0,
            "passed": len(summary_rows) > 0,
        },
        {
            "check": "classification_counts_match_feature_count",
            "expected": len(feature_rows),
            "actual": sum(int(row.get("feature_count") or 0) for row in summary_rows),
            "passed": sum(int(row.get("feature_count") or 0) for row in summary_rows) == len(feature_rows),
        },
        {
            "check": "diagnosis_classification_counts_present",
            "expected": True,
            "actual": isinstance(predecessor_json.get("classification_counts"), dict),
            "passed": isinstance(predecessor_json.get("classification_counts"), dict),
        },
    ]

    source_audit = [
        {
            "check": "source_targets_observed_count",
            "expected": 15,
            "actual": len(source_rows),
            "passed": len(source_rows) == 15,
        },
        {
            "check": "all_source_targets_exist",
            "expected": True,
            "actual": all(boolish(row.get("exists")) for row in source_rows),
            "passed": all(boolish(row.get("exists")) for row in source_rows),
        },
        {
            "check": "all_source_targets_inspection_only",
            "expected": True,
            "actual": all(boolish(row.get("inspection_only")) for row in source_rows),
            "passed": all(boolish(row.get("inspection_only")) for row in source_rows),
        },
    ]

    blocker_audit = [
        {
            "check": "blockers_present",
            "expected": True,
            "actual": len(blocker_rows) >= 1,
            "passed": len(blocker_rows) >= 1,
        },
        {
            "check": "layer6_exit_blocker_active",
            "expected": True,
            "actual": any(row.get("blocker") == "layer6_exit_not_allowed" and boolish(row.get("active")) for row in blocker_rows),
            "passed": any(row.get("blocker") == "layer6_exit_not_allowed" and boolish(row.get("active")) for row in blocker_rows),
        },
    ]

    safety_audit = [
        {
            "required_boundary": boundary,
            "present": boundary in safety_names,
            "passed": boundary in safety_names,
        }
        for boundary in REQUIRED_SAFETY_BOUNDARIES
    ]

    decision_rows = [
        {"decision": "predecessor_valid", "passed": all(boolish(row["passed"]) for row in predecessor_audit)},
        {"decision": "feature_inventory_complete", "passed": all(boolish(row["passed"]) for row in feature_audit)},
        {"decision": "classification_summary_valid", "passed": all(boolish(row["passed"]) for row in classification_audit)},
        {"decision": "source_targets_valid", "passed": all(boolish(row["passed"]) for row in source_audit)},
        {"decision": "blockers_valid", "passed": all(boolish(row["passed"]) for row in blocker_audit)},
        {"decision": "safety_boundaries_preserved", "passed": all(boolish(row["passed"]) for row in safety_audit)},
        {"decision": "recommend_gap_analysis_plan_next", "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(boolish(row["passed"]) for row in predecessor_audit)},
        {"check": "feature_inventory", "passed": all(boolish(row["passed"]) for row in feature_audit)},
        {"check": "classification_summary", "passed": all(boolish(row["passed"]) for row in classification_audit)},
        {"check": "source_targets", "passed": all(boolish(row["passed"]) for row in source_audit)},
        {"check": "blockers", "passed": all(boolish(row["passed"]) for row in blocker_audit)},
        {"check": "safety_boundaries", "passed": all(boolish(row["passed"]) for row in safety_audit)},
        {"check": "decision", "passed": all(boolish(row["passed"]) for row in decision_rows)},
    ]

    all_checks_passed = all(boolish(row["passed"]) for row in checks)

    recommended_rows = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": RECOMMENDED_PATH,
            "reason": "Inventory implementation is valid; next layer should plan gap analysis before any wiring, tuning, or historical validation.",
            "passed": True,
        }
    ]

    write_csv(CHECKS_CSV, checks)
    write_csv(PREDECESSOR_CSV, predecessor_audit)
    write_csv(FEATURE_AUDIT_CSV, feature_audit)
    write_csv(CLASSIFICATION_AUDIT_CSV, classification_audit)
    write_csv(SOURCE_TARGET_AUDIT_CSV, source_audit)
    write_csv(BLOCKER_AUDIT_CSV, blocker_audit)
    write_csv(SAFETY_AUDIT_CSV, safety_audit)
    write_csv(DECISION_CSV, decision_rows)
    write_csv(RECOMMENDED_CSV, recommended_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_injection_inventory_implementation_audit_complete" if all_checks_passed else "layer_6_model_projection_realism_injection_inventory_implementation_audit_failed",
        "all_checks_passed": all_checks_passed,
        "predecessor_layer": "6NX",
        "predecessor_diagnosis": predecessor_json.get("diagnosis"),
        "features_audited": len(feature_audit),
        "source_targets_audited": len(source_rows),
        "classification_counts": predecessor_json.get("classification_counts"),
        "gap_analysis_planning_allowed_next": True if all_checks_passed else False,
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
        "recommended_path": RECOMMENDED_PATH,
        "generated_csv_artifacts": [
            str(CHECKS_CSV),
            str(PREDECESSOR_CSV),
            str(FEATURE_AUDIT_CSV),
            str(CLASSIFICATION_AUDIT_CSV),
            str(SOURCE_TARGET_AUDIT_CSV),
            str(BLOCKER_AUDIT_CSV),
            str(SAFETY_AUDIT_CSV),
            str(DECISION_CSV),
            str(RECOMMENDED_CSV),
        ],
        "generated_json_artifacts": [str(DIAGNOSIS_JSON)],
    }

    DIAGNOSIS_JSON.write_text(json.dumps(diagnosis, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
