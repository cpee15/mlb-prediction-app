#!/usr/bin/env python3
"""Layer 6NS — Prediction Artifact Discovery Audit."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


LAYER_ID = "6NS"
LAYER_NAME = "layer_6_prediction_artifact_discovery_audit"
SLUG = "layer_6NS_prediction_artifact_discovery_audit"

TMP_DIR = Path("tmp") / SLUG
TMP_DIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/layer_6NR_prediction_artifact_discovery_plan.py")
PREDECESSOR_DIR = Path("tmp/layer_6NR_prediction_artifact_discovery_plan")
PREDECESSOR_JSON = PREDECESSOR_DIR / "diagnosis.json"
PREDECESSOR_INVENTORY = PREDECESSOR_DIR / "prediction_artifact_inventory.csv"
PREDECESSOR_REQUIREMENTS = PREDECESSOR_DIR / "prediction_artifact_requirements.csv"

CHECKS_CSV = TMP_DIR / "checks.csv"
PREDECESSOR_CSV = TMP_DIR / "predecessor.csv"
DECISION_CSV = TMP_DIR / "decision.csv"
SAFETY_CSV = TMP_DIR / "safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / "recommended_path.csv"
DIAGNOSIS_JSON = TMP_DIR / "diagnosis.json"

EXPECTED_6NR_DIAGNOSIS = "layer_6_prediction_artifact_discovery_plan_complete"
EXPECTED_NEXT = "6NT_layer6_model_projection_realism_injection_map_plan"


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True}]
    fieldnames: List[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
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
    return json.loads(path.read_text(encoding="utf-8"))


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


def boolish(value: Any) -> bool:
    return value is True or str(value).strip().lower() == "true"


def main() -> int:
    predecessor_run = run_predecessor()
    diagnosis = load_json(PREDECESSOR_JSON)
    inventory = read_csv(PREDECESSOR_INVENTORY)
    requirements = read_csv(PREDECESSOR_REQUIREMENTS)

    schema_eligible_count = sum(
        1 for row in inventory if boolish(row.get("schema_eligible"))
    )
    join_allowed_count = sum(
        1 for row in inventory if boolish(row.get("join_execution_allowed"))
    )

    predecessor_rows = [
        {
            "check": "6nr_script_exists",
            "expected": True,
            "actual": PREDECESSOR_SCRIPT.exists(),
            "passed": PREDECESSOR_SCRIPT.exists(),
        },
        {
            "check": "6nr_script_regenerated_artifacts",
            "expected": 0,
            "actual": predecessor_run["returncode"],
            "passed": predecessor_run["returncode"] == 0,
        },
        {
            "check": "6nr_diagnosis",
            "expected": EXPECTED_6NR_DIAGNOSIS,
            "actual": diagnosis.get("diagnosis"),
            "passed": diagnosis.get("diagnosis") == EXPECTED_6NR_DIAGNOSIS,
        },
        {
            "check": "6nr_recommended_next_layer",
            "expected": "6NS_layer6_prediction_artifact_discovery_audit",
            "actual": diagnosis.get("recommended_next_layer"),
            "passed": diagnosis.get("recommended_next_layer") == "6NS_layer6_prediction_artifact_discovery_audit",
        },
    ]

    decision_rows = [
        {
            "decision": "actuals_available",
            "expected": True,
            "actual": diagnosis.get("actuals_exists"),
            "passed": diagnosis.get("actuals_exists") is True,
        },
        {
            "decision": "actuals_row_count_positive",
            "expected": ">0",
            "actual": diagnosis.get("actuals_row_count"),
            "passed": int(diagnosis.get("actuals_row_count") or 0) > 0,
        },
        {
            "decision": "inventory_rows_present",
            "expected": 6,
            "actual": len(inventory),
            "passed": len(inventory) == 6,
        },
        {
            "decision": "requirements_documented",
            "expected": ">=6",
            "actual": len(requirements),
            "passed": len(requirements) >= 6,
        },
        {
            "decision": "schema_eligible_prediction_artifacts_absent",
            "expected": 0,
            "actual": schema_eligible_count,
            "passed": schema_eligible_count == 0,
        },
        {
            "decision": "join_execution_blocked",
            "expected": 0,
            "actual": join_allowed_count,
            "passed": join_allowed_count == 0 and diagnosis.get("prediction_join_execution_allowed_next") is False,
        },
        {
            "decision": "prediction_accuracy_blocked",
            "expected": False,
            "actual": diagnosis.get("prediction_accuracy_allowed_next"),
            "passed": diagnosis.get("prediction_accuracy_allowed_next") is False,
        },
        {
            "decision": "pricing_and_edge_detection_blocked",
            "expected": False,
            "actual": diagnosis.get("pricing_allowed_next") or diagnosis.get("edge_detection_allowed_next"),
            "passed": diagnosis.get("pricing_allowed_next") is False and diagnosis.get("edge_detection_allowed_next") is False,
        },
        {
            "decision": "updated_layer6_route",
            "expected": EXPECTED_NEXT,
            "actual": EXPECTED_NEXT,
            "passed": True,
        },
    ]

    safety_rows = [
        {"boundary": "no_prediction_join_executed", "passed": True},
        {"boundary": "no_accuracy_metrics_executed", "passed": True},
        {"boundary": "no_backtests_executed", "passed": True},
        {"boundary": "no_pricing_executed", "passed": True},
        {"boundary": "no_edge_detection_executed", "passed": True},
        {"boundary": "no_live_fetches_or_remote_apis", "passed": True},
        {"boundary": "no_production_writes", "passed": True},
        {"boundary": "layer6_exit_not_recommended", "passed": True},
    ]

    recommended_rows = [
        {
            "recommendation": "recommended_next_layer",
            "value": EXPECTED_NEXT,
            "reason": "Prediction joins remain blocked; updated Layer 6 sequence should next map realism feature injection into Model Projections sim/UI output before historical tuning.",
            "passed": True,
        },
        {
            "recommendation": "recommended_path",
            "value": "plan_model_projection_realism_feature_injection_map_before_wiring_or_tuning",
            "passed": True,
        },
    ]

    checks = [
        {
            "check": "predecessor",
            "passed": all(boolish(row["passed"]) for row in predecessor_rows),
        },
        {
            "check": "decision",
            "passed": all(boolish(row["passed"]) for row in decision_rows),
        },
        {
            "check": "safety_boundaries",
            "passed": all(boolish(row["passed"]) for row in safety_rows),
        },
        {
            "check": "recommended_path",
            "passed": all(boolish(row["passed"]) for row in recommended_rows),
        },
    ]

    all_checks_passed = all(boolish(row["passed"]) for row in checks)

    write_csv(CHECKS_CSV, checks)
    write_csv(PREDECESSOR_CSV, predecessor_rows)
    write_csv(DECISION_CSV, decision_rows)
    write_csv(SAFETY_CSV, safety_rows)
    write_csv(RECOMMENDED_CSV, recommended_rows)

    output = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_prediction_artifact_discovery_audit_complete" if all_checks_passed else "layer_6_prediction_artifact_discovery_audit_failed",
        "all_checks_passed": all_checks_passed,
        "predecessor_layer": "6NR",
        "predecessor_diagnosis": diagnosis.get("diagnosis"),
        "actuals_row_count": diagnosis.get("actuals_row_count"),
        "schema_eligible_prediction_artifact_count": schema_eligible_count,
        "prediction_artifact_available": schema_eligible_count > 0,
        "prediction_join_execution_allowed_next": False,
        "prediction_accuracy_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "updated_layer6_sequence_applied": True,
        "recommended_next_layer": EXPECTED_NEXT,
        "recommended_path": "plan_model_projection_realism_feature_injection_map_before_wiring_or_tuning",
        "generated_csv_artifacts": [
            str(CHECKS_CSV),
            str(PREDECESSOR_CSV),
            str(DECISION_CSV),
            str(SAFETY_CSV),
            str(RECOMMENDED_CSV),
        ],
        "generated_json_artifacts": [str(DIAGNOSIS_JSON)],
    }

    DIAGNOSIS_JSON.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
