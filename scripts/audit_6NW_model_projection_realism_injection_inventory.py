#!/usr/bin/env python3
"""Layer 6NW — Model Projection Realism Injection Inventory Audit."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


LAYER_ID = "6NW"
LAYER_NAME = "layer6_model_projection_realism_injection_inventory_audit"
SLUG = "layer_6NW_model_projection_realism_injection_inventory_audit"

TMP_DIR = Path("tmp") / SLUG
TMP_DIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/plan_6NV_model_projection_realism_injection_inventory.py")
PREDECESSOR_DIR = Path("tmp/layer_6NV_model_projection_realism_injection_inventory_plan")

PREDECESSOR_JSON = PREDECESSOR_DIR / "diagnosis.json"
SOURCE_TARGETS_CSV_IN = PREDECESSOR_DIR / "source_targets.csv"
CLASSIFICATION_CSV_IN = PREDECESSOR_DIR / "classification_contract.csv"
FEATURE_PLAN_CSV_IN = PREDECESSOR_DIR / "feature_inventory_plan.csv"
EVIDENCE_FIELDS_CSV_IN = PREDECESSOR_DIR / "evidence_fields.csv"
SAFETY_CSV_IN = PREDECESSOR_DIR / "safety_boundaries.csv"

CHECKS_CSV = TMP_DIR / "checks.csv"
PREDECESSOR_CSV = TMP_DIR / "predecessor.csv"
FEATURE_AUDIT_CSV = TMP_DIR / "feature_plan_audit.csv"
SOURCE_TARGET_AUDIT_CSV = TMP_DIR / "source_target_audit.csv"
CLASSIFICATION_AUDIT_CSV = TMP_DIR / "classification_contract_audit.csv"
EVIDENCE_AUDIT_CSV = TMP_DIR / "evidence_field_audit.csv"
SAFETY_AUDIT_CSV = TMP_DIR / "safety_audit.csv"
DECISION_CSV = TMP_DIR / "decision.csv"
RECOMMENDED_CSV = TMP_DIR / "recommended_path.csv"
DIAGNOSIS_JSON = TMP_DIR / "diagnosis.json"

EXPECTED_PREDECESSOR_DIAGNOSIS = "layer_6_model_projection_realism_injection_inventory_plan_complete"
RECOMMENDED_NEXT_LAYER = "6NX_layer6_model_projection_realism_injection_inventory_implementation"
RECOMMENDED_PATH = "implement_source_inspection_inventory_for_realism_feature_injection_status"

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

REQUIRED_CLASSIFICATIONS = [
    "absent",
    "present_not_reachable",
    "sim_reachable_not_projection_reachable",
    "projection_reachable_not_ui_visible",
    "ui_visible_diagnostic_only",
    "active_in_model_projection_output",
]

REQUIRED_EVIDENCE_FIELDS = [
    "feature_family",
    "classification",
    "exists_status",
    "sim_reachability_status",
    "projection_reachability_status",
    "ui_visibility_status",
    "active_status",
    "diagnostic_only_status",
    "source_files_with_evidence",
    "evidence_summary",
    "blocker",
    "recommended_next_action",
]

REQUIRED_SAFETY_BOUNDARIES = [
    "planning_only",
    "source_inspection_not_executed",
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
    source_rows = read_csv(SOURCE_TARGETS_CSV_IN)
    classification_rows = read_csv(CLASSIFICATION_CSV_IN)
    feature_rows = read_csv(FEATURE_PLAN_CSV_IN)
    evidence_rows = read_csv(EVIDENCE_FIELDS_CSV_IN)
    safety_rows = read_csv(SAFETY_CSV_IN)

    feature_names = {row.get("feature_family") for row in feature_rows}
    source_paths = {row.get("source_path") for row in source_rows}
    classification_names = {row.get("classification") for row in classification_rows}
    evidence_names = {row.get("field") for row in evidence_rows}
    safety_names = {row.get("boundary") for row in safety_rows}

    predecessor_audit = [
        {
            "check": "6nv_script_exists",
            "expected": True,
            "actual": PREDECESSOR_SCRIPT.exists(),
            "passed": PREDECESSOR_SCRIPT.exists(),
        },
        {
            "check": "6nv_script_regenerates_artifacts",
            "expected": 0,
            "actual": predecessor_run["returncode"],
            "passed": predecessor_run["returncode"] == 0,
        },
        {
            "check": "6nv_diagnosis",
            "expected": EXPECTED_PREDECESSOR_DIAGNOSIS,
            "actual": predecessor_json.get("diagnosis"),
            "passed": predecessor_json.get("diagnosis") == EXPECTED_PREDECESSOR_DIAGNOSIS,
        },
        {
            "check": "6nv_all_checks_passed",
            "expected": True,
            "actual": predecessor_json.get("all_checks_passed"),
            "passed": predecessor_json.get("all_checks_passed") is True,
        },
    ]

    feature_audit = [
        {
            "required_feature": feature,
            "present": feature in feature_names,
            "passed": feature in feature_names,
        }
        for feature in REQUIRED_FEATURES
    ]

    source_audit = [
        {
            "check": "source_target_count",
            "expected": ">=10",
            "actual": len(source_rows),
            "passed": len(source_rows) >= 10,
        },
        {
            "check": "model_projections_included",
            "expected": True,
            "actual": "mlb_app/model_projections.py" in source_paths,
            "passed": "mlb_app/model_projections.py" in source_paths,
        },
        {
            "check": "frontend_model_projections_page_included",
            "expected": True,
            "actual": "frontend/src/pages/ModelProjectionsPage.jsx" in source_paths,
            "passed": "frontend/src/pages/ModelProjectionsPage.jsx" in source_paths,
        },
        {
            "check": "game_simulator_included",
            "expected": True,
            "actual": "mlb_app/simulation/game_simulator.py" in source_paths,
            "passed": "mlb_app/simulation/game_simulator.py" in source_paths,
        },
        {
            "check": "base_out_adapter_included",
            "expected": True,
            "actual": "mlb_app/simulation/layer6_base_out_transition_adapter.py" in source_paths,
            "passed": "mlb_app/simulation/layer6_base_out_transition_adapter.py" in source_paths,
        },
    ]

    classification_audit = [
        {
            "required_classification": classification,
            "present": classification in classification_names,
            "passed": classification in classification_names,
        }
        for classification in REQUIRED_CLASSIFICATIONS
    ]

    evidence_audit = [
        {
            "required_field": field,
            "present": field in evidence_names,
            "passed": field in evidence_names,
        }
        for field in REQUIRED_EVIDENCE_FIELDS
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
        {"decision": "feature_plan_complete", "passed": all(boolish(row["passed"]) for row in feature_audit)},
        {"decision": "source_targets_complete", "passed": all(boolish(row["passed"]) for row in source_audit)},
        {"decision": "classification_contract_complete", "passed": all(boolish(row["passed"]) for row in classification_audit)},
        {"decision": "evidence_fields_complete", "passed": all(boolish(row["passed"]) for row in evidence_audit)},
        {"decision": "safety_boundaries_preserved", "passed": all(boolish(row["passed"]) for row in safety_audit)},
        {"decision": "recommend_implementation_next", "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(boolish(row["passed"]) for row in predecessor_audit)},
        {"check": "feature_plan", "passed": all(boolish(row["passed"]) for row in feature_audit)},
        {"check": "source_targets", "passed": all(boolish(row["passed"]) for row in source_audit)},
        {"check": "classification_contract", "passed": all(boolish(row["passed"]) for row in classification_audit)},
        {"check": "evidence_fields", "passed": all(boolish(row["passed"]) for row in evidence_audit)},
        {"check": "safety_boundaries", "passed": all(boolish(row["passed"]) for row in safety_audit)},
        {"check": "decision", "passed": all(boolish(row["passed"]) for row in decision_rows)},
    ]

    all_checks_passed = all(boolish(row["passed"]) for row in checks)

    recommended_rows = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": RECOMMENDED_PATH,
            "reason": "Inventory plan is valid; next layer may inspect local source files and classify each feature's injection status without wiring or tuning.",
            "passed": True,
        }
    ]

    write_csv(CHECKS_CSV, checks)
    write_csv(PREDECESSOR_CSV, predecessor_audit)
    write_csv(FEATURE_AUDIT_CSV, feature_audit)
    write_csv(SOURCE_TARGET_AUDIT_CSV, source_audit)
    write_csv(CLASSIFICATION_AUDIT_CSV, classification_audit)
    write_csv(EVIDENCE_AUDIT_CSV, evidence_audit)
    write_csv(SAFETY_AUDIT_CSV, safety_audit)
    write_csv(DECISION_CSV, decision_rows)
    write_csv(RECOMMENDED_CSV, recommended_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_injection_inventory_audit_complete" if all_checks_passed else "layer_6_model_projection_realism_injection_inventory_audit_failed",
        "all_checks_passed": all_checks_passed,
        "predecessor_layer": "6NV",
        "predecessor_diagnosis": predecessor_json.get("diagnosis"),
        "feature_families_audited": len(feature_audit),
        "source_targets_audited": len(source_rows),
        "classification_contract_count": len(classification_audit),
        "evidence_field_count": len(evidence_audit),
        "source_inspection_allowed_next": True if all_checks_passed else False,
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
            str(SOURCE_TARGET_AUDIT_CSV),
            str(CLASSIFICATION_AUDIT_CSV),
            str(EVIDENCE_AUDIT_CSV),
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
