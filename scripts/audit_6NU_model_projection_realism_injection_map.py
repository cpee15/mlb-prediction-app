#!/usr/bin/env python3
"""Layer 6NU — Model Projection Realism Injection Map Audit."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


LAYER_ID = "6NU"
LAYER_NAME = "layer6_model_projection_realism_injection_map_audit"
SLUG = "layer_6NU_model_projection_realism_injection_map_audit"

TMP_DIR = Path("tmp") / SLUG
TMP_DIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/plan_6NT_model_projection_realism_injection_map.py")
PREDECESSOR_DIR = Path("tmp/layer_6NT_model_projection_realism_injection_map_plan")
PREDECESSOR_JSON = PREDECESSOR_DIR / "diagnosis.json"
FEATURE_SCOPE_CSV = PREDECESSOR_DIR / "realism_feature_scope.csv"
INJECTION_PATH_CSV = PREDECESSOR_DIR / "injection_path_contract.csv"
BASE_ADVANCEMENT_CSV = PREDECESSOR_DIR / "base_advancement_transition_scope.csv"
OPENER_BULK_CSV = PREDECESSOR_DIR / "opener_bulk_pitcher_scope.csv"
SAFETY_CSV_IN = PREDECESSOR_DIR / "safety_boundaries.csv"

CHECKS_CSV = TMP_DIR / "checks.csv"
PREDECESSOR_CSV = TMP_DIR / "predecessor.csv"
FEATURE_AUDIT_CSV = TMP_DIR / "feature_scope_audit.csv"
INJECTION_AUDIT_CSV = TMP_DIR / "injection_path_audit.csv"
BASE_ADVANCEMENT_AUDIT_CSV = TMP_DIR / "base_advancement_audit.csv"
OPENER_BULK_AUDIT_CSV = TMP_DIR / "opener_bulk_audit.csv"
SAFETY_AUDIT_CSV = TMP_DIR / "safety_audit.csv"
DECISION_CSV = TMP_DIR / "decision.csv"
RECOMMENDED_CSV = TMP_DIR / "recommended_path.csv"
DIAGNOSIS_JSON = TMP_DIR / "diagnosis.json"

EXPECTED_6NT_DIAGNOSIS = "layer_6_model_projection_realism_injection_map_plan_complete"
RECOMMENDED_NEXT_LAYER = "6NV_layer6_model_projection_realism_injection_inventory_plan"
RECOMMENDED_PATH = "plan_source_inventory_for_realism_feature_injection_status_before_wiring"


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

REQUIRED_INJECTION_STEPS = [
    "realism_feature_logic",
    "simulation_state_mutation",
    "game_simulation_output",
    "model_projection_builder",
    "api_payload_or_workspace",
    "model_projections_ui_display",
]

REQUIRED_SAFETY_BOUNDARIES = [
    "planning_only",
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
    feature_rows = read_csv(FEATURE_SCOPE_CSV)
    injection_rows = read_csv(INJECTION_PATH_CSV)
    base_rows = read_csv(BASE_ADVANCEMENT_CSV)
    opener_rows = read_csv(OPENER_BULK_CSV)
    safety_rows_in = read_csv(SAFETY_CSV_IN)

    feature_names = {row.get("feature_family") for row in feature_rows}
    injection_names = {row.get("name") for row in injection_rows}
    safety_names = {row.get("boundary") for row in safety_rows_in}

    predecessor_rows = [
        {
            "check": "6nt_script_exists",
            "expected": True,
            "actual": PREDECESSOR_SCRIPT.exists(),
            "passed": PREDECESSOR_SCRIPT.exists(),
        },
        {
            "check": "6nt_script_regenerates_artifacts",
            "expected": 0,
            "actual": predecessor_run["returncode"],
            "passed": predecessor_run["returncode"] == 0,
        },
        {
            "check": "6nt_diagnosis",
            "expected": EXPECTED_6NT_DIAGNOSIS,
            "actual": predecessor_json.get("diagnosis"),
            "passed": predecessor_json.get("diagnosis") == EXPECTED_6NT_DIAGNOSIS,
        },
        {
            "check": "6nt_all_checks_passed",
            "expected": True,
            "actual": predecessor_json.get("all_checks_passed"),
            "passed": predecessor_json.get("all_checks_passed") is True,
        },
    ]

    feature_audit_rows = [
        {
            "required_feature": feature,
            "present": feature in feature_names,
            "passed": feature in feature_names,
        }
        for feature in REQUIRED_FEATURES
    ]

    injection_audit_rows = [
        {
            "required_step": step,
            "present": step in injection_names,
            "passed": step in injection_names,
        }
        for step in REQUIRED_INJECTION_STEPS
    ]

    base_audit_rows = [
        {
            "check": "base_advancement_cases_count",
            "expected": ">=8",
            "actual": len(base_rows),
            "passed": len(base_rows) >= 8,
        },
        {
            "check": "all_base_advancement_cases_probabilistic",
            "expected": True,
            "actual": all(boolish(row.get("probabilistic_required")) for row in base_rows),
            "passed": all(boolish(row.get("probabilistic_required")) for row in base_rows),
        },
        {
            "check": "single_runner_on_first_included",
            "expected": True,
            "actual": any(row.get("event") == "single" and row.get("start_state") == "runner_on_1st" for row in base_rows),
            "passed": any(row.get("event") == "single" and row.get("start_state") == "runner_on_1st" for row in base_rows),
        },
        {
            "check": "double_runner_on_first_included",
            "expected": True,
            "actual": any(row.get("event") == "double" and row.get("start_state") == "runner_on_1st" for row in base_rows),
            "passed": any(row.get("event") == "double" and row.get("start_state") == "runner_on_1st" for row in base_rows),
        },
        {
            "check": "ground_ball_double_play_context_included",
            "expected": True,
            "actual": any(row.get("event") == "ground_ball" for row in base_rows),
            "passed": any(row.get("event") == "ground_ball" for row in base_rows),
        },
    ]

    opener_audit_rows = [
        {
            "check": "opener_bulk_cases_count",
            "expected": ">=5",
            "actual": len(opener_rows),
            "passed": len(opener_rows) >= 5,
        },
        {
            "check": "opener_detection_included",
            "expected": True,
            "actual": any(row.get("component") == "opener_detection" for row in opener_rows),
            "passed": any(row.get("component") == "opener_detection" for row in opener_rows),
        },
        {
            "check": "opener_length_distribution_included",
            "expected": True,
            "actual": any(row.get("component") == "opener_length_distribution" for row in opener_rows),
            "passed": any(row.get("component") == "opener_length_distribution" for row in opener_rows),
        },
        {
            "check": "bulk_pitcher_entry_included",
            "expected": True,
            "actual": any(row.get("component") == "bulk_pitcher_entry" for row in opener_rows),
            "passed": any(row.get("component") == "bulk_pitcher_entry" for row in opener_rows),
        },
        {
            "check": "bulk_pitcher_length_distribution_included",
            "expected": True,
            "actual": any(row.get("component") == "bulk_pitcher_length_distribution" for row in opener_rows),
            "passed": any(row.get("component") == "bulk_pitcher_length_distribution" for row in opener_rows),
        },
    ]

    safety_audit_rows = [
        {
            "required_boundary": boundary,
            "present": boundary in safety_names,
            "passed": boundary in safety_names,
        }
        for boundary in REQUIRED_SAFETY_BOUNDARIES
    ]

    decision_rows = [
        {
            "decision": "predecessor_valid",
            "expected": True,
            "actual": all(boolish(row["passed"]) for row in predecessor_rows),
            "passed": all(boolish(row["passed"]) for row in predecessor_rows),
        },
        {
            "decision": "feature_scope_complete",
            "expected": True,
            "actual": all(boolish(row["passed"]) for row in feature_audit_rows),
            "passed": all(boolish(row["passed"]) for row in feature_audit_rows),
        },
        {
            "decision": "injection_path_contract_complete",
            "expected": True,
            "actual": all(boolish(row["passed"]) for row in injection_audit_rows),
            "passed": all(boolish(row["passed"]) for row in injection_audit_rows),
        },
        {
            "decision": "base_advancement_scope_complete",
            "expected": True,
            "actual": all(boolish(row["passed"]) for row in base_audit_rows),
            "passed": all(boolish(row["passed"]) for row in base_audit_rows),
        },
        {
            "decision": "opener_bulk_scope_complete",
            "expected": True,
            "actual": all(boolish(row["passed"]) for row in opener_audit_rows),
            "passed": all(boolish(row["passed"]) for row in opener_audit_rows),
        },
        {
            "decision": "safety_boundaries_preserved",
            "expected": True,
            "actual": all(boolish(row["passed"]) for row in safety_audit_rows),
            "passed": all(boolish(row["passed"]) for row in safety_audit_rows),
        },
        {
            "decision": "recommend_next_inventory_plan",
            "expected": RECOMMENDED_NEXT_LAYER,
            "actual": RECOMMENDED_NEXT_LAYER,
            "passed": True,
        },
    ]

    checks = [
        {"check": "predecessor", "passed": all(boolish(row["passed"]) for row in predecessor_rows)},
        {"check": "feature_scope", "passed": all(boolish(row["passed"]) for row in feature_audit_rows)},
        {"check": "injection_path", "passed": all(boolish(row["passed"]) for row in injection_audit_rows)},
        {"check": "base_advancement", "passed": all(boolish(row["passed"]) for row in base_audit_rows)},
        {"check": "opener_bulk", "passed": all(boolish(row["passed"]) for row in opener_audit_rows)},
        {"check": "safety", "passed": all(boolish(row["passed"]) for row in safety_audit_rows)},
        {"check": "decision", "passed": all(boolish(row["passed"]) for row in decision_rows)},
    ]

    all_checks_passed = all(boolish(row["passed"]) for row in checks)

    recommended_rows = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": RECOMMENDED_PATH,
            "reason": "The 6NT planning map is valid; next layer should plan source inventory to classify each realism feature as absent, present, sim-reachable, projection-reachable, ui-visible, or active before wiring.",
            "passed": True,
        }
    ]

    write_csv(CHECKS_CSV, checks)
    write_csv(PREDECESSOR_CSV, predecessor_rows)
    write_csv(FEATURE_AUDIT_CSV, feature_audit_rows)
    write_csv(INJECTION_AUDIT_CSV, injection_audit_rows)
    write_csv(BASE_ADVANCEMENT_AUDIT_CSV, base_audit_rows)
    write_csv(OPENER_BULK_AUDIT_CSV, opener_audit_rows)
    write_csv(SAFETY_AUDIT_CSV, safety_audit_rows)
    write_csv(DECISION_CSV, decision_rows)
    write_csv(RECOMMENDED_CSV, recommended_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_injection_map_audit_complete" if all_checks_passed else "layer_6_model_projection_realism_injection_map_audit_failed",
        "all_checks_passed": all_checks_passed,
        "predecessor_layer": "6NT",
        "predecessor_diagnosis": predecessor_json.get("diagnosis"),
        "feature_scope_complete": all(boolish(row["passed"]) for row in feature_audit_rows),
        "feature_families_audited": len(feature_audit_rows),
        "opener_bulk_pitcher_confirmed_in_scope": any(row.get("required_feature") == "opener_bulk_pitcher" and boolish(row.get("passed")) for row in feature_audit_rows),
        "base_advancement_transitions_confirmed_in_scope": any(row.get("required_feature") == "base_advancement_transitions" and boolish(row.get("passed")) for row in feature_audit_rows),
        "base_advancement_transition_cases_audited": len(base_rows),
        "injection_path_contract_complete": all(boolish(row["passed"]) for row in injection_audit_rows),
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
            str(INJECTION_AUDIT_CSV),
            str(BASE_ADVANCEMENT_AUDIT_CSV),
            str(OPENER_BULK_AUDIT_CSV),
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
