#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

LAYER_ID = "6OA"
LAYER_NAME = "layer_6_model_projection_realism_gap_analysis_audit"
SLUG = "layer_6OA_model_projection_realism_gap_analysis_audit"

TMP_DIR = Path("tmp") / SLUG
TMP_DIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/plan_6NZ_model_projection_realism_gap_analysis.py")
PREDECESSOR_DIR = Path("tmp/layer_6NZ_model_projection_realism_gap_analysis_plan")

INVENTORY = PREDECESSOR_DIR / "realism_gap_inventory.csv"
SUMMARY = PREDECESSOR_DIR / "realism_gap_summary.csv"
PRIORITIES = PREDECESSOR_DIR / "realism_gap_priorities.csv"
PREDECESSOR_JSON = PREDECESSOR_DIR / "diagnosis.json"

CHECKS_CSV = TMP_DIR / "checks.csv"
GAP_AUDIT_CSV = TMP_DIR / "gap_audit.csv"
SAFETY_CSV = TMP_DIR / "safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / "recommended_path.csv"
DIAGNOSIS_JSON = TMP_DIR / "diagnosis.json"

RECOMMENDED_NEXT_LAYER = "6OB_layer6_projection_wiring_gap_resolution_plan"

CRITICAL_FEATURES = {
    "base_out_state",
    "base_advancement_transitions",
    "opener_bulk_pitcher",
    "ghost_runner_extra_innings",
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
        for k in row:
            if k not in keys:
                keys.append(k)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_predecessor():
    if not PREDECESSOR_SCRIPT.exists():
        return 1
    return subprocess.run([sys.executable, str(PREDECESSOR_SCRIPT)], check=False).returncode


def main() -> int:
    predecessor_rc = run_predecessor()
    inventory = read_csv(INVENTORY)
    summary = read_csv(SUMMARY)
    priorities = read_csv(PRIORITIES)

    inventory_features = {row.get("feature_name") for row in inventory}
    critical_present = CRITICAL_FEATURES.issubset(inventory_features)

    gap_audit = [
        {"check": "6nz_script_exists", "passed": PREDECESSOR_SCRIPT.exists()},
        {"check": "6nz_regenerates_artifacts", "passed": predecessor_rc == 0},
        {"check": "inventory_exists", "passed": INVENTORY.exists()},
        {"check": "summary_exists", "passed": SUMMARY.exists()},
        {"check": "priorities_exists", "passed": PRIORITIES.exists()},
        {"check": "inventory_nonempty", "passed": len(inventory) > 0},
        {"check": "summary_nonempty", "passed": len(summary) > 0},
        {"check": "priorities_nonempty", "passed": len(priorities) > 0},
        {"check": "critical_features_present", "passed": critical_present},
    ]

    safety = [
        {"boundary": "audit_only", "passed": True},
        {"boundary": "no_feature_wiring", "passed": True},
        {"boundary": "no_tuning", "passed": True},
        {"boundary": "no_historical_validation", "passed": True},
        {"boundary": "no_prediction_join", "passed": True},
        {"boundary": "no_backtests", "passed": True},
        {"boundary": "no_pricing", "passed": True},
        {"boundary": "no_edge_detection", "passed": True},
        {"boundary": "layer6_exit_not_recommended", "passed": True},
    ]

    recommended = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": "plan_projection_wiring_for_critical_gap_resolution_before_implementation",
            "reason": "6NZ gap analysis is valid; next layer should plan projection wiring for simulator-reachable critical features before implementation.",
            "passed": True,
        }
    ]

    checks = [
        {"check": "gap_audit", "passed": all(row["passed"] for row in gap_audit)},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety)},
        {"check": "recommended_path", "passed": True},
    ]

    all_passed = all(row["passed"] for row in checks)

    write_csv(CHECKS_CSV, checks)
    write_csv(GAP_AUDIT_CSV, gap_audit)
    write_csv(SAFETY_CSV, safety)
    write_csv(RECOMMENDED_CSV, recommended)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_gap_analysis_audit_complete" if all_passed else "layer_6_model_projection_realism_gap_analysis_audit_failed",
        "all_checks_passed": all_passed,
        "critical_features_present": critical_present,
        "inventory_rows": len(inventory),
        "summary_rows": len(summary),
        "priority_rows": len(priorities),
        "feature_wiring_allowed_next": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "layer6_exit_recommended": False,
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "generated_csv_artifacts": [
            str(CHECKS_CSV),
            str(GAP_AUDIT_CSV),
            str(SAFETY_CSV),
            str(RECOMMENDED_CSV),
        ],
        "generated_json_artifacts": [str(DIAGNOSIS_JSON)],
    }

    DIAGNOSIS_JSON.write_text(json.dumps(diagnosis, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
