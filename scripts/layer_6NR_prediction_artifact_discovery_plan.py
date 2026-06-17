#!/usr/bin/env python3
"""
Layer 6NR — Prediction Artifact Discovery Plan

Planning-only layer.

This script inventories candidate prediction artifact locations and emits
evidence describing whether prediction join execution can proceed later.

Non-goals:
- no joins
- no prediction accuracy metrics
- no calibration metrics
- no ROI metrics
- no pricing
- no edge detection
- no tuning
- no activation
- no production writes
- no live fetches
- no remote APIs
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List


LAYER_ID = "6NR"
LAYER_NAME = "layer_6_prediction_artifact_discovery_plan"

ACTUALS_PATH = Path("data/local/historical_actuals.csv")

CANDIDATE_PREDICTION_PATHS = [
    "data/local/predictions.csv",
    "data/local/historical_predictions.csv",
    "data/local/model_predictions.csv",
    "tmp/predictions.csv",
    "tmp/historical_predictions.csv",
    "tmp/model_predictions.csv",
]

REQUIRED_PREDICTION_COLUMNS = [
    "game_pk",
    "predicted_home_win_probability",
    "prediction_timestamp",
    "model_version",
]

OPTIONAL_DOCUMENTED_COLUMNS = [
    "predicted_away_win_probability",
    "predicted_home_score_mean",
    "predicted_away_score_mean",
    "predicted_total_runs_mean",
    "simulation_run_id",
    "source_artifact",
]

OUT_DIR = Path("tmp/layer_6NR_prediction_artifact_discovery_plan")
DISCOVERY_CSV = OUT_DIR / "prediction_artifact_inventory.csv"
REQUIREMENTS_CSV = OUT_DIR / "prediction_artifact_requirements.csv"
DIAGNOSIS_JSON = OUT_DIR / "diagnosis.json"


def read_header(path: Path) -> List[str]:
    if not path.exists() or not path.is_file():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return []


def count_rows(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return sum(1 for _ in reader)


def bool_text(value: bool) -> str:
    return "true" if value else "false"


def inventory_candidate(path_text: str) -> Dict[str, str]:
    path = Path(path_text)
    exists = path.exists() and path.is_file()
    header = read_header(path)
    missing_required = [c for c in REQUIRED_PREDICTION_COLUMNS if c not in header]
    row_count = count_rows(path) if exists else 0

    schema_eligible = exists and row_count > 0 and not missing_required

    return {
        "layer_id": LAYER_ID,
        "candidate_path": path_text,
        "exists": bool_text(exists),
        "row_count": str(row_count),
        "columns": "|".join(header),
        "required_columns": "|".join(REQUIRED_PREDICTION_COLUMNS),
        "missing_required_columns": "|".join(missing_required),
        "schema_eligible": bool_text(schema_eligible),
        "join_execution_allowed": "false",
        "reason": (
            "candidate_schema_complete_but_join_execution_deferred_to_future_layer"
            if schema_eligible
            else "missing_or_schema_ineligible_prediction_artifact"
        ),
    }


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    actuals_exists = ACTUALS_PATH.exists() and ACTUALS_PATH.is_file()
    actuals_row_count = count_rows(ACTUALS_PATH) if actuals_exists else 0

    inventory_rows = [inventory_candidate(p) for p in CANDIDATE_PREDICTION_PATHS]
    write_csv(DISCOVERY_CSV, inventory_rows)

    requirement_rows = [
        {
            "layer_id": LAYER_ID,
            "requirement_category": "required_schema",
            "requirement": ",".join(REQUIRED_PREDICTION_COLUMNS),
            "status": "documented",
        },
        {
            "layer_id": LAYER_ID,
            "requirement_category": "provenance",
            "requirement": "prediction_timestamp and model_version must be present; source artifact or simulation_run_id should be documented when available",
            "status": "documented",
        },
        {
            "layer_id": LAYER_ID,
            "requirement_category": "join_eligibility",
            "requirement": "candidate must contain game_pk and one row per predicted game before future join execution",
            "status": "documented",
        },
        {
            "layer_id": LAYER_ID,
            "requirement_category": "validation_rules",
            "requirement": "home win probability must be numeric and bounded [0,1]; game_pk non-null; timestamp/model_version non-null; duplicates reviewed",
            "status": "documented",
        },
        {
            "layer_id": LAYER_ID,
            "requirement_category": "reconstruction_pathway",
            "requirement": "if no usable artifact exists, create a future reconstruction layer that emits historical prediction rows without joining or scoring them",
            "status": "documented",
        },
        {
            "layer_id": LAYER_ID,
            "requirement_category": "safety_boundary",
            "requirement": "actuals-only state is preserved; prediction joins and accuracy metrics remain blocked",
            "status": "documented",
        },
    ]
    write_csv(REQUIREMENTS_CSV, requirement_rows)

    eligible_candidates = [r for r in inventory_rows if r["schema_eligible"] == "true"]

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_prediction_artifact_discovery_plan_complete",
        "actuals_path": str(ACTUALS_PATH),
        "actuals_exists": actuals_exists,
        "actuals_row_count": actuals_row_count,
        "candidate_prediction_artifact_count": len(CANDIDATE_PREDICTION_PATHS),
        "schema_eligible_prediction_artifact_count": len(eligible_candidates),
        "prediction_artifact_available": bool(eligible_candidates),
        "prediction_join_execution_allowed_next": False,
        "prediction_accuracy_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "required_prediction_schema": REQUIRED_PREDICTION_COLUMNS,
        "optional_documented_prediction_fields": OPTIONAL_DOCUMENTED_COLUMNS,
        "generated_csv_artifacts": [
            str(DISCOVERY_CSV),
            str(REQUIREMENTS_CSV),
        ],
        "generated_json_artifacts": [
            str(DIAGNOSIS_JSON),
        ],
        "recommended_next_layer": "6NS_layer6_prediction_artifact_discovery_audit",
        "recommended_path": (
            "audit_prediction_artifact_discovery_plan_before_any_join_execution"
        ),
    }

    with DIAGNOSIS_JSON.open("w", encoding="utf-8") as f:
        json.dump(diagnosis, f, indent=2, sort_keys=True)
        f.write("\n")

    print(json.dumps(diagnosis, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
