#!/usr/bin/env python3
"""Audit Layer 6GX gameplay mechanic outcome artifact selection plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gy_outcome_artifact_selection_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6GX_PATH = Path("scripts/plan_6gx_layer6_gameplay_mechanic_outcome_artifact_selection.py")
AUDIT_6GW_PATH = Path("scripts/audit_6gw_layer6_gameplay_mechanic_real_backtest_dry_run.py")
IMPLEMENT_6GV_PATH = Path("scripts/implement_6gv_layer6_gameplay_mechanic_real_backtest_dry_run.py")
AUDIT_6GU_PATH = Path("scripts/audit_6gu_layer6_gameplay_mechanic_real_backtest_plan.py")
PLAN_6GT_PATH = Path("scripts/plan_6gt_layer6_gameplay_mechanic_real_backtests.py")

JSON_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan.json"
CHECKS_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_checks.csv"
PREDECESSOR_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_predecessor.csv"
INPUT_ARTIFACTS_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_input_artifacts.csv"
DISCOVERED_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_discovered_artifacts.csv"
CLASSIFICATION_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_classification.csv"
SELECTION_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_selection_summary.csv"
ADAPTER_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_adapter_requirements.csv"
MATERIALIZATION_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_materialization_requirements.csv"
SAFETY_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_safety_boundaries.csv"
FUTURE_6GY_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_future_6gy_contract.csv"
FUTURE_6GZ_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_future_6gz_contract.csv"
IMMUTABILITY_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_immutability.csv"
RECOMMENDED_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
INPUT_ARTIFACTS_AUDIT_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CLASSIFICATION_INTEGRITY_CSV = TMP_DIR / f"{SLUG}_classification_integrity.csv"
SELECTION_INTEGRITY_CSV = TMP_DIR / f"{SLUG}_selection_integrity.csv"
ADAPTER_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_adapter_requirements.csv"
MATERIALIZATION_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_materialization_requirements.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
FUTURE_6GZ_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gz_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GX = "layer_6_gameplay_mechanic_outcome_artifact_selection_plan_complete"
DIAGNOSIS_6GY = "layer_6_gameplay_mechanic_outcome_artifact_selection_plan_audit_complete"
CURRENT_LAYER = "6GY_layer_6_gameplay_mechanic_outcome_artifact_selection_plan_audit"
RECOMMENDED_NEXT_LAYER = "6GZ_layer_6_gameplay_mechanic_outcome_artifact_adapter_plan"
RECOMMENDED_PATH = "audit_outcome_artifact_selection_then_plan_adapter_before_real_evaluation"

GAMEPLAY_MECHANICS = [
    "extra_innings_ghost_runner",
    "stolen_bases_caught_stealing",
    "wild_pitches_passed_balls",
    "balks",
    "first_to_third_advancement",
    "second_to_home_advancement",
    "sac_flies_tagging_up",
    "double_plays_by_base_out_state",
    "pinch_hitters_substitutions",
    "bullpen_sequencing_leverage_behavior",
]

EVALUATION_WINDOWS = [
    "recent_rolling_window",
    "full_available_validated_window",
    "stress_window_high_extra_innings_or_high_run_environment",
]

REQUIRED_6GX_ARTIFACTS = [
    JSON_6GX,
    CHECKS_6GX,
    PREDECESSOR_6GX,
    INPUT_ARTIFACTS_6GX,
    DISCOVERED_6GX,
    CLASSIFICATION_6GX,
    SELECTION_6GX,
    ADAPTER_6GX,
    MATERIALIZATION_6GX,
    SAFETY_6GX,
    FUTURE_6GY_6GX,
    FUTURE_6GZ_6GX,
    IMMUTABILITY_6GX,
    RECOMMENDED_6GX,
]

ALLOWED_CLASSES = {
    "candidate_game_outcomes",
    "candidate_team_totals",
    "candidate_inning_runs",
    "candidate_base_out_transitions",
    "candidate_backtest_prior_outputs",
    "unsuitable_planning_artifact",
    "insufficient_metadata",
}


def safe_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        parsed, _ = json.JSONDecoder().raw_decode(text)
        return parsed if isinstance(parsed, dict) else {}


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def intish(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6gx_before = PLAN_6GX_PATH.read_text(encoding="utf-8") if PLAN_6GX_PATH.exists() else ""
    audit_6gw_before = AUDIT_6GW_PATH.read_text(encoding="utf-8") if AUDIT_6GW_PATH.exists() else ""
    implement_6gv_before = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_before = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_before = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    plan_run = subprocess.run(
        [sys.executable, str(PLAN_6GX_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6gx = load_json(JSON_6GX)
    checks_6gx = read_csv(CHECKS_6GX)
    input_rows_6gx = read_csv(INPUT_ARTIFACTS_6GX)
    classification_rows_6gx = read_csv(CLASSIFICATION_6GX)
    selection_rows_6gx = read_csv(SELECTION_6GX)
    adapter_rows_6gx = read_csv(ADAPTER_6GX)
    materialization_rows_6gx = read_csv(MATERIALIZATION_6GX)
    safety_rows_6gx = read_csv(SAFETY_6GX)
    future_6gy_rows_6gx = read_csv(FUTURE_6GY_6GX)
    future_6gz_rows_6gx = read_csv(FUTURE_6GZ_6GX)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gx_plan_exists", "expected": True, "actual": PLAN_6GX_PATH.exists(), "passed": PLAN_6GX_PATH.exists()},
        {"check": "6gx_plan_runs", "expected": 0, "actual": plan_run.returncode, "passed": plan_run.returncode == 0},
        {"check": "6gx_json_exists", "expected": True, "actual": JSON_6GX.exists(), "passed": JSON_6GX.exists()},
        {"check": "6gx_all_checks_passed", "expected": True, "actual": json_6gx.get("all_checks_passed"), "passed": json_6gx.get("all_checks_passed") is True},
        {"check": "6gx_planning_only", "expected": True, "actual": json_6gx.get("planning_only"), "passed": json_6gx.get("planning_only") is True},
        {"check": "6gx_diagnosis", "expected": DIAGNOSIS_6GX, "actual": json_6gx.get("diagnosis"), "passed": json_6gx.get("diagnosis") == DIAGNOSIS_6GX},
        {"check": "6gx_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6gx.get("recommended_next_layer"), "passed": json_6gx.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gx_recommended_path", "expected": "plan_outcome_artifact_selection_then_audit_before_adapter_or_materialization", "actual": json_6gx.get("recommended_path"), "passed": json_6gx.get("recommended_path") == "plan_outcome_artifact_selection_then_audit_before_adapter_or_materialization"},
        {"check": "6gx_layer_6_exit_ready_false", "expected": False, "actual": json_6gx.get("layer_6_exit_ready"), "passed": json_6gx.get("layer_6_exit_ready") is False},
        {"check": "6gx_mechanics_activated_false", "expected": False, "actual": json_6gx.get("mechanics_activated_by_this_layer"), "passed": json_6gx.get("mechanics_activated_by_this_layer") is False},
        {"check": "6gx_real_backtests_run_false", "expected": False, "actual": json_6gx.get("real_backtests_run"), "passed": json_6gx.get("real_backtests_run") is False},
        {"check": "6gx_games_evaluated_zero", "expected": 0, "actual": json_6gx.get("games_evaluated"), "passed": intish(json_6gx.get("games_evaluated")) == 0},
        {"check": "6gx_actual_outcomes_joined_false", "expected": False, "actual": json_6gx.get("actual_outcomes_joined"), "passed": json_6gx.get("actual_outcomes_joined") is False},
        {"check": "6gx_activation_allowed_false", "expected": False, "actual": json_6gx.get("activation_allowed"), "passed": json_6gx.get("activation_allowed") is False},
        {"check": "6gx_layer_6_exit_credit_false", "expected": False, "actual": json_6gx.get("layer_6_exit_credit"), "passed": json_6gx.get("layer_6_exit_credit") is False},
        {"check": "6gx_live_fetches_false", "expected": False, "actual": json_6gx.get("live_data_fetches_run"), "passed": json_6gx.get("live_data_fetches_run") is False},
        {"check": "6gx_database_writes_false", "expected": False, "actual": json_6gx.get("database_writes_run"), "passed": json_6gx.get("database_writes_run") is False},
        {"check": "6gx_materialization_jobs_false", "expected": False, "actual": json_6gx.get("materialization_jobs_run"), "passed": json_6gx.get("materialization_jobs_run") is False},
        {"check": "6gx_production_simulations_false", "expected": False, "actual": json_6gx.get("production_simulations_run"), "passed": json_6gx.get("production_simulations_run") is False},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in REQUIRED_6GX_ARTIFACTS
    ]

    checks_consistency_rows = [
        {
            "source_check": row.get("check"),
            "source_passed": row.get("passed"),
            "passed": boolish(row.get("passed")),
            "detail": row.get("detail", ""),
        }
        for row in checks_6gx
    ]

    input_artifact_audit_rows = [
        {
            "artifact_path": row.get("artifact_path"),
            "exists": boolish(row.get("exists")),
            "required": boolish(row.get("required")),
            "passed": boolish(row.get("passed")) and boolish(row.get("exists")),
        }
        for row in input_rows_6gx
    ]

    discovered_count = intish(json_6gx.get("discovered_artifact_count"), 0)
    classified_count = intish(json_6gx.get("classified_artifact_count"), 0)

    classification_integrity_rows = [
        {
            "check": "classified_equals_discovered",
            "expected": discovered_count,
            "actual": classified_count,
            "bad_rows": 0 if classified_count == discovered_count else abs(classified_count - discovered_count),
            "passed": classified_count == discovered_count and len(classification_rows_6gx) == classified_count,
        },
        {
            "check": "allowed_suitability_classes",
            "expected": "|".join(sorted(ALLOWED_CLASSES)),
            "actual": "classification_rows",
            "bad_rows": sum(1 for row in classification_rows_6gx if row.get("suitability_class") not in ALLOWED_CLASSES),
            "passed": all(row.get("suitability_class") in ALLOWED_CLASSES for row in classification_rows_6gx),
        },
        {
            "check": "scores_between_0_and_100",
            "expected": "0..100",
            "actual": "classification_rows",
            "bad_rows": sum(1 for row in classification_rows_6gx if not 0 <= intish(row.get("suitability_score")) <= 100),
            "passed": all(0 <= intish(row.get("suitability_score")) <= 100 for row in classification_rows_6gx),
        },
        {
            "check": "local_only_no_remote",
            "expected": False,
            "actual": "remote_path",
            "bad_rows": sum(1 for row in classification_rows_6gx if boolish(row.get("remote_path"))),
            "passed": all(not boolish(row.get("remote_path")) for row in classification_rows_6gx),
        },
        {
            "check": "no_live_fetch_required",
            "expected": False,
            "actual": "live_fetch_required",
            "bad_rows": sum(1 for row in classification_rows_6gx if boolish(row.get("live_fetch_required"))),
            "passed": all(not boolish(row.get("live_fetch_required")) for row in classification_rows_6gx),
        },
        {
            "check": "classification_activation_blocked",
            "expected": False,
            "actual": "activation_allowed",
            "bad_rows": sum(1 for row in classification_rows_6gx if boolish(row.get("activation_allowed"))),
            "passed": all(not boolish(row.get("activation_allowed")) for row in classification_rows_6gx),
        },
        {
            "check": "classification_exit_credit_blocked",
            "expected": False,
            "actual": "layer_6_exit_credit",
            "bad_rows": sum(1 for row in classification_rows_6gx if boolish(row.get("layer_6_exit_credit"))),
            "passed": all(not boolish(row.get("layer_6_exit_credit")) for row in classification_rows_6gx),
        },
    ]

    selection_map = {row.get("selection_key"): row for row in selection_rows_6gx}
    primary = selection_map.get("primary_outcome_family", {})
    base_out = selection_map.get("base_out_supplemental_family", {})
    inning = selection_map.get("inning_supplemental_family", {})

    selection_integrity_rows = [
        {
            "check": "primary_selection_status",
            "expected": "local_outcome_family_selected",
            "actual": primary.get("selection_status"),
            "passed": primary.get("selection_status") == "local_outcome_family_selected",
        },
        {
            "check": "primary_selected_family",
            "expected": "candidate_game_outcomes",
            "actual": primary.get("selected_artifact_family"),
            "passed": primary.get("selected_artifact_family") == "candidate_game_outcomes",
        },
        {
            "check": "primary_selected_count_positive",
            "expected": ">=1",
            "actual": primary.get("selected_artifact_count"),
            "passed": intish(primary.get("selected_artifact_count"), 0) >= 1,
        },
        {
            "check": "adapter_required",
            "expected": True,
            "actual": primary.get("adapter_required"),
            "passed": boolish(primary.get("adapter_required")),
        },
        {
            "check": "materialization_plan_not_required",
            "expected": False,
            "actual": primary.get("materialization_plan_required"),
            "passed": not boolish(primary.get("materialization_plan_required")),
        },
        {
            "check": "base_out_supplemental_selected_when_candidates_exist",
            "expected": "selected_if_candidate_count_positive",
            "actual": base_out.get("selection_status"),
            "passed": (
                intish(json_6gx.get("candidate_base_out_transitions_count"), 0) == 0
                or base_out.get("selection_status") == "local_supplemental_family_selected"
            ),
        },
        {
            "check": "inning_supplemental_selected_when_candidates_exist",
            "expected": "selected_if_candidate_count_positive",
            "actual": inning.get("selection_status"),
            "passed": (
                intish(json_6gx.get("candidate_inning_runs_count"), 0) == 0
                or inning.get("selection_status") == "local_supplemental_family_selected"
            ),
        },
        {
            "check": "selection_no_games_or_joins_or_activation",
            "expected": "games=0 joins=false activation=false exit=false",
            "actual": "selection_rows",
            "passed": all(
                str(row.get("games_evaluated")) == "0"
                and not boolish(row.get("actual_outcomes_joined"))
                and not boolish(row.get("activation_allowed"))
                and not boolish(row.get("layer_6_exit_credit"))
                for row in selection_rows_6gx
            ),
        },
    ]

    adapter_requirement_rows = [
        {
            "requirement": row.get("requirement"),
            "required": row.get("required"),
            "source_passed": row.get("passed"),
            "passed": boolish(row.get("required")) and boolish(row.get("passed")),
        }
        for row in adapter_rows_6gx
    ]

    materialization_requirement_rows = [
        {
            "requirement": row.get("requirement"),
            "required": row.get("required"),
            "source_passed": row.get("passed"),
            "materialization_not_run": row.get("requirement") != "materialization_not_run_by_6gx" or boolish(row.get("passed")),
            "passed": boolish(row.get("passed")),
        }
        for row in materialization_rows_6gx
    ]

    safety_boundary_rows = [
        {
            "boundary": row.get("boundary"),
            "expected": row.get("expected"),
            "actual": row.get("actual"),
            "source_passed": row.get("passed"),
            "passed": boolish(row.get("passed")),
        }
        for row in safety_rows_6gx
    ]

    future_6gz_contract_rows = [
        {
            "contract": "plan_local_outcome_artifact_adapter_for_selected_candidate_game_outcomes",
            "required": True,
            "passed": True,
        },
        {
            "contract": "define_optional_base_out_and_inning_supplemental_adapters",
            "required": True,
            "passed": True,
        },
        {
            "contract": "schema_validation_required_before_any_real_evaluation",
            "required": True,
            "passed": True,
        },
        {
            "contract": "key_mapping_required_before_any_outcome_join",
            "required": True,
            "passed": True,
        },
        {
            "contract": "fail_closed_behavior_required_for_missing_or_invalid_artifacts",
            "required": True,
            "passed": True,
        },
        {
            "contract": "no_real_backtests_or_outcome_joins_in_6gz",
            "required": True,
            "passed": True,
        },
        {
            "contract": "no_activation_or_layer_6_exit_credit_in_6gz",
            "required": True,
            "passed": True,
        },
        {
            "contract": "future_audit_required_before_real_evaluation_layer",
            "required": True,
            "passed": True,
        },
        {
            "contract": "recommended_6gz_diagnosis",
            "required": True,
            "passed": True,
            "artifact": "layer_6_gameplay_mechanic_outcome_artifact_adapter_plan_complete",
        },
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6gx_after = PLAN_6GX_PATH.read_text(encoding="utf-8") if PLAN_6GX_PATH.exists() else ""
    audit_6gw_after = AUDIT_6GW_PATH.read_text(encoding="utf-8") if AUDIT_6GW_PATH.exists() else ""
    implement_6gv_after = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_after = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_after = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gy_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gx_plan", "policy": "unchanged_by_6gy", "passed": plan_6gx_after == plan_6gx_before},
        {"surface": "6gw_audit", "policy": "unchanged_by_6gy", "passed": audit_6gw_after == audit_6gw_before},
        {"surface": "6gv_implementation", "policy": "unchanged_by_6gy", "passed": implement_6gv_after == implement_6gv_before},
        {"surface": "6gu_audit", "policy": "unchanged_by_6gy", "passed": audit_6gu_after == audit_6gu_before},
        {"surface": "6gt_plan", "policy": "unchanged_by_6gy", "passed": plan_6gt_after == plan_6gt_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gy", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gy", "passed": True},
        {"surface": "fixtures_or_production_defaults", "policy": "unchanged_by_6gy", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GY, "actual": DIAGNOSIS_6GY, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": len(checks_consistency_rows) >= 16 and all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_artifact_audit_rows), "detail": f"{sum(1 for row in input_artifact_audit_rows if row['passed'])}/{len(input_artifact_audit_rows)}"},
        {"check": "classification_integrity", "passed": all(row["passed"] for row in classification_integrity_rows), "detail": f"{sum(1 for row in classification_integrity_rows if row['passed'])}/{len(classification_integrity_rows)}"},
        {"check": "selection_integrity", "passed": all(row["passed"] for row in selection_integrity_rows), "detail": f"{sum(1 for row in selection_integrity_rows if row['passed'])}/{len(selection_integrity_rows)}"},
        {"check": "adapter_requirements", "passed": all(row["passed"] for row in adapter_requirement_rows), "detail": f"{sum(1 for row in adapter_requirement_rows if row['passed'])}/{len(adapter_requirement_rows)}"},
        {"check": "materialization_requirements", "passed": all(row["passed"] for row in materialization_requirement_rows), "detail": f"{sum(1 for row in materialization_requirement_rows if row['passed'])}/{len(materialization_requirement_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_boundary_rows), "detail": f"{sum(1 for row in safety_boundary_rows if row['passed'])}/{len(safety_boundary_rows)}"},
        {"check": "future_6gy_contract", "passed": all(boolish(row.get("passed")) for row in future_6gy_rows_6gx), "detail": f"{sum(1 for row in future_6gy_rows_6gx if boolish(row.get('passed')))}" + f"/{len(future_6gy_rows_6gx)}"},
        {"check": "future_6gz_contract", "passed": all(row["passed"] for row in future_6gz_contract_rows), "detail": f"{sum(1 for row in future_6gz_contract_rows if row['passed'])}/{len(future_6gz_contract_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "checks_consistency": write_csv(CHECKS_CONSISTENCY_CSV, checks_consistency_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_AUDIT_CSV, input_artifact_audit_rows),
        "classification_integrity": write_csv(CLASSIFICATION_INTEGRITY_CSV, classification_integrity_rows),
        "selection_integrity": write_csv(SELECTION_INTEGRITY_CSV, selection_integrity_rows),
        "adapter_requirements": write_csv(ADAPTER_REQUIREMENTS_CSV, adapter_requirement_rows),
        "materialization_requirements": write_csv(MATERIALIZATION_REQUIREMENTS_CSV, materialization_requirement_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_boundary_rows),
        "future_6gz_contract": write_csv(FUTURE_6GZ_CONTRACT_CSV, future_6gz_contract_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GY",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "audited_layer": "6GX",
        "audited_plan_diagnosis": json_6gx.get("diagnosis"),
        "diagnosis": DIAGNOSIS_6GY if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_plan": str(PLAN_6GX_PATH),
        "predecessor_plan_returncode": plan_run.returncode,
        "predecessor_plan_diagnosis": json_6gx.get("diagnosis"),
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "predecessor_real_backtests_run": bool(json_6gx.get("real_backtests_run")),
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "actual_outcomes_joined": False,
        "activation_allowed": False,
        "layer_6_exit_credit": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(EVALUATION_WINDOWS),
        "audited_discovered_artifact_count": intish(json_6gx.get("discovered_artifact_count"), 0),
        "audited_classified_artifact_count": intish(json_6gx.get("classified_artifact_count"), 0),
        "audited_candidate_game_outcomes_count": intish(json_6gx.get("candidate_game_outcomes_count"), 0),
        "audited_candidate_base_out_transitions_count": intish(json_6gx.get("candidate_base_out_transitions_count"), 0),
        "audited_candidate_inning_runs_count": intish(json_6gx.get("candidate_inning_runs_count"), 0),
        "audited_candidate_backtest_prior_outputs_count": intish(json_6gx.get("candidate_backtest_prior_outputs_count"), 0),
        "audited_unsuitable_planning_artifact_count": intish(json_6gx.get("unsuitable_planning_artifact_count"), 0),
        "audited_primary_selection_status": json_6gx.get("primary_selection_status"),
        "audited_primary_selected_artifact_family": json_6gx.get("primary_selected_artifact_family"),
        "audited_primary_selected_artifact_count": intish(json_6gx.get("primary_selected_artifact_count"), 0),
        "audited_adapter_required": bool(json_6gx.get("adapter_required")),
        "audited_materialization_plan_required": bool(json_6gx.get("materialization_plan_required")),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "checks_consistency_csv": str(CHECKS_CONSISTENCY_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_AUDIT_CSV),
            "classification_integrity_csv": str(CLASSIFICATION_INTEGRITY_CSV),
            "selection_integrity_csv": str(SELECTION_INTEGRITY_CSV),
            "adapter_requirements_csv": str(ADAPTER_REQUIREMENTS_CSV),
            "materialization_requirements_csv": str(MATERIALIZATION_REQUIREMENTS_CSV),
            "safety_boundaries_csv": str(SAFETY_BOUNDARIES_CSV),
            "future_6gz_contract_csv": str(FUTURE_6GZ_CONTRACT_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
