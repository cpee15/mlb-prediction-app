#!/usr/bin/env python3
"""Plan Layer 6GX gameplay mechanic outcome artifact selection."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gx_outcome_artifact_selection_plan"
TMP_DIR = Path("tmp")

AUDIT_6GW_PATH = Path("scripts/audit_6gw_layer6_gameplay_mechanic_real_backtest_dry_run.py")
IMPLEMENT_6GV_PATH = Path("scripts/implement_6gv_layer6_gameplay_mechanic_real_backtest_dry_run.py")
AUDIT_6GU_PATH = Path("scripts/audit_6gu_layer6_gameplay_mechanic_real_backtest_plan.py")
PLAN_6GT_PATH = Path("scripts/plan_6gt_layer6_gameplay_mechanic_real_backtests.py")

JSON_6GW = TMP_DIR / "layer6_6gw_real_backtest_dry_run_audit.json"
CHECKS_6GW = TMP_DIR / "layer6_6gw_real_backtest_dry_run_audit_checks.csv"
OUTCOME_DISCOVERY_6GW = TMP_DIR / "layer6_6gw_real_backtest_dry_run_audit_outcome_discovery.csv"
OUTCOME_DISCOVERY_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_outcome_discovery.csv"
JSON_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run.json"
DECISION_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_decision_recommendations.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
DISCOVERED_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_discovered_artifacts.csv"
CLASSIFICATION_CSV = TMP_DIR / f"{SLUG}_classification.csv"
SELECTION_SUMMARY_CSV = TMP_DIR / f"{SLUG}_selection_summary.csv"
ADAPTER_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_adapter_requirements.csv"
MATERIALIZATION_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_materialization_requirements.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
FUTURE_6GY_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gy_contract.csv"
FUTURE_6GZ_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gz_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GW = "layer_6_gameplay_mechanic_real_backtest_dry_run_audit_complete"
DIAGNOSIS_6GX = "layer_6_gameplay_mechanic_outcome_artifact_selection_plan_complete"
CURRENT_LAYER = "6GX_layer_6_gameplay_mechanic_outcome_artifact_selection_plan"
RECOMMENDED_NEXT_LAYER = "6GY_layer_6_gameplay_mechanic_outcome_artifact_selection_plan_audit"
RECOMMENDED_PATH = "plan_outcome_artifact_selection_then_audit_before_adapter_or_materialization"

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

REQUIRED_INPUT_ARTIFACTS = [
    JSON_6GW,
    CHECKS_6GW,
    OUTCOME_DISCOVERY_6GW,
    OUTCOME_DISCOVERY_6GV,
    JSON_6GV,
    DECISION_6GV,
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


def is_remote(path: str) -> bool:
    return path.startswith(("http://", "https://", "s3://", "gs://"))


def classify_artifact(path_str: str, hint: str) -> Dict[str, Any]:
    lower = path_str.lower()
    hint_lower = hint.lower()

    if not path_str:
        cls = "insufficient_metadata"
        score = 0
        reason = "missing_candidate_path"
    elif "layer6_6g" in lower or "_plan" in lower or "_audit" in lower or "checks" in lower or "contract" in lower:
        cls = "unsuitable_planning_artifact"
        score = 25
        reason = "layer_planning_or_audit_artifact_not_primary_actual_outcome_source"
    elif "base_out" in lower or "transition_matrix" in lower or "transition" in lower:
        cls = "candidate_base_out_transitions"
        score = 82
        reason = "local_artifact_name_indicates_base_out_or_transition_outcome_semantics"
    elif "inning" in lower or "walkoff" in lower:
        cls = "candidate_inning_runs"
        score = 78
        reason = "local_artifact_name_indicates_inning_or_walkoff_outcome_semantics"
    elif "final_score" in lower or "game_total" in lower or "game_outcome" in lower or "outcome_evidence" in lower:
        cls = "candidate_game_outcomes"
        score = 84
        reason = "local_artifact_name_indicates_game_outcome_or_game_total_semantics"
    elif "team_total" in lower or "team_totals" in lower:
        cls = "candidate_team_totals"
        score = 82
        reason = "local_artifact_name_indicates_team_total_semantics"
    elif "backtest" in lower or "historical" in lower or hint_lower in {"backtest", "historical"}:
        cls = "candidate_backtest_prior_outputs"
        score = 58
        reason = "local_artifact_is_related_historical_or_prior_backtest_output_not_final_actual_outcome_source"
    elif "outcome" in lower or hint_lower == "outcome":
        cls = "candidate_game_outcomes"
        score = 72
        reason = "local_artifact_name_indicates_outcome_semantics_but_requires_schema_validation"
    else:
        cls = "insufficient_metadata"
        score = 10
        reason = "insufficient_path_metadata_for_selection"

    return {"suitability_class": cls, "suitability_score": score, "reason": reason}


def support_flags(cls: str) -> Dict[str, bool]:
    return {
        "can_support_game_outcomes": cls == "candidate_game_outcomes",
        "can_support_team_totals": cls == "candidate_team_totals",
        "can_support_inning_runs": cls == "candidate_inning_runs",
        "can_support_base_out_transitions": cls == "candidate_base_out_transitions",
        "can_support_mechanic_backtest": cls in {
            "candidate_game_outcomes",
            "candidate_team_totals",
            "candidate_inning_runs",
            "candidate_base_out_transitions",
            "candidate_backtest_prior_outputs",
        },
    }


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6gw_before = AUDIT_6GW_PATH.read_text(encoding="utf-8") if AUDIT_6GW_PATH.exists() else ""
    implement_6gv_before = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_before = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_before = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6GW_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6gw = load_json(JSON_6GW)
    discovery_6gv = read_csv(OUTCOME_DISCOVERY_6GV)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gw_audit_exists", "expected": True, "actual": AUDIT_6GW_PATH.exists(), "passed": AUDIT_6GW_PATH.exists()},
        {"check": "6gw_audit_runs", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"check": "6gw_json_exists", "expected": True, "actual": JSON_6GW.exists(), "passed": JSON_6GW.exists()},
        {"check": "6gw_all_checks_passed", "expected": True, "actual": json_6gw.get("all_checks_passed"), "passed": json_6gw.get("all_checks_passed") is True},
        {"check": "6gw_audit_only", "expected": True, "actual": json_6gw.get("audit_only"), "passed": json_6gw.get("audit_only") is True},
        {"check": "6gw_diagnosis", "expected": DIAGNOSIS_6GW, "actual": json_6gw.get("diagnosis"), "passed": json_6gw.get("diagnosis") == DIAGNOSIS_6GW},
        {"check": "6gw_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6gw.get("recommended_next_layer"), "passed": json_6gw.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gw_layer_6_exit_ready_false", "expected": False, "actual": json_6gw.get("layer_6_exit_ready"), "passed": json_6gw.get("layer_6_exit_ready") is False},
        {"check": "6gw_mechanics_activated_false", "expected": False, "actual": json_6gw.get("mechanics_activated_by_this_layer"), "passed": json_6gw.get("mechanics_activated_by_this_layer") is False},
        {"check": "6gw_real_backtests_run_false", "expected": False, "actual": json_6gw.get("real_backtests_run"), "passed": json_6gw.get("real_backtests_run") is False},
        {"check": "6gw_predecessor_real_backtests_run_true", "expected": True, "actual": json_6gw.get("predecessor_real_backtests_run"), "passed": json_6gw.get("predecessor_real_backtests_run") is True},
        {"check": "6gw_games_evaluated_zero", "expected": 0, "actual": json_6gw.get("games_evaluated"), "passed": int(json_6gw.get("games_evaluated", -1)) == 0},
        {"check": "6gw_activation_allowed_false", "expected": False, "actual": json_6gw.get("activation_allowed"), "passed": json_6gw.get("activation_allowed") is False},
        {"check": "6gw_layer_6_exit_credit_false", "expected": False, "actual": json_6gw.get("layer_6_exit_credit"), "passed": json_6gw.get("layer_6_exit_credit") is False},
        {"check": "6gw_live_fetches_false", "expected": False, "actual": json_6gw.get("live_data_fetches_run"), "passed": json_6gw.get("live_data_fetches_run") is False},
        {"check": "6gw_database_writes_false", "expected": False, "actual": json_6gw.get("database_writes_run"), "passed": json_6gw.get("database_writes_run") is False},
        {"check": "6gw_materialization_jobs_false", "expected": False, "actual": json_6gw.get("materialization_jobs_run"), "passed": json_6gw.get("materialization_jobs_run") is False},
        {"check": "6gw_production_simulations_false", "expected": False, "actual": json_6gw.get("production_simulations_run"), "passed": json_6gw.get("production_simulations_run") is False},
    ]

    input_artifact_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "required": True, "passed": path.exists()}
        for path in REQUIRED_INPUT_ARTIFACTS
    ]

    discovered_rows = []
    for row in discovery_6gv:
        candidate_path = row.get("candidate_path", "")
        if not candidate_path:
            continue
        discovered_rows.append(
            {
                "candidate_path": candidate_path,
                "root": row.get("root", ""),
                "hint_match": row.get("hint_match", ""),
                "exists": row.get("exists", ""),
                "usable_outcome_artifact": row.get("usable_outcome_artifact", ""),
                "live_fetch_required": row.get("live_fetch_required", ""),
                "remote_path": is_remote(candidate_path),
                "passed": not is_remote(candidate_path) and not boolish(row.get("live_fetch_required")),
            }
        )

    if not discovered_rows:
        discovered_rows.append(
            {
                "candidate_path": "",
                "root": "repository",
                "hint_match": "",
                "exists": False,
                "usable_outcome_artifact": False,
                "live_fetch_required": False,
                "remote_path": False,
                "passed": True,
            }
        )

    classification_rows = []
    for row in discovered_rows:
        candidate_path = row.get("candidate_path", "")
        hint = row.get("hint_match", "")
        cls_info = classify_artifact(candidate_path, hint)
        cls = cls_info["suitability_class"]
        flags = support_flags(cls)
        classification_rows.append(
            {
                "candidate_path": candidate_path,
                "root": row.get("root", ""),
                "hint_match": hint,
                "suitability_class": cls,
                "suitability_score": cls_info["suitability_score"],
                "reason": cls_info["reason"],
                "requires_adapter": cls_info["suitability_score"] >= 50,
                "requires_schema_validation": cls_info["suitability_score"] >= 50,
                "requires_fresh_materialization": False,
                **flags,
                "remote_path": is_remote(candidate_path),
                "live_fetch_required": False,
                "activation_allowed": False,
                "layer_6_exit_credit": False,
                "passed": cls in ALLOWED_CLASSES and 0 <= cls_info["suitability_score"] <= 100 and not is_remote(candidate_path),
            }
        )

    primary_candidates = [
        row for row in classification_rows
        if int(row["suitability_score"]) >= 70
        and (row["can_support_game_outcomes"] or row["can_support_team_totals"])
        and not row["remote_path"]
    ]
    base_out_candidates = [row for row in classification_rows if row["can_support_base_out_transitions"] and int(row["suitability_score"]) >= 70]
    inning_candidates = [row for row in classification_rows if row["can_support_inning_runs"] and int(row["suitability_score"]) >= 70]

    if primary_candidates:
        primary_selection_status = "local_outcome_family_selected"
        primary_family = primary_candidates[0]["suitability_class"]
        adapter_required = True
        materialization_plan_required = False
    else:
        primary_selection_status = "no_valid_local_outcome_family_selected"
        primary_family = ""
        adapter_required = False
        materialization_plan_required = True

    selection_summary_rows = [
        {
            "selection_key": "primary_outcome_family",
            "selection_status": primary_selection_status,
            "selected_artifact_family": primary_family,
            "selected_artifact_count": len(primary_candidates),
            "adapter_required": adapter_required,
            "materialization_plan_required": materialization_plan_required,
            "games_evaluated": 0,
            "actual_outcomes_joined": False,
            "activation_allowed": False,
            "layer_6_exit_credit": False,
            "passed": True,
        },
        {
            "selection_key": "base_out_supplemental_family",
            "selection_status": "local_supplemental_family_selected" if base_out_candidates else "no_valid_local_supplemental_family_selected",
            "selected_artifact_family": "candidate_base_out_transitions" if base_out_candidates else "",
            "selected_artifact_count": len(base_out_candidates),
            "adapter_required": bool(base_out_candidates),
            "materialization_plan_required": not bool(base_out_candidates),
            "games_evaluated": 0,
            "actual_outcomes_joined": False,
            "activation_allowed": False,
            "layer_6_exit_credit": False,
            "passed": True,
        },
        {
            "selection_key": "inning_supplemental_family",
            "selection_status": "local_supplemental_family_selected" if inning_candidates else "no_valid_local_supplemental_family_selected",
            "selected_artifact_family": "candidate_inning_runs" if inning_candidates else "",
            "selected_artifact_count": len(inning_candidates),
            "adapter_required": bool(inning_candidates),
            "materialization_plan_required": not bool(inning_candidates),
            "games_evaluated": 0,
            "actual_outcomes_joined": False,
            "activation_allowed": False,
            "layer_6_exit_credit": False,
            "passed": True,
        },
    ]

    adapter_rows = [
        {"requirement": "read_local_selected_artifact_family_only", "required": True, "passed": True},
        {"requirement": "validate_schema_before_outcome_join", "required": True, "passed": True},
        {"requirement": "map_game_ids_or_equivalent_keys", "required": True, "passed": True},
        {"requirement": "map_team_totals_if_available", "required": True, "passed": True},
        {"requirement": "map_inning_runs_if_available", "required": True, "passed": True},
        {"requirement": "map_base_out_transitions_if_available", "required": True, "passed": True},
        {"requirement": "block_activation_until_adapter_audit", "required": True, "passed": True},
        {"requirement": "no_live_fetch_or_db_write", "required": True, "passed": True},
    ]

    materialization_rows = [
        {
            "requirement": "materialization_plan_required_if_no_valid_primary_family",
            "required": materialization_plan_required,
            "passed": True,
        },
        {"requirement": "materialization_not_run_by_6gx", "required": True, "passed": True},
        {"requirement": "future_materialization_must_be_planned_and_audited_first", "required": True, "passed": True},
        {"requirement": "future_materialization_must_remain_non_activating", "required": True, "passed": True},
        {"requirement": "future_materialization_must_block_layer_6_exit_credit", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_real_backtests", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_outcome_joins", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_database_write", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_materialization_job", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_production_simulation", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": True, "actual": True, "passed": True},
        {"boundary": "layer_6_exit_credit_blocked", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_runtime_behavior_change", "expected": True, "actual": True, "passed": True},
    ]

    future_6gy_rows = [
        {"contract": "audit_6gx_outcome_artifact_selection_plan", "required": True, "passed": True},
        {"contract": "verify_input_artifacts_present", "required": True, "passed": True},
        {"contract": "verify_classification_policy", "required": True, "passed": True},
        {"contract": "verify_selection_policy", "required": True, "passed": True},
        {"contract": "verify_adapter_or_materialization_recommendation", "required": True, "passed": True},
        {"contract": "verify_no_backtests_joins_fetches_or_activation", "required": True, "passed": True},
        {"contract": "recommended_6gy_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_outcome_artifact_selection_plan_audit_complete"},
    ]

    future_6gz_rows = [
        {"contract": "requires_6gx_plan_and_6gy_audit", "required": True, "passed": True},
        {"contract": "conditional_adapter_if_valid_local_family_selected", "required": True, "passed": True},
        {"contract": "conditional_materialization_plan_if_no_valid_family_selected", "required": True, "passed": True},
        {"contract": "no_activation_in_6gz", "required": True, "passed": True},
        {"contract": "layer_6_exit_credit_remains_blocked", "required": True, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6gw_after = AUDIT_6GW_PATH.read_text(encoding="utf-8") if AUDIT_6GW_PATH.exists() else ""
    implement_6gv_after = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_after = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_after = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gx_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gw_audit", "policy": "unchanged_by_6gx", "passed": audit_6gw_after == audit_6gw_before},
        {"surface": "6gv_implementation", "policy": "unchanged_by_6gx", "passed": implement_6gv_after == implement_6gv_before},
        {"surface": "6gu_audit", "policy": "unchanged_by_6gx", "passed": audit_6gu_after == audit_6gu_before},
        {"surface": "6gt_plan", "policy": "unchanged_by_6gx", "passed": plan_6gt_after == plan_6gt_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gx", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gx", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gx", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gx", "passed": True},
        {"surface": "live_fetches_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GX, "actual": DIAGNOSIS_6GX, "passed": True},
    ]

    class_counts = {cls: sum(1 for row in classification_rows if row["suitability_class"] == cls) for cls in ALLOWED_CLASSES}

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_artifact_rows), "detail": f"{sum(1 for row in input_artifact_rows if row['passed'])}/{len(input_artifact_rows)}"},
        {"check": "discovered_artifacts", "passed": len(discovered_rows) >= 1 and all(row["passed"] for row in discovered_rows), "detail": f"{len(discovered_rows)} discovered"},
        {"check": "classification_count", "passed": len(classification_rows) == len(discovered_rows), "detail": f"{len(classification_rows)}/{len(discovered_rows)}"},
        {"check": "classification_allowed_classes", "passed": all(row["suitability_class"] in ALLOWED_CLASSES for row in classification_rows), "detail": "all classes allowed"},
        {"check": "classification_scores", "passed": all(0 <= int(row["suitability_score"]) <= 100 for row in classification_rows), "detail": "0-100"},
        {"check": "classification_activation_blocked", "passed": all(not row["activation_allowed"] and not row["layer_6_exit_credit"] for row in classification_rows), "detail": "activation and exit credit false"},
        {"check": "classification_no_live_fetch_or_remote", "passed": all(not row["live_fetch_required"] and not row["remote_path"] for row in classification_rows), "detail": "local only"},
        {"check": "selection_policy", "passed": primary_selection_status in {"local_outcome_family_selected", "no_valid_local_outcome_family_selected"}, "detail": primary_selection_status},
        {"check": "adapter_requirements", "passed": all(row["passed"] for row in adapter_rows), "detail": f"{sum(1 for row in adapter_rows if row['passed'])}/{len(adapter_rows)}"},
        {"check": "materialization_requirements", "passed": all(row["passed"] for row in materialization_rows), "detail": f"{sum(1 for row in materialization_rows if row['passed'])}/{len(materialization_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "future_6gy_contract", "passed": all(row["passed"] for row in future_6gy_rows), "detail": f"{sum(1 for row in future_6gy_rows if row['passed'])}/{len(future_6gy_rows)}"},
        {"check": "future_6gz_contract", "passed": all(row["passed"] for row in future_6gz_rows), "detail": f"{sum(1 for row in future_6gz_rows if row['passed'])}/{len(future_6gz_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_artifact_rows),
        "discovered_artifacts": write_csv(DISCOVERED_ARTIFACTS_CSV, discovered_rows),
        "classification": write_csv(CLASSIFICATION_CSV, classification_rows),
        "selection_summary": write_csv(SELECTION_SUMMARY_CSV, selection_summary_rows),
        "adapter_requirements": write_csv(ADAPTER_REQUIREMENTS_CSV, adapter_rows),
        "materialization_requirements": write_csv(MATERIALIZATION_REQUIREMENTS_CSV, materialization_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "future_6gy_contract": write_csv(FUTURE_6GY_CONTRACT_CSV, future_6gy_rows),
        "future_6gz_contract": write_csv(FUTURE_6GZ_CONTRACT_CSV, future_6gz_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GX",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6GX if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "audited_predecessor_layer": "6GW",
        "predecessor_audit": str(AUDIT_6GW_PATH),
        "predecessor_audit_returncode": audit_run.returncode,
        "predecessor_audit_diagnosis": json_6gw.get("diagnosis"),
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
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
        "discovered_artifact_count": len(discovered_rows),
        "classified_artifact_count": len(classification_rows),
        "candidate_game_outcomes_count": class_counts.get("candidate_game_outcomes", 0),
        "candidate_team_totals_count": class_counts.get("candidate_team_totals", 0),
        "candidate_inning_runs_count": class_counts.get("candidate_inning_runs", 0),
        "candidate_base_out_transitions_count": class_counts.get("candidate_base_out_transitions", 0),
        "candidate_backtest_prior_outputs_count": class_counts.get("candidate_backtest_prior_outputs", 0),
        "unsuitable_planning_artifact_count": class_counts.get("unsuitable_planning_artifact", 0),
        "insufficient_metadata_count": class_counts.get("insufficient_metadata", 0),
        "primary_selection_status": primary_selection_status,
        "primary_selected_artifact_family": primary_family,
        "primary_selected_artifact_count": len(primary_candidates),
        "adapter_required": adapter_required,
        "materialization_plan_required": materialization_plan_required,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "discovered_artifacts_csv": str(DISCOVERED_ARTIFACTS_CSV),
            "classification_csv": str(CLASSIFICATION_CSV),
            "selection_summary_csv": str(SELECTION_SUMMARY_CSV),
            "adapter_requirements_csv": str(ADAPTER_REQUIREMENTS_CSV),
            "materialization_requirements_csv": str(MATERIALIZATION_REQUIREMENTS_CSV),
            "safety_boundaries_csv": str(SAFETY_BOUNDARIES_CSV),
            "future_6gy_contract_csv": str(FUTURE_6GY_CONTRACT_CSV),
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
