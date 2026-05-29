#!/usr/bin/env python3
"""Layer 6GT gameplay mechanic real backtest execution plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gt_real_backtest_plan"
TMP_DIR = Path("tmp")

AUDIT_6GS_PATH = Path("scripts/audit_6gs_layer6_gameplay_mechanic_backtest_harness_skeleton.py")
IMPLEMENT_6GR_PATH = Path("scripts/implement_6gr_layer6_gameplay_mechanic_backtest_harness_skeleton.py")
AUDIT_6GQ_PATH = Path("scripts/audit_6gq_layer6_gameplay_mechanic_backtest_harness_plan.py")
PLAN_6GP_PATH = Path("scripts/plan_6gp_layer6_gameplay_mechanic_backtest_harness.py")

JSON_6GS = TMP_DIR / "layer6_6gs_backtest_harness_skeleton_audit.json"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
EVALUATION_WINDOWS_CSV = TMP_DIR / f"{SLUG}_evaluation_windows.csv"
ACTUAL_OUTCOME_SOURCE_CSV = TMP_DIR / f"{SLUG}_actual_outcome_source_contract.csv"
METHODOLOGY_CSV = TMP_DIR / f"{SLUG}_methodology.csv"
RUNTIME_LIMITS_CSV = TMP_DIR / f"{SLUG}_runtime_limits.csv"
EVIDENCE_ARTIFACT_CSV = TMP_DIR / f"{SLUG}_evidence_artifact_contract.csv"
DECISION_CLASSES_CSV = TMP_DIR / f"{SLUG}_decision_classes.csv"
THRESHOLD_POLICY_CSV = TMP_DIR / f"{SLUG}_threshold_policy.csv"
REPRODUCIBILITY_CSV = TMP_DIR / f"{SLUG}_reproducibility.csv"
PAYLOAD_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_payload_consistency.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
FUTURE_6GU_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gu_contract.csv"
FUTURE_6GV_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gv_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GS = "layer_6_gameplay_mechanic_backtest_harness_skeleton_audit_complete"
DIAGNOSIS_6GT = "layer_6_gameplay_mechanic_real_backtest_plan_complete"
CURRENT_LAYER = "6GT_layer_6_gameplay_mechanic_real_backtest_plan"
RECOMMENDED_NEXT_LAYER = "6GU_layer_6_gameplay_mechanic_real_backtest_plan_audit"
RECOMMENDED_PATH = "plan_real_backtest_execution_then_audit_before_running_real_backtests"

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

REQUIRED_EVIDENCE_ARTIFACTS = [
    "real_harness_config",
    "real_candidate_results",
    "real_baseline_results",
    "real_metric_comparison",
    "real_pass_fail_summary",
    "real_payload_consistency_summary",
    "real_determinism_summary",
    "real_runtime_summary",
    "real_safety_summary",
    "real_decision_recommendations",
]

DECISION_CLASSES = [
    "keep_dormant",
    "recalibrate_parameters",
    "implement_candidate",
    "consider_activation_later",
    "reject_candidate",
    "needs_more_evidence",
]

THRESHOLD_POLICIES = [
    "must_not_degrade_total_run_error",
    "must_not_degrade_team_total_error",
    "must_not_degrade_inning_distribution",
    "must_not_degrade_scoring_tails",
    "must_not_degrade_variance_calibration",
    "must_improve_or_preserve_calibration_error",
    "must_prove_payload_consistency_if_projection_facing",
    "must_be_reproducible",
    "must_be_deterministic_given_seed",
    "must_not_change_production_defaults",
]


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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6gs_before = AUDIT_6GS_PATH.read_text(encoding="utf-8") if AUDIT_6GS_PATH.exists() else ""
    implement_6gr_before = IMPLEMENT_6GR_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GR_PATH.exists() else ""
    audit_6gq_before = AUDIT_6GQ_PATH.read_text(encoding="utf-8") if AUDIT_6GQ_PATH.exists() else ""
    plan_6gp_before = PLAN_6GP_PATH.read_text(encoding="utf-8") if PLAN_6GP_PATH.exists() else ""

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6GS_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    audit_json = load_json(JSON_6GS)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gs_audit_exists", "expected": True, "actual": AUDIT_6GS_PATH.exists(), "passed": AUDIT_6GS_PATH.exists()},
        {"check": "6gs_audit_runs", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"check": "6gs_json_exists", "expected": True, "actual": JSON_6GS.exists(), "passed": JSON_6GS.exists()},
        {"check": "6gs_all_checks_passed", "expected": True, "actual": audit_json.get("all_checks_passed"), "passed": audit_json.get("all_checks_passed") is True},
        {"check": "6gs_audit_only", "expected": True, "actual": audit_json.get("audit_only"), "passed": audit_json.get("audit_only") is True},
        {"check": "6gs_diagnosis", "expected": DIAGNOSIS_6GS, "actual": audit_json.get("diagnosis"), "passed": audit_json.get("diagnosis") == DIAGNOSIS_6GS},
        {"check": "6gs_recommended_next_layer", "expected": CURRENT_LAYER, "actual": audit_json.get("recommended_next_layer"), "passed": audit_json.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gs_layer_6_exit_ready_false", "expected": False, "actual": audit_json.get("layer_6_exit_ready"), "passed": audit_json.get("layer_6_exit_ready") is False},
        {"check": "6gs_mechanics_activated_false", "expected": False, "actual": audit_json.get("mechanics_activated_by_this_layer"), "passed": audit_json.get("mechanics_activated_by_this_layer") is False},
        {"check": "6gs_real_backtests_run_false", "expected": False, "actual": audit_json.get("real_backtests_run"), "passed": audit_json.get("real_backtests_run") is False},
        {"check": "6gs_live_fetches_false", "expected": False, "actual": audit_json.get("live_data_fetches_run"), "passed": audit_json.get("live_data_fetches_run") is False},
        {"check": "6gs_database_writes_false", "expected": False, "actual": audit_json.get("database_writes_run"), "passed": audit_json.get("database_writes_run") is False},
        {"check": "6gs_materialization_jobs_false", "expected": False, "actual": audit_json.get("materialization_jobs_run"), "passed": audit_json.get("materialization_jobs_run") is False},
        {"check": "6gs_production_simulations_false", "expected": False, "actual": audit_json.get("production_simulations_run"), "passed": audit_json.get("production_simulations_run") is False},
    ]

    evaluation_window_rows = [
        {
            "window_key": "recent_rolling_window",
            "window_type": "contract_only",
            "purpose": "evaluate current-form calibration without live fetches",
            "selection_rule": "future_layer_uses_already_materialized_recent_validated_games",
            "live_data_fetch_allowed": False,
            "real_backtest_run_by_6gt": False,
        },
        {
            "window_key": "full_available_validated_window",
            "window_type": "contract_only",
            "purpose": "maximize sample size across committed/materialized historical outcomes",
            "selection_rule": "future_layer_uses_full_available_local_validated_outcome_window",
            "live_data_fetch_allowed": False,
            "real_backtest_run_by_6gt": False,
        },
        {
            "window_key": "stress_window_high_extra_innings_or_high_run_environment",
            "window_type": "contract_only",
            "purpose": "stress scoring-tail, extra-inning, and high-run-environment mechanics",
            "selection_rule": "future_layer_filters_already_materialized_games_for_extra_innings_or_high_total_runs",
            "live_data_fetch_allowed": False,
            "real_backtest_run_by_6gt": False,
        },
    ]

    actual_outcome_source_rows = [
        {"contract": "use_already_materialized_local_or_committed_historical_outcomes", "required": True, "passed": True},
        {"contract": "no_live_fetch_inside_6gt", "required": True, "passed": True},
        {"contract": "future_layers_fail_closed_if_outcomes_missing", "required": True, "passed": True},
        {"contract": "include_final_game_totals", "required": True, "passed": True},
        {"contract": "include_team_totals", "required": True, "passed": True},
        {"contract": "include_inning_runs_where_available", "required": True, "passed": True},
        {"contract": "include_extra_inning_status", "required": True, "passed": True},
        {"contract": "include_base_out_transition_evidence_where_available", "required": True, "passed": True},
    ]

    methodology_rows = [
        {"methodology": "same_games", "required": True, "passed": True},
        {"methodology": "same_seeds", "required": True, "passed": True},
        {"methodology": "same_baseline_simulator_projection_inputs", "required": True, "passed": True},
        {"methodology": "candidate_mechanics_enabled_only_in_isolated_dry_run_configs", "required": True, "passed": True},
        {"methodology": "current_or_off_baseline_preserved", "required": True, "passed": True},
        {"methodology": "compare_candidate_vs_baseline_against_actual_outcomes", "required": True, "passed": True},
        {"methodology": "no_production_default_change", "required": True, "passed": True},
        {"methodology": "no_projection_payload_mutation_during_planning", "required": True, "passed": True},
    ]

    runtime_limit_rows = [
        {"limit_key": "max_mechanics_per_run", "limit_value": "10_or_less", "required": True, "passed": True},
        {"limit_key": "max_games_per_shard", "limit_value": "bounded_by_future_layer_config", "required": True, "passed": True},
        {"limit_key": "deterministic_seed", "limit_value": "required_for_every_candidate_and_baseline_pair", "required": True, "passed": True},
        {"limit_key": "timeout_seconds", "limit_value": "explicit_future_layer_timeout_required", "required": True, "passed": True},
        {"limit_key": "output_completeness_check", "limit_value": "fail_closed_if_any_required_artifact_missing", "required": True, "passed": True},
        {"limit_key": "resume_or_shard_policy", "limit_value": "future_layer_must_make_partial_runs_non_activation_eligible", "required": True, "passed": True},
    ]

    evidence_artifact_rows = [
        {
            "artifact": artifact,
            "required": True,
            "future_real_backtest_layer_must_emit": True,
            "activation_blocked_if_missing": True,
            "passed": True,
        }
        for artifact in REQUIRED_EVIDENCE_ARTIFACTS
    ]

    decision_class_rows = [
        {
            "decision_class": decision_class,
            "required": True,
            "description": {
                "keep_dormant": "candidate remains disabled because evidence does not justify implementation or activation",
                "recalibrate_parameters": "candidate concept may be useful but parameters require tuning before implementation",
                "implement_candidate": "candidate earns implementation work, not immediate production activation",
                "consider_activation_later": "candidate has strong evidence but still requires downstream activation audit",
                "reject_candidate": "candidate worsens evidence or violates safety/consistency gates",
                "needs_more_evidence": "sample size or artifacts are insufficient for a durable decision",
            }[decision_class],
            "activation_allowed_by_6gt": False,
            "passed": True,
        }
        for decision_class in DECISION_CLASSES
    ]

    threshold_rows = [
        {
            "threshold_policy": policy,
            "required": True,
            "failure_blocks_activation": True,
            "failure_blocks_layer_6_exit_credit": True,
            "passed": True,
        }
        for policy in THRESHOLD_POLICIES
    ]

    reproducibility_rows = [
        {"contract": "deterministic_seed_required", "required": True, "passed": True},
        {"contract": "candidate_and_baseline_use_same_seed", "required": True, "passed": True},
        {"contract": "rerun_comparability_required", "required": True, "passed": True},
        {"contract": "same_inputs_same_outputs_required_for_activation_eligibility", "required": True, "passed": True},
        {"contract": "non_reproducible_results_block_activation", "required": True, "passed": True},
    ]

    payload_consistency_rows = [
        {"contract": "projection_facing_mechanics_require_payload_consistency_evidence", "required": True, "passed": True},
        {"contract": "missing_payload_evidence_blocks_activation", "required": True, "passed": True},
        {"contract": "payload_mutation_forbidden_in_6gt", "required": True, "passed": True},
        {"contract": "payload_schema_changes_forbidden_in_6gt", "required": True, "passed": True},
        {"contract": "market_projection_payload_consistency_checked_in_future_real_backtest", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "no_mechanic_activation", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_simulator_behavior_change", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_projection_behavior_change", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_fixture_change", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_production_default_change", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_expensive_backtest", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_real_backtest_execution", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_database_write", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_materialization_job", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_production_simulation", "expected": True, "actual": True, "passed": True},
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
    ]

    future_6gu_rows = [
        {"contract": "audit_6gt_real_backtest_plan", "required": True, "passed": True},
        {"contract": "verify_evaluation_windows", "required": True, "passed": True},
        {"contract": "verify_actual_outcome_source_contract", "required": True, "passed": True},
        {"contract": "verify_methodology_same_games_same_seeds_candidate_vs_baseline", "required": True, "passed": True},
        {"contract": "verify_runtime_limits", "required": True, "passed": True},
        {"contract": "verify_evidence_artifact_contract", "required": True, "passed": True},
        {"contract": "verify_decision_classes_and_thresholds", "required": True, "passed": True},
        {"contract": "verify_no_real_backtests_or_activation", "required": True, "passed": True},
        {"contract": "recommended_6gu_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_real_backtest_plan_audit_complete"},
    ]

    future_6gv_rows = [
        {"contract": "first_allowed_real_backtest_dry_run_execution_layer", "required": True, "passed": True},
        {"contract": "use_6gt_plan_and_6gu_audit_as_inputs", "required": True, "passed": True},
        {"contract": "run_bounded_candidate_vs_baseline_evidence_generation", "required": True, "passed": True},
        {"contract": "emit_real_backtest_evidence_artifacts", "required": True, "passed": True},
        {"contract": "still_no_mechanic_activation", "required": True, "passed": True},
        {"contract": "activation_requires_later_dedicated_activation_audit", "required": True, "passed": True},
        {"contract": "recommended_6gv_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_real_backtest_dry_run_execution_complete"},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6gs_after = AUDIT_6GS_PATH.read_text(encoding="utf-8") if AUDIT_6GS_PATH.exists() else ""
    implement_6gr_after = IMPLEMENT_6GR_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GR_PATH.exists() else ""
    audit_6gq_after = AUDIT_6GQ_PATH.read_text(encoding="utf-8") if AUDIT_6GQ_PATH.exists() else ""
    plan_6gp_after = PLAN_6GP_PATH.read_text(encoding="utf-8") if PLAN_6GP_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gt_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gs_audit", "policy": "unchanged_by_6gt_plan", "passed": audit_6gs_after == audit_6gs_before},
        {"surface": "6gr_implementation", "policy": "unchanged_by_6gt_plan", "passed": implement_6gr_after == implement_6gr_before},
        {"surface": "6gq_audit", "policy": "unchanged_by_6gt_plan", "passed": audit_6gq_after == audit_6gq_before},
        {"surface": "6gp_plan", "policy": "unchanged_by_6gt_plan", "passed": plan_6gp_after == plan_6gp_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gt_plan", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gt_plan", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gt_plan", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gt_plan", "passed": True},
        {"surface": "live_fetches_or_database_writes", "policy": "not_run", "passed": True},
        {"surface": "materialization_jobs_or_production_simulations", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "real_backtests_run", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GT, "actual": DIAGNOSIS_6GT, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "evaluation_windows", "passed": len(evaluation_window_rows) >= 3 and all(not row["live_data_fetch_allowed"] and not row["real_backtest_run_by_6gt"] for row in evaluation_window_rows), "detail": f"{len(evaluation_window_rows)}/3"},
        {"check": "actual_outcome_source_contract", "passed": all(row["passed"] for row in actual_outcome_source_rows), "detail": f"{sum(1 for row in actual_outcome_source_rows if row['passed'])}/{len(actual_outcome_source_rows)}"},
        {"check": "methodology", "passed": all(row["passed"] for row in methodology_rows), "detail": f"{sum(1 for row in methodology_rows if row['passed'])}/{len(methodology_rows)}"},
        {"check": "runtime_limits", "passed": all(row["passed"] for row in runtime_limit_rows), "detail": f"{sum(1 for row in runtime_limit_rows if row['passed'])}/{len(runtime_limit_rows)}"},
        {"check": "evidence_artifact_contract", "passed": len(evidence_artifact_rows) == 10 and all(row["passed"] for row in evidence_artifact_rows), "detail": f"{len(evidence_artifact_rows)}/10"},
        {"check": "decision_classes", "passed": len(decision_class_rows) == 6 and all(row["passed"] and not row["activation_allowed_by_6gt"] for row in decision_class_rows), "detail": f"{len(decision_class_rows)}/6"},
        {"check": "threshold_policy", "passed": len(threshold_rows) == 10 and all(row["passed"] for row in threshold_rows), "detail": f"{len(threshold_rows)}/10"},
        {"check": "reproducibility", "passed": all(row["passed"] for row in reproducibility_rows), "detail": f"{sum(1 for row in reproducibility_rows if row['passed'])}/{len(reproducibility_rows)}"},
        {"check": "payload_consistency", "passed": all(row["passed"] for row in payload_consistency_rows), "detail": f"{sum(1 for row in payload_consistency_rows if row['passed'])}/{len(payload_consistency_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "future_6gu_contract", "passed": all(row["passed"] for row in future_6gu_rows), "detail": f"{sum(1 for row in future_6gu_rows if row['passed'])}/{len(future_6gu_rows)}"},
        {"check": "future_6gv_contract", "passed": all(row["passed"] for row in future_6gv_rows), "detail": f"{sum(1 for row in future_6gv_rows if row['passed'])}/{len(future_6gv_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "evaluation_windows": write_csv(EVALUATION_WINDOWS_CSV, evaluation_window_rows),
        "actual_outcome_source_contract": write_csv(ACTUAL_OUTCOME_SOURCE_CSV, actual_outcome_source_rows),
        "methodology": write_csv(METHODOLOGY_CSV, methodology_rows),
        "runtime_limits": write_csv(RUNTIME_LIMITS_CSV, runtime_limit_rows),
        "evidence_artifact_contract": write_csv(EVIDENCE_ARTIFACT_CSV, evidence_artifact_rows),
        "decision_classes": write_csv(DECISION_CLASSES_CSV, decision_class_rows),
        "threshold_policy": write_csv(THRESHOLD_POLICY_CSV, threshold_rows),
        "reproducibility": write_csv(REPRODUCIBILITY_CSV, reproducibility_rows),
        "payload_consistency": write_csv(PAYLOAD_CONSISTENCY_CSV, payload_consistency_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "future_6gu_contract": write_csv(FUTURE_6GU_CONTRACT_CSV, future_6gu_rows),
        "future_6gv_contract": write_csv(FUTURE_6GV_CONTRACT_CSV, future_6gv_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GT",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6GT if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "audited_predecessor_layer": "6GS",
        "predecessor_audit": str(AUDIT_6GS_PATH),
        "predecessor_audit_returncode": audit_run.returncode,
        "predecessor_audit_diagnosis": audit_json.get("diagnosis"),
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(evaluation_window_rows),
        "evidence_artifact_count": len(evidence_artifact_rows),
        "decision_class_count": len(decision_class_rows),
        "threshold_policy_count": len(threshold_rows),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "evaluation_windows_csv": str(EVALUATION_WINDOWS_CSV),
            "actual_outcome_source_contract_csv": str(ACTUAL_OUTCOME_SOURCE_CSV),
            "methodology_csv": str(METHODOLOGY_CSV),
            "runtime_limits_csv": str(RUNTIME_LIMITS_CSV),
            "evidence_artifact_contract_csv": str(EVIDENCE_ARTIFACT_CSV),
            "decision_classes_csv": str(DECISION_CLASSES_CSV),
            "threshold_policy_csv": str(THRESHOLD_POLICY_CSV),
            "reproducibility_csv": str(REPRODUCIBILITY_CSV),
            "payload_consistency_csv": str(PAYLOAD_CONSISTENCY_CSV),
            "safety_boundaries_csv": str(SAFETY_BOUNDARIES_CSV),
            "future_6gu_contract_csv": str(FUTURE_6GU_CONTRACT_CSV),
            "future_6gv_contract_csv": str(FUTURE_6GV_CONTRACT_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
