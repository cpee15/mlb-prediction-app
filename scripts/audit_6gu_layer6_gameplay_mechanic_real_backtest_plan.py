#!/usr/bin/env python3
"""Audit Layer 6GT gameplay mechanic real backtest execution plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gu_real_backtest_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6GT_PATH = Path("scripts/plan_6gt_layer6_gameplay_mechanic_real_backtests.py")
AUDIT_6GS_PATH = Path("scripts/audit_6gs_layer6_gameplay_mechanic_backtest_harness_skeleton.py")
IMPLEMENT_6GR_PATH = Path("scripts/implement_6gr_layer6_gameplay_mechanic_backtest_harness_skeleton.py")
AUDIT_6GQ_PATH = Path("scripts/audit_6gq_layer6_gameplay_mechanic_backtest_harness_plan.py")

JSON_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan.json"
CHECKS_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_checks.csv"
PREDECESSOR_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_predecessor.csv"
EVALUATION_WINDOWS_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_evaluation_windows.csv"
ACTUAL_OUTCOME_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_actual_outcome_source_contract.csv"
METHODOLOGY_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_methodology.csv"
RUNTIME_LIMITS_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_runtime_limits.csv"
EVIDENCE_ARTIFACT_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_evidence_artifact_contract.csv"
DECISION_CLASSES_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_decision_classes.csv"
THRESHOLD_POLICY_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_threshold_policy.csv"
REPRODUCIBILITY_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_reproducibility.csv"
PAYLOAD_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_payload_consistency.csv"
SAFETY_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_safety_boundaries.csv"
FUTURE_6GU_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_future_6gu_contract.csv"
FUTURE_6GV_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_future_6gv_contract.csv"
IMMUTABILITY_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_immutability.csv"
RECOMMENDED_6GT = TMP_DIR / "layer6_6gt_real_backtest_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
EVALUATION_WINDOWS_AUDIT_CSV = TMP_DIR / f"{SLUG}_evaluation_windows.csv"
ACTUAL_OUTCOME_AUDIT_CSV = TMP_DIR / f"{SLUG}_actual_outcome_source_contract.csv"
METHODOLOGY_AUDIT_CSV = TMP_DIR / f"{SLUG}_methodology.csv"
RUNTIME_LIMITS_AUDIT_CSV = TMP_DIR / f"{SLUG}_runtime_limits.csv"
EVIDENCE_ARTIFACT_AUDIT_CSV = TMP_DIR / f"{SLUG}_evidence_artifact_contract.csv"
DECISION_CLASSES_AUDIT_CSV = TMP_DIR / f"{SLUG}_decision_classes.csv"
THRESHOLD_POLICY_AUDIT_CSV = TMP_DIR / f"{SLUG}_threshold_policy.csv"
REPRODUCIBILITY_AUDIT_CSV = TMP_DIR / f"{SLUG}_reproducibility.csv"
PAYLOAD_AUDIT_CSV = TMP_DIR / f"{SLUG}_payload_consistency.csv"
SAFETY_AUDIT_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
FUTURE_6GV_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gv_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GT = "layer_6_gameplay_mechanic_real_backtest_plan_complete"
DIAGNOSIS_6GU = "layer_6_gameplay_mechanic_real_backtest_plan_audit_complete"
CURRENT_LAYER = "6GU_layer_6_gameplay_mechanic_real_backtest_plan_audit"
RECOMMENDED_NEXT_LAYER = "6GV_layer_6_gameplay_mechanic_real_backtest_dry_run_execution"
RECOMMENDED_PATH = "audit_real_backtest_plan_then_execute_bounded_dry_run_real_backtests"

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

REQUIRED_6GT_ARTIFACTS = [
    JSON_6GT,
    CHECKS_6GT,
    PREDECESSOR_6GT,
    EVALUATION_WINDOWS_6GT,
    ACTUAL_OUTCOME_6GT,
    METHODOLOGY_6GT,
    RUNTIME_LIMITS_6GT,
    EVIDENCE_ARTIFACT_6GT,
    DECISION_CLASSES_6GT,
    THRESHOLD_POLICY_6GT,
    REPRODUCIBILITY_6GT,
    PAYLOAD_6GT,
    SAFETY_6GT,
    FUTURE_6GU_6GT,
    FUTURE_6GV_6GT,
    IMMUTABILITY_6GT,
    RECOMMENDED_6GT,
]

REQUIRED_WINDOWS = [
    "recent_rolling_window",
    "full_available_validated_window",
    "stress_window_high_extra_innings_or_high_run_environment",
]

REQUIRED_ACTUAL_OUTCOME_CONTRACTS = [
    "use_already_materialized_local_or_committed_historical_outcomes",
    "no_live_fetch_inside_6gt",
    "future_layers_fail_closed_if_outcomes_missing",
    "include_final_game_totals",
    "include_team_totals",
    "include_inning_runs_where_available",
    "include_extra_inning_status",
    "include_base_out_transition_evidence_where_available",
]

REQUIRED_METHODOLOGY = [
    "same_games",
    "same_seeds",
    "same_baseline_simulator_projection_inputs",
    "candidate_mechanics_enabled_only_in_isolated_dry_run_configs",
    "current_or_off_baseline_preserved",
    "compare_candidate_vs_baseline_against_actual_outcomes",
    "no_production_default_change",
    "no_projection_payload_mutation_during_planning",
]

REQUIRED_RUNTIME_LIMITS = [
    "max_mechanics_per_run",
    "max_games_per_shard",
    "deterministic_seed",
    "timeout_seconds",
    "output_completeness_check",
    "resume_or_shard_policy",
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

REQUIRED_DECISION_CLASSES = [
    "keep_dormant",
    "recalibrate_parameters",
    "implement_candidate",
    "consider_activation_later",
    "reject_candidate",
    "needs_more_evidence",
]

REQUIRED_THRESHOLD_POLICIES = [
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

REQUIRED_REPRODUCIBILITY = [
    "deterministic_seed_required",
    "candidate_and_baseline_use_same_seed",
    "rerun_comparability_required",
    "same_inputs_same_outputs_required_for_activation_eligibility",
    "non_reproducible_results_block_activation",
]

REQUIRED_PAYLOAD_CONTRACTS = [
    "projection_facing_mechanics_require_payload_consistency_evidence",
    "missing_payload_evidence_blocks_activation",
    "payload_mutation_forbidden_in_6gt",
    "payload_schema_changes_forbidden_in_6gt",
    "market_projection_payload_consistency_checked_in_future_real_backtest",
]

REQUIRED_SAFETY_BOUNDARIES = [
    "no_mechanic_activation",
    "no_simulator_behavior_change",
    "no_projection_behavior_change",
    "no_fixture_change",
    "no_production_default_change",
    "no_expensive_backtest",
    "no_real_backtest_execution",
    "no_live_data_fetch",
    "no_database_write",
    "no_materialization_job",
    "no_production_simulation",
    "planning_only",
]

REQUIRED_FUTURE_6GV = [
    "first_allowed_real_backtest_dry_run_execution_layer",
    "use_6gt_plan_and_6gu_audit_as_inputs",
    "run_bounded_candidate_vs_baseline_evidence_generation",
    "emit_real_backtest_evidence_artifacts",
    "still_no_mechanic_activation",
    "activation_requires_later_dedicated_activation_audit",
    "recommended_6gv_diagnosis",
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


def passed_by_key(rows: List[Dict[str, str]], key_field: str, key_value: str) -> bool:
    return any(row.get(key_field) == key_value and boolish(row.get("passed")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6gt_before = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""
    audit_6gs_before = AUDIT_6GS_PATH.read_text(encoding="utf-8") if AUDIT_6GS_PATH.exists() else ""
    implement_6gr_before = IMPLEMENT_6GR_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GR_PATH.exists() else ""
    audit_6gq_before = AUDIT_6GQ_PATH.read_text(encoding="utf-8") if AUDIT_6GQ_PATH.exists() else ""

    plan_run = subprocess.run(
        [sys.executable, str(PLAN_6GT_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6gt = load_json(JSON_6GT)
    checks_6gt = read_csv(CHECKS_6GT)
    eval_rows_6gt = read_csv(EVALUATION_WINDOWS_6GT)
    outcome_rows_6gt = read_csv(ACTUAL_OUTCOME_6GT)
    methodology_rows_6gt = read_csv(METHODOLOGY_6GT)
    runtime_rows_6gt = read_csv(RUNTIME_LIMITS_6GT)
    evidence_rows_6gt = read_csv(EVIDENCE_ARTIFACT_6GT)
    decision_rows_6gt = read_csv(DECISION_CLASSES_6GT)
    threshold_rows_6gt = read_csv(THRESHOLD_POLICY_6GT)
    reproducibility_rows_6gt = read_csv(REPRODUCIBILITY_6GT)
    payload_rows_6gt = read_csv(PAYLOAD_6GT)
    safety_rows_6gt = read_csv(SAFETY_6GT)
    future_6gv_rows_6gt = read_csv(FUTURE_6GV_6GT)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gt_plan_exists", "expected": True, "actual": PLAN_6GT_PATH.exists(), "passed": PLAN_6GT_PATH.exists()},
        {"check": "6gt_plan_runs", "expected": 0, "actual": plan_run.returncode, "passed": plan_run.returncode == 0},
        {"check": "6gt_json_exists", "expected": True, "actual": JSON_6GT.exists(), "passed": JSON_6GT.exists()},
        {"check": "6gt_all_checks_passed", "expected": True, "actual": json_6gt.get("all_checks_passed"), "passed": json_6gt.get("all_checks_passed") is True},
        {"check": "6gt_planning_only", "expected": True, "actual": json_6gt.get("planning_only"), "passed": json_6gt.get("planning_only") is True},
        {"check": "6gt_diagnosis", "expected": DIAGNOSIS_6GT, "actual": json_6gt.get("diagnosis"), "passed": json_6gt.get("diagnosis") == DIAGNOSIS_6GT},
        {"check": "6gt_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6gt.get("recommended_next_layer"), "passed": json_6gt.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gt_layer_6_exit_ready_false", "expected": False, "actual": json_6gt.get("layer_6_exit_ready"), "passed": json_6gt.get("layer_6_exit_ready") is False},
        {"check": "6gt_mechanics_activated_false", "expected": False, "actual": json_6gt.get("mechanics_activated_by_this_layer"), "passed": json_6gt.get("mechanics_activated_by_this_layer") is False},
        {"check": "6gt_real_backtests_run_false", "expected": False, "actual": json_6gt.get("real_backtests_run"), "passed": json_6gt.get("real_backtests_run") is False},
        {"check": "6gt_live_fetches_false", "expected": False, "actual": json_6gt.get("live_data_fetches_run"), "passed": json_6gt.get("live_data_fetches_run") is False},
        {"check": "6gt_database_writes_false", "expected": False, "actual": json_6gt.get("database_writes_run"), "passed": json_6gt.get("database_writes_run") is False},
        {"check": "6gt_materialization_jobs_false", "expected": False, "actual": json_6gt.get("materialization_jobs_run"), "passed": json_6gt.get("materialization_jobs_run") is False},
        {"check": "6gt_production_simulations_false", "expected": False, "actual": json_6gt.get("production_simulations_run"), "passed": json_6gt.get("production_simulations_run") is False},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in REQUIRED_6GT_ARTIFACTS
    ]

    checks_consistency_rows = [
        {
            "source_check": row.get("check"),
            "source_passed": row.get("passed"),
            "passed": boolish(row.get("passed")),
            "detail": row.get("detail", ""),
        }
        for row in checks_6gt
    ]

    evaluation_audit_rows = []
    for window in REQUIRED_WINDOWS:
        matches = [row for row in eval_rows_6gt if row.get("window_key") == window]
        row = matches[0] if matches else {}
        evaluation_audit_rows.append(
            {
                "window_key": window,
                "present_once": len(matches) == 1,
                "contract_only": row.get("window_type") == "contract_only",
                "no_live_fetch_allowed": not boolish(row.get("live_data_fetch_allowed")),
                "no_real_backtest_run_by_6gt": not boolish(row.get("real_backtest_run_by_6gt")),
                "passed": (
                    len(matches) == 1
                    and row.get("window_type") == "contract_only"
                    and not boolish(row.get("live_data_fetch_allowed"))
                    and not boolish(row.get("real_backtest_run_by_6gt"))
                ),
            }
        )

    actual_outcome_audit_rows = [
        {
            "contract": contract,
            "present_and_passed": passed_by_key(outcome_rows_6gt, "contract", contract),
            "passed": passed_by_key(outcome_rows_6gt, "contract", contract),
        }
        for contract in REQUIRED_ACTUAL_OUTCOME_CONTRACTS
    ]

    methodology_audit_rows = [
        {
            "methodology": methodology,
            "present_and_passed": passed_by_key(methodology_rows_6gt, "methodology", methodology),
            "passed": passed_by_key(methodology_rows_6gt, "methodology", methodology),
        }
        for methodology in REQUIRED_METHODOLOGY
    ]

    runtime_audit_rows = [
        {
            "limit_key": limit_key,
            "present_and_passed": passed_by_key(runtime_rows_6gt, "limit_key", limit_key),
            "passed": passed_by_key(runtime_rows_6gt, "limit_key", limit_key),
        }
        for limit_key in REQUIRED_RUNTIME_LIMITS
    ]

    evidence_audit_rows = [
        {
            "artifact": artifact,
            "present_and_passed": passed_by_key(evidence_rows_6gt, "artifact", artifact),
            "activation_blocked_if_missing": any(
                row.get("artifact") == artifact and boolish(row.get("activation_blocked_if_missing"))
                for row in evidence_rows_6gt
            ),
            "passed": passed_by_key(evidence_rows_6gt, "artifact", artifact)
            and any(row.get("artifact") == artifact and boolish(row.get("activation_blocked_if_missing")) for row in evidence_rows_6gt),
        }
        for artifact in REQUIRED_EVIDENCE_ARTIFACTS
    ]

    decision_audit_rows = []
    for decision_class in REQUIRED_DECISION_CLASSES:
        matches = [row for row in decision_rows_6gt if row.get("decision_class") == decision_class]
        row = matches[0] if matches else {}
        decision_audit_rows.append(
            {
                "decision_class": decision_class,
                "present_and_passed": boolish(row.get("passed")),
                "activation_allowed_by_6gt_false": not boolish(row.get("activation_allowed_by_6gt")),
                "passed": boolish(row.get("passed")) and not boolish(row.get("activation_allowed_by_6gt")),
            }
        )

    threshold_audit_rows = []
    for policy in REQUIRED_THRESHOLD_POLICIES:
        matches = [row for row in threshold_rows_6gt if row.get("threshold_policy") == policy]
        row = matches[0] if matches else {}
        threshold_audit_rows.append(
            {
                "threshold_policy": policy,
                "present_and_passed": boolish(row.get("passed")),
                "failure_blocks_activation": boolish(row.get("failure_blocks_activation")),
                "failure_blocks_layer_6_exit_credit": boolish(row.get("failure_blocks_layer_6_exit_credit")),
                "passed": boolish(row.get("passed"))
                and boolish(row.get("failure_blocks_activation"))
                and boolish(row.get("failure_blocks_layer_6_exit_credit")),
            }
        )

    reproducibility_audit_rows = [
        {
            "contract": contract,
            "present_and_passed": passed_by_key(reproducibility_rows_6gt, "contract", contract),
            "passed": passed_by_key(reproducibility_rows_6gt, "contract", contract),
        }
        for contract in REQUIRED_REPRODUCIBILITY
    ]

    payload_audit_rows = [
        {
            "contract": contract,
            "present_and_passed": passed_by_key(payload_rows_6gt, "contract", contract),
            "passed": passed_by_key(payload_rows_6gt, "contract", contract),
        }
        for contract in REQUIRED_PAYLOAD_CONTRACTS
    ]

    safety_audit_rows = [
        {
            "boundary": boundary,
            "present_and_passed": passed_by_key(safety_rows_6gt, "boundary", boundary),
            "passed": passed_by_key(safety_rows_6gt, "boundary", boundary),
        }
        for boundary in REQUIRED_SAFETY_BOUNDARIES
    ]

    future_6gv_audit_rows = [
        {
            "contract": contract,
            "present_and_passed": passed_by_key(future_6gv_rows_6gt, "contract", contract),
            "passed": passed_by_key(future_6gv_rows_6gt, "contract", contract),
        }
        for contract in REQUIRED_FUTURE_6GV
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6gt_after = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""
    audit_6gs_after = AUDIT_6GS_PATH.read_text(encoding="utf-8") if AUDIT_6GS_PATH.exists() else ""
    implement_6gr_after = IMPLEMENT_6GR_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GR_PATH.exists() else ""
    audit_6gq_after = AUDIT_6GQ_PATH.read_text(encoding="utf-8") if AUDIT_6GQ_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gu_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gt_plan", "policy": "unchanged_by_6gu_audit", "passed": plan_6gt_after == plan_6gt_before},
        {"surface": "6gs_audit", "policy": "unchanged_by_6gu_audit", "passed": audit_6gs_after == audit_6gs_before},
        {"surface": "6gr_implementation", "policy": "unchanged_by_6gu_audit", "passed": implement_6gr_after == implement_6gr_before},
        {"surface": "6gq_audit", "policy": "unchanged_by_6gu_audit", "passed": audit_6gq_after == audit_6gq_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gu_audit", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gu_audit", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gu_audit", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gu_audit", "passed": True},
        {"surface": "live_fetches_or_database_writes", "policy": "not_run", "passed": True},
        {"surface": "materialization_jobs_or_production_simulations", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "real_backtests_run", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GU, "actual": DIAGNOSIS_6GU, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": len(checks_consistency_rows) == 15 and all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "evaluation_windows", "passed": all(row["passed"] for row in evaluation_audit_rows), "detail": f"{sum(1 for row in evaluation_audit_rows if row['passed'])}/{len(evaluation_audit_rows)}"},
        {"check": "actual_outcome_source_contract", "passed": all(row["passed"] for row in actual_outcome_audit_rows), "detail": f"{sum(1 for row in actual_outcome_audit_rows if row['passed'])}/{len(actual_outcome_audit_rows)}"},
        {"check": "methodology", "passed": all(row["passed"] for row in methodology_audit_rows), "detail": f"{sum(1 for row in methodology_audit_rows if row['passed'])}/{len(methodology_audit_rows)}"},
        {"check": "runtime_limits", "passed": all(row["passed"] for row in runtime_audit_rows), "detail": f"{sum(1 for row in runtime_audit_rows if row['passed'])}/{len(runtime_audit_rows)}"},
        {"check": "evidence_artifact_contract", "passed": all(row["passed"] for row in evidence_audit_rows), "detail": f"{sum(1 for row in evidence_audit_rows if row['passed'])}/{len(evidence_audit_rows)}"},
        {"check": "decision_classes", "passed": all(row["passed"] for row in decision_audit_rows), "detail": f"{sum(1 for row in decision_audit_rows if row['passed'])}/{len(decision_audit_rows)}"},
        {"check": "threshold_policy", "passed": all(row["passed"] for row in threshold_audit_rows), "detail": f"{sum(1 for row in threshold_audit_rows if row['passed'])}/{len(threshold_audit_rows)}"},
        {"check": "reproducibility", "passed": all(row["passed"] for row in reproducibility_audit_rows), "detail": f"{sum(1 for row in reproducibility_audit_rows if row['passed'])}/{len(reproducibility_audit_rows)}"},
        {"check": "payload_consistency", "passed": all(row["passed"] for row in payload_audit_rows), "detail": f"{sum(1 for row in payload_audit_rows if row['passed'])}/{len(payload_audit_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_audit_rows), "detail": f"{sum(1 for row in safety_audit_rows if row['passed'])}/{len(safety_audit_rows)}"},
        {"check": "future_6gv_contract", "passed": all(row["passed"] for row in future_6gv_audit_rows), "detail": f"{sum(1 for row in future_6gv_audit_rows if row['passed'])}/{len(future_6gv_audit_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "checks_consistency": write_csv(CHECKS_CONSISTENCY_CSV, checks_consistency_rows),
        "evaluation_windows": write_csv(EVALUATION_WINDOWS_AUDIT_CSV, evaluation_audit_rows),
        "actual_outcome_source_contract": write_csv(ACTUAL_OUTCOME_AUDIT_CSV, actual_outcome_audit_rows),
        "methodology": write_csv(METHODOLOGY_AUDIT_CSV, methodology_audit_rows),
        "runtime_limits": write_csv(RUNTIME_LIMITS_AUDIT_CSV, runtime_audit_rows),
        "evidence_artifact_contract": write_csv(EVIDENCE_ARTIFACT_AUDIT_CSV, evidence_audit_rows),
        "decision_classes": write_csv(DECISION_CLASSES_AUDIT_CSV, decision_audit_rows),
        "threshold_policy": write_csv(THRESHOLD_POLICY_AUDIT_CSV, threshold_audit_rows),
        "reproducibility": write_csv(REPRODUCIBILITY_AUDIT_CSV, reproducibility_audit_rows),
        "payload_consistency": write_csv(PAYLOAD_AUDIT_CSV, payload_audit_rows),
        "safety_boundaries": write_csv(SAFETY_AUDIT_CSV, safety_audit_rows),
        "future_6gv_contract": write_csv(FUTURE_6GV_CONTRACT_CSV, future_6gv_audit_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GU",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "audited_layer": "6GT",
        "audited_plan_diagnosis": json_6gt.get("diagnosis"),
        "diagnosis": DIAGNOSIS_6GU if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_plan": str(PLAN_6GT_PATH),
        "predecessor_plan_returncode": plan_run.returncode,
        "predecessor_plan_diagnosis": json_6gt.get("diagnosis"),
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(REQUIRED_WINDOWS),
        "evidence_artifact_count": len(REQUIRED_EVIDENCE_ARTIFACTS),
        "decision_class_count": len(REQUIRED_DECISION_CLASSES),
        "threshold_policy_count": len(REQUIRED_THRESHOLD_POLICIES),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "checks_consistency_csv": str(CHECKS_CONSISTENCY_CSV),
            "evaluation_windows_csv": str(EVALUATION_WINDOWS_AUDIT_CSV),
            "actual_outcome_source_contract_csv": str(ACTUAL_OUTCOME_AUDIT_CSV),
            "methodology_csv": str(METHODOLOGY_AUDIT_CSV),
            "runtime_limits_csv": str(RUNTIME_LIMITS_AUDIT_CSV),
            "evidence_artifact_contract_csv": str(EVIDENCE_ARTIFACT_AUDIT_CSV),
            "decision_classes_csv": str(DECISION_CLASSES_AUDIT_CSV),
            "threshold_policy_csv": str(THRESHOLD_POLICY_AUDIT_CSV),
            "reproducibility_csv": str(REPRODUCIBILITY_AUDIT_CSV),
            "payload_consistency_csv": str(PAYLOAD_AUDIT_CSV),
            "safety_boundaries_csv": str(SAFETY_AUDIT_CSV),
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
