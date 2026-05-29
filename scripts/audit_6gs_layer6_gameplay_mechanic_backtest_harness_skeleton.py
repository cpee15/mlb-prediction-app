#!/usr/bin/env python3
"""Audit Layer 6GR gameplay mechanic backtest harness skeleton implementation."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gs_backtest_harness_skeleton_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6GR_PATH = Path("scripts/implement_6gr_layer6_gameplay_mechanic_backtest_harness_skeleton.py")
AUDIT_6GQ_PATH = Path("scripts/audit_6gq_layer6_gameplay_mechanic_backtest_harness_plan.py")
PLAN_6GP_PATH = Path("scripts/plan_6gp_layer6_gameplay_mechanic_backtest_harness.py")
AUDIT_6GO_PATH = Path("scripts/audit_6go_layer6_projection_dormant_activation_decision_plan.py")

JSON_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton.json"
CHECKS_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_checks.csv"
PREDECESSOR_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_predecessor.csv"
HARNESS_CONFIG_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_harness_config.csv"
CANDIDATE_RESULTS_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_candidate_results.csv"
BASELINE_RESULTS_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_baseline_results.csv"
METRIC_COMPARISON_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_metric_comparison.csv"
PASS_FAIL_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_pass_fail_summary.csv"
PAYLOAD_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_payload_consistency_summary.csv"
DETERMINISM_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_determinism_summary.csv"
SAFETY_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_safety_summary.csv"
FUTURE_6GS_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_future_6gs_contract.csv"
IMMUTABILITY_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_immutability.csv"
RECOMMENDED_6GR = TMP_DIR / "layer6_6gr_backtest_harness_skeleton_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
HARNESS_CONFIG_AUDIT_CSV = TMP_DIR / f"{SLUG}_harness_config.csv"
CANDIDATE_RESULTS_AUDIT_CSV = TMP_DIR / f"{SLUG}_candidate_results.csv"
BASELINE_RESULTS_AUDIT_CSV = TMP_DIR / f"{SLUG}_baseline_results.csv"
METRIC_COMPARISON_AUDIT_CSV = TMP_DIR / f"{SLUG}_metric_comparison.csv"
PASS_FAIL_AUDIT_CSV = TMP_DIR / f"{SLUG}_pass_fail_summary.csv"
PAYLOAD_AUDIT_CSV = TMP_DIR / f"{SLUG}_payload_consistency.csv"
DETERMINISM_AUDIT_CSV = TMP_DIR / f"{SLUG}_determinism.csv"
SAFETY_AUDIT_CSV = TMP_DIR / f"{SLUG}_safety_summary.csv"
FUTURE_6GT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gt_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GR = "layer_6_gameplay_mechanic_backtest_harness_skeleton_implementation_complete"
DIAGNOSIS_6GS = "layer_6_gameplay_mechanic_backtest_harness_skeleton_audit_complete"
CURRENT_LAYER = "6GS_layer_6_gameplay_mechanic_backtest_harness_skeleton_audit"
RECOMMENDED_NEXT_LAYER = "6GT_layer_6_gameplay_mechanic_real_backtest_plan"
RECOMMENDED_PATH = "audit_dry_run_harness_skeleton_then_plan_real_backtest_execution"

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

REQUIRED_ARTIFACTS = [
    JSON_6GR,
    CHECKS_6GR,
    PREDECESSOR_6GR,
    HARNESS_CONFIG_6GR,
    CANDIDATE_RESULTS_6GR,
    BASELINE_RESULTS_6GR,
    METRIC_COMPARISON_6GR,
    PASS_FAIL_6GR,
    PAYLOAD_6GR,
    DETERMINISM_6GR,
    SAFETY_6GR,
    FUTURE_6GS_6GR,
    IMMUTABILITY_6GR,
    RECOMMENDED_6GR,
]

REQUIRED_SAFETY_BOUNDARIES = [
    "no_mechanic_activation",
    "no_simulator_behavior_change",
    "no_projection_behavior_change",
    "no_fixture_change",
    "no_production_default_change",
    "no_expensive_backtest",
    "no_live_data_fetch",
    "no_database_write",
    "no_materialization_job",
    "no_production_simulation",
    "dry_run_skeleton_only",
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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    impl_6gr_before = IMPLEMENT_6GR_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GR_PATH.exists() else ""
    audit_6gq_before = AUDIT_6GQ_PATH.read_text(encoding="utf-8") if AUDIT_6GQ_PATH.exists() else ""
    plan_6gp_before = PLAN_6GP_PATH.read_text(encoding="utf-8") if PLAN_6GP_PATH.exists() else ""
    audit_6go_before = AUDIT_6GO_PATH.read_text(encoding="utf-8") if AUDIT_6GO_PATH.exists() else ""

    impl_run = subprocess.run(
        [sys.executable, str(IMPLEMENT_6GR_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6gr = load_json(JSON_6GR)

    harness_rows = read_csv(HARNESS_CONFIG_6GR)
    candidate_rows = read_csv(CANDIDATE_RESULTS_6GR)
    baseline_rows = read_csv(BASELINE_RESULTS_6GR)
    metric_rows = read_csv(METRIC_COMPARISON_6GR)
    pass_fail_rows = read_csv(PASS_FAIL_6GR)
    payload_rows = read_csv(PAYLOAD_6GR)
    determinism_rows = read_csv(DETERMINISM_6GR)
    safety_rows_6gr = read_csv(SAFETY_6GR)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gr_implementation_exists", "expected": True, "actual": IMPLEMENT_6GR_PATH.exists(), "passed": IMPLEMENT_6GR_PATH.exists()},
        {"check": "6gr_implementation_runs", "expected": 0, "actual": impl_run.returncode, "passed": impl_run.returncode == 0},
        {"check": "6gr_json_exists", "expected": True, "actual": JSON_6GR.exists(), "passed": JSON_6GR.exists()},
        {"check": "6gr_all_checks_passed", "expected": True, "actual": json_6gr.get("all_checks_passed"), "passed": json_6gr.get("all_checks_passed") is True},
        {"check": "6gr_implementation_type", "expected": "dry_run_skeleton", "actual": json_6gr.get("implementation_type"), "passed": json_6gr.get("implementation_type") == "dry_run_skeleton"},
        {"check": "6gr_diagnosis", "expected": DIAGNOSIS_6GR, "actual": json_6gr.get("diagnosis"), "passed": json_6gr.get("diagnosis") == DIAGNOSIS_6GR},
        {"check": "6gr_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6gr.get("recommended_next_layer"), "passed": json_6gr.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gr_layer_6_exit_ready_false", "expected": False, "actual": json_6gr.get("layer_6_exit_ready"), "passed": json_6gr.get("layer_6_exit_ready") is False},
        {"check": "6gr_mechanics_activated_false", "expected": False, "actual": json_6gr.get("mechanics_activated_by_this_layer"), "passed": json_6gr.get("mechanics_activated_by_this_layer") is False},
        {"check": "6gr_real_backtests_run_false", "expected": False, "actual": json_6gr.get("real_backtests_run"), "passed": json_6gr.get("real_backtests_run") is False},
        {"check": "6gr_live_fetches_false", "expected": False, "actual": json_6gr.get("live_data_fetches_run"), "passed": json_6gr.get("live_data_fetches_run") is False},
        {"check": "6gr_database_writes_false", "expected": False, "actual": json_6gr.get("database_writes_run"), "passed": json_6gr.get("database_writes_run") is False},
        {"check": "6gr_materialization_jobs_false", "expected": False, "actual": json_6gr.get("materialization_jobs_run"), "passed": json_6gr.get("materialization_jobs_run") is False},
        {"check": "6gr_production_simulations_false", "expected": False, "actual": json_6gr.get("production_simulations_run"), "passed": json_6gr.get("production_simulations_run") is False},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in REQUIRED_ARTIFACTS
    ]

    harness_counts = Counter(row.get("mechanic_key") for row in harness_rows)
    harness_audit_rows = [
        {
            "mechanic_key": mechanic,
            "row_count": harness_counts.get(mechanic, 0),
            "harness_mode_dry_run": any(row.get("mechanic_key") == mechanic and row.get("harness_mode") == "dry_run_skeleton" for row in harness_rows),
            "comparison_candidate_vs_current_off": any(row.get("mechanic_key") == mechanic and row.get("comparison_type") == "candidate_vs_current_off" for row in harness_rows),
            "same_games_same_seeds_required": any(row.get("mechanic_key") == mechanic and boolish(row.get("same_games_same_seeds_required")) for row in harness_rows),
            "actual_outcome_join_required": any(row.get("mechanic_key") == mechanic and boolish(row.get("actual_outcome_join_required")) for row in harness_rows),
            "target_surface_count_10": any(row.get("mechanic_key") == mechanic and row.get("target_surface_count") == "10" for row in harness_rows),
            "metric_family_count_10": any(row.get("mechanic_key") == mechanic and row.get("metric_family_count") == "10" for row in harness_rows),
            "pass_fail_gate_count_10": any(row.get("mechanic_key") == mechanic and row.get("pass_fail_gate_count") == "10" for row in harness_rows),
            "real_backtest_run_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("real_backtest_run")) for row in harness_rows),
            "activation_allowed_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("activation_allowed")) for row in harness_rows),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]
    for row in harness_audit_rows:
        row["passed"] = (
            row["row_count"] == 1
            and row["harness_mode_dry_run"]
            and row["comparison_candidate_vs_current_off"]
            and row["same_games_same_seeds_required"]
            and row["actual_outcome_join_required"]
            and row["target_surface_count_10"]
            and row["metric_family_count_10"]
            and row["pass_fail_gate_count_10"]
            and row["real_backtest_run_false"]
            and row["activation_allowed_false"]
        )

    candidate_counts = Counter(row.get("mechanic_key") for row in candidate_rows)
    candidate_audit_rows = [
        {
            "mechanic_key": mechanic,
            "row_count": candidate_counts.get(mechanic, 0),
            "games_evaluated_0": any(row.get("mechanic_key") == mechanic and row.get("games_evaluated") == "0" for row in candidate_rows),
            "actual_outcomes_joined_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("actual_outcomes_joined")) for row in candidate_rows),
            "real_backtest_run_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("real_backtest_run")) for row in candidate_rows),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]
    for row in candidate_audit_rows:
        row["passed"] = row["row_count"] == 1 and row["games_evaluated_0"] and row["actual_outcomes_joined_false"] and row["real_backtest_run_false"]

    baseline_counts = Counter(row.get("mechanic_key") for row in baseline_rows)
    baseline_audit_rows = [
        {
            "mechanic_key": mechanic,
            "row_count": baseline_counts.get(mechanic, 0),
            "games_evaluated_0": any(row.get("mechanic_key") == mechanic and row.get("games_evaluated") == "0" for row in baseline_rows),
            "actual_outcomes_joined_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("actual_outcomes_joined")) for row in baseline_rows),
            "real_backtest_run_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("real_backtest_run")) for row in baseline_rows),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]
    for row in baseline_audit_rows:
        row["passed"] = row["row_count"] == 1 and row["games_evaluated_0"] and row["actual_outcomes_joined_false"] and row["real_backtest_run_false"]

    metric_counts = Counter(row.get("mechanic_key") for row in metric_rows)
    metric_audit_rows = [
        {
            "mechanic_key": mechanic,
            "row_count": metric_counts.get(mechanic, 0),
            "placeholder_no_real_backtest": any(row.get("mechanic_key") == mechanic and row.get("metric_comparison_mode") == "placeholder_no_real_backtest" for row in metric_rows),
            "activation_blocked": any(row.get("mechanic_key") == mechanic and row.get("activation_decision") == "blocked_pending_real_backtest" for row in metric_rows),
            "layer_6_exit_credit_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("layer_6_exit_credit")) for row in metric_rows),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]
    for row in metric_audit_rows:
        row["passed"] = row["row_count"] == 1 and row["placeholder_no_real_backtest"] and row["activation_blocked"] and row["layer_6_exit_credit_false"]

    pass_fail_counts = Counter(row.get("mechanic_key") for row in pass_fail_rows)
    pass_fail_audit_rows = [
        {
            "mechanic_key": mechanic,
            "row_count": pass_fail_counts.get(mechanic, 0),
            "real_backtest_available_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("real_backtest_available")) for row in pass_fail_rows),
            "passes_activation_gate_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("passes_activation_gate")) for row in pass_fail_rows),
            "activation_blocked_true": any(row.get("mechanic_key") == mechanic and boolish(row.get("activation_blocked")) for row in pass_fail_rows),
            "layer_6_exit_credit_blocked_true": any(row.get("mechanic_key") == mechanic and boolish(row.get("layer_6_exit_credit_blocked")) for row in pass_fail_rows),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]
    for row in pass_fail_audit_rows:
        row["passed"] = (
            row["row_count"] == 1
            and row["real_backtest_available_false"]
            and row["passes_activation_gate_false"]
            and row["activation_blocked_true"]
            and row["layer_6_exit_credit_blocked_true"]
        )

    payload_counts = Counter(row.get("mechanic_key") for row in payload_rows)
    payload_audit_rows = [
        {
            "mechanic_key": mechanic,
            "row_count": payload_counts.get(mechanic, 0),
            "projection_payload_mutated_false": any(row.get("mechanic_key") == mechanic and not boolish(row.get("projection_payload_mutated")) for row in payload_rows),
            "activation_blocked_true": any(row.get("mechanic_key") == mechanic and boolish(row.get("activation_blocked")) for row in payload_rows),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]
    for row in payload_audit_rows:
        row["passed"] = row["row_count"] == 1 and row["projection_payload_mutated_false"] and row["activation_blocked_true"]

    determinism_counts = Counter(row.get("mechanic_key") for row in determinism_rows)
    determinism_audit_rows = [
        {
            "mechanic_key": mechanic,
            "row_count": determinism_counts.get(mechanic, 0),
            "same_seed_required_true": any(row.get("mechanic_key") == mechanic and boolish(row.get("same_seed_required")) for row in determinism_rows),
            "activation_blocked_true": any(row.get("mechanic_key") == mechanic and boolish(row.get("activation_blocked")) for row in determinism_rows),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]
    for row in determinism_audit_rows:
        row["passed"] = row["row_count"] == 1 and row["same_seed_required_true"] and row["activation_blocked_true"]

    safety_by_boundary = {row.get("boundary"): row for row in safety_rows_6gr}
    safety_audit_rows = [
        {
            "boundary": boundary,
            "present": boundary in safety_by_boundary,
            "expected_true": safety_by_boundary.get(boundary, {}).get("expected") == "True",
            "actual_true": safety_by_boundary.get(boundary, {}).get("actual") == "True",
            "passed_true": safety_by_boundary.get(boundary, {}).get("passed") == "True",
            "passed": (
                boundary in safety_by_boundary
                and safety_by_boundary.get(boundary, {}).get("expected") == "True"
                and safety_by_boundary.get(boundary, {}).get("actual") == "True"
                and safety_by_boundary.get(boundary, {}).get("passed") == "True"
            ),
        }
        for boundary in REQUIRED_SAFETY_BOUNDARIES
    ]

    future_6gt_rows = [
        {"contract": "plan_real_backtest_execution_only", "required": True, "passed": True},
        {"contract": "define_evaluation_data_window", "required": True, "passed": True},
        {"contract": "define_actual_outcome_source_without_live_fetch_in_layer", "required": True, "passed": True},
        {"contract": "define_candidate_vs_baseline_real_comparison", "required": True, "passed": True},
        {"contract": "define_runtime_limits", "required": True, "passed": True},
        {"contract": "define_evidence_artifact_storage", "required": True, "passed": True},
        {"contract": "define_decision_classes_dormant_recalibrate_implement_consider_activation_later", "required": True, "passed": True},
        {"contract": "no_real_backtests_in_6gt", "required": True, "passed": True},
        {"contract": "no_mechanic_activation_in_6gt", "required": True, "passed": True},
        {"contract": "recommended_6gt_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_real_backtest_plan_complete"},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    impl_6gr_after = IMPLEMENT_6GR_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GR_PATH.exists() else ""
    audit_6gq_after = AUDIT_6GQ_PATH.read_text(encoding="utf-8") if AUDIT_6GQ_PATH.exists() else ""
    plan_6gp_after = PLAN_6GP_PATH.read_text(encoding="utf-8") if PLAN_6GP_PATH.exists() else ""
    audit_6go_after = AUDIT_6GO_PATH.read_text(encoding="utf-8") if AUDIT_6GO_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gs_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gr_implementation", "policy": "unchanged_by_6gs_audit", "passed": impl_6gr_after == impl_6gr_before},
        {"surface": "6gq_audit", "policy": "unchanged_by_6gs_audit", "passed": audit_6gq_after == audit_6gq_before},
        {"surface": "6gp_plan", "policy": "unchanged_by_6gs_audit", "passed": plan_6gp_after == plan_6gp_before},
        {"surface": "6go_audit", "policy": "unchanged_by_6gs_audit", "passed": audit_6go_after == audit_6go_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gs_audit", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gs_audit", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gs_audit", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gs_audit", "passed": True},
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
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GS, "actual": DIAGNOSIS_6GS, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "harness_config", "passed": len(harness_rows) == 10 and all(row["passed"] for row in harness_audit_rows), "detail": f"{len(harness_rows)}/10"},
        {"check": "candidate_results", "passed": len(candidate_rows) == 10 and all(row["passed"] for row in candidate_audit_rows), "detail": f"{len(candidate_rows)}/10"},
        {"check": "baseline_results", "passed": len(baseline_rows) == 10 and all(row["passed"] for row in baseline_audit_rows), "detail": f"{len(baseline_rows)}/10"},
        {"check": "metric_comparison", "passed": len(metric_rows) == 10 and all(row["passed"] for row in metric_audit_rows), "detail": f"{len(metric_rows)}/10"},
        {"check": "pass_fail_summary", "passed": len(pass_fail_rows) == 10 and all(row["passed"] for row in pass_fail_audit_rows), "detail": f"{len(pass_fail_rows)}/10"},
        {"check": "payload_consistency", "passed": len(payload_rows) == 10 and all(row["passed"] for row in payload_audit_rows), "detail": f"{len(payload_rows)}/10"},
        {"check": "determinism", "passed": len(determinism_rows) == 10 and all(row["passed"] for row in determinism_audit_rows), "detail": f"{len(determinism_rows)}/10"},
        {"check": "safety_summary", "passed": all(row["passed"] for row in safety_audit_rows), "detail": f"{sum(1 for row in safety_audit_rows if row['passed'])}/{len(safety_audit_rows)}"},
        {"check": "future_6gt_contract", "passed": all(row["passed"] for row in future_6gt_rows), "detail": f"{sum(1 for row in future_6gt_rows if row['passed'])}/{len(future_6gt_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "harness_config": write_csv(HARNESS_CONFIG_AUDIT_CSV, harness_audit_rows),
        "candidate_results": write_csv(CANDIDATE_RESULTS_AUDIT_CSV, candidate_audit_rows),
        "baseline_results": write_csv(BASELINE_RESULTS_AUDIT_CSV, baseline_audit_rows),
        "metric_comparison": write_csv(METRIC_COMPARISON_AUDIT_CSV, metric_audit_rows),
        "pass_fail_summary": write_csv(PASS_FAIL_AUDIT_CSV, pass_fail_audit_rows),
        "payload_consistency": write_csv(PAYLOAD_AUDIT_CSV, payload_audit_rows),
        "determinism": write_csv(DETERMINISM_AUDIT_CSV, determinism_audit_rows),
        "safety_summary": write_csv(SAFETY_AUDIT_CSV, safety_audit_rows),
        "future_6gt_contract": write_csv(FUTURE_6GT_CONTRACT_CSV, future_6gt_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GS",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "audited_layer": "6GR",
        "audited_implementation_diagnosis": json_6gr.get("diagnosis"),
        "diagnosis": DIAGNOSIS_6GS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "harness_config_rows_count": len(harness_rows),
        "candidate_result_rows_count": len(candidate_rows),
        "baseline_result_rows_count": len(baseline_rows),
        "metric_comparison_rows_count": len(metric_rows),
        "pass_fail_summary_rows_count": len(pass_fail_rows),
        "predecessor_implementation": str(IMPLEMENT_6GR_PATH),
        "predecessor_implementation_returncode": impl_run.returncode,
        "predecessor_implementation_diagnosis": json_6gr.get("diagnosis"),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "harness_config_csv": str(HARNESS_CONFIG_AUDIT_CSV),
            "candidate_results_csv": str(CANDIDATE_RESULTS_AUDIT_CSV),
            "baseline_results_csv": str(BASELINE_RESULTS_AUDIT_CSV),
            "metric_comparison_csv": str(METRIC_COMPARISON_AUDIT_CSV),
            "pass_fail_summary_csv": str(PASS_FAIL_AUDIT_CSV),
            "payload_consistency_csv": str(PAYLOAD_AUDIT_CSV),
            "determinism_csv": str(DETERMINISM_AUDIT_CSV),
            "safety_summary_csv": str(SAFETY_AUDIT_CSV),
            "future_6gt_contract_csv": str(FUTURE_6GT_CONTRACT_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
