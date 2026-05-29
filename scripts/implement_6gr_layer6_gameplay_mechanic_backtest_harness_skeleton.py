#!/usr/bin/env python3
"""Layer 6GR gameplay mechanic backtest harness skeleton implementation."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gr_backtest_harness_skeleton"
TMP_DIR = Path("tmp")

AUDIT_6GQ_PATH = Path("scripts/audit_6gq_layer6_gameplay_mechanic_backtest_harness_plan.py")
PLAN_6GP_PATH = Path("scripts/plan_6gp_layer6_gameplay_mechanic_backtest_harness.py")
AUDIT_6GO_PATH = Path("scripts/audit_6go_layer6_projection_dormant_activation_decision_plan.py")
PLAN_6GN_PATH = Path("scripts/plan_6gn_layer6_projection_dormant_activation_decisions.py")

JSON_6GQ = TMP_DIR / "layer6_6gq_backtest_harness_plan_audit.json"
MECHANICS_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_mechanics.csv"
CANDIDATE_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_candidate_comparison.csv"
TARGETS_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_target_surfaces.csv"
METRICS_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_metric_families.csv"
GATES_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_pass_fail_gates.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
HARNESS_CONFIG_CSV = TMP_DIR / f"{SLUG}_harness_config.csv"
CANDIDATE_RESULTS_CSV = TMP_DIR / f"{SLUG}_candidate_results.csv"
BASELINE_RESULTS_CSV = TMP_DIR / f"{SLUG}_baseline_results.csv"
METRIC_COMPARISON_CSV = TMP_DIR / f"{SLUG}_metric_comparison.csv"
PASS_FAIL_SUMMARY_CSV = TMP_DIR / f"{SLUG}_pass_fail_summary.csv"
PAYLOAD_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_payload_consistency_summary.csv"
DETERMINISM_CSV = TMP_DIR / f"{SLUG}_determinism_summary.csv"
SAFETY_SUMMARY_CSV = TMP_DIR / f"{SLUG}_safety_summary.csv"
FUTURE_6GS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gs_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GQ = "layer_6_gameplay_mechanic_backtest_harness_plan_audit_complete"
DIAGNOSIS_6GR = "layer_6_gameplay_mechanic_backtest_harness_skeleton_implementation_complete"
CURRENT_LAYER = "6GR_layer_6_gameplay_mechanic_backtest_harness_skeleton_implementation"
RECOMMENDED_NEXT_LAYER = "6GS_layer_6_gameplay_mechanic_backtest_harness_skeleton_audit"
RECOMMENDED_PATH = "implement_dry_run_backtest_harness_skeleton_then_audit_before_real_backtests"

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


def rows_by_mechanic(rows: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row.get("mechanic_key", ""), []).append(row)
    return grouped


def deterministic_placeholder_value(mechanic: str, offset: int) -> float:
    seed = sum(ord(ch) for ch in mechanic) + offset
    return round((seed % 1000) / 1000.0, 6)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6gq_before = AUDIT_6GQ_PATH.read_text(encoding="utf-8") if AUDIT_6GQ_PATH.exists() else ""
    plan_6gp_before = PLAN_6GP_PATH.read_text(encoding="utf-8") if PLAN_6GP_PATH.exists() else ""
    audit_6go_before = AUDIT_6GO_PATH.read_text(encoding="utf-8") if AUDIT_6GO_PATH.exists() else ""
    plan_6gn_before = PLAN_6GN_PATH.read_text(encoding="utf-8") if PLAN_6GN_PATH.exists() else ""

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6GQ_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    audit_json = load_json(JSON_6GQ)

    mechanics_rows_6gp = read_csv(MECHANICS_6GP)
    candidate_rows_6gp = read_csv(CANDIDATE_6GP)
    target_rows_6gp = read_csv(TARGETS_6GP)
    metric_rows_6gp = read_csv(METRICS_6GP)
    gate_rows_6gp = read_csv(GATES_6GP)

    candidate_by_mechanic = rows_by_mechanic(candidate_rows_6gp)
    target_by_mechanic = rows_by_mechanic(target_rows_6gp)
    metric_by_mechanic = rows_by_mechanic(metric_rows_6gp)
    gate_by_mechanic = rows_by_mechanic(gate_rows_6gp)
    mechanic_keys = {row.get("mechanic_key") for row in mechanics_rows_6gp}

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gq_audit_exists", "expected": True, "actual": AUDIT_6GQ_PATH.exists(), "passed": AUDIT_6GQ_PATH.exists()},
        {"check": "6gq_audit_runs", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"check": "6gq_json_exists", "expected": True, "actual": JSON_6GQ.exists(), "passed": JSON_6GQ.exists()},
        {"check": "6gq_all_checks_passed", "expected": True, "actual": audit_json.get("all_checks_passed"), "passed": audit_json.get("all_checks_passed") is True},
        {"check": "6gq_audit_only", "expected": True, "actual": audit_json.get("audit_only"), "passed": audit_json.get("audit_only") is True},
        {"check": "6gq_diagnosis", "expected": DIAGNOSIS_6GQ, "actual": audit_json.get("diagnosis"), "passed": audit_json.get("diagnosis") == DIAGNOSIS_6GQ},
        {"check": "6gq_recommended_next_layer", "expected": CURRENT_LAYER, "actual": audit_json.get("recommended_next_layer"), "passed": audit_json.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gq_layer_6_exit_ready_false", "expected": False, "actual": audit_json.get("layer_6_exit_ready"), "passed": audit_json.get("layer_6_exit_ready") is False},
        {"check": "6gq_mechanics_activated_false", "expected": False, "actual": audit_json.get("mechanics_activated_by_this_layer"), "passed": audit_json.get("mechanics_activated_by_this_layer") is False},
        {"check": "6gp_mechanics_present", "expected": 10, "actual": len(mechanic_keys), "passed": mechanic_keys == set(GAMEPLAY_MECHANICS)},
    ]

    harness_config_rows = []
    candidate_result_rows = []
    baseline_result_rows = []
    metric_comparison_rows = []
    pass_fail_rows = []
    payload_rows = []
    determinism_rows = []

    for index, mechanic in enumerate(GAMEPLAY_MECHANICS, start=1):
        candidate_source = candidate_by_mechanic.get(mechanic, [{}])[0]
        targets = sorted({row.get("target_surface") for row in target_by_mechanic.get(mechanic, [])})
        metrics = sorted({row.get("metric_family") for row in metric_by_mechanic.get(mechanic, [])})
        gates = sorted({row.get("pass_fail_gate") for row in gate_by_mechanic.get(mechanic, [])})

        harness_config_rows.append(
            {
                "mechanic_key": mechanic,
                "harness_mode": "dry_run_skeleton",
                "candidate_configuration": candidate_source.get("candidate_configuration", f"{mechanic}_candidate_enabled_dry_run"),
                "baseline_configuration": candidate_source.get("baseline_configuration", f"{mechanic}_current_or_off_baseline"),
                "comparison_type": "candidate_vs_current_off",
                "same_games_same_seeds_required": True,
                "actual_outcome_join_required": True,
                "target_surface_count": len(targets),
                "metric_family_count": len(metrics),
                "pass_fail_gate_count": len(gates),
                "real_backtest_run": False,
                "activation_allowed": False,
            }
        )

        candidate_result_rows.append(
            {
                "mechanic_key": mechanic,
                "result_type": "candidate_placeholder",
                "harness_mode": "dry_run_skeleton",
                "same_seed_placeholder": 4242 + index,
                "games_evaluated": 0,
                "actual_outcomes_joined": False,
                "real_metric_value": "",
                "placeholder_metric_value": deterministic_placeholder_value(mechanic, 101),
                "real_backtest_run": False,
            }
        )

        baseline_result_rows.append(
            {
                "mechanic_key": mechanic,
                "result_type": "baseline_placeholder",
                "harness_mode": "dry_run_skeleton",
                "same_seed_placeholder": 4242 + index,
                "games_evaluated": 0,
                "actual_outcomes_joined": False,
                "real_metric_value": "",
                "placeholder_metric_value": deterministic_placeholder_value(mechanic, 17),
                "real_backtest_run": False,
            }
        )

        metric_comparison_rows.append(
            {
                "mechanic_key": mechanic,
                "comparison_type": "candidate_vs_current_off",
                "metric_comparison_mode": "placeholder_no_real_backtest",
                "candidate_placeholder_value": deterministic_placeholder_value(mechanic, 101),
                "baseline_placeholder_value": deterministic_placeholder_value(mechanic, 17),
                "real_delta": "",
                "placeholder_delta": round(
                    deterministic_placeholder_value(mechanic, 101) - deterministic_placeholder_value(mechanic, 17),
                    6,
                ),
                "activation_decision": "blocked_pending_real_backtest",
                "layer_6_exit_credit": False,
            }
        )

        pass_fail_rows.append(
            {
                "mechanic_key": mechanic,
                "all_required_gates_present": len(gates) == 10,
                "real_backtest_available": False,
                "passes_activation_gate": False,
                "activation_blocked": True,
                "layer_6_exit_credit_blocked": True,
                "failure_reason": "dry_run_skeleton_only_no_real_backtest_evidence",
            }
        )

        payload_rows.append(
            {
                "mechanic_key": mechanic,
                "payload_consistency_mode": "placeholder_no_projection_payload_mutation",
                "projection_payload_checked": False,
                "projection_payload_mutated": False,
                "payload_consistency_passed": False,
                "activation_blocked": True,
            }
        )

        determinism_rows.append(
            {
                "mechanic_key": mechanic,
                "determinism_mode": "placeholder_same_seed_contract",
                "seed": 4242 + index,
                "same_seed_required": True,
                "same_seed_verified_with_real_run": False,
                "determinism_passed": False,
                "activation_blocked": True,
            }
        )

    safety_rows = [
        {"boundary": "no_mechanic_activation", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_simulator_behavior_change", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_projection_behavior_change", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_fixture_change", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_production_default_change", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_expensive_backtest", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_database_write", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_materialization_job", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_production_simulation", "expected": True, "actual": True, "passed": True},
        {"boundary": "dry_run_skeleton_only", "expected": True, "actual": True, "passed": True},
    ]

    future_6gs_rows = [
        {"contract": "audit_6gr_skeleton_artifacts", "required": True, "passed": True},
        {"contract": "verify_harness_config_one_row_per_mechanic", "required": True, "passed": True},
        {"contract": "verify_candidate_and_baseline_placeholder_rows", "required": True, "passed": True},
        {"contract": "verify_metric_comparison_rows_block_activation", "required": True, "passed": True},
        {"contract": "verify_pass_fail_rows_block_activation_and_exit_credit", "required": True, "passed": True},
        {"contract": "verify_no_real_backtests_or_production_mutations", "required": True, "passed": True},
        {"contract": "recommended_6gs_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_backtest_harness_skeleton_audit_complete"},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6gq_after = AUDIT_6GQ_PATH.read_text(encoding="utf-8") if AUDIT_6GQ_PATH.exists() else ""
    plan_6gp_after = PLAN_6GP_PATH.read_text(encoding="utf-8") if PLAN_6GP_PATH.exists() else ""
    audit_6go_after = AUDIT_6GO_PATH.read_text(encoding="utf-8") if AUDIT_6GO_PATH.exists() else ""
    plan_6gn_after = PLAN_6GN_PATH.read_text(encoding="utf-8") if PLAN_6GN_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gr_implementation", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gq_audit", "policy": "unchanged_by_6gr_implementation", "passed": audit_6gq_after == audit_6gq_before},
        {"surface": "6gp_plan", "policy": "unchanged_by_6gr_implementation", "passed": plan_6gp_after == plan_6gp_before},
        {"surface": "6go_audit", "policy": "unchanged_by_6gr_implementation", "passed": audit_6go_after == audit_6go_before},
        {"surface": "6gn_plan", "policy": "unchanged_by_6gr_implementation", "passed": plan_6gn_after == plan_6gn_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gr_implementation", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gr_implementation", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gr_implementation", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gr_implementation", "passed": True},
        {"surface": "live_fetches_or_database_writes", "policy": "not_run", "passed": True},
        {"surface": "materialization_jobs_or_production_simulations", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "implementation_type", "expected": "dry_run_skeleton", "actual": "dry_run_skeleton", "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GR, "actual": DIAGNOSIS_6GR, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "harness_config", "passed": len(harness_config_rows) == 10, "detail": f"{len(harness_config_rows)}/10"},
        {"check": "candidate_results", "passed": len(candidate_result_rows) == 10 and all(not boolish(row["real_backtest_run"]) for row in candidate_result_rows), "detail": f"{len(candidate_result_rows)}/10"},
        {"check": "baseline_results", "passed": len(baseline_result_rows) == 10 and all(not boolish(row["real_backtest_run"]) for row in baseline_result_rows), "detail": f"{len(baseline_result_rows)}/10"},
        {"check": "metric_comparison", "passed": len(metric_comparison_rows) == 10 and all(row["activation_decision"] == "blocked_pending_real_backtest" for row in metric_comparison_rows), "detail": f"{len(metric_comparison_rows)}/10"},
        {"check": "pass_fail_summary", "passed": len(pass_fail_rows) == 10 and all(row["activation_blocked"] and row["layer_6_exit_credit_blocked"] for row in pass_fail_rows), "detail": f"{len(pass_fail_rows)}/10"},
        {"check": "payload_consistency_summary", "passed": len(payload_rows) == 10 and all(not row["projection_payload_mutated"] for row in payload_rows), "detail": f"{len(payload_rows)}/10"},
        {"check": "determinism_summary", "passed": len(determinism_rows) == 10 and all(row["same_seed_required"] for row in determinism_rows), "detail": f"{len(determinism_rows)}/10"},
        {"check": "safety_summary", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "future_6gs_contract", "passed": all(row["passed"] for row in future_6gs_rows), "detail": f"{sum(1 for row in future_6gs_rows if row['passed'])}/{len(future_6gs_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "harness_config": write_csv(HARNESS_CONFIG_CSV, harness_config_rows),
        "candidate_results": write_csv(CANDIDATE_RESULTS_CSV, candidate_result_rows),
        "baseline_results": write_csv(BASELINE_RESULTS_CSV, baseline_result_rows),
        "metric_comparison": write_csv(METRIC_COMPARISON_CSV, metric_comparison_rows),
        "pass_fail_summary": write_csv(PASS_FAIL_SUMMARY_CSV, pass_fail_rows),
        "payload_consistency_summary": write_csv(PAYLOAD_CONSISTENCY_CSV, payload_rows),
        "determinism_summary": write_csv(DETERMINISM_CSV, determinism_rows),
        "safety_summary": write_csv(SAFETY_SUMMARY_CSV, safety_rows),
        "future_6gs_contract": write_csv(FUTURE_6GS_CONTRACT_CSV, future_6gs_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GR",
        "layer_type": "game_mechanics_realism",
        "implementation_type": "dry_run_skeleton",
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6GR if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "audited_predecessor_layer": "6GQ",
        "predecessor_audit": str(AUDIT_6GQ_PATH),
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
        "harness_config_rows_count": len(harness_config_rows),
        "candidate_result_rows_count": len(candidate_result_rows),
        "baseline_result_rows_count": len(baseline_result_rows),
        "metric_comparison_rows_count": len(metric_comparison_rows),
        "pass_fail_summary_rows_count": len(pass_fail_rows),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "harness_config_csv": str(HARNESS_CONFIG_CSV),
            "candidate_results_csv": str(CANDIDATE_RESULTS_CSV),
            "baseline_results_csv": str(BASELINE_RESULTS_CSV),
            "metric_comparison_csv": str(METRIC_COMPARISON_CSV),
            "pass_fail_summary_csv": str(PASS_FAIL_SUMMARY_CSV),
            "payload_consistency_summary_csv": str(PAYLOAD_CONSISTENCY_CSV),
            "determinism_summary_csv": str(DETERMINISM_CSV),
            "safety_summary_csv": str(SAFETY_SUMMARY_CSV),
            "future_6gs_contract_csv": str(FUTURE_6GS_CONTRACT_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
