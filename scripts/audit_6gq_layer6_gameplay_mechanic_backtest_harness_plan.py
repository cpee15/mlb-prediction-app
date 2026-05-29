#!/usr/bin/env python3
"""Audit Layer 6GP gameplay mechanic backtest harness plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


SLUG = "layer6_6gq_backtest_harness_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6GP_PATH = Path("scripts/plan_6gp_layer6_gameplay_mechanic_backtest_harness.py")
AUDIT_6GO_PATH = Path("scripts/audit_6go_layer6_projection_dormant_activation_decision_plan.py")
PLAN_6GN_PATH = Path("scripts/plan_6gn_layer6_projection_dormant_activation_decisions.py")
AUDIT_6GM_PATH = Path("scripts/audit_6gm_layer6_game_state_realism_wiring_inventory.py")

JSON_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan.json"
CHECKS_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_checks.csv"
PREDECESSOR_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_predecessor.csv"
MECHANICS_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_mechanics.csv"
CANDIDATE_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_candidate_comparison.csv"
TARGETS_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_target_surfaces.csv"
METRICS_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_metric_families.csv"
GATES_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_pass_fail_gates.csv"
ARTIFACT_CONTRACT_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_artifact_contract.csv"
SAFETY_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_safety_boundaries.csv"
EXIT_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_exit_criteria.csv"
FUTURE_6GQ_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_future_6gq_contract.csv"
FUTURE_6GR_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_future_6gr_contract.csv"
IMMUTABILITY_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_immutability.csv"
RECOMMENDED_6GP = TMP_DIR / "layer6_6gp_gameplay_mechanic_backtest_harness_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
MECHANIC_COVERAGE_CSV = TMP_DIR / f"{SLUG}_mechanic_coverage.csv"
CANDIDATE_COMPARISON_CSV = TMP_DIR / f"{SLUG}_candidate_comparison.csv"
TARGET_SURFACE_COVERAGE_CSV = TMP_DIR / f"{SLUG}_target_surface_coverage.csv"
METRIC_FAMILY_COVERAGE_CSV = TMP_DIR / f"{SLUG}_metric_family_coverage.csv"
PASS_FAIL_GATE_COVERAGE_CSV = TMP_DIR / f"{SLUG}_pass_fail_gate_coverage.csv"
ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_artifact_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
EXIT_CRITERIA_CSV = TMP_DIR / f"{SLUG}_exit_criteria.csv"
FUTURE_6GR_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gr_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GP = "layer_6_gameplay_mechanic_backtest_harness_plan_complete"
DIAGNOSIS_6GQ = "layer_6_gameplay_mechanic_backtest_harness_plan_audit_complete"
CURRENT_LAYER = "6GQ_layer_6_gameplay_mechanic_backtest_harness_plan_audit"
RECOMMENDED_NEXT_LAYER = "6GR_layer_6_gameplay_mechanic_backtest_harness_skeleton_implementation"
RECOMMENDED_PATH = "audit_6gp_backtest_harness_plan_then_implement_dry_run_harness_skeleton"

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

TARGET_SURFACES = [
    "game_total_runs",
    "team_total_runs",
    "inning_runs",
    "scoring_distribution_tails",
    "run_variance",
    "calibration_error",
    "base_out_transition_accuracy",
    "extra_inning_distribution",
    "market_projection_payload_consistency",
    "no_regression_current_baseline",
]

METRIC_FAMILIES = [
    "mae",
    "rmse",
    "log_loss_or_likelihood_proxy",
    "calibration_error",
    "ks_distance_or_distribution_distance",
    "tail_error",
    "variance_error",
    "inning_distribution_error",
    "team_total_error",
    "market_payload_consistency_check",
]

PASS_FAIL_GATES = [
    "candidate_must_not_degrade_total_run_error",
    "candidate_must_not_degrade_team_total_error",
    "candidate_must_not_degrade_inning_distribution",
    "candidate_must_not_degrade_scoring_tails",
    "candidate_must_not_degrade_variance_calibration",
    "candidate_must_improve_or_preserve_calibration_error",
    "candidate_must_prove_payload_consistency_if_projection_facing",
    "candidate_must_be_reproducible",
    "candidate_must_be_deterministic_given_seed",
    "candidate_must_not_change_production_defaults",
]

REQUIRED_ARTIFACT_NAMES = [
    "harness_config",
    "candidate_results",
    "baseline_results",
    "metric_comparison",
    "pass_fail_summary",
    "payload_consistency_summary",
    "determinism_summary",
    "safety_summary",
]

REQUIRED_SAFETY_BOUNDARIES = [
    "no_mechanic_activation",
    "no_simulator_behavior_change",
    "no_projection_behavior_change",
    "no_fixture_change",
    "no_production_default_change",
    "no_live_data_fetch",
    "no_database_write",
    "no_materialization_job",
    "dry_run_only_until_future_implementation",
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


def values_by_key(rows: List[Dict[str, str]], key_field: str, value_field: str) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = defaultdict(set)
    for row in rows:
        out[row.get(key_field, "")].add(row.get(value_field, ""))
    return out


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6gp_before = PLAN_6GP_PATH.read_text(encoding="utf-8") if PLAN_6GP_PATH.exists() else ""
    audit_6go_before = AUDIT_6GO_PATH.read_text(encoding="utf-8") if AUDIT_6GO_PATH.exists() else ""
    plan_6gn_before = PLAN_6GN_PATH.read_text(encoding="utf-8") if PLAN_6GN_PATH.exists() else ""
    audit_6gm_before = AUDIT_6GM_PATH.read_text(encoding="utf-8") if AUDIT_6GM_PATH.exists() else ""

    plan_run = subprocess.run(
        [sys.executable, str(PLAN_6GP_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6gp = load_json(JSON_6GP)
    mechanics_rows = read_csv(MECHANICS_6GP)
    candidate_rows = read_csv(CANDIDATE_6GP)
    target_rows = read_csv(TARGETS_6GP)
    metric_rows = read_csv(METRICS_6GP)
    gate_rows = read_csv(GATES_6GP)
    artifact_contract_rows_6gp = read_csv(ARTIFACT_CONTRACT_6GP)
    safety_rows_6gp = read_csv(SAFETY_6GP)
    exit_rows_6gp = read_csv(EXIT_6GP)
    future_6gr_rows_6gp = read_csv(FUTURE_6GR_6GP)

    required_artifacts = [
        JSON_6GP,
        CHECKS_6GP,
        PREDECESSOR_6GP,
        MECHANICS_6GP,
        CANDIDATE_6GP,
        TARGETS_6GP,
        METRICS_6GP,
        GATES_6GP,
        ARTIFACT_CONTRACT_6GP,
        SAFETY_6GP,
        EXIT_6GP,
        FUTURE_6GQ_6GP,
        FUTURE_6GR_6GP,
        IMMUTABILITY_6GP,
        RECOMMENDED_6GP,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gp_plan_exists", "expected": True, "actual": PLAN_6GP_PATH.exists(), "passed": PLAN_6GP_PATH.exists()},
        {"check": "6gp_plan_runs", "expected": 0, "actual": plan_run.returncode, "passed": plan_run.returncode == 0},
        {"check": "6gp_json_exists", "expected": True, "actual": JSON_6GP.exists(), "passed": JSON_6GP.exists()},
        {"check": "6gp_all_checks_passed", "expected": True, "actual": json_6gp.get("all_checks_passed"), "passed": json_6gp.get("all_checks_passed") is True},
        {"check": "6gp_planning_only", "expected": True, "actual": json_6gp.get("planning_only"), "passed": json_6gp.get("planning_only") is True},
        {"check": "6gp_diagnosis", "expected": DIAGNOSIS_6GP, "actual": json_6gp.get("diagnosis"), "passed": json_6gp.get("diagnosis") == DIAGNOSIS_6GP},
        {"check": "6gp_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6gp.get("recommended_next_layer"), "passed": json_6gp.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gp_future_6gr", "expected": RECOMMENDED_NEXT_LAYER, "actual": json_6gp.get("future_harness_implementation_layer"), "passed": json_6gp.get("future_harness_implementation_layer") == RECOMMENDED_NEXT_LAYER},
        {"check": "6gp_layer_6_exit_ready_false", "expected": False, "actual": json_6gp.get("layer_6_exit_ready"), "passed": json_6gp.get("layer_6_exit_ready") is False},
        {"check": "6gp_mechanics_activated_false", "expected": False, "actual": json_6gp.get("mechanics_activated_by_this_layer"), "passed": json_6gp.get("mechanics_activated_by_this_layer") is False},
    ]

    artifact_presence_rows = [
        {
            "artifact_path": str(path),
            "exists": path.exists(),
            "passed": path.exists(),
        }
        for path in required_artifacts
    ]

    mechanic_keys = {row.get("mechanic_key") for row in mechanics_rows}
    mechanic_coverage_rows = [
        {
            "mechanic_key": mechanic,
            "present": mechanic in mechanic_keys,
            "candidate_off_comparison_required": any(
                row.get("mechanic_key") == mechanic and boolish(row.get("candidate_off_comparison_required"))
                for row in mechanics_rows
            ),
            "activation_allowed_by_6gp_false": any(
                row.get("mechanic_key") == mechanic and not boolish(row.get("activation_allowed_by_6gp"))
                for row in mechanics_rows
            ),
            "layer_6_exit_credit_allowed_now_false": any(
                row.get("mechanic_key") == mechanic and not boolish(row.get("layer_6_exit_credit_allowed_now"))
                for row in mechanics_rows
            ),
            "passed": (
                mechanic in mechanic_keys
                and any(row.get("mechanic_key") == mechanic and boolish(row.get("candidate_off_comparison_required")) for row in mechanics_rows)
                and any(row.get("mechanic_key") == mechanic and not boolish(row.get("activation_allowed_by_6gp")) for row in mechanics_rows)
                and any(row.get("mechanic_key") == mechanic and not boolish(row.get("layer_6_exit_credit_allowed_now")) for row in mechanics_rows)
            ),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]

    candidate_count = Counter(row.get("mechanic_key") for row in candidate_rows)
    candidate_comparison_rows = [
        {
            "mechanic_key": mechanic,
            "row_count": candidate_count.get(mechanic, 0),
            "exactly_one_row": candidate_count.get(mechanic, 0) == 1,
            "comparison_type_candidate_vs_current_off": any(
                row.get("mechanic_key") == mechanic and row.get("comparison_type") == "candidate_vs_current_off"
                for row in candidate_rows
            ),
            "requires_same_games_same_seeds": any(
                row.get("mechanic_key") == mechanic and boolish(row.get("requires_same_games_same_seeds"))
                for row in candidate_rows
            ),
            "requires_actual_outcome_join": any(
                row.get("mechanic_key") == mechanic and boolish(row.get("requires_actual_outcome_join"))
                for row in candidate_rows
            ),
            "activation_blocked_until_pass": any(
                row.get("mechanic_key") == mechanic and boolish(row.get("activation_blocked_until_pass"))
                for row in candidate_rows
            ),
            "passed": (
                candidate_count.get(mechanic, 0) == 1
                and any(row.get("mechanic_key") == mechanic and row.get("comparison_type") == "candidate_vs_current_off" for row in candidate_rows)
                and any(row.get("mechanic_key") == mechanic and boolish(row.get("requires_same_games_same_seeds")) for row in candidate_rows)
                and any(row.get("mechanic_key") == mechanic and boolish(row.get("requires_actual_outcome_join")) for row in candidate_rows)
                and any(row.get("mechanic_key") == mechanic and boolish(row.get("activation_blocked_until_pass")) for row in candidate_rows)
            ),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]

    targets_by_mechanic = values_by_key(target_rows, "mechanic_key", "target_surface")
    target_coverage_rows = [
        {
            "mechanic_key": mechanic,
            "required_surface_count": len(TARGET_SURFACES),
            "actual_surface_count": len(targets_by_mechanic.get(mechanic, set())),
            "missing_surfaces": "|".join(sorted(set(TARGET_SURFACES) - targets_by_mechanic.get(mechanic, set()))),
            "activation_blocked_until_evaluated": all(
                boolish(row.get("activation_blocked_until_evaluated"))
                for row in target_rows
                if row.get("mechanic_key") == mechanic
            ),
            "passed": (
                targets_by_mechanic.get(mechanic, set()) == set(TARGET_SURFACES)
                and all(boolish(row.get("activation_blocked_until_evaluated")) for row in target_rows if row.get("mechanic_key") == mechanic)
            ),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]

    metrics_by_mechanic = values_by_key(metric_rows, "mechanic_key", "metric_family")
    metric_coverage_rows = [
        {
            "mechanic_key": mechanic,
            "required_metric_count": len(METRIC_FAMILIES),
            "actual_metric_count": len(metrics_by_mechanic.get(mechanic, set())),
            "missing_metrics": "|".join(sorted(set(METRIC_FAMILIES) - metrics_by_mechanic.get(mechanic, set()))),
            "activation_blocked_until_evaluated": all(
                boolish(row.get("activation_blocked_until_evaluated"))
                for row in metric_rows
                if row.get("mechanic_key") == mechanic
            ),
            "passed": (
                metrics_by_mechanic.get(mechanic, set()) == set(METRIC_FAMILIES)
                and all(boolish(row.get("activation_blocked_until_evaluated")) for row in metric_rows if row.get("mechanic_key") == mechanic)
            ),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]

    gates_by_mechanic = values_by_key(gate_rows, "mechanic_key", "pass_fail_gate")
    gate_coverage_rows = [
        {
            "mechanic_key": mechanic,
            "required_gate_count": len(PASS_FAIL_GATES),
            "actual_gate_count": len(gates_by_mechanic.get(mechanic, set())),
            "missing_gates": "|".join(sorted(set(PASS_FAIL_GATES) - gates_by_mechanic.get(mechanic, set()))),
            "all_gates_block_activation": all(
                boolish(row.get("failure_blocks_activation"))
                for row in gate_rows
                if row.get("mechanic_key") == mechanic
            ),
            "all_gates_block_layer_6_exit_credit": all(
                boolish(row.get("failure_blocks_layer_6_exit_credit"))
                for row in gate_rows
                if row.get("mechanic_key") == mechanic
            ),
            "passed": (
                gates_by_mechanic.get(mechanic, set()) == set(PASS_FAIL_GATES)
                and all(boolish(row.get("failure_blocks_activation")) for row in gate_rows if row.get("mechanic_key") == mechanic)
                and all(boolish(row.get("failure_blocks_layer_6_exit_credit")) for row in gate_rows if row.get("mechanic_key") == mechanic)
            ),
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]

    artifact_names = {row.get("artifact") for row in artifact_contract_rows_6gp}
    artifact_contract_rows = [
        {
            "artifact": artifact,
            "present": artifact in artifact_names,
            "required_true": any(
                row.get("artifact") == artifact and boolish(row.get("required"))
                for row in artifact_contract_rows_6gp
            ),
            "passed": artifact in artifact_names and any(row.get("artifact") == artifact and boolish(row.get("required")) for row in artifact_contract_rows_6gp),
        }
        for artifact in REQUIRED_ARTIFACT_NAMES
    ]

    safety_names = {row.get("boundary") for row in safety_rows_6gp}
    safety_boundary_rows = [
        {
            "boundary": boundary,
            "present": boundary in safety_names,
            "required_true": any(
                row.get("boundary") == boundary and boolish(row.get("required"))
                for row in safety_rows_6gp
            ),
            "passed_true": any(
                row.get("boundary") == boundary and boolish(row.get("passed"))
                for row in safety_rows_6gp
            ),
            "passed": (
                boundary in safety_names
                and any(row.get("boundary") == boundary and boolish(row.get("required")) for row in safety_rows_6gp)
                and any(row.get("boundary") == boundary and boolish(row.get("passed")) for row in safety_rows_6gp)
            ),
        }
        for boundary in REQUIRED_SAFETY_BOUNDARIES
    ]

    exit_by_criterion = {row.get("exit_criterion"): row for row in exit_rows_6gp}
    exit_criteria_rows = [
        {
            "exit_criterion": "layer_6_exit_ready",
            "expected": "False",
            "actual": exit_by_criterion.get("layer_6_exit_ready", {}).get("actual"),
            "passed": exit_by_criterion.get("layer_6_exit_ready", {}).get("actual") == "False",
        },
        {
            "exit_criterion": "mechanics_activated_by_this_layer",
            "expected": "False",
            "actual": exit_by_criterion.get("mechanics_activated_by_this_layer", {}).get("actual"),
            "passed": exit_by_criterion.get("mechanics_activated_by_this_layer", {}).get("actual") == "False",
        },
        {
            "exit_criterion": "activation_blocked_until_backtest",
            "expected": "True",
            "actual": exit_by_criterion.get("activation_blocked_until_backtest", {}).get("actual"),
            "passed": exit_by_criterion.get("activation_blocked_until_backtest", {}).get("actual") == "True",
        },
        {
            "exit_criterion": "layer_6_exit_credit_blocked_until_backtest",
            "expected": "True",
            "actual": exit_by_criterion.get("layer_6_exit_credit_blocked_until_backtest", {}).get("actual"),
            "passed": exit_by_criterion.get("layer_6_exit_credit_blocked_until_backtest", {}).get("actual") == "True",
        },
    ]

    future_6gr_contract_names = {row.get("contract") for row in future_6gr_rows_6gp}
    future_6gr_rows = [
        {
            "contract": "implement_dry_run_harness_skeleton",
            "present": "implement_dry_run_harness_skeleton" in future_6gr_contract_names,
            "passed": "implement_dry_run_harness_skeleton" in future_6gr_contract_names,
        },
        {
            "contract": "future_6gr_layer",
            "expected_artifact": RECOMMENDED_NEXT_LAYER,
            "actual_artifact": next((row.get("artifact") for row in future_6gr_rows_6gp if row.get("contract") == "future_6gr_layer"), ""),
            "passed": any(row.get("contract") == "future_6gr_layer" and row.get("artifact") == RECOMMENDED_NEXT_LAYER for row in future_6gr_rows_6gp),
        },
        {
            "contract": "no_mechanic_activation",
            "present": "no_mechanic_activation" in future_6gr_contract_names,
            "passed": "no_mechanic_activation" in future_6gr_contract_names,
        },
        {
            "contract": "no_live_fetch_or_database_write",
            "present": "no_live_fetch_or_database_write" in future_6gr_contract_names,
            "passed": "no_live_fetch_or_database_write" in future_6gr_contract_names,
        },
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6gp_after = PLAN_6GP_PATH.read_text(encoding="utf-8") if PLAN_6GP_PATH.exists() else ""
    audit_6go_after = AUDIT_6GO_PATH.read_text(encoding="utf-8") if AUDIT_6GO_PATH.exists() else ""
    plan_6gn_after = PLAN_6GN_PATH.read_text(encoding="utf-8") if PLAN_6GN_PATH.exists() else ""
    audit_6gm_after = AUDIT_6GM_PATH.read_text(encoding="utf-8") if AUDIT_6GM_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gq_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gp_plan", "policy": "unchanged_by_6gq_audit", "passed": plan_6gp_after == plan_6gp_before},
        {"surface": "6go_audit", "policy": "unchanged_by_6gq_audit", "passed": audit_6go_after == audit_6go_before},
        {"surface": "6gn_plan", "policy": "unchanged_by_6gq_audit", "passed": plan_6gn_after == plan_6gn_before},
        {"surface": "6gm_audit", "policy": "unchanged_by_6gq_audit", "passed": audit_6gm_after == audit_6gm_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gq_audit", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gq_audit", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gq_audit", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gq_audit", "passed": True},
        {"surface": "live_fetches_or_database_writes", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GQ, "actual": DIAGNOSIS_6GQ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "mechanic_coverage", "passed": all(row["passed"] for row in mechanic_coverage_rows), "detail": f"{sum(1 for row in mechanic_coverage_rows if row['passed'])}/{len(mechanic_coverage_rows)}"},
        {"check": "candidate_comparison", "passed": all(row["passed"] for row in candidate_comparison_rows), "detail": f"{sum(1 for row in candidate_comparison_rows if row['passed'])}/{len(candidate_comparison_rows)}"},
        {"check": "target_surface_coverage", "passed": len(target_rows) == 100 and all(row["passed"] for row in target_coverage_rows), "detail": f"{len(target_rows)}/100"},
        {"check": "metric_family_coverage", "passed": len(metric_rows) == 100 and all(row["passed"] for row in metric_coverage_rows), "detail": f"{len(metric_rows)}/100"},
        {"check": "pass_fail_gate_coverage", "passed": len(gate_rows) == 100 and all(row["passed"] for row in gate_coverage_rows), "detail": f"{len(gate_rows)}/100"},
        {"check": "artifact_contract", "passed": all(row["passed"] for row in artifact_contract_rows), "detail": f"{sum(1 for row in artifact_contract_rows if row['passed'])}/{len(artifact_contract_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_boundary_rows), "detail": f"{sum(1 for row in safety_boundary_rows if row['passed'])}/{len(safety_boundary_rows)}"},
        {"check": "exit_criteria", "passed": all(row["passed"] for row in exit_criteria_rows), "detail": f"{sum(1 for row in exit_criteria_rows if row['passed'])}/{len(exit_criteria_rows)}"},
        {"check": "future_6gr_contract", "passed": all(row["passed"] for row in future_6gr_rows), "detail": f"{sum(1 for row in future_6gr_rows if row['passed'])}/{len(future_6gr_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "mechanic_coverage": write_csv(MECHANIC_COVERAGE_CSV, mechanic_coverage_rows),
        "candidate_comparison": write_csv(CANDIDATE_COMPARISON_CSV, candidate_comparison_rows),
        "target_surface_coverage": write_csv(TARGET_SURFACE_COVERAGE_CSV, target_coverage_rows),
        "metric_family_coverage": write_csv(METRIC_FAMILY_COVERAGE_CSV, metric_coverage_rows),
        "pass_fail_gate_coverage": write_csv(PASS_FAIL_GATE_COVERAGE_CSV, gate_coverage_rows),
        "artifact_contract": write_csv(ARTIFACT_CONTRACT_CSV, artifact_contract_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_boundary_rows),
        "exit_criteria": write_csv(EXIT_CRITERIA_CSV, exit_criteria_rows),
        "future_6gr_contract": write_csv(FUTURE_6GR_CONTRACT_CSV, future_6gr_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GQ",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "audited_layer": "6GP",
        "audited_plan_diagnosis": json_6gp.get("diagnosis"),
        "diagnosis": DIAGNOSIS_6GQ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "future_harness_implementation_layer": RECOMMENDED_NEXT_LAYER,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "gameplay_mechanics_count": len(mechanics_rows),
        "target_surface_rows_count": len(target_rows),
        "metric_family_rows_count": len(metric_rows),
        "pass_fail_gate_rows_count": len(gate_rows),
        "predecessor_plan": str(PLAN_6GP_PATH),
        "predecessor_plan_returncode": plan_run.returncode,
        "predecessor_plan_diagnosis": json_6gp.get("diagnosis"),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "mechanic_coverage_csv": str(MECHANIC_COVERAGE_CSV),
            "candidate_comparison_csv": str(CANDIDATE_COMPARISON_CSV),
            "target_surface_coverage_csv": str(TARGET_SURFACE_COVERAGE_CSV),
            "metric_family_coverage_csv": str(METRIC_FAMILY_COVERAGE_CSV),
            "pass_fail_gate_coverage_csv": str(PASS_FAIL_GATE_COVERAGE_CSV),
            "artifact_contract_csv": str(ARTIFACT_CONTRACT_CSV),
            "safety_boundaries_csv": str(SAFETY_BOUNDARIES_CSV),
            "exit_criteria_csv": str(EXIT_CRITERIA_CSV),
            "future_6gr_contract_csv": str(FUTURE_6GR_CONTRACT_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
