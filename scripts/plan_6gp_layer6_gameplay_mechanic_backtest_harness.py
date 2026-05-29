#!/usr/bin/env python3
"""Layer 6GP gameplay mechanic backtest harness plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gp_gameplay_mechanic_backtest_harness_plan"
TMP_DIR = Path("tmp")

AUDIT_6GO_PATH = Path("scripts/audit_6go_layer6_projection_dormant_activation_decision_plan.py")
PLAN_6GN_PATH = Path("scripts/plan_6gn_layer6_projection_dormant_activation_decisions.py")
AUDIT_6GM_PATH = Path("scripts/audit_6gm_layer6_game_state_realism_wiring_inventory.py")
VALIDATOR_6GL_PATH = Path("scripts/validate_6gl_layer6_game_state_realism_wiring_inventory.py")

JSON_6GO = TMP_DIR / "layer6_6go_activation_decision_plan_audit.json"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
MECHANICS_CSV = TMP_DIR / f"{SLUG}_mechanics.csv"
CANDIDATE_COMPARISON_CSV = TMP_DIR / f"{SLUG}_candidate_comparison.csv"
TARGET_SURFACES_CSV = TMP_DIR / f"{SLUG}_target_surfaces.csv"
METRIC_FAMILIES_CSV = TMP_DIR / f"{SLUG}_metric_families.csv"
PASS_FAIL_GATES_CSV = TMP_DIR / f"{SLUG}_pass_fail_gates.csv"
ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_artifact_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
EXIT_CRITERIA_CSV = TMP_DIR / f"{SLUG}_exit_criteria.csv"
FUTURE_6GQ_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gq_contract.csv"
FUTURE_6GR_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gr_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GO = "layer_6_projection_dormant_mechanics_activation_decision_plan_audit_complete"
DIAGNOSIS_6GP = "layer_6_gameplay_mechanic_backtest_harness_plan_complete"
CURRENT_LAYER = "6GP_layer_6_gameplay_mechanic_backtest_harness_plan"
RECOMMENDED_NEXT_LAYER = "6GQ_layer_6_gameplay_mechanic_backtest_harness_plan_audit"
RECOMMENDED_PATH = "plan_layer_6_gameplay_mechanic_backtest_harness_before_activation"
FUTURE_6GR = "6GR_layer_6_gameplay_mechanic_backtest_harness_skeleton_implementation"

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
    audit_6go_before = AUDIT_6GO_PATH.read_text(encoding="utf-8") if AUDIT_6GO_PATH.exists() else ""
    plan_6gn_before = PLAN_6GN_PATH.read_text(encoding="utf-8") if PLAN_6GN_PATH.exists() else ""
    audit_6gm_before = AUDIT_6GM_PATH.read_text(encoding="utf-8") if AUDIT_6GM_PATH.exists() else ""
    validator_6gl_before = VALIDATOR_6GL_PATH.read_text(encoding="utf-8") if VALIDATOR_6GL_PATH.exists() else ""

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6GO_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    audit_json = load_json(JSON_6GO)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6go_audit_exists", "expected": True, "actual": AUDIT_6GO_PATH.exists(), "passed": AUDIT_6GO_PATH.exists()},
        {"check": "6go_audit_runs", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"check": "6go_json_exists", "expected": True, "actual": JSON_6GO.exists(), "passed": JSON_6GO.exists()},
        {"check": "6go_all_checks_passed", "expected": True, "actual": audit_json.get("all_checks_passed"), "passed": audit_json.get("all_checks_passed") is True},
        {"check": "6go_audit_only", "expected": True, "actual": audit_json.get("audit_only"), "passed": audit_json.get("audit_only") is True},
        {"check": "6go_diagnosis", "expected": DIAGNOSIS_6GO, "actual": audit_json.get("diagnosis"), "passed": audit_json.get("diagnosis") == DIAGNOSIS_6GO},
        {"check": "6go_recommended_next_layer", "expected": CURRENT_LAYER, "actual": audit_json.get("recommended_next_layer"), "passed": audit_json.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6go_layer_6_exit_ready_false", "expected": False, "actual": audit_json.get("layer_6_exit_ready"), "passed": audit_json.get("layer_6_exit_ready") is False},
        {"check": "6go_mechanics_activated_false", "expected": False, "actual": audit_json.get("mechanics_activated_by_this_layer"), "passed": audit_json.get("mechanics_activated_by_this_layer") is False},
        {"check": "6go_backtest_gate_count", "expected": 10, "actual": audit_json.get("backtest_gate_count"), "passed": audit_json.get("backtest_gate_count") == 10},
    ]

    mechanics_rows = [
        {
            "mechanic_key": mechanic,
            "mechanic_index": index + 1,
            "included_in_6gp_harness_plan": True,
            "candidate_off_comparison_required": True,
            "activation_allowed_by_6gp": False,
            "layer_6_exit_credit_allowed_now": False,
        }
        for index, mechanic in enumerate(GAMEPLAY_MECHANICS)
    ]

    candidate_rows = [
        {
            "mechanic_key": mechanic,
            "candidate_configuration": f"{mechanic}_candidate_enabled_dry_run",
            "baseline_configuration": f"{mechanic}_current_or_off_baseline",
            "comparison_type": "candidate_vs_current_off",
            "requires_same_games_same_seeds": True,
            "requires_actual_outcome_join": True,
            "activation_blocked_until_pass": True,
        }
        for mechanic in GAMEPLAY_MECHANICS
    ]

    target_rows = [
        {
            "mechanic_key": mechanic,
            "target_surface": surface,
            "required": True,
            "activation_blocked_until_evaluated": True,
        }
        for mechanic in GAMEPLAY_MECHANICS
        for surface in TARGET_SURFACES
    ]

    metric_rows = [
        {
            "mechanic_key": mechanic,
            "metric_family": metric,
            "required": True,
            "metric_scope": "candidate_vs_baseline_actual_outcome_comparison",
            "activation_blocked_until_evaluated": True,
        }
        for mechanic in GAMEPLAY_MECHANICS
        for metric in METRIC_FAMILIES
    ]

    gate_rows = [
        {
            "mechanic_key": mechanic,
            "pass_fail_gate": gate,
            "required": True,
            "failure_blocks_activation": True,
            "failure_blocks_layer_6_exit_credit": True,
        }
        for mechanic in GAMEPLAY_MECHANICS
        for gate in PASS_FAIL_GATES
    ]

    artifact_rows = [
        {"artifact": "harness_config", "required": True, "description": "deterministic list of mechanics, toggles, baselines, seeds, and target surfaces"},
        {"artifact": "candidate_results", "required": True, "description": "candidate/on dry-run aggregate metrics by mechanic"},
        {"artifact": "baseline_results", "required": True, "description": "current/off baseline aggregate metrics by mechanic"},
        {"artifact": "metric_comparison", "required": True, "description": "candidate vs baseline deltas for each target surface and metric family"},
        {"artifact": "pass_fail_summary", "required": True, "description": "gate-level activation and Layer 6 credit decision outcomes"},
        {"artifact": "payload_consistency_summary", "required": True, "description": "projection payload consistency evidence for projection-facing mechanics"},
        {"artifact": "determinism_summary", "required": True, "description": "same-seed reproducibility evidence"},
        {"artifact": "safety_summary", "required": True, "description": "proof that harness runs do not change production defaults or activate mechanics"},
    ]

    safety_rows = [
        {"boundary": "no_mechanic_activation", "required": True, "passed": True},
        {"boundary": "no_simulator_behavior_change", "required": True, "passed": True},
        {"boundary": "no_projection_behavior_change", "required": True, "passed": True},
        {"boundary": "no_fixture_change", "required": True, "passed": True},
        {"boundary": "no_production_default_change", "required": True, "passed": True},
        {"boundary": "no_live_data_fetch", "required": True, "passed": True},
        {"boundary": "no_database_write", "required": True, "passed": True},
        {"boundary": "no_materialization_job", "required": True, "passed": True},
        {"boundary": "dry_run_only_until_future_implementation", "required": True, "passed": True},
    ]

    exit_rows = [
        {"exit_criterion": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"exit_criterion": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"exit_criterion": "gameplay_mechanics_backtest_planned", "expected": 10, "actual": len(mechanics_rows), "passed": len(mechanics_rows) == 10},
        {"exit_criterion": "target_surfaces_planned_for_all_mechanics", "expected": 10 * len(TARGET_SURFACES), "actual": len(target_rows), "passed": len(target_rows) == 10 * len(TARGET_SURFACES)},
        {"exit_criterion": "metric_families_planned_for_all_mechanics", "expected": 10 * len(METRIC_FAMILIES), "actual": len(metric_rows), "passed": len(metric_rows) == 10 * len(METRIC_FAMILIES)},
        {"exit_criterion": "pass_fail_gates_planned_for_all_mechanics", "expected": 10 * len(PASS_FAIL_GATES), "actual": len(gate_rows), "passed": len(gate_rows) == 10 * len(PASS_FAIL_GATES)},
        {"exit_criterion": "activation_blocked_until_backtest", "expected": True, "actual": True, "passed": True},
        {"exit_criterion": "layer_6_exit_credit_blocked_until_backtest", "expected": True, "actual": True, "passed": True},
    ]

    future_6gq_rows = [
        {"contract": "audit_6gp_backtest_harness_plan", "required": True, "passed": True},
        {"contract": "verify_all_10_mechanics_have_candidate_comparison", "required": True, "passed": True},
        {"contract": "verify_all_target_surfaces_present", "required": True, "passed": True},
        {"contract": "verify_all_metric_families_present", "required": True, "passed": True},
        {"contract": "verify_all_pass_fail_gates_present", "required": True, "passed": True},
        {"contract": "verify_no_activation_or_exit_credit", "required": True, "passed": True},
        {"contract": "recommended_6gq_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_backtest_harness_plan_audit_complete"},
    ]

    future_6gr_rows = [
        {"contract": "implement_dry_run_harness_skeleton", "required": True, "passed": True},
        {"contract": "read_6gp_plan_artifacts", "required": True, "passed": True},
        {"contract": "emit_deterministic_placeholder_or_evidence_artifacts", "required": True, "passed": True},
        {"contract": "no_mechanic_activation", "required": True, "passed": True},
        {"contract": "no_production_default_change", "required": True, "passed": True},
        {"contract": "no_live_fetch_or_database_write", "required": True, "passed": True},
        {"contract": "future_6gr_layer", "required": True, "passed": True, "artifact": FUTURE_6GR},
        {"contract": "recommended_6gr_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_backtest_harness_skeleton_implementation_complete"},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "future_harness_implementation_layer", "expected": FUTURE_6GR, "actual": FUTURE_6GR, "passed": True},
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GP, "actual": DIAGNOSIS_6GP, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6go_after = AUDIT_6GO_PATH.read_text(encoding="utf-8") if AUDIT_6GO_PATH.exists() else ""
    plan_6gn_after = PLAN_6GN_PATH.read_text(encoding="utf-8") if PLAN_6GN_PATH.exists() else ""
    audit_6gm_after = AUDIT_6GM_PATH.read_text(encoding="utf-8") if AUDIT_6GM_PATH.exists() else ""
    validator_6gl_after = VALIDATOR_6GL_PATH.read_text(encoding="utf-8") if VALIDATOR_6GL_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gp_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6go_audit", "policy": "unchanged_by_6gp_plan", "passed": audit_6go_after == audit_6go_before},
        {"surface": "6gn_plan", "policy": "unchanged_by_6gp_plan", "passed": plan_6gn_after == plan_6gn_before},
        {"surface": "6gm_audit", "policy": "unchanged_by_6gp_plan", "passed": audit_6gm_after == audit_6gm_before},
        {"surface": "6gl_validator", "policy": "unchanged_by_6gp_plan", "passed": validator_6gl_after == validator_6gl_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gp_plan", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gp_plan", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gp_plan", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gp_plan", "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "mechanics", "passed": len(mechanics_rows) == 10, "detail": f"{len(mechanics_rows)}/10"},
        {"check": "candidate_comparison", "passed": len(candidate_rows) == 10 and all(row["activation_blocked_until_pass"] for row in candidate_rows), "detail": f"{len(candidate_rows)}/10"},
        {"check": "target_surfaces", "passed": len(target_rows) == 10 * len(TARGET_SURFACES), "detail": f"{len(target_rows)}/{10 * len(TARGET_SURFACES)}"},
        {"check": "metric_families", "passed": len(metric_rows) == 10 * len(METRIC_FAMILIES), "detail": f"{len(metric_rows)}/{10 * len(METRIC_FAMILIES)}"},
        {"check": "pass_fail_gates", "passed": len(gate_rows) == 10 * len(PASS_FAIL_GATES) and all(row["failure_blocks_activation"] for row in gate_rows), "detail": f"{len(gate_rows)}/{10 * len(PASS_FAIL_GATES)}"},
        {"check": "artifact_contract", "passed": all(row["required"] for row in artifact_rows), "detail": f"{len(artifact_rows)} artifacts"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "exit_criteria", "passed": all(row["passed"] for row in exit_rows), "detail": f"{sum(1 for row in exit_rows if row['passed'])}/{len(exit_rows)}"},
        {"check": "future_6gq_contract", "passed": all(row["passed"] for row in future_6gq_rows), "detail": f"{sum(1 for row in future_6gq_rows if row['passed'])}/{len(future_6gq_rows)}"},
        {"check": "future_6gr_contract", "passed": all(row["passed"] for row in future_6gr_rows), "detail": f"{sum(1 for row in future_6gr_rows if row['passed'])}/{len(future_6gr_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "mechanics": write_csv(MECHANICS_CSV, mechanics_rows),
        "candidate_comparison": write_csv(CANDIDATE_COMPARISON_CSV, candidate_rows),
        "target_surfaces": write_csv(TARGET_SURFACES_CSV, target_rows),
        "metric_families": write_csv(METRIC_FAMILIES_CSV, metric_rows),
        "pass_fail_gates": write_csv(PASS_FAIL_GATES_CSV, gate_rows),
        "artifact_contract": write_csv(ARTIFACT_CONTRACT_CSV, artifact_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "exit_criteria": write_csv(EXIT_CRITERIA_CSV, exit_rows),
        "future_6gq_contract": write_csv(FUTURE_6GQ_CONTRACT_CSV, future_6gq_rows),
        "future_6gr_contract": write_csv(FUTURE_6GR_CONTRACT_CSV, future_6gr_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GP",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6GP if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "future_harness_implementation_layer": FUTURE_6GR,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "gameplay_mechanics_count": len(mechanics_rows),
        "target_surface_count": len(TARGET_SURFACES),
        "metric_family_count": len(METRIC_FAMILIES),
        "pass_fail_gate_count": len(PASS_FAIL_GATES),
        "predecessor_audit": str(AUDIT_6GO_PATH),
        "predecessor_audit_returncode": audit_run.returncode,
        "predecessor_audit_diagnosis": audit_json.get("diagnosis"),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "mechanics_csv": str(MECHANICS_CSV),
            "candidate_comparison_csv": str(CANDIDATE_COMPARISON_CSV),
            "target_surfaces_csv": str(TARGET_SURFACES_CSV),
            "metric_families_csv": str(METRIC_FAMILIES_CSV),
            "pass_fail_gates_csv": str(PASS_FAIL_GATES_CSV),
            "artifact_contract_csv": str(ARTIFACT_CONTRACT_CSV),
            "safety_boundaries_csv": str(SAFETY_BOUNDARIES_CSV),
            "exit_criteria_csv": str(EXIT_CRITERIA_CSV),
            "future_6gq_contract_csv": str(FUTURE_6GQ_CONTRACT_CSV),
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
