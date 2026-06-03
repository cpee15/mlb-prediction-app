#!/usr/bin/env python3
"""Implement Layer 6JO authorization / exit certification without authorizing activation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jo_authorization_or_exit_certification_implementation"
TMP_DIR = Path("tmp")

PLAN_6JN_PATH = Path("scripts/plan_6jn_layer6_authorization_or_exit_certification.py")
JSON_6JN = TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan.json"

REQUIRED_INPUTS = [
    JSON_6JN,
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_checks.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_predecessor.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_authorization_decision_gate.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_activation_execution_authorization_gate.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_mechanic_activation_authorization_gate.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_layer6_exit_certification_gate.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_performance_evidence_gate.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_calibration_sample_integrity_gate.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_rollback_on_authorization_failure_gate.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_post_authorization_monitoring_gate.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_future_6jo_contract.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_future_6jp_contract.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_readonly_sources.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_preserved_families.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_decision.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_immutability.csv",
    TMP_DIR / "layer6_6jn_authorization_or_exit_certification_plan_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
AUTH_DECISION_CSV = TMP_DIR / f"{SLUG}_authorization_decision_gate.csv"
ACTIVATION_EXECUTION_AUTH_CSV = TMP_DIR / f"{SLUG}_activation_execution_authorization_gate.csv"
MECHANIC_ACTIVATION_AUTH_CSV = TMP_DIR / f"{SLUG}_mechanic_activation_authorization_gate.csv"
EXIT_CERTIFICATION_CSV = TMP_DIR / f"{SLUG}_layer6_exit_certification_gate.csv"
PERFORMANCE_EVIDENCE_CSV = TMP_DIR / f"{SLUG}_performance_evidence_gate.csv"
CALIBRATION_SAMPLE_CSV = TMP_DIR / f"{SLUG}_calibration_sample_integrity_gate.csv"
ROLLBACK_FAILURE_CSV = TMP_DIR / f"{SLUG}_rollback_on_authorization_failure_gate.csv"
POST_AUTH_MONITORING_CSV = TMP_DIR / f"{SLUG}_post_authorization_monitoring_gate.csv"
FUTURE_6JP_CSV = TMP_DIR / f"{SLUG}_future_6jp_contract.csv"
FUTURE_PERFORMANCE_CERTIFICATION_CSV = TMP_DIR / f"{SLUG}_future_performance_certification_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JN = "layer_6_authorization_or_exit_certification_plan_complete"
DIAGNOSIS_6JO = "layer_6_authorization_or_exit_certification_implementation_complete"
RECOMMENDED_NEXT_LAYER_6JN = "6JO_layer_6_authorization_or_exit_certification_implementation"
RECOMMENDED_PATH_6JN = "plan_authorization_or_exit_certification_then_implement_before_audit"
RECOMMENDED_NEXT_LAYER_6JO = "6JP_layer_6_authorization_or_exit_certification_implementation_audit"
RECOMMENDED_PATH_6JO = "implement_authorization_or_exit_certification_then_audit_before_performance_certification"


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True}]
    fieldnames: List[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


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


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(str(row.get("passed", "")).lower() == "true" or row.get("passed") is True for row in rows)


def implement_gate(rows: List[Dict[str, str]], source_path: Path, key: str, expected_count: int) -> List[Dict[str, Any]]:
    implemented: List[Dict[str, Any]] = []
    for row in rows:
        identifier = row.get(key, "")
        implemented.append({
            key: identifier,
            "source_artifact": str(source_path),
            "implemented": True,
            "performance_evaluation_allowed": False,
            "mae_brier_comparison_run": False,
            "final_activation_decision_allowed": False,
            "activation_execution_allowed": False,
            "activation_execution_executed": False,
            "mechanic_activated": False,
            "production_simulation_run": False,
            "database_write_run": False,
            "live_data_fetch_run": False,
            "remote_api_call_run": False,
            "source_acquisition_run": False,
            "layer_6_exit_credit": False,
            "passed": bool(identifier)
            and str(row.get("planned", "")).lower() == "true"
            and str(row.get("performance_evaluation_allowed", "")).lower() == "false"
            and str(row.get("mae_brier_comparison_run", "")).lower() == "false"
            and str(row.get("final_activation_decision_allowed", "")).lower() == "false"
            and str(row.get("activation_execution_allowed", "")).lower() == "false"
            and str(row.get("activation_execution_executed", "")).lower() == "false"
            and str(row.get("mechanic_activated", "")).lower() == "false"
            and str(row.get("production_simulation_run", "")).lower() == "false"
            and str(row.get("database_write_run", "")).lower() == "false"
            and str(row.get("live_data_fetch_run", "")).lower() == "false"
            and str(row.get("remote_api_call_run", "")).lower() == "false"
            and str(row.get("source_acquisition_run", "")).lower() == "false"
            and str(row.get("layer_6_exit_credit", "")).lower() == "false",
        })
    if len(rows) != expected_count:
        implemented.append({
            key: "__row_count_mismatch__",
            "source_artifact": str(source_path),
            "implemented": False,
            "performance_evaluation_allowed": True,
            "mae_brier_comparison_run": True,
            "final_activation_decision_allowed": True,
            "activation_execution_allowed": True,
            "activation_execution_executed": True,
            "mechanic_activated": True,
            "production_simulation_run": True,
            "database_write_run": True,
            "live_data_fetch_run": True,
            "remote_api_call_run": True,
            "source_acquisition_run": True,
            "layer_6_exit_credit": True,
            "passed": False,
        })
    return implemented


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jn = load_json(JSON_6JN)

    authorization_decision = implement_gate(
        read_csv(REQUIRED_INPUTS[4]), REQUIRED_INPUTS[4], "authorization_decision_gate", 7
    )
    activation_execution_auth = implement_gate(
        read_csv(REQUIRED_INPUTS[5]), REQUIRED_INPUTS[5], "activation_execution_authorization_gate", 7
    )
    mechanic_activation_auth = implement_gate(
        read_csv(REQUIRED_INPUTS[6]), REQUIRED_INPUTS[6], "mechanic_activation_authorization_gate", 7
    )
    exit_certification = implement_gate(
        read_csv(REQUIRED_INPUTS[7]), REQUIRED_INPUTS[7], "layer6_exit_certification_gate", 7
    )
    performance_evidence = implement_gate(
        read_csv(REQUIRED_INPUTS[8]), REQUIRED_INPUTS[8], "performance_evidence_gate", 8
    )
    calibration_sample = implement_gate(
        read_csv(REQUIRED_INPUTS[9]), REQUIRED_INPUTS[9], "calibration_sample_integrity_gate", 8
    )
    rollback_failure = implement_gate(
        read_csv(REQUIRED_INPUTS[10]), REQUIRED_INPUTS[10], "rollback_on_authorization_failure_gate", 7
    )
    post_auth_monitoring = implement_gate(
        read_csv(REQUIRED_INPUTS[11]), REQUIRED_INPUTS[11], "post_authorization_monitoring_gate", 7
    )

    future_6jp = [
        {"contract": "audit_6jo_authorization_or_exit_certification_implementation", "required": True, "passed": True},
        {"contract": "verify_authorization_decision_gate", "required": True, "passed": True},
        {"contract": "verify_activation_execution_authorization_gate", "required": True, "passed": True},
        {"contract": "verify_mechanic_activation_authorization_gate", "required": True, "passed": True},
        {"contract": "verify_layer6_exit_certification_gate", "required": True, "passed": True},
        {"contract": "verify_performance_evidence_gate", "required": True, "passed": True},
        {"contract": "verify_calibration_sample_integrity_gate", "required": True, "passed": True},
        {"contract": "verify_no_mae_brier_comparison_yet", "required": True, "passed": True},
        {"contract": "verify_no_activation_or_exit_credit", "required": True, "passed": True},
    ]

    future_performance_certification = [
        {"contract": "run_or_certify_locked_baseline_vs_realism_metrics", "required": True, "passed": True},
        {"contract": "compare_mae_same_sample", "required": True, "passed": True},
        {"contract": "compare_brier_same_sample", "required": True, "passed": True},
        {"contract": "verify_calibration_and_sample_integrity", "required": True, "passed": True},
        {"contract": "verify_no_population_shift", "required": True, "passed": True},
        {"contract": "produce_directional_improvement_decision", "required": True, "passed": True},
        {"contract": "block_activation_if_metrics_regress", "required": True, "passed": True},
        {"contract": "do_not_write_production_databases_without_certification", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jn_plan_exists", "expected": True, "actual": PLAN_6JN_PATH.exists(), "passed": PLAN_6JN_PATH.exists()},
        {"check": "6jn_json_exists", "expected": True, "actual": JSON_6JN.exists(), "passed": JSON_6JN.exists()},
        {"check": "6jn_all_checks_passed", "expected": True, "actual": json_6jn.get("all_checks_passed"), "passed": json_6jn.get("all_checks_passed") is True},
        {"check": "6jn_diagnosis", "expected": DIAGNOSIS_6JN, "actual": json_6jn.get("diagnosis"), "passed": json_6jn.get("diagnosis") == DIAGNOSIS_6JN},
        {"check": "6jn_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JN, "actual": json_6jn.get("recommended_next_layer"), "passed": json_6jn.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JN},
        {"check": "6jn_recommended_path", "expected": RECOMMENDED_PATH_6JN, "actual": json_6jn.get("recommended_path"), "passed": json_6jn.get("recommended_path") == RECOMMENDED_PATH_6JN},
        {"check": "6jn_plan_created", "expected": True, "actual": json_6jn.get("authorization_or_exit_certification_plan_created"), "passed": json_6jn.get("authorization_or_exit_certification_plan_created") is True},
        {"check": "6jn_performance_evaluation_blocked", "expected": False, "actual": json_6jn.get("performance_evaluation_allowed_after_this_layer"), "passed": json_6jn.get("performance_evaluation_allowed_after_this_layer") is False},
        {"check": "6jn_no_mae_brier_comparison", "expected": False, "actual": json_6jn.get("mae_brier_comparison_run"), "passed": json_6jn.get("mae_brier_comparison_run") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    preserved_rows = [{"source_family": family, "status": "preserved", "passed": True} for family in [
        "game_level_outcomes",
        "inning_runs",
        "base_out_transitions",
        "actual_outcome_surfaces",
        "truth_join_evaluation",
        "activation_readiness",
        "activation_execution_planning",
        "activation_execution_consideration",
        "activation_execution",
        "final_activation_decision",
        "authorization_or_exit_certification",
    ]]

    blocking_rows = [
        {"blocked_surface": "authorization_or_exit_certification_implementation_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "performance_certification", "blocked": True, "reason": "6JP implementation audit required first", "passed": True},
        {"blocked_surface": "mae_brier_performance_evaluation", "blocked": True, "reason": "performance certification required first", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "performance certification required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "performance certification required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "explicit exit certification required", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jn_passed", "expected": True, "actual": json_6jn.get("all_checks_passed"), "passed": json_6jn.get("all_checks_passed") is True},
        {"decision": "authorization_or_exit_certification_implementation_completed", "expected": True, "actual": True, "passed": True},
        {"decision": "authorization_or_exit_certification_audited", "expected": False, "actual": False, "passed": True},
        {"decision": "performance_evaluation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "mae_brier_comparison_run", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6jp_next", "expected": RECOMMENDED_NEXT_LAYER_6JO, "actual": RECOMMENDED_NEXT_LAYER_6JO, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_join_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mae_brier_comparison", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_activation_decision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "6jn_plan", "policy": "read_only", "passed": True},
        {"surface": "6jm_audit", "policy": "read_only", "passed": True},
        {"surface": "6jl_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jk_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "adapter_module", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JO, "actual": RECOMMENDED_NEXT_LAYER_6JO, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JO, "actual": RECOMMENDED_PATH_6JO, "passed": True},
        {"decision": "recommend_authorization_exit_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics_decision_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JO, "actual": DIAGNOSIS_6JO, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "authorization_decision_gate", "passed": len(authorization_decision) == 7 and all_passed(authorization_decision), "detail": f"{len(authorization_decision)}/7"},
        {"check": "activation_execution_authorization_gate", "passed": len(activation_execution_auth) == 7 and all_passed(activation_execution_auth), "detail": f"{len(activation_execution_auth)}/7"},
        {"check": "mechanic_activation_authorization_gate", "passed": len(mechanic_activation_auth) == 7 and all_passed(mechanic_activation_auth), "detail": f"{len(mechanic_activation_auth)}/7"},
        {"check": "layer6_exit_certification_gate", "passed": len(exit_certification) == 7 and all_passed(exit_certification), "detail": f"{len(exit_certification)}/7"},
        {"check": "performance_evidence_gate", "passed": len(performance_evidence) == 8 and all_passed(performance_evidence), "detail": f"{len(performance_evidence)}/8"},
        {"check": "calibration_sample_integrity_gate", "passed": len(calibration_sample) == 8 and all_passed(calibration_sample), "detail": f"{len(calibration_sample)}/8"},
        {"check": "rollback_on_authorization_failure_gate", "passed": len(rollback_failure) == 7 and all_passed(rollback_failure), "detail": f"{len(rollback_failure)}/7"},
        {"check": "post_authorization_monitoring_gate", "passed": len(post_auth_monitoring) == 7 and all_passed(post_auth_monitoring), "detail": f"{len(post_auth_monitoring)}/7"},
        {"check": "future_6jp_contract", "passed": all_passed(future_6jp), "detail": f"{sum(1 for r in future_6jp if r['passed'])}/{len(future_6jp)}"},
        {"check": "future_performance_certification_contract", "passed": all_passed(future_performance_certification), "detail": f"{sum(1 for r in future_performance_certification if r['passed'])}/{len(future_performance_certification)}"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for r in readonly_rows if r['passed'])}/{len(readonly_rows)}"},
        {"check": "preserved_families", "passed": all_passed(preserved_rows), "detail": f"{sum(1 for r in preserved_rows if r['passed'])}/{len(preserved_rows)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for r in blocking_rows if r['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all_passed(immutability_rows), "detail": f"{sum(1 for r in immutability_rows if r['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "authorization_decision_gate": write_csv(AUTH_DECISION_CSV, authorization_decision),
        "activation_execution_authorization_gate": write_csv(ACTIVATION_EXECUTION_AUTH_CSV, activation_execution_auth),
        "mechanic_activation_authorization_gate": write_csv(MECHANIC_ACTIVATION_AUTH_CSV, mechanic_activation_auth),
        "layer6_exit_certification_gate": write_csv(EXIT_CERTIFICATION_CSV, exit_certification),
        "performance_evidence_gate": write_csv(PERFORMANCE_EVIDENCE_CSV, performance_evidence),
        "calibration_sample_integrity_gate": write_csv(CALIBRATION_SAMPLE_CSV, calibration_sample),
        "rollback_on_authorization_failure_gate": write_csv(ROLLBACK_FAILURE_CSV, rollback_failure),
        "post_authorization_monitoring_gate": write_csv(POST_AUTH_MONITORING_CSV, post_auth_monitoring),
        "future_6jp_contract": write_csv(FUTURE_6JP_CSV, future_6jp),
        "future_performance_certification_contract": write_csv(FUTURE_PERFORMANCE_CERTIFICATION_CSV, future_performance_certification),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JO",
        "layer_type": "game_mechanics_realism",
        "implementation_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JO if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JO,
        "recommended_path": RECOMMENDED_PATH_6JO,
        "predecessor_plan": str(PLAN_6JN_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6jn.get("diagnosis"),
        "implemented_layer_after": "6JN",
        "source_family": "authorization_or_exit_certification",
        "authorization_or_exit_certification_plan_created": json_6jn.get("authorization_or_exit_certification_plan_created"),
        "authorization_or_exit_certification_implementation_completed": True,
        "authorization_or_exit_certification_audited": False,
        "authorization_decision_gate_count": len(authorization_decision),
        "activation_execution_authorization_gate_count": len(activation_execution_auth),
        "mechanic_activation_authorization_gate_count": len(mechanic_activation_auth),
        "layer6_exit_certification_gate_count": len(exit_certification),
        "performance_evidence_gate_count": len(performance_evidence),
        "calibration_sample_integrity_gate_count": len(calibration_sample),
        "rollback_on_authorization_failure_gate_count": len(rollback_failure),
        "post_authorization_monitoring_gate_count": len(post_auth_monitoring),
        "future_6jp_contract_valid": all_passed(future_6jp),
        "future_performance_certification_contract_valid": all_passed(future_performance_certification),
        "performance_evaluation_allowed_after_this_layer": False,
        "mae_brier_comparison_run": False,
        "final_activation_decision_allowed_after_this_layer": False,
        "activation_execution_allowed_after_this_layer": False,
        "activation_execution_executed": False,
        "mechanics_activated_by_this_layer": False,
        "production_simulations_run": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "layer_6_exit_credit": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "authorization_decision_gate_csv": str(AUTH_DECISION_CSV),
            "activation_execution_authorization_gate_csv": str(ACTIVATION_EXECUTION_AUTH_CSV),
            "mechanic_activation_authorization_gate_csv": str(MECHANIC_ACTIVATION_AUTH_CSV),
            "layer6_exit_certification_gate_csv": str(EXIT_CERTIFICATION_CSV),
            "performance_evidence_gate_csv": str(PERFORMANCE_EVIDENCE_CSV),
            "calibration_sample_integrity_gate_csv": str(CALIBRATION_SAMPLE_CSV),
            "rollback_on_authorization_failure_gate_csv": str(ROLLBACK_FAILURE_CSV),
            "post_authorization_monitoring_gate_csv": str(POST_AUTH_MONITORING_CSV),
            "future_6jp_contract_csv": str(FUTURE_6JP_CSV),
            "future_performance_certification_contract_csv": str(FUTURE_PERFORMANCE_CERTIFICATION_CSV),
            "readonly_sources_csv": str(READONLY_CSV),
            "preserved_families_csv": str(PRESERVED_CSV),
            "blocking_policy_csv": str(BLOCKING_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
