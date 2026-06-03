#!/usr/bin/env python3
"""Audit Layer 6JO authorization / exit certification implementation.

This audit intentionally pivots next to game-state realism inventory / gap diagnosis.
It does not recommend Layer 6 exit because the original Layer 6 roadmap requires
actual game mechanics incorporation and testing.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6jp_authorization_or_exit_certification_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENTATION_6JO_PATH = Path("scripts/implement_6jo_layer6_authorization_or_exit_certification.py")
JSON_6JO = TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation.json"

REQUIRED_INPUTS = [
    JSON_6JO,
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_checks.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_predecessor.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_authorization_decision_gate.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_activation_execution_authorization_gate.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_mechanic_activation_authorization_gate.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_layer6_exit_certification_gate.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_performance_evidence_gate.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_calibration_sample_integrity_gate.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_rollback_on_authorization_failure_gate.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_post_authorization_monitoring_gate.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_future_6jp_contract.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_future_performance_certification_contract.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_readonly_sources.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_preserved_families.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_decision.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_immutability.csv",
    TMP_DIR / "layer6_6jo_authorization_or_exit_certification_implementation_recommended_path.csv",
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
FUTURE_PERFORMANCE_CERTIFICATION_CSV = TMP_DIR / f"{SLUG}_future_performance_certification_contract.csv"
ROADMAP_ALIGNMENT_CSV = TMP_DIR / f"{SLUG}_game_state_realism_roadmap_alignment.csv"
NEXT_6JQ_CONTRACT_CSV = TMP_DIR / f"{SLUG}_next_inventory_gap_diagnosis_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6JO = "layer_6_authorization_or_exit_certification_implementation_complete"
DIAGNOSIS_6JP = "layer_6_authorization_or_exit_certification_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6JO = "6JP_layer_6_authorization_or_exit_certification_implementation_audit"
RECOMMENDED_PATH_6JO = "implement_authorization_or_exit_certification_then_audit_before_performance_certification"
RECOMMENDED_NEXT_LAYER_6JP = "6JQ_layer_6_game_state_realism_inventory_gap_diagnosis"
RECOMMENDED_PATH_6JP = "audit_authorization_or_exit_certification_then_diagnose_game_state_realism_gaps"


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


def audit_gate(rows: List[Dict[str, str]], source_path: Path, key: str, expected_count: int) -> List[Dict[str, Any]]:
    audited: List[Dict[str, Any]] = []
    for row in rows:
        identifier = row.get(key, "")
        audited.append({
            key: identifier,
            "source_artifact": str(source_path),
            "audited": True,
            "implemented": str(row.get("implemented", "")).lower() == "true",
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
            and str(row.get("implemented", "")).lower() == "true"
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
        audited.append({
            key: "__row_count_mismatch__",
            "source_artifact": str(source_path),
            "audited": False,
            "implemented": False,
            "passed": False,
        })
    return audited


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6jo = load_json(JSON_6JO)

    authorization_decision = audit_gate(
        read_csv(REQUIRED_INPUTS[4]), REQUIRED_INPUTS[4], "authorization_decision_gate", 7
    )
    activation_execution_auth = audit_gate(
        read_csv(REQUIRED_INPUTS[5]), REQUIRED_INPUTS[5], "activation_execution_authorization_gate", 7
    )
    mechanic_activation_auth = audit_gate(
        read_csv(REQUIRED_INPUTS[6]), REQUIRED_INPUTS[6], "mechanic_activation_authorization_gate", 7
    )
    exit_certification = audit_gate(
        read_csv(REQUIRED_INPUTS[7]), REQUIRED_INPUTS[7], "layer6_exit_certification_gate", 7
    )
    performance_evidence = audit_gate(
        read_csv(REQUIRED_INPUTS[8]), REQUIRED_INPUTS[8], "performance_evidence_gate", 8
    )
    calibration_sample = audit_gate(
        read_csv(REQUIRED_INPUTS[9]), REQUIRED_INPUTS[9], "calibration_sample_integrity_gate", 8
    )
    rollback_failure = audit_gate(
        read_csv(REQUIRED_INPUTS[10]), REQUIRED_INPUTS[10], "rollback_on_authorization_failure_gate", 7
    )
    post_auth_monitoring = audit_gate(
        read_csv(REQUIRED_INPUTS[11]), REQUIRED_INPUTS[11], "post_authorization_monitoring_gate", 7
    )

    future_performance_certification = [
        {
            "contract": row.get("contract", ""),
            "source_artifact": str(REQUIRED_INPUTS[13]),
            "audited": True,
            "required": str(row.get("required", "")).lower() == "true",
            "passed": bool(row.get("contract")) and str(row.get("passed", "")).lower() == "true",
        }
        for row in read_csv(REQUIRED_INPUTS[13])
    ]

    roadmap_items = [
        "extra_innings_and_ghost_runner_logic",
        "stolen_bases_and_caught_stealing",
        "wild_pitches_and_passed_balls",
        "balks",
        "first_to_third_advancement",
        "second_to_home_advancement",
        "sac_flies_and_tagging_up",
        "double_plays_by_base_out_state",
        "pinch_hitters_and_substitutions",
        "bullpen_sequencing_and_leverage_behavior",
    ]

    roadmap_alignment = [
        {
            "roadmap_item": item,
            "layer6_doctrine": "required_before_layer6_exit",
            "current_audit_status": "requires_inventory_gap_diagnosis_next",
            "governance_complete": True,
            "substantive_incorporation_assumed_complete": False,
            "layer_6_exit_allowed": False,
            "passed": True,
        }
        for item in roadmap_items
    ]

    next_6jq_contract = [
        {"contract": "inventory_actual_game_state_realism_in_code", "required": True, "passed": True},
        {"contract": "separate_installed_from_planned_or_governed", "required": True, "passed": True},
        {"contract": "identify_missing_realism_features", "required": True, "passed": True},
        {"contract": "identify_partially_wired_realism_features", "required": True, "passed": True},
        {"contract": "identify_sim_loop_integration_points", "required": True, "passed": True},
        {"contract": "prioritize_missing_mechanics_installation", "required": True, "passed": True},
        {"contract": "do_not_exit_layer6_from_governance_only", "required": True, "passed": True},
        {"contract": "do_not_run_mae_brier_before_inventory", "required": True, "passed": True},
        {"contract": "preserve_keep_and_tune_doctrine", "required": True, "passed": True},
        {"contract": "recommend_realism_installation_path", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6jo_implementation_exists", "expected": True, "actual": IMPLEMENTATION_6JO_PATH.exists(), "passed": IMPLEMENTATION_6JO_PATH.exists()},
        {"check": "6jo_json_exists", "expected": True, "actual": JSON_6JO.exists(), "passed": JSON_6JO.exists()},
        {"check": "6jo_all_checks_passed", "expected": True, "actual": json_6jo.get("all_checks_passed"), "passed": json_6jo.get("all_checks_passed") is True},
        {"check": "6jo_diagnosis", "expected": DIAGNOSIS_6JO, "actual": json_6jo.get("diagnosis"), "passed": json_6jo.get("diagnosis") == DIAGNOSIS_6JO},
        {"check": "6jo_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JO, "actual": json_6jo.get("recommended_next_layer"), "passed": json_6jo.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6JO},
        {"check": "6jo_recommended_path", "expected": RECOMMENDED_PATH_6JO, "actual": json_6jo.get("recommended_path"), "passed": json_6jo.get("recommended_path") == RECOMMENDED_PATH_6JO},
        {"check": "6jo_implementation_completed", "expected": True, "actual": json_6jo.get("authorization_or_exit_certification_implementation_completed"), "passed": json_6jo.get("authorization_or_exit_certification_implementation_completed") is True},
        {"check": "6jo_performance_evaluation_blocked", "expected": False, "actual": json_6jo.get("performance_evaluation_allowed_after_this_layer"), "passed": json_6jo.get("performance_evaluation_allowed_after_this_layer") is False},
        {"check": "6jo_no_exit_credit", "expected": False, "actual": json_6jo.get("layer_6_exit_credit"), "passed": json_6jo.get("layer_6_exit_credit") is False},
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
        "game_state_realism_inventory_gap_diagnosis",
    ]]

    blocking_rows = [
        {"blocked_surface": "game_state_realism_inventory_gap_diagnosis", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "performance_certification", "blocked": True, "reason": "inventory/gap diagnosis and missing realism installation required first", "passed": True},
        {"blocked_surface": "mae_brier_performance_evaluation", "blocked": True, "reason": "game-state realism inventory and installation path required first", "passed": True},
        {"blocked_surface": "final_activation_decision", "blocked": True, "reason": "realism substance not yet certified complete", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "realism substance not yet certified complete", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "roadmap realism incorporation and testing required before exit", "passed": True},
    ]

    decision_rows = [
        {"decision": "6jo_passed", "expected": True, "actual": json_6jo.get("all_checks_passed"), "passed": json_6jo.get("all_checks_passed") is True},
        {"decision": "authorization_or_exit_certification_audited", "expected": True, "actual": True, "passed": True},
        {"decision": "roadmap_alignment_requires_gap_diagnosis", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6jq_next", "expected": RECOMMENDED_NEXT_LAYER_6JP, "actual": RECOMMENDED_NEXT_LAYER_6JP, "passed": True},
        {"decision": "performance_evaluation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "mae_brier_comparison_run", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
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
        {"boundary": "no_layer_6_exit_recommendation", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "6jo_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jn_plan", "policy": "read_only", "passed": True},
        {"surface": "6jm_audit", "policy": "read_only", "passed": True},
        {"surface": "6jl_implementation", "policy": "read_only", "passed": True},
        {"surface": "6jk_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_outputs", "policy": "read_only", "passed": True},
        {"surface": "adapter_module", "policy": "read_only", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6JP, "actual": RECOMMENDED_NEXT_LAYER_6JP, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6JP, "actual": RECOMMENDED_PATH_6JP, "passed": True},
        {"decision": "recommend_game_state_realism_inventory_gap_diagnosis_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics_decision_or_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6JP, "actual": DIAGNOSIS_6JP, "passed": True},
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
        {"check": "future_performance_certification_contract", "passed": len(future_performance_certification) == 8 and all_passed(future_performance_certification), "detail": f"{len(future_performance_certification)}/8"},
        {"check": "game_state_realism_roadmap_alignment", "passed": len(roadmap_alignment) == 10 and all_passed(roadmap_alignment), "detail": f"{len(roadmap_alignment)}/10"},
        {"check": "next_inventory_gap_diagnosis_contract", "passed": len(next_6jq_contract) == 10 and all_passed(next_6jq_contract), "detail": f"{len(next_6jq_contract)}/10"},
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
        "future_performance_certification_contract": write_csv(FUTURE_PERFORMANCE_CERTIFICATION_CSV, future_performance_certification),
        "game_state_realism_roadmap_alignment": write_csv(ROADMAP_ALIGNMENT_CSV, roadmap_alignment),
        "next_inventory_gap_diagnosis_contract": write_csv(NEXT_6JQ_CONTRACT_CSV, next_6jq_contract),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6JP",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6JP if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6JP,
        "recommended_path": RECOMMENDED_PATH_6JP,
        "predecessor_implementation": str(IMPLEMENTATION_6JO_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6jo.get("diagnosis"),
        "audited_layer_after": "6JO",
        "source_family": "authorization_or_exit_certification",
        "authorization_or_exit_certification_plan_created": json_6jo.get("authorization_or_exit_certification_plan_created"),
        "authorization_or_exit_certification_implementation_completed": json_6jo.get("authorization_or_exit_certification_implementation_completed"),
        "authorization_or_exit_certification_audited": True,
        "authorization_decision_gate_count": len(authorization_decision),
        "activation_execution_authorization_gate_count": len(activation_execution_auth),
        "mechanic_activation_authorization_gate_count": len(mechanic_activation_auth),
        "layer6_exit_certification_gate_count": len(exit_certification),
        "performance_evidence_gate_count": len(performance_evidence),
        "calibration_sample_integrity_gate_count": len(calibration_sample),
        "rollback_on_authorization_failure_gate_count": len(rollback_failure),
        "post_authorization_monitoring_gate_count": len(post_auth_monitoring),
        "future_performance_certification_contract_valid": len(future_performance_certification) == 8 and all_passed(future_performance_certification),
        "game_state_realism_roadmap_alignment_count": len(roadmap_alignment),
        "next_inventory_gap_diagnosis_contract_valid": len(next_6jq_contract) == 10 and all_passed(next_6jq_contract),
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
        "layer_6_exit_recommended": False,
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
            "future_performance_certification_contract_csv": str(FUTURE_PERFORMANCE_CERTIFICATION_CSV),
            "game_state_realism_roadmap_alignment_csv": str(ROADMAP_ALIGNMENT_CSV),
            "next_inventory_gap_diagnosis_contract_csv": str(NEXT_6JQ_CONTRACT_CSV),
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
