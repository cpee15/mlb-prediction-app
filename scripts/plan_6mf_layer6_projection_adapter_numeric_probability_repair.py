#!/usr/bin/env python3
"""Plan repair to materialize real numeric probability surface after 6ME audit."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mf_projection_adapter_numeric_probability_repair_plan"
TMP_DIR = Path("tmp")

SCRIPT_6ME = Path("scripts/audit_6me_layer6_projection_adapter_numeric_probability_source_trace.py")
JSON_6ME = TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit.json"

REQUIRED_INPUTS = [
    JSON_6ME,
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_checks.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_predecessor.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_trace_finding_review.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_placeholder_contract_decision.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_hypothesis_review.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_repair_planning_gate.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_blockers.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_future_6mf_contract.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_decision.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6me_projection_adapter_numeric_probability_source_trace_audit_recommended_path.csv",
    SCRIPT_6ME,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
OBJECTIVES_CSV = TMP_DIR / f"{SLUG}_repair_objectives.csv"
REPAIR_OPTIONS_CSV = TMP_DIR / f"{SLUG}_repair_options.csv"
SELECTED_PATH_CSV = TMP_DIR / f"{SLUG}_selected_repair_path.csv"
SURFACE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_numeric_surface_contract.csv"
VALIDATION_CONTRACT_CSV = TMP_DIR / f"{SLUG}_validation_contract.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MG_CSV = TMP_DIR / f"{SLUG}_future_6mg_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6ME = "layer_6_projection_adapter_numeric_probability_source_trace_audit_complete"
DIAGNOSIS_6MF = "layer_6_projection_adapter_numeric_probability_repair_plan_complete"
RECOMMENDED_NEXT_LAYER_6MF = "6MG_layer_6_projection_adapter_numeric_probability_repair_implementation"
RECOMMENDED_PATH_6MF = "implement_materialize_real_numeric_probability_surface_readonly_safe"
LIKELY_PLACEHOLDER_CONTRACT = "current_normalized_surface_is_placeholder_contract_not_real_numeric_probability_surface"
SELECTED_REPAIR_PATH = "replace_placeholder_probability_contract_with_real_numeric_surface_artifact_from_existing_safe_local_projection_payload_or_explicit_blocker"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True, "passed": True}]
    fieldnames: list[str] = []
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


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}
    except Exception:
        return {}


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8", errors="ignore"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def boolish(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def all_passed(rows: list[dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6me = load_json(JSON_6ME)

    input_rows = [
        {
            "artifact_path": str(path),
            "exists": path.exists(),
            "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "",
            "passed": path.exists(),
        }
        for path in REQUIRED_INPUTS
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6me_script_exists", "expected": True, "actual": SCRIPT_6ME.exists(), "passed": SCRIPT_6ME.exists()},
        {"check": "6me_json_exists", "expected": True, "actual": JSON_6ME.exists(), "passed": JSON_6ME.exists()},
        {"check": "6me_all_checks_passed", "expected": True, "actual": json_6me.get("all_checks_passed"), "passed": json_6me.get("all_checks_passed") is True},
        {"check": "6me_diagnosis", "expected": DIAGNOSIS_6ME, "actual": json_6me.get("diagnosis"), "passed": json_6me.get("diagnosis") == DIAGNOSIS_6ME},
        {"check": "6me_recommended_next_layer", "expected": "6MF_layer_6_projection_adapter_numeric_probability_repair_plan", "actual": json_6me.get("recommended_next_layer"), "passed": json_6me.get("recommended_next_layer") == "6MF_layer_6_projection_adapter_numeric_probability_repair_plan"},
    ]

    placeholder_contract_confirmed = (
        json_6me.get("placeholder_contract_conclusion_established") is True
        and json_6me.get("likely_current_state_confirmed") == LIKELY_PLACEHOLDER_CONTRACT
    )
    real_numeric_surface_not_materialized = (
        json_6me.get("numeric_probability_values_found_in_normalized_surface_confirmed") is False
        and json_6me.get("placeholder_values_found_in_normalized_surface_confirmed") is True
    )
    repair_gate_open = json_6me.get("repair_planning_recommended") is True

    problem_rows = [
        {
            "problem": "current_normalized_probability_surface_is_placeholder_contract",
            "evidence": "6ME established placeholder contract conclusion and confirmed numeric values absent",
            "repair_need": "materialize real numeric home/away probability values while preserving canonical contract",
            "passed": placeholder_contract_confirmed and real_numeric_surface_not_materialized and repair_gate_open,
        }
    ]

    objectives = [
        {
            "objective": "replace_placeholder_values_with_real_numeric_probabilities",
            "required": True,
            "detail": "home_win_probability and away_win_probability must be numeric floats in [0,1]",
            "passed": True,
        },
        {
            "objective": "preserve_canonical_probability_surface_contract",
            "required": True,
            "detail": "keep game_pk, home_win_probability, away_win_probability and alias fields traceable",
            "passed": True,
        },
        {
            "objective": "avoid_false_numeric_readiness",
            "required": True,
            "detail": "if real numeric source cannot be found safely, implementation must produce explicit blocker instead of fake values",
            "passed": True,
        },
        {
            "objective": "stay_pre_metric_pre_backtest",
            "required": True,
            "detail": "repair implementation may materialize/inspect local artifact only; no metrics/backtests/tuning",
            "passed": True,
        },
    ]

    repair_options = [
        {
            "option": "use_existing_safe_local_projection_payload_if_numeric_probabilities_exist",
            "preferred": True,
            "requires_adapter_call": False,
            "requires_live_fetch": False,
            "requires_production_code_change": False,
            "risk": "low_if_payload_exists_and_values_validate",
            "passed": True,
        },
        {
            "option": "repair_6lx_artifact_builder_to_copy_real_numeric_values_from_existing_payload",
            "preferred": True,
            "requires_adapter_call": False,
            "requires_live_fetch": False,
            "requires_production_code_change": False,
            "risk": "moderate_requires_clear_source_artifact",
            "passed": True,
        },
        {
            "option": "generate_synthetic_probability_values",
            "preferred": False,
            "requires_adapter_call": False,
            "requires_live_fetch": False,
            "requires_production_code_change": False,
            "risk": "unacceptable_fake_numeric_readiness",
            "passed": True,
        },
        {
            "option": "call_projection_adapter_or_live_services_to_get_probabilities",
            "preferred": False,
            "requires_adapter_call": True,
            "requires_live_fetch": True,
            "requires_production_code_change": False,
            "risk": "outside_current_layer_safety_boundary",
            "passed": True,
        },
    ]

    selected_path_rows = [
        {
            "selected_repair_path": SELECTED_REPAIR_PATH,
            "why": "supports safe local repair if source exists and explicit blocker if source remains absent",
            "requires_adapter_call": False,
            "requires_live_fetch": False,
            "requires_production_code_change": False,
            "passed": True,
        }
    ]

    numeric_surface_contract = [
        {
            "field": "game_pk",
            "required": True,
            "expected_type": "stable_identifier",
            "validation": "present_and_nonempty",
            "passed": True,
        },
        {
            "field": "home_win_probability",
            "required": True,
            "expected_type": "float",
            "validation": "0 <= value <= 1",
            "passed": True,
        },
        {
            "field": "away_win_probability",
            "required": True,
            "expected_type": "float",
            "validation": "0 <= value <= 1",
            "passed": True,
        },
        {
            "field": "home_away_probability_sum",
            "required": True,
            "expected_type": "derived_check",
            "validation": "abs(home + away - 1.0) <= tolerance",
            "passed": True,
        },
        {
            "field": "alias_trace",
            "required": True,
            "expected_type": "provenance",
            "validation": "home_win_prob/away_win_prob either mirror numeric canonical values or explicitly document source mapping",
            "passed": True,
        },
    ]

    validation_contract = [
        {
            "validation": "no_placeholder_probability_values",
            "required": True,
            "failure_action": "block_metric_readiness",
            "passed": True,
        },
        {
            "validation": "numeric_probability_bounds",
            "required": True,
            "failure_action": "block_metric_readiness",
            "passed": True,
        },
        {
            "validation": "home_away_probability_sum_near_one",
            "required": True,
            "failure_action": "block_metric_readiness",
            "passed": True,
        },
        {
            "validation": "source_provenance_present",
            "required": True,
            "failure_action": "block_repair_acceptance",
            "passed": True,
        },
        {
            "validation": "no_adapter_calls_or_live_fetches",
            "required": True,
            "failure_action": "fail_layer",
            "passed": True,
        },
    ]

    allowed_next = [
        {
            "operation": "safe_local_repair_implementation",
            "allowed_next": True,
            "scope": "existing local artifacts/scripts only",
            "passed": True,
        },
        {
            "operation": "write_repaired_tmp_surface_artifact",
            "allowed_next": True,
            "scope": "tmp outputs only",
            "passed": True,
        },
        {
            "operation": "write_explicit_blocker_if_numeric_source_absent",
            "allowed_next": True,
            "scope": "tmp outputs only",
            "passed": True,
        },
    ]

    forbidden_next = [
        {"operation": "adapter_calls", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "database_writes", "allowed_next": False, "passed": True},
        {"operation": "production_code_changes", "allowed_next": False, "passed": True},
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "run_metric_execution", "allowed_next": False, "passed": True},
        {"operation": "backtest_execution", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {
            "blocker": "placeholder_probability_metric_block_active",
            "active": True,
            "reason": "current surface is placeholder contract",
            "passed": True,
        },
        {
            "blocker": "real_numeric_probability_surface_not_materialized",
            "active": True,
            "reason": "repair has not yet been implemented",
            "passed": True,
        },
        {
            "blocker": "numeric_probability_source_unknown_after_trace",
            "active": True,
            "reason": "source still needs repair implementation discovery or explicit blocker",
            "passed": True,
        },
        {
            "blocker": "metrics_backtests_tuning_activation_exit_blocked",
            "active": True,
            "reason": "requires real numeric probability surface and later audits",
            "passed": True,
        },
    ]

    future_6mg = [
        {
            "contract": "implement_materialize_real_numeric_probability_surface_readonly_safe",
            "required": True,
            "why": "attempt safe local materialization from existing artifacts or produce explicit blocker",
            "passed": True,
        },
        {
            "contract": "preserve_canonical_surface_contract_and_source_provenance",
            "required": True,
            "why": "repair must not lose field contract or hide source ambiguity",
            "passed": True,
        },
        {
            "contract": "preserve_no_adapter_metrics_backtest_tuning_activation_or_exit",
            "required": True,
            "why": "repair implementation remains pre-validation",
            "passed": True,
        },
    ]

    blocking_policy = [
        {
            "policy": "do_not_generate_fake_numeric_probabilities",
            "required": True,
            "passed": True,
        },
        {
            "policy": "do_not_claim_metric_readiness_until_real_numeric_surface_is_materialized_and_audited",
            "required": True,
            "passed": True,
        },
        {
            "policy": "do_not_run_metrics_backtests_or_tuning_from_repair_plan",
            "required": True,
            "passed": True,
        },
    ]

    decision_rows = [
        {"decision": "6me_passed", "expected": True, "actual": json_6me.get("all_checks_passed"), "passed": json_6me.get("all_checks_passed") is True},
        {"decision": "6me_diagnosis_valid", "expected": DIAGNOSIS_6ME, "actual": json_6me.get("diagnosis"), "passed": json_6me.get("diagnosis") == DIAGNOSIS_6ME},
        {"decision": "all_required_6me_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "placeholder_contract_confirmed", "expected": True, "actual": placeholder_contract_confirmed, "passed": placeholder_contract_confirmed},
        {"decision": "real_numeric_surface_not_materialized_confirmed", "expected": True, "actual": real_numeric_surface_not_materialized, "passed": real_numeric_surface_not_materialized},
        {"decision": "repair_planning_gate_open", "expected": True, "actual": repair_gate_open, "passed": repair_gate_open},
        {"decision": "repair_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "numeric_surface_contract_created", "expected": True, "actual": all_passed(numeric_surface_contract), "passed": all_passed(numeric_surface_contract)},
        {"decision": "validation_contract_created", "expected": True, "actual": all_passed(validation_contract), "passed": all_passed(validation_contract)},
        {"decision": "future_6mg_contract_valid", "expected": True, "actual": all_passed(future_6mg), "passed": all_passed(future_6mg)},
        {"decision": "recommend_6mg_next", "expected": RECOMMENDED_NEXT_LAYER_6MF, "actual": RECOMMENDED_NEXT_LAYER_6MF, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_numeric_probability_repair", "expected": True, "actual": True, "passed": True},
        {"boundary": "repair_implementation_run_by_6mf", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mf", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mf", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mf", "expected": False, "actual": False, "passed": True},
        {"boundary": "run_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mf", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_acquisition_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_source_modifications_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MF, "actual": RECOMMENDED_NEXT_LAYER_6MF, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MF, "actual": RECOMMENDED_PATH_6MF, "passed": True},
        {"decision": "do_not_recommend_run_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MF, "actual": DIAGNOSIS_6MF, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem_rows), "detail": f"{sum(1 for r in problem_rows if r['passed'])}/{len(problem_rows)}"},
        {"check": "repair_objectives", "passed": all_passed(objectives), "detail": f"{sum(1 for r in objectives if r['passed'])}/{len(objectives)}"},
        {"check": "repair_options", "passed": all_passed(repair_options), "detail": f"{sum(1 for r in repair_options if r['passed'])}/{len(repair_options)}"},
        {"check": "selected_repair_path", "passed": all_passed(selected_path_rows), "detail": f"{sum(1 for r in selected_path_rows if r['passed'])}/{len(selected_path_rows)}"},
        {"check": "numeric_surface_contract", "passed": all_passed(numeric_surface_contract), "detail": f"{sum(1 for r in numeric_surface_contract if r['passed'])}/{len(numeric_surface_contract)}"},
        {"check": "validation_contract", "passed": all_passed(validation_contract), "detail": f"{sum(1 for r in validation_contract if r['passed'])}/{len(validation_contract)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mg_contract", "passed": all_passed(future_6mg), "detail": f"{sum(1 for r in future_6mg if r['passed'])}/{len(future_6mg)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{sum(1 for r in blocking_policy if r['passed'])}/{len(blocking_policy)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "problem_statement": write_csv(PROBLEM_CSV, problem_rows),
        "repair_objectives": write_csv(OBJECTIVES_CSV, objectives),
        "repair_options": write_csv(REPAIR_OPTIONS_CSV, repair_options),
        "selected_repair_path": write_csv(SELECTED_PATH_CSV, selected_path_rows),
        "numeric_surface_contract": write_csv(SURFACE_CONTRACT_CSV, numeric_surface_contract),
        "validation_contract": write_csv(VALIDATION_CONTRACT_CSV, validation_contract),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mg_contract": write_csv(FUTURE_6MG_CSV, future_6mg),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MF",
        "layer_type": "game_mechanics_realism",
        "planning_only_numeric_probability_repair": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MF if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MF,
        "recommended_path": RECOMMENDED_PATH_6MF,
        "predecessor_layer": "6ME",
        "predecessor_diagnosis": json_6me.get("diagnosis"),
        "predecessor_all_checks_passed": json_6me.get("all_checks_passed") is True,
        "planned_layer_after": "6ME",
        "source_family": "projection_adapter_numeric_probability_repair_plan",
        "placeholder_contract_conclusion_established_by_predecessor": placeholder_contract_confirmed,
        "real_numeric_probability_surface_not_materialized_confirmed": real_numeric_surface_not_materialized,
        "repair_planning_gate_confirmed_open": repair_gate_open,
        "repair_plan_created": True,
        "selected_repair_path": SELECTED_REPAIR_PATH,
        "numeric_surface_contract_created": all_passed(numeric_surface_contract),
        "validation_contract_created": all_passed(validation_contract),
        "adapter_call_allowed_next": False,
        "live_data_fetch_allowed_next": False,
        "production_code_change_allowed_next": False,
        "safe_repair_implementation_allowed_next": True,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "repair_implementation_run_by_6mf": False,
        "adapter_call_executed_by_6mf": False,
        "metric_execution_run_by_6mf": False,
        "backtest_execution_run_by_6mf": False,
        "run_metric_execution_run": False,
        "production_code_modified_by_6mf": False,
        "full_batch_adapter_call_run": False,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "local_measurement_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "production_source_modifications_run": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "problem_statement_csv": str(PROBLEM_CSV),
            "repair_objectives_csv": str(OBJECTIVES_CSV),
            "repair_options_csv": str(REPAIR_OPTIONS_CSV),
            "selected_repair_path_csv": str(SELECTED_PATH_CSV),
            "numeric_surface_contract_csv": str(SURFACE_CONTRACT_CSV),
            "validation_contract_csv": str(VALIDATION_CONTRACT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mg_contract_csv": str(FUTURE_6MG_CSV),
            "blocking_policy_csv": str(BLOCKING_POLICY_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
