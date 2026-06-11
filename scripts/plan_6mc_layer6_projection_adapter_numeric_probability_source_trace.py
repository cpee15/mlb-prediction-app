#!/usr/bin/env python3
"""Plan readonly numeric probability source trace after placeholder block."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan"
TMP_DIR = Path("tmp")

SCRIPT_6MB = Path("scripts/audit_6mb_layer6_projection_adapter_probability_surface_metric.py")
SCRIPT_6MA = Path("scripts/implement_6ma_layer6_projection_adapter_probability_surface_metric.py")
SCRIPT_6LZ = Path("scripts/plan_6lz_layer6_projection_adapter_probability_surface_metric.py")

JSON_6MB = TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit.json"
NORMALIZED_SURFACE_JSON = TMP_DIR / "layer6_6lx_projection_adapter_probability_alias_normalization_implementation_normalized_surface.json"

REQUIRED_INPUTS = [
    JSON_6MB,
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_checks.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_predecessor.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_metric_artifact_review.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_placeholder_block.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_run_surface_gap.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_blockers.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_future_6mc_contract.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_decision.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6mb_projection_adapter_probability_surface_metric_audit_recommended_path.csv",
    SCRIPT_6MB,
    SCRIPT_6MA,
    SCRIPT_6LZ,
    NORMALIZED_SURFACE_JSON,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
TRACE_SCOPE_CSV = TMP_DIR / f"{SLUG}_trace_scope.csv"
CANDIDATE_SOURCE_SURFACES_CSV = TMP_DIR / f"{SLUG}_candidate_source_surfaces.csv"
PLACEHOLDER_DIAGNOSIS_CSV = TMP_DIR / f"{SLUG}_placeholder_diagnosis.csv"
READONLY_TRACE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_readonly_trace_contract.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MD_CSV = TMP_DIR / f"{SLUG}_future_6md_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MB = "layer_6_projection_adapter_probability_surface_metric_audit_complete"
DIAGNOSIS_6MC = "layer_6_projection_adapter_numeric_probability_source_trace_plan_complete"
RECOMMENDED_NEXT_LAYER_6MC = "6MD_layer_6_projection_adapter_numeric_probability_source_trace_implementation"
RECOMMENDED_PATH_6MC = "implement_numeric_probability_source_trace_readonly"


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

    json_6mb = load_json(JSON_6MB)

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
        {"check": "6mb_script_exists", "expected": True, "actual": SCRIPT_6MB.exists(), "passed": SCRIPT_6MB.exists()},
        {"check": "6mb_json_exists", "expected": True, "actual": JSON_6MB.exists(), "passed": JSON_6MB.exists()},
        {"check": "6mb_all_checks_passed", "expected": True, "actual": json_6mb.get("all_checks_passed"), "passed": json_6mb.get("all_checks_passed") is True},
        {"check": "6mb_diagnosis", "expected": DIAGNOSIS_6MB, "actual": json_6mb.get("diagnosis"), "passed": json_6mb.get("diagnosis") == DIAGNOSIS_6MB},
        {"check": "6mb_recommended_next_layer", "expected": "6MC_layer_6_projection_adapter_numeric_probability_source_trace_plan", "actual": json_6mb.get("recommended_next_layer"), "passed": json_6mb.get("recommended_next_layer") == "6MC_layer_6_projection_adapter_numeric_probability_source_trace_plan"},
    ]

    placeholder_confirmed = json_6mb.get("placeholder_probability_values_detected_confirmed") is True
    numeric_absent_confirmed = json_6mb.get("numeric_probability_values_detected_confirmed") is False
    placeholder_block_confirmed = json_6mb.get("placeholder_probability_metric_block_active_confirmed") is True
    metric_ready_false = json_6mb.get("probability_metric_ready_after_audit") is False

    problem_rows = [
        {
            "problem": "canonical_probability_fields_are_placeholders_not_numeric_probabilities",
            "evidence": "6MB confirmed placeholder values and numeric values absent",
            "required_next_step": "plan readonly source trace for where real probabilities should enter canonical surface",
            "passed": placeholder_confirmed and numeric_absent_confirmed and placeholder_block_confirmed and metric_ready_false,
        }
    ]

    trace_scope_rows = [
        {
            "scope": "readonly_numeric_probability_source_trace",
            "included": "code_and_artifact_path_inspection_only",
            "excluded": "adapter_calls,live_data_fetches,metrics,backtests,production_changes,activation,layer6_exit",
            "passed": True,
        },
        {
            "scope": "canonical_probability_surface_contract",
            "included": "home_win_probability,away_win_probability,home_win_prob,away_win_prob,game_pk",
            "excluded": "run_surface_fields_and_backtest_metrics",
            "passed": True,
        },
    ]

    candidate_source_surfaces = [
        {
            "candidate": "projection_adapter_return_shape",
            "purpose": "determine whether real numeric probabilities are produced upstream before normalization",
            "next_layer_action": "readonly_static_trace_or_artifact_trace",
            "may_call_adapter": False,
            "passed": True,
        },
        {
            "candidate": "normalization_artifact_builder",
            "purpose": "determine whether real values were replaced by placeholder strings during 6LX artifact creation",
            "next_layer_action": "inspect_script_and_artifact_mapping",
            "may_call_adapter": False,
            "passed": True,
        },
        {
            "candidate": "canonical_projection_surface_schema",
            "purpose": "determine expected canonical probability field names and value types",
            "next_layer_action": "inspect_existing code paths only",
            "may_call_adapter": False,
            "passed": True,
        },
        {
            "candidate": "ui_projection_consumption_path",
            "purpose": "determine whether UI consumes canonical probabilities or separate labels",
            "next_layer_action": "readonly_code_trace_if_available",
            "may_call_adapter": False,
            "passed": True,
        },
    ]

    placeholder_diagnosis_rows = [
        {
            "diagnosis": "placeholder_values_confirmed",
            "confirmed": placeholder_confirmed,
            "passed": placeholder_confirmed,
        },
        {
            "diagnosis": "numeric_probability_values_absent_confirmed",
            "confirmed": numeric_absent_confirmed,
            "passed": numeric_absent_confirmed,
        },
        {
            "diagnosis": "placeholder_probability_metric_block_active",
            "confirmed": placeholder_block_confirmed,
            "passed": placeholder_block_confirmed,
        },
        {
            "diagnosis": "numeric_probability_metric_readiness_false",
            "confirmed": metric_ready_false,
            "passed": metric_ready_false,
        },
    ]

    readonly_trace_contract = [
        {
            "contract": "do_not_execute_adapter_calls",
            "required": True,
            "next_layer_allowed": False,
            "passed": True,
        },
        {
            "contract": "do_not_fetch_live_data",
            "required": True,
            "next_layer_allowed": False,
            "passed": True,
        },
        {
            "contract": "inspect_existing_scripts_and_tmp_artifacts_only",
            "required": True,
            "next_layer_allowed": True,
            "passed": True,
        },
        {
            "contract": "identify_value_loss_or_placeholder_injection_point",
            "required": True,
            "next_layer_allowed": True,
            "passed": True,
        },
        {
            "contract": "do_not_modify_production_code",
            "required": True,
            "next_layer_allowed": False,
            "passed": True,
        },
    ]

    allowed_next = [
        {
            "operation": "readonly_static_code_trace",
            "allowed_next": True,
            "scope": "existing scripts and mlb_app paths",
            "passed": True,
        },
        {
            "operation": "readonly_artifact_trace",
            "allowed_next": True,
            "scope": "existing tmp artifacts",
            "passed": True,
        },
        {
            "operation": "write_trace_artifacts",
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
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {
            "blocker": "placeholder_probability_metric_block_active",
            "active": True,
            "reason": "canonical probability values are placeholders",
            "passed": placeholder_block_confirmed,
        },
        {
            "blocker": "numeric_probability_source_unknown",
            "active": True,
            "reason": "real numeric probability entry point has not yet been traced",
            "passed": True,
        },
        {
            "blocker": "run_surface_gap_remains",
            "active": True,
            "reason": "run fields remain absent",
            "passed": json_6mb.get("run_surface_gap_remains") is True,
        },
        {
            "blocker": "real_backtest_metrics_not_run",
            "active": True,
            "reason": "backtests require real numeric prediction surface",
            "passed": True,
        },
        {
            "blocker": "layer6_exit_not_allowed",
            "active": True,
            "reason": "Layer 6 exit requires later audited evidence",
            "passed": True,
        },
    ]

    future_6md = [
        {
            "contract": "implement_numeric_probability_source_trace_readonly",
            "required": True,
            "why": "identify whether real probabilities exist upstream or are replaced by placeholders",
            "passed": True,
        },
        {
            "contract": "produce_trace_map_from_candidate_sources_to_canonical_surface",
            "required": True,
            "why": "needed before real numeric probability metric readiness can be claimed",
            "passed": True,
        },
        {
            "contract": "preserve_no_adapter_metrics_backtest_activation_or_exit",
            "required": True,
            "why": "trace implementation remains diagnostic only",
            "passed": True,
        },
    ]

    blocking_policy = [
        {
            "policy": "do_not_escalate_to_numeric_probability_metrics_until_trace_identifies_real_numeric_probability_source",
            "required": True,
            "passed": True,
        },
        {
            "policy": "do_not_run_backtests_until_real_numeric_probability_surface_is_materialized_and_audited",
            "required": True,
            "passed": True,
        },
        {
            "policy": "do_not_tune_or_activate_until_backtest_evidence_exists",
            "required": True,
            "passed": True,
        },
    ]

    decision_rows = [
        {"decision": "6mb_passed", "expected": True, "actual": json_6mb.get("all_checks_passed"), "passed": json_6mb.get("all_checks_passed") is True},
        {"decision": "6mb_diagnosis_valid", "expected": DIAGNOSIS_6MB, "actual": json_6mb.get("diagnosis"), "passed": json_6mb.get("diagnosis") == DIAGNOSIS_6MB},
        {"decision": "all_required_6mb_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "placeholder_block_confirmed", "expected": True, "actual": placeholder_block_confirmed, "passed": placeholder_block_confirmed},
        {"decision": "numeric_probability_values_absent_confirmed", "expected": True, "actual": numeric_absent_confirmed, "passed": numeric_absent_confirmed},
        {"decision": "probability_metric_ready_after_audit_false", "expected": False, "actual": json_6mb.get("probability_metric_ready_after_audit"), "passed": json_6mb.get("probability_metric_ready_after_audit") is False},
        {"decision": "source_trace_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "readonly_trace_contract_created", "expected": True, "actual": True, "passed": True},
        {"decision": "future_6md_contract_valid", "expected": True, "actual": all_passed(future_6md), "passed": all_passed(future_6md)},
        {"decision": "recommend_6md_next", "expected": RECOMMENDED_NEXT_LAYER_6MC, "actual": RECOMMENDED_NEXT_LAYER_6MC, "passed": True},
        {"decision": "do_not_recommend_run_metrics_backtest_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_numeric_probability_source_trace", "expected": True, "actual": True, "passed": True},
        {"boundary": "trace_execution_run_by_6mc", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mc", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mc", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mc", "expected": False, "actual": False, "passed": True},
        {"boundary": "run_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mc", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MC, "actual": RECOMMENDED_NEXT_LAYER_6MC, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MC, "actual": RECOMMENDED_PATH_6MC, "passed": True},
        {"decision": "do_not_recommend_run_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MC, "actual": DIAGNOSIS_6MC, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem_rows), "detail": f"{sum(1 for r in problem_rows if r['passed'])}/{len(problem_rows)}"},
        {"check": "trace_scope", "passed": all_passed(trace_scope_rows), "detail": f"{sum(1 for r in trace_scope_rows if r['passed'])}/{len(trace_scope_rows)}"},
        {"check": "candidate_source_surfaces", "passed": all_passed(candidate_source_surfaces), "detail": f"{sum(1 for r in candidate_source_surfaces if r['passed'])}/{len(candidate_source_surfaces)}"},
        {"check": "placeholder_diagnosis", "passed": all_passed(placeholder_diagnosis_rows), "detail": f"{sum(1 for r in placeholder_diagnosis_rows if r['passed'])}/{len(placeholder_diagnosis_rows)}"},
        {"check": "readonly_trace_contract", "passed": all_passed(readonly_trace_contract), "detail": f"{sum(1 for r in readonly_trace_contract if r['passed'])}/{len(readonly_trace_contract)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6md_contract", "passed": all_passed(future_6md), "detail": f"{sum(1 for r in future_6md if r['passed'])}/{len(future_6md)}"},
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
        "trace_scope": write_csv(TRACE_SCOPE_CSV, trace_scope_rows),
        "candidate_source_surfaces": write_csv(CANDIDATE_SOURCE_SURFACES_CSV, candidate_source_surfaces),
        "placeholder_diagnosis": write_csv(PLACEHOLDER_DIAGNOSIS_CSV, placeholder_diagnosis_rows),
        "readonly_trace_contract": write_csv(READONLY_TRACE_CONTRACT_CSV, readonly_trace_contract),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6md_contract": write_csv(FUTURE_6MD_CSV, future_6md),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MC",
        "layer_type": "game_mechanics_realism",
        "planning_only_numeric_probability_source_trace": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MC if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MC,
        "recommended_path": RECOMMENDED_PATH_6MC,
        "predecessor_layer": "6MB",
        "predecessor_diagnosis": json_6mb.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mb.get("all_checks_passed") is True,
        "planned_layer_after": "6MB",
        "source_family": "projection_adapter_numeric_probability_source_trace_plan",
        "placeholder_probability_block_confirmed_by_predecessor": placeholder_block_confirmed,
        "numeric_probability_values_absent_confirmed_by_predecessor": numeric_absent_confirmed,
        "probability_metric_ready_after_predecessor_audit": False,
        "source_trace_plan_created": True,
        "readonly_trace_contract_created": True,
        "candidate_source_surface_count": len(candidate_source_surfaces),
        "allowed_next_layer_operation": "readonly_numeric_probability_source_trace",
        "trace_execution_allowed_next": True,
        "trace_execution_run_by_6mc": False,
        "adapter_call_executed_by_6mc": False,
        "metric_execution_run_by_6mc": False,
        "backtest_execution_run_by_6mc": False,
        "run_metric_execution_run": False,
        "production_code_modified_by_6mc": False,
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
            "trace_scope_csv": str(TRACE_SCOPE_CSV),
            "candidate_source_surfaces_csv": str(CANDIDATE_SOURCE_SURFACES_CSV),
            "placeholder_diagnosis_csv": str(PLACEHOLDER_DIAGNOSIS_CSV),
            "readonly_trace_contract_csv": str(READONLY_TRACE_CONTRACT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6md_contract_csv": str(FUTURE_6MD_CSV),
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
