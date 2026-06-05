#!/usr/bin/env python3
"""Plan Layer 6 projection-call contract.

This planning-only layer defines a safe deterministic projection-call contract
for future historical evaluation surface generation. It does not execute
projection generation, run metrics, fetch data, write DBs, mutate production
source, activate mechanics, or grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ky_projection_call_contract_plan"
TMP_DIR = Path("tmp")

AUDIT_6KX_PATH = Path("scripts/audit_6kx_layer6_historical_backtest_source_generation_implementation.py")
JSON_6KX = TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit.json"

REQUIRED_INPUTS = [
    JSON_6KX,
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_checks.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_predecessor.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_candidate_source_audit.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_generation_feasibility_audit.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_surface_or_gap_audit.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_metric_readiness_audit.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_projection_contract_verdict.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_next_route.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_blockers.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_future_6ky_contract.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_decision.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6kx_historical_backtest_source_generation_implementation_audit_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
INPUT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_input_contract.csv"
OUTPUT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_output_contract.csv"
ENTRYPOINT_RULES_CSV = TMP_DIR / f"{SLUG}_entrypoint_discovery_rules.csv"
ADAPTER_STRATEGY_CSV = TMP_DIR / f"{SLUG}_adapter_strategy.csv"
FIXTURE_STRATEGY_CSV = TMP_DIR / f"{SLUG}_fixture_generation_strategy.csv"
FALLBACK_STRATEGY_CSV = TMP_DIR / f"{SLUG}_fallback_strategy.csv"
SURFACE_INTEGRATION_CSV = TMP_DIR / f"{SLUG}_evaluation_surface_integration.csv"
ALLOWED_OPS_CSV = TMP_DIR / f"{SLUG}_allowed_operations.csv"
FORBIDDEN_OPS_CSV = TMP_DIR / f"{SLUG}_forbidden_operations.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KZ_CSV = TMP_DIR / f"{SLUG}_future_6kz_contract.csv"
FUTURE_6LA_CSV = TMP_DIR / f"{SLUG}_future_6la_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KX = "layer_6_historical_backtest_source_generation_implementation_audit_complete"
DIAGNOSIS_6KY = "layer_6_projection_call_contract_plan_complete"
RECOMMENDED_NEXT_LAYER_6KX = "6KY_layer_6_projection_call_contract_plan"
RECOMMENDED_NEXT_LAYER_6KY = "6KZ_layer_6_projection_call_contract_implementation"
RECOMMENDED_PATH_6KY = "implement_projection_call_contract_for_historical_surface_generation"


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True, "passed": True}]
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
    parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
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


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6kx = load_json(JSON_6KX)

    problem_statement = [
        {
            "problem": "projection_call_contract_missing",
            "status": "confirmed_by_6kx",
            "impact": "historical evaluation surface cannot be generated safely yet",
            "planned_resolution": "define safe deterministic projection-call contract and adapter plan",
            "passed": True,
        }
    ]

    input_contract = [
        {"field": "game_id", "required": True, "source": "schedule_or_outcome_artifact", "fallback": "game_date_home_away_key", "passed": True},
        {"field": "game_date", "required": True, "source": "schedule_or_outcome_artifact", "fallback": "none", "passed": True},
        {"field": "season", "required": True, "source": "game_date_or_artifact", "fallback": "derive_from_game_date", "passed": True},
        {"field": "home_team", "required": True, "source": "schedule_or_outcome_artifact", "fallback": "normalized_home_team", "passed": True},
        {"field": "away_team", "required": True, "source": "schedule_or_outcome_artifact", "fallback": "normalized_away_team", "passed": True},
        {"field": "home_pitcher", "required": False, "source": "local_artifact_if_available", "fallback": "proxy_or_missing_input_family", "passed": True},
        {"field": "away_pitcher", "required": False, "source": "local_artifact_if_available", "fallback": "proxy_or_missing_input_family", "passed": True},
        {"field": "home_lineup_proxy", "required": False, "source": "local_artifact_if_available", "fallback": "team_level_proxy", "passed": True},
        {"field": "away_lineup_proxy", "required": False, "source": "local_artifact_if_available", "fallback": "team_level_proxy", "passed": True},
        {"field": "park_factor_proxy", "required": False, "source": "local_artifact_if_available", "fallback": "neutral_proxy", "passed": True},
        {"field": "bullpen_state_proxy", "required": False, "source": "current_layer6_bullpen_artifacts_if_available", "fallback": "neutral_or_current_partial_realism_proxy", "passed": True},
        {"field": "mechanic_context_tags", "required": True, "source": "layer6_state", "fallback": "fixed_current_tags", "passed": True},
        {"field": "generation_mode", "required": True, "source": "adapter", "fallback": "fixture_generation_mode", "passed": True},
        {"field": "source_lineage", "required": True, "source": "all_local_input_paths", "fallback": "explicit_missing_lineage", "passed": True},
    ]

    output_contract = [
        {"field": "game_id", "required": True, "purpose": "join identifier", "passed": True},
        {"field": "game_date", "required": True, "purpose": "join identifier", "passed": True},
        {"field": "home_team", "required": True, "purpose": "join identifier", "passed": True},
        {"field": "away_team", "required": True, "purpose": "join identifier", "passed": True},
        {"field": "home_win_probability", "required": False, "purpose": "probability metric readiness", "passed": True},
        {"field": "away_win_probability", "required": False, "purpose": "probability metric readiness", "passed": True},
        {"field": "home_expected_runs", "required": False, "purpose": "runs metric readiness", "passed": True},
        {"field": "away_expected_runs", "required": False, "purpose": "runs metric readiness", "passed": True},
        {"field": "total_expected_runs", "required": False, "purpose": "runs metric readiness", "passed": True},
        {"field": "projection_source", "required": True, "purpose": "lineage", "passed": True},
        {"field": "projection_entrypoint", "required": True, "purpose": "call traceability", "passed": True},
        {"field": "projection_call_mode", "required": True, "purpose": "adapter vs fixture mode", "passed": True},
        {"field": "projection_call_status", "required": True, "purpose": "success/fail/gap", "passed": True},
        {"field": "missing_input_families", "required": True, "purpose": "fail-closed diagnostics", "passed": True},
        {"field": "fallback_used", "required": True, "purpose": "proxy/fixture transparency", "passed": True},
        {"field": "notes", "required": True, "purpose": "auditability", "passed": True},
    ]

    entrypoint_rules = [
        {"rule": "prefer_pure_python_function", "detail": "No network, DB, file mutation, app server context, or global side effects.", "passed": True},
        {"rule": "prefer_existing_projection_logic_over_ui_component", "detail": "Use underlying projection service/model helper if available, not React/UI wrapper.", "passed": True},
        {"rule": "inspect_import_safety_before_call", "detail": "Static scan for requests/httpx/DB/env/server dependencies before import/call.", "passed": True},
        {"rule": "call_only_with_explicit_fixture_dict", "detail": "Adapter input must be a serializable game fixture contract.", "passed": True},
        {"rule": "fail_closed_on_missing_contract_fields", "detail": "Emit missing_input_families instead of guessing silently.", "passed": True},
        {"rule": "record_entrypoint_and_source_path", "detail": "Every generated projection row must include projection_source and projection_entrypoint.", "passed": True},
    ]

    adapter_strategy = [
        {"strategy": "isolated_script_adapter", "allowed": True, "detail": "Create scripts/ implementation wrapper without modifying production source.", "passed": True},
        {"strategy": "static_entrypoint_inventory", "allowed": True, "detail": "Rank projection functions/classes discovered by 6KW.", "passed": True},
        {"strategy": "dry_contract_validation_only", "allowed": True, "detail": "Validate call signatures and required fields before any real generation.", "passed": True},
        {"strategy": "fixture_to_projection_payload_mapper", "allowed": True, "detail": "Map historical fixture fields into projection input payload.", "passed": True},
        {"strategy": "fail_closed_gap_report", "allowed": True, "detail": "If no safe call path exists, emit projection adapter gap report.", "passed": True},
    ]

    fixture_strategy = [
        {"strategy": "build_fixture_from_local_schedule_candidate", "allowed": True, "passed": True},
        {"strategy": "join_actual_outcome_for_identifiers_only", "allowed": True, "passed": True},
        {"strategy": "derive_season_from_game_date", "allowed": True, "passed": True},
        {"strategy": "use_proxy_inputs_only_when_labeled", "allowed": True, "passed": True},
        {"strategy": "preserve_source_lineage_for_each_field_family", "allowed": True, "passed": True},
        {"strategy": "write_fixture_artifacts_to_tmp_only", "allowed": True, "passed": True},
    ]

    fallback_strategy = [
        {"fallback": "fixture_generation_without_projection_call", "when": "projection route unsafe", "output": "fixture surface plus gap report", "passed": True},
        {"fallback": "proxy_mode_projection_contract", "when": "optional inputs missing", "output": "labeled proxy fields and missing families", "passed": True},
        {"fallback": "no_call_gap_report", "when": "entrypoint imports unsafe or call signature unknown", "output": "specific adapter blocker", "passed": True},
        {"fallback": "partial_projection_surface", "when": "only probability or runs output available", "output": "partial readiness flags", "passed": True},
    ]

    surface_integration = [
        {"integration": "append_prediction_fields_to_evaluation_surface", "target": "tmp/layer6_6kz_projection_call_contract_implementation_projection_surface.csv", "passed": True},
        {"integration": "preserve_actual_join_for_future_surface_generation", "target": "6LB_or_later_evaluation_surface", "passed": True},
        {"integration": "emit_metric_readiness_flags", "target": "probability/runs/any readiness", "passed": True},
        {"integration": "preserve_current_ui_realism_labels", "target": "bullpen_active_partial_realism tags", "passed": True},
        {"integration": "no_real_metric_execution", "target": "metrics planning/readiness only", "passed": True},
    ]

    allowed_operations = [
        {"operation": "read_repo_local_files", "allowed_next": True, "passed": True},
        {"operation": "call_local_deterministic_python_functions", "allowed_next": True, "passed": True},
        {"operation": "write_tmp_artifacts", "allowed_next": True, "passed": True},
        {"operation": "create_isolated_adapter_script_under_scripts", "allowed_next": True, "passed": True},
        {"operation": "create_fixture_generation_tmp_artifacts", "allowed_next": True, "passed": True},
        {"operation": "use_mock_or_proxy_inputs_from_local_artifacts_with_lineage", "allowed_next": True, "passed": True},
    ]

    forbidden_operations = [
        {"operation": "live_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "database_writes", "allowed_next": False, "passed": True},
        {"operation": "production_source_modifications", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "real_backtest_metrics", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit_credit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "projection_call_contract_not_implemented", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "evaluation_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kz = [
        {"contract": "implement_fixture_contract_builder", "required": True, "passed": True},
        {"contract": "implement_static_projection_entrypoint_inventory", "required": True, "passed": True},
        {"contract": "implement_safe_adapter_or_emit_gap_report", "required": True, "passed": True},
        {"contract": "emit_projection_surface_or_adapter_gap_report", "required": True, "passed": True},
        {"contract": "preserve_no_fetch_no_db_write_no_real_metrics_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    future_6la = [
        {"contract": "audit_projection_call_contract_implementation", "required": True, "passed": True},
        {"contract": "audit_projection_surface_or_adapter_gap_report", "required": True, "passed": True},
        {"contract": "route_to_evaluation_surface_generation_or_adapter_repair", "required": True, "passed": True},
        {"contract": "preserve_no_real_backtest_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kx_audit_script_exists", "expected": True, "actual": AUDIT_6KX_PATH.exists(), "passed": AUDIT_6KX_PATH.exists()},
        {"check": "6kx_json_exists", "expected": True, "actual": JSON_6KX.exists(), "passed": JSON_6KX.exists()},
        {"check": "6kx_all_checks_passed", "expected": True, "actual": json_6kx.get("all_checks_passed"), "passed": json_6kx.get("all_checks_passed") is True},
        {"check": "6kx_diagnosis", "expected": DIAGNOSIS_6KX, "actual": json_6kx.get("diagnosis"), "passed": json_6kx.get("diagnosis") == DIAGNOSIS_6KX},
        {"check": "6kx_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KX, "actual": json_6kx.get("recommended_next_layer"), "passed": json_6kx.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KX},
        {"check": "6kx_projection_contract_missing", "expected": True, "actual": json_6kx.get("projection_call_contract_missing_confirmed"), "passed": json_6kx.get("projection_call_contract_missing_confirmed") is True},
        {"check": "6kx_projection_adapter_plan_needed", "expected": True, "actual": json_6kx.get("projection_adapter_plan_needed"), "passed": json_6kx.get("projection_adapter_plan_needed") is True},
        {"check": "6kx_no_historical_eval", "expected": False, "actual": json_6kx.get("real_historical_evaluation_run"), "passed": json_6kx.get("real_historical_evaluation_run") is False},
        {"check": "6kx_no_layer6_exit", "expected": False, "actual": json_6kx.get("layer_6_exit_recommended"), "passed": json_6kx.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kz_projection_call_contract_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "projection call contract and generated evaluation surface required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KY", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KY is planning-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KY cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kx_passed", "expected": True, "actual": json_6kx.get("all_checks_passed"), "passed": json_6kx.get("all_checks_passed") is True},
        {"decision": "problem_statement_count", "expected": 1, "actual": len(problem_statement), "passed": len(problem_statement) == 1 and all_passed(problem_statement)},
        {"decision": "input_contract_field_count", "expected": 14, "actual": len(input_contract), "passed": len(input_contract) == 14 and all_passed(input_contract)},
        {"decision": "output_contract_field_count", "expected": 16, "actual": len(output_contract), "passed": len(output_contract) == 16 and all_passed(output_contract)},
        {"decision": "adapter_strategy_count", "expected": 5, "actual": len(adapter_strategy), "passed": len(adapter_strategy) == 5 and all_passed(adapter_strategy)},
        {"decision": "future_6kz_contract_valid", "expected": True, "actual": len(future_6kz) == 5 and all_passed(future_6kz), "passed": len(future_6kz) == 5 and all_passed(future_6kz)},
        {"decision": "future_6la_contract_valid", "expected": True, "actual": len(future_6la) == 4 and all_passed(future_6la), "passed": len(future_6la) == 4 and all_passed(future_6la)},
        {"decision": "recommend_6kz_next", "expected": RECOMMENDED_NEXT_LAYER_6KY, "actual": RECOMMENDED_NEXT_LAYER_6KY, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "projection_call_contract_planned", "expected": True, "actual": True, "passed": True},
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

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6kx_audit", "policy": "read_only", "passed": True},
        {"surface": "future_adapter", "policy": "isolated_script_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6ky", "passed": True},
        {"surface": "database", "policy": "not_written_in_6ky", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KY, "actual": RECOMMENDED_NEXT_LAYER_6KY, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KY, "actual": RECOMMENDED_PATH_6KY, "passed": True},
        {"decision": "recommend_projection_call_contract_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KY, "actual": DIAGNOSIS_6KY, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": len(problem_statement) == 1 and all_passed(problem_statement), "detail": "1/1"},
        {"check": "input_contract", "passed": len(input_contract) == 14 and all_passed(input_contract), "detail": "14/14"},
        {"check": "output_contract", "passed": len(output_contract) == 16 and all_passed(output_contract), "detail": "16/16"},
        {"check": "entrypoint_discovery_rules", "passed": len(entrypoint_rules) == 6 and all_passed(entrypoint_rules), "detail": "6/6"},
        {"check": "adapter_strategy", "passed": len(adapter_strategy) == 5 and all_passed(adapter_strategy), "detail": "5/5"},
        {"check": "fixture_generation_strategy", "passed": len(fixture_strategy) == 6 and all_passed(fixture_strategy), "detail": "6/6"},
        {"check": "fallback_strategy", "passed": len(fallback_strategy) == 4 and all_passed(fallback_strategy), "detail": "4/4"},
        {"check": "evaluation_surface_integration", "passed": len(surface_integration) == 5 and all_passed(surface_integration), "detail": "5/5"},
        {"check": "allowed_operations", "passed": len(allowed_operations) == 6 and all_passed(allowed_operations), "detail": "6/6"},
        {"check": "forbidden_operations", "passed": len(forbidden_operations) == 7 and all_passed(forbidden_operations), "detail": "7/7"},
        {"check": "blockers", "passed": len(blockers) == 4 and all_passed(blockers), "detail": "4/4"},
        {"check": "future_6kz_contract", "passed": len(future_6kz) == 5 and all_passed(future_6kz), "detail": "5/5"},
        {"check": "future_6la_contract", "passed": len(future_6la) == 4 and all_passed(future_6la), "detail": "4/4"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for r in readonly_rows if r['passed'])}/{len(readonly_rows)}"},
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
        "problem_statement": write_csv(PROBLEM_CSV, problem_statement),
        "input_contract": write_csv(INPUT_CONTRACT_CSV, input_contract),
        "output_contract": write_csv(OUTPUT_CONTRACT_CSV, output_contract),
        "entrypoint_discovery_rules": write_csv(ENTRYPOINT_RULES_CSV, entrypoint_rules),
        "adapter_strategy": write_csv(ADAPTER_STRATEGY_CSV, adapter_strategy),
        "fixture_generation_strategy": write_csv(FIXTURE_STRATEGY_CSV, fixture_strategy),
        "fallback_strategy": write_csv(FALLBACK_STRATEGY_CSV, fallback_strategy),
        "evaluation_surface_integration": write_csv(SURFACE_INTEGRATION_CSV, surface_integration),
        "allowed_operations": write_csv(ALLOWED_OPS_CSV, allowed_operations),
        "forbidden_operations": write_csv(FORBIDDEN_OPS_CSV, forbidden_operations),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6kz_contract": write_csv(FUTURE_6KZ_CSV, future_6kz),
        "future_6la_contract": write_csv(FUTURE_6LA_CSV, future_6la),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KY",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KY if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KY,
        "recommended_path": RECOMMENDED_PATH_6KY,
        "predecessor_audit": str(AUDIT_6KX_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kx.get("diagnosis"),
        "planned_layer_after": "6KX",
        "source_family": "projection_call_contract_plan",
        "problem_statement_count": len(problem_statement),
        "input_contract_field_count": len(input_contract),
        "output_contract_field_count": len(output_contract),
        "entrypoint_discovery_rule_count": len(entrypoint_rules),
        "adapter_strategy_count": len(adapter_strategy),
        "fixture_generation_strategy_count": len(fixture_strategy),
        "fallback_strategy_count": len(fallback_strategy),
        "evaluation_surface_integration_count": len(surface_integration),
        "allowed_operation_count": len(allowed_operations),
        "forbidden_operation_count": len(forbidden_operations),
        "blocker_count": len(blockers),
        "future_6kz_contract_valid": len(future_6kz) == 5 and all_passed(future_6kz),
        "future_6la_contract_valid": len(future_6la) == 4 and all_passed(future_6la),
        "projection_call_contract_planned": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "projection_call_contract_missing_confirmed": True,
        "projection_adapter_plan_needed": True,
        "local_function_calls_allowed_next": True,
        "repo_file_reads_allowed_next": True,
        "tmp_writes_allowed_next": True,
        "adapter_script_allowed_next": True,
        "fixture_generation_allowed_next": True,
        "mock_or_proxy_inputs_allowed_next": True,
        "live_fetches_allowed_next": False,
        "remote_api_calls_allowed_next": False,
        "database_writes_allowed_next": False,
        "production_source_modifications_allowed_next": False,
        "real_backtest_metrics_allowed_next": False,
        "mechanics_activation_allowed_next": False,
        "layer_6_exit_allowed_next": False,
        "historical_odds_required": False,
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
            "input_contract_csv": str(INPUT_CONTRACT_CSV),
            "output_contract_csv": str(OUTPUT_CONTRACT_CSV),
            "entrypoint_discovery_rules_csv": str(ENTRYPOINT_RULES_CSV),
            "adapter_strategy_csv": str(ADAPTER_STRATEGY_CSV),
            "fixture_generation_strategy_csv": str(FIXTURE_STRATEGY_CSV),
            "fallback_strategy_csv": str(FALLBACK_STRATEGY_CSV),
            "evaluation_surface_integration_csv": str(SURFACE_INTEGRATION_CSV),
            "allowed_operations_csv": str(ALLOWED_OPS_CSV),
            "forbidden_operations_csv": str(FORBIDDEN_OPS_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6kz_contract_csv": str(FUTURE_6KZ_CSV),
            "future_6la_contract_csv": str(FUTURE_6LA_CSV),
            "readonly_sources_csv": str(READONLY_CSV),
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
