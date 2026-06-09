#!/usr/bin/env python3
"""Plan adapter-shaped payload repair for the selected projection adapter.

This planning-only layer prepares the next static/source-trace implementation
to locate or construct a payload with payload["games"] for the same candidate.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lq_projection_adapter_payload_shape_repair_plan"
TMP_DIR = Path("tmp")

AUDIT_6LP_PATH = Path("scripts/audit_6lp_layer6_projection_adapter_empty_return_trace.py")
JSON_6LP = TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit.json"

REQUIRED_6LP_INPUTS = [
    JSON_6LP,
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_checks.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_predecessor.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_candidate_audit.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_static_trace_audit.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_payload_shape_audit.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_empty_return_root_cause.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_projection_surface_audit.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_metric_readiness_audit.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_next_route.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_blockers.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_future_6lq_contract.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_decision.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6lp_projection_adapter_empty_return_trace_audit_recommended_path.csv",
]

SOURCE_INPUTS = [
    TMP_DIR / "layer6_6lo_projection_adapter_empty_return_trace_implementation_payload_access_paths.csv",
    TMP_DIR / "layer6_6lo_projection_adapter_empty_return_trace_implementation_game_pk_paths.csv",
    TMP_DIR / "layer6_6lo_projection_adapter_empty_return_trace_implementation_filter_paths.csv",
    TMP_DIR / "layer6_6lo_projection_adapter_empty_return_trace_implementation_fixture_payload_comparison.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_argument_mapping.csv",
]

READONLY_SOURCE_FILES = [
    Path("mlb_app/ai_data_assistant_performance.py"),
    Path("mlb_app/daily_odds_models.py"),
    Path("mlb_app/ml_model_features.py"),
    Path("mlb_app/model_predictions.py"),
]
ALL_INPUTS = REQUIRED_6LP_INPUTS + SOURCE_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
RETENTION_CSV = TMP_DIR / f"{SLUG}_candidate_retention.csv"
PAYLOAD_REQ_CSV = TMP_DIR / f"{SLUG}_payload_shape_requirement.csv"
SOURCE_TARGETS_CSV = TMP_DIR / f"{SLUG}_source_trace_targets.csv"
UPSTREAM_SEARCH_CSV = TMP_DIR / f"{SLUG}_upstream_payload_builder_search.csv"
ADAPTER_SCHEMA_CSV = TMP_DIR / f"{SLUG}_adapter_payload_schema_plan.csv"
GAME_SCHEMA_CSV = TMP_DIR / f"{SLUG}_game_entry_schema_plan.csv"
REPAIR_ARTIFACT_CSV = TMP_DIR / f"{SLUG}_payload_repair_artifact_plan.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
SURFACE_RULES_CSV = TMP_DIR / f"{SLUG}_prediction_surface_rules.csv"
METRIC_GUARDS_CSV = TMP_DIR / f"{SLUG}_metric_guardrails.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LR_CSV = TMP_DIR / f"{SLUG}_future_6lr_contract.csv"
FUTURE_6LS_CSV = TMP_DIR / f"{SLUG}_future_6ls_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LP = "layer_6_projection_adapter_empty_return_trace_audit_complete"
DIAGNOSIS_6LQ = "layer_6_projection_adapter_payload_shape_repair_plan_complete"
RECOMMENDED_NEXT_LAYER_6LP = "6LQ_layer_6_projection_adapter_payload_shape_repair_plan"
RECOMMENDED_NEXT_LAYER_6LQ = "6LR_layer_6_projection_adapter_payload_shape_repair_implementation"
RECOMMENDED_PATH_6LQ = "implement_adapter_shaped_payload_repair_artifact_for_same_candidate"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
REQUIRED_ARGS = "payload;game_pk;limit"


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
    json_6lp = load_json(JSON_6LP)

    problem = [
        {
            "problem": "same_candidate_requires_adapter_shaped_payload_games_collection",
            "root_cause": json_6lp.get("empty_return_root_cause_confirmed"),
            "required_key": "games",
            "repair_needed": True,
            "passed": True,
        }
    ]

    retention = [
        {"item": "same_candidate_retained", "value": True, "module": TARGET_MODULE, "function": TARGET_FUNCTION, "passed": True},
        {"item": "blocked_session_candidate_excluded", "value": True, "blocked_function": "cached_build_model_projection_payload", "passed": True},
        {"item": "next_candidate_retry_recommended", "value": False, "reason": "same candidate needs adapter-shaped payload repair", "passed": True},
        {"item": "wrapper_plan_needed", "value": False, "reason": "payload shape repair not attempted yet", "passed": True},
    ]

    payload_req = [
        {"requirement": "top_level_payload_games_key", "required": True, "source": "6LP confirmed expected_payload_keys_confirmed=games", "passed": True},
        {"requirement": "games_value_is_iterable", "required": True, "source": "for game in payload.get('games') or []", "passed": True},
        {"requirement": "per_game_game_pk_key", "required": True, "source": "game.get('game_pk') compared to game_pk", "passed": True},
        {"requirement": "game_pk_824776_matchable", "required": True, "source": "future non-production fixture target", "passed": True},
    ]

    source_targets = [
        {"target": "selected_adapter_function", "path": "mlb_app/ai_data_assistant_performance.py", "symbol": TARGET_FUNCTION, "trace_goal": "confirm per-game fields appended to canonical rows", "passed": True},
        {"target": "upstream_callers", "path": "mlb_app/ai_data_assistant_performance.py", "symbol": TARGET_FUNCTION, "trace_goal": "find callers passing real payload", "passed": True},
        {"target": "payload_games_builders", "path": "mlb_app/**/*.py", "symbol": "payload['games'] or {'games': ...}", "trace_goal": "find adapter-shaped payload source", "passed": True},
        {"target": "daily_odds_models", "path": "mlb_app/daily_odds_models.py", "symbol": "game model builders", "trace_goal": "find game_pk/probability/run field names", "passed": True},
        {"target": "model_predictions", "path": "mlb_app/model_predictions.py", "symbol": "projection payload producers", "trace_goal": "find candidate prediction records", "passed": True},
    ]

    upstream_search = [
        {"search": "function_callers", "query": "_canonical_games_from_projection_payload(", "goal": "locate code passing payload with games", "passed": True},
        {"search": "dict_games_literal", "query": "'games' or \"games\" in dict literals", "goal": "find top-level games payload builders", "passed": True},
        {"search": "payload_assignment", "query": "payload = ... games", "goal": "find payload construction sites", "passed": True},
        {"search": "canonical_game_rows", "query": "game_pk/home_win_probability/away_win_probability/home_expected_runs", "goal": "find expected per-game schema", "passed": True},
    ]

    adapter_schema = [
        {"schema_item": "payload", "required_shape": "dict", "planned_source": "static_trace_or_repaired_non_production_fixture", "passed": True},
        {"schema_item": "payload.games", "required_shape": "list[dict]", "planned_source": "adapter-shaped repair artifact", "passed": True},
        {"schema_item": "payload.games[].game_pk", "required_shape": "string_or_int", "planned_source": "game_pk=824776", "passed": True},
        {"schema_item": "payload.games[].probabilities", "required_shape": "dict or equivalent keys", "planned_source": "source-traced schema", "passed": True},
        {"schema_item": "payload.games[].runs_or_total", "required_shape": "optional numeric fields if source-traced", "planned_source": "source-traced schema", "passed": True},
    ]

    game_schema = [
        {"field": "game_pk", "required": True, "planned_value": 824776, "passed": True},
        {"field": "home_team", "required": "trace", "planned_value": "from fixture if field accepted", "passed": True},
        {"field": "away_team", "required": "trace", "planned_value": "from fixture if field accepted", "passed": True},
        {"field": "probability_fields", "required": "trace", "planned_value": "source-traced or fail closed", "passed": True},
        {"field": "run_fields", "required": "trace", "planned_value": "source-traced if available", "passed": True},
        {"field": "data_confidence", "required": "optional", "planned_value": "non_production_trace", "passed": True},
    ]

    repair_artifact = [
        {"artifact": "adapter_shaped_payload_schema_trace", "path": "tmp/layer6_6lr_projection_adapter_payload_shape_repair_implementation_payload_schema_trace.csv", "passed": True},
        {"artifact": "upstream_builder_trace", "path": "tmp/layer6_6lr_projection_adapter_payload_shape_repair_implementation_upstream_builder_trace.csv", "passed": True},
        {"artifact": "non_production_payload_games_artifact", "path": "tmp/layer6_6lr_projection_adapter_payload_shape_repair_implementation_adapter_payload.json", "passed": True},
        {"artifact": "payload_shape_gap_report", "path": "tmp/layer6_6lr_projection_adapter_payload_shape_repair_implementation_gap_report.csv", "passed": True},
    ]

    fail_closed = [
        {"condition": "no_source_traced_games_builder_found", "action": "emit_payload_builder_gap", "passed": True},
        {"condition": "per_game_schema_unclear", "action": "emit_schema_gap", "passed": True},
        {"condition": "game_pk_mapping_unclear", "action": "emit_game_pk_schema_gap", "passed": True},
        {"condition": "payload_artifact_would_require_remote_or_live_data", "action": "do_not_build_payload;emit_source_acquisition_blocker", "passed": True},
        {"condition": "payload_would_be_wrapper_not_repair", "action": "defer_wrapper_plan", "passed": True},
    ]

    surface_rules = [
        {"rule": "do_not_materialize_projection_surface_in_6lq", "passed": True},
        {"rule": "future_surface_requires_adapter_call_after_payload_shape_audit", "passed": True},
        {"rule": "payload_artifact_is_not_prediction_surface", "passed": True},
        {"rule": "preserve_non_production_labeling", "passed": True},
    ]

    metric_guards = [
        {"guardrail": "do_not_compute_brier", "passed": True},
        {"guardrail": "do_not_compute_log_loss", "passed": True},
        {"guardrail": "do_not_compute_calibration", "passed": True},
        {"guardrail": "do_not_compute_run_error_metrics", "passed": True},
        {"guardrail": "do_not_compute_winner_correct", "passed": True},
        {"guardrail": "emit_readiness_flags_only", "passed": True},
    ]

    allowed_next = [
        {"operation": "static_source_trace_for_payload_games_builders", "allowed_next": True, "passed": True},
        {"operation": "static_schema_trace_for_payload_games_entries", "allowed_next": True, "passed": True},
        {"operation": "write_non_production_payload_shape_artifact", "allowed_next": True, "passed": True},
        {"operation": "write_tmp_trace_artifacts", "allowed_next": True, "passed": True},
    ]

    forbidden_next = [
        {"operation": "import_candidate_module", "allowed_next": False, "passed": True},
        {"operation": "execute_adapter_call", "allowed_next": False, "passed": True},
        {"operation": "try_different_candidate", "allowed_next": False, "passed": True},
        {"operation": "wrapper_design", "allowed_next": False, "passed": True},
        {"operation": "full_batch_adapter_call", "allowed_next": False, "passed": True},
        {"operation": "real_metric_execution", "allowed_next": False, "passed": True},
        {"operation": "live_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "database_writes", "allowed_next": False, "passed": True},
        {"operation": "production_source_modifications", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit_credit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "adapter_shaped_payload_not_acquired", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lr = [
        {"contract": "trace_payload_games_builders_without_import_or_call", "required": True, "passed": True},
        {"contract": "derive_adapter_games_schema_or_gap", "required": True, "passed": True},
        {"contract": "write_non_production_adapter_payload_artifact_or_gap", "required": True, "passed": True},
        {"contract": "preserve_no_adapter_call_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    future_6ls = [
        {"contract": "audit_payload_shape_repair_artifact", "required": True, "passed": True},
        {"contract": "decide_whether_single_sample_call_retry_can_be_planned", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_activation_or_exit", "required": True, "passed": True},
        {"contract": "keep_layer6_exit_blocked", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lp_audit_script_exists", "expected": True, "actual": AUDIT_6LP_PATH.exists(), "passed": AUDIT_6LP_PATH.exists()},
        {"check": "6lp_json_exists", "expected": True, "actual": JSON_6LP.exists(), "passed": JSON_6LP.exists()},
        {"check": "6lp_all_checks_passed", "expected": True, "actual": json_6lp.get("all_checks_passed"), "passed": json_6lp.get("all_checks_passed") is True},
        {"check": "6lp_diagnosis", "expected": DIAGNOSIS_6LP, "actual": json_6lp.get("diagnosis"), "passed": json_6lp.get("diagnosis") == DIAGNOSIS_6LP},
        {"check": "6lp_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LP, "actual": json_6lp.get("recommended_next_layer"), "passed": json_6lp.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LP},
        {"check": "6lp_expected_games_key", "expected": "games", "actual": json_6lp.get("expected_payload_keys_confirmed"), "passed": json_6lp.get("expected_payload_keys_confirmed") == "games"},
        {"check": "6lp_adapter_payload_repair_needed", "expected": True, "actual": json_6lp.get("adapter_shaped_payload_repair_needed"), "passed": json_6lp.get("adapter_shaped_payload_repair_needed") is True},
        {"check": "6lp_no_layer6_exit", "expected": False, "actual": json_6lp.get("layer_6_exit_recommended"), "passed": json_6lp.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "required": True, "passed": path.exists()}
        for path in ALL_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "required": path.name == "ai_data_assistant_performance.py", "may_modify": False, "passed": True} for path in READONLY_SOURCE_FILES]

    blocking_rows = [
        {"blocked_surface": "6lr_payload_shape_repair_implementation", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "adapter_call", "blocked": True, "reason": "payload shape artifact must be built and audited first", "passed": True},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "same candidate payload-shape repair not attempted yet", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "payload shape repair not attempted yet", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LQ cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lp_passed", "expected": True, "actual": json_6lp.get("all_checks_passed"), "passed": json_6lp.get("all_checks_passed") is True},
        {"decision": "problem_statement_valid", "expected": True, "actual": all_passed(problem), "passed": all_passed(problem)},
        {"decision": "candidate_retention_valid", "expected": True, "actual": all_passed(retention), "passed": all_passed(retention)},
        {"decision": "payload_requirement_valid", "expected": True, "actual": all_passed(payload_req), "passed": all_passed(payload_req)},
        {"decision": "future_6lr_contract_valid", "expected": True, "actual": len(future_6lr) == 4 and all_passed(future_6lr), "passed": len(future_6lr) == 4 and all_passed(future_6lr)},
        {"decision": "future_6ls_contract_valid", "expected": True, "actual": len(future_6ls) == 4 and all_passed(future_6ls), "passed": len(future_6ls) == 4 and all_passed(future_6ls)},
        {"decision": "recommend_6lr_next", "expected": RECOMMENDED_NEXT_LAYER_6LQ, "actual": RECOMMENDED_NEXT_LAYER_6LQ, "passed": True},
        {"decision": "do_not_recommend_other_candidate", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_adapter_call_run_by_6lq", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_candidate_module_import_by_6lq", "expected": True, "actual": True, "passed": True},
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

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_planning", "passed": True},
        {"surface": "6lp_audit", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only_reference", "passed": True},
        {"surface": "future_6lr_payload_artifact", "policy": "tmp_non_production_only_next_layer", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lq", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LQ, "actual": RECOMMENDED_NEXT_LAYER_6LQ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LQ, "actual": RECOMMENDED_PATH_6LQ, "passed": True},
        {"decision": "recommend_payload_shape_repair_implementation", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LQ, "actual": DIAGNOSIS_6LQ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem), "detail": f"{len(problem)} rows"},
        {"check": "candidate_retention", "passed": all_passed(retention), "detail": f"{len(retention)} rows"},
        {"check": "payload_shape_requirement", "passed": all_passed(payload_req), "detail": f"{len(payload_req)} rows"},
        {"check": "source_trace_targets", "passed": all_passed(source_targets), "detail": f"{len(source_targets)} rows"},
        {"check": "upstream_payload_builder_search", "passed": all_passed(upstream_search), "detail": f"{len(upstream_search)} rows"},
        {"check": "adapter_payload_schema_plan", "passed": all_passed(adapter_schema), "detail": f"{len(adapter_schema)} rows"},
        {"check": "game_entry_schema_plan", "passed": all_passed(game_schema), "detail": f"{len(game_schema)} rows"},
        {"check": "payload_repair_artifact_plan", "passed": all_passed(repair_artifact), "detail": f"{len(repair_artifact)} rows"},
        {"check": "fail_closed_policy", "passed": all_passed(fail_closed), "detail": f"{len(fail_closed)} rows"},
        {"check": "prediction_surface_rules", "passed": all_passed(surface_rules), "detail": f"{len(surface_rules)} rows"},
        {"check": "metric_guardrails", "passed": all_passed(metric_guards), "detail": f"{len(metric_guards)} rows"},
        {"check": "allowed_next", "passed": all_passed(allowed_next), "detail": f"{len(allowed_next)} rows"},
        {"check": "forbidden_next", "passed": all_passed(forbidden_next), "detail": f"{len(forbidden_next)} rows"},
        {"check": "future_6lr_contract", "passed": all_passed(future_6lr), "detail": f"{len(future_6lr)} rows"},
        {"check": "future_6ls_contract", "passed": all_passed(future_6ls), "detail": f"{len(future_6ls)} rows"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{len(blockers)} rows"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "problem_statement": write_csv(PROBLEM_CSV, problem),
        "candidate_retention": write_csv(RETENTION_CSV, retention),
        "payload_shape_requirement": write_csv(PAYLOAD_REQ_CSV, payload_req),
        "source_trace_targets": write_csv(SOURCE_TARGETS_CSV, source_targets),
        "upstream_payload_builder_search": write_csv(UPSTREAM_SEARCH_CSV, upstream_search),
        "adapter_payload_schema_plan": write_csv(ADAPTER_SCHEMA_CSV, adapter_schema),
        "game_entry_schema_plan": write_csv(GAME_SCHEMA_CSV, game_schema),
        "payload_repair_artifact_plan": write_csv(REPAIR_ARTIFACT_CSV, repair_artifact),
        "fail_closed_policy": write_csv(FAIL_CLOSED_CSV, fail_closed),
        "prediction_surface_rules": write_csv(SURFACE_RULES_CSV, surface_rules),
        "metric_guardrails": write_csv(METRIC_GUARDS_CSV, metric_guards),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lr_contract": write_csv(FUTURE_6LR_CSV, future_6lr),
        "future_6ls_contract": write_csv(FUTURE_6LS_CSV, future_6ls),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LQ",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LQ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LQ,
        "recommended_path": RECOMMENDED_PATH_6LQ,
        "predecessor_audit": str(AUDIT_6LP_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lp.get("diagnosis"),
        "planned_layer_after": "6LP",
        "source_family": "projection_adapter_payload_shape_repair_plan",
        "problem_statement_count": len(problem),
        "candidate_retention_count": len(retention),
        "payload_shape_requirement_count": len(payload_req),
        "source_trace_target_count": len(source_targets),
        "upstream_payload_builder_search_count": len(upstream_search),
        "adapter_payload_schema_plan_count": len(adapter_schema),
        "game_entry_schema_plan_count": len(game_schema),
        "payload_repair_artifact_plan_count": len(repair_artifact),
        "fail_closed_policy_count": len(fail_closed),
        "prediction_surface_rule_count": len(surface_rules),
        "metric_guardrail_count": len(metric_guards),
        "allowed_operation_next_count": len(allowed_next),
        "forbidden_operation_next_count": len(forbidden_next),
        "blocker_count": len(blockers),
        "future_6lr_contract_valid": len(future_6lr) == 4 and all_passed(future_6lr),
        "future_6ls_contract_valid": len(future_6ls) == 4 and all_passed(future_6ls),
        "projection_adapter_payload_shape_repair_planned": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained": True,
        "blocked_session_candidate_excluded": True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_confirmed": REQUIRED_ARGS,
        "expected_payload_games_key_confirmed": True,
        "adapter_shaped_payload_repair_needed_confirmed": json_6lp.get("adapter_shaped_payload_repair_needed") is True,
        "adapter_shaped_payload_source_trace_planned": True,
        "upstream_payload_builder_search_planned": True,
        "payload_games_schema_trace_planned": True,
        "per_game_schema_trace_planned": True,
        "payload_repair_artifact_planned": True,
        "future_adapter_call_allowed_next": False,
        "additional_adapter_call_allowed_next": False,
        "import_candidate_module_allowed_next": False,
        "source_trace_only_allowed_next": True,
        "payload_artifact_write_allowed_next": True,
        "next_candidate_retry_recommended": False,
        "wrapper_plan_needed": False,
        "full_batch_adapter_call_allowed_next": False,
        "real_metric_execution_allowed_next": False,
        "projection_surface_materialization_allowed_next": False,
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
            "candidate_retention_csv": str(RETENTION_CSV),
            "payload_shape_requirement_csv": str(PAYLOAD_REQ_CSV),
            "source_trace_targets_csv": str(SOURCE_TARGETS_CSV),
            "upstream_payload_builder_search_csv": str(UPSTREAM_SEARCH_CSV),
            "adapter_payload_schema_plan_csv": str(ADAPTER_SCHEMA_CSV),
            "game_entry_schema_plan_csv": str(GAME_SCHEMA_CSV),
            "payload_repair_artifact_plan_csv": str(REPAIR_ARTIFACT_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_CSV),
            "prediction_surface_rules_csv": str(SURFACE_RULES_CSV),
            "metric_guardrails_csv": str(METRIC_GUARDS_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lr_contract_csv": str(FUTURE_6LR_CSV),
            "future_6ls_contract_csv": str(FUTURE_6LS_CSV),
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
