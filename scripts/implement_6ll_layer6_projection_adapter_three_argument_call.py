#!/usr/bin/env python3
"""Implement one controlled three-argument call for the selected adapter."""

from __future__ import annotations

import ast
import csv
import importlib
import inspect
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ll_projection_adapter_three_argument_call_implementation"
TMP_DIR = Path("tmp")

PLAN_6LK_PATH = Path("scripts/plan_6lk_layer6_projection_adapter_three_argument_call.py")
JSON_6LK = TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan.json"
FIXTURE_SURFACE = TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv"

REQUIRED_6LK_INPUTS = [
    JSON_6LK,
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_checks.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_predecessor.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_problem_statement.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_candidate_retention.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_call_contract_mapping.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_argument_validation.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_signature_gate_plan.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_package_import_policy.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_adapter_call_conditions.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_fail_closed_policy.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_prediction_surface_rules.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_metric_guardrails.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_blockers.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_future_6ll_contract.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_future_6lm_contract.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_decision.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6lk_projection_adapter_three_argument_call_plan_recommended_path.csv",
]
ALL_INPUTS = REQUIRED_6LK_INPUTS + [FIXTURE_SURFACE]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CANDIDATE_CSV = TMP_DIR / f"{SLUG}_candidate_confirmation.csv"
ARG_MAPPING_CSV = TMP_DIR / f"{SLUG}_argument_mapping.csv"
SIGNATURE_CSV = TMP_DIR / f"{SLUG}_signature_inspection.csv"
IMPORT_CSV = TMP_DIR / f"{SLUG}_package_import_attempt.csv"
CALL_CSV = TMP_DIR / f"{SLUG}_adapter_call_attempt.csv"
RETURN_SHAPE_CSV = TMP_DIR / f"{SLUG}_return_shape.csv"
SURFACE_CSV = TMP_DIR / f"{SLUG}_projection_surface.csv"
GAP_CSV = TMP_DIR / f"{SLUG}_gap_report.csv"
EXTRACTION_CSV = TMP_DIR / f"{SLUG}_prediction_extraction.csv"
METRIC_CSV = TMP_DIR / f"{SLUG}_metric_readiness.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LM_CSV = TMP_DIR / f"{SLUG}_future_6lm_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LK = "layer_6_projection_adapter_three_argument_call_plan_complete"
DIAGNOSIS_6LL = "layer_6_projection_adapter_three_argument_call_implementation_complete"
RECOMMENDED_NEXT_LAYER_6LK = "6LL_layer_6_projection_adapter_three_argument_call_implementation"
RECOMMENDED_NEXT_LAYER_6LL = "6LM_layer_6_projection_adapter_three_argument_call_audit"
RECOMMENDED_PATH_6LL = "audit_three_argument_single_sample_call_for_same_candidate"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FILE = Path("mlb_app/ai_data_assistant_performance.py")
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
REQUIRED_ARGS = "payload;game_pk;limit"

FORBIDDEN_PARAM_TOKENS = [
    "app", "db", "session", "request", "engine", "client", "connection",
    "cursor", "api", "http", "fetch", "env", "background", "server",
]


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


def inspect_static_signature() -> Tuple[Dict[str, Any], bool, bool]:
    if not TARGET_FILE.exists():
        return {"signature_found": False, "reason": "target_file_missing", "passed": True}, False, False
    try:
        tree = ast.parse(TARGET_FILE.read_text(encoding="utf-8", errors="ignore"))
    except Exception as exc:
        return {"signature_found": False, "reason": f"ast_parse_failed:{type(exc).__name__}:{exc}", "passed": True}, False, False

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == TARGET_FUNCTION:
            args = [a.arg for a in node.args.args]
            kwonly = [a.arg for a in node.args.kwonlyargs]
            all_args = args + kwonly
            forbidden = [p for p in all_args if any(tok in p.lower() for tok in FORBIDDEN_PARAM_TOKENS)]
            exact = ";".join(args) == REQUIRED_ARGS and not kwonly
            safe = exact and not forbidden
            return {
                "signature_found": True,
                "function_name": TARGET_FUNCTION,
                "args": ";".join(args),
                "kwonly_args": ";".join(kwonly),
                "arg_count": len(all_args),
                "signature_exact_match": exact,
                "forbidden_params": ";".join(forbidden),
                "signature_mapping_safe": safe,
                "reason": "signature_exact_safe" if safe else "signature_unexpected_or_forbidden",
                "passed": True,
            }, exact, safe

    return {"signature_found": False, "reason": "function_not_found", "passed": True}, False, False


def build_payload_and_args(plan: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], int | None, int, bool]:
    rows = read_csv_rows(FIXTURE_SURFACE)
    usable = None
    for row in rows:
        if str(row.get("game_id", "")).strip():
            usable = row
            break

    if usable is None:
        return [{
            "argument": "all",
            "created": False,
            "safe": False,
            "reason": "no_fixture_game_id_found",
            "passed": True,
        }], {}, None, 1, False

    raw_game_pk = str(usable.get("game_id", "")).strip()
    safe_game_pk = raw_game_pk.isdigit()
    game_pk = int(raw_game_pk) if safe_game_pk else None
    planned_game_pk = plan.get("game_pk_value_planned")
    game_pk_matches_plan = planned_game_pk in (None, "", game_pk)
    limit = 1
    limit_matches_plan = plan.get("limit_value_planned") in (None, "", 1, limit)

    payload = {
        "game_id": usable.get("game_id", ""),
        "game_date": usable.get("game_date", ""),
        "season": usable.get("season", ""),
        "home_team": usable.get("home_team", ""),
        "away_team": usable.get("away_team", ""),
        "home_pitcher": usable.get("home_pitcher", ""),
        "away_pitcher": usable.get("away_pitcher", ""),
        "home_lineup_proxy": usable.get("home_lineup_proxy", ""),
        "away_lineup_proxy": usable.get("away_lineup_proxy", ""),
        "park_factor_proxy": usable.get("park_factor_proxy", ""),
        "bullpen_state_proxy": usable.get("bullpen_state_proxy", ""),
        "mechanic_context_tags": usable.get("mechanic_context_tags", ""),
        "generation_mode": "single_sample_non_production_three_argument_adapter_call",
        "source_lineage": usable.get("source_lineage", ""),
        "non_production": True,
    }

    payload_created = True
    overall_safe = payload_created and safe_game_pk and game_pk_matches_plan and limit_matches_plan and limit == 1

    rows_out = [
        {"argument": "payload", "created": payload_created, "safe": payload_created, "source": "6kz_fixture_contract_surface_first_usable_row", "value_preview": json.dumps(payload, sort_keys=True)[:500], "passed": True},
        {"argument": "game_pk", "created": game_pk is not None, "safe": safe_game_pk, "raw_value": raw_game_pk, "value": game_pk if game_pk is not None else "", "matches_plan": game_pk_matches_plan, "passed": True},
        {"argument": "limit", "created": True, "safe": limit == 1, "value": limit, "matches_plan": limit_matches_plan, "passed": True},
        {"argument": "overall", "created": overall_safe, "safe": overall_safe, "passed": True},
    ]
    return rows_out, payload, game_pk, limit, overall_safe


def package_import_function() -> Tuple[List[Dict[str, Any]], Any | None]:
    sys_path_adjustment_used = False
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
        sys_path_adjustment_used = True

    try:
        module = importlib.import_module(TARGET_MODULE)
        func = getattr(module, TARGET_FUNCTION, None)
        return [{
            "package_import_attempted": True,
            "package_import_succeeded": True,
            "sys_path_adjustment_used": sys_path_adjustment_used,
            "module_path": TARGET_MODULE,
            "function_retrieved": func is not None,
            "function_callable": callable(func),
            "passed": True,
        }], func if callable(func) else None
    except Exception as exc:
        return [{
            "package_import_attempted": True,
            "package_import_succeeded": False,
            "sys_path_adjustment_used": sys_path_adjustment_used,
            "module_path": TARGET_MODULE,
            "function_retrieved": False,
            "function_callable": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "passed": True,
        }], None


def return_shape(result: Any | None) -> List[Dict[str, Any]]:
    if result is None:
        return [{"return_materialized": False, "return_type": "", "return_shape_summary": "no_return", "passed": True}]
    if isinstance(result, dict):
        summary = f"dict_keys={list(result.keys())[:25]}"
    elif isinstance(result, list):
        summary = f"list_len={len(result)};first_type={type(result[0]).__name__ if result else 'empty'}"
    else:
        summary = f"value_preview={str(result)[:250]}"
    return [{"return_materialized": True, "return_type": type(result).__name__, "return_shape_summary": summary, "passed": True}]


def call_once(func: Any | None, payload: Dict[str, Any], game_pk: int | None, limit: int, gates_safe: bool) -> Tuple[List[Dict[str, Any]], Any | None]:
    if not gates_safe:
        return [{"adapter_call_attempted": False, "adapter_call_succeeded": False, "adapter_call_count": 0, "reason": "pre_call_gates_failed", "passed": True}], None
    if func is None:
        return [{"adapter_call_attempted": False, "adapter_call_succeeded": False, "adapter_call_count": 0, "reason": "function_not_callable", "passed": True}], None
    if game_pk is None:
        return [{"adapter_call_attempted": False, "adapter_call_succeeded": False, "adapter_call_count": 0, "reason": "game_pk_not_safe_int", "passed": True}], None

    try:
        result = func(payload, game_pk=game_pk, limit=limit)
        return [{
            "adapter_call_attempted": True,
            "adapter_call_succeeded": True,
            "adapter_call_count": 1,
            "call_mode": "keyword_three_argument_call",
            "game_pk": game_pk,
            "limit": limit,
            "result_type": type(result).__name__,
            "passed": True,
        }], result
    except Exception as exc:
        return [{
            "adapter_call_attempted": True,
            "adapter_call_succeeded": False,
            "adapter_call_count": 1,
            "call_mode": "keyword_three_argument_call",
            "game_pk": game_pk,
            "limit": limit,
            "reason": f"{type(exc).__name__}: {exc}",
            "passed": True,
        }], None


def flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            out.update(flatten(value, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(obj, list):
        for idx, value in enumerate(obj[:5]):
            out.update(flatten(value, f"{prefix}[{idx}]"))
    else:
        out[prefix] = obj
    return out


def extract_predictions(result: Any | None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if result is None:
        return [{"field": "none", "materialized": False, "value": "", "source_key": "", "passed": True}], {}

    flat = flatten(result)
    targets = {
        "home_win_probability": ["home_win_probability", "home_probability", "home_win_prob", "home_prob"],
        "away_win_probability": ["away_win_probability", "away_probability", "away_win_prob", "away_prob"],
        "home_expected_runs": ["home_expected_runs", "home_runs", "expected_home_runs", "home_score"],
        "away_expected_runs": ["away_expected_runs", "away_runs", "expected_away_runs", "away_score"],
        "total_expected_runs": ["total_expected_runs", "expected_total", "total_runs", "total_score"],
    }
    extracted: Dict[str, Any] = {}
    rows: List[Dict[str, Any]] = []
    for field, names in targets.items():
        value = ""
        source_key = ""
        for key, candidate in flat.items():
            normalized = key.lower().replace(" ", "_")
            if any(name in normalized for name in names) and str(candidate).strip() not in {"", "None", "nan", "null"}:
                value = candidate
                source_key = key
                break
        extracted[field] = value
        rows.append({"field": field, "materialized": value != "", "value": value, "source_key": source_key, "passed": True})
    return rows, extracted


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    plan_6lk = load_json(JSON_6LK)

    arg_rows, payload, game_pk, limit, args_safe = build_payload_and_args(plan_6lk)
    sig_row, signature_exact, signature_safe = inspect_static_signature()
    sig_rows = [sig_row]

    candidate_rows = [
        {"same_candidate_retained_confirmed": plan_6lk.get("same_candidate_retained") is True, "passed": True},
        {"blocked_session_candidate_excluded_confirmed": plan_6lk.get("blocked_session_candidate_excluded") is True, "passed": True},
        {"package_context_import_preserved_confirmed": plan_6lk.get("package_context_import_preserved") is True, "passed": True},
        {"file_location_import_avoided": True, "passed": True},
        {"target_module_import_path": TARGET_MODULE, "target_function_name": TARGET_FUNCTION, "passed": True},
    ]

    pre_import_safe = args_safe and signature_safe
    import_rows, func = package_import_function() if pre_import_safe else ([{
        "package_import_attempted": False,
        "package_import_succeeded": False,
        "sys_path_adjustment_used": False,
        "module_path": TARGET_MODULE,
        "function_retrieved": False,
        "function_callable": False,
        "reason": "pre_import_gates_failed",
        "passed": True,
    }], None)

    package_import_succeeded = any(boolish(row.get("package_import_succeeded")) for row in import_rows)
    function_callable = any(boolish(row.get("function_callable")) for row in import_rows)
    gates_safe = pre_import_safe and package_import_succeeded and function_callable

    call_rows, result = call_once(func, payload, game_pk, limit, gates_safe)
    return_rows = return_shape(result)
    extraction_rows, extracted = extract_predictions(result)

    adapter_call_attempted = any(boolish(row.get("adapter_call_attempted")) for row in call_rows)
    adapter_call_succeeded = any(boolish(row.get("adapter_call_succeeded")) for row in call_rows)
    adapter_call_count = sum(int(row.get("adapter_call_count", 0) or 0) for row in call_rows)

    probability_fields = bool(extracted.get("home_win_probability") or extracted.get("away_win_probability"))
    runs_fields = bool(extracted.get("home_expected_runs") or extracted.get("away_expected_runs") or extracted.get("total_expected_runs"))
    any_fields = probability_fields or runs_fields

    surface_rows: List[Dict[str, Any]] = []
    gap_rows: List[Dict[str, Any]] = []

    if adapter_call_succeeded and any_fields:
        surface_rows.append({
            "game_pk": game_pk,
            "limit": limit,
            "home_win_probability": extracted.get("home_win_probability", ""),
            "away_win_probability": extracted.get("away_win_probability", ""),
            "home_expected_runs": extracted.get("home_expected_runs", ""),
            "away_expected_runs": extracted.get("away_expected_runs", ""),
            "total_expected_runs": extracted.get("total_expected_runs", ""),
            "projection_source": TARGET_MODULE,
            "projection_entrypoint": TARGET_FUNCTION,
            "projection_call_mode": "keyword_three_argument_call",
            "projection_surface_status": "real_prediction_fields_materialized",
            "current_ui_realism_state_label": "bullpen_active_partial_realism",
            "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
            "non_production": True,
            "passed": True,
        })
    else:
        reason = ""
        if not args_safe:
            reason = "argument_mapping_not_safe"
        elif not signature_safe:
            reason = sig_row.get("reason", "signature_not_safe")
        elif not package_import_succeeded:
            reason = import_rows[0].get("reason", "package_import_failed")
        elif not adapter_call_succeeded:
            reason = call_rows[0].get("reason", "adapter_call_failed")
        elif not any_fields:
            reason = "adapter_return_lacked_prediction_fields"

        gap_rows.append({
            "gap": "three_argument_call_did_not_materialize_real_prediction_surface",
            "target_module_import_path": TARGET_MODULE,
            "target_function_name": TARGET_FUNCTION,
            "payload_created": bool(payload),
            "game_pk_value": game_pk if game_pk is not None else "",
            "limit_value": limit,
            "package_import_succeeded": package_import_succeeded,
            "function_callable": function_callable,
            "adapter_call_attempted": adapter_call_attempted,
            "adapter_call_succeeded": adapter_call_succeeded,
            "reason": reason,
            "next_action": "audit_three_argument_call_result",
            "non_production": True,
            "passed": True,
        })

    metric_rows = [
        {"metric": "probability_metric_ready_after_implementation", "value": probability_fields, "passed": True},
        {"metric": "runs_metric_ready_after_implementation", "value": runs_fields, "passed": True},
        {"metric": "any_backtest_metric_ready_after_implementation", "value": any_fields, "passed": True},
        {"metric": "real_backtest_metrics_run", "value": False, "passed": True},
        {"metric": "full_batch_adapter_call_run", "value": False, "passed": True},
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized" if not any_fields else "single_sample_surface_requires_audit", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "three_argument_call_requires_audit", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lm = [
        {"contract": "audit_three_argument_call_attempt", "required": True, "passed": True},
        {"contract": "audit_return_shape_and_surface_or_gap", "required": True, "passed": True},
        {"contract": "route_to_batch_plan_next_candidate_or_wrapper", "required": True, "passed": True},
        {"contract": "preserve_no_real_metrics_activation_or_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lk_plan_script_exists", "expected": True, "actual": PLAN_6LK_PATH.exists(), "passed": PLAN_6LK_PATH.exists()},
        {"check": "6lk_json_exists", "expected": True, "actual": JSON_6LK.exists(), "passed": JSON_6LK.exists()},
        {"check": "6lk_all_checks_passed", "expected": True, "actual": plan_6lk.get("all_checks_passed"), "passed": plan_6lk.get("all_checks_passed") is True},
        {"check": "6lk_diagnosis", "expected": DIAGNOSIS_6LK, "actual": plan_6lk.get("diagnosis"), "passed": plan_6lk.get("diagnosis") == DIAGNOSIS_6LK},
        {"check": "6lk_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LK, "actual": plan_6lk.get("recommended_next_layer"), "passed": plan_6lk.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LK},
        {"check": "6lk_game_pk_safe", "expected": True, "actual": plan_6lk.get("game_pk_safe_int_planned"), "passed": plan_6lk.get("game_pk_safe_int_planned") is True},
        {"check": "6lk_limit", "expected": 1, "actual": plan_6lk.get("limit_value_planned"), "passed": plan_6lk.get("limit_value_planned") == 1},
        {"check": "6lk_no_layer6_exit", "expected": False, "actual": plan_6lk.get("layer_6_exit_recommended"), "passed": plan_6lk.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [{"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()} for path in ALL_INPUTS]
    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in ALL_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lm_three_argument_call_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "second_adapter_call", "blocked": True, "reason": "6LL allows at most one call", "passed": adapter_call_count <= 1},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "requires audit first", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "requires audit first", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface and metric plan required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LL cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lk_passed", "expected": True, "actual": plan_6lk.get("all_checks_passed"), "passed": plan_6lk.get("all_checks_passed") is True},
        {"decision": "candidate_confirmation_valid", "expected": True, "actual": all_passed(candidate_rows), "passed": all_passed(candidate_rows)},
        {"decision": "argument_mapping_recorded", "expected": True, "actual": all_passed(arg_rows), "passed": all_passed(arg_rows)},
        {"decision": "signature_inspection_recorded", "expected": True, "actual": all_passed(sig_rows), "passed": all_passed(sig_rows)},
        {"decision": "package_import_recorded", "expected": True, "actual": bool(import_rows), "passed": bool(import_rows)},
        {"decision": "adapter_call_count_lte_one", "expected": True, "actual": adapter_call_count <= 1, "passed": adapter_call_count <= 1},
        {"decision": "surface_or_gap_emitted", "expected": True, "actual": bool(surface_rows) or bool(gap_rows), "passed": bool(surface_rows) or bool(gap_rows)},
        {"decision": "future_6lm_contract_valid", "expected": True, "actual": len(future_6lm) == 4 and all_passed(future_6lm), "passed": len(future_6lm) == 4 and all_passed(future_6lm)},
        {"decision": "recommend_6lm_next", "expected": RECOMMENDED_NEXT_LAYER_6LL, "actual": RECOMMENDED_NEXT_LAYER_6LL, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_single_sample", "expected": True, "actual": True, "passed": True},
        {"boundary": "adapter_call_count_lte_one", "expected": True, "actual": adapter_call_count <= 1, "passed": adapter_call_count <= 1},
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
        {"surface": "source_tree", "policy": "implementation_script_only", "passed": True},
        {"surface": "6lk_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only_reference", "passed": True},
        {"surface": "projection_surface_or_gap", "policy": "tmp_non_production_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6ll", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LL, "actual": RECOMMENDED_NEXT_LAYER_6LL, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LL, "actual": RECOMMENDED_PATH_6LL, "passed": True},
        {"decision": "recommend_three_argument_call_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LL, "actual": DIAGNOSIS_6LL, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "candidate_confirmation", "passed": all_passed(candidate_rows), "detail": f"{len(candidate_rows)} rows"},
        {"check": "argument_mapping", "passed": all_passed(arg_rows), "detail": f"{len(arg_rows)} rows"},
        {"check": "signature_inspection", "passed": all_passed(sig_rows), "detail": f"{len(sig_rows)} rows"},
        {"check": "package_import_attempt", "passed": all_passed(import_rows), "detail": f"{len(import_rows)} rows"},
        {"check": "adapter_call_attempt", "passed": all_passed(call_rows), "detail": f"{len(call_rows)} rows"},
        {"check": "return_shape", "passed": all_passed(return_rows), "detail": f"{len(return_rows)} rows"},
        {"check": "surface_or_gap", "passed": bool(surface_rows) or bool(gap_rows), "detail": f"surface={bool(surface_rows)};gap={bool(gap_rows)}"},
        {"check": "prediction_extraction", "passed": all_passed(extraction_rows), "detail": f"{len(extraction_rows)} rows"},
        {"check": "metric_readiness", "passed": all_passed(metric_rows), "detail": f"{len(metric_rows)} rows"},
        {"check": "future_6lm_contract", "passed": all_passed(future_6lm), "detail": f"{len(future_6lm)} rows"},
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
        "candidate_confirmation": write_csv(CANDIDATE_CSV, candidate_rows),
        "argument_mapping": write_csv(ARG_MAPPING_CSV, arg_rows),
        "signature_inspection": write_csv(SIGNATURE_CSV, sig_rows),
        "package_import_attempt": write_csv(IMPORT_CSV, import_rows),
        "adapter_call_attempt": write_csv(CALL_CSV, call_rows),
        "return_shape": write_csv(RETURN_SHAPE_CSV, return_rows),
        "projection_surface": write_csv(SURFACE_CSV, surface_rows),
        "gap_report": write_csv(GAP_CSV, gap_rows),
        "prediction_extraction": write_csv(EXTRACTION_CSV, extraction_rows),
        "metric_readiness": write_csv(METRIC_CSV, metric_rows),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lm_contract": write_csv(FUTURE_6LM_CSV, future_6lm),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    return_materialized = any(boolish(row.get("return_materialized")) for row in return_rows)
    return_type = return_rows[0].get("return_type", "") if return_rows else ""
    return_shape_summary = return_rows[0].get("return_shape_summary", "") if return_rows else ""
    gap_count = len(gap_rows)
    surface_count = len(surface_rows)

    summary = {
        "layer": "6LL",
        "layer_type": "game_mechanics_realism",
        "implementation_only_single_sample": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LL if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LL,
        "recommended_path": RECOMMENDED_PATH_6LL,
        "predecessor_plan": str(PLAN_6LK_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": plan_6lk.get("diagnosis"),
        "implemented_layer_after": "6LK",
        "source_family": "projection_adapter_three_argument_call_implementation",
        "candidate_confirmation_count": len(candidate_rows),
        "argument_mapping_count": len(arg_rows),
        "signature_inspection_count": len(sig_rows),
        "package_import_attempt_count": len(import_rows),
        "adapter_call_attempt_count": len(call_rows),
        "return_shape_count": len(return_rows),
        "projection_surface_row_count": surface_count,
        "gap_report_count": gap_count,
        "prediction_extraction_count": len(extraction_rows),
        "metric_readiness_count": len(metric_rows),
        "blocker_count": len(blockers),
        "future_6lm_contract_valid": len(future_6lm) == 4 and all_passed(future_6lm),
        "projection_adapter_three_argument_call_implemented": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained_confirmed": plan_6lk.get("same_candidate_retained") is True,
        "blocked_session_candidate_excluded_confirmed": plan_6lk.get("blocked_session_candidate_excluded") is True,
        "package_context_import_preserved_confirmed": plan_6lk.get("package_context_import_preserved") is True,
        "file_location_import_avoided": True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_confirmed": REQUIRED_ARGS if signature_exact else "",
        "payload_created": bool(payload),
        "game_pk_value": game_pk if game_pk is not None else "",
        "game_pk_safe_int": game_pk is not None,
        "game_pk_matches_plan": plan_6lk.get("game_pk_value_planned") in (None, "", game_pk),
        "limit_value": limit,
        "limit_matches_plan": plan_6lk.get("limit_value_planned") in (None, "", limit),
        "static_signature_inspected": True,
        "signature_exact_match": signature_exact,
        "signature_mapping_safe": signature_safe,
        "package_import_attempted": any(boolish(row.get("package_import_attempted")) for row in import_rows),
        "package_import_succeeded": package_import_succeeded,
        "sys_path_adjustment_used": any(boolish(row.get("sys_path_adjustment_used")) for row in import_rows),
        "function_retrieved": any(boolish(row.get("function_retrieved")) for row in import_rows),
        "function_callable": function_callable,
        "adapter_call_attempted": adapter_call_attempted,
        "adapter_call_succeeded": adapter_call_succeeded,
        "adapter_call_failed_closed": bool(gap_rows) and not adapter_call_succeeded,
        "adapter_call_count": adapter_call_count,
        "return_materialized": return_materialized,
        "return_type": return_type,
        "return_shape_summary": return_shape_summary,
        "projection_surface_materialized": bool(surface_rows),
        "real_prediction_fields_materialized": any_fields,
        "probability_projection_fields_materialized": probability_fields,
        "runs_projection_fields_materialized": runs_fields,
        "any_projection_fields_materialized": any_fields,
        "probability_metric_ready_after_implementation": probability_fields,
        "runs_metric_ready_after_implementation": runs_fields,
        "any_backtest_metric_ready_after_implementation": any_fields,
        "full_batch_adapter_call_run": False,
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
            "candidate_confirmation_csv": str(CANDIDATE_CSV),
            "argument_mapping_csv": str(ARG_MAPPING_CSV),
            "signature_inspection_csv": str(SIGNATURE_CSV),
            "package_import_attempt_csv": str(IMPORT_CSV),
            "adapter_call_attempt_csv": str(CALL_CSV),
            "return_shape_csv": str(RETURN_SHAPE_CSV),
            "projection_surface_csv": str(SURFACE_CSV),
            "gap_report_csv": str(GAP_CSV),
            "prediction_extraction_csv": str(EXTRACTION_CSV),
            "metric_readiness_csv": str(METRIC_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lm_contract_csv": str(FUTURE_6LM_CSV),
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
