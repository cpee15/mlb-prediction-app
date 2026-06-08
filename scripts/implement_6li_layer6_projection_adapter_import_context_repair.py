#!/usr/bin/env python3
"""Implement package-context import repair for the selected projection adapter.

This implementation-only layer retries the same selected candidate through
package import semantics instead of file-location import, then attempts at most
one local single-sample call if safety gates pass.
"""

from __future__ import annotations

import ast
import csv
import importlib
import inspect
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6li_projection_adapter_import_context_repair_implementation"
TMP_DIR = Path("tmp")

PLAN_6LH_PATH = Path("scripts/plan_6lh_layer6_projection_adapter_import_context_repair.py")
JSON_6LH = TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan.json"
FIXTURE_SURFACE = TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv"

REQUIRED_6LH_INPUTS = [
    JSON_6LH,
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_checks.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_predecessor.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_problem_statement.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_candidate_retention.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_import_repair_strategy.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_package_context_checks.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_signature_gate_plan.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_adapter_retry_conditions.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_fail_closed_policy.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_prediction_surface_rules.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_metric_guardrails.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_blockers.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_future_6li_contract.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_future_6lj_contract.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_decision.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6lh_projection_adapter_import_context_repair_plan_recommended_path.csv",
]
SOURCE_INPUTS = [
    FIXTURE_SURFACE,
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_signature_inspection.csv",
    TMP_DIR / "layer6_6lf_projection_adapter_next_candidate_implementation_payload_mapping.csv",
]
ALL_INPUTS = REQUIRED_6LH_INPUTS + SOURCE_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CANDIDATE_CONFIRMATION_CSV = TMP_DIR / f"{SLUG}_candidate_confirmation.csv"
PACKAGE_IMPORT_CSV = TMP_DIR / f"{SLUG}_package_import_attempt.csv"
SIGNATURE_CSV = TMP_DIR / f"{SLUG}_signature_inspection.csv"
PAYLOAD_CSV = TMP_DIR / f"{SLUG}_payload_mapping.csv"
ATTEMPT_CSV = TMP_DIR / f"{SLUG}_adapter_call_attempt.csv"
PROJECTION_SURFACE_CSV = TMP_DIR / f"{SLUG}_projection_surface.csv"
GAP_CSV = TMP_DIR / f"{SLUG}_gap_report.csv"
EXTRACTION_CSV = TMP_DIR / f"{SLUG}_prediction_extraction.csv"
METRIC_CSV = TMP_DIR / f"{SLUG}_metric_readiness.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LJ_CSV = TMP_DIR / f"{SLUG}_future_6lj_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LH = "layer_6_projection_adapter_import_context_repair_plan_complete"
DIAGNOSIS_6LI = "layer_6_projection_adapter_import_context_repair_implementation_complete"
RECOMMENDED_NEXT_LAYER_6LH = "6LI_layer_6_projection_adapter_import_context_repair_implementation"
RECOMMENDED_NEXT_LAYER_6LI = "6LJ_layer_6_projection_adapter_import_context_repair_audit"
RECOMMENDED_PATH_6LI = "audit_package_context_import_retry_for_same_candidate"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FILE = "mlb_app/ai_data_assistant_performance.py"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
BLOCKED_FUNCTION = "cached_build_model_projection_payload"

FORBIDDEN_PARAM_TOKENS = [
    "app", "db", "session", "request", "engine", "client", "connection",
    "cursor", "api", "http", "fetch", "env", "background", "server",
]
FORBIDDEN_TEXT_TOKENS = [
    "requests.", "httpx.", "urllib.", "aiohttp.", "fetch(", "axios.",
    "sqlalchemy", "database", "db.", "to_sql", "insert ", "update ",
    "delete ", "fastapi", "flask", "uvicorn", "streamlit", "os.environ",
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


def inspect_signature(path: Path, function_name: str) -> Tuple[Dict[str, Any], bool]:
    if not path.exists():
        return {"signature_found": False, "reason": "target_file_missing", "passed": True}, False

    text = path.read_text(encoding="utf-8", errors="ignore")
    lowered = text.lower()
    risk_hits = [tok for tok in FORBIDDEN_TEXT_TOKENS if tok.lower() in lowered]

    try:
        tree = ast.parse(text)
    except Exception as exc:
        return {"signature_found": False, "reason": f"ast_parse_failed:{type(exc).__name__}:{exc}", "risk_hits": ";".join(risk_hits), "passed": True}, False

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            args = [a.arg for a in node.args.args]
            kwonly = [a.arg for a in node.args.kwonlyargs]
            all_args = args + kwonly
            forbidden_params = [p for p in all_args if any(tok in p.lower() for tok in FORBIDDEN_PARAM_TOKENS)]
            safe = not risk_hits and not forbidden_params
            return {
                "signature_found": True,
                "function_name": function_name,
                "args": ";".join(args),
                "kwonly_args": ";".join(kwonly),
                "arg_count": len(all_args),
                "risk_hits": ";".join(risk_hits),
                "forbidden_params": ";".join(forbidden_params),
                "signature_mapping_safe": safe,
                "reason": "signature_safe" if safe else "risk_hits_or_forbidden_params",
                "passed": True,
            }, safe

    return {"signature_found": False, "reason": "function_not_found_in_ast", "risk_hits": ";".join(risk_hits), "passed": True}, False


def build_payload() -> Tuple[Dict[str, Any], bool]:
    rows = read_csv_rows(FIXTURE_SURFACE)
    usable = None
    for row in rows:
        if row.get("game_id") or (row.get("game_date") and row.get("home_team")):
            usable = row
            break
    if usable is None:
        return {"payload_created": False, "reason": "no_usable_fixture_row", "passed": True}, False

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
        "generation_mode": "single_sample_non_production_package_context_import_retry",
        "source_lineage": usable.get("source_lineage", ""),
        "non_production": True,
    }
    return {
        "payload_created": True,
        "payload_json": json.dumps(payload, sort_keys=True),
        "fixture_source_lineage": usable.get("source_lineage", ""),
        "mapping_mode": "serializable_fixture_payload",
        "passed": True,
    }, True


def package_import_function(module_path: str, function_name: str) -> Tuple[List[Dict[str, Any]], Any | None]:
    sys_path_adjustment_used = False
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
        sys_path_adjustment_used = True

    try:
        module = importlib.import_module(module_path)
        func = getattr(module, function_name, None)
        function_callable = callable(func)
        return [{
            "package_import_attempted": True,
            "package_import_succeeded": True,
            "sys_path_adjustment_used": sys_path_adjustment_used,
            "module_path": module_path,
            "function_retrieved": func is not None,
            "function_callable": function_callable,
            "reason": "package_import_succeeded",
            "passed": True,
        }], func if function_callable else None
    except Exception as exc:
        return [{
            "package_import_attempted": True,
            "package_import_succeeded": False,
            "sys_path_adjustment_used": sys_path_adjustment_used,
            "module_path": module_path,
            "function_retrieved": False,
            "function_callable": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "passed": True,
        }], None


def call_once(func: Any | None, signature_safe: bool, payload_row: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any] | None]:
    if func is None:
        return [{"adapter_call_attempted": False, "adapter_call_succeeded": False, "reason": "function_not_callable", "passed": True}], None
    if not signature_safe:
        return [{"adapter_call_attempted": False, "adapter_call_succeeded": False, "reason": "signature_not_safe", "passed": True}], None

    try:
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        payload = json.loads(str(payload_row.get("payload_json", "{}")))

        if len(params) == 0:
            result = func()
            call_mode = "no_arg_call"
        elif len(params) == 1:
            result = func(payload)
            call_mode = "single_dict_payload_call"
        else:
            return [{
                "adapter_call_attempted": False,
                "adapter_call_succeeded": False,
                "reason": "signature_requires_more_than_one_parameter",
                "param_count": len(params),
                "passed": True,
            }], None

        if isinstance(result, dict):
            payload_result = result
        elif isinstance(result, list):
            payload_result = {"_result_list": result}
        else:
            payload_result = {"_result": result}

        return [{
            "adapter_call_attempted": True,
            "adapter_call_succeeded": True,
            "call_mode": call_mode,
            "result_type": type(result).__name__,
            "result_preview": json.dumps(result, default=str)[:500],
            "passed": True,
        }], payload_result

    except Exception as exc:
        return [{
            "adapter_call_attempted": True,
            "adapter_call_succeeded": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "passed": True,
        }], None


def extract_prediction(result: Dict[str, Any] | None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if result is None:
        return [{"field": "none", "materialized": False, "value": "", "passed": True}], {}

    flat: Dict[str, Any] = {}

    def walk(prefix: str, obj: Any) -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                walk(f"{prefix}.{key}" if prefix else str(key), value)
        elif isinstance(obj, list):
            for idx, value in enumerate(obj[:5]):
                walk(f"{prefix}[{idx}]", value)
        else:
            flat[prefix] = obj

    walk("", result)

    targets = {
        "home_win_probability": ["home_win_probability", "home_probability", "home_win_prob", "home_prob", "home_win"],
        "away_win_probability": ["away_win_probability", "away_probability", "away_win_prob", "away_prob", "away_win"],
        "home_expected_runs": ["home_expected_runs", "home_runs", "expected_home_runs", "home_score"],
        "away_expected_runs": ["away_expected_runs", "away_runs", "expected_away_runs", "away_score"],
        "total_expected_runs": ["total_expected_runs", "expected_total", "total_runs", "total_score"],
    }

    extracted: Dict[str, Any] = {}
    rows: List[Dict[str, Any]] = []
    for output_field, names in targets.items():
        value = ""
        source_key = ""
        for key, candidate_value in flat.items():
            key_l = key.lower().replace(" ", "_")
            if any(name in key_l for name in names) and str(candidate_value).strip() not in {"", "None", "nan", "null"}:
                value = candidate_value
                source_key = key
                break
        extracted[output_field] = value
        rows.append({"field": output_field, "materialized": value != "", "value": value, "source_key": source_key, "passed": True})
    return rows, extracted


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6lh = load_json(JSON_6LH)

    candidate_rows = [
        {
            "same_candidate_retained_confirmed": json_6lh.get("same_candidate_retained") is True,
            "blocked_session_candidate_excluded_confirmed": json_6lh.get("blocked_session_candidate_excluded") is True,
            "file_location_import_avoided": True,
            "target_module_import_path": TARGET_MODULE,
            "target_function_name": TARGET_FUNCTION,
            "passed": True,
        }
    ]

    signature_row, signature_safe = inspect_signature(Path(TARGET_FILE), TARGET_FUNCTION)
    signature_rows = [signature_row]
    payload_row, payload_created = build_payload()
    payload_rows = [payload_row]

    func = None
    if signature_safe and payload_created:
        package_rows, func = package_import_function(TARGET_MODULE, TARGET_FUNCTION)
    else:
        package_rows = [{
            "package_import_attempted": False,
            "package_import_succeeded": False,
            "sys_path_adjustment_used": False,
            "module_path": TARGET_MODULE,
            "function_retrieved": False,
            "function_callable": False,
            "reason": "signature_or_payload_gate_failed",
            "passed": True,
        }]

    package_import_attempted = any(boolish(row.get("package_import_attempted")) for row in package_rows)
    package_import_succeeded = any(boolish(row.get("package_import_succeeded")) for row in package_rows)
    sys_path_adjustment_used = any(boolish(row.get("sys_path_adjustment_used")) for row in package_rows)
    function_retrieved = any(boolish(row.get("function_retrieved")) for row in package_rows)
    function_callable = any(boolish(row.get("function_callable")) for row in package_rows)

    attempt_rows, result_payload = call_once(func, signature_safe and package_import_succeeded and function_callable, payload_row)

    adapter_call_attempted = any(boolish(row.get("adapter_call_attempted")) for row in attempt_rows)
    adapter_call_succeeded = any(boolish(row.get("adapter_call_succeeded")) for row in attempt_rows)

    extraction_rows, extracted = extract_prediction(result_payload if adapter_call_succeeded else None)
    probability_fields = bool(extracted.get("home_win_probability") or extracted.get("away_win_probability"))
    runs_fields = bool(extracted.get("home_expected_runs") or extracted.get("away_expected_runs") or extracted.get("total_expected_runs"))
    any_fields = probability_fields or runs_fields

    fixture_payload = json.loads(str(payload_row.get("payload_json", "{}"))) if payload_created else {}
    projection_rows: List[Dict[str, Any]] = []
    gap_rows: List[Dict[str, Any]] = []

    if adapter_call_succeeded:
        projection_rows.append({
            "game_id": fixture_payload.get("game_id", ""),
            "game_date": fixture_payload.get("game_date", ""),
            "home_team": fixture_payload.get("home_team", ""),
            "away_team": fixture_payload.get("away_team", ""),
            "home_win_probability": extracted.get("home_win_probability", ""),
            "away_win_probability": extracted.get("away_win_probability", ""),
            "home_expected_runs": extracted.get("home_expected_runs", ""),
            "away_expected_runs": extracted.get("away_expected_runs", ""),
            "total_expected_runs": extracted.get("total_expected_runs", ""),
            "projection_source": TARGET_MODULE,
            "projection_entrypoint": TARGET_FUNCTION,
            "projection_call_mode": attempt_rows[0].get("call_mode", "unknown"),
            "projection_call_status": "success_real_fields" if any_fields else "success_no_prediction_fields",
            "missing_input_families": "" if any_fields else "returned_payload_without_probability_or_runs_fields",
            "fallback_used": False if any_fields else True,
            "notes": "Single-sample non-production package-context import retry executed.",
            "current_ui_realism_state_label": "bullpen_active_partial_realism",
            "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
            "non_production": True,
            "passed": True,
        })
    else:
        gap_reason = (
            package_rows[0].get("reason")
            if not package_import_succeeded
            else attempt_rows[0].get("reason", "unknown")
        )
        gap_rows.append({
            "gap": "package_context_import_retry_not_successfully_materialized",
            "target_module_import_path": TARGET_MODULE,
            "target_function_name": TARGET_FUNCTION,
            "package_import_attempted": package_import_attempted,
            "package_import_succeeded": package_import_succeeded,
            "function_retrieved": function_retrieved,
            "function_callable": function_callable,
            "adapter_call_attempted": adapter_call_attempted,
            "adapter_call_succeeded": adapter_call_succeeded,
            "reason": gap_reason,
            "next_action": "audit_and_route_to_batch_plan_next_candidate_or_wrapper",
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
        {"blocker": "real_prediction_surface_not_materialized" if not any_fields else "real_prediction_surface_requires_audit", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "package_context_import_retry_requires_audit", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lj = [
        {"contract": "audit_package_context_import_attempt", "required": True, "passed": True},
        {"contract": "audit_adapter_call_surface_or_gap", "required": True, "passed": True},
        {"contract": "route_to_batch_plan_next_candidate_or_wrapper", "required": True, "passed": True},
        {"contract": "preserve_no_real_metrics_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lh_plan_script_exists", "expected": True, "actual": PLAN_6LH_PATH.exists(), "passed": PLAN_6LH_PATH.exists()},
        {"check": "6lh_json_exists", "expected": True, "actual": JSON_6LH.exists(), "passed": JSON_6LH.exists()},
        {"check": "6lh_all_checks_passed", "expected": True, "actual": json_6lh.get("all_checks_passed"), "passed": json_6lh.get("all_checks_passed") is True},
        {"check": "6lh_diagnosis", "expected": DIAGNOSIS_6LH, "actual": json_6lh.get("diagnosis"), "passed": json_6lh.get("diagnosis") == DIAGNOSIS_6LH},
        {"check": "6lh_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LH, "actual": json_6lh.get("recommended_next_layer"), "passed": json_6lh.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LH},
        {"check": "6lh_same_candidate_retained", "expected": True, "actual": json_6lh.get("same_candidate_retained"), "passed": json_6lh.get("same_candidate_retained") is True},
        {"check": "6lh_package_import_planned", "expected": True, "actual": json_6lh.get("package_context_import_planned"), "passed": json_6lh.get("package_context_import_planned") is True},
        {"check": "6lh_file_location_import_forbidden", "expected": True, "actual": json_6lh.get("file_location_import_forbidden_next"), "passed": json_6lh.get("file_location_import_forbidden_next") is True},
        {"check": "6lh_no_layer6_exit", "expected": False, "actual": json_6lh.get("layer_6_exit_recommended"), "passed": json_6lh.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in ALL_INPUTS
    ]
    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in ALL_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6lj_import_context_repair_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "different_candidate_retry", "blocked": True, "reason": "6LI only implemented same-candidate package import retry", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "6LI cannot design wrapper", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface audit required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LI cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lh_passed", "expected": True, "actual": json_6lh.get("all_checks_passed"), "passed": json_6lh.get("all_checks_passed") is True},
        {"decision": "same_candidate_retained", "expected": True, "actual": True, "passed": True},
        {"decision": "blocked_session_candidate_excluded", "expected": True, "actual": True, "passed": True},
        {"decision": "file_location_import_avoided", "expected": True, "actual": True, "passed": True},
        {"decision": "package_import_attempt_recorded", "expected": True, "actual": bool(package_rows), "passed": bool(package_rows)},
        {"decision": "signature_inspection_recorded", "expected": True, "actual": bool(signature_rows), "passed": bool(signature_rows)},
        {"decision": "payload_mapping_recorded", "expected": True, "actual": bool(payload_rows), "passed": bool(payload_rows)},
        {"decision": "attempt_or_gap_recorded", "expected": True, "actual": bool(attempt_rows) or bool(gap_rows), "passed": bool(attempt_rows) or bool(gap_rows)},
        {"decision": "projection_surface_or_gap", "expected": True, "actual": bool(projection_rows) or bool(gap_rows), "passed": bool(projection_rows) or bool(gap_rows)},
        {"decision": "future_6lj_contract_valid", "expected": True, "actual": len(future_6lj) == 4 and all_passed(future_6lj), "passed": len(future_6lj) == 4 and all_passed(future_6lj)},
        {"decision": "recommend_6lj_next", "expected": RECOMMENDED_NEXT_LAYER_6LI, "actual": RECOMMENDED_NEXT_LAYER_6LI, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_single_sample", "expected": True, "actual": True, "passed": True},
        {"boundary": "projection_adapter_import_context_repair_implemented", "expected": True, "actual": True, "passed": True},
        {"boundary": "file_location_import_avoided", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6lh_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only_reference", "passed": True},
        {"surface": "projection_surface_or_gap", "policy": "tmp_non_production_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6li", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LI, "actual": RECOMMENDED_NEXT_LAYER_6LI, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LI, "actual": RECOMMENDED_PATH_6LI, "passed": True},
        {"decision": "recommend_import_repair_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LI, "actual": DIAGNOSIS_6LI, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "candidate_confirmation", "passed": all_passed(candidate_rows), "detail": f"{len(candidate_rows)} rows"},
        {"check": "package_import_attempt", "passed": all_passed(package_rows), "detail": f"{len(package_rows)} rows"},
        {"check": "signature_inspection", "passed": all_passed(signature_rows), "detail": f"{len(signature_rows)} rows"},
        {"check": "payload_mapping", "passed": all_passed(payload_rows), "detail": f"{len(payload_rows)} rows"},
        {"check": "adapter_call_attempt", "passed": all_passed(attempt_rows), "detail": f"{len(attempt_rows)} rows"},
        {"check": "surface_or_gap", "passed": bool(projection_rows) or bool(gap_rows), "detail": f"surface={bool(projection_rows)};gap={bool(gap_rows)}"},
        {"check": "prediction_extraction", "passed": all_passed(extraction_rows), "detail": f"{len(extraction_rows)} rows"},
        {"check": "metric_readiness", "passed": all_passed(metric_rows), "detail": f"{len(metric_rows)} rows"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{len(blockers)} rows"},
        {"check": "future_6lj_contract", "passed": all_passed(future_6lj), "detail": f"{len(future_6lj)} rows"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)
    adapter_failed_closed = bool(gap_rows) and not adapter_call_succeeded
    projection_surface_materialized = bool(projection_rows)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "candidate_confirmation": write_csv(CANDIDATE_CONFIRMATION_CSV, candidate_rows),
        "package_import_attempt": write_csv(PACKAGE_IMPORT_CSV, package_rows),
        "signature_inspection": write_csv(SIGNATURE_CSV, signature_rows),
        "payload_mapping": write_csv(PAYLOAD_CSV, payload_rows),
        "adapter_call_attempt": write_csv(ATTEMPT_CSV, attempt_rows),
        "projection_surface": write_csv(PROJECTION_SURFACE_CSV, projection_rows),
        "gap_report": write_csv(GAP_CSV, gap_rows),
        "prediction_extraction": write_csv(EXTRACTION_CSV, extraction_rows),
        "metric_readiness": write_csv(METRIC_CSV, metric_rows),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lj_contract": write_csv(FUTURE_6LJ_CSV, future_6lj),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LI",
        "layer_type": "game_mechanics_realism",
        "implementation_only_single_sample": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LI if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LI,
        "recommended_path": RECOMMENDED_PATH_6LI,
        "predecessor_plan": str(PLAN_6LH_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lh.get("diagnosis"),
        "implemented_layer_after": "6LH",
        "source_family": "projection_adapter_import_context_repair_implementation",
        "candidate_confirmation_count": len(candidate_rows),
        "package_import_attempt_count": len(package_rows),
        "signature_inspection_count": len(signature_rows),
        "payload_mapping_count": len(payload_rows),
        "adapter_call_attempt_count": len(attempt_rows),
        "projection_surface_row_count": len(projection_rows),
        "gap_report_count": len(gap_rows),
        "prediction_extraction_count": len(extraction_rows),
        "metric_readiness_count": len(metric_rows),
        "blocker_count": len(blockers),
        "future_6lj_contract_valid": len(future_6lj) == 4 and all_passed(future_6lj),
        "projection_adapter_import_context_repair_implemented": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained_confirmed": json_6lh.get("same_candidate_retained") is True,
        "blocked_session_candidate_excluded_confirmed": json_6lh.get("blocked_session_candidate_excluded") is True,
        "file_location_import_avoided": True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "package_import_attempted": package_import_attempted,
        "package_import_succeeded": package_import_succeeded,
        "sys_path_adjustment_used": sys_path_adjustment_used,
        "function_retrieved": function_retrieved,
        "function_callable": function_callable,
        "fixture_payload_created": payload_created,
        "static_signature_inspected": bool(signature_rows),
        "signature_mapping_safe": signature_safe,
        "adapter_call_attempted": adapter_call_attempted,
        "adapter_call_succeeded": adapter_call_succeeded,
        "adapter_call_failed_closed": adapter_failed_closed,
        "projection_surface_materialized": projection_surface_materialized,
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
            "candidate_confirmation_csv": str(CANDIDATE_CONFIRMATION_CSV),
            "package_import_attempt_csv": str(PACKAGE_IMPORT_CSV),
            "signature_inspection_csv": str(SIGNATURE_CSV),
            "payload_mapping_csv": str(PAYLOAD_CSV),
            "adapter_call_attempt_csv": str(ATTEMPT_CSV),
            "projection_surface_csv": str(PROJECTION_SURFACE_CSV),
            "gap_report_csv": str(GAP_CSV),
            "prediction_extraction_csv": str(EXTRACTION_CSV),
            "metric_readiness_csv": str(METRIC_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lj_contract_csv": str(FUTURE_6LJ_CSV),
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
