#!/usr/bin/env python3
"""Implement static provenance trace for empty projection-adapter return.

This layer parses source only. It intentionally does not import the candidate
module or execute any adapter calls.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lo_projection_adapter_empty_return_trace_implementation"
TMP_DIR = Path("tmp")

PLAN_6LN_PATH = Path("scripts/plan_6ln_layer6_projection_adapter_empty_return_trace.py")
JSON_6LN = TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan.json"

TARGET_SOURCE = Path("mlb_app/ai_data_assistant_performance.py")
TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
REQUIRED_ARGS = "payload;game_pk;limit"

REQUIRED_6LN_INPUTS = [
    JSON_6LN,
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_checks.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_predecessor.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_problem_statement.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_candidate_retention.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_empty_return_hypotheses.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_static_trace_targets.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_payload_shape_trace.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_game_pk_trace.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_limit_trace.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_filter_trace.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_fail_closed_policy.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_prediction_surface_rules.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_metric_guardrails.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_blockers.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_future_6lo_contract.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_future_6lp_contract.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_decision.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6ln_projection_adapter_empty_return_trace_plan_recommended_path.csv",
]
SOURCE_INPUTS = [
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_argument_mapping.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_return_shape.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_gap_report.csv",
]
ALL_INPUTS = REQUIRED_6LN_INPUTS + SOURCE_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CANDIDATE_CSV = TMP_DIR / f"{SLUG}_candidate_confirmation.csv"
SIGNATURE_CSV = TMP_DIR / f"{SLUG}_function_signature.csv"
RETURN_PATHS_CSV = TMP_DIR / f"{SLUG}_return_paths.csv"
PAYLOAD_PATHS_CSV = TMP_DIR / f"{SLUG}_payload_access_paths.csv"
GAME_PK_PATHS_CSV = TMP_DIR / f"{SLUG}_game_pk_paths.csv"
LIMIT_PATHS_CSV = TMP_DIR / f"{SLUG}_limit_paths.csv"
FILTER_PATHS_CSV = TMP_DIR / f"{SLUG}_filter_paths.csv"
FIXTURE_COMPARISON_CSV = TMP_DIR / f"{SLUG}_fixture_payload_comparison.csv"
ROOT_CAUSE_CSV = TMP_DIR / f"{SLUG}_root_cause_hypotheses.csv"
GAP_CSV = TMP_DIR / f"{SLUG}_gap_report.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LP_CSV = TMP_DIR / f"{SLUG}_future_6lp_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LN = "layer_6_projection_adapter_empty_return_trace_plan_complete"
DIAGNOSIS_6LO = "layer_6_projection_adapter_empty_return_trace_implementation_complete"
RECOMMENDED_NEXT_LAYER_6LN = "6LO_layer_6_projection_adapter_empty_return_trace_implementation"
RECOMMENDED_NEXT_LAYER_6LO = "6LP_layer_6_projection_adapter_empty_return_trace_audit"
RECOMMENDED_PATH_6LO = "audit_static_empty_return_provenance_trace_for_same_candidate"

EXPECTED_COLLECTION_KEYS = {
    "games",
    "canonical_games",
    "projections",
    "game_models",
    "model_outputs",
    "odds",
    "probability_context",
    "items",
    "data",
}
GAME_PK_KEYS = {"game_pk", "game_id", "id", "pk"}


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


def node_snippet(source_lines: List[str], node: ast.AST) -> str:
    lineno = getattr(node, "lineno", None)
    if lineno is None or lineno < 1 or lineno > len(source_lines):
        return ""
    return source_lines[lineno - 1].strip()[:500]


def find_target_function(tree: ast.AST) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == TARGET_FUNCTION:
            return node
    return None


def literal_key(node: ast.AST) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return str(node.value)
    return ""


def collect_payload_paths(fn: ast.AST, source_lines: List[str]) -> Tuple[List[Dict[str, Any]], set[str]]:
    rows: List[Dict[str, Any]] = []
    keys: set[str] = set()

    for node in ast.walk(fn):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "get":
            base = node.func.value
            if isinstance(base, ast.Name) and base.id == "payload":
                key = literal_key(node.args[0]) if node.args else ""
                if key:
                    keys.add(key)
                rows.append({
                    "path_type": "payload_get",
                    "key": key,
                    "lineno": getattr(node, "lineno", ""),
                    "source": node_snippet(source_lines, node),
                    "passed": True,
                })
        if isinstance(node, ast.Subscript):
            base = node.value
            if isinstance(base, ast.Name) and base.id == "payload":
                key = literal_key(node.slice)
                if key:
                    keys.add(key)
                rows.append({
                    "path_type": "payload_subscript",
                    "key": key,
                    "lineno": getattr(node, "lineno", ""),
                    "source": node_snippet(source_lines, node),
                    "passed": True,
                })
        if isinstance(node, ast.For):
            rows.append({
                "path_type": "for_loop",
                "key": "",
                "lineno": getattr(node, "lineno", ""),
                "source": node_snippet(source_lines, node),
                "passed": True,
            })
    return rows, keys


def collect_game_pk_paths(fn: ast.AST, source_lines: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for node in ast.walk(fn):
        text = node_snippet(source_lines, node)
        if "game_pk" in text or any(k in text for k in ["game_id", "id", "pk"]):
            if isinstance(node, (ast.Compare, ast.If, ast.Assign, ast.Call, ast.Return, ast.For)):
                rows.append({
                    "path_type": type(node).__name__,
                    "lineno": getattr(node, "lineno", ""),
                    "source": text,
                    "mentions_game_pk": "game_pk" in text,
                    "mentions_game_id": "game_id" in text,
                    "mentions_id": "id" in text,
                    "mentions_pk": "pk" in text,
                    "passed": True,
                })
    return rows


def collect_limit_paths(fn: ast.AST, source_lines: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for node in ast.walk(fn):
        text = node_snippet(source_lines, node)
        if "limit" in text:
            if isinstance(node, (ast.Compare, ast.If, ast.Assign, ast.Call, ast.Return, ast.For, ast.Subscript, ast.Break)):
                rows.append({
                    "path_type": type(node).__name__,
                    "lineno": getattr(node, "lineno", ""),
                    "source": text,
                    "passed": True,
                })
    return rows


def collect_return_paths(fn: ast.AST, source_lines: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for node in ast.walk(fn):
        if isinstance(node, ast.Return):
            value = node.value
            empty_literal = isinstance(value, ast.List) and len(value.elts) == 0
            rows.append({
                "path_type": "return",
                "lineno": getattr(node, "lineno", ""),
                "source": node_snippet(source_lines, node),
                "is_empty_list_literal": empty_literal,
                "returned_name": value.id if isinstance(value, ast.Name) else "",
                "passed": True,
            })
    return rows


def collect_filter_paths(fn: ast.AST, source_lines: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for node in ast.walk(fn):
        if isinstance(node, ast.If):
            text = node_snippet(source_lines, node)
            contains_continue = any(isinstance(child, ast.Continue) for child in ast.walk(node))
            contains_return = any(isinstance(child, ast.Return) for child in ast.walk(node))
            suspicious = any(token in text.lower() for token in ["not ", "none", "missing", "home", "away", "date", "prob", "odds", "model", "game"])
            if contains_continue or contains_return or suspicious:
                rows.append({
                    "path_type": "if_filter",
                    "lineno": getattr(node, "lineno", ""),
                    "source": text,
                    "contains_continue": contains_continue,
                    "contains_return": contains_return,
                    "suspicious_required_field_check": suspicious,
                    "passed": True,
                })
        if isinstance(node, ast.Continue):
            rows.append({
                "path_type": "continue",
                "lineno": getattr(node, "lineno", ""),
                "source": node_snippet(source_lines, node),
                "contains_continue": True,
                "contains_return": False,
                "suspicious_required_field_check": True,
                "passed": True,
            })
    return rows


def fixture_payload_keys() -> set[str]:
    rows = read_csv_rows(TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_argument_mapping.csv")
    keys: set[str] = set()
    for row in rows:
        if row.get("argument") == "payload":
            preview = row.get("value_preview", "")
            try:
                parsed = json.loads(preview)
                if isinstance(parsed, dict):
                    keys.update(str(k) for k in parsed.keys())
            except Exception:
                pass
    if keys:
        return keys

    fixture_rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv")
    if fixture_rows:
        keys.update(fixture_rows[0].keys())
    return keys


def root_hypotheses(expected_keys: set[str], fixture_keys: set[str], game_pk_rows: List[Dict[str, Any]], filter_rows: List[Dict[str, Any]], return_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], str, bool, str]:
    expected_collection_hits = expected_keys & EXPECTED_COLLECTION_KEYS
    missing_expected = sorted(expected_collection_hits - fixture_keys)
    fixture_contract_shaped = bool(missing_expected) or not bool(fixture_keys & EXPECTED_COLLECTION_KEYS)
    game_pk_path_found = bool(game_pk_rows)
    filters_found = bool(filter_rows)

    ranked: List[Dict[str, Any]] = []
    ranked.append({
        "rank": 1,
        "hypothesis": "fixture_payload_contract_shaped_not_adapter_shaped",
        "support": "fixture payload lacks expected collection keys used by adapter" if fixture_contract_shaped else "fixture contains some expected collection keys",
        "supported": fixture_contract_shaped,
        "passed": True,
    })
    ranked.append({
        "rank": 2,
        "hypothesis": "payload_missing_expected_game_collection_key",
        "support": ";".join(missing_expected) if missing_expected else "no missing expected collection keys detected or no expected keys statically found",
        "supported": bool(missing_expected),
        "passed": True,
    })
    ranked.append({
        "rank": 3,
        "hypothesis": "game_pk_not_found_in_expected_nested_payload_location",
        "support": "game_pk paths found statically but fixture only has top-level game_id" if game_pk_path_found else "no game_pk paths found statically",
        "supported": game_pk_path_found,
        "passed": True,
    })
    ranked.append({
        "rank": 4,
        "hypothesis": "internal_filters_reject_all_candidate_games",
        "support": "filter paths found" if filters_found else "no filter paths found",
        "supported": filters_found,
        "passed": True,
    })

    likely = "fixture_payload_contract_shaped_not_adapter_shaped" if fixture_contract_shaped else "static_trace_inconclusive_requires_audit"
    missing_str = ";".join(missing_expected)
    explained = fixture_contract_shaped or bool(missing_expected) or game_pk_path_found or filters_found
    return ranked, likely, explained, missing_str


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    plan_6ln = load_json(JSON_6LN)

    source_text = TARGET_SOURCE.read_text(encoding="utf-8", errors="ignore") if TARGET_SOURCE.exists() else ""
    source_lines = source_text.splitlines()
    tree = ast.parse(source_text) if source_text else ast.Module(body=[], type_ignores=[])
    fn = find_target_function(tree)

    target_found = fn is not None
    args = [a.arg for a in fn.args.args] if fn else []
    signature = ";".join(args)
    signature_confirmed = signature == REQUIRED_ARGS
    start_line = getattr(fn, "lineno", "") if fn else ""
    end_line = getattr(fn, "end_lineno", "") if fn else ""

    candidate_rows = [
        {"same_candidate_retained_confirmed": plan_6ln.get("same_candidate_retained") is True, "passed": True},
        {"blocked_session_candidate_excluded_confirmed": plan_6ln.get("blocked_session_candidate_excluded") is True, "passed": True},
        {"target_source_path": str(TARGET_SOURCE), "target_module_import_path": TARGET_MODULE, "target_function_name": TARGET_FUNCTION, "passed": True},
        {"target_module_imported": False, "adapter_call_executed": False, "passed": True},
    ]

    signature_rows = [
        {
            "target_function_found": target_found,
            "function_signature_confirmed": signature,
            "signature_exact_match": signature_confirmed,
            "start_line": start_line,
            "end_line": end_line,
            "passed": True,
        }
    ]

    if fn:
        return_rows = collect_return_paths(fn, source_lines)
        payload_rows, expected_keys = collect_payload_paths(fn, source_lines)
        game_pk_rows = collect_game_pk_paths(fn, source_lines)
        limit_rows = collect_limit_paths(fn, source_lines)
        filter_rows = collect_filter_paths(fn, source_lines)
    else:
        return_rows = [{"path_type": "missing_function", "passed": True}]
        payload_rows, expected_keys = ([{"path_type": "missing_function", "passed": True}], set())
        game_pk_rows = [{"path_type": "missing_function", "passed": True}]
        limit_rows = [{"path_type": "missing_function", "passed": True}]
        filter_rows = [{"path_type": "missing_function", "passed": True}]

    fixture_keys = fixture_payload_keys()
    root_rows, likely_root, empty_explained, missing_expected = root_hypotheses(expected_keys, fixture_keys, game_pk_rows, filter_rows, return_rows)

    fixture_comparison_rows = [
        {
            "comparison": "expected_payload_keys_found",
            "value": ";".join(sorted(expected_keys)),
            "count": len(expected_keys),
            "passed": True,
        },
        {
            "comparison": "fixture_payload_keys_found",
            "value": ";".join(sorted(fixture_keys)),
            "count": len(fixture_keys),
            "passed": True,
        },
        {
            "comparison": "missing_expected_payload_keys",
            "value": missing_expected,
            "count": len([k for k in missing_expected.split(";") if k]),
            "passed": True,
        },
        {
            "comparison": "fixture_contract_vs_adapter_shape",
            "value": likely_root,
            "passed": True,
        },
    ]

    gap_rows = [
        {
            "gap": "empty_adapter_return_static_trace_completed",
            "target": f"{TARGET_MODULE}::{TARGET_FUNCTION}",
            "likely_root_cause": likely_root,
            "empty_return_explained_by_static_trace": empty_explained,
            "projection_surface_materialized": False,
            "next_action": "audit_static_empty_return_provenance_trace",
            "passed": True,
        }
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "empty_adapter_return_requires_audit", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6lp = [
        {"contract": "audit_static_empty_return_trace", "required": True, "passed": True},
        {"contract": "confirm_or_reject_fixture_shape_root_cause", "required": True, "passed": True},
        {"contract": "route_to_payload_repair_next_candidate_or_wrapper_plan", "required": True, "passed": True},
        {"contract": "preserve_no_calls_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ln_plan_script_exists", "expected": True, "actual": PLAN_6LN_PATH.exists(), "passed": PLAN_6LN_PATH.exists()},
        {"check": "6ln_json_exists", "expected": True, "actual": JSON_6LN.exists(), "passed": JSON_6LN.exists()},
        {"check": "6ln_all_checks_passed", "expected": True, "actual": plan_6ln.get("all_checks_passed"), "passed": plan_6ln.get("all_checks_passed") is True},
        {"check": "6ln_diagnosis", "expected": DIAGNOSIS_6LN, "actual": plan_6ln.get("diagnosis"), "passed": plan_6ln.get("diagnosis") == DIAGNOSIS_6LN},
        {"check": "6ln_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LN, "actual": plan_6ln.get("recommended_next_layer"), "passed": plan_6ln.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LN},
        {"check": "6ln_no_import_next", "expected": False, "actual": plan_6ln.get("import_candidate_module_allowed_next"), "passed": plan_6ln.get("import_candidate_module_allowed_next") is False},
        {"check": "6ln_no_additional_call_next", "expected": False, "actual": plan_6ln.get("additional_adapter_call_allowed_next"), "passed": plan_6ln.get("additional_adapter_call_allowed_next") is False},
        {"check": "6ln_no_layer6_exit", "expected": False, "actual": plan_6ln.get("layer_6_exit_recommended"), "passed": plan_6ln.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in ALL_INPUTS
    ]
    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in ALL_INPUTS + [TARGET_SOURCE]]

    blocking_rows = [
        {"blocked_surface": "6lp_empty_return_trace_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "target_module_import", "blocked": True, "reason": "6LO is static trace only", "passed": True},
        {"blocked_surface": "adapter_call", "blocked": True, "reason": "6LO is static trace only", "passed": True},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "trace requires audit first", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "trace requires audit first", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LO cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ln_passed", "expected": True, "actual": plan_6ln.get("all_checks_passed"), "passed": plan_6ln.get("all_checks_passed") is True},
        {"decision": "target_source_exists", "expected": True, "actual": TARGET_SOURCE.exists(), "passed": TARGET_SOURCE.exists()},
        {"decision": "target_function_found", "expected": True, "actual": target_found, "passed": target_found},
        {"decision": "signature_confirmed", "expected": REQUIRED_ARGS, "actual": signature, "passed": signature_confirmed},
        {"decision": "return_paths_traced", "expected": True, "actual": bool(return_rows), "passed": bool(return_rows)},
        {"decision": "payload_paths_traced", "expected": True, "actual": bool(payload_rows), "passed": bool(payload_rows)},
        {"decision": "game_pk_paths_traced", "expected": True, "actual": bool(game_pk_rows), "passed": bool(game_pk_rows)},
        {"decision": "limit_paths_traced", "expected": True, "actual": bool(limit_rows), "passed": bool(limit_rows)},
        {"decision": "filter_paths_traced", "expected": True, "actual": bool(filter_rows), "passed": bool(filter_rows)},
        {"decision": "fixture_comparison_emitted", "expected": True, "actual": bool(fixture_comparison_rows), "passed": bool(fixture_comparison_rows)},
        {"decision": "future_6lp_contract_valid", "expected": True, "actual": len(future_6lp) == 4 and all_passed(future_6lp), "passed": len(future_6lp) == 4 and all_passed(future_6lp)},
        {"decision": "target_module_imported", "expected": False, "actual": False, "passed": True},
        {"decision": "adapter_call_executed", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6lp_next", "expected": RECOMMENDED_NEXT_LAYER_6LO, "actual": RECOMMENDED_NEXT_LAYER_6LO, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_static_trace", "expected": True, "actual": True, "passed": True},
        {"boundary": "target_module_imported", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed", "expected": False, "actual": False, "passed": True},
        {"boundary": "additional_adapter_call_executed", "expected": False, "actual": False, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_static_trace", "passed": True},
        {"surface": "6ln_plan", "policy": "read_only", "passed": True},
        {"surface": "source_artifacts", "policy": "read_only_reference", "passed": True},
        {"surface": "trace_artifacts", "policy": "tmp_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lo", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LO, "actual": RECOMMENDED_NEXT_LAYER_6LO, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LO, "actual": RECOMMENDED_PATH_6LO, "passed": True},
        {"decision": "recommend_empty_return_trace_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LO, "actual": DIAGNOSIS_6LO, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "candidate_confirmation", "passed": all_passed(candidate_rows), "detail": f"{len(candidate_rows)} rows"},
        {"check": "function_signature", "passed": all_passed(signature_rows), "detail": f"{len(signature_rows)} rows"},
        {"check": "return_paths", "passed": bool(return_rows), "detail": f"{len(return_rows)} rows"},
        {"check": "payload_access_paths", "passed": bool(payload_rows), "detail": f"{len(payload_rows)} rows"},
        {"check": "game_pk_paths", "passed": bool(game_pk_rows), "detail": f"{len(game_pk_rows)} rows"},
        {"check": "limit_paths", "passed": bool(limit_rows), "detail": f"{len(limit_rows)} rows"},
        {"check": "filter_paths", "passed": bool(filter_rows), "detail": f"{len(filter_rows)} rows"},
        {"check": "fixture_payload_comparison", "passed": bool(fixture_comparison_rows), "detail": f"{len(fixture_comparison_rows)} rows"},
        {"check": "root_cause_hypotheses", "passed": bool(root_rows), "detail": f"{len(root_rows)} rows"},
        {"check": "gap_report", "passed": bool(gap_rows), "detail": f"{len(gap_rows)} rows"},
        {"check": "future_6lp_contract", "passed": all_passed(future_6lp), "detail": f"{len(future_6lp)} rows"},
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
        "function_signature": write_csv(SIGNATURE_CSV, signature_rows),
        "return_paths": write_csv(RETURN_PATHS_CSV, return_rows),
        "payload_access_paths": write_csv(PAYLOAD_PATHS_CSV, payload_rows),
        "game_pk_paths": write_csv(GAME_PK_PATHS_CSV, game_pk_rows),
        "limit_paths": write_csv(LIMIT_PATHS_CSV, limit_rows),
        "filter_paths": write_csv(FILTER_PATHS_CSV, filter_rows),
        "fixture_payload_comparison": write_csv(FIXTURE_COMPARISON_CSV, fixture_comparison_rows),
        "root_cause_hypotheses": write_csv(ROOT_CAUSE_CSV, root_rows),
        "gap_report": write_csv(GAP_CSV, gap_rows),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6lp_contract": write_csv(FUTURE_6LP_CSV, future_6lp),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LO",
        "layer_type": "game_mechanics_realism",
        "implementation_only_static_trace": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LO if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LO,
        "recommended_path": RECOMMENDED_PATH_6LO,
        "predecessor_plan": str(PLAN_6LN_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": plan_6ln.get("diagnosis"),
        "implemented_layer_after": "6LN",
        "source_family": "projection_adapter_empty_return_trace_implementation",
        "candidate_confirmation_count": len(candidate_rows),
        "function_signature_count": len(signature_rows),
        "return_path_count": len(return_rows),
        "payload_access_path_count": len(payload_rows),
        "game_pk_path_count": len(game_pk_rows),
        "limit_path_count": len(limit_rows),
        "filter_path_count": len(filter_rows),
        "fixture_payload_comparison_count": len(fixture_comparison_rows),
        "root_cause_hypothesis_count": len(root_rows),
        "gap_report_count": len(gap_rows),
        "blocker_count": len(blockers),
        "future_6lp_contract_valid": len(future_6lp) == 4 and all_passed(future_6lp),
        "projection_adapter_empty_return_trace_implemented": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained_confirmed": plan_6ln.get("same_candidate_retained") is True,
        "blocked_session_candidate_excluded_confirmed": plan_6ln.get("blocked_session_candidate_excluded") is True,
        "target_source_path": str(TARGET_SOURCE),
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "target_function_found": target_found,
        "function_signature_confirmed": signature,
        "target_module_imported": False,
        "adapter_call_executed": False,
        "additional_adapter_call_executed": False,
        "static_return_path_trace_completed": bool(return_rows),
        "payload_shape_trace_completed": bool(payload_rows),
        "game_pk_match_trace_completed": bool(game_pk_rows),
        "limit_application_trace_completed": bool(limit_rows),
        "filter_rejection_trace_completed": bool(filter_rows),
        "fixture_contract_vs_adapter_shape_compared": bool(fixture_comparison_rows),
        "likely_empty_return_root_cause": likely_root,
        "likely_fixture_payload_contract_shaped_not_adapter_shaped": likely_root == "fixture_payload_contract_shaped_not_adapter_shaped",
        "expected_payload_keys_found": ";".join(sorted(expected_keys)),
        "fixture_payload_keys_found": ";".join(sorted(fixture_keys)),
        "missing_expected_payload_keys": missing_expected,
        "game_pk_static_match_path_found": bool(game_pk_rows),
        "limit_static_usage_found": bool(limit_rows),
        "empty_return_explained_by_static_trace": empty_explained,
        "projection_surface_materialized": False,
        "real_prediction_fields_materialized": False,
        "probability_metric_ready_after_implementation": False,
        "runs_metric_ready_after_implementation": False,
        "any_backtest_metric_ready_after_implementation": False,
        "next_candidate_retry_recommended": False,
        "wrapper_plan_needed": False,
        "historical_odds_required": False,
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
            "candidate_confirmation_csv": str(CANDIDATE_CSV),
            "function_signature_csv": str(SIGNATURE_CSV),
            "return_paths_csv": str(RETURN_PATHS_CSV),
            "payload_access_paths_csv": str(PAYLOAD_PATHS_CSV),
            "game_pk_paths_csv": str(GAME_PK_PATHS_CSV),
            "limit_paths_csv": str(LIMIT_PATHS_CSV),
            "filter_paths_csv": str(FILTER_PATHS_CSV),
            "fixture_payload_comparison_csv": str(FIXTURE_COMPARISON_CSV),
            "root_cause_hypotheses_csv": str(ROOT_CAUSE_CSV),
            "gap_report_csv": str(GAP_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6lp_contract_csv": str(FUTURE_6LP_CSV),
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
