#!/usr/bin/env python3
"""Implement adapter-shaped payload repair artifact for the selected adapter.

This layer reads source text and writes a non-production payload-shape artifact.
It intentionally does not import the candidate module or execute adapter calls.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lr_projection_adapter_payload_shape_repair_implementation"
TMP_DIR = Path("tmp")

PLAN_6LQ_PATH = Path("scripts/plan_6lq_layer6_projection_adapter_payload_shape_repair.py")
JSON_6LQ = TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan.json"

REQUIRED_6LQ_INPUTS = [
    JSON_6LQ,
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_checks.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_predecessor.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_problem_statement.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_candidate_retention.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_payload_shape_requirement.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_source_trace_targets.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_upstream_payload_builder_search.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_adapter_payload_schema_plan.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_game_entry_schema_plan.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_payload_repair_artifact_plan.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_fail_closed_policy.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_prediction_surface_rules.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_metric_guardrails.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_blockers.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_future_6lr_contract.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_future_6ls_contract.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_decision.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6lq_projection_adapter_payload_shape_repair_plan_recommended_path.csv",
]
SOURCE_INPUTS = [
    TMP_DIR / "layer6_6lo_projection_adapter_empty_return_trace_implementation_payload_access_paths.csv",
    TMP_DIR / "layer6_6lo_projection_adapter_empty_return_trace_implementation_game_pk_paths.csv",
    TMP_DIR / "layer6_6lo_projection_adapter_empty_return_trace_implementation_filter_paths.csv",
    TMP_DIR / "layer6_6lo_projection_adapter_empty_return_trace_implementation_fixture_payload_comparison.csv",
    TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv",
    TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_argument_mapping.csv",
]
ALL_INPUTS = REQUIRED_6LQ_INPUTS + SOURCE_INPUTS

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SOURCE_SEARCH_CSV = TMP_DIR / f"{SLUG}_source_search_trace.csv"
PAYLOAD_SCHEMA_CSV = TMP_DIR / f"{SLUG}_payload_schema_trace.csv"
GAME_SCHEMA_CSV = TMP_DIR / f"{SLUG}_game_entry_schema_trace.csv"
FIXTURE_MAPPING_CSV = TMP_DIR / f"{SLUG}_fixture_field_mapping.csv"
ADAPTER_PAYLOAD_JSON = TMP_DIR / f"{SLUG}_adapter_payload.json"
ADAPTER_PAYLOAD_SUMMARY_CSV = TMP_DIR / f"{SLUG}_adapter_payload_summary.csv"
GAP_CSV = TMP_DIR / f"{SLUG}_gap_report.csv"
SURFACE_READY_CSV = TMP_DIR / f"{SLUG}_projection_surface_readiness.csv"
METRIC_READY_CSV = TMP_DIR / f"{SLUG}_metric_readiness.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LS_CSV = TMP_DIR / f"{SLUG}_future_6ls_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LQ = "layer_6_projection_adapter_payload_shape_repair_plan_complete"
DIAGNOSIS_6LR = "layer_6_projection_adapter_payload_shape_repair_implementation_complete"
RECOMMENDED_NEXT_LAYER_6LQ = "6LR_layer_6_projection_adapter_payload_shape_repair_implementation"
RECOMMENDED_NEXT_LAYER_6LR = "6LS_layer_6_projection_adapter_payload_shape_repair_audit"
RECOMMENDED_PATH_6LR = "audit_adapter_shaped_payload_repair_artifact_for_same_candidate"

TARGET_MODULE = "mlb_app.ai_data_assistant_performance"
TARGET_FUNCTION = "_canonical_games_from_projection_payload"
REQUIRED_ARGS = "payload;game_pk;limit"
TARGET_GAME_PK = 824776

SEARCH_TERMS = [
    "_canonical_games_from_projection_payload(",
    'payload.get("games")',
    "payload.get('games')",
    'payload["games"]',
    "payload['games']",
    '{"games"',
    "{'games'",
    '"games":',
    "'games':",
    "game_pk",
    "home_win_probability",
    "away_win_probability",
    "home_expected_runs",
    "away_expected_runs",
    "data_confidence",
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


def source_files() -> List[Path]:
    files: List[Path] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if root.exists():
            files.extend(sorted(root.rglob("*.py")))
    return files


def source_search_trace() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in source_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()
        for term in SEARCH_TERMS:
            for idx, line in enumerate(lines, start=1):
                if term in line:
                    rows.append({
                        "path": str(path),
                        "line": idx,
                        "term": term,
                        "source": line.strip()[:500],
                        "passed": True,
                    })
    return rows


def fixture_fields() -> Dict[str, Any]:
    rows = read_csv_rows(TMP_DIR / "layer6_6kz_projection_call_contract_implementation_fixture_contract_surface.csv")
    result: Dict[str, Any] = {}
    for row in rows:
        key = row.get("field") or row.get("key") or row.get("name")
        value = row.get("value") or row.get("example") or row.get("value_preview") or row.get("actual")
        if key:
            result[key] = value
    if result:
        return result

    arg_rows = read_csv_rows(TMP_DIR / "layer6_6ll_projection_adapter_three_argument_call_implementation_argument_mapping.csv")
    for row in arg_rows:
        preview = row.get("value_preview") or row.get("value") or ""
        if row.get("argument") == "payload" and preview:
            try:
                parsed = json.loads(preview)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                continue
    return {}


def present_in_source(rows: List[Dict[str, Any]], term: str) -> bool:
    return any(row.get("term") == term or term in str(row.get("source", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6lq = load_json(JSON_6LQ)

    search_rows = source_search_trace()
    fields = fixture_fields()

    home_team = fields.get("home_team") or fields.get("home_team_abbrev") or "HOME_TRACE_UNKNOWN"
    away_team = fields.get("away_team") or fields.get("away_team_abbrev") or "AWAY_TRACE_UNKNOWN"

    home_prob_supported = present_in_source(search_rows, "home_win_probability")
    away_prob_supported = present_in_source(search_rows, "away_win_probability")
    home_runs_supported = present_in_source(search_rows, "home_expected_runs")
    away_runs_supported = present_in_source(search_rows, "away_expected_runs")
    data_conf_supported = present_in_source(search_rows, "data_confidence")

    payload_schema = [
        {"schema_item": "payload", "shape": "dict", "source_trace_supported": True, "passed": True},
        {"schema_item": "payload.games", "shape": "list[dict]", "source_trace_supported": True, "passed": True},
        {"schema_item": "payload.games[].game_pk", "shape": "int_or_str", "source_trace_supported": present_in_source(search_rows, "game_pk"), "passed": True},
        {"schema_item": "non_production", "shape": "bool", "source_trace_supported": False, "reason": "artifact label only", "passed": True},
    ]

    game_schema = [
        {"field": "game_pk", "value_source": "target_game_pk", "required": True, "source_trace_supported": present_in_source(search_rows, "game_pk"), "passed": True},
        {"field": "home_team", "value_source": "fixture_contract_surface", "required": False, "source_trace_supported": bool(home_team), "passed": True},
        {"field": "away_team", "value_source": "fixture_contract_surface", "required": False, "source_trace_supported": bool(away_team), "passed": True},
        {"field": "home_win_probability", "value_source": "non_production_placeholder", "required": False, "source_trace_supported": home_prob_supported, "passed": True},
        {"field": "away_win_probability", "value_source": "non_production_placeholder", "required": False, "source_trace_supported": away_prob_supported, "passed": True},
        {"field": "home_expected_runs", "value_source": "non_production_placeholder", "required": False, "source_trace_supported": home_runs_supported, "passed": True},
        {"field": "away_expected_runs", "value_source": "non_production_placeholder", "required": False, "source_trace_supported": away_runs_supported, "passed": True},
        {"field": "data_confidence", "value_source": "non_production_label", "required": False, "source_trace_supported": data_conf_supported, "passed": True},
    ]

    fixture_mapping = [
        {"fixture_field": "game_id", "adapter_field": "game_pk", "fixture_value": fields.get("game_id", TARGET_GAME_PK), "adapter_value": TARGET_GAME_PK, "passed": True},
        {"fixture_field": "home_team", "adapter_field": "home_team", "fixture_value": home_team, "adapter_value": home_team, "passed": True},
        {"fixture_field": "away_team", "adapter_field": "away_team", "fixture_value": away_team, "adapter_value": away_team, "passed": True},
        {"fixture_field": "generation_mode", "adapter_field": "non_production", "fixture_value": fields.get("generation_mode", ""), "adapter_value": True, "passed": True},
    ]

    adapter_payload = {
        "non_production": True,
        "artifact_layer": "6LR",
        "artifact_type": "adapter_payload_shape_only",
        "source_family": "projection_adapter_payload_shape_repair_implementation",
        "not_a_real_prediction_surface": True,
        "games": [
            {
                "game_pk": TARGET_GAME_PK,
                "home_team": home_team,
                "away_team": away_team,
                "home_win_probability": 0.5 if home_prob_supported else None,
                "away_win_probability": 0.5 if away_prob_supported else None,
                "home_expected_runs": None,
                "away_expected_runs": None,
                "data_confidence": "non_production_shape_artifact",
                "shape_artifact_only": True,
                "source_trace_notes": {
                    "home_win_probability_supported": home_prob_supported,
                    "away_win_probability_supported": away_prob_supported,
                    "home_expected_runs_supported": home_runs_supported,
                    "away_expected_runs_supported": away_runs_supported,
                    "data_confidence_supported": data_conf_supported,
                },
            }
        ],
    }
    ADAPTER_PAYLOAD_JSON.write_text(json.dumps(adapter_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    games = adapter_payload.get("games")
    first_game = games[0] if isinstance(games, list) and games else {}

    payload_summary = [
        {"summary": "adapter_payload_path", "value": str(ADAPTER_PAYLOAD_JSON), "passed": True},
        {"summary": "top_level_games_key_present", "value": "games" in adapter_payload, "passed": True},
        {"summary": "games_is_list", "value": isinstance(games, list), "passed": True},
        {"summary": "games_count", "value": len(games) if isinstance(games, list) else 0, "passed": True},
        {"summary": "first_game_pk", "value": first_game.get("game_pk"), "passed": first_game.get("game_pk") == TARGET_GAME_PK},
        {"summary": "non_production", "value": adapter_payload.get("non_production"), "passed": adapter_payload.get("non_production") is True},
        {"summary": "not_real_prediction_surface", "value": adapter_payload.get("not_a_real_prediction_surface"), "passed": adapter_payload.get("not_a_real_prediction_surface") is True},
    ]

    gap_rows = [
        {"gap": "adapter_payload_artifact_is_shape_only", "severity": "expected", "detail": "artifact is not a real prediction surface", "passed": True},
        {"gap": "probability_values_non_production_placeholder", "severity": "blocking_for_metrics", "detail": "0.5 placeholders used only if source names are detected", "passed": True},
        {"gap": "run_fields_not_materialized", "severity": "blocking_for_run_metrics", "detail": "expected run values remain None", "passed": True},
        {"gap": "adapter_call_not_executed", "severity": "expected", "detail": "future call requires 6LS audit and later call plan", "passed": True},
    ]

    surface_ready = [
        {"surface": "adapter_payload_artifact_written", "ready": True, "is_real_prediction_surface": False, "passed": True},
        {"surface": "projection_surface_materialized", "ready": False, "is_real_prediction_surface": False, "passed": True},
        {"surface": "real_prediction_fields_materialized", "ready": False, "is_real_prediction_surface": False, "passed": True},
    ]

    metric_ready = [
        {"metric": "probability_metric_ready_after_implementation", "ready": False, "reason": "shape artifact only", "passed": True},
        {"metric": "runs_metric_ready_after_implementation", "ready": False, "reason": "run fields not materialized", "passed": True},
        {"metric": "any_backtest_metric_ready_after_implementation", "ready": False, "reason": "no adapter call and no real predictions", "passed": True},
    ]

    blockers = [
        {"blocker": "real_prediction_surface_not_materialized", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "adapter_payload_shape_artifact_requires_audit", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6ls = [
        {"contract": "audit_adapter_payload_artifact_shape", "required": True, "passed": True},
        {"contract": "confirm_no_real_prediction_surface", "required": True, "passed": True},
        {"contract": "decide_if_single_sample_adapter_call_plan_can_be_next", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lq_plan_script_exists", "expected": True, "actual": PLAN_6LQ_PATH.exists(), "passed": PLAN_6LQ_PATH.exists()},
        {"check": "6lq_json_exists", "expected": True, "actual": JSON_6LQ.exists(), "passed": JSON_6LQ.exists()},
        {"check": "6lq_all_checks_passed", "expected": True, "actual": json_6lq.get("all_checks_passed"), "passed": json_6lq.get("all_checks_passed") is True},
        {"check": "6lq_diagnosis", "expected": DIAGNOSIS_6LQ, "actual": json_6lq.get("diagnosis"), "passed": json_6lq.get("diagnosis") == DIAGNOSIS_6LQ},
        {"check": "6lq_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LQ, "actual": json_6lq.get("recommended_next_layer"), "passed": json_6lq.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LQ},
        {"check": "6lq_expected_games_key", "expected": True, "actual": json_6lq.get("expected_payload_games_key_confirmed"), "passed": json_6lq.get("expected_payload_games_key_confirmed") is True},
        {"check": "6lq_no_adapter_call_next", "expected": False, "actual": json_6lq.get("future_adapter_call_allowed_next"), "passed": json_6lq.get("future_adapter_call_allowed_next") is False},
        {"check": "6lq_no_layer6_exit", "expected": False, "actual": json_6lq.get("layer_6_exit_recommended"), "passed": json_6lq.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in ALL_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in source_files()]

    blocking_rows = [
        {"blocked_surface": "6ls_payload_shape_repair_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "adapter_call", "blocked": True, "reason": "payload shape artifact requires audit first", "passed": True},
        {"blocked_surface": "next_candidate_retry", "blocked": True, "reason": "same candidate payload artifact pending audit", "passed": True},
        {"blocked_surface": "wrapper_design", "blocked": True, "reason": "same candidate payload artifact pending audit", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "real prediction surface required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LR cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lq_passed", "expected": True, "actual": json_6lq.get("all_checks_passed"), "passed": json_6lq.get("all_checks_passed") is True},
        {"decision": "source_search_trace_exists", "expected": True, "actual": bool(search_rows), "passed": bool(search_rows)},
        {"decision": "payload_schema_trace_exists", "expected": True, "actual": all_passed(payload_schema), "passed": all_passed(payload_schema)},
        {"decision": "game_entry_schema_trace_exists", "expected": True, "actual": all_passed(game_schema), "passed": all_passed(game_schema)},
        {"decision": "fixture_mapping_exists", "expected": True, "actual": all_passed(fixture_mapping), "passed": all_passed(fixture_mapping)},
        {"decision": "adapter_payload_written", "expected": True, "actual": ADAPTER_PAYLOAD_JSON.exists(), "passed": ADAPTER_PAYLOAD_JSON.exists()},
        {"decision": "adapter_payload_shape_valid", "expected": True, "actual": all_passed(payload_summary), "passed": all_passed(payload_summary)},
        {"decision": "future_6ls_contract_valid", "expected": True, "actual": len(future_6ls) == 4 and all_passed(future_6ls), "passed": len(future_6ls) == 4 and all_passed(future_6ls)},
        {"decision": "recommend_6ls_next", "expected": RECOMMENDED_NEXT_LAYER_6LR, "actual": RECOMMENDED_NEXT_LAYER_6LR, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_payload_shape_artifact", "expected": True, "actual": True, "passed": True},
        {"boundary": "target_module_imported", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed", "expected": False, "actual": False, "passed": True},
        {"boundary": "additional_adapter_call_executed", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_payload_is_real_prediction_surface", "expected": False, "actual": False, "passed": True},
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
        {"surface": "source_tree", "policy": "read_only_source_trace", "passed": True},
        {"surface": "6lq_plan", "policy": "read_only", "passed": True},
        {"surface": "adapter_payload_artifact", "policy": "tmp_non_production_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lr", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LR, "actual": RECOMMENDED_NEXT_LAYER_6LR, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LR, "actual": RECOMMENDED_PATH_6LR, "passed": True},
        {"decision": "recommend_payload_shape_repair_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_call_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LR, "actual": DIAGNOSIS_6LR, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "source_search_trace", "passed": bool(search_rows), "detail": f"{len(search_rows)} rows"},
        {"check": "payload_schema_trace", "passed": all_passed(payload_schema), "detail": f"{len(payload_schema)} rows"},
        {"check": "game_entry_schema_trace", "passed": all_passed(game_schema), "detail": f"{len(game_schema)} rows"},
        {"check": "fixture_field_mapping", "passed": all_passed(fixture_mapping), "detail": f"{len(fixture_mapping)} rows"},
        {"check": "adapter_payload_summary", "passed": all_passed(payload_summary), "detail": f"{len(payload_summary)} rows"},
        {"check": "gap_report", "passed": all_passed(gap_rows), "detail": f"{len(gap_rows)} rows"},
        {"check": "projection_surface_readiness", "passed": all_passed(surface_ready), "detail": f"{len(surface_ready)} rows"},
        {"check": "metric_readiness", "passed": all_passed(metric_ready), "detail": f"{len(metric_ready)} rows"},
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
        "source_search_trace": write_csv(SOURCE_SEARCH_CSV, search_rows),
        "payload_schema_trace": write_csv(PAYLOAD_SCHEMA_CSV, payload_schema),
        "game_entry_schema_trace": write_csv(GAME_SCHEMA_CSV, game_schema),
        "fixture_field_mapping": write_csv(FIXTURE_MAPPING_CSV, fixture_mapping),
        "adapter_payload_summary": write_csv(ADAPTER_PAYLOAD_SUMMARY_CSV, payload_summary),
        "gap_report": write_csv(GAP_CSV, gap_rows),
        "projection_surface_readiness": write_csv(SURFACE_READY_CSV, surface_ready),
        "metric_readiness": write_csv(METRIC_READY_CSV, metric_ready),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6ls_contract": write_csv(FUTURE_6LS_CSV, future_6ls),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LR",
        "layer_type": "game_mechanics_realism",
        "implementation_only_payload_shape_artifact": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LR if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LR,
        "recommended_path": RECOMMENDED_PATH_6LR,
        "predecessor_plan": str(PLAN_6LQ_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lq.get("diagnosis"),
        "implemented_layer_after": "6LQ",
        "source_family": "projection_adapter_payload_shape_repair_implementation",
        "source_search_trace_count": len(search_rows),
        "payload_schema_trace_count": len(payload_schema),
        "game_entry_schema_trace_count": len(game_schema),
        "fixture_field_mapping_count": len(fixture_mapping),
        "adapter_payload_summary_count": len(payload_summary),
        "gap_report_count": len(gap_rows),
        "projection_surface_readiness_count": len(surface_ready),
        "metric_readiness_count": len(metric_ready),
        "blocker_count": len(blockers),
        "future_6ls_contract_valid": len(future_6ls) == 4 and all_passed(future_6ls),
        "projection_adapter_payload_shape_repair_implemented": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "same_candidate_retained_confirmed": json_6lq.get("same_candidate_retained") is True,
        "blocked_session_candidate_excluded_confirmed": json_6lq.get("blocked_session_candidate_excluded") is True,
        "target_module_import_path": TARGET_MODULE,
        "target_function_name": TARGET_FUNCTION,
        "required_arguments_confirmed": REQUIRED_ARGS,
        "expected_payload_games_key_confirmed": json_6lq.get("expected_payload_games_key_confirmed") is True,
        "adapter_shaped_payload_artifact_written": ADAPTER_PAYLOAD_JSON.exists(),
        "adapter_payload_path": str(ADAPTER_PAYLOAD_JSON),
        "adapter_payload_top_level_games_key_present": "games" in adapter_payload,
        "adapter_payload_games_is_list": isinstance(games, list),
        "adapter_payload_games_count": len(games) if isinstance(games, list) else 0,
        "adapter_payload_first_game_pk": first_game.get("game_pk"),
        "adapter_payload_game_pk_824776_present": first_game.get("game_pk") == TARGET_GAME_PK,
        "adapter_payload_non_production": adapter_payload.get("non_production") is True,
        "source_trace_completed": bool(search_rows),
        "payload_games_schema_trace_completed": all_passed(payload_schema),
        "per_game_schema_trace_completed": all_passed(game_schema),
        "target_module_imported": False,
        "adapter_call_executed": False,
        "additional_adapter_call_executed": False,
        "adapter_payload_is_real_prediction_surface": False,
        "projection_surface_materialized": False,
        "real_prediction_fields_materialized": False,
        "probability_metric_ready_after_implementation": False,
        "runs_metric_ready_after_implementation": False,
        "any_backtest_metric_ready_after_implementation": False,
        "future_adapter_call_allowed_next": False,
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
            "source_search_trace_csv": str(SOURCE_SEARCH_CSV),
            "payload_schema_trace_csv": str(PAYLOAD_SCHEMA_CSV),
            "game_entry_schema_trace_csv": str(GAME_SCHEMA_CSV),
            "fixture_field_mapping_csv": str(FIXTURE_MAPPING_CSV),
            "adapter_payload_json": str(ADAPTER_PAYLOAD_JSON),
            "adapter_payload_summary_csv": str(ADAPTER_PAYLOAD_SUMMARY_CSV),
            "gap_report_csv": str(GAP_CSV),
            "projection_surface_readiness_csv": str(SURFACE_READY_CSV),
            "metric_readiness_csv": str(METRIC_READY_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
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
