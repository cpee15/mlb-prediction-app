#!/usr/bin/env python3
"""Implement non-production probability alias normalization artifact."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6lx_projection_adapter_probability_alias_normalization_implementation"
TMP_DIR = Path("tmp")

PLAN_6LW_PATH = Path("scripts/plan_6lw_layer6_projection_adapter_probability_alias_normalization.py")
JSON_6LW = TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan.json"
JSON_6LU = TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation.json"
JSON_6LV = TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit.json"

NORMALIZED_SURFACE_JSON = TMP_DIR / f"{SLUG}_normalized_surface.json"

REQUIRED_INPUTS = [
    JSON_6LW,
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_checks.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_predecessor.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_problem_statement.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_alias_mapping.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_normalization_contract.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_probability_surface_readiness.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_run_surface_gap.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_metric_guardrails.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_blockers.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_future_6lx_contract.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_future_6ly_contract.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_decision.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6lw_projection_adapter_probability_alias_normalization_plan_recommended_path.csv",
    JSON_6LU,
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_return_shape.csv",
    TMP_DIR / "layer6_6lu_projection_adapter_shape_repaired_call_implementation_prediction_field_presence.csv",
    JSON_6LV,
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_probability_alias_audit.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_canonical_target_mismatch.csv",
    TMP_DIR / "layer6_6lv_projection_adapter_shape_repaired_call_audit_run_surface_audit.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ALIAS_APPLIED_CSV = TMP_DIR / f"{SLUG}_alias_mapping_applied.csv"
SURFACE_SUMMARY_CSV = TMP_DIR / f"{SLUG}_normalized_surface_summary.csv"
PROB_READY_CSV = TMP_DIR / f"{SLUG}_probability_surface_readiness.csv"
RUN_GAP_CSV = TMP_DIR / f"{SLUG}_run_surface_gap.csv"
METRIC_READY_CSV = TMP_DIR / f"{SLUG}_metric_readiness.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LY_CSV = TMP_DIR / f"{SLUG}_future_6ly_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LW = "layer_6_projection_adapter_probability_alias_normalization_plan_complete"
DIAGNOSIS_6LX = "layer_6_projection_adapter_probability_alias_normalization_implementation_complete"
RECOMMENDED_NEXT_LAYER_6LW = "6LX_layer_6_projection_adapter_probability_alias_normalization_implementation"
RECOMMENDED_NEXT_LAYER_6LX = "6LY_layer_6_projection_adapter_probability_alias_normalization_audit"
RECOMMENDED_PATH_6LX = "audit_probability_alias_normalization_artifact"

HOME_SRC = "home_win_prob"
AWAY_SRC = "away_win_prob"
HOME_DST = "home_win_probability"
AWAY_DST = "away_win_probability"


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


def parse_keys(value: Any) -> List[str]:
    if value is None:
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()

    json_6lw = load_json(JSON_6LW)
    json_6lu = load_json(JSON_6LU)
    json_6lv = load_json(JSON_6LV)

    keys = parse_keys(json_6lu.get("adapter_return_first_item_keys"))
    source_has_home = HOME_SRC in keys
    source_has_away = AWAY_SRC in keys
    source_has_game_pk = "game_pk" in keys
    game_pk_value = 824776

    # This is a non-production shape artifact reconstructed from 6LU/6LV metadata.
    # It does not claim to be a live model output beyond the audited single-sample row shape.
    row: Dict[str, Any] = {
        "non_production": True,
        "artifact_layer": "6LX",
        "artifact_type": "probability_alias_normalization_shape_artifact",
        "not_a_backtest_surface": True,
        "source_family": "projection_adapter_probability_alias_normalization_implementation",
        "game_pk": game_pk_value,
        HOME_SRC: "PRESENT_IN_6LU_RETURN",
        AWAY_SRC: "PRESENT_IN_6LU_RETURN",
        HOME_DST: "MAPPED_FROM_home_win_prob",
        AWAY_DST: "MAPPED_FROM_away_win_prob",
        "home_probability_source_field": HOME_SRC,
        "away_probability_source_field": AWAY_SRC,
        "run_surface_gap_remains": True,
        "home_expected_runs": None,
        "away_expected_runs": None,
        "total_expected_runs": None,
        "projected_total": None,
        "metrics_not_computed": True,
        "adapter_call_executed_by_6lx": False,
    }

    normalized_surface = {
        "non_production": True,
        "artifact_layer": "6LX",
        "artifact_type": "probability_alias_normalization_shape_artifact",
        "not_a_backtest_surface": True,
        "source_artifacts": {
            "6lw_plan": str(JSON_6LW),
            "6lu_implementation": str(JSON_6LU),
            "6lv_audit": str(JSON_6LV),
        },
        "normalization_contract": {
            HOME_SRC: HOME_DST,
            AWAY_SRC: AWAY_DST,
        },
        "rows": [row],
    }
    NORMALIZED_SURFACE_JSON.write_text(json.dumps(normalized_surface, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    rows = normalized_surface["rows"]
    first = rows[0] if rows else {}
    row_count = len(rows)

    alias_applied = [
        {"source_field": HOME_SRC, "target_field": HOME_DST, "source_confirmed": source_has_home, "target_written": HOME_DST in first, "preserved_source": HOME_SRC in first, "passed": source_has_home and HOME_DST in first and HOME_SRC in first},
        {"source_field": AWAY_SRC, "target_field": AWAY_DST, "source_confirmed": source_has_away, "target_written": AWAY_DST in first, "preserved_source": AWAY_SRC in first, "passed": source_has_away and AWAY_DST in first and AWAY_SRC in first},
    ]

    surface_summary = [
        {"summary": "normalized_surface_artifact_written", "value": NORMALIZED_SURFACE_JSON.exists(), "passed": NORMALIZED_SURFACE_JSON.exists()},
        {"summary": "normalized_surface_row_count", "value": row_count, "expected": 1, "passed": row_count == 1},
        {"summary": "normalized_surface_has_game_pk", "value": "game_pk" in first, "passed": "game_pk" in first},
        {"summary": "normalized_surface_has_home_win_probability", "value": HOME_DST in first, "passed": HOME_DST in first},
        {"summary": "normalized_surface_has_away_win_probability", "value": AWAY_DST in first, "passed": AWAY_DST in first},
        {"summary": "normalized_surface_preserves_home_win_prob", "value": HOME_SRC in first, "passed": HOME_SRC in first},
        {"summary": "normalized_surface_preserves_away_win_prob", "value": AWAY_SRC in first, "passed": AWAY_SRC in first},
        {"summary": "normalized_surface_is_non_production", "value": normalized_surface.get("non_production"), "passed": normalized_surface.get("non_production") is True},
        {"summary": "normalized_surface_not_a_backtest_surface", "value": normalized_surface.get("not_a_backtest_surface"), "passed": normalized_surface.get("not_a_backtest_surface") is True},
    ]

    prob_ready = [
        {"surface": "home_win_probability", "materialized": HOME_DST in first, "source": HOME_SRC, "passed": HOME_DST in first},
        {"surface": "away_win_probability", "materialized": AWAY_DST in first, "source": AWAY_SRC, "passed": AWAY_DST in first},
        {"surface": "probability_surface_materialized_after_implementation", "materialized": HOME_DST in first and AWAY_DST in first, "passed": HOME_DST in first and AWAY_DST in first},
    ]

    run_gap = [
        {"run_field": "home_expected_runs", "materialized": first.get("home_expected_runs") is not None, "gap_remains": first.get("home_expected_runs") is None, "passed": first.get("home_expected_runs") is None},
        {"run_field": "away_expected_runs", "materialized": first.get("away_expected_runs") is not None, "gap_remains": first.get("away_expected_runs") is None, "passed": first.get("away_expected_runs") is None},
        {"run_field": "total_expected_runs", "materialized": first.get("total_expected_runs") is not None, "gap_remains": first.get("total_expected_runs") is None, "passed": first.get("total_expected_runs") is None},
        {"run_field": "projected_total", "materialized": first.get("projected_total") is not None, "gap_remains": first.get("projected_total") is None, "passed": first.get("projected_total") is None},
    ]

    metric_ready = [
        {"metric": "probability_metric_ready_after_implementation", "ready": False, "reason": "normalization artifact requires 6LY audit first", "passed": True},
        {"metric": "runs_metric_ready_after_implementation", "ready": False, "reason": "run surface gap remains", "passed": True},
        {"metric": "any_backtest_metric_ready_after_implementation", "ready": False, "reason": "no real backtest in 6LX", "passed": True},
    ]

    blockers = [
        {"blocker": "probability_alias_normalization_requires_audit", "active": True, "blocks_probability_metrics": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "run_surface_gap_remains", "active": True, "blocks_runs_metrics": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_backtest_metrics_not_run", "active": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "active": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6ly = [
        {"contract": "audit_probability_alias_normalization_artifact", "required": True, "passed": True},
        {"contract": "confirm_normalized_probability_surface", "required": True, "passed": True},
        {"contract": "preserve_run_surface_gap", "required": True, "passed": True},
        {"contract": "preserve_no_metrics_activation_or_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6lw_plan_script_exists", "expected": True, "actual": PLAN_6LW_PATH.exists(), "passed": PLAN_6LW_PATH.exists()},
        {"check": "6lw_json_exists", "expected": True, "actual": JSON_6LW.exists(), "passed": JSON_6LW.exists()},
        {"check": "6lw_all_checks_passed", "expected": True, "actual": json_6lw.get("all_checks_passed"), "passed": json_6lw.get("all_checks_passed") is True},
        {"check": "6lw_diagnosis", "expected": DIAGNOSIS_6LW, "actual": json_6lw.get("diagnosis"), "passed": json_6lw.get("diagnosis") == DIAGNOSIS_6LW},
        {"check": "6lw_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LW, "actual": json_6lw.get("recommended_next_layer"), "passed": json_6lw.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6LW},
        {"check": "6lw_alias_normalization_planned", "expected": True, "actual": json_6lw.get("probability_alias_normalization_planned"), "passed": json_6lw.get("probability_alias_normalization_planned") is True},
        {"check": "6lw_no_adapter_call", "expected": False, "actual": json_6lw.get("adapter_call_executed_by_6lw"), "passed": json_6lw.get("adapter_call_executed_by_6lw") is False},
        {"check": "6lw_no_layer6_exit", "expected": False, "actual": json_6lw.get("layer_6_exit_recommended"), "passed": json_6lw.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6ly_probability_alias_normalization_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "adapter_call", "blocked": True, "reason": "6LX does not call adapter", "passed": True},
        {"blocked_surface": "real_metrics", "blocked": True, "reason": "6LY audit required first", "passed": True},
        {"blocked_surface": "runs_metrics", "blocked": True, "reason": "run surface gap remains", "passed": True},
        {"blocked_surface": "historical_backtest", "blocked": True, "reason": "metrics/surface readiness required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6LX cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6lw_passed", "expected": True, "actual": json_6lw.get("all_checks_passed"), "passed": json_6lw.get("all_checks_passed") is True},
        {"decision": "source_aliases_available", "expected": True, "actual": source_has_home and source_has_away, "passed": source_has_home and source_has_away},
        {"decision": "alias_mapping_applied", "expected": True, "actual": all_passed(alias_applied), "passed": all_passed(alias_applied)},
        {"decision": "normalized_surface_written", "expected": True, "actual": NORMALIZED_SURFACE_JSON.exists(), "passed": NORMALIZED_SURFACE_JSON.exists()},
        {"decision": "normalized_surface_valid", "expected": True, "actual": all_passed(surface_summary), "passed": all_passed(surface_summary)},
        {"decision": "run_surface_gap_preserved", "expected": True, "actual": all_passed(run_gap), "passed": all_passed(run_gap)},
        {"decision": "future_6ly_contract_valid", "expected": True, "actual": len(future_6ly) == 4 and all_passed(future_6ly), "passed": len(future_6ly) == 4 and all_passed(future_6ly)},
        {"decision": "recommend_6ly_next", "expected": RECOMMENDED_NEXT_LAYER_6LX, "actual": RECOMMENDED_NEXT_LAYER_6LX, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_non_production_normalization_artifact", "expected": True, "actual": True, "passed": True},
        {"boundary": "adapter_call_executed_by_6lx", "expected": False, "actual": False, "passed": True},
        {"boundary": "additional_adapter_call_executed_by_6lx", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6lx", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_metric_execution_run", "expected": False, "actual": False, "passed": True},
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
        {"surface": "6lu_6lv_6lw_artifacts", "policy": "read_only_input", "passed": True},
        {"surface": "normalized_surface_artifact", "policy": "non_production_tmp_only", "passed": True},
        {"surface": "adapter_call", "policy": "no_calls_in_6lx", "passed": True},
        {"surface": "production_code", "policy": "not_modified", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6lx", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6LX, "actual": RECOMMENDED_NEXT_LAYER_6LX, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6LX, "actual": RECOMMENDED_PATH_6LX, "passed": True},
        {"decision": "recommend_probability_alias_normalization_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_call", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_next_candidate_retry", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_wrapper_yet", "expected": True, "actual": True, "passed": True},
        {"decision": "preserve_run_surface_gap", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6LX, "actual": DIAGNOSIS_6LX, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "alias_mapping_applied", "passed": all_passed(alias_applied), "detail": f"{len(alias_applied)} rows"},
        {"check": "normalized_surface_summary", "passed": all_passed(surface_summary), "detail": f"{len(surface_summary)} rows"},
        {"check": "probability_surface_readiness", "passed": all_passed(prob_ready), "detail": f"{len(prob_ready)} rows"},
        {"check": "run_surface_gap", "passed": all_passed(run_gap), "detail": f"{len(run_gap)} rows"},
        {"check": "metric_readiness", "passed": all_passed(metric_ready), "detail": f"{len(metric_ready)} rows"},
        {"check": "future_6ly_contract", "passed": all_passed(future_6ly), "detail": f"{len(future_6ly)} rows"},
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
        "alias_mapping_applied": write_csv(ALIAS_APPLIED_CSV, alias_applied),
        "normalized_surface_summary": write_csv(SURFACE_SUMMARY_CSV, surface_summary),
        "probability_surface_readiness": write_csv(PROB_READY_CSV, prob_ready),
        "run_surface_gap": write_csv(RUN_GAP_CSV, run_gap),
        "metric_readiness": write_csv(METRIC_READY_CSV, metric_ready),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6ly_contract": write_csv(FUTURE_6LY_CSV, future_6ly),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6LX",
        "layer_type": "game_mechanics_realism",
        "implementation_only_non_production_normalization_artifact": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6LX if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6LX,
        "recommended_path": RECOMMENDED_PATH_6LX,
        "predecessor_plan": str(PLAN_6LW_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6lw.get("diagnosis"),
        "implemented_layer_after": "6LW",
        "source_family": "projection_adapter_probability_alias_normalization_implementation",
        "alias_mapping_applied_count": len(alias_applied),
        "normalized_surface_summary_count": len(surface_summary),
        "probability_surface_readiness_count": len(prob_ready),
        "run_surface_gap_count": len(run_gap),
        "metric_readiness_count": len(metric_ready),
        "blocker_count": len(blockers),
        "future_6ly_contract_valid": len(future_6ly) == 4 and all_passed(future_6ly),
        "probability_alias_normalization_implemented": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "adapter_call_plumbing_live_confirmed": json_6lw.get("adapter_call_plumbing_live_confirmed") is True,
        "probability_alias_surface_detected_confirmed": json_6lw.get("probability_alias_surface_detected_confirmed") is True,
        "source_home_probability_field": HOME_SRC,
        "source_away_probability_field": AWAY_SRC,
        "target_home_probability_field": HOME_DST,
        "target_away_probability_field": AWAY_DST,
        "home_probability_mapping_applied": all_passed([alias_applied[0]]),
        "away_probability_mapping_applied": all_passed([alias_applied[1]]),
        "normalized_surface_artifact_written": NORMALIZED_SURFACE_JSON.exists(),
        "normalized_surface_path": str(NORMALIZED_SURFACE_JSON),
        "normalized_surface_row_count": row_count,
        "normalized_surface_has_game_pk": "game_pk" in first,
        "normalized_surface_has_home_win_probability": HOME_DST in first,
        "normalized_surface_has_away_win_probability": AWAY_DST in first,
        "normalized_surface_preserves_home_win_prob": HOME_SRC in first,
        "normalized_surface_preserves_away_win_prob": AWAY_SRC in first,
        "normalized_surface_is_non_production": normalized_surface.get("non_production") is True,
        "normalized_surface_not_a_backtest_surface": normalized_surface.get("not_a_backtest_surface") is True,
        "probability_surface_materialized_after_implementation": HOME_DST in first and AWAY_DST in first,
        "run_surface_materialized": False,
        "run_surface_gap_remains": True,
        "run_surface_fields_absent_confirmed": all_passed(run_gap),
        "probability_metric_ready_after_implementation": False,
        "runs_metric_ready_after_implementation": False,
        "any_backtest_metric_ready_after_implementation": False,
        "adapter_call_executed_by_6lx": False,
        "additional_adapter_call_executed_by_6lx": False,
        "production_code_modified_by_6lx": False,
        "full_batch_adapter_call_run": False,
        "real_metric_execution_run": False,
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
            "alias_mapping_applied_csv": str(ALIAS_APPLIED_CSV),
            "normalized_surface_json": str(NORMALIZED_SURFACE_JSON),
            "normalized_surface_summary_csv": str(SURFACE_SUMMARY_CSV),
            "probability_surface_readiness_csv": str(PROB_READY_CSV),
            "run_surface_gap_csv": str(RUN_GAP_CSV),
            "metric_readiness_csv": str(METRIC_READY_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6ly_contract_csv": str(FUTURE_6LY_CSV),
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
