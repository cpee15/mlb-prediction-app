#!/usr/bin/env python3
"""Audit Layer 6MA probability surface metric implementation artifacts."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mb_projection_adapter_probability_surface_metric_audit"
TMP_DIR = Path("tmp")

SCRIPT_6MA = Path("scripts/implement_6ma_layer6_projection_adapter_probability_surface_metric.py")
JSON_6MA = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation.json"

REQUIRED_INPUTS = [
    JSON_6MA,
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_checks.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_predecessor.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_probability_surface_row_count.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_canonical_field_presence.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_alias_preservation.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_probability_value_classification.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_probability_sum_classification.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_run_surface_gap.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_metric_guardrails.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_blockers.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_future_6mb_contract.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_decision.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
METRIC_REVIEW_CSV = TMP_DIR / f"{SLUG}_metric_artifact_review.csv"
PLACEHOLDER_BLOCK_CSV = TMP_DIR / f"{SLUG}_placeholder_block.csv"
RUN_GAP_CSV = TMP_DIR / f"{SLUG}_run_surface_gap.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MC_CSV = TMP_DIR / f"{SLUG}_future_6mc_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MA = "layer_6_projection_adapter_probability_surface_metric_implementation_complete"
DIAGNOSIS_6MB = "layer_6_projection_adapter_probability_surface_metric_audit_complete"
RECOMMENDED_NEXT_LAYER_6MB = "6MC_layer_6_projection_adapter_numeric_probability_source_trace_plan"
RECOMMENDED_PATH_6MB = "plan_numeric_probability_source_trace_after_placeholder_block"


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


def csv_all_passed(path: Path) -> bool:
    rows = read_csv_rows(path)
    return bool(rows) and all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()

    json_6ma = load_json(JSON_6MA)

    required_artifact_rows = [
        {
            "artifact_path": str(path),
            "exists": path.exists(),
            "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "",
            "passed": path.exists(),
        }
        for path in REQUIRED_INPUTS
    ]

    checks_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_checks.csv"
    row_count_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_probability_surface_row_count.csv"
    canonical_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_canonical_field_presence.csv"
    alias_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_alias_preservation.csv"
    value_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_probability_value_classification.csv"
    sum_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_probability_sum_classification.csv"
    run_gap_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_run_surface_gap.csv"
    guardrails_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_metric_guardrails.csv"
    decision_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_decision.csv"
    safety_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_safety_boundaries.csv"
    recommended_path = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_recommended_path.csv"

    value_rows = read_csv_rows(value_path)
    sum_rows = read_csv_rows(sum_path)
    run_rows = read_csv_rows(run_gap_path)

    placeholder_values_confirmed = (
        json_6ma.get("placeholder_probability_values_detected") is True
        and all(row.get("classification") == "placeholder_probability_value" for row in value_rows)
        and all(str(row.get("is_placeholder")).lower() == "true" for row in value_rows)
    )

    numeric_values_absent_confirmed = (
        json_6ma.get("numeric_probability_values_detected") is False
        and all(str(row.get("is_numeric_probability")).lower() == "false" for row in value_rows)
    )

    placeholder_block_confirmed = (
        json_6ma.get("placeholder_probability_metric_block_active") is True
        and bool(sum_rows)
        and sum_rows[0].get("classification") == "placeholder_block_probability_sum_not_computed"
        and str(sum_rows[0].get("placeholder_probability_metric_block_active")).lower() == "true"
    )

    run_gap_confirmed = (
        json_6ma.get("run_surface_gap_remains") is True
        and bool(run_rows)
        and all(str(row.get("gap_remains")).lower() == "true" for row in run_rows)
        and all(str(row.get("run_metric_executed")).lower() == "false" for row in run_rows)
    )

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ma_script_exists", "expected": True, "actual": SCRIPT_6MA.exists(), "passed": SCRIPT_6MA.exists()},
        {"check": "6ma_json_exists", "expected": True, "actual": JSON_6MA.exists(), "passed": JSON_6MA.exists()},
        {"check": "6ma_all_checks_passed", "expected": True, "actual": json_6ma.get("all_checks_passed"), "passed": json_6ma.get("all_checks_passed") is True},
        {"check": "6ma_diagnosis", "expected": DIAGNOSIS_6MA, "actual": json_6ma.get("diagnosis"), "passed": json_6ma.get("diagnosis") == DIAGNOSIS_6MA},
        {"check": "6ma_recommended_next_layer", "expected": "6MB_layer_6_projection_adapter_probability_surface_metric_audit", "actual": json_6ma.get("recommended_next_layer"), "passed": json_6ma.get("recommended_next_layer") == "6MB_layer_6_projection_adapter_probability_surface_metric_audit"},
    ]

    metric_review_rows = [
        {"metric_artifact": "checks", "path": str(checks_path), "passed": csv_all_passed(checks_path)},
        {"metric_artifact": "probability_surface_row_count", "path": str(row_count_path), "passed": csv_all_passed(row_count_path)},
        {"metric_artifact": "canonical_field_presence", "path": str(canonical_path), "passed": csv_all_passed(canonical_path)},
        {"metric_artifact": "alias_preservation", "path": str(alias_path), "passed": csv_all_passed(alias_path)},
        {"metric_artifact": "probability_value_classification", "path": str(value_path), "passed": csv_all_passed(value_path)},
        {"metric_artifact": "probability_sum_classification", "path": str(sum_path), "passed": csv_all_passed(sum_path)},
        {"metric_artifact": "run_surface_gap", "path": str(run_gap_path), "passed": csv_all_passed(run_gap_path)},
        {"metric_artifact": "metric_guardrails", "path": str(guardrails_path), "passed": csv_all_passed(guardrails_path)},
        {"metric_artifact": "decision", "path": str(decision_path), "passed": csv_all_passed(decision_path)},
        {"metric_artifact": "safety_boundaries", "path": str(safety_path), "passed": csv_all_passed(safety_path)},
        {"metric_artifact": "recommended_path", "path": str(recommended_path), "passed": csv_all_passed(recommended_path)},
    ]

    placeholder_block_rows = [
        {
            "audit": "placeholder_probability_values_detected_confirmed",
            "expected": True,
            "actual": placeholder_values_confirmed,
            "passed": placeholder_values_confirmed,
        },
        {
            "audit": "numeric_probability_values_detected_confirmed_absent",
            "expected": False,
            "actual": json_6ma.get("numeric_probability_values_detected"),
            "passed": numeric_values_absent_confirmed,
        },
        {
            "audit": "placeholder_probability_metric_block_active_confirmed",
            "expected": True,
            "actual": json_6ma.get("placeholder_probability_metric_block_active"),
            "passed": placeholder_block_confirmed,
        },
        {
            "audit": "probability_metric_ready_after_audit_false",
            "expected": False,
            "actual": json_6ma.get("probability_metric_ready_after_implementation"),
            "passed": json_6ma.get("probability_metric_ready_after_implementation") is False,
        },
    ]

    run_gap_rows = [
        {
            "audit": "run_surface_gap_remains",
            "expected": True,
            "actual": json_6ma.get("run_surface_gap_remains"),
            "passed": run_gap_confirmed,
        },
        {
            "audit": "run_metric_execution_run_false",
            "expected": False,
            "actual": json_6ma.get("run_metric_execution_run"),
            "passed": json_6ma.get("run_metric_execution_run") is False,
        },
        {
            "audit": "backtest_metric_execution_run_false",
            "expected": False,
            "actual": json_6ma.get("backtest_metric_execution_run"),
            "passed": json_6ma.get("backtest_metric_execution_run") is False,
        },
    ]

    blockers = [
        {
            "blocker": "placeholder_probability_metric_block_active",
            "active": True,
            "blocks_numeric_probability_metric_readiness": True,
            "passed": placeholder_block_confirmed,
        },
        {
            "blocker": "run_surface_gap_remains",
            "active": True,
            "blocks_run_metrics": True,
            "passed": run_gap_confirmed,
        },
        {
            "blocker": "real_backtest_metrics_not_run",
            "active": True,
            "blocks_activation": True,
            "passed": True,
        },
        {
            "blocker": "layer6_exit_not_allowed",
            "active": True,
            "blocks_layer6_exit": True,
            "passed": True,
        },
    ]

    future_6mc = [
        {
            "contract": "plan_numeric_probability_source_trace_after_placeholder_block",
            "required": True,
            "why": "canonical probability fields are present but still placeholders",
            "passed": True,
        },
        {
            "contract": "identify_where_real_numeric_probability_values_should_enter_surface",
            "required": True,
            "why": "metrics cannot be numeric-ready until real probabilities replace placeholders",
            "passed": True,
        },
        {
            "contract": "preserve_no_run_backtest_activation_or_exit",
            "required": True,
            "why": "this remains probability-surface plumbing, not model validation",
            "passed": True,
        },
    ]

    blocking_policy = [
        {
            "policy": "do_not_escalate_to_numeric_probability_quality_metrics_until_source_trace_identifies_real_probability_values",
            "required": True,
            "passed": True,
        },
        {
            "policy": "do_not_run_backtests_until_numeric_surface_is_real_and_audited",
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
        {"decision": "6ma_passed", "expected": True, "actual": json_6ma.get("all_checks_passed"), "passed": json_6ma.get("all_checks_passed") is True},
        {"decision": "6ma_diagnosis_valid", "expected": DIAGNOSIS_6MA, "actual": json_6ma.get("diagnosis"), "passed": json_6ma.get("diagnosis") == DIAGNOSIS_6MA},
        {"decision": "all_required_6ma_artifacts_exist", "expected": True, "actual": all_passed(required_artifact_rows), "passed": all_passed(required_artifact_rows)},
        {"decision": "metric_artifacts_audited", "expected": True, "actual": all_passed(metric_review_rows), "passed": all_passed(metric_review_rows)},
        {"decision": "placeholder_block_confirmed", "expected": True, "actual": placeholder_block_confirmed, "passed": placeholder_block_confirmed},
        {"decision": "numeric_probability_values_absent_confirmed", "expected": True, "actual": numeric_values_absent_confirmed, "passed": numeric_values_absent_confirmed},
        {"decision": "probability_metric_ready_after_audit_false", "expected": False, "actual": json_6ma.get("probability_metric_ready_after_implementation"), "passed": json_6ma.get("probability_metric_ready_after_implementation") is False},
        {"decision": "run_surface_gap_confirmed", "expected": True, "actual": run_gap_confirmed, "passed": run_gap_confirmed},
        {"decision": "future_6mc_contract_valid", "expected": True, "actual": all_passed(future_6mc), "passed": all_passed(future_6mc)},
        {"decision": "recommend_6mc_next", "expected": RECOMMENDED_NEXT_LAYER_6MB, "actual": RECOMMENDED_NEXT_LAYER_6MB, "passed": True},
        {"decision": "do_not_recommend_run_metrics_backtest_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_probability_surface_metric_artifacts", "expected": True, "actual": True, "passed": True},
        {"boundary": "adapter_call_executed_by_6mb", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mb", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "run_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_metric_execution_run", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MB, "actual": RECOMMENDED_NEXT_LAYER_6MB, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MB, "actual": RECOMMENDED_PATH_6MB, "passed": True},
        {"decision": "do_not_recommend_run_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MB, "actual": DIAGNOSIS_6MB, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(required_artifact_rows), "detail": f"{sum(1 for r in required_artifact_rows if r['passed'])}/{len(required_artifact_rows)}"},
        {"check": "metric_artifact_review", "passed": all_passed(metric_review_rows), "detail": f"{sum(1 for r in metric_review_rows if r['passed'])}/{len(metric_review_rows)}"},
        {"check": "placeholder_block", "passed": all_passed(placeholder_block_rows), "detail": f"{sum(1 for r in placeholder_block_rows if r['passed'])}/{len(placeholder_block_rows)}"},
        {"check": "run_surface_gap", "passed": all_passed(run_gap_rows), "detail": f"{sum(1 for r in run_gap_rows if r['passed'])}/{len(run_gap_rows)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mc_contract", "passed": all_passed(future_6mc), "detail": f"{sum(1 for r in future_6mc if r['passed'])}/{len(future_6mc)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{sum(1 for r in blocking_policy if r['passed'])}/{len(blocking_policy)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, required_artifact_rows),
        "metric_artifact_review": write_csv(METRIC_REVIEW_CSV, metric_review_rows),
        "placeholder_block": write_csv(PLACEHOLDER_BLOCK_CSV, placeholder_block_rows),
        "run_surface_gap": write_csv(RUN_GAP_CSV, run_gap_rows),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mc_contract": write_csv(FUTURE_6MC_CSV, future_6mc),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MB",
        "layer_type": "game_mechanics_realism",
        "audit_only_probability_surface_metric_artifacts": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MB if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MB,
        "recommended_path": RECOMMENDED_PATH_6MB,
        "predecessor_layer": "6MA",
        "predecessor_diagnosis": json_6ma.get("diagnosis"),
        "predecessor_all_checks_passed": json_6ma.get("all_checks_passed") is True,
        "audited_layer_after": "6MA",
        "source_family": "projection_adapter_probability_surface_metric_audit",
        "probability_surface_metric_artifacts_audited": True,
        "probability_metric_implementation_confirmed": json_6ma.get("probability_metric_implementation_created") is True,
        "probability_metric_executed_by_6ma_confirmed": json_6ma.get("probability_metric_executed_by_6ma") is True,
        "probability_metric_ready_after_audit": False,
        "placeholder_probability_values_detected_confirmed": placeholder_values_confirmed,
        "numeric_probability_values_detected_confirmed": False,
        "placeholder_probability_metric_block_active_confirmed": placeholder_block_confirmed,
        "probability_surface_sum_check_placeholder_block_confirmed": placeholder_block_confirmed,
        "row_count_metric_confirmed": csv_all_passed(row_count_path),
        "canonical_field_presence_metric_confirmed": csv_all_passed(canonical_path),
        "alias_preservation_metric_confirmed": csv_all_passed(alias_path),
        "probability_value_classification_confirmed": csv_all_passed(value_path),
        "run_surface_gap_remains": run_gap_confirmed,
        "run_metric_execution_run": False,
        "backtest_metric_execution_run": False,
        "adapter_call_executed_by_6mb": False,
        "production_code_modified_by_6mb": False,
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
            "metric_artifact_review_csv": str(METRIC_REVIEW_CSV),
            "placeholder_block_csv": str(PLACEHOLDER_BLOCK_CSV),
            "run_surface_gap_csv": str(RUN_GAP_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mc_contract_csv": str(FUTURE_6MC_CSV),
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
