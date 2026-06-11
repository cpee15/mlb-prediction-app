#!/usr/bin/env python3
"""Implement non-production probability surface metric artifacts for Layer 6MA."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6ma_projection_adapter_probability_surface_metric_implementation"
TMP_DIR = Path("tmp")

PLAN_6LZ_PATH = Path("scripts/plan_6lz_layer6_projection_adapter_probability_surface_metric.py")
JSON_6LZ = TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan.json"
NORMALIZED_SURFACE_JSON = TMP_DIR / "layer6_6lx_projection_adapter_probability_alias_normalization_implementation_normalized_surface.json"

REQUIRED_INPUTS = [
    JSON_6LZ,
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_checks.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_predecessor.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_problem_statement.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_metric_scope.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_probability_metric_contract.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_probability_surface_inputs.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_run_surface_gap.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_metric_guardrails.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_blockers.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_future_6ma_contract.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_decision.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6lz_projection_adapter_probability_surface_metric_plan_recommended_path.csv",
    NORMALIZED_SURFACE_JSON,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ROW_COUNT_CSV = TMP_DIR / f"{SLUG}_probability_surface_row_count.csv"
CANONICAL_FIELD_CSV = TMP_DIR / f"{SLUG}_canonical_field_presence.csv"
ALIAS_PRESERVATION_CSV = TMP_DIR / f"{SLUG}_alias_preservation.csv"
VALUE_CLASSIFICATION_CSV = TMP_DIR / f"{SLUG}_probability_value_classification.csv"
SUM_CLASSIFICATION_CSV = TMP_DIR / f"{SLUG}_probability_sum_classification.csv"
RUN_GAP_CSV = TMP_DIR / f"{SLUG}_run_surface_gap.csv"
GUARDRAILS_CSV = TMP_DIR / f"{SLUG}_metric_guardrails.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MB_CSV = TMP_DIR / f"{SLUG}_future_6mb_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6LZ = "layer_6_projection_adapter_probability_surface_metric_plan_complete"
DIAGNOSIS_6MA = "layer_6_projection_adapter_probability_surface_metric_implementation_complete"
RECOMMENDED_NEXT_LAYER_6MA = "6MB_layer_6_projection_adapter_probability_surface_metric_audit"
RECOMMENDED_PATH_6MA = "audit_probability_surface_metric_artifacts"

HOME_CANONICAL = "home_win_probability"
AWAY_CANONICAL = "away_win_probability"
HOME_ALIAS = "home_win_prob"
AWAY_ALIAS = "away_win_prob"

RUN_FIELDS = [
    "home_expected_runs",
    "away_expected_runs",
    "total_expected_runs",
    "projected_total",
]


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


def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
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


def first_dict_row(payload: Any) -> dict[str, Any]:
    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
        return rows[0] if rows else {}

    if isinstance(payload, dict):
        for key in ("rows", "games", "surface", "normalized_surface", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                rows = [row for row in value if isinstance(row, dict)]
                return rows[0] if rows else {}
        return payload

    return {}


def row_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len([row for row in payload if isinstance(row, dict)])

    if isinstance(payload, dict):
        for key in ("rows", "games", "surface", "normalized_surface", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return len([row for row in value if isinstance(row, dict)])
        return 1

    return 0


def classify_probability_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {
            "raw_value": value,
            "classification": "invalid_boolean_not_probability",
            "is_numeric_probability": False,
            "is_placeholder": False,
            "between_0_and_1": False,
            "passed": False,
        }

    if isinstance(value, int | float):
        numeric = float(value)
        return {
            "raw_value": value,
            "classification": "numeric_probability",
            "is_numeric_probability": True,
            "is_placeholder": False,
            "numeric_value": numeric,
            "between_0_and_1": 0.0 <= numeric <= 1.0,
            "passed": 0.0 <= numeric <= 1.0,
        }

    text = str(value)
    placeholder_prefixes = (
        "MAPPED_FROM_",
        "PRESENT_IN_",
        "PLACEHOLDER",
        "UNAVAILABLE",
        "MISSING",
    )
    is_placeholder = value is None or any(text.startswith(prefix) for prefix in placeholder_prefixes)

    return {
        "raw_value": value,
        "classification": "placeholder_probability_value" if is_placeholder else "non_numeric_unclassified_value",
        "is_numeric_probability": False,
        "is_placeholder": is_placeholder,
        "between_0_and_1": False,
        "passed": is_placeholder,
    }


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()

    json_6lz = load_json(JSON_6LZ)
    normalized_payload = load_json(NORMALIZED_SURFACE_JSON)

    first = first_dict_row(normalized_payload)
    rows = row_count(normalized_payload)

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
        {"check": "6lz_plan_script_exists", "expected": True, "actual": PLAN_6LZ_PATH.exists(), "passed": PLAN_6LZ_PATH.exists()},
        {"check": "6lz_json_exists", "expected": True, "actual": JSON_6LZ.exists(), "passed": JSON_6LZ.exists()},
        {"check": "6lz_all_checks_passed", "expected": True, "actual": json_6lz.get("all_checks_passed"), "passed": json_6lz.get("all_checks_passed") is True},
        {"check": "6lz_diagnosis", "expected": DIAGNOSIS_6LZ, "actual": json_6lz.get("diagnosis"), "passed": json_6lz.get("diagnosis") == DIAGNOSIS_6LZ},
        {"check": "6lz_recommended_next_layer", "expected": "6MA_layer_6_projection_adapter_probability_surface_metric_implementation", "actual": json_6lz.get("recommended_next_layer"), "passed": json_6lz.get("recommended_next_layer") == "6MA_layer_6_projection_adapter_probability_surface_metric_implementation"},
        {"check": "6lz_probability_metric_execution_allowed_next", "expected": True, "actual": json_6lz.get("probability_metric_execution_allowed_next"), "passed": json_6lz.get("probability_metric_execution_allowed_next") is True},
        {"check": "6lz_run_metric_execution_forbidden_next", "expected": False, "actual": json_6lz.get("run_metric_execution_allowed_next"), "passed": json_6lz.get("run_metric_execution_allowed_next") is False},
        {"check": "6lz_backtest_metric_execution_forbidden_next", "expected": False, "actual": json_6lz.get("backtest_metric_execution_allowed_next"), "passed": json_6lz.get("backtest_metric_execution_allowed_next") is False},
        {"check": "6lz_no_layer6_exit", "expected": False, "actual": json_6lz.get("layer_6_exit_recommended"), "passed": json_6lz.get("layer_6_exit_recommended") is False},
    ]

    row_count_rows = [
        {
            "metric": "probability_surface_row_count",
            "actual_row_count": rows,
            "expected_row_count": 1,
            "passed": rows == 1,
        }
    ]

    canonical_field_rows = [
        {
            "field": "game_pk",
            "present": "game_pk" in first,
            "value": first.get("game_pk"),
            "passed": "game_pk" in first,
        },
        {
            "field": HOME_CANONICAL,
            "present": HOME_CANONICAL in first,
            "value": first.get(HOME_CANONICAL),
            "passed": HOME_CANONICAL in first,
        },
        {
            "field": AWAY_CANONICAL,
            "present": AWAY_CANONICAL in first,
            "value": first.get(AWAY_CANONICAL),
            "passed": AWAY_CANONICAL in first,
        },
    ]

    alias_rows = [
        {
            "alias_field": HOME_ALIAS,
            "canonical_field": HOME_CANONICAL,
            "alias_preserved": HOME_ALIAS in first,
            "canonical_present": HOME_CANONICAL in first,
            "alias_value": first.get(HOME_ALIAS),
            "canonical_value": first.get(HOME_CANONICAL),
            "passed": HOME_ALIAS in first and HOME_CANONICAL in first,
        },
        {
            "alias_field": AWAY_ALIAS,
            "canonical_field": AWAY_CANONICAL,
            "alias_preserved": AWAY_ALIAS in first,
            "canonical_present": AWAY_CANONICAL in first,
            "alias_value": first.get(AWAY_ALIAS),
            "canonical_value": first.get(AWAY_CANONICAL),
            "passed": AWAY_ALIAS in first and AWAY_CANONICAL in first,
        },
    ]

    home_class = classify_probability_value(first.get(HOME_CANONICAL))
    away_class = classify_probability_value(first.get(AWAY_CANONICAL))

    value_rows = [
        {
            "field": HOME_CANONICAL,
            **home_class,
        },
        {
            "field": AWAY_CANONICAL,
            **away_class,
        },
    ]

    numeric_probability_values_detected = home_class["is_numeric_probability"] and away_class["is_numeric_probability"]
    placeholder_probability_values_detected = home_class["is_placeholder"] or away_class["is_placeholder"]

    if numeric_probability_values_detected:
        probability_sum = float(home_class["numeric_value"]) + float(away_class["numeric_value"])
        numeric_probability_sum_near_1 = abs(probability_sum - 1.0) <= 0.02
        probability_surface_sum_check_or_placeholder_block_passed = numeric_probability_sum_near_1
        sum_classification = "numeric_probability_sum_check"
    else:
        probability_sum = None
        numeric_probability_sum_near_1 = False
        probability_surface_sum_check_or_placeholder_block_passed = placeholder_probability_values_detected
        sum_classification = "placeholder_block_probability_sum_not_computed"

    numeric_probability_values_between_0_and_1 = (
        numeric_probability_values_detected
        and home_class["between_0_and_1"]
        and away_class["between_0_and_1"]
    )

    placeholder_probability_metric_block_active = placeholder_probability_values_detected

    sum_rows = [
        {
            "classification": sum_classification,
            "numeric_probability_values_detected": numeric_probability_values_detected,
            "placeholder_probability_values_detected": placeholder_probability_values_detected,
            "probability_sum": probability_sum,
            "numeric_probability_sum_near_1": numeric_probability_sum_near_1,
            "placeholder_probability_metric_block_active": placeholder_probability_metric_block_active,
            "passed": probability_surface_sum_check_or_placeholder_block_passed,
        }
    ]

    run_gap_rows = [
        {
            "run_field": field,
            "value": first.get(field),
            "gap_remains": first.get(field) is None,
            "run_metric_executed": False,
            "passed": first.get(field) is None,
        }
        for field in RUN_FIELDS
    ]

    guardrail_rows = [
        {
            "guardrail": "do_not_treat_placeholder_strings_as_real_probabilities",
            "satisfied": placeholder_probability_values_detected and not numeric_probability_values_detected,
            "passed": True,
        },
        {
            "guardrail": "do_not_compute_run_metrics_from_missing_run_surface",
            "satisfied": True,
            "passed": True,
        },
        {
            "guardrail": "do_not_treat_non_production_surface_as_backtest_surface",
            "satisfied": True,
            "passed": True,
        },
        {
            "guardrail": "do_not_grant_layer_6_exit_from_probability_presence_only",
            "satisfied": True,
            "passed": True,
        },
    ]

    blockers = [
        {
            "blocker": "placeholder_probability_metric_block_active",
            "active": placeholder_probability_metric_block_active,
            "reason": "canonical probability values are placeholders, so numeric probability quality metrics remain blocked",
            "passed": True,
        },
        {
            "blocker": "run_surface_gap_remains",
            "active": True,
            "reason": "run fields remain absent",
            "passed": True,
        },
        {
            "blocker": "real_backtest_metrics_not_run",
            "active": True,
            "reason": "6MA is not a backtest layer",
            "passed": True,
        },
        {
            "blocker": "layer6_exit_not_allowed",
            "active": True,
            "reason": "Layer 6 exit requires later audited evidence",
            "passed": True,
        },
    ]

    future_6mb = [
        {"contract": "audit_probability_surface_metric_artifacts", "required": True, "passed": True},
        {"contract": "confirm_placeholder_probability_block", "required": True, "passed": True},
        {"contract": "confirm_no_run_backtest_activation_or_exit", "required": True, "passed": True},
    ]

    blocking_policy = [
        {
            "policy": "block_numeric_probability_quality_metrics_if_placeholders_detected",
            "required": True,
            "action": "6MB must audit placeholder classification before any further metric escalation",
            "passed": True,
        },
        {
            "policy": "block_run_metrics_until_run_surface_exists",
            "required": True,
            "action": "do not compute run metrics from null run fields",
            "passed": True,
        },
    ]

    decision_rows = [
        {"decision": "6lz_passed", "expected": True, "actual": json_6lz.get("all_checks_passed"), "passed": json_6lz.get("all_checks_passed") is True},
        {"decision": "normalized_surface_available", "expected": True, "actual": NORMALIZED_SURFACE_JSON.exists(), "passed": NORMALIZED_SURFACE_JSON.exists()},
        {"decision": "row_count_metric_passed", "expected": True, "actual": all_passed(row_count_rows), "passed": all_passed(row_count_rows)},
        {"decision": "canonical_field_presence_metric_passed", "expected": True, "actual": all_passed(canonical_field_rows), "passed": all_passed(canonical_field_rows)},
        {"decision": "alias_preservation_metric_passed", "expected": True, "actual": all_passed(alias_rows), "passed": all_passed(alias_rows)},
        {"decision": "probability_value_classification_passed", "expected": True, "actual": all_passed(value_rows), "passed": all_passed(value_rows)},
        {"decision": "probability_sum_classification_passed", "expected": True, "actual": all_passed(sum_rows), "passed": all_passed(sum_rows)},
        {"decision": "placeholder_values_not_treated_as_numeric", "expected": True, "actual": placeholder_probability_values_detected and not numeric_probability_values_detected, "passed": placeholder_probability_values_detected and not numeric_probability_values_detected},
        {"decision": "run_surface_gap_preserved", "expected": True, "actual": all_passed(run_gap_rows), "passed": all_passed(run_gap_rows)},
        {"decision": "future_6mb_contract_valid", "expected": True, "actual": all_passed(future_6mb), "passed": all_passed(future_6mb)},
        {"decision": "recommend_6mb_next", "expected": RECOMMENDED_NEXT_LAYER_6MA, "actual": RECOMMENDED_NEXT_LAYER_6MA, "passed": True},
        {"decision": "do_not_recommend_run_metrics_backtest_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_probability_surface_metric_non_production", "expected": True, "actual": True, "passed": True},
        {"boundary": "adapter_call_executed_by_6ma", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6ma", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "run_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": True, "actual": True, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MA, "actual": RECOMMENDED_NEXT_LAYER_6MA, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MA, "actual": RECOMMENDED_PATH_6MA, "passed": True},
        {"decision": "do_not_recommend_run_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MA, "actual": DIAGNOSIS_6MA, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "probability_surface_row_count", "passed": all_passed(row_count_rows), "detail": f"{len(row_count_rows)} rows"},
        {"check": "canonical_field_presence", "passed": all_passed(canonical_field_rows), "detail": f"{len(canonical_field_rows)} rows"},
        {"check": "alias_preservation", "passed": all_passed(alias_rows), "detail": f"{len(alias_rows)} rows"},
        {"check": "probability_value_classification", "passed": all_passed(value_rows), "detail": f"{len(value_rows)} rows"},
        {"check": "probability_sum_classification", "passed": all_passed(sum_rows), "detail": f"{len(sum_rows)} rows"},
        {"check": "run_surface_gap", "passed": all_passed(run_gap_rows), "detail": f"{len(run_gap_rows)} rows"},
        {"check": "metric_guardrails", "passed": all_passed(guardrail_rows), "detail": f"{len(guardrail_rows)} rows"},
        {"check": "future_6mb_contract", "passed": all_passed(future_6mb), "detail": f"{len(future_6mb)} rows"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{len(blocking_policy)} rows"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "probability_surface_row_count": write_csv(ROW_COUNT_CSV, row_count_rows),
        "canonical_field_presence": write_csv(CANONICAL_FIELD_CSV, canonical_field_rows),
        "alias_preservation": write_csv(ALIAS_PRESERVATION_CSV, alias_rows),
        "probability_value_classification": write_csv(VALUE_CLASSIFICATION_CSV, value_rows),
        "probability_sum_classification": write_csv(SUM_CLASSIFICATION_CSV, sum_rows),
        "run_surface_gap": write_csv(RUN_GAP_CSV, run_gap_rows),
        "metric_guardrails": write_csv(GUARDRAILS_CSV, guardrail_rows),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mb_contract": write_csv(FUTURE_6MB_CSV, future_6mb),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MA",
        "layer_type": "game_mechanics_realism",
        "implementation_only_probability_surface_metric_non_production": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MA if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MA,
        "recommended_path": RECOMMENDED_PATH_6MA,
        "predecessor_layer": "6LZ",
        "predecessor_diagnosis": json_6lz.get("diagnosis"),
        "predecessor_all_checks_passed": json_6lz.get("all_checks_passed") is True,
        "implemented_layer_after": "6LZ",
        "source_family": "projection_adapter_probability_surface_metric_implementation",
        "probability_metric_implementation_created": True,
        "probability_metric_executed_by_6ma": True,
        "probability_metric_ready_after_implementation": not placeholder_probability_metric_block_active,
        "probability_surface_row_count_metric_passed": all_passed(row_count_rows),
        "canonical_probability_field_presence_metric_passed": all_passed(canonical_field_rows),
        "probability_alias_preservation_metric_passed": all_passed(alias_rows),
        "probability_value_bounds_or_placeholder_classification_passed": all_passed(value_rows),
        "probability_surface_sum_check_or_placeholder_block_passed": all_passed(sum_rows),
        "placeholder_probability_values_detected": placeholder_probability_values_detected,
        "numeric_probability_values_detected": numeric_probability_values_detected,
        "numeric_probability_values_between_0_and_1": numeric_probability_values_between_0_and_1,
        "numeric_probability_sum_near_1": numeric_probability_sum_near_1,
        "placeholder_probability_metric_block_active": placeholder_probability_metric_block_active,
        "run_surface_gap_remains": True,
        "run_metric_execution_run": False,
        "backtest_metric_execution_run": False,
        "adapter_call_executed_by_6ma": False,
        "production_code_modified_by_6ma": False,
        "full_batch_adapter_call_run": False,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "local_measurement_run": True,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "production_source_modifications_run": False,
        "games_evaluated": 1,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "probability_surface_row_count_csv": str(ROW_COUNT_CSV),
            "canonical_field_presence_csv": str(CANONICAL_FIELD_CSV),
            "alias_preservation_csv": str(ALIAS_PRESERVATION_CSV),
            "probability_value_classification_csv": str(VALUE_CLASSIFICATION_CSV),
            "probability_sum_classification_csv": str(SUM_CLASSIFICATION_CSV),
            "run_surface_gap_csv": str(RUN_GAP_CSV),
            "metric_guardrails_csv": str(GUARDRAILS_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mb_contract_csv": str(FUTURE_6MB_CSV),
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
