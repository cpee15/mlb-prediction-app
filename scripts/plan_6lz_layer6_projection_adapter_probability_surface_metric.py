#!/usr/bin/env python3
"""6LZ Layer 6 projection adapter probability surface metric plan.

This is a planning-only layer.

It consumes the successful 6LY probability alias normalization audit and creates
a plan for the next layer to implement probability-surface metrics on the
audited normalized non-production surface.

It does not:
- execute metrics
- run backtests
- execute adapter calls
- fetch live data
- write databases
- modify production code
- activate mechanics
- grant Layer 6 exit
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


LAYER = "6LZ"
LAYER_TYPE = "game_mechanics_realism"
LAYER_MODE = "planning_only_probability_surface_metric"
SOURCE_FAMILY = "projection_adapter_probability_surface_metric_plan"

SLUG = "layer6_6lz_projection_adapter_probability_surface_metric_plan"
TMP = Path("tmp")

PREDECESSOR_JSON = TMP / "layer6_6ly_projection_adapter_probability_alias_normalization_audit.json"
PREDECESSOR_CHECKS = TMP / "layer6_6ly_projection_adapter_probability_alias_normalization_audit_checks.csv"
PREDECESSOR_PREDECESSOR = TMP / "layer6_6ly_projection_adapter_probability_alias_normalization_audit_predecessor.csv"
PREDECESSOR_INPUT_ARTIFACTS = TMP / "layer6_6ly_projection_adapter_probability_alias_normalization_audit_input_artifacts.csv"
PREDECESSOR_DECISION = TMP / "layer6_6ly_projection_adapter_probability_alias_normalization_audit_decision.csv"
PREDECESSOR_SAFETY = TMP / "layer6_6ly_projection_adapter_probability_alias_normalization_audit_safety_boundaries.csv"
PREDECESSOR_RECOMMENDED = TMP / "layer6_6ly_projection_adapter_probability_alias_normalization_audit_recommended_path.csv"

NORMALIZED_SURFACE = TMP / "layer6_6lx_projection_adapter_probability_alias_normalization_implementation_normalized_surface.json"

OUT_JSON = TMP / f"{SLUG}.json"
OUT_CHECKS = TMP / f"{SLUG}_checks.csv"
OUT_PREDECESSOR = TMP / f"{SLUG}_predecessor.csv"
OUT_INPUT_ARTIFACTS = TMP / f"{SLUG}_input_artifacts.csv"
OUT_PROBLEM = TMP / f"{SLUG}_problem_statement.csv"
OUT_METRIC_SCOPE = TMP / f"{SLUG}_metric_scope.csv"
OUT_ALLOWED_NEXT = TMP / f"{SLUG}_allowed_operations_next.csv"
OUT_FORBIDDEN_NEXT = TMP / f"{SLUG}_forbidden_operations_next.csv"
OUT_METRIC_CONTRACT = TMP / f"{SLUG}_probability_metric_contract.csv"
OUT_SURFACE_INPUTS = TMP / f"{SLUG}_probability_surface_inputs.csv"
OUT_RUN_GAP = TMP / f"{SLUG}_run_surface_gap.csv"
OUT_GUARDRAILS = TMP / f"{SLUG}_metric_guardrails.csv"
OUT_BLOCKERS = TMP / f"{SLUG}_blockers.csv"
OUT_FUTURE_6MA = TMP / f"{SLUG}_future_6ma_contract.csv"
OUT_BLOCKING_POLICY = TMP / f"{SLUG}_blocking_policy.csv"
OUT_DECISION = TMP / f"{SLUG}_decision.csv"
OUT_SAFETY = TMP / f"{SLUG}_safety_boundaries.csv"
OUT_RECOMMENDED = TMP / f"{SLUG}_recommended_path.csv"

EXPECTED_PREDECESSOR_LAYER = "6LY"
EXPECTED_PREDECESSOR_DIAGNOSIS = "probability_alias_normalization_artifact_audited"

PASS_DIAGNOSIS = "layer_6_projection_adapter_probability_surface_metric_plan_complete"
FAIL_DIAGNOSIS = "layer_6_projection_adapter_probability_surface_metric_plan_blocked_or_failed"

PASS_NEXT_LAYER = "6MA_layer_6_projection_adapter_probability_surface_metric_implementation"
PASS_RECOMMENDED_PATH = "implement_probability_surface_metric_on_audited_normalized_surface"

FAIL_NEXT_LAYER = "6LZ_layer_6_projection_adapter_probability_surface_metric_plan_repair"
FAIL_RECOMMENDED_PATH = "repair_6ly_audit_or_normalized_surface_before_probability_metric_plan"

RUN_FIELDS = [
    "home_expected_runs",
    "away_expected_runs",
    "total_expected_runs",
    "projected_total",
]

SAFETY_BOUNDARIES = {
    "adapter_calls_allowed": False,
    "metrics_allowed": False,
    "backtests_allowed": False,
    "live_data_fetch_allowed": False,
    "database_writes_allowed": False,
    "production_code_changes_allowed": False,
    "mechanics_activation_allowed": False,
    "layer_6_exit_allowed": False,
}

ALLOWED_OPERATIONS_NEXT = [
    {
        "operation": "read_audited_normalized_probability_surface",
        "allowed_next": True,
        "scope": "non_production_artifact_only",
    },
    {
        "operation": "compute_probability_surface_presence_metric",
        "allowed_next": True,
        "scope": "home_win_probability_and_away_win_probability_presence_and_bounds",
    },
    {
        "operation": "write_probability_metric_artifacts",
        "allowed_next": True,
        "scope": "tmp_artifacts_only",
    },
]

FORBIDDEN_OPERATIONS_NEXT = [
    "adapter_calls",
    "live_data_fetches",
    "database_writes",
    "production_code_changes",
    "backtest_metric_execution",
    "run_surface_metric_execution",
    "mechanics_activation",
    "layer_6_exit",
]

PROBABILITY_METRIC_CONTRACT = [
    {
        "metric": "probability_surface_row_count",
        "input_fields": "game_pk,home_win_probability,away_win_probability",
        "expected_result": "one_non_production_row",
        "implementation_layer": "6MA",
    },
    {
        "metric": "canonical_probability_field_presence",
        "input_fields": "home_win_probability,away_win_probability",
        "expected_result": "both_fields_present",
        "implementation_layer": "6MA",
    },
    {
        "metric": "probability_alias_preservation",
        "input_fields": "home_win_prob,away_win_prob",
        "expected_result": "both_alias_fields_preserved",
        "implementation_layer": "6MA",
    },
    {
        "metric": "probability_value_bounds_or_placeholder_classification",
        "input_fields": "home_win_probability,away_win_probability",
        "expected_result": "numeric_probabilities_between_0_and_1_or_explicit_non_numeric_placeholder_classification",
        "implementation_layer": "6MA",
    },
    {
        "metric": "probability_surface_sum_check",
        "input_fields": "home_win_probability,away_win_probability",
        "expected_result": "numeric_probabilities_sum_near_1_or_explicit_placeholder_block",
        "implementation_layer": "6MA",
    },
]

METRIC_GUARDRAILS = [
    {
        "guardrail": "do_not_treat_placeholder_strings_as_real_probabilities",
        "required": True,
    },
    {
        "guardrail": "do_not_compute_run_metrics_from_missing_run_surface",
        "required": True,
    },
    {
        "guardrail": "do_not_treat_non_production_surface_as_backtest_surface",
        "required": True,
    },
    {
        "guardrail": "do_not_grant_layer_6_exit_from_probability_presence_only",
        "required": True,
    },
]


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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


def check_row(check: str, passed: bool, detail: Any) -> dict[str, Any]:
    return {
        "check": check,
        "passed": bool(passed),
        "detail": detail,
    }


def main() -> None:
    TMP.mkdir(exist_ok=True)

    required_6ly_artifacts = [
        PREDECESSOR_JSON,
        PREDECESSOR_CHECKS,
        PREDECESSOR_PREDECESSOR,
        PREDECESSOR_INPUT_ARTIFACTS,
        PREDECESSOR_DECISION,
        PREDECESSOR_SAFETY,
        PREDECESSOR_RECOMMENDED,
    ]

    predecessor_exists = PREDECESSOR_JSON.exists()
    normalized_surface_exists = NORMALIZED_SURFACE.exists()

    predecessor = read_json(PREDECESSOR_JSON) if predecessor_exists else {}
    normalized_payload = read_json(NORMALIZED_SURFACE) if normalized_surface_exists else None
    normalized_row = first_dict_row(normalized_payload)
    normalized_rows = row_count(normalized_payload)

    predecessor_layer = predecessor.get("layer")
    predecessor_diagnosis = predecessor.get("diagnosis")
    predecessor_all_checks_passed = predecessor.get("all_checks_passed") is True

    normalized_surface_has_game_pk = "game_pk" in normalized_row
    normalized_surface_has_home_win_probability = "home_win_probability" in normalized_row
    normalized_surface_has_away_win_probability = "away_win_probability" in normalized_row
    normalized_surface_preserves_home_win_prob = "home_win_prob" in normalized_row
    normalized_surface_preserves_away_win_prob = "away_win_prob" in normalized_row

    normalized_surface_is_non_production = (
        normalized_row.get("non_production") is True
        or normalized_row.get("artifact_scope") == "non_production"
        or predecessor.get("normalized_surface_is_non_production") is True
    )

    normalized_surface_not_a_backtest_surface = (
        normalized_row.get("not_a_backtest_surface") is True
        or normalized_row.get("is_backtest_surface") is False
        or predecessor.get("normalized_surface_not_a_backtest_surface") is True
    )

    run_surface_gap_remains = bool(normalized_row) and all(
        normalized_row.get(field) is None for field in RUN_FIELDS
    )

    all_required_6ly_artifacts_exist = all(path.exists() for path in required_6ly_artifacts)

    probability_metric_plan_created = True
    probability_metric_execution_allowed_next = True
    run_metric_execution_allowed_next = False
    backtest_metric_execution_allowed_next = False

    checks = [
        check_row("predecessor_json_exists", predecessor_exists, str(PREDECESSOR_JSON)),
        check_row("predecessor_layer_is_6ly", predecessor_layer == EXPECTED_PREDECESSOR_LAYER, predecessor_layer),
        check_row(
            "predecessor_diagnosis_expected",
            predecessor_diagnosis == EXPECTED_PREDECESSOR_DIAGNOSIS,
            predecessor_diagnosis,
        ),
        check_row("predecessor_all_checks_passed", predecessor_all_checks_passed, predecessor_all_checks_passed),
        check_row(
            "all_required_6ly_artifacts_exist",
            all_required_6ly_artifacts_exist,
            [str(path) for path in required_6ly_artifacts if not path.exists()],
        ),
        check_row("normalized_surface_artifact_exists", normalized_surface_exists, str(NORMALIZED_SURFACE)),
        check_row("normalized_surface_row_count_equals_1", normalized_rows == 1, normalized_rows),
        check_row("normalized_surface_has_game_pk", normalized_surface_has_game_pk, normalized_row.get("game_pk")),
        check_row(
            "normalized_surface_has_home_win_probability",
            normalized_surface_has_home_win_probability,
            normalized_row.get("home_win_probability"),
        ),
        check_row(
            "normalized_surface_has_away_win_probability",
            normalized_surface_has_away_win_probability,
            normalized_row.get("away_win_probability"),
        ),
        check_row(
            "normalized_surface_preserves_home_win_prob",
            normalized_surface_preserves_home_win_prob,
            normalized_row.get("home_win_prob"),
        ),
        check_row(
            "normalized_surface_preserves_away_win_prob",
            normalized_surface_preserves_away_win_prob,
            normalized_row.get("away_win_prob"),
        ),
        check_row(
            "normalized_surface_is_non_production",
            normalized_surface_is_non_production,
            normalized_surface_is_non_production,
        ),
        check_row(
            "normalized_surface_not_a_backtest_surface",
            normalized_surface_not_a_backtest_surface,
            normalized_surface_not_a_backtest_surface,
        ),
        check_row("probability_metric_plan_created", probability_metric_plan_created, probability_metric_plan_created),
        check_row(
            "probability_metric_execution_allowed_next",
            probability_metric_execution_allowed_next,
            probability_metric_execution_allowed_next,
        ),
        check_row("probability_metric_executed_by_6lz", True, False),
        check_row("run_surface_gap_remains", run_surface_gap_remains, {field: normalized_row.get(field) for field in RUN_FIELDS}),
        check_row(
            "run_metric_execution_allowed_next_is_false",
            run_metric_execution_allowed_next is False,
            run_metric_execution_allowed_next,
        ),
        check_row(
            "backtest_metric_execution_allowed_next_is_false",
            backtest_metric_execution_allowed_next is False,
            backtest_metric_execution_allowed_next,
        ),
        check_row("adapter_call_executed_by_6lz_is_false", True, False),
        check_row("production_code_modified_by_6lz_is_false", True, False),
        check_row("full_batch_adapter_call_run_is_false", True, False),
        check_row("real_metric_execution_run_is_false", True, False),
        check_row("real_historical_evaluation_run_is_false", True, False),
        check_row("production_simulations_run_is_false", True, False),
        check_row("local_measurement_run_is_false", True, False),
        check_row("activation_execution_allowed_after_this_layer_is_false", True, False),
        check_row("mechanics_activated_by_this_layer_is_false", True, False),
        check_row("layer_6_exit_recommended_is_false", True, False),
        check_row("database_writes_run_is_false", True, False),
        check_row("live_data_fetches_run_is_false", True, False),
        check_row("remote_api_calls_run_is_false", True, False),
        check_row("source_acquisition_performed_by_this_layer_is_false", True, False),
        check_row("production_source_modifications_run_is_false", True, False),
    ]

    all_checks_passed = all(item["passed"] for item in checks)
    diagnosis = PASS_DIAGNOSIS if all_checks_passed else FAIL_DIAGNOSIS
    recommended_next_layer = PASS_NEXT_LAYER if all_checks_passed else FAIL_NEXT_LAYER
    recommended_path = PASS_RECOMMENDED_PATH if all_checks_passed else FAIL_RECOMMENDED_PATH

    blockers = []
    blockers.extend(item["check"] for item in checks if not item["passed"])
    blockers.extend([
        "run_surface_gap_remains",
        "real_backtest_metrics_not_run",
        "layer6_exit_not_allowed",
    ])

    artifact_paths = {
        "json": str(OUT_JSON),
        "checks": str(OUT_CHECKS),
        "predecessor": str(OUT_PREDECESSOR),
        "input_artifacts": str(OUT_INPUT_ARTIFACTS),
        "problem_statement": str(OUT_PROBLEM),
        "metric_scope": str(OUT_METRIC_SCOPE),
        "allowed_operations_next": str(OUT_ALLOWED_NEXT),
        "forbidden_operations_next": str(OUT_FORBIDDEN_NEXT),
        "probability_metric_contract": str(OUT_METRIC_CONTRACT),
        "probability_surface_inputs": str(OUT_SURFACE_INPUTS),
        "run_surface_gap": str(OUT_RUN_GAP),
        "metric_guardrails": str(OUT_GUARDRAILS),
        "blockers": str(OUT_BLOCKERS),
        "future_6ma_contract": str(OUT_FUTURE_6MA),
        "blocking_policy": str(OUT_BLOCKING_POLICY),
        "decision": str(OUT_DECISION),
        "safety_boundaries": str(OUT_SAFETY),
        "recommended_path": str(OUT_RECOMMENDED),
    }

    result = {
        "layer": LAYER,
        "layer_type": LAYER_TYPE,
        "planning_only_probability_surface_metric": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis,
        "recommended_next_layer": recommended_next_layer,
        "recommended_path": recommended_path,
        "predecessor_layer": predecessor_layer,
        "predecessor_diagnosis": predecessor_diagnosis,
        "predecessor_all_checks_passed": predecessor_all_checks_passed,
        "planned_layer_after": EXPECTED_PREDECESSOR_LAYER,
        "source_family": SOURCE_FAMILY,
        "probability_surface_normalized_and_audited": predecessor.get("probability_surface_normalized_and_audited") is True,
        "normalized_surface_artifact_available": normalized_surface_exists,
        "normalized_surface_row_count": normalized_rows,
        "normalized_surface_has_game_pk": normalized_surface_has_game_pk,
        "normalized_surface_has_home_win_probability": normalized_surface_has_home_win_probability,
        "normalized_surface_has_away_win_probability": normalized_surface_has_away_win_probability,
        "normalized_surface_preserves_home_win_prob": normalized_surface_preserves_home_win_prob,
        "normalized_surface_preserves_away_win_prob": normalized_surface_preserves_away_win_prob,
        "normalized_surface_is_non_production": normalized_surface_is_non_production,
        "normalized_surface_not_a_backtest_surface": normalized_surface_not_a_backtest_surface,
        "probability_metric_plan_created": probability_metric_plan_created,
        "probability_metric_execution_allowed_next": probability_metric_execution_allowed_next,
        "probability_metric_ready_after_plan": False,
        "probability_metric_executed_by_6lz": False,
        "run_surface_gap_remains": run_surface_gap_remains,
        "run_metric_execution_allowed_next": run_metric_execution_allowed_next,
        "backtest_metric_execution_allowed_next": backtest_metric_execution_allowed_next,
        "adapter_call_executed_by_6lz": False,
        "production_code_modified_by_6lz": False,
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
        "artifact_paths": artifact_paths,
        "blockers": blockers,
        "checks": checks,
    }

    OUT_JSON.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    write_csv(OUT_CHECKS, checks, ["check", "passed", "detail"])

    write_csv(
        OUT_PREDECESSOR,
        [{
            "artifact": str(PREDECESSOR_JSON),
            "exists": predecessor_exists,
            "layer": predecessor_layer,
            "diagnosis": predecessor_diagnosis,
            "all_checks_passed": predecessor_all_checks_passed,
            "expected_layer": EXPECTED_PREDECESSOR_LAYER,
            "expected_diagnosis": EXPECTED_PREDECESSOR_DIAGNOSIS,
        }],
        ["artifact", "exists", "layer", "diagnosis", "all_checks_passed", "expected_layer", "expected_diagnosis"],
    )

    write_csv(
        OUT_INPUT_ARTIFACTS,
        [{"artifact": str(path), "exists": path.exists(), "required": True} for path in required_6ly_artifacts + [NORMALIZED_SURFACE]],
        ["artifact", "exists", "required"],
    )

    write_csv(
        OUT_PROBLEM,
        [{
            "problem": "plan_probability_surface_metric_after_alias_normalization_audit",
            "why": "probability aliases are normalized and audited, but no probability metric has been executed yet",
            "blocked_from": "run_metrics,backtest_metrics,activation,layer_6_exit",
        }],
        ["problem", "why", "blocked_from"],
    )

    write_csv(
        OUT_METRIC_SCOPE,
        [{
            "scope": "probability_surface_only",
            "included": "game_pk,home_win_probability,away_win_probability,home_win_prob,away_win_prob",
            "excluded": "run_surface_metrics,backtest_metrics,market_edge_detection,activation",
        }],
        ["scope", "included", "excluded"],
    )

    write_csv(
        OUT_ALLOWED_NEXT,
        ALLOWED_OPERATIONS_NEXT,
        ["operation", "allowed_next", "scope"],
    )

    write_csv(
        OUT_FORBIDDEN_NEXT,
        [{"operation": operation, "allowed_next": False} for operation in FORBIDDEN_OPERATIONS_NEXT],
        ["operation", "allowed_next"],
    )

    write_csv(
        OUT_METRIC_CONTRACT,
        PROBABILITY_METRIC_CONTRACT,
        ["metric", "input_fields", "expected_result", "implementation_layer"],
    )

    write_csv(
        OUT_SURFACE_INPUTS,
        [{
            "artifact": str(NORMALIZED_SURFACE),
            "row_count": normalized_rows,
            "has_game_pk": normalized_surface_has_game_pk,
            "has_home_win_probability": normalized_surface_has_home_win_probability,
            "has_away_win_probability": normalized_surface_has_away_win_probability,
            "preserves_home_win_prob": normalized_surface_preserves_home_win_prob,
            "preserves_away_win_prob": normalized_surface_preserves_away_win_prob,
            "non_production": normalized_surface_is_non_production,
            "not_a_backtest_surface": normalized_surface_not_a_backtest_surface,
        }],
        [
            "artifact",
            "row_count",
            "has_game_pk",
            "has_home_win_probability",
            "has_away_win_probability",
            "preserves_home_win_prob",
            "preserves_away_win_prob",
            "non_production",
            "not_a_backtest_surface",
        ],
    )

    write_csv(
        OUT_RUN_GAP,
        [{
            "run_surface_gap_remains": run_surface_gap_remains,
            "home_expected_runs": normalized_row.get("home_expected_runs"),
            "away_expected_runs": normalized_row.get("away_expected_runs"),
            "total_expected_runs": normalized_row.get("total_expected_runs"),
            "projected_total": normalized_row.get("projected_total"),
            "run_metric_execution_allowed_next": run_metric_execution_allowed_next,
        }],
        [
            "run_surface_gap_remains",
            "home_expected_runs",
            "away_expected_runs",
            "total_expected_runs",
            "projected_total",
            "run_metric_execution_allowed_next",
        ],
    )

    write_csv(
        OUT_GUARDRAILS,
        METRIC_GUARDRAILS,
        ["guardrail", "required"],
    )

    write_csv(
        OUT_BLOCKERS,
        [{"blocker": blocker} for blocker in blockers],
        ["blocker"],
    )

    write_csv(
        OUT_FUTURE_6MA,
        [{
            "future_layer": PASS_NEXT_LAYER,
            "required_action": "implement_probability_surface_metric_on_audited_normalized_surface",
            "allowed_to_execute_probability_metric": True,
            "allowed_to_execute_run_metric": False,
            "allowed_to_execute_backtest_metric": False,
            "allowed_to_activate": False,
            "allowed_to_exit_layer_6": False,
        }],
        [
            "future_layer",
            "required_action",
            "allowed_to_execute_probability_metric",
            "allowed_to_execute_run_metric",
            "allowed_to_execute_backtest_metric",
            "allowed_to_activate",
            "allowed_to_exit_layer_6",
        ],
    )

    write_csv(
        OUT_BLOCKING_POLICY,
        [{
            "policy": "block_if_probability_surface_missing_or_placeholder_values_are_misclassified",
            "required": True,
            "action": "do_not_execute_metric_until_6lz_plan_passes_and_6ma_implementation_checks_inputs",
        }],
        ["policy", "required", "action"],
    )

    write_csv(
        OUT_DECISION,
        [{
            "all_checks_passed": all_checks_passed,
            "diagnosis": diagnosis,
            "recommended_next_layer": recommended_next_layer,
            "recommended_path": recommended_path,
            "probability_metric_plan_created": probability_metric_plan_created,
            "probability_metric_execution_allowed_next": probability_metric_execution_allowed_next,
            "run_surface_gap_remains": run_surface_gap_remains,
            "layer_6_exit_recommended": False,
        }],
        [
            "all_checks_passed",
            "diagnosis",
            "recommended_next_layer",
            "recommended_path",
            "probability_metric_plan_created",
            "probability_metric_execution_allowed_next",
            "run_surface_gap_remains",
            "layer_6_exit_recommended",
        ],
    )

    write_csv(
        OUT_SAFETY,
        [{"boundary": key, "allowed": value} for key, value in SAFETY_BOUNDARIES.items()],
        ["boundary", "allowed"],
    )

    write_csv(
        OUT_RECOMMENDED,
        [{
            "recommended_next_layer": recommended_next_layer,
            "recommended_path": recommended_path,
        }],
        ["recommended_next_layer", "recommended_path"],
    )

    print(json.dumps({
        "layer": LAYER,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis,
        "recommended_next_layer": recommended_next_layer,
        "recommended_path": recommended_path,
        "blockers": blockers,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
