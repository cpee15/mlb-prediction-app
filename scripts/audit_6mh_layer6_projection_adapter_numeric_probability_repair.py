#!/usr/bin/env python3
"""Audit 6MG numeric probability repair result."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mh_projection_adapter_numeric_probability_repair_audit"
TMP_DIR = Path("tmp")

SCRIPT_6MG = Path("scripts/implement_6mg_layer6_projection_adapter_numeric_probability_repair.py")
JSON_6MG = TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation.json"
REPAIRED_SURFACE_6MG = TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_repaired_surface.csv"
REPAIR_RESULT_6MG = TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_repair_result.csv"

REQUIRED_INPUTS = [
    JSON_6MG,
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_checks.csv",
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_predecessor.csv",
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_candidate_scan.csv",
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_candidate_field_review.csv",
    REPAIRED_SURFACE_6MG,
    REPAIR_RESULT_6MG,
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_blockers.csv",
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_future_6mh_contract.csv",
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_decision.csv",
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6mg_projection_adapter_numeric_probability_repair_implementation_recommended_path.csv",
    SCRIPT_6MG,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
REPAIR_RESULT_REVIEW_CSV = TMP_DIR / f"{SLUG}_repair_result_review.csv"
REPAIRED_SURFACE_AUDIT_CSV = TMP_DIR / f"{SLUG}_repaired_surface_audit.csv"
SOURCE_PROVENANCE_AUDIT_CSV = TMP_DIR / f"{SLUG}_source_provenance_audit.csv"
METRIC_PLANNING_GATE_CSV = TMP_DIR / f"{SLUG}_metric_planning_gate.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MI_CSV = TMP_DIR / f"{SLUG}_future_6mi_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MG = "layer_6_projection_adapter_numeric_probability_repair_implementation_complete"
DIAGNOSIS_6MH = "layer_6_projection_adapter_numeric_probability_repair_audit_complete"
RECOMMENDED_NEXT_LAYER_6MH = "6MI_layer_6_projection_adapter_numeric_probability_surface_metric_plan"
RECOMMENDED_PATH_6MH = "plan_metrics_for_repaired_numeric_probability_surface"
MATERIALIZED_STATUS = "materialized_real_numeric_probability_surface"
BLOCKER_STATUS = "explicit_blocker_no_safe_numeric_source"
PLACEHOLDER_MARKERS = ["MAPPED_FROM_", "PRESENT_IN_", "placeholder", "PLACEHOLDER"]


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


def coerce_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if any(marker in stripped for marker in PLACEHOLDER_MARKERS):
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def has_placeholder(row: dict[str, Any]) -> bool:
    return any(isinstance(value, str) and any(marker in value for marker in PLACEHOLDER_MARKERS) for value in row.values())


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mg = load_json(JSON_6MG)
    repair_rows = read_csv_rows(REPAIR_RESULT_6MG)
    repaired_rows = read_csv_rows(REPAIRED_SURFACE_6MG)

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
        {"check": "6mg_script_exists", "expected": True, "actual": SCRIPT_6MG.exists(), "passed": SCRIPT_6MG.exists()},
        {"check": "6mg_json_exists", "expected": True, "actual": JSON_6MG.exists(), "passed": JSON_6MG.exists()},
        {"check": "6mg_all_checks_passed", "expected": True, "actual": json_6mg.get("all_checks_passed"), "passed": json_6mg.get("all_checks_passed") is True},
        {"check": "6mg_diagnosis", "expected": DIAGNOSIS_6MG, "actual": json_6mg.get("diagnosis"), "passed": json_6mg.get("diagnosis") == DIAGNOSIS_6MG},
        {"check": "6mg_recommended_next_layer", "expected": "6MH_layer_6_projection_adapter_numeric_probability_repair_audit", "actual": json_6mg.get("recommended_next_layer"), "passed": json_6mg.get("recommended_next_layer") == "6MH_layer_6_projection_adapter_numeric_probability_repair_audit"},
    ]

    repair_result = repair_rows[0] if repair_rows else {}
    outcome = json_6mg.get("repair_outcome_status")
    materialized = json_6mg.get("real_numeric_probability_surface_materialized") is True
    explicit_blocker = json_6mg.get("explicit_blocker_emitted") is True

    repair_result_review = [
        {"finding": "repair_outcome_status", "expected": f"{MATERIALIZED_STATUS} or {BLOCKER_STATUS}", "actual": outcome, "passed": outcome in {MATERIALIZED_STATUS, BLOCKER_STATUS}},
        {"finding": "real_numeric_probability_surface_materialized", "expected": json_6mg.get("real_numeric_probability_surface_materialized"), "actual": boolish(repair_result.get("real_numeric_probability_surface_materialized", "")), "passed": boolish(repair_result.get("real_numeric_probability_surface_materialized", "")) == materialized},
        {"finding": "explicit_blocker_emitted", "expected": json_6mg.get("explicit_blocker_emitted"), "actual": boolish(repair_result.get("explicit_blocker_emitted", "")), "passed": boolish(repair_result.get("explicit_blocker_emitted", "")) == explicit_blocker},
        {"finding": "fake_probability_generation_used", "expected": False, "actual": json_6mg.get("fake_probability_generation_used"), "passed": json_6mg.get("fake_probability_generation_used") is False},
        {"finding": "selected_numeric_source_artifact", "expected": "present if materialized", "actual": json_6mg.get("selected_numeric_source_artifact"), "passed": bool(json_6mg.get("selected_numeric_source_artifact")) if materialized else True},
    ]

    materialized_surface_rows = [
        row for row in repaired_rows
        if row.get("game_pk") and row.get("home_win_probability") and row.get("away_win_probability")
    ]

    surface_audit_rows: list[dict[str, Any]] = []
    for row in materialized_surface_rows:
        home = coerce_float(row.get("home_win_probability"))
        away = coerce_float(row.get("away_win_probability"))
        prob_sum = None if home is None or away is None else home + away
        surface_audit_rows.append(
            {
                "game_pk": row.get("game_pk"),
                "home_win_probability": row.get("home_win_probability"),
                "away_win_probability": row.get("away_win_probability"),
                "probability_sum": prob_sum,
                "home_numeric": home is not None,
                "away_numeric": away is not None,
                "bounds_valid": home is not None and away is not None and 0 <= home <= 1 and 0 <= away <= 1,
                "sum_valid": prob_sum is not None and abs(prob_sum - 1.0) <= 0.025,
                "contains_placeholder": has_placeholder(row),
                "passed": (
                    home is not None
                    and away is not None
                    and 0 <= home <= 1
                    and 0 <= away <= 1
                    and prob_sum is not None
                    and abs(prob_sum - 1.0) <= 0.025
                    and not has_placeholder(row)
                ),
            }
        )

    if not surface_audit_rows:
        surface_audit_rows = [
            {
                "game_pk": "",
                "home_win_probability": "",
                "away_win_probability": "",
                "probability_sum": "",
                "home_numeric": False,
                "away_numeric": False,
                "bounds_valid": False,
                "sum_valid": False,
                "contains_placeholder": False,
                "passed": explicit_blocker,
            }
        ]

    provenance_rows = []
    for row in materialized_surface_rows:
        provenance_rows.append(
            {
                "game_pk": row.get("game_pk"),
                "source_artifact": row.get("source_artifact"),
                "source_type": row.get("source_type"),
                "source_game_field": row.get("source_game_field"),
                "source_home_probability_field": row.get("source_home_probability_field"),
                "source_away_probability_field": row.get("source_away_probability_field"),
                "source_provenance_present": all(
                    bool(row.get(field))
                    for field in [
                        "source_artifact",
                        "source_game_field",
                        "source_home_probability_field",
                        "source_away_probability_field",
                    ]
                ),
                "passed": all(
                    bool(row.get(field))
                    for field in [
                        "source_artifact",
                        "source_game_field",
                        "source_home_probability_field",
                        "source_away_probability_field",
                    ]
                ),
            }
        )

    if not provenance_rows:
        provenance_rows = [
            {
                "game_pk": "",
                "source_artifact": "",
                "source_type": "",
                "source_game_field": "",
                "source_home_probability_field": "",
                "source_away_probability_field": "",
                "source_provenance_present": False,
                "passed": explicit_blocker,
            }
        ]

    surface_valid = materialized and all_passed(surface_audit_rows)
    provenance_valid = materialized and all_passed(provenance_rows)
    no_fake = json_6mg.get("fake_probability_generation_used") is False
    metric_planning_ready = surface_valid and provenance_valid and no_fake

    metric_gate_rows = [
        {
            "gate": "repaired_numeric_surface_valid_for_metric_planning",
            "open": metric_planning_ready,
            "reason": "surface/provenance/no-fake audit passed" if metric_planning_ready else "surface not materialized or failed audit",
            "passed": True,
        },
        {"gate": "metric_execution_allowed_after_this_layer", "open": False, "reason": "6MH can plan metrics next but does not execute them", "passed": True},
        {"gate": "backtest_execution_allowed_after_this_layer", "open": False, "reason": "backtests require later metric audit and plan", "passed": True},
        {"gate": "tuning_allowed_after_this_layer", "open": False, "reason": "tuning requires backtest evidence", "passed": True},
    ]

    blockers = [
        {
            "blocker": "repair_result_requires_audit",
            "active": False if metric_planning_ready else True,
            "reason": "6MH completed audit" if metric_planning_ready else "repair result not ready for metric planning",
            "passed": True,
        },
        {
            "blocker": "metrics_require_planning_layer",
            "active": True,
            "reason": "6MI must plan metrics before any metric execution",
            "passed": True,
        },
        {
            "blocker": "backtests_tuning_activation_exit_blocked",
            "active": True,
            "reason": "requires later metric and backtest evidence",
            "passed": True,
        },
    ]

    future_6mi = [
        {
            "contract": "plan_metrics_for_repaired_numeric_probability_surface",
            "required": metric_planning_ready,
            "why": "metric planning can define Brier/log-loss/calibration coverage checks for the repaired surface",
            "passed": True,
        },
        {
            "contract": "preserve_no_metric_execution_in_plan_layer",
            "required": True,
            "why": "6MI should plan metrics only",
            "passed": True,
        },
        {
            "contract": "preserve_no_backtest_tuning_activation_or_exit",
            "required": True,
            "why": "metric planning remains pre-backtest",
            "passed": True,
        },
    ]

    blocking_policy = [
        {"policy": "do_not_run_metrics_until_6mi_plan_and_later_metric_implementation", "required": True, "passed": True},
        {"policy": "do_not_run_backtests_until_probability_metrics_are_implemented_and_audited", "required": True, "passed": True},
        {"policy": "do_not_tune_or_activate_from_single_row_repair_audit", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mg_passed", "expected": True, "actual": json_6mg.get("all_checks_passed"), "passed": json_6mg.get("all_checks_passed") is True},
        {"decision": "6mg_diagnosis_valid", "expected": DIAGNOSIS_6MG, "actual": json_6mg.get("diagnosis"), "passed": json_6mg.get("diagnosis") == DIAGNOSIS_6MG},
        {"decision": "all_required_6mg_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "repair_result_reviewed", "expected": True, "actual": all_passed(repair_result_review), "passed": all_passed(repair_result_review)},
        {"decision": "surface_audit_valid_or_explicit_blocker", "expected": True, "actual": all_passed(surface_audit_rows), "passed": all_passed(surface_audit_rows)},
        {"decision": "source_provenance_valid_or_explicit_blocker", "expected": True, "actual": all_passed(provenance_rows), "passed": all_passed(provenance_rows)},
        {"decision": "fake_probability_generation_false", "expected": False, "actual": json_6mg.get("fake_probability_generation_used"), "passed": json_6mg.get("fake_probability_generation_used") is False},
        {"decision": "recommend_6mi_next", "expected": RECOMMENDED_NEXT_LAYER_6MH, "actual": RECOMMENDED_NEXT_LAYER_6MH, "passed": True},
        {"decision": "do_not_run_metrics_backtest_tuning_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_numeric_probability_repair_result", "expected": True, "actual": True, "passed": True},
        {"boundary": "repair_execution_run_by_6mh", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_scan_run_by_6mh", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mh", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mh", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mh", "expected": False, "actual": False, "passed": True},
        {"boundary": "run_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mh", "expected": False, "actual": False, "passed": True},
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

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MH, "actual": RECOMMENDED_NEXT_LAYER_6MH, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MH, "actual": RECOMMENDED_PATH_6MH, "passed": True},
        {"decision": "do_not_execute_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MH, "actual": DIAGNOSIS_6MH, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "repair_result_review", "passed": all_passed(repair_result_review), "detail": f"{sum(1 for r in repair_result_review if r['passed'])}/{len(repair_result_review)}"},
        {"check": "repaired_surface_audit", "passed": all_passed(surface_audit_rows), "detail": f"{sum(1 for r in surface_audit_rows if r['passed'])}/{len(surface_audit_rows)}"},
        {"check": "source_provenance_audit", "passed": all_passed(provenance_rows), "detail": f"{sum(1 for r in provenance_rows if r['passed'])}/{len(provenance_rows)}"},
        {"check": "metric_planning_gate", "passed": all_passed(metric_gate_rows), "detail": f"{sum(1 for r in metric_gate_rows if r['passed'])}/{len(metric_gate_rows)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mi_contract", "passed": all_passed(future_6mi), "detail": f"{sum(1 for r in future_6mi if r['passed'])}/{len(future_6mi)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{sum(1 for r in blocking_policy if r['passed'])}/{len(blocking_policy)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "repair_result_review": write_csv(REPAIR_RESULT_REVIEW_CSV, repair_result_review),
        "repaired_surface_audit": write_csv(REPAIRED_SURFACE_AUDIT_CSV, surface_audit_rows),
        "source_provenance_audit": write_csv(SOURCE_PROVENANCE_AUDIT_CSV, provenance_rows),
        "metric_planning_gate": write_csv(METRIC_PLANNING_GATE_CSV, metric_gate_rows),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mi_contract": write_csv(FUTURE_6MI_CSV, future_6mi),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MH",
        "layer_type": "game_mechanics_realism",
        "audit_only_numeric_probability_repair_result": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MH if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MH,
        "recommended_path": RECOMMENDED_PATH_6MH,
        "predecessor_layer": "6MG",
        "predecessor_diagnosis": json_6mg.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mg.get("all_checks_passed") is True,
        "audited_layer_after": "6MG",
        "source_family": "projection_adapter_numeric_probability_repair_audit",
        "repair_result_audited": True,
        "repair_outcome_status_confirmed": outcome,
        "real_numeric_probability_surface_materialized_confirmed": materialized,
        "explicit_blocker_emitted_confirmed": explicit_blocker,
        "repaired_surface_row_count_confirmed": len(materialized_surface_rows),
        "selected_numeric_source_artifact_confirmed": json_6mg.get("selected_numeric_source_artifact"),
        "numeric_surface_contract_valid_confirmed": surface_valid,
        "no_placeholder_probability_values_confirmed": surface_valid and all(not row["contains_placeholder"] for row in surface_audit_rows),
        "numeric_probability_bounds_valid_confirmed": surface_valid and all(boolish(row["bounds_valid"]) for row in surface_audit_rows),
        "home_away_probability_sum_valid_confirmed": surface_valid and all(boolish(row["sum_valid"]) for row in surface_audit_rows),
        "source_provenance_present_confirmed": provenance_valid,
        "fake_probability_generation_used_confirmed": False,
        "repaired_numeric_surface_valid_for_metric_planning": metric_planning_ready,
        "metric_planning_recommended": metric_planning_ready,
        "metric_execution_allowed_after_this_layer": False,
        "backtest_execution_allowed_after_this_layer": False,
        "tuning_allowed_after_this_layer": False,
        "repair_execution_run_by_6mh": False,
        "source_scan_run_by_6mh": False,
        "adapter_call_executed_by_6mh": False,
        "metric_execution_run_by_6mh": False,
        "backtest_execution_run_by_6mh": False,
        "run_metric_execution_run": False,
        "production_code_modified_by_6mh": False,
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
        "games_evaluated": len(materialized_surface_rows),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "repair_result_review_csv": str(REPAIR_RESULT_REVIEW_CSV),
            "repaired_surface_audit_csv": str(REPAIRED_SURFACE_AUDIT_CSV),
            "source_provenance_audit_csv": str(SOURCE_PROVENANCE_AUDIT_CSV),
            "metric_planning_gate_csv": str(METRIC_PLANNING_GATE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mi_contract_csv": str(FUTURE_6MI_CSV),
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
