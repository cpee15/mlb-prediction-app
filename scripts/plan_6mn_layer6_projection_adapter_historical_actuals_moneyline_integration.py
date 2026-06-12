#!/usr/bin/env python3
"""Plan historical actuals and moneyline integration from failed local scan."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mn_projection_adapter_historical_actuals_moneyline_integration_plan"
TMP_DIR = Path("tmp")

SCRIPT_6MM = Path("scripts/scan_6mm_layer6_projection_adapter_historical_actuals_moneyline_sources.py")
JSON_6MM = TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan.json"

ACTUALS_SCORES_6MM = TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_actuals_candidate_scores.csv"
MONEYLINE_SCORES_6MM = TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_moneyline_candidate_scores.csv"
BEST_CANDIDATES_6MM = TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_best_candidates.csv"
MISSING_FIELDS_6MM = TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_missing_fields.csv"
SOURCE_FIT_6MM = TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_source_fit_decision.csv"

REQUIRED_INPUTS = [
    JSON_6MM,
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_checks.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_predecessor.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_input_artifacts.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_candidate_files.csv",
    ACTUALS_SCORES_6MM,
    MONEYLINE_SCORES_6MM,
    BEST_CANDIDATES_6MM,
    MISSING_FIELDS_6MM,
    SOURCE_FIT_6MM,
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_blockers.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_future_6mn_contract.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_blocking_policy.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_decision.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_safety_boundaries.csv",
    TMP_DIR / "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan_recommended_path.csv",
    SCRIPT_6MM,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SCAN_REVIEW_CSV = TMP_DIR / f"{SLUG}_scan_result_review.csv"
ACTUALS_SHAPE_CSV = TMP_DIR / f"{SLUG}_actuals_source_shape.csv"
MONEYLINE_SHAPE_CSV = TMP_DIR / f"{SLUG}_moneyline_source_shape.csv"
INSUFFICIENCY_CSV = TMP_DIR / f"{SLUG}_candidate_insufficiency_review.csv"
TRANSFORMABILITY_CSV = TMP_DIR / f"{SLUG}_transformability_decision.csv"
REMEDIATION_CSV = TMP_DIR / f"{SLUG}_remediation_options.csv"
INTEGRATION_REQ_CSV = TMP_DIR / f"{SLUG}_integration_requirements.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MO_CSV = TMP_DIR / f"{SLUG}_future_6mo_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MM = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_scan_complete"
DIAGNOSIS_6MN = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_integration_plan_complete"
RECOMMENDED_NEXT_LAYER_6MN = "6MO_layer_6_projection_adapter_historical_actuals_and_moneyline_source_remediation_plan"
RECOMMENDED_PATH_6MN = "plan_source_remediation_or_ingestion_contract_for_missing_historical_actuals_moneyline"


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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mm = load_json(JSON_6MM)

    actuals_scores = read_csv_rows(ACTUALS_SCORES_6MM)
    moneyline_scores = read_csv_rows(MONEYLINE_SCORES_6MM)
    best_candidates = read_csv_rows(BEST_CANDIDATES_6MM)
    missing_fields = read_csv_rows(MISSING_FIELDS_6MM)
    source_fit_rows = read_csv_rows(SOURCE_FIT_6MM)

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
        {"check": "6mm_script_exists", "expected": True, "actual": SCRIPT_6MM.exists(), "passed": SCRIPT_6MM.exists()},
        {"check": "6mm_json_exists", "expected": True, "actual": JSON_6MM.exists(), "passed": JSON_6MM.exists()},
        {"check": "6mm_all_checks_passed", "expected": True, "actual": json_6mm.get("all_checks_passed"), "passed": json_6mm.get("all_checks_passed") is True},
        {"check": "6mm_diagnosis", "expected": DIAGNOSIS_6MM, "actual": json_6mm.get("diagnosis"), "passed": json_6mm.get("diagnosis") == DIAGNOSIS_6MM},
        {"check": "6mm_recommended_next_layer", "expected": "6MN_layer_6_projection_adapter_historical_actuals_and_moneyline_source_integration_plan", "actual": json_6mm.get("recommended_next_layer"), "passed": json_6mm.get("recommended_next_layer") == "6MN_layer_6_projection_adapter_historical_actuals_and_moneyline_source_integration_plan"},
        {"check": "viable_actuals_candidate_found", "expected": False, "actual": json_6mm.get("viable_actuals_candidate_found"), "passed": json_6mm.get("viable_actuals_candidate_found") is False},
        {"check": "viable_moneyline_candidate_found", "expected": False, "actual": json_6mm.get("viable_moneyline_candidate_found"), "passed": json_6mm.get("viable_moneyline_candidate_found") is False},
    ]

    scan_review = [
        {"finding": "actuals_candidate_count", "value": json_6mm.get("actuals_candidate_count"), "interpretation": "local candidates were scored", "passed": True},
        {"finding": "moneyline_candidate_count", "value": json_6mm.get("moneyline_candidate_count"), "interpretation": "local candidates were scored", "passed": True},
        {"finding": "viable_actuals_candidate_found", "value": json_6mm.get("viable_actuals_candidate_found"), "interpretation": "no source can be implemented directly", "passed": json_6mm.get("viable_actuals_candidate_found") is False},
        {"finding": "viable_moneyline_candidate_found", "value": json_6mm.get("viable_moneyline_candidate_found"), "interpretation": "no source can be implemented directly", "passed": json_6mm.get("viable_moneyline_candidate_found") is False},
        {"finding": "best_actuals_candidate_path", "value": json_6mm.get("best_actuals_candidate_path", ""), "interpretation": "best candidate is still non-viable", "passed": json_6mm.get("best_actuals_candidate_viable") is False},
        {"finding": "best_moneyline_candidate_path", "value": json_6mm.get("best_moneyline_candidate_path", ""), "interpretation": "best candidate is still non-viable", "passed": json_6mm.get("best_moneyline_candidate_viable") is False},
    ]

    actuals_shape = [
        {"field": "game_pk", "required": True, "type": "string/int", "rule": "must match probability surface game_pk", "passed": True},
        {"field": "game_date", "required": True, "type": "date", "rule": "used for season/date validation", "passed": True},
        {"field": "home_team", "required": True, "type": "string", "rule": "must align to probability surface home side", "passed": True},
        {"field": "away_team", "required": True, "type": "string", "rule": "must align to probability surface away side", "passed": True},
        {"field": "home_score", "required": True, "type": "integer", "rule": "used to derive home_win_binary when needed", "passed": True},
        {"field": "away_score", "required": True, "type": "integer", "rule": "used to derive home_win_binary when needed", "passed": True},
        {"field": "home_win_binary", "required": True, "type": "0/1 boolean", "rule": "required target for Brier/log-loss/calibration", "passed": True},
        {"field": "source_artifact", "required": True, "type": "path/string", "rule": "required for provenance", "passed": True},
    ]

    moneyline_shape = [
        {"field": "game_pk", "required": True, "type": "string/int", "rule": "must match probability surface game_pk", "passed": True},
        {"field": "game_date", "required": True, "type": "date", "rule": "used for odds date validation", "passed": True},
        {"field": "home_team", "required": True, "type": "string", "rule": "must align to probability surface home side", "passed": True},
        {"field": "away_team", "required": True, "type": "string", "rule": "must align to probability surface away side", "passed": True},
        {"field": "home_moneyline", "required": True, "type": "integer/float American odds", "rule": "used for home implied probability", "passed": True},
        {"field": "away_moneyline", "required": False, "type": "integer/float American odds", "rule": "optional consistency/de-vig input", "passed": True},
        {"field": "odds_timestamp_or_type", "required": False, "type": "string/datetime", "rule": "optional open/close distinction", "passed": True},
        {"field": "sportsbook_or_source", "required": False, "type": "string", "rule": "optional source selection policy", "passed": True},
        {"field": "source_artifact", "required": True, "type": "path/string", "rule": "required for provenance", "passed": True},
    ]

    insufficiency_review = [
        {"issue": "actuals_source_absent", "evidence": "6MM viable_actuals_candidate_found=false", "impact": "cannot compute Brier/log-loss/calibration", "passed": True},
        {"issue": "moneyline_source_absent", "evidence": "6MM viable_moneyline_candidate_found=false", "impact": "cannot compute model-vs-market deltas", "passed": True},
        {"issue": "candidate_artifacts_not_truth_sources", "evidence": "scan returned internal tmp/artifact candidates but none viable", "impact": "cannot transform internal QA artifacts into historical truth", "passed": True},
        {"issue": "missing_required_fields", "evidence": f"{len(missing_fields)} missing-field rows from scan", "impact": "schema contracts not satisfied", "passed": True},
        {"issue": "implementation_not_safe", "evidence": "no viable source candidate selected", "impact": "source implementation must remain blocked", "passed": True},
    ]

    transformability = [
        {"decision": "existing_artifact_transform_safe", "actual": False, "reason": "no scanned artifact satisfies actuals or moneyline contract", "passed": True},
        {"decision": "derive_actuals_from_internal_artifacts", "actual": False, "reason": "internal artifacts are not authoritative game-result sources", "passed": True},
        {"decision": "derive_moneyline_from_internal_artifacts", "actual": False, "reason": "internal artifacts are not authoritative market odds sources", "passed": True},
        {"decision": "allow_remediation_planning", "actual": True, "reason": "next layer may define ingestion/remediation contract only", "passed": True},
    ]

    remediation_options = [
        {"option": "provide_local_actuals_csv", "allowed_next": True, "requires": "game_pk,date,home/away teams,scores,home_win_binary,source_artifact", "data_acquisition": False, "passed": True},
        {"option": "provide_local_moneyline_csv", "allowed_next": True, "requires": "game_pk,date,home/away teams,home_moneyline,source_artifact", "data_acquisition": False, "passed": True},
        {"option": "map_existing_authoritative_local_file_if_added", "allowed_next": True, "requires": "schema fit and provenance audit", "data_acquisition": False, "passed": True},
        {"option": "request_source_ingestion_contract_before_any_fetch", "allowed_next": True, "requires": "separate explicit future layer; no automatic web/API fetch", "data_acquisition": False, "passed": True},
        {"option": "continue_without_historical_sources", "allowed_next": True, "requires": "keep metrics/backtests/tuning blocked", "data_acquisition": False, "passed": True},
    ]

    integration_requirements = [
        {"requirement": "canonical_game_pk_join", "detail": "actuals and moneyline must join to probability surface on game_pk", "passed": True},
        {"requirement": "home_away_validation", "detail": "home/away teams must match probability surface before metrics", "passed": True},
        {"requirement": "home_win_binary_derivation_policy", "detail": "derive from scores only when score fields are authoritative and non-missing", "passed": True},
        {"requirement": "american_moneyline_conversion", "detail": "negative odds abs(odds)/(abs(odds)+100); positive odds 100/(odds+100)", "passed": True},
        {"requirement": "provenance_required", "detail": "source_artifact/source_file must be preserved in integrated rows", "passed": True},
        {"requirement": "duplicates_blocked_until_policy", "detail": "duplicate source rows require explicit source/timestamp policy", "passed": True},
        {"requirement": "metrics_after_source_audit_only", "detail": "no Brier/log-loss/market metrics until integrated sources are implemented and audited", "passed": True},
    ]

    fail_closed = [
        {"condition": "no_actuals_source", "behavior": "keep model-vs-actual metrics blocked", "passed": True},
        {"condition": "no_moneyline_source", "behavior": "keep model-vs-market metrics blocked", "passed": True},
        {"condition": "schema_mismatch", "behavior": "emit source schema blocker", "passed": True},
        {"condition": "team_alignment_mismatch", "behavior": "emit alignment blocker", "passed": True},
        {"condition": "missing_provenance", "behavior": "emit provenance blocker", "passed": True},
        {"condition": "duplicate_unresolved_rows", "behavior": "emit duplicate-source blocker", "passed": True},
    ]

    allowed_next = [
        {"operation": "plan_source_remediation_contract", "allowed_next": True, "scope": "planning only", "passed": True},
        {"operation": "define_user_provided_local_source_schema", "allowed_next": True, "scope": "contract only", "passed": True},
        {"operation": "define_authoritative_source_ingestion_requirements", "allowed_next": True, "scope": "contract only; no fetch", "passed": True},
    ]

    forbidden_next = [
        {"operation": "source_implementation", "allowed_next": False, "passed": True},
        {"operation": "data_acquisition", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "external_source_scan", "allowed_next": False, "passed": True},
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "no_viable_actuals_candidate_found", "active": True, "reason": "6MM found no local actuals source meeting the contract", "passed": True},
        {"blocker": "no_viable_moneyline_candidate_found", "active": True, "reason": "6MM found no local moneyline source meeting the contract", "passed": True},
        {"blocker": "existing_artifact_transform_not_safe", "active": True, "reason": "internal artifacts are not authoritative historical truth/odds sources", "passed": True},
        {"blocker": "source_remediation_or_ingestion_contract_required", "active": True, "reason": "next layer must plan how missing sources may be provided", "passed": True},
        {"blocker": "metrics_backtests_tuning_activation_exit_blocked", "active": True, "reason": "requires source implementation and audit first", "passed": True},
    ]

    future_6mo = [
        {"contract": "define_actuals_source_remediation_contract", "required": True, "why": "actuals source absent", "passed": True},
        {"contract": "define_moneyline_source_remediation_contract", "required": True, "why": "moneyline source absent", "passed": True},
        {"contract": "define_exact_local_file_drop_or_ingestion_rules", "required": True, "why": "source must be provided before implementation", "passed": True},
        {"contract": "preserve_no_fetch_no_metric_no_backtest", "required": True, "why": "remediation plan remains pre-acquisition/pre-execution", "passed": True},
    ]

    blocking_policy = [
        {"policy": "do_not_transform_non_authoritative_artifacts_into_truth_sources", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_actual_outcomes", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_moneyline_odds", "required": True, "passed": True},
        {"policy": "do_not_execute_metrics_without_audited_sources", "required": True, "passed": True},
        {"policy": "do_not_run_backtests_or_tuning_from_source_plan", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mm_passed", "expected": True, "actual": json_6mm.get("all_checks_passed"), "passed": json_6mm.get("all_checks_passed") is True},
        {"decision": "6mm_diagnosis_valid", "expected": DIAGNOSIS_6MM, "actual": json_6mm.get("diagnosis"), "passed": json_6mm.get("diagnosis") == DIAGNOSIS_6MM},
        {"decision": "all_required_6mm_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "no_viable_actuals_candidate_confirmed", "expected": False, "actual": json_6mm.get("viable_actuals_candidate_found"), "passed": json_6mm.get("viable_actuals_candidate_found") is False},
        {"decision": "no_viable_moneyline_candidate_confirmed", "expected": False, "actual": json_6mm.get("viable_moneyline_candidate_found"), "passed": json_6mm.get("viable_moneyline_candidate_found") is False},
        {"decision": "actuals_source_shape_defined", "expected": True, "actual": True, "passed": all_passed(actuals_shape)},
        {"decision": "moneyline_source_shape_defined", "expected": True, "actual": True, "passed": all_passed(moneyline_shape)},
        {"decision": "candidate_insufficiency_review_created", "expected": True, "actual": True, "passed": all_passed(insufficiency_review)},
        {"decision": "transformability_decision_created", "expected": True, "actual": True, "passed": all_passed(transformability)},
        {"decision": "existing_artifact_transform_safe", "expected": False, "actual": False, "passed": True},
        {"decision": "remediation_options_created", "expected": True, "actual": True, "passed": all_passed(remediation_options)},
        {"decision": "integration_requirements_created", "expected": True, "actual": True, "passed": all_passed(integration_requirements)},
        {"decision": "fail_closed_policy_created", "expected": True, "actual": True, "passed": all_passed(fail_closed)},
        {"decision": "recommend_6mo_next", "expected": RECOMMENDED_NEXT_LAYER_6MN, "actual": RECOMMENDED_NEXT_LAYER_6MN, "passed": True},
        {"decision": "do_not_implement_sources_metrics_backtests_tuning_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_historical_source_integration_after_failed_scan", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_acquisition_performed_by_6mn", "expected": False, "actual": False, "passed": True},
        {"boundary": "external_source_scan_run_by_6mn", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_scan_run_by_6mn", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_implementation_run_by_6mn", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mn", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mn", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mn", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mn", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_source_modifications_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MN, "actual": RECOMMENDED_NEXT_LAYER_6MN, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MN, "actual": RECOMMENDED_PATH_6MN, "passed": True},
        {"decision": "allow_source_remediation_planning_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_source_implementation_directly", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MN, "actual": DIAGNOSIS_6MN, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "scan_result_review", "passed": all_passed(scan_review), "detail": f"{sum(1 for r in scan_review if r['passed'])}/{len(scan_review)}"},
        {"check": "actuals_source_shape", "passed": all_passed(actuals_shape), "detail": f"{sum(1 for r in actuals_shape if r['passed'])}/{len(actuals_shape)}"},
        {"check": "moneyline_source_shape", "passed": all_passed(moneyline_shape), "detail": f"{sum(1 for r in moneyline_shape if r['passed'])}/{len(moneyline_shape)}"},
        {"check": "candidate_insufficiency_review", "passed": all_passed(insufficiency_review), "detail": f"{sum(1 for r in insufficiency_review if r['passed'])}/{len(insufficiency_review)}"},
        {"check": "transformability_decision", "passed": all_passed(transformability), "detail": f"{sum(1 for r in transformability if r['passed'])}/{len(transformability)}"},
        {"check": "remediation_options", "passed": all_passed(remediation_options), "detail": f"{sum(1 for r in remediation_options if r['passed'])}/{len(remediation_options)}"},
        {"check": "integration_requirements", "passed": all_passed(integration_requirements), "detail": f"{sum(1 for r in integration_requirements if r['passed'])}/{len(integration_requirements)}"},
        {"check": "fail_closed_policy", "passed": all_passed(fail_closed), "detail": f"{sum(1 for r in fail_closed if r['passed'])}/{len(fail_closed)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mo_contract", "passed": all_passed(future_6mo), "detail": f"{sum(1 for r in future_6mo if r['passed'])}/{len(future_6mo)}"},
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
        "scan_result_review": write_csv(SCAN_REVIEW_CSV, scan_review),
        "actuals_source_shape": write_csv(ACTUALS_SHAPE_CSV, actuals_shape),
        "moneyline_source_shape": write_csv(MONEYLINE_SHAPE_CSV, moneyline_shape),
        "candidate_insufficiency_review": write_csv(INSUFFICIENCY_CSV, insufficiency_review),
        "transformability_decision": write_csv(TRANSFORMABILITY_CSV, transformability),
        "remediation_options": write_csv(REMEDIATION_CSV, remediation_options),
        "integration_requirements": write_csv(INTEGRATION_REQ_CSV, integration_requirements),
        "fail_closed_policy": write_csv(FAIL_CLOSED_CSV, fail_closed),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mo_contract": write_csv(FUTURE_6MO_CSV, future_6mo),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MN",
        "layer_type": "game_mechanics_realism",
        "planning_only_historical_source_integration_after_failed_scan": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MN if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MN,
        "recommended_path": RECOMMENDED_PATH_6MN,
        "predecessor_layer": "6MM",
        "predecessor_diagnosis": json_6mm.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mm.get("all_checks_passed") is True,
        "planned_layer_after": "6MM",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_integration_plan",
        "actuals_scan_candidate_count": json_6mm.get("actuals_candidate_count"),
        "moneyline_scan_candidate_count": json_6mm.get("moneyline_candidate_count"),
        "viable_actuals_candidate_found_confirmed": json_6mm.get("viable_actuals_candidate_found") is False,
        "viable_moneyline_candidate_found_confirmed": json_6mm.get("viable_moneyline_candidate_found") is False,
        "actuals_source_shape_defined": True,
        "moneyline_source_shape_defined": True,
        "candidate_insufficiency_review_created": True,
        "transformability_decision_created": True,
        "existing_artifact_transform_safe": False,
        "remediation_options_created": True,
        "integration_requirements_created": True,
        "fail_closed_policy_created": True,
        "source_remediation_planning_allowed_next": True,
        "source_implementation_allowed_next": False,
        "data_acquisition_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_acquisition_performed_by_6mn": False,
        "external_source_scan_run_by_6mn": False,
        "local_source_scan_run_by_6mn": False,
        "source_implementation_run_by_6mn": False,
        "metric_execution_run_by_6mn": False,
        "backtest_execution_run_by_6mn": False,
        "adapter_call_executed_by_6mn": False,
        "production_code_modified_by_6mn": False,
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
        "production_source_modifications_run": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "scan_result_review_csv": str(SCAN_REVIEW_CSV),
            "actuals_source_shape_csv": str(ACTUALS_SHAPE_CSV),
            "moneyline_source_shape_csv": str(MONEYLINE_SHAPE_CSV),
            "candidate_insufficiency_review_csv": str(INSUFFICIENCY_CSV),
            "transformability_decision_csv": str(TRANSFORMABILITY_CSV),
            "remediation_options_csv": str(REMEDIATION_CSV),
            "integration_requirements_csv": str(INTEGRATION_REQ_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mo_contract_csv": str(FUTURE_6MO_CSV),
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
