#!/usr/bin/env python3
"""Plan local historical actuals and moneyline source integration."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan"
TMP_DIR = Path("tmp")

SCRIPT_6MK = Path("scripts/audit_6mk_layer6_projection_adapter_numeric_probability_surface_metrics.py")
JSON_6MK = TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit.json"

REQUIRED_INPUTS = [
    JSON_6MK,
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_checks.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_predecessor.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_surface_health_audit.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_distribution_health_audit.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_model_vs_actual_audit.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_model_vs_market_audit.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_missing_data_audit.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_total_scope_audit.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_blockers.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_future_6ml_contract.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_blocking_policy.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_decision.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6mk_projection_adapter_numeric_probability_surface_metric_audit_recommended_path.csv",
    SCRIPT_6MK,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
ACTUALS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_actuals_contract.csv"
MONEYLINE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_moneyline_contract.csv"
ALIGNMENT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_alignment_contract.csv"
MARKET_CONVERSION_CSV = TMP_DIR / f"{SLUG}_market_conversion_contract.csv"
QUALITY_POLICY_CSV = TMP_DIR / f"{SLUG}_quality_policy.csv"
FAIL_CLOSED_POLICY_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
TOTAL_SCOPE_POLICY_CSV = TMP_DIR / f"{SLUG}_total_scope_policy.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MM_CSV = TMP_DIR / f"{SLUG}_future_6mm_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MK = "layer_6_projection_adapter_numeric_probability_surface_metric_audit_complete"
DIAGNOSIS_6ML = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_plan_complete"
RECOMMENDED_NEXT_LAYER_6ML = "6MM_layer_6_projection_adapter_historical_actuals_and_moneyline_source_scan"
RECOMMENDED_PATH_6ML = "scan_local_historical_actuals_and_moneyline_source_candidates_readonly"


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
    json_6mk = load_json(JSON_6MK)

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
        {"check": "6mk_script_exists", "expected": True, "actual": SCRIPT_6MK.exists(), "passed": SCRIPT_6MK.exists()},
        {"check": "6mk_json_exists", "expected": True, "actual": JSON_6MK.exists(), "passed": JSON_6MK.exists()},
        {"check": "6mk_all_checks_passed", "expected": True, "actual": json_6mk.get("all_checks_passed"), "passed": json_6mk.get("all_checks_passed") is True},
        {"check": "6mk_diagnosis", "expected": DIAGNOSIS_6MK, "actual": json_6mk.get("diagnosis"), "passed": json_6mk.get("diagnosis") == DIAGNOSIS_6MK},
        {"check": "6mk_recommended_next_layer", "expected": "6ML_layer_6_projection_adapter_historical_actuals_and_moneyline_source_plan", "actual": json_6mk.get("recommended_next_layer"), "passed": json_6mk.get("recommended_next_layer") == "6ML_layer_6_projection_adapter_historical_actuals_and_moneyline_source_plan"},
        {"check": "metric_artifacts_valid_for_historical_source_planning", "expected": True, "actual": json_6mk.get("metric_artifacts_valid_for_historical_source_planning"), "passed": json_6mk.get("metric_artifacts_valid_for_historical_source_planning") is True},
        {"check": "historical_actuals_source_required_next", "expected": True, "actual": json_6mk.get("historical_actuals_source_required_next"), "passed": json_6mk.get("historical_actuals_source_required_next") is True},
        {"check": "historical_moneyline_source_required_next", "expected": True, "actual": json_6mk.get("historical_moneyline_source_required_next"), "passed": json_6mk.get("historical_moneyline_source_required_next") is True},
    ]

    actuals_contract = [
        {"field": "game_pk", "required": True, "accepted_aliases": "game_id, mlb_game_id, event_id", "purpose": "join actual result to probability surface", "passed": True},
        {"field": "game_date", "required": True, "accepted_aliases": "date, official_date", "purpose": "temporal validation and duplicate resolution", "passed": True},
        {"field": "home_team", "required": True, "accepted_aliases": "home_team_abbrev, home_abbrev, home", "purpose": "home/away alignment", "passed": True},
        {"field": "away_team", "required": True, "accepted_aliases": "away_team_abbrev, away_abbrev, away", "purpose": "home/away alignment", "passed": True},
        {"field": "home_score", "required": True, "accepted_aliases": "home_runs, home_final_score", "purpose": "derive actual winner if binary missing", "passed": True},
        {"field": "away_score", "required": True, "accepted_aliases": "away_runs, away_final_score", "purpose": "derive actual winner if binary missing", "passed": True},
        {"field": "home_win_binary", "required": True, "accepted_aliases": "home_win, actual_home_win, winner_side", "purpose": "Brier/log-loss/calibration target", "passed": True},
        {"field": "source_artifact", "required": True, "accepted_aliases": "source_file, provenance", "purpose": "source traceability", "passed": True},
    ]

    moneyline_contract = [
        {"field": "game_pk", "required": True, "accepted_aliases": "game_id, mlb_game_id, event_id", "purpose": "join odds to probability surface", "passed": True},
        {"field": "game_date", "required": True, "accepted_aliases": "date, odds_date", "purpose": "temporal validation and duplicate resolution", "passed": True},
        {"field": "home_team", "required": True, "accepted_aliases": "home_team_abbrev, home_abbrev, home", "purpose": "home/away alignment", "passed": True},
        {"field": "away_team", "required": True, "accepted_aliases": "away_team_abbrev, away_abbrev, away", "purpose": "home/away alignment", "passed": True},
        {"field": "home_moneyline", "required": True, "accepted_aliases": "home_ml, moneyline_home, home_close_moneyline", "purpose": "market implied home probability", "passed": True},
        {"field": "away_moneyline", "required": False, "accepted_aliases": "away_ml, moneyline_away, away_close_moneyline", "purpose": "optional consistency check", "passed": True},
        {"field": "odds_timestamp_or_type", "required": False, "accepted_aliases": "snapshot_type, open_close, market_time", "purpose": "open/close distinction if available", "passed": True},
        {"field": "sportsbook_or_source", "required": False, "accepted_aliases": "book, source_book", "purpose": "duplicate/source policy", "passed": True},
        {"field": "source_artifact", "required": True, "accepted_aliases": "source_file, provenance", "purpose": "source traceability", "passed": True},
    ]

    alignment_contract = [
        {"rule": "primary_join_key", "requirement": "game_pk must join probability surface to actuals and moneyline odds", "passed": True},
        {"rule": "home_away_identity", "requirement": "home/away teams must match probability surface after aliases are normalized", "passed": True},
        {"rule": "actual_binary_derivation", "requirement": "home_win_binary may be direct or derived from scores; ties/unknowns fail closed", "passed": True},
        {"rule": "odds_side_alignment", "requirement": "home_moneyline must represent home side after home/away validation", "passed": True},
        {"rule": "duplicate_actual_resolution", "requirement": "duplicate actual rows require identical winner or explicit blocker", "passed": True},
        {"rule": "duplicate_moneyline_resolution", "requirement": "duplicate odds rows require open/close/source policy before execution", "passed": True},
        {"rule": "provenance_required", "requirement": "joined actual/odds rows must carry source_artifact", "passed": True},
    ]

    market_conversion_contract = [
        {"conversion": "american_negative", "formula": "abs(odds) / (abs(odds) + 100)", "example": "-150 -> 0.6000", "passed": True},
        {"conversion": "american_positive", "formula": "100 / (odds + 100)", "example": "+120 -> 0.4545", "passed": True},
        {"conversion": "zero_or_missing_odds", "formula": "invalid -> blocker", "example": "0 or blank -> missing/invalid odds blocker", "passed": True},
        {"conversion": "vig_handling_initial_policy", "formula": "raw implied probability first; de-vig only in later audited layer", "example": "market comparison readiness before pricing model", "passed": True},
        {"conversion": "open_close_policy", "formula": "if open/close both available, preserve both; default comparison blocked until policy chosen", "example": "closing-line value is not executed in source plan", "passed": True},
    ]

    quality_policy = [
        {"policy": "missing_actuals", "decision": "emit missing_actuals blocker; do not generate outcomes", "passed": True},
        {"policy": "missing_moneyline_odds", "decision": "emit missing_moneyline blocker; do not generate odds", "passed": True},
        {"policy": "invalid_moneyline_odds", "decision": "odds equal zero/non-numeric are invalid and blocked", "passed": True},
        {"policy": "team_mismatch", "decision": "home/away mismatch blocks joined metric execution", "passed": True},
        {"policy": "duplicate_actual_rows", "decision": "allow only if winner identical; otherwise block", "passed": True},
        {"policy": "duplicate_moneyline_rows", "decision": "block until source/open-close policy can choose row", "passed": True},
        {"policy": "provenance_missing", "decision": "block historical source use until provenance present", "passed": True},
    ]

    fail_closed_policy = [
        {"missing_or_invalid_input": "actual outcomes", "behavior": "write readiness/blocker artifact; no Brier/log-loss/calibration execution", "passed": True},
        {"missing_or_invalid_input": "historical moneyline odds", "behavior": "write readiness/blocker artifact; no market comparison execution", "passed": True},
        {"missing_or_invalid_input": "total surface", "behavior": "keep totals out of scope until separate surface traced/audited", "passed": True},
        {"missing_or_invalid_input": "team alignment mismatch", "behavior": "write explicit alignment blocker", "passed": True},
        {"missing_or_invalid_input": "duplicate unresolved sources", "behavior": "write duplicate-source blocker", "passed": True},
    ]

    total_scope_policy = [
        {"scope_item": "moneyline_win_probability_metrics", "in_scope": True, "reason": "current repaired surface is win probability", "passed": True},
        {"scope_item": "actual_outcome_scoring_metrics", "in_scope": True, "reason": "requires local actuals source scan/implementation first", "passed": True},
        {"scope_item": "model_vs_market_moneyline_metrics", "in_scope": True, "reason": "requires local moneyline odds source scan/implementation first", "passed": True},
        {"scope_item": "total_runs_metrics", "in_scope": False, "reason": "requires separate total-runs surface trace/audit", "passed": True},
        {"scope_item": "over_under_metrics", "in_scope": False, "reason": "requires separate over/under probability surface trace/audit", "passed": True},
        {"scope_item": "historical_total_odds_comparison", "in_scope": False, "reason": "requires both total surface and total odds source", "passed": True},
    ]

    allowed_next = [
        {"operation": "readonly_local_actuals_source_candidate_scan", "allowed_next": True, "scope": "local files only; no acquisition", "passed": True},
        {"operation": "readonly_local_moneyline_source_candidate_scan", "allowed_next": True, "scope": "local files only; no acquisition", "passed": True},
        {"operation": "source_candidate_schema_review", "allowed_next": True, "scope": "headers/row counts/provenance only", "passed": True},
    ]

    forbidden_next = [
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
        {"blocker": "local_actuals_source_not_scanned", "active": True, "reason": "actuals contract planned but source candidates not scanned", "passed": True},
        {"blocker": "local_moneyline_source_not_scanned", "active": True, "reason": "moneyline contract planned but source candidates not scanned", "passed": True},
        {"blocker": "historical_source_not_implemented_or_audited", "active": True, "reason": "source scan/implementation/audit required before metric scoring", "passed": True},
        {"blocker": "backtests_tuning_activation_exit_blocked", "active": True, "reason": "requires historical source integration and audited historical metric execution", "passed": True},
    ]

    future_6mm = [
        {"contract": "scan_local_actuals_candidates_readonly", "required": True, "why": "find local actual outcome candidate files without acquisition", "passed": True},
        {"contract": "scan_local_moneyline_candidates_readonly", "required": True, "why": "find local moneyline odds candidate files without acquisition", "passed": True},
        {"contract": "report_candidate_schema_fit", "required": True, "why": "score headers against actuals/moneyline contracts", "passed": True},
        {"contract": "do_not_fetch_external_sources", "required": True, "why": "6MM remains readonly local scan", "passed": True},
        {"contract": "do_not_execute_metrics_or_backtests", "required": True, "why": "candidate scan is pre-integration", "passed": True},
    ]

    blocking_policy = [
        {"policy": "do_not_generate_fake_actual_outcomes", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_moneyline_odds", "required": True, "passed": True},
        {"policy": "do_not_score_probabilities_until_sources_are_implemented_and_audited", "required": True, "passed": True},
        {"policy": "do_not_run_backtests_until historical metrics are implemented and audited", "required": True, "passed": True},
        {"policy": "do_not_tune_or_activate_from_source_plan", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6mk_passed", "expected": True, "actual": json_6mk.get("all_checks_passed"), "passed": json_6mk.get("all_checks_passed") is True},
        {"decision": "6mk_diagnosis_valid", "expected": DIAGNOSIS_6MK, "actual": json_6mk.get("diagnosis"), "passed": json_6mk.get("diagnosis") == DIAGNOSIS_6MK},
        {"decision": "all_required_6mk_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "metric_artifacts_valid_for_historical_source_planning_confirmed", "expected": True, "actual": json_6mk.get("metric_artifacts_valid_for_historical_source_planning"), "passed": json_6mk.get("metric_artifacts_valid_for_historical_source_planning") is True},
        {"decision": "historical_actuals_source_required_confirmed", "expected": True, "actual": json_6mk.get("historical_actuals_source_required_next"), "passed": json_6mk.get("historical_actuals_source_required_next") is True},
        {"decision": "historical_moneyline_source_required_confirmed", "expected": True, "actual": json_6mk.get("historical_moneyline_source_required_next"), "passed": json_6mk.get("historical_moneyline_source_required_next") is True},
        {"decision": "actuals_contract_created", "expected": True, "actual": True, "passed": all_passed(actuals_contract)},
        {"decision": "moneyline_contract_created", "expected": True, "actual": True, "passed": all_passed(moneyline_contract)},
        {"decision": "alignment_contract_created", "expected": True, "actual": True, "passed": all_passed(alignment_contract)},
        {"decision": "market_conversion_contract_created", "expected": True, "actual": True, "passed": all_passed(market_conversion_contract)},
        {"decision": "quality_and_fail_closed_policy_created", "expected": True, "actual": True, "passed": all_passed(quality_policy) and all_passed(fail_closed_policy)},
        {"decision": "totals_scope_policy_created", "expected": True, "actual": True, "passed": all_passed(total_scope_policy)},
        {"decision": "recommend_6mm_next", "expected": RECOMMENDED_NEXT_LAYER_6ML, "actual": RECOMMENDED_NEXT_LAYER_6ML, "passed": True},
        {"decision": "do_not_run_source_scan_metric_backtest_tuning_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only_historical_actuals_moneyline_sources", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_acquisition_performed_by_6ml", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_scan_run_by_6ml", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6ml", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6ml", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6ml", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6ml", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6ML, "actual": RECOMMENDED_NEXT_LAYER_6ML, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6ML, "actual": RECOMMENDED_PATH_6ML, "passed": True},
        {"decision": "allow_readonly_local_source_scan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6ML, "actual": DIAGNOSIS_6ML, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "actuals_contract", "passed": all_passed(actuals_contract), "detail": f"{sum(1 for r in actuals_contract if r['passed'])}/{len(actuals_contract)}"},
        {"check": "moneyline_contract", "passed": all_passed(moneyline_contract), "detail": f"{sum(1 for r in moneyline_contract if r['passed'])}/{len(moneyline_contract)}"},
        {"check": "alignment_contract", "passed": all_passed(alignment_contract), "detail": f"{sum(1 for r in alignment_contract if r['passed'])}/{len(alignment_contract)}"},
        {"check": "market_conversion_contract", "passed": all_passed(market_conversion_contract), "detail": f"{sum(1 for r in market_conversion_contract if r['passed'])}/{len(market_conversion_contract)}"},
        {"check": "quality_policy", "passed": all_passed(quality_policy), "detail": f"{sum(1 for r in quality_policy if r['passed'])}/{len(quality_policy)}"},
        {"check": "fail_closed_policy", "passed": all_passed(fail_closed_policy), "detail": f"{sum(1 for r in fail_closed_policy if r['passed'])}/{len(fail_closed_policy)}"},
        {"check": "total_scope_policy", "passed": all_passed(total_scope_policy), "detail": f"{sum(1 for r in total_scope_policy if r['passed'])}/{len(total_scope_policy)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mm_contract", "passed": all_passed(future_6mm), "detail": f"{sum(1 for r in future_6mm if r['passed'])}/{len(future_6mm)}"},
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
        "actuals_contract": write_csv(ACTUALS_CONTRACT_CSV, actuals_contract),
        "moneyline_contract": write_csv(MONEYLINE_CONTRACT_CSV, moneyline_contract),
        "alignment_contract": write_csv(ALIGNMENT_CONTRACT_CSV, alignment_contract),
        "market_conversion_contract": write_csv(MARKET_CONVERSION_CSV, market_conversion_contract),
        "quality_policy": write_csv(QUALITY_POLICY_CSV, quality_policy),
        "fail_closed_policy": write_csv(FAIL_CLOSED_POLICY_CSV, fail_closed_policy),
        "total_scope_policy": write_csv(TOTAL_SCOPE_POLICY_CSV, total_scope_policy),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mm_contract": write_csv(FUTURE_6MM_CSV, future_6mm),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6ML",
        "layer_type": "game_mechanics_realism",
        "planning_only_historical_actuals_moneyline_sources": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6ML if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6ML,
        "recommended_path": RECOMMENDED_PATH_6ML,
        "predecessor_layer": "6MK",
        "predecessor_diagnosis": json_6mk.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mk.get("all_checks_passed") is True,
        "planned_layer_after": "6MK",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_plan",
        "metric_artifacts_valid_for_historical_source_planning_confirmed": json_6mk.get("metric_artifacts_valid_for_historical_source_planning") is True,
        "historical_actuals_source_required_confirmed": json_6mk.get("historical_actuals_source_required_next") is True,
        "historical_moneyline_source_required_confirmed": json_6mk.get("historical_moneyline_source_required_next") is True,
        "actuals_contract_created": True,
        "moneyline_contract_created": True,
        "alignment_contract_created": True,
        "market_conversion_contract_created": True,
        "quality_policy_created": True,
        "fail_closed_policy_created": True,
        "total_scope_policy_created": True,
        "source_scan_allowed_next": True,
        "data_acquisition_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "actuals_required_field_count": len([row for row in actuals_contract if row["required"] is True]),
        "moneyline_required_field_count": len([row for row in moneyline_contract if row["required"] is True]),
        "alignment_required_field_count": len(alignment_contract),
        "source_acquisition_performed_by_6ml": False,
        "source_scan_run_by_6ml": False,
        "metric_execution_run_by_6ml": False,
        "backtest_execution_run_by_6ml": False,
        "adapter_call_executed_by_6ml": False,
        "production_code_modified_by_6ml": False,
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
            "actuals_contract_csv": str(ACTUALS_CONTRACT_CSV),
            "moneyline_contract_csv": str(MONEYLINE_CONTRACT_CSV),
            "alignment_contract_csv": str(ALIGNMENT_CONTRACT_CSV),
            "market_conversion_contract_csv": str(MARKET_CONVERSION_CSV),
            "quality_policy_csv": str(QUALITY_POLICY_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_POLICY_CSV),
            "total_scope_policy_csv": str(TOTAL_SCOPE_POLICY_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mm_contract_csv": str(FUTURE_6MM_CSV),
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
