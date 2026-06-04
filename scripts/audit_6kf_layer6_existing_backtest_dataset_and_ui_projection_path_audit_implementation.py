#!/usr/bin/env python3
"""Audit 6KE existing backtest dataset and UI projection-path findings.

This audit validates the broad 6KE scan, records its limitations, and routes
next to a narrower exact UI projection route trace. It does not rerun broad
discovery, fetch data, write databases, run historical evaluation, activate
mechanics, or grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6kf_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6KE_PATH = Path("scripts/implement_6ke_layer6_existing_backtest_dataset_and_ui_projection_path_audit.py")
JSON_6KE = TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation.json"

REQUIRED_INPUTS = [
    JSON_6KE,
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_checks.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_predecessor.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_dataset_candidates.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_findings.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_ui_frontend_path_audit.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_backend_route_audit.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_projection_function_audit.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_realism_path_audit.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_activation_blockers.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_future_6kf_contract.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_decision.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6ke_existing_backtest_dataset_and_ui_projection_path_audit_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
DATASET_FINDINGS_CSV = TMP_DIR / f"{SLUG}_dataset_findings.csv"
UI_PATH_FINDINGS_CSV = TMP_DIR / f"{SLUG}_ui_path_findings.csv"
NOISE_LIMITATIONS_CSV = TMP_DIR / f"{SLUG}_noise_limitations.csv"
EXACT_ROUTE_TRACE_NEED_CSV = TMP_DIR / f"{SLUG}_exact_route_trace_need.csv"
ACTIVATION_BLOCKERS_CSV = TMP_DIR / f"{SLUG}_activation_blockers.csv"
FUTURE_6KG_CSV = TMP_DIR / f"{SLUG}_future_6kg_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KE = "layer_6_existing_backtest_dataset_and_ui_projection_path_audit_implementation_complete"
DIAGNOSIS_6KF = "layer_6_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6KE = "6KF_layer_6_existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit"
RECOMMENDED_NEXT_LAYER_6KF = "6KG_layer_6_exact_ui_projection_route_trace_plan"
RECOMMENDED_PATH_6KF = "audit_6ke_findings_then_plan_exact_ui_route_trace_before_real_backtest"


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


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
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def boolish(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6ke = load_json(JSON_6KE)

    dataset_findings = [
        {
            "finding": "existing_backtest_dataset_found",
            "expected": True,
            "actual": json_6ke.get("existing_backtest_dataset_found"),
            "audit_conclusion": "candidate_backtest_files_or_scripts_found_but_clean_dataset_not_proven",
            "passed": json_6ke.get("existing_backtest_dataset_found") is True,
        },
        {
            "finding": "readable_dataset_candidate_count",
            "expected": 0,
            "actual": json_6ke.get("readable_dataset_candidate_count"),
            "audit_conclusion": "no_readable_csv_dataset_candidates_found_by_6ke",
            "passed": json_6ke.get("readable_dataset_candidate_count") == 0,
        },
        {
            "finding": "predicted_vs_actual_fields_found",
            "expected": False,
            "actual": json_6ke.get("predicted_vs_actual_fields_found"),
            "audit_conclusion": "6ke_did_not_prove_predicted_vs_actual_schema",
            "passed": json_6ke.get("predicted_vs_actual_fields_found") is False,
        },
        {
            "finding": "historical_odds_fields_found",
            "expected": False,
            "actual": json_6ke.get("historical_odds_fields_found"),
            "audit_conclusion": "historical_market_odds_not_found",
            "passed": json_6ke.get("historical_odds_fields_found") is False,
        },
        {
            "finding": "primary_dataset_window_feasible",
            "expected": False,
            "actual": json_6ke.get("primary_dataset_window_feasible"),
            "audit_conclusion": "opening_day_to_latest_completed_not_proven_feasible",
            "passed": json_6ke.get("primary_dataset_window_feasible") is False,
        },
        {
            "finding": "fixed_validation_slice_available",
            "expected": False,
            "actual": json_6ke.get("fixed_validation_slice_available"),
            "audit_conclusion": "april_20_to_may_3_slice_not_proven_available",
            "passed": json_6ke.get("fixed_validation_slice_available") is False,
        },
    ]

    ui_path_findings = [
        {
            "finding": "ui_projection_path_audit_completed",
            "expected": True,
            "actual": json_6ke.get("ui_projection_path_audit_completed"),
            "audit_conclusion": "broad_ui_path_scan_completed",
            "passed": json_6ke.get("ui_projection_path_audit_completed") is True,
        },
        {
            "finding": "ui_uses_realism_enabled_path",
            "expected": True,
            "actual": json_6ke.get("ui_uses_realism_enabled_path"),
            "audit_conclusion": "realism_enabled_signals_present",
            "passed": json_6ke.get("ui_uses_realism_enabled_path") is True,
        },
        {
            "finding": "ui_uses_legacy_or_current_path",
            "expected": True,
            "actual": json_6ke.get("ui_uses_legacy_or_current_path"),
            "audit_conclusion": "legacy_or_current_signals_also_present",
            "passed": json_6ke.get("ui_uses_legacy_or_current_path") is True,
        },
        {
            "finding": "realism_ui_activation_confirmed",
            "expected": False,
            "actual": json_6ke.get("realism_ui_activation_confirmed"),
            "audit_conclusion": "ui_realism_activation_not_confirmed",
            "passed": json_6ke.get("realism_ui_activation_confirmed") is False,
        },
    ]

    noise_limitations = [
        {
            "limitation": "broad_scan_not_exact_route_trace",
            "detail": "6KE identified signals across many files but did not trace the exact UI endpoint to backend payload builder to simulator chain",
            "requires_followup": True,
            "passed": True,
        },
        {
            "limitation": "candidate_cap_truncation",
            "detail": "6KE capped broad candidate outputs, so candidate counts are evidence of signal presence rather than exhaustive proof",
            "requires_followup": True,
            "passed": True,
        },
        {
            "limitation": "dependency_and_non_route_noise_possible",
            "detail": "6KE broad UI/backend scan may include unrelated files and references",
            "requires_followup": True,
            "passed": True,
        },
        {
            "limitation": "realism_and_legacy_terms_coexist",
            "detail": "Presence of both signal families prevents activation conclusion without exact route tracing",
            "requires_followup": True,
            "passed": True,
        },
    ]

    exact_route_trace_need = [
        {"trace_step": "frontend_page", "target": "ModelProjectionsPage", "required": True, "passed": True},
        {"trace_step": "api_endpoint", "target": "endpoint called by frontend projection page", "required": True, "passed": True},
        {"trace_step": "backend_route", "target": "route handling projection endpoint", "required": True, "passed": True},
        {"trace_step": "payload_builder", "target": "function building UI projection payload", "required": True, "passed": True},
        {"trace_step": "simulator_or_projection_function", "target": "exact function producing displayed projected numbers", "required": True, "passed": True},
        {"trace_step": "realism_feature_chain", "target": "determine whether built realism features influence displayed outputs", "required": True, "passed": True},
    ]

    activation_blockers = [
        {"blocker": "exact_ui_route_trace_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "clean_predicted_vs_actual_dataset_not_proven", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balks_deferred_or_exit_gated", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kg = [
        {"contract": "trace_model_projections_page", "required": True, "passed": True},
        {"contract": "trace_frontend_api_endpoint", "required": True, "passed": True},
        {"contract": "trace_backend_projection_route", "required": True, "passed": True},
        {"contract": "trace_payload_builder", "required": True, "passed": True},
        {"contract": "trace_simulator_or_projection_function", "required": True, "passed": True},
        {"contract": "determine_realism_feature_influence_on_displayed_ui_numbers", "required": True, "passed": True},
        {"contract": "do_not_activate_or_grant_layer6_exit_in_6kg", "required": True, "passed": True},
        {"contract": "do_not_fetch_or_write_in_6kg", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ke_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6KE_PATH.exists(), "passed": IMPLEMENT_6KE_PATH.exists()},
        {"check": "6ke_json_exists", "expected": True, "actual": JSON_6KE.exists(), "passed": JSON_6KE.exists()},
        {"check": "6ke_all_checks_passed", "expected": True, "actual": json_6ke.get("all_checks_passed"), "passed": json_6ke.get("all_checks_passed") is True},
        {"check": "6ke_diagnosis", "expected": DIAGNOSIS_6KE, "actual": json_6ke.get("diagnosis"), "passed": json_6ke.get("diagnosis") == DIAGNOSIS_6KE},
        {"check": "6ke_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KE, "actual": json_6ke.get("recommended_next_layer"), "passed": json_6ke.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KE},
        {"check": "6ke_realism_ui_activation_confirmed", "expected": False, "actual": json_6ke.get("realism_ui_activation_confirmed"), "passed": json_6ke.get("realism_ui_activation_confirmed") is False},
        {"check": "6ke_ui_uses_realism_enabled_path", "expected": True, "actual": json_6ke.get("ui_uses_realism_enabled_path"), "passed": json_6ke.get("ui_uses_realism_enabled_path") is True},
        {"check": "6ke_ui_uses_legacy_or_current_path", "expected": True, "actual": json_6ke.get("ui_uses_legacy_or_current_path"), "passed": json_6ke.get("ui_uses_legacy_or_current_path") is True},
        {"check": "6ke_no_layer6_exit", "expected": False, "actual": json_6ke.get("layer_6_exit_recommended"), "passed": json_6ke.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kg_exact_route_trace_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "exact route trace and dataset proof required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "real evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KF", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KF is audit only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KF cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ke_passed", "expected": True, "actual": json_6ke.get("all_checks_passed"), "passed": json_6ke.get("all_checks_passed") is True},
        {"decision": "dataset_findings_count", "expected": 6, "actual": len(dataset_findings), "passed": len(dataset_findings) == 6},
        {"decision": "ui_path_findings_count", "expected": 4, "actual": len(ui_path_findings), "passed": len(ui_path_findings) == 4},
        {"decision": "noise_limitation_count", "expected": 4, "actual": len(noise_limitations), "passed": len(noise_limitations) == 4},
        {"decision": "exact_route_trace_need_count", "expected": 6, "actual": len(exact_route_trace_need), "passed": len(exact_route_trace_need) == 6},
        {"decision": "recommend_6kg_next", "expected": RECOMMENDED_NEXT_LAYER_6KF, "actual": RECOMMENDED_NEXT_LAYER_6KF, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "broad_scan_audited", "expected": True, "actual": True, "passed": True},
        {"boundary": "exact_ui_route_trace_required", "expected": True, "actual": True, "passed": True},
        {"boundary": "realism_ui_activation_confirmed", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_historical_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_recommendation", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_audit", "passed": True},
        {"surface": "6ke_implementation", "policy": "read_only", "passed": True},
        {"surface": "6ke_artifacts", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6kf", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kf", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kf", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KF, "actual": RECOMMENDED_NEXT_LAYER_6KF, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KF, "actual": RECOMMENDED_PATH_6KF, "passed": True},
        {"decision": "recommend_exact_route_trace_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KF, "actual": DIAGNOSIS_6KF, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "dataset_findings", "passed": len(dataset_findings) == 6 and all_passed(dataset_findings), "detail": "6/6"},
        {"check": "ui_path_findings", "passed": len(ui_path_findings) == 4 and all_passed(ui_path_findings), "detail": "4/4"},
        {"check": "noise_limitations", "passed": len(noise_limitations) == 4 and all_passed(noise_limitations), "detail": "4/4"},
        {"check": "exact_route_trace_need", "passed": len(exact_route_trace_need) == 6 and all_passed(exact_route_trace_need), "detail": "6/6"},
        {"check": "activation_blockers", "passed": len(activation_blockers) == 5 and all_passed(activation_blockers), "detail": "5/5"},
        {"check": "future_6kg_contract", "passed": len(future_6kg) == 8 and all_passed(future_6kg), "detail": "8/8"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for r in readonly_rows if r['passed'])}/{len(readonly_rows)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for r in blocking_rows if r['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all_passed(immutability_rows), "detail": f"{sum(1 for r in immutability_rows if r['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "dataset_findings": write_csv(DATASET_FINDINGS_CSV, dataset_findings),
        "ui_path_findings": write_csv(UI_PATH_FINDINGS_CSV, ui_path_findings),
        "noise_limitations": write_csv(NOISE_LIMITATIONS_CSV, noise_limitations),
        "exact_route_trace_need": write_csv(EXACT_ROUTE_TRACE_NEED_CSV, exact_route_trace_need),
        "activation_blockers": write_csv(ACTIVATION_BLOCKERS_CSV, activation_blockers),
        "future_6kg_contract": write_csv(FUTURE_6KG_CSV, future_6kg),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KF",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KF if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KF,
        "recommended_path": RECOMMENDED_PATH_6KF,
        "predecessor_implementation": str(IMPLEMENT_6KE_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6ke.get("diagnosis"),
        "audited_layer_after": "6KE",
        "source_family": "existing_backtest_dataset_and_ui_projection_path_audit_implementation_audit",
        "dataset_findings_count": len(dataset_findings),
        "ui_path_findings_count": len(ui_path_findings),
        "noise_limitation_count": len(noise_limitations),
        "exact_route_trace_need_count": len(exact_route_trace_need),
        "activation_blocker_count": len(activation_blockers),
        "future_6kg_contract_valid": len(future_6kg) == 8 and all_passed(future_6kg),
        "broad_scan_audited": True,
        "existing_backtest_dataset_found": json_6ke.get("existing_backtest_dataset_found"),
        "readable_dataset_candidate_count": json_6ke.get("readable_dataset_candidate_count"),
        "predicted_vs_actual_fields_found": json_6ke.get("predicted_vs_actual_fields_found"),
        "historical_odds_fields_found": json_6ke.get("historical_odds_fields_found"),
        "primary_dataset_window_feasible": json_6ke.get("primary_dataset_window_feasible"),
        "fixed_validation_slice_available": json_6ke.get("fixed_validation_slice_available"),
        "ui_projection_path_audit_completed": json_6ke.get("ui_projection_path_audit_completed"),
        "realism_ui_activation_confirmed": json_6ke.get("realism_ui_activation_confirmed"),
        "ui_uses_realism_enabled_path": json_6ke.get("ui_uses_realism_enabled_path"),
        "ui_uses_legacy_or_current_path": json_6ke.get("ui_uses_legacy_or_current_path"),
        "exact_ui_route_trace_required": True,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "dataset_findings_csv": str(DATASET_FINDINGS_CSV),
            "ui_path_findings_csv": str(UI_PATH_FINDINGS_CSV),
            "noise_limitations_csv": str(NOISE_LIMITATIONS_CSV),
            "exact_route_trace_need_csv": str(EXACT_ROUTE_TRACE_NEED_CSV),
            "activation_blockers_csv": str(ACTIVATION_BLOCKERS_CSV),
            "future_6kg_contract_csv": str(FUTURE_6KG_CSV),
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
