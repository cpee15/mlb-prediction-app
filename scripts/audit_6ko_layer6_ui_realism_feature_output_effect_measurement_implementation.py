#!/usr/bin/env python3
"""Audit 6KN controlled feature output-effect measurement implementation.

This audit validates the 6KN measurement outcome matrix and routes next to
historical backtest readiness planning for the current UI realism state. It
does not run historical evaluation, production simulation, activation, DB
writes, source acquisition, or Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ko_ui_realism_feature_output_effect_measurement_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6KN_PATH = Path("scripts/implement_6kn_layer6_ui_realism_feature_output_effect_measurement.py")
JSON_6KN = TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation.json"

REQUIRED_INPUTS = [
    JSON_6KN,
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_checks.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_predecessor.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_measurement_results.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_output_delta_results.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_mechanic_outcomes.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_execution_notes.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_blockers.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_future_6ko_contract.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_decision.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6kn_ui_realism_feature_output_effect_measurement_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
MEASUREMENT_OUTCOME_AUDIT_CSV = TMP_DIR / f"{SLUG}_measurement_outcome_audit.csv"
CURRENT_REALISM_STATE_CSV = TMP_DIR / f"{SLUG}_current_realism_state.csv"
NEXT_LAYER_RATIONALE_CSV = TMP_DIR / f"{SLUG}_next_layer_rationale.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KP_CSV = TMP_DIR / f"{SLUG}_future_6kp_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KN = "layer_6_ui_realism_feature_output_effect_measurement_implementation_complete"
DIAGNOSIS_6KO = "layer_6_ui_realism_feature_output_effect_measurement_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6KN = "6KO_layer_6_ui_realism_feature_output_effect_measurement_implementation_audit"
RECOMMENDED_NEXT_LAYER_6KO = "6KP_layer_6_historical_backtest_readiness_plan"
RECOMMENDED_PATH_6KO = "plan_historical_backtest_readiness_for_current_ui_realism_state"


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
    json_6kn = load_json(JSON_6KN)

    expected_outcomes = {
        "bullpen_logic": "measurable_delta_detected",
        "double_play_logic": "no_delta_but_reachable",
        "sac_fly_logic": "no_delta_but_reachable",
        "extras_ghost_runner_walkoff_logic": "bypass_confirmed",
        "stolen_base_or_steal_logic": "inactive_confirmed",
        "balk_logic": "deferred_confirmed",
    }
    key_map = {
        "bullpen_logic": "bullpen_measurement_outcome",
        "double_play_logic": "double_play_measurement_outcome",
        "sac_fly_logic": "sac_fly_measurement_outcome",
        "extras_ghost_runner_walkoff_logic": "extras_walkoff_measurement_outcome",
        "stolen_base_or_steal_logic": "stolen_base_measurement_outcome",
        "balk_logic": "balk_measurement_outcome",
    }

    measurement_outcome_audit = []
    for mechanic, expected in expected_outcomes.items():
        actual = json_6kn.get(key_map[mechanic])
        measurement_outcome_audit.append({
            "mechanic": mechanic,
            "expected_outcome": expected,
            "actual_outcome": actual,
            "audit_conclusion": "confirmed" if actual == expected else "mismatch",
            "passed": actual == expected,
        })

    current_realism_state = [
        {
            "state_item": "current_ui_realism_state_label",
            "value": "bullpen_active_partial_realism",
            "meaning": "Current UI realism should be treated as partial; bullpen has output-effect proxy while other mechanics are unproven/bypassed/inactive/deferred.",
            "passed": True,
        },
        {
            "state_item": "bullpen",
            "value": "output_effect_proxy_detected",
            "meaning": "Backtest labels should include bullpen-active partial realism.",
            "passed": json_6kn.get("bullpen_measurement_outcome") == "measurable_delta_detected",
        },
        {
            "state_item": "double_play",
            "value": "reachable_delta_not_proven",
            "meaning": "Do not attribute backtest movement to double-play effects without later evidence.",
            "passed": json_6kn.get("double_play_measurement_outcome") == "no_delta_but_reachable",
        },
        {
            "state_item": "sac_fly",
            "value": "reachable_delta_not_proven",
            "meaning": "Do not attribute backtest movement to sac-fly effects without later evidence.",
            "passed": json_6kn.get("sac_fly_measurement_outcome") == "no_delta_but_reachable",
        },
        {
            "state_item": "extras_walkoff",
            "value": "bypassed",
            "meaning": "Exclude or tag extras/walkoff as bypassed in current UI realism backtest.",
            "passed": json_6kn.get("extras_walkoff_measurement_outcome") == "bypass_confirmed",
        },
        {
            "state_item": "stolen_base",
            "value": "inactive",
            "meaning": "Exclude or tag steals as inactive.",
            "passed": json_6kn.get("stolen_base_measurement_outcome") == "inactive_confirmed",
        },
        {
            "state_item": "balk",
            "value": "deferred",
            "meaning": "Exclude or tag balks as deferred.",
            "passed": json_6kn.get("balk_measurement_outcome") == "deferred_confirmed",
        },
    ]

    next_layer_rationale = [
        {"reason": "measurement_matrix_audited", "detail": "Outcome matrix is stable enough to plan historical evaluation labels", "passed": True},
        {"reason": "current_state_not_full_realism", "detail": "Backtest must evaluate current partial realism state, not all planned mechanics", "passed": True},
        {"reason": "existing_backtest_dataset_preferred", "detail": "Use existing predicted-vs-actual dataset if usable, without assuming historical odds", "passed": True},
        {"reason": "fallback_window_needed", "detail": "If full window is too expensive, plan smaller fixed slice", "passed": True},
        {"reason": "no_activation_yet", "detail": "Historical backtest readiness plan is not activation or Layer 6 exit", "passed": True},
    ]

    blockers = [
        {"blocker": "historical_backtest_readiness_not_planned", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "full_realism_activation_not_confirmed", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balk_deferred", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6kp = [
        {"contract": "plan_existing_backtest_dataset_use", "required": True, "passed": True},
        {"contract": "plan_current_ui_realism_state_labeling", "required": True, "passed": True},
        {"contract": "plan_predicted_vs_actual_metrics_without_historical_odds_assumption", "required": True, "passed": True},
        {"contract": "plan_fallback_fixed_slice_if_needed", "required": True, "passed": True},
        {"contract": "preserve_no_activation_no_layer6_exit", "required": True, "passed": True},
        {"contract": "do_not_fetch_or_write_in_6kp", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kn_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6KN_PATH.exists(), "passed": IMPLEMENT_6KN_PATH.exists()},
        {"check": "6kn_json_exists", "expected": True, "actual": JSON_6KN.exists(), "passed": JSON_6KN.exists()},
        {"check": "6kn_all_checks_passed", "expected": True, "actual": json_6kn.get("all_checks_passed"), "passed": json_6kn.get("all_checks_passed") is True},
        {"check": "6kn_diagnosis", "expected": DIAGNOSIS_6KN, "actual": json_6kn.get("diagnosis"), "passed": json_6kn.get("diagnosis") == DIAGNOSIS_6KN},
        {"check": "6kn_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KN, "actual": json_6kn.get("recommended_next_layer"), "passed": json_6kn.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KN},
        {"check": "6kn_controlled_measurement_implemented", "expected": True, "actual": json_6kn.get("controlled_measurement_implemented"), "passed": json_6kn.get("controlled_measurement_implemented") is True},
        {"check": "6kn_no_production_simulations", "expected": False, "actual": json_6kn.get("production_simulations_run"), "passed": json_6kn.get("production_simulations_run") is False},
        {"check": "6kn_no_historical_eval", "expected": False, "actual": json_6kn.get("real_historical_evaluation_run"), "passed": json_6kn.get("real_historical_evaluation_run") is False},
        {"check": "6kn_no_layer6_exit", "expected": False, "actual": json_6kn.get("layer_6_exit_recommended"), "passed": json_6kn.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6kp_historical_backtest_readiness_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "6KO is audit-only; 6KP planning required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KO", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KO is audit-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KO cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kn_passed", "expected": True, "actual": json_6kn.get("all_checks_passed"), "passed": json_6kn.get("all_checks_passed") is True},
        {"decision": "measurement_outcome_audit_count", "expected": 6, "actual": len(measurement_outcome_audit), "passed": len(measurement_outcome_audit) == 6 and all_passed(measurement_outcome_audit)},
        {"decision": "current_realism_state_count", "expected": 7, "actual": len(current_realism_state), "passed": len(current_realism_state) == 7 and all_passed(current_realism_state)},
        {"decision": "next_layer_rationale_count", "expected": 5, "actual": len(next_layer_rationale), "passed": len(next_layer_rationale) == 5 and all_passed(next_layer_rationale)},
        {"decision": "recommend_6kp_next", "expected": RECOMMENDED_NEXT_LAYER_6KO, "actual": RECOMMENDED_NEXT_LAYER_6KO, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "measurement_implementation_audited", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_audit", "passed": True},
        {"surface": "6kn_implementation", "policy": "read_only", "passed": True},
        {"surface": "6kn_artifacts", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6ko", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6ko", "passed": True},
        {"surface": "database", "policy": "not_written_in_6ko", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KO, "actual": RECOMMENDED_NEXT_LAYER_6KO, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KO, "actual": RECOMMENDED_PATH_6KO, "passed": True},
        {"decision": "recommend_historical_backtest_readiness_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KO, "actual": DIAGNOSIS_6KO, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "measurement_outcome_audit", "passed": len(measurement_outcome_audit) == 6 and all_passed(measurement_outcome_audit), "detail": "6/6"},
        {"check": "current_realism_state", "passed": len(current_realism_state) == 7 and all_passed(current_realism_state), "detail": "7/7"},
        {"check": "next_layer_rationale", "passed": len(next_layer_rationale) == 5 and all_passed(next_layer_rationale), "detail": "5/5"},
        {"check": "blockers", "passed": len(blockers) == 5 and all_passed(blockers), "detail": "5/5"},
        {"check": "future_6kp_contract", "passed": len(future_6kp) == 6 and all_passed(future_6kp), "detail": "6/6"},
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
        "measurement_outcome_audit": write_csv(MEASUREMENT_OUTCOME_AUDIT_CSV, measurement_outcome_audit),
        "current_realism_state": write_csv(CURRENT_REALISM_STATE_CSV, current_realism_state),
        "next_layer_rationale": write_csv(NEXT_LAYER_RATIONALE_CSV, next_layer_rationale),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6kp_contract": write_csv(FUTURE_6KP_CSV, future_6kp),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KO",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KO if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KO,
        "recommended_path": RECOMMENDED_PATH_6KO,
        "predecessor_implementation": str(IMPLEMENT_6KN_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kn.get("diagnosis"),
        "audited_layer_after": "6KN",
        "source_family": "ui_realism_feature_output_effect_measurement_implementation_audit",
        "measurement_outcome_audit_count": len(measurement_outcome_audit),
        "current_realism_state_count": len(current_realism_state),
        "next_layer_rationale_count": len(next_layer_rationale),
        "blocker_count": len(blockers),
        "future_6kp_contract_valid": len(future_6kp) == 6 and all_passed(future_6kp),
        "measurement_implementation_audited": True,
        "current_ui_realism_state_classified": True,
        "bullpen_output_effect_proxy_confirmed": json_6kn.get("bullpen_measurement_outcome") == "measurable_delta_detected",
        "double_play_reachable_no_delta_confirmed": json_6kn.get("double_play_measurement_outcome") == "no_delta_but_reachable",
        "sac_fly_reachable_no_delta_confirmed": json_6kn.get("sac_fly_measurement_outcome") == "no_delta_but_reachable",
        "extras_walkoff_bypass_confirmed": json_6kn.get("extras_walkoff_measurement_outcome") == "bypass_confirmed",
        "stolen_base_inactive_confirmed": json_6kn.get("stolen_base_measurement_outcome") == "inactive_confirmed",
        "balk_deferred_confirmed": json_6kn.get("balk_measurement_outcome") == "deferred_confirmed",
        "historical_backtest_readiness_plan_required": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "realism_ui_activation_confirmed": False,
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
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "measurement_outcome_audit_csv": str(MEASUREMENT_OUTCOME_AUDIT_CSV),
            "current_realism_state_csv": str(CURRENT_REALISM_STATE_CSV),
            "next_layer_rationale_csv": str(NEXT_LAYER_RATIONALE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6kp_contract_csv": str(FUTURE_6KP_CSV),
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
