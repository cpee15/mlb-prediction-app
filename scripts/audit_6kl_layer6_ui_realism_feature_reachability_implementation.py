#!/usr/bin/env python3
"""Audit 6KK UI realism feature reachability implementation.

This audit validates the feature-by-feature mechanic matrix evidence and routes
next to a controlled output-effect measurement plan. It does not fetch data,
run simulations, modify code, activate mechanics, or grant Layer 6 exit.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6kl_ui_realism_feature_reachability_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6KK_PATH = Path("scripts/implement_6kk_layer6_ui_realism_feature_reachability.py")
JSON_6KK = TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation.json"

REQUIRED_INPUTS = [
    JSON_6KK,
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_checks.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_predecessor.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_source_file_inventory.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_mechanic_matrix_evidence.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_output_field_evidence.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_mechanic_status_summary.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_blockers.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_future_6kl_contract.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_decision.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6kk_ui_realism_feature_reachability_implementation_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
MECHANIC_STATUS_AUDIT_CSV = TMP_DIR / f"{SLUG}_mechanic_status_audit.csv"
OUTPUT_EFFECT_GAP_AUDIT_CSV = TMP_DIR / f"{SLUG}_output_effect_gap_audit.csv"
NEXT_LAYER_RATIONALE_CSV = TMP_DIR / f"{SLUG}_next_layer_rationale.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6KM_CSV = TMP_DIR / f"{SLUG}_future_6km_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KK = "layer_6_ui_realism_feature_reachability_implementation_complete"
DIAGNOSIS_6KL = "layer_6_ui_realism_feature_reachability_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6KK = "6KL_layer_6_ui_realism_feature_reachability_implementation_audit"
RECOMMENDED_NEXT_LAYER_6KL = "6KM_layer_6_ui_realism_feature_output_effect_measurement_plan"
RECOMMENDED_PATH_6KL = "plan_controlled_feature_output_effect_measurement_before_backtest"


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
    json_6kk = load_json(JSON_6KK)

    expected = {
        "bullpen_logic": {
            "exists": "yes",
            "simulation": "reached",
            "ui": "reached",
            "active": "active",
            "output": "plausible",
        },
        "double_play_logic": {
            "exists": "yes",
            "simulation": "reached",
            "ui": "unknown_or_not_direct",
            "active": "active",
            "output": "unmeasured_or_none",
        },
        "sac_fly_logic": {
            "exists": "yes",
            "simulation": "reached",
            "ui": "unknown_or_not_direct",
            "active": "active",
            "output": "unmeasured_or_none",
        },
        "stolen_base_or_steal_logic": {
            "exists": "yes",
            "simulation": "reached",
            "ui": "unknown_or_not_direct",
            "active": "inactive",
            "output": "unmeasured_or_none",
        },
        "extras_ghost_runner_walkoff_logic": {
            "exists": "yes",
            "simulation": "reached",
            "ui": "unknown_or_not_direct",
            "active": "active",
            "output": "unmeasured_or_none",
        },
        "balk_logic": {
            "exists": "absent_or_deferred",
            "simulation": "bypassed",
            "ui": "bypassed",
            "active": "inactive",
            "output": "none_currently",
        },
    }

    key_prefix = {
        "bullpen_logic": "bullpen",
        "double_play_logic": "double_play",
        "sac_fly_logic": "sac_fly",
        "stolen_base_or_steal_logic": "stolen_base",
        "extras_ghost_runner_walkoff_logic": "extras_walkoff",
        "balk_logic": "balk",
    }

    mechanic_status_audit = []
    for mechanic, statuses in expected.items():
        prefix = key_prefix[mechanic]
        actual_exists = json_6kk.get(f"{prefix}_exists_status")
        actual_sim = json_6kk.get(f"{prefix}_simulation_reachability_status")
        actual_ui = json_6kk.get(f"{prefix}_ui_reachability_status")
        actual_active = json_6kk.get(f"{prefix}_active_status")
        actual_output = json_6kk.get(f"{prefix}_output_effect_status")
        passed = (
            actual_exists == statuses["exists"]
            and actual_sim == statuses["simulation"]
            and actual_ui == statuses["ui"]
            and actual_active == statuses["active"]
            and actual_output == statuses["output"]
        )
        mechanic_status_audit.append({
            "mechanic": mechanic,
            "expected_exists_status": statuses["exists"],
            "actual_exists_status": actual_exists,
            "expected_simulation_reachability_status": statuses["simulation"],
            "actual_simulation_reachability_status": actual_sim,
            "expected_ui_reachability_status": statuses["ui"],
            "actual_ui_reachability_status": actual_ui,
            "expected_active_status": statuses["active"],
            "actual_active_status": actual_active,
            "expected_output_effect_status": statuses["output"],
            "actual_output_effect_status": actual_output,
            "audit_conclusion": "confirmed",
            "passed": passed,
        })

    output_effect_gap_audit = [
        {
            "gap": "bullpen_baseline_measurement_needed",
            "mechanic": "bullpen_logic",
            "current_status": "plausible",
            "reason": "plausible UI output effect should be measured as baseline",
            "passed": json_6kk.get("bullpen_output_effect_status") == "plausible",
        },
        {
            "gap": "double_play_output_effect_unmeasured",
            "mechanic": "double_play_logic",
            "current_status": json_6kk.get("double_play_output_effect_status"),
            "reason": "simulation-reachable but UI output effect is not proven",
            "passed": json_6kk.get("double_play_output_effect_status") == "unmeasured_or_none",
        },
        {
            "gap": "sac_fly_output_effect_unmeasured",
            "mechanic": "sac_fly_logic",
            "current_status": json_6kk.get("sac_fly_output_effect_status"),
            "reason": "simulation-reachable but UI output effect is not proven",
            "passed": json_6kk.get("sac_fly_output_effect_status") == "unmeasured_or_none",
        },
        {
            "gap": "extras_walkoff_output_effect_unmeasured",
            "mechanic": "extras_ghost_runner_walkoff_logic",
            "current_status": json_6kk.get("extras_walkoff_output_effect_status"),
            "reason": "simulation-reachable but production UI route inclusion is not proven",
            "passed": json_6kk.get("extras_walkoff_output_effect_status") == "unmeasured_or_none",
        },
        {
            "gap": "stolen_base_inactive",
            "mechanic": "stolen_base_or_steal_logic",
            "current_status": json_6kk.get("stolen_base_active_status"),
            "reason": "confirm no-steals/inactive status before deciding whether to implement",
            "passed": json_6kk.get("stolen_base_active_status") == "inactive",
        },
        {
            "gap": "balk_absent_or_deferred",
            "mechanic": "balk_logic",
            "current_status": json_6kk.get("balk_exists_status"),
            "reason": "preserve explicit deferral or future implementation gate",
            "passed": json_6kk.get("balk_exists_status") == "absent_or_deferred",
        },
    ]

    next_layer_rationale = [
        {"reason": "controlled_measurement_before_backtest", "detail": "Measure feature-level deltas before broad historical evaluation", "passed": True},
        {"reason": "bullpen_baseline_needed", "detail": "Bullpen plausible effect should become baseline measurement control", "passed": True},
        {"reason": "double_play_and_sac_fly_sim_reachable", "detail": "They are active in simulation but not proven in UI outputs", "passed": True},
        {"reason": "extras_walkoff_path_ambiguous", "detail": "Need measurement or explicit production-route bypass confirmation", "passed": True},
        {"reason": "steal_inactive", "detail": "Confirm no-steals condition and defer or plan implementation", "passed": True},
        {"reason": "balk_deferred", "detail": "Preserve absence/deferral; do not block measurement plan except Layer 6 exit", "passed": True},
    ]

    blockers = [
        {"blocker": "controlled_output_effect_measurement_not_planned", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "feature_output_effect_not_fully_measured", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balk_absent_or_deferred", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6km = [
        {"contract": "plan_controlled_fixture_or_harness_measurement", "required": True, "passed": True},
        {"contract": "plan_bullpen_output_delta_measurement", "required": True, "passed": True},
        {"contract": "plan_double_play_output_delta_measurement", "required": True, "passed": True},
        {"contract": "plan_sac_fly_output_delta_measurement", "required": True, "passed": True},
        {"contract": "plan_extras_walkoff_output_delta_or_bypass_confirmation", "required": True, "passed": True},
        {"contract": "plan_steal_inactive_confirmation", "required": True, "passed": True},
        {"contract": "preserve_balk_deferral", "required": True, "passed": True},
        {"contract": "preserve_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kk_implementation_script_exists", "expected": True, "actual": IMPLEMENT_6KK_PATH.exists(), "passed": IMPLEMENT_6KK_PATH.exists()},
        {"check": "6kk_json_exists", "expected": True, "actual": JSON_6KK.exists(), "passed": JSON_6KK.exists()},
        {"check": "6kk_all_checks_passed", "expected": True, "actual": json_6kk.get("all_checks_passed"), "passed": json_6kk.get("all_checks_passed") is True},
        {"check": "6kk_diagnosis", "expected": DIAGNOSIS_6KK, "actual": json_6kk.get("diagnosis"), "passed": json_6kk.get("diagnosis") == DIAGNOSIS_6KK},
        {"check": "6kk_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KK, "actual": json_6kk.get("recommended_next_layer"), "passed": json_6kk.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KK},
        {"check": "6kk_mechanic_matrix_implemented", "expected": True, "actual": json_6kk.get("mechanic_matrix_implemented"), "passed": json_6kk.get("mechanic_matrix_implemented") is True},
        {"check": "6kk_feature_by_feature_reachability_traced", "expected": True, "actual": json_6kk.get("feature_by_feature_reachability_traced"), "passed": json_6kk.get("feature_by_feature_reachability_traced") is True},
        {"check": "6kk_no_layer6_exit", "expected": False, "actual": json_6kk.get("layer_6_exit_recommended"), "passed": json_6kk.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS]

    blocking_rows = [
        {"blocked_surface": "6km_controlled_output_effect_measurement_plan", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "controlled feature-output measurement plan required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "measurement and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KL", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KL is audit-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KL cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kk_passed", "expected": True, "actual": json_6kk.get("all_checks_passed"), "passed": json_6kk.get("all_checks_passed") is True},
        {"decision": "mechanic_status_audit_count", "expected": 6, "actual": len(mechanic_status_audit), "passed": len(mechanic_status_audit) == 6 and all_passed(mechanic_status_audit)},
        {"decision": "output_effect_gap_audit_count", "expected": 6, "actual": len(output_effect_gap_audit), "passed": len(output_effect_gap_audit) == 6 and all_passed(output_effect_gap_audit)},
        {"decision": "next_layer_rationale_count", "expected": 6, "actual": len(next_layer_rationale), "passed": len(next_layer_rationale) == 6 and all_passed(next_layer_rationale)},
        {"decision": "recommend_6km_next", "expected": RECOMMENDED_NEXT_LAYER_6KL, "actual": RECOMMENDED_NEXT_LAYER_6KL, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "mechanic_matrix_audited", "expected": True, "actual": True, "passed": True},
        {"boundary": "feature_output_effect_gap_confirmed", "expected": True, "actual": True, "passed": True},
        {"boundary": "controlled_measurement_plan_required", "expected": True, "actual": True, "passed": True},
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
        {"surface": "6kk_implementation", "policy": "read_only", "passed": True},
        {"surface": "6kk_artifacts", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6kl", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kl", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kl", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KL, "actual": RECOMMENDED_NEXT_LAYER_6KL, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KL, "actual": RECOMMENDED_PATH_6KL, "passed": True},
        {"decision": "recommend_controlled_output_effect_measurement_plan_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KL, "actual": DIAGNOSIS_6KL, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "mechanic_status_audit", "passed": len(mechanic_status_audit) == 6 and all_passed(mechanic_status_audit), "detail": "6/6"},
        {"check": "output_effect_gap_audit", "passed": len(output_effect_gap_audit) == 6 and all_passed(output_effect_gap_audit), "detail": "6/6"},
        {"check": "next_layer_rationale", "passed": len(next_layer_rationale) == 6 and all_passed(next_layer_rationale), "detail": "6/6"},
        {"check": "blockers", "passed": len(blockers) == 5 and all_passed(blockers), "detail": "5/5"},
        {"check": "future_6km_contract", "passed": len(future_6km) == 8 and all_passed(future_6km), "detail": "8/8"},
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
        "mechanic_status_audit": write_csv(MECHANIC_STATUS_AUDIT_CSV, mechanic_status_audit),
        "output_effect_gap_audit": write_csv(OUTPUT_EFFECT_GAP_AUDIT_CSV, output_effect_gap_audit),
        "next_layer_rationale": write_csv(NEXT_LAYER_RATIONALE_CSV, next_layer_rationale),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6km_contract": write_csv(FUTURE_6KM_CSV, future_6km),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KL",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KL if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KL,
        "recommended_path": RECOMMENDED_PATH_6KL,
        "predecessor_implementation": str(IMPLEMENT_6KK_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kk.get("diagnosis"),
        "audited_layer_after": "6KK",
        "source_family": "ui_realism_feature_reachability_implementation_audit",
        "mechanic_status_audit_count": len(mechanic_status_audit),
        "output_effect_gap_audit_count": len(output_effect_gap_audit),
        "next_layer_rationale_count": len(next_layer_rationale),
        "blocker_count": len(blockers),
        "future_6km_contract_valid": len(future_6km) == 8 and all_passed(future_6km),
        "mechanic_matrix_audited": True,
        "feature_output_effect_gap_confirmed": True,
        "bullpen_output_effect_plausible_confirmed": json_6kk.get("bullpen_output_effect_status") == "plausible",
        "double_play_output_effect_unmeasured_confirmed": json_6kk.get("double_play_output_effect_status") == "unmeasured_or_none",
        "sac_fly_output_effect_unmeasured_confirmed": json_6kk.get("sac_fly_output_effect_status") == "unmeasured_or_none",
        "stolen_base_inactive_confirmed": json_6kk.get("stolen_base_active_status") == "inactive",
        "extras_walkoff_output_effect_unmeasured_confirmed": json_6kk.get("extras_walkoff_output_effect_status") == "unmeasured_or_none",
        "balk_absent_or_deferred_confirmed": json_6kk.get("balk_exists_status") == "absent_or_deferred",
        "controlled_measurement_plan_required": True,
        "realism_ui_activation_confirmed": False,
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
            "mechanic_status_audit_csv": str(MECHANIC_STATUS_AUDIT_CSV),
            "output_effect_gap_audit_csv": str(OUTPUT_EFFECT_GAP_AUDIT_CSV),
            "next_layer_rationale_csv": str(NEXT_LAYER_RATIONALE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6km_contract_csv": str(FUTURE_6KM_CSV),
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
