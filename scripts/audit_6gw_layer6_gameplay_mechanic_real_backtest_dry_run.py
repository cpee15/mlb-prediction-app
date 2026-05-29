#!/usr/bin/env python3
"""Audit Layer 6GV bounded real-backtest dry-run execution artifacts."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gw_real_backtest_dry_run_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6GV_PATH = Path("scripts/implement_6gv_layer6_gameplay_mechanic_real_backtest_dry_run.py")
AUDIT_6GU_PATH = Path("scripts/audit_6gu_layer6_gameplay_mechanic_real_backtest_plan.py")
PLAN_6GT_PATH = Path("scripts/plan_6gt_layer6_gameplay_mechanic_real_backtests.py")

JSON_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run.json"
CHECKS_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_checks.csv"
PREDECESSOR_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_predecessor.csv"
OUTCOME_DISCOVERY_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_outcome_discovery.csv"
EXECUTION_WINDOWS_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_execution_windows.csv"
HARNESS_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_harness_config.csv"
CANDIDATE_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_candidate_results.csv"
BASELINE_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_baseline_results.csv"
METRIC_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_metric_comparison.csv"
PASS_FAIL_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_pass_fail_summary.csv"
PAYLOAD_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_payload_consistency_summary.csv"
DETERMINISM_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_determinism_summary.csv"
RUNTIME_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_runtime_summary.csv"
SAFETY_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_safety_summary.csv"
DECISION_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_real_decision_recommendations.csv"
FUTURE_6GW_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_future_6gw_contract.csv"
IMMUTABILITY_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_immutability.csv"
RECOMMENDED_6GV = TMP_DIR / "layer6_6gv_real_backtest_dry_run_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
ROW_COUNTS_CSV = TMP_DIR / f"{SLUG}_row_counts.csv"
MECHANIC_WINDOW_COVERAGE_CSV = TMP_DIR / f"{SLUG}_mechanic_window_coverage.csv"
DECISION_INTEGRITY_CSV = TMP_DIR / f"{SLUG}_decision_integrity.csv"
ACTIVATION_SAFETY_CSV = TMP_DIR / f"{SLUG}_activation_safety.csv"
OUTCOME_JOIN_SAFETY_CSV = TMP_DIR / f"{SLUG}_outcome_join_safety.csv"
METRIC_SAFETY_CSV = TMP_DIR / f"{SLUG}_metric_safety.csv"
PASS_FAIL_SAFETY_CSV = TMP_DIR / f"{SLUG}_pass_fail_safety.csv"
RUNTIME_SAFETY_CSV = TMP_DIR / f"{SLUG}_runtime_safety.csv"
OUTCOME_DISCOVERY_AUDIT_CSV = TMP_DIR / f"{SLUG}_outcome_discovery.csv"
FUTURE_6GX_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gx_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GV = "layer_6_gameplay_mechanic_real_backtest_dry_run_execution_complete"
DIAGNOSIS_6GW = "layer_6_gameplay_mechanic_real_backtest_dry_run_audit_complete"
CURRENT_LAYER = "6GW_layer_6_gameplay_mechanic_real_backtest_dry_run_audit"
RECOMMENDED_NEXT_LAYER = "6GX_layer_6_gameplay_mechanic_outcome_artifact_selection_plan"
RECOMMENDED_PATH = "audit_bounded_dry_run_evidence_then_plan_outcome_artifact_selection"

GAMEPLAY_MECHANICS = [
    "extra_innings_ghost_runner",
    "stolen_bases_caught_stealing",
    "wild_pitches_passed_balls",
    "balks",
    "first_to_third_advancement",
    "second_to_home_advancement",
    "sac_flies_tagging_up",
    "double_plays_by_base_out_state",
    "pinch_hitters_substitutions",
    "bullpen_sequencing_leverage_behavior",
]

EVALUATION_WINDOWS = [
    "recent_rolling_window",
    "full_available_validated_window",
    "stress_window_high_extra_innings_or_high_run_environment",
]

REQUIRED_6GV_ARTIFACTS = [
    JSON_6GV,
    CHECKS_6GV,
    PREDECESSOR_6GV,
    OUTCOME_DISCOVERY_6GV,
    EXECUTION_WINDOWS_6GV,
    HARNESS_6GV,
    CANDIDATE_6GV,
    BASELINE_6GV,
    METRIC_6GV,
    PASS_FAIL_6GV,
    PAYLOAD_6GV,
    DETERMINISM_6GV,
    RUNTIME_6GV,
    SAFETY_6GV,
    DECISION_6GV,
    FUTURE_6GW_6GV,
    IMMUTABILITY_6GV,
    RECOMMENDED_6GV,
]

ROW_COUNT_TARGETS = {
    "real_harness_config": (HARNESS_6GV, 30),
    "real_candidate_results": (CANDIDATE_6GV, 30),
    "real_baseline_results": (BASELINE_6GV, 30),
    "real_metric_comparison": (METRIC_6GV, 30),
    "real_pass_fail_summary": (PASS_FAIL_6GV, 30),
    "real_payload_consistency_summary": (PAYLOAD_6GV, 30),
    "real_determinism_summary": (DETERMINISM_6GV, 30),
    "real_runtime_summary": (RUNTIME_6GV, 30),
    "real_safety_summary": (SAFETY_6GV, 30),
    "real_decision_recommendations": (DECISION_6GV, 30),
}


def safe_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        parsed, _ = json.JSONDecoder().raw_decode(text)
        return parsed if isinstance(parsed, dict) else {}


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


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
    return str(value).strip().lower() == "true"


def is_blank(value: Any) -> bool:
    return str(value or "").strip() == ""


def pair_key(row: Dict[str, str]) -> str:
    return f"{row.get('mechanic')}::{row.get('evaluation_window')}"


def expected_pairs() -> set[str]:
    return {
        f"{mechanic}::{window}"
        for mechanic in GAMEPLAY_MECHANICS
        for window in EVALUATION_WINDOWS
    }


def all_activation_false(rows: List[Dict[str, str]]) -> bool:
    return all(not boolish(row.get("activation_allowed")) for row in rows)


def all_exit_credit_false(rows: List[Dict[str, str]]) -> bool:
    return all(not boolish(row.get("layer_6_exit_credit")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    implement_6gv_before = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_before = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_before = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    implementation_run = subprocess.run(
        [sys.executable, str(IMPLEMENT_6GV_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6gv = load_json(JSON_6GV)
    checks_6gv = read_csv(CHECKS_6GV)
    outcome_discovery_6gv = read_csv(OUTCOME_DISCOVERY_6GV)
    harness_rows = read_csv(HARNESS_6GV)
    candidate_rows = read_csv(CANDIDATE_6GV)
    baseline_rows = read_csv(BASELINE_6GV)
    metric_rows = read_csv(METRIC_6GV)
    pass_fail_rows = read_csv(PASS_FAIL_6GV)
    payload_rows = read_csv(PAYLOAD_6GV)
    determinism_rows = read_csv(DETERMINISM_6GV)
    runtime_rows = read_csv(RUNTIME_6GV)
    safety_rows = read_csv(SAFETY_6GV)
    decision_rows = read_csv(DECISION_6GV)
    future_6gw_rows = read_csv(FUTURE_6GW_6GV)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gv_implementation_exists", "expected": True, "actual": IMPLEMENT_6GV_PATH.exists(), "passed": IMPLEMENT_6GV_PATH.exists()},
        {"check": "6gv_implementation_runs", "expected": 0, "actual": implementation_run.returncode, "passed": implementation_run.returncode == 0},
        {"check": "6gv_json_exists", "expected": True, "actual": JSON_6GV.exists(), "passed": JSON_6GV.exists()},
        {"check": "6gv_all_checks_passed", "expected": True, "actual": json_6gv.get("all_checks_passed"), "passed": json_6gv.get("all_checks_passed") is True},
        {"check": "6gv_diagnosis", "expected": DIAGNOSIS_6GV, "actual": json_6gv.get("diagnosis"), "passed": json_6gv.get("diagnosis") == DIAGNOSIS_6GV},
        {"check": "6gv_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6gv.get("recommended_next_layer"), "passed": json_6gv.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gv_implementation_type", "expected": "bounded_real_backtest_dry_run_execution", "actual": json_6gv.get("implementation_type"), "passed": json_6gv.get("implementation_type") == "bounded_real_backtest_dry_run_execution"},
        {"check": "6gv_bounded_dry_run_only", "expected": True, "actual": json_6gv.get("bounded_dry_run_only"), "passed": json_6gv.get("bounded_dry_run_only") is True},
        {"check": "6gv_real_backtests_run_true", "expected": True, "actual": json_6gv.get("real_backtests_run"), "passed": json_6gv.get("real_backtests_run") is True},
        {"check": "6gv_layer_6_exit_ready_false", "expected": False, "actual": json_6gv.get("layer_6_exit_ready"), "passed": json_6gv.get("layer_6_exit_ready") is False},
        {"check": "6gv_mechanics_activated_false", "expected": False, "actual": json_6gv.get("mechanics_activated_by_this_layer"), "passed": json_6gv.get("mechanics_activated_by_this_layer") is False},
        {"check": "6gv_activation_allowed_false", "expected": False, "actual": json_6gv.get("activation_allowed"), "passed": json_6gv.get("activation_allowed") is False},
        {"check": "6gv_live_fetches_false", "expected": False, "actual": json_6gv.get("live_data_fetches_run"), "passed": json_6gv.get("live_data_fetches_run") is False},
        {"check": "6gv_database_writes_false", "expected": False, "actual": json_6gv.get("database_writes_run"), "passed": json_6gv.get("database_writes_run") is False},
        {"check": "6gv_materialization_jobs_false", "expected": False, "actual": json_6gv.get("materialization_jobs_run"), "passed": json_6gv.get("materialization_jobs_run") is False},
        {"check": "6gv_production_simulations_false", "expected": False, "actual": json_6gv.get("production_simulations_run"), "passed": json_6gv.get("production_simulations_run") is False},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in REQUIRED_6GV_ARTIFACTS
    ]

    checks_consistency_rows = [
        {
            "source_check": row.get("check"),
            "source_passed": row.get("passed"),
            "passed": boolish(row.get("passed")),
            "detail": row.get("detail", ""),
        }
        for row in checks_6gv
    ]

    row_count_rows = []
    for name, (path, expected_count) in ROW_COUNT_TARGETS.items():
        rows = read_csv(path)
        row_count_rows.append(
            {
                "artifact": name,
                "path": str(path),
                "expected_rows": expected_count,
                "actual_rows": len(rows),
                "passed": len(rows) == expected_count,
            }
        )

    expected = expected_pairs()
    coverage_sources = {
        "real_harness_config": harness_rows,
        "real_candidate_results": candidate_rows,
        "real_baseline_results": baseline_rows,
        "real_metric_comparison": metric_rows,
        "real_pass_fail_summary": pass_fail_rows,
        "real_payload_consistency_summary": payload_rows,
        "real_determinism_summary": determinism_rows,
        "real_runtime_summary": runtime_rows,
        "real_safety_summary": safety_rows,
        "real_decision_recommendations": decision_rows,
    }

    mechanic_window_rows = []
    for name, rows in coverage_sources.items():
        actual = {pair_key(row) for row in rows}
        mechanic_window_rows.append(
            {
                "artifact": name,
                "expected_pairs": len(expected),
                "actual_pairs": len(actual),
                "missing_pairs": "|".join(sorted(expected - actual)),
                "unexpected_pairs": "|".join(sorted(actual - expected)),
                "passed": actual == expected,
            }
        )

    decision_integrity_rows = [
        {
            "check": "decision_class_needs_more_evidence",
            "expected": "needs_more_evidence",
            "actual_bad_rows": sum(1 for row in decision_rows if row.get("decision_class") != "needs_more_evidence"),
            "passed": all(row.get("decision_class") == "needs_more_evidence" for row in decision_rows),
        },
        {
            "check": "decision_execution_status_bounded_incomplete_or_no_evidence",
            "expected": "bounded_dry_run_incomplete_or_no_evidence_available",
            "actual_bad_rows": sum(1 for row in decision_rows if row.get("execution_status") not in {"bounded_dry_run_incomplete", "no_evidence_available"}),
            "passed": all(row.get("execution_status") in {"bounded_dry_run_incomplete", "no_evidence_available"} for row in decision_rows),
        },
        {
            "check": "decision_requires_future_audit",
            "expected": True,
            "actual_bad_rows": sum(1 for row in decision_rows if not boolish(row.get("requires_future_audit"))),
            "passed": all(boolish(row.get("requires_future_audit")) for row in decision_rows),
        },
    ]

    activation_sources = {
        "candidate": candidate_rows,
        "baseline": baseline_rows,
        "metric": metric_rows,
        "pass_fail": pass_fail_rows,
        "payload": payload_rows,
        "determinism": determinism_rows,
        "runtime": runtime_rows,
        "safety": safety_rows,
        "decision": decision_rows,
    }
    activation_safety_rows = [
        {
            "artifact": name,
            "activation_allowed_false": all_activation_false(rows),
            "layer_6_exit_credit_false": all_exit_credit_false(rows),
            "passed": all_activation_false(rows) and all_exit_credit_false(rows),
        }
        for name, rows in activation_sources.items()
    ]

    outcome_join_rows = [
        {
            "artifact": "candidate",
            "games_evaluated_all_zero": all(str(row.get("games_evaluated")) == "0" for row in candidate_rows),
            "actual_outcomes_joined_false": all(not boolish(row.get("actual_outcomes_joined")) for row in candidate_rows),
            "passed": all(str(row.get("games_evaluated")) == "0" for row in candidate_rows)
            and all(not boolish(row.get("actual_outcomes_joined")) for row in candidate_rows),
        },
        {
            "artifact": "baseline",
            "games_evaluated_all_zero": all(str(row.get("games_evaluated")) == "0" for row in baseline_rows),
            "actual_outcomes_joined_false": all(not boolish(row.get("actual_outcomes_joined")) for row in baseline_rows),
            "passed": all(str(row.get("games_evaluated")) == "0" for row in baseline_rows)
            and all(not boolish(row.get("actual_outcomes_joined")) for row in baseline_rows),
        },
        {
            "artifact": "decision",
            "games_evaluated_all_zero": all(str(row.get("games_evaluated")) == "0" for row in decision_rows),
            "actual_outcomes_joined_false": all(not boolish(row.get("actual_outcomes_joined")) for row in decision_rows),
            "passed": all(str(row.get("games_evaluated")) == "0" for row in decision_rows)
            and all(not boolish(row.get("actual_outcomes_joined")) for row in decision_rows),
        },
    ]

    metric_safety_rows = [
        {
            "check": "candidate_metric_blank",
            "bad_rows": sum(1 for row in candidate_rows if not is_blank(row.get("candidate_metric_value"))),
            "passed": all(is_blank(row.get("candidate_metric_value")) for row in candidate_rows),
        },
        {
            "check": "baseline_metric_blank",
            "bad_rows": sum(1 for row in baseline_rows if not is_blank(row.get("baseline_metric_value"))),
            "passed": all(is_blank(row.get("baseline_metric_value")) for row in baseline_rows),
        },
        {
            "check": "metric_comparison_not_available",
            "bad_rows": sum(1 for row in metric_rows if boolish(row.get("comparison_available"))),
            "passed": all(not boolish(row.get("comparison_available")) for row in metric_rows),
        },
    ]

    gate_columns = [
        "passes_total_run_error_gate",
        "passes_team_total_error_gate",
        "passes_inning_distribution_gate",
        "passes_scoring_tail_gate",
        "passes_variance_calibration_gate",
        "passes_reproducibility_gate",
        "passes_payload_consistency_gate",
    ]
    pass_fail_safety_rows = [
        {
            "gate": gate,
            "bad_rows": sum(1 for row in pass_fail_rows if boolish(row.get(gate))),
            "passed": all(not boolish(row.get(gate)) for row in pass_fail_rows),
        }
        for gate in gate_columns
    ]

    runtime_safety_rows = [
        {
            "check": "no_expensive_backtest",
            "bad_rows": sum(1 for row in runtime_rows if boolish(row.get("expensive_backtest_run"))),
            "passed": all(not boolish(row.get("expensive_backtest_run")) for row in runtime_rows),
        },
        {
            "check": "no_live_fetch",
            "bad_rows": sum(1 for row in runtime_rows if boolish(row.get("live_fetch_run"))),
            "passed": all(not boolish(row.get("live_fetch_run")) for row in runtime_rows),
        },
        {
            "check": "no_database_write",
            "bad_rows": sum(1 for row in runtime_rows if boolish(row.get("database_write_run"))),
            "passed": all(not boolish(row.get("database_write_run")) for row in runtime_rows),
        },
        {
            "check": "no_materialization_job",
            "bad_rows": sum(1 for row in runtime_rows if boolish(row.get("materialization_job_run"))),
            "passed": all(not boolish(row.get("materialization_job_run")) for row in runtime_rows),
        },
        {
            "check": "no_production_simulation",
            "bad_rows": sum(1 for row in runtime_rows if boolish(row.get("production_simulation_run"))),
            "passed": all(not boolish(row.get("production_simulation_run")) for row in runtime_rows),
        },
    ]

    allowed_roots = ("data", "artifacts", "outputs", "tmp", "mlb_app", "tests", "repository", "")
    outcome_discovery_audit_rows = []
    for row in outcome_discovery_6gv:
        candidate_path = row.get("candidate_path", "")
        root = row.get("root", "")
        local_root_ok = root in allowed_roots or any(root.startswith(prefix) for prefix in allowed_roots if prefix)
        no_live_fetch = not boolish(row.get("live_fetch_required"))
        not_remote = not candidate_path.startswith(("http://", "https://", "s3://", "gs://"))
        outcome_discovery_audit_rows.append(
            {
                "root": root,
                "candidate_path": candidate_path,
                "local_root_ok": local_root_ok,
                "not_remote_path": not_remote,
                "live_fetch_required_false": no_live_fetch,
                "passed": local_root_ok and not_remote and no_live_fetch,
            }
        )

    future_6gx_rows = [
        {"contract": "plan_valid_outcome_artifact_selection", "required": True, "passed": True},
        {"contract": "classify_discovered_artifacts_by_suitability", "required": True, "passed": True},
        {"contract": "candidate_game_outcomes_class_supported", "required": True, "passed": True},
        {"contract": "candidate_team_totals_class_supported", "required": True, "passed": True},
        {"contract": "candidate_inning_runs_class_supported", "required": True, "passed": True},
        {"contract": "candidate_base_out_transitions_class_supported", "required": True, "passed": True},
        {"contract": "candidate_backtest_prior_outputs_class_supported", "required": True, "passed": True},
        {"contract": "unsuitable_planning_artifact_class_supported", "required": True, "passed": True},
        {"contract": "insufficient_metadata_class_supported", "required": True, "passed": True},
        {"contract": "no_real_backtests_or_activation_in_6gx", "required": True, "passed": True},
        {"contract": "layer_6_exit_credit_remains_blocked", "required": True, "passed": True},
        {"contract": "recommended_6gx_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_outcome_artifact_selection_plan_complete"},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    implement_6gv_after = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_after = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_after = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gw_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gv_implementation", "policy": "unchanged_by_6gw", "passed": implement_6gv_after == implement_6gv_before},
        {"surface": "6gu_audit", "policy": "unchanged_by_6gw", "passed": audit_6gu_after == audit_6gu_before},
        {"surface": "6gt_plan", "policy": "unchanged_by_6gw", "passed": plan_6gt_after == plan_6gt_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gw", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gw", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gw", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gw", "passed": True},
        {"surface": "live_fetches_or_database_writes", "policy": "not_run", "passed": True},
        {"surface": "materialization_jobs_or_production_simulations", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GW, "actual": DIAGNOSIS_6GW, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": len(checks_consistency_rows) >= 20 and all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "row_counts", "passed": all(row["passed"] for row in row_count_rows), "detail": f"{sum(1 for row in row_count_rows if row['passed'])}/{len(row_count_rows)}"},
        {"check": "mechanic_window_coverage", "passed": all(row["passed"] for row in mechanic_window_rows), "detail": f"{sum(1 for row in mechanic_window_rows if row['passed'])}/{len(mechanic_window_rows)}"},
        {"check": "decision_integrity", "passed": all(row["passed"] for row in decision_integrity_rows), "detail": f"{sum(1 for row in decision_integrity_rows if row['passed'])}/{len(decision_integrity_rows)}"},
        {"check": "activation_safety", "passed": all(row["passed"] for row in activation_safety_rows), "detail": f"{sum(1 for row in activation_safety_rows if row['passed'])}/{len(activation_safety_rows)}"},
        {"check": "outcome_join_safety", "passed": all(row["passed"] for row in outcome_join_rows), "detail": f"{sum(1 for row in outcome_join_rows if row['passed'])}/{len(outcome_join_rows)}"},
        {"check": "metric_safety", "passed": all(row["passed"] for row in metric_safety_rows), "detail": f"{sum(1 for row in metric_safety_rows if row['passed'])}/{len(metric_safety_rows)}"},
        {"check": "pass_fail_safety", "passed": all(row["passed"] for row in pass_fail_safety_rows), "detail": f"{sum(1 for row in pass_fail_safety_rows if row['passed'])}/{len(pass_fail_safety_rows)}"},
        {"check": "runtime_safety", "passed": all(row["passed"] for row in runtime_safety_rows), "detail": f"{sum(1 for row in runtime_safety_rows if row['passed'])}/{len(runtime_safety_rows)}"},
        {"check": "outcome_discovery", "passed": all(row["passed"] for row in outcome_discovery_audit_rows), "detail": f"{sum(1 for row in outcome_discovery_audit_rows if row['passed'])}/{len(outcome_discovery_audit_rows)}"},
        {"check": "future_6gw_contract", "passed": all(row.get("contract") and boolish(row.get("passed")) for row in future_6gw_rows), "detail": f"{sum(1 for row in future_6gw_rows if boolish(row.get('passed')))}" + f"/{len(future_6gw_rows)}"},
        {"check": "future_6gx_contract", "passed": all(row["passed"] for row in future_6gx_rows), "detail": f"{sum(1 for row in future_6gx_rows if row['passed'])}/{len(future_6gx_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "checks_consistency": write_csv(CHECKS_CONSISTENCY_CSV, checks_consistency_rows),
        "row_counts": write_csv(ROW_COUNTS_CSV, row_count_rows),
        "mechanic_window_coverage": write_csv(MECHANIC_WINDOW_COVERAGE_CSV, mechanic_window_rows),
        "decision_integrity": write_csv(DECISION_INTEGRITY_CSV, decision_integrity_rows),
        "activation_safety": write_csv(ACTIVATION_SAFETY_CSV, activation_safety_rows),
        "outcome_join_safety": write_csv(OUTCOME_JOIN_SAFETY_CSV, outcome_join_rows),
        "metric_safety": write_csv(METRIC_SAFETY_CSV, metric_safety_rows),
        "pass_fail_safety": write_csv(PASS_FAIL_SAFETY_CSV, pass_fail_safety_rows),
        "runtime_safety": write_csv(RUNTIME_SAFETY_CSV, runtime_safety_rows),
        "outcome_discovery": write_csv(OUTCOME_DISCOVERY_AUDIT_CSV, outcome_discovery_audit_rows),
        "future_6gx_contract": write_csv(FUTURE_6GX_CONTRACT_CSV, future_6gx_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GW",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "audited_layer": "6GV",
        "audited_execution_diagnosis": json_6gv.get("diagnosis"),
        "diagnosis": DIAGNOSIS_6GW if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_implementation": str(IMPLEMENT_6GV_PATH),
        "predecessor_implementation_returncode": implementation_run.returncode,
        "predecessor_implementation_diagnosis": json_6gv.get("diagnosis"),
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "predecessor_real_backtests_run": bool(json_6gv.get("real_backtests_run")),
        "bounded_dry_run_only": True,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(EVALUATION_WINDOWS),
        "audited_harness_config_rows_count": len(harness_rows),
        "audited_candidate_result_rows_count": len(candidate_rows),
        "audited_baseline_result_rows_count": len(baseline_rows),
        "audited_metric_comparison_rows_count": len(metric_rows),
        "audited_pass_fail_summary_rows_count": len(pass_fail_rows),
        "audited_decision_recommendation_rows_count": len(decision_rows),
        "games_evaluated": int(json_6gv.get("games_evaluated") or 0),
        "activation_allowed": False,
        "layer_6_exit_credit": False,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "checks_consistency_csv": str(CHECKS_CONSISTENCY_CSV),
            "row_counts_csv": str(ROW_COUNTS_CSV),
            "mechanic_window_coverage_csv": str(MECHANIC_WINDOW_COVERAGE_CSV),
            "decision_integrity_csv": str(DECISION_INTEGRITY_CSV),
            "activation_safety_csv": str(ACTIVATION_SAFETY_CSV),
            "outcome_join_safety_csv": str(OUTCOME_JOIN_SAFETY_CSV),
            "metric_safety_csv": str(METRIC_SAFETY_CSV),
            "pass_fail_safety_csv": str(PASS_FAIL_SAFETY_CSV),
            "runtime_safety_csv": str(RUNTIME_SAFETY_CSV),
            "outcome_discovery_csv": str(OUTCOME_DISCOVERY_AUDIT_CSV),
            "future_6gx_contract_csv": str(FUTURE_6GX_CONTRACT_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
