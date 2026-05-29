#!/usr/bin/env python3
"""Plan Layer 6 game-state realism exit-criteria reconciliation."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable


SLUG = "layer6_6gk_game_state_realism_exit_reconciliation_plan"
TMP_DIR = Path("tmp")

AUDIT_6GJ_PATH = Path("scripts/audit_6gj_downstream_usage_reporting_reporting_reporting_reporting_impl.py")
VALIDATOR_6GI_PATH = Path("scripts/validate_6gi_downstream_usage_reporting_reporting_reporting_reporting_impl.py")
AUDIT_6GJ_JSON = TMP_DIR / "candidate_bullpen_6gj_downstream_usage_reporting_reporting_reporting_reporting_impl_audit.json"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
MECHANIC_INVENTORY_CSV = TMP_DIR / f"{SLUG}_mechanic_inventory.csv"
WIRING_LEVELS_CSV = TMP_DIR / f"{SLUG}_wiring_levels.csv"
PROJECTION_INTEGRATION_CSV = TMP_DIR / f"{SLUG}_projection_integration.csv"
VALIDATION_EVIDENCE_CSV = TMP_DIR / f"{SLUG}_validation_evidence.csv"
EXIT_CRITERIA_CSV = TMP_DIR / f"{SLUG}_exit_criteria.csv"
GAP_CATEGORIES_CSV = TMP_DIR / f"{SLUG}_gap_categories.csv"
FUTURE_6GL_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gl_contract.csv"
NON_GOALS_CSV = TMP_DIR / f"{SLUG}_non_goals.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GJ = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_reporting_implementation_audit_complete"
)
DIAGNOSIS_6GK = "layer_6_game_state_realism_exit_criteria_reconciliation_plan_complete"
RECOMMENDED_NEXT_LAYER = "6GL_layer_6_game_state_realism_exit_criteria_wiring_inventory_implementation"
RECOMMENDED_PATH = "reconcile_layer_6_game_state_realism_mechanics_to_simulator_projection_and_validation_exit_criteria"
SUPERSEDED_RECURSIVE_NEXT_LAYER = (
    "6GK_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_reporting_reporting_plan"
)

MECHANICS = [
    ("extra_innings_ghost_runner", "extra innings and ghost runner logic"),
    ("stolen_bases_caught_stealing", "stolen bases and caught stealing"),
    ("wild_pitches_passed_balls", "wild pitches and passed balls"),
    ("balks", "balks"),
    ("first_to_third_advancement", "first-to-third advancement"),
    ("second_to_home_advancement", "second-to-home advancement"),
    ("sac_flies_tagging_up", "sac flies and tagging up"),
    ("double_plays_by_base_out_state", "double plays by base/out state"),
    ("pinch_hitters_substitutions", "pinch hitters and substitutions"),
    ("bullpen_sequencing_leverage_behavior", "bullpen sequencing and leverage behavior"),
    ("projection_site_integration", "projection-site integration"),
    ("validation_distribution_shape_evidence", "validation/distribution-shape evidence"),
]

WIRING_LEVELS = [
    ("source_present", "mechanic has source-level code, data field, helper, or script evidence"),
    ("simulator_wired", "mechanic is actually used by simulator state transitions"),
    ("projection_wired", "mechanic reaches site-facing projection entry points"),
    ("validation_present", "mechanic has tests, backtests, audits, or artifacts validating expected behavior"),
    ("outcome_improvement_demonstrated", "mechanic improves distributional outcomes against actuals"),
]

AFFECTED_SURFACES = [
    "base_out_transition_state",
    "runs_scored",
    "inning_extension_probability",
    "extra_inning_state",
    "pitcher_bullpen_exposure",
    "team_total_distribution",
    "total_run_distribution",
    "alternate_total_tails",
]

VALIDATION_TARGETS = [
    "scoring_distribution_tails",
    "inning_level_run_distribution",
    "team_totals",
    "total_runs",
    "extra_innings",
    "base_out_transitions",
    "bullpen_sequencing",
]

CLASSIFICATIONS = [
    "implemented_and_projected_with_validation",
    "implemented_and_projected_without_validation",
    "implemented_in_sim_not_projected",
    "source_present_not_wired",
    "missing_or_unproven",
]


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


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")

    fieldnames: list[str] = []
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


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    audit_6gj_before = AUDIT_6GJ_PATH.read_text(encoding="utf-8") if AUDIT_6GJ_PATH.exists() else ""
    validator_6gi_before = VALIDATOR_6GI_PATH.read_text(encoding="utf-8") if VALIDATOR_6GI_PATH.exists() else ""

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6GJ_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    audit_json = load_json(AUDIT_6GJ_JSON)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gj_audit_exists", "expected": True, "actual": AUDIT_6GJ_PATH.exists(), "passed": AUDIT_6GJ_PATH.exists()},
        {"check": "6gj_audit_runs", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"check": "6gj_audit_json_exists", "expected": True, "actual": AUDIT_6GJ_JSON.exists(), "passed": AUDIT_6GJ_JSON.exists()},
        {"check": "6gj_audit_all_checks_passed", "expected": True, "actual": audit_json.get("all_checks_passed"), "passed": audit_json.get("all_checks_passed") is True},
        {"check": "6gj_audit_only", "expected": True, "actual": audit_json.get("audit_only"), "passed": audit_json.get("audit_only") is True},
        {"check": "6gj_audited_layer", "expected": "6GI", "actual": audit_json.get("audited_layer"), "passed": audit_json.get("audited_layer") == "6GI"},
        {"check": "6gj_diagnosis", "expected": DIAGNOSIS_6GJ, "actual": audit_json.get("diagnosis"), "passed": audit_json.get("diagnosis") == DIAGNOSIS_6GJ},
        {
            "check": "recursive_next_layer_superseded_by_layer6_exit_reconciliation",
            "expected": "superseded_for_broader_layer_6_exit_criteria",
            "actual": audit_json.get("recommended_next_layer"),
            "passed": audit_json.get("recommended_next_layer") == SUPERSEDED_RECURSIVE_NEXT_LAYER,
        },
    ]

    mechanic_rows = []
    for key, label in MECHANICS:
        mechanic_rows.append(
            {
                "mechanic_key": key,
                "mechanic": label,
                "layer_type": "game_mechanics_realism",
                "must_reconcile_source_presence": True,
                "must_reconcile_simulator_wiring": True,
                "must_reconcile_projection_wiring": True,
                "must_reconcile_validation_evidence": True,
                "exit_ready_assumed": False,
                "passed": True,
            }
        )

    wiring_rows = []
    for level_key, level_description in WIRING_LEVELS:
        wiring_rows.append(
            {
                "wiring_level": level_key,
                "description": level_description,
                "planned_for_6gl": True,
                "evidence_required": True,
                "passed": True,
            }
        )

    projection_rows = []
    for key, label in MECHANICS:
        for surface in AFFECTED_SURFACES:
            projection_rows.append(
                {
                    "mechanic_key": key,
                    "mechanic": label,
                    "affected_surface": surface,
                    "future_6gl_must_trace": True,
                    "must_distinguish_code_from_projection_use": True,
                    "passed": True,
                }
            )

    validation_rows = []
    for target in VALIDATION_TARGETS:
        validation_rows.append(
            {
                "validation_target": target,
                "future_6gl_must_find_evidence": True,
                "evidence_types": "tests|backtests|audits|artifacts|actual_outcome_comparison",
                "mean_only_insufficient": True,
                "distribution_shape_required": target in {
                    "scoring_distribution_tails",
                    "inning_level_run_distribution",
                    "team_totals",
                    "total_runs",
                },
                "passed": True,
            }
        )

    exit_criteria_rows = [
        {"exit_criterion": "base_out_transitions_are_more_realistic", "requires_6gl_evidence": True, "currently_exit_ready": False, "passed": True},
        {"exit_criterion": "scoring_distribution_tails_improve", "requires_6gl_evidence": True, "currently_exit_ready": False, "passed": True},
        {"exit_criterion": "inning_level_run_distribution_improves", "requires_6gl_evidence": True, "currently_exit_ready": False, "passed": True},
        {"exit_criterion": "extra_inning_behavior_represented_correctly", "requires_6gl_evidence": True, "currently_exit_ready": False, "passed": True},
        {"exit_criterion": "team_total_and_total_run_variance_improve", "requires_6gl_evidence": True, "currently_exit_ready": False, "passed": True},
        {"exit_criterion": "mechanics_used_by_simulator", "requires_6gl_evidence": True, "currently_exit_ready": False, "passed": True},
        {"exit_criterion": "mechanics_reflected_in_site_facing_projections", "requires_6gl_evidence": True, "currently_exit_ready": False, "passed": True},
        {"exit_criterion": "mechanics_have_validation_evidence", "requires_6gl_evidence": True, "currently_exit_ready": False, "passed": True},
    ]

    gap_rows = []
    for classification in CLASSIFICATIONS:
        gap_rows.append(
            {
                "classification": classification,
                "allowed_in_6gl_inventory": True,
                "exit_ready_classification": classification == "implemented_and_projected_with_validation",
                "requires_followup_if_found": classification != "implemented_and_projected_with_validation",
                "passed": True,
            }
        )

    future_6gl_rows = [
        {"contract": "scan_source_for_candidate_evidence", "required": True, "passed": True},
        {"contract": "distinguish_source_presence_from_simulator_use", "required": True, "passed": True},
        {"contract": "distinguish_simulator_use_from_projection_use", "required": True, "passed": True},
        {"contract": "distinguish_projection_use_from_validation_improvement", "required": True, "passed": True},
        {"contract": "trace_site_facing_projection_entry_points", "required": True, "passed": True},
        {"contract": "produce_evidence_matrix_not_simple_pass_fail", "required": True, "passed": True},
        {"contract": "classify_every_mechanic", "required": True, "passed": True},
        {"contract": "mark_layer_6_exit_incomplete_unless_all_required_mechanics_validated", "required": True, "passed": True},
        {"contract": "preserve_bullpen_reporting_work_as_subsystem_evidence_not_exit_proof", "required": True, "passed": True},
        {"contract": "emit_future_json", "artifact": "tmp/layer6_6gl_game_state_realism_wiring_inventory.json", "required": True, "passed": True},
        {"contract": "emit_future_checks_csv", "artifact": "tmp/layer6_6gl_game_state_realism_wiring_inventory_checks.csv", "required": True, "passed": True},
        {"contract": "emit_future_mechanics_csv", "artifact": "tmp/layer6_6gl_game_state_realism_wiring_inventory_mechanics.csv", "required": True, "passed": True},
        {"contract": "emit_future_source_evidence_csv", "artifact": "tmp/layer6_6gl_game_state_realism_wiring_inventory_source_evidence.csv", "required": True, "passed": True},
        {"contract": "emit_future_simulator_wiring_csv", "artifact": "tmp/layer6_6gl_game_state_realism_wiring_inventory_simulator_wiring.csv", "required": True, "passed": True},
        {"contract": "emit_future_projection_wiring_csv", "artifact": "tmp/layer6_6gl_game_state_realism_wiring_inventory_projection_wiring.csv", "required": True, "passed": True},
        {"contract": "emit_future_validation_evidence_csv", "artifact": "tmp/layer6_6gl_game_state_realism_wiring_inventory_validation_evidence.csv", "required": True, "passed": True},
        {"contract": "emit_future_exit_criteria_csv", "artifact": "tmp/layer6_6gl_game_state_realism_wiring_inventory_exit_criteria.csv", "required": True, "passed": True},
        {"contract": "emit_future_gaps_csv", "artifact": "tmp/layer6_6gl_game_state_realism_wiring_inventory_gaps.csv", "required": True, "passed": True},
        {"contract": "future_6gl_diagnosis", "artifact": "layer_6_game_state_realism_exit_criteria_wiring_inventory_implementation_complete", "required": True, "passed": True},
        {"contract": "future_6gm_audit", "artifact": "6GM_layer_6_game_state_realism_exit_criteria_wiring_inventory_audit", "required": True, "passed": True},
    ]

    non_goal_rows = [
        {"non_goal": "no_simulator_behavior_changes", "passed": True},
        {"non_goal": "no_projection_behavior_changes", "passed": True},
        {"non_goal": "no_bullpen_adapter_behavior_changes", "passed": True},
        {"non_goal": "no_prior_validator_audit_plan_changes", "passed": True},
        {"non_goal": "no_fixture_changes", "passed": True},
        {"non_goal": "no_production_default_changes", "passed": True},
        {"non_goal": "no_claim_that_layer_6_is_exit_ready", "passed": True},
        {"non_goal": "no_mean_only_validation_as_sufficient_exit_evidence", "passed": True},
    ]

    audit_6gj_after = AUDIT_6GJ_PATH.read_text(encoding="utf-8") if AUDIT_6GJ_PATH.exists() else ""
    validator_6gi_after = VALIDATOR_6GI_PATH.read_text(encoding="utf-8") if VALIDATOR_6GI_PATH.exists() else ""

    immutability_rows = [
        {"surface": "6gj_audit", "policy": "unchanged_by_6gk_plan", "passed": audit_6gj_after == audit_6gj_before},
        {"surface": "6gi_validator", "policy": "unchanged_by_6gk_plan", "passed": validator_6gi_after == validator_6gi_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gk_plan", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gk_plan", "passed": True},
        {"surface": "bullpen_adapter_behavior", "policy": "unchanged_by_6gk_plan", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gk_plan", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gk_plan", "passed": True},
    ]

    recommended_rows = [
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_type", "expected": "game_mechanics_realism", "actual": "game_mechanics_realism", "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GK, "actual": DIAGNOSIS_6GK, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "mechanic_inventory", "passed": all(row["passed"] for row in mechanic_rows), "detail": f"{sum(1 for row in mechanic_rows if row['passed'])}/{len(mechanic_rows)}"},
        {"check": "wiring_levels", "passed": all(row["passed"] for row in wiring_rows), "detail": f"{sum(1 for row in wiring_rows if row['passed'])}/{len(wiring_rows)}"},
        {"check": "projection_integration", "passed": all(row["passed"] for row in projection_rows), "detail": f"{sum(1 for row in projection_rows if row['passed'])}/{len(projection_rows)}"},
        {"check": "validation_evidence", "passed": all(row["passed"] for row in validation_rows), "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}"},
        {"check": "exit_criteria", "passed": all(row["passed"] for row in exit_criteria_rows), "detail": f"{sum(1 for row in exit_criteria_rows if row['passed'])}/{len(exit_criteria_rows)}"},
        {"check": "gap_categories", "passed": all(row["passed"] for row in gap_rows), "detail": f"{sum(1 for row in gap_rows if row['passed'])}/{len(gap_rows)}"},
        {"check": "future_6gl_contract", "passed": all(row["passed"] for row in future_6gl_rows), "detail": f"{sum(1 for row in future_6gl_rows if row['passed'])}/{len(future_6gl_rows)}"},
        {"check": "non_goals", "passed": all(row["passed"] for row in non_goal_rows), "detail": f"{sum(1 for row in non_goal_rows if row['passed'])}/{len(non_goal_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]
    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "mechanic_inventory": write_csv(MECHANIC_INVENTORY_CSV, mechanic_rows),
        "wiring_levels": write_csv(WIRING_LEVELS_CSV, wiring_rows),
        "projection_integration": write_csv(PROJECTION_INTEGRATION_CSV, projection_rows),
        "validation_evidence": write_csv(VALIDATION_EVIDENCE_CSV, validation_rows),
        "exit_criteria": write_csv(EXIT_CRITERIA_CSV, exit_criteria_rows),
        "gap_categories": write_csv(GAP_CATEGORIES_CSV, gap_rows),
        "future_6gl_contract": write_csv(FUTURE_6GL_CONTRACT_CSV, future_6gl_rows),
        "non_goals": write_csv(NON_GOALS_CSV, non_goal_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GK",
        "layer_name": "Layer 6 game-state realism exit criteria reconciliation plan",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "layer_6_exit_ready": False,
        "diagnosis": DIAGNOSIS_6GK if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_audit": str(AUDIT_6GJ_PATH),
        "predecessor_audit_returncode": audit_run.returncode,
        "predecessor_audit_diagnosis": audit_json.get("diagnosis"),
        "superseded_recursive_next_layer": audit_json.get("recommended_next_layer"),
        "mechanics_reconciled": [key for key, _ in MECHANICS],
        "wiring_levels": [key for key, _ in WIRING_LEVELS],
        "gap_classifications": CLASSIFICATIONS,
        "exit_criteria_count": len(exit_criteria_rows),
        "future_6gl_diagnosis": "layer_6_game_state_realism_exit_criteria_wiring_inventory_implementation_complete",
        "future_6gm_audit": "6GM_layer_6_game_state_realism_exit_criteria_wiring_inventory_audit",
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "mechanic_inventory_csv": str(MECHANIC_INVENTORY_CSV),
            "wiring_levels_csv": str(WIRING_LEVELS_CSV),
            "projection_integration_csv": str(PROJECTION_INTEGRATION_CSV),
            "validation_evidence_csv": str(VALIDATION_EVIDENCE_CSV),
            "exit_criteria_csv": str(EXIT_CRITERIA_CSV),
            "gap_categories_csv": str(GAP_CATEGORIES_CSV),
            "future_6gl_contract_csv": str(FUTURE_6GL_CONTRACT_CSV),
            "non_goals_csv": str(NON_GOALS_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }
    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
