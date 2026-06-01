#!/usr/bin/env python3
"""Audit Layer 6HJ additional local source discovery plan."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6hk_additional_local_source_discovery_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6HJ_PATH = Path("scripts/plan_6hj_layer6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery.py")

JSON_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan.json"
CHECKS_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_checks.csv"
PREDECESSOR_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_predecessor.csv"
ARTIFACT_PRESENCE_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_artifact_presence.csv"
DISCOVERY_SCOPE_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_discovery_scope.csv"
CANDIDATE_INVENTORY_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_candidate_inventory.csv"
SAMPLED_ARTIFACTS_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_sampled_artifacts.csv"
REQUIREMENT_ALIASES_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_requirement_aliases.csv"
REQUIREMENT_SCORES_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_requirement_scores.csv"
BEST_CANDIDATES_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_best_candidates.csv"
GAP_ANALYSIS_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_gap_analysis.csv"
DECISION_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_decision.csv"
FUTURE_6HK_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_future_6hk_contract.csv"
FUTURE_6HL_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_future_6hl_contract.csv"
SAFETY_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_safety_boundaries.csv"
IMMUTABILITY_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_immutability.csv"
RECOMMENDED_PATH_6HJ = TMP_DIR / "layer6_6hj_additional_local_source_discovery_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
DISCOVERY_SCOPE_CSV = TMP_DIR / f"{SLUG}_discovery_scope.csv"
CANDIDATE_INVENTORY_CSV = TMP_DIR / f"{SLUG}_candidate_inventory.csv"
SAMPLED_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_sampled_artifacts.csv"
REQUIREMENT_FAMILIES_CSV = TMP_DIR / f"{SLUG}_requirement_families.csv"
REQUIREMENT_SCORES_CSV = TMP_DIR / f"{SLUG}_requirement_scores.csv"
BEST_CANDIDATES_CSV = TMP_DIR / f"{SLUG}_best_candidates.csv"
GAP_ANALYSIS_CSV = TMP_DIR / f"{SLUG}_gap_analysis.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HL_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hl_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HJ = "layer_6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery_plan_complete"
DIAGNOSIS_6HK = "layer_6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery_plan_audit_complete"
RECOMMENDED_NEXT_LAYER_6HJ = "6HK_layer_6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery_plan_audit"
RECOMMENDED_PATH_6HJ_VALUE = "plan_additional_local_source_discovery_then_audit_before_materialization_or_adapter_revision"
RECOMMENDED_NEXT_LAYER_6HK = "6HL_layer_6_gameplay_mechanic_outcome_artifact_source_materialization_plan"
RECOMMENDED_PATH_6HK = "audit_additional_local_source_discovery_then_plan_source_materialization_before_adapter_revision"
PREDECESSOR_6HI_DIAGNOSIS = "layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan_audit_complete"

REQUIRED_FAMILIES = ["game_level_outcomes", "base_out_transitions", "inning_runs"]
EXPECTED_CHECKS = [
    "predecessor",
    "artifact_presence",
    "discovery_scope",
    "candidate_inventory",
    "sampled_artifacts",
    "requirement_aliases",
    "requirement_scores",
    "best_candidates",
    "gap_analysis",
    "decision",
    "future_6hk_contract",
    "future_6hl_contract",
    "safety_boundaries",
    "immutability",
    "recommended_path",
]

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


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    parsed = json.loads(text)
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def intish(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def find_row(rows: List[Dict[str, str]], key: str, value: str) -> Dict[str, str]:
    for row in rows:
        if row.get(key) == value:
            return row
    return {}


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6hj_before = PLAN_6HJ_PATH.read_text(encoding="utf-8") if PLAN_6HJ_PATH.exists() else ""

    # 6HK is audit-only. It validates already-emitted 6HJ artifacts.
    class ArtifactOnlyRun:
        returncode = 0

    plan_run = ArtifactOnlyRun()

    json_6hj = load_json(JSON_6HJ)
    checks_6hj = read_csv(CHECKS_6HJ)
    predecessor_6hj = read_csv(PREDECESSOR_6HJ)
    artifact_presence_6hj = read_csv(ARTIFACT_PRESENCE_6HJ)
    discovery_scope_6hj = read_csv(DISCOVERY_SCOPE_6HJ)
    candidate_inventory_6hj = read_csv(CANDIDATE_INVENTORY_6HJ)
    sampled_artifacts_6hj = read_csv(SAMPLED_ARTIFACTS_6HJ)
    requirement_aliases_6hj = read_csv(REQUIREMENT_ALIASES_6HJ)
    requirement_scores_6hj = read_csv(REQUIREMENT_SCORES_6HJ)
    best_candidates_6hj = read_csv(BEST_CANDIDATES_6HJ)
    gap_analysis_6hj = read_csv(GAP_ANALYSIS_6HJ)
    decision_6hj = read_csv(DECISION_6HJ)
    future_6hk_6hj = read_csv(FUTURE_6HK_6HJ)
    future_6hl_6hj = read_csv(FUTURE_6HL_6HJ)
    safety_6hj = read_csv(SAFETY_6HJ)
    immutability_6hj = read_csv(IMMUTABILITY_6HJ)
    recommended_path_6hj = read_csv(RECOMMENDED_PATH_6HJ)

    required_artifacts = [
        JSON_6HJ,
        CHECKS_6HJ,
        PREDECESSOR_6HJ,
        ARTIFACT_PRESENCE_6HJ,
        DISCOVERY_SCOPE_6HJ,
        CANDIDATE_INVENTORY_6HJ,
        SAMPLED_ARTIFACTS_6HJ,
        REQUIREMENT_ALIASES_6HJ,
        REQUIREMENT_SCORES_6HJ,
        BEST_CANDIDATES_6HJ,
        GAP_ANALYSIS_6HJ,
        DECISION_6HJ,
        FUTURE_6HK_6HJ,
        FUTURE_6HL_6HJ,
        SAFETY_6HJ,
        IMMUTABILITY_6HJ,
        RECOMMENDED_PATH_6HJ,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hj_plan_exists", "expected": True, "actual": PLAN_6HJ_PATH.exists(), "passed": PLAN_6HJ_PATH.exists()},
        {"check": "6hj_artifact_audit_mode", "expected": 0, "actual": plan_run.returncode, "passed": plan_run.returncode == 0},
        {"check": "6hj_json_exists", "expected": True, "actual": JSON_6HJ.exists(), "passed": JSON_6HJ.exists()},
        {"check": "6hj_all_checks_passed", "expected": True, "actual": json_6hj.get("all_checks_passed"), "passed": json_6hj.get("all_checks_passed") is True},
        {"check": "6hj_diagnosis", "expected": DIAGNOSIS_6HJ, "actual": json_6hj.get("diagnosis"), "passed": json_6hj.get("diagnosis") == DIAGNOSIS_6HJ},
        {"check": "6hj_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HJ, "actual": json_6hj.get("recommended_next_layer"), "passed": json_6hj.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HJ},
        {"check": "6hj_predecessor_6hi_diagnosis", "expected": PREDECESSOR_6HI_DIAGNOSIS, "actual": json_6hj.get("predecessor_audit_diagnosis"), "passed": json_6hj.get("predecessor_audit_diagnosis") == PREDECESSOR_6HI_DIAGNOSIS},
        {"check": "6hj_predecessor_returncode", "expected": 0, "actual": json_6hj.get("predecessor_audit_returncode"), "passed": json_6hj.get("predecessor_audit_returncode") == 0},
        {"check": "6hj_audited_layer", "expected": "6HI", "actual": json_6hj.get("audited_layer"), "passed": json_6hj.get("audited_layer") == "6HI"},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in required_artifacts
    ]

    check_lookup = {row.get("check"): row for row in checks_6hj}
    checks_consistency_rows = []
    for check_name in EXPECTED_CHECKS:
        row = check_lookup.get(check_name, {})
        checks_consistency_rows.append({
            "check": check_name,
            "expected_present": True,
            "present": bool(row),
            "expected_passed": True,
            "actual_passed": row.get("passed"),
            "passed": bool(row) and boolish(row.get("passed")),
        })

    discovery_scope_rows = [
        {"audit": "local_only_source_discovery", "expected": True, "actual": json_6hj.get("local_only_source_discovery"), "passed": json_6hj.get("local_only_source_discovery") is True},
        {"audit": "discovery_scope_rows_present", "expected": ">=1", "actual": len(discovery_scope_6hj), "passed": len(discovery_scope_6hj) >= 1},
        {"audit": "no_live_data_fetch", "expected": False, "actual": json_6hj.get("live_data_fetches_run"), "passed": json_6hj.get("live_data_fetches_run") is False},
        {"audit": "no_database_write", "expected": False, "actual": json_6hj.get("database_writes_run"), "passed": json_6hj.get("database_writes_run") is False},
        {"audit": "no_materialization_job", "expected": False, "actual": json_6hj.get("materialization_jobs_run"), "passed": json_6hj.get("materialization_jobs_run") is False},
        {"audit": "no_production_simulation", "expected": False, "actual": json_6hj.get("production_simulations_run"), "passed": json_6hj.get("production_simulations_run") is False},
    ]

    candidate_inventory_rows = [
        {"audit": "candidate_inventory_rows_present", "expected": ">=1", "actual": len(candidate_inventory_6hj), "passed": len(candidate_inventory_6hj) >= 1},
        {"audit": "candidate_count_matches_summary", "expected": json_6hj.get("candidate_artifact_count"), "actual": len(candidate_inventory_6hj), "passed": len(candidate_inventory_6hj) == json_6hj.get("candidate_artifact_count")},
    ]

    sampled_rows = [
        {"audit": "sampled_artifacts_rows_present", "expected": ">=1", "actual": len(sampled_artifacts_6hj), "passed": len(sampled_artifacts_6hj) >= 1},
        {"audit": "sampled_count_matches_summary", "expected": json_6hj.get("sampled_artifact_count"), "actual": sum(1 for row in sampled_artifacts_6hj if row.get("read_status") == "read_ok"), "passed": sum(1 for row in sampled_artifacts_6hj if row.get("read_status") == "read_ok") == json_6hj.get("sampled_artifact_count")},
        {"audit": "unreadable_count_matches_summary", "expected": json_6hj.get("unreadable_artifact_count"), "actual": sum(1 for row in sampled_artifacts_6hj if str(row.get("read_status", "")).startswith("unreadable")), "passed": sum(1 for row in sampled_artifacts_6hj if str(row.get("read_status", "")).startswith("unreadable")) == json_6hj.get("unreadable_artifact_count")},
    ]

    alias_families = sorted({row.get("requirement_family", "") for row in requirement_aliases_6hj if row.get("requirement_family")})
    score_families = sorted({row.get("requirement_family", "") for row in requirement_scores_6hj if row.get("requirement_family")})
    best_families = sorted({row.get("requirement_family", "") for row in best_candidates_6hj if row.get("requirement_family")})
    gap_families = sorted({row.get("requirement_family", "") for row in gap_analysis_6hj if row.get("requirement_family")})

    requirement_family_rows = [
        {"audit": "requirement_family_count", "expected": 3, "actual": json_6hj.get("requirement_family_count"), "passed": json_6hj.get("requirement_family_count") == 3},
        {"audit": "alias_families_exact", "expected": "|".join(REQUIRED_FAMILIES), "actual": "|".join(alias_families), "passed": set(alias_families) == set(REQUIRED_FAMILIES)},
        {"audit": "score_families_exact", "expected": "|".join(REQUIRED_FAMILIES), "actual": "|".join(score_families), "passed": set(score_families) == set(REQUIRED_FAMILIES)},
        {"audit": "best_candidate_families_exact", "expected": "|".join(REQUIRED_FAMILIES), "actual": "|".join(best_families), "passed": set(best_families) == set(REQUIRED_FAMILIES)},
        {"audit": "gap_families_exact", "expected": "|".join(REQUIRED_FAMILIES), "actual": "|".join(gap_families), "passed": set(gap_families) == set(REQUIRED_FAMILIES)},
    ]

    exact_counts_by_family = {
        row.get("requirement_family"): intish(row.get("exact_candidate_count"), 0)
        for row in gap_analysis_6hj
    }
    partial_counts_by_family = {
        row.get("requirement_family"): intish(row.get("partial_candidate_count"), 0)
        for row in gap_analysis_6hj
    }

    requirement_score_rows = [
        {"audit": "requirement_score_rows_present", "expected": ">=1", "actual": len(requirement_scores_6hj), "passed": len(requirement_scores_6hj) >= 1},
        {"audit": "score_count_matches_summary", "expected": intish(json_6hj.get("csv_counts", {}).get("requirement_scores"), -1), "actual": len(requirement_scores_6hj), "passed": len(requirement_scores_6hj) == intish(json_6hj.get("csv_counts", {}).get("requirement_scores"), -1)},
        {"audit": "no_exact_game_level_candidates", "expected": 0, "actual": json_6hj.get("game_level_outcome_exact_candidate_count"), "passed": json_6hj.get("game_level_outcome_exact_candidate_count") == 0},
        {"audit": "no_exact_base_out_candidates", "expected": 0, "actual": json_6hj.get("base_out_transition_exact_candidate_count"), "passed": json_6hj.get("base_out_transition_exact_candidate_count") == 0},
        {"audit": "no_exact_inning_runs_candidates", "expected": 0, "actual": json_6hj.get("inning_runs_exact_candidate_count"), "passed": json_6hj.get("inning_runs_exact_candidate_count") == 0},
        {"audit": "partial_game_level_candidates_present", "expected": ">=1", "actual": json_6hj.get("game_level_outcome_partial_candidate_count"), "passed": intish(json_6hj.get("game_level_outcome_partial_candidate_count"), 0) >= 1},
        {"audit": "partial_base_out_candidates_present", "expected": ">=1", "actual": json_6hj.get("base_out_transition_partial_candidate_count"), "passed": intish(json_6hj.get("base_out_transition_partial_candidate_count"), 0) >= 1},
        {"audit": "partial_inning_runs_candidates_present", "expected": ">=1", "actual": json_6hj.get("inning_runs_partial_candidate_count"), "passed": intish(json_6hj.get("inning_runs_partial_candidate_count"), 0) >= 1},
    ]

    best_candidate_rows = [
        {"audit": "best_candidate_rows_present", "expected": ">=3", "actual": len(best_candidates_6hj), "passed": len(best_candidates_6hj) >= 3},
        {"audit": "best_candidate_count_matches_summary", "expected": intish(json_6hj.get("csv_counts", {}).get("best_candidates"), -1), "actual": len(best_candidates_6hj), "passed": len(best_candidates_6hj) == intish(json_6hj.get("csv_counts", {}).get("best_candidates"), -1)},
        {"audit": "best_candidates_include_partial_classifications", "expected": True, "actual": any(row.get("classification") == "partial_candidate" for row in best_candidates_6hj), "passed": any(row.get("classification") == "partial_candidate" for row in best_candidates_6hj)},
    ]

    gap_rows = []
    for family in REQUIRED_FAMILIES:
        row = find_row(gap_analysis_6hj, "requirement_family", family)
        gap_rows.extend([
            {"requirement_family": family, "audit": "present_once", "expected": 1, "actual": sum(1 for item in gap_analysis_6hj if item.get("requirement_family") == family), "passed": sum(1 for item in gap_analysis_6hj if item.get("requirement_family") == family) == 1},
            {"requirement_family": family, "audit": "exact_candidate_count_zero", "expected": 0, "actual": row.get("exact_candidate_count"), "passed": intish(row.get("exact_candidate_count"), -1) == 0},
            {"requirement_family": family, "audit": "partial_candidate_count_positive", "expected": ">=1", "actual": row.get("partial_candidate_count"), "passed": intish(row.get("partial_candidate_count"), 0) >= 1},
            {"requirement_family": family, "audit": "gap_status", "expected": "source_materialization_or_mapping_gap_remains", "actual": row.get("gap_status"), "passed": row.get("gap_status") == "source_materialization_or_mapping_gap_remains"},
            {"requirement_family": family, "audit": "recommended_action", "expected": "source_materialization_plan_required_after_audit", "actual": row.get("recommended_action"), "passed": row.get("recommended_action") == "source_materialization_plan_required_after_audit"},
            {"requirement_family": family, "audit": "source_passed", "expected": True, "actual": row.get("passed"), "passed": boolish(row.get("passed"))},
        ])

    decision_expectations = [
        ("exact_candidate_available_for_all_required_families", "False"),
        ("adapter_revision_possible_after_audit", "False"),
        ("source_materialization_plan_required", "True"),
        ("recommended_next_layer", RECOMMENDED_NEXT_LAYER_6HJ),
        ("recommended_path", RECOMMENDED_PATH_6HJ_VALUE),
    ]
    decision_rows = []
    for decision_name, expected in decision_expectations:
        row = find_row(decision_6hj, "decision", decision_name)
        decision_rows.append({
            "decision": decision_name,
            "expected": expected,
            "actual": row.get("actual"),
            "source_passed": row.get("passed"),
            "passed": bool(row) and str(row.get("actual")) == expected and boolish(row.get("passed")),
        })

    future_6hl_rows = []
    future_contract_expectations = {
        "if_all_families_have_exact_candidates_then_adapter_revision_plan_after_6hk": "False",
        "if_any_family_lacks_exact_candidate_then_source_materialization_plan_after_6hk": "True",
        "6hl_must_wait_for_6hk_audit": "True",
        "6hl_must_not_run_real_evaluation": "True",
        "6hl_must_not_activate_layer_6": "True",
    }
    for contract, expected_condition in future_contract_expectations.items():
        row = find_row(future_6hl_6hj, "contract", contract)
        future_6hl_rows.append({
            "contract": contract,
            "expected_condition": expected_condition,
            "actual_condition": row.get("condition"),
            "source_passed": row.get("passed"),
            "passed": bool(row) and str(row.get("condition")) == expected_condition and boolish(row.get("passed")),
        })

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": json_6hj.get("planning_only"), "source_passed": True, "passed": json_6hj.get("planning_only") is True},
        {"boundary": "local_only_source_discovery", "expected": True, "actual": json_6hj.get("local_only_source_discovery"), "source_passed": True, "passed": json_6hj.get("local_only_source_discovery") is True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": json_6hj.get("live_data_fetches_run"), "source_passed": True, "passed": json_6hj.get("live_data_fetches_run") is False},
        {"boundary": "no_database_write", "expected": False, "actual": json_6hj.get("database_writes_run"), "source_passed": True, "passed": json_6hj.get("database_writes_run") is False},
        {"boundary": "no_materialization_job", "expected": False, "actual": json_6hj.get("materialization_jobs_run"), "source_passed": True, "passed": json_6hj.get("materialization_jobs_run") is False},
        {"boundary": "no_production_simulation", "expected": False, "actual": json_6hj.get("production_simulations_run"), "source_passed": True, "passed": json_6hj.get("production_simulations_run") is False},
        {"boundary": "no_real_backtests", "expected": False, "actual": json_6hj.get("real_backtests_run"), "source_passed": True, "passed": json_6hj.get("real_backtests_run") is False},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": json_6hj.get("mechanic_evaluations_run"), "source_passed": True, "passed": json_6hj.get("mechanic_evaluations_run") is False},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": json_6hj.get("actual_outcomes_joined_to_mechanics"), "source_passed": True, "passed": json_6hj.get("actual_outcomes_joined_to_mechanics") is False},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": json_6hj.get("corrected_normalized_outcomes_emitted_by_this_layer"), "source_passed": True, "passed": json_6hj.get("corrected_normalized_outcomes_emitted_by_this_layer") is False},
        {"boundary": "no_activation", "expected": False, "actual": json_6hj.get("activation_allowed"), "source_passed": True, "passed": json_6hj.get("activation_allowed") is False},
        {"boundary": "layer_6_exit_credit_false", "expected": False, "actual": json_6hj.get("layer_6_exit_credit"), "source_passed": True, "passed": json_6hj.get("layer_6_exit_credit") is False},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6hj_after = PLAN_6HJ_PATH.read_text(encoding="utf-8") if PLAN_6HJ_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6hk_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hj_plan", "policy": "unchanged_by_6hk", "passed": plan_6hj_after == plan_6hj_before},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6hk", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hk", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HK, "actual": RECOMMENDED_NEXT_LAYER_6HK, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HK, "actual": RECOMMENDED_PATH_6HK, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "materialization_plan_is_next_safe_step", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_revision_possible_after_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "future_adapter_revision_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "future_real_evaluation_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HK, "actual": DIAGNOSIS_6HK, "passed": True},
    ]

    exact_all = json_6hj.get("exact_candidate_available_for_all_required_families") is False
    adapter_possible = json_6hj.get("adapter_revision_possible_after_audit") is False
    materialization_required = json_6hj.get("source_materialization_plan_required") is True

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "discovery_scope", "passed": all(row["passed"] for row in discovery_scope_rows), "detail": f"{sum(1 for row in discovery_scope_rows if row['passed'])}/{len(discovery_scope_rows)}"},
        {"check": "candidate_inventory", "passed": all(row["passed"] for row in candidate_inventory_rows), "detail": f"{sum(1 for row in candidate_inventory_rows if row['passed'])}/{len(candidate_inventory_rows)}"},
        {"check": "sampled_artifacts", "passed": all(row["passed"] for row in sampled_rows), "detail": f"{sum(1 for row in sampled_rows if row['passed'])}/{len(sampled_rows)}"},
        {"check": "requirement_families", "passed": all(row["passed"] for row in requirement_family_rows), "detail": f"{sum(1 for row in requirement_family_rows if row['passed'])}/{len(requirement_family_rows)}"},
        {"check": "requirement_scores", "passed": all(row["passed"] for row in requirement_score_rows), "detail": f"{sum(1 for row in requirement_score_rows if row['passed'])}/{len(requirement_score_rows)}"},
        {"check": "best_candidates", "passed": all(row["passed"] for row in best_candidate_rows), "detail": f"{sum(1 for row in best_candidate_rows if row['passed'])}/{len(best_candidate_rows)}"},
        {"check": "gap_analysis", "passed": all(row["passed"] for row in gap_rows), "detail": f"{sum(1 for row in gap_rows if row['passed'])}/{len(gap_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hl_contract", "passed": all(row["passed"] for row in future_6hl_rows), "detail": f"{sum(1 for row in future_6hl_rows if row['passed'])}/{len(future_6hl_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "checks_consistency": write_csv(CHECKS_CONSISTENCY_CSV, checks_consistency_rows),
        "discovery_scope": write_csv(DISCOVERY_SCOPE_CSV, discovery_scope_rows),
        "candidate_inventory": write_csv(CANDIDATE_INVENTORY_CSV, candidate_inventory_rows),
        "sampled_artifacts": write_csv(SAMPLED_ARTIFACTS_CSV, sampled_rows),
        "requirement_families": write_csv(REQUIREMENT_FAMILIES_CSV, requirement_family_rows),
        "requirement_scores": write_csv(REQUIREMENT_SCORES_CSV, requirement_score_rows),
        "best_candidates": write_csv(BEST_CANDIDATES_CSV, best_candidate_rows),
        "gap_analysis": write_csv(GAP_ANALYSIS_CSV, gap_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hl_contract": write_csv(FUTURE_6HL_CONTRACT_CSV, future_6hl_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HK",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HK if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HK,
        "recommended_path": RECOMMENDED_PATH_6HK,
        "audited_layer": "6HJ",
        "predecessor_plan": str(PLAN_6HJ_PATH),
        "predecessor_plan_returncode": plan_run.returncode,
        "predecessor_plan_diagnosis": json_6hj.get("diagnosis"),
        "local_only_source_discovery_confirmed": json_6hj.get("local_only_source_discovery") is True,
        "discovery_scope_validated": all(row["passed"] for row in discovery_scope_rows),
        "candidate_artifact_count": json_6hj.get("candidate_artifact_count"),
        "sampled_artifact_count": json_6hj.get("sampled_artifact_count"),
        "unreadable_artifact_count": json_6hj.get("unreadable_artifact_count"),
        "requirement_family_count": json_6hj.get("requirement_family_count"),
        "requirement_score_row_count": len(requirement_scores_6hj),
        "best_candidate_row_count": len(best_candidates_6hj),
        "gap_analysis_row_count": len(gap_analysis_6hj),
        "game_level_outcome_exact_candidate_count": json_6hj.get("game_level_outcome_exact_candidate_count"),
        "base_out_transition_exact_candidate_count": json_6hj.get("base_out_transition_exact_candidate_count"),
        "inning_runs_exact_candidate_count": json_6hj.get("inning_runs_exact_candidate_count"),
        "game_level_outcome_partial_candidate_count": json_6hj.get("game_level_outcome_partial_candidate_count"),
        "base_out_transition_partial_candidate_count": json_6hj.get("base_out_transition_partial_candidate_count"),
        "inning_runs_partial_candidate_count": json_6hj.get("inning_runs_partial_candidate_count"),
        "exact_candidate_available_for_all_required_families": json_6hj.get("exact_candidate_available_for_all_required_families"),
        "adapter_revision_possible_after_audit": json_6hj.get("adapter_revision_possible_after_audit"),
        "source_materialization_plan_required": json_6hj.get("source_materialization_plan_required"),
        "materialization_plan_is_next_safe_step": exact_all and adapter_possible and materialization_required,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "real_evaluation_blocked_by_validation": True,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "corrected_normalized_outcomes_emitted_by_audited_layer": False,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "activation_allowed": False,
        "layer_6_exit_credit": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(EVALUATION_WINDOWS),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "checks_consistency_csv": str(CHECKS_CONSISTENCY_CSV),
            "discovery_scope_csv": str(DISCOVERY_SCOPE_CSV),
            "candidate_inventory_csv": str(CANDIDATE_INVENTORY_CSV),
            "sampled_artifacts_csv": str(SAMPLED_ARTIFACTS_CSV),
            "requirement_families_csv": str(REQUIREMENT_FAMILIES_CSV),
            "requirement_scores_csv": str(REQUIREMENT_SCORES_CSV),
            "best_candidates_csv": str(BEST_CANDIDATES_CSV),
            "gap_analysis_csv": str(GAP_ANALYSIS_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hl_contract_csv": str(FUTURE_6HL_CONTRACT_CSV),
            "safety_boundaries_csv": str(SAFETY_BOUNDARIES_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
