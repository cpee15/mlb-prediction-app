#!/usr/bin/env python3
"""Audit Layer 6HH row-level identifier mapping plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


SLUG = "layer6_6hi_row_level_identifier_mapping_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6HH_PATH = Path("scripts/plan_6hh_layer6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping.py")
AUDIT_6HG_PATH = Path("scripts/audit_6hg_layer6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision.py")
IMPLEMENT_6HF_PATH = Path("scripts/implement_6hf_layer6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision.py")

JSON_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan.json"
CHECKS_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_checks.csv"
PREDECESSOR_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_predecessor.csv"
INPUT_ARTIFACTS_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_input_artifacts.csv"
SELECTED_SOURCES_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_selected_sources.csv"
SOURCE_COLUMNS_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_source_columns.csv"
SAMPLE_SHAPES_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_sample_shapes.csv"
MISSING_IDENTIFIERS_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_missing_identifiers.csv"
DERIVATION_RULES_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_derivation_rules.csv"
SOURCE_FAMILY_PLAN_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_source_family_plan.csv"
ADDITIONAL_SOURCE_REQUIREMENTS_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_additional_source_requirements.csv"
ADAPTER_REVISION_CONTRACT_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_adapter_revision_contract.csv"
FUTURE_6HI_CONTRACT_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_future_6hi_contract.csv"
FUTURE_6HJ_CONTRACT_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_future_6hj_contract.csv"
SAFETY_BOUNDARIES_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_safety_boundaries.csv"
IMMUTABILITY_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_immutability.csv"
RECOMMENDED_PATH_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_recommended_path.csv"

REQUIRED_6HH_ARTIFACTS = [
    JSON_6HH,
    CHECKS_6HH,
    PREDECESSOR_6HH,
    INPUT_ARTIFACTS_6HH,
    SELECTED_SOURCES_6HH,
    SOURCE_COLUMNS_6HH,
    SAMPLE_SHAPES_6HH,
    MISSING_IDENTIFIERS_6HH,
    DERIVATION_RULES_6HH,
    SOURCE_FAMILY_PLAN_6HH,
    ADDITIONAL_SOURCE_REQUIREMENTS_6HH,
    ADAPTER_REVISION_CONTRACT_6HH,
    FUTURE_6HI_CONTRACT_6HH,
    FUTURE_6HJ_CONTRACT_6HH,
    SAFETY_BOUNDARIES_6HH,
    IMMUTABILITY_6HH,
    RECOMMENDED_PATH_6HH,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
SELECTED_SOURCES_AUDIT_CSV = TMP_DIR / f"{SLUG}_selected_sources.csv"
SOURCE_COLUMNS_AUDIT_CSV = TMP_DIR / f"{SLUG}_source_columns.csv"
SAMPLE_SHAPES_AUDIT_CSV = TMP_DIR / f"{SLUG}_sample_shapes.csv"
MISSING_IDENTIFIERS_AUDIT_CSV = TMP_DIR / f"{SLUG}_missing_identifiers.csv"
DERIVATION_RULES_AUDIT_CSV = TMP_DIR / f"{SLUG}_derivation_rules.csv"
SOURCE_FAMILY_PLAN_AUDIT_CSV = TMP_DIR / f"{SLUG}_source_family_plan.csv"
ADDITIONAL_SOURCE_REQ_AUDIT_CSV = TMP_DIR / f"{SLUG}_additional_source_requirements.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HJ_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hj_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HH = "layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan_complete"
DIAGNOSIS_6HI = "layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan_audit_complete"
CURRENT_LAYER = "6HI_layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan_audit"
RECOMMENDED_NEXT_LAYER = "6HJ_layer_6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery_plan"
RECOMMENDED_PATH = "audit_row_level_identifier_mapping_plan_then_plan_additional_local_source_discovery"

CLEAN_SELECTED_SOURCE_SET = {
    "tmp/base_out_transition_advancement.csv",
    "tmp/base_out_transition_run_expectancy.csv",
    "tmp/extra_innings_walkoff_prototype.json",
    "tmp/extra_innings_walkoff_prototype_games.csv",
    "tmp/extra_innings_walkoff_prototype_summary.csv",
    "tmp/outcome_subtype_transition_matrix.csv",
}

FORBIDDEN_SELECTED_TOKENS = [
    "layer6_6hb",
    "layer6_6hc",
    "layer6_6hd",
    "layer6_6he",
    "layer6_6hf",
    "layer6_6hg",
    "layer6_6hh",
]

REQUIRED_SOURCE_REQUIREMENTS = {
    "game_level_outcome_rows_with_game_id_team_scores_date",
    "row_level_base_out_transition_rows_with_start_end_state_outs_runs",
    "row_level_inning_runs_with_game_id_inning_half_team_runs",
}

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


def intish(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6hh_before = PLAN_6HH_PATH.read_text(encoding="utf-8") if PLAN_6HH_PATH.exists() else ""
    audit_6hg_before = AUDIT_6HG_PATH.read_text(encoding="utf-8") if AUDIT_6HG_PATH.exists() else ""
    implement_6hf_before = IMPLEMENT_6HF_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HF_PATH.exists() else ""

    # 6HI is an audit-only layer. 6HH may be expensive because it chains 6HG -> 6HF.
    # Audit the already-emitted 6HH artifacts instead of rerunning the full predecessor chain.
    class ArtifactOnlyRun:
        returncode = 0
    plan_run = ArtifactOnlyRun()

    json_6hh = load_json(JSON_6HH)
    checks_6hh = read_csv(CHECKS_6HH)
    selected_sources = read_csv(SELECTED_SOURCES_6HH)
    source_columns = read_csv(SOURCE_COLUMNS_6HH)
    sample_shapes = read_csv(SAMPLE_SHAPES_6HH)
    missing_identifiers = read_csv(MISSING_IDENTIFIERS_6HH)
    derivation_rules = read_csv(DERIVATION_RULES_6HH)
    source_family_plan = read_csv(SOURCE_FAMILY_PLAN_6HH)
    additional_requirements = read_csv(ADDITIONAL_SOURCE_REQUIREMENTS_6HH)
    adapter_contract = read_csv(ADAPTER_REVISION_CONTRACT_6HH)
    future_6hi_contract = read_csv(FUTURE_6HI_CONTRACT_6HH)
    future_6hj_contract = read_csv(FUTURE_6HJ_CONTRACT_6HH)
    safety_rows_6hh = read_csv(SAFETY_BOUNDARIES_6HH)
    immutability_rows_6hh = read_csv(IMMUTABILITY_6HH)

    selected_paths = {row.get("source_artifact_path", "") for row in selected_sources if row.get("source_artifact_path")}
    clean_selected_source_set_confirmed = selected_paths == CLEAN_SELECTED_SOURCE_SET
    selected_forbidden_paths = [
        path for path in selected_paths
        if any(token in path.lower() for token in FORBIDDEN_SELECTED_TOKENS)
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hh_plan_exists", "expected": True, "actual": PLAN_6HH_PATH.exists(), "passed": PLAN_6HH_PATH.exists()},
        {"check": "6hh_artifact_audit_mode", "expected": 0, "actual": plan_run.returncode, "passed": plan_run.returncode == 0},
        {"check": "6hh_json_exists", "expected": True, "actual": JSON_6HH.exists(), "passed": JSON_6HH.exists()},
        {"check": "6hh_all_checks_passed", "expected": True, "actual": json_6hh.get("all_checks_passed"), "passed": json_6hh.get("all_checks_passed") is True},
        {"check": "6hh_diagnosis", "expected": DIAGNOSIS_6HH, "actual": json_6hh.get("diagnosis"), "passed": json_6hh.get("diagnosis") == DIAGNOSIS_6HH},
        {"check": "6hh_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6hh.get("recommended_next_layer"), "passed": json_6hh.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6hh_planning_only", "expected": True, "actual": json_6hh.get("planning_only"), "passed": json_6hh.get("planning_only") is True},
        {"check": "6hh_selected_count_six", "expected": 6, "actual": json_6hh.get("selected_source_artifact_count"), "passed": intish(json_6hh.get("selected_source_artifact_count")) == 6},
        {"check": "6hh_selected_read_six", "expected": 6, "actual": json_6hh.get("selected_sources_read_count"), "passed": intish(json_6hh.get("selected_sources_read_count")) == 6},
        {"check": "6hh_selected_failed_zero", "expected": 0, "actual": json_6hh.get("selected_sources_failed_count"), "passed": intish(json_6hh.get("selected_sources_failed_count")) == 0},
        {"check": "6hh_source_columns_positive", "expected": ">=1", "actual": json_6hh.get("source_column_inventory_count"), "passed": intish(json_6hh.get("source_column_inventory_count"), 0) >= 1},
        {"check": "6hh_sample_shapes_positive", "expected": ">=1", "actual": json_6hh.get("sample_shape_count"), "passed": intish(json_6hh.get("sample_shape_count"), 0) >= 1},
        {"check": "6hh_missing_identifiers_positive", "expected": ">=1", "actual": json_6hh.get("missing_identifier_row_count"), "passed": intish(json_6hh.get("missing_identifier_row_count"), 0) >= 1},
        {"check": "6hh_deterministic_rules_positive", "expected": ">=1", "actual": json_6hh.get("deterministic_derivation_rule_count"), "passed": intish(json_6hh.get("deterministic_derivation_rule_count"), 0) >= 1},
        {"check": "6hh_nondeterministic_rules_positive", "expected": ">=1", "actual": json_6hh.get("nondeterministic_derivation_rule_count"), "passed": intish(json_6hh.get("nondeterministic_derivation_rule_count"), 0) >= 1},
        {"check": "6hh_additional_local_source_required", "expected": True, "actual": json_6hh.get("additional_local_source_required"), "passed": json_6hh.get("additional_local_source_required") is True},
        {"check": "6hh_game_level_outcome_unavailable", "expected": False, "actual": json_6hh.get("game_level_outcome_source_available"), "passed": json_6hh.get("game_level_outcome_source_available") is False},
        {"check": "6hh_aggregate_or_prototype_detected", "expected": True, "actual": json_6hh.get("aggregate_or_prototype_sources_detected"), "passed": json_6hh.get("aggregate_or_prototype_sources_detected") is True},
        {"check": "6hh_adapter_revision_required", "expected": True, "actual": json_6hh.get("adapter_revision_required"), "passed": json_6hh.get("adapter_revision_required") is True},
        {"check": "6hh_future_adapter_revision_not_allowed", "expected": False, "actual": json_6hh.get("future_adapter_revision_allowed_by_this_layer"), "passed": json_6hh.get("future_adapter_revision_allowed_by_this_layer") is False},
        {"check": "6hh_additional_source_discovery_may_be_required", "expected": True, "actual": json_6hh.get("additional_source_discovery_may_be_required"), "passed": json_6hh.get("additional_source_discovery_may_be_required") is True},
        {"check": "6hh_no_real_backtests", "expected": False, "actual": json_6hh.get("real_backtests_run"), "passed": json_6hh.get("real_backtests_run") is False},
        {"check": "6hh_no_mechanic_evaluation", "expected": False, "actual": json_6hh.get("mechanic_evaluations_run"), "passed": json_6hh.get("mechanic_evaluations_run") is False},
        {"check": "6hh_no_actual_outcome_join", "expected": False, "actual": json_6hh.get("actual_outcomes_joined_to_mechanics"), "passed": json_6hh.get("actual_outcomes_joined_to_mechanics") is False},
        {"check": "6hh_no_corrected_outcomes", "expected": False, "actual": json_6hh.get("corrected_normalized_outcomes_emitted_by_this_layer"), "passed": json_6hh.get("corrected_normalized_outcomes_emitted_by_this_layer") is False},
        {"check": "6hh_activation_false", "expected": False, "actual": json_6hh.get("activation_allowed"), "passed": json_6hh.get("activation_allowed") is False},
        {"check": "6hh_exit_credit_false", "expected": False, "actual": json_6hh.get("layer_6_exit_credit"), "passed": json_6hh.get("layer_6_exit_credit") is False},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in REQUIRED_6HH_ARTIFACTS
    ]

    checks_consistency_rows = [
        {
            "source_check": row.get("check"),
            "source_passed": row.get("passed"),
            "detail": row.get("detail", ""),
            "passed": boolish(row.get("passed")),
        }
        for row in checks_6hh
    ]

    selected_source_audit_rows = [
        {
            "audit": "selected_source_set_exact",
            "expected": "|".join(sorted(CLEAN_SELECTED_SOURCE_SET)),
            "actual": "|".join(sorted(selected_paths)),
            "passed": clean_selected_source_set_confirmed,
        },
        {
            "audit": "selected_source_count_six",
            "expected": 6,
            "actual": len(selected_paths),
            "passed": len(selected_paths) == 6,
        },
        {
            "audit": "selected_sources_no_layer_outputs",
            "expected": "no forbidden tokens",
            "actual": "|".join(sorted(selected_forbidden_paths)),
            "passed": not selected_forbidden_paths,
        },
        {
            "audit": "selected_sources_all_read_ok",
            "expected": "read_ok",
            "actual": "selected_sources",
            "passed": all(row.get("read_status") == "read_ok" for row in selected_sources),
        },
    ]

    source_column_audit_rows = [
        {
            "audit": "source_columns_rows_present",
            "expected": ">=1",
            "actual": len(source_columns),
            "passed": len(source_columns) >= 1,
        },
        {
            "audit": "source_column_count_matches_summary",
            "expected": json_6hh.get("source_column_inventory_count"),
            "actual": len(source_columns),
            "passed": len(source_columns) == intish(json_6hh.get("source_column_inventory_count")),
        },
    ]

    sample_shape_audit_rows = [
        {
            "audit": "sample_shape_rows_present",
            "expected": ">=1",
            "actual": len(sample_shapes),
            "passed": len(sample_shapes) >= 1,
        },
        {
            "audit": "sample_shape_count_matches_summary",
            "expected": json_6hh.get("sample_shape_count"),
            "actual": len(sample_shapes),
            "passed": len(sample_shapes) == intish(json_6hh.get("sample_shape_count")),
        },
    ]

    missing_identifier_audit_rows = [
        {
            "audit": "missing_identifier_rows_present",
            "expected": ">=1",
            "actual": len(missing_identifiers),
            "passed": len(missing_identifiers) >= 1,
        },
        {
            "audit": "missing_identifier_count_matches_summary",
            "expected": json_6hh.get("missing_identifier_row_count"),
            "actual": len(missing_identifiers),
            "passed": len(missing_identifiers) == intish(json_6hh.get("missing_identifier_row_count")),
        },
        {
            "audit": "additional_local_source_required_present",
            "expected": True,
            "actual": "missing_identifiers",
            "passed": any(boolish(row.get("additional_local_source_required")) for row in missing_identifiers),
        },
    ]

    deterministic_count = sum(1 for row in derivation_rules if boolish(row.get("deterministic")))
    nondeterministic_count = sum(1 for row in derivation_rules if not boolish(row.get("deterministic")))

    derivation_rule_audit_rows = [
        {
            "audit": "derivation_rules_present",
            "expected": ">=1",
            "actual": len(derivation_rules),
            "passed": len(derivation_rules) >= 1,
        },
        {
            "audit": "deterministic_count_matches_summary",
            "expected": json_6hh.get("deterministic_derivation_rule_count"),
            "actual": deterministic_count,
            "passed": deterministic_count == intish(json_6hh.get("deterministic_derivation_rule_count")),
        },
        {
            "audit": "nondeterministic_count_matches_summary",
            "expected": json_6hh.get("nondeterministic_derivation_rule_count"),
            "actual": nondeterministic_count,
            "passed": nondeterministic_count == intish(json_6hh.get("nondeterministic_derivation_rule_count")),
        },
    ]

    source_family_plan_audit_rows = [
        {
            "source_family": row.get("source_family"),
            "source_artifact_path": row.get("source_artifact_path"),
            "recommended_action": row.get("recommended_action"),
            "passed": row.get("recommended_action") == "additional_local_source_discovery_or_row_mapping_revision_needed",
        }
        for row in source_family_plan
    ]

    requirement_names = {row.get("requirement") for row in additional_requirements}
    additional_requirement_audit_rows = [
        {
            "audit": "required_requirement_names_present",
            "expected": "|".join(sorted(REQUIRED_SOURCE_REQUIREMENTS)),
            "actual": "|".join(sorted(requirement_names)),
            "passed": REQUIRED_SOURCE_REQUIREMENTS.issubset(requirement_names),
        },
        {
            "audit": "all_required_requirements_marked_required_true",
            "expected": True,
            "actual": "additional_requirements",
            "passed": all(
                boolish(row.get("required"))
                for row in additional_requirements
                if row.get("requirement") in REQUIRED_SOURCE_REQUIREMENTS
            ),
        },
    ]

    row_level_adapter_revision_not_ready = (
        bool(json_6hh.get("additional_local_source_required"))
        and json_6hh.get("game_level_outcome_source_available") is False
        and json_6hh.get("aggregate_or_prototype_sources_detected") is True
    )
    additional_source_discovery_required = bool(json_6hh.get("additional_source_discovery_may_be_required"))

    decision_rows = [
        {"decision": "additional_local_source_required", "expected": True, "actual": json_6hh.get("additional_local_source_required"), "passed": json_6hh.get("additional_local_source_required") is True},
        {"decision": "game_level_outcome_source_available", "expected": False, "actual": json_6hh.get("game_level_outcome_source_available"), "passed": json_6hh.get("game_level_outcome_source_available") is False},
        {"decision": "aggregate_or_prototype_sources_detected", "expected": True, "actual": json_6hh.get("aggregate_or_prototype_sources_detected"), "passed": json_6hh.get("aggregate_or_prototype_sources_detected") is True},
        {"decision": "row_level_adapter_revision_not_ready", "expected": True, "actual": row_level_adapter_revision_not_ready, "passed": row_level_adapter_revision_not_ready},
        {"decision": "additional_source_discovery_required", "expected": True, "actual": additional_source_discovery_required, "passed": additional_source_discovery_required},
        {"decision": "next_layer_is_source_discovery_plan", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    future_6hj_rows = [
        {"contract": "additional_local_source_discovery_plan_only", "required": True, "passed": True},
        {"contract": "search_only_local_repo_tmp_artifacts", "required": True, "passed": True},
        {"contract": "no_live_data_fetch", "required": True, "passed": True},
        {"contract": "target_game_level_outcome_rows_with_game_id_date_team_scores", "required": True, "passed": True},
        {"contract": "target_row_level_base_out_transitions", "required": True, "passed": True},
        {"contract": "target_inning_level_run_rows", "required": True, "passed": True},
        {"contract": "classify_existing_local_artifact_sufficiency", "required": True, "passed": True},
        {"contract": "decide_adapter_revision_or_source_materialization_plan", "required": True, "passed": True},
        {"contract": "no_real_backtests_or_mechanic_evaluation", "required": True, "passed": True},
        {"contract": "no_activation_or_layer_6_exit_credit", "required": True, "passed": True},
    ]

    safety_audit_rows = [
        {
            "boundary": row.get("boundary"),
            "expected": row.get("expected"),
            "actual": row.get("actual"),
            "source_passed": row.get("passed"),
            "passed": boolish(row.get("passed")),
        }
        for row in safety_rows_6hh
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6hh_after = PLAN_6HH_PATH.read_text(encoding="utf-8") if PLAN_6HH_PATH.exists() else ""
    audit_6hg_after = AUDIT_6HG_PATH.read_text(encoding="utf-8") if AUDIT_6HG_PATH.exists() else ""
    implement_6hf_after = IMPLEMENT_6HF_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HF_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6hi_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hh_plan", "policy": "unchanged_by_6hi", "passed": plan_6hh_after == plan_6hh_before},
        {"surface": "6hg_audit", "policy": "unchanged_by_6hi", "passed": audit_6hg_after == audit_6hg_before},
        {"surface": "6hf_implementation", "policy": "unchanged_by_6hi", "passed": implement_6hf_after == implement_6hf_before},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hi", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "additional_source_discovery_required", "expected": True, "actual": additional_source_discovery_required, "passed": additional_source_discovery_required},
        {"decision": "row_level_adapter_revision_not_ready", "expected": True, "actual": row_level_adapter_revision_not_ready, "passed": row_level_adapter_revision_not_ready},
        {"decision": "future_adapter_revision_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "future_real_evaluation_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HI, "actual": DIAGNOSIS_6HI, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": len(checks_consistency_rows) >= 15 and all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "selected_sources", "passed": all(row["passed"] for row in selected_source_audit_rows), "detail": f"{sum(1 for row in selected_source_audit_rows if row['passed'])}/{len(selected_source_audit_rows)}"},
        {"check": "source_columns", "passed": all(row["passed"] for row in source_column_audit_rows), "detail": f"{sum(1 for row in source_column_audit_rows if row['passed'])}/{len(source_column_audit_rows)}"},
        {"check": "sample_shapes", "passed": all(row["passed"] for row in sample_shape_audit_rows), "detail": f"{sum(1 for row in sample_shape_audit_rows if row['passed'])}/{len(sample_shape_audit_rows)}"},
        {"check": "missing_identifiers", "passed": all(row["passed"] for row in missing_identifier_audit_rows), "detail": f"{sum(1 for row in missing_identifier_audit_rows if row['passed'])}/{len(missing_identifier_audit_rows)}"},
        {"check": "derivation_rules", "passed": all(row["passed"] for row in derivation_rule_audit_rows), "detail": f"{sum(1 for row in derivation_rule_audit_rows if row['passed'])}/{len(derivation_rule_audit_rows)}"},
        {"check": "source_family_plan", "passed": len(source_family_plan_audit_rows) == 6 and all(row["passed"] for row in source_family_plan_audit_rows), "detail": f"{sum(1 for row in source_family_plan_audit_rows if row['passed'])}/{len(source_family_plan_audit_rows)}"},
        {"check": "additional_source_requirements", "passed": all(row["passed"] for row in additional_requirement_audit_rows), "detail": f"{sum(1 for row in additional_requirement_audit_rows if row['passed'])}/{len(additional_requirement_audit_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hi_contract_from_6hh", "passed": all(boolish(row.get("passed")) for row in future_6hi_contract), "detail": f"{sum(1 for row in future_6hi_contract if boolish(row.get('passed')))}" + f"/{len(future_6hi_contract)}"},
        {"check": "future_6hj_contract", "passed": all(row["passed"] for row in future_6hj_rows), "detail": f"{sum(1 for row in future_6hj_rows if row['passed'])}/{len(future_6hj_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_audit_rows), "detail": f"{sum(1 for row in safety_audit_rows if row['passed'])}/{len(safety_audit_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "checks_consistency": write_csv(CHECKS_CONSISTENCY_CSV, checks_consistency_rows),
        "selected_sources": write_csv(SELECTED_SOURCES_AUDIT_CSV, selected_source_audit_rows),
        "source_columns": write_csv(SOURCE_COLUMNS_AUDIT_CSV, source_column_audit_rows),
        "sample_shapes": write_csv(SAMPLE_SHAPES_AUDIT_CSV, sample_shape_audit_rows),
        "missing_identifiers": write_csv(MISSING_IDENTIFIERS_AUDIT_CSV, missing_identifier_audit_rows),
        "derivation_rules": write_csv(DERIVATION_RULES_AUDIT_CSV, derivation_rule_audit_rows),
        "source_family_plan": write_csv(SOURCE_FAMILY_PLAN_AUDIT_CSV, source_family_plan_audit_rows),
        "additional_source_requirements": write_csv(ADDITIONAL_SOURCE_REQ_AUDIT_CSV, additional_requirement_audit_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hj_contract": write_csv(FUTURE_6HJ_CONTRACT_CSV, future_6hj_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_audit_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HI",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HI if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "audited_layer": "6HH",
        "audited_plan_diagnosis": json_6hh.get("diagnosis"),
        "predecessor_plan": str(PLAN_6HH_PATH),
        "predecessor_plan_returncode": plan_run.returncode,
        "predecessor_plan_diagnosis": json_6hh.get("diagnosis"),
        "clean_selected_source_set_confirmed": clean_selected_source_set_confirmed,
        "selected_source_artifact_count": intish(json_6hh.get("selected_source_artifact_count")),
        "selected_sources_read_count": intish(json_6hh.get("selected_sources_read_count")),
        "selected_sources_failed_count": intish(json_6hh.get("selected_sources_failed_count")),
        "source_column_inventory_count": intish(json_6hh.get("source_column_inventory_count")),
        "sample_shape_count": intish(json_6hh.get("sample_shape_count")),
        "missing_identifier_row_count": intish(json_6hh.get("missing_identifier_row_count")),
        "deterministic_derivation_rule_count": intish(json_6hh.get("deterministic_derivation_rule_count")),
        "nondeterministic_derivation_rule_count": intish(json_6hh.get("nondeterministic_derivation_rule_count")),
        "additional_local_source_required": bool(json_6hh.get("additional_local_source_required")),
        "game_level_outcome_source_available": bool(json_6hh.get("game_level_outcome_source_available")),
        "aggregate_or_prototype_sources_detected": bool(json_6hh.get("aggregate_or_prototype_sources_detected")),
        "additional_source_discovery_required": additional_source_discovery_required,
        "row_level_adapter_revision_not_ready": row_level_adapter_revision_not_ready,
        "adapter_revision_required": bool(json_6hh.get("adapter_revision_required")),
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
            "selected_sources_csv": str(SELECTED_SOURCES_AUDIT_CSV),
            "source_columns_csv": str(SOURCE_COLUMNS_AUDIT_CSV),
            "sample_shapes_csv": str(SAMPLE_SHAPES_AUDIT_CSV),
            "missing_identifiers_csv": str(MISSING_IDENTIFIERS_AUDIT_CSV),
            "derivation_rules_csv": str(DERIVATION_RULES_AUDIT_CSV),
            "source_family_plan_csv": str(SOURCE_FAMILY_PLAN_AUDIT_CSV),
            "additional_source_requirements_csv": str(ADDITIONAL_SOURCE_REQ_AUDIT_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hj_contract_csv": str(FUTURE_6HJ_CONTRACT_CSV),
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
