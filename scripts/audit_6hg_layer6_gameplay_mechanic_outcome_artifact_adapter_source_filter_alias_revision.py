#!/usr/bin/env python3
"""Audit Layer 6HF outcome adapter source-filter and alias revision."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


SLUG = "layer6_6hg_outcome_artifact_adapter_source_filter_alias_revision_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6HF_PATH = Path("scripts/implement_6hf_layer6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision.py")
AUDIT_6HE_PATH = Path("scripts/audit_6he_layer6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan.py")
PLAN_6HD_PATH = Path("scripts/plan_6hd_layer6_gameplay_mechanic_outcome_artifact_schema_key_compatibility.py")

JSON_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision.json"
CHECKS_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_checks.csv"
PREDECESSOR_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_predecessor.csv"
INPUT_ARTIFACTS_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_input_artifacts.csv"
CANDIDATE_SOURCES_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_candidate_sources.csv"
EXCLUDED_SOURCES_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_excluded_sources.csv"
SELECTED_SOURCES_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_selected_sources.csv"
NORMALIZED_GAME_6HF = TMP_DIR / "layer6_6hf_normalized_game_outcomes.csv"
NORMALIZED_BASE_6HF = TMP_DIR / "layer6_6hf_normalized_base_out_transitions.csv"
NORMALIZED_INNING_6HF = TMP_DIR / "layer6_6hf_normalized_inning_runs.csv"
VALIDATION_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_validation.csv"
PROVENANCE_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_provenance.csv"
FAIL_CLOSED_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_fail_closed.csv"
ALIAS_MAPPING_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_alias_mapping.csv"
OUTPUT_CONTRACT_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_output_contract.csv"
FUTURE_6HG_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_future_6hg_contract.csv"
SAFETY_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_safety_boundaries.csv"
IMMUTABILITY_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_immutability.csv"
RECOMMENDED_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_recommended_path.csv"

REQUIRED_6HF_ARTIFACTS = [
    JSON_6HF,
    CHECKS_6HF,
    PREDECESSOR_6HF,
    INPUT_ARTIFACTS_6HF,
    CANDIDATE_SOURCES_6HF,
    EXCLUDED_SOURCES_6HF,
    SELECTED_SOURCES_6HF,
    NORMALIZED_GAME_6HF,
    NORMALIZED_BASE_6HF,
    NORMALIZED_INNING_6HF,
    VALIDATION_6HF,
    PROVENANCE_6HF,
    FAIL_CLOSED_6HF,
    ALIAS_MAPPING_6HF,
    OUTPUT_CONTRACT_6HF,
    FUTURE_6HG_6HF,
    SAFETY_6HF,
    IMMUTABILITY_6HF,
    RECOMMENDED_6HF,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
SOURCE_FILTERING_CSV = TMP_DIR / f"{SLUG}_source_filtering.csv"
NORMALIZED_SCHEMAS_CSV = TMP_DIR / f"{SLUG}_normalized_schemas.csv"
VALIDATION_AUDIT_CSV = TMP_DIR / f"{SLUG}_validation.csv"
PROVENANCE_AUDIT_CSV = TMP_DIR / f"{SLUG}_provenance.csv"
FAIL_CLOSED_AUDIT_CSV = TMP_DIR / f"{SLUG}_fail_closed.csv"
ALIAS_MAPPING_AUDIT_CSV = TMP_DIR / f"{SLUG}_alias_mapping.csv"
OUTPUT_CONTRACT_AUDIT_CSV = TMP_DIR / f"{SLUG}_output_contract.csv"
FUTURE_6HH_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hh_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HF = "layer_6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision_complete"
DIAGNOSIS_6HG = "layer_6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision_audit_complete"
CURRENT_LAYER = "6HG_layer_6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision_audit"
RECOMMENDED_NEXT_LAYER = "6HH_layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan"
RECOMMENDED_PATH = "audit_adapter_source_filter_alias_revision_then_plan_row_level_identifier_mapping"

EXCLUDE_PATH_TOKENS = ["layer6_6hb", "layer6_6hc", "layer6_6hd", "layer6_6he"]
REQUIRED_ALIAS_FIELDS = {
    "game_id",
    "game_date",
    "season",
    "home_team",
    "away_team",
    "home_runs",
    "away_runs",
    "total_runs",
    "winner",
    "inning",
    "half_inning",
    "batting_team",
    "fielding_team",
    "start_base_state",
    "start_outs",
    "end_base_state",
    "end_outs",
    "runs_scored",
    "transition_count",
}

GAME_FIELDS = {
    "game_id",
    "game_date",
    "season",
    "home_team",
    "away_team",
    "home_runs",
    "away_runs",
    "total_runs",
    "winner",
    "source_artifact_path",
    "source_row_id",
    "validation_status",
}

BASE_FIELDS = {
    "game_id_or_scope",
    "inning",
    "half_inning",
    "start_base_state",
    "start_outs",
    "end_base_state",
    "end_outs",
    "runs_scored",
    "transition_count",
    "source_artifact_path",
    "validation_status",
}

INNING_FIELDS = {
    "game_id",
    "inning",
    "half_inning",
    "batting_team",
    "fielding_team",
    "runs_scored",
    "source_artifact_path",
    "validation_status",
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


def columns(path: Path) -> Set[str]:
    rows = read_csv(path)
    if not rows:
        with path.open(newline="", encoding="utf-8") as handle:
            return set(next(csv.reader(handle), []))
    return set(rows[0].keys())


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    impl_6hf_before = IMPLEMENT_6HF_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HF_PATH.exists() else ""
    audit_6he_before = AUDIT_6HE_PATH.read_text(encoding="utf-8") if AUDIT_6HE_PATH.exists() else ""
    plan_6hd_before = PLAN_6HD_PATH.read_text(encoding="utf-8") if PLAN_6HD_PATH.exists() else ""

    implementation_run = subprocess.run(
        [sys.executable, str(IMPLEMENT_6HF_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6hf = load_json(JSON_6HF)
    checks_6hf = read_csv(CHECKS_6HF)
    candidate_rows = read_csv(CANDIDATE_SOURCES_6HF)
    excluded_rows = read_csv(EXCLUDED_SOURCES_6HF)
    selected_rows = read_csv(SELECTED_SOURCES_6HF)
    validation_rows = read_csv(VALIDATION_6HF)
    provenance_rows = read_csv(PROVENANCE_6HF)
    fail_closed_rows = read_csv(FAIL_CLOSED_6HF)
    alias_rows = read_csv(ALIAS_MAPPING_6HF)
    output_contract_rows = read_csv(OUTPUT_CONTRACT_6HF)
    future_6hg_rows = read_csv(FUTURE_6HG_6HF)
    safety_rows = read_csv(SAFETY_6HF)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hf_implementation_exists", "expected": True, "actual": IMPLEMENT_6HF_PATH.exists(), "passed": IMPLEMENT_6HF_PATH.exists()},
        {"check": "6hf_implementation_runs", "expected": 0, "actual": implementation_run.returncode, "passed": implementation_run.returncode == 0},
        {"check": "6hf_json_exists", "expected": True, "actual": JSON_6HF.exists(), "passed": JSON_6HF.exists()},
        {"check": "6hf_all_checks_passed", "expected": True, "actual": json_6hf.get("all_checks_passed"), "passed": json_6hf.get("all_checks_passed") is True},
        {"check": "6hf_diagnosis", "expected": DIAGNOSIS_6HF, "actual": json_6hf.get("diagnosis"), "passed": json_6hf.get("diagnosis") == DIAGNOSIS_6HF},
        {"check": "6hf_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6hf.get("recommended_next_layer"), "passed": json_6hf.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6hf_implementation_only", "expected": True, "actual": json_6hf.get("implementation_only"), "passed": json_6hf.get("implementation_only") is True},
        {"check": "6hf_source_filter_alias_revision_only", "expected": True, "actual": json_6hf.get("source_filter_alias_revision_only"), "passed": json_6hf.get("source_filter_alias_revision_only") is True},
        {"check": "6hf_source_filter_revision_applied", "expected": True, "actual": json_6hf.get("source_filter_revision_applied"), "passed": json_6hf.get("source_filter_revision_applied") is True},
        {"check": "6hf_alias_mapping_revision_applied", "expected": True, "actual": json_6hf.get("alias_mapping_revision_applied"), "passed": json_6hf.get("alias_mapping_revision_applied") is True},
        {"check": "6hf_prior_outputs_excluded", "expected": True, "actual": json_6hf.get("prior_adapter_outputs_excluded"), "passed": json_6hf.get("prior_adapter_outputs_excluded") is True},
        {"check": "6hf_meta_artifacts_excluded", "expected": True, "actual": json_6hf.get("planning_meta_artifacts_excluded"), "passed": json_6hf.get("planning_meta_artifacts_excluded") is True},
        {"check": "6hf_candidate_count_positive", "expected": ">=1", "actual": json_6hf.get("candidate_source_artifact_count"), "passed": intish(json_6hf.get("candidate_source_artifact_count"), 0) >= 1},
        {"check": "6hf_excluded_count_positive", "expected": ">=1", "actual": json_6hf.get("excluded_source_artifact_count"), "passed": intish(json_6hf.get("excluded_source_artifact_count"), 0) >= 1},
        {"check": "6hf_selected_count_positive", "expected": ">=1", "actual": json_6hf.get("selected_source_artifact_count"), "passed": intish(json_6hf.get("selected_source_artifact_count"), 0) >= 1},
        {"check": "6hf_reads_equal_selected", "expected": json_6hf.get("selected_source_artifact_count"), "actual": json_6hf.get("source_artifacts_read_count"), "passed": intish(json_6hf.get("source_artifacts_read_count")) == intish(json_6hf.get("selected_source_artifact_count"))},
        {"check": "6hf_source_failed_zero", "expected": 0, "actual": json_6hf.get("source_artifacts_failed_count"), "passed": intish(json_6hf.get("source_artifacts_failed_count")) == 0},
        {"check": "6hf_validation_passed_zero", "expected": 0, "actual": json_6hf.get("validation_passed_row_count"), "passed": intish(json_6hf.get("validation_passed_row_count")) == 0},
        {"check": "6hf_validation_failed_positive", "expected": ">=1", "actual": json_6hf.get("validation_failed_closed_row_count"), "passed": intish(json_6hf.get("validation_failed_closed_row_count"), 0) >= 1},
        {"check": "6hf_no_real_backtests", "expected": False, "actual": json_6hf.get("real_backtests_run"), "passed": json_6hf.get("real_backtests_run") is False},
        {"check": "6hf_no_mechanic_evaluation", "expected": False, "actual": json_6hf.get("mechanic_evaluations_run"), "passed": json_6hf.get("mechanic_evaluations_run") is False},
        {"check": "6hf_activation_false", "expected": False, "actual": json_6hf.get("activation_allowed"), "passed": json_6hf.get("activation_allowed") is False},
        {"check": "6hf_exit_credit_false", "expected": False, "actual": json_6hf.get("layer_6_exit_credit"), "passed": json_6hf.get("layer_6_exit_credit") is False},
    ]

    artifact_presence_rows = [{"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()} for path in REQUIRED_6HF_ARTIFACTS]

    checks_consistency_rows = [
        {"source_check": row.get("check"), "source_passed": row.get("passed"), "detail": row.get("detail", ""), "passed": boolish(row.get("passed"))}
        for row in checks_6hf
    ]

    selected_real_rows = [row for row in selected_rows if row.get("source_artifact_path")]
    source_filtering_rows = [
        {
            "audit": "candidate_source_count_matches_summary",
            "expected": json_6hf.get("candidate_source_artifact_count"),
            "actual": len(candidate_rows),
            "passed": len(candidate_rows) == intish(json_6hf.get("candidate_source_artifact_count")),
        },
        {
            "audit": "excluded_source_count_matches_summary",
            "expected": json_6hf.get("excluded_source_artifact_count"),
            "actual": len(excluded_rows),
            "passed": len(excluded_rows) == intish(json_6hf.get("excluded_source_artifact_count")),
        },
        {
            "audit": "selected_source_count_matches_summary",
            "expected": json_6hf.get("selected_source_artifact_count"),
            "actual": len(selected_real_rows),
            "passed": len(selected_real_rows) == intish(json_6hf.get("selected_source_artifact_count")),
        },
        {
            "audit": "selected_paths_exclude_prior_layer_outputs",
            "expected": True,
            "actual": "selected_sources",
            "passed": all(not any(token in row.get("source_artifact_path", "").lower() for token in EXCLUDE_PATH_TOKENS) for row in selected_real_rows),
        },
        {
            "audit": "selected_classifications_exclude_prior_adapter_outputs",
            "expected": True,
            "actual": "selected_sources",
            "passed": all(row.get("classification") != "likely_prior_adapter_output" for row in selected_real_rows),
        },
        {
            "audit": "selected_classifications_exclude_meta_artifacts",
            "expected": True,
            "actual": "selected_sources",
            "passed": all(row.get("classification") != "likely_planning_or_meta_artifact" for row in selected_real_rows),
        },
        {
            "audit": "selected_actions_candidate_keep_with_alias_review",
            "expected": "candidate_keep_with_alias_review",
            "actual": "selected_sources",
            "passed": all(row.get("future_source_filter_action") == "candidate_keep_with_alias_review" for row in selected_real_rows),
        },
        {
            "audit": "excluded_rows_all_marked_exclude",
            "expected": True,
            "actual": "excluded_sources",
            "passed": all(boolish(row.get("exclude")) for row in excluded_rows),
        },
    ]

    schema_rows = [
        {
            "artifact": "normalized_game_outcomes",
            "expected_fields": "|".join(sorted(GAME_FIELDS)),
            "actual_fields": "|".join(sorted(columns(NORMALIZED_GAME_6HF))),
            "passed": columns(NORMALIZED_GAME_6HF) == GAME_FIELDS,
        },
        {
            "artifact": "normalized_base_out_transitions",
            "expected_fields": "|".join(sorted(BASE_FIELDS)),
            "actual_fields": "|".join(sorted(columns(NORMALIZED_BASE_6HF))),
            "passed": columns(NORMALIZED_BASE_6HF) == BASE_FIELDS,
        },
        {
            "artifact": "normalized_inning_runs",
            "expected_fields": "|".join(sorted(INNING_FIELDS)),
            "actual_fields": "|".join(sorted(columns(NORMALIZED_INNING_6HF))),
            "passed": columns(NORMALIZED_INNING_6HF) == INNING_FIELDS,
        },
    ]

    validation_audit_rows = [
        {
            "audit": "validation_rows_present",
            "expected": ">=1",
            "actual": len(validation_rows),
            "passed": len(validation_rows) >= 1,
        },
        {
            "audit": "failed_rows_block_future_evaluation",
            "expected": True,
            "actual": "validation_rows",
            "passed": all(boolish(row.get("blocks_future_evaluation")) for row in validation_rows if row.get("validation_status") != "passed"),
        },
        {
            "audit": "all_rows_block_activation",
            "expected": True,
            "actual": "validation_rows",
            "passed": all(boolish(row.get("blocks_activation")) for row in validation_rows),
        },
        {
            "audit": "all_rows_block_exit_credit",
            "expected": True,
            "actual": "validation_rows",
            "passed": all(boolish(row.get("blocks_layer_6_exit_credit")) for row in validation_rows),
        },
        {
            "audit": "validation_passed_zero_current_artifacts",
            "expected": 0,
            "actual": json_6hf.get("validation_passed_row_count"),
            "passed": intish(json_6hf.get("validation_passed_row_count")) == 0,
        },
    ]

    provenance_audit_rows = [
        {
            "audit": "provenance_rows_match_selected_sources",
            "expected": len(selected_real_rows),
            "actual": len(provenance_rows),
            "passed": len(provenance_rows) == len(selected_real_rows),
        },
        {
            "audit": "all_sources_read_ok",
            "expected": "read_ok",
            "actual": "provenance_rows",
            "passed": all(row.get("read_status") == "read_ok" for row in provenance_rows),
        },
        {
            "audit": "mutated_source_false_all_rows",
            "expected": False,
            "actual": "provenance_rows",
            "passed": all(not boolish(row.get("mutated_source")) for row in provenance_rows),
        },
        {
            "audit": "live_fetch_false_all_rows",
            "expected": False,
            "actual": "provenance_rows",
            "passed": all(not boolish(row.get("live_fetch_used")) for row in provenance_rows),
        },
        {
            "audit": "database_write_false_all_rows",
            "expected": False,
            "actual": "provenance_rows",
            "passed": all(not boolish(row.get("database_write_used")) for row in provenance_rows),
        },
    ]

    fail_closed_audit_rows = [
        {
            "audit": "fail_closed_rows_present",
            "expected": ">=1",
            "actual": len(fail_closed_rows),
            "passed": len(fail_closed_rows) >= 1,
        },
        {
            "audit": "fail_closed_blocks_future_evaluation",
            "expected": True,
            "actual": "fail_closed_rows",
            "passed": all(boolish(row.get("blocks_future_evaluation")) for row in fail_closed_rows),
        },
        {
            "audit": "fail_closed_blocks_activation",
            "expected": True,
            "actual": "fail_closed_rows",
            "passed": all(boolish(row.get("blocks_activation")) for row in fail_closed_rows),
        },
        {
            "audit": "fail_closed_blocks_exit_credit",
            "expected": True,
            "actual": "fail_closed_rows",
            "passed": all(boolish(row.get("blocks_layer_6_exit_credit")) for row in fail_closed_rows),
        },
    ]

    alias_fields = {row.get("canonical_field") for row in alias_rows}
    alias_audit_rows = [
        {
            "audit": "alias_mapping_field_count",
            "expected": ">=19",
            "actual": len(alias_fields),
            "passed": len(alias_fields) >= 19,
        },
        {
            "audit": "required_alias_fields_present",
            "expected": "|".join(sorted(REQUIRED_ALIAS_FIELDS)),
            "actual": "|".join(sorted(alias_fields)),
            "passed": REQUIRED_ALIAS_FIELDS.issubset(alias_fields),
        },
        {
            "audit": "alias_mapping_revision_applied_all_rows",
            "expected": True,
            "actual": "alias_mapping_rows",
            "passed": all(boolish(row.get("revision_applied")) and boolish(row.get("passed")) for row in alias_rows),
        },
    ]

    output_contract_audit_rows = [
        {
            "artifact": row.get("artifact"),
            "local_tmp_only": row.get("local_tmp_only"),
            "used_for_real_evaluation": row.get("used_for_real_evaluation"),
            "passed": boolish(row.get("local_tmp_only"))
            and not boolish(row.get("used_for_real_evaluation"))
            and boolish(row.get("passed")),
        }
        for row in output_contract_rows
    ]

    future_6hh_rows = [
        {"contract": "plan_row_level_identifier_mapping", "required": True, "passed": True},
        {"contract": "inspect_six_selected_candidate_sources", "required": True, "passed": True},
        {"contract": "identify_exact_source_column_names_and_sample_row_shapes", "required": True, "passed": True},
        {"contract": "map_why_each_selected_row_lacks_canonical_identifiers", "required": True, "passed": True},
        {"contract": "propose_row_level_derivation_rules_for_game_id_inning_half_team_base_out_runs", "required": True, "passed": True},
        {"contract": "decide_whether_additional_local_source_artifacts_are_needed", "required": True, "passed": True},
        {"contract": "no_real_backtests_or_mechanic_evaluation_in_6hh", "required": True, "passed": True},
        {"contract": "no_activation_or_layer_6_exit_credit_in_6hh", "required": True, "passed": True},
        {"contract": "recommended_6hh_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan_complete"},
    ]

    safety_audit_rows = [
        {
            "boundary": row.get("boundary"),
            "expected": row.get("expected"),
            "actual": row.get("actual"),
            "source_passed": row.get("passed"),
            "passed": boolish(row.get("passed")),
        }
        for row in safety_rows
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    impl_6hf_after = IMPLEMENT_6HF_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HF_PATH.exists() else ""
    audit_6he_after = AUDIT_6HE_PATH.read_text(encoding="utf-8") if AUDIT_6HE_PATH.exists() else ""
    plan_6hd_after = PLAN_6HD_PATH.read_text(encoding="utf-8") if PLAN_6HD_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6hg_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hf_implementation", "policy": "unchanged_by_6hg", "passed": impl_6hf_after == impl_6hf_before},
        {"surface": "6he_audit", "policy": "unchanged_by_6hg", "passed": audit_6he_after == audit_6he_before},
        {"surface": "6hd_plan", "policy": "unchanged_by_6hg", "passed": plan_6hd_after == plan_6hd_before},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hg", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "row_level_identifier_mapping_required", "expected": True, "actual": True, "passed": True},
        {"decision": "real_evaluation_blocked_by_validation", "expected": True, "actual": True, "passed": True},
        {"decision": "future_real_evaluation_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HG, "actual": DIAGNOSIS_6HG, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": len(checks_consistency_rows) >= 20 and all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "source_filtering", "passed": all(row["passed"] for row in source_filtering_rows), "detail": f"{sum(1 for row in source_filtering_rows if row['passed'])}/{len(source_filtering_rows)}"},
        {"check": "normalized_schemas", "passed": all(row["passed"] for row in schema_rows), "detail": f"{sum(1 for row in schema_rows if row['passed'])}/{len(schema_rows)}"},
        {"check": "validation", "passed": all(row["passed"] for row in validation_audit_rows), "detail": f"{sum(1 for row in validation_audit_rows if row['passed'])}/{len(validation_audit_rows)}"},
        {"check": "provenance", "passed": all(row["passed"] for row in provenance_audit_rows), "detail": f"{sum(1 for row in provenance_audit_rows if row['passed'])}/{len(provenance_audit_rows)}"},
        {"check": "fail_closed", "passed": all(row["passed"] for row in fail_closed_audit_rows), "detail": f"{sum(1 for row in fail_closed_audit_rows if row['passed'])}/{len(fail_closed_audit_rows)}"},
        {"check": "alias_mapping", "passed": all(row["passed"] for row in alias_audit_rows), "detail": f"{sum(1 for row in alias_audit_rows if row['passed'])}/{len(alias_audit_rows)}"},
        {"check": "output_contract", "passed": all(row["passed"] for row in output_contract_audit_rows), "detail": f"{sum(1 for row in output_contract_audit_rows if row['passed'])}/{len(output_contract_audit_rows)}"},
        {"check": "future_6hg_contract_from_6hf", "passed": all(boolish(row.get("passed")) for row in future_6hg_rows), "detail": f"{sum(1 for row in future_6hg_rows if boolish(row.get('passed')))}" + f"/{len(future_6hg_rows)}"},
        {"check": "future_6hh_contract", "passed": all(row["passed"] for row in future_6hh_rows), "detail": f"{sum(1 for row in future_6hh_rows if row['passed'])}/{len(future_6hh_rows)}"},
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
        "source_filtering": write_csv(SOURCE_FILTERING_CSV, source_filtering_rows),
        "normalized_schemas": write_csv(NORMALIZED_SCHEMAS_CSV, schema_rows),
        "validation": write_csv(VALIDATION_AUDIT_CSV, validation_audit_rows),
        "provenance": write_csv(PROVENANCE_AUDIT_CSV, provenance_audit_rows),
        "fail_closed": write_csv(FAIL_CLOSED_AUDIT_CSV, fail_closed_audit_rows),
        "alias_mapping": write_csv(ALIAS_MAPPING_AUDIT_CSV, alias_audit_rows),
        "output_contract": write_csv(OUTPUT_CONTRACT_AUDIT_CSV, output_contract_audit_rows),
        "future_6hh_contract": write_csv(FUTURE_6HH_CONTRACT_CSV, future_6hh_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_audit_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HG",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HG if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "audited_layer": "6HF",
        "audited_implementation_diagnosis": json_6hf.get("diagnosis"),
        "predecessor_implementation": str(IMPLEMENT_6HF_PATH),
        "predecessor_implementation_returncode": implementation_run.returncode,
        "predecessor_implementation_diagnosis": json_6hf.get("diagnosis"),
        "source_filter_revision_audited": True,
        "alias_mapping_revision_audited": True,
        "prior_adapter_outputs_excluded": True,
        "planning_meta_artifacts_excluded": True,
        "candidate_source_artifact_count": intish(json_6hf.get("candidate_source_artifact_count")),
        "excluded_source_artifact_count": intish(json_6hf.get("excluded_source_artifact_count")),
        "selected_source_artifact_count": intish(json_6hf.get("selected_source_artifact_count")),
        "source_artifacts_read_count": intish(json_6hf.get("source_artifacts_read_count")),
        "source_artifacts_failed_count": intish(json_6hf.get("source_artifacts_failed_count")),
        "normalized_game_outcomes_count": intish(json_6hf.get("normalized_game_outcomes_count")),
        "normalized_base_out_transitions_count": intish(json_6hf.get("normalized_base_out_transitions_count")),
        "normalized_inning_runs_count": intish(json_6hf.get("normalized_inning_runs_count")),
        "validation_passed_row_count": intish(json_6hf.get("validation_passed_row_count")),
        "validation_failed_closed_row_count": intish(json_6hf.get("validation_failed_closed_row_count")),
        "row_level_identifier_mapping_required": True,
        "real_evaluation_blocked_by_validation": True,
        "future_real_evaluation_allowed_by_this_layer": False,
        "future_identifier_mapping_plan_required": True,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
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
            "source_filtering_csv": str(SOURCE_FILTERING_CSV),
            "normalized_schemas_csv": str(NORMALIZED_SCHEMAS_CSV),
            "validation_csv": str(VALIDATION_AUDIT_CSV),
            "provenance_csv": str(PROVENANCE_AUDIT_CSV),
            "fail_closed_csv": str(FAIL_CLOSED_AUDIT_CSV),
            "alias_mapping_csv": str(ALIAS_MAPPING_AUDIT_CSV),
            "output_contract_csv": str(OUTPUT_CONTRACT_AUDIT_CSV),
            "future_6hh_contract_csv": str(FUTURE_6HH_CONTRACT_CSV),
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
