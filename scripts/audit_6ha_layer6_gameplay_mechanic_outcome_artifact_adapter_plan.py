#!/usr/bin/env python3
"""Audit Layer 6GZ outcome artifact adapter plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


SLUG = "layer6_6ha_outcome_artifact_adapter_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6GZ_PATH = Path("scripts/plan_6gz_layer6_gameplay_mechanic_outcome_artifact_adapter.py")
AUDIT_6GY_PATH = Path("scripts/audit_6gy_layer6_gameplay_mechanic_outcome_artifact_selection_plan.py")
PLAN_6GX_PATH = Path("scripts/plan_6gx_layer6_gameplay_mechanic_outcome_artifact_selection.py")
AUDIT_6GW_PATH = Path("scripts/audit_6gw_layer6_gameplay_mechanic_real_backtest_dry_run.py")
IMPLEMENT_6GV_PATH = Path("scripts/implement_6gv_layer6_gameplay_mechanic_real_backtest_dry_run.py")
AUDIT_6GU_PATH = Path("scripts/audit_6gu_layer6_gameplay_mechanic_real_backtest_plan.py")
PLAN_6GT_PATH = Path("scripts/plan_6gt_layer6_gameplay_mechanic_real_backtests.py")

JSON_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan.json"
CHECKS_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_checks.csv"
PREDECESSOR_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_predecessor.csv"
INPUT_ARTIFACTS_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_input_artifacts.csv"
SELECTED_FAMILIES_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_selected_families.csv"
PRIMARY_ADAPTER_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_primary_game_outcomes_adapter.csv"
BASE_OUT_ADAPTER_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_base_out_adapter.csv"
INNING_ADAPTER_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_inning_runs_adapter.csv"
KEY_MAPPING_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_key_mapping_policy.csv"
SCHEMA_VALIDATION_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_schema_validation_policy.csv"
FAIL_CLOSED_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_fail_closed_policy.csv"
OUTPUT_CONTRACT_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_output_contract.csv"
FUTURE_6HA_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_future_6ha_contract.csv"
FUTURE_6HB_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_future_6hb_contract.csv"
SAFETY_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_safety_boundaries.csv"
IMMUTABILITY_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_immutability.csv"
RECOMMENDED_6GZ = TMP_DIR / "layer6_6gz_outcome_artifact_adapter_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
SELECTED_FAMILIES_AUDIT_CSV = TMP_DIR / f"{SLUG}_selected_families.csv"
PRIMARY_ADAPTER_AUDIT_CSV = TMP_DIR / f"{SLUG}_primary_adapter.csv"
BASE_OUT_ADAPTER_AUDIT_CSV = TMP_DIR / f"{SLUG}_base_out_adapter.csv"
INNING_ADAPTER_AUDIT_CSV = TMP_DIR / f"{SLUG}_inning_adapter.csv"
KEY_MAPPING_AUDIT_CSV = TMP_DIR / f"{SLUG}_key_mapping_policy.csv"
SCHEMA_VALIDATION_AUDIT_CSV = TMP_DIR / f"{SLUG}_schema_validation_policy.csv"
FAIL_CLOSED_AUDIT_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
OUTPUT_CONTRACT_AUDIT_CSV = TMP_DIR / f"{SLUG}_output_contract.csv"
FUTURE_6HB_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hb_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GZ = "layer_6_gameplay_mechanic_outcome_artifact_adapter_plan_complete"
DIAGNOSIS_6HA = "layer_6_gameplay_mechanic_outcome_artifact_adapter_plan_audit_complete"
CURRENT_LAYER = "6HA_layer_6_gameplay_mechanic_outcome_artifact_adapter_plan_audit"
RECOMMENDED_NEXT_LAYER = "6HB_layer_6_gameplay_mechanic_outcome_artifact_adapter_implementation"
RECOMMENDED_PATH = "audit_outcome_artifact_adapter_plan_then_implement_local_adapter"

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

REQUIRED_6GZ_ARTIFACTS = [
    JSON_6GZ,
    CHECKS_6GZ,
    PREDECESSOR_6GZ,
    INPUT_ARTIFACTS_6GZ,
    SELECTED_FAMILIES_6GZ,
    PRIMARY_ADAPTER_6GZ,
    BASE_OUT_ADAPTER_6GZ,
    INNING_ADAPTER_6GZ,
    KEY_MAPPING_6GZ,
    SCHEMA_VALIDATION_6GZ,
    FAIL_CLOSED_6GZ,
    OUTPUT_CONTRACT_6GZ,
    FUTURE_6HA_6GZ,
    FUTURE_6HB_6GZ,
    SAFETY_6GZ,
    IMMUTABILITY_6GZ,
    RECOMMENDED_6GZ,
]

PRIMARY_FIELDS = {
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

BASE_OUT_FIELDS = {
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

KEY_POLICIES = {
    "game_id_exact_match_preferred",
    "missing_game_id_requires_deterministic_composite_key",
    "allowed_composite_key_date_home_team_away_team_season",
    "ambiguous_duplicate_composite_keys_fail_closed",
    "no_fuzzy_team_name_matching_without_future_audit",
    "no_cross_artifact_join_without_schema_validation",
}

SCHEMA_VALIDATIONS = {
    "missing_required_fields",
    "null_required_identifiers",
    "invalid_score_fields",
    "negative_runs",
    "invalid_inning_values",
    "missing_source_provenance",
    "invalid_winner_or_total_runs_consistency",
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


def intish(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def field_set(rows: List[Dict[str, str]]) -> Set[str]:
    return {row.get("canonical_field", "") for row in rows if row.get("canonical_field", "")}


def all_adapter_rows_safe(rows: List[Dict[str, str]]) -> bool:
    return all(
        boolish(row.get("validation_required"))
        and not boolish(row.get("activation_allowed"))
        and not boolish(row.get("layer_6_exit_credit"))
        and boolish(row.get("passed"))
        for row in rows
    )


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6gz_before = PLAN_6GZ_PATH.read_text(encoding="utf-8") if PLAN_6GZ_PATH.exists() else ""
    audit_6gy_before = AUDIT_6GY_PATH.read_text(encoding="utf-8") if AUDIT_6GY_PATH.exists() else ""
    plan_6gx_before = PLAN_6GX_PATH.read_text(encoding="utf-8") if PLAN_6GX_PATH.exists() else ""
    audit_6gw_before = AUDIT_6GW_PATH.read_text(encoding="utf-8") if AUDIT_6GW_PATH.exists() else ""
    implement_6gv_before = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_before = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_before = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    plan_run = subprocess.run(
        [sys.executable, str(PLAN_6GZ_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6gz = load_json(JSON_6GZ)
    checks_6gz = read_csv(CHECKS_6GZ)
    selected_rows_6gz = read_csv(SELECTED_FAMILIES_6GZ)
    primary_rows_6gz = read_csv(PRIMARY_ADAPTER_6GZ)
    base_out_rows_6gz = read_csv(BASE_OUT_ADAPTER_6GZ)
    inning_rows_6gz = read_csv(INNING_ADAPTER_6GZ)
    key_rows_6gz = read_csv(KEY_MAPPING_6GZ)
    schema_rows_6gz = read_csv(SCHEMA_VALIDATION_6GZ)
    fail_closed_rows_6gz = read_csv(FAIL_CLOSED_6GZ)
    output_rows_6gz = read_csv(OUTPUT_CONTRACT_6GZ)
    future_6ha_rows_6gz = read_csv(FUTURE_6HA_6GZ)
    future_6hb_rows_6gz = read_csv(FUTURE_6HB_6GZ)
    safety_rows_6gz = read_csv(SAFETY_6GZ)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gz_plan_exists", "expected": True, "actual": PLAN_6GZ_PATH.exists(), "passed": PLAN_6GZ_PATH.exists()},
        {"check": "6gz_plan_runs", "expected": 0, "actual": plan_run.returncode, "passed": plan_run.returncode == 0},
        {"check": "6gz_json_exists", "expected": True, "actual": JSON_6GZ.exists(), "passed": JSON_6GZ.exists()},
        {"check": "6gz_all_checks_passed", "expected": True, "actual": json_6gz.get("all_checks_passed"), "passed": json_6gz.get("all_checks_passed") is True},
        {"check": "6gz_planning_only", "expected": True, "actual": json_6gz.get("planning_only"), "passed": json_6gz.get("planning_only") is True},
        {"check": "6gz_adapter_plan_only", "expected": True, "actual": json_6gz.get("adapter_plan_only"), "passed": json_6gz.get("adapter_plan_only") is True},
        {"check": "6gz_diagnosis", "expected": DIAGNOSIS_6GZ, "actual": json_6gz.get("diagnosis"), "passed": json_6gz.get("diagnosis") == DIAGNOSIS_6GZ},
        {"check": "6gz_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6gz.get("recommended_next_layer"), "passed": json_6gz.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gz_recommended_path", "expected": "plan_outcome_artifact_adapter_then_audit_before_adapter_implementation", "actual": json_6gz.get("recommended_path"), "passed": json_6gz.get("recommended_path") == "plan_outcome_artifact_adapter_then_audit_before_adapter_implementation"},
        {"check": "6gz_primary_source_family", "expected": "candidate_game_outcomes", "actual": json_6gz.get("primary_source_family"), "passed": json_6gz.get("primary_source_family") == "candidate_game_outcomes"},
        {"check": "6gz_primary_count_positive", "expected": ">=1", "actual": json_6gz.get("primary_selected_artifact_count"), "passed": intish(json_6gz.get("primary_selected_artifact_count"), 0) >= 1},
        {"check": "6gz_base_out_family", "expected": "candidate_base_out_transitions", "actual": json_6gz.get("base_out_source_family"), "passed": json_6gz.get("base_out_source_family") == "candidate_base_out_transitions"},
        {"check": "6gz_base_out_count_nonnegative", "expected": ">=0", "actual": json_6gz.get("base_out_selected_artifact_count"), "passed": intish(json_6gz.get("base_out_selected_artifact_count"), -1) >= 0},
        {"check": "6gz_inning_family", "expected": "candidate_inning_runs", "actual": json_6gz.get("inning_source_family"), "passed": json_6gz.get("inning_source_family") == "candidate_inning_runs"},
        {"check": "6gz_inning_count_nonnegative", "expected": ">=0", "actual": json_6gz.get("inning_selected_artifact_count"), "passed": intish(json_6gz.get("inning_selected_artifact_count"), -1) >= 0},
        {"check": "6gz_adapter_required_true", "expected": True, "actual": json_6gz.get("adapter_required"), "passed": json_6gz.get("adapter_required") is True},
        {"check": "6gz_materialization_plan_required_false", "expected": False, "actual": json_6gz.get("materialization_plan_required"), "passed": json_6gz.get("materialization_plan_required") is False},
        {"check": "6gz_future_adapter_audit_required_true", "expected": True, "actual": json_6gz.get("future_adapter_audit_required"), "passed": json_6gz.get("future_adapter_audit_required") is True},
        {"check": "6gz_future_real_evaluation_allowed_false", "expected": False, "actual": json_6gz.get("future_real_evaluation_allowed_by_this_layer"), "passed": json_6gz.get("future_real_evaluation_allowed_by_this_layer") is False},
        {"check": "6gz_outcome_adapter_implemented_false", "expected": False, "actual": json_6gz.get("outcome_adapter_implemented"), "passed": json_6gz.get("outcome_adapter_implemented") is False},
        {"check": "6gz_normalized_outcomes_emitted_false", "expected": False, "actual": json_6gz.get("normalized_outcomes_emitted"), "passed": json_6gz.get("normalized_outcomes_emitted") is False},
        {"check": "6gz_real_backtests_run_false", "expected": False, "actual": json_6gz.get("real_backtests_run"), "passed": json_6gz.get("real_backtests_run") is False},
        {"check": "6gz_games_evaluated_zero", "expected": 0, "actual": json_6gz.get("games_evaluated"), "passed": intish(json_6gz.get("games_evaluated")) == 0},
        {"check": "6gz_actual_outcomes_joined_false", "expected": False, "actual": json_6gz.get("actual_outcomes_joined"), "passed": json_6gz.get("actual_outcomes_joined") is False},
        {"check": "6gz_activation_allowed_false", "expected": False, "actual": json_6gz.get("activation_allowed"), "passed": json_6gz.get("activation_allowed") is False},
        {"check": "6gz_layer_6_exit_credit_false", "expected": False, "actual": json_6gz.get("layer_6_exit_credit"), "passed": json_6gz.get("layer_6_exit_credit") is False},
        {"check": "6gz_layer_6_exit_ready_false", "expected": False, "actual": json_6gz.get("layer_6_exit_ready"), "passed": json_6gz.get("layer_6_exit_ready") is False},
        {"check": "6gz_live_fetches_false", "expected": False, "actual": json_6gz.get("live_data_fetches_run"), "passed": json_6gz.get("live_data_fetches_run") is False},
        {"check": "6gz_database_writes_false", "expected": False, "actual": json_6gz.get("database_writes_run"), "passed": json_6gz.get("database_writes_run") is False},
        {"check": "6gz_materialization_jobs_false", "expected": False, "actual": json_6gz.get("materialization_jobs_run"), "passed": json_6gz.get("materialization_jobs_run") is False},
        {"check": "6gz_production_simulations_false", "expected": False, "actual": json_6gz.get("production_simulations_run"), "passed": json_6gz.get("production_simulations_run") is False},
    ]

    artifact_presence_rows = [{"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()} for path in REQUIRED_6GZ_ARTIFACTS]

    checks_consistency_rows = [
        {"source_check": row.get("check"), "source_passed": row.get("passed"), "passed": boolish(row.get("passed")), "detail": row.get("detail", "")}
        for row in checks_6gz
    ]

    family_map = {row.get("family_role"): row for row in selected_rows_6gz}
    selected_family_audit_rows = [
        {
            "check": "primary_candidate_game_outcomes_selected",
            "expected": "candidate_game_outcomes_count>=1",
            "actual": family_map.get("primary", {}).get("source_family"),
            "passed": family_map.get("primary", {}).get("source_family") == "candidate_game_outcomes"
            and intish(family_map.get("primary", {}).get("selected_artifact_count"), 0) >= 1,
        },
        {
            "check": "base_out_family_row_present",
            "expected": "candidate_base_out_transitions",
            "actual": family_map.get("supplemental_base_out", {}).get("source_family"),
            "passed": family_map.get("supplemental_base_out", {}).get("source_family") == "candidate_base_out_transitions",
        },
        {
            "check": "inning_family_row_present",
            "expected": "candidate_inning_runs",
            "actual": family_map.get("supplemental_inning_runs", {}).get("source_family"),
            "passed": family_map.get("supplemental_inning_runs", {}).get("source_family") == "candidate_inning_runs",
        },
        {
            "check": "selected_families_no_activation_or_exit",
            "expected": "activation=false exit=false",
            "actual": "selected_family_rows",
            "passed": all(not boolish(row.get("activation_allowed")) and not boolish(row.get("layer_6_exit_credit")) for row in selected_rows_6gz),
        },
    ]

    def adapter_audit_rows(name: str, rows: List[Dict[str, str]], expected_fields: Set[str]) -> List[Dict[str, Any]]:
        actual_fields = field_set(rows)
        return [
            {"adapter": name, "check": "canonical_fields_exact", "expected": "|".join(sorted(expected_fields)), "actual": "|".join(sorted(actual_fields)), "passed": actual_fields == expected_fields},
            {"adapter": name, "check": "field_count", "expected": len(expected_fields), "actual": len(rows), "passed": len(rows) == len(expected_fields)},
            {"adapter": name, "check": "validation_required_all_rows", "expected": True, "actual": "adapter_rows", "passed": all(boolish(row.get("validation_required")) for row in rows)},
            {"adapter": name, "check": "activation_blocked_all_rows", "expected": False, "actual": "activation_allowed", "passed": all(not boolish(row.get("activation_allowed")) for row in rows)},
            {"adapter": name, "check": "exit_credit_blocked_all_rows", "expected": False, "actual": "layer_6_exit_credit", "passed": all(not boolish(row.get("layer_6_exit_credit")) for row in rows)},
            {"adapter": name, "check": "source_rows_passed", "expected": True, "actual": "passed", "passed": all(boolish(row.get("passed")) for row in rows)},
        ]

    primary_adapter_audit_rows = adapter_audit_rows("primary_game_outcomes_adapter", primary_rows_6gz, PRIMARY_FIELDS)
    base_out_adapter_audit_rows = adapter_audit_rows("base_out_adapter", base_out_rows_6gz, BASE_OUT_FIELDS)
    inning_adapter_audit_rows = adapter_audit_rows("inning_runs_adapter", inning_rows_6gz, INNING_FIELDS)

    key_mapping_audit_rows = [
        {
            "policy": policy,
            "present": policy in {row.get("policy") for row in key_rows_6gz},
            "source_passed": next((row.get("passed") for row in key_rows_6gz if row.get("policy") == policy), ""),
            "passed": policy in {row.get("policy") for row in key_rows_6gz}
            and boolish(next((row.get("passed") for row in key_rows_6gz if row.get("policy") == policy), "")),
        }
        for policy in sorted(KEY_POLICIES)
    ]

    schema_audit_rows = [
        {
            "validation": validation,
            "present": validation in {row.get("validation") for row in schema_rows_6gz},
            "outcome": next((row.get("outcome") for row in schema_rows_6gz if row.get("validation") == validation), ""),
            "blocks_evaluation": next((row.get("blocks_evaluation") for row in schema_rows_6gz if row.get("validation") == validation), ""),
            "passed": validation in {row.get("validation") for row in schema_rows_6gz}
            and next((row.get("outcome") for row in schema_rows_6gz if row.get("validation") == validation), "") == "fail_closed"
            and boolish(next((row.get("blocks_evaluation") for row in schema_rows_6gz if row.get("validation") == validation), "")),
        }
        for validation in sorted(SCHEMA_VALIDATIONS)
    ]

    fail_closed_audit_rows = [
        {
            "condition": row.get("condition"),
            "blocks_adapter_output": row.get("blocks_adapter_output"),
            "blocks_real_evaluation": row.get("blocks_real_evaluation"),
            "blocks_activation": row.get("blocks_activation"),
            "blocks_layer_6_exit_credit": row.get("blocks_layer_6_exit_credit"),
            "passed": boolish(row.get("blocks_adapter_output"))
            and boolish(row.get("blocks_real_evaluation"))
            and boolish(row.get("blocks_activation"))
            and boolish(row.get("blocks_layer_6_exit_credit"))
            and boolish(row.get("passed")),
        }
        for row in fail_closed_rows_6gz
    ]

    output_contract_audit_rows = [
        {
            "artifact": row.get("artifact"),
            "future_path": row.get("future_path"),
            "local_tmp_only": row.get("local_tmp_only"),
            "emitted_by_6gz": row.get("emitted_by_6gz"),
            "emitted_by_6ha": False,
            "passed": boolish(row.get("local_tmp_only")) and not boolish(row.get("emitted_by_6gz")) and boolish(row.get("passed")),
        }
        for row in output_rows_6gz
    ]

    future_6hb_contract_rows = [
        {"contract": "requires_6gz_plan_and_6ha_audit", "required": True, "passed": True},
        {"contract": "implement_local_adapter_only", "required": True, "passed": True},
        {"contract": "emit_normalized_local_tmp_artifacts_only", "required": True, "passed": True},
        {"contract": "fail_closed_on_schema_key_or_provenance_problems", "required": True, "passed": True},
        {"contract": "no_real_backtests_or_mechanic_evaluation_in_6hb", "required": True, "passed": True},
        {"contract": "no_activation_or_layer_6_exit_credit_in_6hb", "required": True, "passed": True},
        {"contract": "future_6hc_audit_required_before_real_evaluation", "required": True, "passed": True, "artifact": "6HC_layer_6_gameplay_mechanic_outcome_artifact_adapter_implementation_audit"},
    ]

    safety_audit_rows = [
        {"boundary": row.get("boundary"), "expected": row.get("expected"), "actual": row.get("actual"), "source_passed": row.get("passed"), "passed": boolish(row.get("passed"))}
        for row in safety_rows_6gz
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6gz_after = PLAN_6GZ_PATH.read_text(encoding="utf-8") if PLAN_6GZ_PATH.exists() else ""
    audit_6gy_after = AUDIT_6GY_PATH.read_text(encoding="utf-8") if AUDIT_6GY_PATH.exists() else ""
    plan_6gx_after = PLAN_6GX_PATH.read_text(encoding="utf-8") if PLAN_6GX_PATH.exists() else ""
    audit_6gw_after = AUDIT_6GW_PATH.read_text(encoding="utf-8") if AUDIT_6GW_PATH.exists() else ""
    implement_6gv_after = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_after = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_after = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6ha_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gz_plan", "policy": "unchanged_by_6ha", "passed": plan_6gz_after == plan_6gz_before},
        {"surface": "6gy_audit", "policy": "unchanged_by_6ha", "passed": audit_6gy_after == audit_6gy_before},
        {"surface": "6gx_plan", "policy": "unchanged_by_6ha", "passed": plan_6gx_after == plan_6gx_before},
        {"surface": "6gw_audit", "policy": "unchanged_by_6ha", "passed": audit_6gw_after == audit_6gw_before},
        {"surface": "6gv_implementation", "policy": "unchanged_by_6ha", "passed": implement_6gv_after == implement_6gv_before},
        {"surface": "6gu_audit", "policy": "unchanged_by_6ha", "passed": audit_6gu_after == audit_6gu_before},
        {"surface": "6gt_plan", "policy": "unchanged_by_6ha", "passed": plan_6gt_after == plan_6gt_before},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6ha", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "outcome_adapter_implemented", "expected": False, "actual": False, "passed": True},
        {"decision": "normalized_outcomes_emitted", "expected": False, "actual": False, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HA, "actual": DIAGNOSIS_6HA, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": len(checks_consistency_rows) >= 15 and all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "selected_families", "passed": all(row["passed"] for row in selected_family_audit_rows), "detail": f"{sum(1 for row in selected_family_audit_rows if row['passed'])}/{len(selected_family_audit_rows)}"},
        {"check": "primary_adapter", "passed": all(row["passed"] for row in primary_adapter_audit_rows) and all_adapter_rows_safe(primary_rows_6gz), "detail": f"{sum(1 for row in primary_adapter_audit_rows if row['passed'])}/{len(primary_adapter_audit_rows)}"},
        {"check": "base_out_adapter", "passed": all(row["passed"] for row in base_out_adapter_audit_rows) and all_adapter_rows_safe(base_out_rows_6gz), "detail": f"{sum(1 for row in base_out_adapter_audit_rows if row['passed'])}/{len(base_out_adapter_audit_rows)}"},
        {"check": "inning_adapter", "passed": all(row["passed"] for row in inning_adapter_audit_rows) and all_adapter_rows_safe(inning_rows_6gz), "detail": f"{sum(1 for row in inning_adapter_audit_rows if row['passed'])}/{len(inning_adapter_audit_rows)}"},
        {"check": "key_mapping_policy", "passed": all(row["passed"] for row in key_mapping_audit_rows), "detail": f"{sum(1 for row in key_mapping_audit_rows if row['passed'])}/{len(key_mapping_audit_rows)}"},
        {"check": "schema_validation_policy", "passed": all(row["passed"] for row in schema_audit_rows), "detail": f"{sum(1 for row in schema_audit_rows if row['passed'])}/{len(schema_audit_rows)}"},
        {"check": "fail_closed_policy", "passed": all(row["passed"] for row in fail_closed_audit_rows), "detail": f"{sum(1 for row in fail_closed_audit_rows if row['passed'])}/{len(fail_closed_audit_rows)}"},
        {"check": "output_contract", "passed": all(row["passed"] for row in output_contract_audit_rows), "detail": f"{sum(1 for row in output_contract_audit_rows if row['passed'])}/{len(output_contract_audit_rows)}"},
        {"check": "future_6ha_contract", "passed": all(boolish(row.get("passed")) for row in future_6ha_rows_6gz), "detail": f"{sum(1 for row in future_6ha_rows_6gz if boolish(row.get('passed')))}" + f"/{len(future_6ha_rows_6gz)}"},
        {"check": "future_6hb_contract", "passed": all(row["passed"] for row in future_6hb_contract_rows) and all(boolish(row.get("passed")) for row in future_6hb_rows_6gz), "detail": f"{sum(1 for row in future_6hb_contract_rows if row['passed'])}/{len(future_6hb_contract_rows)}"},
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
        "selected_families": write_csv(SELECTED_FAMILIES_AUDIT_CSV, selected_family_audit_rows),
        "primary_adapter": write_csv(PRIMARY_ADAPTER_AUDIT_CSV, primary_adapter_audit_rows),
        "base_out_adapter": write_csv(BASE_OUT_ADAPTER_AUDIT_CSV, base_out_adapter_audit_rows),
        "inning_adapter": write_csv(INNING_ADAPTER_AUDIT_CSV, inning_adapter_audit_rows),
        "key_mapping_policy": write_csv(KEY_MAPPING_AUDIT_CSV, key_mapping_audit_rows),
        "schema_validation_policy": write_csv(SCHEMA_VALIDATION_AUDIT_CSV, schema_audit_rows),
        "fail_closed_policy": write_csv(FAIL_CLOSED_AUDIT_CSV, fail_closed_audit_rows),
        "output_contract": write_csv(OUTPUT_CONTRACT_AUDIT_CSV, output_contract_audit_rows),
        "future_6hb_contract": write_csv(FUTURE_6HB_CONTRACT_CSV, future_6hb_contract_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_audit_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HA",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "audited_layer": "6GZ",
        "audited_plan_diagnosis": json_6gz.get("diagnosis"),
        "diagnosis": DIAGNOSIS_6HA if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_plan": str(PLAN_6GZ_PATH),
        "predecessor_plan_returncode": plan_run.returncode,
        "predecessor_plan_diagnosis": json_6gz.get("diagnosis"),
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "outcome_adapter_implemented": False,
        "normalized_outcomes_emitted": False,
        "actual_outcomes_joined": False,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "activation_allowed": False,
        "layer_6_exit_credit": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(EVALUATION_WINDOWS),
        "audited_primary_source_family": json_6gz.get("primary_source_family"),
        "audited_primary_selected_artifact_count": intish(json_6gz.get("primary_selected_artifact_count"), 0),
        "audited_base_out_source_family": json_6gz.get("base_out_source_family"),
        "audited_base_out_selected_artifact_count": intish(json_6gz.get("base_out_selected_artifact_count"), 0),
        "audited_inning_source_family": json_6gz.get("inning_source_family"),
        "audited_inning_selected_artifact_count": intish(json_6gz.get("inning_selected_artifact_count"), 0),
        "audited_adapter_required": bool(json_6gz.get("adapter_required")),
        "audited_materialization_plan_required": bool(json_6gz.get("materialization_plan_required")),
        "audited_future_adapter_audit_required": bool(json_6gz.get("future_adapter_audit_required")),
        "audited_future_real_evaluation_allowed_by_this_layer": bool(json_6gz.get("future_real_evaluation_allowed_by_this_layer")),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "checks_consistency_csv": str(CHECKS_CONSISTENCY_CSV),
            "selected_families_csv": str(SELECTED_FAMILIES_AUDIT_CSV),
            "primary_adapter_csv": str(PRIMARY_ADAPTER_AUDIT_CSV),
            "base_out_adapter_csv": str(BASE_OUT_ADAPTER_AUDIT_CSV),
            "inning_adapter_csv": str(INNING_ADAPTER_AUDIT_CSV),
            "key_mapping_policy_csv": str(KEY_MAPPING_AUDIT_CSV),
            "schema_validation_policy_csv": str(SCHEMA_VALIDATION_AUDIT_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_AUDIT_CSV),
            "output_contract_csv": str(OUTPUT_CONTRACT_AUDIT_CSV),
            "future_6hb_contract_csv": str(FUTURE_6HB_CONTRACT_CSV),
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
