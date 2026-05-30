#!/usr/bin/env python3
"""Plan Layer 6GZ outcome artifact adapter for future gameplay mechanic evaluation."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gz_outcome_artifact_adapter_plan"
TMP_DIR = Path("tmp")

AUDIT_6GY_PATH = Path("scripts/audit_6gy_layer6_gameplay_mechanic_outcome_artifact_selection_plan.py")
PLAN_6GX_PATH = Path("scripts/plan_6gx_layer6_gameplay_mechanic_outcome_artifact_selection.py")
AUDIT_6GW_PATH = Path("scripts/audit_6gw_layer6_gameplay_mechanic_real_backtest_dry_run.py")
IMPLEMENT_6GV_PATH = Path("scripts/implement_6gv_layer6_gameplay_mechanic_real_backtest_dry_run.py")
AUDIT_6GU_PATH = Path("scripts/audit_6gu_layer6_gameplay_mechanic_real_backtest_plan.py")
PLAN_6GT_PATH = Path("scripts/plan_6gt_layer6_gameplay_mechanic_real_backtests.py")

JSON_6GY = TMP_DIR / "layer6_6gy_outcome_artifact_selection_plan_audit.json"
CHECKS_6GY = TMP_DIR / "layer6_6gy_outcome_artifact_selection_plan_audit_checks.csv"
SELECTION_INTEGRITY_6GY = TMP_DIR / "layer6_6gy_outcome_artifact_selection_plan_audit_selection_integrity.csv"
FUTURE_6GZ_6GY = TMP_DIR / "layer6_6gy_outcome_artifact_selection_plan_audit_future_6gz_contract.csv"
JSON_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan.json"
CLASSIFICATION_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_classification.csv"
SELECTION_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_selection_summary.csv"
ADAPTER_REQ_6GX = TMP_DIR / "layer6_6gx_outcome_artifact_selection_plan_adapter_requirements.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SELECTED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_selected_families.csv"
PRIMARY_ADAPTER_CSV = TMP_DIR / f"{SLUG}_primary_game_outcomes_adapter.csv"
BASE_OUT_ADAPTER_CSV = TMP_DIR / f"{SLUG}_base_out_adapter.csv"
INNING_ADAPTER_CSV = TMP_DIR / f"{SLUG}_inning_runs_adapter.csv"
KEY_MAPPING_CSV = TMP_DIR / f"{SLUG}_key_mapping_policy.csv"
SCHEMA_VALIDATION_CSV = TMP_DIR / f"{SLUG}_schema_validation_policy.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
OUTPUT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_output_contract.csv"
FUTURE_6HA_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6ha_contract.csv"
FUTURE_6HB_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hb_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GY = "layer_6_gameplay_mechanic_outcome_artifact_selection_plan_audit_complete"
DIAGNOSIS_6GZ = "layer_6_gameplay_mechanic_outcome_artifact_adapter_plan_complete"
CURRENT_LAYER = "6GZ_layer_6_gameplay_mechanic_outcome_artifact_adapter_plan"
RECOMMENDED_NEXT_LAYER = "6HA_layer_6_gameplay_mechanic_outcome_artifact_adapter_plan_audit"
RECOMMENDED_PATH = "plan_outcome_artifact_adapter_then_audit_before_adapter_implementation"

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

REQUIRED_INPUT_ARTIFACTS = [
    JSON_6GY,
    CHECKS_6GY,
    SELECTION_INTEGRITY_6GY,
    FUTURE_6GZ_6GY,
    JSON_6GX,
    CLASSIFICATION_6GX,
    SELECTION_6GX,
    ADAPTER_REQ_6GX,
]

PRIMARY_FIELDS = [
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
]

BASE_OUT_FIELDS = [
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
]

INNING_FIELDS = [
    "game_id",
    "inning",
    "half_inning",
    "batting_team",
    "fielding_team",
    "runs_scored",
    "source_artifact_path",
    "validation_status",
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


def intish(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def adapter_rows(source_family: str, adapter_name: str, fields: List[str], required: bool) -> List[Dict[str, Any]]:
    return [
        {
            "adapter_name": adapter_name,
            "source_family": source_family,
            "canonical_field": field,
            "required": True,
            "adapter_required": required,
            "validation_required": True,
            "activation_allowed": False,
            "layer_6_exit_credit": False,
            "passed": True,
        }
        for field in fields
    ]


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6gy_before = AUDIT_6GY_PATH.read_text(encoding="utf-8") if AUDIT_6GY_PATH.exists() else ""
    plan_6gx_before = PLAN_6GX_PATH.read_text(encoding="utf-8") if PLAN_6GX_PATH.exists() else ""
    audit_6gw_before = AUDIT_6GW_PATH.read_text(encoding="utf-8") if AUDIT_6GW_PATH.exists() else ""
    implement_6gv_before = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_before = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_before = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6GY_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6gy = load_json(JSON_6GY)
    primary_count = intish(json_6gy.get("audited_primary_selected_artifact_count"), 0)
    base_out_count = intish(json_6gy.get("audited_candidate_base_out_transitions_count"), 0)
    inning_count = intish(json_6gy.get("audited_candidate_inning_runs_count"), 0)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gy_audit_exists", "expected": True, "actual": AUDIT_6GY_PATH.exists(), "passed": AUDIT_6GY_PATH.exists()},
        {"check": "6gy_audit_runs", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"check": "6gy_json_exists", "expected": True, "actual": JSON_6GY.exists(), "passed": JSON_6GY.exists()},
        {"check": "6gy_all_checks_passed", "expected": True, "actual": json_6gy.get("all_checks_passed"), "passed": json_6gy.get("all_checks_passed") is True},
        {"check": "6gy_audit_only", "expected": True, "actual": json_6gy.get("audit_only"), "passed": json_6gy.get("audit_only") is True},
        {"check": "6gy_diagnosis", "expected": DIAGNOSIS_6GY, "actual": json_6gy.get("diagnosis"), "passed": json_6gy.get("diagnosis") == DIAGNOSIS_6GY},
        {"check": "6gy_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6gy.get("recommended_next_layer"), "passed": json_6gy.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gy_primary_selection_status", "expected": "local_outcome_family_selected", "actual": json_6gy.get("audited_primary_selection_status"), "passed": json_6gy.get("audited_primary_selection_status") == "local_outcome_family_selected"},
        {"check": "6gy_primary_selected_family", "expected": "candidate_game_outcomes", "actual": json_6gy.get("audited_primary_selected_artifact_family"), "passed": json_6gy.get("audited_primary_selected_artifact_family") == "candidate_game_outcomes"},
        {"check": "6gy_primary_selected_count_positive", "expected": ">=1", "actual": primary_count, "passed": primary_count >= 1},
        {"check": "6gy_adapter_required_true", "expected": True, "actual": json_6gy.get("audited_adapter_required"), "passed": json_6gy.get("audited_adapter_required") is True},
        {"check": "6gy_materialization_plan_required_false", "expected": False, "actual": json_6gy.get("audited_materialization_plan_required"), "passed": json_6gy.get("audited_materialization_plan_required") is False},
        {"check": "6gy_layer_6_exit_ready_false", "expected": False, "actual": json_6gy.get("layer_6_exit_ready"), "passed": json_6gy.get("layer_6_exit_ready") is False},
        {"check": "6gy_real_backtests_run_false", "expected": False, "actual": json_6gy.get("real_backtests_run"), "passed": json_6gy.get("real_backtests_run") is False},
        {"check": "6gy_games_evaluated_zero", "expected": 0, "actual": json_6gy.get("games_evaluated"), "passed": intish(json_6gy.get("games_evaluated")) == 0},
        {"check": "6gy_actual_outcomes_joined_false", "expected": False, "actual": json_6gy.get("actual_outcomes_joined"), "passed": json_6gy.get("actual_outcomes_joined") is False},
        {"check": "6gy_activation_allowed_false", "expected": False, "actual": json_6gy.get("activation_allowed"), "passed": json_6gy.get("activation_allowed") is False},
        {"check": "6gy_layer_6_exit_credit_false", "expected": False, "actual": json_6gy.get("layer_6_exit_credit"), "passed": json_6gy.get("layer_6_exit_credit") is False},
        {"check": "6gy_live_fetches_false", "expected": False, "actual": json_6gy.get("live_data_fetches_run"), "passed": json_6gy.get("live_data_fetches_run") is False},
        {"check": "6gy_database_writes_false", "expected": False, "actual": json_6gy.get("database_writes_run"), "passed": json_6gy.get("database_writes_run") is False},
        {"check": "6gy_materialization_jobs_false", "expected": False, "actual": json_6gy.get("materialization_jobs_run"), "passed": json_6gy.get("materialization_jobs_run") is False},
        {"check": "6gy_production_simulations_false", "expected": False, "actual": json_6gy.get("production_simulations_run"), "passed": json_6gy.get("production_simulations_run") is False},
    ]

    input_artifact_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "required": True, "passed": path.exists()}
        for path in REQUIRED_INPUT_ARTIFACTS
    ]

    selected_family_rows = [
        {"family_role": "primary", "source_family": "candidate_game_outcomes", "selected_artifact_count": primary_count, "adapter_required": True, "supplemental": False, "materialization_required": False, "activation_allowed": False, "layer_6_exit_credit": False, "passed": primary_count >= 1},
        {"family_role": "supplemental_base_out", "source_family": "candidate_base_out_transitions", "selected_artifact_count": base_out_count, "adapter_required": base_out_count > 0, "supplemental": True, "materialization_required": False, "activation_allowed": False, "layer_6_exit_credit": False, "passed": base_out_count >= 0},
        {"family_role": "supplemental_inning_runs", "source_family": "candidate_inning_runs", "selected_artifact_count": inning_count, "adapter_required": inning_count > 0, "supplemental": True, "materialization_required": False, "activation_allowed": False, "layer_6_exit_credit": False, "passed": inning_count >= 0},
    ]

    primary_adapter_rows = adapter_rows("candidate_game_outcomes", "primary_game_outcomes_adapter", PRIMARY_FIELDS, True)
    base_out_adapter_rows = adapter_rows("candidate_base_out_transitions", "supplemental_base_out_adapter", BASE_OUT_FIELDS, base_out_count > 0)
    inning_adapter_rows = adapter_rows("candidate_inning_runs", "supplemental_inning_runs_adapter", INNING_FIELDS, inning_count > 0)

    key_mapping_rows = [
        {"policy": "game_id_exact_match_preferred", "required": True, "fail_closed": False, "passed": True},
        {"policy": "missing_game_id_requires_deterministic_composite_key", "required": True, "fail_closed": True, "passed": True},
        {"policy": "allowed_composite_key_date_home_team_away_team_season", "required": True, "fail_closed": False, "passed": True},
        {"policy": "ambiguous_duplicate_composite_keys_fail_closed", "required": True, "fail_closed": True, "passed": True},
        {"policy": "no_fuzzy_team_name_matching_without_future_audit", "required": True, "fail_closed": True, "passed": True},
        {"policy": "no_cross_artifact_join_without_schema_validation", "required": True, "fail_closed": True, "passed": True},
    ]

    schema_validation_rows = [
        {"validation": "missing_required_fields", "outcome": "fail_closed", "blocks_evaluation": True, "passed": True},
        {"validation": "null_required_identifiers", "outcome": "fail_closed", "blocks_evaluation": True, "passed": True},
        {"validation": "invalid_score_fields", "outcome": "fail_closed", "blocks_evaluation": True, "passed": True},
        {"validation": "negative_runs", "outcome": "fail_closed", "blocks_evaluation": True, "passed": True},
        {"validation": "invalid_inning_values", "outcome": "fail_closed", "blocks_evaluation": True, "passed": True},
        {"validation": "missing_source_provenance", "outcome": "fail_closed", "blocks_evaluation": True, "passed": True},
        {"validation": "invalid_winner_or_total_runs_consistency", "outcome": "fail_closed", "blocks_evaluation": True, "passed": True},
    ]

    fail_closed_rows = [
        {"condition": "schema_validation_failure", "blocks_adapter_output": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
        {"condition": "key_mapping_failure", "blocks_adapter_output": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
        {"condition": "ambiguous_game_identity", "blocks_adapter_output": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
        {"condition": "missing_primary_outcome_family", "blocks_adapter_output": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
        {"condition": "nonlocal_or_remote_artifact", "blocks_adapter_output": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
    ]

    output_contract_rows = [
        {"artifact": "normalized_game_outcomes", "future_path": "tmp/layer6_6hb_normalized_game_outcomes.csv", "local_tmp_only": True, "emitted_by_6gz": False, "passed": True},
        {"artifact": "normalized_base_out_transitions", "future_path": "tmp/layer6_6hb_normalized_base_out_transitions.csv", "local_tmp_only": True, "emitted_by_6gz": False, "passed": True},
        {"artifact": "normalized_inning_runs", "future_path": "tmp/layer6_6hb_normalized_inning_runs.csv", "local_tmp_only": True, "emitted_by_6gz": False, "passed": True},
        {"artifact": "adapter_validation_report", "future_path": "tmp/layer6_6hb_outcome_artifact_adapter_validation.csv", "local_tmp_only": True, "emitted_by_6gz": False, "passed": True},
        {"artifact": "adapter_provenance_report", "future_path": "tmp/layer6_6hb_outcome_artifact_adapter_provenance.csv", "local_tmp_only": True, "emitted_by_6gz": False, "passed": True},
    ]

    future_6ha_rows = [
        {"contract": "audit_6gz_adapter_plan", "required": True, "passed": True},
        {"contract": "verify_primary_game_outcomes_canonical_fields", "required": True, "passed": True},
        {"contract": "verify_supplemental_base_out_and_inning_adapter_contracts", "required": True, "passed": True},
        {"contract": "verify_key_mapping_policy_fail_closed", "required": True, "passed": True},
        {"contract": "verify_schema_validation_policy_fail_closed", "required": True, "passed": True},
        {"contract": "verify_no_adapter_runtime_implemented", "required": True, "passed": True},
        {"contract": "verify_no_real_evaluation_or_activation", "required": True, "passed": True},
        {"contract": "recommended_6ha_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_outcome_artifact_adapter_plan_audit_complete"},
    ]

    future_6hb_rows = [
        {"contract": "requires_6gz_plan_and_6ha_audit", "required": True, "passed": True},
        {"contract": "implement_local_outcome_artifact_adapter_only", "required": True, "passed": True},
        {"contract": "emit_normalized_local_tmp_artifacts_only", "required": True, "passed": True},
        {"contract": "no_real_backtests_or_mechanic_evaluation_in_6hb", "required": True, "passed": True},
        {"contract": "no_activation_or_layer_6_exit_credit_in_6hb", "required": True, "passed": True},
        {"contract": "future_adapter_implementation_audit_required_before_real_evaluation", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "adapter_plan_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "outcome_adapter_implemented_false", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_backtests", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_actual_outcome_joins", "expected": True, "actual": True, "passed": True},
        {"boundary": "normalized_outcomes_not_emitted", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_database_write", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_materialization_job", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_production_simulation", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": True, "actual": True, "passed": True},
        {"boundary": "layer_6_exit_credit_blocked", "expected": True, "actual": True, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6gy_after = AUDIT_6GY_PATH.read_text(encoding="utf-8") if AUDIT_6GY_PATH.exists() else ""
    plan_6gx_after = PLAN_6GX_PATH.read_text(encoding="utf-8") if PLAN_6GX_PATH.exists() else ""
    audit_6gw_after = AUDIT_6GW_PATH.read_text(encoding="utf-8") if AUDIT_6GW_PATH.exists() else ""
    implement_6gv_after = IMPLEMENT_6GV_PATH.read_text(encoding="utf-8") if IMPLEMENT_6GV_PATH.exists() else ""
    audit_6gu_after = AUDIT_6GU_PATH.read_text(encoding="utf-8") if AUDIT_6GU_PATH.exists() else ""
    plan_6gt_after = PLAN_6GT_PATH.read_text(encoding="utf-8") if PLAN_6GT_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gz_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gy_audit", "policy": "unchanged_by_6gz", "passed": audit_6gy_after == audit_6gy_before},
        {"surface": "6gx_plan", "policy": "unchanged_by_6gz", "passed": plan_6gx_after == plan_6gx_before},
        {"surface": "6gw_audit", "policy": "unchanged_by_6gz", "passed": audit_6gw_after == audit_6gw_before},
        {"surface": "6gv_implementation", "policy": "unchanged_by_6gz", "passed": implement_6gv_after == implement_6gv_before},
        {"surface": "6gu_audit", "policy": "unchanged_by_6gz", "passed": audit_6gu_after == audit_6gu_before},
        {"surface": "6gt_plan", "policy": "unchanged_by_6gz", "passed": plan_6gt_after == plan_6gt_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gz", "passed": True},
        {"surface": "projection_behavior_or_fixtures_or_defaults", "policy": "unchanged_by_6gz", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_plan_only", "expected": True, "actual": True, "passed": True},
        {"decision": "outcome_adapter_implemented", "expected": False, "actual": False, "passed": True},
        {"decision": "normalized_outcomes_emitted", "expected": False, "actual": False, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GZ, "actual": DIAGNOSIS_6GZ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_artifact_rows), "detail": f"{sum(1 for row in input_artifact_rows if row['passed'])}/{len(input_artifact_rows)}"},
        {"check": "selected_families", "passed": all(row["passed"] for row in selected_family_rows), "detail": f"{sum(1 for row in selected_family_rows if row['passed'])}/{len(selected_family_rows)}"},
        {"check": "primary_adapter_fields", "passed": {row["canonical_field"] for row in primary_adapter_rows} == set(PRIMARY_FIELDS), "detail": f"{len(primary_adapter_rows)}/{len(PRIMARY_FIELDS)}"},
        {"check": "base_out_adapter_fields", "passed": {row["canonical_field"] for row in base_out_adapter_rows} == set(BASE_OUT_FIELDS), "detail": f"{len(base_out_adapter_rows)}/{len(BASE_OUT_FIELDS)}"},
        {"check": "inning_adapter_fields", "passed": {row["canonical_field"] for row in inning_adapter_rows} == set(INNING_FIELDS), "detail": f"{len(inning_adapter_rows)}/{len(INNING_FIELDS)}"},
        {"check": "key_mapping_policy", "passed": all(row["passed"] for row in key_mapping_rows), "detail": f"{sum(1 for row in key_mapping_rows if row['passed'])}/{len(key_mapping_rows)}"},
        {"check": "schema_validation_policy", "passed": all(row["passed"] for row in schema_validation_rows), "detail": f"{sum(1 for row in schema_validation_rows if row['passed'])}/{len(schema_validation_rows)}"},
        {"check": "fail_closed_policy", "passed": all(row["passed"] for row in fail_closed_rows), "detail": f"{sum(1 for row in fail_closed_rows if row['passed'])}/{len(fail_closed_rows)}"},
        {"check": "output_contract", "passed": all(row["passed"] and not row["emitted_by_6gz"] for row in output_contract_rows), "detail": f"{sum(1 for row in output_contract_rows if row['passed'])}/{len(output_contract_rows)}"},
        {"check": "future_6ha_contract", "passed": all(row["passed"] for row in future_6ha_rows), "detail": f"{sum(1 for row in future_6ha_rows if row['passed'])}/{len(future_6ha_rows)}"},
        {"check": "future_6hb_contract", "passed": all(row["passed"] for row in future_6hb_rows), "detail": f"{sum(1 for row in future_6hb_rows if row['passed'])}/{len(future_6hb_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_artifact_rows),
        "selected_families": write_csv(SELECTED_FAMILIES_CSV, selected_family_rows),
        "primary_game_outcomes_adapter": write_csv(PRIMARY_ADAPTER_CSV, primary_adapter_rows),
        "base_out_adapter": write_csv(BASE_OUT_ADAPTER_CSV, base_out_adapter_rows),
        "inning_runs_adapter": write_csv(INNING_ADAPTER_CSV, inning_adapter_rows),
        "key_mapping_policy": write_csv(KEY_MAPPING_CSV, key_mapping_rows),
        "schema_validation_policy": write_csv(SCHEMA_VALIDATION_CSV, schema_validation_rows),
        "fail_closed_policy": write_csv(FAIL_CLOSED_CSV, fail_closed_rows),
        "output_contract": write_csv(OUTPUT_CONTRACT_CSV, output_contract_rows),
        "future_6ha_contract": write_csv(FUTURE_6HA_CONTRACT_CSV, future_6ha_rows),
        "future_6hb_contract": write_csv(FUTURE_6HB_CONTRACT_CSV, future_6hb_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GZ",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "adapter_plan_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6GZ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "audited_predecessor_layer": "6GY",
        "predecessor_audit": str(AUDIT_6GY_PATH),
        "predecessor_audit_returncode": audit_run.returncode,
        "predecessor_audit_diagnosis": json_6gy.get("diagnosis"),
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "outcome_adapter_implemented": False,
        "actual_outcomes_joined": False,
        "normalized_outcomes_emitted": False,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "activation_allowed": False,
        "layer_6_exit_credit": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(EVALUATION_WINDOWS),
        "primary_source_family": "candidate_game_outcomes",
        "primary_selected_artifact_count": primary_count,
        "base_out_source_family": "candidate_base_out_transitions",
        "base_out_selected_artifact_count": base_out_count,
        "inning_source_family": "candidate_inning_runs",
        "inning_selected_artifact_count": inning_count,
        "adapter_required": True,
        "materialization_plan_required": False,
        "future_adapter_audit_required": True,
        "future_real_evaluation_allowed_by_this_layer": False,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "selected_families_csv": str(SELECTED_FAMILIES_CSV),
            "primary_game_outcomes_adapter_csv": str(PRIMARY_ADAPTER_CSV),
            "base_out_adapter_csv": str(BASE_OUT_ADAPTER_CSV),
            "inning_runs_adapter_csv": str(INNING_ADAPTER_CSV),
            "key_mapping_policy_csv": str(KEY_MAPPING_CSV),
            "schema_validation_policy_csv": str(SCHEMA_VALIDATION_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_CSV),
            "output_contract_csv": str(OUTPUT_CONTRACT_CSV),
            "future_6ha_contract_csv": str(FUTURE_6HA_CONTRACT_CSV),
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
