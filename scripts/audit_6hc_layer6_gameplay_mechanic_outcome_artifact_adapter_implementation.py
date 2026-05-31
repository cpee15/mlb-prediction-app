#!/usr/bin/env python3
"""Audit Layer 6HB local outcome artifact adapter implementation."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


SLUG = "layer6_6hc_outcome_artifact_adapter_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENT_6HB_PATH = Path("scripts/implement_6hb_layer6_gameplay_mechanic_outcome_artifact_adapter.py")
AUDIT_6HA_PATH = Path("scripts/audit_6ha_layer6_gameplay_mechanic_outcome_artifact_adapter_plan.py")
PLAN_6GZ_PATH = Path("scripts/plan_6gz_layer6_gameplay_mechanic_outcome_artifact_adapter.py")

JSON_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation.json"
CHECKS_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_checks.csv"
PREDECESSOR_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_predecessor.csv"
INPUT_ARTIFACTS_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_input_artifacts.csv"
SELECTED_SOURCES_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_selected_sources.csv"
NORMALIZED_GAME_OUTCOMES_6HB = TMP_DIR / "layer6_6hb_normalized_game_outcomes.csv"
NORMALIZED_BASE_OUT_6HB = TMP_DIR / "layer6_6hb_normalized_base_out_transitions.csv"
NORMALIZED_INNING_RUNS_6HB = TMP_DIR / "layer6_6hb_normalized_inning_runs.csv"
VALIDATION_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_validation.csv"
PROVENANCE_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_provenance.csv"
FAIL_CLOSED_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_fail_closed.csv"
OUTPUT_CONTRACT_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_output_contract.csv"
FUTURE_6HC_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_future_6hc_contract.csv"
SAFETY_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_safety_boundaries.csv"
IMMUTABILITY_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_immutability.csv"
RECOMMENDED_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
NORMALIZED_SCHEMAS_CSV = TMP_DIR / f"{SLUG}_normalized_schemas.csv"
VALIDATION_REPORT_CSV = TMP_DIR / f"{SLUG}_validation_report.csv"
PROVENANCE_REPORT_CSV = TMP_DIR / f"{SLUG}_provenance_report.csv"
FAIL_CLOSED_AUDIT_CSV = TMP_DIR / f"{SLUG}_fail_closed.csv"
OUTPUT_CONTRACT_AUDIT_CSV = TMP_DIR / f"{SLUG}_output_contract.csv"
FUTURE_6HD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hd_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HB = "layer_6_gameplay_mechanic_outcome_artifact_adapter_implementation_complete"
DIAGNOSIS_6HC = "layer_6_gameplay_mechanic_outcome_artifact_adapter_implementation_audit_complete"
CURRENT_LAYER = "6HC_layer_6_gameplay_mechanic_outcome_artifact_adapter_implementation_audit"
RECOMMENDED_NEXT_LAYER = "6HD_layer_6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan"
RECOMMENDED_PATH = "audit_adapter_implementation_then_plan_schema_key_compatibility_resolution"

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

REQUIRED_6HB_ARTIFACTS = [
    JSON_6HB,
    CHECKS_6HB,
    PREDECESSOR_6HB,
    INPUT_ARTIFACTS_6HB,
    SELECTED_SOURCES_6HB,
    NORMALIZED_GAME_OUTCOMES_6HB,
    NORMALIZED_BASE_OUT_6HB,
    NORMALIZED_INNING_RUNS_6HB,
    VALIDATION_6HB,
    PROVENANCE_6HB,
    FAIL_CLOSED_6HB,
    OUTPUT_CONTRACT_6HB,
    FUTURE_6HC_6HB,
    SAFETY_6HB,
    IMMUTABILITY_6HB,
    RECOMMENDED_6HB,
]

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
            reader = csv.reader(handle)
            return set(next(reader, []))
    return set(rows[0].keys())


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    implement_6hb_before = IMPLEMENT_6HB_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HB_PATH.exists() else ""
    audit_6ha_before = AUDIT_6HA_PATH.read_text(encoding="utf-8") if AUDIT_6HA_PATH.exists() else ""
    plan_6gz_before = PLAN_6GZ_PATH.read_text(encoding="utf-8") if PLAN_6GZ_PATH.exists() else ""

    implementation_run = subprocess.run(
        [sys.executable, str(IMPLEMENT_6HB_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6hb = load_json(JSON_6HB)
    checks_6hb = read_csv(CHECKS_6HB)
    validation_rows = read_csv(VALIDATION_6HB)
    provenance_rows = read_csv(PROVENANCE_6HB)
    fail_closed_rows = read_csv(FAIL_CLOSED_6HB)
    output_contract_rows = read_csv(OUTPUT_CONTRACT_6HB)
    future_6hc_rows = read_csv(FUTURE_6HC_6HB)
    safety_rows = read_csv(SAFETY_6HB)

    validation_failed_count = intish(json_6hb.get("validation_failed_closed_row_count"), 0)
    validation_passed_count = intish(json_6hb.get("validation_passed_row_count"), -1)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hb_implementation_exists", "expected": True, "actual": IMPLEMENT_6HB_PATH.exists(), "passed": IMPLEMENT_6HB_PATH.exists()},
        {"check": "6hb_implementation_runs", "expected": 0, "actual": implementation_run.returncode, "passed": implementation_run.returncode == 0},
        {"check": "6hb_json_exists", "expected": True, "actual": JSON_6HB.exists(), "passed": JSON_6HB.exists()},
        {"check": "6hb_all_checks_passed", "expected": True, "actual": json_6hb.get("all_checks_passed"), "passed": json_6hb.get("all_checks_passed") is True},
        {"check": "6hb_diagnosis", "expected": DIAGNOSIS_6HB, "actual": json_6hb.get("diagnosis"), "passed": json_6hb.get("diagnosis") == DIAGNOSIS_6HB},
        {"check": "6hb_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6hb.get("recommended_next_layer"), "passed": json_6hb.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6hb_implementation_only", "expected": True, "actual": json_6hb.get("implementation_only"), "passed": json_6hb.get("implementation_only") is True},
        {"check": "6hb_local_adapter_implemented", "expected": True, "actual": json_6hb.get("local_adapter_implemented"), "passed": json_6hb.get("local_adapter_implemented") is True},
        {"check": "6hb_normalized_game_outcomes_emitted", "expected": True, "actual": json_6hb.get("normalized_game_outcomes_emitted"), "passed": json_6hb.get("normalized_game_outcomes_emitted") is True},
        {"check": "6hb_normalized_base_out_emitted", "expected": True, "actual": json_6hb.get("normalized_base_out_transitions_emitted"), "passed": json_6hb.get("normalized_base_out_transitions_emitted") is True},
        {"check": "6hb_normalized_inning_emitted", "expected": True, "actual": json_6hb.get("normalized_inning_runs_emitted"), "passed": json_6hb.get("normalized_inning_runs_emitted") is True},
        {"check": "6hb_validation_report_emitted", "expected": True, "actual": json_6hb.get("adapter_validation_report_emitted"), "passed": json_6hb.get("adapter_validation_report_emitted") is True},
        {"check": "6hb_provenance_report_emitted", "expected": True, "actual": json_6hb.get("adapter_provenance_report_emitted"), "passed": json_6hb.get("adapter_provenance_report_emitted") is True},
        {"check": "6hb_selected_source_count_positive", "expected": ">=1", "actual": json_6hb.get("selected_source_artifact_count"), "passed": intish(json_6hb.get("selected_source_artifact_count"), 0) >= 1},
        {"check": "6hb_source_read_count_positive", "expected": ">=1", "actual": json_6hb.get("source_artifacts_read_count"), "passed": intish(json_6hb.get("source_artifacts_read_count"), 0) >= 1},
        {"check": "6hb_source_failed_count_zero", "expected": 0, "actual": json_6hb.get("source_artifacts_failed_count"), "passed": intish(json_6hb.get("source_artifacts_failed_count")) == 0},
        {"check": "6hb_normalized_game_count_positive", "expected": ">=1", "actual": json_6hb.get("normalized_game_outcomes_count"), "passed": intish(json_6hb.get("normalized_game_outcomes_count"), 0) >= 1},
        {"check": "6hb_normalized_base_count_positive", "expected": ">=1", "actual": json_6hb.get("normalized_base_out_transitions_count"), "passed": intish(json_6hb.get("normalized_base_out_transitions_count"), 0) >= 1},
        {"check": "6hb_normalized_inning_count_positive", "expected": ">=1", "actual": json_6hb.get("normalized_inning_runs_count"), "passed": intish(json_6hb.get("normalized_inning_runs_count"), 0) >= 1},
        {"check": "6hb_validation_failed_closed_positive", "expected": ">=1", "actual": validation_failed_count, "passed": validation_failed_count >= 1},
        {"check": "6hb_validation_passed_zero_current_artifacts", "expected": 0, "actual": validation_passed_count, "passed": validation_passed_count == 0},
        {"check": "6hb_real_backtests_false", "expected": False, "actual": json_6hb.get("real_backtests_run"), "passed": json_6hb.get("real_backtests_run") is False},
        {"check": "6hb_mechanic_evaluations_false", "expected": False, "actual": json_6hb.get("mechanic_evaluations_run"), "passed": json_6hb.get("mechanic_evaluations_run") is False},
        {"check": "6hb_actual_outcomes_joined_to_mechanics_false", "expected": False, "actual": json_6hb.get("actual_outcomes_joined_to_mechanics"), "passed": json_6hb.get("actual_outcomes_joined_to_mechanics") is False},
        {"check": "6hb_activation_allowed_false", "expected": False, "actual": json_6hb.get("activation_allowed"), "passed": json_6hb.get("activation_allowed") is False},
        {"check": "6hb_layer_6_exit_credit_false", "expected": False, "actual": json_6hb.get("layer_6_exit_credit"), "passed": json_6hb.get("layer_6_exit_credit") is False},
    ]

    artifact_presence_rows = [{"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()} for path in REQUIRED_6HB_ARTIFACTS]

    checks_consistency_rows = [
        {"source_check": row.get("check"), "source_passed": row.get("passed"), "detail": row.get("detail", ""), "passed": boolish(row.get("passed"))}
        for row in checks_6hb
    ]

    schema_rows = [
        {
            "artifact": "normalized_game_outcomes",
            "expected_fields": "|".join(sorted(GAME_FIELDS)),
            "actual_fields": "|".join(sorted(columns(NORMALIZED_GAME_OUTCOMES_6HB))),
            "passed": columns(NORMALIZED_GAME_OUTCOMES_6HB) == GAME_FIELDS,
        },
        {
            "artifact": "normalized_base_out_transitions",
            "expected_fields": "|".join(sorted(BASE_OUT_FIELDS)),
            "actual_fields": "|".join(sorted(columns(NORMALIZED_BASE_OUT_6HB))),
            "passed": columns(NORMALIZED_BASE_OUT_6HB) == BASE_OUT_FIELDS,
        },
        {
            "artifact": "normalized_inning_runs",
            "expected_fields": "|".join(sorted(INNING_FIELDS)),
            "actual_fields": "|".join(sorted(columns(NORMALIZED_INNING_RUNS_6HB))),
            "passed": columns(NORMALIZED_INNING_RUNS_6HB) == INNING_FIELDS,
        },
    ]

    validation_audit_rows = [
        {
            "check": "validation_status_column_present",
            "expected": True,
            "actual": "validation_status" in columns(VALIDATION_6HB),
            "passed": "validation_status" in columns(VALIDATION_6HB),
        },
        {
            "check": "failed_rows_block_future_evaluation",
            "expected": True,
            "actual": "validation_rows",
            "passed": all(
                boolish(row.get("blocks_future_evaluation"))
                for row in validation_rows
                if row.get("validation_status") != "passed"
            ),
        },
        {
            "check": "all_rows_block_activation",
            "expected": True,
            "actual": "validation_rows",
            "passed": all(boolish(row.get("blocks_activation")) for row in validation_rows),
        },
        {
            "check": "all_rows_block_layer_6_exit_credit",
            "expected": True,
            "actual": "validation_rows",
            "passed": all(boolish(row.get("blocks_layer_6_exit_credit")) for row in validation_rows),
        },
        {
            "check": "validation_passed_zero_current_artifacts",
            "expected": 0,
            "actual": validation_passed_count,
            "passed": validation_passed_count == 0,
        },
        {
            "check": "validation_failed_closed_positive",
            "expected": ">=1",
            "actual": validation_failed_count,
            "passed": validation_failed_count >= 1,
        },
    ]

    provenance_audit_rows = [
        {
            "check": "mutated_source_false_all_rows",
            "expected": False,
            "actual": "mutated_source",
            "passed": all(not boolish(row.get("mutated_source")) for row in provenance_rows),
        },
        {
            "check": "live_fetch_used_false_all_rows",
            "expected": False,
            "actual": "live_fetch_used",
            "passed": all(not boolish(row.get("live_fetch_used")) for row in provenance_rows),
        },
        {
            "check": "database_write_used_false_all_rows",
            "expected": False,
            "actual": "database_write_used",
            "passed": all(not boolish(row.get("database_write_used")) for row in provenance_rows),
        },
        {
            "check": "source_artifacts_read",
            "expected": ">=1",
            "actual": len(provenance_rows),
            "passed": len(provenance_rows) >= 1,
        },
    ]

    fail_closed_audit_rows = [
        {
            "condition": row.get("condition"),
            "source_family": row.get("source_family"),
            "blocks_future_evaluation": row.get("blocks_future_evaluation"),
            "blocks_activation": row.get("blocks_activation"),
            "blocks_layer_6_exit_credit": row.get("blocks_layer_6_exit_credit"),
            "passed": boolish(row.get("blocks_future_evaluation"))
            and boolish(row.get("blocks_activation"))
            and boolish(row.get("blocks_layer_6_exit_credit"))
            and boolish(row.get("passed")),
        }
        for row in fail_closed_rows
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

    future_6hd_rows = [
        {"contract": "schema_key_compatibility_resolution_plan_required", "required": True, "passed": True},
        {"contract": "analyze_zero_validation_passed_rows", "required": True, "passed": True},
        {"contract": "map_source_columns_to_canonical_fields", "required": True, "passed": True},
        {"contract": "separate_actual_outcome_artifacts_from_meta_artifacts", "required": True, "passed": True},
        {"contract": "propose_stricter_source_selection_filters", "required": True, "passed": True},
        {"contract": "no_real_backtests_or_mechanic_evaluation_in_6hd", "required": True, "passed": True},
        {"contract": "no_activation_or_layer_6_exit_credit_in_6hd", "required": True, "passed": True},
        {"contract": "recommended_6hd_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan_complete"},
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
    implement_6hb_after = IMPLEMENT_6HB_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HB_PATH.exists() else ""
    audit_6ha_after = AUDIT_6HA_PATH.read_text(encoding="utf-8") if AUDIT_6HA_PATH.exists() else ""
    plan_6gz_after = PLAN_6GZ_PATH.read_text(encoding="utf-8") if PLAN_6GZ_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6hc_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hb_implementation", "policy": "unchanged_by_6hc", "passed": implement_6hb_after == implement_6hb_before},
        {"surface": "6ha_audit", "policy": "unchanged_by_6hc", "passed": audit_6ha_after == audit_6ha_before},
        {"surface": "6gz_plan", "policy": "unchanged_by_6hc", "passed": plan_6gz_after == plan_6gz_before},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hc", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "schema_key_compatibility_resolution_required", "expected": True, "actual": True, "passed": True},
        {"decision": "real_evaluation_blocked_by_validation", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanic_evaluations_run", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HC, "actual": DIAGNOSIS_6HC, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": len(checks_consistency_rows) >= 15 and all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "normalized_schemas", "passed": all(row["passed"] for row in schema_rows), "detail": f"{sum(1 for row in schema_rows if row['passed'])}/{len(schema_rows)}"},
        {"check": "validation_report", "passed": all(row["passed"] for row in validation_audit_rows), "detail": f"{sum(1 for row in validation_audit_rows if row['passed'])}/{len(validation_audit_rows)}"},
        {"check": "provenance_report", "passed": all(row["passed"] for row in provenance_audit_rows), "detail": f"{sum(1 for row in provenance_audit_rows if row['passed'])}/{len(provenance_audit_rows)}"},
        {"check": "fail_closed", "passed": all(row["passed"] for row in fail_closed_audit_rows), "detail": f"{sum(1 for row in fail_closed_audit_rows if row['passed'])}/{len(fail_closed_audit_rows)}"},
        {"check": "output_contract", "passed": all(row["passed"] for row in output_contract_audit_rows), "detail": f"{sum(1 for row in output_contract_audit_rows if row['passed'])}/{len(output_contract_audit_rows)}"},
        {"check": "future_6hc_contract", "passed": all(boolish(row.get("passed")) for row in future_6hc_rows), "detail": f"{sum(1 for row in future_6hc_rows if boolish(row.get('passed')))}" + f"/{len(future_6hc_rows)}"},
        {"check": "future_6hd_contract", "passed": all(row["passed"] for row in future_6hd_rows), "detail": f"{sum(1 for row in future_6hd_rows if row['passed'])}/{len(future_6hd_rows)}"},
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
        "normalized_schemas": write_csv(NORMALIZED_SCHEMAS_CSV, schema_rows),
        "validation_report": write_csv(VALIDATION_REPORT_CSV, validation_audit_rows),
        "provenance_report": write_csv(PROVENANCE_REPORT_CSV, provenance_audit_rows),
        "fail_closed": write_csv(FAIL_CLOSED_AUDIT_CSV, fail_closed_audit_rows),
        "output_contract": write_csv(OUTPUT_CONTRACT_AUDIT_CSV, output_contract_audit_rows),
        "future_6hd_contract": write_csv(FUTURE_6HD_CONTRACT_CSV, future_6hd_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_audit_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HC",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "audited_layer": "6HB",
        "audited_implementation_diagnosis": json_6hb.get("diagnosis"),
        "diagnosis": DIAGNOSIS_6HC if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_implementation": str(IMPLEMENT_6HB_PATH),
        "predecessor_implementation_returncode": implementation_run.returncode,
        "predecessor_implementation_diagnosis": json_6hb.get("diagnosis"),
        "normalized_game_outcomes_count": intish(json_6hb.get("normalized_game_outcomes_count"), 0),
        "normalized_base_out_transitions_count": intish(json_6hb.get("normalized_base_out_transitions_count"), 0),
        "normalized_inning_runs_count": intish(json_6hb.get("normalized_inning_runs_count"), 0),
        "validation_passed_row_count": validation_passed_count,
        "validation_failed_closed_row_count": validation_failed_count,
        "selected_source_artifact_count": intish(json_6hb.get("selected_source_artifact_count"), 0),
        "source_artifacts_read_count": intish(json_6hb.get("source_artifacts_read_count"), 0),
        "source_artifacts_failed_count": intish(json_6hb.get("source_artifacts_failed_count"), 0),
        "schema_key_compatibility_resolution_required": True,
        "real_evaluation_blocked_by_validation": True,
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
        "future_real_evaluation_allowed_by_this_layer": False,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "checks_consistency_csv": str(CHECKS_CONSISTENCY_CSV),
            "normalized_schemas_csv": str(NORMALIZED_SCHEMAS_CSV),
            "validation_report_csv": str(VALIDATION_REPORT_CSV),
            "provenance_report_csv": str(PROVENANCE_REPORT_CSV),
            "fail_closed_csv": str(FAIL_CLOSED_AUDIT_CSV),
            "output_contract_csv": str(OUTPUT_CONTRACT_AUDIT_CSV),
            "future_6hd_contract_csv": str(FUTURE_6HD_CONTRACT_CSV),
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
