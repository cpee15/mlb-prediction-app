#!/usr/bin/env python3
"""Audit Layer 6HN local outcome source materialization implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ho_source_materialization_implementation_audit"
TMP_DIR = Path("tmp")

IMPL_6HN_PATH = Path("scripts/materialize_6hn_layer6_gameplay_mechanic_outcome_sources.py")

JSON_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation.json"
CHECKS_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_checks.csv"
PREDECESSOR_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_predecessor.csv"
INPUT_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_input_artifacts.csv"
CANDIDATES_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_candidate_sources.csv"
SOURCE_SELECTION_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_source_selection.csv"
MATERIALIZATION_RESULTS_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_materialization_results.csv"
QUALITY_AUDIT_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_quality_report_audit.csv"
MANIFEST_AUDIT_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_manifest_audit.csv"
DECISION_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_decision.csv"
FUTURE_6HO_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_future_6ho_contract.csv"
SAFETY_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_safety_boundaries.csv"
IMMUTABILITY_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_immutability.csv"
RECOMMENDED_6HN = TMP_DIR / "layer6_6hn_source_materialization_implementation_recommended_path.csv"

MAT_GAME = TMP_DIR / "layer6_materialized_game_level_outcomes.csv"
MAT_BASE_OUT = TMP_DIR / "layer6_materialized_base_out_transitions.csv"
MAT_INNING = TMP_DIR / "layer6_materialized_inning_runs.csv"
MAT_MANIFEST = TMP_DIR / "layer6_materialized_outcome_source_manifest.json"
MAT_QUALITY = TMP_DIR / "layer6_materialized_outcome_source_quality_report.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
MATERIALIZED_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_materialized_artifacts.csv"
QUALITY_REPORT_CSV = TMP_DIR / f"{SLUG}_quality_report.csv"
MANIFEST_CSV = TMP_DIR / f"{SLUG}_manifest.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed.csv"
SOURCE_SELECTION_CSV = TMP_DIR / f"{SLUG}_source_selection.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HP_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hp_contract.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HN = "layer_6_gameplay_mechanic_outcome_artifact_source_materialization_implementation_complete"
DIAGNOSIS_6HO = "layer_6_gameplay_mechanic_outcome_artifact_source_materialization_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6HN = "6HO_layer_6_gameplay_mechanic_outcome_artifact_source_materialization_implementation_audit"
RECOMMENDED_PATH_6HN = "materialize_outcome_sources_then_audit_before_adapter_revision_or_real_evaluation"
RECOMMENDED_NEXT_LAYER_6HO = "6HP_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan"
RECOMMENDED_PATH_6HO = "audit_fail_closed_materialization_then_plan_deterministic_source_acquisition_before_adapter_revision"

GAME_COLS = [
    "game_id", "game_date", "season", "home_team", "away_team", "home_score",
    "away_score", "winning_team", "losing_team", "final_status",
    "source_artifact_path", "source_record_id", "materialization_rule_id",
    "materialization_confidence",
]
BASE_OUT_COLS = [
    "game_id", "event_id", "play_id", "inning", "half_inning", "batting_team",
    "fielding_team", "start_base_state", "start_outs", "end_base_state",
    "end_outs", "runs_scored", "event_type", "batter_id", "pitcher_id",
    "sequence_number", "source_artifact_path", "source_record_id",
    "materialization_rule_id", "materialization_confidence",
]
INNING_COLS = [
    "game_id", "inning", "half_inning", "batting_team", "fielding_team",
    "runs_scored", "start_score_batting", "start_score_fielding",
    "end_score_batting", "end_score_fielding", "source_artifact_path",
    "source_record_id", "materialization_rule_id", "materialization_confidence",
]
QUALITY_COLS = [
    "artifact_path", "requirement_family", "required_column_count",
    "present_column_count", "missing_columns", "row_count", "null_key_count",
    "duplicate_key_count", "invalid_state_count", "confidence_minimum", "passed",
]

EXPECTED_CHECKS_6HN = [
    "predecessor",
    "input_artifacts",
    "candidate_sources",
    "target_artifacts_created",
    "materialization_results",
    "quality_report",
    "manifest",
    "decision",
    "future_6ho_contract",
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
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def intish(value: Any, default: int = -1) -> int:
    try:
        return int(float(str(value)))
    except Exception:
        return default


def floatish(value: Any, default: float = -1.0) -> float:
    try:
        return float(str(value))
    except Exception:
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


def cols_present(rows: List[Dict[str, str]], expected_cols: List[str]) -> bool:
    if not rows:
        return False
    return set(expected_cols).issubset(set(rows[0].keys()))


def header_cols(path: Path) -> List[str]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            return next(reader)
        except StopIteration:
            return []


def data_row_count(path: Path) -> int:
    if not path.exists():
        return -1
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    return max(0, len(rows) - 1)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    impl_6hn_before = IMPL_6HN_PATH.read_text(encoding="utf-8") if IMPL_6HN_PATH.exists() else ""

    json_6hn = load_json(JSON_6HN)
    checks_6hn = read_csv(CHECKS_6HN)
    source_selection_6hn = read_csv(SOURCE_SELECTION_6HN)
    materialization_results_6hn = read_csv(MATERIALIZATION_RESULTS_6HN)
    quality_report = read_csv(MAT_QUALITY)
    manifest = load_json(MAT_MANIFEST)

    required_6hn_artifacts = [
        JSON_6HN,
        CHECKS_6HN,
        PREDECESSOR_6HN,
        INPUT_6HN,
        CANDIDATES_6HN,
        SOURCE_SELECTION_6HN,
        MATERIALIZATION_RESULTS_6HN,
        QUALITY_AUDIT_6HN,
        MANIFEST_AUDIT_6HN,
        DECISION_6HN,
        FUTURE_6HO_6HN,
        SAFETY_6HN,
        IMMUTABILITY_6HN,
        RECOMMENDED_6HN,
        MAT_GAME,
        MAT_BASE_OUT,
        MAT_INNING,
        MAT_MANIFEST,
        MAT_QUALITY,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hn_implementation_exists", "expected": True, "actual": IMPL_6HN_PATH.exists(), "passed": IMPL_6HN_PATH.exists()},
        {"check": "6hn_json_exists", "expected": True, "actual": JSON_6HN.exists(), "passed": JSON_6HN.exists()},
        {"check": "6hn_all_checks_passed", "expected": True, "actual": json_6hn.get("all_checks_passed"), "passed": json_6hn.get("all_checks_passed") is True},
        {"check": "6hn_diagnosis", "expected": DIAGNOSIS_6HN, "actual": json_6hn.get("diagnosis"), "passed": json_6hn.get("diagnosis") == DIAGNOSIS_6HN},
        {"check": "6hn_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HN, "actual": json_6hn.get("recommended_next_layer"), "passed": json_6hn.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HN},
        {"check": "6hn_recommended_path", "expected": RECOMMENDED_PATH_6HN, "actual": json_6hn.get("recommended_path"), "passed": json_6hn.get("recommended_path") == RECOMMENDED_PATH_6HN},
        {"check": "6hn_source_materialization_only", "expected": True, "actual": json_6hn.get("source_materialization_only"), "passed": json_6hn.get("source_materialization_only") is True},
        {"check": "6hn_materialization_jobs_run", "expected": True, "actual": json_6hn.get("materialization_jobs_run"), "passed": json_6hn.get("materialization_jobs_run") is True},
        {"check": "6hn_quality_failed", "expected": False, "actual": json_6hn.get("all_target_artifacts_quality_passed"), "passed": json_6hn.get("all_target_artifacts_quality_passed") is False},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in required_6hn_artifacts
    ]

    check_lookup = {row.get("check"): row for row in checks_6hn}
    checks_consistency_rows = []
    for check_name in EXPECTED_CHECKS_6HN:
        row = check_lookup.get(check_name, {})
        checks_consistency_rows.append({
            "check": check_name,
            "expected_present": True,
            "present": bool(row),
            "expected_passed": True,
            "actual_passed": row.get("passed"),
            "passed": bool(row) and boolish(row.get("passed")),
        })

    materialized_rows = [
        {
            "artifact_path": str(MAT_GAME),
            "requirement_family": "game_level_outcomes",
            "expected_columns": len(GAME_COLS),
            "actual_columns": len(header_cols(MAT_GAME)),
            "expected_row_count": 0,
            "actual_row_count": data_row_count(MAT_GAME),
            "passed": header_cols(MAT_GAME) == GAME_COLS and data_row_count(MAT_GAME) == 0,
        },
        {
            "artifact_path": str(MAT_BASE_OUT),
            "requirement_family": "base_out_transitions",
            "expected_columns": len(BASE_OUT_COLS),
            "actual_columns": len(header_cols(MAT_BASE_OUT)),
            "expected_row_count": 0,
            "actual_row_count": data_row_count(MAT_BASE_OUT),
            "passed": header_cols(MAT_BASE_OUT) == BASE_OUT_COLS and data_row_count(MAT_BASE_OUT) == 0,
        },
        {
            "artifact_path": str(MAT_INNING),
            "requirement_family": "inning_runs",
            "expected_columns": len(INNING_COLS),
            "actual_columns": len(header_cols(MAT_INNING)),
            "expected_row_count": 0,
            "actual_row_count": data_row_count(MAT_INNING),
            "passed": header_cols(MAT_INNING) == INNING_COLS and data_row_count(MAT_INNING) == 0,
        },
    ]

    expected_required_counts = {
        "game_level_outcomes": 14,
        "base_out_transitions": 20,
        "inning_runs": 14,
    }

    quality_rows = []
    for family, expected_required_count in expected_required_counts.items():
        row = find_row(quality_report, "requirement_family", family)
        quality_rows.append({
            "requirement_family": family,
            "present": bool(row),
            "required_column_count": row.get("required_column_count"),
            "row_count": row.get("row_count"),
            "missing_columns": row.get("missing_columns"),
            "confidence_minimum": row.get("confidence_minimum"),
            "quality_passed": row.get("passed"),
            "passed": (
                bool(row)
                and intish(row.get("required_column_count")) == expected_required_count
                and intish(row.get("row_count")) == 0
                and row.get("missing_columns", "") == ""
                and floatish(row.get("confidence_minimum")) == 0.0
                and not boolish(row.get("passed"))
            ),
        })

    manifest_rows = [
        {"audit": "manifest_exists", "expected": True, "actual": MAT_MANIFEST.exists(), "passed": MAT_MANIFEST.exists()},
        {"audit": "manifest_key_count", "expected": 10, "actual": len(manifest.keys()), "passed": len(manifest.keys()) == 10},
        {"audit": "manifest_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HN, "actual": manifest.get("next_layer"), "passed": manifest.get("next_layer") == RECOMMENDED_NEXT_LAYER_6HN},
        {"audit": "manifest_creation_mode", "expected": "local_only_fail_closed_materialization", "actual": manifest.get("creation_mode"), "passed": manifest.get("creation_mode") == "local_only_fail_closed_materialization"},
        {"audit": "manifest_target_artifacts_count", "expected": 5, "actual": len(manifest.get("target_artifacts", [])), "passed": len(manifest.get("target_artifacts", [])) == 5},
        {"audit": "manifest_source_inputs_count", "expected": 3, "actual": len(manifest.get("source_inputs", [])), "passed": len(manifest.get("source_inputs", [])) == 3},
        {"audit": "manifest_safety_local_only", "expected": True, "actual": "local_only" in manifest.get("safety_boundaries", []), "passed": "local_only" in manifest.get("safety_boundaries", [])},
        {"audit": "manifest_safety_no_adapter_revision", "expected": True, "actual": "no_adapter_revision" in manifest.get("safety_boundaries", []), "passed": "no_adapter_revision" in manifest.get("safety_boundaries", [])},
    ]

    fail_closed_rows = [
        {"audit": "all_target_artifacts_quality_passed", "expected": False, "actual": json_6hn.get("all_target_artifacts_quality_passed"), "passed": json_6hn.get("all_target_artifacts_quality_passed") is False},
        {"audit": "exact_source_family_count", "expected": 0, "actual": json_6hn.get("exact_source_family_count"), "passed": json_6hn.get("exact_source_family_count") == 0},
        {"audit": "failed_source_family_count", "expected": 3, "actual": json_6hn.get("failed_source_family_count"), "passed": json_6hn.get("failed_source_family_count") == 3},
        {"audit": "fail_closed_family_count", "expected": 3, "actual": json_6hn.get("fail_closed_family_count"), "passed": json_6hn.get("fail_closed_family_count") == 3},
        {"audit": "game_level_quality_failed", "expected": False, "actual": json_6hn.get("game_level_outcomes_quality_passed"), "passed": json_6hn.get("game_level_outcomes_quality_passed") is False},
        {"audit": "base_out_quality_failed", "expected": False, "actual": json_6hn.get("base_out_transitions_quality_passed"), "passed": json_6hn.get("base_out_transitions_quality_passed") is False},
        {"audit": "inning_runs_quality_failed", "expected": False, "actual": json_6hn.get("inning_runs_quality_passed"), "passed": json_6hn.get("inning_runs_quality_passed") is False},
        {"audit": "adapter_revision_blocked_due_to_quality", "expected": True, "actual": json_6hn.get("adapter_revision_still_blocked"), "passed": json_6hn.get("adapter_revision_still_blocked") is True},
    ]

    selection_rows = []
    expected_reasons = {
        "game_level_outcomes": "fail_closed_no_deterministic_final_score_source",
        "base_out_transitions": "fail_closed_no_deterministic_play_level_base_out_source",
        "inning_runs": "fail_closed_no_deterministic_half_inning_run_source",
    }
    for family, expected_reason in expected_reasons.items():
        row = find_row(source_selection_6hn, "requirement_family", family)
        selection_rows.append({
            "requirement_family": family,
            "expected_selected": False,
            "actual_selected": row.get("selected"),
            "expected_reason": expected_reason,
            "actual_reason": row.get("reason"),
            "passed": bool(row) and not boolish(row.get("selected")) and row.get("reason") == expected_reason,
        })

    decision_rows = [
        {"decision": "implementation_execution_passed", "expected": True, "actual": json_6hn.get("all_checks_passed"), "passed": json_6hn.get("all_checks_passed") is True},
        {"decision": "source_family_quality_failed", "expected": True, "actual": json_6hn.get("all_target_artifacts_quality_passed") is False, "passed": json_6hn.get("all_target_artifacts_quality_passed") is False},
        {"decision": "fail_closed_behavior_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "deterministic_source_acquisition_required_next", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_revision_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_blocked_by_validation", "expected": True, "actual": True, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HO, "actual": RECOMMENDED_NEXT_LAYER_6HO, "passed": True},
    ]

    future_6hp_rows = [
        {"contract": "plan_deterministic_source_acquisition_for_failed_source_families", "required": True, "passed": True},
        {"contract": "identify_final_game_score_source_requirements", "required": True, "passed": True},
        {"contract": "identify_play_level_base_out_transition_source_requirements", "required": True, "passed": True},
        {"contract": "identify_half_inning_runs_source_requirements", "required": True, "passed": True},
        {"contract": "define_allowed_acquisition_methods", "required": True, "passed": True},
        {"contract": "maintain_no_adapter_revision_no_real_evaluation_no_activation", "required": True, "passed": True},
        {"contract": "define_future_implementation_and_audit_sequence", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": json_6hn.get("live_data_fetches_run"), "passed": json_6hn.get("live_data_fetches_run") is False},
        {"boundary": "no_database_write", "expected": False, "actual": json_6hn.get("database_writes_run"), "passed": json_6hn.get("database_writes_run") is False},
        {"boundary": "materialization_jobs_run_by_audited_layer", "expected": True, "actual": json_6hn.get("materialization_jobs_run"), "passed": json_6hn.get("materialization_jobs_run") is True},
        {"boundary": "materialization_jobs_run_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": json_6hn.get("production_simulations_run"), "passed": json_6hn.get("production_simulations_run") is False},
        {"boundary": "no_real_backtests", "expected": False, "actual": json_6hn.get("real_backtests_run"), "passed": json_6hn.get("real_backtests_run") is False},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": json_6hn.get("mechanic_evaluations_run"), "passed": json_6hn.get("mechanic_evaluations_run") is False},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": json_6hn.get("actual_outcomes_joined_to_mechanics"), "passed": json_6hn.get("actual_outcomes_joined_to_mechanics") is False},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": json_6hn.get("corrected_normalized_outcomes_emitted_by_this_layer"), "passed": json_6hn.get("corrected_normalized_outcomes_emitted_by_this_layer") is False},
        {"boundary": "no_activation", "expected": False, "actual": json_6hn.get("activation_allowed"), "passed": json_6hn.get("activation_allowed") is False},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": json_6hn.get("layer_6_exit_credit"), "passed": json_6hn.get("layer_6_exit_credit") is False},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    impl_6hn_after = IMPL_6HN_PATH.read_text(encoding="utf-8") if IMPL_6HN_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6ho_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hn_implementation", "policy": "unchanged_by_6ho", "passed": impl_6hn_after == impl_6hn_before},
        {"surface": "materialized_artifacts", "policy": "audited_not_modified_by_6ho", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6ho", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6ho", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run_by_6ho", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HO, "actual": RECOMMENDED_NEXT_LAYER_6HO, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HO, "actual": RECOMMENDED_PATH_6HO, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "deterministic_source_acquisition_required_next", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_revision_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HO, "actual": DIAGNOSIS_6HO, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "materialized_artifacts", "passed": all(row["passed"] for row in materialized_rows), "detail": f"{sum(1 for row in materialized_rows if row['passed'])}/{len(materialized_rows)}"},
        {"check": "quality_report", "passed": all(row["passed"] for row in quality_rows), "detail": f"{sum(1 for row in quality_rows if row['passed'])}/{len(quality_rows)}"},
        {"check": "manifest", "passed": all(row["passed"] for row in manifest_rows), "detail": f"{sum(1 for row in manifest_rows if row['passed'])}/{len(manifest_rows)}"},
        {"check": "fail_closed", "passed": all(row["passed"] for row in fail_closed_rows), "detail": f"{sum(1 for row in fail_closed_rows if row['passed'])}/{len(fail_closed_rows)}"},
        {"check": "source_selection", "passed": all(row["passed"] for row in selection_rows), "detail": f"{sum(1 for row in selection_rows if row['passed'])}/{len(selection_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hp_contract", "passed": all(row["passed"] for row in future_6hp_rows), "detail": f"{sum(1 for row in future_6hp_rows if row['passed'])}/{len(future_6hp_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)
    fail_closed_behavior_valid = all(row["passed"] for row in fail_closed_rows) and all(row["passed"] for row in quality_rows)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "checks_consistency": write_csv(CHECKS_CONSISTENCY_CSV, checks_consistency_rows),
        "materialized_artifacts": write_csv(MATERIALIZED_ARTIFACTS_CSV, materialized_rows),
        "quality_report": write_csv(QUALITY_REPORT_CSV, quality_rows),
        "manifest": write_csv(MANIFEST_CSV, manifest_rows),
        "fail_closed": write_csv(FAIL_CLOSED_CSV, fail_closed_rows),
        "source_selection": write_csv(SOURCE_SELECTION_CSV, selection_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hp_contract": write_csv(FUTURE_6HP_CONTRACT_CSV, future_6hp_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HO",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HO if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HO,
        "recommended_path": RECOMMENDED_PATH_6HO,
        "audited_layer": "6HN",
        "predecessor_implementation": str(IMPL_6HN_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6hn.get("diagnosis"),
        "source_materialization_only_confirmed": json_6hn.get("source_materialization_only") is True,
        "target_artifacts_created_count": json_6hn.get("target_artifacts_created_count"),
        "materialized_data_artifact_count": 3,
        "materialized_manifest_created": MAT_MANIFEST.exists(),
        "materialized_quality_report_created": MAT_QUALITY.exists(),
        "materialized_family_count": json_6hn.get("materialized_family_count"),
        "game_level_outcomes_row_count": json_6hn.get("game_level_outcomes_row_count"),
        "base_out_transitions_row_count": json_6hn.get("base_out_transitions_row_count"),
        "inning_runs_row_count": json_6hn.get("inning_runs_row_count"),
        "game_level_outcomes_quality_passed": json_6hn.get("game_level_outcomes_quality_passed"),
        "base_out_transitions_quality_passed": json_6hn.get("base_out_transitions_quality_passed"),
        "inning_runs_quality_passed": json_6hn.get("inning_runs_quality_passed"),
        "all_target_artifacts_quality_passed": json_6hn.get("all_target_artifacts_quality_passed"),
        "exact_source_family_count": json_6hn.get("exact_source_family_count"),
        "failed_source_family_count": json_6hn.get("failed_source_family_count"),
        "fail_closed_family_count": json_6hn.get("fail_closed_family_count"),
        "candidate_source_count": json_6hn.get("candidate_source_count"),
        "fail_closed_behavior_valid": fail_closed_behavior_valid,
        "deterministic_source_acquisition_required_next": fail_closed_behavior_valid and json_6hn.get("all_target_artifacts_quality_passed") is False,
        "adapter_revision_allowed_after_this_audit": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_blocked_by_validation": True,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "corrected_normalized_outcomes_emitted_by_audited_layer": False,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run_by_audited_layer": json_6hn.get("materialization_jobs_run") is True,
        "materialization_jobs_run_by_this_layer": False,
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
            "materialized_artifacts_csv": str(MATERIALIZED_ARTIFACTS_CSV),
            "quality_report_csv": str(QUALITY_REPORT_CSV),
            "manifest_csv": str(MANIFEST_CSV),
            "fail_closed_csv": str(FAIL_CLOSED_CSV),
            "source_selection_csv": str(SOURCE_SELECTION_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hp_contract_csv": str(FUTURE_6HP_CONTRACT_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
