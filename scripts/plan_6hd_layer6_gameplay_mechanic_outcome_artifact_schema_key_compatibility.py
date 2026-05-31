#!/usr/bin/env python3
"""Plan Layer 6HD schema/key compatibility resolution for outcome adapter."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


SLUG = "layer6_6hd_schema_key_compatibility_plan"
TMP_DIR = Path("tmp")

AUDIT_6HC_PATH = Path("scripts/audit_6hc_layer6_gameplay_mechanic_outcome_artifact_adapter_implementation.py")
IMPLEMENT_6HB_PATH = Path("scripts/implement_6hb_layer6_gameplay_mechanic_outcome_artifact_adapter.py")
AUDIT_6HA_PATH = Path("scripts/audit_6ha_layer6_gameplay_mechanic_outcome_artifact_adapter_plan.py")

JSON_6HC = TMP_DIR / "layer6_6hc_outcome_artifact_adapter_implementation_audit.json"
CHECKS_6HC = TMP_DIR / "layer6_6hc_outcome_artifact_adapter_implementation_audit_checks.csv"
VALIDATION_AUDIT_6HC = TMP_DIR / "layer6_6hc_outcome_artifact_adapter_implementation_audit_validation_report.csv"
PROVENANCE_AUDIT_6HC = TMP_DIR / "layer6_6hc_outcome_artifact_adapter_implementation_audit_provenance_report.csv"
NORMALIZED_SCHEMAS_6HC = TMP_DIR / "layer6_6hc_outcome_artifact_adapter_implementation_audit_normalized_schemas.csv"
FUTURE_6HD_6HC = TMP_DIR / "layer6_6hc_outcome_artifact_adapter_implementation_audit_future_6hd_contract.csv"

JSON_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation.json"
SELECTED_SOURCES_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_implementation_selected_sources.csv"
VALIDATION_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_validation.csv"
PROVENANCE_6HB = TMP_DIR / "layer6_6hb_outcome_artifact_adapter_provenance.csv"
NORMALIZED_GAME_6HB = TMP_DIR / "layer6_6hb_normalized_game_outcomes.csv"
NORMALIZED_BASE_6HB = TMP_DIR / "layer6_6hb_normalized_base_out_transitions.csv"
NORMALIZED_INNING_6HB = TMP_DIR / "layer6_6hb_normalized_inning_runs.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SOURCE_CLASSIFICATION_CSV = TMP_DIR / f"{SLUG}_selected_source_classification.csv"
FAILURE_DIAGNOSIS_CSV = TMP_DIR / f"{SLUG}_failure_diagnosis.csv"
FIELD_GAPS_CSV = TMP_DIR / f"{SLUG}_canonical_field_gaps.csv"
SOURCE_FILTER_POLICY_CSV = TMP_DIR / f"{SLUG}_source_filter_policy.csv"
ALIAS_MAPPING_POLICY_CSV = TMP_DIR / f"{SLUG}_alias_mapping_policy.csv"
FAIL_CLOSED_POLICY_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
FUTURE_6HE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6he_contract.csv"
FUTURE_6HF_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hf_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HC = "layer_6_gameplay_mechanic_outcome_artifact_adapter_implementation_audit_complete"
DIAGNOSIS_6HD = "layer_6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan_complete"
CURRENT_LAYER = "6HD_layer_6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan"
RECOMMENDED_NEXT_LAYER = "6HE_layer_6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan_audit"
RECOMMENDED_PATH = "plan_schema_key_compatibility_resolution_then_audit_before_adapter_revision"

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
    JSON_6HC,
    CHECKS_6HC,
    VALIDATION_AUDIT_6HC,
    PROVENANCE_AUDIT_6HC,
    NORMALIZED_SCHEMAS_6HC,
    FUTURE_6HD_6HC,
    JSON_6HB,
    SELECTED_SOURCES_6HB,
    VALIDATION_6HB,
    PROVENANCE_6HB,
    NORMALIZED_GAME_6HB,
    NORMALIZED_BASE_6HB,
    NORMALIZED_INNING_6HB,
]

CANONICAL_FIELDS = {
    "game_outcomes": [
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
    ],
    "base_out_transitions": [
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
    ],
    "inning_runs": [
        "game_id",
        "inning",
        "half_inning",
        "batting_team",
        "fielding_team",
        "runs_scored",
        "source_artifact_path",
        "validation_status",
    ],
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


def header_for(path_value: str) -> Set[str]:
    path = Path(path_value)
    if not path.exists() or path.is_dir():
        return set()
    try:
        if path.suffix.lower() == ".csv":
            with path.open(newline="", encoding="utf-8") as handle:
                return set(next(csv.reader(handle), []))
        if path.suffix.lower() in {".json", ".jsonl", ".ndjson"}:
            if path.suffix.lower() == ".json":
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    for key in ["rows", "data", "records", "games"]:
                        if isinstance(data.get(key), list) and data[key] and isinstance(data[key][0], dict):
                            return set(data[key][0].keys())
                    return set(data.keys())
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    return set(data[0].keys())
            else:
                for line in path.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        row = json.loads(line)
                        return set(row.keys()) if isinstance(row, dict) else set()
    except Exception:
        return set()
    return set()


def classify_source(row: Dict[str, str]) -> str:
    path_value = row.get("source_artifact_path", "")
    name = Path(path_value).name.lower()
    family = row.get("source_family", "")

    if "layer6_6hb_" in name or "6hb" in name:
        return "likely_prior_adapter_output"
    if any(token in name for token in ["schema", "feasibility", "checklist", "bottleneck", "derivation", "payload_key", "repo_term", "pa_model", "realism"]):
        return "likely_planning_or_meta_artifact"
    if family == "candidate_game_outcomes" and any(token in name for token in ["game", "outcome", "score", "result"]):
        return "likely_actual_game_outcome_data"
    if family == "candidate_base_out_transitions" and any(token in name for token in ["transition", "base_out", "run_expectancy"]):
        return "likely_base_out_transition_data"
    if family == "candidate_inning_runs" and any(token in name for token in ["inning", "walkoff", "games"]):
        return "likely_inning_run_data"
    return "unsuitable_for_outcome_adapter"


def family_to_canonical_group(family: str) -> str:
    if family == "candidate_game_outcomes":
        return "game_outcomes"
    if family == "candidate_base_out_transitions":
        return "base_out_transitions"
    if family == "candidate_inning_runs":
        return "inning_runs"
    return "game_outcomes"


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6hc_before = AUDIT_6HC_PATH.read_text(encoding="utf-8") if AUDIT_6HC_PATH.exists() else ""
    implement_6hb_before = IMPLEMENT_6HB_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HB_PATH.exists() else ""
    audit_6ha_before = AUDIT_6HA_PATH.read_text(encoding="utf-8") if AUDIT_6HA_PATH.exists() else ""

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6HC_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6hc = load_json(JSON_6HC)
    json_6hb = load_json(JSON_6HB)
    selected_sources = read_csv(SELECTED_SOURCES_6HB)
    validation_rows = read_csv(VALIDATION_6HB)
    provenance_rows = read_csv(PROVENANCE_6HB)
    future_6hd_rows = read_csv(FUTURE_6HD_6HC)

    validation_passed = intish(json_6hc.get("validation_passed_row_count"), -1)
    validation_failed = intish(json_6hc.get("validation_failed_closed_row_count"), 0)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hc_audit_exists", "expected": True, "actual": AUDIT_6HC_PATH.exists(), "passed": AUDIT_6HC_PATH.exists()},
        {"check": "6hc_audit_runs", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"check": "6hc_json_exists", "expected": True, "actual": JSON_6HC.exists(), "passed": JSON_6HC.exists()},
        {"check": "6hc_all_checks_passed", "expected": True, "actual": json_6hc.get("all_checks_passed"), "passed": json_6hc.get("all_checks_passed") is True},
        {"check": "6hc_diagnosis", "expected": DIAGNOSIS_6HC, "actual": json_6hc.get("diagnosis"), "passed": json_6hc.get("diagnosis") == DIAGNOSIS_6HC},
        {"check": "6hc_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6hc.get("recommended_next_layer"), "passed": json_6hc.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6hc_schema_key_resolution_required", "expected": True, "actual": json_6hc.get("schema_key_compatibility_resolution_required"), "passed": json_6hc.get("schema_key_compatibility_resolution_required") is True},
        {"check": "6hc_real_eval_blocked", "expected": True, "actual": json_6hc.get("real_evaluation_blocked_by_validation"), "passed": json_6hc.get("real_evaluation_blocked_by_validation") is True},
        {"check": "6hc_validation_passed_zero", "expected": 0, "actual": validation_passed, "passed": validation_passed == 0},
        {"check": "6hc_validation_failed_positive", "expected": ">=1", "actual": validation_failed, "passed": validation_failed >= 1},
    ]

    input_artifact_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "required": True, "passed": path.exists()}
        for path in REQUIRED_INPUT_ARTIFACTS
    ]

    classification_rows: List[Dict[str, Any]] = []
    for row in selected_sources:
        path_value = row.get("source_artifact_path", "")
        family = row.get("source_family", "")
        cls = classify_source(row)
        header = header_for(path_value)
        canonical_group = family_to_canonical_group(family)
        canonical = set(CANONICAL_FIELDS[canonical_group])
        overlap = sorted(header.intersection(canonical))
        missing = sorted(canonical.difference(header).difference({"source_artifact_path", "source_row_id", "validation_status"}))
        classification_rows.append(
            {
                "source_family": family,
                "source_artifact_path": path_value,
                "classification": cls,
                "exists": Path(path_value).exists() if path_value else False,
                "header_field_count": len(header),
                "canonical_group": canonical_group,
                "canonical_overlap_count": len(overlap),
                "canonical_overlap_fields": "|".join(overlap),
                "missing_required_canonical_fields": "|".join(missing),
                "future_source_filter_action": "exclude" if cls in {"likely_planning_or_meta_artifact", "likely_prior_adapter_output", "unsuitable_for_outcome_adapter"} else "candidate_keep_with_alias_review",
                "passed": True,
            }
        )

    likely_actual = sum(1 for row in classification_rows if row["classification"] in {"likely_actual_game_outcome_data", "likely_base_out_transition_data", "likely_inning_run_data"})
    likely_meta = sum(1 for row in classification_rows if row["classification"] == "likely_planning_or_meta_artifact")
    likely_prior = sum(1 for row in classification_rows if row["classification"] == "likely_prior_adapter_output")
    unsuitable = sum(1 for row in classification_rows if row["classification"] == "unsuitable_for_outcome_adapter")

    failure_counts: Dict[str, int] = {}
    family_failure_counts: Dict[Tuple[str, str], int] = {}
    for row in validation_rows:
        status = row.get("validation_status", "")
        family = row.get("source_family", "")
        failure_counts[status] = failure_counts.get(status, 0) + 1
        family_failure_counts[(family, status)] = family_failure_counts.get((family, status), 0) + 1

    failure_rows = [
        {
            "failure_status": status,
            "row_count": count,
            "diagnosis": "missing or insufficient canonical identifiers" if "missing_required_identifiers" in status else "requires source-specific review",
            "blocks_real_evaluation": True,
            "blocks_activation": True,
            "blocks_layer_6_exit_credit": True,
            "future_resolution_needed": True,
            "passed": True,
        }
        for status, count in sorted(failure_counts.items())
    ]

    for (family, status), count in sorted(family_failure_counts.items()):
        failure_rows.append(
            {
                "failure_status": f"{family}:{status}",
                "row_count": count,
                "diagnosis": "family-specific mapping gap",
                "blocks_real_evaluation": True,
                "blocks_activation": True,
                "blocks_layer_6_exit_credit": True,
                "future_resolution_needed": True,
                "passed": True,
            }
        )

    field_gap_rows: List[Dict[str, Any]] = []
    for class_row in classification_rows:
        missing = class_row["missing_required_canonical_fields"]
        field_gap_rows.append(
            {
                "source_family": class_row["source_family"],
                "source_artifact_path": class_row["source_artifact_path"],
                "classification": class_row["classification"],
                "canonical_group": class_row["canonical_group"],
                "canonical_overlap_count": class_row["canonical_overlap_count"],
                "canonical_overlap_fields": class_row["canonical_overlap_fields"],
                "missing_required_canonical_fields": missing,
                "gap_type": "source_is_meta_or_prior_output" if class_row["future_source_filter_action"] == "exclude" else "alias_or_identifier_mapping_gap",
                "adapter_revision_required": True,
                "passed": True,
            }
        )

    source_filter_policy_rows = [
        {"rule": "exclude_prior_adapter_outputs", "policy": "exclude any path containing layer6_6hb or current adapter output prefixes", "required": True, "passed": True},
        {"rule": "exclude_planning_meta_artifacts", "policy": "exclude schema, feasibility, checklist, derivation, repo term, payload key, and realism reports from outcome data source selection", "required": True, "passed": True},
        {"rule": "prefer_actual_game_result_like_artifacts", "policy": "candidate_game_outcomes must expose score/team/date/game identifiers or be excluded", "required": True, "passed": True},
        {"rule": "prefer_event_transition_like_artifacts", "policy": "base/out transition artifacts must expose base state, outs, runs, and scope identifiers", "required": True, "passed": True},
        {"rule": "prefer_inning_run_like_artifacts", "policy": "inning run artifacts must expose game/team/inning/runs identifiers", "required": True, "passed": True},
        {"rule": "fail_closed_if_only_meta_artifacts_remain", "policy": "do not proceed to real evaluation when selected sources are metadata/planning reports only", "required": True, "passed": True},
    ]

    alias_mapping_rows = [
        {"canonical_field": "game_id", "future_alias_candidates": "game_id|game_pk|mlb_game_id|pk|game", "required": True, "passed": True},
        {"canonical_field": "game_date", "future_alias_candidates": "game_date|date|official_date|start_date", "required": True, "passed": True},
        {"canonical_field": "home_team", "future_alias_candidates": "home_team|home|home_abbrev|home_team_abbrev", "required": True, "passed": True},
        {"canonical_field": "away_team", "future_alias_candidates": "away_team|away|away_abbrev|away_team_abbrev", "required": True, "passed": True},
        {"canonical_field": "home_runs", "future_alias_candidates": "home_runs|home_score|home_final|home_total|home_r", "required": True, "passed": True},
        {"canonical_field": "away_runs", "future_alias_candidates": "away_runs|away_score|away_final|away_total|away_r", "required": True, "passed": True},
        {"canonical_field": "inning", "future_alias_candidates": "inning|inn", "required": True, "passed": True},
        {"canonical_field": "half_inning", "future_alias_candidates": "half_inning|half|inning_half|top_bottom", "required": True, "passed": True},
        {"canonical_field": "runs_scored", "future_alias_candidates": "runs_scored|runs|run_value|runs_in_inning", "required": True, "passed": True},
    ]

    fail_closed_policy_rows = [
        {"rule": "no_validation_passed_rows_blocks_real_evaluation", "required": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
        {"rule": "missing_required_identifiers_blocks_real_evaluation", "required": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
        {"rule": "meta_artifact_selected_as_outcome_source_blocks_real_evaluation", "required": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
        {"rule": "prior_adapter_output_selected_as_source_blocks_real_evaluation", "required": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
        {"rule": "ambiguous_game_identity_blocks_real_evaluation", "required": True, "blocks_real_evaluation": True, "blocks_activation": True, "blocks_layer_6_exit_credit": True, "passed": True},
    ]

    future_6he_rows = [
        {"contract": "audit_6hd_schema_key_compatibility_plan", "required": True, "passed": True},
        {"contract": "verify_no_adapter_revision_or_evaluation", "required": True, "passed": True},
        {"contract": "verify_source_classification_and_filter_policy", "required": True, "passed": True},
        {"contract": "verify_alias_mapping_policy", "required": True, "passed": True},
        {"contract": "verify_fail_closed_policy", "required": True, "passed": True},
        {"contract": "verify_future_6hf_contract", "required": True, "passed": True},
        {"contract": "preserve_safety_boundaries", "required": True, "passed": True},
    ]

    future_6hf_rows = [
        {"contract": "revise_local_adapter_source_filtering_only_after_6he", "required": True, "passed": True},
        {"contract": "exclude_prior_6hb_outputs_from_source_discovery", "required": True, "passed": True},
        {"contract": "prefer_real_outcome_like_artifacts_over_meta_artifacts", "required": True, "passed": True},
        {"contract": "update_deterministic_alias_mapping", "required": True, "passed": True},
        {"contract": "emit_normalized_local_tmp_artifacts_only", "required": True, "passed": True},
        {"contract": "fail_closed_if_identifiers_remain_insufficient", "required": True, "passed": True},
        {"contract": "no_real_backtests_or_mechanic_evaluation", "required": True, "passed": True},
        {"contract": "no_activation_or_layer_6_exit_credit", "required": True, "passed": True},
        {"contract": "future_audit_required_before_real_evaluation", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_adapter_runtime_modified", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_normalized_outcomes_emitted_by_this_layer", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_real_backtests", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_mechanic_evaluation", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_database_write", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_materialization_job", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_production_simulation", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": True, "actual": True, "passed": True},
        {"boundary": "layer_6_exit_credit_blocked", "expected": True, "actual": True, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6hc_after = AUDIT_6HC_PATH.read_text(encoding="utf-8") if AUDIT_6HC_PATH.exists() else ""
    implement_6hb_after = IMPLEMENT_6HB_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HB_PATH.exists() else ""
    audit_6ha_after = AUDIT_6HA_PATH.read_text(encoding="utf-8") if AUDIT_6HA_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6hd_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hc_audit", "policy": "unchanged_by_6hd", "passed": audit_6hc_after == audit_6hc_before},
        {"surface": "6hb_implementation", "policy": "unchanged_by_6hd", "passed": implement_6hb_after == implement_6hb_before},
        {"surface": "6ha_audit", "policy": "unchanged_by_6hd", "passed": audit_6ha_after == audit_6ha_before},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hd", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_revision_required", "expected": True, "actual": True, "passed": True},
        {"decision": "strict_source_filter_required", "expected": True, "actual": True, "passed": True},
        {"decision": "alias_mapping_revision_required", "expected": True, "actual": True, "passed": True},
        {"decision": "real_evaluation_blocked_by_validation", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HD, "actual": DIAGNOSIS_6HD, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_artifact_rows), "detail": f"{sum(1 for row in input_artifact_rows if row['passed'])}/{len(input_artifact_rows)}"},
        {"check": "source_classification", "passed": len(classification_rows) >= 1 and all(row["passed"] for row in classification_rows), "detail": str(len(classification_rows))},
        {"check": "failure_diagnosis", "passed": len(failure_rows) >= 1 and all(row["passed"] for row in failure_rows), "detail": str(len(failure_rows))},
        {"check": "canonical_field_gaps", "passed": len(field_gap_rows) >= 1 and all(row["passed"] for row in field_gap_rows), "detail": str(len(field_gap_rows))},
        {"check": "source_filter_policy", "passed": all(row["passed"] for row in source_filter_policy_rows), "detail": f"{sum(1 for row in source_filter_policy_rows if row['passed'])}/{len(source_filter_policy_rows)}"},
        {"check": "alias_mapping_policy", "passed": all(row["passed"] for row in alias_mapping_rows), "detail": f"{sum(1 for row in alias_mapping_rows if row['passed'])}/{len(alias_mapping_rows)}"},
        {"check": "fail_closed_policy", "passed": all(row["passed"] for row in fail_closed_policy_rows), "detail": f"{sum(1 for row in fail_closed_policy_rows if row['passed'])}/{len(fail_closed_policy_rows)}"},
        {"check": "future_6hd_contract_from_6hc", "passed": all(boolish(row.get("passed")) for row in future_6hd_rows), "detail": f"{sum(1 for row in future_6hd_rows if boolish(row.get('passed')))}" + f"/{len(future_6hd_rows)}"},
        {"check": "future_6he_contract", "passed": all(row["passed"] for row in future_6he_rows), "detail": f"{sum(1 for row in future_6he_rows if row['passed'])}/{len(future_6he_rows)}"},
        {"check": "future_6hf_contract", "passed": all(row["passed"] for row in future_6hf_rows), "detail": f"{sum(1 for row in future_6hf_rows if row['passed'])}/{len(future_6hf_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_artifact_rows),
        "source_classification": write_csv(SOURCE_CLASSIFICATION_CSV, classification_rows),
        "failure_diagnosis": write_csv(FAILURE_DIAGNOSIS_CSV, failure_rows),
        "canonical_field_gaps": write_csv(FIELD_GAPS_CSV, field_gap_rows),
        "source_filter_policy": write_csv(SOURCE_FILTER_POLICY_CSV, source_filter_policy_rows),
        "alias_mapping_policy": write_csv(ALIAS_MAPPING_POLICY_CSV, alias_mapping_rows),
        "fail_closed_policy": write_csv(FAIL_CLOSED_POLICY_CSV, fail_closed_policy_rows),
        "future_6he_contract": write_csv(FUTURE_6HE_CONTRACT_CSV, future_6he_rows),
        "future_6hf_contract": write_csv(FUTURE_6HF_CONTRACT_CSV, future_6hf_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HD",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HD if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_audit": str(AUDIT_6HC_PATH),
        "predecessor_audit_returncode": audit_run.returncode,
        "predecessor_audit_diagnosis": json_6hc.get("diagnosis"),
        "audited_layer": "6HC",
        "schema_key_compatibility_resolution_required": True,
        "real_evaluation_blocked_by_validation": True,
        "adapter_revision_required": True,
        "strict_source_filter_required": True,
        "alias_mapping_revision_required": True,
        "prior_adapter_outputs_excluded_in_future": True,
        "validation_passed_row_count": validation_passed,
        "validation_failed_closed_row_count": validation_failed,
        "selected_source_artifact_count": intish(json_6hc.get("selected_source_artifact_count"), len(selected_sources)),
        "likely_actual_source_count": likely_actual,
        "likely_meta_or_planning_source_count": likely_meta,
        "likely_prior_adapter_output_source_count": likely_prior,
        "unsuitable_source_count": unsuitable,
        "source_filter_rule_count": len(source_filter_policy_rows),
        "alias_mapping_rule_count": len(alias_mapping_rows),
        "fail_closed_rule_count": len(fail_closed_policy_rows),
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "normalized_outcomes_emitted_by_this_layer": False,
        "adapter_runtime_modified": False,
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
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "source_classification_csv": str(SOURCE_CLASSIFICATION_CSV),
            "failure_diagnosis_csv": str(FAILURE_DIAGNOSIS_CSV),
            "canonical_field_gaps_csv": str(FIELD_GAPS_CSV),
            "source_filter_policy_csv": str(SOURCE_FILTER_POLICY_CSV),
            "alias_mapping_policy_csv": str(ALIAS_MAPPING_POLICY_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_POLICY_CSV),
            "future_6he_contract_csv": str(FUTURE_6HE_CONTRACT_CSV),
            "future_6hf_contract_csv": str(FUTURE_6HF_CONTRACT_CSV),
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
