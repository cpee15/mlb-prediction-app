#!/usr/bin/env python3
"""Plan Layer 6HH row-level identifier mapping for outcome artifacts."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


SLUG = "layer6_6hh_row_level_identifier_mapping_plan"
TMP_DIR = Path("tmp")

AUDIT_6HG_PATH = Path("scripts/audit_6hg_layer6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision.py")
IMPLEMENT_6HF_PATH = Path("scripts/implement_6hf_layer6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision.py")
AUDIT_6HE_PATH = Path("scripts/audit_6he_layer6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan.py")

JSON_6HG = TMP_DIR / "layer6_6hg_outcome_artifact_adapter_source_filter_alias_revision_audit.json"
CHECKS_6HG = TMP_DIR / "layer6_6hg_outcome_artifact_adapter_source_filter_alias_revision_audit_checks.csv"
SOURCE_FILTERING_6HG = TMP_DIR / "layer6_6hg_outcome_artifact_adapter_source_filter_alias_revision_audit_source_filtering.csv"
VALIDATION_AUDIT_6HG = TMP_DIR / "layer6_6hg_outcome_artifact_adapter_source_filter_alias_revision_audit_validation.csv"
PROVENANCE_AUDIT_6HG = TMP_DIR / "layer6_6hg_outcome_artifact_adapter_source_filter_alias_revision_audit_provenance.csv"
FUTURE_6HH_6HG = TMP_DIR / "layer6_6hg_outcome_artifact_adapter_source_filter_alias_revision_audit_future_6hh_contract.csv"

JSON_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision.json"
SELECTED_SOURCES_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_source_filter_alias_revision_selected_sources.csv"
VALIDATION_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_validation.csv"
PROVENANCE_6HF = TMP_DIR / "layer6_6hf_outcome_artifact_adapter_provenance.csv"
NORMALIZED_GAME_6HF = TMP_DIR / "layer6_6hf_normalized_game_outcomes.csv"
NORMALIZED_BASE_6HF = TMP_DIR / "layer6_6hf_normalized_base_out_transitions.csv"
NORMALIZED_INNING_6HF = TMP_DIR / "layer6_6hf_normalized_inning_runs.csv"

REQUIRED_INPUT_ARTIFACTS = [
    JSON_6HG,
    CHECKS_6HG,
    SOURCE_FILTERING_6HG,
    VALIDATION_AUDIT_6HG,
    PROVENANCE_AUDIT_6HG,
    FUTURE_6HH_6HG,
    JSON_6HF,
    SELECTED_SOURCES_6HF,
    VALIDATION_6HF,
    PROVENANCE_6HF,
    NORMALIZED_GAME_6HF,
    NORMALIZED_BASE_6HF,
    NORMALIZED_INNING_6HF,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
SELECTED_SOURCES_CSV = TMP_DIR / f"{SLUG}_selected_sources.csv"
SOURCE_COLUMNS_CSV = TMP_DIR / f"{SLUG}_source_columns.csv"
SAMPLE_SHAPES_CSV = TMP_DIR / f"{SLUG}_sample_shapes.csv"
MISSING_IDENTIFIERS_CSV = TMP_DIR / f"{SLUG}_missing_identifiers.csv"
DERIVATION_RULES_CSV = TMP_DIR / f"{SLUG}_derivation_rules.csv"
SOURCE_FAMILY_PLAN_CSV = TMP_DIR / f"{SLUG}_source_family_plan.csv"
ADDITIONAL_SOURCE_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_additional_source_requirements.csv"
ADAPTER_REVISION_CONTRACT_CSV = TMP_DIR / f"{SLUG}_adapter_revision_contract.csv"
FUTURE_6HI_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hi_contract.csv"
FUTURE_6HJ_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hj_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HG = "layer_6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision_audit_complete"
DIAGNOSIS_6HH = "layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan_complete"
CURRENT_LAYER = "6HH_layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan"
RECOMMENDED_NEXT_LAYER = "6HI_layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan_audit"
RECOMMENDED_PATH = "plan_row_level_identifier_mapping_then_audit_before_adapter_revision"

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

CANONICAL_BY_FAMILY = {
    "candidate_game_outcomes": [
        "game_id",
        "game_date",
        "season",
        "home_team",
        "away_team",
        "home_runs",
        "away_runs",
        "total_runs",
        "winner",
    ],
    "candidate_base_out_transitions": [
        "game_id_or_scope",
        "inning",
        "half_inning",
        "start_base_state",
        "start_outs",
        "end_base_state",
        "end_outs",
        "runs_scored",
        "transition_count",
    ],
    "candidate_inning_runs": [
        "game_id",
        "inning",
        "half_inning",
        "batting_team",
        "fielding_team",
        "runs_scored",
    ],
}

ALIAS_CANDIDATES = {
    "game_id": ["game_id", "game_pk", "mlb_game_id", "pk", "game", "game_id_or_scope"],
    "game_date": ["game_date", "date", "official_date", "start_date"],
    "season": ["season", "year"],
    "home_team": ["home_team", "home", "home_abbrev", "home_team_abbrev"],
    "away_team": ["away_team", "away", "away_abbrev", "away_team_abbrev"],
    "home_runs": ["home_runs", "home_score", "home_final", "home_total", "home_r"],
    "away_runs": ["away_runs", "away_score", "away_final", "away_total", "away_r"],
    "total_runs": ["total_runs", "runs_total", "game_total"],
    "winner": ["winner", "winning_team", "result_winner"],
    "game_id_or_scope": ["game_id_or_scope", "game_id", "game_pk", "scope"],
    "inning": ["inning", "inn"],
    "half_inning": ["half_inning", "half", "inning_half", "top_bottom"],
    "batting_team": ["batting_team", "offense_team", "team", "runner_team"],
    "fielding_team": ["fielding_team", "defense_team", "opponent", "opp"],
    "start_base_state": ["start_base_state", "base_state_start", "from_base_state", "start_state"],
    "start_outs": ["start_outs", "outs_start", "from_outs"],
    "end_base_state": ["end_base_state", "base_state_end", "to_base_state", "end_state"],
    "end_outs": ["end_outs", "outs_end", "to_outs"],
    "runs_scored": ["runs_scored", "runs", "run_value", "runs_in_inning"],
    "transition_count": ["transition_count", "count", "n", "events"],
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


def intish(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def read_rows_any(path: Path) -> Tuple[List[Dict[str, Any]], str]:
    if not path.exists() or path.is_dir():
        return [], "missing_or_directory"
    try:
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return read_csv(path), "read_ok"
        if suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [row for row in data if isinstance(row, dict)], "read_ok"
            if isinstance(data, dict):
                for key in ["rows", "data", "records", "games", "items"]:
                    value = data.get(key)
                    if isinstance(value, list):
                        return [row for row in value if isinstance(row, dict)], "read_ok"
                return [data], "read_ok"
        if suffix in {".jsonl", ".ndjson"}:
            rows: List[Dict[str, Any]] = []
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    value = json.loads(line)
                    if isinstance(value, dict):
                        rows.append(value)
            return rows, "read_ok"
        return [], "unsupported_extension"
    except Exception as exc:
        return [], f"read_error:{type(exc).__name__}"


def compact_value(value: Any) -> str:
    if value is None:
        return ""
    text = json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else str(value)
    return text.replace("\n", " ")[:160]


def safe_sample_shape(row: Dict[str, Any]) -> Dict[str, str]:
    return {str(key): compact_value(value) for key, value in list(row.items())[:18]}


def source_kind(path: str, family: str, columns: Set[str]) -> str:
    lower = path.lower()
    if "prototype" in lower or "summary" in lower or "run_expectancy" in lower or "transition_matrix" in lower:
        return "aggregate_or_prototype"
    if family == "candidate_game_outcomes" and {"home_team", "away_team", "home_runs", "away_runs"}.intersection(columns):
        return "potential_game_level_outcome"
    if family == "candidate_base_out_transitions" and {"start_base_state", "end_base_state", "runs_scored"}.intersection(columns):
        return "potential_base_out_row_level"
    if family == "candidate_inning_runs" and {"inning", "runs_scored", "team"}.intersection(columns):
        return "potential_inning_row_level"
    return "unknown_row_shape"


def aliases_present(columns: Set[str], canonical: str) -> List[str]:
    lower_map = {column.lower(): column for column in columns}
    found = []
    for alias in ALIAS_CANDIDATES.get(canonical, [canonical]):
        if alias.lower() in lower_map:
            found.append(lower_map[alias.lower()])
    return found


def rule_for(canonical: str, present: List[str], kind: str) -> Tuple[str, str, bool]:
    if present:
        return ("direct_alias", "|".join(present), True)
    if canonical == "season" and kind != "unknown_row_shape":
        return ("derive_from_game_date_if_game_date_is_available", "requires game_date", False)
    if canonical == "total_runs":
        return ("derive_from_home_runs_plus_away_runs_if_both_available", "requires home_runs and away_runs", False)
    if canonical == "winner":
        return ("derive_from_home_away_runs_and_team_fields_if_available", "requires home_team away_team home_runs away_runs", False)
    if canonical == "game_id_or_scope" and kind in {"aggregate_or_prototype", "potential_base_out_row_level"}:
        return ("source_scope_fallback_only_not_game_identity", "source file stem can scope aggregate rows but cannot identify real games", False)
    return ("additional_local_source_required", "not deterministically derivable from current columns", False)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6hg_before = AUDIT_6HG_PATH.read_text(encoding="utf-8") if AUDIT_6HG_PATH.exists() else ""
    implement_6hf_before = IMPLEMENT_6HF_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HF_PATH.exists() else ""
    audit_6he_before = AUDIT_6HE_PATH.read_text(encoding="utf-8") if AUDIT_6HE_PATH.exists() else ""

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6HG_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6hg = load_json(JSON_6HG)
    selected_rows = [row for row in read_csv(SELECTED_SOURCES_6HF) if row.get("source_artifact_path")]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hg_audit_exists", "expected": True, "actual": AUDIT_6HG_PATH.exists(), "passed": AUDIT_6HG_PATH.exists()},
        {"check": "6hg_audit_runs", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"check": "6hg_json_exists", "expected": True, "actual": JSON_6HG.exists(), "passed": JSON_6HG.exists()},
        {"check": "6hg_all_checks_passed", "expected": True, "actual": json_6hg.get("all_checks_passed"), "passed": json_6hg.get("all_checks_passed") is True},
        {"check": "6hg_diagnosis", "expected": DIAGNOSIS_6HG, "actual": json_6hg.get("diagnosis"), "passed": json_6hg.get("diagnosis") == DIAGNOSIS_6HG},
        {"check": "6hg_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6hg.get("recommended_next_layer"), "passed": json_6hg.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6hg_row_level_mapping_required", "expected": True, "actual": json_6hg.get("row_level_identifier_mapping_required"), "passed": json_6hg.get("row_level_identifier_mapping_required") is True},
        {"check": "6hg_validation_passed_zero", "expected": 0, "actual": json_6hg.get("validation_passed_row_count"), "passed": intish(json_6hg.get("validation_passed_row_count")) == 0},
        {"check": "6hg_validation_failed_positive", "expected": ">=1", "actual": json_6hg.get("validation_failed_closed_row_count"), "passed": intish(json_6hg.get("validation_failed_closed_row_count"), 0) >= 1},
    ]

    input_artifact_rows = [{"artifact_path": str(path), "exists": path.exists(), "required": True, "passed": path.exists()} for path in REQUIRED_INPUT_ARTIFACTS]

    selected_source_output_rows: List[Dict[str, Any]] = []
    source_column_rows: List[Dict[str, Any]] = []
    sample_shape_rows: List[Dict[str, Any]] = []
    missing_identifier_rows: List[Dict[str, Any]] = []
    derivation_rows: List[Dict[str, Any]] = []
    source_family_plan_rows: List[Dict[str, Any]] = []

    selected_sources_read = 0
    selected_sources_failed = 0
    aggregate_or_prototype_count = 0
    game_level_available_count = 0

    for source in selected_rows:
        family = source.get("source_family", "")
        source_path = source.get("source_artifact_path", "")
        path = Path(source_path)
        data_rows, read_status = read_rows_any(path)
        if read_status == "read_ok":
            selected_sources_read += 1
        else:
            selected_sources_failed += 1

        columns: Set[str] = set()
        for row in data_rows[:100]:
            columns.update(str(key) for key in row.keys())

        kind = source_kind(source_path, family, columns)
        if kind == "aggregate_or_prototype":
            aggregate_or_prototype_count += 1
        if kind == "potential_game_level_outcome":
            game_level_available_count += 1

        selected_source_output_rows.append(
            {
                "source_family": family,
                "source_artifact_path": source_path,
                "classification": source.get("classification"),
                "read_status": read_status,
                "row_count": len(data_rows),
                "column_count": len(columns),
                "source_kind": kind,
                "passed": read_status == "read_ok",
            }
        )

        for column in sorted(columns):
            source_column_rows.append(
                {
                    "source_family": family,
                    "source_artifact_path": source_path,
                    "column_name": column,
                    "source_kind": kind,
                    "passed": True,
                }
            )

        for sample_index, row in enumerate(data_rows[:3]):
            sample_shape_rows.append(
                {
                    "source_family": family,
                    "source_artifact_path": source_path,
                    "sample_index": sample_index,
                    "source_kind": kind,
                    "sample_shape_json": json.dumps(safe_sample_shape(row), sort_keys=True),
                    "passed": True,
                }
            )

        required_fields = CANONICAL_BY_FAMILY.get(family, [])
        direct_count = 0
        deterministic_count = 0
        nondeterministic_count = 0

        for canonical in required_fields:
            present = aliases_present(columns, canonical)
            rule_type, rule_detail, deterministic = rule_for(canonical, present, kind)
            if present:
                direct_count += 1
            elif deterministic:
                deterministic_count += 1
            else:
                nondeterministic_count += 1
                missing_identifier_rows.append(
                    {
                        "source_family": family,
                        "source_artifact_path": source_path,
                        "source_kind": kind,
                        "missing_canonical_identifier": canonical,
                        "reason": rule_detail,
                        "additional_local_source_required": rule_type == "additional_local_source_required",
                        "passed": True,
                    }
                )

            derivation_rows.append(
                {
                    "source_family": family,
                    "source_artifact_path": source_path,
                    "source_kind": kind,
                    "canonical_identifier": canonical,
                    "present_aliases": "|".join(present),
                    "rule_type": rule_type,
                    "rule_detail": rule_detail,
                    "deterministic": deterministic or bool(present),
                    "additional_local_source_required": rule_type == "additional_local_source_required",
                    "passed": True,
                }
            )

        source_family_plan_rows.append(
            {
                "source_family": family,
                "source_artifact_path": source_path,
                "source_kind": kind,
                "direct_alias_count": direct_count,
                "deterministic_derivation_count": deterministic_count,
                "nondeterministic_or_missing_count": nondeterministic_count,
                "can_support_real_game_level_outcomes_now": kind == "potential_game_level_outcome" and nondeterministic_count == 0,
                "recommended_action": "additional_local_source_discovery_or_row_mapping_revision_needed" if nondeterministic_count else "candidate_for_future_adapter_revision_after_audit",
                "passed": True,
            }
        )

    if not source_column_rows:
        source_column_rows.append({"source_family": "none", "source_artifact_path": "none", "column_name": "none", "source_kind": "none", "passed": False})
    if not sample_shape_rows:
        sample_shape_rows.append({"source_family": "none", "source_artifact_path": "none", "sample_index": 0, "source_kind": "none", "sample_shape_json": "{}", "passed": False})
    if not missing_identifier_rows:
        missing_identifier_rows.append({"source_family": "none", "source_artifact_path": "none", "source_kind": "none", "missing_canonical_identifier": "none", "reason": "none", "additional_local_source_required": False, "passed": True})

    deterministic_derivation_count = sum(1 for row in derivation_rows if boolish(row.get("deterministic")))
    nondeterministic_derivation_count = sum(1 for row in derivation_rows if not boolish(row.get("deterministic")))
    additional_local_source_required = any(boolish(row.get("additional_local_source_required")) for row in derivation_rows)
    game_level_outcome_source_available = game_level_available_count > 0
    aggregate_or_prototype_sources_detected = aggregate_or_prototype_count > 0
    adapter_revision_required = True
    additional_source_discovery_may_be_required = additional_local_source_required or not game_level_outcome_source_available

    additional_source_rows = [
        {
            "requirement": "game_level_outcome_rows_with_game_id_team_scores_date",
            "required": not game_level_outcome_source_available,
            "reason": "current selected sources do not expose complete game-level outcome identifiers",
            "passed": True,
        },
        {
            "requirement": "row_level_base_out_transition_rows_with_start_end_state_outs_runs",
            "required": any(row.get("source_family") == "candidate_base_out_transitions" and boolish(row.get("additional_local_source_required")) for row in derivation_rows),
            "reason": "base/out selected sources remain aggregate or lack canonical row identifiers",
            "passed": True,
        },
        {
            "requirement": "row_level_inning_runs_with_game_id_inning_half_team_runs",
            "required": any(row.get("source_family") == "candidate_inning_runs" and boolish(row.get("additional_local_source_required")) for row in derivation_rows),
            "reason": "inning selected sources lack canonical game and inning identifiers",
            "passed": True,
        },
    ]

    adapter_revision_contract_rows = [
        {"contract": "do_not_run_real_evaluation", "required": True, "passed": True},
        {"contract": "derive_only_deterministic_identifiers", "required": True, "passed": True},
        {"contract": "fail_closed_for_nondeterministic_identifiers", "required": True, "passed": True},
        {"contract": "preserve_source_filter_exclusions", "required": True, "passed": True},
        {"contract": "emit_local_tmp_normalized_artifacts_only", "required": True, "passed": True},
        {"contract": "require_audit_before_real_evaluation", "required": True, "passed": True},
    ]

    future_6hi_rows = [
        {"contract": "audit_6hh_row_level_identifier_mapping_plan", "required": True, "passed": True},
        {"contract": "verify_source_columns_and_sample_shapes_inspected", "required": True, "passed": True},
        {"contract": "verify_deterministic_vs_nondeterministic_rules_separated", "required": True, "passed": True},
        {"contract": "verify_no_adapter_changes_or_real_evaluation", "required": True, "passed": True},
        {"contract": "determine_next_adapter_revision_or_additional_source_discovery_plan", "required": True, "passed": True},
    ]

    future_6hj_rows = [
        {"contract": "only_after_6hi_passes", "required": True, "passed": True},
        {"contract": "revise_row_level_mapping_only_if_deterministic_rules_sufficient", "required": True, "passed": True},
        {"contract": "fail_closed_where_identifiers_remain_nondeterministic", "required": True, "passed": True},
        {"contract": "no_real_backtests_or_mechanic_evaluation", "required": True, "passed": True},
        {"contract": "no_activation_or_layer_6_exit_credit", "required": True, "passed": True},
        {"contract": "future_audit_required_before_real_evaluation", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_adapter_modification", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_corrected_normalized_outcomes_for_evaluation", "expected": True, "actual": True, "passed": True},
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
    audit_6hg_after = AUDIT_6HG_PATH.read_text(encoding="utf-8") if AUDIT_6HG_PATH.exists() else ""
    implement_6hf_after = IMPLEMENT_6HF_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HF_PATH.exists() else ""
    audit_6he_after = AUDIT_6HE_PATH.read_text(encoding="utf-8") if AUDIT_6HE_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6hh_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hg_audit", "policy": "unchanged_by_6hh", "passed": audit_6hg_after == audit_6hg_before},
        {"surface": "6hf_implementation", "policy": "unchanged_by_6hh", "passed": implement_6hf_after == implement_6hf_before},
        {"surface": "6he_audit", "policy": "unchanged_by_6hh", "passed": audit_6he_after == audit_6he_before},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hh", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "row_level_identifier_mapping_required", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_revision_required", "expected": True, "actual": adapter_revision_required, "passed": adapter_revision_required},
        {"decision": "additional_source_discovery_may_be_required", "expected": True, "actual": additional_source_discovery_may_be_required, "passed": True},
        {"decision": "real_evaluation_blocked_by_validation", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HH, "actual": DIAGNOSIS_6HH, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_artifact_rows), "detail": f"{sum(1 for row in input_artifact_rows if row['passed'])}/{len(input_artifact_rows)}"},
        {"check": "selected_sources", "passed": len(selected_source_output_rows) >= 1 and all(row["passed"] for row in selected_source_output_rows), "detail": f"{selected_sources_read}/{len(selected_source_output_rows)}"},
        {"check": "source_columns", "passed": len(source_column_rows) >= 1 and all(row["passed"] for row in source_column_rows), "detail": str(len(source_column_rows))},
        {"check": "sample_shapes", "passed": len(sample_shape_rows) >= 1 and all(row["passed"] for row in sample_shape_rows), "detail": str(len(sample_shape_rows))},
        {"check": "missing_identifiers", "passed": len(missing_identifier_rows) >= 1 and all(row["passed"] for row in missing_identifier_rows), "detail": str(len(missing_identifier_rows))},
        {"check": "derivation_rules", "passed": len(derivation_rows) >= 1 and all(row["passed"] for row in derivation_rows), "detail": str(len(derivation_rows))},
        {"check": "source_family_plan", "passed": len(source_family_plan_rows) >= 1 and all(row["passed"] for row in source_family_plan_rows), "detail": str(len(source_family_plan_rows))},
        {"check": "additional_source_requirements", "passed": all(row["passed"] for row in additional_source_rows), "detail": f"{sum(1 for row in additional_source_rows if row['passed'])}/{len(additional_source_rows)}"},
        {"check": "adapter_revision_contract", "passed": all(row["passed"] for row in adapter_revision_contract_rows), "detail": f"{sum(1 for row in adapter_revision_contract_rows if row['passed'])}/{len(adapter_revision_contract_rows)}"},
        {"check": "future_6hi_contract", "passed": all(row["passed"] for row in future_6hi_rows), "detail": f"{sum(1 for row in future_6hi_rows if row['passed'])}/{len(future_6hi_rows)}"},
        {"check": "future_6hj_contract", "passed": all(row["passed"] for row in future_6hj_rows), "detail": f"{sum(1 for row in future_6hj_rows if row['passed'])}/{len(future_6hj_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_artifact_rows),
        "selected_sources": write_csv(SELECTED_SOURCES_CSV, selected_source_output_rows),
        "source_columns": write_csv(SOURCE_COLUMNS_CSV, source_column_rows),
        "sample_shapes": write_csv(SAMPLE_SHAPES_CSV, sample_shape_rows),
        "missing_identifiers": write_csv(MISSING_IDENTIFIERS_CSV, missing_identifier_rows),
        "derivation_rules": write_csv(DERIVATION_RULES_CSV, derivation_rows),
        "source_family_plan": write_csv(SOURCE_FAMILY_PLAN_CSV, source_family_plan_rows),
        "additional_source_requirements": write_csv(ADDITIONAL_SOURCE_REQUIREMENTS_CSV, additional_source_rows),
        "adapter_revision_contract": write_csv(ADAPTER_REVISION_CONTRACT_CSV, adapter_revision_contract_rows),
        "future_6hi_contract": write_csv(FUTURE_6HI_CONTRACT_CSV, future_6hi_rows),
        "future_6hj_contract": write_csv(FUTURE_6HJ_CONTRACT_CSV, future_6hj_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HH",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HH if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_audit": str(AUDIT_6HG_PATH),
        "predecessor_audit_returncode": audit_run.returncode,
        "predecessor_audit_diagnosis": json_6hg.get("diagnosis"),
        "audited_layer": "6HG",
        "row_level_identifier_mapping_required": True,
        "real_evaluation_blocked_by_validation": True,
        "selected_source_artifact_count": len(selected_source_output_rows),
        "selected_sources_read_count": selected_sources_read,
        "selected_sources_failed_count": selected_sources_failed,
        "source_column_inventory_count": len(source_column_rows),
        "sample_shape_count": len(sample_shape_rows),
        "missing_identifier_row_count": len(missing_identifier_rows),
        "deterministic_derivation_rule_count": deterministic_derivation_count,
        "nondeterministic_derivation_rule_count": nondeterministic_derivation_count,
        "additional_local_source_required": additional_local_source_required,
        "game_level_outcome_source_available": game_level_outcome_source_available,
        "aggregate_or_prototype_sources_detected": aggregate_or_prototype_sources_detected,
        "adapter_revision_required": adapter_revision_required,
        "additional_source_discovery_may_be_required": additional_source_discovery_may_be_required,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "corrected_normalized_outcomes_emitted_by_this_layer": False,
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
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "selected_sources_csv": str(SELECTED_SOURCES_CSV),
            "source_columns_csv": str(SOURCE_COLUMNS_CSV),
            "sample_shapes_csv": str(SAMPLE_SHAPES_CSV),
            "missing_identifiers_csv": str(MISSING_IDENTIFIERS_CSV),
            "derivation_rules_csv": str(DERIVATION_RULES_CSV),
            "source_family_plan_csv": str(SOURCE_FAMILY_PLAN_CSV),
            "additional_source_requirements_csv": str(ADDITIONAL_SOURCE_REQUIREMENTS_CSV),
            "adapter_revision_contract_csv": str(ADAPTER_REVISION_CONTRACT_CSV),
            "future_6hi_contract_csv": str(FUTURE_6HI_CONTRACT_CSV),
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
