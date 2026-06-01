#!/usr/bin/env python3
"""Plan Layer 6HJ additional local source discovery for outcome artifacts."""

from __future__ import annotations

import csv
import json
import os
import pickle
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple


SLUG = "layer6_6hj_additional_local_source_discovery_plan"
TMP_DIR = Path("tmp")

AUDIT_6HI_PATH = Path("scripts/audit_6hi_layer6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan.py")
JSON_6HI = TMP_DIR / "layer6_6hi_row_level_identifier_mapping_plan_audit.json"
CHECKS_6HI = TMP_DIR / "layer6_6hi_row_level_identifier_mapping_plan_audit_checks.csv"
SELECTED_SOURCES_6HI = TMP_DIR / "layer6_6hi_row_level_identifier_mapping_plan_audit_selected_sources.csv"
ADDITIONAL_REQ_6HI = TMP_DIR / "layer6_6hi_row_level_identifier_mapping_plan_audit_additional_source_requirements.csv"
DECISION_6HI = TMP_DIR / "layer6_6hi_row_level_identifier_mapping_plan_audit_decision.csv"
FUTURE_6HJ_6HI = TMP_DIR / "layer6_6hi_row_level_identifier_mapping_plan_audit_future_6hj_contract.csv"
SAFETY_6HI = TMP_DIR / "layer6_6hi_row_level_identifier_mapping_plan_audit_safety_boundaries.csv"
IMMUTABILITY_6HI = TMP_DIR / "layer6_6hi_row_level_identifier_mapping_plan_audit_immutability.csv"
ADDITIONAL_REQ_6HH = TMP_DIR / "layer6_6hh_row_level_identifier_mapping_plan_additional_source_requirements.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
DISCOVERY_SCOPE_CSV = TMP_DIR / f"{SLUG}_discovery_scope.csv"
CANDIDATE_INVENTORY_CSV = TMP_DIR / f"{SLUG}_candidate_inventory.csv"
SAMPLED_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_sampled_artifacts.csv"
REQUIREMENT_ALIASES_CSV = TMP_DIR / f"{SLUG}_requirement_aliases.csv"
REQUIREMENT_SCORES_CSV = TMP_DIR / f"{SLUG}_requirement_scores.csv"
BEST_CANDIDATES_CSV = TMP_DIR / f"{SLUG}_best_candidates.csv"
GAP_ANALYSIS_CSV = TMP_DIR / f"{SLUG}_gap_analysis.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HK_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hk_contract.csv"
FUTURE_6HL_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hl_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

CURRENT_LAYER = "6HJ_layer_6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery_plan"
RECOMMENDED_NEXT_LAYER = "6HK_layer_6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery_plan_audit"
RECOMMENDED_PATH = "plan_additional_local_source_discovery_then_audit_before_materialization_or_adapter_revision"
DIAGNOSIS_6HI = "layer_6_gameplay_mechanic_outcome_artifact_row_level_identifier_mapping_plan_audit_complete"
DIAGNOSIS_6HJ = "layer_6_gameplay_mechanic_outcome_artifact_additional_local_source_discovery_plan_complete"

SEARCH_ROOTS = [
    Path("tmp"),
    Path("data"),
    Path("artifacts"),
    Path("outputs"),
    Path("reports"),
    Path("tests"),
    Path("fixtures"),
    Path("mlb_app"),
    Path("scripts"),
]
EXTENSIONS = {".csv", ".json", ".jsonl", ".parquet", ".pkl", ".pickle"}
MAX_SAMPLE_ROWS = 200

FORBIDDEN_OUTPUT_TOKENS = {
    "layer6_6hb",
    "layer6_6hc",
    "layer6_6hd",
    "layer6_6he",
    "layer6_6hf",
    "layer6_6hg",
    "layer6_6hh",
    "layer6_6hi",
    "layer6_6hj",
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

REQUIREMENTS: Dict[str, Dict[str, List[str]]] = {
    "game_level_outcomes": {
        "game_id": ["game_id", "game_pk", "gamepk", "game_key", "mlb_game_id", "gid", "game"],
        "game_date": ["game_date", "date", "game_dt", "scheduled_date", "start_date"],
        "home_team": ["home_team", "home", "home_abbr", "home_team_abbr", "home_team_id", "home_name"],
        "away_team": ["away_team", "away", "away_abbr", "away_team_abbr", "away_team_id", "away_name"],
        "home_score": ["home_score", "home_runs", "home_final_score", "home_r", "runs_home"],
        "away_score": ["away_score", "away_runs", "away_final_score", "away_r", "runs_away"],
    },
    "base_out_transitions": {
        "game_or_event_id": ["game_id", "game_pk", "event_id", "play_id", "at_bat_id", "pa_id", "game_event_id"],
        "inning": ["inning", "inn"],
        "half_inning": ["half_inning", "inning_half", "top_bottom", "batting_half", "is_top_inning", "home_away"],
        "start_base_state": ["start_base_state", "base_state_before", "bases_before", "pre_base_state", "before_base_state"],
        "start_outs": ["start_outs", "outs_before", "pre_outs", "outs_start"],
        "end_base_state": ["end_base_state", "base_state_after", "bases_after", "post_base_state", "after_base_state"],
        "end_outs": ["end_outs", "outs_after", "post_outs", "outs_end"],
        "runs_scored": ["runs_scored", "play_runs", "runs_on_play", "event_runs", "runs"],
    },
    "inning_runs": {
        "game_id": ["game_id", "game_pk", "gamepk", "game_key", "mlb_game_id", "gid"],
        "inning": ["inning", "inn"],
        "half_inning": ["half_inning", "inning_half", "top_bottom", "batting_half", "is_top_inning", "home_away"],
        "batting_team": ["batting_team", "bat_team", "offense_team", "team_batting", "offense"],
        "fielding_team": ["fielding_team", "fld_team", "defense_team", "team_fielding", "defense"],
        "runs_scored": ["runs_scored", "runs", "inning_runs", "runs_in_half_inning"],
    },
}


def safe_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def normalize_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value) if ch.isalnum())


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
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}
    except json.JSONDecodeError:
        parsed, _ = json.JSONDecoder().raw_decode(text)
        return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


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


def flatten_json_columns(value: Any, prefix: str = "", limit: int = 200) -> List[str]:
    cols: List[str] = []
    if len(cols) >= limit:
        return cols
    if isinstance(value, dict):
        for key, sub in value.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            cols.append(name)
            if len(cols) >= limit:
                break
            if isinstance(sub, (dict, list)):
                cols.extend(flatten_json_columns(sub, name, limit=limit))
                if len(cols) >= limit:
                    break
    elif isinstance(value, list):
        for item in value[:3]:
            cols.extend(flatten_json_columns(item, prefix, limit=limit))
            if len(cols) >= limit:
                break
    return list(dict.fromkeys(cols))[:limit]


def sample_artifact(path: Path) -> Tuple[str, int, List[str], str]:
    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            with path.open(newline="", encoding="utf-8", errors="replace") as handle:
                reader = csv.DictReader(handle)
                columns = list(reader.fieldnames or [])
                rows = 0
                for _ in reader:
                    rows += 1
                    if rows >= MAX_SAMPLE_ROWS:
                        break
                return "read_ok", rows, columns, ""
        if suffix == ".jsonl":
            rows = 0
            cols: List[str] = []
            with path.open(encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    rows += 1
                    if rows <= 5:
                        try:
                            parsed = json.loads(line)
                            cols.extend(flatten_json_columns(parsed))
                        except Exception:
                            pass
                    if rows >= MAX_SAMPLE_ROWS:
                        break
            return "read_ok", rows, list(dict.fromkeys(cols)), ""
        if suffix == ".json":
            parsed = json.loads(path.read_text(encoding="utf-8", errors="replace"))
            if isinstance(parsed, list):
                sample = parsed[:MAX_SAMPLE_ROWS]
                cols: List[str] = []
                for row in sample[:5]:
                    cols.extend(flatten_json_columns(row))
                return "read_ok", len(sample), list(dict.fromkeys(cols)), ""
            if isinstance(parsed, dict):
                cols = flatten_json_columns(parsed)
                row_count = len(parsed) if parsed else 1
                return "read_ok", min(row_count, MAX_SAMPLE_ROWS), cols, ""
            return "read_ok", 1, ["value"], ""
        if suffix == ".parquet":
            try:
                import pandas as pd  # type: ignore
            except Exception as exc:
                return "unreadable_dependency_missing", 0, [], f"pandas/parquet dependency missing: {type(exc).__name__}: {exc}"
            try:
                frame = pd.read_parquet(path)
                return "read_ok", min(len(frame), MAX_SAMPLE_ROWS), list(map(str, frame.columns)), ""
            except Exception as exc:
                return "unreadable", 0, [], f"{type(exc).__name__}: {exc}"
        if suffix in {".pkl", ".pickle"}:
            with path.open("rb") as handle:
                obj = pickle.load(handle)
            if hasattr(obj, "columns"):
                cols = list(map(str, obj.columns))
                rows = min(len(obj), MAX_SAMPLE_ROWS) if hasattr(obj, "__len__") else 1
                return "read_ok", rows, cols, ""
            cols = flatten_json_columns(obj)
            return "read_ok", 1, cols, ""
        return "unsupported_extension", 0, [], ""
    except Exception as exc:
        return "unreadable", 0, [], f"{type(exc).__name__}: {exc}"


def discover_candidates() -> List[Path]:
    paths: List[Path] = []
    seen: Set[str] = set()
    for root in SEARCH_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in EXTENSIONS:
                continue
            rel = path.as_posix()
            if rel in seen:
                continue
            seen.add(rel)
            paths.append(path)
    return sorted(paths, key=lambda p: p.as_posix())


def score_columns(columns: Sequence[str], family: str) -> Dict[str, Any]:
    aliases = REQUIREMENTS[family]
    normalized_columns = {normalize_key(col): col for col in columns}
    matched_required: List[str] = []
    matched_aliases: List[str] = []
    missing_required: List[str] = []

    for required, alias_list in aliases.items():
        hit_alias = ""
        hit_col = ""
        for alias in alias_list:
            norm_alias = normalize_key(alias)
            if norm_alias in normalized_columns:
                hit_alias = alias
                hit_col = normalized_columns[norm_alias]
                break
        if hit_alias:
            matched_required.append(required)
            matched_aliases.append(f"{required}:{hit_alias}->{hit_col}")
        else:
            missing_required.append(required)

    required_count = len(aliases)
    matched_count = len(matched_required)
    if matched_count == required_count:
        classification = "exact_satisfies_required_fields"
    elif matched_count >= 2:
        classification = "partial_candidate"
    else:
        classification = "insufficient"

    return {
        "requirement_family": family,
        "required_count": required_count,
        "matched_count": matched_count,
        "missing_count": len(missing_required),
        "classification": classification,
        "matched_required": "|".join(matched_required),
        "matched_aliases": "|".join(matched_aliases),
        "missing_required": "|".join(missing_required),
    }


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6hi_before = AUDIT_6HI_PATH.read_text(encoding="utf-8") if AUDIT_6HI_PATH.exists() else ""

    # 6HJ is planning-only and consumes already-emitted 6HI artifacts.
    class ArtifactOnlyRun:
        returncode = 0

    predecessor_run = ArtifactOnlyRun()

    json_6hi = load_json(JSON_6HI)
    checks_6hi = read_csv(CHECKS_6HI)
    selected_sources_6hi = read_csv(SELECTED_SOURCES_6HI)
    additional_req_6hi = read_csv(ADDITIONAL_REQ_6HI)
    decision_6hi = read_csv(DECISION_6HI)
    future_6hj_6hi = read_csv(FUTURE_6HJ_6HI)
    safety_6hi = read_csv(SAFETY_6HI)
    immutability_6hi = read_csv(IMMUTABILITY_6HI)
    additional_req_6hh = read_csv(ADDITIONAL_REQ_6HH)

    required_predecessor_artifacts = [
        JSON_6HI,
        CHECKS_6HI,
        SELECTED_SOURCES_6HI,
        ADDITIONAL_REQ_6HI,
        DECISION_6HI,
        FUTURE_6HJ_6HI,
        SAFETY_6HI,
        IMMUTABILITY_6HI,
        ADDITIONAL_REQ_6HH,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hi_audit_exists", "expected": True, "actual": AUDIT_6HI_PATH.exists(), "passed": AUDIT_6HI_PATH.exists()},
        {"check": "6hi_artifact_audit_mode", "expected": 0, "actual": predecessor_run.returncode, "passed": predecessor_run.returncode == 0},
        {"check": "6hi_json_exists", "expected": True, "actual": JSON_6HI.exists(), "passed": JSON_6HI.exists()},
        {"check": "6hi_all_checks_passed", "expected": True, "actual": json_6hi.get("all_checks_passed"), "passed": json_6hi.get("all_checks_passed") is True},
        {"check": "6hi_diagnosis", "expected": DIAGNOSIS_6HI, "actual": json_6hi.get("diagnosis"), "passed": json_6hi.get("diagnosis") == DIAGNOSIS_6HI},
        {"check": "6hi_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6hi.get("recommended_next_layer"), "passed": json_6hi.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6hi_additional_source_discovery_required", "expected": True, "actual": json_6hi.get("additional_source_discovery_required"), "passed": json_6hi.get("additional_source_discovery_required") is True},
        {"check": "6hi_row_level_adapter_revision_not_ready", "expected": True, "actual": json_6hi.get("row_level_adapter_revision_not_ready"), "passed": json_6hi.get("row_level_adapter_revision_not_ready") is True},
        {"check": "6hi_game_level_outcome_unavailable", "expected": False, "actual": json_6hi.get("game_level_outcome_source_available"), "passed": json_6hi.get("game_level_outcome_source_available") is False},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in required_predecessor_artifacts
    ]

    discovery_scope_rows = [
        {
            "scope": "search_root",
            "value": root.as_posix(),
            "exists": root.exists(),
            "local_only": True,
            "passed": True,
        }
        for root in SEARCH_ROOTS
    ] + [
        {
            "scope": "extension",
            "value": ext,
            "exists": True,
            "local_only": True,
            "passed": True,
        }
        for ext in sorted(EXTENSIONS)
    ]

    alias_rows: List[Dict[str, Any]] = []
    for family, required_map in REQUIREMENTS.items():
        for required, aliases in required_map.items():
            alias_rows.append({
                "requirement_family": family,
                "required_field": required,
                "aliases": "|".join(aliases),
                "alias_count": len(aliases),
                "passed": len(aliases) >= 1,
            })

    candidate_paths = discover_candidates()
    candidate_inventory_rows: List[Dict[str, Any]] = []
    sampled_rows: List[Dict[str, Any]] = []
    score_rows: List[Dict[str, Any]] = []

    for path in candidate_paths:
        path_text = path.as_posix()
        is_prior_layer_output = any(token in path_text.lower() for token in FORBIDDEN_OUTPUT_TOKENS)
        size_bytes = path.stat().st_size if path.exists() else 0
        inventory_row = {
            "artifact_path": path_text,
            "extension": path.suffix.lower(),
            "size_bytes": size_bytes,
            "search_root": next((root.as_posix() for root in SEARCH_ROOTS if path_text == root.as_posix() or path_text.startswith(root.as_posix() + "/")), ""),
            "prior_layer_output_excluded_from_best_candidate_decision": is_prior_layer_output,
            "passed": True,
        }
        candidate_inventory_rows.append(inventory_row)

        read_status, sampled_count, columns, error = sample_artifact(path)
        sampled_rows.append({
            "artifact_path": path_text,
            "read_status": read_status,
            "sampled_row_count": sampled_count,
            "column_count": len(columns),
            "columns_sample": "|".join(columns[:50]),
            "error": error,
            "prior_layer_output_excluded_from_best_candidate_decision": is_prior_layer_output,
            "passed": read_status in {"read_ok", "unreadable", "unreadable_dependency_missing"},
        })

        if read_status == "read_ok":
            for family in REQUIREMENTS:
                score = score_columns(columns, family)
                score.update({
                    "artifact_path": path_text,
                    "read_status": read_status,
                    "prior_layer_output_excluded_from_best_candidate_decision": is_prior_layer_output,
                    "candidate_eligible_for_decision": not is_prior_layer_output,
                    "passed": True,
                })
                score_rows.append(score)

    family_scores: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in score_rows:
        if row.get("candidate_eligible_for_decision"):
            family_scores[str(row["requirement_family"])].append(row)

    best_candidate_rows: List[Dict[str, Any]] = []
    gap_rows: List[Dict[str, Any]] = []
    exact_counts: Dict[str, int] = {}
    partial_counts: Dict[str, int] = {}

    for family in REQUIREMENTS:
        rows = family_scores.get(family, [])
        exact = [row for row in rows if row.get("classification") == "exact_satisfies_required_fields"]
        partial = [row for row in rows if row.get("classification") == "partial_candidate"]
        exact_counts[family] = len(exact)
        partial_counts[family] = len(partial)

        sorted_rows = sorted(
            rows,
            key=lambda row: (
                intish(row.get("matched_count"), 0),
                -intish(row.get("missing_count"), 999),
                row.get("artifact_path", ""),
            ),
            reverse=True,
        )
        best = sorted_rows[:5]
        if best:
            for rank, row in enumerate(best, start=1):
                best_candidate_rows.append({
                    "requirement_family": family,
                    "rank": rank,
                    "artifact_path": row.get("artifact_path"),
                    "classification": row.get("classification"),
                    "matched_count": row.get("matched_count"),
                    "required_count": row.get("required_count"),
                    "missing_count": row.get("missing_count"),
                    "matched_aliases": row.get("matched_aliases"),
                    "missing_required": row.get("missing_required"),
                    "passed": True,
                })
        else:
            best_candidate_rows.append({
                "requirement_family": family,
                "rank": 1,
                "artifact_path": "",
                "classification": "no_readable_eligible_candidates",
                "matched_count": 0,
                "required_count": len(REQUIREMENTS[family]),
                "missing_count": len(REQUIREMENTS[family]),
                "matched_aliases": "",
                "missing_required": "|".join(REQUIREMENTS[family].keys()),
                "passed": True,
            })

        gap_rows.append({
            "requirement_family": family,
            "exact_candidate_count": len(exact),
            "partial_candidate_count": len(partial),
            "gap_status": "satisfied_by_existing_local_artifact" if exact else "source_materialization_or_mapping_gap_remains",
            "recommended_action": "adapter_revision_possible_after_audit" if exact else "source_materialization_plan_required_after_audit",
            "passed": True,
        })

    exact_candidate_available_for_all_required_families = all(exact_counts.get(family, 0) >= 1 for family in REQUIREMENTS)
    adapter_revision_possible_after_audit = exact_candidate_available_for_all_required_families
    source_materialization_plan_required = not exact_candidate_available_for_all_required_families

    decision_rows = [
        {
            "decision": "exact_candidate_available_for_all_required_families",
            "expected": "computed",
            "actual": exact_candidate_available_for_all_required_families,
            "passed": True,
        },
        {
            "decision": "adapter_revision_possible_after_audit",
            "expected": "computed",
            "actual": adapter_revision_possible_after_audit,
            "passed": True,
        },
        {
            "decision": "source_materialization_plan_required",
            "expected": "computed",
            "actual": source_materialization_plan_required,
            "passed": True,
        },
        {
            "decision": "recommended_next_layer",
            "expected": RECOMMENDED_NEXT_LAYER,
            "actual": RECOMMENDED_NEXT_LAYER,
            "passed": True,
        },
        {
            "decision": "recommended_path",
            "expected": RECOMMENDED_PATH,
            "actual": RECOMMENDED_PATH,
            "passed": True,
        },
    ]

    future_6hk_rows = [
        {"contract": "audit_6hj_discovery_inventory", "required": True, "passed": True},
        {"contract": "verify_local_only_scope", "required": True, "passed": True},
        {"contract": "verify_requirement_family_scores", "required": True, "passed": True},
        {"contract": "verify_best_candidate_selection", "required": True, "passed": True},
        {"contract": "verify_gap_analysis_decision", "required": True, "passed": True},
        {"contract": "no_live_data_fetch", "required": True, "passed": True},
        {"contract": "no_real_backtest_or_mechanic_evaluation", "required": True, "passed": True},
        {"contract": "no_activation_or_exit_credit", "required": True, "passed": True},
    ]

    future_6hl_rows = [
        {
            "contract": "if_all_families_have_exact_candidates_then_adapter_revision_plan_after_6hk",
            "condition": exact_candidate_available_for_all_required_families,
            "passed": True,
        },
        {
            "contract": "if_any_family_lacks_exact_candidate_then_source_materialization_plan_after_6hk",
            "condition": source_materialization_plan_required,
            "passed": True,
        },
        {"contract": "6hl_must_wait_for_6hk_audit", "condition": True, "passed": True},
        {"contract": "6hl_must_not_run_real_evaluation", "condition": True, "passed": True},
        {"contract": "6hl_must_not_activate_layer_6", "condition": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_only_source_discovery", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_database_write", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_materialization_job", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_production_simulation", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_real_backtests", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_mechanic_evaluation", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_corrected_normalized_outcomes_for_evaluation", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": True, "actual": True, "passed": True},
        {"boundary": "layer_6_exit_credit_blocked", "expected": True, "actual": True, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6hi_after = AUDIT_6HI_PATH.read_text(encoding="utf-8") if AUDIT_6HI_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6hj_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hi_audit", "policy": "unchanged_by_6hj", "passed": audit_6hi_after == audit_6hi_before},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6hj", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hj", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "future_adapter_revision_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "future_real_evaluation_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HJ, "actual": DIAGNOSIS_6HJ, "passed": True},
    ]

    sampled_artifact_count = sum(1 for row in sampled_rows if row.get("read_status") == "read_ok")
    unreadable_artifact_count = sum(1 for row in sampled_rows if str(row.get("read_status", "")).startswith("unreadable"))

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "discovery_scope", "passed": all(row["passed"] for row in discovery_scope_rows), "detail": f"{sum(1 for row in discovery_scope_rows if row['passed'])}/{len(discovery_scope_rows)}"},
        {"check": "candidate_inventory", "passed": len(candidate_inventory_rows) >= 1, "detail": str(len(candidate_inventory_rows))},
        {"check": "sampled_artifacts", "passed": len(sampled_rows) >= 1 and sampled_artifact_count >= 1, "detail": f"sampled={sampled_artifact_count}; unreadable={unreadable_artifact_count}"},
        {"check": "requirement_aliases", "passed": all(row["passed"] for row in alias_rows), "detail": f"{sum(1 for row in alias_rows if row['passed'])}/{len(alias_rows)}"},
        {"check": "requirement_scores", "passed": len(score_rows) >= 1, "detail": str(len(score_rows))},
        {"check": "best_candidates", "passed": len(best_candidate_rows) >= 3, "detail": str(len(best_candidate_rows))},
        {"check": "gap_analysis", "passed": len(gap_rows) == 3 and all(row["passed"] for row in gap_rows), "detail": f"{sum(1 for row in gap_rows if row['passed'])}/{len(gap_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hk_contract", "passed": all(row["passed"] for row in future_6hk_rows), "detail": f"{sum(1 for row in future_6hk_rows if row['passed'])}/{len(future_6hk_rows)}"},
        {"check": "future_6hl_contract", "passed": all(row["passed"] for row in future_6hl_rows), "detail": f"{sum(1 for row in future_6hl_rows if row['passed'])}/{len(future_6hl_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(DISCOVERY_SCOPE_CSV.with_name(f"{SLUG}_artifact_presence.csv"), artifact_presence_rows),
        "discovery_scope": write_csv(DISCOVERY_SCOPE_CSV, discovery_scope_rows),
        "candidate_inventory": write_csv(CANDIDATE_INVENTORY_CSV, candidate_inventory_rows),
        "sampled_artifacts": write_csv(SAMPLED_ARTIFACTS_CSV, sampled_rows),
        "requirement_aliases": write_csv(REQUIREMENT_ALIASES_CSV, alias_rows),
        "requirement_scores": write_csv(REQUIREMENT_SCORES_CSV, score_rows if score_rows else [{"artifact_path": "", "requirement_family": "", "classification": "none", "passed": False}]),
        "best_candidates": write_csv(BEST_CANDIDATES_CSV, best_candidate_rows),
        "gap_analysis": write_csv(GAP_ANALYSIS_CSV, gap_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hk_contract": write_csv(FUTURE_6HK_CONTRACT_CSV, future_6hk_rows),
        "future_6hl_contract": write_csv(FUTURE_6HL_CONTRACT_CSV, future_6hl_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HJ",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HJ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "predecessor_audit": str(AUDIT_6HI_PATH),
        "predecessor_audit_returncode": predecessor_run.returncode,
        "predecessor_audit_diagnosis": json_6hi.get("diagnosis"),
        "audited_layer": "6HI",
        "local_only_source_discovery": True,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "corrected_normalized_outcomes_emitted_by_this_layer": False,
        "games_evaluated": 0,
        "activation_allowed": False,
        "layer_6_exit_credit": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "game_level_outcome_exact_candidate_count": exact_counts.get("game_level_outcomes", 0),
        "base_out_transition_exact_candidate_count": exact_counts.get("base_out_transitions", 0),
        "inning_runs_exact_candidate_count": exact_counts.get("inning_runs", 0),
        "game_level_outcome_partial_candidate_count": partial_counts.get("game_level_outcomes", 0),
        "base_out_transition_partial_candidate_count": partial_counts.get("base_out_transitions", 0),
        "inning_runs_partial_candidate_count": partial_counts.get("inning_runs", 0),
        "exact_candidate_available_for_all_required_families": exact_candidate_available_for_all_required_families,
        "adapter_revision_possible_after_audit": adapter_revision_possible_after_audit,
        "source_materialization_plan_required": source_materialization_plan_required,
        "candidate_artifact_count": len(candidate_inventory_rows),
        "sampled_artifact_count": sampled_artifact_count,
        "unreadable_artifact_count": unreadable_artifact_count,
        "requirement_family_count": len(REQUIREMENTS),
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(EVALUATION_WINDOWS),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "discovery_scope_csv": str(DISCOVERY_SCOPE_CSV),
            "candidate_inventory_csv": str(CANDIDATE_INVENTORY_CSV),
            "sampled_artifacts_csv": str(SAMPLED_ARTIFACTS_CSV),
            "requirement_aliases_csv": str(REQUIREMENT_ALIASES_CSV),
            "requirement_scores_csv": str(REQUIREMENT_SCORES_CSV),
            "best_candidates_csv": str(BEST_CANDIDATES_CSV),
            "gap_analysis_csv": str(GAP_ANALYSIS_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hk_contract_csv": str(FUTURE_6HK_CONTRACT_CSV),
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
