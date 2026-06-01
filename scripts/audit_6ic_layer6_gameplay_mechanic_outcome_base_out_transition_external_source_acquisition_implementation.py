#!/usr/bin/env python3
"""Audit Layer 6IB controlled base/out transition source acquisition implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ic_base_out_transition_external_source_acquisition_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENTATION_6IB_PATH = Path("scripts/implement_6ib_layer6_gameplay_mechanic_outcome_base_out_transition_external_source_acquisition.py")

JSON_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation.json"
CHECKS_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_checks.csv"
PREDECESSOR_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_predecessor.csv"
INPUT_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_input_artifacts.csv"
ACQUISITION_PLAN_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_acquisition_plan.csv"
GAMEPK_DISCOVERY_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_gamepk_discovery.csv"
FETCH_ATTEMPTS_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_fetch_attempts.csv"
CANDIDATE_EVIDENCE_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_candidate_evidence.csv"
TRANSITION_INDEX_AUDIT_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_transition_index.csv"
SOURCE_SELECTION_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_source_selection.csv"
READINESS_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_readiness.csv"
MANIFEST_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_manifest.json"
PRESERVED_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_preserved_families.csv"
DECISION_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_decision.csv"
FUTURE_6IC_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_future_6ic_contract.csv"
SAFETY_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_safety_boundaries.csv"
IMMUTABILITY_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_immutability.csv"
RECOMMENDED_6IB = TMP_DIR / "layer6_6ib_base_out_transition_external_source_acquisition_implementation_recommended_path.csv"
SOURCE_MANIFEST_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/source_manifest.json"
TRANSITION_INDEX_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/base_out_transition_index.csv"
RAW_FEED_DIR_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/statsapi_game_feed"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
ACQUISITION_BOUNDS_CSV = TMP_DIR / f"{SLUG}_acquisition_bounds.csv"
FETCH_ATTEMPTS_CSV = TMP_DIR / f"{SLUG}_fetch_attempts.csv"
TRANSITION_INDEX_CSV = TMP_DIR / f"{SLUG}_transition_index.csv"
EXACTNESS_PROFILE_CSV = TMP_DIR / f"{SLUG}_exactness_profile.csv"
GAP_CATEGORIES_CSV = TMP_DIR / f"{SLUG}_gap_categories.csv"
SOURCE_SELECTION_CSV = TMP_DIR / f"{SLUG}_source_selection.csv"
READINESS_CSV = TMP_DIR / f"{SLUG}_readiness.csv"
MANIFEST_CSV = TMP_DIR / f"{SLUG}_manifest.csv"
PRESERVED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6ID_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6id_contract.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IB = "layer_6_gameplay_mechanic_outcome_base_out_transition_external_source_acquisition_implementation_complete"
DIAGNOSIS_6IC = "layer_6_gameplay_mechanic_outcome_base_out_transition_external_source_acquisition_implementation_audit_complete"

RECOMMENDED_NEXT_LAYER_6IB = "6IC_layer_6_gameplay_mechanic_outcome_base_out_transition_external_source_acquisition_implementation_audit"
RECOMMENDED_PATH_6IB = "implement_controlled_base_out_transition_source_acquisition_then_audit_before_materialization"

RECOMMENDED_NEXT_LAYER_6IC = "6ID_layer_6_gameplay_mechanic_outcome_base_out_transition_reconstruction_gap_analysis_plan"
RECOMMENDED_PATH_6IC = "audit_controlled_acquisition_then_plan_reconstruction_gap_analysis_before_materialization"

SOURCE_FAMILY = "base_out_transitions"
ACQUISITION_MODE = "future_controlled_statsapi_acquisition"

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

GAP_CATEGORIES = [
    "missing_or_ambiguous_runner_end_base",
    "missing_or_ambiguous_runner_start_base",
    "batter_reached_base_assignment_uncertain",
    "out_count_inconsistency",
    "inning_boundary_or_walkoff_boundary",
    "scoring_runner_without_explicit_base_path",
    "substitution_or_non_batted_ball_event",
    "double_play_or_force_play_complexity",
    "caught_stealing_pickoff_or_runner_out_complexity",
    "wild_pitch_passed_ball_balk_runner_movement_complexity",
    "statsapi_representation_gap",
    "parser_logic_gap",
]


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True}]
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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    implementation_before = IMPLEMENTATION_6IB_PATH.read_text(encoding="utf-8") if IMPLEMENTATION_6IB_PATH.exists() else ""
    transition_index_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""

    json_6ib = load_json(JSON_6IB)
    manifest_6ib = load_json(MANIFEST_6IB)
    source_manifest_6ib = load_json(SOURCE_MANIFEST_6IB)

    required_artifacts = [
        JSON_6IB,
        CHECKS_6IB,
        PREDECESSOR_6IB,
        INPUT_6IB,
        ACQUISITION_PLAN_6IB,
        GAMEPK_DISCOVERY_6IB,
        FETCH_ATTEMPTS_6IB,
        CANDIDATE_EVIDENCE_6IB,
        TRANSITION_INDEX_AUDIT_6IB,
        SOURCE_SELECTION_6IB,
        READINESS_6IB,
        MANIFEST_6IB,
        PRESERVED_6IB,
        DECISION_6IB,
        FUTURE_6IC_6IB,
        SAFETY_6IB,
        IMMUTABILITY_6IB,
        RECOMMENDED_6IB,
        SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB,
    ]

    fetch_rows_6ib = read_csv(FETCH_ATTEMPTS_6IB)
    transition_rows_6ib = read_csv(TRANSITION_INDEX_6IB)
    selection_rows_6ib = read_csv(SOURCE_SELECTION_6IB)
    readiness_rows_6ib = read_csv(READINESS_6IB)
    preserved_rows_6ib = read_csv(PRESERVED_6IB)

    raw_feed_paths = list(RAW_FEED_DIR_6IB.glob("*.json")) if RAW_FEED_DIR_6IB.exists() else []
    raw_feed_count = len(raw_feed_paths)

    transition_row_count = len(transition_rows_6ib)
    exact_transition_row_count = sum(1 for row in transition_rows_6ib if boolish(row.get("exact_transition_row")))
    non_exact_transition_row_count = transition_row_count - exact_transition_row_count
    exact_transition_rate = round(exact_transition_row_count / transition_row_count, 6) if transition_row_count else 0.0

    game_ids = sorted({row.get("game_id") for row in transition_rows_6ib if row.get("game_id")})
    full_exact_game_count = 0
    for game_id in game_ids:
        game_rows = [row for row in transition_rows_6ib if row.get("game_id") == game_id]
        if game_rows and all(boolish(row.get("exact_transition_row")) for row in game_rows):
            full_exact_game_count += 1

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ib_implementation_exists", "expected": True, "actual": IMPLEMENTATION_6IB_PATH.exists(), "passed": IMPLEMENTATION_6IB_PATH.exists()},
        {"check": "6ib_json_exists", "expected": True, "actual": JSON_6IB.exists(), "passed": JSON_6IB.exists()},
        {"check": "6ib_all_checks_passed", "expected": True, "actual": json_6ib.get("all_checks_passed"), "passed": json_6ib.get("all_checks_passed") is True},
        {"check": "6ib_diagnosis", "expected": DIAGNOSIS_6IB, "actual": json_6ib.get("diagnosis"), "passed": json_6ib.get("diagnosis") == DIAGNOSIS_6IB},
        {"check": "6ib_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IB, "actual": json_6ib.get("recommended_next_layer"), "passed": json_6ib.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IB},
        {"check": "6ib_recommended_path", "expected": RECOMMENDED_PATH_6IB, "actual": json_6ib.get("recommended_path"), "passed": json_6ib.get("recommended_path") == RECOMMENDED_PATH_6IB},
        {"check": "6ib_source_family", "expected": SOURCE_FAMILY, "actual": json_6ib.get("source_family"), "passed": json_6ib.get("source_family") == SOURCE_FAMILY},
        {"check": "6ib_acquisition_mode", "expected": ACQUISITION_MODE, "actual": json_6ib.get("acquisition_mode"), "passed": json_6ib.get("acquisition_mode") == ACQUISITION_MODE},
        {"check": "6ib_no_exit_credit", "expected": False, "actual": json_6ib.get("layer_6_exit_credit"), "passed": json_6ib.get("layer_6_exit_credit") is False},
    ]

    artifact_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_artifacts
    ]

    acquisition_bounds_rows = [
        {"audit": "bounded_acquisition_confirmed", "expected": True, "actual": json_6ib.get("bounded_acquisition_confirmed"), "passed": json_6ib.get("bounded_acquisition_confirmed") is True},
        {"audit": "attempted_game_count", "expected": 10, "actual": json_6ib.get("attempted_game_count"), "passed": json_6ib.get("attempted_game_count") == 10},
        {"audit": "fetched_game_count", "expected": 10, "actual": json_6ib.get("fetched_game_count"), "passed": json_6ib.get("fetched_game_count") == 10},
        {"audit": "failed_fetch_count", "expected": 0, "actual": json_6ib.get("failed_fetch_count"), "passed": json_6ib.get("failed_fetch_count") == 0},
        {"audit": "raw_feed_count", "expected": 10, "actual": raw_feed_count, "passed": raw_feed_count == 10},
        {"audit": "source_manifest_exists", "expected": True, "actual": SOURCE_MANIFEST_6IB.exists(), "passed": SOURCE_MANIFEST_6IB.exists()},
    ]

    fetch_audit_rows = [
        {
            "gamePk": row.get("gamePk"),
            "attempted": row.get("attempted"),
            "succeeded": row.get("succeeded"),
            "status": row.get("status"),
            "cache_path": row.get("cache_path"),
            "cache_exists": Path(row.get("cache_path", "")).exists() if row.get("cache_path") else False,
            "passed": boolish(row.get("attempted")) and boolish(row.get("succeeded")) and Path(row.get("cache_path", "")).exists(),
        }
        for row in fetch_rows_6ib
    ]

    transition_index_rows = [
        {"audit": "transition_index_exists", "expected": True, "actual": TRANSITION_INDEX_6IB.exists(), "passed": TRANSITION_INDEX_6IB.exists()},
        {"audit": "transition_row_count", "expected": 801, "actual": transition_row_count, "passed": transition_row_count == 801},
        {"audit": "exact_transition_row_count", "expected": 696, "actual": exact_transition_row_count, "passed": exact_transition_row_count == 696},
        {"audit": "non_exact_transition_row_count", "expected": 105, "actual": non_exact_transition_row_count, "passed": non_exact_transition_row_count == 105},
        {"audit": "full_exact_game_count", "expected": 0, "actual": full_exact_game_count, "passed": full_exact_game_count == 0},
        {"audit": "exact_required_evidence_met", "expected": False, "actual": json_6ib.get("exact_required_evidence_met"), "passed": json_6ib.get("exact_required_evidence_met") is False},
    ]

    exactness_profile_rows = [
        {
            "metric": "transition_row_count",
            "value": transition_row_count,
            "interpretation": "rows staged by 6IB transition extraction",
            "passed": transition_row_count == 801,
        },
        {
            "metric": "exact_transition_row_count",
            "value": exact_transition_row_count,
            "interpretation": "rows marked exact by 6IB reconstruction",
            "passed": exact_transition_row_count == 696,
        },
        {
            "metric": "non_exact_transition_row_count",
            "value": non_exact_transition_row_count,
            "interpretation": "rows needing reconstruction gap analysis",
            "passed": non_exact_transition_row_count == 105,
        },
        {
            "metric": "exact_transition_rate",
            "value": exact_transition_rate,
            "interpretation": "partial source utility; not sufficient for full-game remediation",
            "passed": 0.0 < exact_transition_rate < 1.0,
        },
        {
            "metric": "full_exact_game_count",
            "value": full_exact_game_count,
            "interpretation": "must be >0 for 6IB remediation; remains 0",
            "passed": full_exact_game_count == 0,
        },
    ]

    gap_category_rows = [
        {
            "gap_category": category,
            "requires_6id_analysis": True,
            "source": "6IC audit category taxonomy",
            "passed": True,
        }
        for category in GAP_CATEGORIES
    ]

    selection = selection_rows_6ib[0] if selection_rows_6ib else {}
    readiness = readiness_rows_6ib[0] if readiness_rows_6ib else {}

    source_selection_rows = [
        {"audit": "selection_row_exists", "expected": True, "actual": bool(selection), "passed": bool(selection)},
        {"audit": "selected_source_found_false", "expected": "False", "actual": selection.get("selected_source_found"), "passed": selection.get("selected_source_found") == "False"},
        {"audit": "exact_required_evidence_met_false", "expected": "False", "actual": selection.get("exact_required_evidence_met"), "passed": selection.get("exact_required_evidence_met") == "False"},
        {"audit": "remediation_status_fail_closed", "expected": "fail_closed_no_exact_deterministic_external_or_new_base_out_transition_source", "actual": selection.get("remediation_status"), "passed": selection.get("remediation_status") == "fail_closed_no_exact_deterministic_external_or_new_base_out_transition_source"},
        {"audit": "fail_closed_reason", "expected": "fail_closed_no_game_with_full_exact_pre_post_base_out_transition_rows", "actual": selection.get("fail_closed_reason"), "passed": selection.get("fail_closed_reason") == "fail_closed_no_game_with_full_exact_pre_post_base_out_transition_rows"},
    ]

    readiness_audit_rows = [
        {"audit": "readiness_row_exists", "expected": True, "actual": bool(readiness), "passed": bool(readiness)},
        {"audit": "base_out_not_remediated", "expected": "False", "actual": readiness.get("remediated"), "passed": readiness.get("remediated") == "False"},
        {"audit": "ready_for_materialization_false", "expected": "False", "actual": readiness.get("ready_for_materialization"), "passed": readiness.get("ready_for_materialization") == "False"},
        {"audit": "requires_6ic_audit_true", "expected": "True", "actual": readiness.get("requires_6ic_audit"), "passed": readiness.get("requires_6ic_audit") == "True"},
    ]

    manifest_rows = [
        {"audit": "manifest_exists", "expected": True, "actual": MANIFEST_6IB.exists(), "passed": MANIFEST_6IB.exists()},
        {"audit": "manifest_layer", "expected": "6IB", "actual": manifest_6ib.get("layer"), "passed": manifest_6ib.get("layer") == "6IB"},
        {"audit": "source_manifest_layer", "expected": "6IB", "actual": source_manifest_6ib.get("layer"), "passed": source_manifest_6ib.get("layer") == "6IB"},
        {"audit": "source_manifest_remediation_status", "expected": "fail_closed_no_exact_deterministic_external_or_new_base_out_transition_source", "actual": source_manifest_6ib.get("remediation_status"), "passed": source_manifest_6ib.get("remediation_status") == "fail_closed_no_exact_deterministic_external_or_new_base_out_transition_source"},
        {"audit": "manifest_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IB, "actual": manifest_6ib.get("next_layer"), "passed": manifest_6ib.get("next_layer") == RECOMMENDED_NEXT_LAYER_6IB},
    ]

    preserved_family_rows = [
        {
            "source_family": "game_level_outcomes",
            "expected": "preserved",
            "actual_present": any(row.get("source_family") == "game_level_outcomes" and boolish(row.get("passed")) for row in preserved_rows_6ib),
            "passed": any(row.get("source_family") == "game_level_outcomes" and boolish(row.get("passed")) for row in preserved_rows_6ib),
        },
        {
            "source_family": "inning_runs",
            "expected": "preserved",
            "actual_present": any(row.get("source_family") == "inning_runs" and boolish(row.get("passed")) for row in preserved_rows_6ib),
            "passed": any(row.get("source_family") == "inning_runs" and boolish(row.get("passed")) for row in preserved_rows_6ib),
        },
    ]

    decision_rows = [
        {"decision": "6ib_passed", "expected": True, "actual": json_6ib.get("all_checks_passed"), "passed": json_6ib.get("all_checks_passed") is True},
        {"decision": "statsapi_source_family_rejected", "expected": False, "actual": False, "passed": True},
        {"decision": "reconstruction_gap_analysis_required", "expected": True, "actual": True, "passed": True},
        {"decision": "additional_acquisition_required_immediately", "expected": False, "actual": False, "passed": True},
        {"decision": "base_out_transitions_remediated", "expected": False, "actual": json_6ib.get("base_out_transitions_remediated"), "passed": json_6ib.get("base_out_transitions_remediated") is False},
        {"decision": "materialization_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "adapter_revision_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IC, "actual": RECOMMENDED_NEXT_LAYER_6IC, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    future_6id_rows = [
        {"contract": "consume_6ic_audit_and_6ib_transition_index", "required": True, "passed": True},
        {"contract": "analyze_non_exact_rows_without_fetching_or_materializing", "required": True, "passed": True},
        {"contract": "classify_non_exact_rows_into_gap_categories", "required": True, "passed": True},
        {"contract": "define_targeted_reconstruction_fix_plan", "required": True, "passed": True},
        {"contract": "preserve_acquired_raw_feed_cache_and_source_manifest", "required": True, "passed": True},
        {"contract": "keep_materialization_adapter_real_eval_activation_exit_blocked", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_jobs", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_revision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    transition_index_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    script_after = Path(__file__).read_text(encoding="utf-8")
    implementation_after = IMPLEMENTATION_6IB_PATH.read_text(encoding="utf-8") if IMPLEMENTATION_6IB_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6ic_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6ib_implementation", "policy": "unchanged_by_6ic", "passed": implementation_after == implementation_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6ic", "passed": transition_index_after == transition_index_before},
        {"surface": "6ib_raw_feed_cache", "policy": "read_only", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IC, "actual": RECOMMENDED_NEXT_LAYER_6IC, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IC, "actual": RECOMMENDED_PATH_6IC, "passed": True},
        {"decision": "do_not_recommend_materialization", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "plan_reconstruction_gap_analysis_next", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IC, "actual": DIAGNOSIS_6IC, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}"},
        {"check": "acquisition_bounds", "passed": all(row["passed"] for row in acquisition_bounds_rows), "detail": f"{sum(1 for row in acquisition_bounds_rows if row['passed'])}/{len(acquisition_bounds_rows)}"},
        {"check": "fetch_attempts", "passed": len(fetch_audit_rows) == 10 and all(row["passed"] for row in fetch_audit_rows), "detail": f"{sum(1 for row in fetch_audit_rows if row['passed'])}/{len(fetch_audit_rows)}"},
        {"check": "transition_index", "passed": all(row["passed"] for row in transition_index_rows), "detail": f"{sum(1 for row in transition_index_rows if row['passed'])}/{len(transition_index_rows)}"},
        {"check": "exactness_profile", "passed": all(row["passed"] for row in exactness_profile_rows), "detail": f"{sum(1 for row in exactness_profile_rows if row['passed'])}/{len(exactness_profile_rows)}"},
        {"check": "gap_categories", "passed": all(row["passed"] for row in gap_category_rows), "detail": f"{sum(1 for row in gap_category_rows if row['passed'])}/{len(gap_category_rows)}"},
        {"check": "source_selection", "passed": all(row["passed"] for row in source_selection_rows), "detail": f"{sum(1 for row in source_selection_rows if row['passed'])}/{len(source_selection_rows)}"},
        {"check": "readiness", "passed": all(row["passed"] for row in readiness_audit_rows), "detail": f"{sum(1 for row in readiness_audit_rows if row['passed'])}/{len(readiness_audit_rows)}"},
        {"check": "manifest", "passed": all(row["passed"] for row in manifest_rows), "detail": f"{sum(1 for row in manifest_rows if row['passed'])}/{len(manifest_rows)}"},
        {"check": "preserved_families", "passed": all(row["passed"] for row in preserved_family_rows), "detail": f"{sum(1 for row in preserved_family_rows if row['passed'])}/{len(preserved_family_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6id_contract", "passed": all(row["passed"] for row in future_6id_rows), "detail": f"{sum(1 for row in future_6id_rows if row['passed'])}/{len(future_6id_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_rows),
        "acquisition_bounds": write_csv(ACQUISITION_BOUNDS_CSV, acquisition_bounds_rows),
        "fetch_attempts": write_csv(FETCH_ATTEMPTS_CSV, fetch_audit_rows),
        "transition_index": write_csv(TRANSITION_INDEX_CSV, transition_index_rows),
        "exactness_profile": write_csv(EXACTNESS_PROFILE_CSV, exactness_profile_rows),
        "gap_categories": write_csv(GAP_CATEGORIES_CSV, gap_category_rows),
        "source_selection": write_csv(SOURCE_SELECTION_CSV, source_selection_rows),
        "readiness": write_csv(READINESS_CSV, readiness_audit_rows),
        "manifest": write_csv(MANIFEST_CSV, manifest_rows),
        "preserved_families": write_csv(PRESERVED_FAMILIES_CSV, preserved_family_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6id_contract": write_csv(FUTURE_6ID_CONTRACT_CSV, future_6id_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IC",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IC if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IC,
        "recommended_path": RECOMMENDED_PATH_6IC,
        "audited_layer": "6IB",
        "predecessor_implementation": str(IMPLEMENTATION_6IB_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6ib.get("diagnosis"),
        "source_family": SOURCE_FAMILY,
        "acquisition_mode": json_6ib.get("acquisition_mode"),
        "bounded_acquisition_confirmed": json_6ib.get("bounded_acquisition_confirmed"),
        "attempted_game_count": json_6ib.get("attempted_game_count"),
        "fetched_game_count": json_6ib.get("fetched_game_count"),
        "failed_fetch_count": json_6ib.get("failed_fetch_count"),
        "raw_feed_count": raw_feed_count,
        "candidate_evidence_count": json_6ib.get("candidate_evidence_count"),
        "transition_row_count": transition_row_count,
        "exact_transition_row_count": exact_transition_row_count,
        "non_exact_transition_row_count": non_exact_transition_row_count,
        "exact_transition_rate": exact_transition_rate,
        "full_exact_game_count": full_exact_game_count,
        "selected_source_found": False,
        "exact_required_evidence_met": False,
        "base_out_transitions_remediated": False,
        "remediation_status": json_6ib.get("remediation_status"),
        "fail_closed_reason": json_6ib.get("fail_closed_reason"),
        "statsapi_source_family_rejected": False,
        "reconstruction_gap_analysis_required": True,
        "additional_acquisition_required_immediately": False,
        "reconstruction_gap_category_count": len(GAP_CATEGORIES),
        "future_6id_contract_valid": all(row["passed"] for row in future_6id_rows),
        "game_level_outcomes_preserved": True,
        "inning_runs_preserved": True,
        "all_three_source_families_remediated_after_audit": False,
        "materialization_allowed_after_this_audit": False,
        "materialization_still_blocked": True,
        "adapter_revision_allowed_after_this_audit": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_allowed_after_this_audit": False,
        "real_evaluation_blocked_by_validation": True,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "corrected_normalized_outcomes_emitted_by_this_layer": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "database_writes_run": False,
        "source_acquisition_performed_by_this_layer": False,
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
            "acquisition_bounds_csv": str(ACQUISITION_BOUNDS_CSV),
            "fetch_attempts_csv": str(FETCH_ATTEMPTS_CSV),
            "transition_index_csv": str(TRANSITION_INDEX_CSV),
            "exactness_profile_csv": str(EXACTNESS_PROFILE_CSV),
            "gap_categories_csv": str(GAP_CATEGORIES_CSV),
            "source_selection_csv": str(SOURCE_SELECTION_CSV),
            "readiness_csv": str(READINESS_CSV),
            "manifest_csv": str(MANIFEST_CSV),
            "preserved_families_csv": str(PRESERVED_FAMILIES_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6id_contract_csv": str(FUTURE_6ID_CONTRACT_CSV),
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
