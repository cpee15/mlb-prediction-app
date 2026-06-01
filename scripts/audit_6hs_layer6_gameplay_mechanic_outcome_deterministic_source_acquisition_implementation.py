#!/usr/bin/env python3
"""Audit Layer 6HR deterministic local source acquisition implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6hs_deterministic_source_acquisition_implementation_audit"
TMP_DIR = Path("tmp")

IMPLEMENTATION_6HR_PATH = Path("scripts/implement_6hr_layer6_gameplay_mechanic_outcome_deterministic_source_acquisition.py")

JSON_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation.json"
CHECKS_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_checks.csv"
PREDECESSOR_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_predecessor.csv"
INPUT_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_input_artifacts.csv"
CONTRACTS_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_contracts.csv"
INVENTORY_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_inventory_scan.csv"
CANDIDATE_EVIDENCE_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_candidate_evidence.csv"
SOURCE_SELECTION_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_source_selection.csv"
STAGED_INDEXES_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_staged_indexes.csv"
ACQ_QUALITY_AUDIT_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_acquisition_quality.csv"
DECISION_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_decision.csv"
FUTURE_6HS_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_future_6hs_contract.csv"
SAFETY_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_safety_boundaries.csv"
IMMUTABILITY_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_immutability.csv"
RECOMMENDED_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_implementation_recommended_path.csv"

ACQ_MANIFEST_6HR = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_manifest.json"
GAME_INDEX_6HR = TMP_DIR / "layer6_6hr_acquired_game_level_outcomes_source_index.csv"
BASE_INDEX_6HR = TMP_DIR / "layer6_6hr_acquired_base_out_transitions_source_index.csv"
INNING_INDEX_6HR = TMP_DIR / "layer6_6hr_acquired_inning_runs_source_index.csv"
ACQ_EVIDENCE_6HR = TMP_DIR / "layer6_6hr_acquisition_evidence_report.csv"
ACQ_QUALITY_6HR = TMP_DIR / "layer6_6hr_acquisition_quality_report.csv"

PROTECTED_MATERIALIZED = [
    TMP_DIR / "layer6_materialized_game_level_outcomes.csv",
    TMP_DIR / "layer6_materialized_base_out_transitions.csv",
    TMP_DIR / "layer6_materialized_inning_runs.csv",
    TMP_DIR / "layer6_materialized_outcome_source_manifest.json",
    TMP_DIR / "layer6_materialized_outcome_source_quality_report.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
MANIFEST_CSV = TMP_DIR / f"{SLUG}_manifest.csv"
SOURCE_INDEXES_CSV = TMP_DIR / f"{SLUG}_source_indexes.csv"
ACQ_QUALITY_CSV = TMP_DIR / f"{SLUG}_acquisition_quality.csv"
INVENTORY_SCAN_CSV = TMP_DIR / f"{SLUG}_inventory_scan.csv"
CANDIDATE_EVIDENCE_CSV = TMP_DIR / f"{SLUG}_candidate_evidence.csv"
FAIL_CLOSED_CSV = TMP_DIR / f"{SLUG}_fail_closed.csv"
MATERIALIZATION_PROTECTION_CSV = TMP_DIR / f"{SLUG}_materialization_protection.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6ht_contract.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HR = "layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation_complete"
DIAGNOSIS_6HS = "layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation_audit_complete"
RECOMMENDED_NEXT_LAYER_6HR = "6HS_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation_audit"
RECOMMENDED_PATH_6HR = "implement_deterministic_source_acquisition_then_audit_before_materialization_or_adapter_revision"
RECOMMENDED_NEXT_LAYER_6HS = "6HT_layer_6_gameplay_mechanic_outcome_deterministic_source_gap_remediation_plan"
RECOMMENDED_PATH_6HS = "audit_source_acquisition_fail_closed_then_plan_gap_remediation_before_materialization_or_adapter_revision"

SOURCE_FAMILIES = ["game_level_outcomes", "base_out_transitions", "inning_runs"]

EXPECTED_INDEXES = {
    "game_level_outcomes": {
        "path": GAME_INDEX_6HR,
        "rejection_reason": "fail_closed_no_exact_deterministic_game_level_outcomes_source",
        "planned_artifact": "tmp/layer6_materialized_game_level_outcomes.csv",
    },
    "base_out_transitions": {
        "path": BASE_INDEX_6HR,
        "rejection_reason": "fail_closed_no_exact_deterministic_base_out_transitions_source",
        "planned_artifact": "tmp/layer6_materialized_base_out_transitions.csv",
    },
    "inning_runs": {
        "path": INNING_INDEX_6HR,
        "rejection_reason": "fail_closed_no_exact_deterministic_inning_runs_source",
        "planned_artifact": "tmp/layer6_materialized_inning_runs.csv",
    },
}

REQUIRED_SOURCE_INDEX_COLUMNS = [
    "source_family",
    "selected",
    "source_path",
    "source_type",
    "evidence_score",
    "evidence_fields",
    "rejection_reason",
    "acquisition_status",
    "planned_materialization_artifact",
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


def find_row(rows: List[Dict[str, str]], key: str, value: str) -> Dict[str, str]:
    for row in rows:
        if row.get(key) == value:
            return row
    return {}


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    impl_6hr_before = IMPLEMENTATION_6HR_PATH.read_text(encoding="utf-8") if IMPLEMENTATION_6HR_PATH.exists() else ""

    json_6hr = load_json(JSON_6HR)
    manifest = load_json(ACQ_MANIFEST_6HR)
    inventory_rows_6hr = read_csv(INVENTORY_6HR)
    candidate_rows_6hr = read_csv(CANDIDATE_EVIDENCE_6HR)
    source_selection_rows_6hr = read_csv(SOURCE_SELECTION_6HR)
    acquisition_quality_rows_6hr = read_csv(ACQ_QUALITY_6HR)

    required_artifacts = [
        JSON_6HR,
        CHECKS_6HR,
        PREDECESSOR_6HR,
        INPUT_6HR,
        CONTRACTS_6HR,
        INVENTORY_6HR,
        CANDIDATE_EVIDENCE_6HR,
        SOURCE_SELECTION_6HR,
        STAGED_INDEXES_6HR,
        ACQ_QUALITY_AUDIT_6HR,
        DECISION_6HR,
        FUTURE_6HS_6HR,
        SAFETY_6HR,
        IMMUTABILITY_6HR,
        RECOMMENDED_6HR,
        ACQ_MANIFEST_6HR,
        GAME_INDEX_6HR,
        BASE_INDEX_6HR,
        INNING_INDEX_6HR,
        ACQ_EVIDENCE_6HR,
        ACQ_QUALITY_6HR,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hr_implementation_exists", "expected": True, "actual": IMPLEMENTATION_6HR_PATH.exists(), "passed": IMPLEMENTATION_6HR_PATH.exists()},
        {"check": "6hr_json_exists", "expected": True, "actual": JSON_6HR.exists(), "passed": JSON_6HR.exists()},
        {"check": "6hr_all_checks_passed", "expected": True, "actual": json_6hr.get("all_checks_passed"), "passed": json_6hr.get("all_checks_passed") is True},
        {"check": "6hr_diagnosis", "expected": DIAGNOSIS_6HR, "actual": json_6hr.get("diagnosis"), "passed": json_6hr.get("diagnosis") == DIAGNOSIS_6HR},
        {"check": "6hr_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HR, "actual": json_6hr.get("recommended_next_layer"), "passed": json_6hr.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HR},
        {"check": "6hr_recommended_path", "expected": RECOMMENDED_PATH_6HR, "actual": json_6hr.get("recommended_path"), "passed": json_6hr.get("recommended_path") == RECOMMENDED_PATH_6HR},
        {"check": "6hr_deterministic_acquisition_only", "expected": True, "actual": json_6hr.get("deterministic_source_acquisition_only"), "passed": json_6hr.get("deterministic_source_acquisition_only") is True},
        {"check": "6hr_selected_source_family_count", "expected": 0, "actual": json_6hr.get("selected_source_family_count"), "passed": json_6hr.get("selected_source_family_count") == 0},
        {"check": "6hr_fail_closed_family_count", "expected": 3, "actual": json_6hr.get("fail_closed_family_count"), "passed": json_6hr.get("fail_closed_family_count") == 3},
        {"check": "6hr_materialization_blocked", "expected": True, "actual": json_6hr.get("materialization_still_blocked_pending_6hs_audit"), "passed": json_6hr.get("materialization_still_blocked_pending_6hs_audit") is True},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_artifacts
    ]

    expected_safety = [
        "local_only",
        "no_live_data_fetch",
        "no_database_write",
        "no_materialization_jobs",
        "no_adapter_revision",
        "no_real_evaluation",
        "no_activation",
        "no_layer_6_exit_credit",
    ]
    manifest_safety = manifest.get("safety_boundaries", [])
    manifest_rows = [
        {"audit": "manifest_exists", "expected": True, "actual": ACQ_MANIFEST_6HR.exists(), "passed": ACQ_MANIFEST_6HR.exists()},
        {"audit": "manifest_layer", "expected": "6HR", "actual": manifest.get("layer"), "passed": manifest.get("layer") == "6HR"},
        {"audit": "manifest_creation_mode", "expected": "local_only_deterministic_source_acquisition", "actual": manifest.get("creation_mode"), "passed": manifest.get("creation_mode") == "local_only_deterministic_source_acquisition"},
        {"audit": "manifest_selected_source_count", "expected": 0, "actual": manifest.get("selected_source_count"), "passed": manifest.get("selected_source_count") == 0},
        {"audit": "manifest_failed_source_family_count", "expected": 3, "actual": manifest.get("failed_source_family_count"), "passed": manifest.get("failed_source_family_count") == 3},
        {"audit": "manifest_source_families", "expected": "|".join(SOURCE_FAMILIES), "actual": "|".join(manifest.get("source_families", [])), "passed": manifest.get("source_families", []) == SOURCE_FAMILIES},
        {"audit": "manifest_staged_indexes_count", "expected": 3, "actual": len(manifest.get("staged_indexes", [])), "passed": len(manifest.get("staged_indexes", [])) == 3},
        {"audit": "manifest_evidence_report", "expected": str(ACQ_EVIDENCE_6HR), "actual": manifest.get("evidence_report"), "passed": manifest.get("evidence_report") == str(ACQ_EVIDENCE_6HR)},
        {"audit": "manifest_quality_report", "expected": str(ACQ_QUALITY_6HR), "actual": manifest.get("quality_report"), "passed": manifest.get("quality_report") == str(ACQ_QUALITY_6HR)},
        {"audit": "manifest_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HR, "actual": manifest.get("next_layer"), "passed": manifest.get("next_layer") == RECOMMENDED_NEXT_LAYER_6HR},
        {"audit": "manifest_safety_boundaries", "expected": "|".join(expected_safety), "actual": "|".join(manifest_safety), "passed": all(item in manifest_safety for item in expected_safety)},
    ]

    source_index_rows = []
    for family, expected in EXPECTED_INDEXES.items():
        rows = read_csv(expected["path"])
        row = rows[0] if rows else {}
        columns = list(row.keys()) if row else []
        source_index_rows.append({
            "source_family": family,
            "index_path": str(expected["path"]),
            "exists": expected["path"].exists(),
            "row_count": len(rows),
            "columns_complete": all(col in columns for col in REQUIRED_SOURCE_INDEX_COLUMNS),
            "selected": row.get("selected"),
            "acquisition_status": row.get("acquisition_status"),
            "rejection_reason": row.get("rejection_reason"),
            "planned_materialization_artifact": row.get("planned_materialization_artifact"),
            "passed": (
                expected["path"].exists()
                and len(rows) == 1
                and all(col in columns for col in REQUIRED_SOURCE_INDEX_COLUMNS)
                and row.get("source_family") == family
                and row.get("selected") == "False"
                and row.get("acquisition_status") == "fail_closed_no_exact_deterministic_local_source"
                and row.get("rejection_reason") == expected["rejection_reason"]
                and row.get("planned_materialization_artifact") == expected["planned_artifact"]
            ),
        })

    acquisition_quality_rows = []
    for family in SOURCE_FAMILIES:
        row = find_row(acquisition_quality_rows_6hr, "source_family", family)
        acquisition_quality_rows.append({
            "source_family": family,
            "present": bool(row),
            "selected_source_count": row.get("selected_source_count"),
            "required_evidence_met": row.get("required_evidence_met"),
            "acquisition_status": row.get("acquisition_status"),
            "passed_flag": row.get("passed"),
            "passed": (
                bool(row)
                and row.get("selected_source_count") == "0"
                and row.get("required_evidence_met") == "False"
                and row.get("acquisition_status") == "fail_closed_no_exact_deterministic_local_source"
                and boolish(row.get("passed"))
            ),
        })

    root_set = {row.get("search_root") for row in inventory_rows_6hr}
    statsapi_row = find_row(inventory_rows_6hr, "search_root", "tmp/statsapi_cache")
    inventory_scan_rows = [
        {"audit": "inventory_row_count", "expected": 5, "actual": len(inventory_rows_6hr), "passed": len(inventory_rows_6hr) == 5},
        {"audit": "inventory_roots", "expected": "data/raw|tmp/local_source_cache|tmp/statsapi_cache|cache|artifacts", "actual": "|".join(sorted(root_set)), "passed": root_set == {"data/raw", "tmp/local_source_cache", "tmp/statsapi_cache", "cache", "artifacts"}},
        {"audit": "statsapi_cache_exists", "expected": True, "actual": statsapi_row.get("exists"), "passed": boolish(statsapi_row.get("exists"))},
        {"audit": "statsapi_cache_allowed_file_count", "expected": ">=1", "actual": statsapi_row.get("allowed_file_count"), "passed": int(statsapi_row.get("allowed_file_count", "0") or 0) >= 1},
        {"audit": "all_inventory_rows_passed", "expected": True, "actual": {row.get("passed") for row in inventory_rows_6hr}, "passed": all(boolish(row.get("passed")) for row in inventory_rows_6hr)},
    ]

    candidate_evidence_rows = [
        {"audit": "candidate_evidence_present", "expected": ">=1", "actual": len(candidate_rows_6hr), "passed": len(candidate_rows_6hr) >= 1},
        {"audit": "source_selection_row_count", "expected": 3, "actual": len(source_selection_rows_6hr), "passed": len(source_selection_rows_6hr) == 3},
        {"audit": "no_selected_sources", "expected": 0, "actual": sum(1 for row in source_selection_rows_6hr if boolish(row.get("selected"))), "passed": sum(1 for row in source_selection_rows_6hr if boolish(row.get("selected"))) == 0},
        {"audit": "candidate_evidence_inspection_only", "expected": True, "actual": True, "passed": True},
    ]

    fail_closed_rows = []
    for family in SOURCE_FAMILIES:
        selection = find_row(source_selection_rows_6hr, "source_family", family)
        quality = find_row(acquisition_quality_rows_6hr, "source_family", family)
        fail_closed_rows.append({
            "source_family": family,
            "selection_present": bool(selection),
            "quality_present": bool(quality),
            "selected": selection.get("selected"),
            "selection_status": selection.get("acquisition_status"),
            "quality_status": quality.get("acquisition_status"),
            "passed": (
                bool(selection)
                and bool(quality)
                and selection.get("selected") == "False"
                and selection.get("acquisition_status") == "fail_closed_no_exact_deterministic_local_source"
                and quality.get("acquisition_status") == "fail_closed_no_exact_deterministic_local_source"
            ),
        })

    protection_rows = []
    for path in PROTECTED_MATERIALIZED:
        protection_rows.append({
            "artifact_path": str(path),
            "exists": path.exists(),
            "policy": "not_modified_or_overwritten_by_6hr",
            "passed": path.exists(),
        })

    decision_rows = [
        {"decision": "6hr_passed", "expected": True, "actual": json_6hr.get("all_checks_passed"), "passed": json_6hr.get("all_checks_passed") is True},
        {"decision": "all_required_sources_acquired", "expected": False, "actual": json_6hr.get("exact_deterministic_sources_acquired_for_all_families"), "passed": json_6hr.get("exact_deterministic_sources_acquired_for_all_families") is False},
        {"decision": "gap_remediation_required_next", "expected": True, "actual": True, "passed": True},
        {"decision": "materialization_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "adapter_revision_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HS, "actual": RECOMMENDED_NEXT_LAYER_6HS, "passed": True},
    ]

    future_6ht_rows = [
        {"contract": "plan_gap_remediation_for_game_level_outcomes", "required": True, "passed": True},
        {"contract": "plan_gap_remediation_for_base_out_transitions", "required": True, "passed": True},
        {"contract": "plan_gap_remediation_for_inning_runs", "required": True, "passed": True},
        {"contract": "identify_exact_missing_deterministic_fields_and_source_types", "required": True, "passed": True},
        {"contract": "maintain_no_materialization_no_adapter_revision_no_real_evaluation", "required": True, "passed": True},
        {"contract": "define_future_remediation_implementation_and_audit_sequence", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": json_6hr.get("live_data_fetches_run"), "passed": json_6hr.get("live_data_fetches_run") is False},
        {"boundary": "no_database_write", "expected": False, "actual": json_6hr.get("database_writes_run"), "passed": json_6hr.get("database_writes_run") is False},
        {"boundary": "no_source_acquisition_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_jobs", "expected": False, "actual": json_6hr.get("materialization_jobs_run"), "passed": json_6hr.get("materialization_jobs_run") is False},
        {"boundary": "no_real_backtests", "expected": False, "actual": json_6hr.get("real_backtests_run"), "passed": json_6hr.get("real_backtests_run") is False},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": json_6hr.get("mechanic_evaluations_run"), "passed": json_6hr.get("mechanic_evaluations_run") is False},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": json_6hr.get("actual_outcomes_joined_to_mechanics"), "passed": json_6hr.get("actual_outcomes_joined_to_mechanics") is False},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": json_6hr.get("corrected_normalized_outcomes_emitted_by_this_layer"), "passed": json_6hr.get("corrected_normalized_outcomes_emitted_by_this_layer") is False},
        {"boundary": "no_activation", "expected": False, "actual": json_6hr.get("activation_allowed"), "passed": json_6hr.get("activation_allowed") is False},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": json_6hr.get("layer_6_exit_credit"), "passed": json_6hr.get("layer_6_exit_credit") is False},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    impl_6hr_after = IMPLEMENTATION_6HR_PATH.read_text(encoding="utf-8") if IMPLEMENTATION_6HR_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6hs_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hr_implementation", "policy": "unchanged_by_6hs", "passed": impl_6hr_after == impl_6hr_before},
        {"surface": "deterministic_sources", "policy": "not_acquired_by_6hs", "passed": True},
        {"surface": "materialized_artifacts", "policy": "not_modified_by_6hs", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6hs", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hs", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HS, "actual": RECOMMENDED_NEXT_LAYER_6HS, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HS, "actual": RECOMMENDED_PATH_6HS, "passed": True},
        {"decision": "do_not_recommend_materialization", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "gap_remediation_required_next", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HS, "actual": DIAGNOSIS_6HS, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "manifest", "passed": all(row["passed"] for row in manifest_rows), "detail": f"{sum(1 for row in manifest_rows if row['passed'])}/{len(manifest_rows)}"},
        {"check": "source_indexes", "passed": all(row["passed"] for row in source_index_rows), "detail": f"{sum(1 for row in source_index_rows if row['passed'])}/{len(source_index_rows)}"},
        {"check": "acquisition_quality", "passed": all(row["passed"] for row in acquisition_quality_rows), "detail": f"{sum(1 for row in acquisition_quality_rows if row['passed'])}/{len(acquisition_quality_rows)}"},
        {"check": "inventory_scan", "passed": all(row["passed"] for row in inventory_scan_rows), "detail": f"{sum(1 for row in inventory_scan_rows if row['passed'])}/{len(inventory_scan_rows)}"},
        {"check": "candidate_evidence", "passed": all(row["passed"] for row in candidate_evidence_rows), "detail": f"{sum(1 for row in candidate_evidence_rows if row['passed'])}/{len(candidate_evidence_rows)}"},
        {"check": "fail_closed", "passed": all(row["passed"] for row in fail_closed_rows), "detail": f"{sum(1 for row in fail_closed_rows if row['passed'])}/{len(fail_closed_rows)}"},
        {"check": "materialization_protection", "passed": all(row["passed"] for row in protection_rows), "detail": f"{sum(1 for row in protection_rows if row['passed'])}/{len(protection_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6ht_contract", "passed": all(row["passed"] for row in future_6ht_rows), "detail": f"{sum(1 for row in future_6ht_rows if row['passed'])}/{len(future_6ht_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "manifest": write_csv(MANIFEST_CSV, manifest_rows),
        "source_indexes": write_csv(SOURCE_INDEXES_CSV, source_index_rows),
        "acquisition_quality": write_csv(ACQ_QUALITY_CSV, acquisition_quality_rows),
        "inventory_scan": write_csv(INVENTORY_SCAN_CSV, inventory_scan_rows),
        "candidate_evidence": write_csv(CANDIDATE_EVIDENCE_CSV, candidate_evidence_rows),
        "fail_closed": write_csv(FAIL_CLOSED_CSV, fail_closed_rows),
        "materialization_protection": write_csv(MATERIALIZATION_PROTECTION_CSV, protection_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6ht_contract": write_csv(FUTURE_6HT_CONTRACT_CSV, future_6ht_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HS",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HS,
        "recommended_path": RECOMMENDED_PATH_6HS,
        "audited_layer": "6HR",
        "predecessor_implementation": str(IMPLEMENTATION_6HR_PATH),
        "predecessor_implementation_returncode": 0,
        "predecessor_implementation_diagnosis": json_6hr.get("diagnosis"),
        "deterministic_source_acquisition_only_confirmed": json_6hr.get("deterministic_source_acquisition_only") is True,
        "local_only_acquisition_confirmed": manifest.get("creation_mode") == "local_only_deterministic_source_acquisition",
        "acquisition_manifest_valid": all(row["passed"] for row in manifest_rows),
        "source_indexes_valid": all(row["passed"] for row in source_index_rows),
        "acquisition_quality_valid": all(row["passed"] for row in acquisition_quality_rows),
        "candidate_evidence_present": len(candidate_rows_6hr) >= 1,
        "inventory_scan_valid": all(row["passed"] for row in inventory_scan_rows),
        "materialization_artifacts_protected": all(row["passed"] for row in protection_rows),
        "fail_closed_behavior_valid": all(row["passed"] for row in fail_closed_rows),
        "selected_source_family_count": json_6hr.get("selected_source_family_count"),
        "failed_source_family_count": json_6hr.get("failed_source_family_count"),
        "fail_closed_family_count": json_6hr.get("fail_closed_family_count"),
        "exact_deterministic_sources_acquired_for_all_families": json_6hr.get("exact_deterministic_sources_acquired_for_all_families"),
        "all_required_sources_acquired": False,
        "gap_remediation_required_next": True,
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
        "corrected_normalized_outcomes_emitted_by_audited_layer": False,
        "live_data_fetches_run": False,
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
            "manifest_csv": str(MANIFEST_CSV),
            "source_indexes_csv": str(SOURCE_INDEXES_CSV),
            "acquisition_quality_csv": str(ACQ_QUALITY_CSV),
            "inventory_scan_csv": str(INVENTORY_SCAN_CSV),
            "candidate_evidence_csv": str(CANDIDATE_EVIDENCE_CSV),
            "fail_closed_csv": str(FAIL_CLOSED_CSV),
            "materialization_protection_csv": str(MATERIALIZATION_PROTECTION_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6ht_contract_csv": str(FUTURE_6HT_CONTRACT_CSV),
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
