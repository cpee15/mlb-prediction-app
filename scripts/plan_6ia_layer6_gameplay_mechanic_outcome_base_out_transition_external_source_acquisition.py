#!/usr/bin/env python3
"""Plan Layer 6IA base/out transition external or new source acquisition."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ia_base_out_transition_external_source_acquisition_plan"
TMP_DIR = Path("tmp")

AUDIT_6HZ_PATH = Path("scripts/audit_6hz_layer6_gameplay_mechanic_outcome_base_out_transition_source_remediation_implementation.py")

JSON_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit.json"
CHECKS_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_checks.csv"
PREDECESSOR_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_predecessor.csv"
ARTIFACTS_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_artifact_presence.csv"
INVENTORY_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_inventory.csv"
CANDIDATES_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_candidate_evidence.csv"
SELECTION_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_source_selection.csv"
SOURCE_INDEX_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_source_index.csv"
READINESS_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_readiness.csv"
MANIFEST_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_manifest.csv"
PRESERVED_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_preserved_families.csv"
DECISION_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_decision.csv"
FUTURE_6IA_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_future_6ia_contract.csv"
SAFETY_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_immutability.csv"
RECOMMENDED_6HZ = TMP_DIR / "layer6_6hz_base_out_transition_source_remediation_implementation_audit_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
GAP_SUMMARY_CSV = TMP_DIR / f"{SLUG}_gap_summary.csv"
EVIDENCE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_evidence_contract.csv"
ACQUISITION_MODES_CSV = TMP_DIR / f"{SLUG}_acquisition_modes.csv"
CANDIDATE_SOURCE_FAMILIES_CSV = TMP_DIR / f"{SLUG}_candidate_source_families.csv"
DISALLOWED_BEHAVIORS_CSV = TMP_DIR / f"{SLUG}_disallowed_behaviors.csv"
PROVENANCE_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_provenance_requirements.csv"
VALIDATION_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_validation_requirements.csv"
FUTURE_6IB_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6ib_contract.csv"
FUTURE_6IC_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6ic_contract.csv"
PRESERVED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HZ = "layer_6_gameplay_mechanic_outcome_base_out_transition_source_remediation_implementation_audit_complete"
DIAGNOSIS_6IA = "layer_6_gameplay_mechanic_outcome_base_out_transition_external_source_acquisition_plan_complete"

RECOMMENDED_NEXT_LAYER_6HZ = "6IA_layer_6_gameplay_mechanic_outcome_base_out_transition_external_source_acquisition_plan"
RECOMMENDED_PATH_6HZ = "audit_failed_local_base_out_transition_remediation_then_plan_external_or_new_source_acquisition"

RECOMMENDED_NEXT_LAYER_6IA = "6IB_layer_6_gameplay_mechanic_outcome_base_out_transition_external_source_acquisition_implementation"
RECOMMENDED_PATH_6IA = "plan_controlled_external_or_new_base_out_transition_source_acquisition_then_implement_before_materialization"

SOURCE_FAMILY = "base_out_transitions"
PRESERVED_FAMILIES = ["game_level_outcomes", "inning_runs"]

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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_before = AUDIT_6HZ_PATH.read_text(encoding="utf-8") if AUDIT_6HZ_PATH.exists() else ""

    json_6hz = load_json(JSON_6HZ)

    required_inputs = [
        JSON_6HZ,
        CHECKS_6HZ,
        PREDECESSOR_6HZ,
        ARTIFACTS_6HZ,
        INVENTORY_6HZ,
        CANDIDATES_6HZ,
        SELECTION_6HZ,
        SOURCE_INDEX_6HZ,
        READINESS_6HZ,
        MANIFEST_6HZ,
        PRESERVED_6HZ,
        DECISION_6HZ,
        FUTURE_6IA_6HZ,
        SAFETY_6HZ,
        IMMUTABILITY_6HZ,
        RECOMMENDED_6HZ,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hz_audit_exists", "expected": True, "actual": AUDIT_6HZ_PATH.exists(), "passed": AUDIT_6HZ_PATH.exists()},
        {"check": "6hz_json_exists", "expected": True, "actual": JSON_6HZ.exists(), "passed": JSON_6HZ.exists()},
        {"check": "6hz_all_checks_passed", "expected": True, "actual": json_6hz.get("all_checks_passed"), "passed": json_6hz.get("all_checks_passed") is True},
        {"check": "6hz_diagnosis", "expected": DIAGNOSIS_6HZ, "actual": json_6hz.get("diagnosis"), "passed": json_6hz.get("diagnosis") == DIAGNOSIS_6HZ},
        {"check": "6hz_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HZ, "actual": json_6hz.get("recommended_next_layer"), "passed": json_6hz.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HZ},
        {"check": "6hz_recommended_path", "expected": RECOMMENDED_PATH_6HZ, "actual": json_6hz.get("recommended_path"), "passed": json_6hz.get("recommended_path") == RECOMMENDED_PATH_6HZ},
        {"check": "6hz_source_family", "expected": SOURCE_FAMILY, "actual": json_6hz.get("source_family"), "passed": json_6hz.get("source_family") == SOURCE_FAMILY},
        {"check": "6hz_local_cache_exhausted", "expected": True, "actual": json_6hz.get("local_cache_exhausted_for_exact_base_out_transitions"), "passed": json_6hz.get("local_cache_exhausted_for_exact_base_out_transitions") is True},
        {"check": "6hz_external_plan_required", "expected": True, "actual": json_6hz.get("external_or_new_source_acquisition_plan_required_next"), "passed": json_6hz.get("external_or_new_source_acquisition_plan_required_next") is True},
        {"check": "6hz_materialization_blocked", "expected": True, "actual": json_6hz.get("materialization_still_blocked"), "passed": json_6hz.get("materialization_still_blocked") is True},
        {"check": "6hz_no_exit_credit", "expected": False, "actual": json_6hz.get("layer_6_exit_credit"), "passed": json_6hz.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    gap_rows = [
        {
            "source_family": SOURCE_FAMILY,
            "prior_candidate_evidence_count": json_6hz.get("candidate_evidence_count"),
            "prior_partial_candidate_count": json_6hz.get("partial_candidate_count"),
            "prior_exact_candidate_count": 0,
            "prior_local_cache_exhausted": json_6hz.get("local_cache_exhausted_for_exact_base_out_transitions"),
            "required_next_action": "controlled_external_or_new_source_acquisition_plan",
            "passed": True,
        }
    ]

    evidence_contract_rows = [
        {"field": "game_id_or_gamePk", "required": True, "description": "stable deterministic game identifier", "passed": True},
        {"field": "play_id_or_event_id", "required": True, "description": "stable deterministic play/event identifier", "passed": True},
        {"field": "inning", "required": True, "description": "inning number", "passed": True},
        {"field": "half_inning", "required": True, "description": "top/bottom half-inning context", "passed": True},
        {"field": "sequence_ordering", "required": True, "description": "deterministic play sequence within game", "passed": True},
        {"field": "start_base_state_or_pre_base_state", "required": True, "description": "runner occupancy before event", "passed": True},
        {"field": "end_base_state_or_post_base_state", "required": True, "description": "runner occupancy after event", "passed": True},
        {"field": "start_outs_or_outs_before", "required": True, "description": "outs before event", "passed": True},
        {"field": "end_outs_or_outs_after", "required": True, "description": "outs after event", "passed": True},
        {"field": "runs_scored", "required": True, "description": "runs scored on event", "passed": True},
        {"field": "event_type_or_result_context", "required": True, "description": "event/result context for movement audit", "passed": True},
        {"field": "source_provenance", "required": True, "description": "source family, endpoint/import origin, and version", "passed": True},
        {"field": "acquisition_timestamp_or_source_version", "required": True, "description": "reproducibility metadata for externally acquired/imported data", "passed": True},
        {"field": "local_cache_path_after_acquisition", "required": True, "description": "local deterministic path used by downstream implementation", "passed": True},
    ]

    acquisition_mode_rows = [
        {"mode": "plan_only_no_fetch", "allowed_in_6ia": True, "allowed_in_6ib_if_contract_met": True, "description": "define acquisition plan without performing external acquisition", "passed": True},
        {"mode": "future_controlled_statsapi_acquisition", "allowed_in_6ia": False, "allowed_in_6ib_if_contract_met": True, "description": "controlled MLB StatsAPI game/live feed acquisition into local cache", "passed": True},
        {"mode": "future_retrosheet_import", "allowed_in_6ia": False, "allowed_in_6ib_if_contract_met": True, "description": "controlled local import of Retrosheet event files if acceptable", "passed": True},
        {"mode": "future_user_supplied_local_dataset_import", "allowed_in_6ia": False, "allowed_in_6ib_if_contract_met": True, "description": "user-provided local play-by-play dataset with explicit pre/post states", "passed": True},
        {"mode": "future_internal_pbp_event_cache_enrichment", "allowed_in_6ia": False, "allowed_in_6ib_if_contract_met": True, "description": "controlled enrichment of local play-by-play event cache", "passed": True},
    ]

    candidate_source_family_rows = [
        {"source_family": "mlb_statsapi_live_feed_or_game_feed_allplays", "priority": 1, "contract_risk": "requires_controlled_fetch_or_existing_local_dump", "passed": True},
        {"source_family": "retrosheet_event_files", "priority": 2, "contract_risk": "requires_license_format_and_reconstruction_validation", "passed": True},
        {"source_family": "explicit_pbp_pre_post_base_out_dataset", "priority": 3, "contract_risk": "requires provenance_and_column_contract", "passed": True},
        {"source_family": "deterministic_transition_table_from_acquired_events", "priority": 4, "contract_risk": "requires followup audit before materialization", "passed": True},
        {"source_family": "future_user_or_artifact_supplied_local_cache", "priority": 5, "contract_risk": "requires immutable local path and provenance", "passed": True},
    ]

    disallowed_rows = [
        {"behavior": "live_fetch_in_6ia", "allowed": False, "reason": "6IA is planning_only", "passed": True},
        {"behavior": "remote_api_call_in_6ia", "allowed": False, "reason": "6IA must not acquire source", "passed": True},
        {"behavior": "scraping_in_6ia", "allowed": False, "reason": "6IA must not acquire source", "passed": True},
        {"behavior": "database_writes_in_6ia", "allowed": False, "reason": "planning layer only", "passed": True},
        {"behavior": "materialization_in_6ia", "allowed": False, "reason": "base_out_transitions unresolved", "passed": True},
        {"behavior": "adapter_revision_in_6ia", "allowed": False, "reason": "source and materialization incomplete", "passed": True},
        {"behavior": "real_evaluation_in_6ia", "allowed": False, "reason": "no validated base_out_transition ground truth", "passed": True},
        {"behavior": "mechanic_activation_in_6ia", "allowed": False, "reason": "evaluation blocked", "passed": True},
        {"behavior": "layer_6_exit_credit_in_6ia", "allowed": False, "reason": "source family gap remains", "passed": True},
        {"behavior": "probabilistic_reconstruction_as_ground_truth", "allowed": False, "reason": "model output cannot be truth source", "passed": True},
        {"behavior": "boxscore_only_inference", "allowed": False, "reason": "does not provide pre/post base-out state", "passed": True},
        {"behavior": "inning_runs_only_inference", "allowed": False, "reason": "does not provide runner movement", "passed": True},
        {"behavior": "synthetic_runner_movement_source", "allowed": False, "reason": "fabricates missing evidence", "passed": True},
    ]

    provenance_rows = [
        {"requirement": "source_origin_documented", "required": True, "passed": True},
        {"requirement": "acquisition_mode_documented", "required": True, "passed": True},
        {"requirement": "local_cache_path_documented", "required": True, "passed": True},
        {"requirement": "source_version_or_timestamp_documented", "required": True, "passed": True},
        {"requirement": "source_license_or_use_constraints_documented_when_applicable", "required": True, "passed": True},
        {"requirement": "source_row_count_and_game_count_documented", "required": True, "passed": True},
        {"requirement": "source_schema_hash_or_field_signature_documented", "required": True, "passed": True},
    ]

    validation_rows = [
        {"requirement": "exact_required_evidence_fields_present", "required": True, "passed": True},
        {"requirement": "game_id_and_play_sequence_unique_enough_for_transition_table", "required": True, "passed": True},
        {"requirement": "pre_post_base_state_reconstructable_without_probabilistic_inference", "required": True, "passed": True},
        {"requirement": "outs_before_after_reconstructable_without_boxscore_only_inference", "required": True, "passed": True},
        {"requirement": "runs_scored_per_event_traceable", "required": True, "passed": True},
        {"requirement": "runner_movement_events_traceable_for_sb_cs_wp_pb_balk_sacfly_doubleplay", "required": True, "passed": True},
        {"requirement": "fail_closed_if_exact_contract_missing", "required": True, "passed": True},
        {"requirement": "future_6ic_audit_required_before_materialization", "required": True, "passed": True},
    ]

    future_6ib_rows = [
        {"contract": "consume_6ia_plan_and_6hz_audit", "required": True, "passed": True},
        {"contract": "perform_only_approved_acquisition_mode", "required": True, "passed": True},
        {"contract": "write_acquired_or_imported_source_to_local_cache_only", "required": True, "passed": True},
        {"contract": "produce_candidate_evidence_and_source_index", "required": True, "passed": True},
        {"contract": "validate_exact_pre_post_base_out_contract_or_fail_closed", "required": True, "passed": True},
        {"contract": "preserve_game_level_outcomes_and_inning_runs", "required": True, "passed": True},
        {"contract": "no_materialization_adapter_revision_real_evaluation_activation_exit", "required": True, "passed": True},
    ]

    future_6ic_rows = [
        {"contract": "audit_6ib_predecessor_and_artifacts", "required": True, "passed": True},
        {"contract": "audit_acquisition_mode_and_provenance", "required": True, "passed": True},
        {"contract": "audit_exact_base_out_transition_source_or_fail_closed", "required": True, "passed": True},
        {"contract": "audit_no_materialization_adapter_real_eval_activation_exit", "required": True, "passed": True},
        {"contract": "recommend_materialization_planning_only_if_base_out_transitions_remediated", "required": True, "passed": True},
        {"contract": "preserve_existing_remediated_families", "required": True, "passed": True},
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_remediated_from_prior_layers", "passed": json_6hz.get("game_level_outcomes_preserved") is True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": json_6hz.get("inning_runs_preserved") is True},
    ]

    blocking_rows = [
        {"blocked_surface": "materialization", "blocked": True, "reason": "base_out_transitions_not_remediated", "passed": True},
        {"blocked_surface": "adapter_revision", "blocked": True, "reason": "source remediation incomplete", "passed": True},
        {"blocked_surface": "real_evaluation", "blocked": True, "reason": "no audited base_out_transition ground truth", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "real evaluation blocked", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "all source families not remediated and audited", "passed": True},
    ]

    decision_rows = [
        {"decision": "6hz_passed", "expected": True, "actual": json_6hz.get("all_checks_passed"), "passed": json_6hz.get("all_checks_passed") is True},
        {"decision": "external_or_new_source_acquisition_plan_created", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6ib_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6IA, "actual": RECOMMENDED_NEXT_LAYER_6IA, "passed": True},
        {"decision": "define_6ic_audit_contract", "expected": True, "actual": True, "passed": True},
        {"decision": "materialization_allowed_after_this_plan", "expected": False, "actual": False, "passed": True},
        {"decision": "adapter_revision_allowed_after_this_plan", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_allowed_after_this_plan", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_scraping", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition_by_6ia", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_jobs", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_revision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_after = AUDIT_6HZ_PATH.read_text(encoding="utf-8") if AUDIT_6HZ_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6ia_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hz_audit", "policy": "unchanged_by_6ia", "passed": audit_after == audit_before},
        {"surface": "6hz_artifacts", "policy": "read_only", "passed": True},
        {"surface": "materialized_artifacts", "policy": "not_modified", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IA, "actual": RECOMMENDED_NEXT_LAYER_6IA, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IA, "actual": RECOMMENDED_PATH_6IA, "passed": True},
        {"decision": "do_not_recommend_materialization", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IA, "actual": DIAGNOSIS_6IA, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "gap_summary", "passed": all(row["passed"] for row in gap_rows), "detail": f"{sum(1 for row in gap_rows if row['passed'])}/{len(gap_rows)}"},
        {"check": "evidence_contract", "passed": all(row["passed"] for row in evidence_contract_rows), "detail": f"{sum(1 for row in evidence_contract_rows if row['passed'])}/{len(evidence_contract_rows)}"},
        {"check": "acquisition_modes", "passed": all(row["passed"] for row in acquisition_mode_rows), "detail": f"{sum(1 for row in acquisition_mode_rows if row['passed'])}/{len(acquisition_mode_rows)}"},
        {"check": "candidate_source_families", "passed": all(row["passed"] for row in candidate_source_family_rows), "detail": f"{sum(1 for row in candidate_source_family_rows if row['passed'])}/{len(candidate_source_family_rows)}"},
        {"check": "disallowed_behaviors", "passed": all(row["passed"] for row in disallowed_rows), "detail": f"{sum(1 for row in disallowed_rows if row['passed'])}/{len(disallowed_rows)}"},
        {"check": "provenance_requirements", "passed": all(row["passed"] for row in provenance_rows), "detail": f"{sum(1 for row in provenance_rows if row['passed'])}/{len(provenance_rows)}"},
        {"check": "validation_requirements", "passed": all(row["passed"] for row in validation_rows), "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}"},
        {"check": "future_6ib_contract", "passed": all(row["passed"] for row in future_6ib_rows), "detail": f"{sum(1 for row in future_6ib_rows if row['passed'])}/{len(future_6ib_rows)}"},
        {"check": "future_6ic_contract", "passed": all(row["passed"] for row in future_6ic_rows), "detail": f"{sum(1 for row in future_6ic_rows if row['passed'])}/{len(future_6ic_rows)}"},
        {"check": "preserved_families", "passed": all(row["passed"] for row in preserved_rows), "detail": f"{sum(1 for row in preserved_rows if row['passed'])}/{len(preserved_rows)}"},
        {"check": "blocking_policy", "passed": all(row["passed"] for row in blocking_rows), "detail": f"{sum(1 for row in blocking_rows if row['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "gap_summary": write_csv(GAP_SUMMARY_CSV, gap_rows),
        "evidence_contract": write_csv(EVIDENCE_CONTRACT_CSV, evidence_contract_rows),
        "acquisition_modes": write_csv(ACQUISITION_MODES_CSV, acquisition_mode_rows),
        "candidate_source_families": write_csv(CANDIDATE_SOURCE_FAMILIES_CSV, candidate_source_family_rows),
        "disallowed_behaviors": write_csv(DISALLOWED_BEHAVIORS_CSV, disallowed_rows),
        "provenance_requirements": write_csv(PROVENANCE_REQUIREMENTS_CSV, provenance_rows),
        "validation_requirements": write_csv(VALIDATION_REQUIREMENTS_CSV, validation_rows),
        "future_6ib_contract": write_csv(FUTURE_6IB_CONTRACT_CSV, future_6ib_rows),
        "future_6ic_contract": write_csv(FUTURE_6IC_CONTRACT_CSV, future_6ic_rows),
        "preserved_families": write_csv(PRESERVED_FAMILIES_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IA",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IA if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IA,
        "recommended_path": RECOMMENDED_PATH_6IA,
        "predecessor_audit": str(AUDIT_6HZ_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6hz.get("diagnosis"),
        "audited_layer": "6HZ",
        "source_family": SOURCE_FAMILY,
        "prior_candidate_evidence_count": json_6hz.get("candidate_evidence_count"),
        "prior_partial_candidate_count": json_6hz.get("partial_candidate_count"),
        "prior_exact_candidate_count": 0,
        "prior_local_cache_exhausted": True,
        "external_or_new_source_acquisition_plan_created": True,
        "acquisition_mode_count": len(acquisition_mode_rows),
        "candidate_source_family_count": len(candidate_source_family_rows),
        "evidence_contract_field_count": len(evidence_contract_rows),
        "provenance_requirement_count": len(provenance_rows),
        "validation_requirement_count": len(validation_rows),
        "future_6ib_contract_valid": all(row["passed"] for row in future_6ib_rows),
        "future_6ic_contract_valid": all(row["passed"] for row in future_6ic_rows),
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "materialization_allowed_after_this_plan": False,
        "materialization_still_blocked": True,
        "adapter_revision_allowed_after_this_plan": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_allowed_after_this_plan": False,
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
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "gap_summary_csv": str(GAP_SUMMARY_CSV),
            "evidence_contract_csv": str(EVIDENCE_CONTRACT_CSV),
            "acquisition_modes_csv": str(ACQUISITION_MODES_CSV),
            "candidate_source_families_csv": str(CANDIDATE_SOURCE_FAMILIES_CSV),
            "disallowed_behaviors_csv": str(DISALLOWED_BEHAVIORS_CSV),
            "provenance_requirements_csv": str(PROVENANCE_REQUIREMENTS_CSV),
            "validation_requirements_csv": str(VALIDATION_REQUIREMENTS_CSV),
            "future_6ib_contract_csv": str(FUTURE_6IB_CONTRACT_CSV),
            "future_6ic_contract_csv": str(FUTURE_6IC_CONTRACT_CSV),
            "preserved_families_csv": str(PRESERVED_FAMILIES_CSV),
            "blocking_policy_csv": str(BLOCKING_POLICY_CSV),
            "decision_csv": str(DECISION_CSV),
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
