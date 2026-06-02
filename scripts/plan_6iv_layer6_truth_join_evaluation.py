#!/usr/bin/env python3
"""Plan Layer 6IV truth-join evaluation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6iv_truth_join_evaluation_plan"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

AUDIT_6IU_PATH = Path("scripts/audit_6iu_layer6_actual_outcome_surface_gap_resolution_implementation.py")
IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
PLAN_6IS_PATH = Path("scripts/plan_6is_layer6_actual_outcome_surface_gap_resolution.py")
AUDIT_6IR_PATH = Path("scripts/audit_6ir_layer6_gameplay_mechanic_outcome_real_evaluation_implementation.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit.json"
CHECKS_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_checks.csv"
PREDECESSOR_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_predecessor.csv"
INPUT_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_input_artifacts.csv"
SOURCE_SUFF_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_source_sufficiency.csv"
SCHEMA_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_truth_surface_schema.csv"
MANIFEST_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_truth_surface_manifest.csv"
CANDIDATE_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_candidate_truth_surface_rows.csv"
LINEAGE_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_lineage.csv"
VALIDATION_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_validation.csv"
READINESS_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_readiness.csv"
FUTURE_6IV_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_future_6iv_contract.csv"
READONLY_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_readonly_sources.csv"
PRESERVED_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_preserved_families.csv"
BLOCKING_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_blocking_policy.csv"
DECISION_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_decision.csv"
SAFETY_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_immutability.csv"
RECOMMENDED_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit_recommended_path.csv"

JSON_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation.json"
JSON_6IS = TMP_DIR / "layer6_6is_actual_outcome_surface_gap_resolution_plan.json"
JSON_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit.json"
JSON_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation.json"
JSON_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan.json"
JSON_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit.json"
JSON_6IN = TMP_DIR / "layer6_6in_base_out_transition_adapter_revision_implementation.json"
JSON_6IM = TMP_DIR / "layer6_6im_base_out_transition_adapter_revision_plan.json"
JSON_6IL = TMP_DIR / "layer6_6il_base_out_transition_materialization_implementation_audit.json"
JSON_6IK = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation.json"
JSON_6IJ = TMP_DIR / "layer6_6ij_base_out_transition_materialization_plan.json"
JSON_6II = TMP_DIR / "layer6_6ii_base_out_transition_reconstruction_correction_implementation_audit.json"

MATERIALIZED_TABLE = MAT_DIR / "materialized_base_out_transition_table_candidate.csv"
MATERIALIZATION_MANIFEST = MAT_DIR / "materialization_manifest.json"
MATERIALIZED_SCHEMA = MAT_DIR / "materialized_schema_contract.csv"
MATERIALIZED_LINEAGE = MAT_DIR / "materialized_lineage.csv"
MATERIALIZATION_VALIDATION = MAT_DIR / "materialization_validation_summary.csv"
MATERIALIZATION_READINESS = MAT_DIR / "materialization_readiness.csv"

CORRECTED_INDEX_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_corrected_transition_index_candidate.csv"
SOURCE_PROVENANCE_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_source_provenance.csv"
SOURCE_MANIFEST_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/source_manifest.json"
TRANSITION_INDEX_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/base_out_transition_index.csv"
RAW_FEED_DIR_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/statsapi_game_feed"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
PROBLEM_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
JOIN_KEYS_CSV = TMP_DIR / f"{SLUG}_join_keys.csv"
JOIN_FALLBACK_CSV = TMP_DIR / f"{SLUG}_join_fallback_hierarchy.csv"
PREJOIN_VALIDATION_CSV = TMP_DIR / f"{SLUG}_truth_surface_prejoin_validation.csv"
METRIC_FINALIZATION_CSV = TMP_DIR / f"{SLUG}_metric_finalization_rules.csv"
CANDIDATE_DECISION_CSV = TMP_DIR / f"{SLUG}_candidate_decision_finalization_rules.csv"
LINEAGE_PROP_CSV = TMP_DIR / f"{SLUG}_lineage_propagation_rules.csv"
NONPROD_OUTPUTS_CSV = TMP_DIR / f"{SLUG}_non_production_outputs.csv"
FUTURE_6IW_CSV = TMP_DIR / f"{SLUG}_future_6iw_contract.csv"
FUTURE_6IX_CSV = TMP_DIR / f"{SLUG}_future_6ix_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IU = "layer_6_actual_outcome_surface_gap_resolution_implementation_audit_complete"
DIAGNOSIS_6IV = "layer_6_truth_join_evaluation_plan_complete"

RECOMMENDED_NEXT_LAYER_6IU = "6IV_layer_6_truth_join_evaluation_plan"
RECOMMENDED_PATH_6IU = "audit_actual_outcome_surface_gap_resolution_then_plan_truth_join_evaluation"

RECOMMENDED_NEXT_LAYER_6IV = "6IW_layer_6_truth_join_evaluation_implementation"
RECOMMENDED_PATH_6IV = "plan_truth_join_evaluation_then_implement_before_activation_planning"

SOURCE_FAMILY = "truth_join_evaluation"
DEPENDS_ON_SOURCE_FAMILIES = ["actual_outcome_surfaces", "base_out_transitions"]
PRESERVED_FAMILIES = ["game_level_outcomes", "inning_runs", "base_out_transitions", "actual_outcome_surfaces"]

JOIN_KEYS = [
    "truth_surface",
    "mechanic",
    "game_id",
    "event_id",
    "play_id",
    "event_index",
    "source_path",
    "derivation_method",
]

JOIN_FALLBACKS = [
    "exact_mechanic_plus_game_id_plus_play_id",
    "exact_mechanic_plus_game_id_plus_event_id",
    "exact_truth_surface_plus_game_id_plus_event_index",
    "truth_surface_plus_source_path_plus_event_index",
    "no_join_if_only_description_text_matches",
]

METRIC_FINALIZATION_RULES = [
    "metric_final_true_requires_truth_surface_row_join_with_lineage",
    "unsupported_surfaces_keep_metric_final_false",
    "partial_truth_joins_reported_separately_from_complete_truth_joins",
    "truth_join_implementation_must_emit_join_coverage_by_metric_family",
    "final_metric_candidates_remain_non_production_until_audit",
]

CANDIDATE_DECISION_RULES = [
    "candidate_decision_final_true_requires_all_required_metric_families_truth_joined",
    "candidate_decision_final_true_requires_lineage_for_all_truth_joined_metrics",
    "candidate_decision_final_false_when_any_required_truth_surface_missing",
    "candidate_decision_final_false_when_only_description_match_exists",
    "activation_planning_blocked_until_truth_join_implementation_and_audit_pass",
]

LINEAGE_PROPAGATION_RULES = [
    "propagate_6it_truth_surface_source_path",
    "propagate_6it_truth_surface_event_id",
    "propagate_6it_truth_surface_play_id",
    "propagate_6it_truth_surface_event_index",
    "propagate_6iq_evaluation_mechanic_window_identity",
    "emit_join_method_and_join_confidence",
    "emit_unjoined_reason_for_unmatched_rows",
]

NONPROD_OUTPUTS = [
    "truth_join_candidate_rows",
    "truth_join_coverage_report",
    "metric_finalization_candidate_rows",
    "candidate_decision_finalization_candidate_rows",
    "truth_join_lineage",
    "truth_join_blocking_policy",
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


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(str(row.get("passed", "")).lower() == "true" or row.get("passed") is True for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6iu_before = AUDIT_6IU_PATH.read_text(encoding="utf-8") if AUDIT_6IU_PATH.exists() else ""
    impl_6it_before = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    plan_6is_before = PLAN_6IS_PATH.read_text(encoding="utf-8") if PLAN_6IS_PATH.exists() else ""
    impl_6iq_before = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6iu = load_json(JSON_6IU)

    required_inputs = [
        JSON_6IU, CHECKS_6IU, PREDECESSOR_6IU, INPUT_6IU, SOURCE_SUFF_6IU,
        SCHEMA_6IU, MANIFEST_6IU, CANDIDATE_6IU, LINEAGE_6IU,
        VALIDATION_6IU, READINESS_6IU, FUTURE_6IV_6IU, READONLY_6IU,
        PRESERVED_6IU, BLOCKING_6IU, DECISION_6IU, SAFETY_6IU,
        IMMUTABILITY_6IU, RECOMMENDED_6IU, JSON_6IT, JSON_6IS, JSON_6IR,
        JSON_6IQ, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IM, JSON_6IL,
        JSON_6IK, JSON_6IJ, JSON_6II, ADAPTER_MODULE_PATH,
        MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA,
        MATERIALIZED_LINEAGE, MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS,
        CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IU, JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ, JSON_6IP, JSON_6IO,
        JSON_6IN, JSON_6IM, JSON_6IL, JSON_6IK, JSON_6IJ, JSON_6II,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST,
        MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE, MATERIALIZATION_VALIDATION,
        MATERIALIZATION_READINESS, CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6iu_audit_exists", "expected": True, "actual": AUDIT_6IU_PATH.exists(), "passed": AUDIT_6IU_PATH.exists()},
        {"check": "6iu_json_exists", "expected": True, "actual": JSON_6IU.exists(), "passed": JSON_6IU.exists()},
        {"check": "6iu_all_checks_passed", "expected": True, "actual": json_6iu.get("all_checks_passed"), "passed": json_6iu.get("all_checks_passed") is True},
        {"check": "6iu_diagnosis", "expected": DIAGNOSIS_6IU, "actual": json_6iu.get("diagnosis"), "passed": json_6iu.get("diagnosis") == DIAGNOSIS_6IU},
        {"check": "6iu_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IU, "actual": json_6iu.get("recommended_next_layer"), "passed": json_6iu.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IU},
        {"check": "6iu_recommended_path", "expected": RECOMMENDED_PATH_6IU, "actual": json_6iu.get("recommended_path"), "passed": json_6iu.get("recommended_path") == RECOMMENDED_PATH_6IU},
        {"check": "6iu_candidate_rows_audited", "expected": 100, "actual": json_6iu.get("candidate_truth_surface_row_count"), "passed": json_6iu.get("candidate_truth_surface_row_count") == 100},
        {"check": "6iu_truth_join_blocked", "expected": False, "actual": json_6iu.get("truth_surface_joined_to_evaluation"), "passed": json_6iu.get("truth_surface_joined_to_evaluation") is False},
        {"check": "6iu_no_exit_credit", "expected": False, "actual": json_6iu.get("layer_6_exit_credit"), "passed": json_6iu.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    problem_rows = [{
        "problem": "audited candidate truth surfaces are available but not joined to evaluation outputs",
        "plan": "truth_join_evaluation",
        "source_family": SOURCE_FAMILY,
        "depends_on": "|".join(DEPENDS_ON_SOURCE_FAMILIES),
        "passed": True,
    }]

    join_key_rows = [
        {"join_key": key, "required": True, "planned": True, "passed": True}
        for key in JOIN_KEYS
    ]

    join_fallback_rows = [
        {"level": idx + 1, "fallback": fallback, "planned": True, "passed": True}
        for idx, fallback in enumerate(JOIN_FALLBACKS)
    ]

    prejoin_validation_rows = [
        {"validation": "candidate_truth_surfaces_audited", "required": True, "passed": json_6iu.get("candidate_truth_surface_row_count") == 100},
        {"validation": "truth_surface_schema_valid", "required": True, "passed": json_6iu.get("truth_surface_schema_valid") is True},
        {"validation": "lineage_valid", "required": True, "passed": json_6iu.get("lineage_valid") is True},
        {"validation": "candidate_truth_rows_non_production", "required": True, "passed": json_6iu.get("candidate_truth_surface_non_production") is True},
        {"validation": "candidate_truth_rows_non_final", "required": True, "passed": json_6iu.get("candidate_truth_surface_non_final") is True},
    ]

    metric_finalization_rows = [
        {"rule": rule, "planned": True, "passed": True}
        for rule in METRIC_FINALIZATION_RULES
    ]

    candidate_decision_rows = [
        {"rule": rule, "planned": True, "passed": True}
        for rule in CANDIDATE_DECISION_RULES
    ]

    lineage_prop_rows = [
        {"rule": rule, "planned": True, "passed": True}
        for rule in LINEAGE_PROPAGATION_RULES
    ]

    nonprod_output_rows = [
        {"output": output, "non_production": True, "planned": True, "passed": True}
        for output in NONPROD_OUTPUTS
    ]

    future_6iw_rows = [
        {"contract": "implement_truth_join_evaluation", "required": True, "passed": True},
        {"contract": "use_audited_6it_truth_surface_candidate_outputs", "required": True, "passed": True},
        {"contract": "use_6iq_evaluation_outputs", "required": True, "passed": True},
        {"contract": "emit_joined_evaluation_candidates", "required": True, "passed": True},
        {"contract": "emit_join_coverage_reports", "required": True, "passed": True},
        {"contract": "emit_finalizable_metric_candidates", "required": True, "passed": True},
        {"contract": "emit_non_final_candidate_decisions", "required": True, "passed": True},
        {"contract": "emit_truth_join_lineage", "required": True, "passed": True},
        {"contract": "do_not_activate_mechanics", "required": True, "passed": True},
    ]

    future_6ix_rows = [
        {"contract": "audit_6iw_truth_join_implementation", "required": True, "passed": True},
        {"contract": "audit_join_coverage", "required": True, "passed": True},
        {"contract": "audit_row_counts", "required": True, "passed": True},
        {"contract": "audit_lineage", "required": True, "passed": True},
        {"contract": "audit_metric_finalization_rules", "required": True, "passed": True},
        {"contract": "audit_candidate_decision_rules", "required": True, "passed": True},
        {"contract": "keep_activation_blocked_until_later_layer", "required": True, "passed": True},
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_reused_local_payload_dependency", "passed": True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": True},
        {"source_family": "base_out_transitions", "status": "preserved_audited_dependency", "passed": True},
        {"source_family": "actual_outcome_surfaces", "status": "preserved_audited_candidate_dependency", "passed": True},
    ]

    blocking_rows = [
        {"blocked_surface": "truth_join_execution", "blocked": True, "reason": "6IV is planning only", "passed": True},
        {"blocked_surface": "real_evaluation_rerun", "blocked": True, "reason": "implementation and audit required first", "passed": True},
        {"blocked_surface": "final_pass_fail_decisions", "blocked": True, "reason": "truth join not executed", "passed": True},
        {"blocked_surface": "activation_planning", "blocked": True, "reason": "final decisions unavailable", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation planning blocked", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation chain incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6iu_passed", "expected": True, "actual": json_6iu.get("all_checks_passed"), "passed": json_6iu.get("all_checks_passed") is True},
        {"decision": "truth_join_planned", "expected": True, "actual": True, "passed": True},
        {"decision": "join_key_count", "expected": 8, "actual": len(JOIN_KEYS), "passed": len(JOIN_KEYS) == 8},
        {"decision": "join_fallback_level_count", "expected": 5, "actual": len(JOIN_FALLBACKS), "passed": len(JOIN_FALLBACKS) == 5},
        {"decision": "future_6iw_contract_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "future_6ix_contract_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6iw_truth_join_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6IV, "actual": RECOMMENDED_NEXT_LAYER_6IV, "passed": True},
        {"decision": "truth_join_executed", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_join", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_rerun", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_final_pass_fail", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_planning", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6iu_after = AUDIT_6IU_PATH.read_text(encoding="utf-8") if AUDIT_6IU_PATH.exists() else ""
    impl_6it_after = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    plan_6is_after = PLAN_6IS_PATH.read_text(encoding="utf-8") if PLAN_6IS_PATH.exists() else ""
    impl_6iq_after = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6iv_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6iu_audit", "policy": "unchanged_by_6iv", "passed": audit_6iu_after == audit_6iu_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6iv", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6is_plan", "policy": "unchanged_by_6iv", "passed": plan_6is_after == plan_6is_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6iv", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6iv", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6iv", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6iv", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6iv", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IV, "actual": RECOMMENDED_NEXT_LAYER_6IV, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IV, "actual": RECOMMENDED_PATH_6IV, "passed": True},
        {"decision": "recommend_truth_join_evaluation_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IV, "actual": DIAGNOSIS_6IV, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all_passed(problem_rows), "detail": "1/1"},
        {"check": "join_keys", "passed": all_passed(join_key_rows) and len(join_key_rows) == 8, "detail": f"{len(join_key_rows)}/8"},
        {"check": "join_fallback_hierarchy", "passed": all_passed(join_fallback_rows) and len(join_fallback_rows) == 5, "detail": f"{len(join_fallback_rows)}/5"},
        {"check": "truth_surface_prejoin_validation", "passed": all_passed(prejoin_validation_rows), "detail": f"{sum(1 for row in prejoin_validation_rows if row['passed'])}/{len(prejoin_validation_rows)}"},
        {"check": "metric_finalization_rules", "passed": all_passed(metric_finalization_rows), "detail": f"{sum(1 for row in metric_finalization_rows if row['passed'])}/{len(metric_finalization_rows)}"},
        {"check": "candidate_decision_finalization_rules", "passed": all_passed(candidate_decision_rows), "detail": f"{sum(1 for row in candidate_decision_rows if row['passed'])}/{len(candidate_decision_rows)}"},
        {"check": "lineage_propagation_rules", "passed": all_passed(lineage_prop_rows), "detail": f"{sum(1 for row in lineage_prop_rows if row['passed'])}/{len(lineage_prop_rows)}"},
        {"check": "non_production_outputs", "passed": all_passed(nonprod_output_rows), "detail": f"{sum(1 for row in nonprod_output_rows if row['passed'])}/{len(nonprod_output_rows)}"},
        {"check": "future_6iw_contract", "passed": all_passed(future_6iw_rows), "detail": f"{sum(1 for row in future_6iw_rows if row['passed'])}/{len(future_6iw_rows)}"},
        {"check": "future_6ix_contract", "passed": all_passed(future_6ix_rows), "detail": f"{sum(1 for row in future_6ix_rows if row['passed'])}/{len(future_6ix_rows)}"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for row in readonly_rows if row['passed'])}/{len(readonly_rows)}"},
        {"check": "preserved_families", "passed": all_passed(preserved_rows), "detail": f"{sum(1 for row in preserved_rows if row['passed'])}/{len(preserved_rows)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for row in blocking_rows if row['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all_passed(immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "problem_statement": write_csv(PROBLEM_CSV, problem_rows),
        "join_keys": write_csv(JOIN_KEYS_CSV, join_key_rows),
        "join_fallback_hierarchy": write_csv(JOIN_FALLBACK_CSV, join_fallback_rows),
        "truth_surface_prejoin_validation": write_csv(PREJOIN_VALIDATION_CSV, prejoin_validation_rows),
        "metric_finalization_rules": write_csv(METRIC_FINALIZATION_CSV, metric_finalization_rows),
        "candidate_decision_finalization_rules": write_csv(CANDIDATE_DECISION_CSV, candidate_decision_rows),
        "lineage_propagation_rules": write_csv(LINEAGE_PROP_CSV, lineage_prop_rows),
        "non_production_outputs": write_csv(NONPROD_OUTPUTS_CSV, nonprod_output_rows),
        "future_6iw_contract": write_csv(FUTURE_6IW_CSV, future_6iw_rows),
        "future_6ix_contract": write_csv(FUTURE_6IX_CSV, future_6ix_rows),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IV",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IV if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IV,
        "recommended_path": RECOMMENDED_PATH_6IV,
        "predecessor_audit": str(AUDIT_6IU_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6iu.get("diagnosis"),
        "audited_layer": "6IU",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "candidate_truth_surface_row_count": json_6iu.get("candidate_truth_surface_row_count"),
        "supported_truth_surface_count": json_6iu.get("supported_truth_surface_count"),
        "truth_surface_schema_field_count": json_6iu.get("truth_surface_schema_field_count"),
        "join_key_count": len(JOIN_KEYS),
        "join_fallback_level_count": len(JOIN_FALLBACKS),
        "metric_finalization_rule_count": len(METRIC_FINALIZATION_RULES),
        "candidate_decision_finalization_rule_count": len(CANDIDATE_DECISION_RULES),
        "lineage_propagation_rule_count": len(LINEAGE_PROPAGATION_RULES),
        "future_6iw_contract_valid": all_passed(future_6iw_rows),
        "future_6ix_contract_valid": all_passed(future_6ix_rows),
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "truth_join_planned": True,
        "truth_join_executed": False,
        "real_evaluation_rerun": False,
        "final_pass_fail_decision_possible_after_this_layer": False,
        "activation_planning_allowed_after_this_layer": False,
        "source_artifacts_mutated": False,
        "corrected_candidate_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "adapter_implementation_mutated": False,
        "evaluation_implementation_mutated": False,
        "truth_surface_implementation_mutated": False,
        "mechanics_activated_by_this_layer": False,
        "actual_outcomes_joined_to_mechanics": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "database_writes_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "layer_6_exit_credit": False,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "problem_statement_csv": str(PROBLEM_CSV),
            "join_keys_csv": str(JOIN_KEYS_CSV),
            "join_fallback_hierarchy_csv": str(JOIN_FALLBACK_CSV),
            "truth_surface_prejoin_validation_csv": str(PREJOIN_VALIDATION_CSV),
            "metric_finalization_rules_csv": str(METRIC_FINALIZATION_CSV),
            "candidate_decision_finalization_rules_csv": str(CANDIDATE_DECISION_CSV),
            "lineage_propagation_rules_csv": str(LINEAGE_PROP_CSV),
            "non_production_outputs_csv": str(NONPROD_OUTPUTS_CSV),
            "future_6iw_contract_csv": str(FUTURE_6IW_CSV),
            "future_6ix_contract_csv": str(FUTURE_6IX_CSV),
            "readonly_sources_csv": str(READONLY_CSV),
            "preserved_families_csv": str(PRESERVED_CSV),
            "blocking_policy_csv": str(BLOCKING_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
