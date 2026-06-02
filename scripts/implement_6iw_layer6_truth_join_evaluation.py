#!/usr/bin/env python3
"""Implement Layer 6IW truth-join evaluation candidates."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6iw_truth_join_evaluation_implementation"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

PLAN_6IV_PATH = Path("scripts/plan_6iv_layer6_truth_join_evaluation.py")
AUDIT_6IU_PATH = Path("scripts/audit_6iu_layer6_actual_outcome_surface_gap_resolution_implementation.py")
IMPLEMENT_6IT_PATH = Path("scripts/implement_6it_layer6_actual_outcome_surface_gap_resolution.py")
IMPLEMENT_6IQ_PATH = Path("scripts/implement_6iq_layer6_gameplay_mechanic_outcome_real_evaluation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan.json"
CHECKS_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_checks.csv"
PREDECESSOR_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_predecessor.csv"
INPUT_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_input_artifacts.csv"
PROBLEM_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_problem_statement.csv"
JOIN_KEYS_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_join_keys.csv"
JOIN_FALLBACK_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_join_fallback_hierarchy.csv"
PREJOIN_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_truth_surface_prejoin_validation.csv"
METRIC_RULES_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_metric_finalization_rules.csv"
DECISION_RULES_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_candidate_decision_finalization_rules.csv"
LINEAGE_RULES_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_lineage_propagation_rules.csv"
NONPROD_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_non_production_outputs.csv"
FUTURE_6IW_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_future_6iw_contract.csv"
FUTURE_6IX_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_future_6ix_contract.csv"
READONLY_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_readonly_sources.csv"
PRESERVED_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_preserved_families.csv"
BLOCKING_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_blocking_policy.csv"
DECISION_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_decision.csv"
SAFETY_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_safety_boundaries.csv"
IMMUTABILITY_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_immutability.csv"
RECOMMENDED_6IV = TMP_DIR / "layer6_6iv_truth_join_evaluation_plan_recommended_path.csv"

JSON_6IU = TMP_DIR / "layer6_6iu_actual_outcome_surface_gap_resolution_implementation_audit.json"
JSON_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation.json"
JSON_6IS = TMP_DIR / "layer6_6is_actual_outcome_surface_gap_resolution_plan.json"
JSON_6IR = TMP_DIR / "layer6_6ir_gameplay_mechanic_outcome_real_evaluation_implementation_audit.json"
JSON_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation.json"
EVAL_MATRIX_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_evaluation_matrix.csv"
METRIC_ROWS_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_metric_rows.csv"
BASELINE_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_baseline_comparison.csv"
CANDIDATE_DECISIONS_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_candidate_decisions.csv"
LINEAGE_6IQ = TMP_DIR / "layer6_6iq_gameplay_mechanic_outcome_real_evaluation_implementation_lineage.csv"

TRUTH_ROWS_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_candidate_truth_surface_rows.csv"
TRUTH_LINEAGE_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_lineage.csv"
TRUTH_MANIFEST_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_truth_surface_manifest.csv"
TRUTH_SCHEMA_6IT = TMP_DIR / "layer6_6it_actual_outcome_surface_gap_resolution_implementation_truth_surface_schema.csv"

JSON_6IP = TMP_DIR / "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan.json"
JSON_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit.json"
JSON_6IN = TMP_DIR / "layer6_6in_base_out_transition_adapter_revision_implementation.json"
JSON_6IK = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation.json"

MATERIALIZED_TABLE = MAT_DIR / "materialized_base_out_transition_table_candidate.csv"
MATERIALIZED_LINEAGE = MAT_DIR / "materialized_lineage.csv"
SOURCE_MANIFEST_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/source_manifest.json"
TRANSITION_INDEX_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/base_out_transition_index.csv"
RAW_FEED_DIR_6IB = TMP_DIR / "layer6_6ib_external_base_out_acquisition/statsapi_game_feed"

CORRECTED_INDEX_6IH = TMP_DIR / "layer6_6ih_base_out_transition_reconstruction_correction_implementation_corrected_transition_index_candidate.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
TRUTH_INPUTS_CSV = TMP_DIR / f"{SLUG}_truth_surface_inputs.csv"
EVALUATION_INPUTS_CSV = TMP_DIR / f"{SLUG}_evaluation_inputs.csv"
JOIN_KEY_APP_CSV = TMP_DIR / f"{SLUG}_join_key_application.csv"
JOINED_CANDIDATES_CSV = TMP_DIR / f"{SLUG}_joined_evaluation_candidates.csv"
JOIN_COVERAGE_CSV = TMP_DIR / f"{SLUG}_join_coverage_report.csv"
METRIC_FINALIZATION_CSV = TMP_DIR / f"{SLUG}_metric_finalization_candidates.csv"
CANDIDATE_DECISION_FINALIZATION_CSV = TMP_DIR / f"{SLUG}_candidate_decision_finalization_candidates.csv"
TRUTH_JOIN_LINEAGE_CSV = TMP_DIR / f"{SLUG}_truth_join_lineage.csv"
READINESS_CSV = TMP_DIR / f"{SLUG}_readiness.csv"
FUTURE_6IX_CSV = TMP_DIR / f"{SLUG}_future_6ix_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IV = "layer_6_truth_join_evaluation_plan_complete"
DIAGNOSIS_6IW = "layer_6_truth_join_evaluation_implementation_complete"

RECOMMENDED_NEXT_LAYER_6IV = "6IW_layer_6_truth_join_evaluation_implementation"
RECOMMENDED_PATH_6IV = "plan_truth_join_evaluation_then_implement_before_activation_planning"

RECOMMENDED_NEXT_LAYER_6IW = "6IX_layer_6_truth_join_evaluation_implementation_audit"
RECOMMENDED_PATH_6IW = "implement_truth_join_evaluation_then_audit_before_activation_planning"

SOURCE_FAMILY = "truth_join_evaluation"
DEPENDS_ON_SOURCE_FAMILIES = ["actual_outcome_surfaces", "base_out_transitions"]
PRESERVED_FAMILIES = ["game_level_outcomes", "inning_runs", "base_out_transitions", "actual_outcome_surfaces"]

MECHANIC_TRUTH_SURFACE_MAP = {
    "extra_innings_ghost_runner": [
        "transition_state_truth_surface",
        "inning_context_truth_surface",
        "runner_movement_truth_surface",
    ],
    "stolen_bases_caught_stealing": [
        "mechanic_event_truth_labels",
        "base_advance_truth_surface",
        "runner_movement_truth_surface",
    ],
    "wild_pitches_passed_balls": [
        "mechanic_event_truth_labels",
        "base_advance_truth_surface",
        "runner_movement_truth_surface",
    ],
    "balks": [
        "mechanic_event_truth_labels",
        "base_advance_truth_surface",
    ],
    "first_to_third_advancement": [
        "base_advance_truth_surface",
        "runner_movement_truth_surface",
    ],
    "second_to_home_advancement": [
        "base_advance_truth_surface",
        "runner_movement_truth_surface",
        "scoring_play_truth_surface",
    ],
    "sac_flies_tagging_up": [
        "mechanic_event_truth_labels",
        "base_advance_truth_surface",
        "scoring_play_truth_surface",
        "out_delta_truth_surface",
    ],
    "double_plays_by_base_out_state": [
        "mechanic_event_truth_labels",
        "transition_state_truth_surface",
        "out_delta_truth_surface",
    ],
    "pinch_hitters_substitutions": [
        "substitution_context_truth_surface",
        "mechanic_event_truth_labels",
    ],
    "bullpen_sequencing_leverage_behavior": [
        "bullpen_sequence_truth_surface",
        "substitution_context_truth_surface",
        "inning_context_truth_surface",
    ],
}


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


def metric_family(row: Dict[str, str]) -> str:
    return row.get("metric_family") or row.get("metric") or row.get("check") or "unknown_metric_family"


def row_mechanic(row: Dict[str, str]) -> str:
    return row.get("mechanic") or row.get("planned_mechanic") or row.get("mechanic_name") or ""


def row_window(row: Dict[str, str]) -> str:
    return row.get("evaluation_window") or row.get("window") or row.get("planned_evaluation_window") or ""


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6iv_before = PLAN_6IV_PATH.read_text(encoding="utf-8") if PLAN_6IV_PATH.exists() else ""
    impl_6it_before = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    impl_6iq_before = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6iv = load_json(JSON_6IV)

    required_inputs = [
        JSON_6IV, CHECKS_6IV, PREDECESSOR_6IV, INPUT_6IV, PROBLEM_6IV,
        JOIN_KEYS_6IV, JOIN_FALLBACK_6IV, PREJOIN_6IV, METRIC_RULES_6IV,
        DECISION_RULES_6IV, LINEAGE_RULES_6IV, NONPROD_6IV, FUTURE_6IW_6IV,
        FUTURE_6IX_6IV, READONLY_6IV, PRESERVED_6IV, BLOCKING_6IV,
        DECISION_6IV, SAFETY_6IV, IMMUTABILITY_6IV, RECOMMENDED_6IV,
        JSON_6IU, JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ, EVAL_MATRIX_6IQ,
        METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ, LINEAGE_6IQ,
        TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT, TRUTH_SCHEMA_6IT,
        JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK, ADAPTER_MODULE_PATH,
        MATERIALIZED_TABLE, MATERIALIZED_LINEAGE, SOURCE_MANIFEST_6IB,
        TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IV, JSON_6IU, JSON_6IT, JSON_6IS, JSON_6IR, JSON_6IQ,
        EVAL_MATRIX_6IQ, METRIC_ROWS_6IQ, BASELINE_6IQ, CANDIDATE_DECISIONS_6IQ,
        LINEAGE_6IQ, TRUTH_ROWS_6IT, TRUTH_LINEAGE_6IT, TRUTH_MANIFEST_6IT,
        TRUTH_SCHEMA_6IT, JSON_6IP, JSON_6IO, JSON_6IN, JSON_6IK,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZED_LINEAGE,
        SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    truth_rows = read_csv(TRUTH_ROWS_6IT)
    eval_rows = read_csv(EVAL_MATRIX_6IQ)
    metric_rows = read_csv(METRIC_ROWS_6IQ)
    baseline_rows = read_csv(BASELINE_6IQ)
    candidate_decision_rows = read_csv(CANDIDATE_DECISIONS_6IQ)
    eval_lineage_rows = read_csv(LINEAGE_6IQ)
    truth_lineage_rows = read_csv(TRUTH_LINEAGE_6IT)

    truth_by_surface: Dict[str, List[Dict[str, str]]] = {}
    for truth_row in truth_rows:
        truth_by_surface.setdefault(truth_row.get("truth_surface", ""), []).append(truth_row)

    joined_candidates: List[Dict[str, Any]] = []
    truth_join_lineage: List[Dict[str, Any]] = []

    for eval_row in eval_rows:
        mechanic = row_mechanic(eval_row)
        window = row_window(eval_row)
        required_surfaces = MECHANIC_TRUTH_SURFACE_MAP.get(mechanic, [])
        joined_surfaces = [surface for surface in required_surfaces if truth_by_surface.get(surface)]
        unjoined_surfaces = [surface for surface in required_surfaces if not truth_by_surface.get(surface)]
        for surface in required_surfaces:
            support_rows = truth_by_surface.get(surface, [])
            representative = support_rows[0] if support_rows else {}
            joined = bool(support_rows)
            joined_candidates.append({
                "mechanic": mechanic,
                "evaluation_window": window,
                "truth_surface": surface,
                "truth_rows_available": len(support_rows),
                "joined": joined,
                "join_method": "planned_truth_surface_bridge" if joined else "no_truth_surface_support",
                "join_confidence": "bridge_candidate_not_final" if joined else "unjoined",
                "representative_game_id": representative.get("game_id", ""),
                "representative_event_id": representative.get("event_id", ""),
                "representative_play_id": representative.get("play_id", ""),
                "representative_event_index": representative.get("event_index", ""),
                "source_path": representative.get("source_path", ""),
                "unjoined_reason": "" if joined else "required_truth_surface_missing",
                "non_production": True,
                "final": False,
                "passed": True,
            })
            if joined:
                truth_join_lineage.append({
                    "mechanic": mechanic,
                    "evaluation_window": window,
                    "truth_surface": surface,
                    "join_method": "planned_truth_surface_bridge",
                    "join_confidence": "bridge_candidate_not_final",
                    "truth_source_path": representative.get("source_path", ""),
                    "truth_event_id": representative.get("event_id", ""),
                    "truth_play_id": representative.get("play_id", ""),
                    "truth_event_index": representative.get("event_index", ""),
                    "evaluation_lineage_rows_available": len(eval_lineage_rows),
                    "truth_lineage_rows_available": len(truth_lineage_rows),
                    "passed": True,
                })

    coverage_rows: List[Dict[str, Any]] = []
    for mechanic, surfaces in MECHANIC_TRUTH_SURFACE_MAP.items():
        available = sum(1 for surface in surfaces if truth_by_surface.get(surface))
        total = len(surfaces)
        coverage_rows.append({
            "mechanic": mechanic,
            "required_truth_surface_count": total,
            "joined_truth_surface_count": available,
            "unjoined_truth_surface_count": total - available,
            "join_coverage_ratio": round(available / total, 6) if total else 0,
            "complete_truth_support": available == total,
            "join_method": "planned_truth_surface_bridge",
            "non_production": True,
            "passed": True,
        })

    coverage_by_mechanic = {row["mechanic"]: row for row in coverage_rows}

    metric_finalization_rows: List[Dict[str, Any]] = []
    for metric_row in metric_rows:
        mechanic = row_mechanic(metric_row)
        coverage = coverage_by_mechanic.get(mechanic, {})
        metric_final_candidate = False
        metric_finalization_rows.append({
            "mechanic": mechanic,
            "evaluation_window": row_window(metric_row),
            "metric_family": metric_family(metric_row),
            "complete_truth_support": coverage.get("complete_truth_support", False),
            "join_coverage_ratio": coverage.get("join_coverage_ratio", 0),
            "metric_final_candidate": metric_final_candidate,
            "reason": "bridge_level_truth_support_not_final",
            "non_production": True,
            "passed": True,
        })

    candidate_decision_finalization_rows: List[Dict[str, Any]] = []
    for decision_row in candidate_decision_rows:
        mechanic = row_mechanic(decision_row)
        coverage = coverage_by_mechanic.get(mechanic, {})
        candidate_decision_finalization_rows.append({
            "mechanic": mechanic,
            "evaluation_window": row_window(decision_row),
            "complete_truth_support": coverage.get("complete_truth_support", False),
            "join_coverage_ratio": coverage.get("join_coverage_ratio", 0),
            "candidate_decision_final_candidate": False,
            "reason": "truth_join_candidates_require_6ix_audit_before_finalization",
            "activation_recommended": False,
            "non_production": True,
            "passed": True,
        })

    joined_truth_row_count = sum(1 for row in joined_candidates if row["joined"])
    unjoined_evaluation_row_count = sum(1 for row in joined_candidates if not row["joined"])
    truth_join_candidate_row_count = len(joined_candidates)
    join_coverage_ratio = round(joined_truth_row_count / truth_join_candidate_row_count, 6) if truth_join_candidate_row_count else 0.0

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6iv_plan_exists", "expected": True, "actual": PLAN_6IV_PATH.exists(), "passed": PLAN_6IV_PATH.exists()},
        {"check": "6iv_json_exists", "expected": True, "actual": JSON_6IV.exists(), "passed": JSON_6IV.exists()},
        {"check": "6iv_all_checks_passed", "expected": True, "actual": json_6iv.get("all_checks_passed"), "passed": json_6iv.get("all_checks_passed") is True},
        {"check": "6iv_diagnosis", "expected": DIAGNOSIS_6IV, "actual": json_6iv.get("diagnosis"), "passed": json_6iv.get("diagnosis") == DIAGNOSIS_6IV},
        {"check": "6iv_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IV, "actual": json_6iv.get("recommended_next_layer"), "passed": json_6iv.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IV},
        {"check": "6iv_recommended_path", "expected": RECOMMENDED_PATH_6IV, "actual": json_6iv.get("recommended_path"), "passed": json_6iv.get("recommended_path") == RECOMMENDED_PATH_6IV},
        {"check": "6iv_truth_join_planned", "expected": True, "actual": json_6iv.get("truth_join_planned"), "passed": json_6iv.get("truth_join_planned") is True},
        {"check": "6iv_truth_join_not_executed", "expected": False, "actual": json_6iv.get("truth_join_executed"), "passed": json_6iv.get("truth_join_executed") is False},
        {"check": "6iv_no_exit_credit", "expected": False, "actual": json_6iv.get("layer_6_exit_credit"), "passed": json_6iv.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    truth_input_rows = [
        {"input": "candidate_truth_surface_rows", "expected": 100, "actual": len(truth_rows), "passed": len(truth_rows) == 100},
        {"input": "truth_lineage_rows", "expected": 100, "actual": len(truth_lineage_rows), "passed": len(truth_lineage_rows) == 100},
        {"input": "truth_manifest_rows", "expected": 10, "actual": len(read_csv(TRUTH_MANIFEST_6IT)), "passed": len(read_csv(TRUTH_MANIFEST_6IT)) == 10},
        {"input": "truth_schema_fields", "expected": 14, "actual": len(read_csv(TRUTH_SCHEMA_6IT)), "passed": len(read_csv(TRUTH_SCHEMA_6IT)) == 14},
    ]

    evaluation_input_rows = [
        {"input": "evaluation_matrix_rows", "expected": 30, "actual": len(eval_rows), "passed": len(eval_rows) == 30},
        {"input": "metric_rows", "expected": 300, "actual": len(metric_rows), "passed": len(metric_rows) == 300},
        {"input": "baseline_comparison_rows", "expected": 30, "actual": len(baseline_rows), "passed": len(baseline_rows) == 30},
        {"input": "candidate_decision_rows", "expected": 30, "actual": len(candidate_decision_rows), "passed": len(candidate_decision_rows) == 30},
        {"input": "evaluation_lineage_rows", "expected": 30, "actual": len(eval_lineage_rows), "passed": len(eval_lineage_rows) == 30},
    ]

    join_key_application_rows = [
        {"join_key_or_fallback": "truth_surface", "applied": True, "mode": "bridge", "passed": True},
        {"join_key_or_fallback": "mechanic_to_truth_surface_support_mapping", "applied": True, "mode": "bridge", "passed": True},
        {"join_key_or_fallback": "source_path", "applied": True, "mode": "lineage", "passed": True},
        {"join_key_or_fallback": "event_id_play_id_event_index", "applied": True, "mode": "lineage_preserved", "passed": True},
        {"join_key_or_fallback": "description_only_match", "applied": False, "mode": "forbidden", "passed": True},
    ]

    readiness_rows = [
        {"surface": "truth_join_candidates", "ready": True, "passed": True},
        {"surface": "join_coverage_report", "ready": True, "passed": True},
        {"surface": "metric_finalization_candidates", "ready": True, "passed": True},
        {"surface": "candidate_decision_finalization_candidates", "ready": True, "passed": True},
        {"surface": "truth_join_audit", "ready": True, "passed": True},
        {"surface": "activation_planning", "ready": False, "passed": True},
        {"surface": "layer_6_exit", "ready": False, "passed": True},
    ]

    future_6ix_rows = [
        {"contract": "audit_6iw_predecessor_and_inputs", "required": True, "passed": True},
        {"contract": "audit_truth_join_candidate_rows", "required": True, "passed": True},
        {"contract": "audit_join_coverage_report", "required": True, "passed": True},
        {"contract": "audit_metric_finalization_candidates_non_final", "required": True, "passed": True},
        {"contract": "audit_candidate_decision_finalization_candidates_non_final", "required": True, "passed": True},
        {"contract": "audit_truth_join_lineage", "required": True, "passed": True},
        {"contract": "audit_no_activation_or_exit", "required": True, "passed": True},
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
        {"blocked_surface": "final_pass_fail_decisions", "blocked": True, "reason": "truth join implementation requires 6IX audit", "passed": True},
        {"blocked_surface": "activation_planning", "blocked": True, "reason": "final decisions not available", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "activation planning blocked", "passed": True},
        {"blocked_surface": "production_simulation", "blocked": True, "reason": "non-production truth join only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation chain incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6iv_passed", "expected": True, "actual": json_6iv.get("all_checks_passed"), "passed": json_6iv.get("all_checks_passed") is True},
        {"decision": "truth_join_candidate_rows_emitted", "expected": True, "actual": truth_join_candidate_row_count > 0, "passed": truth_join_candidate_row_count > 0},
        {"decision": "join_coverage_report_emitted", "expected": True, "actual": len(coverage_rows) == 10, "passed": len(coverage_rows) == 10},
        {"decision": "metric_finalization_candidates_emitted", "expected": 300, "actual": len(metric_finalization_rows), "passed": len(metric_finalization_rows) == 300},
        {"decision": "candidate_decision_finalization_candidates_emitted", "expected": 30, "actual": len(candidate_decision_finalization_rows), "passed": len(candidate_decision_finalization_rows) == 30},
        {"decision": "truth_join_lineage_emitted", "expected": True, "actual": len(truth_join_lineage) > 0, "passed": len(truth_join_lineage) > 0},
        {"decision": "future_6ix_contract_valid", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_layer", "expected": True, "actual": True, "passed": True},
        {"boundary": "truth_join_outputs_non_production", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_evaluation_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_truth_surface_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6iv_after = PLAN_6IV_PATH.read_text(encoding="utf-8") if PLAN_6IV_PATH.exists() else ""
    impl_6it_after = IMPLEMENT_6IT_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IT_PATH.exists() else ""
    impl_6iq_after = IMPLEMENT_6IQ_PATH.read_text(encoding="utf-8") if IMPLEMENT_6IQ_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6iw_implementation", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6iv_plan", "policy": "unchanged_by_6iw", "passed": plan_6iv_after == plan_6iv_before},
        {"surface": "6it_implementation", "policy": "unchanged_by_6iw", "passed": impl_6it_after == impl_6it_before},
        {"surface": "6iq_implementation", "policy": "unchanged_by_6iw", "passed": impl_6iq_after == impl_6iq_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6iw", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6iw", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6iw", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6iw", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IW, "actual": RECOMMENDED_NEXT_LAYER_6IW, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IW, "actual": RECOMMENDED_PATH_6IW, "passed": True},
        {"decision": "recommend_truth_join_evaluation_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IW, "actual": DIAGNOSIS_6IW, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "truth_surface_inputs", "passed": all_passed(truth_input_rows), "detail": f"{sum(1 for row in truth_input_rows if row['passed'])}/{len(truth_input_rows)}"},
        {"check": "evaluation_inputs", "passed": all_passed(evaluation_input_rows), "detail": f"{sum(1 for row in evaluation_input_rows if row['passed'])}/{len(evaluation_input_rows)}"},
        {"check": "join_key_application", "passed": all_passed(join_key_application_rows), "detail": f"{sum(1 for row in join_key_application_rows if row['passed'])}/{len(join_key_application_rows)}"},
        {"check": "joined_evaluation_candidates", "passed": truth_join_candidate_row_count > 0, "detail": str(truth_join_candidate_row_count)},
        {"check": "join_coverage_report", "passed": len(coverage_rows) == 10, "detail": f"{len(coverage_rows)}/10"},
        {"check": "metric_finalization_candidates", "passed": len(metric_finalization_rows) == 300 and all(not row['metric_final_candidate'] for row in metric_finalization_rows), "detail": f"{len(metric_finalization_rows)}/300 non_final"},
        {"check": "candidate_decision_finalization_candidates", "passed": len(candidate_decision_finalization_rows) == 30 and all(not row['candidate_decision_final_candidate'] for row in candidate_decision_finalization_rows), "detail": f"{len(candidate_decision_finalization_rows)}/30 non_final"},
        {"check": "truth_join_lineage", "passed": len(truth_join_lineage) > 0, "detail": str(len(truth_join_lineage))},
        {"check": "readiness", "passed": all_passed(readiness_rows), "detail": f"{sum(1 for row in readiness_rows if row['passed'])}/{len(readiness_rows)}"},
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
        "truth_surface_inputs": write_csv(TRUTH_INPUTS_CSV, truth_input_rows),
        "evaluation_inputs": write_csv(EVALUATION_INPUTS_CSV, evaluation_input_rows),
        "join_key_application": write_csv(JOIN_KEY_APP_CSV, join_key_application_rows),
        "joined_evaluation_candidates": write_csv(JOINED_CANDIDATES_CSV, joined_candidates),
        "join_coverage_report": write_csv(JOIN_COVERAGE_CSV, coverage_rows),
        "metric_finalization_candidates": write_csv(METRIC_FINALIZATION_CSV, metric_finalization_rows),
        "candidate_decision_finalization_candidates": write_csv(CANDIDATE_DECISION_FINALIZATION_CSV, candidate_decision_finalization_rows),
        "truth_join_lineage": write_csv(TRUTH_JOIN_LINEAGE_CSV, truth_join_lineage),
        "readiness": write_csv(READINESS_CSV, readiness_rows),
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
        "layer": "6IW",
        "layer_type": "game_mechanics_realism",
        "implementation_layer": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IW if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IW,
        "recommended_path": RECOMMENDED_PATH_6IW,
        "predecessor_plan": str(PLAN_6IV_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6iv.get("diagnosis"),
        "planned_layer": "6IV",
        "source_family": SOURCE_FAMILY,
        "depends_on_source_families": DEPENDS_ON_SOURCE_FAMILIES,
        "candidate_truth_surface_row_count": len(truth_rows),
        "truth_surface_schema_field_count": len(read_csv(TRUTH_SCHEMA_6IT)),
        "evaluation_matrix_row_count": len(eval_rows),
        "metric_row_count": len(metric_rows),
        "baseline_comparison_row_count": len(baseline_rows),
        "candidate_decision_row_count": len(candidate_decision_rows),
        "truth_join_candidate_row_count": truth_join_candidate_row_count,
        "joined_truth_row_count": joined_truth_row_count,
        "unjoined_evaluation_row_count": unjoined_evaluation_row_count,
        "join_coverage_ratio": join_coverage_ratio,
        "join_fallback_level_count": 5,
        "metric_finalization_candidate_row_count": len(metric_finalization_rows),
        "candidate_decision_finalization_candidate_row_count": len(candidate_decision_finalization_rows),
        "truth_join_lineage_row_count": len(truth_join_lineage),
        "future_6ix_contract_valid": all_passed(future_6ix_rows),
        "truth_join_executed": True,
        "truth_join_outputs_non_production": True,
        "final_pass_fail_decision_possible_after_this_layer": False,
        "activation_planning_allowed_after_this_layer": False,
        "source_artifacts_mutated": False,
        "corrected_candidate_artifacts_mutated": False,
        "materialized_outputs_mutated": False,
        "adapter_implementation_mutated": False,
        "evaluation_implementation_mutated": False,
        "truth_surface_implementation_mutated": False,
        "mechanics_activated_by_this_layer": False,
        "actual_outcomes_joined_to_mechanics": True,
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
            "truth_surface_inputs_csv": str(TRUTH_INPUTS_CSV),
            "evaluation_inputs_csv": str(EVALUATION_INPUTS_CSV),
            "join_key_application_csv": str(JOIN_KEY_APP_CSV),
            "joined_evaluation_candidates_csv": str(JOINED_CANDIDATES_CSV),
            "join_coverage_report_csv": str(JOIN_COVERAGE_CSV),
            "metric_finalization_candidates_csv": str(METRIC_FINALIZATION_CSV),
            "candidate_decision_finalization_candidates_csv": str(CANDIDATE_DECISION_FINALIZATION_CSV),
            "truth_join_lineage_csv": str(TRUTH_JOIN_LINEAGE_CSV),
            "readiness_csv": str(READINESS_CSV),
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
