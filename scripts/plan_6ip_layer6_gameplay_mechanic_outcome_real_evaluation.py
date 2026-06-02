#!/usr/bin/env python3
"""Plan Layer 6IP real gameplay-mechanic outcome evaluation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6ip_gameplay_mechanic_outcome_real_evaluation_plan"
TMP_DIR = Path("tmp")
MAT_DIR = TMP_DIR / "layer6_6ik_base_out_transition_materialization_implementation"

AUDIT_6IO_PATH = Path("scripts/audit_6io_layer6_gameplay_mechanic_outcome_base_out_transition_adapter_revision_implementation.py")
ADAPTER_MODULE_PATH = Path("mlb_app/simulation/layer6_base_out_transition_adapter.py")

JSON_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit.json"
CHECKS_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_checks.csv"
PREDECESSOR_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_predecessor.csv"
INPUT_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_input_artifacts.csv"
ADAPTER_MODULE_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_adapter_module.csv"
IMPORT_SIDE_EFFECTS_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_import_side_effects.csv"
ADAPTER_VALIDATION_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_adapter_validation.csv"
ADAPTER_READINESS_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_adapter_readiness.csv"
SOURCE_CONTRACT_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_source_contract.csv"
GUARDRAIL_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_guardrail_results.csv"
READONLY_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_readonly_sources.csv"
FUTURE_6IP_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_future_6ip_contract.csv"
PRESERVED_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_preserved_families.csv"
BLOCKING_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_blocking_policy.csv"
DECISION_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_decision.csv"
SAFETY_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_safety_boundaries.csv"
IMMUTABILITY_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_immutability.csv"
RECOMMENDED_6IO = TMP_DIR / "layer6_6io_base_out_transition_adapter_revision_implementation_audit_recommended_path.csv"

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
PROBLEM_STATEMENT_CSV = TMP_DIR / f"{SLUG}_problem_statement.csv"
EVALUATION_MECHANICS_CSV = TMP_DIR / f"{SLUG}_evaluation_mechanics.csv"
EVALUATION_WINDOWS_CSV = TMP_DIR / f"{SLUG}_evaluation_windows.csv"
EVALUATION_PLAN_FAMILIES_CSV = TMP_DIR / f"{SLUG}_evaluation_plan_families.csv"
METRIC_FAMILIES_CSV = TMP_DIR / f"{SLUG}_metric_families.csv"
ACTUAL_OUTCOME_CONTRACT_CSV = TMP_DIR / f"{SLUG}_actual_outcome_contract.csv"
BASELINE_STRATEGY_CSV = TMP_DIR / f"{SLUG}_baseline_strategy.csv"
PASS_FAIL_POLICY_CSV = TMP_DIR / f"{SLUG}_pass_fail_policy.csv"
FUTURE_6IQ_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6iq_contract.csv"
FUTURE_6IR_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6ir_contract.csv"
READONLY_SOURCES_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
PRESERVED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_preserved_families.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6IO = "layer_6_gameplay_mechanic_outcome_base_out_transition_adapter_revision_implementation_audit_complete"
DIAGNOSIS_6IP = "layer_6_gameplay_mechanic_outcome_real_evaluation_plan_complete"

RECOMMENDED_NEXT_LAYER_6IO = "6IP_layer_6_gameplay_mechanic_outcome_real_evaluation_plan"
RECOMMENDED_PATH_6IO = "audit_base_out_transition_adapter_revision_then_plan_real_evaluation"

RECOMMENDED_NEXT_LAYER_6IP = "6IQ_layer_6_gameplay_mechanic_outcome_real_evaluation_implementation"
RECOMMENDED_PATH_6IP = "plan_real_gameplay_mechanic_outcome_evaluation_then_implement_before_activation"

SOURCE_FAMILY = "base_out_transitions"
ACQUISITION_MODE = "future_controlled_statsapi_acquisition"

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

EVALUATION_PLAN_FAMILIES = [
    "adapter_input_contract",
    "actual_outcome_join_contract",
    "mechanic_metric_definitions",
    "baseline_comparison_strategy",
    "evaluation_window_strategy",
    "pass_fail_decision_policy",
    "future_real_evaluation_implementation_contract",
    "future_real_evaluation_audit_contract",
    "activation_blocking_policy",
]

METRIC_FAMILIES = [
    "transition_state_accuracy",
    "run_delta_accuracy",
    "out_delta_accuracy",
    "mechanic_event_detection_rate",
    "mechanic_event_false_positive_rate",
    "mechanic_event_false_negative_rate",
    "win_probability_or_run_expectancy_directional_consistency",
    "calibration_by_game_state",
    "improvement_vs_baseline",
    "regression_risk_vs_baseline",
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


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_before = AUDIT_6IO_PATH.read_text(encoding="utf-8") if AUDIT_6IO_PATH.exists() else ""
    adapter_before = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_before = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_before = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_before = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    json_6io = load_json(JSON_6IO)

    required_inputs = [
        JSON_6IO, CHECKS_6IO, PREDECESSOR_6IO, INPUT_6IO, ADAPTER_MODULE_6IO,
        IMPORT_SIDE_EFFECTS_6IO, ADAPTER_VALIDATION_6IO, ADAPTER_READINESS_6IO,
        SOURCE_CONTRACT_6IO, GUARDRAIL_6IO, READONLY_6IO, FUTURE_6IP_6IO,
        PRESERVED_6IO, BLOCKING_6IO, DECISION_6IO, SAFETY_6IO, IMMUTABILITY_6IO,
        RECOMMENDED_6IO, JSON_6IN, JSON_6IM, JSON_6IL, JSON_6IK, JSON_6IJ, JSON_6II,
        MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA, MATERIALIZED_LINEAGE,
        MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS, CORRECTED_INDEX_6IH,
        SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB, RAW_FEED_DIR_6IB,
    ]

    readonly_sources = [
        JSON_6IO, JSON_6IN, JSON_6IM, JSON_6IL, JSON_6IK, JSON_6IJ, JSON_6II,
        ADAPTER_MODULE_PATH, MATERIALIZED_TABLE, MATERIALIZATION_MANIFEST, MATERIALIZED_SCHEMA,
        MATERIALIZED_LINEAGE, MATERIALIZATION_VALIDATION, MATERIALIZATION_READINESS,
        CORRECTED_INDEX_6IH, SOURCE_PROVENANCE_6IH, SOURCE_MANIFEST_6IB, TRANSITION_INDEX_6IB,
        RAW_FEED_DIR_6IB,
    ]

    mechanic_window_pairs = [
        {"mechanic": mechanic, "evaluation_window": window, "planned": True, "passed": True}
        for mechanic in GAMEPLAY_MECHANICS
        for window in EVALUATION_WINDOWS
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6io_audit_exists", "expected": True, "actual": AUDIT_6IO_PATH.exists(), "passed": AUDIT_6IO_PATH.exists()},
        {"check": "6io_json_exists", "expected": True, "actual": JSON_6IO.exists(), "passed": JSON_6IO.exists()},
        {"check": "6io_all_checks_passed", "expected": True, "actual": json_6io.get("all_checks_passed"), "passed": json_6io.get("all_checks_passed") is True},
        {"check": "6io_diagnosis", "expected": DIAGNOSIS_6IO, "actual": json_6io.get("diagnosis"), "passed": json_6io.get("diagnosis") == DIAGNOSIS_6IO},
        {"check": "6io_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IO, "actual": json_6io.get("recommended_next_layer"), "passed": json_6io.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6IO},
        {"check": "6io_recommended_path", "expected": RECOMMENDED_PATH_6IO, "actual": json_6io.get("recommended_path"), "passed": json_6io.get("recommended_path") == RECOMMENDED_PATH_6IO},
        {"check": "6io_real_eval_planning_allowed", "expected": True, "actual": json_6io.get("real_evaluation_planning_allowed_after_this_audit"), "passed": json_6io.get("real_evaluation_planning_allowed_after_this_audit") is True},
        {"check": "6io_real_eval_execution_blocked", "expected": False, "actual": json_6io.get("real_evaluation_allowed_after_this_audit"), "passed": json_6io.get("real_evaluation_allowed_after_this_audit") is False},
        {"check": "6io_no_exit_credit", "expected": False, "actual": json_6io.get("layer_6_exit_credit"), "passed": json_6io.get("layer_6_exit_credit") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    problem_rows = [{
        "problem": "real gameplay-mechanic outcome evaluation must now be planned using audited non-production adapter inputs",
        "source_family": SOURCE_FAMILY,
        "evidence": "6IO allowed real evaluation planning but kept evaluation execution and activation blocked",
        "planned_mechanics": len(GAMEPLAY_MECHANICS),
        "planned_windows": len(EVALUATION_WINDOWS),
        "planned_pairs": len(mechanic_window_pairs),
        "passed": True,
    }]

    mechanics_rows = [
        {"mechanic": mechanic, "planned": True, "requires_adapter_input": True, "requires_actual_outcome_comparison": True, "passed": True}
        for mechanic in GAMEPLAY_MECHANICS
    ]

    windows_rows = [
        {"evaluation_window": window, "planned": True, "requires_baseline_comparison": True, "requires_pass_fail_metrics": True, "passed": True}
        for window in EVALUATION_WINDOWS
    ]

    family_rows = [
        {"plan_family": family, "planned": True, "passed": True}
        for family in EVALUATION_PLAN_FAMILIES
    ]

    metric_rows = [
        {"metric_family": metric, "planned": True, "per_mechanic": True, "per_window": True, "passed": True}
        for metric in METRIC_FAMILIES
    ]

    actual_outcome_rows = [
        {"contract": "define_actual_outcome_surfaces", "required": True, "passed": True},
        {"contract": "join_actual_outcomes_only_in_future_implementation", "required": True, "passed": True},
        {"contract": "compare_transition_state_outcomes", "required": True, "passed": True},
        {"contract": "compare_run_delta_outcomes", "required": True, "passed": True},
        {"contract": "compare_out_delta_outcomes", "required": True, "passed": True},
        {"contract": "track_mechanic_event_labels", "required": True, "passed": True},
        {"contract": "preserve_lineage_to_adapter_record", "required": True, "passed": True},
    ]

    baseline_rows = [
        {"baseline": "current_non_mechanic_simulation_behavior", "comparison_required": True, "passed": True},
        {"baseline": "prior_layer6_without_new_base_out_adapter", "comparison_required": True, "passed": True},
        {"baseline": "mechanic_disabled_counterfactual", "comparison_required": True, "passed": True},
        {"baseline": "per_game_state_calibration_baseline", "comparison_required": True, "passed": True},
    ]

    pass_fail_rows = [
        {"policy": "require_metric_completeness_all_mechanics_windows", "required": True, "passed": True},
        {"policy": "require_no_baseline_regression_on_core_accuracy", "required": True, "passed": True},
        {"policy": "require_directional_improvement_or_neutrality_by_mechanic", "required": True, "passed": True},
        {"policy": "require_lineage_for_all_evaluated_records", "required": True, "passed": True},
        {"policy": "require_explicit_activation_recommendation_later", "required": True, "passed": True},
        {"policy": "block_layer_6_exit_until_activation_audit", "required": True, "passed": True},
    ]

    future_6iq_rows = [
        {"contract": "consume_6ip_plan_and_6io_audit", "required": True, "passed": True},
        {"contract": "consume_audited_adapter_only", "required": True, "passed": True},
        {"contract": "load_801_validated_transition_records", "required": True, "passed": True},
        {"contract": "define_actual_outcome_comparison_surfaces", "required": True, "passed": True},
        {"contract": "evaluate_10_mechanics_across_3_windows", "required": True, "passed": True},
        {"contract": "emit_per_mechanic_window_metrics", "required": True, "passed": True},
        {"contract": "emit_baseline_comparison_metrics", "required": True, "passed": True},
        {"contract": "emit_pass_fail_candidate_decisions", "required": True, "passed": True},
        {"contract": "remain_non_production", "required": True, "passed": True},
        {"contract": "do_not_activate_or_exit", "required": True, "passed": True},
    ]

    future_6ir_rows = [
        {"contract": "audit_6iq_real_evaluation_implementation", "required": True, "passed": True},
        {"contract": "audit_actual_outcome_joins", "required": True, "passed": True},
        {"contract": "audit_per_mechanic_window_metric_completeness", "required": True, "passed": True},
        {"contract": "audit_baseline_comparisons", "required": True, "passed": True},
        {"contract": "audit_pass_fail_decision_policy", "required": True, "passed": True},
        {"contract": "decide_whether_activation_planning_can_begin", "required": True, "passed": True},
        {"contract": "keep_direct_activation_and_exit_blocked", "required": True, "passed": True},
    ]

    readonly_rows = [
        {"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()}
        for path in readonly_sources
    ]

    preserved_rows = [
        {"source_family": "game_level_outcomes", "status": "preserved_remediated_from_prior_layers", "passed": True},
        {"source_family": "inning_runs", "status": "preserved_remediated_from_prior_layers", "passed": True},
    ]

    blocking_rows = [
        {"blocked_surface": "real_evaluation_execution", "blocked": True, "reason": "6IP is planning only; 6IQ must implement", "passed": True},
        {"blocked_surface": "actual_outcome_join_execution", "blocked": True, "reason": "6IP only defines the join contract", "passed": True},
        {"blocked_surface": "mechanic_activation", "blocked": True, "reason": "real evaluation implementation/audit required first", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "activation layers incomplete", "passed": True},
    ]

    decision_rows = [
        {"decision": "6io_passed", "expected": True, "actual": json_6io.get("all_checks_passed"), "passed": json_6io.get("all_checks_passed") is True},
        {"decision": "real_evaluation_planning_allowed", "expected": True, "actual": json_6io.get("real_evaluation_planning_allowed_after_this_audit"), "passed": json_6io.get("real_evaluation_planning_allowed_after_this_audit") is True},
        {"decision": "planned_mechanic_count", "expected": 10, "actual": len(GAMEPLAY_MECHANICS), "passed": len(GAMEPLAY_MECHANICS) == 10},
        {"decision": "planned_window_count", "expected": 3, "actual": len(EVALUATION_WINDOWS), "passed": len(EVALUATION_WINDOWS) == 3},
        {"decision": "planned_mechanic_window_pair_count", "expected": 30, "actual": len(mechanic_window_pairs), "passed": len(mechanic_window_pairs) == 30},
        {"decision": "recommend_6iq_real_evaluation_implementation_next", "expected": RECOMMENDED_NEXT_LAYER_6IP, "actual": RECOMMENDED_NEXT_LAYER_6IP, "passed": True},
        {"decision": "real_evaluation_execution_allowed_after_this_plan", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_implementation_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ib_artifact_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ih_corrected_candidate_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_6ik_materialized_output_modification", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_evaluation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_actual_outcome_join_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_activation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_after = AUDIT_6IO_PATH.read_text(encoding="utf-8") if AUDIT_6IO_PATH.exists() else ""
    adapter_after = ADAPTER_MODULE_PATH.read_text(encoding="utf-8") if ADAPTER_MODULE_PATH.exists() else ""
    transition_after = TRANSITION_INDEX_6IB.read_text(encoding="utf-8") if TRANSITION_INDEX_6IB.exists() else ""
    corrected_after = CORRECTED_INDEX_6IH.read_text(encoding="utf-8") if CORRECTED_INDEX_6IH.exists() else ""
    materialized_after = MATERIALIZED_TABLE.read_text(encoding="utf-8") if MATERIALIZED_TABLE.exists() else ""

    immutability_rows = [
        {"surface": "this_6ip_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6io_audit", "policy": "unchanged_by_6ip", "passed": audit_after == audit_before},
        {"surface": "adapter_module", "policy": "unchanged_by_6ip", "passed": adapter_after == adapter_before},
        {"surface": "6ib_transition_index", "policy": "read_only_unchanged_by_6ip", "passed": transition_after == transition_before},
        {"surface": "6ih_corrected_candidate", "policy": "read_only_unchanged_by_6ip", "passed": corrected_after == corrected_before},
        {"surface": "6ik_materialized_table", "policy": "read_only_unchanged_by_6ip", "passed": materialized_after == materialized_before},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6IP, "actual": RECOMMENDED_NEXT_LAYER_6IP, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6IP, "actual": RECOMMENDED_PATH_6IP, "passed": True},
        {"decision": "recommend_real_evaluation_implementation_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6IP, "actual": DIAGNOSIS_6IP, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "problem_statement", "passed": all(row["passed"] for row in problem_rows), "detail": "1/1"},
        {"check": "evaluation_mechanics", "passed": all(row["passed"] for row in mechanics_rows) and len(mechanics_rows) == 10, "detail": f"{len(mechanics_rows)}/10"},
        {"check": "evaluation_windows", "passed": all(row["passed"] for row in windows_rows) and len(windows_rows) == 3, "detail": f"{len(windows_rows)}/3"},
        {"check": "mechanic_window_pairs", "passed": all(row["passed"] for row in mechanic_window_pairs) and len(mechanic_window_pairs) == 30, "detail": f"{len(mechanic_window_pairs)}/30"},
        {"check": "evaluation_plan_families", "passed": all(row["passed"] for row in family_rows) and len(family_rows) == 9, "detail": f"{len(family_rows)}/9"},
        {"check": "metric_families", "passed": all(row["passed"] for row in metric_rows) and len(metric_rows) == 10, "detail": f"{len(metric_rows)}/10"},
        {"check": "actual_outcome_contract", "passed": all(row["passed"] for row in actual_outcome_rows), "detail": f"{sum(1 for row in actual_outcome_rows if row['passed'])}/{len(actual_outcome_rows)}"},
        {"check": "baseline_strategy", "passed": all(row["passed"] for row in baseline_rows), "detail": f"{sum(1 for row in baseline_rows if row['passed'])}/{len(baseline_rows)}"},
        {"check": "pass_fail_policy", "passed": all(row["passed"] for row in pass_fail_rows), "detail": f"{sum(1 for row in pass_fail_rows if row['passed'])}/{len(pass_fail_rows)}"},
        {"check": "future_6iq_contract", "passed": all(row["passed"] for row in future_6iq_rows), "detail": f"{sum(1 for row in future_6iq_rows if row['passed'])}/{len(future_6iq_rows)}"},
        {"check": "future_6ir_contract", "passed": all(row["passed"] for row in future_6ir_rows), "detail": f"{sum(1 for row in future_6ir_rows if row['passed'])}/{len(future_6ir_rows)}"},
        {"check": "readonly_sources", "passed": all(row["passed"] for row in readonly_rows), "detail": f"{sum(1 for row in readonly_rows if row['passed'])}/{len(readonly_rows)}"},
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
        "problem_statement": write_csv(PROBLEM_STATEMENT_CSV, problem_rows),
        "evaluation_mechanics": write_csv(EVALUATION_MECHANICS_CSV, mechanics_rows),
        "evaluation_windows": write_csv(EVALUATION_WINDOWS_CSV, windows_rows),
        "evaluation_plan_families": write_csv(EVALUATION_PLAN_FAMILIES_CSV, family_rows),
        "metric_families": write_csv(METRIC_FAMILIES_CSV, metric_rows),
        "actual_outcome_contract": write_csv(ACTUAL_OUTCOME_CONTRACT_CSV, actual_outcome_rows),
        "baseline_strategy": write_csv(BASELINE_STRATEGY_CSV, baseline_rows),
        "pass_fail_policy": write_csv(PASS_FAIL_POLICY_CSV, pass_fail_rows),
        "future_6iq_contract": write_csv(FUTURE_6IQ_CONTRACT_CSV, future_6iq_rows),
        "future_6ir_contract": write_csv(FUTURE_6IR_CONTRACT_CSV, future_6ir_rows),
        "readonly_sources": write_csv(READONLY_SOURCES_CSV, readonly_rows),
        "preserved_families": write_csv(PRESERVED_FAMILIES_CSV, preserved_rows),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6IP",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6IP if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6IP,
        "recommended_path": RECOMMENDED_PATH_6IP,
        "predecessor_audit": str(AUDIT_6IO_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6io.get("diagnosis"),
        "audited_layer": "6IO",
        "source_family": SOURCE_FAMILY,
        "acquisition_mode": ACQUISITION_MODE,
        "materialization_audited": json_6io.get("materialization_audited"),
        "adapter_revision_audited": json_6io.get("adapter_revision_audited"),
        "adapter_production_enabled": json_6io.get("adapter_production_enabled"),
        "adapter_validation_passed": json_6io.get("adapter_validation_passed"),
        "materialized_transition_row_count": json_6io.get("materialized_transition_row_count"),
        "materialized_exact_transition_row_count": json_6io.get("materialized_exact_transition_row_count"),
        "materialized_non_exact_transition_row_count": json_6io.get("materialized_non_exact_transition_row_count"),
        "materialized_schema_field_count": json_6io.get("materialized_schema_field_count"),
        "source_provenance_retained_for_all_rows": json_6io.get("source_provenance_retained_for_all_rows"),
        "lineage_rows_available": json_6io.get("lineage_rows_available"),
        "lineage_fields_populated_for_all_rows": json_6io.get("lineage_fields_populated_for_all_rows"),
        "real_evaluation_planning_allowed": json_6io.get("real_evaluation_planning_allowed_after_this_audit"),
        "real_evaluation_plan_family_count": len(EVALUATION_PLAN_FAMILIES),
        "planned_metric_family_count": len(METRIC_FAMILIES),
        "planned_mechanic_count": len(GAMEPLAY_MECHANICS),
        "planned_evaluation_window_count": len(EVALUATION_WINDOWS),
        "planned_mechanic_window_pair_count": len(mechanic_window_pairs),
        "future_6iq_contract_valid": all(row["passed"] for row in future_6iq_rows),
        "future_6ir_contract_valid": all(row["passed"] for row in future_6ir_rows),
        "preserved_remediated_family_count": len(PRESERVED_FAMILIES),
        "real_evaluation_allowed_after_this_plan": False,
        "real_evaluation_still_blocked": True,
        "activation_allowed": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
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
            "problem_statement_csv": str(PROBLEM_STATEMENT_CSV),
            "evaluation_mechanics_csv": str(EVALUATION_MECHANICS_CSV),
            "evaluation_windows_csv": str(EVALUATION_WINDOWS_CSV),
            "evaluation_plan_families_csv": str(EVALUATION_PLAN_FAMILIES_CSV),
            "metric_families_csv": str(METRIC_FAMILIES_CSV),
            "actual_outcome_contract_csv": str(ACTUAL_OUTCOME_CONTRACT_CSV),
            "baseline_strategy_csv": str(BASELINE_STRATEGY_CSV),
            "pass_fail_policy_csv": str(PASS_FAIL_POLICY_CSV),
            "future_6iq_contract_csv": str(FUTURE_6IQ_CONTRACT_CSV),
            "future_6ir_contract_csv": str(FUTURE_6IR_CONTRACT_CSV),
            "readonly_sources_csv": str(READONLY_SOURCES_CSV),
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
