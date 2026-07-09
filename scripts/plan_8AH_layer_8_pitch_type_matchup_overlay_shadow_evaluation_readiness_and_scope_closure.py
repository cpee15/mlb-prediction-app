#!/usr/bin/env python3
"""
Layer 8AH
Layer 8 Pitch-Type Matchup Overlay Shadow Evaluation Readiness and
Scope Closure Plan

Closes Layer 8 under deterministic, diagnostic-only, shadow-observable scope
and defines the bounded handoff into point-in-time historical evaluation
planning.

Planning only.

This layer does not:
- join historical outcomes;
- evaluate predictive accuracy or calibration;
- tune models, weights, thresholds, or fallbacks;
- run historical backtests;
- activate the matchup overlay in production;
- modify simulation or canonical probabilities;
- compare projections with betting markets;
- price wagers, detect edges, or recommend bets.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "8AH"
LAYER_NAME = (
    "layer_8_pitch_type_matchup_overlay_shadow_evaluation_"
    "readiness_and_scope_closure_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8AH_pitch_type_matchup_overlay_shadow_evaluation_"
    "readiness_and_scope_closure"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8AG_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_"
    "observability_history_quality_gate_contract.py"
)


LAYER_8_WORKSTREAMS = [
    {
        "workstream_id": "L8-W01",
        "workstream": "repository_inventory",
        "layers": "8A",
        "status": "complete",
        "capability": (
            "Repository support for pitch identity, arsenal usage, matchup "
            "behavior, provenance, diagnostics, and validation was inventoried."
        ),
    },
    {
        "workstream_id": "L8-W02",
        "workstream": "canonical_pitch_taxonomy",
        "layers": "8B-8C",
        "status": "complete",
        "capability": (
            "Canonical pitch identities, aliases, source precedence, "
            "normalization, unknown handling, and provenance are implemented."
        ),
    },
    {
        "workstream_id": "L8-W03",
        "workstream": "pitcher_arsenal_profiles",
        "layers": "8D-8E",
        "status": "complete",
        "capability": (
            "Deterministic diagnostic pitcher arsenal profiles are implemented."
        ),
    },
    {
        "workstream_id": "L8-W04",
        "workstream": "batter_pitch_type_response_profiles",
        "layers": "8F-8G",
        "status": "complete",
        "capability": (
            "Deterministic diagnostic batter pitch-type response profiles "
            "are implemented."
        ),
    },
    {
        "workstream_id": "L8-W05",
        "workstream": "pitcher_batter_matchup_overlay",
        "layers": "8H-8I",
        "status": "complete",
        "capability": (
            "Pitcher arsenals and batter response profiles are aligned through "
            "a deterministic diagnostic matchup overlay."
        ),
    },
    {
        "workstream_id": "L8-W06",
        "workstream": "overlay_observability",
        "layers": "8J-8K",
        "status": "complete",
        "capability": (
            "Overlay status, coverage, fallback use, entries, summaries, and "
            "deterministic observation identities are observable."
        ),
    },
    {
        "workstream_id": "L8-W07",
        "workstream": "shadow_dataset",
        "layers": "8L-8M",
        "status": "complete",
        "capability": (
            "Deterministic append-only shadow rows, partitions, manifests, "
            "schema fingerprints, and source lineage are implemented."
        ),
    },
    {
        "workstream_id": "L8-W08",
        "workstream": "shadow_dataset_quality_gate",
        "layers": "8N-8O",
        "status": "complete",
        "capability": (
            "Structural, schema, identity, coverage, usage, and lineage quality "
            "gates are implemented."
        ),
    },
    {
        "workstream_id": "L8-W09",
        "workstream": "shadow_collection",
        "layers": "8P-8Q",
        "status": "complete",
        "capability": (
            "Quality-gated shadow datasets and reports can be collected in a "
            "deterministic append-only manifest."
        ),
    },
    {
        "workstream_id": "L8-W10",
        "workstream": "collection_observability",
        "layers": "8R-8S",
        "status": "complete",
        "capability": (
            "Collection integrity, coverage, duplicates, conflicts, warnings, "
            "and rejected records are observable."
        ),
    },
    {
        "workstream_id": "L8-W11",
        "workstream": "retention_classification",
        "layers": "8T-8U",
        "status": "complete",
        "capability": (
            "Deterministic logical retention decisions and immutable ledgers "
            "are implemented without physical deletion."
        ),
    },
    {
        "workstream_id": "L8-W12",
        "workstream": "retention_observability",
        "layers": "8V-8W",
        "status": "complete",
        "capability": (
            "Retention decisions, policy windows, ages, duplicates, and "
            "quarantine signals are observable."
        ),
    },
    {
        "workstream_id": "L8-W13",
        "workstream": "retention_observability_history",
        "layers": "8X-8Y",
        "status": "complete",
        "capability": (
            "Retention-observability snapshots and reports have immutable, "
            "append-only deterministic history."
        ),
    },
    {
        "workstream_id": "L8-W14",
        "workstream": "retention_observability_history_quality_gate",
        "layers": "8Z-8AA",
        "status": "complete",
        "capability": (
            "History digest, identity, ordering, source-digest, status-count, "
            "duplicate, and authority quality checks are implemented."
        ),
    },
    {
        "workstream_id": "L8-W15",
        "workstream": "history_quality_gate_observability",
        "layers": "8AB-8AC",
        "status": "complete",
        "capability": (
            "The history quality gate has deterministic diagnostic "
            "observability."
        ),
    },
    {
        "workstream_id": "L8-W16",
        "workstream": "history_quality_gate_observability_history",
        "layers": "8AD-8AE",
        "status": "complete",
        "capability": (
            "Quality-gate-observability snapshots have immutable append-only "
            "history."
        ),
    },
    {
        "workstream_id": "L8-W17",
        "workstream": "observability_history_quality_gate",
        "layers": "8AF-8AG",
        "status": "complete",
        "capability": (
            "The immutable quality-gate-observability history has a "
            "deterministic quality gate."
        ),
    },
]


EVALUATION_READINESS_CAPABILITIES = [
    {
        "capability_id": "L8-R01",
        "capability": "canonical_pitch_identity",
        "ready": True,
        "evidence": "8C-v1 canonical taxonomy and normalization",
    },
    {
        "capability_id": "L8-R02",
        "capability": "pitcher_profile_versioning",
        "ready": True,
        "evidence": "8E-v1 pitcher arsenal profiles",
    },
    {
        "capability_id": "L8-R03",
        "capability": "batter_profile_versioning",
        "ready": True,
        "evidence": "8G-v1 batter response profiles",
    },
    {
        "capability_id": "L8-R04",
        "capability": "matchup_overlay_versioning",
        "ready": True,
        "evidence": "8I-v1 matchup overlays",
    },
    {
        "capability_id": "L8-R05",
        "capability": "coverage_and_fallback_observability",
        "ready": True,
        "evidence": "8K-v1 overlay observability",
    },
    {
        "capability_id": "L8-R06",
        "capability": "deterministic_shadow_row_identity",
        "ready": True,
        "evidence": "8M-v1 shadow dataset",
    },
    {
        "capability_id": "L8-R07",
        "capability": "schema_and_manifest_integrity",
        "ready": True,
        "evidence": "8O-v1 shadow dataset quality gate",
    },
    {
        "capability_id": "L8-R08",
        "capability": "append_only_collection",
        "ready": True,
        "evidence": "8Q-v1 shadow collection",
    },
    {
        "capability_id": "L8-R09",
        "capability": "collection_integrity_observability",
        "ready": True,
        "evidence": "8S-v1 collection observability",
    },
    {
        "capability_id": "L8-R10",
        "capability": "logical_retention_governance",
        "ready": True,
        "evidence": "8U-v1 retention decisions",
    },
    {
        "capability_id": "L8-R11",
        "capability": "immutable_observability_history",
        "ready": True,
        "evidence": "8Y-v1 and 8AE-v1 immutable histories",
    },
    {
        "capability_id": "L8-R12",
        "capability": "multi_level_integrity_quality_gates",
        "ready": True,
        "evidence": "8AA-v1 and 8AG-v1 quality gates",
    },
]


UNRESOLVED_EVALUATION_GATES = [
    {
        "gate_id": "L8-G01",
        "gate": "point_in_time_event_identity",
        "resolved": False,
        "required_next": (
            "Define game, plate-appearance, pitch, pitcher, batter, lineup, "
            "and timestamp identities for outcome-safe evaluation."
        ),
    },
    {
        "gate_id": "L8-G02",
        "gate": "prediction_cutoff_semantics",
        "resolved": False,
        "required_next": (
            "Define exactly when each profile and matchup observation becomes "
            "eligible for an event."
        ),
    },
    {
        "gate_id": "L8-G03",
        "gate": "future_information_exclusion",
        "resolved": False,
        "required_next": (
            "Prove that no source records, profiles, or aggregates created "
            "after the evaluated event can enter the feature payload."
        ),
    },
    {
        "gate_id": "L8-G04",
        "gate": "historical_outcome_contract",
        "resolved": False,
        "required_next": (
            "Define bounded pitch-level, plate-appearance-level, contact, and "
            "run-value outcome schemas."
        ),
    },
    {
        "gate_id": "L8-G05",
        "gate": "baseline_prediction_contract",
        "resolved": False,
        "required_next": (
            "Define the frozen baseline predictions against which Layer 8 "
            "incremental value will be measured."
        ),
    },
    {
        "gate_id": "L8-G06",
        "gate": "augmented_prediction_contract",
        "resolved": False,
        "required_next": (
            "Define diagnostic baseline-plus-overlay predictions without "
            "changing production probabilities."
        ),
    },
    {
        "gate_id": "L8-G07",
        "gate": "evaluation_metric_contract",
        "resolved": False,
        "required_next": (
            "Define log loss, Brier score, calibration, discrimination, and "
            "coverage-segment evaluation rules."
        ),
    },
    {
        "gate_id": "L8-G08",
        "gate": "out_of_time_validation_contract",
        "resolved": False,
        "required_next": (
            "Define training, validation, and untouched future test periods."
        ),
    },
    {
        "gate_id": "L8-G09",
        "gate": "incremental_value_and_ablation_contract",
        "resolved": False,
        "required_next": (
            "Define pitcher-only, batter-only, handedness-only, aggregate, "
            "and pitch-type overlay comparisons."
        ),
    },
    {
        "gate_id": "L8-G10",
        "gate": "uncertainty_and_stability_contract",
        "resolved": False,
        "required_next": (
            "Define confidence intervals, bootstrap uncertainty, seasonal "
            "stability, and subgroup stability."
        ),
    },
    {
        "gate_id": "L8-G11",
        "gate": "shadow_runtime_integration",
        "resolved": False,
        "required_next": (
            "Define side-by-side baseline and augmented runtime evaluation "
            "without production authority."
        ),
    },
    {
        "gate_id": "L8-G12",
        "gate": "production_acceptance_thresholds",
        "resolved": False,
        "required_next": (
            "Define explicit predictive, calibration, coverage, stability, "
            "runtime, and rollback requirements before activation."
        ),
    },
]


CLOSURE_DECISIONS = [
    {
        "decision_id": "L8-D01",
        "decision": "layer_8_diagnostic_scope_complete",
        "value": True,
        "reason": (
            "The taxonomy, profiles, overlay, observability, shadow data, "
            "quality, collection, retention, history, and integrity chain "
            "are implemented and audited."
        ),
    },
    {
        "decision_id": "L8-D02",
        "decision": "layer_8_scientifically_validated",
        "value": False,
        "reason": (
            "No point-in-time outcome join or predictive evaluation has been "
            "performed."
        ),
    },
    {
        "decision_id": "L8-D03",
        "decision": "layer_8_production_ready",
        "value": False,
        "reason": (
            "The matchup overlay has not demonstrated incremental out-of-"
            "sample value or passed production acceptance gates."
        ),
    },
    {
        "decision_id": "L8-D04",
        "decision": "production_overlay_activation_authorized",
        "value": False,
        "reason": (
            "Production and simulation authority remain explicitly denied."
        ),
    },
    {
        "decision_id": "L8-D05",
        "decision": "historical_evaluation_planning_authorized",
        "value": True,
        "reason": (
            "Layer 9 may plan a bounded point-in-time historical evaluation "
            "contract."
        ),
    },
    {
        "decision_id": "L8-D06",
        "decision": "historical_outcome_join_authorized",
        "value": False,
        "reason": (
            "Outcome joining requires a separately reviewed point-in-time "
            "contract and implementation."
        ),
    },
    {
        "decision_id": "L8-D07",
        "decision": "predictive_evaluation_execution_authorized",
        "value": False,
        "reason": (
            "Execution remains unauthorized until the historical evaluation "
            "contract is planned and implemented."
        ),
    },
    {
        "decision_id": "L8-D08",
        "decision": "tuning_backtest_pricing_edge_authorized",
        "value": False,
        "reason": (
            "Tuning, backtests, pricing, market comparison, and edge work "
            "remain out of scope."
        ),
    },
]


LAYER_9_HANDOFF_REQUIREMENTS = [
    {
        "requirement_id": "L9-H01",
        "requirement": "inventory_event_and_outcome_sources",
    },
    {
        "requirement_id": "L9-H02",
        "requirement": "define_point_in_time_feature_cutoffs",
    },
    {
        "requirement_id": "L9-H03",
        "requirement": "define_future_information_exclusion_rules",
    },
    {
        "requirement_id": "L9-H04",
        "requirement": "define_event_identity_and_join_keys",
    },
    {
        "requirement_id": "L9-H05",
        "requirement": "define_baseline_and_augmented_prediction_contracts",
    },
    {
        "requirement_id": "L9-H06",
        "requirement": "define_outcome_targets_and_evaluation_metrics",
    },
    {
        "requirement_id": "L9-H07",
        "requirement": "define_out_of_time_validation_and_ablation_design",
    },
    {
        "requirement_id": "L9-H08",
        "requirement": "preserve_shadow_only_non_authoritative_execution",
    },
]


ACCEPTANCE_CRITERIA = [
    {"criterion_id": "L8-C01", "criterion": "layer_8AG_dependency_verified"},
    {"criterion_id": "L8-C02", "criterion": "all_layer_8_workstreams_inventoried"},
    {"criterion_id": "L8-C03", "criterion": "all_layer_8_workstreams_complete"},
    {"criterion_id": "L8-C04", "criterion": "evaluation_readiness_capabilities_documented"},
    {"criterion_id": "L8-C05", "criterion": "unresolved_evaluation_gates_documented"},
    {"criterion_id": "L8-C06", "criterion": "diagnostic_scope_marked_complete"},
    {"criterion_id": "L8-C07", "criterion": "scientific_validation_marked_incomplete"},
    {"criterion_id": "L8-C08", "criterion": "production_readiness_marked_false"},
    {"criterion_id": "L8-C09", "criterion": "production_activation_unauthorized"},
    {"criterion_id": "L8-C10", "criterion": "historical_evaluation_planning_allowed"},
    {"criterion_id": "L8-C11", "criterion": "historical_outcome_join_not_yet_allowed"},
    {"criterion_id": "L8-C12", "criterion": "predictive_evaluation_not_yet_allowed"},
    {"criterion_id": "L8-C13", "criterion": "tuning_and_backtesting_not_allowed"},
    {"criterion_id": "L8-C14", "criterion": "pricing_and_edge_work_not_allowed"},
    {"criterion_id": "L8-C15", "criterion": "layer_9_handoff_requirements_defined"},
    {"criterion_id": "L8-C16", "criterion": "deterministic_csv_and_json_artifacts_emitted"},
]


PROHIBITED_AUTHORITIES = [
    "historical_outcome_join",
    "predictive_accuracy_evaluation",
    "calibration_evaluation",
    "model_training",
    "parameter_tuning",
    "threshold_tuning",
    "fallback_tuning",
    "backtest_execution",
    "production_overlay_integration",
    "production_matchup_activation",
    "production_pitch_selection_change",
    "production_pitch_sequencing_change",
    "simulation_state_change",
    "simulation_probability_change",
    "canonical_probability_authority_change",
    "physical_record_deletion",
    "retention_action_execution",
    "pricing",
    "market_comparison",
    "edge_detection",
    "bet_recommendation",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    payload: Any,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def string_constants(
    path: Path,
) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            path.read_text(
                encoding="utf-8",
                errors="ignore",
            ),
            filename=str(path),
        )
    except SyntaxError:
        return set()

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_quality_gate_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    all_workstreams_complete = all(
        row["status"] == "complete"
        for row in LAYER_8_WORKSTREAMS
    )

    all_readiness_capabilities_ready = all(
        row["ready"]
        for row in EVALUATION_READINESS_CAPABILITIES
    )

    all_evaluation_gates_unresolved = all(
        row["resolved"] is False
        for row in UNRESOLVED_EVALUATION_GATES
    )

    closure_decisions = {
        row["decision"]: row["value"]
        for row in CLOSURE_DECISIONS
    }

    planning_checks = [
        {
            "check": "eight_ag_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "seventeen_layer_8_workstreams_inventoried",
            "actual": len(LAYER_8_WORKSTREAMS),
            "expected": 17,
            "passed": len(LAYER_8_WORKSTREAMS) == 17,
        },
        {
            "check": "all_layer_8_workstreams_complete",
            "actual": all_workstreams_complete,
            "expected": True,
            "passed": all_workstreams_complete,
        },
        {
            "check": "twelve_evaluation_readiness_capabilities_defined",
            "actual": len(EVALUATION_READINESS_CAPABILITIES),
            "expected": 12,
            "passed": len(EVALUATION_READINESS_CAPABILITIES) == 12,
        },
        {
            "check": "all_readiness_capabilities_ready",
            "actual": all_readiness_capabilities_ready,
            "expected": True,
            "passed": all_readiness_capabilities_ready,
        },
        {
            "check": "twelve_unresolved_evaluation_gates_defined",
            "actual": len(UNRESOLVED_EVALUATION_GATES),
            "expected": 12,
            "passed": len(UNRESOLVED_EVALUATION_GATES) == 12,
        },
        {
            "check": "evaluation_gates_explicitly_unresolved",
            "actual": all_evaluation_gates_unresolved,
            "expected": True,
            "passed": all_evaluation_gates_unresolved,
        },
        {
            "check": "eight_closure_decisions_defined",
            "actual": len(CLOSURE_DECISIONS),
            "expected": 8,
            "passed": len(CLOSURE_DECISIONS) == 8,
        },
        {
            "check": "diagnostic_scope_marked_complete",
            "actual": closure_decisions[
                "layer_8_diagnostic_scope_complete"
            ],
            "expected": True,
            "passed": closure_decisions[
                "layer_8_diagnostic_scope_complete"
            ]
            is True,
        },
        {
            "check": "scientific_validation_marked_incomplete",
            "actual": closure_decisions[
                "layer_8_scientifically_validated"
            ],
            "expected": False,
            "passed": closure_decisions[
                "layer_8_scientifically_validated"
            ]
            is False,
        },
        {
            "check": "production_readiness_marked_false",
            "actual": closure_decisions[
                "layer_8_production_ready"
            ],
            "expected": False,
            "passed": closure_decisions[
                "layer_8_production_ready"
            ]
            is False,
        },
        {
            "check": "production_activation_unauthorized",
            "actual": closure_decisions[
                "production_overlay_activation_authorized"
            ],
            "expected": False,
            "passed": closure_decisions[
                "production_overlay_activation_authorized"
            ]
            is False,
        },
        {
            "check": "historical_evaluation_planning_authorized",
            "actual": closure_decisions[
                "historical_evaluation_planning_authorized"
            ],
            "expected": True,
            "passed": closure_decisions[
                "historical_evaluation_planning_authorized"
            ]
            is True,
        },
        {
            "check": "historical_outcome_join_unauthorized",
            "actual": closure_decisions[
                "historical_outcome_join_authorized"
            ],
            "expected": False,
            "passed": closure_decisions[
                "historical_outcome_join_authorized"
            ]
            is False,
        },
        {
            "check": "predictive_evaluation_execution_unauthorized",
            "actual": closure_decisions[
                "predictive_evaluation_execution_authorized"
            ],
            "expected": False,
            "passed": closure_decisions[
                "predictive_evaluation_execution_authorized"
            ]
            is False,
        },
        {
            "check": "eight_layer_9_handoff_requirements_defined",
            "actual": len(LAYER_9_HANDOFF_REQUIREMENTS),
            "expected": 8,
            "passed": len(LAYER_9_HANDOFF_REQUIREMENTS) == 8,
        },
        {
            "check": "sixteen_acceptance_criteria_defined",
            "actual": len(ACCEPTANCE_CRITERIA),
            "expected": 16,
            "passed": len(ACCEPTANCE_CRITERIA) == 16,
        },
        {
            "check": "tuning_backtest_pricing_edge_authority_absent",
            "actual": closure_decisions[
                "tuning_backtest_pricing_edge_authorized"
            ],
            "expected": False,
            "passed": closure_decisions[
                "tuning_backtest_pricing_edge_authorized"
            ]
            is False,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in planning_checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "Layer 8 closes under diagnostic-only, shadow-observable "
                "scope."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "point_in_time_historical_evaluation_contract_planning"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Layer 9 may plan bounded point-in-time historical evaluation "
                "without executing outcome joins or predictive evaluation."
            ),
        }
    )

    diagnosis_name = (
        "layer_8_pitch_type_matchup_overlay_shadow_evaluation_"
        "readiness_and_scope_closure_plan_complete"
        if all_checks_passed
        else
        "layer_8_pitch_type_matchup_overlay_shadow_evaluation_"
        "readiness_and_scope_closure_plan_failed"
    )

    recommended_next_layer = (
        "9A_pitch_type_matchup_overlay_point_in_time_historical_"
        "evaluation_inventory_and_contract_plan"
        if all_checks_passed
        else
        "8AH_layer_8_pitch_type_matchup_overlay_shadow_evaluation_"
        "readiness_and_scope_closure_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "layer_8_workstreams.csv": LAYER_8_WORKSTREAMS,
        "evaluation_readiness_capabilities.csv": (
            EVALUATION_READINESS_CAPABILITIES
        ),
        "unresolved_evaluation_gates.csv": (
            UNRESOLVED_EVALUATION_GATES
        ),
        "closure_decisions.csv": CLOSURE_DECISIONS,
        "layer_9_handoff_requirements.csv": (
            LAYER_9_HANDOFF_REQUIREMENTS
        ),
        "acceptance_criteria.csv": ACCEPTANCE_CRITERIA,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        "layer_8_workstreams.csv": [
            "workstream_id",
            "workstream",
            "layers",
            "status",
            "capability",
        ],
        "evaluation_readiness_capabilities.csv": [
            "capability_id",
            "capability",
            "ready",
            "evidence",
        ],
        "unresolved_evaluation_gates.csv": [
            "gate_id",
            "gate",
            "resolved",
            "required_next",
        ],
        "closure_decisions.csv": [
            "decision_id",
            "decision",
            "value",
            "reason",
        ],
        "layer_9_handoff_requirements.csv": [
            "requirement_id",
            "requirement",
        ],
        "acceptance_criteria.csv": [
            "criterion_id",
            "criterion",
        ],
        "authority_boundaries.csv": [
            "authority",
            "granted",
            "reason",
        ],
    }

    for filename, rows in artifacts.items():
        write_csv(
            OUTPUT_DIR / filename,
            fieldnames[filename],
            rows,
        )

    write_csv(
        OUTPUT_DIR / "recommended_path.csv",
        [
            "recommended_next_layer",
            "recommended_action",
            "entry_condition",
            "passed",
        ],
        [
            {
                "recommended_next_layer": recommended_next_layer,
                "recommended_action": (
                    "Plan a bounded point-in-time historical evaluation "
                    "inventory and contract."
                    if all_checks_passed
                    else
                    "Remediate failed 8AH closure checks."
                ),
                "entry_condition": (
                    "All eighteen 8AH closure checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "planning_checks_required": len(
            planning_checks
        ),
        "planning_checks_passed": sum(
            row["passed"]
            for row in planning_checks
        ),
        "layer_8_workstreams_inventoried": len(
            LAYER_8_WORKSTREAMS
        ),
        "layer_8_workstreams_complete": sum(
            row["status"] == "complete"
            for row in LAYER_8_WORKSTREAMS
        ),
        "evaluation_readiness_capabilities_defined": len(
            EVALUATION_READINESS_CAPABILITIES
        ),
        "evaluation_readiness_capabilities_ready": sum(
            row["ready"]
            for row in EVALUATION_READINESS_CAPABILITIES
        ),
        "unresolved_evaluation_gates_defined": len(
            UNRESOLVED_EVALUATION_GATES
        ),
        "closure_decisions_defined": len(
            CLOSURE_DECISIONS
        ),
        "layer_9_handoff_requirements_defined": len(
            LAYER_9_HANDOFF_REQUIREMENTS
        ),
        "acceptance_criteria_defined": len(
            ACCEPTANCE_CRITERIA
        ),
        "layer_8_scope": (
            "deterministic_diagnostic_shadow_observable_only"
        ),
        "layer_8_diagnostic_scope_complete": True,
        "layer_8_scientifically_validated": False,
        "layer_8_production_ready": False,
        "historical_outcome_joined": False,
        "predictive_evaluation_executed": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "tuning_executed": False,
        "backtest_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "closure_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer8_completed": all_checks_passed,
        "new_production_authority_granted": False,
        "historical_evaluation_contract_planning_allowed_next": (
            all_checks_passed
        ),
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "production_matchup_overlay_integration_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "recommended_next_layer": recommended_next_layer,
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / filename)
            for filename in [
                *artifacts.keys(),
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "closure_summary.json"),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        json.dumps(
            diagnosis,
            indent=2,
        )
    )

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
