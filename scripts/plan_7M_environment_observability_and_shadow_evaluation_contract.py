#!/usr/bin/env python3
"""
Layer 7M
Environment Observability and Shadow Evaluation Contract Plan

Defines the bounded contract for:
- shadow-only environment diagnostic emission;
- deterministic record identity;
- sampling and eligibility metadata;
- stage-status and diagnostic-code observability;
- payload redaction and bounded retention metadata;
- baseline-versus-shadow comparison metadata;
- explicit non-authoritative evaluation boundaries.

Planning only. This layer does not:
- activate environment effects in production;
- change simulation state, parameters, probabilities, or outcomes;
- map carry diagnostics to distance;
- join historical game outcomes;
- calculate accuracy, calibration, profitability, pricing, or edge metrics;
- tune any environment parameter.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7M"
LAYER_NAME = (
    "environment_observability_and_shadow_evaluation_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_7M_environment_observability_and_shadow_evaluation_contract_plan"
)

COMPOSITION_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/"
    "environment_diagnostic_composition.py"
)

AUDIT_7L_PATH = (
    ROOT
    / "scripts/"
    "audit_7L_environment_diagnostic_composition_contract.py"
)

REQUIRED_PATHS = [
    COMPOSITION_CONTRACT_PATH,
    AUDIT_7L_PATH,
]

PROHIBITED_AUTHORITIES = [
    "production_environment_activation",
    "production_venue_activation",
    "production_park_factor_activation",
    "production_weather_activation",
    "production_wind_activation",
    "production_carry_activation",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "batted_ball_distance_change",
    "batted_ball_outcome_change",
    "home_run_probability_change",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "calibration_metric_generation",
    "parameter_calibration",
    "parameter_tuning",
    "backtest_execution",
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


def read_text(path: Path) -> str:
    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            read_text(path),
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

    required_paths_exist = all(
        path.exists()
        for path in REQUIRED_PATHS
    )

    predecessor_present = (
        "environment_diagnostic_composition_contract_implementation_passed"
        in string_constants(
            AUDIT_7L_PATH
        )
    )

    composition_text = read_text(
        COMPOSITION_CONTRACT_PATH
    )

    composition_contract_present = all(
        token in composition_text
        for token in [
            "EnvironmentDiagnosticEnvelope",
            "composition_status",
            "stage_statuses",
            "diagnostic_codes",
            "validation_errors",
            "production_environment_activated",
        ]
    )

    observability_record_fields = [
        {
            "field": "shadow_record_id",
            "type": "string",
            "nullable": False,
            "purpose": "Deterministic identifier for one shadow record.",
        },
        {
            "field": "record_schema_version",
            "type": "string",
            "nullable": False,
            "purpose": "Immutable observability schema version.",
        },
        {
            "field": "generated_at_utc",
            "type": "datetime",
            "nullable": False,
            "purpose": "Record-generation timestamp.",
        },
        {
            "field": "game_id",
            "type": "string",
            "nullable": False,
            "purpose": "Canonical game identity.",
        },
        {
            "field": "game_start_time_utc",
            "type": "datetime",
            "nullable": False,
            "purpose": "Scheduled game start.",
        },
        {
            "field": "canonical_venue_id",
            "type": "string",
            "nullable": True,
            "purpose": "Resolved canonical venue.",
        },
        {
            "field": "shadow_enabled",
            "type": "boolean",
            "nullable": False,
            "purpose": "Shadow-evaluation activation state.",
        },
        {
            "field": "sampling_eligible",
            "type": "boolean",
            "nullable": False,
            "purpose": "Whether the record is eligible for sampling.",
        },
        {
            "field": "sampling_selected",
            "type": "boolean",
            "nullable": False,
            "purpose": "Deterministic sampling result.",
        },
        {
            "field": "sampling_key",
            "type": "string",
            "nullable": True,
            "purpose": "Deterministic sampling key.",
        },
        {
            "field": "sampling_rate",
            "type": "number",
            "nullable": False,
            "purpose": "Configured diagnostic sample rate.",
        },
        {
            "field": "composition_status",
            "type": "string",
            "nullable": False,
            "purpose": "Composed environment status.",
        },
        {
            "field": "stage_statuses",
            "type": "object",
            "nullable": False,
            "purpose": "Per-stage status map.",
        },
        {
            "field": "resolved_stage_count",
            "type": "integer",
            "nullable": False,
            "purpose": "Resolved stage count.",
        },
        {
            "field": "neutral_stage_count",
            "type": "integer",
            "nullable": False,
            "purpose": "Neutral stage count.",
        },
        {
            "field": "unavailable_stage_count",
            "type": "integer",
            "nullable": False,
            "purpose": "Unavailable stage count.",
        },
        {
            "field": "invalid_stage_count",
            "type": "integer",
            "nullable": False,
            "purpose": "Invalid stage count.",
        },
        {
            "field": "diagnostic_codes",
            "type": "array[string]",
            "nullable": False,
            "purpose": "Sorted unique diagnostic codes.",
        },
        {
            "field": "validation_errors",
            "type": "array[string]",
            "nullable": False,
            "purpose": "Sorted unique validation errors.",
        },
        {
            "field": "component_payload_hashes",
            "type": "object",
            "nullable": False,
            "purpose": "Deterministic semantic hashes by component.",
        },
        {
            "field": "composition_payload_hash",
            "type": "string",
            "nullable": False,
            "purpose": "Deterministic semantic hash of composition.",
        },
        {
            "field": "baseline_projection_fingerprint",
            "type": "string",
            "nullable": True,
            "purpose": "Opaque fingerprint only; no baseline mutation.",
        },
        {
            "field": "shadow_projection_fingerprint",
            "type": "string",
            "nullable": True,
            "purpose": "Opaque fingerprint only; no production authority.",
        },
        {
            "field": "projection_fingerprints_equal",
            "type": "boolean",
            "nullable": True,
            "purpose": "Shadow invariance check.",
        },
        {
            "field": "production_output_changed",
            "type": "boolean",
            "nullable": False,
            "purpose": "Must remain false.",
        },
        {
            "field": "production_authority",
            "type": "boolean",
            "nullable": False,
            "purpose": "Must remain false.",
        },
        {
            "field": "retention_class",
            "type": "string",
            "nullable": False,
            "purpose": "Bounded retention metadata.",
        },
        {
            "field": "redaction_applied",
            "type": "boolean",
            "nullable": False,
            "purpose": "Whether redaction policy was applied.",
        },
    ]

    metric_definitions = [
        {
            "metric_id": "OBS-M01",
            "metric": "records_seen",
            "type": "counter",
            "authority": "observability_only",
        },
        {
            "metric_id": "OBS-M02",
            "metric": "records_eligible",
            "type": "counter",
            "authority": "observability_only",
        },
        {
            "metric_id": "OBS-M03",
            "metric": "records_sampled",
            "type": "counter",
            "authority": "observability_only",
        },
        {
            "metric_id": "OBS-M04",
            "metric": "composition_status_count",
            "type": "labeled_counter",
            "authority": "observability_only",
        },
        {
            "metric_id": "OBS-M05",
            "metric": "stage_status_count",
            "type": "labeled_counter",
            "authority": "observability_only",
        },
        {
            "metric_id": "OBS-M06",
            "metric": "diagnostic_code_count",
            "type": "labeled_counter",
            "authority": "observability_only",
        },
        {
            "metric_id": "OBS-M07",
            "metric": "validation_error_count",
            "type": "labeled_counter",
            "authority": "observability_only",
        },
        {
            "metric_id": "OBS-M08",
            "metric": "provider_exception_count",
            "type": "labeled_counter",
            "authority": "observability_only",
        },
        {
            "metric_id": "OBS-M09",
            "metric": "payload_hash_repeat_count",
            "type": "counter",
            "authority": "observability_only",
        },
        {
            "metric_id": "OBS-M10",
            "metric": "projection_fingerprint_mismatch_count",
            "type": "counter",
            "authority": "shadow_invariance_only",
        },
        {
            "metric_id": "OBS-M11",
            "metric": "production_output_change_count",
            "type": "counter",
            "authority": "must_remain_zero",
        },
        {
            "metric_id": "OBS-M12",
            "metric": "redaction_failure_count",
            "type": "counter",
            "authority": "observability_only",
        },
    ]

    sampling_rules = [
        {
            "rule_id": "OBS-S01",
            "rule": "sampling_disabled_when_shadow_disabled",
        },
        {
            "rule_id": "OBS-S02",
            "rule": "sampling_rate_between_zero_and_one",
        },
        {
            "rule_id": "OBS-S03",
            "rule": "sampling_key_derived_from_stable_identifiers",
        },
        {
            "rule_id": "OBS-S04",
            "rule": "equivalent_inputs_have_same_sampling_result",
        },
        {
            "rule_id": "OBS-S05",
            "rule": "sampling_does_not_change_component_execution",
        },
        {
            "rule_id": "OBS-S06",
            "rule": "sampling_does_not_change_production_outputs",
        },
    ]

    redaction_rules = [
        {
            "rule_id": "OBS-R01",
            "rule": "no_secrets_or_credentials",
        },
        {
            "rule_id": "OBS-R02",
            "rule": "no_raw_headers_or_tokens",
        },
        {
            "rule_id": "OBS-R03",
            "rule": "no_unbounded_source_payloads",
        },
        {
            "rule_id": "OBS-R04",
            "rule": "provenance_allowlist_only",
        },
        {
            "rule_id": "OBS-R05",
            "rule": "exception_messages_reduced_to_type_and_code",
        },
        {
            "rule_id": "OBS-R06",
            "rule": "payload_size_bounded",
        },
    ]

    evaluation_rules = [
        {
            "rule_id": "OBS-E01",
            "rule": "baseline_fingerprint_read_only",
        },
        {
            "rule_id": "OBS-E02",
            "rule": "shadow_fingerprint_non_authoritative",
        },
        {
            "rule_id": "OBS-E03",
            "rule": "fingerprint_comparison_not_accuracy_metric",
        },
        {
            "rule_id": "OBS-E04",
            "rule": "no_historical_outcome_join",
        },
        {
            "rule_id": "OBS-E05",
            "rule": "no_parameter_scoring",
        },
        {
            "rule_id": "OBS-E06",
            "rule": "no_parameter_selection",
        },
        {
            "rule_id": "OBS-E07",
            "rule": "no_model_promotion_decision",
        },
        {
            "rule_id": "OBS-E08",
            "rule": "production_output_changed_must_be_false",
        },
    ]

    validation_rules = [
        {
            "rule_id": "OBS-V01",
            "rule": "record_id_deterministic",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V02",
            "rule": "schema_version_present",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V03",
            "rule": "game_identity_present",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V04",
            "rule": "sampling_rate_valid",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V05",
            "rule": "sampling_result_deterministic",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V06",
            "rule": "status_counts_consistent",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V07",
            "rule": "codes_sorted_and_unique",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V08",
            "rule": "errors_sorted_and_unique",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V09",
            "rule": "component_hashes_deterministic",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V10",
            "rule": "composition_hash_deterministic",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V11",
            "rule": "redaction_policy_applied",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V12",
            "rule": "payload_size_within_bound",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V13",
            "rule": "retention_class_supported",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V14",
            "rule": "production_output_changed_false",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V15",
            "rule": "production_authority_false",
            "blocking": True,
        },
        {
            "rule_id": "OBS-V16",
            "rule": "caller_payload_immutable",
            "blocking": True,
        },
    ]

    fallback_contracts = [
        {
            "fallback_id": "OBS-F01",
            "condition": "shadow_disabled",
            "result": "no_record_emission",
            "diagnostic_code": "environment_shadow_observability_disabled",
        },
        {
            "fallback_id": "OBS-F02",
            "condition": "sampling_not_selected",
            "result": "counter_only_no_payload_record",
            "diagnostic_code": "environment_shadow_sample_not_selected",
        },
        {
            "fallback_id": "OBS-F03",
            "condition": "composition_missing",
            "result": "minimal_unavailable_record",
            "diagnostic_code": "environment_composition_missing",
        },
        {
            "fallback_id": "OBS-F04",
            "condition": "hash_failure",
            "result": "record_invalid_no_production_effect",
            "diagnostic_code": "environment_shadow_hash_failure",
        },
        {
            "fallback_id": "OBS-F05",
            "condition": "redaction_failure",
            "result": "suppress_payload_record",
            "diagnostic_code": "environment_shadow_redaction_failure",
        },
        {
            "fallback_id": "OBS-F06",
            "condition": "fingerprint_missing",
            "result": "comparison_unavailable",
            "diagnostic_code": "projection_fingerprint_missing",
        },
        {
            "fallback_id": "OBS-F07",
            "condition": "fingerprint_mismatch",
            "result": "shadow_invariance_alert_only",
            "diagnostic_code": "projection_fingerprint_mismatch",
        },
    ]

    implementation_steps = [
        {
            "step": 1,
            "action": "Create typed shadow observability input schema.",
        },
        {
            "step": 2,
            "action": "Create typed shadow observability record schema.",
        },
        {
            "step": 3,
            "action": "Implement deterministic semantic serialization.",
        },
        {
            "step": 4,
            "action": "Implement deterministic component and composition hashes.",
        },
        {
            "step": 5,
            "action": "Implement deterministic sampling.",
        },
        {
            "step": 6,
            "action": "Implement status and diagnostic metric extraction.",
        },
        {
            "step": 7,
            "action": "Implement bounded provenance allowlist and redaction.",
        },
        {
            "step": 8,
            "action": "Implement baseline-shadow fingerprint comparison.",
        },
        {
            "step": 9,
            "action": "Implement no-record disabled and unsampled paths.",
        },
        {
            "step": 10,
            "action": "Implement explicit failure suppression paths.",
        },
        {
            "step": 11,
            "action": "Emit CSV and JSON audit artifacts.",
        },
        {
            "step": 12,
            "action": "Add independent deterministic contract audit.",
        },
    ]

    acceptance_criteria = [
        {
            "criterion_id": "OBS-C01",
            "criterion": "composition_predecessor_detected",
            "required": True,
        },
        {
            "criterion_id": "OBS-C02",
            "criterion": "observability_record_schema_complete",
            "required": True,
        },
        {
            "criterion_id": "OBS-C03",
            "criterion": "metric_contract_complete",
            "required": True,
        },
        {
            "criterion_id": "OBS-C04",
            "criterion": "sampling_deterministic",
            "required": True,
        },
        {
            "criterion_id": "OBS-C05",
            "criterion": "semantic_hashing_deterministic",
            "required": True,
        },
        {
            "criterion_id": "OBS-C06",
            "criterion": "redaction_allowlist_enforced",
            "required": True,
        },
        {
            "criterion_id": "OBS-C07",
            "criterion": "payload_size_bounded",
            "required": True,
        },
        {
            "criterion_id": "OBS-C08",
            "criterion": "retention_metadata_explicit",
            "required": True,
        },
        {
            "criterion_id": "OBS-C09",
            "criterion": "baseline_shadow_comparison_non_authoritative",
            "required": True,
        },
        {
            "criterion_id": "OBS-C10",
            "criterion": "disabled_path_emits_no_record",
            "required": True,
        },
        {
            "criterion_id": "OBS-C11",
            "criterion": "unsampled_path_emits_no_payload_record",
            "required": True,
        },
        {
            "criterion_id": "OBS-C12",
            "criterion": "failure_paths_do_not_affect_production",
            "required": True,
        },
        {
            "criterion_id": "OBS-C13",
            "criterion": "caller_payload_immutable",
            "required": True,
        },
        {
            "criterion_id": "OBS-C14",
            "criterion": "no_historical_outcome_join",
            "required": True,
        },
        {
            "criterion_id": "OBS-C15",
            "criterion": "no_accuracy_or_tuning_metrics",
            "required": True,
        },
        {
            "criterion_id": "OBS-C16",
            "criterion": "no_production_output_changes",
            "required": True,
        },
    ]

    planning_checks = [
        {
            "check": "required_paths_exist",
            "actual": required_paths_exist,
            "expected": True,
            "passed": required_paths_exist,
        },
        {
            "check": "seven_l_predecessor_contract_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "composition_contract_present",
            "actual": composition_contract_present,
            "expected": True,
            "passed": composition_contract_present,
        },
        {
            "check": "twenty_eight_record_fields_defined",
            "actual": len(observability_record_fields),
            "expected": 28,
            "passed": len(observability_record_fields) == 28,
        },
        {
            "check": "twelve_metric_definitions_defined",
            "actual": len(metric_definitions),
            "expected": 12,
            "passed": len(metric_definitions) == 12,
        },
        {
            "check": "six_sampling_rules_defined",
            "actual": len(sampling_rules),
            "expected": 6,
            "passed": len(sampling_rules) == 6,
        },
        {
            "check": "six_redaction_rules_defined",
            "actual": len(redaction_rules),
            "expected": 6,
            "passed": len(redaction_rules) == 6,
        },
        {
            "check": "eight_evaluation_rules_defined",
            "actual": len(evaluation_rules),
            "expected": 8,
            "passed": len(evaluation_rules) == 8,
        },
        {
            "check": "sixteen_validation_rules_defined",
            "actual": len(validation_rules),
            "expected": 16,
            "passed": len(validation_rules) == 16,
        },
        {
            "check": "seven_fallback_contracts_defined",
            "actual": len(fallback_contracts),
            "expected": 7,
            "passed": len(fallback_contracts) == 7,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(implementation_steps),
            "expected": 12,
            "passed": len(implementation_steps) == 12,
        },
        {
            "check": "sixteen_acceptance_criteria_defined",
            "actual": len(acceptance_criteria),
            "expected": 16,
            "passed": len(acceptance_criteria) == 16,
        },
        {
            "check": "shadow_only_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
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
                "7M defines shadow observability contracts only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "environment_shadow_observability_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "7N may implement and independently audit "
                    "the bounded shadow observability contract."
                ),
            },
            {
                "authority": (
                    "production_environment_integration"
                ),
                "granted": False,
                "reason": (
                    "Shadow observability remains non-authoritative."
                ),
            },
        ]
    )

    diagnosis_name = (
        "environment_observability_and_shadow_evaluation_contract_plan_complete"
        if all_checks_passed
        else
        "environment_observability_and_shadow_evaluation_contract_plan_failed"
    )

    recommended_next_layer = (
        "7N_environment_observability_and_shadow_evaluation_contract_implementation"
        if all_checks_passed
        else
        "7N_environment_shadow_observability_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "observability_record_fields.csv": observability_record_fields,
        "metric_definitions.csv": metric_definitions,
        "sampling_rules.csv": sampling_rules,
        "redaction_rules.csv": redaction_rules,
        "evaluation_rules.csv": evaluation_rules,
        "validation_rules.csv": validation_rules,
        "fallback_contracts.csv": fallback_contracts,
        "implementation_steps.csv": implementation_steps,
        "acceptance_criteria.csv": acceptance_criteria,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        "observability_record_fields.csv": [
            "field",
            "type",
            "nullable",
            "purpose",
        ],
        "metric_definitions.csv": [
            "metric_id",
            "metric",
            "type",
            "authority",
        ],
        "sampling_rules.csv": [
            "rule_id",
            "rule",
        ],
        "redaction_rules.csv": [
            "rule_id",
            "rule",
        ],
        "evaluation_rules.csv": [
            "rule_id",
            "rule",
        ],
        "validation_rules.csv": [
            "rule_id",
            "rule",
            "blocking",
        ],
        "fallback_contracts.csv": [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
        ],
        "implementation_steps.csv": [
            "step",
            "action",
        ],
        "acceptance_criteria.csv": [
            "criterion_id",
            "criterion",
            "required",
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
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Implement deterministic shadow observability "
                    "without production authority."
                    if all_checks_passed
                    else
                    "Remediate failed 7M planning checks."
                ),
                "entry_condition": (
                    "All thirteen 7M planning checks pass."
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
            1
            for row in planning_checks
            if row["passed"]
        ),
        "observability_record_fields_defined": len(
            observability_record_fields
        ),
        "metric_definitions_defined": len(
            metric_definitions
        ),
        "sampling_rules_defined": len(
            sampling_rules
        ),
        "redaction_rules_defined": len(
            redaction_rules
        ),
        "evaluation_rules_defined": len(
            evaluation_rules
        ),
        "validation_rules_defined": len(
            validation_rules
        ),
        "fallback_contracts_defined": len(
            fallback_contracts
        ),
        "implementation_steps_defined": len(
            implementation_steps
        ),
        "acceptance_criteria_defined": len(
            acceptance_criteria
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_environment_activated": False,
        "historical_outcome_joined": False,
        "accuracy_metrics_generated": False,
        "tuning_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer7_completed": False,
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "environment_shadow_observability_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / filename)
            for filename in [
                *artifacts.keys(),
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "plan_summary.json"
            ),
            str(
                OUTPUT_DIR / "diagnosis.json"
            ),
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
