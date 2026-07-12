#!/usr/bin/env python3
"""
Layer 9U
Pitch-Type Matchup Overlay Historical Comparative Metric Result Interpretation Contract Plan

Plans bounded interpretation rules for Layer 9T diagnostic metric records.

Planning only.

This layer defines:

- admissible interpretation inputs and lineage;
- interpretation statuses for emitted, suppressed, invalid, and coverage metrics;
- directional descriptions for paired metric deltas;
- support, consistency, and missingness constraints;
- language and claim prohibitions;
- deterministic interpretation-record fields and artifacts;
- authority boundaries for Layer 9V.

This layer does not:

- recompute metrics;
- estimate uncertainty or statistical significance;
- declare superiority, equivalence, activation, or production readiness;
- select thresholds or tune models;
- alter production probabilities, simulations, pricing, markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9U"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_result_interpretation_contract_plan"
)

PLAN_VERSION = (
    "layer_9U_historical_comparative_metric_result_"
    "interpretation_contract_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9U_pitch_type_matchup_overlay_"
    "historical_comparative_metric_result_interpretation_contract_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9T_pitch_type_matchup_overlay_"
    "historical_comparative_metric_calculation_contract.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9T_historical_comparative_metric_calculation_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_calculation_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_metric_result_"
    "interpretation_contract_planning"
)


INPUT_RULES = [
    {
        "rule_id": "HCINT-I01",
        "rule": "metric_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HCINT-I02",
        "rule": "metric_record_id_must_be_unique",
    },
    {
        "rule_id": "HCINT-I03",
        "rule": "source_comparison_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HCINT-I04",
        "rule": "metric_status_must_be_recognized",
    },
    {
        "rule_id": "HCINT-I05",
        "rule": "better_direction_must_match_metric_definition",
    },
    {
        "rule_id": "HCINT-I06",
        "rule": "candidate_eligible_and_excluded_counts_must_reconcile",
    },
    {
        "rule_id": "HCINT-I07",
        "rule": "suppressed_metric_records_must_not_emit_metric_values",
    },
    {
        "rule_id": "HCINT-I08",
        "rule": "eligible_performance_records_must_emit_paired_values_and_delta",
    },
    {
        "rule_id": "HCINT-I09",
        "rule": "coverage_records_must_not_emit_performance_delta",
    },
]

INTERPRETATION_STATUSES = [
    {
        "status": "directionally_lower_augmented_loss",
        "applies_when": (
            "metric_status=metric_eligible and better_direction=lower "
            "and delta<0"
        ),
    },
    {
        "status": "directionally_higher_augmented_loss",
        "applies_when": (
            "metric_status=metric_eligible and better_direction=lower "
            "and delta>0"
        ),
    },
    {
        "status": "directionally_equal_observed_value",
        "applies_when": (
            "metric_status=metric_eligible and delta=0"
        ),
    },
    {
        "status": "directionally_higher_augmented_score",
        "applies_when": (
            "metric_status=metric_eligible and better_direction=higher "
            "and delta>0"
        ),
    },
    {
        "status": "directionally_lower_augmented_score",
        "applies_when": (
            "metric_status=metric_eligible and better_direction=higher "
            "and delta<0"
        ),
    },
    {
        "status": "coverage_only",
        "applies_when": "metric_family=coverage",
    },
    {
        "status": "insufficient_support",
        "applies_when": "metric_status=insufficient_support",
    },
    {
        "status": "input_invalid",
        "applies_when": (
            "metric_status indicates incompatible, invalid, or lineage failure"
        ),
    },
    {
        "status": "not_interpretable",
        "applies_when": "no authorized interpretation rule applies",
    },
]

DIRECTION_RULES = [
    {
        "rule_id": "HCINT-D01",
        "better_direction": "lower",
        "negative_delta_meaning": "lower_augmented_observed_loss",
        "positive_delta_meaning": "higher_augmented_observed_loss",
        "zero_delta_meaning": "equal_observed_metric_value",
    },
    {
        "rule_id": "HCINT-D02",
        "better_direction": "higher",
        "negative_delta_meaning": "lower_augmented_observed_score",
        "positive_delta_meaning": "higher_augmented_observed_score",
        "zero_delta_meaning": "equal_observed_metric_value",
    },
    {
        "rule_id": "HCINT-D03",
        "better_direction": "descriptive_only",
        "negative_delta_meaning": "not_applicable",
        "positive_delta_meaning": "not_applicable",
        "zero_delta_meaning": "not_applicable",
    },
]

CLAIM_BOUNDARIES = [
    {
        "boundary_id": "HCINT-B01",
        "rule": (
            "Directional metric language must use observed or diagnostic "
            "wording only."
        ),
    },
    {
        "boundary_id": "HCINT-B02",
        "rule": (
            "A negative lower-is-better delta may not be called superiority."
        ),
    },
    {
        "boundary_id": "HCINT-B03",
        "rule": (
            "A zero delta may not be called equivalence."
        ),
    },
    {
        "boundary_id": "HCINT-B04",
        "rule": (
            "A favorable point estimate may not imply statistical significance."
        ),
    },
    {
        "boundary_id": "HCINT-B05",
        "rule": (
            "Suppressed or invalid records may not receive directional interpretation."
        ),
    },
    {
        "boundary_id": "HCINT-B06",
        "rule": (
            "Coverage metrics may describe availability only, not predictive quality."
        ),
    },
    {
        "boundary_id": "HCINT-B07",
        "rule": (
            "Subgroup direction may not be generalized beyond its aggregation key."
        ),
    },
    {
        "boundary_id": "HCINT-B08",
        "rule": (
            "No interpretation may authorize activation, production use, pricing, "
            "market comparison, or betting."
        ),
    },
]

CONSISTENCY_RULES = [
    {
        "rule_id": "HCINT-C01",
        "rule": (
            "Each interpretation must preserve metric name, aggregation name, "
            "aggregation key, support counts, and source lineage."
        ),
    },
    {
        "rule_id": "HCINT-C02",
        "rule": (
            "Conflicting directions across metrics must remain separately reported."
        ),
    },
    {
        "rule_id": "HCINT-C03",
        "rule": (
            "Conflicting directions across aggregations must remain separately reported."
        ),
    },
    {
        "rule_id": "HCINT-C04",
        "rule": (
            "No composite score may be created without a separate explicit contract."
        ),
    },
    {
        "rule_id": "HCINT-C05",
        "rule": (
            "Missingness and support limitations must accompany any directional label."
        ),
    },
    {
        "rule_id": "HCINT-C06",
        "rule": (
            "Interpretation records must be reproducible under reversed input ordering."
        ),
    },
]

INTERPRETATION_FIELDS = [
    {"ordinal": 1, "field": "interpretation_contract_version"},
    {"ordinal": 2, "field": "interpretation_record_id"},
    {"ordinal": 3, "field": "metric_record_id"},
    {"ordinal": 4, "field": "metric_name"},
    {"ordinal": 5, "field": "metric_family"},
    {"ordinal": 6, "field": "aggregation_name"},
    {"ordinal": 7, "field": "aggregation_key"},
    {"ordinal": 8, "field": "candidate_pair_count"},
    {"ordinal": 9, "field": "eligible_pair_count"},
    {"ordinal": 10, "field": "excluded_pair_count"},
    {"ordinal": 11, "field": "baseline_metric_value"},
    {"ordinal": 12, "field": "augmented_metric_value"},
    {"ordinal": 13, "field": "augmented_minus_baseline_delta"},
    {"ordinal": 14, "field": "better_direction"},
    {"ordinal": 15, "field": "support_satisfied"},
    {"ordinal": 16, "field": "source_metric_status"},
    {"ordinal": 17, "field": "interpretation_status"},
    {"ordinal": 18, "field": "directional_observation"},
    {"ordinal": 19, "field": "interpretation_limitations"},
    {"ordinal": 20, "field": "interpretation_eligible"},
    {"ordinal": 21, "field": "interpretation_exclusion_codes"},
    {"ordinal": 22, "field": "source_comparison_digest"},
    {"ordinal": 23, "field": "source_metric_record_digest"},
    {"ordinal": 24, "field": "interpretation_identity_digest"},
    {"ordinal": 25, "field": "interpretation_record_digest"},
]

EXCLUSION_CODES = [
    {
        "code": "historical_interpretation_metric_record_invalid",
        "category": "source",
    },
    {
        "code": "historical_interpretation_metric_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_interpretation_metric_status_unrecognized",
        "category": "contract",
    },
    {
        "code": "historical_interpretation_direction_unrecognized",
        "category": "contract",
    },
    {
        "code": "historical_interpretation_insufficient_support",
        "category": "support",
    },
    {
        "code": "historical_interpretation_metric_values_missing",
        "category": "value",
    },
    {
        "code": "historical_interpretation_delta_missing",
        "category": "value",
    },
    {
        "code": "historical_interpretation_coverage_only",
        "category": "coverage",
    },
    {
        "code": "historical_interpretation_source_metric_invalid",
        "category": "source",
    },
    {
        "code": "historical_interpretation_duplicate_metric_identity",
        "category": "cardinality",
    },
]

ORDERING_FIELDS = [
    {"ordinal": 1, "field": "aggregation_name"},
    {"ordinal": 2, "field": "aggregation_key"},
    {"ordinal": 3, "field": "metric_family"},
    {"ordinal": 4, "field": "metric_name"},
    {"ordinal": 5, "field": "interpretation_status"},
    {"ordinal": 6, "field": "interpretation_record_id"},
]

IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "verify_layer_9u_plan_and_layer_9t_predecessor",
    },
    {
        "ordinal": 2,
        "step": "replay_layer_9t_metric_records",
    },
    {
        "ordinal": 3,
        "step": "validate_metric_record_identity_and_lineage",
    },
    {
        "ordinal": 4,
        "step": "validate_support_counts_and_metric_status",
    },
    {
        "ordinal": 5,
        "step": "classify_coverage_suppressed_invalid_and_emitted_records",
    },
    {
        "ordinal": 6,
        "step": "apply_direction_rules_to_emitted_performance_records",
    },
    {
        "ordinal": 7,
        "step": "attach_support_missingness_and_scope_limitations",
    },
    {
        "ordinal": 8,
        "step": "enforce_claim_language_boundaries",
    },
    {
        "ordinal": 9,
        "step": "derive_interpretation_identity_and_record_digests",
    },
    {
        "ordinal": 10,
        "step": "reconcile_interpretation_counts_to_metric_records",
    },
    {
        "ordinal": 11,
        "step": "replay_interpretation_under_reversed_input_order",
    },
    {
        "ordinal": 12,
        "step": "write_temporary_diagnostic_artifacts_only",
    },
]

DIAGNOSTIC_ARTIFACTS = [
    {"artifact": "planning_checks.csv"},
    {"artifact": "input_rules.csv"},
    {"artifact": "interpretation_statuses.csv"},
    {"artifact": "direction_rules.csv"},
    {"artifact": "claim_boundaries.csv"},
    {"artifact": "consistency_rules.csv"},
    {"artifact": "interpretation_field_contract.csv"},
    {"artifact": "exclusion_code_catalog.csv"},
    {"artifact": "ordering_fields.csv"},
    {"artifact": "implementation_steps.csv"},
    {"artifact": "authority_boundaries.csv"},
    {"artifact": "interpretation_plan_summary.json"},
    {"artifact": "diagnosis.json"},
]

PROHIBITED_AUTHORITIES = [
    "activation_recommendation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "equivalence_declaration",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "pricing",
    "production_historical_prediction_materialization",
    "production_matchup_activation",
    "production_overlay_integration",
    "production_readiness_declaration",
    "simulation_probability_change",
    "simulation_state_change",
    "statistical_significance_evaluation",
    "superiority_declaration",
    "threshold_tuning",
    "uncertainty_estimation",
]


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_payload(payload: Any) -> str:
    return hashlib.sha256(
        canonical_json_bytes(payload)
    ).hexdigest()


def string_constants(path: Path) -> set[str]:
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


def write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(fieldnames),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    predecessor_verified = (
        PREDECESSOR_PATH.exists()
        and EXPECTED_PREDECESSOR_VERSION
        in predecessor_constants
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    field_names = [
        row["field"]
        for row in INTERPRETATION_FIELDS
    ]

    status_names = [
        row["status"]
        for row in INTERPRETATION_STATUSES
    ]

    checks = [
        {
            "check": "nine_t_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "nine_input_rules_defined",
            "actual": len(INPUT_RULES),
            "expected": 9,
            "passed": len(INPUT_RULES) == 9,
        },
        {
            "check": "nine_interpretation_statuses_defined",
            "actual": len(INTERPRETATION_STATUSES),
            "expected": 9,
            "passed": (
                len(INTERPRETATION_STATUSES) == 9
                and len(set(status_names)) == 9
            ),
        },
        {
            "check": "three_direction_rules_defined",
            "actual": len(DIRECTION_RULES),
            "expected": 3,
            "passed": len(DIRECTION_RULES) == 3,
        },
        {
            "check": "eight_claim_boundaries_defined",
            "actual": len(CLAIM_BOUNDARIES),
            "expected": 8,
            "passed": len(CLAIM_BOUNDARIES) == 8,
        },
        {
            "check": "six_consistency_rules_defined",
            "actual": len(CONSISTENCY_RULES),
            "expected": 6,
            "passed": len(CONSISTENCY_RULES) == 6,
        },
        {
            "check": "twenty_five_interpretation_fields_defined",
            "actual": len(INTERPRETATION_FIELDS),
            "expected": 25,
            "passed": (
                len(INTERPRETATION_FIELDS) == 25
                and len(set(field_names)) == 25
            ),
        },
        {
            "check": "ten_exclusion_codes_defined",
            "actual": len(EXCLUSION_CODES),
            "expected": 10,
            "passed": len(EXCLUSION_CODES) == 10,
        },
        {
            "check": "six_ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 6,
            "passed": len(ORDERING_FIELDS) == 6,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 12,
            "passed": len(IMPLEMENTATION_STEPS) == 12,
        },
        {
            "check": "thirteen_diagnostic_artifacts_defined",
            "actual": len(DIAGNOSTIC_ARTIFACTS),
            "expected": 13,
            "passed": len(DIAGNOSTIC_ARTIFACTS) == 13,
        },
        {
            "check": "lower_direction_rule_defined",
            "actual": True,
            "expected": True,
            "passed": any(
                row["better_direction"] == "lower"
                for row in DIRECTION_RULES
            ),
        },
        {
            "check": "higher_direction_rule_defined",
            "actual": True,
            "expected": True,
            "passed": any(
                row["better_direction"] == "higher"
                for row in DIRECTION_RULES
            ),
        },
        {
            "check": "coverage_only_interpretation_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "coverage_only"
                in set(status_names)
            ),
        },
        {
            "check": "insufficient_support_interpretation_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "insufficient_support"
                in set(status_names)
            ),
        },
        {
            "check": "superiority_claim_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                "superiority"
                in row["rule"].lower()
                for row in CLAIM_BOUNDARIES
            ),
        },
        {
            "check": "equivalence_claim_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                "equivalence"
                in row["rule"].lower()
                for row in CLAIM_BOUNDARIES
            ),
        },
        {
            "check": "metric_recomputation_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "uncertainty_not_estimated",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "superiority_not_declared",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "production_and_betting_authority_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in checks
    )

    plan_digest = sha256_payload(
        {
            "plan_version": PLAN_VERSION,
            "input_rules": INPUT_RULES,
            "interpretation_statuses": (
                INTERPRETATION_STATUSES
            ),
            "direction_rules": DIRECTION_RULES,
            "claim_boundaries": CLAIM_BOUNDARIES,
            "consistency_rules": CONSISTENCY_RULES,
            "interpretation_fields": (
                INTERPRETATION_FIELDS
            ),
            "exclusion_codes": EXCLUSION_CODES,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": (
                IMPLEMENTATION_STEPS
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_comparative_"
        "metric_result_interpretation_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "metric_result_interpretation_contract_plan_failed"
    )

    next_layer = (
        "9V_pitch_type_matchup_overlay_historical_comparative_"
        "metric_result_interpretation_contract_implementation"
        if all_checks_passed
        else
        "9U_pitch_type_matchup_overlay_historical_comparative_"
        "metric_result_interpretation_contract_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "input_rules.csv",
        ["rule_id", "rule"],
        INPUT_RULES,
    )

    write_csv(
        OUTPUT_DIR / "interpretation_statuses.csv",
        ["status", "applies_when"],
        INTERPRETATION_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "direction_rules.csv",
        [
            "rule_id",
            "better_direction",
            "negative_delta_meaning",
            "positive_delta_meaning",
            "zero_delta_meaning",
        ],
        DIRECTION_RULES,
    )

    write_csv(
        OUTPUT_DIR / "claim_boundaries.csv",
        ["boundary_id", "rule"],
        CLAIM_BOUNDARIES,
    )

    write_csv(
        OUTPUT_DIR / "consistency_rules.csv",
        ["rule_id", "rule"],
        CONSISTENCY_RULES,
    )

    write_csv(
        OUTPUT_DIR
        / "interpretation_field_contract.csv",
        ["ordinal", "field"],
        INTERPRETATION_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "exclusion_code_catalog.csv",
        ["code", "category"],
        EXCLUSION_CODES,
    )

    write_csv(
        OUTPUT_DIR / "ordering_fields.csv",
        ["ordinal", "field"],
        ORDERING_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        ["ordinal", "step"],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        [
            {
                "authority": authority,
                "granted": False,
                "reason": (
                    "Layer 9U is planning-only and grants no "
                    "uncertainty, significance, superiority, "
                    "activation, production, market, pricing, "
                    "or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_metric_result_"
                    "interpretation_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9V may classify and describe Layer 9T "
                    "diagnostic metric records using bounded "
                    "observational language only."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_verified": predecessor_verified,
        "input_rules": len(INPUT_RULES),
        "interpretation_statuses": len(
            INTERPRETATION_STATUSES
        ),
        "direction_rules": len(DIRECTION_RULES),
        "claim_boundaries": len(CLAIM_BOUNDARIES),
        "consistency_rules": len(CONSISTENCY_RULES),
        "interpretation_fields": len(
            INTERPRETATION_FIELDS
        ),
        "exclusion_codes": len(EXCLUSION_CODES),
        "ordering_fields": len(ORDERING_FIELDS),
        "implementation_steps": len(
            IMPLEMENTATION_STEPS
        ),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(checks),
        "plan_digest": plan_digest,
        "interpretation_records_materialized": 0,
        "metrics_recomputed": 0,
        "uncertainty_estimates_calculated": 0,
        "statistical_significance_tests_calculated": 0,
        "superiority_decisions_emitted": 0,
        "activation_recommendations_emitted": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "interpretation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_comparative_metric_result_"
            "interpretation_contract_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "recommended_next_layer": next_layer,
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(f"Layer: {LAYER_ID} — {LAYER_NAME}")
    print(f"Plan version: {PLAN_VERSION}")
    print(
        "Predecessor verified: "
        f"{predecessor_verified}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Interpretation statuses: "
        f"{len(INTERPRETATION_STATUSES)}"
    )
    print(
        "Interpretation fields: "
        f"{len(INTERPRETATION_FIELDS)}"
    )
    print(
        "Claim boundaries: "
        f"{len(CLAIM_BOUNDARIES)}"
    )
    print(
        "Interpretation records materialized: 0"
    )
    print("Metrics recomputed: 0")
    print("Uncertainty estimates calculated: 0")
    print(
        "Statistical significance tests calculated: 0"
    )
    print("Superiority decisions emitted: 0")
    print(
        "Activation recommendations emitted: 0"
    )
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Betting edges calculated: 0")
    print(f"Diagnosis: {diagnosis_name}")
    print(
        "Authority granted: "
        f"{diagnosis['authority_granted']}"
    )
    print(
        "Recommended next layer: "
        f"{next_layer}"
    )
    print(
        "Artifacts: "
        f"{OUTPUT_DIR.relative_to(ROOT)}"
    )

    if not all_checks_passed:
        failed_checks = [
            row["check"]
            for row in checks
            if not row["passed"]
        ]
        print(
            "FAILED CHECKS: "
            + ", ".join(failed_checks)
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
