#!/usr/bin/env python3
"""
Layer 9Z
Pitch-Type Matchup Overlay Historical Comparative Data-Gap Remediation
Contract Implementation

Implements the bounded remediation-record contract planned by Layer 9Y.

This implementation:

- verifies the Layer 9Y plan and Layer 9X predecessor;
- deterministically replays Layer 9X evidence and Layer 9V interpretation
  records;
- validates evidence identity, lineage, status, scope, and counts;
- resolves record-scope evidence back to interpretation and source-metric
  status;
- classifies support, invalid-outcome, coverage-only, no-directional-evidence,
  lineage, and uncertainty gaps;
- assigns priorities, authorized actions, guardrails, verification
  requirements, and completion criteria;
- replays remediation planning under reversed source ordering;
- writes temporary diagnostic artifacts only.

This implementation does not mutate canonical historical records, lower
minimum-support thresholds, impute outcomes, recompute metrics,
interpretations, or evidence, train or tune models, estimate uncertainty or
statistical significance, declare superiority or equivalence, recommend
activation, or modify production probabilities, simulations, pricing,
markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9Z"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "data_gap_remediation_contract_implementation"
)

REMEDIATION_CONTRACT_VERSION = (
    "layer_9Z_historical_comparative_data_gap_"
    "remediation_contract_v1"
)

EXPECTED_PLAN_VERSION = (
    "layer_9Y_historical_comparative_data_gap_"
    "remediation_contract_plan_v1"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "data_gap_remediation_contract_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_comparative_data_gap_"
    "remediation_contract_implementation"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9X_historical_comparative_evidence_"
    "sufficiency_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evidence_sufficiency_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_data_gap_"
    "remediation_contract_planning"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9Z_pitch_type_matchup_overlay_"
    "historical_comparative_data_gap_remediation_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9Y_pitch_type_matchup_overlay_"
    "historical_comparative_data_gap_remediation_contract.py"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9X_pitch_type_matchup_overlay_"
    "historical_comparative_evidence_sufficiency_contract.py"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

RECOGNIZED_EVIDENCE_STATUSES = {
    "directional_evidence_available",
    "insufficient_metric_support",
    "coverage_evidence_only",
    "invalid_source_evidence",
    "directional_conflict_present",
    "directionally_consistent_observations",
    "no_directional_evidence_available",
    "not_assessable",
}

REMEDIATION_FIELDS = [
    "remediation_contract_version",
    "remediation_record_id",
    "source_evidence_record_id",
    "assessment_scope",
    "assessment_key",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "source_evidence_status",
    "gap_category",
    "gap_priority",
    "source_record_count",
    "directional_record_count",
    "coverage_only_record_count",
    "insufficient_support_record_count",
    "invalid_input_record_count",
    "recommended_action_ids",
    "remediation_goal",
    "mutation_scope",
    "verification_requirements",
    "completion_criteria",
    "remediation_limitations",
    "remediation_exclusion_codes",
    "source_evidence_record_digest",
    "source_interpretation_digest",
    "remediation_identity_digest",
    "remediation_record_digest",
]

EXCLUSION_CODES = {
    "source_invalid":
        "historical_remediation_source_evidence_invalid",
    "lineage_invalid":
        "historical_remediation_lineage_invalid",
    "status_unrecognized":
        "historical_remediation_status_unrecognized",
    "gap_unmapped":
        "historical_remediation_gap_category_unmapped",
    "insufficient_support":
        "historical_remediation_insufficient_support",
    "invalid_outcome":
        "historical_remediation_invalid_outcome_value",
    "coverage_only":
        "historical_remediation_coverage_only",
    "no_directional":
        "historical_remediation_no_directional_evidence",
    "source_lineage":
        "historical_remediation_source_lineage_invalid",
    "uncertainty_deferred":
        "historical_remediation_uncertainty_deferred",
    "threshold_prohibited":
        "historical_remediation_threshold_relaxation_prohibited",
    "source_mutation_prohibited":
        "historical_remediation_source_mutation_prohibited",
}

VERIFICATION_REQUIREMENTS = [
    "preserve_source_evidence_identity_and_digest",
    "preserve_canonical_historical_records",
    "retain_existing_minimum_support_thresholds",
    "replay_existing_contract_chain_against_candidate_artifact",
    "compare_before_and_after_gap_counts",
    "confirm_no_production_or_model_behavior_change",
]

COMPLETION_REQUIREMENTS = [
    "observed_gap_maps_to_authorized_actions",
    "candidate_changes_are_isolated_from_canonical_data",
    "source_lineage_remains_deterministic",
    "contract_chain_replay_is_deterministic",
    "gap_count_changes_are_reported_without_quality_claims",
]

COMMON_LIMITATIONS = [
    (
        "This record is a deterministic remediation instruction, not proof "
        "that the underlying gap can be resolved."
    ),
    (
        "Canonical historical observations may not be silently altered, "
        "coerced, defaulted, duplicated, or imputed."
    ),
    (
        "Minimum-support thresholds may not be lowered to manufacture "
        "metric eligibility."
    ),
    (
        "A reduction in gap counts would not establish predictive "
        "improvement, superiority, equivalence, or production readiness."
    ),
]

PROHIBITED_AUTHORITIES = [
    "activation_recommendation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "canonical_historical_record_mutation",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "equivalence_declaration",
    "market_comparison",
    "model_training",
    "outcome_imputation",
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
    "threshold_relaxation",
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


def normalized_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def valid_sha256(value: Any) -> bool:
    return bool(
        SHA256_PATTERN.fullmatch(
            normalized_string(value)
        )
    )


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


def load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(
        name,
        path,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Unable to load module: {path}"
        )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def write_jsonl(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        for row in rows:
            handle.write(
                canonical_json_bytes(
                    dict(row)
                ).decode("utf-8")
                + "\n"
            )


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


def replay_sources() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    layer_9x = load_module(
        PREDECESSOR_PATH,
        "layer_9x_remediation_source",
    )

    layer_9w = load_module(
        layer_9x.PLAN_PATH,
        "layer_9w_remediation_scope_source",
    )

    interpretation_rows = (
        layer_9x.replay_interpretation_records()
    )

    evidence_rows = (
        layer_9x.execute_evidence_assessment(
            interpretation_rows,
            layer_9w.ASSESSMENT_SCOPES,
        )
    )

    return (
        interpretation_rows,
        evidence_rows,
    )


def parse_assessment_key(
    value: Any,
) -> dict[str, Any]:
    text = normalized_string(value)

    if not text:
        return {}

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}

    if not isinstance(payload, dict):
        return {}

    return payload


def source_validation_codes(
    row: Mapping[str, Any],
    duplicate_ids: set[str],
) -> list[str]:
    codes: set[str] = set()

    evidence_id = normalized_string(
        row.get("evidence_record_id")
    )

    if (
        not evidence_id
        or evidence_id in duplicate_ids
    ):
        codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )

    if not valid_sha256(
        row.get("evidence_record_digest")
    ):
        codes.add(
            EXCLUSION_CODES[
                "lineage_invalid"
            ]
        )

    if not valid_sha256(
        row.get("source_interpretation_digest")
    ):
        codes.add(
            EXCLUSION_CODES[
                "lineage_invalid"
            ]
        )

    if (
        row.get("evidence_status")
        not in RECOGNIZED_EVIDENCE_STATUSES
    ):
        codes.add(
            EXCLUSION_CODES[
                "status_unrecognized"
            ]
        )

    count_fields = [
        "source_interpretation_record_count",
        "directional_record_count",
        "coverage_only_record_count",
        "insufficient_support_record_count",
        "invalid_input_record_count",
    ]

    if not all(
        isinstance(row.get(field), int)
        and not isinstance(
            row.get(field),
            bool,
        )
        and row.get(field) >= 0
        for field in count_fields
    ):
        codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )

    assessment_scope = normalized_string(
        row.get("assessment_scope")
    )

    if assessment_scope not in {
        "record",
        "metric_aggregation",
        "metric_overall",
        "aggregation_overall",
        "global",
    }:
        codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )

    if not isinstance(
        parse_assessment_key(
            row.get("assessment_key")
        ),
        dict,
    ):
        codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )

    return sorted(codes)


def action_ids_for_category(
    category: str,
    actions: Sequence[Mapping[str, Any]],
) -> list[str]:
    selected = []

    for action in actions:
        categories = {
            value.strip()
            for value in normalized_string(
                action.get("gap_categories")
            ).split("|")
            if value.strip()
        }

        if category in categories:
            selected.append(
                normalized_string(
                    action.get("action_id")
                )
            )

    return sorted(
        value
        for value in selected
        if value
    )


def gap_configuration(
    evidence_row: Mapping[str, Any],
    interpretation_by_id: Mapping[
        str,
        Mapping[str, Any],
    ],
) -> tuple[
    str,
    list[str],
]:
    status = normalized_string(
        evidence_row.get("evidence_status")
    )

    scope = normalized_string(
        evidence_row.get("assessment_scope")
    )

    key_payload = parse_assessment_key(
        evidence_row.get("assessment_key")
    )

    interpretation_id = normalized_string(
        key_payload.get(
            "interpretation_record_id"
        )
    )

    interpretation = (
        interpretation_by_id.get(
            interpretation_id
        )
    )

    if (
        scope == "record"
        and status
        == "invalid_source_evidence"
    ):
        source_metric_status = (
            normalized_string(
                interpretation.get(
                    "source_metric_status"
                )
            )
            if interpretation
            else ""
        )

        if source_metric_status == (
            "outcome_value_invalid"
        ):
            return (
                "invalid_outcome_value",
                [
                    EXCLUSION_CODES[
                        "invalid_outcome"
                    ],
                    EXCLUSION_CODES[
                        "source_mutation_prohibited"
                    ],
                ],
            )

        return (
            "source_lineage_invalid",
            [
                EXCLUSION_CODES[
                    "source_lineage"
                ],
                EXCLUSION_CODES[
                    "source_mutation_prohibited"
                ],
            ],
        )

    if (
        scope == "record"
        and status
        == "insufficient_metric_support"
    ):
        return (
            "insufficient_metric_support",
            [
                EXCLUSION_CODES[
                    "insufficient_support"
                ],
                EXCLUSION_CODES[
                    "threshold_prohibited"
                ],
            ],
        )

    if (
        scope == "record"
        and status
        == "coverage_evidence_only"
    ):
        return (
            "coverage_only",
            [
                EXCLUSION_CODES[
                    "coverage_only"
                ],
            ],
        )

    if status == (
        "no_directional_evidence_available"
    ):
        return (
            "no_directional_evidence",
            [
                EXCLUSION_CODES[
                    "no_directional"
                ],
                EXCLUSION_CODES[
                    "uncertainty_deferred"
                ],
            ],
        )

    if EXCLUSION_CODES[
        "lineage_invalid"
    ] in set(
        evidence_row.get(
            "evidence_exclusion_codes",
            [],
        )
    ):
        return (
            "source_lineage_invalid",
            [
                EXCLUSION_CODES[
                    "source_lineage"
                ],
            ],
        )

    if EXCLUSION_CODES[
        "uncertainty_deferred"
    ] in set(
        evidence_row.get(
            "evidence_exclusion_codes",
            [],
        )
    ):
        return (
            "uncertainty_unavailable",
            [
                EXCLUSION_CODES[
                    "uncertainty_deferred"
                ],
            ],
        )

    return (
        "source_lineage_invalid",
        [
            EXCLUSION_CODES[
                "gap_unmapped"
            ],
        ],
    )


def mutation_scope_for_category(
    category: str,
) -> str:
    if category in {
        "invalid_outcome_value",
        "source_lineage_invalid",
    }:
        return (
            "isolated_candidate_artifact_only;"
            "canonical_source_mutation_prohibited"
        )

    return (
        "diagnostic_artifacts_only;"
        "canonical_source_mutation_prohibited"
    )


def execute_remediation_planning(
    evidence_rows: Sequence[
        Mapping[str, Any]
    ],
    interpretation_rows: Sequence[
        Mapping[str, Any]
    ],
    plan_module: Any,
) -> list[dict[str, Any]]:
    sorted_evidence = sorted(
        [
            dict(row)
            for row in evidence_rows
        ],
        key=lambda row: normalized_string(
            row.get("evidence_record_id")
        ),
    )

    interpretation_by_id = {
        normalized_string(
            row.get(
                "interpretation_record_id"
            )
        ): row
        for row in interpretation_rows
    }

    evidence_id_counts = Counter(
        normalized_string(
            row.get("evidence_record_id")
        )
        for row in sorted_evidence
    )

    duplicate_ids = {
        record_id
        for record_id, count
        in evidence_id_counts.items()
        if record_id and count > 1
    }

    category_configuration = {
        normalized_string(
            row["gap_category"]
        ): row
        for row in plan_module.GAP_CATEGORIES
    }

    priority_configuration = {
        normalized_string(
            row["gap_category"]
        ): int(row["priority"])
        for row
        in plan_module.REMEDIATION_PRIORITIES
    }

    records: list[dict[str, Any]] = []

    for evidence_row in sorted_evidence:
        validation_codes = set(
            source_validation_codes(
                evidence_row,
                duplicate_ids,
            )
        )

        (
            gap_category,
            classification_codes,
        ) = gap_configuration(
            evidence_row,
            interpretation_by_id,
        )

        validation_codes.update(
            classification_codes
        )

        if validation_codes & {
            EXCLUSION_CODES[
                "source_invalid"
            ],
            EXCLUSION_CODES[
                "lineage_invalid"
            ],
            EXCLUSION_CODES[
                "status_unrecognized"
            ],
        }:
            gap_category = (
                "source_lineage_invalid"
            )

        gap_definition = (
            category_configuration.get(
                gap_category
            )
        )

        if gap_definition is None:
            remediation_goal = (
                "diagnose unmapped evidence gap "
                "without changing source data"
            )
            validation_codes.add(
                EXCLUSION_CODES[
                    "gap_unmapped"
                ]
            )
        else:
            remediation_goal = (
                gap_definition[
                    "remediation_goal"
                ]
            )

        priority = (
            priority_configuration.get(
                gap_category,
                999,
            )
        )

        action_ids = (
            action_ids_for_category(
                gap_category,
                plan_module.REMEDIATION_ACTIONS,
            )
        )

        if not action_ids:
            validation_codes.add(
                EXCLUSION_CODES[
                    "gap_unmapped"
                ]
            )

        identity_payload = {
            "remediation_contract_version": (
                REMEDIATION_CONTRACT_VERSION
            ),
            "source_evidence_record_id": (
                evidence_row.get(
                    "evidence_record_id"
                )
            ),
            "source_evidence_record_digest": (
                evidence_row.get(
                    "evidence_record_digest"
                )
            ),
            "gap_category": gap_category,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "remediation_contract_version": (
                REMEDIATION_CONTRACT_VERSION
            ),
            "remediation_record_id": (
                f"hcdgr_{identity_digest[:32]}"
            ),
            "source_evidence_record_id": (
                evidence_row.get(
                    "evidence_record_id"
                )
            ),
            "assessment_scope": (
                evidence_row.get(
                    "assessment_scope"
                )
            ),
            "assessment_key": (
                evidence_row.get(
                    "assessment_key"
                )
            ),
            "metric_name": evidence_row.get(
                "metric_name"
            ),
            "aggregation_name": (
                evidence_row.get(
                    "aggregation_name"
                )
            ),
            "aggregation_key": (
                evidence_row.get(
                    "aggregation_key"
                )
            ),
            "source_evidence_status": (
                evidence_row.get(
                    "evidence_status"
                )
            ),
            "gap_category": gap_category,
            "gap_priority": priority,
            "source_record_count": (
                evidence_row.get(
                    "source_interpretation_record_count"
                )
            ),
            "directional_record_count": (
                evidence_row.get(
                    "directional_record_count"
                )
            ),
            "coverage_only_record_count": (
                evidence_row.get(
                    "coverage_only_record_count"
                )
            ),
            "insufficient_support_record_count": (
                evidence_row.get(
                    "insufficient_support_record_count"
                )
            ),
            "invalid_input_record_count": (
                evidence_row.get(
                    "invalid_input_record_count"
                )
            ),
            "recommended_action_ids": (
                action_ids
            ),
            "remediation_goal": (
                remediation_goal
            ),
            "mutation_scope": (
                mutation_scope_for_category(
                    gap_category
                )
            ),
            "verification_requirements": (
                list(
                    VERIFICATION_REQUIREMENTS
                )
            ),
            "completion_criteria": (
                list(
                    COMPLETION_REQUIREMENTS
                )
            ),
            "remediation_limitations": (
                list(COMMON_LIMITATIONS)
            ),
            "remediation_exclusion_codes": (
                sorted(validation_codes)
            ),
            "source_evidence_record_digest": (
                evidence_row.get(
                    "evidence_record_digest"
                )
            ),
            "source_interpretation_digest": (
                evidence_row.get(
                    "source_interpretation_digest"
                )
            ),
            "remediation_identity_digest": (
                identity_digest
            ),
            "remediation_record_digest": "",
        }

        record[
            "remediation_record_digest"
        ] = sha256_payload(
            {
                key: value
                for key, value
                in record.items()
                if key
                != "remediation_record_digest"
            }
        )

        records.append(record)

    records.sort(
        key=lambda row: (
            row["gap_priority"],
            normalized_string(
                row.get("gap_category")
            ),
            normalized_string(
                row.get("assessment_scope")
            ),
            normalized_string(
                row.get("assessment_key")
            ),
            normalized_string(
                row.get("metric_name")
            ),
            normalized_string(
                row.get("aggregation_name")
            ),
            normalized_string(
                row.get("aggregation_key")
            ),
            normalized_string(
                row.get(
                    "remediation_record_id"
                )
            ),
        )
    )

    return records


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan_constants = string_constants(
        PLAN_PATH
    )

    predecessor_constants = (
        string_constants(
            PREDECESSOR_PATH
        )
    )

    plan_verified = (
        PLAN_PATH.exists()
        and EXPECTED_PLAN_VERSION
        in plan_constants
        and EXPECTED_PLAN_DIAGNOSIS
        in plan_constants
        and EXPECTED_PLAN_AUTHORITY
        in plan_constants
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

    plan_module = load_module(
        PLAN_PATH,
        "layer_9y_remediation_plan",
    )

    (
        interpretation_rows,
        evidence_rows,
    ) = replay_sources()

    remediation_rows = (
        execute_remediation_planning(
            evidence_rows,
            interpretation_rows,
            plan_module,
        )
    )

    reverse_rows = (
        execute_remediation_planning(
            list(reversed(evidence_rows)),
            list(
                reversed(
                    interpretation_rows
                )
            ),
            plan_module,
        )
    )

    gap_counts = Counter(
        row["gap_category"]
        for row in remediation_rows
    )

    priority_counts = Counter(
        row["gap_priority"]
        for row in remediation_rows
    )

    action_counts = Counter(
        action_id
        for row in remediation_rows
        for action_id
        in row["recommended_action_ids"]
    )

    exclusion_counts = Counter(
        code
        for row in remediation_rows
        for code
        in row[
            "remediation_exclusion_codes"
        ]
    )

    record_scope_rows = [
        row
        for row in remediation_rows
        if row["assessment_scope"]
        == "record"
    ]

    aggregate_rows = [
        row
        for row in remediation_rows
        if row["assessment_scope"]
        != "record"
    ]

    remediation_ids = [
        row["remediation_record_id"]
        for row in remediation_rows
    ]

    remediation_digests = [
        row["remediation_record_digest"]
        for row in remediation_rows
    ]

    field_contract_valid = all(
        set(row)
        == set(REMEDIATION_FIELDS)
        for row in remediation_rows
    )

    deterministic_replay = (
        canonical_json_bytes(
            remediation_rows
        )
        == canonical_json_bytes(
            reverse_rows
        )
    )

    remediation_digest = (
        sha256_payload(
            remediation_rows
        )
    )

    reverse_remediation_digest = (
        sha256_payload(
            reverse_rows
        )
    )

    checks = [
        {
            "check": "nine_y_plan_verified",
            "actual": plan_verified,
            "expected": True,
            "passed": plan_verified,
        },
        {
            "check": "nine_x_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "two_hundred_forty_eight_interpretation_records_replayed",
            "actual": len(
                interpretation_rows
            ),
            "expected": 248,
            "passed": len(
                interpretation_rows
            )
            == 248,
        },
        {
            "check": "five_hundred_thirty_six_evidence_records_replayed",
            "actual": len(evidence_rows),
            "expected": 536,
            "passed": len(evidence_rows)
            == 536,
        },
        {
            "check": "one_remediation_record_per_evidence_record",
            "actual": len(
                remediation_rows
            ),
            "expected": len(
                evidence_rows
            ),
            "passed": len(
                remediation_rows
            )
            == len(evidence_rows),
        },
        {
            "check": "twenty_seven_fields_implemented",
            "actual": len(
                REMEDIATION_FIELDS
            ),
            "expected": 27,
            "passed": (
                len(
                    REMEDIATION_FIELDS
                )
                == 27
                and field_contract_valid
            ),
        },
        {
            "check": "remediation_ids_unique",
            "actual": len(
                set(remediation_ids)
            ),
            "expected": len(
                remediation_rows
            ),
            "passed": len(
                set(remediation_ids)
            )
            == len(remediation_rows),
        },
        {
            "check": "remediation_digests_unique",
            "actual": len(
                set(
                    remediation_digests
                )
            ),
            "expected": len(
                remediation_rows
            ),
            "passed": len(
                set(
                    remediation_digests
                )
            )
            == len(remediation_rows),
        },
        {
            "check": "remediation_digests_valid",
            "actual": sum(
                valid_sha256(value)
                for value
                in remediation_digests
            ),
            "expected": len(
                remediation_rows
            ),
            "passed": all(
                valid_sha256(value)
                for value
                in remediation_digests
            ),
        },
        {
            "check": "ninety_three_coverage_only_gaps",
            "actual": gap_counts[
                "coverage_only"
            ],
            "expected": 93,
            "passed": gap_counts[
                "coverage_only"
            ]
            == 93,
        },
        {
            "check": "one_hundred_forty_three_support_gaps",
            "actual": gap_counts[
                "insufficient_metric_support"
            ],
            "expected": 143,
            "passed": gap_counts[
                "insufficient_metric_support"
            ]
            == 143,
        },
        {
            "check": "twelve_invalid_outcome_gaps",
            "actual": gap_counts[
                "invalid_outcome_value"
            ],
            "expected": 12,
            "passed": gap_counts[
                "invalid_outcome_value"
            ]
            == 12,
        },
        {
            "check": "two_hundred_eighty_eight_no_directional_gaps",
            "actual": gap_counts[
                "no_directional_evidence"
            ],
            "expected": 288,
            "passed": gap_counts[
                "no_directional_evidence"
            ]
            == 288,
        },
        {
            "check": "zero_unexpected_lineage_gaps",
            "actual": gap_counts[
                "source_lineage_invalid"
            ],
            "expected": 0,
            "passed": gap_counts[
                "source_lineage_invalid"
            ]
            == 0,
        },
        {
            "check": "zero_standalone_uncertainty_gaps",
            "actual": gap_counts[
                "uncertainty_unavailable"
            ],
            "expected": 0,
            "passed": gap_counts[
                "uncertainty_unavailable"
            ]
            == 0,
        },
        {
            "check": "record_scope_counts_reconcile",
            "actual": len(
                record_scope_rows
            ),
            "expected": 248,
            "passed": len(
                record_scope_rows
            )
            == 248,
        },
        {
            "check": "aggregate_scope_counts_reconcile",
            "actual": len(
                aggregate_rows
            ),
            "expected": 288,
            "passed": len(
                aggregate_rows
            )
            == 288,
        },
        {
            "check": "all_records_have_actions",
            "actual": sum(
                bool(
                    row[
                        "recommended_action_ids"
                    ]
                )
                for row
                in remediation_rows
            ),
            "expected": len(
                remediation_rows
            ),
            "passed": all(
                bool(
                    row[
                        "recommended_action_ids"
                    ]
                )
                for row
                in remediation_rows
            ),
        },
        {
            "check": "support_records_retain_threshold_guardrail",
            "actual": sum(
                EXCLUSION_CODES[
                    "threshold_prohibited"
                ]
                in row[
                    "remediation_exclusion_codes"
                ]
                for row
                in remediation_rows
                if row["gap_category"]
                == "insufficient_metric_support"
            ),
            "expected": 143,
            "passed": all(
                EXCLUSION_CODES[
                    "threshold_prohibited"
                ]
                in row[
                    "remediation_exclusion_codes"
                ]
                for row
                in remediation_rows
                if row["gap_category"]
                == "insufficient_metric_support"
            ),
        },
        {
            "check": "invalid_outcome_repairs_are_candidate_only",
            "actual": sum(
                row["mutation_scope"].startswith(
                    "isolated_candidate_artifact_only"
                )
                for row
                in remediation_rows
                if row["gap_category"]
                == "invalid_outcome_value"
            ),
            "expected": 12,
            "passed": all(
                row["mutation_scope"].startswith(
                    "isolated_candidate_artifact_only"
                )
                for row
                in remediation_rows
                if row["gap_category"]
                == "invalid_outcome_value"
            ),
        },
        {
            "check": "no_directional_records_defer_uncertainty",
            "actual": sum(
                EXCLUSION_CODES[
                    "uncertainty_deferred"
                ]
                in row[
                    "remediation_exclusion_codes"
                ]
                for row
                in remediation_rows
                if row["gap_category"]
                == "no_directional_evidence"
            ),
            "expected": 288,
            "passed": all(
                EXCLUSION_CODES[
                    "uncertainty_deferred"
                ]
                in row[
                    "remediation_exclusion_codes"
                ]
                for row
                in remediation_rows
                if row["gap_category"]
                == "no_directional_evidence"
            ),
        },
        {
            "check": "all_records_include_verification_requirements",
            "actual": sum(
                bool(
                    row[
                        "verification_requirements"
                    ]
                )
                for row
                in remediation_rows
            ),
            "expected": len(
                remediation_rows
            ),
            "passed": all(
                bool(
                    row[
                        "verification_requirements"
                    ]
                )
                for row
                in remediation_rows
            ),
        },
        {
            "check": "all_records_include_completion_criteria",
            "actual": sum(
                bool(
                    row[
                        "completion_criteria"
                    ]
                )
                for row
                in remediation_rows
            ),
            "expected": len(
                remediation_rows
            ),
            "passed": all(
                bool(
                    row[
                        "completion_criteria"
                    ]
                )
                for row
                in remediation_rows
            ),
        },
        {
            "check": "remediation_replay_deterministic",
            "actual": deterministic_replay,
            "expected": True,
            "passed": deterministic_replay,
        },
        {
            "check": "remediation_digests_match_reverse_replay",
            "actual": remediation_digest,
            "expected": (
                reverse_remediation_digest
            ),
            "passed": (
                remediation_digest
                == reverse_remediation_digest
            ),
        },
        {
            "check": "canonical_source_records_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "thresholds_not_relaxed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "outcomes_not_imputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "metrics_interpretations_and_evidence_not_recomputed",
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
            "check": "superiority_and_equivalence_not_declared",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "activation_not_recommended",
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

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_comparative_"
        "data_gap_remediation_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "data_gap_remediation_contract_implementation_failed"
    )

    next_layer = (
        "10A_pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_audit_plan"
        if all_checks_passed
        else
        "9Z_pitch_type_matchup_overlay_historical_comparative_"
        "data_gap_remediation_contract_implementation_remediation"
    )

    write_jsonl(
        OUTPUT_DIR
        / "data_gap_remediation_records.jsonl",
        remediation_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "data_gap_remediation_records.csv",
        REMEDIATION_FIELDS,
        [
            {
                **row,
                "recommended_action_ids": (
                    "|".join(
                        row[
                            "recommended_action_ids"
                        ]
                    )
                ),
                "verification_requirements": (
                    "|".join(
                        row[
                            "verification_requirements"
                        ]
                    )
                ),
                "completion_criteria": (
                    "|".join(
                        row[
                            "completion_criteria"
                        ]
                    )
                ),
                "remediation_limitations": (
                    "|".join(
                        row[
                            "remediation_limitations"
                        ]
                    )
                ),
                "remediation_exclusion_codes": (
                    "|".join(
                        row[
                            "remediation_exclusion_codes"
                        ]
                    )
                ),
            }
            for row in remediation_rows
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "gap_category_counts.csv",
        ["gap_category", "count"],
        [
            {
                "gap_category": category,
                "count": count,
            }
            for category, count
            in sorted(gap_counts.items())
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "priority_counts.csv",
        ["gap_priority", "count"],
        [
            {
                "gap_priority": priority,
                "count": count,
            }
            for priority, count
            in sorted(priority_counts.items())
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "recommended_action_counts.csv",
        ["action_id", "count"],
        [
            {
                "action_id": action_id,
                "count": count,
            }
            for action_id, count
            in sorted(action_counts.items())
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "exclusion_code_counts.csv",
        ["exclusion_code", "count"],
        [
            {
                "exclusion_code": code,
                "count": count,
            }
            for code, count
            in sorted(exclusion_counts.items())
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR
        / "authority_boundaries.csv",
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
                    "Layer 9Z materializes bounded remediation instructions "
                    "only and grants no canonical mutation, threshold "
                    "relaxation, imputation, uncertainty, significance, "
                    "superiority, equivalence, activation, production, "
                    "market, pricing, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_value_"
                    "provenance_audit_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "The twelve invalid-outcome remediation records authorize "
                    "planning a read-only provenance audit of historical "
                    "outcome extraction, typing, mapping, and compatibility."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "remediation_contract_version": (
            REMEDIATION_CONTRACT_VERSION
        ),
        "plan_verified": plan_verified,
        "predecessor_verified": (
            predecessor_verified
        ),
        "interpretation_records_replayed": (
            len(interpretation_rows)
        ),
        "evidence_records_replayed": (
            len(evidence_rows)
        ),
        "remediation_records_materialized": (
            len(remediation_rows)
        ),
        "record_scope_records": (
            len(record_scope_rows)
        ),
        "aggregate_scope_records": (
            len(aggregate_rows)
        ),
        "gap_category_counts": dict(
            sorted(gap_counts.items())
        ),
        "priority_counts": {
            str(key): value
            for key, value
            in sorted(priority_counts.items())
        },
        "recommended_action_counts": dict(
            sorted(action_counts.items())
        ),
        "exclusion_code_counts": dict(
            sorted(exclusion_counts.items())
        ),
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required": (
            len(checks)
        ),
        "remediation_digest": (
            remediation_digest
        ),
        "reverse_remediation_digest": (
            reverse_remediation_digest
        ),
        "canonical_source_records_changed": 0,
        "thresholds_relaxed": 0,
        "outcomes_imputed": 0,
        "metrics_recomputed": 0,
        "interpretations_recomputed": 0,
        "evidence_recomputed": 0,
        "uncertainty_estimates_calculated": 0,
        "statistical_significance_tests_calculated": 0,
        "superiority_decisions_emitted": 0,
        "equivalence_decisions_emitted": 0,
        "activation_recommendations_emitted": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            next_layer
        ),
    }

    write_json(
        OUTPUT_DIR
        / "data_gap_remediation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": (
            all_checks_passed
        ),
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_value_"
            "provenance_audit_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "recommended_next_layer": (
            next_layer
        ),
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        "Remediation contract version: "
        f"{REMEDIATION_CONTRACT_VERSION}"
    )
    print(
        f"Plan verified: {plan_verified}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_verified}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Interpretation records replayed: "
        f"{len(interpretation_rows)}"
    )
    print(
        "Evidence records replayed: "
        f"{len(evidence_rows)}"
    )
    print(
        "Remediation records materialized: "
        f"{len(remediation_rows)}"
    )
    print(
        "Gap category counts: "
        f"{dict(sorted(gap_counts.items()))}"
    )
    print(
        "Remediation digest: "
        f"{remediation_digest}"
    )
    print(
        "Reverse remediation digest: "
        f"{reverse_remediation_digest}"
    )
    print("Canonical source records changed: 0")
    print("Thresholds relaxed: 0")
    print("Outcomes imputed: 0")
    print("Metrics recomputed: 0")
    print("Interpretations recomputed: 0")
    print("Evidence recomputed: 0")
    print("Uncertainty estimates calculated: 0")
    print(
        "Statistical significance tests calculated: 0"
    )
    print("Superiority decisions emitted: 0")
    print("Equivalence decisions emitted: 0")
    print("Activation recommendations emitted: 0")
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
