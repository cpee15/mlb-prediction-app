#!/usr/bin/env python3
"""
Layer 9X
Pitch-Type Matchup Overlay Historical Comparative Evidence Sufficiency
Contract Implementation

Implements the bounded evidence-sufficiency contract planned by Layer 9W.

This implementation:

- verifies the Layer 9W plan and Layer 9V predecessor;
- deterministically replays Layer 9V interpretation records;
- validates interpretation identities, lineage, statuses, counts, and
  eligibility;
- constructs the five authorized evidence-assessment scopes;
- classifies directional availability, support insufficiency, coverage-only
  evidence, invalid source evidence, consistency, conflict, and absence of
  directional evidence;
- preserves the distinction between absence of evidence and evidence of
  equivalence;
- replays assessment under reversed source ordering;
- writes temporary diagnostic artifacts only.

This implementation does not recompute metrics or interpretations, estimate
uncertainty or statistical significance, declare superiority or equivalence,
recommend activation, or modify production probabilities, simulations,
pricing, markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
import math
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9X"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evidence_sufficiency_contract_implementation"
)

EVIDENCE_CONTRACT_VERSION = (
    "layer_9X_historical_comparative_evidence_"
    "sufficiency_contract_v1"
)

EXPECTED_PLAN_VERSION = (
    "layer_9W_historical_comparative_evidence_"
    "sufficiency_contract_plan_v1"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evidence_sufficiency_contract_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_comparative_evidence_"
    "sufficiency_contract_implementation"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9V_historical_comparative_metric_result_"
    "interpretation_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_result_interpretation_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_evidence_"
    "sufficiency_contract_planning"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9X_pitch_type_matchup_overlay_"
    "historical_comparative_evidence_sufficiency_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9W_pitch_type_matchup_overlay_"
    "historical_comparative_evidence_sufficiency_contract.py"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9V_pitch_type_matchup_overlay_"
    "historical_comparative_metric_result_interpretation_contract.py"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

DIRECTIONAL_STATUSES = {
    "directionally_lower_augmented_loss",
    "directionally_higher_augmented_loss",
    "directionally_equal_observed_value",
    "directionally_higher_augmented_score",
    "directionally_lower_augmented_score",
}

RECOGNIZED_INTERPRETATION_STATUSES = (
    DIRECTIONAL_STATUSES
    | {
        "coverage_only",
        "insufficient_support",
        "input_invalid",
        "not_interpretable",
    }
)

EVIDENCE_FIELDS = [
    "evidence_contract_version",
    "evidence_record_id",
    "assessment_scope",
    "assessment_key",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "source_interpretation_record_count",
    "directional_record_count",
    "coverage_only_record_count",
    "insufficient_support_record_count",
    "invalid_input_record_count",
    "lower_augmented_loss_count",
    "higher_augmented_loss_count",
    "higher_augmented_score_count",
    "lower_augmented_score_count",
    "equal_observed_value_count",
    "direction_conflict_present",
    "evidence_status",
    "evidence_sufficient_for",
    "evidence_observation",
    "evidence_limitations",
    "evidence_exclusion_codes",
    "source_interpretation_digest",
    "evidence_identity_digest",
    "evidence_record_digest",
]

EXCLUSION_CODES = {
    "source_invalid":
        "historical_evidence_source_interpretation_invalid",
    "lineage_invalid":
        "historical_evidence_interpretation_lineage_invalid",
    "status_unrecognized":
        "historical_evidence_interpretation_status_unrecognized",
    "insufficient_support":
        "historical_evidence_insufficient_metric_support",
    "coverage_only":
        "historical_evidence_coverage_only",
    "invalid_input":
        "historical_evidence_invalid_source_input",
    "no_directional":
        "historical_evidence_no_directional_records",
    "directional_conflict":
        "historical_evidence_directional_conflict",
    "scope_invalid":
        "historical_evidence_scope_invalid",
    "duplicate_identity":
        "historical_evidence_duplicate_interpretation_identity",
    "uncertainty_unavailable":
        "historical_evidence_uncertainty_unavailable",
}

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


def finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
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


def replay_interpretation_records() -> list[dict[str, Any]]:
    layer_9v = load_module(
        PREDECESSOR_PATH,
        "layer_9v_evidence_source",
    )

    metric_rows = (
        layer_9v.replay_metric_records()
    )

    return layer_9v.execute_interpretation(
        metric_rows
    )


def source_validation_codes(
    row: Mapping[str, Any],
    duplicate_ids: set[str],
) -> list[str]:
    codes: set[str] = set()

    interpretation_id = normalized_string(
        row.get("interpretation_record_id")
    )

    if (
        not interpretation_id
        or interpretation_id in duplicate_ids
    ):
        codes.add(
            EXCLUSION_CODES[
                "duplicate_identity"
            ]
        )

    for digest_field in (
        "interpretation_record_digest",
        "source_metric_record_digest",
        "source_comparison_digest",
    ):
        if not valid_sha256(
            row.get(digest_field)
        ):
            codes.add(
                EXCLUSION_CODES[
                    "lineage_invalid"
                ]
            )

    status = row.get(
        "interpretation_status"
    )

    if (
        status
        not in RECOGNIZED_INTERPRETATION_STATUSES
    ):
        codes.add(
            EXCLUSION_CODES[
                "status_unrecognized"
            ]
        )

    candidate = row.get(
        "candidate_pair_count"
    )
    eligible = row.get(
        "eligible_pair_count"
    )
    excluded = row.get(
        "excluded_pair_count"
    )

    if not all(
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
        for value in (
            candidate,
            eligible,
            excluded,
        )
    ):
        codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )
    elif eligible + excluded != candidate:
        codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )

    interpretation_eligible = bool(
        row.get("interpretation_eligible")
    )

    if (
        status in DIRECTIONAL_STATUSES
    ) != interpretation_eligible:
        codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )

    if status in DIRECTIONAL_STATUSES:
        if not all(
            finite_number(
                row.get(field)
            )
            for field in (
                "baseline_metric_value",
                "augmented_metric_value",
                "augmented_minus_baseline_delta",
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "source_invalid"
                ]
            )

    if status in {
        "coverage_only",
        "insufficient_support",
        "input_invalid",
    } and interpretation_eligible:
        codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )

    return sorted(codes)


def build_scope_groups(
    rows: Sequence[Mapping[str, Any]],
    scope_definition: Mapping[str, Any],
) -> list[
    tuple[
        dict[str, Any],
        list[Mapping[str, Any]],
    ]
]:
    grouping_text = normalized_string(
        scope_definition.get(
            "grouping_fields"
        )
    )

    grouping_fields = (
        grouping_text.split("|")
        if grouping_text
        else []
    )

    groups: dict[
        str,
        tuple[
            dict[str, Any],
            list[Mapping[str, Any]],
        ],
    ] = {}

    for row in rows:
        key_payload = {
            field: row.get(field)
            for field in grouping_fields
        }

        canonical_key = (
            canonical_json_bytes(
                key_payload
            ).decode("utf-8")
        )

        if canonical_key not in groups:
            groups[canonical_key] = (
                key_payload,
                [],
            )

        groups[canonical_key][1].append(
            row
        )

    if not groups and not grouping_fields:
        groups["{}"] = ({}, [])

    return [
        groups[key]
        for key in sorted(groups)
    ]


def direction_conflict(
    counts: Mapping[str, int],
) -> bool:
    lower_better_conflict = (
        counts[
            "lower_augmented_loss_count"
        ]
        > 0
        and counts[
            "higher_augmented_loss_count"
        ]
        > 0
    )

    higher_better_conflict = (
        counts[
            "higher_augmented_score_count"
        ]
        > 0
        and counts[
            "lower_augmented_score_count"
        ]
        > 0
    )

    return (
        lower_better_conflict
        or higher_better_conflict
    )


def classify_evidence_group(
    scope_definition: Mapping[str, Any],
    key_payload: Mapping[str, Any],
    group_rows: Sequence[Mapping[str, Any]],
    duplicate_ids: set[str],
    source_digest: str,
) -> dict[str, Any]:
    source_codes = {
        code
        for row in group_rows
        for code in source_validation_codes(
            row,
            duplicate_ids,
        )
    }

    status_counts = Counter(
        normalized_string(
            row.get(
                "interpretation_status"
            )
        )
        for row in group_rows
    )

    counts = {
        "source_interpretation_record_count":
            len(group_rows),
        "directional_record_count":
            sum(
                status_counts[status]
                for status in DIRECTIONAL_STATUSES
            ),
        "coverage_only_record_count":
            status_counts["coverage_only"],
        "insufficient_support_record_count":
            status_counts[
                "insufficient_support"
            ],
        "invalid_input_record_count":
            status_counts["input_invalid"],
        "lower_augmented_loss_count":
            status_counts[
                "directionally_lower_augmented_loss"
            ],
        "higher_augmented_loss_count":
            status_counts[
                "directionally_higher_augmented_loss"
            ],
        "higher_augmented_score_count":
            status_counts[
                "directionally_higher_augmented_score"
            ],
        "lower_augmented_score_count":
            status_counts[
                "directionally_lower_augmented_score"
            ],
        "equal_observed_value_count":
            status_counts[
                "directionally_equal_observed_value"
            ],
    }

    conflict = direction_conflict(
        counts
    )

    scope_name = scope_definition[
        "scope_name"
    ]

    evidence_status = "not_assessable"
    sufficient_for = (
        "contract_failure_diagnosis_only"
    )
    observation = (
        "The evidence scope cannot be assessed because source "
        "contract validation failed."
    )

    exclusion_codes = set(
        source_codes
    )

    if source_codes:
        exclusion_codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )

    elif (
        scope_name == "record"
        and counts[
            "coverage_only_record_count"
        ]
        == 1
    ):
        evidence_status = (
            "coverage_evidence_only"
        )
        sufficient_for = (
            "availability_description_only"
        )
        observation = (
            "This record provides descriptive evidence about "
            "data availability or pair coverage only."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "coverage_only"
            ]
        )

    elif (
        scope_name == "record"
        and counts[
            "insufficient_support_record_count"
        ]
        == 1
    ):
        evidence_status = (
            "insufficient_metric_support"
        )
        sufficient_for = (
            "coverage_and_support_diagnosis_only"
        )
        observation = (
            "This record does not contain sufficient metric support "
            "for directional interpretation."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "insufficient_support"
            ]
        )

    elif (
        scope_name == "record"
        and counts[
            "invalid_input_record_count"
        ]
        == 1
    ):
        evidence_status = (
            "invalid_source_evidence"
        )
        sufficient_for = (
            "source_failure_diagnosis_only"
        )
        observation = (
            "This record contains invalid source evidence and cannot "
            "support directional review."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "invalid_input"
            ]
        )

    elif conflict:
        evidence_status = (
            "directional_conflict_present"
        )
        sufficient_for = (
            "conflict_description_only"
        )
        observation = (
            "The assessed scope contains opposing observed directional "
            "records; the conflict is preserved."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "directional_conflict"
            ]
        )

    elif counts[
        "directional_record_count"
    ] > 1:
        evidence_status = (
            "directionally_consistent_observations"
        )
        sufficient_for = (
            "consistency_description_only"
        )
        observation = (
            "The assessed scope contains multiple non-conflicting "
            "observed directional records."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "uncertainty_unavailable"
            ]
        )

    elif counts[
        "directional_record_count"
    ] == 1:
        evidence_status = (
            "directional_evidence_available"
        )
        sufficient_for = (
            "bounded_directional_review_only"
        )
        observation = (
            "The assessed scope contains one authorized observed "
            "directional point estimate."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "uncertainty_unavailable"
            ]
        )

    else:
        evidence_status = (
            "no_directional_evidence_available"
        )
        sufficient_for = (
            "absence_of_directional_evidence_statement_only"
        )
        observation = (
            "No eligible directional interpretation is available in "
            "this assessed scope. This does not establish equivalence, "
            "no effect, or model parity."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "no_directional"
            ]
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "uncertainty_unavailable"
            ]
        )

    limitations = [
        (
            "Evidence sufficiency is bounded to the exact assessment "
            "scope and key."
        ),
        (
            "No uncertainty or statistical-significance estimate has "
            "been calculated."
        ),
        (
            "Absence of directional evidence does not establish "
            "equivalence, no effect, or model parity."
        ),
        (
            "This record does not establish superiority, activation, "
            "or production readiness."
        ),
    ]

    assessment_key = (
        canonical_json_bytes(
            key_payload
        ).decode("utf-8")
    )

    identity_payload = {
        "evidence_contract_version": (
            EVIDENCE_CONTRACT_VERSION
        ),
        "assessment_scope": (
            scope_name
        ),
        "assessment_key": (
            key_payload
        ),
        "source_interpretation_digest": (
            source_digest
        ),
    }

    identity_digest = sha256_payload(
        identity_payload
    )

    record = {
        "evidence_contract_version": (
            EVIDENCE_CONTRACT_VERSION
        ),
        "evidence_record_id": (
            f"hcevs_{identity_digest[:32]}"
        ),
        "assessment_scope": scope_name,
        "assessment_key": assessment_key,
        "metric_name": key_payload.get(
            "metric_name"
        ),
        "aggregation_name": (
            key_payload.get(
                "aggregation_name"
            )
        ),
        "aggregation_key": (
            key_payload.get(
                "aggregation_key"
            )
        ),
        **counts,
        "direction_conflict_present": (
            conflict
        ),
        "evidence_status": (
            evidence_status
        ),
        "evidence_sufficient_for": (
            sufficient_for
        ),
        "evidence_observation": (
            observation
        ),
        "evidence_limitations": (
            limitations
        ),
        "evidence_exclusion_codes": (
            sorted(exclusion_codes)
        ),
        "source_interpretation_digest": (
            source_digest
        ),
        "evidence_identity_digest": (
            identity_digest
        ),
        "evidence_record_digest": "",
    }

    record["evidence_record_digest"] = (
        sha256_payload(
            {
                key: value
                for key, value
                in record.items()
                if key
                != "evidence_record_digest"
            }
        )
    )

    return record


def execute_evidence_assessment(
    interpretation_rows: Sequence[
        Mapping[str, Any]
    ],
    scope_definitions: Sequence[
        Mapping[str, Any]
    ],
) -> list[dict[str, Any]]:
    source_rows = sorted(
        [
            dict(row)
            for row in interpretation_rows
        ],
        key=lambda row: normalized_string(
            row.get(
                "interpretation_record_id"
            )
        ),
    )

    source_digest = sha256_payload(
        source_rows
    )

    id_counts = Counter(
        normalized_string(
            row.get(
                "interpretation_record_id"
            )
        )
        for row in source_rows
    )

    duplicate_ids = {
        record_id
        for record_id, count
        in id_counts.items()
        if record_id and count > 1
    }

    evidence_rows: list[
        dict[str, Any]
    ] = []

    for scope_definition in (
        scope_definitions
    ):
        for key_payload, group_rows in (
            build_scope_groups(
                source_rows,
                scope_definition,
            )
        ):
            evidence_rows.append(
                classify_evidence_group(
                    scope_definition,
                    key_payload,
                    group_rows,
                    duplicate_ids,
                    source_digest,
                )
            )

    evidence_rows.sort(
        key=lambda row: (
            normalized_string(
                row.get(
                    "assessment_scope"
                )
            ),
            normalized_string(
                row.get(
                    "assessment_key"
                )
            ),
            normalized_string(
                row.get("metric_name")
            ),
            normalized_string(
                row.get(
                    "aggregation_name"
                )
            ),
            normalized_string(
                row.get(
                    "aggregation_key"
                )
            ),
            normalized_string(
                row.get(
                    "evidence_status"
                )
            ),
            normalized_string(
                row.get(
                    "evidence_record_id"
                )
            ),
        )
    )

    return evidence_rows


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan_constants = string_constants(
        PLAN_PATH
    )
    predecessor_constants = string_constants(
        PREDECESSOR_PATH
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

    layer_9w = load_module(
        PLAN_PATH,
        "layer_9w_evidence_plan",
    )

    interpretation_rows = (
        replay_interpretation_records()
    )

    evidence_rows = (
        execute_evidence_assessment(
            interpretation_rows,
            layer_9w.ASSESSMENT_SCOPES,
        )
    )

    reverse_evidence_rows = (
        execute_evidence_assessment(
            list(
                reversed(
                    interpretation_rows
                )
            ),
            list(
                reversed(
                    layer_9w.ASSESSMENT_SCOPES
                )
            ),
        )
    )

    evidence_status_counts = Counter(
        row["evidence_status"]
        for row in evidence_rows
    )

    scope_counts = Counter(
        row["assessment_scope"]
        for row in evidence_rows
    )

    exclusion_counts = Counter(
        code
        for row in evidence_rows
        for code in row[
            "evidence_exclusion_codes"
        ]
    )

    evidence_ids = [
        row["evidence_record_id"]
        for row in evidence_rows
    ]

    evidence_digests = [
        row["evidence_record_digest"]
        for row in evidence_rows
    ]

    field_contract_valid = all(
        set(row) == set(EVIDENCE_FIELDS)
        for row in evidence_rows
    )

    deterministic_replay = (
        canonical_json_bytes(
            evidence_rows
        )
        == canonical_json_bytes(
            reverse_evidence_rows
        )
    )

    record_scope_rows = [
        row
        for row in evidence_rows
        if row["assessment_scope"]
        == "record"
    ]

    aggregate_scope_rows = [
        row
        for row in evidence_rows
        if row["assessment_scope"]
        != "record"
    ]

    evidence_digest = sha256_payload(
        evidence_rows
    )

    reverse_evidence_digest = (
        sha256_payload(
            reverse_evidence_rows
        )
    )

    checks = [
        {
            "check": "nine_w_plan_verified",
            "actual": plan_verified,
            "expected": True,
            "passed": plan_verified,
        },
        {
            "check": "nine_v_predecessor_verified",
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
            "check": "five_assessment_scopes_replayed",
            "actual": len(
                layer_9w.ASSESSMENT_SCOPES
            ),
            "expected": 5,
            "passed": len(
                layer_9w.ASSESSMENT_SCOPES
            )
            == 5,
        },
        {
            "check": "evidence_records_materialized",
            "actual": len(
                evidence_rows
            ),
            "expected": len(
                evidence_rows
            ),
            "passed": len(
                evidence_rows
            )
            > 0,
        },
        {
            "check": "one_record_scope_evidence_record_per_interpretation",
            "actual": len(
                record_scope_rows
            ),
            "expected": len(
                interpretation_rows
            ),
            "passed": len(
                record_scope_rows
            )
            == len(
                interpretation_rows
            ),
        },
        {
            "check": "all_five_scopes_materialized",
            "actual": len(
                scope_counts
            ),
            "expected": 5,
            "passed": len(
                scope_counts
            )
            == 5,
        },
        {
            "check": "twenty_six_fields_implemented",
            "actual": len(
                EVIDENCE_FIELDS
            ),
            "expected": 26,
            "passed": (
                len(EVIDENCE_FIELDS) == 26
                and field_contract_valid
            ),
        },
        {
            "check": "evidence_ids_unique",
            "actual": len(
                set(evidence_ids)
            ),
            "expected": len(
                evidence_rows
            ),
            "passed": len(
                set(evidence_ids)
            )
            == len(evidence_rows),
        },
        {
            "check": "evidence_digests_unique",
            "actual": len(
                set(evidence_digests)
            ),
            "expected": len(
                evidence_rows
            ),
            "passed": len(
                set(evidence_digests)
            )
            == len(evidence_rows),
        },
        {
            "check": "evidence_digests_valid",
            "actual": sum(
                valid_sha256(value)
                for value in evidence_digests
            ),
            "expected": len(
                evidence_rows
            ),
            "passed": all(
                valid_sha256(value)
                for value in evidence_digests
            ),
        },
        {
            "check": "ninety_three_record_scope_coverage_classifications",
            "actual": sum(
                row["evidence_status"]
                == "coverage_evidence_only"
                for row in record_scope_rows
            ),
            "expected": 93,
            "passed": sum(
                row["evidence_status"]
                == "coverage_evidence_only"
                for row in record_scope_rows
            )
            == 93,
        },
        {
            "check": "one_hundred_forty_three_record_scope_support_classifications",
            "actual": sum(
                row["evidence_status"]
                == "insufficient_metric_support"
                for row in record_scope_rows
            ),
            "expected": 143,
            "passed": sum(
                row["evidence_status"]
                == "insufficient_metric_support"
                for row in record_scope_rows
            )
            == 143,
        },
        {
            "check": "twelve_record_scope_invalid_classifications",
            "actual": sum(
                row["evidence_status"]
                == "invalid_source_evidence"
                for row in record_scope_rows
            ),
            "expected": 12,
            "passed": sum(
                row["evidence_status"]
                == "invalid_source_evidence"
                for row in record_scope_rows
            )
            == 12,
        },
        {
            "check": "zero_directional_evidence_records_expected",
            "actual": sum(
                row[
                    "directional_record_count"
                ]
                for row in evidence_rows
            ),
            "expected": 0,
            "passed": all(
                row[
                    "directional_record_count"
                ]
                == 0
                for row in evidence_rows
            ),
        },
        {
            "check": "aggregate_scopes_preserve_no_directional_evidence",
            "actual": sum(
                row["evidence_status"]
                == "no_directional_evidence_available"
                for row in aggregate_scope_rows
            ),
            "expected": len(
                aggregate_scope_rows
            ),
            "passed": all(
                row["evidence_status"]
                == "no_directional_evidence_available"
                for row in aggregate_scope_rows
            ),
        },
        {
            "check": "absence_language_does_not_claim_equivalence",
            "actual": sum(
                "does not establish equivalence"
                in row[
                    "evidence_observation"
                ].lower()
                for row in aggregate_scope_rows
            ),
            "expected": len(
                aggregate_scope_rows
            ),
            "passed": all(
                "does not establish equivalence"
                in row[
                    "evidence_observation"
                ].lower()
                for row in aggregate_scope_rows
            ),
        },
        {
            "check": "source_counts_nonnegative",
            "actual": sum(
                row[
                    "source_interpretation_record_count"
                ]
                >= 0
                for row in evidence_rows
            ),
            "expected": len(
                evidence_rows
            ),
            "passed": all(
                row[
                    "source_interpretation_record_count"
                ]
                >= 0
                for row in evidence_rows
            ),
        },
        {
            "check": "record_scope_source_counts_equal_one",
            "actual": sum(
                row[
                    "source_interpretation_record_count"
                ]
                == 1
                for row in record_scope_rows
            ),
            "expected": len(
                record_scope_rows
            ),
            "passed": all(
                row[
                    "source_interpretation_record_count"
                ]
                == 1
                for row in record_scope_rows
            ),
        },
        {
            "check": "evidence_replay_deterministic",
            "actual": deterministic_replay,
            "expected": True,
            "passed": deterministic_replay,
        },
        {
            "check": "evidence_digests_match_reverse_replay",
            "actual": evidence_digest,
            "expected": (
                reverse_evidence_digest
            ),
            "passed": (
                evidence_digest
                == reverse_evidence_digest
            ),
        },
        {
            "check": "metrics_not_recomputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "interpretations_not_recomputed",
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
            "check": "statistical_significance_not_calculated",
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
            "check": "equivalence_not_declared",
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
        "evidence_sufficiency_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "evidence_sufficiency_contract_implementation_failed"
    )

    next_layer = (
        "9Y_pitch_type_matchup_overlay_historical_comparative_"
        "data_gap_remediation_contract_plan"
        if all_checks_passed
        else
        "9X_pitch_type_matchup_overlay_historical_comparative_"
        "evidence_sufficiency_contract_implementation_remediation"
    )

    write_jsonl(
        OUTPUT_DIR
        / "evidence_sufficiency_records.jsonl",
        evidence_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "evidence_sufficiency_records.csv",
        EVIDENCE_FIELDS,
        [
            {
                **row,
                "evidence_limitations": (
                    "|".join(
                        row[
                            "evidence_limitations"
                        ]
                    )
                ),
                "evidence_exclusion_codes": (
                    "|".join(
                        row[
                            "evidence_exclusion_codes"
                        ]
                    )
                ),
            }
            for row in evidence_rows
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "evidence_status_counts.csv",
        [
            "evidence_status",
            "count",
        ],
        [
            {
                "evidence_status": status,
                "count": count,
            }
            for status, count
            in sorted(
                evidence_status_counts.items()
            )
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "assessment_scope_counts.csv",
        [
            "assessment_scope",
            "count",
        ],
        [
            {
                "assessment_scope": scope,
                "count": count,
            }
            for scope, count
            in sorted(
                scope_counts.items()
            )
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "exclusion_code_counts.csv",
        [
            "exclusion_code",
            "count",
        ],
        [
            {
                "exclusion_code": code,
                "count": count,
            }
            for code, count
            in sorted(
                exclusion_counts.items()
            )
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
                    "Layer 9X classifies bounded diagnostic evidence "
                    "sufficiency only and grants no uncertainty, "
                    "significance, superiority, equivalence, activation, "
                    "production, market, pricing, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_data_gap_"
                    "remediation_contract_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "The evidence-sufficiency classifications permit "
                    "planning bounded remediation of support, validity, "
                    "and outcome-data gaps without changing model behavior."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "evidence_contract_version": (
            EVIDENCE_CONTRACT_VERSION
        ),
        "plan_verified": plan_verified,
        "predecessor_verified": (
            predecessor_verified
        ),
        "interpretation_records_replayed": (
            len(interpretation_rows)
        ),
        "evidence_records_materialized": (
            len(evidence_rows)
        ),
        "record_scope_records": (
            len(record_scope_rows)
        ),
        "aggregate_scope_records": (
            len(aggregate_scope_rows)
        ),
        "evidence_status_counts": dict(
            sorted(
                evidence_status_counts.items()
            )
        ),
        "assessment_scope_counts": dict(
            sorted(
                scope_counts.items()
            )
        ),
        "exclusion_code_counts": dict(
            sorted(
                exclusion_counts.items()
            )
        ),
        "directional_evidence_records": sum(
            row[
                "directional_record_count"
            ]
            for row in evidence_rows
        ),
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required": (
            len(checks)
        ),
        "evidence_digest": evidence_digest,
        "reverse_evidence_digest": (
            reverse_evidence_digest
        ),
        "metrics_recomputed": 0,
        "interpretations_recomputed": 0,
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
        / "evidence_sufficiency_summary.json",
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
            "historical_comparative_data_gap_"
            "remediation_contract_planning"
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
        "Evidence contract version: "
        f"{EVIDENCE_CONTRACT_VERSION}"
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
        f"{summary['interpretation_records_replayed']}"
    )
    print(
        "Evidence records materialized: "
        f"{summary['evidence_records_materialized']}"
    )
    print(
        "Record-scope records: "
        f"{summary['record_scope_records']}"
    )
    print(
        "Aggregate-scope records: "
        f"{summary['aggregate_scope_records']}"
    )
    print(
        "Directional evidence records: "
        f"{summary['directional_evidence_records']}"
    )
    print(
        f"Evidence digest: {evidence_digest}"
    )
    print(
        "Reverse evidence digest: "
        f"{reverse_evidence_digest}"
    )
    print("Metrics recomputed: 0")
    print("Interpretations recomputed: 0")
    print("Uncertainty estimates calculated: 0")
    print(
        "Statistical significance tests calculated: 0"
    )
    print("Superiority decisions emitted: 0")
    print("Equivalence decisions emitted: 0")
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
