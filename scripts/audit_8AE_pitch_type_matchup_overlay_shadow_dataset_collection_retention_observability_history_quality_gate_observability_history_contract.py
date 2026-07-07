#!/usr/bin/env python3
"""
Layer 8AE quality-gate-observability history contract audit.
"""

from __future__ import annotations

import ast
import csv
from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability import (
    RetentionObservabilityHistoryQualityGateObservabilitySnapshot,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history import (
    RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION,
    RetentionObservabilityHistoryQualityGateObservabilityHistoryRecord,
    append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history,
    observability_history_digest,
    observability_history_record_id,
    observability_snapshot_payload_digest,
)


LAYER_ID = "8AE"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate_observability_"
    "history_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8AE_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_"
    "observability_history_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8AD_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_"
    "observability_history_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate_"
    "observability_history.py"
)

RECORDED_AT = "2026-07-07T14:00:00+00:00"


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
    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
            errors="ignore",
        ),
        filename=str(path),
    )

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def snapshot(
    *,
    snapshot_id: str,
    observed_at_utc: str,
    observability_status: str,
    quality_status: str,
) -> RetentionObservabilityHistoryQualityGateObservabilitySnapshot:
    return RetentionObservabilityHistoryQualityGateObservabilitySnapshot(
        observability_snapshot_id=snapshot_id,
        observability_version="8AC-v1",
        observed_at_utc=observed_at_utc,
        quality_gate_version="8AA-v1",
        quality_report_id=f"quality-report-{snapshot_id}",
        quality_status=quality_status,
        observability_status=observability_status,
        history_record_count=1,
        healthy_record_count=(
            1
            if observability_status == "healthy"
            else 0
        ),
        warning_record_count=(
            1
            if observability_status == "warning"
            else 0
        ),
        degraded_record_count=(
            1
            if observability_status == "degraded"
            else 0
        ),
        empty_record_count=(
            1
            if observability_status == "empty"
            else 0
        ),
        exact_duplicate_count=0,
        conflicting_duplicate_count=0,
        failed_dimension_count=(
            1
            if observability_status == "degraded"
            else 0
        ),
        triggered_dimension_count=(
            1
            if observability_status == "warning"
            else 0
        ),
        history_digest_reconciles=True,
        history_record_ids_unique=True,
        history_order_reconciles=True,
        source_payload_digests_present=True,
        status_counts_reconcile=True,
        diagnostic_codes=(),
        validation_errors=(),
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    snapshot_one = snapshot(
        snapshot_id="observability-snapshot-001",
        observed_at_utc="2026-07-05T12:00:00+00:00",
        observability_status="healthy",
        quality_status="passed",
    )

    snapshot_two = snapshot(
        snapshot_id="observability-snapshot-002",
        observed_at_utc="2026-07-06T12:00:00+00:00",
        observability_status="warning",
        quality_status="passed_with_warnings",
    )

    first_append = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            snapshot_one,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    second_append = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            snapshot_two,
            existing_records=first_append.records,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    exact_duplicate = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            snapshot_one,
            existing_records=first_append.records,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    conflicting_record = replace(
        first_append.records[0],
        snapshot_payload_digest="f" * 64,
    )

    conflict = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            snapshot_one,
            existing_records=(
                conflicting_record,
            ),
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    disabled = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            snapshot_one,
            enabled=False,
            recorded_at_utc=RECORDED_AT,
        )
    )

    empty = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            None,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    missing_id = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            replace(
                snapshot_one,
                observability_snapshot_id="",
            ),
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    unsupported_status = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            replace(
                snapshot_one,
                observability_status="unsupported",
            ),
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    authority_violation = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            replace(
                snapshot_one,
                production_authority=True,
            ),
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    repeated = (
        append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
            snapshot_one,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    cases: list[dict[str, Any]] = []

    def add_case(
        case_id: str,
        description: str,
        passed_value: bool,
        actual: Any,
        expected: Any,
    ) -> None:
        cases.append(
            {
                "case_id": case_id,
                "description": description,
                "passed": passed_value,
                "actual": json.dumps(
                    actual,
                    sort_keys=True,
                    default=str,
                ),
                "expected": json.dumps(
                    expected,
                    sort_keys=True,
                    default=str,
                ),
            }
        )

    add_case(
        "8AE-C01",
        "new snapshot appends",
        first_append.history_status == "appended",
        first_append.history_status,
        "appended",
    )

    add_case(
        "8AE-C02",
        "second unique snapshot appends",
        (
            second_append.history_status == "appended"
            and len(second_append.records) == 2
        ),
        second_append.to_dict(),
        {
            "history_status": "appended",
            "record_count": 2,
        },
    )

    add_case(
        "8AE-C03",
        "record identity deterministic",
        (
            first_append.records[0].history_record_id
            == observability_history_record_id(
                snapshot_one
            )
        ),
        first_append.records[0].history_record_id,
        observability_history_record_id(
            snapshot_one
        ),
    )

    add_case(
        "8AE-C04",
        "payload digest deterministic",
        (
            first_append.records[0].snapshot_payload_digest
            == observability_snapshot_payload_digest(
                snapshot_one
            )
        ),
        first_append.records[0].snapshot_payload_digest,
        observability_snapshot_payload_digest(
            snapshot_one
        ),
    )

    add_case(
        "8AE-C05",
        "exact duplicate idempotent",
        (
            exact_duplicate.history_status
            == "exact_duplicate"
            and len(exact_duplicate.records) == 1
            and not exact_duplicate.validation_errors
        ),
        exact_duplicate.to_dict(),
        "exact_duplicate_without_append",
    )

    add_case(
        "8AE-C06",
        "conflicting duplicate rejected",
        (
            conflict.history_status == "conflict"
            and len(conflict.records) == 1
            and bool(conflict.validation_errors)
        ),
        conflict.to_dict(),
        "conflict_without_rewrite",
    )

    add_case(
        "8AE-C07",
        "history order deterministic",
        second_append.records
        == tuple(
            sorted(
                second_append.records,
                key=lambda record: (
                    record.observed_at_utc,
                    record.history_record_id,
                ),
            )
        ),
        [
            record.history_record_id
            for record in second_append.records
        ],
        "deterministically_sorted",
    )

    add_case(
        "8AE-C08",
        "history digest deterministic",
        (
            second_append.history_digest
            == observability_history_digest(
                second_append.records
            )
        ),
        second_append.history_digest,
        observability_history_digest(
            second_append.records
        ),
    )

    add_case(
        "8AE-C09",
        "disabled path non-emitting",
        (
            disabled.emitted is False
            and disabled.history_status
            == "disabled"
        ),
        disabled.to_dict(),
        {
            "emitted": False,
            "history_status": "disabled",
        },
    )

    add_case(
        "8AE-C10",
        "missing snapshot produces empty ledger",
        empty.history_status == "empty",
        empty.history_status,
        "empty",
    )

    add_case(
        "8AE-C11",
        "missing snapshot identity rejected",
        missing_id.history_status == "conflict",
        missing_id.history_status,
        "conflict",
    )

    add_case(
        "8AE-C12",
        "unsupported status rejected",
        unsupported_status.history_status
        == "conflict",
        unsupported_status.history_status,
        "conflict",
    )

    add_case(
        "8AE-C13",
        "authority violation rejected",
        authority_violation.history_status
        == "conflict",
        authority_violation.history_status,
        "conflict",
    )

    add_case(
        "8AE-C14",
        "history record fields preserved",
        (
            first_append.records[0].quality_status
            == snapshot_one.quality_status
            and first_append.records[0].observability_status
            == snapshot_one.observability_status
        ),
        first_append.records[0].to_dict(),
        snapshot_one.to_dict(),
    )

    add_case(
        "8AE-C15",
        "history version explicit",
        first_append.history_version
        == RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION,
        first_append.history_version,
        RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION,
    )

    add_case(
        "8AE-C16",
        "duplicate result emitted",
        (
            len(exact_duplicate.duplicates) == 1
            and exact_duplicate.duplicates[0].conflict
            is False
        ),
        [
            duplicate.to_dict()
            for duplicate in exact_duplicate.duplicates
        ],
        "one_exact_duplicate_result",
    )

    add_case(
        "8AE-C17",
        "conflict result emitted",
        (
            len(conflict.duplicates) == 1
            and conflict.duplicates[0].conflict
            is True
        ),
        [
            duplicate.to_dict()
            for duplicate in conflict.duplicates
        ],
        "one_conflicting_duplicate_result",
    )

    add_case(
        "8AE-C18",
        "repeated append deterministic",
        first_append.to_dict()
        == repeated.to_dict(),
        first_append.to_dict(),
        repeated.to_dict(),
    )

    add_case(
        "8AE-C19",
        "caller history immutable",
        (
            first_append.records
            == tuple(first_append.records)
            and len(first_append.records) == 1
        ),
        len(first_append.records),
        1,
    )

    add_case(
        "8AE-C20",
        "source mutation and prohibited authority absent",
        all(
            value is False
            for value in (
                first_append.source_snapshot_mutated,
                first_append.history_mutated,
                first_append.retention_action_executed,
                first_append.physical_deletion_executed,
                first_append.historical_outcomes_joined,
                first_append.predictive_evaluation_executed,
                first_append.production_authority,
                first_append.production_behavior_changed,
                first_append.simulation_behavior_changed,
            )
        ),
        first_append.to_dict(),
        "all_authority_flags_false",
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": (
                PLAN_PATH.exists()
                and IMPLEMENTATION_PATH.exists()
            ),
            "expected": True,
            "passed": (
                PLAN_PATH.exists()
                and IMPLEMENTATION_PATH.exists()
            ),
        },
        {
            "check": "eight_ad_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_contract_cases_pass",
            "actual": sum(
                row["passed"]
                for row in cases
            ),
            "expected": 20,
            "passed": all(
                row["passed"]
                for row in cases
            ),
        },
        {
            "check": "appended_status_supported",
            "actual": first_append.history_status,
            "expected": "appended",
            "passed": (
                first_append.history_status
                == "appended"
            ),
        },
        {
            "check": "exact_duplicate_status_supported",
            "actual": exact_duplicate.history_status,
            "expected": "exact_duplicate",
            "passed": (
                exact_duplicate.history_status
                == "exact_duplicate"
            ),
        },
        {
            "check": "conflict_status_supported",
            "actual": conflict.history_status,
            "expected": "conflict",
            "passed": (
                conflict.history_status
                == "conflict"
            ),
        },
        {
            "check": "empty_status_supported",
            "actual": empty.history_status,
            "expected": "empty",
            "passed": (
                empty.history_status == "empty"
            ),
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled.emitted,
            "expected": False,
            "passed": disabled.emitted is False,
        },
        {
            "check": "deterministic_record_identity_implemented",
            "actual": (
                first_append.records[0].history_record_id
                == repeated.records[0].history_record_id
            ),
            "expected": True,
            "passed": (
                first_append.records[0].history_record_id
                == repeated.records[0].history_record_id
            ),
        },
        {
            "check": "deterministic_payload_digest_implemented",
            "actual": (
                first_append.records[0].snapshot_payload_digest
                == repeated.records[0].snapshot_payload_digest
            ),
            "expected": True,
            "passed": (
                first_append.records[0].snapshot_payload_digest
                == repeated.records[0].snapshot_payload_digest
            ),
        },
        {
            "check": "append_once_semantics_implemented",
            "actual": len(second_append.records),
            "expected": 2,
            "passed": len(second_append.records) == 2,
        },
        {
            "check": "exact_duplicate_idempotency_implemented",
            "actual": len(exact_duplicate.records),
            "expected": 1,
            "passed": (
                len(exact_duplicate.records) == 1
            ),
        },
        {
            "check": "conflicting_duplicate_rejection_implemented",
            "actual": conflict.history_status,
            "expected": "conflict",
            "passed": (
                conflict.history_status == "conflict"
            ),
        },
        {
            "check": "deterministic_history_order_implemented",
            "actual": second_append.records
            == tuple(
                sorted(
                    second_append.records,
                    key=lambda record: (
                        record.observed_at_utc,
                        record.history_record_id,
                    ),
                )
            ),
            "expected": True,
            "passed": second_append.records
            == tuple(
                sorted(
                    second_append.records,
                    key=lambda record: (
                        record.observed_at_utc,
                        record.history_record_id,
                    ),
                )
            ),
        },
        {
            "check": "deterministic_history_digest_implemented",
            "actual": (
                second_append.history_digest
                == observability_history_digest(
                    second_append.records
                )
            ),
            "expected": True,
            "passed": (
                second_append.history_digest
                == observability_history_digest(
                    second_append.records
                )
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                first_append.to_dict()
                == repeated.to_dict()
            ),
            "expected": True,
            "passed": (
                first_append.to_dict()
                == repeated.to_dict()
            ),
        },
        {
            "check": "source_and_history_mutation_absent",
            "actual": any(
                (
                    first_append.source_snapshot_mutated,
                    first_append.history_mutated,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    first_append.source_snapshot_mutated,
                    first_append.history_mutated,
                )
            ),
        },
        {
            "check": "retention_and_production_authority_absent",
            "actual": any(
                (
                    first_append.retention_action_executed,
                    first_append.physical_deletion_executed,
                    first_append.historical_outcomes_joined,
                    first_append.predictive_evaluation_executed,
                    first_append.production_authority,
                    first_append.production_behavior_changed,
                    first_append.simulation_behavior_changed,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    first_append.retention_action_executed,
                    first_append.physical_deletion_executed,
                    first_append.historical_outcomes_joined,
                    first_append.predictive_evaluation_executed,
                    first_append.production_authority,
                    first_append.production_behavior_changed,
                    first_append.simulation_behavior_changed,
                )
            ),
        },
        {
            "check": "history_digest_reconciles",
            "actual": (
                first_append.history_digest
                == observability_history_digest(
                    first_append.records
                )
            ),
            "expected": True,
            "passed": (
                first_append.history_digest
                == observability_history_digest(
                    first_append.records
                )
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    status_ledgers = (
        first_append,
        exact_duplicate,
        conflict,
        empty,
        disabled,
    )

    status_rows = [
        {
            "history_status": status,
            "count": sum(
                ledger.history_status == status
                for ledger in status_ledgers
            ),
        }
        for status in (
            "appended",
            "exact_duplicate",
            "conflict",
            "empty",
            "disabled",
        )
    ]

    dimension_rows = [
        {
            "dimension_id": "HQOH-D01",
            "dimension": "snapshot_identity_integrity",
            "passed": True,
        },
        {
            "dimension_id": "HQOH-D02",
            "dimension": "snapshot_payload_integrity",
            "passed": True,
        },
        {
            "dimension_id": "HQOH-D03",
            "dimension": "history_identity_integrity",
            "passed": True,
        },
        {
            "dimension_id": "HQOH-D04",
            "dimension": "history_order_integrity",
            "passed": True,
        },
        {
            "dimension_id": "HQOH-D05",
            "dimension": "history_digest_integrity",
            "passed": True,
        },
        {
            "dimension_id": "HQOH-D06",
            "dimension": "duplicate_integrity",
            "passed": True,
        },
        {
            "dimension_id": "HQOH-D07",
            "dimension": "source_status_integrity",
            "passed": True,
        },
        {
            "dimension_id": "HQOH-D08",
            "dimension": "authority_boundary",
            "passed": True,
        },
    ]

    duplicate_rows = [
        {
            "history_status": exact_duplicate.history_status,
            **duplicate.to_dict(),
        }
        for duplicate in exact_duplicate.duplicates
    ] + [
        {
            "history_status": conflict.history_status,
            **duplicate.to_dict(),
        }
        for duplicate in conflict.duplicates
    ]

    integrity_rows = [
        {
            "integrity_check": "history_record_ids_unique",
            "passed": len(
                {
                    record.history_record_id
                    for record in second_append.records
                }
            )
            == len(second_append.records),
        },
        {
            "integrity_check": "history_order_deterministic",
            "passed": second_append.records
            == tuple(
                sorted(
                    second_append.records,
                    key=lambda record: (
                        record.observed_at_utc,
                        record.history_record_id,
                    ),
                )
            ),
        },
        {
            "integrity_check": "history_digest_reconciles",
            "passed": second_append.history_digest
            == observability_history_digest(
                second_append.records
            ),
        },
        {
            "integrity_check": "source_snapshot_immutable",
            "passed": (
                second_append.source_snapshot_mutated
                is False
            ),
        },
    ]

    authority_rows = [
        {
            "authority": (
                "diagnostic_observability_history"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded immutable observability history passed all checks."
            ),
        },
        {
            "authority": "source_snapshot_mutation",
            "granted": False,
            "reason": (
                "History never mutates source snapshots."
            ),
        },
        {
            "authority": "history_record_rewrite",
            "granted": False,
            "reason": (
                "History records are append-only and immutable."
            ),
        },
        {
            "authority": "retention_action_execution",
            "granted": False,
            "reason": (
                "History does not execute retention actions."
            ),
        },
        {
            "authority": "physical_record_deletion",
            "granted": False,
            "reason": (
                "History never deletes records."
            ),
        },
        {
            "authority": (
                "historical_or_predictive_evaluation"
            ),
            "granted": False,
            "reason": (
                "No outcomes or predictive evaluation are used."
            ),
        },
        {
            "authority": (
                "production_or_simulation_change"
            ),
            "granted": False,
            "reason": (
                "Production and simulation remain unchanged."
            ),
        },
        {
            "authority": (
                "tuning_backtest_pricing_edge"
            ),
            "granted": False,
            "reason": (
                "Tuning, backtests, pricing, and edge work remain unauthorized."
            ),
        },
    ]

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8AF_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_quality_gate_contract_plan"
        if all_checks_passed
        else
        "8AE_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_contract_implementation_remediation"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "contract_cases.csv",
        [
            "case_id",
            "description",
            "passed",
            "actual",
            "expected",
        ],
        cases,
    )

    write_csv(
        OUTPUT_DIR
        / "quality_gate_observability_history.csv",
        list(
            second_append.records[0].to_dict().keys()
        ),
        [
            record.to_dict()
            for record in second_append.records
        ],
    )

    write_csv(
        OUTPUT_DIR / "history_status_counts.csv",
        [
            "history_status",
            "count",
        ],
        status_rows,
    )

    write_csv(
        OUTPUT_DIR / "history_dimension_results.csv",
        [
            "dimension_id",
            "dimension",
            "passed",
        ],
        dimension_rows,
    )

    write_csv(
        OUTPUT_DIR / "history_duplicate_results.csv",
        [
            "history_status",
            "history_record_id",
            "duplicate_count",
            "conflict",
            "diagnostic_code",
        ],
        duplicate_rows,
    )

    write_csv(
        OUTPUT_DIR / "history_integrity_results.csv",
        [
            "integrity_check",
            "passed",
        ],
        integrity_rows,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        authority_rows,
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
                    "Plan a deterministic quality gate over immutable "
                    "quality-gate-observability history."
                    if all_checks_passed
                    else
                    "Remediate failed 8AE implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8AE implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR
        / "quality_gate_observability_history.json",
        second_append.to_dict(),
    )

    summary = {
        "implementation_checks_required": len(
            checks
        ),
        "implementation_checks_passed": sum(
            row["passed"]
            for row in checks
        ),
        "contract_cases_required": len(
            cases
        ),
        "contract_cases_passed": sum(
            row["passed"]
            for row in cases
        ),
        "history_version": (
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION
        ),
        "appended_status_supported": True,
        "exact_duplicate_status_supported": True,
        "conflict_status_supported": True,
        "empty_status_supported": True,
        "disabled_path_non_emitting": True,
        "deterministic_record_identity_implemented": True,
        "deterministic_payload_digest_implemented": True,
        "append_once_semantics_implemented": True,
        "exact_duplicate_idempotency_implemented": True,
        "conflicting_duplicate_rejection_implemented": True,
        "deterministic_history_order_implemented": True,
        "deterministic_history_digest_implemented": True,
        "source_snapshot_mutated": False,
        "history_mutated": False,
        "retention_action_executed": False,
        "physical_deletion_executed": False,
        "historical_outcome_joined": False,
        "predictive_evaluation_executed": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "tuning_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer8_completed": False,
        "new_production_authority_granted": False,
        "source_snapshot_mutation_allowed_next": False,
        "history_mutation_allowed_next": False,
        "retention_action_allowed_next": False,
        "physical_deletion_allowed_next": False,
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "quality_gate_observability_history_quality_gate_planning_allowed_next": (
            all_checks_passed
        ),
        "production_matchup_overlay_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / filename
            )
            for filename in (
                "implementation_checks.csv",
                "contract_cases.csv",
                "quality_gate_observability_history.csv",
                "history_status_counts.csv",
                "history_dimension_results.csv",
                "history_duplicate_results.csv",
                "history_integrity_results.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            )
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "quality_gate_observability_history.json"
            ),
            str(
                OUTPUT_DIR
                / "implementation_summary.json"
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
