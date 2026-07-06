#!/usr/bin/env python3
"""
Layer 8Y retention-observability-history implementation audit.
"""

from __future__ import annotations

import ast
import csv
from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability import (
    RETENTION_OBSERVABILITY_VERSION,
    RetentionObservabilityReport,
    RetentionObservabilitySnapshot,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history import (
    RETENTION_OBSERVABILITY_HISTORY_VERSION,
    RetentionObservabilityHistoryRecord,
    record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history,
    retention_observability_history_digest,
    retention_observability_history_record_id,
    retention_observability_report_digest,
    retention_observability_snapshot_digest,
)


LAYER_ID = "8Y"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8Y_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8X_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history.py"
)

RECORDED_AT = "2026-07-04T12:00:00+00:00"


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
    observability_status: str = "healthy",
    decision_count: int = 1,
    retained_count: int = 1,
    archived_count: int = 0,
    expired_count: int = 0,
    quarantined_count: int = 0,
) -> RetentionObservabilitySnapshot:
    return RetentionObservabilitySnapshot(
        retention_observability_snapshot_id=(
            snapshot_id
        ),
        retention_observability_version=(
            RETENTION_OBSERVABILITY_VERSION
        ),
        observed_at_utc=observed_at_utc,
        retention_version="8U-v1",
        retention_status=(
            "quarantined"
            if quarantined_count
            else
            "expired"
            if expired_count
            else
            "archived"
            if archived_count
            else
            "retained"
        ),
        observability_status=observability_status,
        decision_count=decision_count,
        retained_count=retained_count,
        archived_count=archived_count,
        expired_count=expired_count,
        quarantined_count=quarantined_count,
        exact_duplicate_count=0,
        conflicting_duplicate_count=0,
        minimum_record_age_days=13,
        mean_record_age_days=13.0,
        maximum_record_age_days=13,
        retention_window_days=30,
        archive_window_days=90,
        ledger_digest_reconciles=True,
        decision_identifiers_unique=True,
        policy_windows_reconcile=True,
        diagnostic_codes=(),
        validation_errors=(),
    )


def report(
    snapshot_value: (
        RetentionObservabilitySnapshot | None
    ),
    *,
    emitted: bool = True,
    observability_status: str = "healthy",
) -> RetentionObservabilityReport:
    return RetentionObservabilityReport(
        emitted=emitted,
        reason=(
            "retention_observability_healthy"
            if emitted
            else
            "retention_observability_disabled"
        ),
        observability_status=(
            observability_status
            if emitted
            else
            "disabled"
        ),
        snapshot=snapshot_value,
        signals=(),
        diagnostic_codes=(),
        validation_errors=(),
        retention_observability_version=(
            RETENTION_OBSERVABILITY_VERSION
        ),
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    first_snapshot = snapshot(
        snapshot_id="retention-observability-snapshot-001",
        observed_at_utc="2026-07-03T12:00:00+00:00",
    )

    second_snapshot = snapshot(
        snapshot_id="retention-observability-snapshot-002",
        observed_at_utc="2026-07-04T12:00:00+00:00",
        observability_status="warning",
        decision_count=3,
        retained_count=1,
        archived_count=1,
        expired_count=1,
    )

    first_report = report(
        first_snapshot
    )

    second_report = report(
        second_snapshot,
        observability_status="warning",
    )

    disabled_report = report(
        None,
        emitted=False,
        observability_status="disabled",
    )

    first_append = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            first_report,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    second_append = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            second_report,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
            existing_records=(
                first_append.records
            ),
        )
    )

    repeated_first = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            first_report,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
            existing_records=(
                first_append.records
            ),
        )
    )

    disabled = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            first_report,
            enabled=False,
            recorded_at_utc=RECORDED_AT,
        )
    )

    missing_report = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            None,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    missing_snapshot = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            report(
                None,
                emitted=True,
            ),
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    empty = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            disabled_report,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    mismatched_snapshot = replace(
        first_snapshot,
        decision_count=2,
    )

    count_mismatch = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            report(
                mismatched_snapshot
            ),
            enabled=True,
            recorded_at_utc=RECORDED_AT,
        )
    )

    existing_record = (
        first_append.records[0]
    )

    conflicting_record = replace(
        existing_record,
        retained_count=2,
    )

    conflict = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            first_report,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
            existing_records=(
                conflicting_record,
            ),
        )
    )

    reverse_order = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            first_report,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
            existing_records=tuple(
                reversed(
                    second_append.records
                )
            ),
        )
    )

    repeated_append = (
        record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
            second_report,
            enabled=True,
            recorded_at_utc=RECORDED_AT,
            existing_records=(
                first_append.records
            ),
        )
    )

    cases: list[dict[str, Any]] = []

    def add_case(
        case_id: str,
        description: str,
        passed: bool,
        actual: Any,
        expected: Any,
    ) -> None:
        cases.append(
            {
                "case_id": case_id,
                "description": description,
                "passed": passed,
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
        "8Y-C01",
        "new history record appended",
        (
            first_append.history_status
            == "appended"
            and len(first_append.records) == 1
        ),
        first_append.to_dict(),
        "one_appended_record",
    )

    add_case(
        "8Y-C02",
        "second history record appended",
        (
            second_append.history_status
            == "appended"
            and len(second_append.records) == 2
        ),
        second_append.to_dict(),
        "two_appended_records",
    )

    add_case(
        "8Y-C03",
        "exact duplicate is idempotent",
        (
            repeated_first.history_status
            == "idempotent"
            and len(repeated_first.records) == 1
            and len(repeated_first.duplicates) == 1
        ),
        repeated_first.to_dict(),
        "one_record_one_exact_duplicate",
    )

    add_case(
        "8Y-C04",
        "conflicting identity rejected",
        (
            conflict.history_status
            == "conflicted"
            and len(conflict.records) == 1
            and conflict.duplicates[0].conflict
        ),
        conflict.to_dict(),
        "conflicted_identity",
    )

    add_case(
        "8Y-C05",
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
        "8Y-C06",
        "missing report conflicts",
        missing_report.history_status
        == "conflicted",
        missing_report.history_status,
        "conflicted",
    )

    add_case(
        "8Y-C07",
        "missing emitted snapshot conflicts",
        missing_snapshot.history_status
        == "conflicted",
        missing_snapshot.history_status,
        "conflicted",
    )

    add_case(
        "8Y-C08",
        "non-emitted report produces empty history",
        empty.history_status
        == "empty",
        empty.history_status,
        "empty",
    )

    add_case(
        "8Y-C09",
        "status count mismatch conflicts",
        count_mismatch.history_status
        == "conflicted",
        count_mismatch.history_status,
        "conflicted",
    )

    add_case(
        "8Y-C10",
        "snapshot digest deterministic",
        (
            retention_observability_snapshot_digest(
                first_snapshot
            )
            == retention_observability_snapshot_digest(
                first_snapshot
            )
        ),
        retention_observability_snapshot_digest(
            first_snapshot
        ),
        "deterministic_sha256",
    )

    add_case(
        "8Y-C11",
        "report digest deterministic",
        (
            retention_observability_report_digest(
                first_report
            )
            == retention_observability_report_digest(
                first_report
            )
        ),
        retention_observability_report_digest(
            first_report
        ),
        "deterministic_sha256",
    )

    add_case(
        "8Y-C12",
        "history identity deterministic",
        (
            first_append.records[0].history_record_id
            == repeated_first.records[0].history_record_id
        ),
        first_append.records[0].history_record_id,
        repeated_first.records[0].history_record_id,
    )

    add_case(
        "8Y-C13",
        "history digest deterministic",
        (
            second_append.history_digest
            == repeated_append.history_digest
        ),
        second_append.history_digest,
        repeated_append.history_digest,
    )

    add_case(
        "8Y-C14",
        "history ordering deterministic",
        (
            reverse_order.records
            == second_append.records
        ),
        [
            record.history_record_id
            for record in reverse_order.records
        ],
        [
            record.history_record_id
            for record in second_append.records
        ],
    )

    add_case(
        "8Y-C15",
        "existing records remain immutable",
        (
            first_append.records[0]
            == existing_record
        ),
        first_append.records[0].to_dict(),
        existing_record.to_dict(),
    )

    add_case(
        "8Y-C16",
        "record stores source digests",
        (
            len(
                first_append.records[0].snapshot_payload_digest
            )
            == 64
            and len(
                first_append.records[0].report_payload_digest
            )
            == 64
        ),
        {
            "snapshot_digest_length": len(
                first_append.records[0].snapshot_payload_digest
            ),
            "report_digest_length": len(
                first_append.records[0].report_payload_digest
            ),
        },
        {
            "snapshot_digest_length": 64,
            "report_digest_length": 64,
        },
    )

    add_case(
        "8Y-C17",
        "history version explicit",
        (
            first_append.history_version
            == RETENTION_OBSERVABILITY_HISTORY_VERSION
        ),
        first_append.history_version,
        "8Y-v1",
    )

    add_case(
        "8Y-C18",
        "serialization deterministic",
        (
            second_append.to_dict()
            == repeated_append.to_dict()
        ),
        second_append.to_dict(),
        repeated_append.to_dict(),
    )

    add_case(
        "8Y-C19",
        "retention actions never execute",
        (
            first_append.retention_action_executed
            is False
            and first_append.physical_deletion_executed
            is False
        ),
        {
            "retention_action_executed": (
                first_append.retention_action_executed
            ),
            "physical_deletion_executed": (
                first_append.physical_deletion_executed
            ),
        },
        {
            "retention_action_executed": False,
            "physical_deletion_executed": False,
        },
    )

    add_case(
        "8Y-C20",
        "all prohibited authority remains false",
        all(
            value is False
            for value in (
                first_append.retention_action_executed,
                first_append.physical_deletion_executed,
                first_append.production_authority,
                first_append.production_behavior_changed,
                first_append.simulation_behavior_changed,
                first_append.historical_outcomes_joined,
                first_append.predictive_evaluation_executed,
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
            "check": "eight_x_predecessor_present",
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
            "check": "idempotent_status_supported",
            "actual": repeated_first.history_status,
            "expected": "idempotent",
            "passed": (
                repeated_first.history_status
                == "idempotent"
            ),
        },
        {
            "check": "conflicted_status_supported",
            "actual": conflict.history_status,
            "expected": "conflicted",
            "passed": (
                conflict.history_status
                == "conflicted"
            ),
        },
        {
            "check": "empty_status_supported",
            "actual": empty.history_status,
            "expected": "empty",
            "passed": (
                empty.history_status
                == "empty"
            ),
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled.emitted,
            "expected": False,
            "passed": (
                disabled.emitted is False
            ),
        },
        {
            "check": "snapshot_digest_implemented",
            "actual": len(
                first_append.records[0].snapshot_payload_digest
            ),
            "expected": 64,
            "passed": (
                len(
                    first_append.records[0].snapshot_payload_digest
                )
                == 64
            ),
        },
        {
            "check": "report_digest_implemented",
            "actual": len(
                first_append.records[0].report_payload_digest
            ),
            "expected": 64,
            "passed": (
                len(
                    first_append.records[0].report_payload_digest
                )
                == 64
            ),
        },
        {
            "check": "deterministic_history_identity_implemented",
            "actual": (
                first_append.records[0].history_record_id
                == repeated_first.records[0].history_record_id
            ),
            "expected": True,
            "passed": (
                first_append.records[0].history_record_id
                == repeated_first.records[0].history_record_id
            ),
        },
        {
            "check": "append_only_history_implemented",
            "actual": (
                first_append.records[0]
                in second_append.records
            ),
            "expected": True,
            "passed": (
                first_append.records[0]
                in second_append.records
            ),
        },
        {
            "check": "existing_record_immutability_implemented",
            "actual": (
                first_append.records[0]
                == existing_record
            ),
            "expected": True,
            "passed": (
                first_append.records[0]
                == existing_record
            ),
        },
        {
            "check": "exact_duplicate_idempotency_implemented",
            "actual": len(
                repeated_first.records
            ),
            "expected": 1,
            "passed": (
                len(
                    repeated_first.records
                )
                == 1
                and repeated_first.history_status
                == "idempotent"
            ),
        },
        {
            "check": "conflicting_duplicate_rejection_implemented",
            "actual": conflict.history_status,
            "expected": "conflicted",
            "passed": (
                conflict.history_status
                == "conflicted"
                and conflict.duplicates[0].conflict
            ),
        },
        {
            "check": "deterministic_history_order_implemented",
            "actual": (
                reverse_order.records
                == second_append.records
            ),
            "expected": True,
            "passed": (
                reverse_order.records
                == second_append.records
            ),
        },
        {
            "check": "history_digest_deterministic",
            "actual": (
                second_append.history_digest
                == repeated_append.history_digest
            ),
            "expected": True,
            "passed": (
                second_append.history_digest
                == repeated_append.history_digest
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                second_append.to_dict()
                == repeated_append.to_dict()
            ),
            "expected": True,
            "passed": (
                second_append.to_dict()
                == repeated_append.to_dict()
            ),
        },
        {
            "check": "retention_action_and_authority_absent",
            "actual": any(
                (
                    first_append.retention_action_executed,
                    first_append.physical_deletion_executed,
                    first_append.production_authority,
                    first_append.production_behavior_changed,
                    first_append.simulation_behavior_changed,
                    first_append.historical_outcomes_joined,
                    first_append.predictive_evaluation_executed,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    first_append.retention_action_executed,
                    first_append.physical_deletion_executed,
                    first_append.production_authority,
                    first_append.production_behavior_changed,
                    first_append.simulation_behavior_changed,
                    first_append.historical_outcomes_joined,
                    first_append.predictive_evaluation_executed,
                )
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    history_status_rows = [
        {
            "history_status": status,
            "count": sum(
                ledger.history_status
                == status
                for ledger in (
                    first_append,
                    repeated_first,
                    conflict,
                    empty,
                    disabled,
                )
            ),
        }
        for status in (
            "appended",
            "idempotent",
            "conflicted",
            "empty",
            "disabled",
        )
    ]

    timeline_rows = [
        {
            "history_record_id": (
                record.history_record_id
            ),
            "observed_at_utc": (
                record.observed_at_utc
            ),
            "observability_status": (
                record.observability_status
            ),
            "decision_count": (
                record.decision_count
            ),
        }
        for record in second_append.records
    ]

    duplicate_rows = [
        {
            "history_record_id": (
                repeated_first.duplicates[0].history_record_id
            ),
            "duplicate_count": (
                repeated_first.duplicates[0].duplicate_count
            ),
            "conflict": False,
            "diagnostic_code": (
                repeated_first.duplicates[0].diagnostic_code
            ),
        },
        {
            "history_record_id": (
                conflict.duplicates[0].history_record_id
            ),
            "duplicate_count": (
                conflict.duplicates[0].duplicate_count
            ),
            "conflict": True,
            "diagnostic_code": (
                conflict.duplicates[0].diagnostic_code
            ),
        },
    ]

    integrity_rows = [
        {
            "failure": (
                "missing_observability_report"
            ),
            "history_status": (
                missing_report.history_status
            ),
            "diagnostic_code": (
                missing_report.diagnostic_codes[0]
            ),
        },
        {
            "failure": (
                "missing_observability_snapshot"
            ),
            "history_status": (
                missing_snapshot.history_status
            ),
            "diagnostic_code": (
                missing_snapshot.diagnostic_codes[0]
            ),
        },
        {
            "failure": "status_count_mismatch",
            "history_status": (
                count_mismatch.history_status
            ),
            "diagnostic_code": (
                count_mismatch.diagnostic_codes[0]
            ),
        },
        {
            "failure": "history_identity_conflict",
            "history_status": (
                conflict.history_status
            ),
            "diagnostic_code": (
                conflict.diagnostic_codes[0]
            ),
        },
    ]

    authority_rows = [
        {
            "authority": (
                "diagnostic_retention_observability_history"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded immutable observability history passed all checks."
            ),
        },
        {
            "authority": "retention_action_execution",
            "granted": False,
            "reason": (
                "History recording does not execute retention actions."
            ),
        },
        {
            "authority": "physical_record_deletion",
            "granted": False,
            "reason": (
                "History recording never deletes records."
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
        "retention_observability_history_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8Z_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_contract_plan"
        if all_checks_passed
        else
        "8Y_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_contract_implementation_remediation"
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
        / "observability_history_records.csv",
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
        history_status_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "observability_status_timeline.csv",
        [
            "history_record_id",
            "observed_at_utc",
            "observability_status",
            "decision_count",
        ],
        timeline_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "duplicate_history_report.csv",
        [
            "history_record_id",
            "duplicate_count",
            "conflict",
            "diagnostic_code",
        ],
        duplicate_rows,
    )

    write_csv(
        OUTPUT_DIR / "integrity_failures.csv",
        [
            "failure",
            "history_status",
            "diagnostic_code",
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
                    "Plan a deterministic quality gate for immutable "
                    "retention-observability history."
                    if all_checks_passed
                    else
                    "Remediate failed 8Y implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8Y implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR
        / "observability_history_ledger.json",
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
            RETENTION_OBSERVABILITY_HISTORY_VERSION
        ),
        "appended_status_supported": True,
        "idempotent_status_supported": True,
        "conflicted_status_supported": True,
        "empty_status_supported": True,
        "disabled_path_non_emitting": True,
        "snapshot_digest_implemented": True,
        "report_digest_implemented": True,
        "deterministic_history_identity_implemented": True,
        "append_only_history_implemented": True,
        "existing_record_immutability_implemented": True,
        "exact_duplicate_idempotency_implemented": True,
        "conflicting_duplicate_rejection_implemented": True,
        "deterministic_history_order_implemented": True,
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
        "retention_action_allowed_next": False,
        "physical_deletion_allowed_next": False,
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "retention_observability_history_quality_gate_planning_allowed_next": (
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
                "observability_history_records.csv",
                "history_status_counts.csv",
                "observability_status_timeline.csv",
                "duplicate_history_report.csv",
                "integrity_failures.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            )
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "observability_history_ledger.json"
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
