#!/usr/bin/env python3
"""
Layer 8AA retention-observability-history quality-gate audit.
"""

from __future__ import annotations

import ast
import csv
from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history import (
    RetentionObservabilityHistoryDuplicate,
    RetentionObservabilityHistoryLedger,
    RetentionObservabilityHistoryRecord,
    retention_observability_history_digest,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate import (
    RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION,
    evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality,
)


LAYER_ID = "8AA"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8AA_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8Z_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate.py"
)

EVALUATED_AT = "2026-07-06T12:00:00+00:00"


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


def history_record(
    *,
    record_id: str,
    observed_at_utc: str,
    observability_status: str,
) -> RetentionObservabilityHistoryRecord:
    return RetentionObservabilityHistoryRecord(
        history_record_id=record_id,
        history_version="8Y-v1",
        recorded_at_utc="2026-07-06T10:00:00+00:00",
        retention_observability_snapshot_id=(
            f"snapshot-{record_id}"
        ),
        retention_observability_version="8W-v1",
        observed_at_utc=observed_at_utc,
        retention_version="8U-v1",
        retention_status=(
            "quarantined"
            if observability_status == "degraded"
            else
            "expired"
            if observability_status == "warning"
            else
            "retained"
        ),
        observability_status=observability_status,
        decision_count=1,
        retained_count=(
            1
            if observability_status == "healthy"
            else 0
        ),
        archived_count=0,
        expired_count=(
            1
            if observability_status == "warning"
            else 0
        ),
        quarantined_count=(
            1
            if observability_status == "degraded"
            else 0
        ),
        exact_duplicate_count=0,
        conflicting_duplicate_count=0,
        ledger_digest_reconciles=True,
        decision_identifiers_unique=True,
        policy_windows_reconcile=True,
        snapshot_payload_digest="a" * 64,
        report_payload_digest="b" * 64,
        diagnostic_codes=(),
        validation_errors=(),
    )


def history_ledger(
    records: tuple[
        RetentionObservabilityHistoryRecord,
        ...,
    ],
    *,
    duplicates: tuple[
        RetentionObservabilityHistoryDuplicate,
        ...,
    ] = (),
) -> RetentionObservabilityHistoryLedger:
    return RetentionObservabilityHistoryLedger(
        emitted=True,
        reason="retention_observability_history_appended",
        history_status=(
            "empty"
            if not records
            else "appended"
        ),
        records=records,
        duplicates=duplicates,
        history_digest=(
            retention_observability_history_digest(
                records
            )
        ),
        recorded_at_utc="2026-07-06T10:00:00+00:00",
        diagnostic_codes=(),
        validation_errors=(),
        history_version="8Y-v1",
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    healthy_record = history_record(
        record_id="history-record-001",
        observed_at_utc="2026-07-03T12:00:00+00:00",
        observability_status="healthy",
    )

    warning_record = history_record(
        record_id="history-record-002",
        observed_at_utc="2026-07-04T12:00:00+00:00",
        observability_status="warning",
    )

    degraded_record = history_record(
        record_id="history-record-003",
        observed_at_utc="2026-07-05T12:00:00+00:00",
        observability_status="degraded",
    )

    passed_ledger = history_ledger(
        (healthy_record,)
    )

    warning_ledger = history_ledger(
        (
            healthy_record,
            warning_record,
        ),
        duplicates=(
            RetentionObservabilityHistoryDuplicate(
                history_record_id=(
                    healthy_record.history_record_id
                ),
                duplicate_count=1,
                conflict=False,
                diagnostic_code=(
                    "matchup_shadow_retention_observability_history_exact_duplicate"
                ),
            ),
        ),
    )

    degraded_ledger = history_ledger(
        (
            healthy_record,
            degraded_record,
        )
    )

    empty_ledger = history_ledger(
        ()
    )

    conflict_ledger = history_ledger(
        (healthy_record,),
        duplicates=(
            RetentionObservabilityHistoryDuplicate(
                history_record_id=(
                    healthy_record.history_record_id
                ),
                duplicate_count=1,
                conflict=True,
                diagnostic_code=(
                    "matchup_shadow_retention_observability_history_identity_conflict"
                ),
            ),
        ),
    )

    bad_digest_ledger = replace(
        passed_ledger,
        history_digest="0" * 64,
    )

    duplicate_identity_ledger = history_ledger(
        (
            healthy_record,
            healthy_record,
        )
    )

    reversed_order_ledger = history_ledger(
        (
            warning_record,
            healthy_record,
        )
    )

    missing_source_digest_record = replace(
        healthy_record,
        snapshot_payload_digest="",
    )

    missing_source_digest_ledger = history_ledger(
        (missing_source_digest_record,)
    )

    authority_ledger = replace(
        passed_ledger,
        production_authority=True,
    )

    passed = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            passed_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    warning = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            warning_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    degraded = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            degraded_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    empty = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            empty_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    disabled = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            passed_ledger,
            enabled=False,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    missing_ledger = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            None,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    conflict = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            conflict_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    bad_digest = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            bad_digest_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    duplicate_identity = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            duplicate_identity_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    reversed_order = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            reversed_order_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    missing_source_digest = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            missing_source_digest_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    authority = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            authority_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    repeated = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
            passed_ledger,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
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
        "8AA-C01",
        "passed status emitted",
        passed.quality_status == "passed",
        passed.quality_status,
        "passed",
    )

    add_case(
        "8AA-C02",
        "warning status emitted",
        warning.quality_status
        == "passed_with_warnings",
        warning.quality_status,
        "passed_with_warnings",
    )

    add_case(
        "8AA-C03",
        "degraded record forces failure",
        degraded.quality_status == "failed",
        degraded.quality_status,
        "failed",
    )

    add_case(
        "8AA-C04",
        "empty status emitted",
        empty.quality_status == "empty",
        empty.quality_status,
        "empty",
    )

    add_case(
        "8AA-C05",
        "disabled path non-emitting",
        (
            disabled.emitted is False
            and disabled.quality_status
            == "disabled"
        ),
        disabled.to_dict(),
        {
            "emitted": False,
            "quality_status": "disabled",
        },
    )

    add_case(
        "8AA-C06",
        "missing ledger fails",
        missing_ledger.quality_status
        == "failed",
        missing_ledger.quality_status,
        "failed",
    )

    add_case(
        "8AA-C07",
        "history digest reconciles",
        passed.history_digest_reconciles,
        passed.history_digest_reconciles,
        True,
    )

    add_case(
        "8AA-C08",
        "history digest mismatch fails",
        bad_digest.quality_status
        == "failed",
        bad_digest.quality_status,
        "failed",
    )

    add_case(
        "8AA-C09",
        "history identities unique",
        passed.history_record_ids_unique,
        passed.history_record_ids_unique,
        True,
    )

    add_case(
        "8AA-C10",
        "duplicate history identities fail",
        duplicate_identity.quality_status
        == "failed",
        duplicate_identity.quality_status,
        "failed",
    )

    add_case(
        "8AA-C11",
        "history order reconciles",
        passed.history_order_reconciles,
        passed.history_order_reconciles,
        True,
    )

    add_case(
        "8AA-C12",
        "history order mismatch fails",
        reversed_order.quality_status
        == "failed",
        reversed_order.quality_status,
        "failed",
    )

    add_case(
        "8AA-C13",
        "source payload digests validate",
        passed.source_payload_digests_present,
        passed.source_payload_digests_present,
        True,
    )

    add_case(
        "8AA-C14",
        "missing source digest fails",
        missing_source_digest.quality_status
        == "failed",
        missing_source_digest.quality_status,
        "failed",
    )

    add_case(
        "8AA-C15",
        "observability status counts reconcile",
        (
            warning.status_counts_reconcile
            and warning.healthy_record_count == 1
            and warning.warning_record_count == 1
        ),
        warning.to_dict(),
        {
            "healthy": 1,
            "warning": 1,
            "reconciles": True,
        },
    )

    add_case(
        "8AA-C16",
        "conflicting duplicate forces failure",
        conflict.quality_status == "failed",
        conflict.quality_status,
        "failed",
    )

    add_case(
        "8AA-C17",
        "authority violation forces failure",
        authority.quality_status == "failed",
        authority.quality_status,
        "failed",
    )

    add_case(
        "8AA-C18",
        "quality report identity deterministic",
        (
            passed.quality_report_id
            == repeated.quality_report_id
        ),
        passed.quality_report_id,
        repeated.quality_report_id,
    )

    add_case(
        "8AA-C19",
        "serialization deterministic",
        passed.to_dict()
        == repeated.to_dict(),
        passed.to_dict(),
        repeated.to_dict(),
    )

    add_case(
        "8AA-C20",
        "history mutation and prohibited authority absent",
        all(
            value is False
            for value in (
                passed.history_mutated,
                passed.retention_action_executed,
                passed.physical_deletion_executed,
                passed.production_authority,
                passed.production_behavior_changed,
                passed.simulation_behavior_changed,
                passed.historical_outcomes_joined,
                passed.predictive_evaluation_executed,
            )
        ),
        passed.to_dict(),
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
            "check": "eight_z_predecessor_present",
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
            "check": "passed_status_supported",
            "actual": passed.quality_status,
            "expected": "passed",
            "passed": (
                passed.quality_status == "passed"
            ),
        },
        {
            "check": "passed_with_warnings_status_supported",
            "actual": warning.quality_status,
            "expected": "passed_with_warnings",
            "passed": (
                warning.quality_status
                == "passed_with_warnings"
            ),
        },
        {
            "check": "failed_status_supported",
            "actual": degraded.quality_status,
            "expected": "failed",
            "passed": (
                degraded.quality_status
                == "failed"
            ),
        },
        {
            "check": "empty_status_supported",
            "actual": empty.quality_status,
            "expected": "empty",
            "passed": (
                empty.quality_status == "empty"
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
            "check": "history_digest_reconciliation_implemented",
            "actual": passed.history_digest_reconciles,
            "expected": True,
            "passed": (
                passed.history_digest_reconciles
                is True
            ),
        },
        {
            "check": "history_identity_validation_implemented",
            "actual": passed.history_record_ids_unique,
            "expected": True,
            "passed": (
                passed.history_record_ids_unique
                is True
            ),
        },
        {
            "check": "history_order_validation_implemented",
            "actual": passed.history_order_reconciles,
            "expected": True,
            "passed": (
                passed.history_order_reconciles
                is True
            ),
        },
        {
            "check": "source_payload_digest_validation_implemented",
            "actual": passed.source_payload_digests_present,
            "expected": True,
            "passed": (
                passed.source_payload_digests_present
                is True
            ),
        },
        {
            "check": "status_count_reconciliation_implemented",
            "actual": warning.status_counts_reconcile,
            "expected": True,
            "passed": (
                warning.status_counts_reconcile
                is True
            ),
        },
        {
            "check": "warning_path_implemented",
            "actual": warning.quality_status,
            "expected": "passed_with_warnings",
            "passed": (
                warning.quality_status
                == "passed_with_warnings"
            ),
        },
        {
            "check": "degraded_failure_precedence_implemented",
            "actual": degraded.quality_status,
            "expected": "failed",
            "passed": (
                degraded.quality_status
                == "failed"
            ),
        },
        {
            "check": "conflict_failure_precedence_implemented",
            "actual": conflict.quality_status,
            "expected": "failed",
            "passed": (
                conflict.quality_status
                == "failed"
            ),
        },
        {
            "check": "quality_report_identity_deterministic",
            "actual": (
                passed.quality_report_id
                == repeated.quality_report_id
            ),
            "expected": True,
            "passed": (
                passed.quality_report_id
                == repeated.quality_report_id
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                passed.to_dict()
                == repeated.to_dict()
            ),
            "expected": True,
            "passed": (
                passed.to_dict()
                == repeated.to_dict()
            ),
        },
        {
            "check": "history_mutation_and_authority_absent",
            "actual": any(
                (
                    passed.history_mutated,
                    passed.retention_action_executed,
                    passed.physical_deletion_executed,
                    passed.production_authority,
                    passed.production_behavior_changed,
                    passed.simulation_behavior_changed,
                    passed.historical_outcomes_joined,
                    passed.predictive_evaluation_executed,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    passed.history_mutated,
                    passed.retention_action_executed,
                    passed.physical_deletion_executed,
                    passed.production_authority,
                    passed.production_behavior_changed,
                    passed.simulation_behavior_changed,
                    passed.historical_outcomes_joined,
                    passed.predictive_evaluation_executed,
                )
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    reports = (
        passed,
        warning,
        degraded,
        empty,
        disabled,
    )

    quality_status_rows = [
        {
            "quality_status": status,
            "count": sum(
                report.quality_status == status
                for report in reports
            ),
        }
        for status in (
            "passed",
            "passed_with_warnings",
            "failed",
            "empty",
            "disabled",
        )
    ]

    dimension_rows = [
        dimension.to_dict()
        for dimension in warning.dimensions
    ]

    failure_rows = [
        {
            "failure": "degraded_history_record",
            "quality_status": degraded.quality_status,
            "diagnostic_code": (
                "matchup_shadow_retention_observability_history_quality_gate_degraded_record"
            ),
        },
        {
            "failure": "conflicting_duplicate",
            "quality_status": conflict.quality_status,
            "diagnostic_code": (
                "matchup_shadow_retention_observability_history_quality_gate_conflicting_duplicate"
            ),
        },
        {
            "failure": "history_digest_mismatch",
            "quality_status": bad_digest.quality_status,
            "diagnostic_code": (
                "matchup_shadow_retention_observability_history_quality_gate_digest_mismatch"
            ),
        },
        {
            "failure": "history_identity_conflict",
            "quality_status": (
                duplicate_identity.quality_status
            ),
            "diagnostic_code": (
                "matchup_shadow_retention_observability_history_quality_gate_identity_conflict"
            ),
        },
        {
            "failure": "history_order_mismatch",
            "quality_status": (
                reversed_order.quality_status
            ),
            "diagnostic_code": (
                "matchup_shadow_retention_observability_history_quality_gate_order_mismatch"
            ),
        },
        {
            "failure": "source_digest_missing",
            "quality_status": (
                missing_source_digest.quality_status
            ),
            "diagnostic_code": (
                "matchup_shadow_retention_observability_history_quality_gate_source_digest_missing"
            ),
        },
    ]

    warning_rows = [
        {
            "warning": "warning_observability_record",
            "count": warning.warning_record_count,
            "quality_status": warning.quality_status,
        },
        {
            "warning": "exact_duplicate",
            "count": warning.exact_duplicate_count,
            "quality_status": warning.quality_status,
        },
    ]

    authority_rows = [
        {
            "authority": (
                "diagnostic_history_quality_gate"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded diagnostic history quality gate passed all checks."
            ),
        },
        {
            "authority": "history_mutation",
            "granted": False,
            "reason": (
                "The quality gate never mutates history."
            ),
        },
        {
            "authority": "retention_action_execution",
            "granted": False,
            "reason": (
                "The quality gate does not execute retention actions."
            ),
        },
        {
            "authority": "physical_record_deletion",
            "granted": False,
            "reason": (
                "The quality gate never deletes records."
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
        "retention_observability_history_quality_gate_contract_"
        "implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_contract_"
        "implementation_failed"
    )

    recommended_next_layer = (
        "8AB_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_observability_contract_plan"
        if all_checks_passed
        else
        "8AA_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_contract_"
        "implementation_remediation"
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
        OUTPUT_DIR / "history_quality_report.csv",
        [
            key
            for key in passed.to_dict().keys()
            if key != "dimensions"
        ],
        [
            {
                key: value
                for key, value in report.to_dict().items()
                if key != "dimensions"
            }
            for report in (
                passed,
                warning,
                degraded,
                empty,
            )
        ],
    )

    write_csv(
        OUTPUT_DIR / "quality_status_counts.csv",
        [
            "quality_status",
            "count",
        ],
        quality_status_rows,
    )

    write_csv(
        OUTPUT_DIR / "quality_dimension_results.csv",
        list(
            dimension_rows[0].keys()
        ),
        dimension_rows,
    )

    write_csv(
        OUTPUT_DIR / "history_integrity_failures.csv",
        [
            "failure",
            "quality_status",
            "diagnostic_code",
        ],
        failure_rows,
    )

    write_csv(
        OUTPUT_DIR / "history_warning_signals.csv",
        [
            "warning",
            "count",
            "quality_status",
        ],
        warning_rows,
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
                    "Plan deterministic observability for history quality-gate reports."
                    if all_checks_passed
                    else
                    "Remediate failed 8AA implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8AA implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "history_quality_report.json",
        warning.to_dict(),
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
        "quality_gate_version": (
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION
        ),
        "passed_status_supported": True,
        "passed_with_warnings_status_supported": True,
        "failed_status_supported": True,
        "empty_status_supported": True,
        "disabled_path_non_emitting": True,
        "history_digest_reconciliation_implemented": True,
        "history_identity_validation_implemented": True,
        "history_order_validation_implemented": True,
        "source_payload_digest_validation_implemented": True,
        "status_count_reconciliation_implemented": True,
        "warning_path_implemented": True,
        "failure_precedence_implemented": True,
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
        "history_quality_gate_observability_planning_allowed_next": (
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
                "history_quality_report.csv",
                "quality_status_counts.csv",
                "quality_dimension_results.csv",
                "history_integrity_failures.csv",
                "history_warning_signals.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            )
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "history_quality_report.json"
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
