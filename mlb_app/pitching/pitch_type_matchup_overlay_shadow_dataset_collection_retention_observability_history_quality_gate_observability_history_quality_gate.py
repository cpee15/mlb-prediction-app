"""
Deterministic diagnostic quality gate over immutable Layer 8AE
quality-gate-observability history ledgers.

The quality gate validates:

- history-ledger digest integrity;
- history-record identity uniqueness;
- deterministic history ordering;
- snapshot-payload digests;
- source-version lineage;
- status-count reconciliation;
- duplicate integrity;
- production-authority boundaries.

The gate is disabled by default and never mutates source ledgers or records,
executes retention actions, joins outcomes, evaluates predictive performance,
or changes production or simulation behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history import (
    RetentionObservabilityHistoryQualityGateObservabilityHistoryLedger,
    RetentionObservabilityHistoryQualityGateObservabilityHistoryRecord,
    observability_history_digest,
)


RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION = (
    "8AG-v1"
)


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityDimension:
    dimension_id: str
    dimension: str
    passed: bool
    triggered: bool
    actual: str
    expected: str
    diagnostic_code: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityReport:
    quality_report_id: str
    quality_gate_version: str
    evaluated_at_utc: str
    history_version: str
    history_digest: str
    quality_status: str
    history_record_count: int
    appended_record_count: int
    warning_record_count: int
    degraded_record_count: int
    empty_record_count: int
    exact_duplicate_count: int
    conflicting_duplicate_count: int
    unique_history_record_count: int
    history_digest_reconciles: bool
    history_record_ids_unique: bool
    history_order_reconciles: bool
    snapshot_payload_digests_present: bool
    source_versions_present: bool
    status_counts_reconcile: bool
    failed_dimension_count: int
    triggered_dimension_count: int
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        payload["validation_errors"] = list(
            self.validation_errors
        )
        return payload


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityGateResult:
    emitted: bool
    reason: str
    report: (
        RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityReport
        | None
    )
    dimensions: tuple[
        RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityDimension,
        ...,
    ]
    history_mutated: bool = False
    quality_report_mutated: bool = False
    retention_action_executed: bool = False
    physical_deletion_executed: bool = False
    historical_outcomes_joined: bool = False
    predictive_evaluation_executed: bool = False
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "emitted": self.emitted,
            "reason": self.reason,
            "report": (
                self.report.to_dict()
                if self.report is not None
                else None
            ),
            "dimensions": [
                dimension.to_dict()
                for dimension in self.dimensions
            ],
            "history_mutated": self.history_mutated,
            "quality_report_mutated": (
                self.quality_report_mutated
            ),
            "retention_action_executed": (
                self.retention_action_executed
            ),
            "physical_deletion_executed": (
                self.physical_deletion_executed
            ),
            "historical_outcomes_joined": (
                self.historical_outcomes_joined
            ),
            "predictive_evaluation_executed": (
                self.predictive_evaluation_executed
            ),
            "production_authority": (
                self.production_authority
            ),
            "production_behavior_changed": (
                self.production_behavior_changed
            ),
            "simulation_behavior_changed": (
                self.simulation_behavior_changed
            ),
        }


def _canonical_json(
    payload: Any,
) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _sha256(
    payload: Any,
) -> str:
    return hashlib.sha256(
        _canonical_json(payload).encode("utf-8")
    ).hexdigest()


def _sorted_unique_strings(
    values: Iterable[str],
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                value
                for value in values
                if isinstance(value, str)
                and value
            }
        )
    )


def _is_sha256(
    value: Any,
) -> bool:
    if not isinstance(value, str):
        return False

    if len(value) != 64:
        return False

    return all(
        character in "0123456789abcdef"
        for character in value.lower()
    )


def _records_are_sorted(
    records: tuple[
        RetentionObservabilityHistoryQualityGateObservabilityHistoryRecord,
        ...,
    ],
) -> bool:
    return records == tuple(
        sorted(
            records,
            key=lambda record: (
                record.observed_at_utc,
                record.history_record_id,
            ),
        )
    )


def _quality_report_identity_payload(
    *,
    evaluated_at_utc: str,
    history_version: str,
    history_digest: str,
    quality_status: str,
    history_record_count: int,
    appended_record_count: int,
    warning_record_count: int,
    degraded_record_count: int,
    empty_record_count: int,
    exact_duplicate_count: int,
    conflicting_duplicate_count: int,
    unique_history_record_count: int,
    history_digest_reconciles: bool,
    history_record_ids_unique: bool,
    history_order_reconciles: bool,
    snapshot_payload_digests_present: bool,
    source_versions_present: bool,
    status_counts_reconcile: bool,
    failed_dimension_count: int,
    triggered_dimension_count: int,
    diagnostic_codes: tuple[str, ...],
    validation_errors: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "quality_gate_version": (
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION
        ),
        "evaluated_at_utc": evaluated_at_utc,
        "history_version": history_version,
        "history_digest": history_digest,
        "quality_status": quality_status,
        "history_record_count": history_record_count,
        "appended_record_count": appended_record_count,
        "warning_record_count": warning_record_count,
        "degraded_record_count": degraded_record_count,
        "empty_record_count": empty_record_count,
        "exact_duplicate_count": exact_duplicate_count,
        "conflicting_duplicate_count": (
            conflicting_duplicate_count
        ),
        "unique_history_record_count": (
            unique_history_record_count
        ),
        "history_digest_reconciles": (
            history_digest_reconciles
        ),
        "history_record_ids_unique": (
            history_record_ids_unique
        ),
        "history_order_reconciles": (
            history_order_reconciles
        ),
        "snapshot_payload_digests_present": (
            snapshot_payload_digests_present
        ),
        "source_versions_present": (
            source_versions_present
        ),
        "status_counts_reconcile": (
            status_counts_reconcile
        ),
        "failed_dimension_count": (
            failed_dimension_count
        ),
        "triggered_dimension_count": (
            triggered_dimension_count
        ),
        "diagnostic_codes": list(
            diagnostic_codes
        ),
        "validation_errors": list(
            validation_errors
        ),
    }


def observability_history_quality_report_id(
    **payload: Any,
) -> str:
    digest = _sha256(
        _quality_report_identity_payload(
            **payload
        )
    )

    return (
        "matchup-shadow-retention-observability-history-"
        "quality-gate-observability-history-quality-report-"
        + digest[:20]
    )


def evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
    history_ledger: (
        RetentionObservabilityHistoryQualityGateObservabilityHistoryLedger
        | None
    ),
    *,
    enabled: bool = False,
    evaluated_at_utc: str,
) -> RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityGateResult:
    if not enabled:
        return RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityGateResult(
            emitted=False,
            reason=(
                "retention_observability_history_quality_gate_"
                "observability_history_quality_gate_disabled"
            ),
            report=None,
            dimensions=(),
        )

    if history_ledger is None:
        diagnostic_code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_ledger_missing"
        )

        dimensions = (
            RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityDimension(
                dimension_id="HQOHQ-D01",
                dimension="history_digest_integrity",
                passed=False,
                triggered=True,
                actual="missing_history_ledger",
                expected="history_ledger_present",
                diagnostic_code=diagnostic_code,
            ),
        )

        diagnostic_codes = (
            diagnostic_code,
        )
        validation_errors = (
            diagnostic_code,
        )

        identity_payload = {
            "evaluated_at_utc": evaluated_at_utc,
            "history_version": "",
            "history_digest": "",
            "quality_status": "failed",
            "history_record_count": 0,
            "appended_record_count": 0,
            "warning_record_count": 0,
            "degraded_record_count": 0,
            "empty_record_count": 0,
            "exact_duplicate_count": 0,
            "conflicting_duplicate_count": 0,
            "unique_history_record_count": 0,
            "history_digest_reconciles": False,
            "history_record_ids_unique": False,
            "history_order_reconciles": False,
            "snapshot_payload_digests_present": False,
            "source_versions_present": False,
            "status_counts_reconcile": False,
            "failed_dimension_count": 1,
            "triggered_dimension_count": 1,
            "diagnostic_codes": diagnostic_codes,
            "validation_errors": validation_errors,
        }

        report = RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityReport(
            quality_report_id=(
                observability_history_quality_report_id(
                    **identity_payload
                )
            ),
            quality_gate_version=(
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION
            ),
            **identity_payload,
        )

        return RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityGateResult(
            emitted=True,
            reason=(
                "retention_observability_history_quality_gate_"
                "observability_history_quality_gate_failed"
            ),
            report=report,
            dimensions=dimensions,
        )

    records = tuple(
        history_ledger.records
    )

    history_record_count = len(
        records
    )

    record_ids = [
        record.history_record_id
        for record in records
    ]

    unique_history_record_count = len(
        set(record_ids)
    )

    warning_record_count = sum(
        record.observability_status == "warning"
        or record.quality_status
        == "passed_with_warnings"
        for record in records
    )

    degraded_record_count = sum(
        record.observability_status == "degraded"
        or record.quality_status == "failed"
        for record in records
    )

    empty_record_count = sum(
        record.observability_status == "empty"
        or record.quality_status == "empty"
        for record in records
    )

    exact_duplicate_count = (
        sum(
            record.exact_duplicate_count
            for record in records
        )
        + sum(
            duplicate.duplicate_count
            for duplicate in history_ledger.duplicates
            if not duplicate.conflict
        )
    )

    conflicting_duplicate_count = (
        sum(
            record.conflicting_duplicate_count
            for record in records
        )
        + sum(
            duplicate.duplicate_count
            for duplicate in history_ledger.duplicates
            if duplicate.conflict
        )
    )

    appended_record_count = (
        history_record_count
    )

    recomputed_digest = (
        observability_history_digest(
            records
        )
    )

    history_digest_reconciles = (
        _is_sha256(
            history_ledger.history_digest
        )
        and history_ledger.history_digest
        == recomputed_digest
    )

    history_record_ids_unique = (
        all(record_ids)
        and unique_history_record_count
        == history_record_count
    )

    history_order_reconciles = (
        _records_are_sorted(
            records
        )
    )

    snapshot_payload_digests_present = all(
        _is_sha256(
            record.snapshot_payload_digest
        )
        for record in records
    )

    source_versions_present = all(
        (
            record.history_version
            and record.observability_version
            and record.quality_gate_version
            and record.quality_report_id
        )
        for record in records
    )

    status_counts = (
        appended_record_count,
        warning_record_count,
        degraded_record_count,
        empty_record_count,
        exact_duplicate_count,
        conflicting_duplicate_count,
        unique_history_record_count,
    )

    status_counts_nonnegative = all(
        value >= 0
        for value in status_counts
    )

    status_counts_reconcile = (
        status_counts_nonnegative
        and appended_record_count
        == history_record_count
        and unique_history_record_count
        <= history_record_count
        and warning_record_count
        <= history_record_count
        and degraded_record_count
        <= history_record_count
        and empty_record_count
        <= history_record_count
    )

    authority_boundary_passed = all(
        value is False
        for value in (
            history_ledger.history_mutated,
            history_ledger.source_snapshot_mutated,
            history_ledger.retention_action_executed,
            history_ledger.physical_deletion_executed,
            history_ledger.historical_outcomes_joined,
            history_ledger.predictive_evaluation_executed,
            history_ledger.production_authority,
            history_ledger.production_behavior_changed,
            history_ledger.simulation_behavior_changed,
        )
    ) and all(
        record.production_authority is False
        for record in records
    )

    dimensions: list[
        RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityDimension
    ] = []

    def add_dimension(
        *,
        dimension_id: str,
        dimension: str,
        passed: bool,
        triggered: bool,
        actual: Any,
        expected: Any,
        diagnostic_code: str,
    ) -> None:
        dimensions.append(
            RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityDimension(
                dimension_id=dimension_id,
                dimension=dimension,
                passed=passed,
                triggered=triggered,
                actual=_canonical_json(
                    actual
                ),
                expected=_canonical_json(
                    expected
                ),
                diagnostic_code=(
                    diagnostic_code
                    if triggered
                    else ""
                ),
            )
        )

    add_dimension(
        dimension_id="HQOHQ-D01",
        dimension="history_digest_integrity",
        passed=history_digest_reconciles,
        triggered=not history_digest_reconciles,
        actual={
            "stored": history_ledger.history_digest,
            "recomputed": recomputed_digest,
        },
        expected="matching_sha256_digests",
        diagnostic_code=(
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_digest_mismatch"
        ),
    )

    add_dimension(
        dimension_id="HQOHQ-D02",
        dimension="history_identity_integrity",
        passed=history_record_ids_unique,
        triggered=not history_record_ids_unique,
        actual={
            "record_count": history_record_count,
            "unique_record_count": (
                unique_history_record_count
            ),
        },
        expected="all_history_record_ids_present_and_unique",
        diagnostic_code=(
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_identity_conflict"
        ),
    )

    add_dimension(
        dimension_id="HQOHQ-D03",
        dimension="history_order_integrity",
        passed=history_order_reconciles,
        triggered=not history_order_reconciles,
        actual=[
            record.history_record_id
            for record in records
        ],
        expected="deterministic_history_order",
        diagnostic_code=(
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_order_mismatch"
        ),
    )

    add_dimension(
        dimension_id="HQOHQ-D04",
        dimension="snapshot_payload_digest_integrity",
        passed=snapshot_payload_digests_present,
        triggered=(
            not snapshot_payload_digests_present
        ),
        actual=[
            record.snapshot_payload_digest
            for record in records
        ],
        expected="all_snapshot_payload_digests_valid_sha256",
        diagnostic_code=(
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_source_digest_missing"
        ),
    )

    add_dimension(
        dimension_id="HQOHQ-D05",
        dimension="source_version_integrity",
        passed=source_versions_present,
        triggered=not source_versions_present,
        actual=[
            {
                "history_version": record.history_version,
                "observability_version": (
                    record.observability_version
                ),
                "quality_gate_version": (
                    record.quality_gate_version
                ),
                "quality_report_id": (
                    record.quality_report_id
                ),
            }
            for record in records
        ],
        expected="all_source_versions_and_report_ids_present",
        diagnostic_code=(
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_source_version_missing"
        ),
    )

    add_dimension(
        dimension_id="HQOHQ-D06",
        dimension="status_count_integrity",
        passed=status_counts_reconcile,
        triggered=not status_counts_reconcile,
        actual={
            "history_record_count": history_record_count,
            "appended_record_count": appended_record_count,
            "warning_record_count": warning_record_count,
            "degraded_record_count": degraded_record_count,
            "empty_record_count": empty_record_count,
            "unique_history_record_count": (
                unique_history_record_count
            ),
        },
        expected="status_counts_reconcile",
        diagnostic_code=(
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_status_count_mismatch"
        ),
    )

    duplicate_integrity_passed = (
        conflicting_duplicate_count == 0
    )

    duplicate_warning_triggered = (
        exact_duplicate_count > 0
    )

    add_dimension(
        dimension_id="HQOHQ-D07",
        dimension="duplicate_integrity",
        passed=duplicate_integrity_passed,
        triggered=(
            not duplicate_integrity_passed
            or duplicate_warning_triggered
        ),
        actual={
            "exact_duplicate_count": (
                exact_duplicate_count
            ),
            "conflicting_duplicate_count": (
                conflicting_duplicate_count
            ),
        },
        expected={
            "exact_duplicates": "warning_only",
            "conflicting_duplicates": 0,
        },
        diagnostic_code=(
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_"
            + (
                "identity_conflict"
                if conflicting_duplicate_count > 0
                else "exact_duplicate_warning"
            )
        ),
    )

    add_dimension(
        dimension_id="HQOHQ-D08",
        dimension="authority_boundary",
        passed=authority_boundary_passed,
        triggered=not authority_boundary_passed,
        actual={
            "production_authority": (
                history_ledger.production_authority
            ),
            "history_mutated": (
                history_ledger.history_mutated
            ),
            "retention_action_executed": (
                history_ledger.retention_action_executed
            ),
        },
        expected="all_authority_and_mutation_flags_false",
        diagnostic_code=(
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_authority_violation"
        ),
    )

    failed_dimensions = [
        dimension
        for dimension in dimensions
        if not dimension.passed
    ]

    triggered_dimensions = [
        dimension
        for dimension in dimensions
        if dimension.triggered
    ]

    diagnostics = [
        dimension.diagnostic_code
        for dimension in triggered_dimensions
        if dimension.diagnostic_code
    ]

    validation_errors = [
        dimension.diagnostic_code
        for dimension in failed_dimensions
        if dimension.diagnostic_code
    ]

    if degraded_record_count > 0:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_gate_degraded_record"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    diagnostic_codes = (
        _sorted_unique_strings(
            diagnostics
        )
    )

    validation_errors_tuple = (
        _sorted_unique_strings(
            validation_errors
        )
    )

    if history_record_count == 0:
        quality_status = "empty"
    elif validation_errors_tuple:
        quality_status = "failed"
    elif (
        warning_record_count > 0
        or exact_duplicate_count > 0
        or empty_record_count > 0
    ):
        quality_status = (
            "passed_with_warnings"
        )
    else:
        quality_status = "passed"

    history_digest = (
        history_ledger.history_digest
        if isinstance(
            history_ledger.history_digest,
            str,
        )
        else ""
    )

    identity_payload = {
        "evaluated_at_utc": evaluated_at_utc,
        "history_version": (
            history_ledger.history_version
        ),
        "history_digest": history_digest,
        "quality_status": quality_status,
        "history_record_count": history_record_count,
        "appended_record_count": (
            appended_record_count
        ),
        "warning_record_count": (
            warning_record_count
        ),
        "degraded_record_count": (
            degraded_record_count
        ),
        "empty_record_count": (
            empty_record_count
        ),
        "exact_duplicate_count": (
            exact_duplicate_count
        ),
        "conflicting_duplicate_count": (
            conflicting_duplicate_count
        ),
        "unique_history_record_count": (
            unique_history_record_count
        ),
        "history_digest_reconciles": (
            history_digest_reconciles
        ),
        "history_record_ids_unique": (
            history_record_ids_unique
        ),
        "history_order_reconciles": (
            history_order_reconciles
        ),
        "snapshot_payload_digests_present": (
            snapshot_payload_digests_present
        ),
        "source_versions_present": (
            source_versions_present
        ),
        "status_counts_reconcile": (
            status_counts_reconcile
        ),
        "failed_dimension_count": len(
            failed_dimensions
        ),
        "triggered_dimension_count": len(
            triggered_dimensions
        ),
        "diagnostic_codes": diagnostic_codes,
        "validation_errors": (
            validation_errors_tuple
        ),
    }

    report = RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityReport(
        quality_report_id=(
            observability_history_quality_report_id(
                **identity_payload
            )
        ),
        quality_gate_version=(
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION
        ),
        **identity_payload,
    )

    return RetentionObservabilityHistoryQualityGateObservabilityHistoryQualityGateResult(
        emitted=True,
        reason=(
            "retention_observability_history_quality_gate_"
            "observability_history_quality_gate_"
            + quality_status
        ),
        report=report,
        dimensions=tuple(
            dimensions
        ),
    )
