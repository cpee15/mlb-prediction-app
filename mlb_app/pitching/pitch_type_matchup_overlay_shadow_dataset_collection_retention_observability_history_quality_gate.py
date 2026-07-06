"""
Deterministic diagnostic quality gate for Layer 8Y retention-observability
history.

The gate validates history digest integrity, record identities, deterministic
ordering, source payload digests, observability status counts, duplicate
integrity, and authority boundaries.

It does not mutate history, execute retention actions, delete records, join
historical outcomes, perform predictive evaluation, or change production or
simulation behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history import (
    RetentionObservabilityHistoryLedger,
    RetentionObservabilityHistoryRecord,
    retention_observability_history_digest,
)


RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION = "8AA-v1"

SUPPORTED_HISTORY_QUALITY_STATUSES = frozenset(
    {
        "passed",
        "passed_with_warnings",
        "failed",
        "empty",
        "disabled",
    }
)

SUPPORTED_SOURCE_OBSERVABILITY_STATUSES = frozenset(
    {
        "healthy",
        "warning",
        "degraded",
        "empty",
    }
)


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityDimension:
    dimension_id: str
    dimension: str
    passed: bool
    triggered: bool
    observed_value: str
    expected_value: str
    diagnostic_code: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityReport:
    emitted: bool
    reason: str
    quality_report_id: str | None
    quality_gate_version: str
    evaluated_at_utc: str
    history_version: str | None
    history_digest: str | None
    quality_status: str
    history_record_count: int
    healthy_record_count: int
    warning_record_count: int
    degraded_record_count: int
    empty_record_count: int
    unique_history_record_count: int
    exact_duplicate_count: int
    conflicting_duplicate_count: int
    history_digest_reconciles: bool
    history_record_ids_unique: bool
    history_order_reconciles: bool
    source_payload_digests_present: bool
    status_counts_reconcile: bool
    production_authority_absent: bool
    dimensions: tuple[
        RetentionObservabilityHistoryQualityDimension,
        ...,
    ]
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    quality_gate_passed: bool
    history_mutated: bool = False
    retention_action_executed: bool = False
    physical_deletion_executed: bool = False
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False
    historical_outcomes_joined: bool = False
    predictive_evaluation_executed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "emitted": self.emitted,
            "reason": self.reason,
            "quality_report_id": self.quality_report_id,
            "quality_gate_version": self.quality_gate_version,
            "evaluated_at_utc": self.evaluated_at_utc,
            "history_version": self.history_version,
            "history_digest": self.history_digest,
            "quality_status": self.quality_status,
            "history_record_count": self.history_record_count,
            "healthy_record_count": self.healthy_record_count,
            "warning_record_count": self.warning_record_count,
            "degraded_record_count": self.degraded_record_count,
            "empty_record_count": self.empty_record_count,
            "unique_history_record_count": (
                self.unique_history_record_count
            ),
            "exact_duplicate_count": self.exact_duplicate_count,
            "conflicting_duplicate_count": (
                self.conflicting_duplicate_count
            ),
            "history_digest_reconciles": (
                self.history_digest_reconciles
            ),
            "history_record_ids_unique": (
                self.history_record_ids_unique
            ),
            "history_order_reconciles": (
                self.history_order_reconciles
            ),
            "source_payload_digests_present": (
                self.source_payload_digests_present
            ),
            "status_counts_reconcile": (
                self.status_counts_reconcile
            ),
            "production_authority_absent": (
                self.production_authority_absent
            ),
            "dimensions": [
                dimension.to_dict()
                for dimension in self.dimensions
            ],
            "diagnostic_codes": list(
                self.diagnostic_codes
            ),
            "validation_errors": list(
                self.validation_errors
            ),
            "quality_gate_passed": self.quality_gate_passed,
            "history_mutated": self.history_mutated,
            "retention_action_executed": (
                self.retention_action_executed
            ),
            "physical_deletion_executed": (
                self.physical_deletion_executed
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
            "historical_outcomes_joined": (
                self.historical_outcomes_joined
            ),
            "predictive_evaluation_executed": (
                self.predictive_evaluation_executed
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


def _valid_sha256(
    value: str,
) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(
            character in "0123456789abcdef"
            for character in value.lower()
        )
    )


def _dimension(
    dimension_id: str,
    dimension: str,
    *,
    passed: bool,
    triggered: bool,
    observed: Any,
    expected: Any,
    diagnostic_code: str | None,
) -> RetentionObservabilityHistoryQualityDimension:
    return RetentionObservabilityHistoryQualityDimension(
        dimension_id=dimension_id,
        dimension=dimension,
        passed=passed,
        triggered=triggered,
        observed_value=_canonical_json(
            observed
        ),
        expected_value=_canonical_json(
            expected
        ),
        diagnostic_code=diagnostic_code,
    )


def _quality_report_id(
    *,
    evaluated_at_utc: str,
    history_version: str,
    history_digest: str | None,
    quality_status: str,
    history_record_count: int,
    history_digest_reconciles: bool,
    history_record_ids_unique: bool,
    history_order_reconciles: bool,
    source_payload_digests_present: bool,
    status_counts_reconcile: bool,
    production_authority_absent: bool,
) -> str:
    digest = _sha256(
        {
            "quality_gate_version": (
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION
            ),
            "evaluated_at_utc": evaluated_at_utc,
            "history_version": history_version,
            "history_digest": history_digest,
            "quality_status": quality_status,
            "history_record_count": history_record_count,
            "history_digest_reconciles": (
                history_digest_reconciles
            ),
            "history_record_ids_unique": (
                history_record_ids_unique
            ),
            "history_order_reconciles": (
                history_order_reconciles
            ),
            "source_payload_digests_present": (
                source_payload_digests_present
            ),
            "status_counts_reconcile": (
                status_counts_reconcile
            ),
            "production_authority_absent": (
                production_authority_absent
            ),
        }
    )

    return (
        "matchup-shadow-retention-observability-history-quality-"
        + digest[:20]
    )


def _ordered_records(
    records: Iterable[
        RetentionObservabilityHistoryRecord
    ],
) -> tuple[
    RetentionObservabilityHistoryRecord,
    ...,
]:
    return tuple(
        sorted(
            records,
            key=lambda record: (
                record.observed_at_utc,
                record.history_record_id,
            ),
        )
    )


def evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality(
    history_ledger: RetentionObservabilityHistoryLedger | None,
    *,
    enabled: bool = False,
    evaluated_at_utc: str,
) -> RetentionObservabilityHistoryQualityReport:
    if not enabled:
        return RetentionObservabilityHistoryQualityReport(
            emitted=False,
            reason="retention_observability_history_quality_gate_disabled",
            quality_report_id=None,
            quality_gate_version=(
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION
            ),
            evaluated_at_utc=evaluated_at_utc,
            history_version=None,
            history_digest=None,
            quality_status="disabled",
            history_record_count=0,
            healthy_record_count=0,
            warning_record_count=0,
            degraded_record_count=0,
            empty_record_count=0,
            unique_history_record_count=0,
            exact_duplicate_count=0,
            conflicting_duplicate_count=0,
            history_digest_reconciles=False,
            history_record_ids_unique=False,
            history_order_reconciles=False,
            source_payload_digests_present=False,
            status_counts_reconcile=False,
            production_authority_absent=True,
            dimensions=(),
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_quality_gate_disabled",
            ),
            validation_errors=(),
            quality_gate_passed=False,
        )

    if history_ledger is None:
        return RetentionObservabilityHistoryQualityReport(
            emitted=True,
            reason="retention_observability_history_quality_gate_failed",
            quality_report_id=None,
            quality_gate_version=(
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION
            ),
            evaluated_at_utc=evaluated_at_utc,
            history_version=None,
            history_digest=None,
            quality_status="failed",
            history_record_count=0,
            healthy_record_count=0,
            warning_record_count=0,
            degraded_record_count=0,
            empty_record_count=0,
            unique_history_record_count=0,
            exact_duplicate_count=0,
            conflicting_duplicate_count=0,
            history_digest_reconciles=False,
            history_record_ids_unique=False,
            history_order_reconciles=False,
            source_payload_digests_present=False,
            status_counts_reconcile=False,
            production_authority_absent=True,
            dimensions=(),
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_quality_gate_ledger_missing",
            ),
            validation_errors=(
                "matchup_shadow_retention_observability_history_quality_gate_ledger_missing",
            ),
            quality_gate_passed=False,
        )

    records = tuple(
        history_ledger.records
    )

    ordered_records = _ordered_records(
        records
    )

    history_order_reconciles = (
        records == ordered_records
    )

    record_ids = [
        record.history_record_id
        for record in records
    ]

    history_record_ids_unique = (
        len(record_ids)
        == len(set(record_ids))
    )

    recomputed_digest = (
        retention_observability_history_digest(
            records
        )
    )

    history_digest_reconciles = (
        history_ledger.history_digest is not None
        and history_ledger.history_digest
        == recomputed_digest
    )

    source_payload_digests_present = all(
        (
            _valid_sha256(
                record.snapshot_payload_digest
            )
            and _valid_sha256(
                record.report_payload_digest
            )
        )
        for record in records
    )

    healthy_count = sum(
        record.observability_status
        == "healthy"
        for record in records
    )
    warning_count = sum(
        record.observability_status
        == "warning"
        for record in records
    )
    degraded_count = sum(
        record.observability_status
        == "degraded"
        for record in records
    )
    empty_count = sum(
        record.observability_status
        == "empty"
        for record in records
    )

    statuses_supported = all(
        record.observability_status
        in SUPPORTED_SOURCE_OBSERVABILITY_STATUSES
        for record in records
    )

    status_counts_reconcile = (
        statuses_supported
        and (
            healthy_count
            + warning_count
            + degraded_count
            + empty_count
        )
        == len(records)
    )

    exact_duplicate_count = sum(
        duplicate.duplicate_count
        for duplicate in history_ledger.duplicates
        if not duplicate.conflict
    )

    conflicting_duplicate_count = sum(
        duplicate.duplicate_count
        for duplicate in history_ledger.duplicates
        if duplicate.conflict
    )

    production_authority_absent = all(
        value is False
        for value in (
            history_ledger.production_authority,
            history_ledger.production_behavior_changed,
            history_ledger.simulation_behavior_changed,
            history_ledger.retention_action_executed,
            history_ledger.physical_deletion_executed,
            history_ledger.historical_outcomes_joined,
            history_ledger.predictive_evaluation_executed,
        )
    ) and all(
        record.production_authority is False
        for record in records
    )

    diagnostics: list[str] = []
    validation_errors: list[str] = []

    if history_ledger.history_digest is None:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_digest_missing"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_history_quality_gate_digest_missing"
        )
    elif not history_digest_reconciles:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_digest_mismatch"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_history_quality_gate_digest_mismatch"
        )

    if not history_record_ids_unique:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_identity_conflict"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_history_quality_gate_identity_conflict"
        )

    if not history_order_reconciles:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_order_mismatch"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_history_quality_gate_order_mismatch"
        )

    if not source_payload_digests_present:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_source_digest_missing"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_history_quality_gate_source_digest_missing"
        )

    if not status_counts_reconcile:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_count_mismatch"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_history_quality_gate_count_mismatch"
        )

    if conflicting_duplicate_count:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_conflicting_duplicate"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_history_quality_gate_conflicting_duplicate"
        )

    if degraded_count:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_degraded_record"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_history_quality_gate_degraded_record"
        )

    if not production_authority_absent:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_authority_violation"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_history_quality_gate_authority_violation"
        )

    if warning_count:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_warning_records"
        )

    if exact_duplicate_count:
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_exact_duplicates"
        )

    hard_failure = any(
        (
            not history_digest_reconciles,
            not history_record_ids_unique,
            not history_order_reconciles,
            not source_payload_digests_present,
            not status_counts_reconcile,
            conflicting_duplicate_count > 0,
            degraded_count > 0,
            not production_authority_absent,
        )
    )

    if not records:
        quality_status = "empty"
        reason = (
            "retention_observability_history_quality_gate_empty"
        )
        diagnostics.append(
            "matchup_shadow_retention_observability_history_quality_gate_empty"
        )
        quality_gate_passed = False
    elif hard_failure:
        quality_status = "failed"
        reason = (
            "retention_observability_history_quality_gate_failed"
        )
        quality_gate_passed = False
    elif (
        warning_count > 0
        or exact_duplicate_count > 0
    ):
        quality_status = "passed_with_warnings"
        reason = (
            "retention_observability_history_quality_gate_passed_with_warnings"
        )
        quality_gate_passed = True
    else:
        quality_status = "passed"
        reason = (
            "retention_observability_history_quality_gate_passed"
        )
        quality_gate_passed = True

    dimensions = (
        _dimension(
            "HQ-D01",
            "history_digest_integrity",
            passed=history_digest_reconciles,
            triggered=not history_digest_reconciles,
            observed=history_digest_reconciles,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_quality_gate_digest_mismatch"
                if not history_digest_reconciles
                else None
            ),
        ),
        _dimension(
            "HQ-D02",
            "history_identity_integrity",
            passed=history_record_ids_unique,
            triggered=not history_record_ids_unique,
            observed=history_record_ids_unique,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_quality_gate_identity_conflict"
                if not history_record_ids_unique
                else None
            ),
        ),
        _dimension(
            "HQ-D03",
            "history_order_integrity",
            passed=history_order_reconciles,
            triggered=not history_order_reconciles,
            observed=history_order_reconciles,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_quality_gate_order_mismatch"
                if not history_order_reconciles
                else None
            ),
        ),
        _dimension(
            "HQ-D04",
            "source_payload_digest_integrity",
            passed=source_payload_digests_present,
            triggered=not source_payload_digests_present,
            observed=source_payload_digests_present,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_quality_gate_source_digest_missing"
                if not source_payload_digests_present
                else None
            ),
        ),
        _dimension(
            "HQ-D05",
            "observability_status_integrity",
            passed=degraded_count == 0,
            triggered=degraded_count > 0,
            observed={
                "healthy": healthy_count,
                "warning": warning_count,
                "degraded": degraded_count,
                "empty": empty_count,
            },
            expected={
                "degraded": 0,
            },
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_quality_gate_degraded_record"
                if degraded_count
                else None
            ),
        ),
        _dimension(
            "HQ-D06",
            "duplicate_integrity",
            passed=conflicting_duplicate_count == 0,
            triggered=(
                conflicting_duplicate_count > 0
                or exact_duplicate_count > 0
            ),
            observed={
                "exact": exact_duplicate_count,
                "conflicting": conflicting_duplicate_count,
            },
            expected={
                "conflicting": 0,
            },
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_quality_gate_conflicting_duplicate"
                if conflicting_duplicate_count
                else
                "matchup_shadow_retention_observability_history_quality_gate_exact_duplicates"
                if exact_duplicate_count
                else None
            ),
        ),
        _dimension(
            "HQ-D07",
            "status_count_reconciliation",
            passed=status_counts_reconcile,
            triggered=not status_counts_reconcile,
            observed=status_counts_reconcile,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_quality_gate_count_mismatch"
                if not status_counts_reconcile
                else None
            ),
        ),
        _dimension(
            "HQ-D08",
            "authority_boundary",
            passed=production_authority_absent,
            triggered=not production_authority_absent,
            observed=production_authority_absent,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_quality_gate_authority_violation"
                if not production_authority_absent
                else None
            ),
        ),
    )

    quality_report_id = _quality_report_id(
        evaluated_at_utc=evaluated_at_utc,
        history_version=history_ledger.history_version,
        history_digest=history_ledger.history_digest,
        quality_status=quality_status,
        history_record_count=len(records),
        history_digest_reconciles=(
            history_digest_reconciles
        ),
        history_record_ids_unique=(
            history_record_ids_unique
        ),
        history_order_reconciles=(
            history_order_reconciles
        ),
        source_payload_digests_present=(
            source_payload_digests_present
        ),
        status_counts_reconcile=(
            status_counts_reconcile
        ),
        production_authority_absent=(
            production_authority_absent
        ),
    )

    return RetentionObservabilityHistoryQualityReport(
        emitted=True,
        reason=reason,
        quality_report_id=quality_report_id,
        quality_gate_version=(
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION
        ),
        evaluated_at_utc=evaluated_at_utc,
        history_version=history_ledger.history_version,
        history_digest=history_ledger.history_digest,
        quality_status=quality_status,
        history_record_count=len(records),
        healthy_record_count=healthy_count,
        warning_record_count=warning_count,
        degraded_record_count=degraded_count,
        empty_record_count=empty_count,
        unique_history_record_count=len(
            set(record_ids)
        ),
        exact_duplicate_count=exact_duplicate_count,
        conflicting_duplicate_count=(
            conflicting_duplicate_count
        ),
        history_digest_reconciles=(
            history_digest_reconciles
        ),
        history_record_ids_unique=(
            history_record_ids_unique
        ),
        history_order_reconciles=(
            history_order_reconciles
        ),
        source_payload_digests_present=(
            source_payload_digests_present
        ),
        status_counts_reconcile=(
            status_counts_reconcile
        ),
        production_authority_absent=(
            production_authority_absent
        ),
        dimensions=dimensions,
        diagnostic_codes=(
            _sorted_unique_strings(
                diagnostics
            )
        ),
        validation_errors=(
            _sorted_unique_strings(
                validation_errors
            )
        ),
        quality_gate_passed=(
            quality_gate_passed
        ),
    )
