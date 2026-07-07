"""
Deterministic diagnostic observability over Layer 8AA retention-observability
history quality-gate reports.

This module does not mutate source history or quality reports, execute
retention actions, delete records, join outcomes, perform predictive
evaluation, or alter production or simulation behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate import (
    RetentionObservabilityHistoryQualityDimension,
    RetentionObservabilityHistoryQualityReport,
)


RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_VERSION = "8AC-v1"

SUPPORTED_OBSERVABILITY_STATUSES = frozenset(
    {
        "healthy",
        "warning",
        "degraded",
        "empty",
        "disabled",
    }
)

SUPPORTED_QUALITY_STATUSES = frozenset(
    {
        "passed",
        "passed_with_warnings",
        "failed",
        "empty",
        "disabled",
    }
)


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityGateObservabilitySignal:
    signal_id: str
    signal_group: str
    signal_name: str
    passed: bool
    triggered: bool
    observed_value: str
    expected_value: str
    diagnostic_code: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityGateObservabilitySnapshot:
    observability_snapshot_id: str
    observability_version: str
    observed_at_utc: str
    quality_gate_version: str
    quality_report_id: str
    quality_status: str
    observability_status: str
    history_record_count: int
    healthy_record_count: int
    warning_record_count: int
    degraded_record_count: int
    empty_record_count: int
    exact_duplicate_count: int
    conflicting_duplicate_count: int
    failed_dimension_count: int
    triggered_dimension_count: int
    history_digest_reconciles: bool
    history_record_ids_unique: bool
    history_order_reconciles: bool
    source_payload_digests_present: bool
    status_counts_reconcile: bool
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    production_authority: bool = False

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
class RetentionObservabilityHistoryQualityGateObservabilityReport:
    emitted: bool
    reason: str
    observability_status: str
    snapshot: (
        RetentionObservabilityHistoryQualityGateObservabilitySnapshot
        | None
    )
    signals: tuple[
        RetentionObservabilityHistoryQualityGateObservabilitySignal,
        ...,
    ]
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    observability_version: str
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
            "observability_status": self.observability_status,
            "snapshot": (
                self.snapshot.to_dict()
                if self.snapshot is not None
                else None
            ),
            "signals": [
                signal.to_dict()
                for signal in self.signals
            ],
            "diagnostic_codes": list(
                self.diagnostic_codes
            ),
            "validation_errors": list(
                self.validation_errors
            ),
            "observability_version": self.observability_version,
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
            "production_authority": self.production_authority,
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


def _signal(
    signal_id: str,
    signal_group: str,
    signal_name: str,
    *,
    passed: bool,
    triggered: bool,
    observed: Any,
    expected: Any,
    diagnostic_code: str | None,
) -> RetentionObservabilityHistoryQualityGateObservabilitySignal:
    return RetentionObservabilityHistoryQualityGateObservabilitySignal(
        signal_id=signal_id,
        signal_group=signal_group,
        signal_name=signal_name,
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


def _snapshot_id(
    *,
    observed_at_utc: str,
    quality_report: RetentionObservabilityHistoryQualityReport,
    observability_status: str,
    failed_dimension_count: int,
    triggered_dimension_count: int,
) -> str:
    digest = _sha256(
        {
            "observability_version": (
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_VERSION
            ),
            "observed_at_utc": observed_at_utc,
            "quality_gate_version": (
                quality_report.quality_gate_version
            ),
            "quality_report_id": (
                quality_report.quality_report_id
            ),
            "quality_status": (
                quality_report.quality_status
            ),
            "observability_status": observability_status,
            "failed_dimension_count": (
                failed_dimension_count
            ),
            "triggered_dimension_count": (
                triggered_dimension_count
            ),
        }
    )

    return (
        "matchup-shadow-retention-observability-history-"
        "quality-gate-observability-"
        + digest[:20]
    )


def observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
    quality_report: RetentionObservabilityHistoryQualityReport | None,
    *,
    enabled: bool = False,
    observed_at_utc: str,
) -> RetentionObservabilityHistoryQualityGateObservabilityReport:
    if not enabled:
        return RetentionObservabilityHistoryQualityGateObservabilityReport(
            emitted=False,
            reason=(
                "retention_observability_history_quality_gate_"
                "observability_disabled"
            ),
            observability_status="disabled",
            snapshot=None,
            signals=(),
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_disabled",
            ),
            validation_errors=(),
            observability_version=(
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_VERSION
            ),
        )

    if quality_report is None:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_report_missing"
        )

        return RetentionObservabilityHistoryQualityGateObservabilityReport(
            emitted=True,
            reason=(
                "retention_observability_history_quality_gate_"
                "observability_degraded"
            ),
            observability_status="degraded",
            snapshot=None,
            signals=(),
            diagnostic_codes=(code,),
            validation_errors=(code,),
            observability_version=(
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_VERSION
            ),
        )

    dimensions = tuple(
        quality_report.dimensions
    )

    dimension_ids = [
        dimension.dimension_id
        for dimension in dimensions
    ]

    dimension_ids_present = all(
        bool(dimension_id)
        for dimension_id in dimension_ids
    )

    dimension_ids_unique = (
        len(dimension_ids)
        == len(set(dimension_ids))
    )

    failed_dimension_count = sum(
        dimension.passed is False
        for dimension in dimensions
    )

    triggered_dimension_count = sum(
        dimension.triggered is True
        for dimension in dimensions
    )

    quality_report_id_present = bool(
        quality_report.quality_report_id
    )

    quality_gate_version_present = bool(
        quality_report.quality_gate_version
    )

    quality_status_supported = (
        quality_report.quality_status
        in SUPPORTED_QUALITY_STATUSES
    )

    nonnegative_counts = all(
        value >= 0
        for value in (
            quality_report.history_record_count,
            quality_report.healthy_record_count,
            quality_report.warning_record_count,
            quality_report.degraded_record_count,
            quality_report.empty_record_count,
            quality_report.exact_duplicate_count,
            quality_report.conflicting_duplicate_count,
            failed_dimension_count,
            triggered_dimension_count,
        )
    )

    recomputed_status_total = sum(
        (
            quality_report.healthy_record_count,
            quality_report.warning_record_count,
            quality_report.degraded_record_count,
            quality_report.empty_record_count,
        )
    )

    status_counts_reconcile = (
        quality_report.status_counts_reconcile
        and recomputed_status_total
        == quality_report.history_record_count
    )

    production_authority_absent = all(
        value is False
        for value in (
            quality_report.production_authority,
            quality_report.history_mutated,
            quality_report.retention_action_executed,
            quality_report.physical_deletion_executed,
            quality_report.production_behavior_changed,
            quality_report.simulation_behavior_changed,
            quality_report.historical_outcomes_joined,
            quality_report.predictive_evaluation_executed,
        )
    )

    diagnostics = list(
        quality_report.diagnostic_codes
    )
    validation_errors = list(
        quality_report.validation_errors
    )

    if not quality_report_id_present:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_report_id_missing"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if not quality_gate_version_present:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_version_missing"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if not quality_status_supported:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_status_unsupported"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if (
        not dimension_ids_present
        or not dimension_ids_unique
    ):
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_dimension_conflict"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if not nonnegative_counts:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_negative_count"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if not status_counts_reconcile:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_count_mismatch"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if not production_authority_absent:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_authority_violation"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    hard_failure = any(
        (
            quality_report.quality_status == "failed",
            bool(quality_report.validation_errors),
            failed_dimension_count > 0,
            not quality_report.history_digest_reconciles,
            not quality_report.history_record_ids_unique,
            not quality_report.history_order_reconciles,
            not quality_report.source_payload_digests_present,
            not status_counts_reconcile,
            not quality_report_id_present,
            not quality_gate_version_present,
            not quality_status_supported,
            not dimension_ids_present,
            not dimension_ids_unique,
            not nonnegative_counts,
            not production_authority_absent,
        )
    )

    if quality_report.quality_status == "empty":
        observability_status = "empty"
        reason = (
            "retention_observability_history_quality_gate_"
            "observability_empty"
        )
        diagnostics.append(
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_empty"
        )
    elif hard_failure:
        observability_status = "degraded"
        reason = (
            "retention_observability_history_quality_gate_"
            "observability_degraded"
        )
    elif (
        quality_report.quality_status
        == "passed_with_warnings"
        or triggered_dimension_count > 0
        or quality_report.warning_record_count > 0
        or quality_report.exact_duplicate_count > 0
    ):
        observability_status = "warning"
        reason = (
            "retention_observability_history_quality_gate_"
            "observability_warning"
        )
    else:
        observability_status = "healthy"
        reason = (
            "retention_observability_history_quality_gate_"
            "observability_healthy"
        )

    signals = (
        _signal(
            "HQO-S01",
            "quality_status_integrity",
            "quality_status_supported",
            passed=quality_status_supported,
            triggered=not quality_status_supported,
            observed=quality_report.quality_status,
            expected=sorted(
                SUPPORTED_QUALITY_STATUSES
            ),
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_status_unsupported"
                if not quality_status_supported
                else None
            ),
        ),
        _signal(
            "HQO-S02",
            "dimension_integrity",
            "dimension_ids_unique",
            passed=(
                dimension_ids_present
                and dimension_ids_unique
            ),
            triggered=(
                not dimension_ids_present
                or not dimension_ids_unique
                or triggered_dimension_count > 0
            ),
            observed={
                "present": dimension_ids_present,
                "unique": dimension_ids_unique,
                "failed": failed_dimension_count,
                "triggered": triggered_dimension_count,
            },
            expected={
                "present": True,
                "unique": True,
                "failed": 0,
            },
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_dimension_conflict"
                if (
                    not dimension_ids_present
                    or not dimension_ids_unique
                )
                else None
            ),
        ),
        _signal(
            "HQO-S03",
            "history_digest_integrity",
            "history_digest_reconciles",
            passed=quality_report.history_digest_reconciles,
            triggered=(
                not quality_report.history_digest_reconciles
            ),
            observed=(
                quality_report.history_digest_reconciles
            ),
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_digest_failure"
                if not quality_report.history_digest_reconciles
                else None
            ),
        ),
        _signal(
            "HQO-S04",
            "history_identity_integrity",
            "history_record_ids_unique",
            passed=quality_report.history_record_ids_unique,
            triggered=(
                not quality_report.history_record_ids_unique
            ),
            observed=(
                quality_report.history_record_ids_unique
            ),
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_identity_failure"
                if not quality_report.history_record_ids_unique
                else None
            ),
        ),
        _signal(
            "HQO-S05",
            "history_order_integrity",
            "history_order_reconciles",
            passed=quality_report.history_order_reconciles,
            triggered=(
                not quality_report.history_order_reconciles
            ),
            observed=(
                quality_report.history_order_reconciles
            ),
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_order_failure"
                if not quality_report.history_order_reconciles
                else None
            ),
        ),
        _signal(
            "HQO-S06",
            "source_digest_integrity",
            "source_payload_digests_present",
            passed=(
                quality_report.source_payload_digests_present
            ),
            triggered=(
                not quality_report.source_payload_digests_present
            ),
            observed=(
                quality_report.source_payload_digests_present
            ),
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_source_digest_failure"
                if not quality_report.source_payload_digests_present
                else None
            ),
        ),
        _signal(
            "HQO-S07",
            "status_count_integrity",
            "status_counts_reconcile",
            passed=status_counts_reconcile,
            triggered=not status_counts_reconcile,
            observed={
                "reported": (
                    quality_report.status_counts_reconcile
                ),
                "recomputed_total": recomputed_status_total,
                "history_record_count": (
                    quality_report.history_record_count
                ),
            },
            expected={
                "reported": True,
                "recomputed_total_equals_history_record_count": True,
            },
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_count_mismatch"
                if not status_counts_reconcile
                else None
            ),
        ),
        _signal(
            "HQO-S08",
            "authority_boundary",
            "production_authority_absent",
            passed=production_authority_absent,
            triggered=not production_authority_absent,
            observed=production_authority_absent,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_authority_violation"
                if not production_authority_absent
                else None
            ),
        ),
    )

    snapshot_id = _snapshot_id(
        observed_at_utc=observed_at_utc,
        quality_report=quality_report,
        observability_status=observability_status,
        failed_dimension_count=failed_dimension_count,
        triggered_dimension_count=triggered_dimension_count,
    )

    snapshot = (
        RetentionObservabilityHistoryQualityGateObservabilitySnapshot(
            observability_snapshot_id=snapshot_id,
            observability_version=(
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_VERSION
            ),
            observed_at_utc=observed_at_utc,
            quality_gate_version=(
                quality_report.quality_gate_version
            ),
            quality_report_id=(
                quality_report.quality_report_id
                or ""
            ),
            quality_status=quality_report.quality_status,
            observability_status=observability_status,
            history_record_count=(
                quality_report.history_record_count
            ),
            healthy_record_count=(
                quality_report.healthy_record_count
            ),
            warning_record_count=(
                quality_report.warning_record_count
            ),
            degraded_record_count=(
                quality_report.degraded_record_count
            ),
            empty_record_count=(
                quality_report.empty_record_count
            ),
            exact_duplicate_count=(
                quality_report.exact_duplicate_count
            ),
            conflicting_duplicate_count=(
                quality_report.conflicting_duplicate_count
            ),
            failed_dimension_count=(
                failed_dimension_count
            ),
            triggered_dimension_count=(
                triggered_dimension_count
            ),
            history_digest_reconciles=(
                quality_report.history_digest_reconciles
            ),
            history_record_ids_unique=(
                quality_report.history_record_ids_unique
            ),
            history_order_reconciles=(
                quality_report.history_order_reconciles
            ),
            source_payload_digests_present=(
                quality_report.source_payload_digests_present
            ),
            status_counts_reconcile=(
                status_counts_reconcile
            ),
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
        )
    )

    return RetentionObservabilityHistoryQualityGateObservabilityReport(
        emitted=True,
        reason=reason,
        observability_status=observability_status,
        snapshot=snapshot,
        signals=signals,
        diagnostic_codes=(
            snapshot.diagnostic_codes
        ),
        validation_errors=(
            snapshot.validation_errors
        ),
        observability_version=(
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_VERSION
        ),
    )
