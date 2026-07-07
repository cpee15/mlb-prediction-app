"""
Immutable append-only history for Layer 8AC quality-gate-observability
snapshots.

The history contract provides deterministic payload digests, record identities,
record ordering, ledger digests, exact-duplicate idempotency, and conflicting
duplicate rejection.

It does not mutate source snapshots or caller history, execute retention
actions, delete records, join outcomes, perform predictive evaluation, or
change production or simulation behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability import (
    RetentionObservabilityHistoryQualityGateObservabilitySnapshot,
)


RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION = (
    "8AE-v1"
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

SUPPORTED_OBSERVABILITY_STATUSES = frozenset(
    {
        "healthy",
        "warning",
        "degraded",
        "empty",
        "disabled",
    }
)


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityGateObservabilityHistoryRecord:
    history_record_id: str
    history_version: str
    recorded_at_utc: str
    observability_snapshot_id: str
    observability_version: str
    observed_at_utc: str
    quality_gate_version: str
    quality_report_id: str
    quality_status: str
    observability_status: str
    history_record_count: int
    warning_record_count: int
    degraded_record_count: int
    exact_duplicate_count: int
    conflicting_duplicate_count: int
    failed_dimension_count: int
    triggered_dimension_count: int
    history_digest_reconciles: bool
    history_record_ids_unique: bool
    history_order_reconciles: bool
    source_payload_digests_present: bool
    status_counts_reconcile: bool
    snapshot_payload_digest: str
    production_authority: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityGateObservabilityHistoryDuplicate:
    history_record_id: str
    duplicate_count: int
    conflict: bool
    diagnostic_code: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RetentionObservabilityHistoryQualityGateObservabilityHistoryLedger:
    emitted: bool
    reason: str
    history_status: str
    records: tuple[
        RetentionObservabilityHistoryQualityGateObservabilityHistoryRecord,
        ...,
    ]
    duplicates: tuple[
        RetentionObservabilityHistoryQualityGateObservabilityHistoryDuplicate,
        ...,
    ]
    history_digest: str | None
    recorded_at_utc: str
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    history_version: str
    source_snapshot_mutated: bool = False
    history_mutated: bool = False
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
            "history_status": self.history_status,
            "records": [
                record.to_dict()
                for record in self.records
            ],
            "duplicates": [
                duplicate.to_dict()
                for duplicate in self.duplicates
            ],
            "history_digest": self.history_digest,
            "recorded_at_utc": self.recorded_at_utc,
            "diagnostic_codes": list(
                self.diagnostic_codes
            ),
            "validation_errors": list(
                self.validation_errors
            ),
            "history_version": self.history_version,
            "source_snapshot_mutated": (
                self.source_snapshot_mutated
            ),
            "history_mutated": self.history_mutated,
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


def observability_snapshot_payload_digest(
    snapshot: RetentionObservabilityHistoryQualityGateObservabilitySnapshot,
) -> str:
    return _sha256(
        snapshot.to_dict()
    )


def observability_history_record_id(
    snapshot: RetentionObservabilityHistoryQualityGateObservabilitySnapshot,
) -> str:
    digest = _sha256(
        {
            "history_version": (
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION
            ),
            "observability_snapshot_id": (
                snapshot.observability_snapshot_id
            ),
            "observability_version": (
                snapshot.observability_version
            ),
            "observed_at_utc": snapshot.observed_at_utc,
            "quality_report_id": snapshot.quality_report_id,
            "snapshot_payload_digest": (
                observability_snapshot_payload_digest(
                    snapshot
                )
            ),
        }
    )

    return (
        "matchup-shadow-retention-observability-history-"
        "quality-gate-observability-history-"
        + digest[:20]
    )


def observability_history_digest(
    records: Iterable[
        RetentionObservabilityHistoryQualityGateObservabilityHistoryRecord
    ],
) -> str:
    ordered = sorted(
        records,
        key=lambda record: (
            record.observed_at_utc,
            record.history_record_id,
        ),
    )

    return _sha256(
        [
            record.to_dict()
            for record in ordered
        ]
    )


def _record_from_snapshot(
    snapshot: RetentionObservabilityHistoryQualityGateObservabilitySnapshot,
    *,
    recorded_at_utc: str,
) -> RetentionObservabilityHistoryQualityGateObservabilityHistoryRecord:
    return RetentionObservabilityHistoryQualityGateObservabilityHistoryRecord(
        history_record_id=(
            observability_history_record_id(
                snapshot
            )
        ),
        history_version=(
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION
        ),
        recorded_at_utc=recorded_at_utc,
        observability_snapshot_id=(
            snapshot.observability_snapshot_id
        ),
        observability_version=(
            snapshot.observability_version
        ),
        observed_at_utc=snapshot.observed_at_utc,
        quality_gate_version=(
            snapshot.quality_gate_version
        ),
        quality_report_id=(
            snapshot.quality_report_id
        ),
        quality_status=snapshot.quality_status,
        observability_status=(
            snapshot.observability_status
        ),
        history_record_count=(
            snapshot.history_record_count
        ),
        warning_record_count=(
            snapshot.warning_record_count
        ),
        degraded_record_count=(
            snapshot.degraded_record_count
        ),
        exact_duplicate_count=(
            snapshot.exact_duplicate_count
        ),
        conflicting_duplicate_count=(
            snapshot.conflicting_duplicate_count
        ),
        failed_dimension_count=(
            snapshot.failed_dimension_count
        ),
        triggered_dimension_count=(
            snapshot.triggered_dimension_count
        ),
        history_digest_reconciles=(
            snapshot.history_digest_reconciles
        ),
        history_record_ids_unique=(
            snapshot.history_record_ids_unique
        ),
        history_order_reconciles=(
            snapshot.history_order_reconciles
        ),
        source_payload_digests_present=(
            snapshot.source_payload_digests_present
        ),
        status_counts_reconcile=(
            snapshot.status_counts_reconcile
        ),
        snapshot_payload_digest=(
            observability_snapshot_payload_digest(
                snapshot
            )
        ),
    )


def append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
    snapshot: RetentionObservabilityHistoryQualityGateObservabilitySnapshot | None,
    *,
    existing_records: Iterable[
        RetentionObservabilityHistoryQualityGateObservabilityHistoryRecord
    ] = (),
    enabled: bool = False,
    recorded_at_utc: str,
) -> RetentionObservabilityHistoryQualityGateObservabilityHistoryLedger:
    caller_records = tuple(
        existing_records
    )

    if not enabled:
        return RetentionObservabilityHistoryQualityGateObservabilityHistoryLedger(
            emitted=False,
            reason=(
                "retention_observability_history_quality_gate_"
                "observability_history_disabled"
            ),
            history_status="disabled",
            records=caller_records,
            duplicates=(),
            history_digest=None,
            recorded_at_utc=recorded_at_utc,
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_history_disabled",
            ),
            validation_errors=(),
            history_version=(
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION
            ),
        )

    if snapshot is None:
        digest = observability_history_digest(
            caller_records
        )

        return RetentionObservabilityHistoryQualityGateObservabilityHistoryLedger(
            emitted=True,
            reason=(
                "retention_observability_history_quality_gate_"
                "observability_history_empty"
            ),
            history_status="empty",
            records=tuple(
                sorted(
                    caller_records,
                    key=lambda record: (
                        record.observed_at_utc,
                        record.history_record_id,
                    ),
                )
            ),
            duplicates=(),
            history_digest=digest,
            recorded_at_utc=recorded_at_utc,
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_history_snapshot_missing",
            ),
            validation_errors=(),
            history_version=(
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION
            ),
        )

    diagnostics: list[str] = []
    validation_errors: list[str] = []

    if not snapshot.observability_snapshot_id:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_snapshot_id_missing"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if not snapshot.observability_version:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_observability_version_missing"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if not snapshot.observed_at_utc:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_observed_at_missing"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if not snapshot.quality_report_id:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_report_id_missing"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if (
        snapshot.quality_status
        not in SUPPORTED_QUALITY_STATUSES
    ):
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_quality_status_unsupported"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if (
        snapshot.observability_status
        not in SUPPORTED_OBSERVABILITY_STATUSES
    ):
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_status_unsupported"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    counts_nonnegative = all(
        value >= 0
        for value in (
            snapshot.history_record_count,
            snapshot.warning_record_count,
            snapshot.degraded_record_count,
            snapshot.exact_duplicate_count,
            snapshot.conflicting_duplicate_count,
            snapshot.failed_dimension_count,
            snapshot.triggered_dimension_count,
        )
    )

    if not counts_nonnegative:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_negative_count"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    if snapshot.production_authority:
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_authority_violation"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    new_record = _record_from_snapshot(
        snapshot,
        recorded_at_utc=recorded_at_utc,
    )

    if validation_errors:
        ordered_records = tuple(
            sorted(
                caller_records,
                key=lambda record: (
                    record.observed_at_utc,
                    record.history_record_id,
                ),
            )
        )

        return RetentionObservabilityHistoryQualityGateObservabilityHistoryLedger(
            emitted=True,
            reason=(
                "retention_observability_history_quality_gate_"
                "observability_history_rejected"
            ),
            history_status="conflict",
            records=ordered_records,
            duplicates=(),
            history_digest=(
                observability_history_digest(
                    ordered_records
                )
            ),
            recorded_at_utc=recorded_at_utc,
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
            history_version=(
                RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION
            ),
        )

    same_identity_records = tuple(
        record
        for record in caller_records
        if record.history_record_id
        == new_record.history_record_id
    )

    duplicates: list[
        RetentionObservabilityHistoryQualityGateObservabilityHistoryDuplicate
    ] = []

    history_status = "appended"
    reason = (
        "retention_observability_history_quality_gate_"
        "observability_history_appended"
    )

    output_records = caller_records

    if same_identity_records:
        payloads_equal = all(
            record.to_dict()
            == new_record.to_dict()
            for record in same_identity_records
        )

        if payloads_equal:
            history_status = "exact_duplicate"
            reason = (
                "retention_observability_history_quality_gate_"
                "observability_history_exact_duplicate"
            )
            code = (
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_history_exact_duplicate"
            )
            diagnostics.append(code)
            duplicates.append(
                RetentionObservabilityHistoryQualityGateObservabilityHistoryDuplicate(
                    history_record_id=(
                        new_record.history_record_id
                    ),
                    duplicate_count=len(
                        same_identity_records
                    ),
                    conflict=False,
                    diagnostic_code=code,
                )
            )
        else:
            history_status = "conflict"
            reason = (
                "retention_observability_history_quality_gate_"
                "observability_history_conflict"
            )
            code = (
                "matchup_shadow_retention_observability_history_"
                "quality_gate_observability_history_identity_conflict"
            )
            diagnostics.append(code)
            validation_errors.append(code)
            duplicates.append(
                RetentionObservabilityHistoryQualityGateObservabilityHistoryDuplicate(
                    history_record_id=(
                        new_record.history_record_id
                    ),
                    duplicate_count=len(
                        same_identity_records
                    ),
                    conflict=True,
                    diagnostic_code=code,
                )
            )
    else:
        output_records = (
            *caller_records,
            new_record,
        )

    ordered_records = tuple(
        sorted(
            output_records,
            key=lambda record: (
                record.observed_at_utc,
                record.history_record_id,
            ),
        )
    )

    record_ids = [
        record.history_record_id
        for record in ordered_records
    ]

    if len(record_ids) != len(set(record_ids)):
        code = (
            "matchup_shadow_retention_observability_history_"
            "quality_gate_observability_history_identity_conflict"
        )
        diagnostics.append(code)
        validation_errors.append(code)

    digest = observability_history_digest(
        ordered_records
    )

    return RetentionObservabilityHistoryQualityGateObservabilityHistoryLedger(
        emitted=True,
        reason=reason,
        history_status=history_status,
        records=ordered_records,
        duplicates=tuple(duplicates),
        history_digest=digest,
        recorded_at_utc=recorded_at_utc,
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
        history_version=(
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_VERSION
        ),
    )
