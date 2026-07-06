"""
Immutable append-only history for Layer 8W retention observability.

This module records deterministic history entries from retention-observability
reports and snapshots. Existing records remain immutable. Exact duplicates are
idempotent, while conflicting identities are rejected.

It does not execute retention actions, delete or mutate records, alter
observability results, join outcomes, perform predictive evaluation, or change
production or simulation behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability import (
    RetentionObservabilityReport,
    RetentionObservabilitySnapshot,
)


RETENTION_OBSERVABILITY_HISTORY_VERSION = "8Y-v1"

SUPPORTED_HISTORY_STATUSES = frozenset(
    {
        "appended",
        "idempotent",
        "conflicted",
        "empty",
        "disabled",
    }
)


@dataclass(frozen=True)
class RetentionObservabilityHistoryRecord:
    history_record_id: str
    history_version: str
    recorded_at_utc: str
    retention_observability_snapshot_id: str
    retention_observability_version: str
    observed_at_utc: str
    retention_version: str
    retention_status: str
    observability_status: str
    decision_count: int
    retained_count: int
    archived_count: int
    expired_count: int
    quarantined_count: int
    exact_duplicate_count: int
    conflicting_duplicate_count: int
    ledger_digest_reconciles: bool
    decision_identifiers_unique: bool
    policy_windows_reconcile: bool
    snapshot_payload_digest: str
    report_payload_digest: str
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
class RetentionObservabilityHistoryDuplicate:
    history_record_id: str
    duplicate_count: int
    conflict: bool
    diagnostic_code: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RetentionObservabilityHistoryLedger:
    emitted: bool
    reason: str
    history_status: str
    records: tuple[
        RetentionObservabilityHistoryRecord,
        ...,
    ]
    duplicates: tuple[
        RetentionObservabilityHistoryDuplicate,
        ...,
    ]
    history_digest: str | None
    recorded_at_utc: str
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    history_version: str
    append_only: bool = True
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
            "append_only": self.append_only,
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


def retention_observability_snapshot_digest(
    snapshot: RetentionObservabilitySnapshot,
) -> str:
    return _sha256(
        snapshot.to_dict()
    )


def retention_observability_report_digest(
    report: RetentionObservabilityReport,
) -> str:
    return _sha256(
        report.to_dict()
    )


def retention_observability_history_record_id(
    *,
    snapshot: RetentionObservabilitySnapshot,
    snapshot_payload_digest: str,
    report_payload_digest: str,
) -> str:
    digest = _sha256(
        {
            "history_version": (
                RETENTION_OBSERVABILITY_HISTORY_VERSION
            ),
            "retention_observability_snapshot_id": (
                snapshot.retention_observability_snapshot_id
            ),
            "snapshot_payload_digest": (
                snapshot_payload_digest
            ),
            "report_payload_digest": (
                report_payload_digest
            ),
        }
    )

    return (
        "matchup-shadow-retention-observability-history-"
        + digest[:20]
    )


def retention_observability_history_digest(
    records: Iterable[
        RetentionObservabilityHistoryRecord
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


def _record_payload_without_recorded_at(
    record: RetentionObservabilityHistoryRecord,
) -> dict[str, Any]:
    payload = record.to_dict()
    payload.pop(
        "recorded_at_utc",
        None,
    )
    return payload


def _snapshot_counts_reconcile(
    snapshot: RetentionObservabilitySnapshot,
) -> bool:
    counts = (
        snapshot.retained_count,
        snapshot.archived_count,
        snapshot.expired_count,
        snapshot.quarantined_count,
    )

    return (
        all(
            isinstance(value, int)
            and value >= 0
            for value in counts
        )
        and isinstance(
            snapshot.decision_count,
            int,
        )
        and snapshot.decision_count >= 0
        and sum(counts)
        == snapshot.decision_count
    )


def _history_record_from_report(
    report: RetentionObservabilityReport,
    *,
    recorded_at_utc: str,
) -> RetentionObservabilityHistoryRecord:
    snapshot = report.snapshot

    if snapshot is None:
        raise ValueError(
            "retention observability snapshot is required"
        )

    snapshot_payload_digest = (
        retention_observability_snapshot_digest(
            snapshot
        )
    )

    report_payload_digest = (
        retention_observability_report_digest(
            report
        )
    )

    history_record_id = (
        retention_observability_history_record_id(
            snapshot=snapshot,
            snapshot_payload_digest=(
                snapshot_payload_digest
            ),
            report_payload_digest=(
                report_payload_digest
            ),
        )
    )

    return RetentionObservabilityHistoryRecord(
        history_record_id=history_record_id,
        history_version=(
            RETENTION_OBSERVABILITY_HISTORY_VERSION
        ),
        recorded_at_utc=recorded_at_utc,
        retention_observability_snapshot_id=(
            snapshot.retention_observability_snapshot_id
        ),
        retention_observability_version=(
            snapshot.retention_observability_version
        ),
        observed_at_utc=snapshot.observed_at_utc,
        retention_version=snapshot.retention_version,
        retention_status=snapshot.retention_status,
        observability_status=(
            snapshot.observability_status
        ),
        decision_count=snapshot.decision_count,
        retained_count=snapshot.retained_count,
        archived_count=snapshot.archived_count,
        expired_count=snapshot.expired_count,
        quarantined_count=(
            snapshot.quarantined_count
        ),
        exact_duplicate_count=(
            snapshot.exact_duplicate_count
        ),
        conflicting_duplicate_count=(
            snapshot.conflicting_duplicate_count
        ),
        ledger_digest_reconciles=(
            snapshot.ledger_digest_reconciles
        ),
        decision_identifiers_unique=(
            snapshot.decision_identifiers_unique
        ),
        policy_windows_reconcile=(
            snapshot.policy_windows_reconcile
        ),
        snapshot_payload_digest=(
            snapshot_payload_digest
        ),
        report_payload_digest=(
            report_payload_digest
        ),
        diagnostic_codes=(
            _sorted_unique_strings(
                snapshot.diagnostic_codes
            )
        ),
        validation_errors=(
            _sorted_unique_strings(
                snapshot.validation_errors
            )
        ),
    )


def record_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history(
    report: RetentionObservabilityReport | None,
    *,
    enabled: bool = False,
    recorded_at_utc: str,
    existing_records: Iterable[
        RetentionObservabilityHistoryRecord
    ] = (),
) -> RetentionObservabilityHistoryLedger:
    original_records = tuple(
        existing_records
    )

    ordered_existing = tuple(
        sorted(
            original_records,
            key=lambda record: (
                record.observed_at_utc,
                record.history_record_id,
            ),
        )
    )

    if not enabled:
        return RetentionObservabilityHistoryLedger(
            emitted=False,
            reason="retention_observability_history_disabled",
            history_status="disabled",
            records=ordered_existing,
            duplicates=(),
            history_digest=(
                retention_observability_history_digest(
                    ordered_existing
                )
                if ordered_existing
                else None
            ),
            recorded_at_utc=recorded_at_utc,
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_disabled",
            ),
            validation_errors=(),
            history_version=(
                RETENTION_OBSERVABILITY_HISTORY_VERSION
            ),
        )

    if report is None:
        return RetentionObservabilityHistoryLedger(
            emitted=True,
            reason="retention_observability_history_conflicted",
            history_status="conflicted",
            records=ordered_existing,
            duplicates=(),
            history_digest=(
                retention_observability_history_digest(
                    ordered_existing
                )
                if ordered_existing
                else None
            ),
            recorded_at_utc=recorded_at_utc,
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_report_missing",
            ),
            validation_errors=(
                "matchup_shadow_retention_observability_history_report_missing",
            ),
            history_version=(
                RETENTION_OBSERVABILITY_HISTORY_VERSION
            ),
        )

    if (
        report.emitted
        and report.snapshot is None
    ):
        return RetentionObservabilityHistoryLedger(
            emitted=True,
            reason="retention_observability_history_conflicted",
            history_status="conflicted",
            records=ordered_existing,
            duplicates=(),
            history_digest=(
                retention_observability_history_digest(
                    ordered_existing
                )
                if ordered_existing
                else None
            ),
            recorded_at_utc=recorded_at_utc,
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_snapshot_missing",
            ),
            validation_errors=(
                "matchup_shadow_retention_observability_history_snapshot_missing",
            ),
            history_version=(
                RETENTION_OBSERVABILITY_HISTORY_VERSION
            ),
        )

    if not report.emitted:
        return RetentionObservabilityHistoryLedger(
            emitted=True,
            reason="retention_observability_history_empty",
            history_status="empty",
            records=ordered_existing,
            duplicates=(),
            history_digest=(
                retention_observability_history_digest(
                    ordered_existing
                )
                if ordered_existing
                else None
            ),
            recorded_at_utc=recorded_at_utc,
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_empty",
            ),
            validation_errors=(),
            history_version=(
                RETENTION_OBSERVABILITY_HISTORY_VERSION
            ),
        )

    snapshot = report.snapshot

    if snapshot is None:
        raise RuntimeError(
            "snapshot validation did not terminate"
        )

    if not _snapshot_counts_reconcile(
        snapshot
    ):
        return RetentionObservabilityHistoryLedger(
            emitted=True,
            reason="retention_observability_history_conflicted",
            history_status="conflicted",
            records=ordered_existing,
            duplicates=(),
            history_digest=(
                retention_observability_history_digest(
                    ordered_existing
                )
                if ordered_existing
                else None
            ),
            recorded_at_utc=recorded_at_utc,
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_count_mismatch",
            ),
            validation_errors=(
                "matchup_shadow_retention_observability_history_count_mismatch",
            ),
            history_version=(
                RETENTION_OBSERVABILITY_HISTORY_VERSION
            ),
        )

    candidate = _history_record_from_report(
        report,
        recorded_at_utc=recorded_at_utc,
    )

    matching = [
        record
        for record in ordered_existing
        if record.history_record_id
        == candidate.history_record_id
    ]

    if matching:
        exact = all(
            _record_payload_without_recorded_at(
                record
            )
            == _record_payload_without_recorded_at(
                candidate
            )
            for record in matching
        )

        if exact:
            duplicate = (
                RetentionObservabilityHistoryDuplicate(
                    history_record_id=(
                        candidate.history_record_id
                    ),
                    duplicate_count=len(matching),
                    conflict=False,
                    diagnostic_code=(
                        "matchup_shadow_retention_observability_history_exact_duplicate"
                    ),
                )
            )

            return RetentionObservabilityHistoryLedger(
                emitted=True,
                reason="retention_observability_history_idempotent",
                history_status="idempotent",
                records=ordered_existing,
                duplicates=(duplicate,),
                history_digest=(
                    retention_observability_history_digest(
                        ordered_existing
                    )
                ),
                recorded_at_utc=recorded_at_utc,
                diagnostic_codes=(
                    "matchup_shadow_retention_observability_history_exact_duplicate",
                ),
                validation_errors=(),
                history_version=(
                    RETENTION_OBSERVABILITY_HISTORY_VERSION
                ),
            )

        duplicate = (
            RetentionObservabilityHistoryDuplicate(
                history_record_id=(
                    candidate.history_record_id
                ),
                duplicate_count=len(matching),
                conflict=True,
                diagnostic_code=(
                    "matchup_shadow_retention_observability_history_identity_conflict"
                ),
            )
        )

        return RetentionObservabilityHistoryLedger(
            emitted=True,
            reason="retention_observability_history_conflicted",
            history_status="conflicted",
            records=ordered_existing,
            duplicates=(duplicate,),
            history_digest=(
                retention_observability_history_digest(
                    ordered_existing
                )
            ),
            recorded_at_utc=recorded_at_utc,
            diagnostic_codes=(
                "matchup_shadow_retention_observability_history_identity_conflict",
            ),
            validation_errors=(
                "matchup_shadow_retention_observability_history_identity_conflict",
            ),
            history_version=(
                RETENTION_OBSERVABILITY_HISTORY_VERSION
            ),
        )

    resolved_records = tuple(
        sorted(
            (
                *ordered_existing,
                candidate,
            ),
            key=lambda record: (
                record.observed_at_utc,
                record.history_record_id,
            ),
        )
    )

    return RetentionObservabilityHistoryLedger(
        emitted=True,
        reason="retention_observability_history_appended",
        history_status="appended",
        records=resolved_records,
        duplicates=(),
        history_digest=(
            retention_observability_history_digest(
                resolved_records
            )
        ),
        recorded_at_utc=recorded_at_utc,
        diagnostic_codes=(),
        validation_errors=(),
        history_version=(
            RETENTION_OBSERVABILITY_HISTORY_VERSION
        ),
    )
