"""
Diagnostic observability for Layer 8U retention decisions and ledgers.

This module reconciles ledger digests, decision identities, status counts,
record-age distributions, duplicate integrity, quarantine integrity, and
retention-policy windows. It does not execute retention actions, delete or
mutate records, alter production or simulation behavior, join outcomes, or
perform predictive evaluation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from statistics import mean
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention import (
    MatchupOverlayShadowRetentionDecision,
    MatchupOverlayShadowRetentionLedger,
    retention_ledger_digest,
)


RETENTION_OBSERVABILITY_VERSION = "8W-v1"

SUPPORTED_RETENTION_OBSERVABILITY_STATUSES = frozenset(
    {
        "healthy",
        "warning",
        "degraded",
        "empty",
        "disabled",
    }
)


@dataclass(frozen=True)
class RetentionObservabilitySignal:
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
class RetentionObservabilitySnapshot:
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
    minimum_record_age_days: int | None
    mean_record_age_days: float | None
    maximum_record_age_days: int | None
    retention_window_days: int
    archive_window_days: int
    ledger_digest_reconciles: bool
    decision_identifiers_unique: bool
    policy_windows_reconcile: bool
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
class RetentionObservabilityReport:
    emitted: bool
    reason: str
    observability_status: str
    snapshot: RetentionObservabilitySnapshot | None
    signals: tuple[
        RetentionObservabilitySignal,
        ...,
    ]
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    retention_observability_version: str
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
            "observability_status": (
                self.observability_status
            ),
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
            "retention_observability_version": (
                self.retention_observability_version
            ),
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
) -> RetentionObservabilitySignal:
    return RetentionObservabilitySignal(
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


def _policy_reconciles(
    decision: MatchupOverlayShadowRetentionDecision,
) -> bool:
    if decision.quarantine_required:
        return (
            decision.retention_status
            == "quarantined"
            and not decision.eligible_for_retention
            and not decision.eligible_for_archive
            and not decision.eligible_for_expiration
        )

    if (
        decision.record_age_days
        <= decision.retention_window_days
    ):
        return (
            decision.retention_status
            == "retained"
            and decision.eligible_for_retention
            and not decision.eligible_for_archive
            and not decision.eligible_for_expiration
        )

    if (
        decision.record_age_days
        <= decision.archive_window_days
    ):
        return (
            decision.retention_status
            == "archived"
            and decision.eligible_for_archive
            and not decision.eligible_for_retention
            and not decision.eligible_for_expiration
        )

    return (
        decision.retention_status
        == "expired"
        and decision.eligible_for_expiration
        and not decision.eligible_for_retention
        and not decision.eligible_for_archive
    )


def _snapshot_identity(
    *,
    observed_at_utc: str,
    ledger: MatchupOverlayShadowRetentionLedger,
    observability_status: str,
    ledger_digest_reconciles: bool,
    decision_identifiers_unique: bool,
    policy_windows_reconcile: bool,
) -> str:
    digest = _sha256(
        {
            "retention_observability_version": (
                RETENTION_OBSERVABILITY_VERSION
            ),
            "observed_at_utc": observed_at_utc,
            "retention_version": (
                ledger.retention_version
            ),
            "retention_status": (
                ledger.retention_status
            ),
            "ledger_digest": (
                ledger.ledger_digest
            ),
            "observability_status": (
                observability_status
            ),
            "ledger_digest_reconciles": (
                ledger_digest_reconciles
            ),
            "decision_identifiers_unique": (
                decision_identifiers_unique
            ),
            "policy_windows_reconcile": (
                policy_windows_reconcile
            ),
        }
    )

    return (
        "matchup-shadow-retention-observability-"
        + digest[:20]
    )


def observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
    ledger: MatchupOverlayShadowRetentionLedger | None,
    *,
    enabled: bool = False,
    observed_at_utc: str,
) -> RetentionObservabilityReport:
    if not enabled:
        return RetentionObservabilityReport(
            emitted=False,
            reason="retention_observability_disabled",
            observability_status="disabled",
            snapshot=None,
            signals=(),
            diagnostic_codes=(
                "matchup_shadow_retention_observability_disabled",
            ),
            validation_errors=(),
            retention_observability_version=(
                RETENTION_OBSERVABILITY_VERSION
            ),
        )

    if ledger is None:
        return RetentionObservabilityReport(
            emitted=True,
            reason="retention_observability_degraded",
            observability_status="degraded",
            snapshot=None,
            signals=(),
            diagnostic_codes=(
                "matchup_shadow_retention_observability_ledger_missing",
            ),
            validation_errors=(
                "matchup_shadow_retention_observability_ledger_missing",
            ),
            retention_observability_version=(
                RETENTION_OBSERVABILITY_VERSION
            ),
        )

    decisions = tuple(ledger.decisions)
    decision_ids = [
        decision.retention_decision_id
        for decision in decisions
    ]

    decision_identifiers_unique = (
        len(decision_ids)
        == len(set(decision_ids))
    )

    expected_digest = retention_ledger_digest(
        decisions
    )

    ledger_digest_reconciles = (
        ledger.ledger_digest is not None
        and ledger.ledger_digest
        == expected_digest
    )

    policy_results = [
        _policy_reconciles(decision)
        for decision in decisions
    ]

    policy_windows_reconcile = all(
        policy_results
    )

    retained_count = sum(
        decision.retention_status
        == "retained"
        for decision in decisions
    )
    archived_count = sum(
        decision.retention_status
        == "archived"
        for decision in decisions
    )
    expired_count = sum(
        decision.retention_status
        == "expired"
        for decision in decisions
    )
    quarantined_count = sum(
        decision.retention_status
        == "quarantined"
        for decision in decisions
    )

    exact_duplicate_count = sum(
        duplicate.duplicate_count
        for duplicate in ledger.duplicates
        if not duplicate.conflict
    )

    conflicting_duplicate_count = sum(
        duplicate.duplicate_count
        for duplicate in ledger.duplicates
        if duplicate.conflict
    )

    ages = [
        decision.record_age_days
        for decision in decisions
    ]

    minimum_age = (
        min(ages)
        if ages
        else None
    )
    mean_age = (
        float(mean(ages))
        if ages
        else None
    )
    maximum_age = (
        max(ages)
        if ages
        else None
    )

    diagnostics: list[str] = []
    validation_errors: list[str] = []

    if ledger.ledger_digest is None:
        diagnostics.append(
            "matchup_shadow_retention_observability_digest_missing"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_digest_missing"
        )
    elif not ledger_digest_reconciles:
        diagnostics.append(
            "matchup_shadow_retention_observability_digest_mismatch"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_digest_mismatch"
        )

    if not decision_identifiers_unique:
        diagnostics.append(
            "matchup_shadow_retention_observability_identity_conflict"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_identity_conflict"
        )

    if not policy_windows_reconcile:
        diagnostics.append(
            "matchup_shadow_retention_observability_policy_mismatch"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_policy_mismatch"
        )

    if conflicting_duplicate_count:
        diagnostics.append(
            "matchup_shadow_retention_observability_conflict"
        )
        validation_errors.append(
            "matchup_shadow_retention_observability_conflict"
        )

    if exact_duplicate_count:
        diagnostics.append(
            "matchup_shadow_retention_observability_exact_duplicates"
        )

    if archived_count:
        diagnostics.append(
            "matchup_shadow_retention_observability_archived_records"
        )

    if expired_count:
        diagnostics.append(
            "matchup_shadow_retention_observability_expired_records"
        )

    if quarantined_count:
        diagnostics.append(
            "matchup_shadow_retention_observability_quarantined_records"
        )

    degraded = any(
        (
            not ledger_digest_reconciles,
            not decision_identifiers_unique,
            not policy_windows_reconcile,
            conflicting_duplicate_count > 0,
            quarantined_count > 0,
        )
    )

    if not decisions:
        observability_status = "empty"
        reason = "retention_observability_empty"
        diagnostics.append(
            "matchup_shadow_retention_observability_empty"
        )
    elif degraded:
        observability_status = "degraded"
        reason = "retention_observability_degraded"
    elif any(
        (
            exact_duplicate_count > 0,
            archived_count > 0,
            expired_count > 0,
        )
    ):
        observability_status = "warning"
        reason = "retention_observability_warning"
    else:
        observability_status = "healthy"
        reason = "retention_observability_healthy"

    signals = (
        _signal(
            "RO-S01",
            "ledger_integrity",
            "ledger_digest_present",
            passed=ledger.ledger_digest is not None,
            triggered=ledger.ledger_digest is None,
            observed=ledger.ledger_digest is not None,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_digest_missing"
                if ledger.ledger_digest is None
                else None
            ),
        ),
        _signal(
            "RO-S02",
            "ledger_integrity",
            "ledger_digest_reconciles",
            passed=ledger_digest_reconciles,
            triggered=not ledger_digest_reconciles,
            observed=ledger_digest_reconciles,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_digest_mismatch"
                if not ledger_digest_reconciles
                else None
            ),
        ),
        _signal(
            "RO-S03",
            "decision_identity",
            "decision_identifiers_unique",
            passed=decision_identifiers_unique,
            triggered=not decision_identifiers_unique,
            observed=decision_identifiers_unique,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_identity_conflict"
                if not decision_identifiers_unique
                else None
            ),
        ),
        _signal(
            "RO-S04",
            "policy_window_reconciliation",
            "policy_windows_reconcile",
            passed=policy_windows_reconcile,
            triggered=not policy_windows_reconcile,
            observed=policy_windows_reconcile,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_retention_observability_policy_mismatch"
                if not policy_windows_reconcile
                else None
            ),
        ),
        _signal(
            "RO-S05",
            "duplicate_integrity",
            "conflicting_duplicates_absent",
            passed=conflicting_duplicate_count == 0,
            triggered=conflicting_duplicate_count > 0,
            observed=conflicting_duplicate_count,
            expected=0,
            diagnostic_code=(
                "matchup_shadow_retention_observability_conflict"
                if conflicting_duplicate_count
                else None
            ),
        ),
        _signal(
            "RO-W01",
            "duplicate_integrity",
            "exact_duplicates_present",
            passed=True,
            triggered=exact_duplicate_count > 0,
            observed=exact_duplicate_count,
            expected=0,
            diagnostic_code=(
                "matchup_shadow_retention_observability_exact_duplicates"
                if exact_duplicate_count
                else None
            ),
        ),
        _signal(
            "RO-W02",
            "retention_status_distribution",
            "archived_or_expired_records_present",
            passed=True,
            triggered=(
                archived_count > 0
                or expired_count > 0
            ),
            observed={
                "archived": archived_count,
                "expired": expired_count,
            },
            expected={
                "archived": 0,
                "expired": 0,
            },
            diagnostic_code=(
                "matchup_shadow_retention_observability_nonactive_records"
                if archived_count
                or expired_count
                else None
            ),
        ),
        _signal(
            "RO-S06",
            "quarantine_integrity",
            "quarantined_records_absent",
            passed=quarantined_count == 0,
            triggered=quarantined_count > 0,
            observed=quarantined_count,
            expected=0,
            diagnostic_code=(
                "matchup_shadow_retention_observability_quarantined_records"
                if quarantined_count
                else None
            ),
        ),
        _signal(
            "RO-S07",
            "authority_boundary",
            "production_authority_false",
            passed=(
                ledger.production_authority
                is False
            ),
            triggered=(
                ledger.production_authority
                is not False
            ),
            observed=ledger.production_authority,
            expected=False,
            diagnostic_code=None,
        ),
    )

    snapshot = RetentionObservabilitySnapshot(
        retention_observability_snapshot_id=(
            _snapshot_identity(
                observed_at_utc=observed_at_utc,
                ledger=ledger,
                observability_status=(
                    observability_status
                ),
                ledger_digest_reconciles=(
                    ledger_digest_reconciles
                ),
                decision_identifiers_unique=(
                    decision_identifiers_unique
                ),
                policy_windows_reconcile=(
                    policy_windows_reconcile
                ),
            )
        ),
        retention_observability_version=(
            RETENTION_OBSERVABILITY_VERSION
        ),
        observed_at_utc=observed_at_utc,
        retention_version=ledger.retention_version,
        retention_status=ledger.retention_status,
        observability_status=observability_status,
        decision_count=len(decisions),
        retained_count=retained_count,
        archived_count=archived_count,
        expired_count=expired_count,
        quarantined_count=quarantined_count,
        exact_duplicate_count=(
            exact_duplicate_count
        ),
        conflicting_duplicate_count=(
            conflicting_duplicate_count
        ),
        minimum_record_age_days=minimum_age,
        mean_record_age_days=mean_age,
        maximum_record_age_days=maximum_age,
        retention_window_days=(
            ledger.retention_window_days
        ),
        archive_window_days=(
            ledger.archive_window_days
        ),
        ledger_digest_reconciles=(
            ledger_digest_reconciles
        ),
        decision_identifiers_unique=(
            decision_identifiers_unique
        ),
        policy_windows_reconcile=(
            policy_windows_reconcile
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

    return RetentionObservabilityReport(
        emitted=True,
        reason=reason,
        observability_status=(
            observability_status
        ),
        snapshot=snapshot,
        signals=signals,
        diagnostic_codes=(
            snapshot.diagnostic_codes
        ),
        validation_errors=(
            snapshot.validation_errors
        ),
        retention_observability_version=(
            RETENTION_OBSERVABILITY_VERSION
        ),
    )
