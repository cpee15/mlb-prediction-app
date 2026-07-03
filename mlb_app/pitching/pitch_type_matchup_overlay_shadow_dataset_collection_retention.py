"""
Deterministic retention decisions for matchup shadow-dataset collections.

This module creates an immutable, append-only diagnostic retention ledger.
It never physically deletes or mutates collection records and grants no
production, simulation, predictive-evaluation, tuning, pricing, or edge
authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection import (
    MatchupOverlayShadowCollection,
    MatchupOverlayShadowCollectionRecord,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_observability import (
    CollectionObservabilityReport,
)


RETENTION_VERSION = "8U-v1"

SUPPORTED_RETENTION_STATUSES = frozenset(
    {
        "retained",
        "archived",
        "expired",
        "quarantined",
        "disabled",
    }
)


@dataclass(frozen=True)
class MatchupOverlayShadowRetentionDecision:
    retention_decision_id: str
    retention_version: str
    evaluated_at_utc: str
    collection_record_id: str
    collection_version: str
    dataset_version: str
    quality_gate_version: str
    observability_version: str
    collection_status: str
    observability_status: str
    retention_status: str
    retention_reason: str
    record_age_days: int
    retention_window_days: int
    archive_window_days: int
    eligible_for_retention: bool
    eligible_for_archive: bool
    eligible_for_expiration: bool
    quarantine_required: bool
    dataset_payload_digest: str
    quality_report_digest: str
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
class MatchupOverlayShadowRetentionDuplicate:
    retention_decision_id: str
    duplicate_count: int
    conflict: bool
    diagnostic_code: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchupOverlayShadowRetentionLedger:
    emitted: bool
    reason: str
    retention_status: str
    decisions: tuple[
        MatchupOverlayShadowRetentionDecision,
        ...,
    ]
    duplicates: tuple[
        MatchupOverlayShadowRetentionDuplicate,
        ...,
    ]
    ledger_digest: str | None
    evaluated_at_utc: str
    retention_window_days: int
    archive_window_days: int
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    retention_version: str
    append_only: bool = True
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
            "retention_status": self.retention_status,
            "decisions": [
                decision.to_dict()
                for decision in self.decisions
            ],
            "duplicates": [
                duplicate.to_dict()
                for duplicate in self.duplicates
            ],
            "ledger_digest": self.ledger_digest,
            "evaluated_at_utc": self.evaluated_at_utc,
            "retention_window_days": (
                self.retention_window_days
            ),
            "archive_window_days": (
                self.archive_window_days
            ),
            "diagnostic_codes": list(
                self.diagnostic_codes
            ),
            "validation_errors": list(
                self.validation_errors
            ),
            "retention_version": (
                self.retention_version
            ),
            "append_only": self.append_only,
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


def _parse_utc(
    value: str,
) -> datetime:
    parsed = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=timezone.utc
        )

    return parsed.astimezone(timezone.utc)


def _record_age_days(
    *,
    collected_at_utc: str,
    evaluated_at_utc: str,
) -> int:
    collected_at = _parse_utc(
        collected_at_utc
    )
    evaluated_at = _parse_utc(
        evaluated_at_utc
    )

    age_seconds = (
        evaluated_at - collected_at
    ).total_seconds()

    if age_seconds < 0:
        raise ValueError(
            "record_age_days_must_be_nonnegative"
        )

    return int(
        age_seconds // 86400
    )


def _decision_identity(
    *,
    record: MatchupOverlayShadowCollectionRecord,
    observability_version: str,
    observability_status: str,
    retention_status: str,
    retention_window_days: int,
    archive_window_days: int,
) -> str:
    digest = _sha256(
        {
            "retention_version": (
                RETENTION_VERSION
            ),
            "collection_record_id": (
                record.collection_record_id
            ),
            "collection_version": (
                record.collection_version
            ),
            "observability_version": (
                observability_version
            ),
            "observability_status": (
                observability_status
            ),
            "retention_status": (
                retention_status
            ),
            "retention_window_days": (
                retention_window_days
            ),
            "archive_window_days": (
                archive_window_days
            ),
        }
    )

    return (
        "matchup-shadow-retention-"
        + digest[:20]
    )


def retention_ledger_digest(
    decisions: Iterable[
        MatchupOverlayShadowRetentionDecision
    ],
) -> str:
    return _sha256(
        [
            decision.to_dict()
            for decision in sorted(
                decisions,
                key=lambda item: (
                    item.retention_decision_id
                ),
            )
        ]
    )


def _fallback_ledger(
    *,
    reason: str,
    retention_status: str,
    evaluated_at_utc: str,
    retention_window_days: int,
    archive_window_days: int,
    diagnostic_code: str,
    emitted: bool,
) -> MatchupOverlayShadowRetentionLedger:
    return MatchupOverlayShadowRetentionLedger(
        emitted=emitted,
        reason=reason,
        retention_status=retention_status,
        decisions=(),
        duplicates=(),
        ledger_digest=(
            _sha256([])
            if emitted
            else None
        ),
        evaluated_at_utc=evaluated_at_utc,
        retention_window_days=(
            retention_window_days
        ),
        archive_window_days=(
            archive_window_days
        ),
        diagnostic_codes=(
            diagnostic_code,
        ),
        validation_errors=(
            ()
            if not emitted
            else (diagnostic_code,)
        ),
        retention_version=RETENTION_VERSION,
    )


def evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
    collection: MatchupOverlayShadowCollection | None,
    observability_report: CollectionObservabilityReport | None,
    *,
    enabled: bool = False,
    evaluated_at_utc: str,
    retention_window_days: int = 30,
    archive_window_days: int = 90,
    existing_decisions: Iterable[
        MatchupOverlayShadowRetentionDecision
    ] = (),
) -> MatchupOverlayShadowRetentionLedger:
    if not enabled:
        return _fallback_ledger(
            reason="retention_disabled",
            retention_status="disabled",
            evaluated_at_utc=evaluated_at_utc,
            retention_window_days=(
                retention_window_days
            ),
            archive_window_days=(
                archive_window_days
            ),
            diagnostic_code=(
                "matchup_shadow_retention_disabled"
            ),
            emitted=False,
        )

    if collection is None:
        return _fallback_ledger(
            reason="retention_quarantined",
            retention_status="quarantined",
            evaluated_at_utc=evaluated_at_utc,
            retention_window_days=(
                retention_window_days
            ),
            archive_window_days=(
                archive_window_days
            ),
            diagnostic_code=(
                "matchup_shadow_retention_collection_missing"
            ),
            emitted=True,
        )

    if observability_report is None:
        return _fallback_ledger(
            reason="retention_quarantined",
            retention_status="quarantined",
            evaluated_at_utc=evaluated_at_utc,
            retention_window_days=(
                retention_window_days
            ),
            archive_window_days=(
                archive_window_days
            ),
            diagnostic_code=(
                "matchup_shadow_retention_observability_missing"
            ),
            emitted=True,
        )

    policy_valid = (
        isinstance(retention_window_days, int)
        and isinstance(archive_window_days, int)
        and retention_window_days > 0
        and archive_window_days > 0
        and (
            archive_window_days
            > retention_window_days
        )
    )

    if not policy_valid:
        return _fallback_ledger(
            reason="retention_quarantined",
            retention_status="quarantined",
            evaluated_at_utc=evaluated_at_utc,
            retention_window_days=(
                retention_window_days
            ),
            archive_window_days=(
                archive_window_days
            ),
            diagnostic_code=(
                "matchup_shadow_retention_policy_invalid"
            ),
            emitted=True,
        )

    existing = tuple(existing_decisions)

    decisions_by_id = {
        decision.retention_decision_id: (
            decision
        )
        for decision in existing
    }

    duplicate_counts: dict[str, int] = {}
    conflicting_ids: set[str] = set()

    diagnostics: list[str] = []
    validation_errors: list[str] = []

    observability_status = (
        observability_report.observability_status
    )
    observability_version = (
        observability_report.observability_version
    )

    for record in sorted(
        collection.records,
        key=lambda item: (
            item.collection_record_id
        ),
    ):
        decision_diagnostics: list[str] = []
        decision_errors: list[str] = []

        try:
            age_days = _record_age_days(
                collected_at_utc=(
                    record.collected_at_utc
                ),
                evaluated_at_utc=(
                    evaluated_at_utc
                ),
            )
            age_valid = True
        except (TypeError, ValueError):
            age_days = 0
            age_valid = False
            decision_errors.append(
                "matchup_shadow_retention_record_age_invalid"
            )

        quarantine_required = (
            observability_status == "degraded"
            or record.collection_status
            == "rejected"
            or not age_valid
        )

        if observability_status == "degraded":
            decision_diagnostics.append(
                "matchup_shadow_retention_observability_degraded"
            )

        if record.collection_status == "rejected":
            decision_diagnostics.append(
                "matchup_shadow_retention_record_rejected"
            )

        if not age_valid:
            decision_diagnostics.append(
                "matchup_shadow_retention_record_age_invalid"
            )

        if quarantine_required:
            retention_status = "quarantined"
            retention_reason = (
                "quarantine_precedence"
            )
        elif age_days <= retention_window_days:
            retention_status = "retained"
            retention_reason = (
                "within_active_retention_window"
            )
        elif age_days <= archive_window_days:
            retention_status = "archived"
            retention_reason = (
                "within_archive_window"
            )
        else:
            retention_status = "expired"
            retention_reason = (
                "beyond_archive_window"
            )

        decision_id = _decision_identity(
            record=record,
            observability_version=(
                observability_version
            ),
            observability_status=(
                observability_status
            ),
            retention_status=(
                retention_status
            ),
            retention_window_days=(
                retention_window_days
            ),
            archive_window_days=(
                archive_window_days
            ),
        )

        decision = (
            MatchupOverlayShadowRetentionDecision(
                retention_decision_id=(
                    decision_id
                ),
                retention_version=(
                    RETENTION_VERSION
                ),
                evaluated_at_utc=(
                    evaluated_at_utc
                ),
                collection_record_id=(
                    record.collection_record_id
                ),
                collection_version=(
                    record.collection_version
                ),
                dataset_version=(
                    record.dataset_version
                ),
                quality_gate_version=(
                    record.quality_gate_version
                ),
                observability_version=(
                    observability_version
                ),
                collection_status=(
                    record.collection_status
                ),
                observability_status=(
                    observability_status
                ),
                retention_status=(
                    retention_status
                ),
                retention_reason=(
                    retention_reason
                ),
                record_age_days=age_days,
                retention_window_days=(
                    retention_window_days
                ),
                archive_window_days=(
                    archive_window_days
                ),
                eligible_for_retention=(
                    retention_status
                    == "retained"
                ),
                eligible_for_archive=(
                    retention_status
                    == "archived"
                ),
                eligible_for_expiration=(
                    retention_status
                    == "expired"
                ),
                quarantine_required=(
                    quarantine_required
                ),
                dataset_payload_digest=(
                    record.dataset_payload_digest
                ),
                quality_report_digest=(
                    record.quality_report_digest
                ),
                diagnostic_codes=(
                    _sorted_unique_strings(
                        decision_diagnostics
                    )
                ),
                validation_errors=(
                    _sorted_unique_strings(
                        decision_errors
                    )
                ),
            )
        )

        prior = decisions_by_id.get(
            decision_id
        )

        if prior is None:
            decisions_by_id[
                decision_id
            ] = decision
            continue

        duplicate_counts[decision_id] = (
            duplicate_counts.get(
                decision_id,
                0,
            )
            + 1
        )

        if prior.to_dict() != decision.to_dict():
            conflicting_ids.add(
                decision_id
            )
            diagnostics.append(
                "matchup_shadow_retention_conflicting_duplicate"
            )
            validation_errors.append(
                "matchup_shadow_retention_conflicting_duplicate"
            )

    decisions = tuple(
        sorted(
            decisions_by_id.values(),
            key=lambda item: (
                item.retention_decision_id
            ),
        )
    )

    duplicates = tuple(
        MatchupOverlayShadowRetentionDuplicate(
            retention_decision_id=(
                decision_id
            ),
            duplicate_count=count,
            conflict=(
                decision_id
                in conflicting_ids
            ),
            diagnostic_code=(
                "matchup_shadow_retention_conflicting_duplicate"
                if decision_id
                in conflicting_ids
                else
                "matchup_shadow_retention_exact_duplicate"
            ),
        )
        for decision_id, count in sorted(
            duplicate_counts.items()
        )
    )

    if conflicting_ids:
        overall_status = "quarantined"
        reason = "retention_quarantined"
    elif any(
        decision.retention_status
        == "quarantined"
        for decision in decisions
    ):
        overall_status = "quarantined"
        reason = "retention_quarantined"
    elif any(
        decision.retention_status
        == "expired"
        for decision in decisions
    ):
        overall_status = "expired"
        reason = "retention_expired"
    elif any(
        decision.retention_status
        == "archived"
        for decision in decisions
    ):
        overall_status = "archived"
        reason = "retention_archived"
    else:
        overall_status = "retained"
        reason = "retention_retained"

    return MatchupOverlayShadowRetentionLedger(
        emitted=True,
        reason=reason,
        retention_status=overall_status,
        decisions=decisions,
        duplicates=duplicates,
        ledger_digest=(
            retention_ledger_digest(
                decisions
            )
        ),
        evaluated_at_utc=evaluated_at_utc,
        retention_window_days=(
            retention_window_days
        ),
        archive_window_days=(
            archive_window_days
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
        retention_version=RETENTION_VERSION,
    )
