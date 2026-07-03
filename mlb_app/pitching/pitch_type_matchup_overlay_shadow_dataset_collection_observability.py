"""
Diagnostic observability for Layer 8Q matchup shadow-dataset collections.

This module reconciles collection manifests, status counts, duplicate signals,
record identities, collection digests, dataset-size distributions, and coverage
distributions. It does not alter production or simulation behavior and does not
join outcomes or perform predictive evaluation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection import (
    MatchupOverlayShadowCollection,
)


COLLECTION_OBSERVABILITY_VERSION = "8S-v1"

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
class CollectionObservabilitySignal:
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
class CollectionObservabilitySnapshot:
    observability_snapshot_id: str
    observability_version: str
    observed_at_utc: str
    collection_version: str
    collection_status: str
    observability_status: str
    record_count: int
    accepted_count: int
    accepted_with_warnings_count: int
    rejected_count: int
    empty_count: int
    exact_duplicate_count: int
    conflicting_duplicate_count: int
    minimum_dataset_row_count: int | None
    mean_dataset_row_count: float | None
    maximum_dataset_row_count: int | None
    minimum_coverage_share: float | None
    mean_coverage_share: float | None
    maximum_coverage_share: float | None
    manifest_reconciles: bool
    collection_digest_reconciles: bool
    record_identifiers_unique: bool
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
class CollectionObservabilityReport:
    emitted: bool
    reason: str
    observability_status: str
    snapshot: CollectionObservabilitySnapshot | None
    signals: tuple[CollectionObservabilitySignal, ...]
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    observability_version: str
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
            "observability_version": (
                self.observability_version
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


def recompute_collection_digest(
    collection: MatchupOverlayShadowCollection,
) -> str:
    return _sha256(
        [
            record.to_dict()
            for record in collection.records
        ]
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
) -> CollectionObservabilitySignal:
    return CollectionObservabilitySignal(
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


def observe_pitch_type_matchup_overlay_shadow_dataset_collection(
    collection: MatchupOverlayShadowCollection | None,
    *,
    enabled: bool = False,
    observed_at_utc: str,
) -> CollectionObservabilityReport:
    if not enabled:
        return CollectionObservabilityReport(
            emitted=False,
            reason="observability_disabled",
            observability_status="disabled",
            snapshot=None,
            signals=(),
            diagnostic_codes=(
                "matchup_shadow_collection_observability_disabled",
            ),
            validation_errors=(),
            observability_version=(
                COLLECTION_OBSERVABILITY_VERSION
            ),
        )

    if collection is None:
        diagnostic = (
            "matchup_shadow_collection_observability_collection_missing"
        )

        snapshot_id = (
            "matchup-shadow-collection-observability-"
            + _sha256(
                {
                    "observability_version": (
                        COLLECTION_OBSERVABILITY_VERSION
                    ),
                    "collection": None,
                }
            )[:20]
        )

        snapshot = CollectionObservabilitySnapshot(
            observability_snapshot_id=(
                snapshot_id
            ),
            observability_version=(
                COLLECTION_OBSERVABILITY_VERSION
            ),
            observed_at_utc=observed_at_utc,
            collection_version="",
            collection_status="rejected",
            observability_status="degraded",
            record_count=0,
            accepted_count=0,
            accepted_with_warnings_count=0,
            rejected_count=0,
            empty_count=0,
            exact_duplicate_count=0,
            conflicting_duplicate_count=0,
            minimum_dataset_row_count=None,
            mean_dataset_row_count=None,
            maximum_dataset_row_count=None,
            minimum_coverage_share=None,
            mean_coverage_share=None,
            maximum_coverage_share=None,
            manifest_reconciles=False,
            collection_digest_reconciles=False,
            record_identifiers_unique=True,
            diagnostic_codes=(diagnostic,),
            validation_errors=(diagnostic,),
        )

        return CollectionObservabilityReport(
            emitted=True,
            reason="observability_degraded",
            observability_status="degraded",
            snapshot=snapshot,
            signals=(
                _signal(
                    "CO-S01",
                    "manifest_reconciliation",
                    "collection_present",
                    passed=False,
                    triggered=True,
                    observed=False,
                    expected=True,
                    diagnostic_code=diagnostic,
                ),
            ),
            diagnostic_codes=(diagnostic,),
            validation_errors=(diagnostic,),
            observability_version=(
                COLLECTION_OBSERVABILITY_VERSION
            ),
        )

    records = tuple(collection.records)
    duplicates = tuple(collection.duplicates)
    manifest = collection.manifest

    record_ids = [
        record.collection_record_id
        for record in records
    ]

    record_identifiers_unique = (
        len(record_ids)
        == len(set(record_ids))
    )

    accepted_count = sum(
        1
        for record in records
        if record.collection_status
        == "accepted"
    )

    warning_count = sum(
        1
        for record in records
        if record.collection_status
        == "accepted_with_warnings"
    )

    rejected_count = sum(
        1
        for record in records
        if record.collection_status
        == "rejected"
    )

    empty_count = sum(
        1
        for record in records
        if record.collection_status
        == "empty"
    )

    exact_duplicate_count = sum(
        duplicate.duplicate_count
        for duplicate in duplicates
        if not duplicate.conflict
    )

    conflicting_duplicate_count = sum(
        duplicate.duplicate_count
        for duplicate in duplicates
        if duplicate.conflict
    )

    manifest_present = manifest is not None

    manifest_record_count_reconciles = (
        manifest is not None
        and manifest.record_count
        == len(records)
    )

    manifest_accepted_count_reconciles = (
        manifest is not None
        and manifest.accepted_count
        == accepted_count
    )

    manifest_warning_count_reconciles = (
        manifest is not None
        and (
            manifest.accepted_with_warnings_count
            == warning_count
        )
    )

    manifest_rejected_count_reconciles = (
        manifest is not None
        and manifest.rejected_count
        == rejected_count
    )

    manifest_empty_count_reconciles = (
        manifest is not None
        and manifest.empty_count
        == empty_count
    )

    manifest_duplicate_counts_reconcile = (
        manifest is not None
        and manifest.exact_duplicate_count
        == exact_duplicate_count
        and (
            manifest.conflicting_duplicate_count
            == conflicting_duplicate_count
        )
    )

    manifest_reconciles = all(
        (
            manifest_present,
            manifest_record_count_reconciles,
            manifest_accepted_count_reconciles,
            manifest_warning_count_reconciles,
            manifest_rejected_count_reconciles,
            manifest_empty_count_reconciles,
            manifest_duplicate_counts_reconcile,
        )
    )

    recomputed_digest = (
        recompute_collection_digest(
            collection
        )
    )

    collection_digest_reconciles = (
        manifest is not None
        and manifest.collection_digest
        == recomputed_digest
    )

    dataset_row_counts = [
        record.dataset_row_count
        for record in records
        if record.collection_status
        != "rejected"
    ]

    if dataset_row_counts:
        minimum_dataset_row_count = min(
            dataset_row_counts
        )
        mean_dataset_row_count = round(
            sum(dataset_row_counts)
            / len(dataset_row_counts),
            6,
        )
        maximum_dataset_row_count = max(
            dataset_row_counts
        )
    else:
        minimum_dataset_row_count = None
        mean_dataset_row_count = None
        maximum_dataset_row_count = None

    coverage_values = [
        value
        for record in records
        if record.collection_status
        != "rejected"
        for value in (
            record.minimum_coverage_share,
            record.mean_coverage_share,
            record.maximum_coverage_share,
        )
        if value is not None
    ]

    if coverage_values:
        minimum_coverage_share = min(
            coverage_values
        )
        mean_coverage_share = round(
            sum(coverage_values)
            / len(coverage_values),
            6,
        )
        maximum_coverage_share = max(
            coverage_values
        )
    else:
        minimum_coverage_share = None
        mean_coverage_share = None
        maximum_coverage_share = None

    dataset_row_counts_valid = all(
        record.dataset_row_count >= 0
        for record in records
    )

    coverage_values_valid = all(
        (
            value is None
            or 0.0 <= value <= 1.0
        )
        for record in records
        for value in (
            record.minimum_coverage_share,
            record.mean_coverage_share,
            record.maximum_coverage_share,
        )
    )

    record_versions_present = all(
        bool(record.collection_version)
        and bool(record.dataset_version)
        and bool(record.quality_gate_version)
        for record in records
    )

    record_digests_present = all(
        len(record.dataset_payload_digest)
        == 64
        and len(record.quality_report_digest)
        == 64
        for record in records
    )

    production_authority_false = (
        collection.production_authority
        is False
        and (
            manifest is None
            or manifest.production_authority
            is False
        )
        and all(
            record.production_authority
            is False
            for record in records
        )
    )

    signals: list[
        CollectionObservabilitySignal
    ] = []

    validation_errors: list[str] = []
    diagnostics: list[str] = []

    required_signals = [
        (
            "CO-S01",
            "manifest_reconciliation",
            "manifest_present",
            manifest_present,
        ),
        (
            "CO-S02",
            "manifest_reconciliation",
            "manifest_record_count_reconciles",
            manifest_record_count_reconciles,
        ),
        (
            "CO-S03",
            "manifest_reconciliation",
            "manifest_status_counts_reconcile",
            all(
                (
                    manifest_accepted_count_reconciles,
                    manifest_warning_count_reconciles,
                    manifest_rejected_count_reconciles,
                    manifest_empty_count_reconciles,
                )
            ),
        ),
        (
            "CO-S04",
            "duplicate_integrity",
            "manifest_duplicate_counts_reconcile",
            manifest_duplicate_counts_reconcile,
        ),
        (
            "CO-S05",
            "digest_integrity",
            "collection_digest_reconciles",
            collection_digest_reconciles,
        ),
        (
            "CO-S06",
            "record_identity",
            "record_identifiers_unique",
            record_identifiers_unique,
        ),
        (
            "CO-S07",
            "dataset_size_distribution",
            "dataset_row_counts_valid",
            dataset_row_counts_valid,
        ),
        (
            "CO-S08",
            "coverage_distribution",
            "coverage_values_valid",
            coverage_values_valid,
        ),
        (
            "CO-S09",
            "record_identity",
            "record_versions_present",
            record_versions_present,
        ),
        (
            "CO-S10",
            "digest_integrity",
            "record_digests_present",
            record_digests_present,
        ),
        (
            "CO-S11",
            "authority_boundary",
            "production_authority_false",
            production_authority_false,
        ),
    ]

    for (
        signal_id,
        signal_group,
        signal_name,
        passed,
    ) in required_signals:
        diagnostic_code = (
            None
            if passed
            else (
                "matchup_shadow_collection_observability_"
                + signal_name
                + "_failed"
            )
        )

        if diagnostic_code:
            validation_errors.append(
                diagnostic_code
            )

        signals.append(
            _signal(
                signal_id,
                signal_group,
                signal_name,
                passed=passed,
                triggered=not passed,
                observed=passed,
                expected=True,
                diagnostic_code=(
                    diagnostic_code
                ),
            )
        )

    warning_records_present = (
        warning_count > 0
    )
    rejected_records_present = (
        rejected_count > 0
    )
    exact_duplicates_present = (
        exact_duplicate_count > 0
    )
    conflicts_present = (
        conflicting_duplicate_count > 0
    )

    warning_signals = [
        (
            "CO-W01",
            "collection_status_distribution",
            "warning_records_present",
            warning_records_present,
            "matchup_shadow_collection_observability_warning_records",
        ),
        (
            "CO-W02",
            "collection_status_distribution",
            "rejected_records_present",
            rejected_records_present,
            "matchup_shadow_collection_observability_rejected_records",
        ),
        (
            "CO-W03",
            "duplicate_integrity",
            "exact_duplicates_present",
            exact_duplicates_present,
            "matchup_shadow_collection_observability_exact_duplicates",
        ),
        (
            "CO-W04",
            "duplicate_integrity",
            "conflicting_duplicates_present",
            conflicts_present,
            "matchup_shadow_collection_observability_conflicts",
        ),
    ]

    for (
        signal_id,
        signal_group,
        signal_name,
        triggered,
        diagnostic_code,
    ) in warning_signals:
        if triggered:
            diagnostics.append(
                diagnostic_code
            )

        signals.append(
            _signal(
                signal_id,
                signal_group,
                signal_name,
                passed=True,
                triggered=triggered,
                observed=triggered,
                expected=False,
                diagnostic_code=(
                    diagnostic_code
                    if triggered
                    else None
                ),
            )
        )

    reconciliation_failure = (
        not manifest_reconciles
        or not collection_digest_reconciles
        or not record_identifiers_unique
        or bool(validation_errors)
    )

    if (
        collection.collection_status == "empty"
        and manifest is not None
        and manifest.empty_count == 1
        and not reconciliation_failure
    ):
        observability_status = "empty"
        reason = "observability_empty"
        diagnostics.append(
            "matchup_shadow_collection_observability_empty"
        )
    elif (
        conflicts_present
        or rejected_records_present
        or reconciliation_failure
    ):
        observability_status = "degraded"
        reason = "observability_degraded"

        if reconciliation_failure:
            diagnostics.append(
                "matchup_shadow_collection_observability_reconciliation_failed"
            )
    elif (
        warning_records_present
        or exact_duplicates_present
    ):
        observability_status = "warning"
        reason = "observability_warning"
    else:
        observability_status = "healthy"
        reason = "observability_healthy"

    snapshot_identity_payload = {
        "observability_version": (
            COLLECTION_OBSERVABILITY_VERSION
        ),
        "collection_version": (
            collection.collection_version
        ),
        "collection_status": (
            collection.collection_status
        ),
        "record_ids": sorted(record_ids),
        "manifest": (
            manifest.to_dict()
            if manifest is not None
            else None
        ),
        "observability_status": (
            observability_status
        ),
    }

    snapshot_id = (
        "matchup-shadow-collection-observability-"
        + _sha256(
            snapshot_identity_payload
        )[:20]
    )

    snapshot = CollectionObservabilitySnapshot(
        observability_snapshot_id=(
            snapshot_id
        ),
        observability_version=(
            COLLECTION_OBSERVABILITY_VERSION
        ),
        observed_at_utc=observed_at_utc,
        collection_version=(
            collection.collection_version
        ),
        collection_status=(
            collection.collection_status
        ),
        observability_status=(
            observability_status
        ),
        record_count=len(records),
        accepted_count=accepted_count,
        accepted_with_warnings_count=(
            warning_count
        ),
        rejected_count=rejected_count,
        empty_count=empty_count,
        exact_duplicate_count=(
            exact_duplicate_count
        ),
        conflicting_duplicate_count=(
            conflicting_duplicate_count
        ),
        minimum_dataset_row_count=(
            minimum_dataset_row_count
        ),
        mean_dataset_row_count=(
            mean_dataset_row_count
        ),
        maximum_dataset_row_count=(
            maximum_dataset_row_count
        ),
        minimum_coverage_share=(
            minimum_coverage_share
        ),
        mean_coverage_share=(
            mean_coverage_share
        ),
        maximum_coverage_share=(
            maximum_coverage_share
        ),
        manifest_reconciles=(
            manifest_reconciles
        ),
        collection_digest_reconciles=(
            collection_digest_reconciles
        ),
        record_identifiers_unique=(
            record_identifiers_unique
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

    return CollectionObservabilityReport(
        emitted=True,
        reason=reason,
        observability_status=(
            observability_status
        ),
        snapshot=snapshot,
        signals=tuple(signals),
        diagnostic_codes=(
            snapshot.diagnostic_codes
        ),
        validation_errors=(
            snapshot.validation_errors
        ),
        observability_version=(
            COLLECTION_OBSERVABILITY_VERSION
        ),
    )
