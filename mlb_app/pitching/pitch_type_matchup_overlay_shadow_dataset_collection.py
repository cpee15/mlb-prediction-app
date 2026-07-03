"""
Append-only diagnostic collection for quality-gated matchup shadow datasets.

This module collects Layer 8M shadow datasets together with Layer 8O quality
reports. It is diagnostic-only and grants no production, simulation,
predictive-evaluation, tuning, pricing, or edge authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset import (
    MatchupOverlayShadowDataset,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_quality_gate import (
    ShadowDatasetQualityGateReport,
)


COLLECTION_VERSION = "8Q-v1"

SUPPORTED_COLLECTION_STATUSES = frozenset(
    {
        "accepted",
        "accepted_with_warnings",
        "rejected",
        "empty",
        "disabled",
    }
)


@dataclass(frozen=True)
class MatchupOverlayShadowCollectionRecord:
    collection_record_id: str
    collection_version: str
    collected_at_utc: str
    dataset_version: str
    quality_gate_version: str
    dataset_status: str
    gate_status: str
    dataset_row_count: int
    partition_count: int
    duplicate_row_count: int
    minimum_observation_date_utc: str | None
    maximum_observation_date_utc: str | None
    schema_fingerprint: str
    failed_gate_count: int
    warning_gate_count: int
    minimum_coverage_share: float | None
    mean_coverage_share: float | None
    maximum_coverage_share: float | None
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    dataset_payload_digest: str
    quality_report_digest: str
    collection_status: str
    production_authority: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(self.diagnostic_codes)
        payload["validation_errors"] = list(self.validation_errors)
        return payload


@dataclass(frozen=True)
class MatchupOverlayShadowCollectionDuplicate:
    collection_record_id: str
    duplicate_count: int
    conflict: bool
    diagnostic_code: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchupOverlayShadowCollectionManifest:
    collection_version: str
    generated_at_utc: str
    record_count: int
    accepted_count: int
    accepted_with_warnings_count: int
    rejected_count: int
    empty_count: int
    exact_duplicate_count: int
    conflicting_duplicate_count: int
    collection_digest: str
    append_only: bool = True
    production_authority: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchupOverlayShadowCollection:
    emitted: bool
    reason: str
    collection_status: str
    records: tuple[
        MatchupOverlayShadowCollectionRecord,
        ...,
    ]
    duplicates: tuple[
        MatchupOverlayShadowCollectionDuplicate,
        ...,
    ]
    manifest: MatchupOverlayShadowCollectionManifest | None
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    collection_version: str
    append_only: bool = True
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False
    historical_outcomes_joined: bool = False
    predictive_evaluation_executed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "emitted": self.emitted,
            "reason": self.reason,
            "collection_status": self.collection_status,
            "records": [
                record.to_dict()
                for record in self.records
            ],
            "duplicates": [
                duplicate.to_dict()
                for duplicate in self.duplicates
            ],
            "manifest": (
                self.manifest.to_dict()
                if self.manifest is not None
                else None
            ),
            "diagnostic_codes": list(
                self.diagnostic_codes
            ),
            "validation_errors": list(
                self.validation_errors
            ),
            "collection_version": (
                self.collection_version
            ),
            "append_only": self.append_only,
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


def _dataset_payload(
    dataset: MatchupOverlayShadowDataset,
) -> dict[str, Any]:
    return dataset.to_dict()


def _report_payload(
    report: ShadowDatasetQualityGateReport,
) -> dict[str, Any]:
    return report.to_dict()


def dataset_payload_digest(
    dataset: MatchupOverlayShadowDataset,
) -> str:
    return _sha256(
        _dataset_payload(dataset)
    )


def quality_report_digest(
    report: ShadowDatasetQualityGateReport,
) -> str:
    return _sha256(
        _report_payload(report)
    )


def _collection_record_id(
    *,
    dataset_digest: str,
    report_digest: str,
) -> str:
    digest = _sha256(
        {
            "collection_version": (
                COLLECTION_VERSION
            ),
            "dataset_payload_digest": (
                dataset_digest
            ),
            "quality_report_digest": (
                report_digest
            ),
        }
    )

    return f"matchup-shadow-collection-{digest[:20]}"


def _observation_date_bounds(
    dataset: MatchupOverlayShadowDataset,
) -> tuple[str | None, str | None]:
    values = sorted(
        {
            row.observation_date_utc
            for row in dataset.rows
            if isinstance(
                row.observation_date_utc,
                str,
            )
            and row.observation_date_utc
        }
    )

    if not values:
        return None, None

    return values[0], values[-1]


def _map_collection_status(
    report: ShadowDatasetQualityGateReport,
) -> str:
    return {
        "pass": "accepted",
        "warn": "accepted_with_warnings",
        "fail": "rejected",
        "empty": "empty",
        "disabled": "disabled",
    }.get(
        report.gate_status,
        "rejected",
    )


def build_matchup_overlay_shadow_dataset_collection_record(
    dataset: MatchupOverlayShadowDataset | None,
    report: ShadowDatasetQualityGateReport | None,
    *,
    collected_at_utc: str,
) -> MatchupOverlayShadowCollectionRecord:
    validation_errors: list[str] = []
    diagnostics: list[str] = []

    if dataset is None:
        validation_errors.append(
            "matchup_shadow_collection_dataset_missing"
        )

    if report is None:
        validation_errors.append(
            "matchup_shadow_collection_quality_report_missing"
        )

    if dataset is None or report is None:
        dataset_digest = _sha256(
            {
                "dataset": (
                    None
                    if dataset is None
                    else _dataset_payload(dataset)
                )
            }
        )

        report_digest = _sha256(
            {
                "report": (
                    None
                    if report is None
                    else _report_payload(report)
                )
            }
        )

        return MatchupOverlayShadowCollectionRecord(
            collection_record_id=_collection_record_id(
                dataset_digest=dataset_digest,
                report_digest=report_digest,
            ),
            collection_version=COLLECTION_VERSION,
            collected_at_utc=collected_at_utc,
            dataset_version=(
                getattr(
                    dataset,
                    "shadow_dataset_version",
                    "",
                )
                if dataset is not None
                else ""
            ),
            quality_gate_version=(
                report.quality_gate_version
                if report is not None
                else ""
            ),
            dataset_status=(
                dataset.dataset_status
                if dataset is not None
                else "invalid"
            ),
            gate_status=(
                report.gate_status
                if report is not None
                else "fail"
            ),
            dataset_row_count=(
                len(dataset.rows)
                if dataset is not None
                else 0
            ),
            partition_count=(
                len(dataset.partitions)
                if dataset is not None
                else 0
            ),
            duplicate_row_count=0,
            minimum_observation_date_utc=None,
            maximum_observation_date_utc=None,
            schema_fingerprint="",
            failed_gate_count=1,
            warning_gate_count=0,
            minimum_coverage_share=None,
            mean_coverage_share=None,
            maximum_coverage_share=None,
            diagnostic_codes=_sorted_unique_strings(
                validation_errors
            ),
            validation_errors=_sorted_unique_strings(
                validation_errors
            ),
            dataset_payload_digest=dataset_digest,
            quality_report_digest=report_digest,
            collection_status="rejected",
        )

    dataset_digest = dataset_payload_digest(
        dataset
    )
    report_digest = quality_report_digest(
        report
    )

    summary = report.summary
    manifest = dataset.manifest

    if report.gate_status == "fail":
        diagnostics.append(
            "matchup_shadow_collection_quality_failed"
        )
    elif report.gate_status == "warn":
        diagnostics.append(
            "matchup_shadow_collection_quality_warned"
        )
    elif report.gate_status == "empty":
        diagnostics.append(
            "matchup_shadow_collection_dataset_empty"
        )

    if not report.emitted:
        validation_errors.append(
            "matchup_shadow_collection_quality_report_not_emitted"
        )

    if report.gate_status != "disabled":
        if summary is None:
            validation_errors.append(
                "matchup_shadow_collection_quality_summary_missing"
            )
        elif (
            summary.dataset_status
            != dataset.dataset_status
        ):
            validation_errors.append(
                "matchup_shadow_collection_status_mismatch"
            )

    minimum_date, maximum_date = (
        _observation_date_bounds(dataset)
    )

    collection_status = _map_collection_status(
        report
    )

    if validation_errors:
        collection_status = "rejected"

    duplicate_row_count = (
        manifest.duplicate_row_count
        if manifest is not None
        else 0
    )

    schema_fingerprint = (
        manifest.schema_fingerprint
        if manifest is not None
        else ""
    )

    return MatchupOverlayShadowCollectionRecord(
        collection_record_id=_collection_record_id(
            dataset_digest=dataset_digest,
            report_digest=report_digest,
        ),
        collection_version=COLLECTION_VERSION,
        collected_at_utc=collected_at_utc,
        dataset_version=(
            dataset.shadow_dataset_version
        ),
        quality_gate_version=(
            report.quality_gate_version
        ),
        dataset_status=dataset.dataset_status,
        gate_status=report.gate_status,
        dataset_row_count=len(dataset.rows),
        partition_count=len(
            dataset.partitions
        ),
        duplicate_row_count=(
            duplicate_row_count
        ),
        minimum_observation_date_utc=(
            minimum_date
        ),
        maximum_observation_date_utc=(
            maximum_date
        ),
        schema_fingerprint=(
            schema_fingerprint
        ),
        failed_gate_count=(
            summary.failed_gate_count
            if summary is not None
            else 1
        ),
        warning_gate_count=(
            summary.warning_gate_count
            if summary is not None
            else 0
        ),
        minimum_coverage_share=(
            summary.minimum_coverage_share
            if summary is not None
            else None
        ),
        mean_coverage_share=(
            summary.mean_coverage_share
            if summary is not None
            else None
        ),
        maximum_coverage_share=(
            summary.maximum_coverage_share
            if summary is not None
            else None
        ),
        diagnostic_codes=(
            _sorted_unique_strings(
                [
                    *diagnostics,
                    *report.diagnostic_codes,
                ]
            )
        ),
        validation_errors=(
            _sorted_unique_strings(
                [
                    *validation_errors,
                    *report.validation_errors,
                ]
            )
        ),
        dataset_payload_digest=(
            dataset_digest
        ),
        quality_report_digest=(
            report_digest
        ),
        collection_status=(
            collection_status
        ),
    )


def collect_pitch_type_matchup_overlay_shadow_datasets(
    items: Iterable[
        tuple[
            MatchupOverlayShadowDataset | None,
            ShadowDatasetQualityGateReport | None,
        ]
    ],
    *,
    enabled: bool = False,
    collected_at_utc: str,
) -> MatchupOverlayShadowCollection:
    if not enabled:
        return MatchupOverlayShadowCollection(
            emitted=False,
            reason="collection_disabled",
            collection_status="disabled",
            records=(),
            duplicates=(),
            manifest=None,
            diagnostic_codes=(
                "matchup_shadow_collection_disabled",
            ),
            validation_errors=(),
            collection_version=(
                COLLECTION_VERSION
            ),
        )

    candidate_records = [
        build_matchup_overlay_shadow_dataset_collection_record(
            dataset,
            report,
            collected_at_utc=collected_at_utc,
        )
        for dataset, report in items
    ]

    grouped: dict[
        str,
        list[
            MatchupOverlayShadowCollectionRecord
        ],
    ] = {}

    for record in candidate_records:
        grouped.setdefault(
            record.collection_record_id,
            [],
        ).append(record)

    accepted_records: list[
        MatchupOverlayShadowCollectionRecord
    ] = []

    duplicates: list[
        MatchupOverlayShadowCollectionDuplicate
    ] = []

    collection_errors: list[str] = []
    diagnostics: list[str] = []

    for record_id in sorted(grouped):
        group = grouped[record_id]

        serialized = {
            _canonical_json(
                record.to_dict()
            )
            for record in group
        }

        if len(serialized) == 1:
            accepted_records.append(group[0])

            if len(group) > 1:
                duplicates.append(
                    MatchupOverlayShadowCollectionDuplicate(
                        collection_record_id=record_id,
                        duplicate_count=(
                            len(group) - 1
                        ),
                        conflict=False,
                        diagnostic_code=(
                            "matchup_shadow_collection_exact_duplicate"
                        ),
                    )
                )
        else:
            duplicates.append(
                MatchupOverlayShadowCollectionDuplicate(
                    collection_record_id=record_id,
                    duplicate_count=(
                        len(group) - 1
                    ),
                    conflict=True,
                    diagnostic_code=(
                        "matchup_shadow_collection_conflicting_duplicate"
                    ),
                )
            )

            collection_errors.append(
                "matchup_shadow_collection_conflicting_duplicate"
            )

    accepted_records = sorted(
        accepted_records,
        key=lambda record: (
            record.collection_record_id,
            record.dataset_payload_digest,
            record.quality_report_digest,
        ),
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

    status_counts = {
        status: sum(
            1
            for record in accepted_records
            if record.collection_status
            == status
        )
        for status in SUPPORTED_COLLECTION_STATUSES
    }

    collection_digest = _sha256(
        [
            record.to_dict()
            for record in accepted_records
        ]
    )

    manifest = MatchupOverlayShadowCollectionManifest(
        collection_version=(
            COLLECTION_VERSION
        ),
        generated_at_utc=collected_at_utc,
        record_count=len(
            accepted_records
        ),
        accepted_count=status_counts[
            "accepted"
        ],
        accepted_with_warnings_count=(
            status_counts[
                "accepted_with_warnings"
            ]
        ),
        rejected_count=status_counts[
            "rejected"
        ],
        empty_count=status_counts[
            "empty"
        ],
        exact_duplicate_count=(
            exact_duplicate_count
        ),
        conflicting_duplicate_count=(
            conflicting_duplicate_count
        ),
        collection_digest=(
            collection_digest
        ),
    )

    if conflicting_duplicate_count:
        collection_status = "rejected"
        reason = "collection_conflict"
        diagnostics.append(
            "matchup_shadow_collection_conflicting_duplicate"
        )
    elif not accepted_records:
        collection_status = "empty"
        reason = "collection_empty"
    elif status_counts["rejected"]:
        collection_status = "rejected"
        reason = "collection_contains_rejections"
    elif status_counts[
        "accepted_with_warnings"
    ]:
        collection_status = (
            "accepted_with_warnings"
        )
        reason = "collection_accepted_with_warnings"
    elif status_counts["empty"]:
        collection_status = "empty"
        reason = "collection_empty"
    else:
        collection_status = "accepted"
        reason = "collection_accepted"

    return MatchupOverlayShadowCollection(
        emitted=True,
        reason=reason,
        collection_status=collection_status,
        records=tuple(
            accepted_records
        ),
        duplicates=tuple(
            sorted(
                duplicates,
                key=lambda duplicate: (
                    duplicate.collection_record_id,
                    duplicate.conflict,
                ),
            )
        ),
        manifest=manifest,
        diagnostic_codes=(
            _sorted_unique_strings(
                diagnostics
            )
        ),
        validation_errors=(
            _sorted_unique_strings(
                collection_errors
            )
        ),
        collection_version=(
            COLLECTION_VERSION
        ),
    )
