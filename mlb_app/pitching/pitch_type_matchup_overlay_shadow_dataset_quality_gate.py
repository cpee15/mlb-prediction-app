"""
Diagnostic-only quality gates for the Layer 8M matchup shadow dataset.

This module evaluates structural integrity, schema consistency, manifest and
partition reconciliation, and warning-only coverage conditions. It does not
join outcomes, evaluate predictions, tune parameters, or affect production or
simulation behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset import (
    MatchupOverlayShadowDataset,
    SHADOW_DATASET_VERSION,
    SHADOW_ROW_FIELD_ORDER,
)


QUALITY_GATE_VERSION = "8O-v1"

SUPPORTED_DATASET_STATUSES = frozenset(
    {
        "ready",
        "partial",
        "empty",
        "invalid",
        "disabled",
    }
)

REQUIRED_GATE_DEFINITIONS = (
    ("QG-R01", "dataset_emitted"),
    ("QG-R02", "dataset_not_invalid"),
    ("QG-R03", "manifest_present"),
    ("QG-R04", "manifest_row_count_reconciles"),
    ("QG-R05", "manifest_partition_count_reconciles"),
    (
        "QG-R06",
        "manifest_unique_observation_count_reconciles",
    ),
    ("QG-R07", "schema_fingerprint_present"),
    (
        "QG-R08",
        "schema_fingerprint_matches_expected",
    ),
    ("QG-R09", "dataset_row_identifiers_unique"),
    ("QG-R10", "observation_identifiers_present"),
    ("QG-R11", "partition_keys_unique"),
    ("QG-R12", "partition_paths_unique"),
    ("QG-R13", "partition_row_counts_reconcile"),
    ("QG-R14", "observation_dates_valid"),
    ("QG-R15", "coverage_values_valid"),
    ("QG-R16", "count_values_nonnegative"),
    ("QG-R17", "usage_values_valid"),
    ("QG-R18", "source_versions_present"),
    (
        "QG-R19",
        "shadow_dataset_version_consistent",
    ),
    ("QG-R20", "production_authority_false"),
)

WARNING_GATE_DEFINITIONS = (
    ("QG-W01", "partial_rows_present"),
    ("QG-W02", "fallback_rows_present"),
    ("QG-W03", "unknown_pitch_rows_present"),
    ("QG-W04", "pitcher_only_rows_present"),
    ("QG-W05", "duplicate_rows_present"),
    ("QG-W06", "coverage_below_half_present"),
)


@dataclass(frozen=True)
class ShadowDatasetQualityGateResult:
    gate_id: str
    gate_name: str
    gate_type: str
    passed: bool
    triggered: bool
    observed_value: str
    expected_value: str
    diagnostic_code: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ShadowDatasetQualityGateSummary:
    quality_gate_version: str
    evaluated_at_utc: str
    dataset_status: str
    gate_status: str
    row_count: int
    partition_count: int
    duplicate_row_count: int
    invalid_row_count: int
    partial_row_count: int
    complete_row_count: int
    minimum_coverage_share: float | None
    mean_coverage_share: float | None
    maximum_coverage_share: float | None
    fallback_row_count: int
    unknown_pitch_row_count: int
    pitcher_only_row_count: int
    schema_fingerprint_matches: bool
    manifest_reconciles: bool
    partition_manifest_reconciles: bool
    row_identifiers_unique: bool
    source_versions_present: bool
    failed_gate_count: int
    warning_gate_count: int
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False
    historical_outcomes_joined: bool = False
    predictive_evaluation_executed: bool = False

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
class ShadowDatasetQualityGateReport:
    emitted: bool
    reason: str
    gate_status: str
    results: tuple[
        ShadowDatasetQualityGateResult,
        ...,
    ]
    summary: ShadowDatasetQualityGateSummary | None
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    quality_gate_version: str
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False
    historical_outcomes_joined: bool = False
    predictive_evaluation_executed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "emitted": self.emitted,
            "reason": self.reason,
            "gate_status": self.gate_status,
            "results": [
                result.to_dict()
                for result in self.results
            ],
            "summary": (
                self.summary.to_dict()
                if self.summary is not None
                else None
            ),
            "diagnostic_codes": list(
                self.diagnostic_codes
            ),
            "validation_errors": list(
                self.validation_errors
            ),
            "quality_gate_version": (
                self.quality_gate_version
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


def expected_schema_fingerprint() -> str:
    serialized = json.dumps(
        SHADOW_ROW_FIELD_ORDER,
        separators=(",", ":"),
    )
    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()


def _valid_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except (TypeError, ValueError):
        return False
    return True


def _result(
    gate_id: str,
    gate_name: str,
    gate_type: str,
    *,
    passed: bool,
    triggered: bool,
    observed: Any,
    expected: Any,
    diagnostic_code: str | None,
) -> ShadowDatasetQualityGateResult:
    return ShadowDatasetQualityGateResult(
        gate_id=gate_id,
        gate_name=gate_name,
        gate_type=gate_type,
        passed=passed,
        triggered=triggered,
        observed_value=json.dumps(
            observed,
            sort_keys=True,
            default=str,
        ),
        expected_value=json.dumps(
            expected,
            sort_keys=True,
            default=str,
        ),
        diagnostic_code=diagnostic_code,
    )


def evaluate_pitch_type_matchup_overlay_shadow_dataset_quality(
    dataset: MatchupOverlayShadowDataset | None,
    *,
    enabled: bool = False,
    evaluated_at_utc: str | None = None,
) -> ShadowDatasetQualityGateReport:
    if not enabled:
        return ShadowDatasetQualityGateReport(
            emitted=False,
            reason="quality_gate_disabled",
            gate_status="disabled",
            results=(),
            summary=None,
            diagnostic_codes=(
                "matchup_shadow_quality_gate_disabled",
            ),
            validation_errors=(),
            quality_gate_version=QUALITY_GATE_VERSION,
        )

    evaluated_at = (
        evaluated_at_utc
        or datetime.now(
            timezone.utc
        ).isoformat()
    )

    if dataset is None:
        result = _result(
            "QG-R01",
            "dataset_emitted",
            "required",
            passed=False,
            triggered=True,
            observed=None,
            expected=True,
            diagnostic_code=(
                "matchup_shadow_quality_dataset_missing"
            ),
        )

        summary = ShadowDatasetQualityGateSummary(
            quality_gate_version=QUALITY_GATE_VERSION,
            evaluated_at_utc=evaluated_at,
            dataset_status="invalid",
            gate_status="fail",
            row_count=0,
            partition_count=0,
            duplicate_row_count=0,
            invalid_row_count=1,
            partial_row_count=0,
            complete_row_count=0,
            minimum_coverage_share=None,
            mean_coverage_share=None,
            maximum_coverage_share=None,
            fallback_row_count=0,
            unknown_pitch_row_count=0,
            pitcher_only_row_count=0,
            schema_fingerprint_matches=False,
            manifest_reconciles=False,
            partition_manifest_reconciles=False,
            row_identifiers_unique=True,
            source_versions_present=False,
            failed_gate_count=1,
            warning_gate_count=0,
            diagnostic_codes=(
                "matchup_shadow_quality_dataset_missing",
            ),
            validation_errors=(
                "matchup_shadow_quality_dataset_missing",
            ),
        )

        return ShadowDatasetQualityGateReport(
            emitted=True,
            reason="quality_gate_failed",
            gate_status="fail",
            results=(result,),
            summary=summary,
            diagnostic_codes=summary.diagnostic_codes,
            validation_errors=summary.validation_errors,
            quality_gate_version=QUALITY_GATE_VERSION,
        )

    rows = tuple(dataset.rows)
    partitions = tuple(dataset.partitions)
    duplicates = tuple(dataset.duplicates)
    manifest = dataset.manifest

    row_ids = [
        row.dataset_row_id
        for row in rows
    ]
    observation_ids = [
        row.observation_id
        for row in rows
    ]
    partition_keys = [
        partition.partition_key
        for partition in partitions
    ]
    partition_paths = [
        partition.partition_path
        for partition in partitions
    ]

    unique_row_ids = (
        len(row_ids)
        == len(set(row_ids))
    )
    observation_ids_present = all(
        isinstance(value, str)
        and bool(value)
        for value in observation_ids
    )
    partition_keys_unique = (
        len(partition_keys)
        == len(set(partition_keys))
    )
    partition_paths_unique = (
        len(partition_paths)
        == len(set(partition_paths))
    )

    dates_valid = all(
        _valid_iso_date(
            row.observation_date_utc
        )
        for row in rows
    )

    coverage_valid = all(
        0.0 <= row.coverage_share <= 1.0
        for row in rows
    )

    count_values_nonnegative = all(
        value >= 0
        for row in rows
        for value in (
            row.matched_pitch_count,
            row.unmatched_pitch_count,
            row.overlay_entry_count,
            row.fallback_entry_count,
            row.unknown_pitch_entry_count,
            row.pitcher_only_entry_count,
        )
    )

    usage_valid = all(
        (
            value is None
            or 0.0 <= value <= 1.0
        )
        for row in rows
        for value in (
            row.matched_usage_share,
            row.unmatched_usage_share,
        )
    ) and all(
        (
            row.matched_usage_share is None
            or row.unmatched_usage_share is None
            or (
                row.matched_usage_share
                + row.unmatched_usage_share
                <= 1.000001
            )
        )
        for row in rows
    )

    source_versions_present = all(
        bool(row.overlay_version)
        and bool(row.observability_version)
        and bool(row.shadow_dataset_version)
        for row in rows
    )

    dataset_versions_consistent = all(
        row.shadow_dataset_version
        == SHADOW_DATASET_VERSION
        for row in rows
    ) and (
        dataset.shadow_dataset_version
        == SHADOW_DATASET_VERSION
    )

    expected_fingerprint = (
        expected_schema_fingerprint()
    )

    schema_fingerprint_present = (
        manifest is not None
        and bool(
            manifest.schema_fingerprint
        )
    )

    schema_fingerprint_matches = (
        manifest is not None
        and (
            manifest.schema_fingerprint
            == expected_fingerprint
        )
    )

    manifest_row_count_reconciles = (
        manifest is not None
        and manifest.row_count == len(rows)
    )

    manifest_partition_count_reconciles = (
        manifest is not None
        and (
            manifest.partition_count
            == len(partitions)
        )
    )

    manifest_unique_count_reconciles = (
        manifest is not None
        and (
            manifest.unique_observation_count
            == len(set(observation_ids))
        )
    )

    partition_rows_total = sum(
        partition.row_count
        for partition in partitions
    )

    partition_row_counts_reconcile = (
        partition_rows_total == len(rows)
    )

    manifest_reconciles = all(
        (
            manifest_row_count_reconciles,
            manifest_partition_count_reconciles,
            manifest_unique_count_reconciles,
        )
    )

    production_authority_false = (
        dataset.production_authority is False
        and (
            manifest is None
            or manifest.production_authority
            is False
        )
    )

    required_values = {
        "QG-R01": dataset.emitted,
        "QG-R02": (
            dataset.dataset_status
            != "invalid"
        ),
        "QG-R03": manifest is not None,
        "QG-R04": (
            manifest_row_count_reconciles
        ),
        "QG-R05": (
            manifest_partition_count_reconciles
        ),
        "QG-R06": (
            manifest_unique_count_reconciles
        ),
        "QG-R07": (
            schema_fingerprint_present
        ),
        "QG-R08": (
            schema_fingerprint_matches
        ),
        "QG-R09": unique_row_ids,
        "QG-R10": observation_ids_present,
        "QG-R11": partition_keys_unique,
        "QG-R12": partition_paths_unique,
        "QG-R13": (
            partition_row_counts_reconcile
        ),
        "QG-R14": dates_valid,
        "QG-R15": coverage_valid,
        "QG-R16": count_values_nonnegative,
        "QG-R17": usage_valid,
        "QG-R18": source_versions_present,
        "QG-R19": (
            dataset_versions_consistent
        ),
        "QG-R20": (
            production_authority_false
        ),
    }

    results: list[
        ShadowDatasetQualityGateResult
    ] = []

    validation_errors: list[str] = []

    for gate_id, gate_name in (
        REQUIRED_GATE_DEFINITIONS
    ):
        passed = required_values[gate_id]

        diagnostic_code = (
            None
            if passed
            else (
                "matchup_shadow_quality_"
                + gate_name
                + "_failed"
            )
        )

        if diagnostic_code:
            validation_errors.append(
                diagnostic_code
            )

        results.append(
            _result(
                gate_id,
                gate_name,
                "required",
                passed=passed,
                triggered=not passed,
                observed=passed,
                expected=True,
                diagnostic_code=(
                    diagnostic_code
                ),
            )
        )

    partial_row_count = sum(
        1
        for row in rows
        if row.observability_status
        == "partial"
    )

    complete_row_count = sum(
        1
        for row in rows
        if row.observability_status
        == "complete"
    )

    invalid_row_count = sum(
        1
        for row in rows
        if row.observability_status
        == "invalid"
    )

    fallback_row_count = sum(
        1
        for row in rows
        if row.fallback_entry_count > 0
    )

    unknown_pitch_row_count = sum(
        1
        for row in rows
        if row.unknown_pitch_entry_count
        > 0
    )

    pitcher_only_row_count = sum(
        1
        for row in rows
        if row.pitcher_only_entry_count
        > 0
    )

    duplicate_row_count = (
        manifest.duplicate_row_count
        if manifest is not None
        else sum(
            duplicate.duplicate_count
            for duplicate in duplicates
        )
    )

    coverage_below_half_count = sum(
        1
        for row in rows
        if row.coverage_share < 0.5
    )

    warning_values = {
        "QG-W01": (
            partial_row_count > 0
        ),
        "QG-W02": (
            fallback_row_count > 0
        ),
        "QG-W03": (
            unknown_pitch_row_count > 0
        ),
        "QG-W04": (
            pitcher_only_row_count > 0
        ),
        "QG-W05": (
            duplicate_row_count > 0
        ),
        "QG-W06": (
            coverage_below_half_count > 0
        ),
    }

    warning_codes: list[str] = []

    for gate_id, gate_name in (
        WARNING_GATE_DEFINITIONS
    ):
        triggered = warning_values[gate_id]

        diagnostic_code = (
            (
                "matchup_shadow_quality_"
                + gate_name
            )
            if triggered
            else None
        )

        if diagnostic_code:
            warning_codes.append(
                diagnostic_code
            )

        results.append(
            _result(
                gate_id,
                gate_name,
                "warning",
                passed=True,
                triggered=triggered,
                observed=triggered,
                expected=False,
                diagnostic_code=(
                    diagnostic_code
                ),
            )
        )

    failed_gate_count = sum(
        1
        for result in results
        if result.gate_type == "required"
        and not result.passed
    )

    warning_gate_count = sum(
        1
        for result in results
        if result.gate_type == "warning"
        and result.triggered
    )

    coverage_values = [
        row.coverage_share
        for row in rows
    ]

    if coverage_values:
        minimum_coverage = min(
            coverage_values
        )
        mean_coverage = round(
            sum(coverage_values)
            / len(coverage_values),
            6,
        )
        maximum_coverage = max(
            coverage_values
        )
    else:
        minimum_coverage = None
        mean_coverage = None
        maximum_coverage = None

    diagnostics = list(warning_codes)

    if (
        dataset.dataset_status == "empty"
        and not rows
        and failed_gate_count == 0
    ):
        gate_status = "empty"
        reason = "quality_gate_empty"
        diagnostics.append(
            "matchup_shadow_quality_dataset_empty"
        )
    elif failed_gate_count > 0:
        gate_status = "fail"
        reason = "quality_gate_failed"
        diagnostics.append(
            "matchup_shadow_quality_reconciliation_failed"
        )
    elif warning_gate_count > 0:
        gate_status = "warn"
        reason = "quality_gate_warn"
    else:
        gate_status = "pass"
        reason = "quality_gate_passed"

    summary = ShadowDatasetQualityGateSummary(
        quality_gate_version=(
            QUALITY_GATE_VERSION
        ),
        evaluated_at_utc=evaluated_at,
        dataset_status=dataset.dataset_status,
        gate_status=gate_status,
        row_count=len(rows),
        partition_count=len(partitions),
        duplicate_row_count=(
            duplicate_row_count
        ),
        invalid_row_count=(
            invalid_row_count
        ),
        partial_row_count=(
            partial_row_count
        ),
        complete_row_count=(
            complete_row_count
        ),
        minimum_coverage_share=(
            minimum_coverage
        ),
        mean_coverage_share=mean_coverage,
        maximum_coverage_share=(
            maximum_coverage
        ),
        fallback_row_count=(
            fallback_row_count
        ),
        unknown_pitch_row_count=(
            unknown_pitch_row_count
        ),
        pitcher_only_row_count=(
            pitcher_only_row_count
        ),
        schema_fingerprint_matches=(
            schema_fingerprint_matches
        ),
        manifest_reconciles=(
            manifest_reconciles
        ),
        partition_manifest_reconciles=(
            partition_row_counts_reconcile
        ),
        row_identifiers_unique=(
            unique_row_ids
        ),
        source_versions_present=(
            source_versions_present
        ),
        failed_gate_count=(
            failed_gate_count
        ),
        warning_gate_count=(
            warning_gate_count
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

    return ShadowDatasetQualityGateReport(
        emitted=True,
        reason=reason,
        gate_status=gate_status,
        results=tuple(results),
        summary=summary,
        diagnostic_codes=(
            summary.diagnostic_codes
        ),
        validation_errors=(
            summary.validation_errors
        ),
        quality_gate_version=(
            QUALITY_GATE_VERSION
        ),
    )
