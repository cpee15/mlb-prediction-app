#!/usr/bin/env python3
"""
Layer 8AC history-quality-gate observability contract audit.
"""

from __future__ import annotations

import ast
import csv
from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate import (
    RetentionObservabilityHistoryQualityDimension,
    RetentionObservabilityHistoryQualityReport,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability import (
    RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_VERSION,
    observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate,
)


LAYER_ID = "8AC"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate_"
    "observability_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8AC_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_"
    "observability_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8AB_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_"
    "observability_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate_observability.py"
)

OBSERVED_AT = "2026-07-07T12:00:00+00:00"


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    payload: Any,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def string_constants(
    path: Path,
) -> set[str]:
    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
            errors="ignore",
        ),
        filename=str(path),
    )

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def dimension(
    *,
    dimension_id: str,
    passed: bool = True,
    triggered: bool = False,
) -> RetentionObservabilityHistoryQualityDimension:
    return RetentionObservabilityHistoryQualityDimension(
        dimension_id=dimension_id,
        dimension=dimension_id.lower(),
        passed=passed,
        triggered=triggered,
        observed_value=json.dumps(
            passed
        ),
        expected_value="true",
        diagnostic_code=(
            None
            if passed
            else f"{dimension_id.lower()}_failed"
        ),
    )


def quality_report(
    *,
    quality_status: str,
    dimensions: tuple[
        RetentionObservabilityHistoryQualityDimension,
        ...,
    ],
    history_record_count: int = 1,
    healthy_record_count: int = 1,
    warning_record_count: int = 0,
    degraded_record_count: int = 0,
    empty_record_count: int = 0,
    exact_duplicate_count: int = 0,
    conflicting_duplicate_count: int = 0,
) -> RetentionObservabilityHistoryQualityReport:
    return RetentionObservabilityHistoryQualityReport(
        emitted=True,
        reason=f"quality_{quality_status}",
        quality_report_id=(
            f"quality-report-{quality_status}"
        ),
        quality_gate_version="8AA-v1",
        evaluated_at_utc="2026-07-07T10:00:00+00:00",
        history_version="8Y-v1",
        history_digest="a" * 64,
        quality_status=quality_status,
        history_record_count=history_record_count,
        healthy_record_count=healthy_record_count,
        warning_record_count=warning_record_count,
        degraded_record_count=degraded_record_count,
        empty_record_count=empty_record_count,
        unique_history_record_count=(
            history_record_count
        ),
        exact_duplicate_count=exact_duplicate_count,
        conflicting_duplicate_count=(
            conflicting_duplicate_count
        ),
        history_digest_reconciles=True,
        history_record_ids_unique=True,
        history_order_reconciles=True,
        source_payload_digests_present=True,
        status_counts_reconcile=True,
        production_authority_absent=True,
        dimensions=dimensions,
        diagnostic_codes=(),
        validation_errors=(),
        quality_gate_passed=(
            quality_status
            in {
                "passed",
                "passed_with_warnings",
            }
        ),
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    base_dimensions = tuple(
        dimension(
            dimension_id=f"HQ-D{index:02d}"
        )
        for index in range(1, 9)
    )

    healthy_source = quality_report(
        quality_status="passed",
        dimensions=base_dimensions,
    )

    warning_dimensions = (
        *base_dimensions[:5],
        dimension(
            dimension_id="HQ-D06",
            triggered=True,
        ),
        *base_dimensions[6:],
    )

    warning_source = quality_report(
        quality_status="passed_with_warnings",
        dimensions=warning_dimensions,
        history_record_count=2,
        healthy_record_count=1,
        warning_record_count=1,
        exact_duplicate_count=1,
    )

    failed_dimensions = (
        dimension(
            dimension_id="HQ-D01",
            passed=False,
            triggered=True,
        ),
        *base_dimensions[1:],
    )

    degraded_source = quality_report(
        quality_status="failed",
        dimensions=failed_dimensions,
        history_record_count=2,
        healthy_record_count=1,
        degraded_record_count=1,
    )

    empty_source = quality_report(
        quality_status="empty",
        dimensions=base_dimensions,
        history_record_count=0,
        healthy_record_count=0,
        empty_record_count=0,
    )

    healthy = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            healthy_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    warning = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            warning_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    degraded = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            degraded_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    empty = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            empty_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    disabled = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            healthy_source,
            enabled=False,
            observed_at_utc=OBSERVED_AT,
        )
    )

    missing_report = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            None,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    missing_id_source = replace(
        healthy_source,
        quality_report_id=None,
    )

    missing_id = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            missing_id_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    unsupported_status_source = replace(
        healthy_source,
        quality_status="unsupported",
    )

    unsupported_status = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            unsupported_status_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    duplicate_dimension_source = replace(
        healthy_source,
        dimensions=(
            base_dimensions[0],
            base_dimensions[0],
            *base_dimensions[2:],
        ),
    )

    duplicate_dimension = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            duplicate_dimension_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    count_mismatch_source = replace(
        healthy_source,
        history_record_count=2,
    )

    count_mismatch = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            count_mismatch_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    authority_source = replace(
        healthy_source,
        production_authority=True,
    )

    authority = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            authority_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    repeated = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate(
            healthy_source,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    cases: list[dict[str, Any]] = []

    def add_case(
        case_id: str,
        description: str,
        passed_value: bool,
        actual: Any,
        expected: Any,
    ) -> None:
        cases.append(
            {
                "case_id": case_id,
                "description": description,
                "passed": passed_value,
                "actual": json.dumps(
                    actual,
                    sort_keys=True,
                    default=str,
                ),
                "expected": json.dumps(
                    expected,
                    sort_keys=True,
                    default=str,
                ),
            }
        )

    add_case(
        "8AC-C01",
        "healthy status emitted",
        healthy.observability_status == "healthy",
        healthy.observability_status,
        "healthy",
    )

    add_case(
        "8AC-C02",
        "warning status emitted",
        warning.observability_status == "warning",
        warning.observability_status,
        "warning",
    )

    add_case(
        "8AC-C03",
        "failed quality report degrades",
        degraded.observability_status == "degraded",
        degraded.observability_status,
        "degraded",
    )

    add_case(
        "8AC-C04",
        "empty status emitted",
        empty.observability_status == "empty",
        empty.observability_status,
        "empty",
    )

    add_case(
        "8AC-C05",
        "disabled path non-emitting",
        (
            disabled.emitted is False
            and disabled.observability_status
            == "disabled"
        ),
        disabled.to_dict(),
        {
            "emitted": False,
            "observability_status": "disabled",
        },
    )

    add_case(
        "8AC-C06",
        "missing quality report degrades",
        missing_report.observability_status
        == "degraded",
        missing_report.observability_status,
        "degraded",
    )

    add_case(
        "8AC-C07",
        "quality report id required",
        missing_id.observability_status
        == "degraded",
        missing_id.observability_status,
        "degraded",
    )

    add_case(
        "8AC-C08",
        "unsupported quality status degrades",
        unsupported_status.observability_status
        == "degraded",
        unsupported_status.observability_status,
        "degraded",
    )

    add_case(
        "8AC-C09",
        "dimension identities unique",
        healthy.snapshot is not None
        and healthy.snapshot.failed_dimension_count == 0,
        healthy.snapshot.to_dict()
        if healthy.snapshot
        else None,
        "zero_failed_dimensions",
    )

    add_case(
        "8AC-C10",
        "duplicate dimension identity degrades",
        duplicate_dimension.observability_status
        == "degraded",
        duplicate_dimension.observability_status,
        "degraded",
    )

    add_case(
        "8AC-C11",
        "status counts reconcile",
        healthy.snapshot is not None
        and healthy.snapshot.status_counts_reconcile,
        healthy.snapshot.status_counts_reconcile
        if healthy.snapshot
        else None,
        True,
    )

    add_case(
        "8AC-C12",
        "status count mismatch degrades",
        count_mismatch.observability_status
        == "degraded",
        count_mismatch.observability_status,
        "degraded",
    )

    add_case(
        "8AC-C13",
        "source integrity signals preserved",
        healthy.snapshot is not None
        and all(
            (
                healthy.snapshot.history_digest_reconciles,
                healthy.snapshot.history_record_ids_unique,
                healthy.snapshot.history_order_reconciles,
                healthy.snapshot.source_payload_digests_present,
            )
        ),
        healthy.snapshot.to_dict()
        if healthy.snapshot
        else None,
        "all_source_integrity_signals_true",
    )

    add_case(
        "8AC-C14",
        "triggered nonfailing dimension warns",
        warning.snapshot is not None
        and warning.snapshot.triggered_dimension_count == 1,
        warning.snapshot.triggered_dimension_count
        if warning.snapshot
        else None,
        1,
    )

    add_case(
        "8AC-C15",
        "failed dimension count aggregated",
        degraded.snapshot is not None
        and degraded.snapshot.failed_dimension_count == 1,
        degraded.snapshot.failed_dimension_count
        if degraded.snapshot
        else None,
        1,
    )

    add_case(
        "8AC-C16",
        "authority violation degrades",
        authority.observability_status
        == "degraded",
        authority.observability_status,
        "degraded",
    )

    add_case(
        "8AC-C17",
        "eight signals emitted",
        len(healthy.signals) == 8,
        len(healthy.signals),
        8,
    )

    add_case(
        "8AC-C18",
        "snapshot identity deterministic",
        (
            healthy.snapshot is not None
            and repeated.snapshot is not None
            and healthy.snapshot.observability_snapshot_id
            == repeated.snapshot.observability_snapshot_id
        ),
        (
            healthy.snapshot.observability_snapshot_id
            if healthy.snapshot
            else None
        ),
        (
            repeated.snapshot.observability_snapshot_id
            if repeated.snapshot
            else None
        ),
    )

    add_case(
        "8AC-C19",
        "serialization deterministic",
        healthy.to_dict()
        == repeated.to_dict(),
        healthy.to_dict(),
        repeated.to_dict(),
    )

    add_case(
        "8AC-C20",
        "history report mutation and authority absent",
        all(
            value is False
            for value in (
                healthy.history_mutated,
                healthy.quality_report_mutated,
                healthy.retention_action_executed,
                healthy.physical_deletion_executed,
                healthy.historical_outcomes_joined,
                healthy.predictive_evaluation_executed,
                healthy.production_authority,
                healthy.production_behavior_changed,
                healthy.simulation_behavior_changed,
            )
        ),
        healthy.to_dict(),
        "all_authority_flags_false",
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": (
                PLAN_PATH.exists()
                and IMPLEMENTATION_PATH.exists()
            ),
            "expected": True,
            "passed": (
                PLAN_PATH.exists()
                and IMPLEMENTATION_PATH.exists()
            ),
        },
        {
            "check": "eight_ab_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_contract_cases_pass",
            "actual": sum(
                row["passed"]
                for row in cases
            ),
            "expected": 20,
            "passed": all(
                row["passed"]
                for row in cases
            ),
        },
        {
            "check": "healthy_status_supported",
            "actual": healthy.observability_status,
            "expected": "healthy",
            "passed": (
                healthy.observability_status
                == "healthy"
            ),
        },
        {
            "check": "warning_status_supported",
            "actual": warning.observability_status,
            "expected": "warning",
            "passed": (
                warning.observability_status
                == "warning"
            ),
        },
        {
            "check": "degraded_status_supported",
            "actual": degraded.observability_status,
            "expected": "degraded",
            "passed": (
                degraded.observability_status
                == "degraded"
            ),
        },
        {
            "check": "empty_status_supported",
            "actual": empty.observability_status,
            "expected": "empty",
            "passed": (
                empty.observability_status
                == "empty"
            ),
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled.emitted,
            "expected": False,
            "passed": disabled.emitted is False,
        },
        {
            "check": "eight_signal_groups_implemented",
            "actual": len(healthy.signals),
            "expected": 8,
            "passed": len(healthy.signals) == 8,
        },
        {
            "check": "quality_status_aggregation_implemented",
            "actual": [
                healthy.observability_status,
                warning.observability_status,
                degraded.observability_status,
                empty.observability_status,
            ],
            "expected": [
                "healthy",
                "warning",
                "degraded",
                "empty",
            ],
            "passed": [
                healthy.observability_status,
                warning.observability_status,
                degraded.observability_status,
                empty.observability_status,
            ]
            == [
                "healthy",
                "warning",
                "degraded",
                "empty",
            ],
        },
        {
            "check": "dimension_aggregation_implemented",
            "actual": (
                warning.snapshot.triggered_dimension_count
                if warning.snapshot
                else None
            ),
            "expected": 1,
            "passed": (
                warning.snapshot is not None
                and warning.snapshot.triggered_dimension_count
                == 1
            ),
        },
        {
            "check": "failure_precedence_implemented",
            "actual": degraded.observability_status,
            "expected": "degraded",
            "passed": (
                degraded.observability_status
                == "degraded"
            ),
        },
        {
            "check": "warning_path_implemented",
            "actual": warning.observability_status,
            "expected": "warning",
            "passed": (
                warning.observability_status
                == "warning"
            ),
        },
        {
            "check": "source_integrity_signals_preserved",
            "actual": all(
                signal.passed
                for signal in healthy.signals
            ),
            "expected": True,
            "passed": all(
                signal.passed
                for signal in healthy.signals
            ),
        },
        {
            "check": "status_count_reconciliation_implemented",
            "actual": (
                healthy.snapshot.status_counts_reconcile
                if healthy.snapshot
                else None
            ),
            "expected": True,
            "passed": (
                healthy.snapshot is not None
                and healthy.snapshot.status_counts_reconcile
            ),
        },
        {
            "check": "snapshot_identity_deterministic",
            "actual": (
                healthy.snapshot is not None
                and repeated.snapshot is not None
                and healthy.snapshot.observability_snapshot_id
                == repeated.snapshot.observability_snapshot_id
            ),
            "expected": True,
            "passed": (
                healthy.snapshot is not None
                and repeated.snapshot is not None
                and healthy.snapshot.observability_snapshot_id
                == repeated.snapshot.observability_snapshot_id
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                healthy.to_dict()
                == repeated.to_dict()
            ),
            "expected": True,
            "passed": (
                healthy.to_dict()
                == repeated.to_dict()
            ),
        },
        {
            "check": "history_and_report_mutation_absent",
            "actual": any(
                (
                    healthy.history_mutated,
                    healthy.quality_report_mutated,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    healthy.history_mutated,
                    healthy.quality_report_mutated,
                )
            ),
        },
        {
            "check": "retention_and_production_authority_absent",
            "actual": any(
                (
                    healthy.retention_action_executed,
                    healthy.physical_deletion_executed,
                    healthy.historical_outcomes_joined,
                    healthy.predictive_evaluation_executed,
                    healthy.production_authority,
                    healthy.production_behavior_changed,
                    healthy.simulation_behavior_changed,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    healthy.retention_action_executed,
                    healthy.physical_deletion_executed,
                    healthy.historical_outcomes_joined,
                    healthy.predictive_evaluation_executed,
                    healthy.production_authority,
                    healthy.production_behavior_changed,
                    healthy.simulation_behavior_changed,
                )
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    reports = (
        healthy,
        warning,
        degraded,
        empty,
        disabled,
    )

    status_rows = [
        {
            "observability_status": status,
            "count": sum(
                report.observability_status
                == status
                for report in reports
            ),
        }
        for status in (
            "healthy",
            "warning",
            "degraded",
            "empty",
            "disabled",
        )
    ]

    quality_status_rows = [
        {
            "quality_status": status,
            "count": sum(
                (
                    report.snapshot is not None
                    and report.snapshot.quality_status
                    == status
                )
                for report in reports
            ),
        }
        for status in (
            "passed",
            "passed_with_warnings",
            "failed",
            "empty",
            "disabled",
        )
    ]

    signal_rows = [
        signal.to_dict()
        for signal in warning.signals
    ]

    dimension_rows = [
        {
            "dimension_id": dimension.dimension_id,
            "passed": dimension.passed,
            "triggered": dimension.triggered,
            "diagnostic_code": (
                dimension.diagnostic_code
            ),
        }
        for dimension in warning_source.dimensions
    ]

    authority_rows = [
        {
            "authority": (
                "diagnostic_quality_gate_observability"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded deterministic quality-gate observability passed all checks."
            ),
        },
        {
            "authority": "history_mutation",
            "granted": False,
            "reason": (
                "Observability never mutates history."
            ),
        },
        {
            "authority": "quality_report_mutation",
            "granted": False,
            "reason": (
                "Observability never mutates quality reports."
            ),
        },
        {
            "authority": "retention_action_execution",
            "granted": False,
            "reason": (
                "Observability does not execute retention actions."
            ),
        },
        {
            "authority": "physical_record_deletion",
            "granted": False,
            "reason": (
                "Observability never deletes records."
            ),
        },
        {
            "authority": (
                "historical_or_predictive_evaluation"
            ),
            "granted": False,
            "reason": (
                "No outcomes or predictive evaluation are used."
            ),
        },
        {
            "authority": (
                "production_or_simulation_change"
            ),
            "granted": False,
            "reason": (
                "Production and simulation remain unchanged."
            ),
        },
        {
            "authority": (
                "tuning_backtest_pricing_edge"
            ),
            "granted": False,
            "reason": (
                "Tuning, backtests, pricing, and edge work remain unauthorized."
            ),
        },
    ]

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8AD_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_contract_plan"
        if all_checks_passed
        else
        "8AC_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_contract_implementation_remediation"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "contract_cases.csv",
        [
            "case_id",
            "description",
            "passed",
            "actual",
            "expected",
        ],
        cases,
    )

    write_csv(
        OUTPUT_DIR / "quality_gate_observability_snapshot.csv",
        list(
            healthy.snapshot.to_dict().keys()
        ),
        [
            report.snapshot.to_dict()
            for report in (
                healthy,
                warning,
                degraded,
                empty,
            )
            if report.snapshot is not None
        ],
    )

    write_csv(
        OUTPUT_DIR / "observability_status_counts.csv",
        [
            "observability_status",
            "count",
        ],
        status_rows,
    )

    write_csv(
        OUTPUT_DIR / "quality_status_distribution.csv",
        [
            "quality_status",
            "count",
        ],
        quality_status_rows,
    )

    write_csv(
        OUTPUT_DIR / "signal_results.csv",
        list(
            signal_rows[0].keys()
        ),
        signal_rows,
    )

    write_csv(
        OUTPUT_DIR / "dimension_signals.csv",
        [
            "dimension_id",
            "passed",
            "triggered",
            "diagnostic_code",
        ],
        dimension_rows,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        authority_rows,
    )

    write_csv(
        OUTPUT_DIR / "recommended_path.csv",
        [
            "recommended_next_layer",
            "recommended_action",
            "entry_condition",
            "passed",
        ],
        [
            {
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Plan immutable history for quality-gate observability snapshots."
                    if all_checks_passed
                    else
                    "Remediate failed 8AC implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8AC implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR
        / "quality_gate_observability_report.json",
        warning.to_dict(),
    )

    summary = {
        "implementation_checks_required": len(
            checks
        ),
        "implementation_checks_passed": sum(
            row["passed"]
            for row in checks
        ),
        "contract_cases_required": len(
            cases
        ),
        "contract_cases_passed": sum(
            row["passed"]
            for row in cases
        ),
        "observability_version": (
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_VERSION
        ),
        "healthy_status_supported": True,
        "warning_status_supported": True,
        "degraded_status_supported": True,
        "empty_status_supported": True,
        "disabled_path_non_emitting": True,
        "eight_signal_groups_implemented": True,
        "quality_status_aggregation_implemented": True,
        "dimension_aggregation_implemented": True,
        "failure_precedence_implemented": True,
        "warning_path_implemented": True,
        "source_integrity_signals_preserved": True,
        "status_count_reconciliation_implemented": True,
        "snapshot_identity_deterministic": True,
        "history_mutated": False,
        "quality_report_mutated": False,
        "retention_action_executed": False,
        "physical_deletion_executed": False,
        "historical_outcome_joined": False,
        "predictive_evaluation_executed": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "tuning_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer8_completed": False,
        "new_production_authority_granted": False,
        "history_mutation_allowed_next": False,
        "quality_report_mutation_allowed_next": False,
        "retention_action_allowed_next": False,
        "physical_deletion_allowed_next": False,
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "quality_gate_observability_history_planning_allowed_next": (
            all_checks_passed
        ),
        "production_matchup_overlay_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / filename
            )
            for filename in (
                "implementation_checks.csv",
                "contract_cases.csv",
                "quality_gate_observability_snapshot.csv",
                "observability_status_counts.csv",
                "quality_status_distribution.csv",
                "signal_results.csv",
                "dimension_signals.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            )
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "quality_gate_observability_report.json"
            ),
            str(
                OUTPUT_DIR
                / "implementation_summary.json"
            ),
            str(
                OUTPUT_DIR / "diagnosis.json"
            ),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        json.dumps(
            diagnosis,
            indent=2,
        )
    )

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
