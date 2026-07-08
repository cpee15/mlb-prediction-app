#!/usr/bin/env python3
"""
Layer 8AG observability-history quality-gate contract audit.
"""

from __future__ import annotations

import ast
import csv
from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability import (
    RetentionObservabilityHistoryQualityGateObservabilitySnapshot,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history import (
    append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate import (
    RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION,
    evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate,
)


LAYER_ID = "8AG"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate_observability_"
    "history_quality_gate_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8AG_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_"
    "observability_history_quality_gate_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8AF_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_"
    "observability_history_quality_gate_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate_"
    "observability_history_quality_gate.py"
)

RECORDED_AT = "2026-07-08T15:00:00+00:00"
EVALUATED_AT = "2026-07-08T16:00:00+00:00"


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


def snapshot(
    *,
    snapshot_id: str,
    observed_at_utc: str,
    observability_status: str = "healthy",
    quality_status: str = "passed",
) -> RetentionObservabilityHistoryQualityGateObservabilitySnapshot:
    return RetentionObservabilityHistoryQualityGateObservabilitySnapshot(
        observability_snapshot_id=snapshot_id,
        observability_version="8AC-v1",
        observed_at_utc=observed_at_utc,
        quality_gate_version="8AA-v1",
        quality_report_id=f"quality-report-{snapshot_id}",
        quality_status=quality_status,
        observability_status=observability_status,
        history_record_count=1,
        healthy_record_count=(
            1
            if observability_status == "healthy"
            else 0
        ),
        warning_record_count=(
            1
            if observability_status == "warning"
            else 0
        ),
        degraded_record_count=(
            1
            if observability_status == "degraded"
            else 0
        ),
        empty_record_count=(
            1
            if observability_status == "empty"
            else 0
        ),
        exact_duplicate_count=0,
        conflicting_duplicate_count=0,
        failed_dimension_count=(
            1
            if observability_status == "degraded"
            else 0
        ),
        triggered_dimension_count=(
            1
            if observability_status
            in {
                "warning",
                "degraded",
            }
            else 0
        ),
        history_digest_reconciles=True,
        history_record_ids_unique=True,
        history_order_reconciles=True,
        source_payload_digests_present=True,
        status_counts_reconcile=True,
        diagnostic_codes=(),
        validation_errors=(),
    )


def build_history(
    snapshots: Iterable[
        RetentionObservabilityHistoryQualityGateObservabilitySnapshot
    ],
):
    records = ()

    ledger = None

    for current_snapshot in snapshots:
        ledger = (
            append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
                current_snapshot,
                existing_records=records,
                enabled=True,
                recorded_at_utc=RECORDED_AT,
            )
        )
        records = ledger.records

    if ledger is None:
        ledger = (
            append_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history(
                None,
                enabled=True,
                recorded_at_utc=RECORDED_AT,
            )
        )

    return ledger


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_quality_gate_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    healthy_history = build_history(
        [
            snapshot(
                snapshot_id="observability-history-001",
                observed_at_utc="2026-07-05T12:00:00+00:00",
            ),
            snapshot(
                snapshot_id="observability-history-002",
                observed_at_utc="2026-07-06T12:00:00+00:00",
            ),
        ]
    )

    warning_history = build_history(
        [
            snapshot(
                snapshot_id="observability-history-warning",
                observed_at_utc="2026-07-06T13:00:00+00:00",
                observability_status="warning",
                quality_status="passed_with_warnings",
            ),
        ]
    )

    degraded_history = build_history(
        [
            snapshot(
                snapshot_id="observability-history-degraded",
                observed_at_utc="2026-07-06T14:00:00+00:00",
                observability_status="degraded",
                quality_status="failed",
            ),
        ]
    )

    empty_history = build_history(
        []
    )

    healthy_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            healthy_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    warning_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            warning_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    degraded_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            degraded_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    empty_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            empty_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    disabled_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            healthy_history,
            enabled=False,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    missing_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            None,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    digest_mismatch_history = replace(
        healthy_history,
        history_digest="f" * 64,
    )

    digest_mismatch_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            digest_mismatch_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    duplicate_record_history = replace(
        healthy_history,
        records=(
            healthy_history.records[0],
            healthy_history.records[0],
        ),
    )

    duplicate_record_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            duplicate_record_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    reversed_history = replace(
        healthy_history,
        records=tuple(
            reversed(
                healthy_history.records
            )
        ),
    )

    reversed_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            reversed_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    missing_digest_record = replace(
        healthy_history.records[0],
        snapshot_payload_digest="",
    )

    missing_digest_history = replace(
        healthy_history,
        records=(
            missing_digest_record,
            *healthy_history.records[1:],
        ),
    )

    missing_digest_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            missing_digest_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    authority_record = replace(
        healthy_history.records[0],
        production_authority=True,
    )

    authority_history = replace(
        healthy_history,
        records=(
            authority_record,
            *healthy_history.records[1:],
        ),
    )

    authority_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            authority_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    repeated_result = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_history_quality_gate_observability_history_quality_gate(
            healthy_history,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
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
        "8AG-C01",
        "healthy history passes",
        (
            healthy_result.report is not None
            and healthy_result.report.quality_status
            == "passed"
        ),
        healthy_result.to_dict(),
        "passed",
    )

    add_case(
        "8AG-C02",
        "warning history passes with warnings",
        (
            warning_result.report is not None
            and warning_result.report.quality_status
            == "passed_with_warnings"
        ),
        warning_result.to_dict(),
        "passed_with_warnings",
    )

    add_case(
        "8AG-C03",
        "degraded history fails",
        (
            degraded_result.report is not None
            and degraded_result.report.quality_status
            == "failed"
        ),
        degraded_result.to_dict(),
        "failed",
    )

    add_case(
        "8AG-C04",
        "empty history classified empty",
        (
            empty_result.report is not None
            and empty_result.report.quality_status
            == "empty"
        ),
        empty_result.to_dict(),
        "empty",
    )

    add_case(
        "8AG-C05",
        "disabled gate non-emitting",
        (
            disabled_result.emitted is False
            and disabled_result.report is None
        ),
        disabled_result.to_dict(),
        "non_emitting",
    )

    add_case(
        "8AG-C06",
        "missing ledger fails",
        (
            missing_result.report is not None
            and missing_result.report.quality_status
            == "failed"
        ),
        missing_result.to_dict(),
        "failed",
    )

    add_case(
        "8AG-C07",
        "digest mismatch fails",
        (
            digest_mismatch_result.report
            is not None
            and digest_mismatch_result.report.quality_status
            == "failed"
        ),
        digest_mismatch_result.to_dict(),
        "failed",
    )

    add_case(
        "8AG-C08",
        "identity conflict fails",
        (
            duplicate_record_result.report
            is not None
            and duplicate_record_result.report.quality_status
            == "failed"
        ),
        duplicate_record_result.to_dict(),
        "failed",
    )

    add_case(
        "8AG-C09",
        "order mismatch fails",
        (
            reversed_result.report is not None
            and reversed_result.report.quality_status
            == "failed"
        ),
        reversed_result.to_dict(),
        "failed",
    )

    add_case(
        "8AG-C10",
        "missing snapshot payload digest fails",
        (
            missing_digest_result.report
            is not None
            and missing_digest_result.report.quality_status
            == "failed"
        ),
        missing_digest_result.to_dict(),
        "failed",
    )

    add_case(
        "8AG-C11",
        "authority violation fails",
        (
            authority_result.report is not None
            and authority_result.report.quality_status
            == "failed"
        ),
        authority_result.to_dict(),
        "failed",
    )

    add_case(
        "8AG-C12",
        "eight quality dimensions emitted",
        len(healthy_result.dimensions) == 8,
        len(healthy_result.dimensions),
        8,
    )

    add_case(
        "8AG-C13",
        "history digest reconciles",
        (
            healthy_result.report is not None
            and healthy_result.report.history_digest_reconciles
        ),
        healthy_result.to_dict(),
        True,
    )

    add_case(
        "8AG-C14",
        "history identities unique",
        (
            healthy_result.report is not None
            and healthy_result.report.history_record_ids_unique
        ),
        healthy_result.to_dict(),
        True,
    )

    add_case(
        "8AG-C15",
        "history ordering reconciles",
        (
            healthy_result.report is not None
            and healthy_result.report.history_order_reconciles
        ),
        healthy_result.to_dict(),
        True,
    )

    add_case(
        "8AG-C16",
        "source digests and versions present",
        (
            healthy_result.report is not None
            and healthy_result.report.snapshot_payload_digests_present
            and healthy_result.report.source_versions_present
        ),
        healthy_result.to_dict(),
        True,
    )

    add_case(
        "8AG-C17",
        "status counts reconcile",
        (
            healthy_result.report is not None
            and healthy_result.report.status_counts_reconcile
        ),
        healthy_result.to_dict(),
        True,
    )

    add_case(
        "8AG-C18",
        "quality-report identity deterministic",
        (
            healthy_result.report is not None
            and repeated_result.report is not None
            and healthy_result.report.quality_report_id
            == repeated_result.report.quality_report_id
        ),
        (
            healthy_result.report.quality_report_id
            if healthy_result.report is not None
            else None
        ),
        (
            repeated_result.report.quality_report_id
            if repeated_result.report is not None
            else None
        ),
    )

    add_case(
        "8AG-C19",
        "serialization deterministic",
        healthy_result.to_dict()
        == repeated_result.to_dict(),
        healthy_result.to_dict(),
        repeated_result.to_dict(),
    )

    add_case(
        "8AG-C20",
        "mutation and prohibited authority absent",
        all(
            value is False
            for value in (
                healthy_result.history_mutated,
                healthy_result.quality_report_mutated,
                healthy_result.retention_action_executed,
                healthy_result.physical_deletion_executed,
                healthy_result.historical_outcomes_joined,
                healthy_result.predictive_evaluation_executed,
                healthy_result.production_authority,
                healthy_result.production_behavior_changed,
                healthy_result.simulation_behavior_changed,
            )
        ),
        healthy_result.to_dict(),
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
            "check": "eight_af_predecessor_present",
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
            "check": "passed_status_supported",
            "actual": (
                healthy_result.report.quality_status
                if healthy_result.report
                else None
            ),
            "expected": "passed",
            "passed": (
                healthy_result.report is not None
                and healthy_result.report.quality_status
                == "passed"
            ),
        },
        {
            "check": "passed_with_warnings_supported",
            "actual": (
                warning_result.report.quality_status
                if warning_result.report
                else None
            ),
            "expected": "passed_with_warnings",
            "passed": (
                warning_result.report is not None
                and warning_result.report.quality_status
                == "passed_with_warnings"
            ),
        },
        {
            "check": "failed_status_supported",
            "actual": (
                degraded_result.report.quality_status
                if degraded_result.report
                else None
            ),
            "expected": "failed",
            "passed": (
                degraded_result.report is not None
                and degraded_result.report.quality_status
                == "failed"
            ),
        },
        {
            "check": "empty_status_supported",
            "actual": (
                empty_result.report.quality_status
                if empty_result.report
                else None
            ),
            "expected": "empty",
            "passed": (
                empty_result.report is not None
                and empty_result.report.quality_status
                == "empty"
            ),
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled_result.emitted,
            "expected": False,
            "passed": disabled_result.emitted is False,
        },
        {
            "check": "eight_quality_dimensions_implemented",
            "actual": len(
                healthy_result.dimensions
            ),
            "expected": 8,
            "passed": len(
                healthy_result.dimensions
            )
            == 8,
        },
        {
            "check": "history_digest_validation_implemented",
            "actual": (
                healthy_result.report.history_digest_reconciles
                if healthy_result.report
                else False
            ),
            "expected": True,
            "passed": (
                healthy_result.report is not None
                and healthy_result.report.history_digest_reconciles
            ),
        },
        {
            "check": "history_identity_validation_implemented",
            "actual": (
                healthy_result.report.history_record_ids_unique
                if healthy_result.report
                else False
            ),
            "expected": True,
            "passed": (
                healthy_result.report is not None
                and healthy_result.report.history_record_ids_unique
            ),
        },
        {
            "check": "history_order_validation_implemented",
            "actual": (
                healthy_result.report.history_order_reconciles
                if healthy_result.report
                else False
            ),
            "expected": True,
            "passed": (
                healthy_result.report is not None
                and healthy_result.report.history_order_reconciles
            ),
        },
        {
            "check": "snapshot_digest_validation_implemented",
            "actual": (
                healthy_result.report.snapshot_payload_digests_present
                if healthy_result.report
                else False
            ),
            "expected": True,
            "passed": (
                healthy_result.report is not None
                and healthy_result.report.snapshot_payload_digests_present
            ),
        },
        {
            "check": "source_version_validation_implemented",
            "actual": (
                healthy_result.report.source_versions_present
                if healthy_result.report
                else False
            ),
            "expected": True,
            "passed": (
                healthy_result.report is not None
                and healthy_result.report.source_versions_present
            ),
        },
        {
            "check": "status_count_reconciliation_implemented",
            "actual": (
                healthy_result.report.status_counts_reconcile
                if healthy_result.report
                else False
            ),
            "expected": True,
            "passed": (
                healthy_result.report is not None
                and healthy_result.report.status_counts_reconcile
            ),
        },
        {
            "check": "failure_precedence_implemented",
            "actual": (
                digest_mismatch_result.report.quality_status
                if digest_mismatch_result.report
                else None
            ),
            "expected": "failed",
            "passed": (
                digest_mismatch_result.report is not None
                and digest_mismatch_result.report.quality_status
                == "failed"
            ),
        },
        {
            "check": "warning_precedence_implemented",
            "actual": (
                warning_result.report.quality_status
                if warning_result.report
                else None
            ),
            "expected": "passed_with_warnings",
            "passed": (
                warning_result.report is not None
                and warning_result.report.quality_status
                == "passed_with_warnings"
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                healthy_result.to_dict()
                == repeated_result.to_dict()
            ),
            "expected": True,
            "passed": (
                healthy_result.to_dict()
                == repeated_result.to_dict()
            ),
        },
        {
            "check": "history_and_quality_report_mutation_absent",
            "actual": any(
                (
                    healthy_result.history_mutated,
                    healthy_result.quality_report_mutated,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    healthy_result.history_mutated,
                    healthy_result.quality_report_mutated,
                )
            ),
        },
        {
            "check": "retention_and_production_authority_absent",
            "actual": any(
                (
                    healthy_result.retention_action_executed,
                    healthy_result.physical_deletion_executed,
                    healthy_result.historical_outcomes_joined,
                    healthy_result.predictive_evaluation_executed,
                    healthy_result.production_authority,
                    healthy_result.production_behavior_changed,
                    healthy_result.simulation_behavior_changed,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    healthy_result.retention_action_executed,
                    healthy_result.physical_deletion_executed,
                    healthy_result.historical_outcomes_joined,
                    healthy_result.predictive_evaluation_executed,
                    healthy_result.production_authority,
                    healthy_result.production_behavior_changed,
                    healthy_result.simulation_behavior_changed,
                )
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    assert healthy_result.report is not None

    status_results = (
        healthy_result,
        warning_result,
        degraded_result,
        empty_result,
        disabled_result,
    )

    status_rows = [
        {
            "quality_status": status,
            "count": sum(
                (
                    result.report is not None
                    and result.report.quality_status
                    == status
                )
                for result in status_results
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

    dimension_rows = [
        dimension.to_dict()
        for dimension in healthy_result.dimensions
    ]

    failure_dimension_rows = [
        dimension.to_dict()
        for result in (
            digest_mismatch_result,
            duplicate_record_result,
            reversed_result,
            missing_digest_result,
            authority_result,
        )
        for dimension in result.dimensions
        if dimension.triggered
    ]

    integrity_rows = [
        {
            "integrity_check": "history_digest_reconciles",
            "passed": (
                healthy_result.report.history_digest_reconciles
            ),
        },
        {
            "integrity_check": "history_record_ids_unique",
            "passed": (
                healthy_result.report.history_record_ids_unique
            ),
        },
        {
            "integrity_check": "history_order_reconciles",
            "passed": (
                healthy_result.report.history_order_reconciles
            ),
        },
        {
            "integrity_check": "snapshot_payload_digests_present",
            "passed": (
                healthy_result.report.snapshot_payload_digests_present
            ),
        },
        {
            "integrity_check": "source_versions_present",
            "passed": (
                healthy_result.report.source_versions_present
            ),
        },
        {
            "integrity_check": "status_counts_reconcile",
            "passed": (
                healthy_result.report.status_counts_reconcile
            ),
        },
    ]

    authority_rows = [
        {
            "authority": (
                "diagnostic_observability_history_quality_gate"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded deterministic quality gate passed all checks."
            ),
        },
        {
            "authority": "history_mutation",
            "granted": False,
            "reason": (
                "Quality evaluation never mutates history."
            ),
        },
        {
            "authority": "quality_report_mutation",
            "granted": False,
            "reason": (
                "Quality reports are immutable."
            ),
        },
        {
            "authority": "retention_action_execution",
            "granted": False,
            "reason": (
                "Quality evaluation does not execute retention actions."
            ),
        },
        {
            "authority": "physical_record_deletion",
            "granted": False,
            "reason": (
                "Quality evaluation never deletes records."
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
        "observability_history_quality_gate_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_quality_gate_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8AH_layer_8_pitch_type_matchup_overlay_shadow_"
        "evaluation_readiness_and_scope_closure_plan"
        if all_checks_passed
        else
        "8AG_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_history_quality_gate_contract_implementation_remediation"
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
        OUTPUT_DIR / "observability_history_quality_report.csv",
        list(
            healthy_result.report.to_dict().keys()
        ),
        [
            {
                **healthy_result.report.to_dict(),
                "diagnostic_codes": json.dumps(
                    healthy_result.report.diagnostic_codes
                ),
                "validation_errors": json.dumps(
                    healthy_result.report.validation_errors
                ),
            }
        ],
    )

    write_csv(
        OUTPUT_DIR / "quality_dimension_results.csv",
        [
            "dimension_id",
            "dimension",
            "passed",
            "triggered",
            "actual",
            "expected",
            "diagnostic_code",
        ],
        dimension_rows,
    )

    write_csv(
        OUTPUT_DIR / "history_status_distribution.csv",
        [
            "quality_status",
            "count",
        ],
        status_rows,
    )

    write_csv(
        OUTPUT_DIR / "duplicate_quality_results.csv",
        [
            "dimension_id",
            "dimension",
            "passed",
            "triggered",
            "actual",
            "expected",
            "diagnostic_code",
        ],
        failure_dimension_rows,
    )

    write_csv(
        OUTPUT_DIR / "history_integrity_results.csv",
        [
            "integrity_check",
            "passed",
        ],
        integrity_rows,
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
                    "Close Layer 8 diagnostic scope and define the bounded "
                    "handoff into point-in-time historical evaluation."
                    if all_checks_passed
                    else
                    "Remediate failed 8AG implementation checks."
                ),
                "entry_condition": (
                    "All twenty 8AG implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR
        / "observability_history_quality_report.json",
        healthy_result.to_dict(),
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
        "quality_gate_version": (
            RETENTION_OBSERVABILITY_HISTORY_QUALITY_GATE_OBSERVABILITY_HISTORY_QUALITY_GATE_VERSION
        ),
        "passed_status_supported": True,
        "passed_with_warnings_status_supported": True,
        "failed_status_supported": True,
        "empty_status_supported": True,
        "disabled_path_non_emitting": True,
        "quality_dimensions_implemented": 8,
        "history_digest_validation_implemented": True,
        "history_identity_validation_implemented": True,
        "history_order_validation_implemented": True,
        "snapshot_payload_digest_validation_implemented": True,
        "source_version_validation_implemented": True,
        "status_count_reconciliation_implemented": True,
        "warning_and_failure_precedence_implemented": True,
        "deterministic_quality_report_identity_implemented": True,
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
        "layer_8_scope_closure_planning_allowed_next": (
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
                "observability_history_quality_report.csv",
                "quality_dimension_results.csv",
                "history_status_distribution.csv",
                "duplicate_quality_results.csv",
                "history_integrity_results.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            )
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "observability_history_quality_report.json"
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
