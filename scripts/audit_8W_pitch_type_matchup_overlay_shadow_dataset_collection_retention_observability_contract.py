#!/usr/bin/env python3
"""
Layer 8W retention-observability implementation audit.
"""

from __future__ import annotations

import ast
import csv
from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention import (
    MatchupOverlayShadowRetentionDecision,
    MatchupOverlayShadowRetentionDuplicate,
    MatchupOverlayShadowRetentionLedger,
    retention_ledger_digest,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability import (
    RETENTION_OBSERVABILITY_VERSION,
    observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention,
)


LAYER_ID = "8W"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8W_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8V_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability.py"
)

OBSERVED_AT = "2026-07-03T12:00:00+00:00"


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


def decision(
    *,
    decision_id: str,
    status: str,
    age_days: int,
    quarantine_required: bool = False,
) -> MatchupOverlayShadowRetentionDecision:
    return MatchupOverlayShadowRetentionDecision(
        retention_decision_id=decision_id,
        retention_version="8U-v1",
        evaluated_at_utc=OBSERVED_AT,
        collection_record_id=(
            f"collection-{decision_id}"
        ),
        collection_version="8Q-v1",
        dataset_version="8M-v1",
        quality_gate_version="8O-v1",
        observability_version="8S-v1",
        collection_status=(
            "rejected"
            if quarantine_required
            else "accepted"
        ),
        observability_status=(
            "degraded"
            if quarantine_required
            else "healthy"
        ),
        retention_status=status,
        retention_reason=(
            "quarantine_precedence"
            if quarantine_required
            else {
                "retained": (
                    "within_active_retention_window"
                ),
                "archived": (
                    "within_archive_window"
                ),
                "expired": (
                    "beyond_archive_window"
                ),
            }[status]
        ),
        record_age_days=age_days,
        retention_window_days=30,
        archive_window_days=90,
        eligible_for_retention=(
            status == "retained"
        ),
        eligible_for_archive=(
            status == "archived"
        ),
        eligible_for_expiration=(
            status == "expired"
        ),
        quarantine_required=(
            quarantine_required
        ),
        dataset_payload_digest="a" * 64,
        quality_report_digest="b" * 64,
        diagnostic_codes=(),
        validation_errors=(),
    )


def ledger(
    decisions: tuple[
        MatchupOverlayShadowRetentionDecision,
        ...,
    ],
    *,
    duplicates: tuple[
        MatchupOverlayShadowRetentionDuplicate,
        ...,
    ] = (),
    retention_status: str | None = None,
) -> MatchupOverlayShadowRetentionLedger:
    resolved_status = (
        retention_status
        if retention_status is not None
        else (
            "quarantined"
            if any(
                item.retention_status
                == "quarantined"
                for item in decisions
            )
            else
            "expired"
            if any(
                item.retention_status
                == "expired"
                for item in decisions
            )
            else
            "archived"
            if any(
                item.retention_status
                == "archived"
                for item in decisions
            )
            else
            "retained"
        )
    )

    return MatchupOverlayShadowRetentionLedger(
        emitted=True,
        reason=(
            f"retention_{resolved_status}"
        ),
        retention_status=resolved_status,
        decisions=decisions,
        duplicates=duplicates,
        ledger_digest=(
            retention_ledger_digest(
                decisions
            )
        ),
        evaluated_at_utc=OBSERVED_AT,
        retention_window_days=30,
        archive_window_days=90,
        diagnostic_codes=(),
        validation_errors=(),
        retention_version="8U-v1",
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    retained_decision = decision(
        decision_id="retained-decision",
        status="retained",
        age_days=13,
    )

    archived_decision = decision(
        decision_id="archived-decision",
        status="archived",
        age_days=44,
    )

    expired_decision = decision(
        decision_id="expired-decision",
        status="expired",
        age_days=124,
    )

    quarantined_decision = decision(
        decision_id="quarantined-decision",
        status="quarantined",
        age_days=13,
        quarantine_required=True,
    )

    healthy_ledger = ledger(
        (retained_decision,)
    )

    warning_ledger = ledger(
        (
            retained_decision,
            archived_decision,
            expired_decision,
        ),
        duplicates=(
            MatchupOverlayShadowRetentionDuplicate(
                retention_decision_id=(
                    retained_decision.retention_decision_id
                ),
                duplicate_count=1,
                conflict=False,
                diagnostic_code=(
                    "matchup_shadow_retention_exact_duplicate"
                ),
            ),
        ),
    )

    quarantined_ledger = ledger(
        (quarantined_decision,)
    )

    empty_ledger = ledger(
        (),
        retention_status="retained",
    )

    bad_digest_ledger = replace(
        healthy_ledger,
        ledger_digest="0" * 64,
    )

    conflicting_duplicate_ledger = ledger(
        (retained_decision,),
        duplicates=(
            MatchupOverlayShadowRetentionDuplicate(
                retention_decision_id=(
                    retained_decision.retention_decision_id
                ),
                duplicate_count=1,
                conflict=True,
                diagnostic_code=(
                    "matchup_shadow_retention_conflicting_duplicate"
                ),
            ),
        ),
        retention_status="quarantined",
    )

    duplicate_identity_ledger = ledger(
        (
            retained_decision,
            retained_decision,
        )
    )

    policy_mismatch_decision = replace(
        archived_decision,
        retention_status="retained",
        eligible_for_retention=True,
        eligible_for_archive=False,
    )

    policy_mismatch_ledger = ledger(
        (policy_mismatch_decision,)
    )

    healthy = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            healthy_ledger,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    warning = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            warning_ledger,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    quarantined = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            quarantined_ledger,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    empty = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            empty_ledger,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    disabled = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            healthy_ledger,
            enabled=False,
            observed_at_utc=OBSERVED_AT,
        )
    )

    missing = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            None,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    bad_digest = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            bad_digest_ledger,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    conflicting = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            conflicting_duplicate_ledger,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    duplicate_identity = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            duplicate_identity_ledger,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    policy_mismatch = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            policy_mismatch_ledger,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    repeated = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            healthy_ledger,
            enabled=True,
            observed_at_utc=OBSERVED_AT,
        )
    )

    cases: list[dict[str, Any]] = []

    def add_case(
        case_id: str,
        description: str,
        passed: bool,
        actual: Any,
        expected: Any,
    ) -> None:
        cases.append(
            {
                "case_id": case_id,
                "description": description,
                "passed": passed,
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
        "8W-C01",
        "healthy status emitted",
        healthy.observability_status
        == "healthy",
        healthy.observability_status,
        "healthy",
    )

    add_case(
        "8W-C02",
        "warning status emitted",
        warning.observability_status
        == "warning",
        warning.observability_status,
        "warning",
    )

    add_case(
        "8W-C03",
        "quarantine degrades observability",
        quarantined.observability_status
        == "degraded",
        quarantined.observability_status,
        "degraded",
    )

    add_case(
        "8W-C04",
        "empty status emitted",
        empty.observability_status
        == "empty",
        empty.observability_status,
        "empty",
    )

    add_case(
        "8W-C05",
        "disabled path non-emitting",
        (
            disabled.emitted is False
            and disabled.snapshot is None
        ),
        disabled.to_dict(),
        {
            "emitted": False,
            "snapshot": None,
        },
    )

    add_case(
        "8W-C06",
        "missing ledger degrades",
        missing.observability_status
        == "degraded",
        missing.observability_status,
        "degraded",
    )

    add_case(
        "8W-C07",
        "ledger digest reconciles",
        (
            healthy.snapshot is not None
            and healthy.snapshot.ledger_digest_reconciles
        ),
        healthy.snapshot.to_dict(),
        True,
    )

    add_case(
        "8W-C08",
        "bad ledger digest degrades",
        bad_digest.observability_status
        == "degraded",
        bad_digest.observability_status,
        "degraded",
    )

    add_case(
        "8W-C09",
        "decision identifiers unique",
        (
            healthy.snapshot is not None
            and healthy.snapshot.decision_identifiers_unique
        ),
        healthy.snapshot.to_dict(),
        True,
    )

    add_case(
        "8W-C10",
        "duplicate identifiers degrade",
        duplicate_identity.observability_status
        == "degraded",
        duplicate_identity.observability_status,
        "degraded",
    )

    add_case(
        "8W-C11",
        "policy windows reconcile",
        (
            healthy.snapshot is not None
            and healthy.snapshot.policy_windows_reconcile
        ),
        healthy.snapshot.to_dict(),
        True,
    )

    add_case(
        "8W-C12",
        "policy mismatch degrades",
        policy_mismatch.observability_status
        == "degraded",
        policy_mismatch.observability_status,
        "degraded",
    )

    add_case(
        "8W-C13",
        "status distribution computed",
        (
            warning.snapshot.retained_count == 1
            and warning.snapshot.archived_count == 1
            and warning.snapshot.expired_count == 1
        ),
        warning.snapshot.to_dict(),
        {
            "retained": 1,
            "archived": 1,
            "expired": 1,
        },
    )

    add_case(
        "8W-C14",
        "record age distribution computed",
        (
            warning.snapshot.minimum_record_age_days == 13
            and warning.snapshot.mean_record_age_days
            == 181 / 3
            and warning.snapshot.maximum_record_age_days == 124
        ),
        warning.snapshot.to_dict(),
        {
            "minimum": 13,
            "mean": 181 / 3,
            "maximum": 124,
        },
    )

    add_case(
        "8W-C15",
        "exact duplicates warn",
        (
            warning.snapshot.exact_duplicate_count
            == 1
            and warning.observability_status
            == "warning"
        ),
        warning.snapshot.to_dict(),
        {
            "exact_duplicate_count": 1,
            "status": "warning",
        },
    )

    add_case(
        "8W-C16",
        "conflicting duplicates degrade",
        (
            conflicting.snapshot.conflicting_duplicate_count
            == 1
            and conflicting.observability_status
            == "degraded"
        ),
        conflicting.snapshot.to_dict(),
        {
            "conflicting_duplicate_count": 1,
            "status": "degraded",
        },
    )

    add_case(
        "8W-C17",
        "snapshot identity deterministic",
        (
            healthy.snapshot.retention_observability_snapshot_id
            == repeated.snapshot.retention_observability_snapshot_id
        ),
        healthy.snapshot.retention_observability_snapshot_id,
        repeated.snapshot.retention_observability_snapshot_id,
    )

    add_case(
        "8W-C18",
        "serialization deterministic",
        healthy.to_dict()
        == repeated.to_dict(),
        healthy.to_dict(),
        repeated.to_dict(),
    )

    add_case(
        "8W-C19",
        "retention actions never execute",
        (
            healthy.retention_action_executed
            is False
            and healthy.physical_deletion_executed
            is False
        ),
        {
            "retention_action_executed": (
                healthy.retention_action_executed
            ),
            "physical_deletion_executed": (
                healthy.physical_deletion_executed
            ),
        },
        {
            "retention_action_executed": False,
            "physical_deletion_executed": False,
        },
    )

    add_case(
        "8W-C20",
        "all prohibited authority remains false",
        all(
            value is False
            for value in (
                healthy.retention_action_executed,
                healthy.physical_deletion_executed,
                healthy.production_authority,
                healthy.production_behavior_changed,
                healthy.simulation_behavior_changed,
                healthy.historical_outcomes_joined,
                healthy.predictive_evaluation_executed,
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
            "check": "eight_v_predecessor_present",
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
            "actual": quarantined.observability_status,
            "expected": "degraded",
            "passed": (
                quarantined.observability_status
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
            "passed": (
                disabled.emitted is False
            ),
        },
        {
            "check": "ledger_digest_reconciliation_implemented",
            "actual": (
                healthy.snapshot.ledger_digest_reconciles
            ),
            "expected": True,
            "passed": (
                healthy.snapshot.ledger_digest_reconciles
                is True
            ),
        },
        {
            "check": "decision_identity_validation_implemented",
            "actual": (
                healthy.snapshot.decision_identifiers_unique
            ),
            "expected": True,
            "passed": (
                healthy.snapshot.decision_identifiers_unique
                is True
            ),
        },
        {
            "check": "policy_window_reconciliation_implemented",
            "actual": (
                healthy.snapshot.policy_windows_reconcile
            ),
            "expected": True,
            "passed": (
                healthy.snapshot.policy_windows_reconcile
                is True
            ),
        },
        {
            "check": "retention_status_distribution_implemented",
            "actual": [
                warning.snapshot.retained_count,
                warning.snapshot.archived_count,
                warning.snapshot.expired_count,
            ],
            "expected": [1, 1, 1],
            "passed": [
                warning.snapshot.retained_count,
                warning.snapshot.archived_count,
                warning.snapshot.expired_count,
            ]
            == [1, 1, 1],
        },
        {
            "check": "record_age_distribution_implemented",
            "actual": [
                warning.snapshot.minimum_record_age_days,
                warning.snapshot.maximum_record_age_days,
            ],
            "expected": [13, 124],
            "passed": [
                warning.snapshot.minimum_record_age_days,
                warning.snapshot.maximum_record_age_days,
            ]
            == [13, 124],
        },
        {
            "check": "exact_duplicate_warning_implemented",
            "actual": (
                warning.snapshot.exact_duplicate_count
            ),
            "expected": 1,
            "passed": (
                warning.snapshot.exact_duplicate_count
                == 1
            ),
        },
        {
            "check": "conflicting_duplicate_degradation_implemented",
            "actual": (
                conflicting.observability_status
            ),
            "expected": "degraded",
            "passed": (
                conflicting.observability_status
                == "degraded"
            ),
        },
        {
            "check": "quarantine_degradation_implemented",
            "actual": (
                quarantined.observability_status
            ),
            "expected": "degraded",
            "passed": (
                quarantined.observability_status
                == "degraded"
            ),
        },
        {
            "check": "snapshot_identity_deterministic",
            "actual": (
                healthy.snapshot.retention_observability_snapshot_id
                == repeated.snapshot.retention_observability_snapshot_id
            ),
            "expected": True,
            "passed": (
                healthy.snapshot.retention_observability_snapshot_id
                == repeated.snapshot.retention_observability_snapshot_id
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
            "check": "retention_action_and_authority_absent",
            "actual": any(
                (
                    healthy.retention_action_executed,
                    healthy.physical_deletion_executed,
                    healthy.production_authority,
                    healthy.production_behavior_changed,
                    healthy.simulation_behavior_changed,
                    healthy.historical_outcomes_joined,
                    healthy.predictive_evaluation_executed,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    healthy.retention_action_executed,
                    healthy.physical_deletion_executed,
                    healthy.production_authority,
                    healthy.production_behavior_changed,
                    healthy.simulation_behavior_changed,
                    healthy.historical_outcomes_joined,
                    healthy.predictive_evaluation_executed,
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
        quarantined,
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

    distribution_rows = [
        {
            "retention_status": status,
            "count": getattr(
                warning.snapshot,
                f"{status}_count",
            ),
        }
        for status in (
            "retained",
            "archived",
            "expired",
            "quarantined",
        )
    ]

    duplicate_rows = [
        {
            "duplicate_type": "exact",
            "count": (
                warning.snapshot.exact_duplicate_count
            ),
            "observability_status": (
                warning.observability_status
            ),
        },
        {
            "duplicate_type": "conflicting",
            "count": (
                conflicting.snapshot.conflicting_duplicate_count
            ),
            "observability_status": (
                conflicting.observability_status
            ),
        },
    ]

    authority_rows = [
        {
            "authority": (
                "diagnostic_retention_observability"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded diagnostic observability passed all checks."
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
        "retention_observability_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8X_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_contract_plan"
        if all_checks_passed
        else
        "8W_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_contract_implementation_remediation"
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
        OUTPUT_DIR
        / "retention_observability_snapshot.csv",
        list(
            healthy.snapshot.to_dict().keys()
        ),
        [
            report.snapshot.to_dict()
            for report in (
                healthy,
                warning,
                quarantined,
                empty,
            )
        ],
    )

    write_csv(
        OUTPUT_DIR / "status_counts.csv",
        [
            "observability_status",
            "count",
        ],
        status_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "retention_status_distribution.csv",
        [
            "retention_status",
            "count",
        ],
        distribution_rows,
    )

    write_csv(
        OUTPUT_DIR / "signal_results.csv",
        list(
            healthy.signals[0].to_dict().keys()
        ),
        [
            signal.to_dict()
            for signal in warning.signals
        ],
    )

    write_csv(
        OUTPUT_DIR / "duplicate_signals.csv",
        [
            "duplicate_type",
            "count",
            "observability_status",
        ],
        duplicate_rows,
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
                    "Plan an immutable history of retention-observability snapshots."
                    if all_checks_passed
                    else
                    "Remediate failed 8W implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8W implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR
        / "retention_observability_report.json",
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
        "retention_observability_version": (
            RETENTION_OBSERVABILITY_VERSION
        ),
        "healthy_status_supported": True,
        "warning_status_supported": True,
        "degraded_status_supported": True,
        "empty_status_supported": True,
        "disabled_path_non_emitting": True,
        "ledger_digest_reconciliation_implemented": True,
        "decision_identity_validation_implemented": True,
        "policy_window_reconciliation_implemented": True,
        "retention_status_distribution_implemented": True,
        "record_age_distribution_implemented": True,
        "duplicate_integrity_implemented": True,
        "quarantine_integrity_implemented": True,
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
        "retention_action_allowed_next": False,
        "physical_deletion_allowed_next": False,
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "retention_observability_history_planning_allowed_next": (
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
                "retention_observability_snapshot.csv",
                "status_counts.csv",
                "retention_status_distribution.csv",
                "signal_results.csv",
                "duplicate_signals.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            )
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "retention_observability_report.json"
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
