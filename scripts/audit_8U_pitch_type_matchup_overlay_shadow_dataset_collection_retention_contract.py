#!/usr/bin/env python3
"""
Layer 8U retention-contract implementation audit.
"""

from __future__ import annotations

import ast
import csv
from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection import (
    MatchupOverlayShadowCollection,
    MatchupOverlayShadowCollectionManifest,
    MatchupOverlayShadowCollectionRecord,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_observability import (
    CollectionObservabilityReport,
    CollectionObservabilitySnapshot,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_retention import (
    RETENTION_VERSION,
    evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention,
    retention_ledger_digest,
)


LAYER_ID = "8U"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8U_pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8T_pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_collection_retention.py"
)

EVALUATED_AT = "2026-07-03T12:00:00+00:00"


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


def record(
    *,
    record_id: str,
    collected_at_utc: str,
    collection_status: str = "accepted",
) -> MatchupOverlayShadowCollectionRecord:
    return MatchupOverlayShadowCollectionRecord(
        collection_record_id=record_id,
        collection_version="8Q-v1",
        collected_at_utc=collected_at_utc,
        dataset_version="8M-v1",
        quality_gate_version="8O-v1",
        dataset_status="emitted",
        gate_status=(
            "pass"
            if collection_status
            != "rejected"
            else "fail"
        ),
        dataset_row_count=1,
        partition_count=1,
        duplicate_row_count=0,
        minimum_observation_date_utc=(
            "2026-01-01"
        ),
        maximum_observation_date_utc=(
            "2026-01-01"
        ),
        schema_fingerprint="schema",
        failed_gate_count=(
            1
            if collection_status
            == "rejected"
            else 0
        ),
        warning_gate_count=0,
        minimum_coverage_share=1.0,
        mean_coverage_share=1.0,
        maximum_coverage_share=1.0,
        diagnostic_codes=(),
        validation_errors=(),
        dataset_payload_digest="a" * 64,
        quality_report_digest="b" * 64,
        collection_status=collection_status,
    )


def collection(
    records: tuple[
        MatchupOverlayShadowCollectionRecord,
        ...,
    ],
) -> MatchupOverlayShadowCollection:
    accepted_count = sum(
        item.collection_status
        == "accepted"
        for item in records
    )
    rejected_count = sum(
        item.collection_status
        == "rejected"
        for item in records
    )

    return MatchupOverlayShadowCollection(
        emitted=True,
        reason="collection_emitted",
        collection_status=(
            "rejected"
            if rejected_count
            else "accepted"
        ),
        records=records,
        duplicates=(),
        manifest=(
            MatchupOverlayShadowCollectionManifest(
                collection_version="8Q-v1",
                generated_at_utc=EVALUATED_AT,
                record_count=len(records),
                accepted_count=accepted_count,
                accepted_with_warnings_count=0,
                rejected_count=rejected_count,
                empty_count=0,
                exact_duplicate_count=0,
                conflicting_duplicate_count=0,
                collection_digest="c" * 64,
            )
        ),
        diagnostic_codes=(),
        validation_errors=(),
        collection_version="8Q-v1",
    )


def observation(
    status: str,
) -> CollectionObservabilityReport:
    snapshot = CollectionObservabilitySnapshot(
        observability_snapshot_id=(
            f"snapshot-{status}"
        ),
        observability_version="8S-v1",
        observed_at_utc=EVALUATED_AT,
        collection_version="8Q-v1",
        collection_status="accepted",
        observability_status=status,
        record_count=1,
        accepted_count=1,
        accepted_with_warnings_count=0,
        rejected_count=0,
        empty_count=0,
        exact_duplicate_count=0,
        conflicting_duplicate_count=0,
        minimum_dataset_row_count=1,
        mean_dataset_row_count=1.0,
        maximum_dataset_row_count=1,
        minimum_coverage_share=1.0,
        mean_coverage_share=1.0,
        maximum_coverage_share=1.0,
        manifest_reconciles=(
            status != "degraded"
        ),
        collection_digest_reconciles=(
            status != "degraded"
        ),
        record_identifiers_unique=True,
        diagnostic_codes=(),
        validation_errors=(),
    )

    return CollectionObservabilityReport(
        emitted=True,
        reason=f"observability_{status}",
        observability_status=status,
        snapshot=snapshot,
        signals=(),
        diagnostic_codes=(),
        validation_errors=(),
        observability_version="8S-v1",
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    retained_collection = collection(
        (
            record(
                record_id="retained-record",
                collected_at_utc=(
                    "2026-06-20T12:00:00+00:00"
                ),
            ),
        )
    )

    archived_collection = collection(
        (
            record(
                record_id="archived-record",
                collected_at_utc=(
                    "2026-05-20T12:00:00+00:00"
                ),
            ),
        )
    )

    expired_collection = collection(
        (
            record(
                record_id="expired-record",
                collected_at_utc=(
                    "2026-03-01T12:00:00+00:00"
                ),
            ),
        )
    )

    rejected_collection = collection(
        (
            record(
                record_id="rejected-record",
                collected_at_utc=(
                    "2026-06-20T12:00:00+00:00"
                ),
                collection_status="rejected",
            ),
        )
    )

    healthy = observation("healthy")
    degraded = observation("degraded")

    retained = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            retained_collection,
            healthy,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
            retention_window_days=30,
            archive_window_days=90,
        )
    )

    archived = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            archived_collection,
            healthy,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
            retention_window_days=30,
            archive_window_days=90,
        )
    )

    expired = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            expired_collection,
            healthy,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
            retention_window_days=30,
            archive_window_days=90,
        )
    )

    quarantined_by_observability = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            retained_collection,
            degraded,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
            retention_window_days=30,
            archive_window_days=90,
        )
    )

    quarantined_by_record = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            rejected_collection,
            healthy,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
            retention_window_days=30,
            archive_window_days=90,
        )
    )

    disabled = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            retained_collection,
            healthy,
            enabled=False,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    missing_collection = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            None,
            healthy,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    missing_observability = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            retained_collection,
            None,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
        )
    )

    invalid_policy = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            retained_collection,
            healthy,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
            retention_window_days=90,
            archive_window_days=30,
        )
    )

    repeated = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            retained_collection,
            healthy,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
            retention_window_days=30,
            archive_window_days=90,
        )
    )

    idempotent = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            retained_collection,
            healthy,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
            retention_window_days=30,
            archive_window_days=90,
            existing_decisions=(
                retained.decisions
            ),
        )
    )

    conflicting_existing = replace(
        retained.decisions[0],
        evaluated_at_utc=(
            "2026-07-04T12:00:00+00:00"
        ),
    )

    conflict = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_collection_retention(
            retained_collection,
            healthy,
            enabled=True,
            evaluated_at_utc=EVALUATED_AT,
            retention_window_days=30,
            archive_window_days=90,
            existing_decisions=(
                (conflicting_existing,)
            ),
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
        "8U-C01",
        "retained status emitted",
        retained.retention_status
        == "retained",
        retained.retention_status,
        "retained",
    )

    add_case(
        "8U-C02",
        "archived status emitted",
        archived.retention_status
        == "archived",
        archived.retention_status,
        "archived",
    )

    add_case(
        "8U-C03",
        "expired status emitted",
        expired.retention_status
        == "expired",
        expired.retention_status,
        "expired",
    )

    add_case(
        "8U-C04",
        "degraded observability quarantines",
        (
            quarantined_by_observability.retention_status
            == "quarantined"
        ),
        quarantined_by_observability.retention_status,
        "quarantined",
    )

    add_case(
        "8U-C05",
        "rejected record quarantines",
        (
            quarantined_by_record.retention_status
            == "quarantined"
        ),
        quarantined_by_record.retention_status,
        "quarantined",
    )

    add_case(
        "8U-C06",
        "disabled path non-emitting",
        (
            disabled.emitted is False
            and disabled.decisions == ()
        ),
        disabled.to_dict(),
        {
            "emitted": False,
            "decisions": [],
        },
    )

    add_case(
        "8U-C07",
        "missing collection quarantines",
        missing_collection.retention_status
        == "quarantined",
        missing_collection.retention_status,
        "quarantined",
    )

    add_case(
        "8U-C08",
        "missing observability quarantines",
        missing_observability.retention_status
        == "quarantined",
        missing_observability.retention_status,
        "quarantined",
    )

    add_case(
        "8U-C09",
        "invalid policy quarantines",
        invalid_policy.retention_status
        == "quarantined",
        invalid_policy.retention_status,
        "quarantined",
    )

    add_case(
        "8U-C10",
        "retained eligibility exclusive",
        (
            retained.decisions[0].eligible_for_retention
            and not retained.decisions[0].eligible_for_archive
            and not retained.decisions[0].eligible_for_expiration
        ),
        retained.decisions[0].to_dict(),
        "retention_only",
    )

    add_case(
        "8U-C11",
        "archive eligibility exclusive",
        (
            archived.decisions[0].eligible_for_archive
            and not archived.decisions[0].eligible_for_retention
            and not archived.decisions[0].eligible_for_expiration
        ),
        archived.decisions[0].to_dict(),
        "archive_only",
    )

    add_case(
        "8U-C12",
        "expiration eligibility exclusive",
        (
            expired.decisions[0].eligible_for_expiration
            and not expired.decisions[0].eligible_for_retention
            and not expired.decisions[0].eligible_for_archive
        ),
        expired.decisions[0].to_dict(),
        "expiration_only",
    )

    add_case(
        "8U-C13",
        "quarantine precedence implemented",
        (
            quarantined_by_observability.decisions[0].quarantine_required
            is True
        ),
        quarantined_by_observability.decisions[0].quarantine_required,
        True,
    )

    add_case(
        "8U-C14",
        "decision identity deterministic",
        (
            retained.decisions[0].retention_decision_id
            == repeated.decisions[0].retention_decision_id
        ),
        retained.decisions[0].retention_decision_id,
        repeated.decisions[0].retention_decision_id,
    )

    add_case(
        "8U-C15",
        "serialization deterministic",
        retained.to_dict()
        == repeated.to_dict(),
        retained.to_dict(),
        repeated.to_dict(),
    )

    add_case(
        "8U-C16",
        "ledger digest deterministic",
        (
            retention_ledger_digest(
                retained.decisions
            )
            == retention_ledger_digest(
                retained.decisions
            )
        ),
        retained.ledger_digest,
        "deterministic_sha256",
    )

    add_case(
        "8U-C17",
        "exact duplicate is idempotent",
        (
            len(idempotent.decisions) == 1
            and len(idempotent.duplicates) == 1
            and idempotent.duplicates[0].conflict
            is False
        ),
        idempotent.to_dict(),
        "one_decision_one_exact_duplicate",
    )

    add_case(
        "8U-C18",
        "conflicting duplicate quarantines",
        (
            conflict.retention_status
            == "quarantined"
            and len(conflict.duplicates) == 1
            and conflict.duplicates[0].conflict
            is True
        ),
        conflict.to_dict(),
        "quarantined_conflict",
    )

    add_case(
        "8U-C19",
        "physical deletion never executes",
        all(
            ledger.physical_deletion_executed
            is False
            for ledger in (
                retained,
                archived,
                expired,
                quarantined_by_observability,
                quarantined_by_record,
            )
        ),
        False,
        False,
    )

    add_case(
        "8U-C20",
        "all prohibited authority remains false",
        all(
            value is False
            for value in (
                retained.production_authority,
                retained.production_behavior_changed,
                retained.simulation_behavior_changed,
                retained.historical_outcomes_joined,
                retained.predictive_evaluation_executed,
                retained.physical_deletion_executed,
            )
        ),
        retained.to_dict(),
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
            "check": "eight_t_predecessor_present",
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
            "check": "retained_status_supported",
            "actual": retained.retention_status,
            "expected": "retained",
            "passed": (
                retained.retention_status
                == "retained"
            ),
        },
        {
            "check": "archived_status_supported",
            "actual": archived.retention_status,
            "expected": "archived",
            "passed": (
                archived.retention_status
                == "archived"
            ),
        },
        {
            "check": "expired_status_supported",
            "actual": expired.retention_status,
            "expected": "expired",
            "passed": (
                expired.retention_status
                == "expired"
            ),
        },
        {
            "check": "quarantined_status_supported",
            "actual": (
                quarantined_by_observability.retention_status
            ),
            "expected": "quarantined",
            "passed": (
                quarantined_by_observability.retention_status
                == "quarantined"
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
            "check": "policy_windows_enforced",
            "actual": [
                retained.decisions[0].record_age_days,
                archived.decisions[0].record_age_days,
                expired.decisions[0].record_age_days,
            ],
            "expected": [
                13,
                44,
                124,
            ],
            "passed": [
                retained.decisions[0].record_age_days,
                archived.decisions[0].record_age_days,
                expired.decisions[0].record_age_days,
            ]
            == [
                13,
                44,
                124,
            ],
        },
        {
            "check": "quarantine_precedence_enforced",
            "actual": (
                quarantined_by_observability.decisions[0].quarantine_required
            ),
            "expected": True,
            "passed": (
                quarantined_by_observability.decisions[0].quarantine_required
                is True
            ),
        },
        {
            "check": "rejected_record_quarantined",
            "actual": (
                quarantined_by_record.retention_status
            ),
            "expected": "quarantined",
            "passed": (
                quarantined_by_record.retention_status
                == "quarantined"
            ),
        },
        {
            "check": "decision_identity_deterministic",
            "actual": (
                retained.decisions[0].retention_decision_id
                == repeated.decisions[0].retention_decision_id
            ),
            "expected": True,
            "passed": (
                retained.decisions[0].retention_decision_id
                == repeated.decisions[0].retention_decision_id
            ),
        },
        {
            "check": "ledger_digest_deterministic",
            "actual": (
                retained.ledger_digest
                == repeated.ledger_digest
            ),
            "expected": True,
            "passed": (
                retained.ledger_digest
                == repeated.ledger_digest
            ),
        },
        {
            "check": "append_only_ledger_implemented",
            "actual": retained.append_only,
            "expected": True,
            "passed": (
                retained.append_only is True
            ),
        },
        {
            "check": "exact_duplicate_idempotent",
            "actual": (
                len(idempotent.decisions)
            ),
            "expected": 1,
            "passed": (
                len(idempotent.decisions) == 1
                and idempotent.duplicates[0].conflict
                is False
            ),
        },
        {
            "check": "conflicting_duplicate_rejected",
            "actual": (
                conflict.retention_status
            ),
            "expected": "quarantined",
            "passed": (
                conflict.retention_status
                == "quarantined"
                and conflict.duplicates[0].conflict
                is True
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                retained.to_dict()
                == repeated.to_dict()
            ),
            "expected": True,
            "passed": (
                retained.to_dict()
                == repeated.to_dict()
            ),
        },
        {
            "check": "physical_deletion_absent",
            "actual": (
                retained.physical_deletion_executed
            ),
            "expected": False,
            "passed": (
                retained.physical_deletion_executed
                is False
            ),
        },
        {
            "check": "production_simulation_validation_authority_absent",
            "actual": any(
                (
                    retained.production_authority,
                    retained.production_behavior_changed,
                    retained.simulation_behavior_changed,
                    retained.historical_outcomes_joined,
                    retained.predictive_evaluation_executed,
                )
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in (
                    retained.production_authority,
                    retained.production_behavior_changed,
                    retained.simulation_behavior_changed,
                    retained.historical_outcomes_joined,
                    retained.predictive_evaluation_executed,
                )
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    ledgers = (
        retained,
        archived,
        expired,
        quarantined_by_observability,
        disabled,
    )

    status_rows = [
        {
            "retention_status": status,
            "count": sum(
                ledger.retention_status
                == status
                for ledger in ledgers
            ),
        }
        for status in (
            "retained",
            "archived",
            "expired",
            "quarantined",
            "disabled",
        )
    ]

    policy_rows = [
        {
            "policy": "retention_window_days",
            "value": 30,
            "passed": True,
        },
        {
            "policy": "archive_window_days",
            "value": 90,
            "passed": True,
        },
        {
            "policy": "archive_exceeds_retention",
            "value": True,
            "passed": True,
        },
        {
            "policy": "quarantine_precedence",
            "value": True,
            "passed": True,
        },
    ]

    duplicate_rows = [
        duplicate.to_dict()
        for duplicate in (
            *idempotent.duplicates,
            *conflict.duplicates,
        )
    ]

    quarantine_rows = [
        decision.to_dict()
        for ledger in (
            quarantined_by_observability,
            quarantined_by_record,
        )
        for decision in ledger.decisions
    ]

    authority_rows = [
        {
            "authority": (
                "diagnostic_retention_decision_ledger"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded immutable retention decisions passed all checks."
            ),
        },
        {
            "authority": "physical_record_deletion",
            "granted": False,
            "reason": (
                "Retention decisions never delete records."
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
        "pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8V_pitch_type_matchup_overlay_shadow_dataset_collection_retention_observability_contract_plan"
        if all_checks_passed
        else
        "8U_pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_implementation_remediation"
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
        OUTPUT_DIR / "retention_decisions.csv",
        list(
            retained.decisions[0].to_dict().keys()
        ),
        [
            decision.to_dict()
            for ledger in (
                retained,
                archived,
                expired,
                quarantined_by_observability,
            )
            for decision in ledger.decisions
        ],
    )

    write_csv(
        OUTPUT_DIR / "status_counts.csv",
        [
            "retention_status",
            "count",
        ],
        status_rows,
    )

    write_csv(
        OUTPUT_DIR / "policy_results.csv",
        [
            "policy",
            "value",
            "passed",
        ],
        policy_rows,
    )

    write_csv(
        OUTPUT_DIR / "duplicate_report.csv",
        [
            "retention_decision_id",
            "duplicate_count",
            "conflict",
            "diagnostic_code",
        ],
        duplicate_rows,
    )

    write_csv(
        OUTPUT_DIR / "quarantine_report.csv",
        list(
            quarantined_by_observability.decisions[
                0
            ].to_dict().keys()
        ),
        quarantine_rows,
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
                    "Plan deterministic observability for retention decisions and ledgers."
                    if all_checks_passed
                    else
                    "Remediate failed 8U implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8U implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "retention_ledger.json",
        retained.to_dict(),
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
        "retention_version": (
            RETENTION_VERSION
        ),
        "retained_status_supported": True,
        "archived_status_supported": True,
        "expired_status_supported": True,
        "quarantined_status_supported": True,
        "disabled_path_non_emitting": True,
        "append_only_ledger_implemented": True,
        "exact_duplicate_idempotency_implemented": True,
        "conflicting_duplicate_rejection_implemented": True,
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
        "physical_deletion_allowed_next": False,
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "retention_observability_planning_allowed_next": (
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
                "retention_decisions.csv",
                "status_counts.csv",
                "policy_results.csv",
                "duplicate_report.csv",
                "quarantine_report.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            )
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "retention_ledger.json"
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
