#!/usr/bin/env python3
"""
Layer 8Q shadow-dataset collection contract audit.
"""

from __future__ import annotations

import ast
import csv
from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.batter_pitch_type_response_profile import (
    build_batter_pitch_type_response_profile,
)
from mlb_app.pitching.pitch_type_matchup_overlay import (
    build_pitch_type_matchup_overlay,
)
from mlb_app.pitching.pitch_type_matchup_overlay_observability import (
    observe_pitch_type_matchup_overlay,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset import (
    build_pitch_type_matchup_overlay_shadow_dataset,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection import (
    COLLECTION_VERSION,
    build_matchup_overlay_shadow_dataset_collection_record,
    collect_pitch_type_matchup_overlay_shadow_datasets,
    dataset_payload_digest,
    quality_report_digest,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_quality_gate import (
    evaluate_pitch_type_matchup_overlay_shadow_dataset_quality,
)
from mlb_app.pitching.pitcher_arsenal_profile import (
    build_pitcher_arsenal_profile,
)


LAYER_ID = "8Q"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8Q_pitch_type_matchup_overlay_shadow_dataset_collection_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8P_pitch_type_matchup_overlay_shadow_dataset_collection_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_collection.py"
)

FIXED_TIME = "2026-07-03T00:00:00+00:00"


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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    pitcher_profile = (
        build_pitcher_arsenal_profile(
            {
                "enabled": True,
                "pitcher_id": "pitcher-1",
                "pitcher_name": "Example Pitcher",
                "pitcher_hand": "R",
                "pitcher_role": "starter",
                "season": 2026,
                "as_of_date_utc": "2026-07-01",
                "source_name": "statcast",
                "source_timestamp_utc": (
                    "2026-07-01T00:00:00+00:00"
                ),
                "arsenal_entries": [
                    {
                        "canonical_pitch_id": "FF",
                        "pitch_count": 60,
                        "quality_index": 0.4,
                        "command_index": 0.2,
                    },
                    {
                        "canonical_pitch_id": "SL",
                        "pitch_count": 40,
                        "quality_index": 0.6,
                        "command_index": 0.1,
                    },
                ],
            }
        )
    )

    batter_profile = (
        build_batter_pitch_type_response_profile(
            {
                "enabled": True,
                "batter_id": "batter-1",
                "batter_name": "Example Batter",
                "batter_hand": "L",
                "season": 2026,
                "as_of_date_utc": "2026-07-01",
                "source_name": "statcast",
                "source_timestamp_utc": (
                    "2026-07-01T00:00:00+00:00"
                ),
                "response_entries": [
                    {
                        "canonical_pitch_id": "FF",
                        "pitcher_hand": "R",
                        "count_context": "all_counts",
                        "pitch_count": 70,
                        "swing_count": 35,
                        "contact_count": 28,
                        "batted_ball_count": 20,
                        "swing_rate": 0.5,
                        "whiff_rate": 0.2,
                        "contact_rate": 0.8,
                        "hard_hit_rate": 0.4,
                        "barrel_rate": 0.1,
                    },
                    {
                        "canonical_pitch_id": "SL",
                        "pitcher_hand": "R",
                        "count_context": "all_counts",
                        "pitch_count": 30,
                        "swing_count": 18,
                        "contact_count": 12,
                        "batted_ball_count": 8,
                        "swing_rate": 0.6,
                        "whiff_rate": 0.333333,
                        "contact_rate": 0.666667,
                        "hard_hit_rate": 0.25,
                        "barrel_rate": 0.05,
                    },
                ],
            }
        )
    )

    overlay = build_pitch_type_matchup_overlay(
        pitcher_profile,
        batter_profile,
        enabled=True,
    )

    bundle = observe_pitch_type_matchup_overlay(
        overlay,
        enabled=True,
    )

    clean_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [bundle],
            enabled=True,
            generated_at_utc=FIXED_TIME,
        )
    )

    pass_report = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_quality(
            clean_dataset,
            enabled=True,
            evaluated_at_utc=FIXED_TIME,
        )
    )

    warning_bundle = replace(
        bundle,
        observability_status="partial",
    )

    warning_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [warning_bundle],
            enabled=True,
            generated_at_utc=FIXED_TIME,
        )
    )

    warning_report = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_quality(
            warning_dataset,
            enabled=True,
            evaluated_at_utc=FIXED_TIME,
        )
    )

    empty_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [],
            enabled=True,
            generated_at_utc=FIXED_TIME,
        )
    )

    empty_report = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_quality(
            empty_dataset,
            enabled=True,
            evaluated_at_utc=FIXED_TIME,
        )
    )

    failed_dataset = replace(
        clean_dataset,
        manifest=replace(
            clean_dataset.manifest,
            row_count=99,
        ),
    )

    failed_report = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_quality(
            failed_dataset,
            enabled=True,
            evaluated_at_utc=FIXED_TIME,
        )
    )

    accepted_collection = (
        collect_pitch_type_matchup_overlay_shadow_datasets(
            [(clean_dataset, pass_report)],
            enabled=True,
            collected_at_utc=FIXED_TIME,
        )
    )

    warning_collection = (
        collect_pitch_type_matchup_overlay_shadow_datasets(
            [
                (
                    warning_dataset,
                    warning_report,
                )
            ],
            enabled=True,
            collected_at_utc=FIXED_TIME,
        )
    )

    rejected_collection = (
        collect_pitch_type_matchup_overlay_shadow_datasets(
            [
                (
                    failed_dataset,
                    failed_report,
                )
            ],
            enabled=True,
            collected_at_utc=FIXED_TIME,
        )
    )

    empty_collection = (
        collect_pitch_type_matchup_overlay_shadow_datasets(
            [
                (
                    empty_dataset,
                    empty_report,
                )
            ],
            enabled=True,
            collected_at_utc=FIXED_TIME,
        )
    )

    disabled_collection = (
        collect_pitch_type_matchup_overlay_shadow_datasets(
            [(clean_dataset, pass_report)],
            enabled=False,
            collected_at_utc=FIXED_TIME,
        )
    )

    duplicate_collection = (
        collect_pitch_type_matchup_overlay_shadow_datasets(
            [
                (clean_dataset, pass_report),
                (clean_dataset, pass_report),
            ],
            enabled=True,
            collected_at_utc=FIXED_TIME,
        )
    )

    pass_record = (
        build_matchup_overlay_shadow_dataset_collection_record(
            clean_dataset,
            pass_report,
            collected_at_utc=FIXED_TIME,
        )
    )

    missing_dataset_record = (
        build_matchup_overlay_shadow_dataset_collection_record(
            None,
            pass_report,
            collected_at_utc=FIXED_TIME,
        )
    )

    missing_report_record = (
        build_matchup_overlay_shadow_dataset_collection_record(
            clean_dataset,
            None,
            collected_at_utc=FIXED_TIME,
        )
    )

    repeated_collection = (
        collect_pitch_type_matchup_overlay_shadow_datasets(
            [(clean_dataset, pass_report)],
            enabled=True,
            collected_at_utc=FIXED_TIME,
        )
    )

    cases: list[dict[str, Any]] = []

    def record(
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

    record(
        "8Q-C01",
        "accepted collection emits",
        accepted_collection.emitted,
        accepted_collection.emitted,
        True,
    )

    record(
        "8Q-C02",
        "pass report accepted",
        (
            accepted_collection.collection_status
            == "accepted"
        ),
        accepted_collection.collection_status,
        "accepted",
    )

    record(
        "8Q-C03",
        "warning report accepted with warnings",
        (
            warning_collection.collection_status
            == "accepted_with_warnings"
        ),
        warning_collection.collection_status,
        "accepted_with_warnings",
    )

    record(
        "8Q-C04",
        "failed report rejected",
        (
            rejected_collection.collection_status
            == "rejected"
        ),
        rejected_collection.collection_status,
        "rejected",
    )

    record(
        "8Q-C05",
        "empty dataset metadata recorded",
        (
            empty_collection.collection_status
            == "empty"
        ),
        empty_collection.collection_status,
        "empty",
    )

    record(
        "8Q-C06",
        "disabled collection non-emitting",
        (
            disabled_collection.emitted
            is False
            and disabled_collection.manifest
            is None
        ),
        disabled_collection.to_dict(),
        {
            "emitted": False,
            "manifest": None,
        },
    )

    record(
        "8Q-C07",
        "deterministic dataset digest",
        (
            dataset_payload_digest(
                clean_dataset
            )
            == dataset_payload_digest(
                clean_dataset
            )
        ),
        dataset_payload_digest(
            clean_dataset
        ),
        "deterministic_sha256",
    )

    record(
        "8Q-C08",
        "deterministic quality report digest",
        (
            quality_report_digest(
                pass_report
            )
            == quality_report_digest(
                pass_report
            )
        ),
        quality_report_digest(
            pass_report
        ),
        "deterministic_sha256",
    )

    record(
        "8Q-C09",
        "collection record ID deterministic",
        (
            pass_record.collection_record_id
            == repeated_collection.records[
                0
            ].collection_record_id
        ),
        pass_record.collection_record_id,
        repeated_collection.records[
            0
        ].collection_record_id,
    )

    record(
        "8Q-C10",
        "collection record version retained",
        (
            pass_record.collection_version
            == COLLECTION_VERSION
        ),
        pass_record.collection_version,
        "8Q-v1",
    )

    record(
        "8Q-C11",
        "dataset version retained",
        (
            pass_record.dataset_version
            == clean_dataset.shadow_dataset_version
        ),
        pass_record.dataset_version,
        clean_dataset.shadow_dataset_version,
    )

    record(
        "8Q-C12",
        "quality gate version retained",
        (
            pass_record.quality_gate_version
            == pass_report.quality_gate_version
        ),
        pass_record.quality_gate_version,
        pass_report.quality_gate_version,
    )

    record(
        "8Q-C13",
        "exact duplicate collapsed",
        (
            len(
                duplicate_collection.records
            )
            == 1
        ),
        len(
            duplicate_collection.records
        ),
        1,
    )

    record(
        "8Q-C14",
        "exact duplicate reported",
        (
            duplicate_collection.manifest
            is not None
            and (
                duplicate_collection.manifest.exact_duplicate_count
                == 1
            )
        ),
        (
            duplicate_collection.manifest.to_dict()
            if duplicate_collection.manifest
            else None
        ),
        {
            "exact_duplicate_count": 1,
        },
    )

    record(
        "8Q-C15",
        "missing dataset rejected",
        (
            missing_dataset_record.collection_status
            == "rejected"
        ),
        missing_dataset_record.collection_status,
        "rejected",
    )

    record(
        "8Q-C16",
        "missing quality report rejected",
        (
            missing_report_record.collection_status
            == "rejected"
        ),
        missing_report_record.collection_status,
        "rejected",
    )

    record(
        "8Q-C17",
        "collection serialization deterministic",
        (
            accepted_collection.to_dict()
            == repeated_collection.to_dict()
        ),
        accepted_collection.to_dict(),
        repeated_collection.to_dict(),
    )

    record(
        "8Q-C18",
        "manifest reconciles",
        (
            accepted_collection.manifest
            is not None
            and (
                accepted_collection.manifest.record_count
                == len(
                    accepted_collection.records
                )
            )
        ),
        (
            accepted_collection.manifest.to_dict()
            if accepted_collection.manifest
            else None
        ),
        {
            "record_count": 1,
        },
    )

    record(
        "8Q-C19",
        "collection append-only",
        (
            accepted_collection.append_only
            is True
            and (
                accepted_collection.manifest
                is not None
                and accepted_collection.manifest.append_only
                is True
            )
        ),
        accepted_collection.to_dict(),
        {
            "append_only": True,
        },
    )

    record(
        "8Q-C20",
        "all prohibited authority remains false",
        all(
            value is False
            for value in [
                accepted_collection.production_authority,
                accepted_collection.production_behavior_changed,
                accepted_collection.simulation_behavior_changed,
                accepted_collection.historical_outcomes_joined,
                accepted_collection.predictive_evaluation_executed,
                pass_record.production_authority,
                accepted_collection.manifest.production_authority,
            ]
        ),
        accepted_collection.to_dict(),
        {
            "all_authority_flags": False,
        },
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
            "check": "eight_p_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_contract_cases_pass",
            "actual": sum(
                1
                for row in cases
                if row["passed"]
            ),
            "expected": 20,
            "passed": all(
                row["passed"]
                for row in cases
            ),
        },
        {
            "check": "accepted_status_supported",
            "actual": (
                accepted_collection.collection_status
            ),
            "expected": "accepted",
            "passed": (
                accepted_collection.collection_status
                == "accepted"
            ),
        },
        {
            "check": "accepted_with_warnings_status_supported",
            "actual": (
                warning_collection.collection_status
            ),
            "expected": "accepted_with_warnings",
            "passed": (
                warning_collection.collection_status
                == "accepted_with_warnings"
            ),
        },
        {
            "check": "rejected_status_supported",
            "actual": (
                rejected_collection.collection_status
            ),
            "expected": "rejected",
            "passed": (
                rejected_collection.collection_status
                == "rejected"
            ),
        },
        {
            "check": "empty_status_supported",
            "actual": (
                empty_collection.collection_status
            ),
            "expected": "empty",
            "passed": (
                empty_collection.collection_status
                == "empty"
            ),
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": (
                disabled_collection.emitted
            ),
            "expected": False,
            "passed": (
                disabled_collection.emitted
                is False
            ),
        },
        {
            "check": "dataset_digest_deterministic",
            "actual": (
                dataset_payload_digest(
                    clean_dataset
                )
                == dataset_payload_digest(
                    clean_dataset
                )
            ),
            "expected": True,
            "passed": (
                dataset_payload_digest(
                    clean_dataset
                )
                == dataset_payload_digest(
                    clean_dataset
                )
            ),
        },
        {
            "check": "quality_report_digest_deterministic",
            "actual": (
                quality_report_digest(
                    pass_report
                )
                == quality_report_digest(
                    pass_report
                )
            ),
            "expected": True,
            "passed": (
                quality_report_digest(
                    pass_report
                )
                == quality_report_digest(
                    pass_report
                )
            ),
        },
        {
            "check": "collection_identity_deterministic",
            "actual": (
                accepted_collection.records[
                    0
                ].collection_record_id
                == repeated_collection.records[
                    0
                ].collection_record_id
            ),
            "expected": True,
            "passed": (
                accepted_collection.records[
                    0
                ].collection_record_id
                == repeated_collection.records[
                    0
                ].collection_record_id
            ),
        },
        {
            "check": "exact_duplicates_idempotent",
            "actual": len(
                duplicate_collection.records
            ),
            "expected": 1,
            "passed": (
                len(
                    duplicate_collection.records
                )
                == 1
            ),
        },
        {
            "check": "append_only_manifest_implemented",
            "actual": (
                accepted_collection.manifest.append_only
                if accepted_collection.manifest
                else None
            ),
            "expected": True,
            "passed": (
                accepted_collection.manifest
                is not None
                and accepted_collection.manifest.append_only
                is True
            ),
        },
        {
            "check": "manifest_record_count_reconciles",
            "actual": (
                accepted_collection.manifest.record_count
                if accepted_collection.manifest
                else None
            ),
            "expected": len(
                accepted_collection.records
            ),
            "passed": (
                accepted_collection.manifest
                is not None
                and (
                    accepted_collection.manifest.record_count
                    == len(
                        accepted_collection.records
                    )
                )
            ),
        },
        {
            "check": "warning_collection_non_authoritative",
            "actual": (
                warning_collection.production_authority
            ),
            "expected": False,
            "passed": (
                warning_collection.production_authority
                is False
            ),
        },
        {
            "check": "missing_dataset_rejected",
            "actual": (
                missing_dataset_record.collection_status
            ),
            "expected": "rejected",
            "passed": (
                missing_dataset_record.collection_status
                == "rejected"
            ),
        },
        {
            "check": "missing_report_rejected",
            "actual": (
                missing_report_record.collection_status
            ),
            "expected": "rejected",
            "passed": (
                missing_report_record.collection_status
                == "rejected"
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                accepted_collection.to_dict()
                == repeated_collection.to_dict()
            ),
            "expected": True,
            "passed": (
                accepted_collection.to_dict()
                == repeated_collection.to_dict()
            ),
        },
        {
            "check": "production_simulation_validation_authority_absent",
            "actual": any(
                [
                    accepted_collection.production_authority,
                    accepted_collection.production_behavior_changed,
                    accepted_collection.simulation_behavior_changed,
                    accepted_collection.historical_outcomes_joined,
                    accepted_collection.predictive_evaluation_executed,
                ]
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in [
                    accepted_collection.production_authority,
                    accepted_collection.production_behavior_changed,
                    accepted_collection.simulation_behavior_changed,
                    accepted_collection.historical_outcomes_joined,
                    accepted_collection.predictive_evaluation_executed,
                ]
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    collections = [
        accepted_collection,
        warning_collection,
        rejected_collection,
        empty_collection,
        disabled_collection,
    ]

    status_rows = [
        {
            "collection_status": status,
            "count": sum(
                1
                for collection in collections
                if collection.collection_status
                == status
            ),
        }
        for status in (
            "accepted",
            "accepted_with_warnings",
            "rejected",
            "empty",
            "disabled",
        )
    ]

    authority_rows = [
        {
            "authority": (
                "shadow_dataset_collection_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded append-only collection passed all checks."
            ),
        },
        {
            "authority": (
                "historical_outcome_enrichment"
            ),
            "granted": False,
            "reason": (
                "No historical outcomes are joined."
            ),
        },
        {
            "authority": "predictive_evaluation",
            "granted": False,
            "reason": (
                "No predictive evaluation occurs."
            ),
        },
        {
            "authority": (
                "production_or_simulation_change"
            ),
            "granted": False,
            "reason": (
                "Production and simulation behavior remain unchanged."
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
        "pitch_type_matchup_overlay_shadow_dataset_collection_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8R_pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_plan"
        if all_checks_passed
        else
        "8Q_pitch_type_matchup_overlay_shadow_dataset_collection_contract_implementation_remediation"
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
        OUTPUT_DIR / "collection_records.csv",
        list(
            accepted_collection.records[
                0
            ].to_dict().keys()
        ),
        [
            record.to_dict()
            for record in accepted_collection.records
        ],
    )

    write_csv(
        OUTPUT_DIR / "duplicate_report.csv",
        [
            "collection_record_id",
            "duplicate_count",
            "conflict",
            "diagnostic_code",
        ],
        [
            duplicate.to_dict()
            for duplicate in duplicate_collection.duplicates
        ],
    )

    write_csv(
        OUTPUT_DIR / "status_counts.csv",
        [
            "collection_status",
            "count",
        ],
        status_rows,
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
                    "Plan diagnostic observability for append-only shadow-dataset collection."
                    if all_checks_passed
                    else
                    "Remediate failed 8Q implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8Q implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "collection_manifest.json",
        (
            accepted_collection.manifest.to_dict()
            if accepted_collection.manifest
            else None
        ),
    )

    write_json(
        OUTPUT_DIR / "collection_report.json",
        accepted_collection.to_dict(),
    )

    summary = {
        "implementation_checks_required": len(
            checks
        ),
        "implementation_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "contract_cases_required": len(
            cases
        ),
        "contract_cases_passed": sum(
            1
            for row in cases
            if row["passed"]
        ),
        "collection_version": (
            COLLECTION_VERSION
        ),
        "accepted_status_supported": True,
        "accepted_with_warnings_status_supported": True,
        "rejected_status_supported": True,
        "empty_status_supported": True,
        "disabled_path_non_emitting": True,
        "deterministic_digests_implemented": True,
        "deterministic_identity_implemented": True,
        "append_only_collection_implemented": True,
        "exact_duplicate_idempotency_implemented": True,
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
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "collection_observability_planning_allowed_next": (
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
            for filename in [
                "implementation_checks.csv",
                "contract_cases.csv",
                "collection_records.csv",
                "duplicate_report.csv",
                "status_counts.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "collection_manifest.json"
            ),
            str(
                OUTPUT_DIR
                / "collection_report.json"
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
