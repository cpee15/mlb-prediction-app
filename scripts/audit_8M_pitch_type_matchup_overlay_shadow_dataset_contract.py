#!/usr/bin/env python3
"""
Layer 8M pitch-type matchup overlay shadow dataset audit.
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
    SHADOW_DATASET_VERSION,
    build_pitch_type_matchup_overlay_shadow_dataset,
)
from mlb_app.pitching.pitcher_arsenal_profile import (
    build_pitcher_arsenal_profile,
)


LAYER_ID = "8M"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8M_pitch_type_matchup_overlay_shadow_dataset_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8L_pitch_type_matchup_overlay_shadow_dataset_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset.py"
)

FIXED_GENERATED_AT = "2026-07-01T00:00:00+00:00"


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
        "pitch_type_matchup_overlay_shadow_dataset_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    pitcher_profile = build_pitcher_arsenal_profile(
        {
            "enabled": True,
            "pitcher_id": "pitcher-1",
            "pitcher_name": "Example Pitcher",
            "pitcher_hand": "R",
            "pitcher_role": "starter",
            "season": 2026,
            "as_of_date_utc": "2026-06-30",
            "source_name": "statcast",
            "source_timestamp_utc": (
                "2026-06-25T00:00:00+00:00"
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
                    "pitch_count": 30,
                    "quality_index": 0.6,
                    "command_index": 0.1,
                },
                {
                    "canonical_pitch_id": "CH",
                    "pitch_count": 10,
                },
            ],
        }
    )

    batter_profile = (
        build_batter_pitch_type_response_profile(
            {
                "enabled": True,
                "batter_id": "batter-1",
                "batter_name": "Example Batter",
                "batter_hand": "L",
                "season": 2026,
                "as_of_date_utc": "2026-06-30",
                "source_name": "statcast",
                "source_timestamp_utc": (
                    "2026-06-25T00:00:00+00:00"
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

    complete_bundle = (
        observe_pitch_type_matchup_overlay(
            overlay,
            enabled=True,
        )
    )

    second_bundle = replace(
        complete_bundle,
        summary=replace(
            complete_bundle.summary,
            observation_id="matchup-overlay-second",
            as_of_date_utc="2026-07-01",
        ),
    )

    ready_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [
                complete_bundle,
                second_bundle,
            ],
            enabled=True,
            generated_at_utc=FIXED_GENERATED_AT,
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
        "8M-C01",
        "ready dataset emits",
        ready_dataset.emitted
        and ready_dataset.dataset_status == "ready",
        ready_dataset.to_dict(),
        {
            "emitted": True,
            "dataset_status": "ready",
        },
    )

    record(
        "8M-C02",
        "two rows accepted",
        len(ready_dataset.rows) == 2,
        len(ready_dataset.rows),
        2,
    )

    record(
        "8M-C03",
        "row ids deterministic",
        all(
            row.dataset_row_id.startswith(
                "matchup-shadow-"
            )
            for row in ready_dataset.rows
        ),
        [
            row.dataset_row_id
            for row in ready_dataset.rows
        ],
        "matchup-shadow-*",
    )

    record(
        "8M-C04",
        "rows sorted by date",
        [
            row.observation_date_utc
            for row in ready_dataset.rows
        ]
        == [
            "2026-06-30",
            "2026-07-01",
        ],
        [
            row.observation_date_utc
            for row in ready_dataset.rows
        ],
        [
            "2026-06-30",
            "2026-07-01",
        ],
    )

    record(
        "8M-C05",
        "partitions deterministic",
        [
            partition.partition_key
            for partition in ready_dataset.partitions
        ]
        == [
            "2026_06_30",
            "2026_07_01",
        ],
        [
            partition.partition_key
            for partition in ready_dataset.partitions
        ],
        [
            "2026_06_30",
            "2026_07_01",
        ],
    )

    disabled_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [complete_bundle],
            enabled=False,
            generated_at_utc=FIXED_GENERATED_AT,
        )
    )

    record(
        "8M-C06",
        "disabled path non-emitting",
        disabled_dataset.emitted is False
        and disabled_dataset.manifest is None
        and not disabled_dataset.rows,
        disabled_dataset.to_dict(),
        {
            "emitted": False,
            "manifest": None,
            "rows": [],
        },
    )

    empty_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [],
            enabled=True,
            generated_at_utc=FIXED_GENERATED_AT,
        )
    )

    record(
        "8M-C07",
        "empty dataset supported",
        empty_dataset.dataset_status == "empty",
        empty_dataset.dataset_status,
        "empty",
    )

    missing_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [None],
            enabled=True,
            generated_at_utc=FIXED_GENERATED_AT,
        )
    )

    record(
        "8M-C08",
        "missing bundle invalid",
        missing_dataset.dataset_status == "invalid",
        missing_dataset.to_dict(),
        {
            "dataset_status": "invalid",
        },
    )

    non_emitted_bundle = (
        observe_pitch_type_matchup_overlay(
            overlay,
            enabled=False,
        )
    )

    skipped_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [non_emitted_bundle],
            enabled=True,
            generated_at_utc=FIXED_GENERATED_AT,
        )
    )

    record(
        "8M-C09",
        "non-emitted observation skipped",
        skipped_dataset.dataset_status == "empty"
        and (
            "matchup_shadow_observation_not_emitted"
            in skipped_dataset.diagnostic_codes
        ),
        skipped_dataset.to_dict(),
        {
            "dataset_status": "empty",
        },
    )

    exact_duplicate_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [
                complete_bundle,
                complete_bundle,
            ],
            enabled=True,
            generated_at_utc=FIXED_GENERATED_AT,
        )
    )

    record(
        "8M-C10",
        "exact duplicate collapsed",
        len(exact_duplicate_dataset.rows) == 1
        and exact_duplicate_dataset.manifest is not None
        and (
            exact_duplicate_dataset.manifest.duplicate_row_count
            == 1
        ),
        exact_duplicate_dataset.to_dict(),
        {
            "row_count": 1,
            "duplicate_row_count": 1,
        },
    )

    conflicting_bundle = replace(
        complete_bundle,
        summary=replace(
            complete_bundle.summary,
            coverage_share=0.8,
        ),
    )

    conflicting_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [
                complete_bundle,
                conflicting_bundle,
            ],
            enabled=True,
            generated_at_utc=FIXED_GENERATED_AT,
        )
    )

    record(
        "8M-C11",
        "conflicting duplicate invalid",
        conflicting_dataset.dataset_status
        == "invalid"
        and (
            "matchup_shadow_conflicting_duplicate"
            in conflicting_dataset.validation_errors
        ),
        conflicting_dataset.to_dict(),
        {
            "dataset_status": "invalid",
        },
    )

    partial_bundle = replace(
        complete_bundle,
        observability_status="partial",
    )

    partial_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [partial_bundle],
            enabled=True,
            generated_at_utc=FIXED_GENERATED_AT,
        )
    )

    record(
        "8M-C12",
        "partial dataset supported",
        partial_dataset.dataset_status == "partial",
        partial_dataset.dataset_status,
        "partial",
    )

    record(
        "8M-C13",
        "manifest counts reconcile",
        ready_dataset.manifest is not None
        and ready_dataset.manifest.row_count
        == len(ready_dataset.rows)
        and ready_dataset.manifest.partition_count
        == len(ready_dataset.partitions),
        (
            ready_dataset.manifest.to_dict()
            if ready_dataset.manifest
            else None
        ),
        {
            "row_count": 2,
            "partition_count": 2,
        },
    )

    record(
        "8M-C14",
        "manifest date bounds reconcile",
        ready_dataset.manifest is not None
        and (
            ready_dataset.manifest.minimum_observation_date_utc
            == "2026-06-30"
        )
        and (
            ready_dataset.manifest.maximum_observation_date_utc
            == "2026-07-01"
        ),
        (
            ready_dataset.manifest.to_dict()
            if ready_dataset.manifest
            else None
        ),
        {
            "minimum": "2026-06-30",
            "maximum": "2026-07-01",
        },
    )

    record(
        "8M-C15",
        "schema fingerprint deterministic",
        ready_dataset.manifest is not None
        and len(
            ready_dataset.manifest.schema_fingerprint
        )
        == 64,
        (
            ready_dataset.manifest.schema_fingerprint
            if ready_dataset.manifest
            else None
        ),
        "64_character_sha256",
    )

    repeated_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [
                complete_bundle,
                second_bundle,
            ],
            enabled=True,
            generated_at_utc=FIXED_GENERATED_AT,
        )
    )

    record(
        "8M-C16",
        "dataset serialization deterministic",
        ready_dataset.to_dict()
        == repeated_dataset.to_dict(),
        ready_dataset.to_dict(),
        repeated_dataset.to_dict(),
    )

    record(
        "8M-C17",
        "source versions retained",
        all(
            row.overlay_version == "8I-v1"
            and row.observability_version
            == "8K-v1"
            and row.shadow_dataset_version
            == "8M-v1"
            for row in ready_dataset.rows
        ),
        [
            row.to_dict()
            for row in ready_dataset.rows
        ],
        {
            "overlay_version": "8I-v1",
            "observability_version": "8K-v1",
            "shadow_dataset_version": "8M-v1",
        },
    )

    record(
        "8M-C18",
        "append-only flag explicit",
        ready_dataset.append_only is True,
        ready_dataset.append_only,
        True,
    )

    record(
        "8M-C19",
        "manifest production authority false",
        ready_dataset.manifest is not None
        and (
            ready_dataset.manifest.production_authority
            is False
        ),
        (
            ready_dataset.manifest.production_authority
            if ready_dataset.manifest
            else None
        ),
        False,
    )

    record(
        "8M-C20",
        "all prohibited authority remains false",
        all(
            value is False
            for value in [
                ready_dataset.historical_outcomes_joined,
                ready_dataset.predictive_evaluation_executed,
                ready_dataset.production_authority,
                ready_dataset.production_behavior_changed,
                ready_dataset.simulation_behavior_changed,
            ]
        ),
        ready_dataset.to_dict(),
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
            "check": "eight_l_predecessor_present",
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
            "check": "ready_dataset_supported",
            "actual": ready_dataset.dataset_status,
            "expected": "ready",
            "passed": (
                ready_dataset.dataset_status == "ready"
            ),
        },
        {
            "check": "two_rows_accepted",
            "actual": len(ready_dataset.rows),
            "expected": 2,
            "passed": len(ready_dataset.rows) == 2,
        },
        {
            "check": "deterministic_row_ids_supported",
            "actual": all(
                row.dataset_row_id.startswith(
                    "matchup-shadow-"
                )
                for row in ready_dataset.rows
            ),
            "expected": True,
            "passed": all(
                row.dataset_row_id.startswith(
                    "matchup-shadow-"
                )
                for row in ready_dataset.rows
            ),
        },
        {
            "check": "date_ordering_supported",
            "actual": [
                row.observation_date_utc
                for row in ready_dataset.rows
            ],
            "expected": [
                "2026-06-30",
                "2026-07-01",
            ],
            "passed": [
                row.observation_date_utc
                for row in ready_dataset.rows
            ]
            == [
                "2026-06-30",
                "2026-07-01",
            ],
        },
        {
            "check": "partitioning_supported",
            "actual": [
                partition.partition_key
                for partition
                in ready_dataset.partitions
            ],
            "expected": [
                "2026_06_30",
                "2026_07_01",
            ],
            "passed": [
                partition.partition_key
                for partition
                in ready_dataset.partitions
            ]
            == [
                "2026_06_30",
                "2026_07_01",
            ],
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled_dataset.emitted,
            "expected": False,
            "passed": disabled_dataset.emitted is False,
        },
        {
            "check": "empty_dataset_supported",
            "actual": empty_dataset.dataset_status,
            "expected": "empty",
            "passed": (
                empty_dataset.dataset_status == "empty"
            ),
        },
        {
            "check": "invalid_dataset_supported",
            "actual": missing_dataset.dataset_status,
            "expected": "invalid",
            "passed": (
                missing_dataset.dataset_status
                == "invalid"
            ),
        },
        {
            "check": "partial_dataset_supported",
            "actual": partial_dataset.dataset_status,
            "expected": "partial",
            "passed": (
                partial_dataset.dataset_status
                == "partial"
            ),
        },
        {
            "check": "exact_duplicate_collapsed",
            "actual": (
                exact_duplicate_dataset.manifest.duplicate_row_count
                if exact_duplicate_dataset.manifest
                else None
            ),
            "expected": 1,
            "passed": (
                exact_duplicate_dataset.manifest
                is not None
                and len(
                    exact_duplicate_dataset.rows
                )
                == 1
                and (
                    exact_duplicate_dataset.manifest.duplicate_row_count
                    == 1
                )
            ),
        },
        {
            "check": "conflicting_duplicate_invalid",
            "actual": (
                conflicting_dataset.dataset_status
            ),
            "expected": "invalid",
            "passed": (
                conflicting_dataset.dataset_status
                == "invalid"
            ),
        },
        {
            "check": "manifest_counts_reconcile",
            "actual": (
                ready_dataset.manifest.to_dict()
                if ready_dataset.manifest
                else None
            ),
            "expected": {
                "row_count": 2,
                "partition_count": 2,
            },
            "passed": (
                ready_dataset.manifest is not None
                and ready_dataset.manifest.row_count
                == len(ready_dataset.rows)
                and ready_dataset.manifest.partition_count
                == len(ready_dataset.partitions)
            ),
        },
        {
            "check": "schema_fingerprint_supported",
            "actual": (
                ready_dataset.manifest.schema_fingerprint
                if ready_dataset.manifest
                else None
            ),
            "expected": "64_character_sha256",
            "passed": (
                ready_dataset.manifest is not None
                and len(
                    ready_dataset.manifest.schema_fingerprint
                )
                == 64
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                ready_dataset.to_dict()
                == repeated_dataset.to_dict()
            ),
            "expected": True,
            "passed": (
                ready_dataset.to_dict()
                == repeated_dataset.to_dict()
            ),
        },
        {
            "check": "append_only_contract_preserved",
            "actual": ready_dataset.append_only,
            "expected": True,
            "passed": ready_dataset.append_only is True,
        },
        {
            "check": "production_simulation_validation_authority_absent",
            "actual": any(
                [
                    ready_dataset.historical_outcomes_joined,
                    ready_dataset.predictive_evaluation_executed,
                    ready_dataset.production_authority,
                    ready_dataset.production_behavior_changed,
                    ready_dataset.simulation_behavior_changed,
                ]
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in [
                    ready_dataset.historical_outcomes_joined,
                    ready_dataset.predictive_evaluation_executed,
                    ready_dataset.production_authority,
                    ready_dataset.production_behavior_changed,
                    ready_dataset.simulation_behavior_changed,
                ]
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    status_order = [
        "ready",
        "partial",
        "empty",
        "invalid",
        "disabled",
    ]

    datasets = [
        ready_dataset,
        partial_dataset,
        empty_dataset,
        missing_dataset,
        disabled_dataset,
    ]

    status_rows = [
        {
            "dataset_status": status,
            "count": sum(
                1
                for dataset in datasets
                if dataset.dataset_status
                == status
            ),
        }
        for status in status_order
    ]

    authority_rows = [
        {
            "authority": (
                "pitch_type_matchup_overlay_shadow_dataset_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded append-only shadow dataset passed all checks."
            ),
        },
        {
            "authority": (
                "historical_outcome_enrichment"
            ),
            "granted": False,
            "reason": (
                "No outcomes are joined."
            ),
        },
        {
            "authority": (
                "predictive_evaluation"
            ),
            "granted": False,
            "reason": (
                "No predictive evaluation is executed."
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
        "pitch_type_matchup_overlay_shadow_dataset_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8N_pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_plan"
        if all_checks_passed
        else
        "8M_pitch_type_matchup_overlay_shadow_dataset_implementation_remediation"
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

    row_records = [
        row.to_dict()
        for row in ready_dataset.rows
    ]

    write_csv(
        OUTPUT_DIR / "shadow_dataset_rows.csv",
        list(row_records[0].keys())
        if row_records
        else ["dataset_row_id"],
        row_records,
    )

    partition_records = [
        partition.to_dict()
        for partition in ready_dataset.partitions
    ]

    write_csv(
        OUTPUT_DIR / "partition_manifest.csv",
        list(partition_records[0].keys())
        if partition_records
        else ["partition_key"],
        partition_records,
    )

    duplicate_records = [
        duplicate.to_dict()
        for duplicate in exact_duplicate_dataset.duplicates
    ]

    write_csv(
        OUTPUT_DIR / "duplicate_report.csv",
        list(duplicate_records[0].keys())
        if duplicate_records
        else [
            "dataset_row_id",
            "observation_id",
            "observation_date_utc",
            "duplicate_type",
            "duplicate_count",
        ],
        duplicate_records,
    )

    write_csv(
        OUTPUT_DIR / "status_counts.csv",
        [
            "dataset_status",
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
                    "Plan bounded quality gates for the diagnostic shadow dataset."
                    if all_checks_passed
                    else
                    "Remediate failed 8M implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8M implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    manifest_payload = (
        ready_dataset.manifest.to_dict()
        if ready_dataset.manifest
        else {}
    )

    write_json(
        OUTPUT_DIR
        / "shadow_dataset_manifest.json",
        manifest_payload,
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
        "shadow_dataset_version": (
            SHADOW_DATASET_VERSION
        ),
        "row_serialization_implemented": True,
        "date_partitioning_implemented": True,
        "deterministic_row_ids_implemented": True,
        "exact_duplicate_collapse_implemented": True,
        "conflicting_duplicate_detection_implemented": True,
        "schema_fingerprint_implemented": True,
        "append_only_contract_preserved": True,
        "disabled_path_non_emitting": True,
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
        "shadow_dataset_quality_gate_planning_allowed_next": (
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
                "shadow_dataset_rows.csv",
                "partition_manifest.csv",
                "duplicate_report.csv",
                "status_counts.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "shadow_dataset_manifest.json"
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
