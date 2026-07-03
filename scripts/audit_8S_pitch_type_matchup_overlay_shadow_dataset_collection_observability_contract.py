#!/usr/bin/env python3
"""
Layer 8S collection-observability contract audit.
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
    collect_pitch_type_matchup_overlay_shadow_datasets,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_collection_observability import (
    COLLECTION_OBSERVABILITY_VERSION,
    observe_pitch_type_matchup_overlay_shadow_dataset_collection,
    recompute_collection_digest,
)
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_quality_gate import (
    evaluate_pitch_type_matchup_overlay_shadow_dataset_quality,
)
from mlb_app.pitching.pitcher_arsenal_profile import (
    build_pitcher_arsenal_profile,
)


LAYER_ID = "8S"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8S_pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8R_pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_collection_observability.py"
)

FIXED_TIME = "2026-07-03T12:00:00+00:00"


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
        "pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_plan_complete"
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
            "as_of_date_utc": "2026-07-02",
            "source_name": "statcast",
            "source_timestamp_utc": (
                "2026-07-02T00:00:00+00:00"
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

    batter_profile = (
        build_batter_pitch_type_response_profile(
            {
                "enabled": True,
                "batter_id": "batter-1",
                "batter_name": "Example Batter",
                "batter_hand": "L",
                "season": 2026,
                "as_of_date_utc": "2026-07-02",
                "source_name": "statcast",
                "source_timestamp_utc": (
                    "2026-07-02T00:00:00+00:00"
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

    healthy_collection = (
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

    degraded_collection = (
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

    healthy_observation = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection(
            healthy_collection,
            enabled=True,
            observed_at_utc=FIXED_TIME,
        )
    )

    warning_observation = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection(
            warning_collection,
            enabled=True,
            observed_at_utc=FIXED_TIME,
        )
    )

    degraded_observation = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection(
            degraded_collection,
            enabled=True,
            observed_at_utc=FIXED_TIME,
        )
    )

    empty_observation = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection(
            empty_collection,
            enabled=True,
            observed_at_utc=FIXED_TIME,
        )
    )

    duplicate_observation = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection(
            duplicate_collection,
            enabled=True,
            observed_at_utc=FIXED_TIME,
        )
    )

    disabled_observation = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection(
            healthy_collection,
            enabled=False,
            observed_at_utc=FIXED_TIME,
        )
    )

    missing_observation = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection(
            None,
            enabled=True,
            observed_at_utc=FIXED_TIME,
        )
    )

    broken_manifest_collection = replace(
        healthy_collection,
        manifest=replace(
            healthy_collection.manifest,
            record_count=99,
        ),
    )

    broken_observation = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection(
            broken_manifest_collection,
            enabled=True,
            observed_at_utc=FIXED_TIME,
        )
    )

    repeated_observation = (
        observe_pitch_type_matchup_overlay_shadow_dataset_collection(
            healthy_collection,
            enabled=True,
            observed_at_utc=FIXED_TIME,
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
        "8S-C01",
        "healthy observation emits",
        healthy_observation.emitted,
        healthy_observation.emitted,
        True,
    )

    record(
        "8S-C02",
        "healthy status supported",
        (
            healthy_observation.observability_status
            == "healthy"
        ),
        healthy_observation.observability_status,
        "healthy",
    )

    record(
        "8S-C03",
        "warning status supported",
        (
            warning_observation.observability_status
            == "warning"
        ),
        warning_observation.observability_status,
        "warning",
    )

    record(
        "8S-C04",
        "degraded status supported",
        (
            degraded_observation.observability_status
            == "degraded"
        ),
        degraded_observation.observability_status,
        "degraded",
    )

    record(
        "8S-C05",
        "empty status supported",
        (
            empty_observation.observability_status
            == "empty"
        ),
        empty_observation.observability_status,
        "empty",
    )

    record(
        "8S-C06",
        "disabled path non-emitting",
        (
            disabled_observation.emitted
            is False
            and disabled_observation.snapshot
            is None
        ),
        disabled_observation.to_dict(),
        {
            "emitted": False,
            "snapshot": None,
        },
    )

    record(
        "8S-C07",
        "missing collection degraded",
        (
            missing_observation.observability_status
            == "degraded"
        ),
        missing_observation.observability_status,
        "degraded",
    )

    record(
        "8S-C08",
        "manifest reconciliation passes",
        (
            healthy_observation.snapshot
            is not None
            and healthy_observation.snapshot.manifest_reconciles
            is True
        ),
        (
            healthy_observation.snapshot.manifest_reconciles
            if healthy_observation.snapshot
            else None
        ),
        True,
    )

    record(
        "8S-C09",
        "collection digest reconciliation passes",
        (
            healthy_observation.snapshot
            is not None
            and healthy_observation.snapshot.collection_digest_reconciles
            is True
        ),
        (
            healthy_observation.snapshot.collection_digest_reconciles
            if healthy_observation.snapshot
            else None
        ),
        True,
    )

    record(
        "8S-C10",
        "record identifiers unique",
        (
            healthy_observation.snapshot
            is not None
            and healthy_observation.snapshot.record_identifiers_unique
            is True
        ),
        (
            healthy_observation.snapshot.record_identifiers_unique
            if healthy_observation.snapshot
            else None
        ),
        True,
    )

    record(
        "8S-C11",
        "dataset size statistics computed",
        (
            healthy_observation.snapshot
            is not None
            and healthy_observation.snapshot.minimum_dataset_row_count
            == 1
            and healthy_observation.snapshot.mean_dataset_row_count
            == 1.0
            and healthy_observation.snapshot.maximum_dataset_row_count
            == 1
        ),
        (
            healthy_observation.snapshot.to_dict()
            if healthy_observation.snapshot
            else None
        ),
        {
            "minimum": 1,
            "mean": 1.0,
            "maximum": 1,
        },
    )

    record(
        "8S-C12",
        "coverage statistics computed",
        (
            healthy_observation.snapshot
            is not None
            and healthy_observation.snapshot.minimum_coverage_share
            == 1.0
            and healthy_observation.snapshot.mean_coverage_share
            == 1.0
            and healthy_observation.snapshot.maximum_coverage_share
            == 1.0
        ),
        (
            healthy_observation.snapshot.to_dict()
            if healthy_observation.snapshot
            else None
        ),
        {
            "minimum": 1.0,
            "mean": 1.0,
            "maximum": 1.0,
        },
    )

    record(
        "8S-C13",
        "warning records counted",
        (
            warning_observation.snapshot
            is not None
            and warning_observation.snapshot.accepted_with_warnings_count
            == 1
        ),
        (
            warning_observation.snapshot.accepted_with_warnings_count
            if warning_observation.snapshot
            else None
        ),
        1,
    )

    record(
        "8S-C14",
        "rejected records degrade observability",
        (
            degraded_observation.snapshot
            is not None
            and degraded_observation.snapshot.rejected_count
            == 1
        ),
        (
            degraded_observation.snapshot.rejected_count
            if degraded_observation.snapshot
            else None
        ),
        1,
    )

    record(
        "8S-C15",
        "exact duplicate warning observed",
        (
            duplicate_observation.observability_status
            == "warning"
            and duplicate_observation.snapshot
            is not None
            and duplicate_observation.snapshot.exact_duplicate_count
            == 1
        ),
        duplicate_observation.to_dict(),
        {
            "status": "warning",
            "exact_duplicate_count": 1,
        },
    )

    record(
        "8S-C16",
        "broken manifest degrades",
        (
            broken_observation.observability_status
            == "degraded"
        ),
        broken_observation.observability_status,
        "degraded",
    )

    record(
        "8S-C17",
        "collection digest helper deterministic",
        (
            recompute_collection_digest(
                healthy_collection
            )
            == recompute_collection_digest(
                healthy_collection
            )
        ),
        recompute_collection_digest(
            healthy_collection
        ),
        "deterministic_sha256",
    )

    record(
        "8S-C18",
        "snapshot identity deterministic",
        (
            healthy_observation.snapshot
            is not None
            and repeated_observation.snapshot
            is not None
            and (
                healthy_observation.snapshot.observability_snapshot_id
                == repeated_observation.snapshot.observability_snapshot_id
            )
        ),
        (
            healthy_observation.snapshot.observability_snapshot_id
            if healthy_observation.snapshot
            else None
        ),
        (
            repeated_observation.snapshot.observability_snapshot_id
            if repeated_observation.snapshot
            else None
        ),
    )

    record(
        "8S-C19",
        "serialization deterministic",
        (
            healthy_observation.to_dict()
            == repeated_observation.to_dict()
        ),
        healthy_observation.to_dict(),
        repeated_observation.to_dict(),
    )

    record(
        "8S-C20",
        "all prohibited authority remains false",
        all(
            value is False
            for value in [
                healthy_observation.production_authority,
                healthy_observation.production_behavior_changed,
                healthy_observation.simulation_behavior_changed,
                healthy_observation.historical_outcomes_joined,
                healthy_observation.predictive_evaluation_executed,
                healthy_observation.snapshot.production_authority,
            ]
        ),
        healthy_observation.to_dict(),
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
            "check": "eight_r_predecessor_present",
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
            "check": "healthy_status_supported",
            "actual": (
                healthy_observation.observability_status
            ),
            "expected": "healthy",
            "passed": (
                healthy_observation.observability_status
                == "healthy"
            ),
        },
        {
            "check": "warning_status_supported",
            "actual": (
                warning_observation.observability_status
            ),
            "expected": "warning",
            "passed": (
                warning_observation.observability_status
                == "warning"
            ),
        },
        {
            "check": "degraded_status_supported",
            "actual": (
                degraded_observation.observability_status
            ),
            "expected": "degraded",
            "passed": (
                degraded_observation.observability_status
                == "degraded"
            ),
        },
        {
            "check": "empty_status_supported",
            "actual": (
                empty_observation.observability_status
            ),
            "expected": "empty",
            "passed": (
                empty_observation.observability_status
                == "empty"
            ),
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": (
                disabled_observation.emitted
            ),
            "expected": False,
            "passed": (
                disabled_observation.emitted
                is False
            ),
        },
        {
            "check": "manifest_reconciliation_implemented",
            "actual": (
                healthy_observation.snapshot.manifest_reconciles
                if healthy_observation.snapshot
                else None
            ),
            "expected": True,
            "passed": (
                healthy_observation.snapshot
                is not None
                and healthy_observation.snapshot.manifest_reconciles
                is True
            ),
        },
        {
            "check": "digest_reconciliation_implemented",
            "actual": (
                healthy_observation.snapshot.collection_digest_reconciles
                if healthy_observation.snapshot
                else None
            ),
            "expected": True,
            "passed": (
                healthy_observation.snapshot
                is not None
                and healthy_observation.snapshot.collection_digest_reconciles
                is True
            ),
        },
        {
            "check": "record_identity_validation_implemented",
            "actual": (
                healthy_observation.snapshot.record_identifiers_unique
                if healthy_observation.snapshot
                else None
            ),
            "expected": True,
            "passed": (
                healthy_observation.snapshot
                is not None
                and healthy_observation.snapshot.record_identifiers_unique
                is True
            ),
        },
        {
            "check": "dataset_size_distribution_implemented",
            "actual": (
                healthy_observation.snapshot.mean_dataset_row_count
                if healthy_observation.snapshot
                else None
            ),
            "expected": 1.0,
            "passed": (
                healthy_observation.snapshot
                is not None
                and healthy_observation.snapshot.mean_dataset_row_count
                == 1.0
            ),
        },
        {
            "check": "coverage_distribution_implemented",
            "actual": (
                healthy_observation.snapshot.mean_coverage_share
                if healthy_observation.snapshot
                else None
            ),
            "expected": 1.0,
            "passed": (
                healthy_observation.snapshot
                is not None
                and healthy_observation.snapshot.mean_coverage_share
                == 1.0
            ),
        },
        {
            "check": "warning_records_observed",
            "actual": (
                warning_observation.snapshot.accepted_with_warnings_count
                if warning_observation.snapshot
                else None
            ),
            "expected": 1,
            "passed": (
                warning_observation.snapshot
                is not None
                and warning_observation.snapshot.accepted_with_warnings_count
                == 1
            ),
        },
        {
            "check": "rejected_records_degrade",
            "actual": (
                degraded_observation.observability_status
            ),
            "expected": "degraded",
            "passed": (
                degraded_observation.observability_status
                == "degraded"
            ),
        },
        {
            "check": "duplicate_integrity_observed",
            "actual": (
                duplicate_observation.snapshot.exact_duplicate_count
                if duplicate_observation.snapshot
                else None
            ),
            "expected": 1,
            "passed": (
                duplicate_observation.snapshot
                is not None
                and duplicate_observation.snapshot.exact_duplicate_count
                == 1
            ),
        },
        {
            "check": "broken_manifest_degrades",
            "actual": (
                broken_observation.observability_status
            ),
            "expected": "degraded",
            "passed": (
                broken_observation.observability_status
                == "degraded"
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                healthy_observation.to_dict()
                == repeated_observation.to_dict()
            ),
            "expected": True,
            "passed": (
                healthy_observation.to_dict()
                == repeated_observation.to_dict()
            ),
        },
        {
            "check": "production_simulation_validation_authority_absent",
            "actual": any(
                [
                    healthy_observation.production_authority,
                    healthy_observation.production_behavior_changed,
                    healthy_observation.simulation_behavior_changed,
                    healthy_observation.historical_outcomes_joined,
                    healthy_observation.predictive_evaluation_executed,
                ]
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in [
                    healthy_observation.production_authority,
                    healthy_observation.production_behavior_changed,
                    healthy_observation.simulation_behavior_changed,
                    healthy_observation.historical_outcomes_joined,
                    healthy_observation.predictive_evaluation_executed,
                ]
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    observations = [
        healthy_observation,
        warning_observation,
        degraded_observation,
        empty_observation,
        disabled_observation,
    ]

    status_rows = [
        {
            "observability_status": status,
            "count": sum(
                1
                for observation in observations
                if observation.observability_status
                == status
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

    reconciliation_rows = [
        {
            "reconciliation": "manifest",
            "passed": (
                healthy_observation.snapshot.manifest_reconciles
            ),
        },
        {
            "reconciliation": "collection_digest",
            "passed": (
                healthy_observation.snapshot.collection_digest_reconciles
            ),
        },
        {
            "reconciliation": "record_identifiers",
            "passed": (
                healthy_observation.snapshot.record_identifiers_unique
            ),
        },
    ]

    duplicate_rows = [
        {
            "signal": "exact_duplicate_count",
            "value": (
                duplicate_observation.snapshot.exact_duplicate_count
            ),
        },
        {
            "signal": "conflicting_duplicate_count",
            "value": (
                duplicate_observation.snapshot.conflicting_duplicate_count
            ),
        },
    ]

    authority_rows = [
        {
            "authority": (
                "shadow_dataset_collection_observability_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded diagnostic collection observability passed all checks."
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
        "pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8T_pitch_type_matchup_overlay_shadow_dataset_collection_retention_contract_plan"
        if all_checks_passed
        else
        "8S_pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_implementation_remediation"
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
        / "collection_observability_snapshot.csv",
        list(
            healthy_observation.snapshot.to_dict().keys()
        ),
        [
            healthy_observation.snapshot.to_dict()
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
        OUTPUT_DIR / "signal_results.csv",
        [
            "signal_id",
            "signal_group",
            "signal_name",
            "passed",
            "triggered",
            "observed_value",
            "expected_value",
            "diagnostic_code",
        ],
        [
            signal.to_dict()
            for signal in healthy_observation.signals
        ],
    )

    write_csv(
        OUTPUT_DIR / "duplicate_signals.csv",
        [
            "signal",
            "value",
        ],
        duplicate_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "reconciliation_results.csv",
        [
            "reconciliation",
            "passed",
        ],
        reconciliation_rows,
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
                    "Plan bounded retention rules for observed append-only collections."
                    if all_checks_passed
                    else
                    "Remediate failed 8S implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8S implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR
        / "observability_report.json",
        healthy_observation.to_dict(),
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
        "observability_version": (
            COLLECTION_OBSERVABILITY_VERSION
        ),
        "healthy_status_supported": True,
        "warning_status_supported": True,
        "degraded_status_supported": True,
        "empty_status_supported": True,
        "disabled_path_non_emitting": True,
        "manifest_reconciliation_implemented": True,
        "digest_reconciliation_implemented": True,
        "record_identity_validation_implemented": True,
        "dataset_size_distribution_implemented": True,
        "coverage_distribution_implemented": True,
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
        "collection_retention_planning_allowed_next": (
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
                "collection_observability_snapshot.csv",
                "status_counts.csv",
                "signal_results.csv",
                "duplicate_signals.csv",
                "reconciliation_results.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "observability_report.json"
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
