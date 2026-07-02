#!/usr/bin/env python3
"""
Layer 8K pitch-type matchup overlay observability audit.
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
    OBSERVABILITY_VERSION,
    aggregate_matchup_overlay_observations,
    coverage_bucket,
    observe_pitch_type_matchup_overlay,
)
from mlb_app.pitching.pitcher_arsenal_profile import (
    build_pitcher_arsenal_profile,
)


LAYER_ID = "8K"
LAYER_NAME = (
    "pitch_type_matchup_overlay_observability_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8K_pitch_type_matchup_overlay_observability_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8J_pitch_type_matchup_overlay_observability_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_observability.py"
)


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
        "pitch_type_matchup_overlay_observability_contract_plan_complete"
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

    resolved_overlay = (
        build_pitch_type_matchup_overlay(
            pitcher_profile,
            batter_profile,
            enabled=True,
        )
    )

    resolved_bundle = (
        observe_pitch_type_matchup_overlay(
            resolved_overlay,
            enabled=True,
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
        "8K-C01",
        "resolved observability emits",
        resolved_bundle.emitted
        and resolved_bundle.observability_status
        == "complete",
        resolved_bundle.to_dict(),
        {
            "emitted": True,
            "observability_status": "complete",
        },
    )

    record(
        "8K-C02",
        "deterministic observation id emitted",
        resolved_bundle.summary is not None
        and resolved_bundle.summary.observation_id.startswith(
            "matchup-overlay-"
        ),
        (
            resolved_bundle.summary.observation_id
            if resolved_bundle.summary
            else None
        ),
        "matchup-overlay-*",
    )

    record(
        "8K-C03",
        "summary entry count reconciles",
        resolved_bundle.summary is not None
        and (
            resolved_bundle.summary.overlay_entry_count
            == len(resolved_bundle.entries)
            == 3
        ),
        {
            "summary_count": (
                resolved_bundle.summary.overlay_entry_count
                if resolved_bundle.summary
                else None
            ),
            "entry_count": len(
                resolved_bundle.entries
            ),
        },
        {
            "summary_count": 3,
            "entry_count": 3,
        },
    )

    record(
        "8K-C04",
        "pitcher-only count observed",
        resolved_bundle.summary is not None
        and (
            resolved_bundle.summary.pitcher_only_entry_count
            == 1
        ),
        (
            resolved_bundle.summary.pitcher_only_entry_count
            if resolved_bundle.summary
            else None
        ),
        1,
    )

    record(
        "8K-C05",
        "matched and unmatched usage observed",
        resolved_bundle.summary is not None
        and (
            resolved_bundle.summary.matched_usage_share
            == 0.9
        )
        and (
            resolved_bundle.summary.unmatched_usage_share
            == 0.1
        ),
        (
            resolved_bundle.summary.to_dict()
            if resolved_bundle.summary
            else None
        ),
        {
            "matched_usage_share": 0.9,
            "unmatched_usage_share": 0.1,
        },
    )

    disabled_bundle = (
        observe_pitch_type_matchup_overlay(
            resolved_overlay,
            enabled=False,
        )
    )

    record(
        "8K-C06",
        "disabled path non-emitting",
        disabled_bundle.emitted is False
        and disabled_bundle.summary is None
        and not disabled_bundle.entries,
        disabled_bundle.to_dict(),
        {
            "emitted": False,
            "summary": None,
            "entries": [],
        },
    )

    missing_bundle = (
        observe_pitch_type_matchup_overlay(
            None,
            enabled=True,
        )
    )

    record(
        "8K-C07",
        "missing overlay invalid",
        missing_bundle.observability_status
        == "invalid",
        missing_bundle.observability_status,
        "invalid",
    )

    empty_overlay = replace(
        resolved_overlay,
        overlay_entries=(),
        matched_pitch_count=0,
        unmatched_pitch_count=0,
        coverage_share=0.0,
    )

    empty_bundle = (
        observe_pitch_type_matchup_overlay(
            empty_overlay,
            enabled=True,
        )
    )

    record(
        "8K-C08",
        "empty overlay observed",
        empty_bundle.observability_status
        == "empty",
        empty_bundle.observability_status,
        "empty",
    )

    partial_overlay = replace(
        resolved_overlay,
        pitcher_id=None,
    )

    partial_bundle = (
        observe_pitch_type_matchup_overlay(
            partial_overlay,
            enabled=True,
        )
    )

    record(
        "8K-C09",
        "missing optional identity yields partial",
        partial_bundle.observability_status
        == "partial",
        partial_bundle.observability_status,
        "partial",
    )

    invalid_overlay = replace(
        resolved_overlay,
        coverage_share=1.5,
    )

    invalid_bundle = (
        observe_pitch_type_matchup_overlay(
            invalid_overlay,
            enabled=True,
        )
    )

    record(
        "8K-C10",
        "invalid coverage detected",
        invalid_bundle.observability_status
        == "invalid",
        invalid_bundle.to_dict(),
        {
            "observability_status": "invalid",
        },
    )

    fallback_overlay = (
        build_pitch_type_matchup_overlay(
            pitcher_profile,
            batter_profile,
            count_context="two_strike",
            enabled=True,
        )
    )

    fallback_bundle = (
        observe_pitch_type_matchup_overlay(
            fallback_overlay,
            enabled=True,
        )
    )

    record(
        "8K-C11",
        "fallback entries counted",
        fallback_bundle.summary is not None
        and (
            fallback_bundle.summary.fallback_entry_count
            == 2
        ),
        (
            fallback_bundle.summary.fallback_entry_count
            if fallback_bundle.summary
            else None
        ),
        2,
    )

    record(
        "8K-C12",
        "entry ordinals contiguous",
        [
            entry.entry_ordinal
            for entry in resolved_bundle.entries
        ]
        == [0, 1, 2],
        [
            entry.entry_ordinal
            for entry in resolved_bundle.entries
        ],
        [0, 1, 2],
    )

    record(
        "8K-C13",
        "response presence flags correct",
        [
            entry.response_available
            for entry in resolved_bundle.entries
        ]
        == [True, True, False],
        [
            entry.response_available
            for entry in resolved_bundle.entries
        ],
        [True, True, False],
    )

    record(
        "8K-C14",
        "metric presence flags correct",
        resolved_bundle.entries[
            0
        ].swing_rate_present
        and resolved_bundle.entries[
            0
        ].whiff_rate_present
        and resolved_bundle.entries[
            0
        ].contact_rate_present
        and not resolved_bundle.entries[
            2
        ].swing_rate_present,
        [
            entry.to_dict()
            for entry in resolved_bundle.entries
        ],
        "presence_flags_match_fixture",
    )

    repeated_bundle = (
        observe_pitch_type_matchup_overlay(
            resolved_overlay,
            enabled=True,
        )
    )

    record(
        "8K-C15",
        "serialization deterministic",
        resolved_bundle.to_dict()
        == repeated_bundle.to_dict(),
        resolved_bundle.to_dict(),
        repeated_bundle.to_dict(),
    )

    record(
        "8K-C16",
        "coverage buckets deterministic",
        [
            coverage_bucket(value)
            for value in (
                0.0,
                0.2,
                0.6,
                0.9,
                1.0,
            )
        ]
        == [
            "coverage_0",
            "coverage_gt_0_lt_0_5",
            "coverage_gte_0_5_lt_0_8",
            "coverage_gte_0_8_lt_1",
            "coverage_1",
        ],
        [
            coverage_bucket(value)
            for value in (
                0.0,
                0.2,
                0.6,
                0.9,
                1.0,
            )
        ],
        [
            "coverage_0",
            "coverage_gt_0_lt_0_5",
            "coverage_gte_0_5_lt_0_8",
            "coverage_gte_0_8_lt_1",
            "coverage_1",
        ],
    )

    aggregate = (
        aggregate_matchup_overlay_observations(
            [
                resolved_bundle,
                fallback_bundle,
                empty_bundle,
                disabled_bundle,
            ]
        )
    )

    record(
        "8K-C17",
        "aggregate counts overlays",
        aggregate.overlay_count == 4
        and aggregate.emitted_overlay_count
        == 3
        and aggregate.disabled_overlay_count
        == 1,
        aggregate.to_dict(),
        {
            "overlay_count": 4,
            "emitted_overlay_count": 3,
            "disabled_overlay_count": 1,
        },
    )

    record(
        "8K-C18",
        "aggregate coverage bounds ordered",
        aggregate.minimum_coverage_share
        is not None
        and aggregate.maximum_coverage_share
        is not None
        and (
            aggregate.minimum_coverage_share
            <= aggregate.mean_coverage_share
            <= aggregate.maximum_coverage_share
        ),
        aggregate.to_dict(),
        "min_lte_mean_lte_max",
    )

    record(
        "8K-C19",
        "versions retained",
        resolved_bundle.summary is not None
        and (
            resolved_bundle.summary.overlay_version
            == resolved_overlay.overlay_version
        )
        and (
            resolved_bundle.summary.observability_version
            == OBSERVABILITY_VERSION
        ),
        (
            resolved_bundle.summary.to_dict()
            if resolved_bundle.summary
            else None
        ),
        {
            "observability_version": "8K-v1",
        },
    )

    record(
        "8K-C20",
        "production and simulation authority remain false",
        resolved_bundle.summary is not None
        and all(
            value is False
            for value in [
                resolved_bundle.production_authority,
                resolved_bundle.production_behavior_changed,
                resolved_bundle.simulation_behavior_changed,
                resolved_bundle.summary.production_authority,
                resolved_bundle.summary.production_behavior_changed,
                resolved_bundle.summary.simulation_behavior_changed,
                resolved_bundle.summary.historical_outcomes_joined,
                resolved_bundle.summary.predictive_evaluation_executed,
            ]
        ),
        resolved_bundle.to_dict(),
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
            "check": "eight_j_predecessor_present",
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
            "check": "complete_observation_supported",
            "actual": (
                resolved_bundle.observability_status
            ),
            "expected": "complete",
            "passed": (
                resolved_bundle.observability_status
                == "complete"
            ),
        },
        {
            "check": "deterministic_observation_id_supported",
            "actual": (
                resolved_bundle.summary.observation_id
                if resolved_bundle.summary
                else None
            ),
            "expected": "matchup-overlay-*",
            "passed": (
                resolved_bundle.summary is not None
                and resolved_bundle.summary.observation_id.startswith(
                    "matchup-overlay-"
                )
            ),
        },
        {
            "check": "summary_entry_count_reconciles",
            "actual": (
                resolved_bundle.summary.overlay_entry_count
                if resolved_bundle.summary
                else None
            ),
            "expected": 3,
            "passed": (
                resolved_bundle.summary is not None
                and resolved_bundle.summary.overlay_entry_count
                == len(resolved_bundle.entries)
                == 3
            ),
        },
        {
            "check": "usage_shares_reconcile",
            "actual": [
                (
                    resolved_bundle.summary.matched_usage_share
                    if resolved_bundle.summary
                    else None
                ),
                (
                    resolved_bundle.summary.unmatched_usage_share
                    if resolved_bundle.summary
                    else None
                ),
            ],
            "expected": [0.9, 0.1],
            "passed": (
                resolved_bundle.summary is not None
                and resolved_bundle.summary.matched_usage_share
                == 0.9
                and resolved_bundle.summary.unmatched_usage_share
                == 0.1
            ),
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled_bundle.emitted,
            "expected": False,
            "passed": (
                disabled_bundle.emitted is False
            ),
        },
        {
            "check": "missing_overlay_invalid",
            "actual": (
                missing_bundle.observability_status
            ),
            "expected": "invalid",
            "passed": (
                missing_bundle.observability_status
                == "invalid"
            ),
        },
        {
            "check": "empty_observation_supported",
            "actual": (
                empty_bundle.observability_status
            ),
            "expected": "empty",
            "passed": (
                empty_bundle.observability_status
                == "empty"
            ),
        },
        {
            "check": "partial_observation_supported",
            "actual": (
                partial_bundle.observability_status
            ),
            "expected": "partial",
            "passed": (
                partial_bundle.observability_status
                == "partial"
            ),
        },
        {
            "check": "invalid_observation_supported",
            "actual": (
                invalid_bundle.observability_status
            ),
            "expected": "invalid",
            "passed": (
                invalid_bundle.observability_status
                == "invalid"
            ),
        },
        {
            "check": "fallback_counts_supported",
            "actual": (
                fallback_bundle.summary.fallback_entry_count
                if fallback_bundle.summary
                else None
            ),
            "expected": 2,
            "passed": (
                fallback_bundle.summary is not None
                and fallback_bundle.summary.fallback_entry_count
                == 2
            ),
        },
        {
            "check": "entry_ordinals_contiguous",
            "actual": [
                entry.entry_ordinal
                for entry in resolved_bundle.entries
            ],
            "expected": [0, 1, 2],
            "passed": [
                entry.entry_ordinal
                for entry in resolved_bundle.entries
            ]
            == [0, 1, 2],
        },
        {
            "check": "presence_flags_supported",
            "actual": [
                entry.response_available
                for entry in resolved_bundle.entries
            ],
            "expected": [
                True,
                True,
                False,
            ],
            "passed": [
                entry.response_available
                for entry in resolved_bundle.entries
            ]
            == [
                True,
                True,
                False,
            ],
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                resolved_bundle.to_dict()
                == repeated_bundle.to_dict()
            ),
            "expected": True,
            "passed": (
                resolved_bundle.to_dict()
                == repeated_bundle.to_dict()
            ),
        },
        {
            "check": "coverage_buckets_supported",
            "actual": [
                coverage_bucket(value)
                for value in (
                    0.0,
                    0.2,
                    0.6,
                    0.9,
                    1.0,
                )
            ],
            "expected": [
                "coverage_0",
                "coverage_gt_0_lt_0_5",
                "coverage_gte_0_5_lt_0_8",
                "coverage_gte_0_8_lt_1",
                "coverage_1",
            ],
            "passed": [
                coverage_bucket(value)
                for value in (
                    0.0,
                    0.2,
                    0.6,
                    0.9,
                    1.0,
                )
            ]
            == [
                "coverage_0",
                "coverage_gt_0_lt_0_5",
                "coverage_gte_0_5_lt_0_8",
                "coverage_gte_0_8_lt_1",
                "coverage_1",
            ],
        },
        {
            "check": "aggregate_counts_supported",
            "actual": aggregate.overlay_count,
            "expected": 4,
            "passed": (
                aggregate.overlay_count == 4
                and aggregate.emitted_overlay_count
                == 3
                and aggregate.disabled_overlay_count
                == 1
            ),
        },
        {
            "check": "production_simulation_validation_authority_absent",
            "actual": (
                resolved_bundle.production_authority
                or resolved_bundle.production_behavior_changed
                or resolved_bundle.simulation_behavior_changed
                or (
                    resolved_bundle.summary.production_authority
                    if resolved_bundle.summary
                    else True
                )
                or (
                    resolved_bundle.summary.historical_outcomes_joined
                    if resolved_bundle.summary
                    else True
                )
                or (
                    resolved_bundle.summary.predictive_evaluation_executed
                    if resolved_bundle.summary
                    else True
                )
            ),
            "expected": False,
            "passed": (
                resolved_bundle.summary is not None
                and all(
                    value is False
                    for value in [
                        resolved_bundle.production_authority,
                        resolved_bundle.production_behavior_changed,
                        resolved_bundle.simulation_behavior_changed,
                        resolved_bundle.summary.production_authority,
                        resolved_bundle.summary.production_behavior_changed,
                        resolved_bundle.summary.simulation_behavior_changed,
                        resolved_bundle.summary.historical_outcomes_joined,
                        resolved_bundle.summary.predictive_evaluation_executed,
                    ]
                )
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    status_order = [
        "resolved",
        "partial",
        "sparse",
        "stale",
        "unavailable",
        "invalid",
        "disabled",
    ]

    status_rows = [
        {
            "overlay_status": status,
            "count": (
                aggregate.disabled_overlay_count
                if status == "disabled"
                else getattr(
                    aggregate,
                    f"{status}_overlay_count",
                )
            ),
        }
        for status in status_order
    ]

    coverage_values = [
        bundle.summary.coverage_share
        for bundle in (
            resolved_bundle,
            fallback_bundle,
            empty_bundle,
        )
        if bundle.summary is not None
    ]

    coverage_bucket_order = [
        "coverage_0",
        "coverage_gt_0_lt_0_5",
        "coverage_gte_0_5_lt_0_8",
        "coverage_gte_0_8_lt_1",
        "coverage_1",
    ]

    coverage_rows = [
        {
            "coverage_bucket": bucket,
            "count": sum(
                1
                for value in coverage_values
                if coverage_bucket(value)
                == bucket
            ),
        }
        for bucket in coverage_bucket_order
    ]

    fallback_codes = [
        "pitch_type_matchup_count_context_fallback",
        "pitch_type_matchup_pitcher_hand_fallback",
        "pitch_type_matchup_batter_response_missing",
        "pitch_type_matchup_unknown_pitch_retained",
        "pitch_type_matchup_profile_dates_disagree",
    ]

    all_diagnostic_codes = [
        code
        for bundle in (
            resolved_bundle,
            fallback_bundle,
            empty_bundle,
            disabled_bundle,
        )
        for code in bundle.diagnostic_codes
    ]

    all_diagnostic_codes.extend(
        code
        for bundle in (
            resolved_bundle,
            fallback_bundle,
            empty_bundle,
        )
        for entry in bundle.entries
        for code in entry.diagnostic_codes
    )

    fallback_rows = [
        {
            "diagnostic_code": code,
            "count": all_diagnostic_codes.count(
                code
            ),
        }
        for code in fallback_codes
    ]

    authority_rows = [
        {
            "authority": (
                "pitch_type_matchup_overlay_observability_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded deterministic observability passed all checks."
            ),
        },
        {
            "authority": (
                "production_matchup_overlay_integration"
            ),
            "granted": False,
            "reason": (
                "Observability remains diagnostic-only."
            ),
        },
        {
            "authority": (
                "simulation_or_probability_change"
            ),
            "granted": False,
            "reason": (
                "Simulation and probability behavior remain unchanged."
            ),
        },
        {
            "authority": (
                "historical_validation_or_predictive_evaluation"
            ),
            "granted": False,
            "reason": (
                "No outcomes are joined and no predictive evaluation occurs."
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
        "pitch_type_matchup_overlay_observability_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_observability_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8L_pitch_type_matchup_overlay_shadow_dataset_contract_plan"
        if all_checks_passed
        else
        "8K_pitch_type_matchup_overlay_observability_implementation_remediation"
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
        OUTPUT_DIR / "overlay_observations.csv",
        list(
            resolved_bundle.summary.to_dict().keys()
        )
        if resolved_bundle.summary
        else ["observation_id"],
        [
            resolved_bundle.summary.to_dict()
        ]
        if resolved_bundle.summary
        else [],
    )

    entry_rows = [
        entry.to_dict()
        for entry in resolved_bundle.entries
    ]

    write_csv(
        OUTPUT_DIR
        / "overlay_entry_observations.csv",
        list(entry_rows[0].keys())
        if entry_rows
        else ["observation_id"],
        entry_rows,
    )

    write_csv(
        OUTPUT_DIR / "overlay_status_counts.csv",
        [
            "overlay_status",
            "count",
        ],
        status_rows,
    )

    write_csv(
        OUTPUT_DIR / "coverage_distribution.csv",
        [
            "coverage_bucket",
            "count",
        ],
        coverage_rows,
    )

    write_csv(
        OUTPUT_DIR / "fallback_counts.csv",
        [
            "diagnostic_code",
            "count",
        ],
        fallback_rows,
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
                    "Plan a bounded shadow dataset contract for observable matchup overlays."
                    if all_checks_passed
                    else
                    "Remediate failed 8K implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8K implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
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
            OBSERVABILITY_VERSION
        ),
        "summary_serialization_implemented": True,
        "entry_serialization_implemented": True,
        "aggregate_status_counts_implemented": True,
        "coverage_distribution_implemented": True,
        "fallback_counts_implemented": True,
        "deterministic_observation_ids_implemented": True,
        "deterministic_serialization_implemented": True,
        "presence_flags_implemented": True,
        "disabled_path_non_emitting": True,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "matchup_adjustments_activated": False,
        "historical_outcome_joined": False,
        "predictive_evaluation_executed": False,
        "tuning_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR
        / "observability_summary.json",
        {
            **summary,
            "aggregate": aggregate.to_dict(),
        },
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
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "shadow_dataset_planning_allowed_next": (
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
                "overlay_observations.csv",
                "overlay_entry_observations.csv",
                "overlay_status_counts.csv",
                "coverage_distribution.csv",
                "fallback_counts.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "observability_summary.json"
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
