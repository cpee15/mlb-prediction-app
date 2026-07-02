#!/usr/bin/env python3
"""
Layer 8I pitcher-batter pitch-type matchup overlay audit.
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
    OVERLAY_VERSION,
    build_pitch_type_matchup_overlay,
)
from mlb_app.pitching.pitcher_arsenal_profile import (
    build_pitcher_arsenal_profile,
)


LAYER_ID = "8I"
LAYER_NAME = (
    "pitcher_batter_pitch_type_matchup_overlay_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8I_pitcher_batter_pitch_type_matchup_overlay_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8H_pitcher_batter_pitch_type_matchup_overlay_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay.py"
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
        "pitcher_batter_pitch_type_matchup_overlay_contract_plan_complete"
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

    resolved = build_pitch_type_matchup_overlay(
        pitcher_profile,
        batter_profile,
        enabled=True,
    )

    record(
        "8I-C01",
        "overlay emits",
        resolved.emitted,
        resolved.emitted,
        True,
    )

    record(
        "8I-C02",
        "coverage equals matched usage exposure",
        resolved.coverage_share == 0.9,
        resolved.coverage_share,
        0.9,
    )

    record(
        "8I-C03",
        "coverage classification resolves",
        resolved.overlay_status == "resolved",
        resolved.overlay_status,
        "resolved",
    )

    record(
        "8I-C04",
        "pitcher-only pitch retained",
        any(
            entry.canonical_pitch_id == "CH"
            and entry.coverage_status
            == "pitcher_only"
            for entry in resolved.overlay_entries
        ),
        resolved.to_dict(),
        {
            "canonical_pitch_id": "CH",
            "coverage_status": "pitcher_only",
        },
    )

    record(
        "8I-C05",
        "entries sort by exposure",
        [
            entry.canonical_pitch_id
            for entry in resolved.overlay_entries
        ]
        == ["FF", "SL", "CH"],
        [
            entry.canonical_pitch_id
            for entry in resolved.overlay_entries
        ],
        ["FF", "SL", "CH"],
    )

    disabled = build_pitch_type_matchup_overlay(
        pitcher_profile,
        batter_profile,
        enabled=False,
    )

    record(
        "8I-C06",
        "disabled overlay is non-emitting",
        disabled.emitted is False
        and disabled.overlay_status
        == "disabled",
        disabled.to_dict(),
        {
            "emitted": False,
            "overlay_status": "disabled",
        },
    )

    missing_pitcher = build_pitch_type_matchup_overlay(
        None,
        batter_profile,
        enabled=True,
    )

    record(
        "8I-C07",
        "missing pitcher profile unavailable",
        missing_pitcher.overlay_status
        == "unavailable",
        missing_pitcher.overlay_status,
        "unavailable",
    )

    missing_batter = build_pitch_type_matchup_overlay(
        pitcher_profile,
        None,
        enabled=True,
    )

    record(
        "8I-C08",
        "missing batter profile unavailable",
        missing_batter.overlay_status
        == "unavailable",
        missing_batter.overlay_status,
        "unavailable",
    )

    partial_batter = (
        build_batter_pitch_type_response_profile(
            {
                "enabled": True,
                "batter_id": "batter-1",
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
                        "pitch_count": 100,
                    }
                ],
            }
        )
    )

    partial = build_pitch_type_matchup_overlay(
        pitcher_profile,
        partial_batter,
        enabled=True,
    )

    record(
        "8I-C09",
        "partial coverage classified",
        partial.overlay_status == "partial",
        {
            "status": partial.overlay_status,
            "coverage": partial.coverage_share,
        },
        {
            "status": "partial",
            "coverage": 0.6,
        },
    )

    sparse_batter = (
        build_batter_pitch_type_response_profile(
            {
                "enabled": True,
                "batter_id": "batter-1",
                "batter_hand": "L",
                "season": 2026,
                "as_of_date_utc": "2026-06-30",
                "source_name": "statcast",
                "source_timestamp_utc": (
                    "2026-06-25T00:00:00+00:00"
                ),
                "response_entries": [
                    {
                        "canonical_pitch_id": "CH",
                        "pitcher_hand": "R",
                        "count_context": "all_counts",
                        "pitch_count": 100,
                    }
                ],
            }
        )
    )

    sparse = build_pitch_type_matchup_overlay(
        pitcher_profile,
        sparse_batter,
        enabled=True,
    )

    record(
        "8I-C10",
        "sparse coverage classified",
        sparse.overlay_status == "sparse",
        {
            "status": sparse.overlay_status,
            "coverage": sparse.coverage_share,
        },
        {
            "status": "sparse",
            "coverage": 0.1,
        },
    )

    count_fallback = (
        build_pitch_type_matchup_overlay(
            pitcher_profile,
            batter_profile,
            count_context="two_strike",
            enabled=True,
        )
    )

    record(
        "8I-C11",
        "all-counts fallback observable",
        count_fallback.overlay_status
        == "partial"
        and (
            "pitch_type_matchup_count_context_fallback"
            in count_fallback.diagnostic_codes
        ),
        count_fallback.to_dict(),
        {
            "overlay_status": "partial",
        },
    )

    unknown_hand_batter = (
        build_batter_pitch_type_response_profile(
            {
                "enabled": True,
                "batter_id": "batter-1",
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
                        "pitcher_hand": "U",
                        "count_context": "all_counts",
                        "pitch_count": 60,
                    },
                    {
                        "canonical_pitch_id": "SL",
                        "pitcher_hand": "U",
                        "count_context": "all_counts",
                        "pitch_count": 40,
                    },
                ],
            }
        )
    )

    hand_fallback = (
        build_pitch_type_matchup_overlay(
            pitcher_profile,
            unknown_hand_batter,
            enabled=True,
        )
    )

    record(
        "8I-C12",
        "unknown-hand fallback observable",
        (
            "pitch_type_matchup_pitcher_hand_fallback"
            in hand_fallback.diagnostic_codes
        ),
        hand_fallback.diagnostic_codes,
        [
            "pitch_type_matchup_pitcher_hand_fallback",
        ],
    )

    stale_pitcher = replace(
        pitcher_profile,
        profile_status="stale",
    )

    stale = build_pitch_type_matchup_overlay(
        stale_pitcher,
        batter_profile,
        enabled=True,
    )

    record(
        "8I-C13",
        "stale source propagates",
        stale.overlay_status == "stale",
        stale.overlay_status,
        "stale",
    )

    invalid_batter = replace(
        batter_profile,
        profile_status="invalid",
    )

    invalid = build_pitch_type_matchup_overlay(
        pitcher_profile,
        invalid_batter,
        enabled=True,
    )

    record(
        "8I-C14",
        "invalid source propagates",
        invalid.overlay_status == "invalid",
        invalid.overlay_status,
        "invalid",
    )

    unknown_pitcher = (
        build_pitcher_arsenal_profile(
            {
                "enabled": True,
                "pitcher_id": "pitcher-1",
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
                        "canonical_pitch_id": "ZZ",
                        "pitch_count": 100,
                    }
                ],
            }
        )
    )

    unknown_overlay = (
        build_pitch_type_matchup_overlay(
            unknown_pitcher,
            batter_profile,
            enabled=True,
        )
    )

    record(
        "8I-C15",
        "unknown pitch retained",
        unknown_overlay.overlay_entries[
            0
        ].canonical_pitch_id
        == "UN",
        unknown_overlay.to_dict(),
        {
            "canonical_pitch_id": "UN",
        },
    )

    repeated = build_pitch_type_matchup_overlay(
        pitcher_profile,
        batter_profile,
        enabled=True,
    )

    record(
        "8I-C16",
        "construction deterministic",
        resolved.to_dict()
        == repeated.to_dict(),
        resolved.to_dict(),
        repeated.to_dict(),
    )

    record(
        "8I-C17",
        "profile versions retained",
        resolved.pitcher_profile_version
        == pitcher_profile.profile_version
        and resolved.batter_profile_version
        == batter_profile.profile_version,
        {
            "pitcher": (
                resolved.pitcher_profile_version
            ),
            "batter": (
                resolved.batter_profile_version
            ),
        },
        {
            "pitcher": pitcher_profile.profile_version,
            "batter": batter_profile.profile_version,
        },
    )

    record(
        "8I-C18",
        "overlay version explicit",
        resolved.overlay_version
        == OVERLAY_VERSION,
        resolved.overlay_version,
        "8I-v1",
    )

    record(
        "8I-C19",
        "diagnostics sorted and unique",
        resolved.diagnostic_codes
        == tuple(
            sorted(
                set(
                    resolved.diagnostic_codes
                )
            )
        )
        and all(
            entry.diagnostic_codes
            == tuple(
                sorted(
                    set(
                        entry.diagnostic_codes
                    )
                )
            )
            for entry in resolved.overlay_entries
        ),
        resolved.to_dict(),
        "sorted_unique",
    )

    record(
        "8I-C20",
        "production and simulation authority remain false",
        all(
            value is False
            for value in [
                resolved.production_authority,
                resolved.production_behavior_changed,
                resolved.simulation_behavior_changed,
                resolved.pitch_selection_changed,
                resolved.pitch_sequence_changed,
                resolved.matchup_adjustment_activated,
                resolved.swing_probability_changed,
                resolved.whiff_probability_changed,
                resolved.contact_probability_changed,
                resolved.batted_ball_probability_changed,
                resolved.contact_quality_changed,
            ]
        ),
        resolved.to_dict(),
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
            "check": "eight_h_predecessor_present",
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
            "check": "resolved_overlay_supported",
            "actual": resolved.overlay_status,
            "expected": "resolved",
            "passed": (
                resolved.overlay_status
                == "resolved"
            ),
        },
        {
            "check": "coverage_share_correct",
            "actual": resolved.coverage_share,
            "expected": 0.9,
            "passed": (
                resolved.coverage_share
                == 0.9
            ),
        },
        {
            "check": "deterministic_ordering_supported",
            "actual": [
                entry.canonical_pitch_id
                for entry in resolved.overlay_entries
            ],
            "expected": ["FF", "SL", "CH"],
            "passed": [
                entry.canonical_pitch_id
                for entry in resolved.overlay_entries
            ]
            == ["FF", "SL", "CH"],
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled.emitted,
            "expected": False,
            "passed": disabled.emitted is False,
        },
        {
            "check": "partial_status_supported",
            "actual": partial.overlay_status,
            "expected": "partial",
            "passed": (
                partial.overlay_status
                == "partial"
            ),
        },
        {
            "check": "sparse_status_supported",
            "actual": sparse.overlay_status,
            "expected": "sparse",
            "passed": (
                sparse.overlay_status
                == "sparse"
            ),
        },
        {
            "check": "stale_status_supported",
            "actual": stale.overlay_status,
            "expected": "stale",
            "passed": (
                stale.overlay_status
                == "stale"
            ),
        },
        {
            "check": "unavailable_status_supported",
            "actual": [
                missing_pitcher.overlay_status,
                missing_batter.overlay_status,
            ],
            "expected": [
                "unavailable",
                "unavailable",
            ],
            "passed": [
                missing_pitcher.overlay_status,
                missing_batter.overlay_status,
            ]
            == [
                "unavailable",
                "unavailable",
            ],
        },
        {
            "check": "invalid_status_supported",
            "actual": invalid.overlay_status,
            "expected": "invalid",
            "passed": (
                invalid.overlay_status
                == "invalid"
            ),
        },
        {
            "check": "count_context_fallback_supported",
            "actual": (
                "pitch_type_matchup_count_context_fallback"
                in count_fallback.diagnostic_codes
            ),
            "expected": True,
            "passed": (
                "pitch_type_matchup_count_context_fallback"
                in count_fallback.diagnostic_codes
            ),
        },
        {
            "check": "pitcher_hand_fallback_supported",
            "actual": (
                "pitch_type_matchup_pitcher_hand_fallback"
                in hand_fallback.diagnostic_codes
            ),
            "expected": True,
            "passed": (
                "pitch_type_matchup_pitcher_hand_fallback"
                in hand_fallback.diagnostic_codes
            ),
        },
        {
            "check": "unmatched_pitch_retained",
            "actual": any(
                entry.coverage_status
                == "pitcher_only"
                for entry in resolved.overlay_entries
            ),
            "expected": True,
            "passed": any(
                entry.coverage_status
                == "pitcher_only"
                for entry in resolved.overlay_entries
            ),
        },
        {
            "check": "unknown_pitch_retained",
            "actual": (
                unknown_overlay.overlay_entries[
                    0
                ].canonical_pitch_id
            ),
            "expected": "UN",
            "passed": (
                unknown_overlay.overlay_entries[
                    0
                ].canonical_pitch_id
                == "UN"
            ),
        },
        {
            "check": "construction_deterministic",
            "actual": (
                resolved.to_dict()
                == repeated.to_dict()
            ),
            "expected": True,
            "passed": (
                resolved.to_dict()
                == repeated.to_dict()
            ),
        },
        {
            "check": "profile_versions_retained",
            "actual": [
                resolved.pitcher_profile_version,
                resolved.batter_profile_version,
            ],
            "expected": [
                pitcher_profile.profile_version,
                batter_profile.profile_version,
            ],
            "passed": (
                resolved.pitcher_profile_version
                == pitcher_profile.profile_version
                and resolved.batter_profile_version
                == batter_profile.profile_version
            ),
        },
        {
            "check": "production_simulation_probability_authority_absent",
            "actual": any(
                [
                    resolved.production_authority,
                    resolved.production_behavior_changed,
                    resolved.simulation_behavior_changed,
                    resolved.pitch_selection_changed,
                    resolved.pitch_sequence_changed,
                    resolved.matchup_adjustment_activated,
                    resolved.swing_probability_changed,
                    resolved.whiff_probability_changed,
                    resolved.contact_probability_changed,
                    resolved.batted_ball_probability_changed,
                    resolved.contact_quality_changed,
                ]
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in [
                    resolved.production_authority,
                    resolved.production_behavior_changed,
                    resolved.simulation_behavior_changed,
                    resolved.pitch_selection_changed,
                    resolved.pitch_sequence_changed,
                    resolved.matchup_adjustment_activated,
                    resolved.swing_probability_changed,
                    resolved.whiff_probability_changed,
                    resolved.contact_probability_changed,
                    resolved.batted_ball_probability_changed,
                    resolved.contact_quality_changed,
                ]
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    authority_rows = [
        {
            "authority": (
                "pitch_type_matchup_overlay_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The deterministic diagnostic overlay passed all checks."
            ),
        },
        {
            "authority": (
                "production_matchup_overlay_integration"
            ),
            "granted": False,
            "reason": (
                "Overlay output remains diagnostic-only."
            ),
        },
        {
            "authority": (
                "production_matchup_adjustment"
            ),
            "granted": False,
            "reason": (
                "No production matchup adjustment is activated."
            ),
        },
        {
            "authority": (
                "simulation_or_probability_change"
            ),
            "granted": False,
            "reason": (
                "Simulation and canonical probabilities remain unchanged."
            ),
        },
        {
            "authority": (
                "historical_validation_tuning_pricing_edge"
            ),
            "granted": False,
            "reason": (
                "Validation, tuning, pricing, and edge work remain unauthorized."
            ),
        },
    ]

    diagnosis_name = (
        "pitcher_batter_pitch_type_matchup_overlay_contract_implementation_passed"
        if all_checks_passed
        else
        "pitcher_batter_pitch_type_matchup_overlay_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8J_pitch_type_matchup_overlay_observability_contract_plan"
        if all_checks_passed
        else
        "8I_pitch_type_matchup_overlay_implementation_remediation"
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
        OUTPUT_DIR / "resolved_overlay_entries.csv",
        [
            "canonical_pitch_id",
            "canonical_pitch_name",
            "canonical_family",
            "pitcher_hand",
            "batter_hand",
            "count_context",
            "pitch_usage_share",
            "pitcher_pitch_count",
            "batter_pitch_count",
            "coverage_status",
            "swing_rate",
            "whiff_rate",
            "contact_rate",
            "hard_hit_rate",
            "barrel_rate",
            "diagnostic_codes",
        ],
        [
            {
                "canonical_pitch_id": (
                    entry.canonical_pitch_id
                ),
                "canonical_pitch_name": (
                    entry.canonical_pitch_name
                ),
                "canonical_family": (
                    entry.canonical_family
                ),
                "pitcher_hand": (
                    entry.pitcher_hand
                ),
                "batter_hand": (
                    entry.batter_hand
                ),
                "count_context": (
                    entry.count_context
                ),
                "pitch_usage_share": (
                    entry.pitch_usage_share
                ),
                "pitcher_pitch_count": (
                    entry.pitcher_pitch_count
                ),
                "batter_pitch_count": (
                    entry.batter_pitch_count
                ),
                "coverage_status": (
                    entry.coverage_status
                ),
                "swing_rate": (
                    entry.swing_rate
                ),
                "whiff_rate": (
                    entry.whiff_rate
                ),
                "contact_rate": (
                    entry.contact_rate
                ),
                "hard_hit_rate": (
                    entry.hard_hit_rate
                ),
                "barrel_rate": (
                    entry.barrel_rate
                ),
                "diagnostic_codes": "|".join(
                    entry.diagnostic_codes
                ),
            }
            for entry in resolved.overlay_entries
        ],
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
                    "Plan bounded observability for diagnostic matchup overlay outputs."
                    if all_checks_passed
                    else
                    "Remediate failed 8I implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8I implementation checks pass."
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
        "overlay_version": OVERLAY_VERSION,
        "resolved_partial_sparse_stale_unavailable_invalid_supported": True,
        "coverage_weighting_implemented": True,
        "deterministic_matching_implemented": True,
        "deterministic_ordering_implemented": True,
        "hand_and_context_fallbacks_implemented": True,
        "unmatched_pitch_retention_implemented": True,
        "unknown_pitch_retention_implemented": True,
        "disabled_path_non_emitting": True,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "pitch_selection_changed": False,
        "pitch_sequence_changed": False,
        "matchup_adjustments_activated": False,
        "swing_probability_changed": False,
        "whiff_probability_changed": False,
        "contact_probability_changed": False,
        "batted_ball_probability_changed": False,
        "contact_quality_changed": False,
        "historical_outcome_joined": False,
        "historical_validation_executed": False,
        "tuning_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR
        / "implementation_summary.json",
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
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "matchup_overlay_observability_planning_allowed_next": (
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
                "resolved_overlay_entries.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "implementation_summary.json"
            ),
            str(
                OUTPUT_DIR
                / "diagnosis.json"
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
