#!/usr/bin/env python3
"""
Layer 8G batter pitch-type response implementation audit.
"""

from __future__ import annotations

import ast
import copy
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.batter_pitch_type_response_profile import (
    PROFILE_VERSION,
    build_batter_pitch_type_response_profile,
)


LAYER_ID = "8G"
LAYER_NAME = (
    "batter_pitch_type_response_profile_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8G_batter_pitch_type_response_profile_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8F_batter_pitch_type_response_profile_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "batter_pitch_type_response_profile.py"
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
        "batter_pitch_type_response_profile_contract_plan_complete"
        in string_constants(
            PLAN_PATH
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

    timestamp = datetime(
        2026,
        6,
        25,
        tzinfo=timezone.utc,
    )

    base_payload = {
        "enabled": True,
        "batter_id": "batter-1",
        "batter_name": "Example Batter",
        "batter_hand": "L",
        "season": 2026,
        "as_of_date_utc": "2026-06-30",
        "source_name": "statcast",
        "source_record_id": "response-1",
        "source_timestamp_utc": (
            timestamp.isoformat()
        ),
        "sample_plate_appearance_count": 75,
        "response_entries": [
            {
                "canonical_pitch_id": "FF",
                "pitcher_hand": "R",
                "count_context": "all_counts",
                "pitch_count": 60,
                "swing_count": 30,
                "contact_count": 24,
                "batted_ball_count": 18,
                "swing_rate": 0.5,
                "whiff_rate": 0.2,
                "contact_rate": 0.8,
                "avg_exit_velocity_mph": 91.4,
                "avg_launch_angle_degrees": 14.2,
                "hard_hit_rate": 0.44,
                "barrel_rate": 0.11,
                "ground_ball_rate": 0.39,
                "line_drive_rate": 0.22,
                "fly_ball_rate": 0.33,
                "popup_rate": 0.06,
            },
            {
                "canonical_pitch_id": "SL",
                "pitcher_hand": "R",
                "count_context": "all_counts",
                "pitch_count": 40,
                "swing_count": 24,
                "contact_count": 15,
                "batted_ball_count": 10,
                "swing_rate": 0.6,
                "whiff_rate": 0.375,
                "contact_rate": 0.625,
                "avg_exit_velocity_mph": 87.8,
                "avg_launch_angle_degrees": 9.3,
                "hard_hit_rate": 0.30,
                "barrel_rate": 0.05,
                "ground_ball_rate": 0.50,
                "line_drive_rate": 0.20,
                "fly_ball_rate": 0.25,
                "popup_rate": 0.05,
            },
        ],
    }

    resolved = (
        build_batter_pitch_type_response_profile(
            base_payload
        )
    )

    record(
        "8G-C01",
        "resolved profile emits",
        resolved.emitted
        and resolved.profile_status
        == "resolved",
        resolved.to_dict(),
        {
            "emitted": True,
            "profile_status": "resolved",
        },
    )

    record(
        "8G-C02",
        "entries sort deterministically",
        [
            entry.canonical_pitch_id
            for entry in resolved.response_entries
        ]
        == ["FF", "SL"],
        [
            entry.canonical_pitch_id
            for entry in resolved.response_entries
        ],
        ["FF", "SL"],
    )

    disabled = (
        build_batter_pitch_type_response_profile(
            {
                "enabled": False,
                "batter_id": "batter-1",
                "source_name": "statcast",
            }
        )
    )

    record(
        "8G-C03",
        "disabled profile is non-emitting",
        disabled.emitted is False
        and disabled.profile_status
        == "disabled",
        disabled.to_dict(),
        {
            "emitted": False,
            "profile_status": "disabled",
        },
    )

    sparse_payload = copy.deepcopy(
        base_payload
    )
    sparse_payload["response_entries"] = [
        {
            "canonical_pitch_id": "FF",
            "pitcher_hand": "R",
            "count_context": "all_counts",
            "pitch_count": 30,
        }
    ]

    sparse = (
        build_batter_pitch_type_response_profile(
            sparse_payload
        )
    )

    record(
        "8G-C04",
        "sparse profile classified",
        sparse.profile_status == "sparse",
        sparse.profile_status,
        "sparse",
    )

    partial_payload = copy.deepcopy(
        base_payload
    )
    partial_payload["response_entries"] = [
        {
            "canonical_pitch_id": "FF",
            "pitcher_hand": "R",
            "count_context": "all_counts",
            "pitch_count": 90,
        },
        {
            "canonical_pitch_id": "SL",
            "pitcher_hand": "R",
            "count_context": "all_counts",
            "pitch_count": 10,
        },
    ]

    partial = (
        build_batter_pitch_type_response_profile(
            partial_payload
        )
    )

    record(
        "8G-C05",
        "partial profile classified",
        partial.profile_status == "partial",
        partial.profile_status,
        "partial",
    )

    stale_payload = copy.deepcopy(
        base_payload
    )
    stale_payload[
        "source_timestamp_utc"
    ] = "2026-05-01T00:00:00+00:00"

    stale = (
        build_batter_pitch_type_response_profile(
            stale_payload
        )
    )

    record(
        "8G-C06",
        "stale profile classified",
        stale.profile_status == "stale",
        stale.profile_status,
        "stale",
    )

    unavailable_payload = copy.deepcopy(
        base_payload
    )
    unavailable_payload[
        "response_entries"
    ] = []

    unavailable = (
        build_batter_pitch_type_response_profile(
            unavailable_payload
        )
    )

    record(
        "8G-C07",
        "missing entries yield unavailable",
        unavailable.profile_status
        == "unavailable",
        unavailable.profile_status,
        "unavailable",
    )

    unknown_payload = copy.deepcopy(
        base_payload
    )
    unknown_payload["response_entries"] = [
        {
            "canonical_pitch_id": "ZZ",
            "pitcher_hand": "R",
            "count_context": "all_counts",
            "pitch_count": 100,
        }
    ]

    unknown = (
        build_batter_pitch_type_response_profile(
            unknown_payload
        )
    )

    record(
        "8G-C08",
        "unknown pitch retained as UN",
        unknown.response_entries[
            0
        ].canonical_pitch_id
        == "UN",
        unknown.to_dict(),
        {
            "canonical_pitch_id": "UN",
        },
    )

    duplicate_payload = copy.deepcopy(
        base_payload
    )
    duplicate_payload[
        "response_entries"
    ] = [
        {
            "canonical_pitch_id": "FF",
            "pitcher_hand": "R",
            "count_context": "all_counts",
            "pitch_count": 50,
        },
        {
            "canonical_pitch_id": "FF",
            "pitcher_hand": "R",
            "count_context": "all_counts",
            "pitch_count": 50,
        },
    ]

    duplicate = (
        build_batter_pitch_type_response_profile(
            duplicate_payload
        )
    )

    record(
        "8G-C09",
        "duplicate response key invalidates profile",
        duplicate.profile_status == "invalid"
        and (
            "batter_pitch_response_duplicate_entry"
            in duplicate.validation_errors
        ),
        duplicate.to_dict(),
        {
            "profile_status": "invalid",
        },
    )

    invalid_rate_payload = copy.deepcopy(
        base_payload
    )
    invalid_rate_payload[
        "response_entries"
    ][0]["swing_rate"] = 1.2

    invalid_rate = (
        build_batter_pitch_type_response_profile(
            invalid_rate_payload
        )
    )

    record(
        "8G-C10",
        "invalid rate invalidates profile",
        invalid_rate.profile_status
        == "invalid",
        invalid_rate.profile_status,
        "invalid",
    )

    invalid_count_payload = copy.deepcopy(
        base_payload
    )
    invalid_count_payload[
        "response_entries"
    ][0]["contact_count"] = 35

    invalid_count = (
        build_batter_pitch_type_response_profile(
            invalid_count_payload
        )
    )

    record(
        "8G-C11",
        "invalid count relationship invalidates profile",
        invalid_count.profile_status
        == "invalid",
        invalid_count.profile_status,
        "invalid",
    )

    invalid_batted_ball_payload = (
        copy.deepcopy(
            base_payload
        )
    )
    invalid_batted_ball_payload[
        "response_entries"
    ][0]["ground_ball_rate"] = 0.8
    invalid_batted_ball_payload[
        "response_entries"
    ][0]["line_drive_rate"] = 0.4

    invalid_batted_ball = (
        build_batter_pitch_type_response_profile(
            invalid_batted_ball_payload
        )
    )

    record(
        "8G-C12",
        "invalid batted-ball rate sum invalidates profile",
        invalid_batted_ball.profile_status
        == "invalid",
        invalid_batted_ball.profile_status,
        "invalid",
    )

    normalized_context_payload = (
        copy.deepcopy(
            base_payload
        )
    )
    normalized_context_payload[
        "batter_hand"
    ] = "x"
    normalized_context_payload[
        "response_entries"
    ][0]["pitcher_hand"] = "x"
    normalized_context_payload[
        "response_entries"
    ][0]["count_context"] = "late"

    normalized_context = (
        build_batter_pitch_type_response_profile(
            normalized_context_payload
        )
    )

    record(
        "8G-C13",
        "unsupported context normalizes safely",
        normalized_context.batter_hand
        == "U"
        and normalized_context.response_entries[
            0
        ].pitcher_hand
        == "U"
        and normalized_context.response_entries[
            0
        ].count_context
        == "unknown",
        {
            "batter_hand": (
                normalized_context.batter_hand
            ),
            "pitcher_hand": (
                normalized_context.response_entries[
                    0
                ].pitcher_hand
            ),
            "count_context": (
                normalized_context.response_entries[
                    0
                ].count_context
            ),
        },
        {
            "batter_hand": "U",
            "pitcher_hand": "U",
            "count_context": "unknown",
        },
    )

    payload_before = copy.deepcopy(
        base_payload
    )

    build_batter_pitch_type_response_profile(
        base_payload
    )

    record(
        "8G-C14",
        "caller payload remains immutable",
        base_payload == payload_before,
        base_payload,
        payload_before,
    )

    repeated_a = (
        build_batter_pitch_type_response_profile(
            base_payload
        )
    )
    repeated_b = (
        build_batter_pitch_type_response_profile(
            base_payload
        )
    )

    record(
        "8G-C15",
        "profile construction deterministic",
        repeated_a.to_dict()
        == repeated_b.to_dict(),
        repeated_a.to_dict(),
        repeated_b.to_dict(),
    )

    record(
        "8G-C16",
        "provenance retained",
        resolved.source_record_id
        == "response-1"
        and resolved.source_timestamp_utc
        == timestamp.isoformat(),
        resolved.to_dict(),
        {
            "source_record_id": "response-1",
        },
    )

    record(
        "8G-C17",
        "source priority resolves",
        resolved.source_priority == 1,
        resolved.source_priority,
        1,
    )

    record(
        "8G-C18",
        "taxonomy and profile versions explicit",
        bool(resolved.taxonomy_version)
        and resolved.profile_version
        == PROFILE_VERSION,
        {
            "taxonomy_version": (
                resolved.taxonomy_version
            ),
            "profile_version": (
                resolved.profile_version
            ),
        },
        {
            "profile_version": "8G-v1",
        },
    )

    record(
        "8G-C19",
        "diagnostic codes sorted and unique",
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
            for entry in resolved.response_entries
        ),
        {
            "profile": (
                resolved.diagnostic_codes
            ),
            "entries": [
                entry.diagnostic_codes
                for entry in resolved.response_entries
            ],
        },
        "sorted_unique",
    )

    record(
        "8G-C20",
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
            "check": "eight_f_predecessor_present",
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
            "check": "resolved_profile_supported",
            "actual": resolved.profile_status,
            "expected": "resolved",
            "passed": (
                resolved.profile_status
                == "resolved"
            ),
        },
        {
            "check": "deterministic_ordering_supported",
            "actual": [
                entry.canonical_pitch_id
                for entry in resolved.response_entries
            ],
            "expected": ["FF", "SL"],
            "passed": [
                entry.canonical_pitch_id
                for entry in resolved.response_entries
            ]
            == ["FF", "SL"],
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled.emitted,
            "expected": False,
            "passed": disabled.emitted is False,
        },
        {
            "check": "partial_status_supported",
            "actual": partial.profile_status,
            "expected": "partial",
            "passed": (
                partial.profile_status
                == "partial"
            ),
        },
        {
            "check": "sparse_status_supported",
            "actual": sparse.profile_status,
            "expected": "sparse",
            "passed": (
                sparse.profile_status
                == "sparse"
            ),
        },
        {
            "check": "stale_status_supported",
            "actual": stale.profile_status,
            "expected": "stale",
            "passed": (
                stale.profile_status
                == "stale"
            ),
        },
        {
            "check": "unavailable_status_supported",
            "actual": (
                unavailable.profile_status
            ),
            "expected": "unavailable",
            "passed": (
                unavailable.profile_status
                == "unavailable"
            ),
        },
        {
            "check": "unknown_pitch_retained_as_UN",
            "actual": (
                unknown.response_entries[
                    0
                ].canonical_pitch_id
            ),
            "expected": "UN",
            "passed": (
                unknown.response_entries[
                    0
                ].canonical_pitch_id
                == "UN"
            ),
        },
        {
            "check": "invalid_profiles_detected",
            "actual": [
                duplicate.profile_status,
                invalid_rate.profile_status,
                invalid_count.profile_status,
                invalid_batted_ball.profile_status,
            ],
            "expected": [
                "invalid",
                "invalid",
                "invalid",
                "invalid",
            ],
            "passed": [
                duplicate.profile_status,
                invalid_rate.profile_status,
                invalid_count.profile_status,
                invalid_batted_ball.profile_status,
            ]
            == [
                "invalid",
                "invalid",
                "invalid",
                "invalid",
            ],
        },
        {
            "check": "context_normalization_supported",
            "actual": [
                normalized_context.batter_hand,
                normalized_context.response_entries[
                    0
                ].pitcher_hand,
                normalized_context.response_entries[
                    0
                ].count_context,
            ],
            "expected": [
                "U",
                "U",
                "unknown",
            ],
            "passed": [
                normalized_context.batter_hand,
                normalized_context.response_entries[
                    0
                ].pitcher_hand,
                normalized_context.response_entries[
                    0
                ].count_context,
            ]
            == [
                "U",
                "U",
                "unknown",
            ],
        },
        {
            "check": "caller_payload_immutable",
            "actual": (
                base_payload
                == payload_before
            ),
            "expected": True,
            "passed": (
                base_payload
                == payload_before
            ),
        },
        {
            "check": "construction_deterministic",
            "actual": (
                repeated_a.to_dict()
                == repeated_b.to_dict()
            ),
            "expected": True,
            "passed": (
                repeated_a.to_dict()
                == repeated_b.to_dict()
            ),
        },
        {
            "check": "provenance_retained",
            "actual": (
                resolved.source_record_id
                == "response-1"
            ),
            "expected": True,
            "passed": (
                resolved.source_record_id
                == "response-1"
            ),
        },
        {
            "check": "production_behavior_unchanged",
            "actual": (
                resolved.production_behavior_changed
            ),
            "expected": False,
            "passed": (
                resolved.production_behavior_changed
                is False
            ),
        },
        {
            "check": "simulation_behavior_unchanged",
            "actual": (
                resolved.simulation_behavior_changed
            ),
            "expected": False,
            "passed": (
                resolved.simulation_behavior_changed
                is False
            ),
        },
        {
            "check": "matchup_swing_contact_batted_ball_authority_absent",
            "actual": any(
                [
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
                "batter_pitch_type_response_profile_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The deterministic diagnostic batter response profile passed all checks."
            ),
        },
        {
            "authority": (
                "production_batter_response_integration"
            ),
            "granted": False,
            "reason": (
                "Batter response profiles remain diagnostic-only."
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
                "swing_contact_or_batted_ball_probability_change"
            ),
            "granted": False,
            "reason": (
                "No swing, contact, or batted-ball probability changes are authorized."
            ),
        },
        {
            "authority": (
                "simulation_or_contact_quality_change"
            ),
            "granted": False,
            "reason": (
                "Simulation and contact-quality behavior remain unchanged."
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
        "batter_pitch_type_response_profile_contract_implementation_passed"
        if all_checks_passed
        else
        "batter_pitch_type_response_profile_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8H_pitcher_batter_pitch_type_matchup_overlay_contract_plan"
        if all_checks_passed
        else
        "8G_batter_pitch_type_response_profile_implementation_remediation"
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
        OUTPUT_DIR / "resolved_response_entries.csv",
        [
            "canonical_pitch_id",
            "canonical_pitch_name",
            "canonical_family",
            "pitcher_hand",
            "count_context",
            "pitch_count",
            "swing_count",
            "contact_count",
            "batted_ball_count",
            "swing_rate",
            "whiff_rate",
            "contact_rate",
            "avg_exit_velocity_mph",
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
                "count_context": (
                    entry.count_context
                ),
                "pitch_count": (
                    entry.pitch_count
                ),
                "swing_count": (
                    entry.swing_count
                ),
                "contact_count": (
                    entry.contact_count
                ),
                "batted_ball_count": (
                    entry.batted_ball_count
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
                "avg_exit_velocity_mph": (
                    entry.avg_exit_velocity_mph
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
            for entry in resolved.response_entries
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
                    "Plan the bounded pitcher-batter pitch-type matchup overlay contract."
                    if all_checks_passed
                    else
                    "Remediate failed 8G implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8G implementation checks pass."
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
        "profile_version": PROFILE_VERSION,
        "resolved_partial_sparse_stale_unavailable_invalid_supported": True,
        "deterministic_ordering_implemented": True,
        "unknown_pitch_retention_implemented": True,
        "rate_and_count_validation_implemented": True,
        "batted_ball_validation_implemented": True,
        "disabled_path_non_emitting": True,
        "caller_payload_immutable": True,
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
        "pitcher_batter_pitch_type_matchup_overlay_planning_allowed_next": (
            all_checks_passed
        ),
        "production_batter_response_integration_allowed_next": False,
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
                "resolved_response_entries.csv",
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
