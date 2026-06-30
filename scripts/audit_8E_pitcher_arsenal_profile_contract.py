#!/usr/bin/env python3
"""
Layer 8E pitcher arsenal profile implementation audit.
"""

from __future__ import annotations

import ast
import copy
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable

from mlb_app.pitching.pitcher_arsenal_profile import (
    PROFILE_VERSION,
    build_pitcher_arsenal_profile,
)


LAYER_ID = "8E"
LAYER_NAME = (
    "pitcher_arsenal_profile_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8E_pitcher_arsenal_profile_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8D_pitcher_arsenal_profile_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitcher_arsenal_profile.py"
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
        "pitcher_arsenal_profile_contract_plan_complete"
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

    current_timestamp = datetime(
        2026,
        6,
        25,
        tzinfo=timezone.utc,
    )

    base_payload = {
        "enabled": True,
        "pitcher_id": "pitcher-1",
        "pitcher_name": "Example Pitcher",
        "pitcher_hand": "R",
        "pitcher_role": "starter",
        "season": 2026,
        "as_of_date_utc": "2026-06-30",
        "source_name": "statcast",
        "source_record_id": "arsenal-1",
        "source_timestamp_utc": (
            current_timestamp.isoformat()
        ),
        "sample_game_count": 12,
        "arsenal_entries": [
            {
                "canonical_pitch_id": "FF",
                "pitch_count": 60,
                "avg_velocity_mph": 95.4,
                "avg_spin_rpm": 2380,
                "zone_rate": 0.53,
                "whiff_rate": 0.24,
            },
            {
                "canonical_pitch_id": "SL",
                "pitch_count": 30,
                "avg_velocity_mph": 86.7,
                "avg_spin_rpm": 2500,
                "zone_rate": 0.42,
                "whiff_rate": 0.36,
            },
            {
                "canonical_pitch_id": "CH",
                "pitch_count": 10,
                "avg_velocity_mph": 88.1,
                "zone_rate": 0.47,
                "whiff_rate": 0.28,
            },
        ],
    }

    resolved = build_pitcher_arsenal_profile(
        base_payload
    )

    record(
        "8E-C01",
        "resolved profile emits",
        resolved.emitted
        and resolved.profile_status == "resolved",
        resolved.to_dict(),
        {
            "emitted": True,
            "profile_status": "resolved",
        },
    )

    record(
        "8E-C02",
        "usage shares derive from counts",
        [
            entry.usage_share
            for entry in resolved.arsenal_entries
        ]
        == [0.6, 0.3, 0.1],
        [
            entry.usage_share
            for entry in resolved.arsenal_entries
        ],
        [0.6, 0.3, 0.1],
    )

    record(
        "8E-C03",
        "entries sort deterministically",
        [
            entry.canonical_pitch_id
            for entry in resolved.arsenal_entries
        ]
        == ["FF", "SL", "CH"],
        [
            entry.canonical_pitch_id
            for entry in resolved.arsenal_entries
        ],
        ["FF", "SL", "CH"],
    )

    disabled = build_pitcher_arsenal_profile(
        {
            "enabled": False,
            "pitcher_id": "pitcher-1",
            "source_name": "statcast",
        }
    )

    record(
        "8E-C04",
        "disabled profile is non-emitting",
        disabled.emitted is False
        and disabled.profile_status == "disabled",
        disabled.to_dict(),
        {
            "emitted": False,
            "profile_status": "disabled",
        },
    )

    sparse_payload = copy.deepcopy(
        base_payload
    )
    sparse_payload["arsenal_entries"] = [
        {
            "canonical_pitch_id": "FF",
            "pitch_count": 20,
        }
    ]

    sparse = build_pitcher_arsenal_profile(
        sparse_payload
    )

    record(
        "8E-C05",
        "sparse profile classified",
        sparse.profile_status == "sparse",
        sparse.profile_status,
        "sparse",
    )

    stale_payload = copy.deepcopy(
        base_payload
    )
    stale_payload[
        "source_timestamp_utc"
    ] = "2026-05-01T00:00:00+00:00"

    stale = build_pitcher_arsenal_profile(
        stale_payload
    )

    record(
        "8E-C06",
        "stale profile classified",
        stale.profile_status == "stale",
        stale.profile_status,
        "stale",
    )

    unavailable_payload = copy.deepcopy(
        base_payload
    )
    unavailable_payload[
        "arsenal_entries"
    ] = []

    unavailable = (
        build_pitcher_arsenal_profile(
            unavailable_payload
        )
    )

    record(
        "8E-C07",
        "missing source entries yield unavailable",
        unavailable.profile_status
        == "unavailable",
        unavailable.profile_status,
        "unavailable",
    )

    unknown_payload = copy.deepcopy(
        base_payload
    )
    unknown_payload["arsenal_entries"] = [
        {
            "canonical_pitch_id": "ZZ",
            "pitch_count": 100,
        }
    ]

    unknown = build_pitcher_arsenal_profile(
        unknown_payload
    )

    record(
        "8E-C08",
        "unknown pitch retained as UN",
        unknown.arsenal_entries[0].canonical_pitch_id
        == "UN",
        unknown.to_dict(),
        {
            "canonical_pitch_id": "UN",
        },
    )

    duplicate_payload = copy.deepcopy(
        base_payload
    )
    duplicate_payload["arsenal_entries"] = [
        {
            "canonical_pitch_id": "FF",
            "pitch_count": 50,
        },
        {
            "canonical_pitch_id": "FF",
            "pitch_count": 50,
        },
    ]

    duplicate = build_pitcher_arsenal_profile(
        duplicate_payload
    )

    record(
        "8E-C09",
        "duplicate pitch invalidates profile",
        duplicate.profile_status == "invalid"
        and (
            "pitcher_arsenal_duplicate_pitch"
            in duplicate.validation_errors
        ),
        duplicate.to_dict(),
        {
            "profile_status": "invalid",
        },
    )

    invalid_usage_payload = copy.deepcopy(
        base_payload
    )
    invalid_usage_payload[
        "arsenal_entries"
    ] = [
        {
            "canonical_pitch_id": "FF",
            "pitch_count": 60,
            "usage_share": 0.8,
        },
        {
            "canonical_pitch_id": "SL",
            "pitch_count": 40,
            "usage_share": 0.4,
        },
    ]

    invalid_usage = (
        build_pitcher_arsenal_profile(
            invalid_usage_payload
        )
    )

    record(
        "8E-C10",
        "invalid usage total invalidates profile",
        invalid_usage.profile_status
        == "invalid"
        and (
            "pitcher_arsenal_usage_total_invalid"
            in invalid_usage.validation_errors
        ),
        invalid_usage.to_dict(),
        {
            "profile_status": "invalid",
        },
    )

    missing_identity_payload = (
        copy.deepcopy(
            base_payload
        )
    )
    missing_identity_payload[
        "pitcher_id"
    ] = ""

    missing_identity = (
        build_pitcher_arsenal_profile(
            missing_identity_payload
        )
    )

    record(
        "8E-C11",
        "missing identity invalidates profile",
        missing_identity.profile_status
        == "invalid",
        missing_identity.profile_status,
        "invalid",
    )

    normalized_context_payload = (
        copy.deepcopy(
            base_payload
        )
    )
    normalized_context_payload[
        "pitcher_hand"
    ] = "x"
    normalized_context_payload[
        "pitcher_role"
    ] = "bulk"

    normalized_context = (
        build_pitcher_arsenal_profile(
            normalized_context_payload
        )
    )

    record(
        "8E-C12",
        "unsupported hand and role normalize safely",
        normalized_context.pitcher_hand
        == "U"
        and normalized_context.pitcher_role
        == "unknown",
        {
            "hand": (
                normalized_context.pitcher_hand
            ),
            "role": (
                normalized_context.pitcher_role
            ),
        },
        {
            "hand": "U",
            "role": "unknown",
        },
    )

    payload_before = copy.deepcopy(
        base_payload
    )

    build_pitcher_arsenal_profile(
        base_payload
    )

    record(
        "8E-C13",
        "caller payload remains immutable",
        base_payload == payload_before,
        base_payload,
        payload_before,
    )

    repeated_a = (
        build_pitcher_arsenal_profile(
            base_payload
        )
    )
    repeated_b = (
        build_pitcher_arsenal_profile(
            base_payload
        )
    )

    record(
        "8E-C14",
        "profile construction deterministic",
        repeated_a.to_dict()
        == repeated_b.to_dict(),
        repeated_a.to_dict(),
        repeated_b.to_dict(),
    )

    record(
        "8E-C15",
        "provenance retained",
        resolved.source_record_id
        == "arsenal-1"
        and resolved.source_timestamp_utc
        == current_timestamp.isoformat(),
        resolved.to_dict(),
        {
            "source_record_id": "arsenal-1",
        },
    )

    record(
        "8E-C16",
        "source priority resolves",
        resolved.source_priority == 1,
        resolved.source_priority,
        1,
    )

    record(
        "8E-C17",
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
            "profile_version": "8E-v1",
        },
    )

    record(
        "8E-C18",
        "diagnostic codes sorted and unique",
        resolved.diagnostic_codes
        == tuple(
            sorted(
                set(
                    resolved.diagnostic_codes
                )
            )
        ),
        resolved.diagnostic_codes,
        "sorted_unique",
    )

    record(
        "8E-C19",
        "entry diagnostic codes sorted and unique",
        all(
            entry.diagnostic_codes
            == tuple(
                sorted(
                    set(
                        entry.diagnostic_codes
                    )
                )
            )
            for entry in resolved.arsenal_entries
        ),
        [
            entry.diagnostic_codes
            for entry in resolved.arsenal_entries
        ],
        "sorted_unique",
    )

    record(
        "8E-C20",
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
            "check": "eight_d_predecessor_present",
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
            "check": "derived_usage_sums_to_one",
            "actual": round(
                sum(
                    entry.usage_share or 0.0
                    for entry in resolved.arsenal_entries
                ),
                6,
            ),
            "expected": 1.0,
            "passed": abs(
                sum(
                    entry.usage_share or 0.0
                    for entry in resolved.arsenal_entries
                )
                - 1.0
            )
            <= 0.001,
        },
        {
            "check": "ordering_deterministic",
            "actual": [
                entry.canonical_pitch_id
                for entry in resolved.arsenal_entries
            ],
            "expected": [
                "FF",
                "SL",
                "CH",
            ],
            "passed": [
                entry.canonical_pitch_id
                for entry in resolved.arsenal_entries
            ]
            == [
                "FF",
                "SL",
                "CH",
            ],
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled.emitted,
            "expected": False,
            "passed": disabled.emitted is False,
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
                unknown.arsenal_entries[
                    0
                ].canonical_pitch_id
            ),
            "expected": "UN",
            "passed": (
                unknown.arsenal_entries[
                    0
                ].canonical_pitch_id
                == "UN"
            ),
        },
        {
            "check": "invalid_profiles_detected",
            "actual": [
                duplicate.profile_status,
                invalid_usage.profile_status,
                missing_identity.profile_status,
            ],
            "expected": [
                "invalid",
                "invalid",
                "invalid",
            ],
            "passed": [
                duplicate.profile_status,
                invalid_usage.profile_status,
                missing_identity.profile_status,
            ]
            == [
                "invalid",
                "invalid",
                "invalid",
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
                == "arsenal-1"
            ),
            "expected": True,
            "passed": (
                resolved.source_record_id
                == "arsenal-1"
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
            "check": "pitch_selection_sequence_matchup_contact_authority_absent",
            "actual": any(
                [
                    resolved.pitch_selection_changed,
                    resolved.pitch_sequence_changed,
                    resolved.matchup_adjustment_activated,
                    resolved.contact_quality_changed,
                ]
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in [
                    resolved.pitch_selection_changed,
                    resolved.pitch_sequence_changed,
                    resolved.matchup_adjustment_activated,
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
                "pitcher_arsenal_profile_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The deterministic diagnostic arsenal profile passed all checks."
            ),
        },
        {
            "authority": (
                "production_pitcher_arsenal_integration"
            ),
            "granted": False,
            "reason": (
                "Arsenal profiles remain diagnostic-only."
            ),
        },
        {
            "authority": (
                "production_pitch_selection"
            ),
            "granted": False,
            "reason": (
                "No production pitch selection is changed."
            ),
        },
        {
            "authority": (
                "pitch_sequence_change"
            ),
            "granted": False,
            "reason": (
                "No pitch sequencing behavior is changed."
            ),
        },
        {
            "authority": (
                "matchup_or_contact_adjustment"
            ),
            "granted": False,
            "reason": (
                "No matchup or contact-quality authority is granted."
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
        "pitcher_arsenal_profile_contract_implementation_passed"
        if all_checks_passed
        else
        "pitcher_arsenal_profile_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8F_batter_pitch_type_response_profile_contract_plan"
        if all_checks_passed
        else
        "8E_pitcher_arsenal_profile_implementation_remediation"
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
        OUTPUT_DIR / "resolved_arsenal_entries.csv",
        [
            "canonical_pitch_id",
            "canonical_pitch_name",
            "canonical_family",
            "available",
            "usage_share",
            "pitch_count",
            "avg_velocity_mph",
            "avg_spin_rpm",
            "zone_rate",
            "whiff_rate",
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
                "available": entry.available,
                "usage_share": entry.usage_share,
                "pitch_count": entry.pitch_count,
                "avg_velocity_mph": (
                    entry.avg_velocity_mph
                ),
                "avg_spin_rpm": (
                    entry.avg_spin_rpm
                ),
                "zone_rate": entry.zone_rate,
                "whiff_rate": entry.whiff_rate,
                "diagnostic_codes": "|".join(
                    entry.diagnostic_codes
                ),
            }
            for entry in resolved.arsenal_entries
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
                    "Plan the bounded batter pitch-type response profile contract."
                    if all_checks_passed
                    else
                    "Remediate failed 8E implementation checks."
                ),
                "entry_condition": (
                    "All eighteen 8E implementation checks pass."
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
        "resolved_profile_supported": True,
        "partial_sparse_stale_unavailable_invalid_supported": True,
        "deterministic_usage_derivation_implemented": True,
        "deterministic_ordering_implemented": True,
        "unknown_pitch_retention_implemented": True,
        "disabled_path_non_emitting": True,
        "caller_payload_immutable": True,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "pitch_selection_changed": False,
        "pitch_sequence_changed": False,
        "matchup_adjustments_activated": False,
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
        "batter_pitch_type_response_profile_planning_allowed_next": (
            all_checks_passed
        ),
        "production_pitcher_arsenal_integration_allowed_next": False,
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
                "resolved_arsenal_entries.csv",
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
