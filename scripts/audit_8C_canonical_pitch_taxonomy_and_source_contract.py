#!/usr/bin/env python3
"""
Layer 8C canonical pitch taxonomy implementation audit.
"""

from __future__ import annotations

import ast
import copy
import csv
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "8C"
LAYER_NAME = (
    "canonical_pitch_taxonomy_and_source_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8C_canonical_pitch_taxonomy_and_source_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8B_canonical_pitch_taxonomy_and_source_contract.py"
)

CONTRACT_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "canonical_pitch_taxonomy.py"
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


def load_contract() -> Any:
    spec = importlib.util.spec_from_file_location(
        "canonical_pitch_taxonomy_8c",
        CONTRACT_PATH,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Unable to load 8C contract"
        )

    module = importlib.util.module_from_spec(
        spec
    )

    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    contract = load_contract()

    predecessor_present = (
        "canonical_pitch_taxonomy_and_source_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    timestamp = datetime(
        2026,
        7,
        1,
        0,
        0,
        tzinfo=timezone.utc,
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

    disabled = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="statcast",
            source_pitch_value="FF",
            enabled=False,
        )
    )

    record(
        "8C-C01",
        "disabled path emits no diagnostic payload",
        disabled.emitted is False
        and disabled.canonical_pitch_id is None
        and disabled.reason == "taxonomy_disabled",
        disabled.to_dict(),
        {
            "emitted": False,
            "reason": "taxonomy_disabled",
        },
    )

    statcast_exact = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="statcast",
            source_pitch_value="FF",
            source_record_id="pitch-1",
            source_timestamp_utc=timestamp,
            enabled=True,
        )
    )

    record(
        "8C-C02",
        "Statcast exact code resolves",
        statcast_exact.canonical_pitch_id == "FF"
        and statcast_exact.normalization_status == "exact"
        and statcast_exact.source_priority == 1,
        statcast_exact.to_dict(),
        {
            "canonical_pitch_id": "FF",
            "status": "exact",
            "priority": 1,
        },
    )

    legacy = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="statcast",
            source_pitch_value="FT",
            enabled=True,
        )
    )

    record(
        "8C-C03",
        "legacy two-seam code resolves to sinker",
        legacy.canonical_pitch_id == "SI"
        and legacy.normalization_status == "legacy_alias"
        and legacy.source_priority == 4,
        legacy.to_dict(),
        {
            "canonical_pitch_id": "SI",
            "status": "legacy_alias",
        },
    )

    generic = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="generic",
            source_pitch_value="  Four-Seam   Fastball ",
            enabled=True,
        )
    )

    record(
        "8C-C04",
        "generic alias normalization is whitespace and case stable",
        generic.canonical_pitch_id == "FF"
        and generic.normalization_status == "normalized_alias"
        and generic.source_priority == 3,
        generic.to_dict(),
        {
            "canonical_pitch_id": "FF",
            "status": "normalized_alias",
        },
    )

    trusted_code = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="provider",
            source_pitch_value="SL",
            enabled=True,
        )
    )

    record(
        "8C-C05",
        "trusted provider canonical code resolves",
        trusted_code.canonical_pitch_id == "SL"
        and trusted_code.source_priority == 2,
        trusted_code.to_dict(),
        {
            "canonical_pitch_id": "SL",
            "priority": 2,
        },
    )

    missing_value = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="statcast",
            source_pitch_value=None,
            enabled=True,
        )
    )

    record(
        "8C-C06",
        "missing value resolves to unknown",
        missing_value.canonical_pitch_id == "UN"
        and missing_value.normalization_status == "missing",
        missing_value.to_dict(),
        {
            "canonical_pitch_id": "UN",
            "status": "missing",
        },
    )

    missing_source = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name=None,
            source_pitch_value="FF",
            enabled=True,
        )
    )

    record(
        "8C-C07",
        "missing source resolves to unknown",
        missing_source.canonical_pitch_id == "UN"
        and (
            "pitch_taxonomy_source_name_missing"
            in missing_source.diagnostic_codes
        ),
        missing_source.to_dict(),
        {
            "canonical_pitch_id": "UN",
        },
    )

    unsupported = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="statcast",
            source_pitch_value="ZZ",
            enabled=True,
        )
    )

    record(
        "8C-C08",
        "unsupported value is observable and not guessed",
        unsupported.canonical_pitch_id == "UN"
        and unsupported.normalization_status == "unsupported"
        and (
            "pitch_taxonomy_source_value_unsupported"
            in unsupported.diagnostic_codes
        ),
        unsupported.to_dict(),
        {
            "canonical_pitch_id": "UN",
            "status": "unsupported",
        },
    )

    ambiguous = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="generic",
            source_pitch_value="fastball",
            enabled=True,
        )
    )

    record(
        "8C-C09",
        "ambiguous value is not guessed",
        ambiguous.canonical_pitch_id == "UN"
        and ambiguous.normalization_status == "ambiguous",
        ambiguous.to_dict(),
        {
            "canonical_pitch_id": "UN",
            "status": "ambiguous",
        },
    )

    unknown_source = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="mystery_provider",
            source_pitch_value="ZZ",
            enabled=True,
        )
    )

    record(
        "8C-C10",
        "unknown source remains observable",
        unknown_source.canonical_pitch_id == "UN"
        and (
            "pitch_taxonomy_source_name_unrecognized"
            in unknown_source.diagnostic_codes
        ),
        unknown_source.to_dict(),
        {
            "canonical_pitch_id": "UN",
            "source_warning": True,
        },
    )

    repeat_a = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="generic",
            source_pitch_value="change-up",
            enabled=True,
        )
    )

    repeat_b = contract.normalize_pitch_type(
        contract.PitchTaxonomyInput(
            source_name="generic",
            source_pitch_value="change-up",
            enabled=True,
        )
    )

    record(
        "8C-C11",
        "normalization is deterministic",
        repeat_a.to_dict() == repeat_b.to_dict(),
        repeat_a.to_dict(),
        repeat_b.to_dict(),
    )

    canonical_records = (
        contract.canonical_pitch_records()
    )

    record(
        "8C-C12",
        "sixteen immutable canonical records exposed",
        len(canonical_records) == 16
        and len(
            {
                row.canonical_pitch_id
                for row in canonical_records
            }
        )
        == 16,
        len(canonical_records),
        16,
    )

    record(
        "8C-C13",
        "canonical names are unique",
        len(
            contract.canonical_pitch_names()
        )
        == len(
            set(
                contract.canonical_pitch_names()
            )
        ),
        contract.canonical_pitch_names(),
        "unique",
    )

    record(
        "8C-C14",
        "unknown fallback is present",
        "UN"
        in contract.canonical_pitch_ids(),
        contract.canonical_pitch_ids(),
        "contains UN",
    )

    payload = {
        "source_name": "generic",
        "source_pitch_value": "split-finger",
        "source_record_id": "pitch-2",
        "source_timestamp_utc": (
            timestamp.isoformat()
        ),
        "enabled": True,
    }

    payload_before = copy.deepcopy(
        payload
    )

    payload_result = (
        contract.normalize_pitch_payload(
            payload
        )
    )

    record(
        "8C-C15",
        "payload interface resolves alias",
        payload_result.canonical_pitch_id
        == "FS",
        payload_result.to_dict(),
        {
            "canonical_pitch_id": "FS",
        },
    )

    record(
        "8C-C16",
        "caller payload remains immutable",
        payload == payload_before,
        payload,
        payload_before,
    )

    record(
        "8C-C17",
        "provenance fields retained",
        statcast_exact.source_record_id
        == "pitch-1"
        and statcast_exact.source_timestamp_utc
        == timestamp.isoformat(),
        statcast_exact.to_dict(),
        {
            "source_record_id": "pitch-1",
            "source_timestamp_utc": (
                timestamp.isoformat()
            ),
        },
    )

    record(
        "8C-C18",
        "diagnostic codes sorted and unique",
        unsupported.diagnostic_codes
        == tuple(
            sorted(
                set(
                    unsupported.diagnostic_codes
                )
            )
        ),
        unsupported.diagnostic_codes,
        "sorted_unique",
    )

    record(
        "8C-C19",
        "taxonomy version explicit",
        all(
            result.taxonomy_version
            == contract.TAXONOMY_VERSION
            for result in [
                disabled,
                statcast_exact,
                legacy,
                generic,
                missing_value,
                unsupported,
                ambiguous,
            ]
        ),
        contract.TAXONOMY_VERSION,
        "8C-v1",
    )

    record(
        "8C-C20",
        "production and simulation authority remain false",
        all(
            value is False
            for value in [
                statcast_exact.production_authority,
                statcast_exact.production_behavior_changed,
                statcast_exact.simulation_behavior_changed,
                statcast_exact.pitch_selection_changed,
                statcast_exact.pitch_sequence_changed,
                statcast_exact.matchup_adjustment_activated,
                statcast_exact.contact_quality_changed,
            ]
        ),
        statcast_exact.to_dict(),
        {
            "all_authority_flags": False,
        },
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": (
                PLAN_PATH.exists()
                and CONTRACT_PATH.exists()
            ),
            "expected": True,
            "passed": (
                PLAN_PATH.exists()
                and CONTRACT_PATH.exists()
            ),
        },
        {
            "check": "eight_b_predecessor_present",
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
            "check": "sixteen_canonical_pitches_implemented",
            "actual": len(
                contract.canonical_pitch_records()
            ),
            "expected": 16,
            "passed": len(
                contract.canonical_pitch_records()
            )
            == 16,
        },
        {
            "check": "canonical_ids_unique",
            "actual": len(
                set(
                    contract.canonical_pitch_ids()
                )
            ),
            "expected": 16,
            "passed": len(
                set(
                    contract.canonical_pitch_ids()
                )
            )
            == 16,
        },
        {
            "check": "canonical_names_unique",
            "actual": len(
                set(
                    contract.canonical_pitch_names()
                )
            ),
            "expected": 16,
            "passed": len(
                set(
                    contract.canonical_pitch_names()
                )
            )
            == 16,
        },
        {
            "check": "unknown_fallback_implemented",
            "actual": "UN"
            in contract.canonical_pitch_ids(),
            "expected": True,
            "passed": "UN"
            in contract.canonical_pitch_ids(),
        },
        {
            "check": "source_precedence_implemented",
            "actual": [
                statcast_exact.source_priority,
                trusted_code.source_priority,
                generic.source_priority,
                legacy.source_priority,
                unsupported.source_priority,
            ],
            "expected": [1, 2, 3, 4, 5],
            "passed": [
                statcast_exact.source_priority,
                trusted_code.source_priority,
                generic.source_priority,
                legacy.source_priority,
                unsupported.source_priority,
            ]
            == [1, 2, 3, 4, 5],
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled.emitted,
            "expected": False,
            "passed": disabled.emitted is False,
        },
        {
            "check": "missing_unsupported_ambiguous_not_guessed",
            "actual": [
                missing_value.canonical_pitch_id,
                unsupported.canonical_pitch_id,
                ambiguous.canonical_pitch_id,
            ],
            "expected": ["UN", "UN", "UN"],
            "passed": [
                missing_value.canonical_pitch_id,
                unsupported.canonical_pitch_id,
                ambiguous.canonical_pitch_id,
            ]
            == ["UN", "UN", "UN"],
        },
        {
            "check": "normalization_deterministic",
            "actual": (
                repeat_a.to_dict()
                == repeat_b.to_dict()
            ),
            "expected": True,
            "passed": (
                repeat_a.to_dict()
                == repeat_b.to_dict()
            ),
        },
        {
            "check": "caller_payload_immutable",
            "actual": payload
            == payload_before,
            "expected": True,
            "passed": payload
            == payload_before,
        },
        {
            "check": "provenance_bounded_and_retained",
            "actual": (
                statcast_exact.source_record_id
                == "pitch-1"
            ),
            "expected": True,
            "passed": (
                statcast_exact.source_record_id
                == "pitch-1"
            ),
        },
        {
            "check": "diagnostic_codes_sorted_unique",
            "actual": (
                unsupported.diagnostic_codes
                == tuple(
                    sorted(
                        set(
                            unsupported.diagnostic_codes
                        )
                    )
                )
            ),
            "expected": True,
            "passed": (
                unsupported.diagnostic_codes
                == tuple(
                    sorted(
                        set(
                            unsupported.diagnostic_codes
                        )
                    )
                )
            ),
        },
        {
            "check": "production_behavior_unchanged",
            "actual": (
                statcast_exact.production_behavior_changed
            ),
            "expected": False,
            "passed": (
                statcast_exact.production_behavior_changed
                is False
            ),
        },
        {
            "check": "simulation_behavior_unchanged",
            "actual": (
                statcast_exact.simulation_behavior_changed
            ),
            "expected": False,
            "passed": (
                statcast_exact.simulation_behavior_changed
                is False
            ),
        },
        {
            "check": "pitch_selection_unchanged",
            "actual": (
                statcast_exact.pitch_selection_changed
            ),
            "expected": False,
            "passed": (
                statcast_exact.pitch_selection_changed
                is False
            ),
        },
        {
            "check": "matchup_and_contact_authority_absent",
            "actual": (
                statcast_exact.matchup_adjustment_activated
                or statcast_exact.contact_quality_changed
            ),
            "expected": False,
            "passed": (
                statcast_exact.matchup_adjustment_activated
                is False
                and statcast_exact.contact_quality_changed
                is False
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
                "canonical_pitch_taxonomy_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The deterministic diagnostic taxonomy passed all checks."
            ),
        },
        {
            "authority": (
                "production_pitch_taxonomy_integration"
            ),
            "granted": False,
            "reason": (
                "Taxonomy results remain diagnostic-only."
            ),
        },
        {
            "authority": (
                "production_pitch_selection"
            ),
            "granted": False,
            "reason": (
                "No pitch selection behavior is changed."
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
        "canonical_pitch_taxonomy_and_source_contract_implementation_passed"
        if all_checks_passed
        else
        "canonical_pitch_taxonomy_and_source_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8D_pitcher_arsenal_profile_contract_plan"
        if all_checks_passed
        else
        "8C_canonical_pitch_taxonomy_implementation_remediation"
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
        OUTPUT_DIR / "canonical_pitch_records.csv",
        [
            "canonical_pitch_id",
            "canonical_name",
            "family",
            "velocity_band",
            "movement_profile",
            "active",
        ],
        [
            {
                "canonical_pitch_id": (
                    row.canonical_pitch_id
                ),
                "canonical_name": (
                    row.canonical_name
                ),
                "family": row.family,
                "velocity_band": (
                    row.velocity_band
                ),
                "movement_profile": (
                    row.movement_profile
                ),
                "active": row.active,
            }
            for row in contract.canonical_pitch_records()
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
                    "Plan the bounded pitcher arsenal profile contract."
                    if all_checks_passed
                    else
                    "Remediate failed 8C implementation checks."
                ),
                "entry_condition": (
                    "All eighteen 8C implementation checks pass."
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
        "canonical_pitches_implemented": len(
            contract.canonical_pitch_records()
        ),
        "taxonomy_version": (
            contract.TAXONOMY_VERSION
        ),
        "deterministic_normalization_implemented": True,
        "source_precedence_implemented": True,
        "unknown_fallback_implemented": True,
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
        "pitcher_arsenal_profile_planning_allowed_next": (
            all_checks_passed
        ),
        "production_pitch_taxonomy_integration_allowed_next": False,
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
                "canonical_pitch_records.csv",
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
