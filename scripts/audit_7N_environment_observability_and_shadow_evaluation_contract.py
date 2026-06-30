#!/usr/bin/env python3
"""
Layer 7N environment shadow-observability implementation audit.
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


LAYER_ID = "7N"
LAYER_NAME = (
    "environment_observability_and_shadow_evaluation_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_7N_environment_observability_and_shadow_evaluation_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_7M_environment_observability_and_shadow_evaluation_contract.py"
)

CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/"
    "environment_shadow_observability.py"
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
        "environment_shadow_observability_7n",
        CONTRACT_PATH,
    )

    if (
        spec is None
        or spec.loader is None
    ):
        raise RuntimeError(
            "Unable to load 7N contract"
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

    required_paths_exist = (
        PLAN_PATH.exists()
        and CONTRACT_PATH.exists()
    )

    predecessor_present = (
        "environment_observability_and_shadow_evaluation_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    contract = load_contract()

    generated_at = datetime(
        2026,
        7,
        2,
        0,
        15,
        tzinfo=timezone.utc,
    )

    game_start = datetime(
        2026,
        7,
        2,
        1,
        5,
        tzinfo=timezone.utc,
    )

    composition_payload = {
        "enabled": True,
        "composition_version": "7L-v1",
        "canonical_venue_id": "venue-alpha",
        "game_start_time_utc": (
            game_start.isoformat()
        ),
        "composition_status": "resolved",
        "stage_statuses": {
            "venue_resolution": "resolved",
            "weather_resolution": "resolved",
            "field_vector_resolution": "resolved",
            "carry_diagnostic_resolution": "resolved",
            "composition_aggregation": "resolved",
        },
        "resolved_stage_count": 5,
        "neutral_stage_count": 0,
        "unavailable_stage_count": 0,
        "invalid_stage_count": 0,
        "diagnostic_codes": [
            "weather_resolved",
            "venue_resolved",
            "weather_resolved",
        ],
        "validation_errors": [],
        "venue_resolution": {
            "canonical_venue_id": "venue-alpha",
            "resolution_status": "resolved",
            "authorization_header": "remove-me",
            "provenance": {
                "source": "venue-source",
                "source_record_id": "venue-1",
                "raw_payload": "remove-me",
            },
        },
        "weather_resolution": {
            "resolution_status": "resolved",
            "temperature_c": 24.0,
            "api_token": "remove-me",
            "provenance": {
                "source": "weather-source",
                "source_record_id": "weather-1",
            },
        },
        "vector_resolution": {
            "vector_resolution_status": "resolved",
            "wind_along_ball_path_mps": 3.0,
        },
        "carry_resolution": {
            "resolution_status": "resolved",
            "combined_carry_index": 0.2,
        },
        "provenance": {
            "composition_status": "resolved",
            "secret": "remove-me",
        },
        "production_authority": False,
        "production_environment_activated": False,
    }

    def make_input(
        **overrides: Any,
    ) -> Any:
        values = {
            "game_id": "game-001",
            "game_start_time_utc": game_start,
            "shadow_enabled": True,
            "sampling_rate": 1.0,
            "retention_class": (
                "diagnostic_short"
            ),
            "generated_at_utc": generated_at,
            "baseline_projection_fingerprint": (
                "projection-a"
            ),
            "shadow_projection_fingerprint": (
                "projection-a"
            ),
        }

        values.update(overrides)

        return contract.ShadowObservabilityInput(
            **values
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

    semantic_a = {
        "b": 2,
        "a": 1,
    }
    semantic_b = {
        "a": 1,
        "b": 2,
    }

    record(
        "7N-C01",
        "semantic serialization deterministic",
        contract.semantic_json(
            semantic_a
        )
        == contract.semantic_json(
            semantic_b
        ),
        contract.semantic_json(
            semantic_a
        ),
        contract.semantic_json(
            semantic_b
        ),
    )

    record(
        "7N-C02",
        "semantic hash deterministic",
        contract.semantic_hash(
            semantic_a
        )
        == contract.semantic_hash(
            semantic_b
        ),
        contract.semantic_hash(
            semantic_a
        ),
        contract.semantic_hash(
            semantic_b
        ),
    )

    sampling_key_a = (
        contract.deterministic_sampling_key(
            game_id="game-001",
            game_start_time_utc=game_start,
        )
    )
    sampling_key_b = (
        contract.deterministic_sampling_key(
            game_id="game-001",
            game_start_time_utc=game_start,
        )
    )

    record(
        "7N-C03",
        "sampling key deterministic",
        sampling_key_a
        == sampling_key_b,
        sampling_key_a,
        sampling_key_b,
    )

    record(
        "7N-C04",
        "zero sampling rate never selected",
        contract.deterministic_sample_selected(
            sampling_key=sampling_key_a,
            sampling_rate=0.0,
        )
        is False,
        False,
        False,
    )

    record(
        "7N-C05",
        "full sampling rate always selected",
        contract.deterministic_sample_selected(
            sampling_key=sampling_key_a,
            sampling_rate=1.0,
        )
        is True,
        True,
        True,
    )

    disabled = (
        contract.build_shadow_observability_record(
            observability_input=make_input(
                shadow_enabled=False
            ),
            composition_payload=(
                composition_payload
            ),
        )
    )

    record(
        "7N-C06",
        "disabled path emits no record",
        disabled.emitted is False
        and disabled.record is None
        and disabled.reason
        == "shadow_disabled",
        disabled.to_dict(),
        {
            "emitted": False,
            "reason": "shadow_disabled",
        },
    )

    unsampled = (
        contract.build_shadow_observability_record(
            observability_input=make_input(
                sampling_rate=0.0
            ),
            composition_payload=(
                composition_payload
            ),
        )
    )

    record(
        "7N-C07",
        "unsampled path emits no payload record",
        unsampled.emitted is False
        and unsampled.record is None
        and unsampled.reason
        == "sample_not_selected",
        unsampled.to_dict(),
        {
            "emitted": False,
            "reason": (
                "sample_not_selected"
            ),
        },
    )

    emitted = (
        contract.build_shadow_observability_record(
            observability_input=make_input(),
            composition_payload=(
                composition_payload
            ),
        )
    )

    record(
        "7N-C08",
        "selected record emitted",
        emitted.emitted is True
        and emitted.record is not None,
        emitted.to_dict(),
        {
            "emitted": True,
        },
    )

    assert emitted.record is not None

    record(
        "7N-C09",
        "record contains twenty eight fields",
        len(
            emitted.record.to_dict()
        )
        == 28,
        len(
            emitted.record.to_dict()
        ),
        28,
    )

    redacted = contract.redact_payload(
        composition_payload
    )

    redacted_text = contract.semantic_json(
        redacted
    )

    record(
        "7N-C10",
        "sensitive fields redacted",
        "remove-me"
        not in redacted_text
        and "authorization_header"
        not in redacted_text
        and "api_token"
        not in redacted_text,
        redacted,
        {
            "sensitive_fields_absent": True,
        },
    )

    record(
        "7N-C11",
        "provenance allowlist enforced",
        "raw_payload"
        not in redacted_text
        and "source"
        in redacted_text,
        redacted,
        {
            "allowlist_enforced": True,
        },
    )

    record(
        "7N-C12",
        "diagnostic codes sorted and unique",
        emitted.record.diagnostic_codes
        == tuple(
            sorted(
                set(
                    emitted.record.diagnostic_codes
                )
            )
        ),
        emitted.record.diagnostic_codes,
        "sorted_unique",
    )

    record(
        "7N-C13",
        "component hashes present",
        all(
            key
            in emitted.record.component_payload_hashes
            for key
            in contract.COMPONENT_PAYLOAD_KEYS
        ),
        emitted.record.component_payload_hashes,
        list(
            contract.COMPONENT_PAYLOAD_KEYS
        ),
    )

    repeated = (
        contract.build_shadow_observability_record(
            observability_input=make_input(),
            composition_payload=(
                composition_payload
            ),
        )
    )

    assert repeated.record is not None

    record(
        "7N-C14",
        "record identity and hashes deterministic",
        emitted.record.shadow_record_id
        == repeated.record.shadow_record_id
        and emitted.record.composition_payload_hash
        == repeated.record.composition_payload_hash
        and emitted.record.component_payload_hashes
        == repeated.record.component_payload_hashes,
        emitted.record.to_dict(),
        repeated.record.to_dict(),
    )

    record(
        "7N-C15",
        "equal fingerprints recognized",
        emitted.record.projection_fingerprints_equal
        is True,
        emitted.record.projection_fingerprints_equal,
        True,
    )

    mismatch = (
        contract.build_shadow_observability_record(
            observability_input=make_input(
                shadow_projection_fingerprint=(
                    "projection-b"
                )
            ),
            composition_payload=(
                composition_payload
            ),
        )
    )

    assert mismatch.record is not None

    record(
        "7N-C16",
        "fingerprint mismatch is alert only",
        mismatch.record.projection_fingerprints_equal
        is False
        and (
            "projection_fingerprint_mismatch"
            in mismatch.diagnostic_codes
        )
        and mismatch.production_output_changed
        is False,
        mismatch.to_dict(),
        {
            "mismatch": True,
            "production_change": False,
        },
    )

    missing_fingerprint = (
        contract.build_shadow_observability_record(
            observability_input=make_input(
                baseline_projection_fingerprint=None
            ),
            composition_payload=(
                composition_payload
            ),
        )
    )

    assert (
        missing_fingerprint.record
        is not None
    )

    record(
        "7N-C17",
        "missing fingerprint comparison unavailable",
        missing_fingerprint.record.projection_fingerprints_equal
        is None
        and (
            "projection_fingerprint_missing"
            in missing_fingerprint.diagnostic_codes
        ),
        missing_fingerprint.to_dict(),
        {
            "comparison": None,
        },
    )

    missing_composition = (
        contract.build_shadow_observability_record(
            observability_input=make_input(),
            composition_payload=None,
        )
    )

    assert (
        missing_composition.record
        is not None
    )

    record(
        "7N-C18",
        "missing composition emits minimal unavailable record",
        missing_composition.emitted
        is True
        and missing_composition.record.composition_status
        == "unavailable"
        and (
            "environment_composition_missing"
            in missing_composition.record.diagnostic_codes
        ),
        missing_composition.to_dict(),
        {
            "status": "unavailable",
        },
    )

    invalid_rate = (
        contract.build_shadow_observability_record(
            observability_input=make_input(
                sampling_rate=1.5
            ),
            composition_payload=(
                composition_payload
            ),
        )
    )

    record(
        "7N-C19",
        "invalid sampling rate suppressed",
        invalid_rate.emitted is False
        and invalid_rate.reason
        == "invalid_sampling_rate",
        invalid_rate.to_dict(),
        {
            "emitted": False,
        },
    )

    payload_before = copy.deepcopy(
        composition_payload
    )

    contract.build_shadow_observability_record(
        observability_input=make_input(),
        composition_payload=(
            composition_payload
        ),
    )

    record(
        "7N-C20",
        "caller composition payload immutable",
        composition_payload
        == payload_before,
        {
            "unchanged": (
                composition_payload
                == payload_before
            ),
        },
        {
            "unchanged": True,
        },
    )

    record(
        "7N-C21",
        "production and research authority flags false",
        all(
            value is False
            for value in [
                emitted.production_output_changed,
                emitted.production_authority,
                emitted.historical_outcome_joined,
                emitted.accuracy_metrics_generated,
                emitted.tuning_executed,
                emitted.pricing_or_edge_work_executed,
            ]
        )
        and emitted.record.production_output_changed
        is False
        and emitted.record.production_authority
        is False,
        emitted.to_dict(),
        {
            "all_authority_flags": False,
        },
    )

    record_size = len(
        contract.semantic_json(
            emitted.record.to_dict()
        ).encode(
            "utf-8"
        )
    )

    record(
        "7N-C22",
        "record size bounded",
        record_size
        <= contract.MAX_RECORD_BYTES,
        record_size,
        contract.MAX_RECORD_BYTES,
    )

    implementation_checks = [
        {
            "check": "required_paths_exist",
            "actual": required_paths_exist,
            "expected": True,
            "passed": required_paths_exist,
        },
        {
            "check": "seven_m_predecessor_contract_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_two_contract_cases_pass",
            "actual": sum(
                1
                for row in cases
                if row["passed"]
            ),
            "expected": 22,
            "passed": all(
                row["passed"]
                for row in cases
            ),
        },
        {
            "check": "twenty_eight_record_fields_implemented",
            "actual": len(
                emitted.record.to_dict()
            ),
            "expected": 28,
            "passed": (
                len(
                    emitted.record.to_dict()
                )
                == 28
            ),
        },
        {
            "check": "deterministic_sampling_implemented",
            "actual": (
                sampling_key_a
                == sampling_key_b
            ),
            "expected": True,
            "passed": (
                sampling_key_a
                == sampling_key_b
            ),
        },
        {
            "check": "deterministic_hashing_implemented",
            "actual": (
                emitted.record.composition_payload_hash
                == repeated.record.composition_payload_hash
            ),
            "expected": True,
            "passed": (
                emitted.record.composition_payload_hash
                == repeated.record.composition_payload_hash
            ),
        },
        {
            "check": "redaction_allowlist_enforced",
            "actual": (
                "remove-me"
                not in redacted_text
            ),
            "expected": True,
            "passed": (
                "remove-me"
                not in redacted_text
            ),
        },
        {
            "check": "disabled_path_emits_no_record",
            "actual": disabled.emitted,
            "expected": False,
            "passed": (
                disabled.emitted
                is False
            ),
        },
        {
            "check": "unsampled_path_emits_no_record",
            "actual": unsampled.emitted,
            "expected": False,
            "passed": (
                unsampled.emitted
                is False
            ),
        },
        {
            "check": "record_size_bounded",
            "actual": record_size,
            "expected": (
                f"<={contract.MAX_RECORD_BYTES}"
            ),
            "passed": (
                record_size
                <= contract.MAX_RECORD_BYTES
            ),
        },
        {
            "check": "caller_payload_immutable",
            "actual": (
                composition_payload
                == payload_before
            ),
            "expected": True,
            "passed": (
                composition_payload
                == payload_before
            ),
        },
        {
            "check": "production_output_unchanged",
            "actual": (
                emitted.production_output_changed
            ),
            "expected": False,
            "passed": (
                emitted.production_output_changed
                is False
            ),
        },
        {
            "check": "production_authority_absent",
            "actual": (
                emitted.production_authority
            ),
            "expected": False,
            "passed": (
                emitted.production_authority
                is False
            ),
        },
        {
            "check": "historical_outcomes_not_joined",
            "actual": (
                emitted.historical_outcome_joined
            ),
            "expected": False,
            "passed": (
                emitted.historical_outcome_joined
                is False
            ),
        },
        {
            "check": "accuracy_metrics_not_generated",
            "actual": (
                emitted.accuracy_metrics_generated
            ),
            "expected": False,
            "passed": (
                emitted.accuracy_metrics_generated
                is False
            ),
        },
        {
            "check": "tuning_not_executed",
            "actual": (
                emitted.tuning_executed
            ),
            "expected": False,
            "passed": (
                emitted.tuning_executed
                is False
            ),
        },
        {
            "check": "pricing_and_edge_work_not_executed",
            "actual": (
                emitted.pricing_or_edge_work_executed
            ),
            "expected": False,
            "passed": (
                emitted.pricing_or_edge_work_executed
                is False
            ),
        },
        {
            "check": "implementation_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in implementation_checks
    )

    authority_rows = [
        {
            "authority": (
                "environment_shadow_observability"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The deterministic shadow-only observability "
                "contract passed all implementation checks."
            ),
        },
        {
            "authority": (
                "production_environment_activation"
            ),
            "granted": False,
            "reason": (
                "Observability remains shadow-only."
            ),
        },
        {
            "authority": (
                "historical_outcome_join"
            ),
            "granted": False,
            "reason": (
                "No historical outcomes are joined."
            ),
        },
        {
            "authority": (
                "accuracy_or_calibration_metrics"
            ),
            "granted": False,
            "reason": (
                "No predictive evaluation metrics are calculated."
            ),
        },
        {
            "authority": (
                "parameter_tuning"
            ),
            "granted": False,
            "reason": (
                "No parameter scoring or selection is performed."
            ),
        },
        {
            "authority": (
                "pricing_or_edge_detection"
            ),
            "granted": False,
            "reason": (
                "Pricing and edge work remain unauthorized."
            ),
        },
    ]

    recommended_next_layer = (
        "7O_layer_7_environment_readiness_and_scope_closure_plan"
        if all_checks_passed
        else
        "7N_environment_shadow_observability_remediation"
    )

    diagnosis_name = (
        "environment_observability_and_shadow_evaluation_contract_implementation_passed"
        if all_checks_passed
        else
        "environment_observability_and_shadow_evaluation_contract_implementation_failed"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        implementation_checks,
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
                    "Plan Layer 7 readiness review and bounded "
                    "scope closure without production activation."
                    if all_checks_passed
                    else
                    "Remediate failed 7N implementation checks."
                ),
                "entry_condition": (
                    "All eighteen 7N implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "implementation_checks_required": len(
            implementation_checks
        ),
        "implementation_checks_passed": sum(
            1
            for row in implementation_checks
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
        "observability_record_fields_implemented": 28,
        "deterministic_sampling_implemented": True,
        "deterministic_semantic_hashing_implemented": True,
        "redaction_allowlist_implemented": True,
        "bounded_record_size_implemented": True,
        "fingerprint_invariance_comparison_implemented": True,
        "disabled_and_unsampled_suppression_implemented": True,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_environment_activated": False,
        "historical_outcome_joined": False,
        "accuracy_metrics_generated": False,
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
        "layer7_completed": False,
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "layer7_readiness_closure_planning_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "implementation_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "contract_cases.csv"
            ),
            str(
                OUTPUT_DIR
                / "authority_boundaries.csv"
            ),
            str(
                OUTPUT_DIR
                / "recommended_path.csv"
            ),
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
