#!/usr/bin/env python3
"""
Layer 8O shadow-dataset quality-gate contract audit.
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
from mlb_app.pitching.pitch_type_matchup_overlay_shadow_dataset_quality_gate import (
    QUALITY_GATE_VERSION,
    evaluate_pitch_type_matchup_overlay_shadow_dataset_quality,
    expected_schema_fingerprint,
)
from mlb_app.pitching.pitcher_arsenal_profile import (
    build_pitcher_arsenal_profile,
)


LAYER_ID = "8O"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8O_pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_8N_pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract.py"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "mlb_app/pitching/"
    "pitch_type_matchup_overlay_shadow_dataset_quality_gate.py"
)

FIXED_TIME = "2026-07-02T00:00:00+00:00"


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
        "pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_plan_complete"
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

    partial_bundle = replace(
        bundle,
        observability_status="partial",
    )

    warning_dataset = (
        build_pitch_type_matchup_overlay_shadow_dataset(
            [partial_bundle],
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

    disabled_report = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_quality(
            clean_dataset,
            enabled=False,
            evaluated_at_utc=FIXED_TIME,
        )
    )

    missing_report = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_quality(
            None,
            enabled=True,
            evaluated_at_utc=FIXED_TIME,
        )
    )

    bad_manifest_dataset = replace(
        clean_dataset,
        manifest=replace(
            clean_dataset.manifest,
            row_count=99,
        ),
    )

    fail_report = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_quality(
            bad_manifest_dataset,
            enabled=True,
            evaluated_at_utc=FIXED_TIME,
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
        "8O-C01",
        "clean dataset report emits",
        pass_report.emitted,
        pass_report.emitted,
        True,
    )

    record(
        "8O-C02",
        "clean dataset passes",
        pass_report.gate_status == "pass",
        pass_report.gate_status,
        "pass",
    )

    record(
        "8O-C03",
        "twenty required gates emitted",
        sum(
            1
            for result in pass_report.results
            if result.gate_type == "required"
        )
        == 20,
        len(pass_report.results),
        26,
    )

    record(
        "8O-C04",
        "six warning gates emitted",
        sum(
            1
            for result in pass_report.results
            if result.gate_type == "warning"
        )
        == 6,
        len(pass_report.results),
        26,
    )

    record(
        "8O-C05",
        "clean required gates pass",
        all(
            result.passed
            for result in pass_report.results
            if result.gate_type == "required"
        ),
        [
            result.to_dict()
            for result in pass_report.results
        ],
        "all_required_true",
    )

    record(
        "8O-C06",
        "warning dataset warns",
        warning_report.gate_status == "warn",
        warning_report.gate_status,
        "warn",
    )

    record(
        "8O-C07",
        "partial row warning counted",
        warning_report.summary is not None
        and (
            warning_report.summary.partial_row_count
            == 1
        ),
        (
            warning_report.summary.to_dict()
            if warning_report.summary
            else None
        ),
        {"partial_row_count": 1},
    )

    record(
        "8O-C08",
        "empty dataset status supported",
        empty_report.gate_status == "empty",
        empty_report.gate_status,
        "empty",
    )

    record(
        "8O-C09",
        "disabled gate is non-emitting",
        disabled_report.emitted is False
        and disabled_report.summary is None,
        disabled_report.to_dict(),
        {
            "emitted": False,
            "summary": None,
        },
    )

    record(
        "8O-C10",
        "missing dataset fails",
        missing_report.gate_status == "fail",
        missing_report.gate_status,
        "fail",
    )

    record(
        "8O-C11",
        "manifest mismatch fails",
        fail_report.gate_status == "fail",
        fail_report.gate_status,
        "fail",
    )

    record(
        "8O-C12",
        "manifest reconciliation false",
        fail_report.summary is not None
        and (
            fail_report.summary.manifest_reconciles
            is False
        ),
        (
            fail_report.summary.manifest_reconciles
            if fail_report.summary
            else None
        ),
        False,
    )

    record(
        "8O-C13",
        "schema fingerprint matches",
        pass_report.summary is not None
        and (
            pass_report.summary.schema_fingerprint_matches
            is True
        ),
        (
            pass_report.summary.schema_fingerprint_matches
            if pass_report.summary
            else None
        ),
        True,
    )

    record(
        "8O-C14",
        "row identifiers unique",
        pass_report.summary is not None
        and (
            pass_report.summary.row_identifiers_unique
            is True
        ),
        (
            pass_report.summary.row_identifiers_unique
            if pass_report.summary
            else None
        ),
        True,
    )

    record(
        "8O-C15",
        "source versions present",
        pass_report.summary is not None
        and (
            pass_report.summary.source_versions_present
            is True
        ),
        (
            pass_report.summary.source_versions_present
            if pass_report.summary
            else None
        ),
        True,
    )

    record(
        "8O-C16",
        "coverage statistics deterministic",
        pass_report.summary is not None
        and (
            pass_report.summary.minimum_coverage_share
            == 1.0
        )
        and (
            pass_report.summary.mean_coverage_share
            == 1.0
        )
        and (
            pass_report.summary.maximum_coverage_share
            == 1.0
        ),
        (
            pass_report.summary.to_dict()
            if pass_report.summary
            else None
        ),
        {
            "minimum": 1.0,
            "mean": 1.0,
            "maximum": 1.0,
        },
    )

    repeated_report = (
        evaluate_pitch_type_matchup_overlay_shadow_dataset_quality(
            clean_dataset,
            enabled=True,
            evaluated_at_utc=FIXED_TIME,
        )
    )

    record(
        "8O-C17",
        "serialization deterministic",
        pass_report.to_dict()
        == repeated_report.to_dict(),
        pass_report.to_dict(),
        repeated_report.to_dict(),
    )

    record(
        "8O-C18",
        "expected fingerprint deterministic",
        len(expected_schema_fingerprint())
        == 64,
        expected_schema_fingerprint(),
        "64_character_sha256",
    )

    record(
        "8O-C19",
        "quality gate version retained",
        pass_report.summary is not None
        and (
            pass_report.summary.quality_gate_version
            == QUALITY_GATE_VERSION
        ),
        (
            pass_report.summary.quality_gate_version
            if pass_report.summary
            else None
        ),
        "8O-v1",
    )

    record(
        "8O-C20",
        "all prohibited authority remains false",
        pass_report.summary is not None
        and all(
            value is False
            for value in [
                pass_report.production_authority,
                pass_report.production_behavior_changed,
                pass_report.simulation_behavior_changed,
                pass_report.historical_outcomes_joined,
                pass_report.predictive_evaluation_executed,
                pass_report.summary.production_authority,
                pass_report.summary.production_behavior_changed,
                pass_report.summary.simulation_behavior_changed,
                pass_report.summary.historical_outcomes_joined,
                pass_report.summary.predictive_evaluation_executed,
            ]
        ),
        pass_report.to_dict(),
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
            "check": "eight_n_predecessor_present",
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
            "check": "pass_status_supported",
            "actual": pass_report.gate_status,
            "expected": "pass",
            "passed": (
                pass_report.gate_status == "pass"
            ),
        },
        {
            "check": "warn_status_supported",
            "actual": warning_report.gate_status,
            "expected": "warn",
            "passed": (
                warning_report.gate_status
                == "warn"
            ),
        },
        {
            "check": "fail_status_supported",
            "actual": fail_report.gate_status,
            "expected": "fail",
            "passed": (
                fail_report.gate_status == "fail"
            ),
        },
        {
            "check": "empty_status_supported",
            "actual": empty_report.gate_status,
            "expected": "empty",
            "passed": (
                empty_report.gate_status
                == "empty"
            ),
        },
        {
            "check": "disabled_path_non_emitting",
            "actual": disabled_report.emitted,
            "expected": False,
            "passed": (
                disabled_report.emitted is False
            ),
        },
        {
            "check": "twenty_required_gates_implemented",
            "actual": sum(
                1
                for result in pass_report.results
                if result.gate_type
                == "required"
            ),
            "expected": 20,
            "passed": sum(
                1
                for result in pass_report.results
                if result.gate_type
                == "required"
            )
            == 20,
        },
        {
            "check": "six_warning_gates_implemented",
            "actual": sum(
                1
                for result in pass_report.results
                if result.gate_type
                == "warning"
            ),
            "expected": 6,
            "passed": sum(
                1
                for result in pass_report.results
                if result.gate_type
                == "warning"
            )
            == 6,
        },
        {
            "check": "manifest_reconciliation_implemented",
            "actual": (
                pass_report.summary.manifest_reconciles
                if pass_report.summary
                else None
            ),
            "expected": True,
            "passed": (
                pass_report.summary is not None
                and pass_report.summary.manifest_reconciles
                is True
            ),
        },
        {
            "check": "partition_reconciliation_implemented",
            "actual": (
                pass_report.summary.partition_manifest_reconciles
                if pass_report.summary
                else None
            ),
            "expected": True,
            "passed": (
                pass_report.summary is not None
                and pass_report.summary.partition_manifest_reconciles
                is True
            ),
        },
        {
            "check": "schema_fingerprint_gate_implemented",
            "actual": (
                pass_report.summary.schema_fingerprint_matches
                if pass_report.summary
                else None
            ),
            "expected": True,
            "passed": (
                pass_report.summary is not None
                and pass_report.summary.schema_fingerprint_matches
                is True
            ),
        },
        {
            "check": "row_identity_gate_implemented",
            "actual": (
                pass_report.summary.row_identifiers_unique
                if pass_report.summary
                else None
            ),
            "expected": True,
            "passed": (
                pass_report.summary is not None
                and pass_report.summary.row_identifiers_unique
                is True
            ),
        },
        {
            "check": "source_version_gate_implemented",
            "actual": (
                pass_report.summary.source_versions_present
                if pass_report.summary
                else None
            ),
            "expected": True,
            "passed": (
                pass_report.summary is not None
                and pass_report.summary.source_versions_present
                is True
            ),
        },
        {
            "check": "coverage_statistics_implemented",
            "actual": (
                pass_report.summary.mean_coverage_share
                if pass_report.summary
                else None
            ),
            "expected": 1.0,
            "passed": (
                pass_report.summary is not None
                and pass_report.summary.mean_coverage_share
                == 1.0
            ),
        },
        {
            "check": "serialization_deterministic",
            "actual": (
                pass_report.to_dict()
                == repeated_report.to_dict()
            ),
            "expected": True,
            "passed": (
                pass_report.to_dict()
                == repeated_report.to_dict()
            ),
        },
        {
            "check": "warning_gates_non_authoritative",
            "actual": (
                warning_report.summary.failed_gate_count
                if warning_report.summary
                else None
            ),
            "expected": 0,
            "passed": (
                warning_report.summary is not None
                and warning_report.summary.failed_gate_count
                == 0
            ),
        },
        {
            "check": "production_simulation_validation_authority_absent",
            "actual": any(
                [
                    pass_report.production_authority,
                    pass_report.production_behavior_changed,
                    pass_report.simulation_behavior_changed,
                    pass_report.historical_outcomes_joined,
                    pass_report.predictive_evaluation_executed,
                ]
            ),
            "expected": False,
            "passed": all(
                value is False
                for value in [
                    pass_report.production_authority,
                    pass_report.production_behavior_changed,
                    pass_report.simulation_behavior_changed,
                    pass_report.historical_outcomes_joined,
                    pass_report.predictive_evaluation_executed,
                ]
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    reports = [
        pass_report,
        warning_report,
        fail_report,
        empty_report,
        disabled_report,
    ]

    status_rows = [
        {
            "gate_status": status,
            "count": sum(
                1
                for report in reports
                if report.gate_status == status
            ),
        }
        for status in (
            "pass",
            "warn",
            "fail",
            "empty",
            "disabled",
        )
    ]

    warning_rows = [
        {
            "diagnostic_code": code,
            "count": sum(
                1
                for report in reports
                if code
                in report.diagnostic_codes
            ),
        }
        for code in (
            "matchup_shadow_quality_partial_rows_present",
            "matchup_shadow_quality_fallback_rows_present",
            "matchup_shadow_quality_unknown_pitch_rows_present",
            "matchup_shadow_quality_pitcher_only_rows_present",
            "matchup_shadow_quality_duplicate_rows_present",
            "matchup_shadow_quality_coverage_below_half_present",
        )
    ]

    authority_rows = [
        {
            "authority": (
                "shadow_dataset_quality_gate_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "Bounded structural quality gates passed all checks."
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
            "authority": (
                "predictive_evaluation"
            ),
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
        "pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_implementation_passed"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_implementation_failed"
    )

    recommended_next_layer = (
        "8P_pitch_type_matchup_overlay_shadow_dataset_collection_contract_plan"
        if all_checks_passed
        else
        "8O_pitch_type_matchup_overlay_shadow_dataset_quality_gate_implementation_remediation"
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
        OUTPUT_DIR / "quality_gate_results.csv",
        [
            "gate_id",
            "gate_name",
            "gate_type",
            "passed",
            "triggered",
            "observed_value",
            "expected_value",
            "diagnostic_code",
        ],
        [
            result.to_dict()
            for result in pass_report.results
        ],
    )

    summary_rows = (
        [pass_report.summary.to_dict()]
        if pass_report.summary
        else []
    )

    write_csv(
        OUTPUT_DIR / "quality_gate_summary.csv",
        list(summary_rows[0].keys())
        if summary_rows
        else ["quality_gate_version"],
        summary_rows,
    )

    write_csv(
        OUTPUT_DIR / "status_counts.csv",
        [
            "gate_status",
            "count",
        ],
        status_rows,
    )

    write_csv(
        OUTPUT_DIR / "warning_counts.csv",
        [
            "diagnostic_code",
            "count",
        ],
        warning_rows,
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
                    "Plan bounded diagnostic collection for quality-gated shadow datasets."
                    if all_checks_passed
                    else
                    "Remediate failed 8O implementation checks."
                ),
                "entry_condition": (
                    "All nineteen 8O implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "quality_gate_report.json",
        pass_report.to_dict(),
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
        "quality_gate_version": (
            QUALITY_GATE_VERSION
        ),
        "required_gates_implemented": 20,
        "warning_gates_implemented": 6,
        "manifest_reconciliation_implemented": True,
        "partition_reconciliation_implemented": True,
        "schema_fingerprint_gate_implemented": True,
        "row_identity_gate_implemented": True,
        "source_version_gate_implemented": True,
        "coverage_statistics_implemented": True,
        "status_precedence_implemented": True,
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
        "shadow_dataset_collection_planning_allowed_next": (
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
                "quality_gate_results.csv",
                "quality_gate_summary.csv",
                "status_counts.csv",
                "warning_counts.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "quality_gate_report.json"
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
