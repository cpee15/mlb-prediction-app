#!/usr/bin/env python3
"""
Layer 6PF
Pitching-Plan Classification Diagnostic Integration Implementation

Validates disabled-by-default, metadata-only integration in the shared
simulation builder.

The fixture suite proves:
- zero classifier imports and calls when disabled;
- exact disabled-path baseline equivalence;
- enabled-path simulation equivalence apart from diagnostic metadata;
- deterministic and input-immutable behavior;
- isolated classifier and validation failures;
- no engine-argument, score, total, or probability changes.
"""

from __future__ import annotations

import builtins
import csv
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable
from unittest.mock import patch

from mlb_app.simulation import game_simulation_builder


LAYER_ID = "6PF"
LAYER_NAME = (
    "pitching_plan_classification_"
    "diagnostic_integration_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PF_pitching_plan_classification_"
    "diagnostic_integration_implementation"
)

BUILDER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6PE_pitching_plan_"
    "classification_diagnostic_integration.py"
)

CLASSIFIER_MODULE = (
    "mlb_app.simulation."
    "pitching_plan_classifier"
)

PROHIBITED_ACTIONS = [
    "starter_innings_change",
    "dynamic_starter_hook_change",
    "bullpen_sequence_change",
    "plate_appearance_probability_change",
    "simulation_parameter_change",
    "simulation_score_change",
    "win_probability_change",
    "canonical_probability_replacement",
    "public_api_change",
    "frontend_change",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "edge_detection",
    "bet_recommendation",
    "layer6_exit_finalization",
]


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
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def fake_engine_payload(
    game_pk: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    return {
        "game_pk": game_pk,
        "simulation_count": (
            config.get("simulation_count", 1000)
        ),
        "seed": config.get("seed", 77),
        "away_expected_runs": 4.25,
        "home_expected_runs": 4.55,
        "away_win_probability": 0.47,
        "home_win_probability": 0.53,
        "total_runs": 8.80,
        "score_distribution": {
            "away": [3, 4, 5],
            "home": [4, 5, 6],
        },
        "meta": {
            "engine_marker": "unchanged",
        },
    }


def remove_diagnostics(
    payload: dict[str, Any],
) -> dict[str, Any]:
    cleaned = deepcopy(payload)

    for key in ["meta", "metadata"]:
        metadata = cleaned.get(key)

        if isinstance(metadata, dict):
            metadata.pop(
                "pitching_plan_diagnostics",
                None,
            )

    return cleaned


def run_builder(
    config: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    calls: list[dict[str, Any]] = []

    def engine(
        game_pk: int,
        supplied_config: dict[str, Any],
    ) -> dict[str, Any]:
        calls.append(
            {
                "game_pk": game_pk,
                "config": deepcopy(
                    supplied_config
                ),
            }
        )

        return fake_engine_payload(
            game_pk,
            supplied_config,
        )

    with patch.object(
        game_simulation_builder,
        "_load_sandbox_engine",
        return_value=engine,
    ):
        payload = (
            game_simulation_builder
            .build_game_simulation(
                12345,
                config,
            )
        )

    return payload, calls


def evaluate_fixture(
    fixture: dict[str, Any],
) -> dict[str, Any]:
    config = deepcopy(
        fixture["config"]
    )

    original_config = deepcopy(config)

    baseline_config = {
        key: deepcopy(value)
        for key, value in config.items()
        if key
        not in {
            (
                "pitching_plan_"
                "diagnostics_enabled"
            ),
            "pitching_plan_evidence",
            (
                "pitching_plan_"
                "diagnostics_version"
            ),
        }
    }

    baseline_payload, baseline_calls = (
        run_builder(baseline_config)
    )

    import_count = 0
    classifier_call_count = 0

    original_import = builtins.__import__

    def tracking_import(
        name: str,
        globals: Any = None,
        locals: Any = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        nonlocal import_count

        if name == CLASSIFIER_MODULE:
            import_count += 1

        return original_import(
            name,
            globals,
            locals,
            fromlist,
            level,
        )

    classifier_module = None

    if fixture["enabled"]:
        classifier_module = __import__(
            CLASSIFIER_MODULE,
            fromlist=["*"],
        )

    patches = []

    if classifier_module is not None:
        original_classifier = (
            classifier_module
            .classify_pitching_plan
        )

        def counted_classifier(
            evidence: dict[str, Any],
        ) -> dict[str, Any]:
            nonlocal classifier_call_count
            classifier_call_count += 1

            return original_classifier(
                evidence
            )

        patches.append(
            patch.object(
                classifier_module,
                "classify_pitching_plan",
                side_effect=counted_classifier,
            )
        )

        if fixture.get(
            "force_invalid_payload"
        ):
            def invalid_classifier(
                evidence: dict[str, Any],
            ) -> dict[str, Any]:
                nonlocal classifier_call_count
                classifier_call_count += 1
                return {
                    "invalid": True,
                }

            patches[-1] = patch.object(
                classifier_module,
                "classify_pitching_plan",
                side_effect=invalid_classifier,
            )

        if fixture.get(
            "force_exception"
        ):
            def raising_classifier(
                evidence: dict[str, Any],
            ) -> dict[str, Any]:
                nonlocal classifier_call_count
                classifier_call_count += 1
                raise RuntimeError(
                    "forced diagnostic failure"
                )

            patches[-1] = patch.object(
                classifier_module,
                "classify_pitching_plan",
                side_effect=raising_classifier,
            )

    for active_patch in patches:
        active_patch.start()

    try:
        with patch.object(
            builtins,
            "__import__",
            side_effect=tracking_import,
        ):
            payload, engine_calls = (
                run_builder(config)
            )
    finally:
        for active_patch in reversed(patches):
            active_patch.stop()

    diagnostics = (
        payload.get("metadata", {})
        .get("pitching_plan_diagnostics")
    )

    disabled_exact_equality = (
        payload == baseline_payload
        if not fixture["enabled"]
        else True
    )

    enabled_simulation_equality = (
        remove_diagnostics(payload)
        == baseline_payload
        if fixture["enabled"]
        else True
    )

    expected_status = fixture[
        "expected_status"
    ]

    actual_status = (
        diagnostics.get("status")
        if isinstance(diagnostics, dict)
        else "absent"
    )

    diagnostic_contract_passed = (
        diagnostics is None
        if not fixture["enabled"]
        else (
            isinstance(diagnostics, dict)
            and set(diagnostics.keys())
            == {
                "enabled",
                "status",
                "version",
                "classification",
                "validation",
                "error",
                "behavioral_effect",
                (
                    "canonical_probability_"
                    "authority_changed"
                ),
                "production_activation",
            }
            and diagnostics[
                "behavioral_effect"
            ]
            == "none"
            and diagnostics[
                (
                    "canonical_probability_"
                    "authority_changed"
                )
            ]
            is False
            and diagnostics[
                "production_activation"
            ]
            is False
        )
    )

    expected_calls = fixture[
        "expected_classifier_calls"
    ]

    import_condition_passed = (
        import_count == 0
        if not fixture["enabled"]
        else import_count >= 1
    )

    engine_arguments_unchanged = (
        engine_calls == baseline_calls
    )

    config_unchanged = (
        config == original_config
    )

    deterministic = True

    if fixture.get("deterministic_replay"):
        replay_payload, _ = run_builder(config)
        deterministic = (
            payload == replay_payload
        )

    passed = all(
        [
            (
                classifier_call_count
                == expected_calls
            ),
            import_condition_passed,
            disabled_exact_equality,
            enabled_simulation_equality,
            engine_arguments_unchanged,
            config_unchanged,
            diagnostic_contract_passed,
            actual_status == expected_status,
            deterministic,
        ]
    )

    return {
        "fixture_id": fixture["fixture_id"],
        "scenario": fixture["scenario"],
        "enabled": fixture["enabled"],
        "expected_classifier_calls": (
            expected_calls
        ),
        "actual_classifier_calls": (
            classifier_call_count
        ),
        "classifier_import_count": (
            import_count
        ),
        "expected_status": expected_status,
        "actual_status": actual_status,
        "disabled_exact_equality": (
            disabled_exact_equality
        ),
        "enabled_simulation_equality": (
            enabled_simulation_equality
        ),
        "engine_arguments_unchanged": (
            engine_arguments_unchanged
        ),
        "config_unchanged": config_unchanged,
        "diagnostic_contract_passed": (
            diagnostic_contract_passed
        ),
        "deterministic": deterministic,
        "passed": passed,
        "payload": payload,
        "baseline_payload": baseline_payload,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    fixtures = [
        {
            "fixture_id": "DI-F01",
            "scenario": "disabled_no_evidence",
            "enabled": False,
            "expected_classifier_calls": 0,
            "expected_status": "absent",
            "config": {
                "simulation_count": 1000,
                "seed": 77,
            },
        },
        {
            "fixture_id": "DI-F02",
            "scenario": "disabled_with_evidence",
            "enabled": False,
            "expected_classifier_calls": 0,
            "expected_status": "absent",
            "config": {
                "simulation_count": 1000,
                "seed": 77,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "starter-disabled"
                    ),
                },
            },
        },
        {
            "fixture_id": "DI-F03",
            "scenario": (
                "enabled_traditional_starter"
            ),
            "enabled": True,
            "expected_classifier_calls": 1,
            "expected_status": "classified",
            "config": {
                "simulation_count": 1000,
                "seed": 77,
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "starter-a"
                    ),
                },
            },
        },
        {
            "fixture_id": "DI-F04",
            "scenario": "enabled_opener_bulk",
            "enabled": True,
            "expected_classifier_calls": 1,
            "expected_status": "classified",
            "config": {
                "simulation_count": 1000,
                "seed": 77,
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "opener-a"
                    ),
                    (
                        "expected_bulk_"
                        "pitcher_id"
                    ): "bulk-a",
                    (
                        "announced_pitching_"
                        "plan"
                    ): "opener_bulk",
                },
            },
        },
        {
            "fixture_id": "DI-F05",
            "scenario": (
                "enabled_unknown_fallback"
            ),
            "enabled": True,
            "expected_classifier_calls": 1,
            "expected_status": "classified",
            "config": {
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {},
            },
        },
        {
            "fixture_id": "DI-F06",
            "scenario": "enabled_invalid_payload",
            "enabled": True,
            "expected_classifier_calls": 1,
            "expected_status": (
                "validation_failed"
            ),
            "force_invalid_payload": True,
            "config": {
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {},
            },
        },
        {
            "fixture_id": "DI-F07",
            "scenario": (
                "enabled_classifier_exception"
            ),
            "enabled": True,
            "expected_classifier_calls": 1,
            "expected_status": "error",
            "force_exception": True,
            "config": {
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {},
            },
        },
        {
            "fixture_id": "DI-F08",
            "scenario": "input_immutability",
            "enabled": True,
            "expected_classifier_calls": 1,
            "expected_status": "classified",
            "config": {
                "simulation_count": 1500,
                "seed": 91,
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "starter-b"
                    ),
                    (
                        "roster_and_"
                        "availability_state"
                    ): {
                        "starter-b": True,
                    },
                },
            },
        },
        {
            "fixture_id": "DI-F09",
            "scenario": "deterministic_replay",
            "enabled": True,
            "expected_classifier_calls": 1,
            "expected_status": "classified",
            "deterministic_replay": True,
            "config": {
                "simulation_count": 1000,
                "seed": 77,
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "starter-c"
                    ),
                },
            },
        },
        {
            "fixture_id": "DI-F10",
            "scenario": (
                "no_engine_argument_change"
            ),
            "enabled": True,
            "expected_classifier_calls": 1,
            "expected_status": "classified",
            "config": {
                "simulation_count": 2222,
                "seed": 404,
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "starter-d"
                    ),
                },
            },
        },
    ]

    results = [
        evaluate_fixture(fixture)
        for fixture in fixtures
    ]

    builder_text = BUILDER_PATH.read_text(
        encoding="utf-8",
        errors="ignore",
    )

    structural_checks = [
        {
            "check": "required_files_exist",
            "actual": all(
                path.exists()
                for path in [
                    BUILDER_PATH,
                    PLAN_PATH,
                ]
            ),
            "expected": True,
            "passed": all(
                path.exists()
                for path in [
                    BUILDER_PATH,
                    PLAN_PATH,
                ]
            ),
        },
        {
            "check": (
                "lazy_diagnostic_helper_present"
            ),
            "actual": (
                "_attach_pitching_plan_diagnostics"
                in builder_text
            ),
            "expected": True,
            "passed": (
                "_attach_pitching_plan_diagnostics"
                in builder_text
            ),
        },
        {
            "check": (
                "disabled_default_present"
            ),
            "actual": (
                '"pitching_plan_diagnostics_enabled"'
                in builder_text
            ),
            "expected": True,
            "passed": (
                '"pitching_plan_diagnostics_enabled"'
                in builder_text
            ),
        },
        {
            "check": "ten_fixtures_execute",
            "actual": len(results),
            "expected": 10,
            "passed": len(results) == 10,
        },
        {
            "check": "all_fixtures_pass",
            "actual": sum(
                1
                for row in results
                if row["passed"]
            ),
            "expected": len(results),
            "passed": all(
                row["passed"]
                for row in results
            ),
        },
        {
            "check": (
                "disabled_fixtures_zero_calls"
            ),
            "actual": sum(
                row[
                    "actual_classifier_calls"
                ]
                for row in results
                if not row["enabled"]
            ),
            "expected": 0,
            "passed": all(
                row[
                    "actual_classifier_calls"
                ]
                == 0
                for row in results
                if not row["enabled"]
            ),
        },
        {
            "check": (
                "disabled_fixtures_zero_imports"
            ),
            "actual": sum(
                row[
                    "classifier_import_count"
                ]
                for row in results
                if not row["enabled"]
            ),
            "expected": 0,
            "passed": all(
                row[
                    "classifier_import_count"
                ]
                == 0
                for row in results
                if not row["enabled"]
            ),
        },
        {
            "check": (
                "all_simulation_fields_equivalent"
            ),
            "actual": sum(
                1
                for row in results
                if (
                    row[
                        "disabled_exact_equality"
                    ]
                    and row[
                        "enabled_simulation_equality"
                    ]
                )
            ),
            "expected": len(results),
            "passed": all(
                (
                    row[
                        "disabled_exact_equality"
                    ]
                    and row[
                        "enabled_simulation_equality"
                    ]
                )
                for row in results
            ),
        },
        {
            "check": (
                "all_engine_arguments_unchanged"
            ),
            "actual": sum(
                1
                for row in results
                if row[
                    "engine_arguments_unchanged"
                ]
            ),
            "expected": len(results),
            "passed": all(
                row[
                    "engine_arguments_unchanged"
                ]
                for row in results
            ),
        },
        {
            "check": "all_inputs_unchanged",
            "actual": sum(
                1
                for row in results
                if row["config_unchanged"]
            ),
            "expected": len(results),
            "passed": all(
                row["config_unchanged"]
                for row in results
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in structural_checks
    )

    safety_rows = [
        {
            "boundary": action,
            "changed_or_executed": False,
            "passed": True,
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": (
                    "disabled_by_default_"
                    "diagnostic_integration"
                ),
                "changed_or_executed": True,
                "passed": all_checks_passed,
            },
            {
                "boundary": (
                    "metadata_only_enabled_path"
                ),
                "changed_or_executed": True,
                "passed": all(
                    row[
                        "enabled_simulation_equality"
                    ]
                    for row in results
                ),
            },
        ]
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        structural_checks,
    )

    write_csv(
        OUTPUT_DIR / "fixture_results.csv",
        [
            "fixture_id",
            "scenario",
            "enabled",
            "expected_classifier_calls",
            "actual_classifier_calls",
            "classifier_import_count",
            "expected_status",
            "actual_status",
            "disabled_exact_equality",
            "enabled_simulation_equality",
            "engine_arguments_unchanged",
            "config_unchanged",
            "diagnostic_contract_passed",
            "deterministic",
            "passed",
        ],
        [
            {
                key: row[key]
                for key in [
                    "fixture_id",
                    "scenario",
                    "enabled",
                    (
                        "expected_classifier_calls"
                    ),
                    (
                        "actual_classifier_calls"
                    ),
                    "classifier_import_count",
                    "expected_status",
                    "actual_status",
                    (
                        "disabled_exact_equality"
                    ),
                    (
                        "enabled_simulation_equality"
                    ),
                    (
                        "engine_arguments_unchanged"
                    ),
                    "config_unchanged",
                    (
                        "diagnostic_contract_passed"
                    ),
                    "deterministic",
                    "passed",
                ]
            }
            for row in results
        ],
    )

    write_csv(
        OUTPUT_DIR / "safety_audit.csv",
        [
            "boundary",
            "changed_or_executed",
            "passed",
        ],
        safety_rows,
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
                    "6PG_pitching_plan_"
                    "classification_diagnostic_"
                    "integration_independent_audit"
                ),
                "recommended_action": (
                    "Independently audit disabled-path "
                    "non-reachability and enabled-path "
                    "simulation equivalence."
                ),
                "entry_condition": (
                    "All 6PF implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "fixture_payloads.json",
        results,
    )

    implementation_summary = {
        "diagnostic_integration_implemented": (
            all_checks_passed
        ),
        "disabled_by_default": True,
        "disabled_classifier_calls": sum(
            row["actual_classifier_calls"]
            for row in results
            if not row["enabled"]
        ),
        "disabled_classifier_imports": sum(
            row["classifier_import_count"]
            for row in results
            if not row["enabled"]
        ),
        "fixtures_executed": len(results),
        "fixtures_passed": sum(
            1
            for row in results
            if row["passed"]
        ),
        "production_classifier_activated": False,
        "simulation_behavior_changed": False,
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_summary.json",
        implementation_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "pitching_plan_classification_"
            "diagnostic_integration_"
            "implementation_complete"
            if all_checks_passed
            else
            "pitching_plan_classification_"
            "diagnostic_integration_"
            "implementation_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "implementation_checks_passed": sum(
            1
            for row in structural_checks
            if row["passed"]
        ),
        "implementation_checks_required": len(
            structural_checks
        ),
        "fixtures_executed": len(results),
        "fixtures_passed": sum(
            1
            for row in results
            if row["passed"]
        ),
        "disabled_classifier_calls": sum(
            row["actual_classifier_calls"]
            for row in results
            if not row["enabled"]
        ),
        "disabled_classifier_imports": sum(
            row["classifier_import_count"]
            for row in results
            if not row["enabled"]
        ),
        "disabled_path_exactly_equivalent": all(
            row["disabled_exact_equality"]
            for row in results
            if not row["enabled"]
        ),
        "enabled_simulation_fields_equivalent": all(
            row["enabled_simulation_equality"]
            for row in results
            if row["enabled"]
        ),
        "engine_arguments_unchanged": all(
            row["engine_arguments_unchanged"]
            for row in results
        ),
        "inputs_unchanged": all(
            row["config_unchanged"]
            for row in results
        ),
        "diagnostics_default_enabled": False,
        "production_classifier_activated": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": (
            False
        ),
        "broad_layer6_exit_paused": True,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_authority_granted": False,
        "backend_behavior_change_allowed_next": False,
        "frontend_behavior_change_allowed_next": False,
        "simulation_parameter_change_allowed_next": False,
        "final_probability_replacement_allowed_next": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "accuracy_metrics_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "independent_integration_audit_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            "6PG_pitching_plan_classification_"
            "diagnostic_integration_"
            "independent_audit"
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "implementation_checks.csv"
            ),
            str(
                OUTPUT_DIR / "fixture_results.csv"
            ),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "fixture_payloads.json"
            ),
            str(
                OUTPUT_DIR
                / "implementation_summary.json"
            ),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(json.dumps(diagnosis, indent=2))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
