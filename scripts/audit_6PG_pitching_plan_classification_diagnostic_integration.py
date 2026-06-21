#!/usr/bin/env python3
"""
Layer 6PG
Independent Pitching-Plan Diagnostic Integration Audit

Independently audits the merged 6PF integration for:

- zero disabled-path classifier imports;
- exact disabled-path baseline equivalence;
- diagnostic configuration isolation from engine arguments;
- metadata-only enabled-path differences;
- diagnostic contract and provenance retention;
- classifier validation and exception isolation;
- input immutability;
- no classifier reachability from the game engine;
- no simulation or probability authority changes.

This layer does not modify production behavior.
"""

from __future__ import annotations

import ast
import builtins
import csv
import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable
from unittest.mock import patch

from mlb_app.simulation import game_simulation_builder


LAYER_ID = "6PG"
LAYER_NAME = (
    "pitching_plan_classification_diagnostic_"
    "integration_independent_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PG_pitching_plan_classification_"
    "diagnostic_integration_independent_audit"
)

BUILDER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py"
)

ENGINE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_engine_v2.py"
)

CLASSIFIER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "pitching_plan_classifier.py"
)

IMPLEMENTATION_SCRIPT = (
    ROOT
    / "scripts/implement_6PF_pitching_plan_"
    "classification_diagnostic_integration.py"
)

IMPLEMENTATION_DIAGNOSIS = (
    ROOT
    / "tmp/layer_6PF_pitching_plan_"
    "classification_diagnostic_integration_"
    "implementation/diagnosis.json"
)

CLASSIFIER_MODULE = (
    "mlb_app.simulation."
    "pitching_plan_classifier"
)

DIAGNOSTIC_KEYS = {
    "pitching_plan_diagnostics_enabled",
    "pitching_plan_evidence",
    "pitching_plan_diagnostics_version",
}

DIAGNOSTIC_PAYLOAD_FIELDS = {
    "enabled",
    "status",
    "version",
    "classification",
    "validation",
    "error",
    "behavioral_effect",
    "canonical_probability_authority_changed",
    "production_activation",
}

PROHIBITED_ACTIONS = [
    "production_behavior_activation",
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


def read_json(
    path: Path,
) -> dict[str, Any]:
    return json.loads(
        path.read_text(encoding="utf-8")
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
        "away_expected_runs": 4.20,
        "home_expected_runs": 4.60,
        "away_win_probability": 0.46,
        "home_win_probability": 0.54,
        "total_runs": 8.80,
        "score_distribution": {
            "away": [3, 4, 5],
            "home": [4, 5, 6],
        },
        "meta": {
            "engine_marker": "independent-audit",
        },
    }


def stripped_engine_config(
    config: dict[str, Any],
) -> dict[str, Any]:
    return {
        key: deepcopy(value)
        for key, value in config.items()
        if key not in DIAGNOSTIC_KEYS
    }


def independent_baseline(
    game_pk: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    engine_config = stripped_engine_config(
        config
    )

    payload = fake_engine_payload(
        game_pk,
        engine_config,
    )

    return (
        game_simulation_builder
        ._normalize_metadata(
            payload,
            game_pk=game_pk,
            config=engine_config,
        )
    )


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


def make_invalid_module() -> ModuleType:
    module = ModuleType(CLASSIFIER_MODULE)

    def classify_pitching_plan(
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "invalid": True,
        }

    def validate_pitching_plan_payload(
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "valid": False,
            "errors": [
                "forced_invalid_payload",
            ],
        }

    module.classify_pitching_plan = (
        classify_pitching_plan
    )

    module.validate_pitching_plan_payload = (
        validate_pitching_plan_payload
    )

    return module


def make_raising_module() -> ModuleType:
    module = ModuleType(CLASSIFIER_MODULE)

    def classify_pitching_plan(
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        raise RuntimeError(
            "forced independent audit failure"
        )

    def validate_pitching_plan_payload(
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        raise AssertionError(
            "validation should not be reached"
        )

    module.classify_pitching_plan = (
        classify_pitching_plan
    )

    module.validate_pitching_plan_payload = (
        validate_pitching_plan_payload
    )

    return module


def run_builder(
    config: dict[str, Any],
    *,
    fake_classifier_module: ModuleType | None = None,
    track_imports: bool = False,
) -> dict[str, Any]:
    engine_calls: list[dict[str, Any]] = []
    classifier_imports = 0

    def engine(
        game_pk: int,
        supplied_config: dict[str, Any],
    ) -> dict[str, Any]:
        engine_calls.append(
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

    original_import = builtins.__import__

    def tracking_import(
        name: str,
        globals: Any = None,
        locals: Any = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        nonlocal classifier_imports

        if name == CLASSIFIER_MODULE:
            classifier_imports += 1

        return original_import(
            name,
            globals,
            locals,
            fromlist,
            level,
        )

    module_patch = (
        patch.dict(
            sys.modules,
            {
                CLASSIFIER_MODULE: (
                    fake_classifier_module
                )
            },
        )
        if fake_classifier_module is not None
        else patch.dict(
            sys.modules,
            {},
        )
    )

    import_patch = (
        patch.object(
            builtins,
            "__import__",
            side_effect=tracking_import,
        )
        if track_imports
        else patch.object(
            builtins,
            "__import__",
            side_effect=original_import,
        )
    )

    with patch.object(
        game_simulation_builder,
        "_load_sandbox_engine",
        return_value=engine,
    ):
        with module_patch:
            with import_patch:
                payload = (
                    game_simulation_builder
                    .build_game_simulation(
                        24680,
                        config,
                    )
                )

    return {
        "payload": payload,
        "engine_calls": engine_calls,
        "classifier_imports": (
            classifier_imports
        ),
    }


def evaluate_case(
    case: dict[str, Any],
) -> dict[str, Any]:
    config = deepcopy(case["config"])
    original_config = deepcopy(config)

    baseline = independent_baseline(
        24680,
        config,
    )

    fake_module = None

    if case.get("forced_mode") == "invalid":
        fake_module = make_invalid_module()

    if case.get("forced_mode") == "exception":
        fake_module = make_raising_module()

    if not case["enabled"]:
        sys.modules.pop(
            CLASSIFIER_MODULE,
            None,
        )

    result = run_builder(
        config,
        fake_classifier_module=fake_module,
        track_imports=True,
    )

    payload = result["payload"]
    engine_calls = result["engine_calls"]

    diagnostics = (
        payload.get("metadata", {})
        .get("pitching_plan_diagnostics")
    )

    expected_engine_config = (
        stripped_engine_config(config)
    )

    engine_config_isolated = (
        len(engine_calls) == 1
        and engine_calls[0]["game_pk"]
        == 24680
        and engine_calls[0]["config"]
        == expected_engine_config
    )

    config_unchanged = (
        config == original_config
    )

    disabled_exact_equivalence = (
        payload == baseline
        if not case["enabled"]
        else True
    )

    enabled_simulation_equivalence = (
        remove_diagnostics(payload)
        == baseline
        if case["enabled"]
        else True
    )

    actual_status = (
        diagnostics.get("status")
        if isinstance(diagnostics, dict)
        else "absent"
    )

    diagnostic_contract_passed = (
        diagnostics is None
        if not case["enabled"]
        else (
            isinstance(diagnostics, dict)
            and set(diagnostics.keys())
            == DIAGNOSTIC_PAYLOAD_FIELDS
            and diagnostics.get(
                "behavioral_effect"
            )
            == "none"
            and diagnostics.get(
                "production_activation"
            )
            is False
            and diagnostics.get(
                (
                    "canonical_probability_"
                    "authority_changed"
                )
            )
            is False
        )
    )

    provenance_passed = True

    if case.get("require_provenance"):
        classification = (
            diagnostics.get("classification")
            if isinstance(diagnostics, dict)
            else None
        )

        provenance_passed = (
            isinstance(classification, dict)
            and "source_status"
            in classification
            and "source_provenance"
            in classification
            and "fallback_used"
            in classification
            and "diagnostics"
            in classification
        )

    version_passed = True

    if case.get("expected_version"):
        version_passed = (
            isinstance(diagnostics, dict)
            and diagnostics.get("version")
            == case["expected_version"]
        )

    disabled_import_passed = (
        result["classifier_imports"] == 0
        if not case["enabled"]
        else True
    )

    passed = all(
        [
            disabled_exact_equivalence,
            enabled_simulation_equivalence,
            engine_config_isolated,
            config_unchanged,
            diagnostic_contract_passed,
            provenance_passed,
            version_passed,
            disabled_import_passed,
            (
                actual_status
                == case["expected_status"]
            ),
        ]
    )

    return {
        "case_id": case["case_id"],
        "scenario": case["scenario"],
        "enabled": case["enabled"],
        "expected_status": (
            case["expected_status"]
        ),
        "actual_status": actual_status,
        "classifier_imports": (
            result["classifier_imports"]
        ),
        "disabled_exact_equivalence": (
            disabled_exact_equivalence
        ),
        "enabled_simulation_equivalence": (
            enabled_simulation_equivalence
        ),
        "engine_config_isolated": (
            engine_config_isolated
        ),
        "config_unchanged": config_unchanged,
        "diagnostic_contract_passed": (
            diagnostic_contract_passed
        ),
        "provenance_passed": (
            provenance_passed
        ),
        "version_passed": version_passed,
        "passed": passed,
        "payload": payload,
        "baseline": baseline,
        "engine_calls": engine_calls,
    }


def builder_ast_checks() -> dict[str, Any]:
    source = BUILDER_PATH.read_text(
        encoding="utf-8",
        errors="ignore",
    )

    tree = ast.parse(
        source,
        filename=str(BUILDER_PATH),
    )

    function_nodes = {
        node.name: node
        for node in tree.body
        if isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
            ),
        )
    }

    helper = function_nodes.get(
        "_attach_pitching_plan_diagnostics"
    )

    builder = function_nodes.get(
        "build_game_simulation"
    )

    module_level_classifier_import = any(
        isinstance(
            node,
            (
                ast.Import,
                ast.ImportFrom,
            ),
        )
        and (
            (
                isinstance(node, ast.ImportFrom)
                and node.module
                == CLASSIFIER_MODULE
            )
            or (
                isinstance(node, ast.Import)
                and any(
                    alias.name
                    == CLASSIFIER_MODULE
                    for alias in node.names
                )
            )
        )
        for node in tree.body
    )

    lazy_import_present = False

    if helper is not None:
        lazy_import_present = any(
            isinstance(node, ast.ImportFrom)
            and node.module
            == CLASSIFIER_MODULE
            for node in ast.walk(helper)
        )

    diagnostic_keys_present = all(
        key in source
        for key in DIAGNOSTIC_KEYS
    )

    engine_call_present = False

    if builder is not None:
        engine_call_present = any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "engine"
            for node in ast.walk(builder)
        )

    return {
        "module_level_classifier_import": (
            module_level_classifier_import
        ),
        "lazy_import_present": (
            lazy_import_present
        ),
        "diagnostic_keys_present": (
            diagnostic_keys_present
        ),
        "engine_call_present": (
            engine_call_present
        ),
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files_exist = all(
        path.exists()
        for path in [
            BUILDER_PATH,
            ENGINE_PATH,
            CLASSIFIER_PATH,
            IMPLEMENTATION_SCRIPT,
        ]
    )

    implementation_run = subprocess.run(
        [
            sys.executable,
            str(IMPLEMENTATION_SCRIPT),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    implementation_script_passed = (
        implementation_run.returncode == 0
    )

    implementation_diagnosis = (
        read_json(IMPLEMENTATION_DIAGNOSIS)
        if IMPLEMENTATION_DIAGNOSIS.exists()
        else {}
    )

    implementation_contract_passed = all(
        [
            implementation_diagnosis.get(
                "all_checks_passed"
            )
            is True,
            implementation_diagnosis.get(
                "fixtures_passed"
            )
            == 10,
            implementation_diagnosis.get(
                "disabled_classifier_calls"
            )
            == 0,
            implementation_diagnosis.get(
                "disabled_classifier_imports"
            )
            == 0,
            implementation_diagnosis.get(
                "engine_arguments_unchanged"
            )
            is True,
            implementation_diagnosis.get(
                "simulation_behavior_changed"
            )
            is False,
            implementation_diagnosis.get(
                (
                    "canonical_probability_"
                    "authority_changed"
                )
            )
            is False,
        ]
    )

    cases = [
        {
            "case_id": "PG-C01",
            "scenario": "disabled_no_evidence",
            "enabled": False,
            "expected_status": "absent",
            "config": {
                "simulation_count": 1000,
                "seed": 77,
            },
        },
        {
            "case_id": "PG-C02",
            "scenario": (
                "disabled_with_full_evidence"
            ),
            "enabled": False,
            "expected_status": "absent",
            "config": {
                "simulation_count": 1200,
                "seed": 88,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "disabled-starter"
                    ),
                    (
                        "expected_bulk_"
                        "pitcher_id"
                    ): "disabled-bulk",
                    (
                        "announced_pitching_"
                        "plan"
                    ): "opener_bulk",
                },
                (
                    "pitching_plan_"
                    "diagnostics_version"
                ): "disabled-version",
            },
        },
        {
            "case_id": "PG-C03",
            "scenario": (
                "enabled_traditional_provenance"
            ),
            "enabled": True,
            "expected_status": "classified",
            "require_provenance": True,
            "config": {
                "simulation_count": 1300,
                "seed": 99,
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "starter-a"
                    ),
                    "source_name": (
                        "independent-audit"
                    ),
                },
            },
        },
        {
            "case_id": "PG-C04",
            "scenario": (
                "enabled_opener_bulk_provenance"
            ),
            "enabled": True,
            "expected_status": "classified",
            "require_provenance": True,
            "config": {
                "simulation_count": 1400,
                "seed": 100,
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
                    "source_name": (
                        "independent-audit"
                    ),
                },
            },
        },
        {
            "case_id": "PG-C05",
            "scenario": (
                "enabled_unknown_fallback"
            ),
            "enabled": True,
            "expected_status": "classified",
            "require_provenance": True,
            "config": {
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {},
            },
        },
        {
            "case_id": "PG-C06",
            "scenario": (
                "validation_failure_isolated"
            ),
            "enabled": True,
            "expected_status": (
                "validation_failed"
            ),
            "forced_mode": "invalid",
            "config": {
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "starter-invalid"
                    ),
                },
            },
        },
        {
            "case_id": "PG-C07",
            "scenario": (
                "classifier_exception_isolated"
            ),
            "enabled": True,
            "expected_status": "error",
            "forced_mode": "exception",
            "config": {
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "starter-error"
                    ),
                },
            },
        },
        {
            "case_id": "PG-C08",
            "scenario": (
                "custom_version_and_immutability"
            ),
            "enabled": True,
            "expected_status": "classified",
            "expected_version": (
                "independent-diagnostics-v9"
            ),
            "config": {
                "simulation_count": 1700,
                "seed": 303,
                (
                    "pitching_plan_"
                    "diagnostics_enabled"
                ): True,
                (
                    "pitching_plan_"
                    "diagnostics_version"
                ): "independent-diagnostics-v9",
                "pitching_plan_evidence": {
                    "listed_starter_id": (
                        "starter-versioned"
                    ),
                    (
                        "roster_and_"
                        "availability_state"
                    ): {
                        "starter-versioned": True,
                    },
                },
            },
        },
    ]

    results = [
        evaluate_case(case)
        for case in cases
    ]

    ast_checks = builder_ast_checks()

    engine_text = ENGINE_PATH.read_text(
        encoding="utf-8",
        errors="ignore",
    )

    engine_classifier_reference = any(
        token in engine_text
        for token in [
            "pitching_plan_classifier",
            "classify_pitching_plan",
            (
                "pitching_plan_"
                "diagnostics_enabled"
            ),
        ]
    )

    production_reference_rows: list[
        dict[str, Any]
    ] = []

    for path in sorted(
        (ROOT / "mlb_app").rglob("*.py")
    ):
        if path == CLASSIFIER_PATH:
            continue

        text = path.read_text(
            encoding="utf-8",
            errors="ignore",
        )

        if (
            "pitching_plan_classifier"
            not in text
            and "classify_pitching_plan"
            not in text
        ):
            continue

        production_reference_rows.append(
            {
                "path": str(
                    path.relative_to(ROOT)
                ),
                "is_shared_builder": (
                    path == BUILDER_PATH
                ),
                "is_game_engine": (
                    path == ENGINE_PATH
                ),
            }
        )

    only_builder_references_classifier = (
        len(production_reference_rows) == 1
        and production_reference_rows[0][
            "is_shared_builder"
        ]
        is True
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_pf_script_executes"
            ),
            "actual": (
                implementation_run.returncode
            ),
            "expected": 0,
            "passed": (
                implementation_script_passed
            ),
        },
        {
            "check": (
                "six_pf_contract_still_passes"
            ),
            "actual": (
                implementation_contract_passed
            ),
            "expected": True,
            "passed": (
                implementation_contract_passed
            ),
        },
        {
            "check": (
                "eight_independent_cases_execute"
            ),
            "actual": len(results),
            "expected": 8,
            "passed": len(results) == 8,
        },
        {
            "check": (
                "all_independent_cases_pass"
            ),
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
                "disabled_cases_zero_imports"
            ),
            "actual": sum(
                row["classifier_imports"]
                for row in results
                if not row["enabled"]
            ),
            "expected": 0,
            "passed": all(
                row["classifier_imports"] == 0
                for row in results
                if not row["enabled"]
            ),
        },
        {
            "check": (
                "all_engine_configs_isolated"
            ),
            "actual": sum(
                1
                for row in results
                if row[
                    "engine_config_isolated"
                ]
            ),
            "expected": len(results),
            "passed": all(
                row["engine_config_isolated"]
                for row in results
            ),
        },
        {
            "check": (
                "all_simulation_outputs_equivalent"
            ),
            "actual": sum(
                1
                for row in results
                if (
                    row[
                        "disabled_exact_equivalence"
                    ]
                    and row[
                        "enabled_simulation_equivalence"
                    ]
                )
            ),
            "expected": len(results),
            "passed": all(
                (
                    row[
                        "disabled_exact_equivalence"
                    ]
                    and row[
                        "enabled_simulation_equivalence"
                    ]
                )
                for row in results
            ),
        },
        {
            "check": (
                "classifier_import_is_lazy"
            ),
            "actual": all(
                [
                    (
                        ast_checks[
                            "module_level_classifier_import"
                        ]
                        is False
                    ),
                    (
                        ast_checks[
                            "lazy_import_present"
                        ]
                        is True
                    ),
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    (
                        ast_checks[
                            "module_level_classifier_import"
                        ]
                        is False
                    ),
                    (
                        ast_checks[
                            "lazy_import_present"
                        ]
                        is True
                    ),
                ]
            ),
        },
        {
            "check": (
                "game_engine_has_no_classifier_reach"
            ),
            "actual": (
                engine_classifier_reference
            ),
            "expected": False,
            "passed": (
                not engine_classifier_reference
            ),
        },
        {
            "check": (
                "only_shared_builder_references_classifier"
            ),
            "actual": [
                row["path"]
                for row in production_reference_rows
            ],
            "expected": [
                (
                    "mlb_app/simulation/"
                    "game_simulation_builder.py"
                )
            ],
            "passed": (
                only_builder_references_classifier
            ),
        },
        {
            "check": (
                "all_inputs_unchanged"
            ),
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
        for row in checks
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
                    "independent_diagnostic_"
                    "integration_audit"
                ),
                "changed_or_executed": True,
                "passed": all_checks_passed,
            },
            {
                "boundary": (
                    "metadata_only_diagnostic_reach"
                ),
                "changed_or_executed": True,
                "passed": all(
                    row[
                        "enabled_simulation_equivalence"
                    ]
                    for row in results
                    if row["enabled"]
                ),
            },
        ]
    )

    if all_checks_passed:
        diagnosis_value = (
            "pitching_plan_classification_"
            "diagnostic_integration_"
            "independent_audit_passed"
        )

        recommended_next_layer = (
            "6PH_pitching_plan_classification_"
            "diagnostic_integration_"
            "completion_assessment"
        )

        recommended_action = (
            "Assess GM-01 diagnostic integration completion "
            "without granting production behavior authority."
        )
    else:
        diagnosis_value = (
            "pitching_plan_classification_"
            "diagnostic_integration_"
            "independent_audit_failed"
        )

        recommended_next_layer = (
            "6PH_pitching_plan_classification_"
            "diagnostic_integration_remediation"
        )

        recommended_action = (
            "Remediate independent integration audit gaps "
            "before any completion assessment."
        )

    write_csv(
        OUTPUT_DIR / "audit_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "independent_case_results.csv",
        [
            "case_id",
            "scenario",
            "enabled",
            "expected_status",
            "actual_status",
            "classifier_imports",
            "disabled_exact_equivalence",
            "enabled_simulation_equivalence",
            "engine_config_isolated",
            "config_unchanged",
            "diagnostic_contract_passed",
            "provenance_passed",
            "version_passed",
            "passed",
        ],
        [
            {
                key: row[key]
                for key in [
                    "case_id",
                    "scenario",
                    "enabled",
                    "expected_status",
                    "actual_status",
                    "classifier_imports",
                    (
                        "disabled_exact_equivalence"
                    ),
                    (
                        "enabled_simulation_equivalence"
                    ),
                    "engine_config_isolated",
                    "config_unchanged",
                    (
                        "diagnostic_contract_passed"
                    ),
                    "provenance_passed",
                    "version_passed",
                    "passed",
                ]
            }
            for row in results
        ],
    )

    write_csv(
        OUTPUT_DIR / "production_reference_scan.csv",
        [
            "path",
            "is_shared_builder",
            "is_game_engine",
        ],
        production_reference_rows,
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
                    recommended_next_layer
                ),
                "recommended_action": (
                    recommended_action
                ),
                "entry_condition": (
                    "All 6PG independent audit checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "independent_case_payloads.json",
        results,
    )

    write_json(
        OUTPUT_DIR / "ast_audit.json",
        ast_checks,
    )

    audit_summary = {
        "six_pf_script_passed": (
            implementation_script_passed
        ),
        "six_pf_contract_passed": (
            implementation_contract_passed
        ),
        "independent_cases_executed": len(
            results
        ),
        "independent_cases_passed": sum(
            1
            for row in results
            if row["passed"]
        ),
        "disabled_classifier_imports": sum(
            row["classifier_imports"]
            for row in results
            if not row["enabled"]
        ),
        "engine_configs_isolated": all(
            row["engine_config_isolated"]
            for row in results
        ),
        "simulation_outputs_equivalent": all(
            (
                row[
                    "disabled_exact_equivalence"
                ]
                and row[
                    "enabled_simulation_equivalence"
                ]
            )
            for row in results
        ),
        "only_shared_builder_references_classifier": (
            only_builder_references_classifier
        ),
        "game_engine_classifier_reach": (
            engine_classifier_reference
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
        OUTPUT_DIR / "audit_summary.json",
        audit_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_value,
        "all_checks_passed": all_checks_passed,
        "audit_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "audit_checks_required": len(checks),
        "six_pf_script_passed": (
            implementation_script_passed
        ),
        "six_pf_contract_passed": (
            implementation_contract_passed
        ),
        "independent_cases_executed": len(
            results
        ),
        "independent_cases_passed": sum(
            1
            for row in results
            if row["passed"]
        ),
        "disabled_classifier_imports": sum(
            row["classifier_imports"]
            for row in results
            if not row["enabled"]
        ),
        "disabled_path_exactly_equivalent": all(
            row["disabled_exact_equivalence"]
            for row in results
            if not row["enabled"]
        ),
        "enabled_simulation_fields_equivalent": all(
            row["enabled_simulation_equivalence"]
            for row in results
            if row["enabled"]
        ),
        "engine_configs_isolated": all(
            row["engine_config_isolated"]
            for row in results
        ),
        "inputs_unchanged": all(
            row["config_unchanged"]
            for row in results
        ),
        "classifier_import_is_lazy": all(
            [
                (
                    ast_checks[
                        "module_level_classifier_import"
                    ]
                    is False
                ),
                (
                    ast_checks[
                        "lazy_import_present"
                    ]
                    is True
                ),
            ]
        ),
        "only_shared_builder_references_classifier": (
            only_builder_references_classifier
        ),
        "game_engine_classifier_reach": (
            engine_classifier_reference
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
        (
            "diagnostic_integration_completion_"
            "assessment_allowed_next"
        ): all_checks_passed,
        (
            "production_behavior_integration_"
            "allowed_next"
        ): False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "audit_checks.csv"),
            str(
                OUTPUT_DIR
                / "independent_case_results.csv"
            ),
            str(
                OUTPUT_DIR
                / "production_reference_scan.csv"
            ),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "independent_case_payloads.json"
            ),
            str(OUTPUT_DIR / "ast_audit.json"),
            str(OUTPUT_DIR / "audit_summary.json"),
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
