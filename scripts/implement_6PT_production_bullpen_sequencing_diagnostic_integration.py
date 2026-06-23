#!/usr/bin/env python3
"""
Layer 6PT
Production Bullpen Sequencing Diagnostic Integration Implementation

Validates disabled-by-default, metadata-only integration of the pure
bullpen-sequence evaluator through the shared game simulation builder.

No production pitcher-selection, bullpen-transition, or probability
authority is granted.
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PT"

LAYER_NAME = (
    "production_bullpen_sequencing_"
    "diagnostic_integration_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PT_production_bullpen_"
    "sequencing_diagnostic_integration_implementation"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6PS_production_"
    "bullpen_sequencing_diagnostic_integration.py"
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

SIMULATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulator.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "bullpen_sequence_evaluator.py"
)

DIAGNOSTIC_KEYS = {
    "bullpen_sequence_diagnostics_enabled",
    "bullpen_sequence_diagnostics_version",
    "bullpen_sequence_state",
}

EXPECTED_METADATA_FIELDS = {
    "enabled",
    "status",
    "version",
    "evaluation",
    "validation",
    "error",
    "behavioral_effect",
    "canonical_probability_authority_changed",
    "production_activation",
}


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


def read_text(path: Path) -> str:
    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def parse_last_json_object(
    text: str,
) -> dict[str, Any]:
    positions = [
        index
        for index, character in enumerate(text)
        if character == "{"
    ]

    for index in reversed(positions):
        try:
            payload = json.loads(
                text[index:].strip()
            )
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            return payload

    return {}


def static_plan_contract_passes() -> bool:
    if not PLAN_PATH.exists():
        return False

    text = read_text(PLAN_PATH)

    tree = ast.parse(
        text,
        filename=str(PLAN_PATH),
    )

    strings = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    return all(
        [
            (
                "production_bullpen_sequencing_"
                "diagnostic_integration_plan_complete"
            )
            in strings,
            (
                "6PT_production_bullpen_sequencing_"
                "diagnostic_integration_implementation"
            )
            in strings,
            (
                "metadata_only_diagnostic_"
                "implementation_allowed_next"
            )
            in text,
            (
                "production_behavior_"
                "integration_allowed_next"
            )
            in text,
        ]
    )


def reliever(
    pitcher_id: str,
    role: str,
    *,
    availability: str = "available",
    evidence_complete: bool = True,
) -> dict[str, Any]:
    return {
        "pitcher_id": pitcher_id,
        "role": role,
        "throws": "R",
        "quality_score": 0.20,
        "availability_status": availability,
        "fatigue_index": 0.20,
        "recent_usage_count": 0,
        "back_to_back_flag": False,
        "innings_capacity": 1.0,
        "evidence_complete": evidence_complete,
    }


def base_state() -> dict[str, Any]:
    return {
        "team_id": "TEAM",
        "inning": 8,
        "outs": 0,
        "base_state": {
            "first": False,
            "second": False,
            "third": False,
        },
        "score_margin": 1,
        "leverage_proxy": 0.75,
        "current_pitcher_id": "CURRENT",
        "available_relievers": [
            reliever(
                "CLOSER",
                "closer",
            ),
            reliever(
                "SETUP",
                "setup",
            ),
        ],
        "used_pitcher_ids": [],
        "usage_log": [],
        "bullpen_depletion_index": 0.20,
        "extra_inning_flag": False,
    }


def fake_engine_payload(
    game_pk: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    return {
        "game_pk": game_pk,
        "home_win_probability": 0.55,
        "away_win_probability": 0.45,
        "expected_home_runs": 4.4,
        "expected_away_runs": 4.0,
        "simulation_count": (
            config.get("simulation_count")
        ),
        "seed": config.get("seed"),
        "meta": {
            "engine_marker": "fake-engine",
        },
    }


def without_bullpen_metadata(
    payload: dict[str, Any],
) -> dict[str, Any]:
    cleaned = deepcopy(payload)

    for key in ["meta", "metadata"]:
        metadata = deepcopy(
            cleaned.get(key) or {}
        )

        metadata.pop(
            "bullpen_sequence_diagnostics",
            None,
        )

        cleaned[key] = metadata

    return cleaned


def diagnostic_payload(
    result: dict[str, Any],
) -> dict[str, Any]:
    return (
        result.get("meta", {})
        .get("bullpen_sequence_diagnostics", {})
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files_exist = all(
        path.exists()
        for path in [
            PLAN_PATH,
            BUILDER_PATH,
            ENGINE_PATH,
            SIMULATOR_PATH,
            EVALUATOR_PATH,
        ]
    )

    plan_contract_passed = (
        static_plan_contract_passes()
    )

    builder = importlib.import_module(
        "mlb_app.simulation."
        "game_simulation_builder"
    )

    evaluator = importlib.import_module(
        "mlb_app.simulation."
        "bullpen_sequence_evaluator"
    )

    builder_text = read_text(
        BUILDER_PATH
    )

    builder_tree = ast.parse(
        builder_text,
        filename=str(BUILDER_PATH),
    )

    function_nodes = {
        node.name: node
        for node in ast.walk(builder_tree)
        if isinstance(node, ast.FunctionDef)
    }

    attachment_node = function_nodes.get(
        "_attach_bullpen_sequence_diagnostics"
    )

    attachment_function_present = (
        attachment_node is not None
    )

    lazy_import_present = bool(
        attachment_node is not None
        and any(
            isinstance(node, ast.ImportFrom)
            and node.module
            == (
                "mlb_app.simulation."
                "bullpen_sequence_evaluator"
            )
            for node in ast.walk(
                attachment_node
            )
        )
    )

    top_level_imports = [
        node.module
        for node in builder_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module
        == (
            "mlb_app.simulation."
            "bullpen_sequence_evaluator"
        )
    ]

    zero_top_level_imports = (
        top_level_imports == []
    )

    builder_constants = {
        node.value
        for node in ast.walk(builder_tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    diagnostic_keys_stripped = (
        DIAGNOSTIC_KEYS.issubset(
            builder_constants
        )
    )

    engine_text = read_text(
        ENGINE_PATH
    )

    simulator_text = read_text(
        SIMULATOR_PATH
    )

    engine_zero_reachability = not any(
        token in engine_text
        for token in [
            "bullpen_sequence_evaluator",
            "evaluate_bullpen_sequence",
        ]
    )

    simulator_zero_reachability = not any(
        token in simulator_text
        for token in [
            "bullpen_sequence_evaluator",
            "evaluate_bullpen_sequence",
        ]
    )

    disabled_payload = {
        "meta": {
            "marker": "disabled",
        },
        "value": 1,
    }

    disabled_original = deepcopy(
        disabled_payload
    )

    disabled_result = (
        builder
        ._attach_bullpen_sequence_diagnostics(
            disabled_payload,
            config={},
        )
    )

    disabled_exact_equivalence = all(
        [
            disabled_result
            == disabled_original,
            disabled_result
            is disabled_payload,
        ]
    )

    disabled_import_probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json, sys; "
                "from mlb_app.simulation import "
                "game_simulation_builder as b; "
                "name='mlb_app.simulation."
                "bullpen_sequence_evaluator'; "
                "before=(name in sys.modules); "
                "p={'meta': {'x': 1}}; "
                "r=b._attach_bullpen_sequence_diagnostics"
                "(p, config={}); "
                "after=(name in sys.modules); "
                "print(json.dumps({'before': before, "
                "'after': after, 'equal': "
                "r == {'meta': {'x': 1}}}))"
            ),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    disabled_import_data = (
        parse_last_json_object(
            disabled_import_probe.stdout
        )
    )

    disabled_zero_imports = all(
        [
            disabled_import_probe.returncode
            == 0,
            disabled_import_data.get(
                "before"
            )
            is False,
            disabled_import_data.get(
                "after"
            )
            is False,
            disabled_import_data.get(
                "equal"
            )
            is True,
        ]
    )

    original_loader = (
        builder._load_sandbox_engine
    )

    captured_configs: list[
        dict[str, Any]
    ] = []

    def fake_engine(
        game_pk: int,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        captured_configs.append(
            deepcopy(config)
        )

        return fake_engine_payload(
            game_pk,
            config,
        )

    builder._load_sandbox_engine = (
        lambda: fake_engine
    )

    baseline_config = {
        "simulation_count": 100,
        "seed": 17,
    }

    baseline_result = (
        builder.build_game_simulation(
            123,
            deepcopy(baseline_config),
        )
    )

    disabled_config = {
        **baseline_config,
        "bullpen_sequence_diagnostics_enabled": (
            False
        ),
        "bullpen_sequence_diagnostics_version": (
            "unused-version"
        ),
        "bullpen_sequence_state": (
            base_state()
        ),
    }

    disabled_config_original = deepcopy(
        disabled_config
    )

    disabled_builder_result = (
        builder.build_game_simulation(
            123,
            disabled_config,
        )
    )

    enabled_config = {
        **baseline_config,
        "bullpen_sequence_diagnostics_enabled": (
            True
        ),
        "bullpen_sequence_state": (
            base_state()
        ),
    }

    enabled_config_original = deepcopy(
        enabled_config
    )

    enabled_result = (
        builder.build_game_simulation(
            123,
            enabled_config,
        )
    )

    partial_config = deepcopy(
        enabled_config
    )

    partial_config[
        "bullpen_sequence_state"
    ][
        "available_relievers"
    ][0][
        "evidence_complete"
    ] = False

    partial_result = (
        builder.build_game_simulation(
            123,
            partial_config,
        )
    )

    invalid_config = deepcopy(
        enabled_config
    )

    invalid_config[
        "bullpen_sequence_state"
    ].pop(
        "available_relievers"
    )

    invalid_result = (
        builder.build_game_simulation(
            123,
            invalid_config,
        )
    )

    original_validate = (
        evaluator
        .validate_bullpen_sequence_evaluation
    )

    evaluator.validate_bullpen_sequence_evaluation = (
        lambda payload: {
            "valid": False,
            "errors": ["forced_failure"],
        }
    )

    validation_failure_result = (
        builder.build_game_simulation(
            123,
            deepcopy(enabled_config),
        )
    )

    evaluator.validate_bullpen_sequence_evaluation = (
        original_validate
    )

    original_evaluate = (
        evaluator.evaluate_bullpen_sequence
    )

    def raise_error(
        state: Any,
    ) -> dict[str, Any]:
        raise RuntimeError(
            "forced_evaluator_error"
        )

    evaluator.evaluate_bullpen_sequence = (
        raise_error
    )

    error_result = (
        builder.build_game_simulation(
            123,
            deepcopy(enabled_config),
        )
    )

    evaluator.evaluate_bullpen_sequence = (
        original_evaluate
    )

    builder._load_sandbox_engine = (
        original_loader
    )

    disabled_builder_exact_equivalence = (
        disabled_builder_result
        == baseline_result
    )

    enabled_simulation_equivalence = (
        without_bullpen_metadata(
            enabled_result
        )
        == baseline_result
    )

    engine_configs_isolated = all(
        not (
            set(config)
            & DIAGNOSTIC_KEYS
        )
        for config in captured_configs
    )

    caller_configs_unchanged = all(
        [
            disabled_config
            == disabled_config_original,
            enabled_config
            == enabled_config_original,
        ]
    )

    enabled_metadata = diagnostic_payload(
        enabled_result
    )

    partial_metadata = diagnostic_payload(
        partial_result
    )

    invalid_metadata = diagnostic_payload(
        invalid_result
    )

    validation_metadata = diagnostic_payload(
        validation_failure_result
    )

    error_metadata = diagnostic_payload(
        error_result
    )

    metadata_contract_valid = all(
        [
            set(enabled_metadata)
            == EXPECTED_METADATA_FIELDS,
            enabled_metadata.get("enabled")
            is True,
            enabled_metadata.get("status")
            == "evaluated",
            enabled_metadata.get(
                "behavioral_effect"
            )
            == "none",
            enabled_metadata.get(
                "canonical_probability_"
                "authority_changed"
            )
            is False,
            enabled_metadata.get(
                "production_activation"
            )
            is False,
        ]
    )

    fixtures = [
        {
            "fixture_id": "PT-F01",
            "scenario": "config_key_absent",
            "passed": (
                baseline_result
                == disabled_builder_result
            ),
        },
        {
            "fixture_id": "PT-F02",
            "scenario": (
                "config_explicitly_disabled"
            ),
            "passed": (
                disabled_builder_exact_equivalence
            ),
        },
        {
            "fixture_id": "PT-F03",
            "scenario": (
                "enabled_complete_state"
            ),
            "passed": all(
                [
                    enabled_metadata.get(
                        "status"
                    )
                    == "evaluated",
                    enabled_metadata.get(
                        "evaluation"
                    )
                    is not None,
                ]
            ),
        },
        {
            "fixture_id": "PT-F04",
            "scenario": (
                "enabled_partial_state"
            ),
            "passed": all(
                [
                    partial_metadata.get(
                        "status"
                    )
                    == "evaluated",
                    (
                        partial_metadata
                        .get("evaluation", {})
                        .get(
                            "state_completeness"
                        )
                        == "partial"
                    ),
                ]
            ),
        },
        {
            "fixture_id": "PT-F05",
            "scenario": (
                "enabled_invalid_state"
            ),
            "passed": all(
                [
                    invalid_metadata.get(
                        "status"
                    )
                    == "evaluated",
                    (
                        invalid_metadata
                        .get("evaluation", {})
                        .get(
                            "state_completeness"
                        )
                        == "invalid"
                    ),
                ]
            ),
        },
        {
            "fixture_id": "PT-F06",
            "scenario": "validation_failure",
            "passed": (
                validation_metadata.get(
                    "status"
                )
                == "validation_failed"
            ),
        },
        {
            "fixture_id": "PT-F07",
            "scenario": "evaluator_error",
            "passed": all(
                [
                    error_metadata.get(
                        "status"
                    )
                    == "error",
                    error_metadata.get(
                        "error", {}
                    ).get("type")
                    == "RuntimeError",
                ]
            ),
        },
        {
            "fixture_id": "PT-F08",
            "scenario": (
                "engine_config_isolation"
            ),
            "passed": engine_configs_isolated,
        },
        {
            "fixture_id": "PT-F09",
            "scenario": "input_immutability",
            "passed": caller_configs_unchanged,
        },
        {
            "fixture_id": "PT-F10",
            "scenario": (
                "production_authority_guard"
            ),
            "passed": all(
                metadata.get(
                    "behavioral_effect"
                )
                == "none"
                and metadata.get(
                    "canonical_probability_"
                    "authority_changed"
                )
                is False
                and metadata.get(
                    "production_activation"
                )
                is False
                for metadata in [
                    enabled_metadata,
                    partial_metadata,
                    invalid_metadata,
                    validation_metadata,
                    error_metadata,
                ]
            ),
        },
    ]

    fixtures_passed = sum(
        1
        for row in fixtures
        if row["passed"]
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": "six_ps_plan_contract_passes",
            "actual": plan_contract_passed,
            "expected": True,
            "passed": plan_contract_passed,
        },
        {
            "check": "attachment_function_present",
            "actual": attachment_function_present,
            "expected": True,
            "passed": attachment_function_present,
        },
        {
            "check": "lazy_import_present",
            "actual": lazy_import_present,
            "expected": True,
            "passed": lazy_import_present,
        },
        {
            "check": "zero_top_level_imports",
            "actual": zero_top_level_imports,
            "expected": True,
            "passed": zero_top_level_imports,
        },
        {
            "check": "diagnostic_keys_stripped",
            "actual": diagnostic_keys_stripped,
            "expected": True,
            "passed": diagnostic_keys_stripped,
        },
        {
            "check": "disabled_exact_equivalence",
            "actual": disabled_exact_equivalence,
            "expected": True,
            "passed": disabled_exact_equivalence,
        },
        {
            "check": "disabled_zero_imports",
            "actual": disabled_zero_imports,
            "expected": True,
            "passed": disabled_zero_imports,
        },
        {
            "check": (
                "enabled_simulation_equivalence"
            ),
            "actual": (
                enabled_simulation_equivalence
            ),
            "expected": True,
            "passed": (
                enabled_simulation_equivalence
            ),
        },
        {
            "check": "engine_config_isolated",
            "actual": engine_configs_isolated,
            "expected": True,
            "passed": engine_configs_isolated,
        },
        {
            "check": "caller_configs_unchanged",
            "actual": caller_configs_unchanged,
            "expected": True,
            "passed": caller_configs_unchanged,
        },
        {
            "check": "metadata_contract_valid",
            "actual": metadata_contract_valid,
            "expected": True,
            "passed": metadata_contract_valid,
        },
        {
            "check": "engine_and_simulator_zero_reach",
            "actual": (
                engine_zero_reachability
                and simulator_zero_reachability
            ),
            "expected": True,
            "passed": (
                engine_zero_reachability
                and simulator_zero_reachability
            ),
        },
        {
            "check": "ten_fixtures_pass",
            "actual": fixtures_passed,
            "expected": 10,
            "passed": fixtures_passed == 10,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
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
        OUTPUT_DIR / "fixture_results.csv",
        [
            "fixture_id",
            "scenario",
            "passed",
        ],
        fixtures,
    )

    write_csv(
        OUTPUT_DIR / "captured_engine_configs.csv",
        [
            "index",
            "config_json",
            "diagnostic_keys_present",
        ],
        [
            {
                "index": index,
                "config_json": json.dumps(
                    config,
                    sort_keys=True,
                ),
                "diagnostic_keys_present": bool(
                    set(config)
                    & DIAGNOSTIC_KEYS
                ),
            }
            for index, config in enumerate(
                captured_configs,
                start=1,
            )
        ],
    )

    write_json(
        OUTPUT_DIR / "diagnostic_payloads.json",
        {
            "enabled": enabled_metadata,
            "partial": partial_metadata,
            "invalid": invalid_metadata,
            "validation_failure": (
                validation_metadata
            ),
            "error": error_metadata,
        },
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
        "fixtures_required": 10,
        "fixtures_passed": fixtures_passed,
        "plan_contract_passed": (
            plan_contract_passed
        ),
        "disabled_exact_equivalence": (
            disabled_exact_equivalence
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "enabled_simulation_equivalence": (
            enabled_simulation_equivalence
        ),
        "engine_config_isolated": (
            engine_configs_isolated
        ),
        "caller_configs_unchanged": (
            caller_configs_unchanged
        ),
        "metadata_contract_valid": (
            metadata_contract_valid
        ),
        "engine_zero_reachability": (
            engine_zero_reachability
        ),
        "simulator_zero_reachability": (
            simulator_zero_reachability
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_summary.json",
        summary,
    )

    recommended_next_layer = (
        "6PU_production_bullpen_sequencing_"
        "diagnostic_integration_audit"
        if all_checks_passed
        else
        "6PU_production_bullpen_sequencing_"
        "diagnostic_integration_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "production_bullpen_sequencing_"
            "diagnostic_integration_"
            "implementation_complete"
            if all_checks_passed
            else
            "production_bullpen_sequencing_"
            "diagnostic_integration_"
            "implementation_failed"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "implementation_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "implementation_checks_required": len(
            checks
        ),
        "fixtures_passed": fixtures_passed,
        "fixtures_required": 10,
        "six_ps_plan_contract_passed": (
            plan_contract_passed
        ),
        "disabled_exact_equivalence": (
            disabled_exact_equivalence
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "enabled_simulation_equivalence": (
            enabled_simulation_equivalence
        ),
        "engine_config_isolated": (
            engine_configs_isolated
        ),
        "caller_configs_unchanged": (
            caller_configs_unchanged
        ),
        "metadata_contract_valid": (
            metadata_contract_valid
        ),
        "engine_zero_reachability": (
            engine_zero_reachability
        ),
        "simulator_zero_reachability": (
            simulator_zero_reachability
        ),
        "production_bullpen_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
        "broad_layer6_exit_paused": True,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "independent_integration_audit_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
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
                / "fixture_results.csv"
            ),
            str(
                OUTPUT_DIR
                / "captured_engine_configs.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "diagnostic_payloads.json"
            ),
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

    print(json.dumps(diagnosis, indent=2))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
