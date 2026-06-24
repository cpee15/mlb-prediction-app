#!/usr/bin/env python3
"""
Layer 6QH
Position-Player Substitution Diagnostic Integration Implementation

Validates disabled-by-default, metadata-only integration of the pure
position-player substitution evaluator through the shared simulation builder.

No production substitution, lineup, defensive-alignment, base/out-state,
simulation, or probability authority is granted.
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


LAYER_ID = "6QH"

LAYER_NAME = (
    "position_player_substitution_"
    "diagnostic_integration_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6QH_position_player_substitution_"
    "diagnostic_integration_implementation"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6QG_position_player_"
    "substitution_diagnostic_integration.py"
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

INNING_SIMULATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "inning_simulator.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "position_player_substitution_evaluator.py"
)

DIAGNOSTIC_KEYS = {
    "position_player_substitution_diagnostics_enabled",
    "position_player_substitution_diagnostics_version",
    "position_player_substitution_state",
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
        json.dumps(
            payload,
            indent=2,
        )
        + "\n",
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

    tree = ast.parse(
        read_text(PLAN_PATH),
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
                "position_player_substitution_"
                "diagnostic_integration_plan_complete"
            )
            in strings,
            (
                "6QH_position_player_substitution_"
                "diagnostic_integration"
            )
            in strings,
            (
                "diagnostic_implementation_allowed_next"
            )
            in strings,
            (
                "production_behavior_"
                "integration_allowed_next"
            )
            in strings,
        ]
    )


def player(
    player_id: str,
    *,
    active: bool = True,
    already_used: bool = False,
    position: str = "LF",
    eligible_positions: list[str] | None = None,
    offense: float = 0.50,
    running: float = 0.50,
    defense: float = 0.50,
    evidence_complete: bool = True,
) -> dict[str, Any]:
    return {
        "player_id": player_id,
        "active": active,
        "already_used": already_used,
        "primary_position": position,
        "eligible_positions": (
            eligible_positions
            if eligible_positions is not None
            else [position]
        ),
        "bats": "R",
        "offense_score": offense,
        "running_score": running,
        "defense_score": defense,
        "evidence_complete": evidence_complete,
    }


def base_state() -> dict[str, Any]:
    return {
        "inning": 8,
        "half": "bottom",
        "outs": 1,
        "score_margin": -1,
        "base_state": {
            "first": None,
            "second": None,
            "third": None,
        },
        "substitution_type": "pinch_hitter",
        "current_player": player(
            "CURRENT",
            offense=0.35,
            running=0.35,
            defense=0.35,
        ),
        "candidate_players": [
            player(
                "BENCH_A",
                offense=0.85,
                running=0.80,
                defense=0.75,
            ),
            player(
                "BENCH_B",
                offense=0.65,
                running=0.60,
                defense=0.70,
            ),
        ],
        "batting_order": [
            f"PLAYER_{index}"
            for index in range(1, 10)
        ],
        "current_lineup_slot": 5,
        "defensive_alignment": {
            "LF": "CURRENT",
        },
        "used_player_ids": [],
        "designated_hitter_active": False,
        "injury_required": False,
        "evidence_version": "6qh-v1",
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
        "simulation_count": config.get(
            "simulation_count"
        ),
        "seed": config.get(
            "seed"
        ),
        "lineup_marker": "engine-unchanged",
        "defensive_alignment_marker": (
            "engine-unchanged"
        ),
        "base_state_marker": "engine-unchanged",
        "meta": {
            "engine_marker": "fake-engine",
        },
    }


def without_diagnostic_metadata(
    payload: dict[str, Any],
) -> dict[str, Any]:
    cleaned = deepcopy(payload)

    for key in [
        "meta",
        "metadata",
    ]:
        metadata = deepcopy(
            cleaned.get(key)
            or {}
        )

        metadata.pop(
            "position_player_substitution_diagnostics",
            None,
        )

        cleaned[key] = metadata

    return cleaned


def diagnostic_payload(
    result: dict[str, Any],
) -> dict[str, Any]:
    return (
        result.get(
            "meta",
            {},
        )
        .get(
            "position_player_substitution_diagnostics",
            {},
        )
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
            INNING_SIMULATOR_PATH,
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
        "position_player_substitution_evaluator"
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
        for node in ast.walk(
            builder_tree
        )
        if isinstance(
            node,
            ast.FunctionDef,
        )
    }

    attachment_node = function_nodes.get(
        "_attach_position_player_substitution_diagnostics"
    )

    attachment_function_present = (
        attachment_node is not None
    )

    lazy_import_present = bool(
        attachment_node is not None
        and any(
            isinstance(
                node,
                ast.ImportFrom,
            )
            and node.module
            == (
                "mlb_app.simulation."
                "position_player_substitution_evaluator"
            )
            for node in ast.walk(
                attachment_node
            )
        )
    )

    top_level_imports = [
        node.module
        for node in builder_tree.body
        if isinstance(
            node,
            ast.ImportFrom,
        )
        and node.module
        == (
            "mlb_app.simulation."
            "position_player_substitution_evaluator"
        )
    ]

    zero_top_level_imports = (
        top_level_imports == []
    )

    builder_constants = {
        node.value
        for node in ast.walk(
            builder_tree
        )
        if isinstance(
            node,
            ast.Constant,
        )
        and isinstance(
            node.value,
            str,
        )
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
    inning_text = read_text(
        INNING_SIMULATOR_PATH
    )

    reachability_tokens = [
        "position_player_substitution_evaluator",
        "evaluate_position_player_substitution",
    ]

    engine_zero_reachability = not any(
        token in engine_text
        for token in reachability_tokens
    )

    simulator_zero_reachability = not any(
        token in simulator_text
        for token in reachability_tokens
    )

    inning_zero_reachability = not any(
        token in inning_text
        for token in reachability_tokens
    )

    captured_engine_configs: list[
        dict[str, Any]
    ] = []

    def fake_engine(
        game_pk: int,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        captured_engine_configs.append(
            deepcopy(config)
        )

        return fake_engine_payload(
            game_pk,
            config,
        )

    original_loader = (
        builder._load_sandbox_engine
    )

    builder._load_sandbox_engine = (
        lambda: fake_engine
    )

    base_config = {
        "simulation_count": 1000,
        "seed": 42,
    }

    absent_config = deepcopy(
        base_config
    )
    absent_original = deepcopy(
        absent_config
    )

    absent_result = (
        builder.build_game_simulation(
            1001,
            absent_config,
        )
    )

    disabled_config = {
        **deepcopy(
            base_config
        ),
        (
            "position_player_substitution_"
            "diagnostics_enabled"
        ): False,
        (
            "position_player_substitution_"
            "diagnostics_version"
        ): (
            "position-player-substitution-"
            "diagnostics-v1"
        ),
        "position_player_substitution_state": (
            base_state()
        ),
    }

    disabled_original = deepcopy(
        disabled_config
    )

    disabled_result = (
        builder.build_game_simulation(
            1001,
            disabled_config,
        )
    )

    complete_state = base_state()

    enabled_config = {
        **deepcopy(
            base_config
        ),
        (
            "position_player_substitution_"
            "diagnostics_enabled"
        ): True,
        (
            "position_player_substitution_"
            "diagnostics_version"
        ): (
            "position-player-substitution-"
            "diagnostics-v1"
        ),
        "position_player_substitution_state": (
            complete_state
        ),
    }

    enabled_original = deepcopy(
        enabled_config
    )

    enabled_result = (
        builder.build_game_simulation(
            1001,
            enabled_config,
        )
    )

    partial_state = base_state()

    partial_state[
        "candidate_players"
    ][
        0
    ][
        "evidence_complete"
    ] = False

    partial_config = {
        **deepcopy(
            base_config
        ),
        (
            "position_player_substitution_"
            "diagnostics_enabled"
        ): True,
        "position_player_substitution_state": (
            partial_state
        ),
    }

    partial_original = deepcopy(
        partial_config
    )

    partial_result = (
        builder.build_game_simulation(
            1001,
            partial_config,
        )
    )

    invalid_state = base_state()

    invalid_state.pop(
        "batting_order"
    )

    invalid_config = {
        **deepcopy(
            base_config
        ),
        (
            "position_player_substitution_"
            "diagnostics_enabled"
        ): True,
        "position_player_substitution_state": (
            invalid_state
        ),
    }

    invalid_original = deepcopy(
        invalid_config
    )

    invalid_result = (
        builder.build_game_simulation(
            1001,
            invalid_config,
        )
    )

    original_validator = (
        evaluator
        .validate_position_player_substitution_evaluation
    )

    evaluator.validate_position_player_substitution_evaluation = (
        lambda payload: {
            "valid": False,
            "errors": [
                "forced_validation_failure"
            ],
        }
    )

    validation_failure_config = {
        **deepcopy(
            base_config
        ),
        (
            "position_player_substitution_"
            "diagnostics_enabled"
        ): True,
        "position_player_substitution_state": (
            base_state()
        ),
    }

    validation_failure_original = deepcopy(
        validation_failure_config
    )

    validation_failure_result = (
        builder.build_game_simulation(
            1001,
            validation_failure_config,
        )
    )

    evaluator.validate_position_player_substitution_evaluation = (
        original_validator
    )

    original_evaluate = (
        evaluator
        .evaluate_position_player_substitution
    )

    def raise_evaluator_error(
        state: Any,
    ) -> dict[str, Any]:
        raise RuntimeError(
            "forced evaluator error"
        )

    evaluator.evaluate_position_player_substitution = (
        raise_evaluator_error
    )

    evaluator_error_config = {
        **deepcopy(
            base_config
        ),
        (
            "position_player_substitution_"
            "diagnostics_enabled"
        ): True,
        "position_player_substitution_state": (
            base_state()
        ),
    }

    evaluator_error_original = deepcopy(
        evaluator_error_config
    )

    evaluator_error_result = (
        builder.build_game_simulation(
            1001,
            evaluator_error_config,
        )
    )

    evaluator.evaluate_position_player_substitution = (
        original_evaluate
    )

    builder._load_sandbox_engine = (
        original_loader
    )

    absent_diagnostic = diagnostic_payload(
        absent_result
    )

    disabled_diagnostic = diagnostic_payload(
        disabled_result
    )

    enabled_diagnostic = diagnostic_payload(
        enabled_result
    )

    partial_diagnostic = diagnostic_payload(
        partial_result
    )

    invalid_diagnostic = diagnostic_payload(
        invalid_result
    )

    validation_failure_diagnostic = (
        diagnostic_payload(
            validation_failure_result
        )
    )

    evaluator_error_diagnostic = (
        diagnostic_payload(
            evaluator_error_result
        )
    )

    absent_disabled_equivalent = (
        absent_result
        == disabled_result
    )

    enabled_simulation_equivalent = (
        without_diagnostic_metadata(
            enabled_result
        )
        == absent_result
    )

    partial_simulation_equivalent = (
        without_diagnostic_metadata(
            partial_result
        )
        == absent_result
    )

    invalid_simulation_equivalent = (
        without_diagnostic_metadata(
            invalid_result
        )
        == absent_result
    )

    validation_failure_simulation_equivalent = (
        without_diagnostic_metadata(
            validation_failure_result
        )
        == absent_result
    )

    evaluator_error_simulation_equivalent = (
        without_diagnostic_metadata(
            evaluator_error_result
        )
        == absent_result
    )

    inputs_unchanged = all(
        [
            absent_config
            == absent_original,
            disabled_config
            == disabled_original,
            enabled_config
            == enabled_original,
            partial_config
            == partial_original,
            invalid_config
            == invalid_original,
            validation_failure_config
            == validation_failure_original,
            evaluator_error_config
            == evaluator_error_original,
        ]
    )

    engine_configs_isolated = all(
        not any(
            key in config
            for key in DIAGNOSTIC_KEYS
        )
        for config in captured_engine_configs
    )

    enabled_metadata_contract_valid = all(
        [
            set(
                enabled_diagnostic
            )
            == EXPECTED_METADATA_FIELDS,
            enabled_diagnostic.get(
                "enabled"
            )
            is True,
            enabled_diagnostic.get(
                "status"
            )
            == "evaluated",
            enabled_diagnostic.get(
                "version"
            )
            == (
                "position-player-substitution-"
                "diagnostics-v1"
            ),
            isinstance(
                enabled_diagnostic.get(
                    "evaluation"
                ),
                dict,
            ),
            enabled_diagnostic.get(
                "validation",
                {},
            ).get(
                "valid"
            )
            is True,
            enabled_diagnostic.get(
                "error"
            )
            is None,
        ]
    )

    partial_metadata_valid = all(
        [
            partial_diagnostic.get(
                "status"
            )
            == "evaluated",
            partial_diagnostic.get(
                "evaluation",
                {},
            ).get(
                "state_completeness"
            )
            == "partial",
            partial_diagnostic.get(
                "evaluation",
                {},
            ).get(
                "fallback_used"
            )
            is True,
        ]
    )

    invalid_metadata_valid = all(
        [
            invalid_diagnostic.get(
                "status"
            )
            == "evaluated",
            invalid_diagnostic.get(
                "evaluation",
                {},
            ).get(
                "state_completeness"
            )
            == "invalid",
            invalid_diagnostic.get(
                "evaluation",
                {},
            ).get(
                "fallback_used"
            )
            is True,
        ]
    )

    validation_failure_metadata_valid = all(
        [
            validation_failure_diagnostic.get(
                "status"
            )
            == "validation_failed",
            validation_failure_diagnostic.get(
                "validation",
                {},
            ).get(
                "valid"
            )
            is False,
            validation_failure_diagnostic.get(
                "error"
            )
            is None,
        ]
    )

    evaluator_error_metadata_valid = all(
        [
            evaluator_error_diagnostic.get(
                "status"
            )
            == "error",
            evaluator_error_diagnostic.get(
                "evaluation"
            )
            is None,
            evaluator_error_diagnostic.get(
                "validation"
            )
            is None,
            evaluator_error_diagnostic.get(
                "error",
                {},
            ).get(
                "type"
            )
            == "RuntimeError",
        ]
    )

    authority_safe = all(
        diagnostic.get(
            "behavioral_effect"
        )
        == "none"
        and diagnostic.get(
            "canonical_probability_authority_changed"
        )
        is False
        and diagnostic.get(
            "production_activation"
        )
        is False
        for diagnostic in [
            enabled_diagnostic,
            partial_diagnostic,
            invalid_diagnostic,
            validation_failure_diagnostic,
            evaluator_error_diagnostic,
        ]
    )

    production_markers_unchanged = all(
        [
            enabled_result.get(
                "lineup_marker"
            )
            == "engine-unchanged",
            enabled_result.get(
                "defensive_alignment_marker"
            )
            == "engine-unchanged",
            enabled_result.get(
                "base_state_marker"
            )
            == "engine-unchanged",
            enabled_result.get(
                "home_win_probability"
            )
            == 0.55,
            enabled_result.get(
                "away_win_probability"
            )
            == 0.45,
            enabled_result.get(
                "expected_home_runs"
            )
            == 4.4,
            enabled_result.get(
                "expected_away_runs"
            )
            == 4.0,
        ]
    )

    fixtures = [
        {
            "fixture_id": "QH-F01",
            "scenario": (
                "config_key_absent"
            ),
            "passed": all(
                [
                    absent_diagnostic
                    == {},
                    absent_config
                    == absent_original,
                ]
            ),
        },
        {
            "fixture_id": "QH-F02",
            "scenario": (
                "config_explicitly_disabled"
            ),
            "passed": all(
                [
                    disabled_diagnostic
                    == {},
                    absent_disabled_equivalent,
                    disabled_config
                    == disabled_original,
                ]
            ),
        },
        {
            "fixture_id": "QH-F03",
            "scenario": (
                "enabled_complete_state"
            ),
            "passed": all(
                [
                    enabled_metadata_contract_valid,
                    enabled_simulation_equivalent,
                ]
            ),
        },
        {
            "fixture_id": "QH-F04",
            "scenario": (
                "enabled_partial_state"
            ),
            "passed": all(
                [
                    partial_metadata_valid,
                    partial_simulation_equivalent,
                ]
            ),
        },
        {
            "fixture_id": "QH-F05",
            "scenario": (
                "enabled_invalid_state"
            ),
            "passed": all(
                [
                    invalid_metadata_valid,
                    invalid_simulation_equivalent,
                ]
            ),
        },
        {
            "fixture_id": "QH-F06",
            "scenario": (
                "validation_failure"
            ),
            "passed": all(
                [
                    validation_failure_metadata_valid,
                    validation_failure_simulation_equivalent,
                ]
            ),
        },
        {
            "fixture_id": "QH-F07",
            "scenario": (
                "evaluator_error"
            ),
            "passed": all(
                [
                    evaluator_error_metadata_valid,
                    evaluator_error_simulation_equivalent,
                ]
            ),
        },
        {
            "fixture_id": "QH-F08",
            "scenario": (
                "engine_config_isolation"
            ),
            "passed": (
                engine_configs_isolated
            ),
        },
        {
            "fixture_id": "QH-F09",
            "scenario": (
                "input_immutability"
            ),
            "passed": inputs_unchanged,
        },
        {
            "fixture_id": "QH-F10",
            "scenario": (
                "production_authority_guard"
            ),
            "passed": all(
                [
                    authority_safe,
                    production_markers_unchanged,
                ]
            ),
        },
    ]

    fixtures_passed = sum(
        1
        for fixture in fixtures
        if fixture[
            "passed"
        ]
    )

    checks = [
        {
            "check": (
                "required_files_exist"
            ),
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_qg_plan_contract_passed"
            ),
            "actual": (
                plan_contract_passed
            ),
            "expected": True,
            "passed": (
                plan_contract_passed
            ),
        },
        {
            "check": (
                "attachment_function_present"
            ),
            "actual": (
                attachment_function_present
            ),
            "expected": True,
            "passed": (
                attachment_function_present
            ),
        },
        {
            "check": (
                "lazy_import_present"
            ),
            "actual": lazy_import_present,
            "expected": True,
            "passed": lazy_import_present,
        },
        {
            "check": (
                "zero_top_level_evaluator_imports"
            ),
            "actual": len(
                top_level_imports
            ),
            "expected": 0,
            "passed": zero_top_level_imports,
        },
        {
            "check": (
                "diagnostic_keys_stripped"
            ),
            "actual": (
                diagnostic_keys_stripped
            ),
            "expected": True,
            "passed": (
                diagnostic_keys_stripped
            ),
        },
        {
            "check": (
                "engine_zero_reachability"
            ),
            "actual": (
                engine_zero_reachability
            ),
            "expected": True,
            "passed": (
                engine_zero_reachability
            ),
        },
        {
            "check": (
                "simulator_zero_reachability"
            ),
            "actual": (
                simulator_zero_reachability
            ),
            "expected": True,
            "passed": (
                simulator_zero_reachability
            ),
        },
        {
            "check": (
                "inning_simulator_zero_reachability"
            ),
            "actual": (
                inning_zero_reachability
            ),
            "expected": True,
            "passed": (
                inning_zero_reachability
            ),
        },
        {
            "check": (
                "absent_and_disabled_equivalence"
            ),
            "actual": (
                absent_disabled_equivalent
            ),
            "expected": True,
            "passed": (
                absent_disabled_equivalent
            ),
        },
        {
            "check": (
                "enabled_metadata_contract_valid"
            ),
            "actual": (
                enabled_metadata_contract_valid
            ),
            "expected": True,
            "passed": (
                enabled_metadata_contract_valid
            ),
        },
        {
            "check": (
                "all_simulation_payloads_preserved"
            ),
            "actual": all(
                [
                    enabled_simulation_equivalent,
                    partial_simulation_equivalent,
                    invalid_simulation_equivalent,
                    validation_failure_simulation_equivalent,
                    evaluator_error_simulation_equivalent,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    enabled_simulation_equivalent,
                    partial_simulation_equivalent,
                    invalid_simulation_equivalent,
                    validation_failure_simulation_equivalent,
                    evaluator_error_simulation_equivalent,
                ]
            ),
        },
        {
            "check": (
                "inputs_unchanged"
            ),
            "actual": inputs_unchanged,
            "expected": True,
            "passed": inputs_unchanged,
        },
        {
            "check": (
                "engine_configs_isolated"
            ),
            "actual": (
                engine_configs_isolated
            ),
            "expected": True,
            "passed": (
                engine_configs_isolated
            ),
        },
        {
            "check": (
                "ten_fixtures_pass"
            ),
            "actual": fixtures_passed,
            "expected": 10,
            "passed": (
                fixtures_passed
                == 10
            ),
        },
        {
            "check": (
                "production_authority_absent"
            ),
            "actual": authority_safe,
            "expected": True,
            "passed": authority_safe,
        },
    ]

    all_checks_passed = all(
        row[
            "passed"
        ]
        for row in checks
    )

    write_csv(
        OUTPUT_DIR
        / "integration_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR
        / "fixture_results.csv",
        [
            "fixture_id",
            "scenario",
            "passed",
        ],
        fixtures,
    )

    write_csv(
        OUTPUT_DIR
        / "engine_config_capture.csv",
        [
            "call_index",
            "diagnostic_key_present",
            "config_json",
        ],
        [
            {
                "call_index": index,
                "diagnostic_key_present": any(
                    key in config
                    for key in DIAGNOSTIC_KEYS
                ),
                "config_json": json.dumps(
                    config,
                    sort_keys=True,
                ),
            }
            for index, config
            in enumerate(
                captured_engine_configs,
                start=1,
            )
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "reachability_scan.csv",
        [
            "component",
            "evaluator_reachable",
        ],
        [
            {
                "component": "engine",
                "evaluator_reachable": (
                    not engine_zero_reachability
                ),
            },
            {
                "component": "simulator",
                "evaluator_reachable": (
                    not simulator_zero_reachability
                ),
            },
            {
                "component": "inning_simulator",
                "evaluator_reachable": (
                    not inning_zero_reachability
                ),
            },
        ],
    )

    write_json(
        OUTPUT_DIR
        / "diagnostic_examples.json",
        {
            "enabled": enabled_diagnostic,
            "partial": partial_diagnostic,
            "invalid": invalid_diagnostic,
            "validation_failure": (
                validation_failure_diagnostic
            ),
            "evaluator_error": (
                evaluator_error_diagnostic
            ),
        },
    )

    summary = {
        "integration_checks_required": len(
            checks
        ),
        "integration_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "fixtures_required": 10,
        "fixtures_passed": fixtures_passed,
        "six_qg_plan_contract_passed": (
            plan_contract_passed
        ),
        "attachment_function_present": (
            attachment_function_present
        ),
        "lazy_import_present": (
            lazy_import_present
        ),
        "top_level_evaluator_import_count": len(
            top_level_imports
        ),
        "diagnostic_keys_stripped": (
            diagnostic_keys_stripped
        ),
        "engine_zero_reachability": (
            engine_zero_reachability
        ),
        "simulator_zero_reachability": (
            simulator_zero_reachability
        ),
        "inning_simulator_zero_reachability": (
            inning_zero_reachability
        ),
        "absent_disabled_equivalence": (
            absent_disabled_equivalent
        ),
        "engine_configs_isolated": (
            engine_configs_isolated
        ),
        "input_immutability": (
            inputs_unchanged
        ),
        "production_substitutions_changed": False,
        "batting_order_changed": False,
        "lineup_slots_changed": False,
        "defensive_alignment_changed": False,
        "designated_hitter_state_changed": False,
        "base_out_state_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR
        / "integration_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "position_player_substitution_"
            "diagnostic_integration_complete"
            if all_checks_passed
            else
            "position_player_substitution_"
            "diagnostic_integration_incomplete"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "integration_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "integration_checks_required": len(
            checks
        ),
        "fixtures_passed": fixtures_passed,
        "fixtures_required": 10,
        "six_qg_plan_contract_passed": (
            plan_contract_passed
        ),
        "attachment_function_present": (
            attachment_function_present
        ),
        "lazy_import_present": (
            lazy_import_present
        ),
        "top_level_evaluator_import_count": len(
            top_level_imports
        ),
        "diagnostic_keys_stripped": (
            diagnostic_keys_stripped
        ),
        "engine_zero_reachability": (
            engine_zero_reachability
        ),
        "simulator_zero_reachability": (
            simulator_zero_reachability
        ),
        "inning_simulator_zero_reachability": (
            inning_zero_reachability
        ),
        "absent_disabled_equivalence": (
            absent_disabled_equivalent
        ),
        "engine_configs_isolated": (
            engine_configs_isolated
        ),
        "input_immutability": (
            inputs_unchanged
        ),
        "production_substitutions_changed": False,
        "batting_order_changed": False,
        "lineup_slots_changed": False,
        "defensive_alignment_changed": False,
        "designated_hitter_state_changed": False,
        "base_out_state_changed": False,
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
            "6QI_position_player_substitution_"
            "diagnostic_integration_independent_audit"
            if all_checks_passed
            else
            "6QI_position_player_substitution_"
            "diagnostic_integration_remediation"
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "integration_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "fixture_results.csv"
            ),
            str(
                OUTPUT_DIR
                / "engine_config_capture.csv"
            ),
            str(
                OUTPUT_DIR
                / "reachability_scan.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "diagnostic_examples.json"
            ),
            str(
                OUTPUT_DIR
                / "integration_summary.json"
            ),
            str(
                OUTPUT_DIR
                / "diagnosis.json"
            ),
        ],
    }

    write_json(
        OUTPUT_DIR
        / "diagnosis.json",
        diagnosis,
    )

    print(
        json.dumps(
            diagnosis,
            indent=2,
        )
    )

    return (
        0
        if all_checks_passed
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
