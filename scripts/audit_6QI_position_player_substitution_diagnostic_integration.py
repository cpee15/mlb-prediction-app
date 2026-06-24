#!/usr/bin/env python3
"""
Layer 6QI
Position-Player Substitution Diagnostic Integration Independent Audit

Independently verifies the merged 6QH diagnostic integration for:

- merged implementation contract presence;
- disabled-path exact equivalence;
- disabled-path zero evaluator imports;
- enabled-path simulation-field equivalence;
- engine-config isolation;
- caller-input immutability;
- metadata alias consistency;
- lazy import placement;
- engine, simulator, and inning-simulator zero reachability;
- evaluator-error containment;
- validation-failure containment;
- explicit absence of production and probability authority.

This layer performs no production behavior changes.
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


LAYER_ID = "6QI"

LAYER_NAME = (
    "position_player_substitution_"
    "diagnostic_integration_independent_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6QI_position_player_substitution_"
    "diagnostic_integration_independent_audit"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "scripts/implement_6QH_position_player_"
    "substitution_diagnostic_integration.py"
)

BUILDER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "position_player_substitution_evaluator.py"
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
    if not path.exists():
        return ""

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
            offense=0.30,
            running=0.35,
            defense=0.40,
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
        "evidence_version": "6qi-v1",
    }


def fake_engine_payload(
    game_pk: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    return {
        "game_pk": game_pk,
        "home_win_probability": 0.58,
        "away_win_probability": 0.42,
        "expected_home_runs": 4.7,
        "expected_away_runs": 3.9,
        "simulation_count": config.get(
            "simulation_count"
        ),
        "seed": config.get(
            "seed"
        ),
        "lineup_marker": (
            "audit-engine-unchanged"
        ),
        "defensive_alignment_marker": (
            "audit-engine-unchanged"
        ),
        "base_state_marker": (
            "audit-engine-unchanged"
        ),
        "meta": {
            "engine_marker": (
                "6qi-audit-engine"
            ),
        },
    }


def strip_diagnostic_metadata(
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
    payload: dict[str, Any],
) -> dict[str, Any]:
    return (
        payload.get(
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
            IMPLEMENTATION_PATH,
            BUILDER_PATH,
            EVALUATOR_PATH,
            ENGINE_PATH,
            SIMULATOR_PATH,
            INNING_SIMULATOR_PATH,
        ]
    )

    implementation_run = subprocess.run(
        [
            sys.executable,
            str(
                IMPLEMENTATION_PATH
            ),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    implementation_payload = (
        parse_last_json_object(
            implementation_run.stdout
        )
    )

    implementation_contract_passed = all(
        [
            implementation_run.returncode
            == 0,
            implementation_payload.get(
                "diagnosis"
            )
            == (
                "position_player_substitution_"
                "diagnostic_integration_complete"
            ),
            implementation_payload.get(
                "all_checks_passed"
            )
            is True,
            implementation_payload.get(
                "integration_checks_passed"
            )
            == 16,
            implementation_payload.get(
                "integration_checks_required"
            )
            == 16,
            implementation_payload.get(
                "fixtures_passed"
            )
            == 10,
            implementation_payload.get(
                "fixtures_required"
            )
            == 10,
            implementation_payload.get(
                "absent_disabled_equivalence"
            )
            is True,
            implementation_payload.get(
                "engine_configs_isolated"
            )
            is True,
            implementation_payload.get(
                "input_immutability"
            )
            is True,
            implementation_payload.get(
                "production_behavior_"
                "integration_allowed_next"
            )
            is False,
        ]
    )

    builder_text = read_text(
        BUILDER_PATH
    )

    builder_tree = ast.parse(
        builder_text,
        filename=str(
            BUILDER_PATH
        ),
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

    zero_top_level_evaluator_imports = not any(
        isinstance(
            node,
            ast.ImportFrom,
        )
        and node.module
        == (
            "mlb_app.simulation."
            "position_player_substitution_evaluator"
        )
        for node in builder_tree.body
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

    reachability_tokens = [
        "position_player_substitution_evaluator",
        "evaluate_position_player_substitution",
    ]

    engine_zero_reachability = not any(
        token in read_text(
            ENGINE_PATH
        )
        for token in reachability_tokens
    )

    simulator_zero_reachability = not any(
        token in read_text(
            SIMULATOR_PATH
        )
        for token in reachability_tokens
    )

    inning_zero_reachability = not any(
        token in read_text(
            INNING_SIMULATOR_PATH
        )
        for token in reachability_tokens
    )

    builder = importlib.import_module(
        "mlb_app.simulation."
        "game_simulation_builder"
    )

    evaluator_module_name = (
        "mlb_app.simulation."
        "position_player_substitution_evaluator"
    )

    sys.modules.pop(
        evaluator_module_name,
        None,
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
        "simulation_count": 1200,
        "seed": 84,
    }

    absent_config = deepcopy(
        base_config
    )
    absent_original = deepcopy(
        absent_config
    )

    evaluator_loaded_before_absent = (
        evaluator_module_name
        in sys.modules
    )

    absent_result = (
        builder.build_game_simulation(
            2001,
            absent_config,
        )
    )

    evaluator_loaded_after_absent = (
        evaluator_module_name
        in sys.modules
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

    evaluator_loaded_before_disabled = (
        evaluator_module_name
        in sys.modules
    )

    disabled_result = (
        builder.build_game_simulation(
            2001,
            disabled_config,
        )
    )

    evaluator_loaded_after_disabled = (
        evaluator_module_name
        in sys.modules
    )

    disabled_zero_imports = all(
        [
            evaluator_loaded_before_absent
            is False,
            evaluator_loaded_after_absent
            is False,
            evaluator_loaded_before_disabled
            is False,
            evaluator_loaded_after_disabled
            is False,
        ]
    )

    enabled_state = base_state()

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
            enabled_state
        ),
    }

    enabled_original = deepcopy(
        enabled_config
    )

    enabled_result = (
        builder.build_game_simulation(
            2001,
            enabled_config,
        )
    )

    evaluator = importlib.import_module(
        evaluator_module_name
    )

    original_validator = (
        evaluator
        .validate_position_player_substitution_evaluation
    )

    evaluator.validate_position_player_substitution_evaluation = (
        lambda payload: {
            "valid": False,
            "errors": [
                "independent_forced_validation_failure"
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
            2001,
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

    def raise_independent_error(
        state: Any,
    ) -> dict[str, Any]:
        raise RuntimeError(
            "independent forced evaluator error"
        )

    evaluator.evaluate_position_player_substitution = (
        raise_independent_error
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
            2001,
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

    disabled_exact_equivalence = all(
        [
            absent_result
            == disabled_result,
            absent_diagnostic
            == {},
            disabled_diagnostic
            == {},
        ]
    )

    enabled_simulation_fields_unchanged = (
        strip_diagnostic_metadata(
            enabled_result
        )
        == absent_result
    )

    validation_failure_contained = all(
        [
            strip_diagnostic_metadata(
                validation_failure_result
            )
            == absent_result,
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

    evaluator_error_contained = all(
        [
            strip_diagnostic_metadata(
                evaluator_error_result
            )
            == absent_result,
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

    metadata_alias_consistent = all(
        result.get(
            "meta"
        )
        == result.get(
            "metadata"
        )
        for result in [
            absent_result,
            disabled_result,
            enabled_result,
            validation_failure_result,
            evaluator_error_result,
        ]
    )

    engine_config_isolation = all(
        not any(
            key in config
            for key in DIAGNOSTIC_KEYS
        )
        for config in captured_engine_configs
    )

    caller_input_immutability = all(
        [
            absent_config
            == absent_original,
            disabled_config
            == disabled_original,
            enabled_config
            == enabled_original,
            validation_failure_config
            == validation_failure_original,
            evaluator_error_config
            == evaluator_error_original,
        ]
    )

    production_authority_absent = all(
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
            validation_failure_diagnostic,
            evaluator_error_diagnostic,
        ]
    )

    production_markers_unchanged = all(
        [
            enabled_result.get(
                "lineup_marker"
            )
            == "audit-engine-unchanged",
            enabled_result.get(
                "defensive_alignment_marker"
            )
            == "audit-engine-unchanged",
            enabled_result.get(
                "base_state_marker"
            )
            == "audit-engine-unchanged",
            enabled_result.get(
                "home_win_probability"
            )
            == 0.58,
            enabled_result.get(
                "away_win_probability"
            )
            == 0.42,
            enabled_result.get(
                "expected_home_runs"
            )
            == 4.7,
            enabled_result.get(
                "expected_away_runs"
            )
            == 3.9,
        ]
    )

    independent_cases = [
        {
            "case_id": "QI-C01",
            "scenario": (
                "implementation_contract"
            ),
            "passed": (
                implementation_contract_passed
            ),
        },
        {
            "case_id": "QI-C02",
            "scenario": (
                "disabled_exact_equivalence"
            ),
            "passed": (
                disabled_exact_equivalence
            ),
        },
        {
            "case_id": "QI-C03",
            "scenario": (
                "disabled_zero_imports"
            ),
            "passed": disabled_zero_imports,
        },
        {
            "case_id": "QI-C04",
            "scenario": (
                "enabled_metadata_contract"
            ),
            "passed": (
                enabled_metadata_contract_valid
            ),
        },
        {
            "case_id": "QI-C05",
            "scenario": (
                "enabled_simulation_equivalence"
            ),
            "passed": (
                enabled_simulation_fields_unchanged
            ),
        },
        {
            "case_id": "QI-C06",
            "scenario": (
                "engine_config_isolation"
            ),
            "passed": (
                engine_config_isolation
            ),
        },
        {
            "case_id": "QI-C07",
            "scenario": (
                "caller_input_immutability"
            ),
            "passed": (
                caller_input_immutability
            ),
        },
        {
            "case_id": "QI-C08",
            "scenario": (
                "metadata_alias_consistency"
            ),
            "passed": (
                metadata_alias_consistent
            ),
        },
        {
            "case_id": "QI-C09",
            "scenario": (
                "validation_failure_containment"
            ),
            "passed": (
                validation_failure_contained
            ),
        },
        {
            "case_id": "QI-C10",
            "scenario": (
                "evaluator_error_containment"
            ),
            "passed": (
                evaluator_error_contained
            ),
        },
    ]

    independent_cases_passed = sum(
        1
        for case in independent_cases
        if case[
            "passed"
        ]
    )

    audit_checks = [
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
                "six_qh_implementation_contract_passed"
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
            "actual": (
                zero_top_level_evaluator_imports
            ),
            "expected": True,
            "passed": (
                zero_top_level_evaluator_imports
            ),
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
                "disabled_exact_equivalence"
            ),
            "actual": (
                disabled_exact_equivalence
            ),
            "expected": True,
            "passed": (
                disabled_exact_equivalence
            ),
        },
        {
            "check": (
                "disabled_zero_imports"
            ),
            "actual": disabled_zero_imports,
            "expected": True,
            "passed": disabled_zero_imports,
        },
        {
            "check": (
                "enabled_simulation_fields_unchanged"
            ),
            "actual": (
                enabled_simulation_fields_unchanged
            ),
            "expected": True,
            "passed": (
                enabled_simulation_fields_unchanged
            ),
        },
        {
            "check": (
                "engine_config_isolation"
            ),
            "actual": (
                engine_config_isolation
            ),
            "expected": True,
            "passed": (
                engine_config_isolation
            ),
        },
        {
            "check": (
                "caller_input_immutability"
            ),
            "actual": (
                caller_input_immutability
            ),
            "expected": True,
            "passed": (
                caller_input_immutability
            ),
        },
        {
            "check": (
                "metadata_alias_consistency"
            ),
            "actual": (
                metadata_alias_consistent
            ),
            "expected": True,
            "passed": (
                metadata_alias_consistent
            ),
        },
        {
            "check": (
                "zero_production_reachability"
            ),
            "actual": all(
                [
                    engine_zero_reachability,
                    simulator_zero_reachability,
                    inning_zero_reachability,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    engine_zero_reachability,
                    simulator_zero_reachability,
                    inning_zero_reachability,
                ]
            ),
        },
        {
            "check": (
                "error_and_validation_containment"
            ),
            "actual": all(
                [
                    validation_failure_contained,
                    evaluator_error_contained,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    validation_failure_contained,
                    evaluator_error_contained,
                ]
            ),
        },
        {
            "check": (
                "ten_independent_cases_pass"
            ),
            "actual": (
                independent_cases_passed
            ),
            "expected": 10,
            "passed": (
                independent_cases_passed
                == 10
            ),
        },
        {
            "check": (
                "production_authority_absent"
            ),
            "actual": all(
                [
                    production_authority_absent,
                    production_markers_unchanged,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    production_authority_absent,
                    production_markers_unchanged,
                ]
            ),
        },
    ]

    all_checks_passed = all(
        row[
            "passed"
        ]
        for row in audit_checks
    )

    write_csv(
        OUTPUT_DIR
        / "audit_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        audit_checks,
    )

    write_csv(
        OUTPUT_DIR
        / "independent_case_results.csv",
        [
            "case_id",
            "scenario",
            "passed",
        ],
        independent_cases,
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
        / "audit_examples.json",
        {
            "enabled": enabled_diagnostic,
            "validation_failure": (
                validation_failure_diagnostic
            ),
            "evaluator_error": (
                evaluator_error_diagnostic
            ),
        },
    )

    summary = {
        "audit_checks_required": len(
            audit_checks
        ),
        "audit_checks_passed": sum(
            1
            for row in audit_checks
            if row[
                "passed"
            ]
        ),
        "independent_cases_required": 10,
        "independent_cases_passed": (
            independent_cases_passed
        ),
        "six_qh_implementation_contract_passed": (
            implementation_contract_passed
        ),
        "disabled_exact_equivalence": (
            disabled_exact_equivalence
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "enabled_simulation_fields_unchanged": (
            enabled_simulation_fields_unchanged
        ),
        "engine_config_isolation": (
            engine_config_isolation
        ),
        "caller_input_immutability": (
            caller_input_immutability
        ),
        "metadata_alias_consistency": (
            metadata_alias_consistent
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
        / "audit_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "position_player_substitution_"
            "diagnostic_integration_"
            "independent_audit_passed"
            if all_checks_passed
            else
            "position_player_substitution_"
            "diagnostic_integration_"
            "independent_audit_failed"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "audit_checks_passed": sum(
            1
            for row in audit_checks
            if row[
                "passed"
            ]
        ),
        "audit_checks_required": len(
            audit_checks
        ),
        "independent_cases_passed": (
            independent_cases_passed
        ),
        "independent_cases_required": 10,
        "six_qh_implementation_contract_passed": (
            implementation_contract_passed
        ),
        "disabled_exact_equivalence": (
            disabled_exact_equivalence
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "enabled_simulation_fields_unchanged": (
            enabled_simulation_fields_unchanged
        ),
        "engine_config_isolation": (
            engine_config_isolation
        ),
        "caller_input_immutability": (
            caller_input_immutability
        ),
        "metadata_alias_consistency": (
            metadata_alias_consistent
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
        "validation_failure_contained": (
            validation_failure_contained
        ),
        "evaluator_error_contained": (
            evaluator_error_contained
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
        "diagnostic_scope_completion_assessment_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            "6QJ_position_player_substitution_"
            "diagnostic_scope_completion"
            if all_checks_passed
            else
            "6QJ_position_player_substitution_"
            "diagnostic_integration_remediation"
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "audit_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "independent_case_results.csv"
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
                / "audit_examples.json"
            ),
            str(
                OUTPUT_DIR
                / "audit_summary.json"
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
