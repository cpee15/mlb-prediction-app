#!/usr/bin/env python3
"""
Layer 6QA
Stolen-Base and Pickoff State Diagnostic Integration Implementation

Validates disabled-by-default, metadata-only integration of the pure
stolen-base and pickoff evaluator through the shared simulation builder.

No production baserunning, base/out-state, simulation, or probability
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


LAYER_ID = "6QA"

LAYER_NAME = (
    "stolen_base_and_pickoff_state_"
    "diagnostic_integration_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6QA_stolen_base_and_pickoff_state_"
    "diagnostic_integration_implementation"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6PZ_stolen_base_and_"
    "pickoff_state_diagnostic_integration.py"
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
    "stolen_base_pickoff_evaluator.py"
)

DIAGNOSTIC_KEYS = {
    "stolen_base_pickoff_diagnostics_enabled",
    "stolen_base_pickoff_diagnostics_version",
    "stolen_base_pickoff_state",
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


def read_text(
    path: Path,
) -> str:
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

    for index in reversed(
        positions
    ):
        try:
            payload = json.loads(
                text[index:].strip()
            )
        except json.JSONDecodeError:
            continue

        if isinstance(
            payload,
            dict,
        ):
            return payload

    return {}


def static_plan_contract_passes() -> bool:
    if not PLAN_PATH.exists():
        return False

    tree = ast.parse(
        read_text(
            PLAN_PATH
        ),
        filename=str(
            PLAN_PATH
        ),
    )

    strings = {
        node.value
        for node in ast.walk(
            tree
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

    return all(
        [
            (
                "stolen_base_and_pickoff_state_"
                "diagnostic_integration_plan_complete"
            )
            in strings,
            (
                "6QA_stolen_base_and_pickoff_state_"
                "diagnostic_integration_implementation"
            )
            in strings,
            (
                "metadata_only_diagnostic_"
                "implementation_allowed_next"
            )
            in strings,
            (
                "production_behavior_"
                "integration_allowed_next"
            )
            in strings,
        ]
    )


def participant(
    participant_id: str,
    *,
    kind: str,
    evidence_complete: bool = True,
    strength: float = 0.50,
) -> dict[str, Any]:
    if kind == "runner":
        return {
            "runner_id": participant_id,
            "speed_score": strength,
            "attempt_rate": strength,
            "success_rate": strength,
            "lead_quality": strength,
            "fatigue_index": 0.10,
            "injury_limit_flag": False,
            "evidence_complete": (
                evidence_complete
            ),
        }

    if kind == "pitcher":
        return {
            "pitcher_id": participant_id,
            "throws": "R",
            "hold_score": strength,
            "delivery_time_score": (
                strength
            ),
            "pickoff_attempt_rate": (
                strength
            ),
            "pickoff_success_rate": (
                strength * 0.10
            ),
            "evidence_complete": (
                evidence_complete
            ),
        }

    return {
        "catcher_id": participant_id,
        "throws": "R",
        "throwing_score": strength,
        "pop_time_score": strength,
        "caught_stealing_rate": (
            strength
        ),
        "evidence_complete": (
            evidence_complete
        ),
    }


def base_state() -> dict[str, Any]:
    return {
        "inning": 5,
        "half": "top",
        "outs": 1,
        "base_state": {
            "first": True,
            "second": False,
            "third": False,
        },
        "score_margin": 0,
        "runner": participant(
            "RUNNER",
            kind="runner",
            strength=0.80,
        ),
        "origin_base": "first",
        "target_base": "second",
        "pitcher": participant(
            "PITCHER",
            kind="pitcher",
            strength=0.25,
        ),
        "catcher": participant(
            "CATCHER",
            kind="catcher",
            strength=0.25,
        ),
        "batter_id": "BATTER",
        "count": {
            "balls": 1,
            "strikes": 1,
        },
        "disengagements_used": 0,
        "extra_inning_flag": False,
        "evidence_version": "6qa-v1",
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
            config.get(
                "simulation_count"
            )
        ),
        "seed": config.get(
            "seed"
        ),
        "base_state_marker": (
            "engine-unchanged"
        ),
        "meta": {
            "engine_marker": (
                "fake-engine"
            ),
        },
    }


def without_diagnostic_metadata(
    payload: dict[str, Any],
) -> dict[str, Any]:
    cleaned = deepcopy(
        payload
    )

    for key in [
        "meta",
        "metadata",
    ]:
        metadata = deepcopy(
            cleaned.get(
                key
            )
            or {}
        )

        metadata.pop(
            "stolen_base_pickoff_diagnostics",
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
            "stolen_base_pickoff_diagnostics",
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
        "stolen_base_pickoff_evaluator"
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
        "_attach_stolen_base_pickoff_diagnostics"
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
                "stolen_base_pickoff_evaluator"
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
            "stolen_base_pickoff_evaluator"
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
        "stolen_base_pickoff_evaluator",
        (
            "evaluate_stolen_base_"
            "and_pickoff_state"
        ),
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
        ._attach_stolen_base_pickoff_diagnostics(
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
                "stolen_base_pickoff_evaluator'; "
                "before=(name in sys.modules); "
                "p={'meta': {'x': 1}}; "
                "r=b._attach_stolen_base_pickoff_diagnostics"
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
            deepcopy(
                config
            )
        )

        return fake_engine_payload(
            game_pk,
            config,
        )

    builder._load_sandbox_engine = (
        lambda: fake_engine
    )

    base_config = {
        "simulation_count": 1000,
        "seed": 42,
    }

    absent_config_original = deepcopy(
        base_config
    )

    absent_result = (
        builder.build_game_simulation(
            123,
            base_config,
        )
    )

    absent_equivalence = all(
        [
            base_config
            == absent_config_original,
            "stolen_base_pickoff_diagnostics"
            not in absent_result.get(
                "meta",
                {},
            ),
            "stolen_base_pickoff_diagnostics"
            not in absent_result.get(
                "metadata",
                {},
            ),
        ]
    )

    disabled_config = {
        **base_config,
        (
            "stolen_base_pickoff_"
            "diagnostics_enabled"
        ): False,
        (
            "stolen_base_pickoff_"
            "diagnostics_version"
        ): (
            "disabled-version"
        ),
        (
            "stolen_base_pickoff_state"
        ): base_state(),
    }

    disabled_config_original = deepcopy(
        disabled_config
    )

    disabled_builder_result = (
        builder.build_game_simulation(
            124,
            disabled_config,
        )
    )

    explicit_disabled_equivalence = all(
        [
            disabled_config
            == disabled_config_original,
            "stolen_base_pickoff_diagnostics"
            not in disabled_builder_result.get(
                "meta",
                {},
            ),
            "stolen_base_pickoff_diagnostics"
            not in disabled_builder_result.get(
                "metadata",
                {},
            ),
        ]
    )

    complete_state = base_state()

    complete_config = {
        **base_config,
        (
            "stolen_base_pickoff_"
            "diagnostics_enabled"
        ): True,
        (
            "stolen_base_pickoff_"
            "diagnostics_version"
        ): (
            "stolen-base-pickoff-"
            "diagnostics-v1"
        ),
        (
            "stolen_base_pickoff_state"
        ): complete_state,
    }

    complete_config_original = deepcopy(
        complete_config
    )

    complete_result = (
        builder.build_game_simulation(
            125,
            complete_config,
        )
    )

    complete_diag = diagnostic_payload(
        complete_result
    )

    complete_metadata_valid = all(
        [
            set(
                complete_diag
            )
            == EXPECTED_METADATA_FIELDS,
            complete_diag.get(
                "status"
            )
            == "evaluated",
            complete_diag.get(
                "enabled"
            )
            is True,
            complete_diag.get(
                "validation",
                {},
            ).get(
                "valid"
            )
            is True,
            complete_diag.get(
                "behavioral_effect"
            )
            == "none",
            complete_diag.get(
                "canonical_probability_"
                "authority_changed"
            )
            is False,
            complete_diag.get(
                "production_activation"
            )
            is False,
            complete_config
            == complete_config_original,
        ]
    )

    partial_state = base_state()
    partial_state[
        "runner"
    ][
        "evidence_complete"
    ] = False

    partial_config = {
        **base_config,
        (
            "stolen_base_pickoff_"
            "diagnostics_enabled"
        ): True,
        (
            "stolen_base_pickoff_state"
        ): partial_state,
    }

    partial_result = (
        builder.build_game_simulation(
            126,
            partial_config,
        )
    )

    partial_diag = diagnostic_payload(
        partial_result
    )

    partial_metadata_valid = all(
        [
            partial_diag.get(
                "status"
            )
            == "evaluated",
            partial_diag.get(
                "evaluation",
                {},
            ).get(
                "state_completeness"
            )
            == "partial",
            partial_diag.get(
                "evaluation",
                {},
            ).get(
                "fallback_used"
            )
            is True,
        ]
    )

    invalid_state = base_state()
    invalid_state.pop(
        "base_state"
    )

    invalid_config = {
        **base_config,
        (
            "stolen_base_pickoff_"
            "diagnostics_enabled"
        ): True,
        (
            "stolen_base_pickoff_state"
        ): invalid_state,
    }

    invalid_result = (
        builder.build_game_simulation(
            127,
            invalid_config,
        )
    )

    invalid_diag = diagnostic_payload(
        invalid_result
    )

    invalid_metadata_valid = all(
        [
            invalid_diag.get(
                "status"
            )
            == "evaluated",
            invalid_diag.get(
                "evaluation",
                {},
            ).get(
                "state_completeness"
            )
            == "invalid",
            invalid_diag.get(
                "evaluation",
                {},
            ).get(
                "production_activation"
            )
            is False,
        ]
    )

    original_validator = (
        evaluator
        .validate_stolen_base_and_pickoff_evaluation
    )

    evaluator.validate_stolen_base_and_pickoff_evaluation = (
        lambda evaluation: {
            "valid": False,
            "errors": [
                "forced_validation_failure",
            ],
        }
    )

    validation_failure_result = (
        builder.build_game_simulation(
            128,
            complete_config,
        )
    )

    validation_failure_diag = (
        diagnostic_payload(
            validation_failure_result
        )
    )

    validation_failure_valid = all(
        [
            validation_failure_diag.get(
                "status"
            )
            == "validation_failed",
            validation_failure_diag.get(
                "validation",
                {},
            ).get(
                "valid"
            )
            is False,
            validation_failure_result.get(
                "base_state_marker"
            )
            == "engine-unchanged",
        ]
    )

    evaluator.validate_stolen_base_and_pickoff_evaluation = (
        original_validator
    )

    original_evaluate = (
        evaluator
        .evaluate_stolen_base_and_pickoff_state
    )

    def raise_evaluator_error(
        state: Any,
    ) -> dict[str, Any]:
        raise RuntimeError(
            "forced evaluator error"
        )

    evaluator.evaluate_stolen_base_and_pickoff_state = (
        raise_evaluator_error
    )

    error_result = (
        builder.build_game_simulation(
            129,
            complete_config,
        )
    )

    error_diag = diagnostic_payload(
        error_result
    )

    evaluator_error_valid = all(
        [
            error_diag.get(
                "status"
            )
            == "error",
            error_diag.get(
                "error",
                {},
            ).get(
                "type"
            )
            == "RuntimeError",
            error_result.get(
                "base_state_marker"
            )
            == "engine-unchanged",
        ]
    )

    evaluator.evaluate_stolen_base_and_pickoff_state = (
        original_evaluate
    )

    builder._load_sandbox_engine = (
        original_loader
    )

    engine_config_isolation = all(
        not (
            DIAGNOSTIC_KEYS
            & set(
                captured
            )
        )
        for captured in captured_configs
    )

    enabled_without_diag = (
        without_diagnostic_metadata(
            complete_result
        )
    )

    expected_engine_payload = (
        builder._normalize_metadata(
            fake_engine_payload(
                125,
                base_config,
            ),
            game_pk=125,
            config=base_config,
        )
    )

    enabled_simulation_fields_unchanged = (
        enabled_without_diag
        == expected_engine_payload
    )

    fixtures = [
        {
            "fixture_id": "QA-F01",
            "scenario": (
                "config_key_absent"
            ),
            "passed": absent_equivalence,
        },
        {
            "fixture_id": "QA-F02",
            "scenario": (
                "config_explicitly_disabled"
            ),
            "passed": (
                explicit_disabled_equivalence
            ),
        },
        {
            "fixture_id": "QA-F03",
            "scenario": (
                "enabled_complete_state"
            ),
            "passed": (
                complete_metadata_valid
            ),
        },
        {
            "fixture_id": "QA-F04",
            "scenario": (
                "enabled_partial_state"
            ),
            "passed": (
                partial_metadata_valid
            ),
        },
        {
            "fixture_id": "QA-F05",
            "scenario": (
                "enabled_invalid_state"
            ),
            "passed": (
                invalid_metadata_valid
            ),
        },
        {
            "fixture_id": "QA-F06",
            "scenario": (
                "validation_failure"
            ),
            "passed": (
                validation_failure_valid
            ),
        },
        {
            "fixture_id": "QA-F07",
            "scenario": (
                "evaluator_error"
            ),
            "passed": (
                evaluator_error_valid
            ),
        },
        {
            "fixture_id": "QA-F08",
            "scenario": (
                "engine_config_isolation"
            ),
            "passed": (
                engine_config_isolation
            ),
        },
        {
            "fixture_id": "QA-F09",
            "scenario": (
                "input_immutability"
            ),
            "passed": all(
                [
                    complete_config
                    == complete_config_original,
                    disabled_config
                    == disabled_config_original,
                    base_config
                    == absent_config_original,
                ]
            ),
        },
        {
            "fixture_id": "QA-F10",
            "scenario": (
                "production_authority_guard"
            ),
            "passed": all(
                [
                    enabled_simulation_fields_unchanged,
                    complete_result.get(
                        "base_state_marker"
                    )
                    == "engine-unchanged",
                    complete_diag.get(
                        "behavioral_effect"
                    )
                    == "none",
                    complete_diag.get(
                        "production_activation"
                    )
                    is False,
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
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_pz_plan_contract_passed"
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
            "actual": (
                lazy_import_present
            ),
            "expected": True,
            "passed": (
                lazy_import_present
            ),
        },
        {
            "check": (
                "zero_top_level_imports"
            ),
            "actual": (
                zero_top_level_imports
            ),
            "expected": True,
            "passed": (
                zero_top_level_imports
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
            "actual": (
                disabled_zero_imports
            ),
            "expected": True,
            "passed": (
                disabled_zero_imports
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
                "inning_zero_reachability"
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
                "enabled_metadata_contract_valid"
            ),
            "actual": (
                complete_metadata_valid
            ),
            "expected": True,
            "passed": (
                complete_metadata_valid
            ),
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
                "ten_fixtures_pass"
            ),
            "actual": (
                fixtures_passed
            ),
            "expected": 10,
            "passed": (
                fixtures_passed
                == 10
            ),
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
        / "implementation_checks.csv",
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
            "capture_index",
            "config_keys",
            "diagnostic_keys_present",
        ],
        [
            {
                "capture_index": index,
                "config_keys": "|".join(
                    sorted(
                        captured
                    )
                ),
                "diagnostic_keys_present": "|".join(
                    sorted(
                        DIAGNOSTIC_KEYS
                        & set(
                            captured
                        )
                    )
                ),
            }
            for index, captured
            in enumerate(
                captured_configs,
                start=1,
            )
        ],
    )

    write_json(
        OUTPUT_DIR
        / "diagnostic_examples.json",
        {
            "complete": complete_diag,
            "partial": partial_diag,
            "invalid": invalid_diag,
            "validation_failure": (
                validation_failure_diag
            ),
            "evaluator_error": (
                error_diag
            ),
        },
    )

    summary = {
        "implementation_checks_required": len(
            checks
        ),
        "implementation_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "fixtures_required": 10,
        "fixtures_passed": (
            fixtures_passed
        ),
        "six_pz_plan_contract_passed": (
            plan_contract_passed
        ),
        "attachment_function_present": (
            attachment_function_present
        ),
        "lazy_import_present": (
            lazy_import_present
        ),
        "zero_top_level_imports": (
            zero_top_level_imports
        ),
        "diagnostic_keys_stripped": (
            diagnostic_keys_stripped
        ),
        "disabled_exact_equivalence": (
            disabled_exact_equivalence
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "engine_zero_reachability": (
            engine_zero_reachability
        ),
        "simulator_zero_reachability": (
            simulator_zero_reachability
        ),
        "inning_zero_reachability": (
            inning_zero_reachability
        ),
        "engine_config_isolation": (
            engine_config_isolation
        ),
        "enabled_metadata_contract_valid": (
            complete_metadata_valid
        ),
        "enabled_simulation_fields_unchanged": (
            enabled_simulation_fields_unchanged
        ),
        "production_behavior_changed": False,
        "base_out_state_changed": False,
        "simulation_behavior_changed": False,
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR
        / "implementation_summary.json",
        summary,
    )

    recommended_next_layer = (
        "6QB_stolen_base_and_pickoff_state_"
        "diagnostic_integration_independent_audit"
        if all_checks_passed
        else
        "6QB_stolen_base_and_pickoff_state_"
        "diagnostic_integration_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "stolen_base_and_pickoff_state_"
            "diagnostic_integration_"
            "implementation_complete"
            if all_checks_passed
            else
            "stolen_base_and_pickoff_state_"
            "diagnostic_integration_"
            "implementation_incomplete"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "implementation_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "implementation_checks_required": len(
            checks
        ),
        "fixtures_passed": (
            fixtures_passed
        ),
        "fixtures_required": 10,
        "six_pz_plan_contract_passed": (
            plan_contract_passed
        ),
        "attachment_function_present": (
            attachment_function_present
        ),
        "lazy_import_present": (
            lazy_import_present
        ),
        "zero_top_level_imports": (
            zero_top_level_imports
        ),
        "diagnostic_keys_stripped": (
            diagnostic_keys_stripped
        ),
        "disabled_exact_equivalence": (
            disabled_exact_equivalence
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "engine_zero_reachability": (
            engine_zero_reachability
        ),
        "simulator_zero_reachability": (
            simulator_zero_reachability
        ),
        "inning_zero_reachability": (
            inning_zero_reachability
        ),
        "engine_config_isolation": (
            engine_config_isolation
        ),
        "enabled_metadata_contract_valid": (
            complete_metadata_valid
        ),
        "enabled_simulation_fields_unchanged": (
            enabled_simulation_fields_unchanged
        ),
        "production_baserunning_changed": False,
        "base_out_state_changed": False,
        "simulation_behavior_changed": False,
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
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
        (
            "independent_integration_"
            "audit_allowed_next"
        ): all_checks_passed,
        (
            "production_behavior_"
            "integration_allowed_next"
        ): False,
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
                / "engine_config_capture.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "diagnostic_examples.json"
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
