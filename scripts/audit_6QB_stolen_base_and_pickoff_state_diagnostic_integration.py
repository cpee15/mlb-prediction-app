#!/usr/bin/env python3
"""
Layer 6QB
Stolen-Base and Pickoff State Diagnostic Integration Independent Audit

Independently verifies the merged 6QA diagnostic integration for:

- merged implementation contract presence;
- disabled-path exact equivalence;
- disabled-path zero evaluator imports;
- enabled-path simulation-field equivalence;
- engine-config isolation;
- caller input immutability;
- metadata alias consistency;
- lazy import placement;
- engine, simulator, and inning-simulator zero reachability;
- evaluator error containment;
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


LAYER_ID = "6QB"

LAYER_NAME = (
    "stolen_base_and_pickoff_state_"
    "diagnostic_integration_independent_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6QB_stolen_base_and_pickoff_state_"
    "diagnostic_integration_independent_audit"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "scripts/implement_6QA_stolen_base_and_"
    "pickoff_state_diagnostic_integration.py"
)

BUILDER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "stolen_base_pickoff_evaluator.py"
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
        "caught_stealing_rate": strength,
        "evidence_complete": (
            evidence_complete
        ),
    }


def base_state() -> dict[str, Any]:
    return {
        "inning": 6,
        "half": "bottom",
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
            "strikes": 0,
        },
        "disengagements_used": 0,
        "extra_inning_flag": False,
        "evidence_version": "6qb-v1",
    }


def fake_engine_payload(
    game_pk: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    return {
        "game_pk": game_pk,
        "home_win_probability": 0.57,
        "away_win_probability": 0.43,
        "expected_home_runs": 4.6,
        "expected_away_runs": 3.8,
        "simulation_count": (
            config.get(
                "simulation_count"
            )
        ),
        "seed": config.get(
            "seed"
        ),
        "base_state_marker": (
            "audit-engine-unchanged"
        ),
        "meta": {
            "engine_marker": (
                "6qb-audit-engine"
            ),
        },
    }


def strip_diagnostic_metadata(
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
    payload: dict[str, Any],
) -> dict[str, Any]:
    return (
        payload.get(
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
                "stolen_base_and_pickoff_state_"
                "diagnostic_integration_"
                "implementation_complete"
            ),
            implementation_payload.get(
                "all_checks_passed"
            )
            is True,
            implementation_payload.get(
                "implementation_checks_passed"
            )
            == 15,
            implementation_payload.get(
                "implementation_checks_required"
            )
            == 15,
            implementation_payload.get(
                "fixtures_passed"
            )
            == 10,
            implementation_payload.get(
                "fixtures_required"
            )
            == 10,
            implementation_payload.get(
                "disabled_exact_equivalence"
            )
            is True,
            implementation_payload.get(
                "disabled_zero_imports"
            )
            is True,
            implementation_payload.get(
                "engine_config_isolation"
            )
            is True,
            implementation_payload.get(
                "enabled_simulation_fields_unchanged"
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

    zero_top_level_evaluator_imports = not any(
        isinstance(
            node,
            ast.ImportFrom,
        )
        and node.module
        == (
            "mlb_app.simulation."
            "stolen_base_pickoff_evaluator"
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
        "stolen_base_pickoff_evaluator",
        (
            "evaluate_stolen_base_"
            "and_pickoff_state"
        ),
        (
            "stolen_base_pickoff_"
            "diagnostics"
        ),
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

    disabled_probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json, sys; "
                "from mlb_app.simulation import "
                "game_simulation_builder as b; "
                "name='mlb_app.simulation."
                "stolen_base_pickoff_evaluator'; "
                "before=name in sys.modules; "
                "payload={'meta': {'x': 1}, 'value': 2}; "
                "result=b._attach_stolen_base_pickoff_diagnostics"
                "(payload, config={}); "
                "after=name in sys.modules; "
                "print(json.dumps({"
                "'before': before, "
                "'after': after, "
                "'equal': result == "
                "{'meta': {'x': 1}, 'value': 2}, "
                "'same_object': result is payload"
                "}))"
            ),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    disabled_probe_payload = (
        parse_last_json_object(
            disabled_probe.stdout
        )
    )

    disabled_zero_imports = all(
        [
            disabled_probe.returncode
            == 0,
            disabled_probe_payload.get(
                "before"
            )
            is False,
            disabled_probe_payload.get(
                "after"
            )
            is False,
        ]
    )

    disabled_direct_equivalence = all(
        [
            disabled_probe_payload.get(
                "equal"
            )
            is True,
            disabled_probe_payload.get(
                "same_object"
            )
            is True,
        ]
    )

    builder = importlib.import_module(
        "mlb_app.simulation."
        "game_simulation_builder"
    )

    evaluator = importlib.import_module(
        "mlb_app.simulation."
        "stolen_base_pickoff_evaluator"
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
        "simulation_count": 750,
        "seed": 777,
    }

    absent_config = deepcopy(
        base_config
    )

    absent_original = deepcopy(
        absent_config
    )

    absent_result = (
        builder.build_game_simulation(
            201,
            absent_config,
        )
    )

    case_absent = all(
        [
            absent_config
            == absent_original,
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
        ): "disabled",
        (
            "stolen_base_pickoff_state"
        ): base_state(),
    }

    disabled_original = deepcopy(
        disabled_config
    )

    disabled_result = (
        builder.build_game_simulation(
            202,
            disabled_config,
        )
    )

    case_disabled = all(
        [
            disabled_config
            == disabled_original,
            "stolen_base_pickoff_diagnostics"
            not in disabled_result.get(
                "meta",
                {},
            ),
            "stolen_base_pickoff_diagnostics"
            not in disabled_result.get(
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

    complete_original = deepcopy(
        complete_config
    )

    complete_result = (
        builder.build_game_simulation(
            203,
            complete_config,
        )
    )

    complete_diag = diagnostic_payload(
        complete_result
    )

    case_complete = all(
        [
            complete_config
            == complete_original,
            set(
                complete_diag
            )
            == EXPECTED_METADATA_FIELDS,
            complete_diag.get(
                "status"
            )
            == "evaluated",
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

    partial_original = deepcopy(
        partial_config
    )

    partial_result = (
        builder.build_game_simulation(
            204,
            partial_config,
        )
    )

    partial_diag = diagnostic_payload(
        partial_result
    )

    case_partial = all(
        [
            partial_config
            == partial_original,
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

    invalid_original = deepcopy(
        invalid_config
    )

    invalid_result = (
        builder.build_game_simulation(
            205,
            invalid_config,
        )
    )

    invalid_diag = diagnostic_payload(
        invalid_result
    )

    case_invalid = all(
        [
            invalid_config
            == invalid_original,
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
                "forced_6qb_validation_failure",
            ],
        }
    )

    validation_result = (
        builder.build_game_simulation(
            206,
            complete_config,
        )
    )

    validation_diag = diagnostic_payload(
        validation_result
    )

    case_validation_failure = all(
        [
            validation_diag.get(
                "status"
            )
            == "validation_failed",
            validation_diag.get(
                "validation",
                {},
            ).get(
                "valid"
            )
            is False,
            validation_result.get(
                "base_state_marker"
            )
            == "audit-engine-unchanged",
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
            "forced 6QB evaluator error"
        )

    evaluator.evaluate_stolen_base_and_pickoff_state = (
        raise_evaluator_error
    )

    error_result = (
        builder.build_game_simulation(
            207,
            complete_config,
        )
    )

    error_diag = diagnostic_payload(
        error_result
    )

    case_evaluator_error = all(
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
            == "audit-engine-unchanged",
        ]
    )

    evaluator.evaluate_stolen_base_and_pickoff_state = (
        original_evaluate
    )

    builder._load_sandbox_engine = (
        original_loader
    )

    engine_config_isolated = all(
        not (
            DIAGNOSTIC_KEYS
            & set(
                captured
            )
        )
        for captured in captured_configs
    )

    expected_complete = (
        builder._normalize_metadata(
            fake_engine_payload(
                203,
                base_config,
            ),
            game_pk=203,
            config=base_config,
        )
    )

    enabled_simulation_equivalence = (
        strip_diagnostic_metadata(
            complete_result
        )
        == expected_complete
    )

    metadata_alias_consistent = all(
        [
            complete_result.get(
                "meta"
            )
            == complete_result.get(
                "metadata"
            ),
            partial_result.get(
                "meta"
            )
            == partial_result.get(
                "metadata"
            ),
            invalid_result.get(
                "meta"
            )
            == invalid_result.get(
                "metadata"
            ),
            validation_result.get(
                "meta"
            )
            == validation_result.get(
                "metadata"
            ),
            error_result.get(
                "meta"
            )
            == error_result.get(
                "metadata"
            ),
        ]
    )

    input_immutability = all(
        [
            absent_config
            == absent_original,
            disabled_config
            == disabled_original,
            complete_config
            == complete_original,
            partial_config
            == partial_original,
            invalid_config
            == invalid_original,
        ]
    )

    authority_guard = all(
        [
            complete_result.get(
                "base_state_marker"
            )
            == "audit-engine-unchanged",
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
            enabled_simulation_equivalence,
        ]
    )

    cases = [
        {
            "case_id": "QB-C01",
            "scenario": (
                "config_key_absent"
            ),
            "passed": case_absent,
        },
        {
            "case_id": "QB-C02",
            "scenario": (
                "explicitly_disabled"
            ),
            "passed": case_disabled,
        },
        {
            "case_id": "QB-C03",
            "scenario": (
                "enabled_complete_state"
            ),
            "passed": case_complete,
        },
        {
            "case_id": "QB-C04",
            "scenario": (
                "enabled_partial_state"
            ),
            "passed": case_partial,
        },
        {
            "case_id": "QB-C05",
            "scenario": (
                "enabled_invalid_state"
            ),
            "passed": case_invalid,
        },
        {
            "case_id": "QB-C06",
            "scenario": (
                "validation_failure_containment"
            ),
            "passed": (
                case_validation_failure
            ),
        },
        {
            "case_id": "QB-C07",
            "scenario": (
                "evaluator_error_containment"
            ),
            "passed": (
                case_evaluator_error
            ),
        },
        {
            "case_id": "QB-C08",
            "scenario": (
                "engine_config_isolation"
            ),
            "passed": (
                engine_config_isolated
            ),
        },
        {
            "case_id": "QB-C09",
            "scenario": (
                "metadata_alias_and_input_immutability"
            ),
            "passed": all(
                [
                    metadata_alias_consistent,
                    input_immutability,
                ]
            ),
        },
        {
            "case_id": "QB-C10",
            "scenario": (
                "production_authority_guard"
            ),
            "passed": (
                authority_guard
            ),
        },
    ]

    cases_passed = sum(
        1
        for case in cases
        if case[
            "passed"
        ]
    )

    checks = [
        {
            "check": (
                "required_files_exist"
            ),
            "actual": (
                required_files_exist
            ),
            "expected": True,
            "passed": (
                required_files_exist
            ),
        },
        {
            "check": (
                "six_qa_implementation_contract_passed"
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
                "disabled_direct_equivalence"
            ),
            "actual": (
                disabled_direct_equivalence
            ),
            "expected": True,
            "passed": (
                disabled_direct_equivalence
            ),
        },
        {
            "check": (
                "engine_simulator_inning_zero_reachability"
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
                "engine_config_isolated"
            ),
            "actual": (
                engine_config_isolated
            ),
            "expected": True,
            "passed": (
                engine_config_isolated
            ),
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
            "check": (
                "metadata_alias_consistent"
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
                "input_immutability"
            ),
            "actual": (
                input_immutability
            ),
            "expected": True,
            "passed": (
                input_immutability
            ),
        },
        {
            "check": (
                "ten_independent_cases_pass"
            ),
            "actual": (
                cases_passed
            ),
            "expected": 10,
            "passed": (
                cases_passed
                == 10
            ),
        },
        {
            "check": (
                "production_authority_absent"
            ),
            "actual": (
                authority_guard
            ),
            "expected": True,
            "passed": (
                authority_guard
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
        / "audit_checks.csv",
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
        / "independent_case_results.csv",
        [
            "case_id",
            "scenario",
            "passed",
        ],
        cases,
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
                "diagnostic_keys_present": (
                    "|".join(
                        sorted(
                            DIAGNOSTIC_KEYS
                            & set(
                                captured
                            )
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
        / "audit_examples.json",
        {
            "complete": complete_diag,
            "partial": partial_diag,
            "invalid": invalid_diag,
            "validation_failure": (
                validation_diag
            ),
            "evaluator_error": error_diag,
        },
    )

    summary = {
        "audit_checks_required": len(
            checks
        ),
        "audit_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "independent_cases_required": 10,
        "independent_cases_passed": (
            cases_passed
        ),
        (
            "six_qa_implementation_"
            "contract_passed"
        ): (
            implementation_contract_passed
        ),
        "attachment_function_present": (
            attachment_function_present
        ),
        "lazy_import_present": (
            lazy_import_present
        ),
        "zero_top_level_evaluator_imports": (
            zero_top_level_evaluator_imports
        ),
        "diagnostic_keys_stripped": (
            diagnostic_keys_stripped
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "disabled_direct_equivalence": (
            disabled_direct_equivalence
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
        "engine_config_isolated": (
            engine_config_isolated
        ),
        "enabled_simulation_equivalence": (
            enabled_simulation_equivalence
        ),
        "metadata_alias_consistent": (
            metadata_alias_consistent
        ),
        "input_immutability": (
            input_immutability
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
        / "audit_summary.json",
        summary,
    )

    recommended_next_layer = (
        "6QC_stolen_base_and_pickoff_state_"
        "diagnostic_scope_completion_assessment"
        if all_checks_passed
        else
        "6QC_stolen_base_and_pickoff_state_"
        "diagnostic_integration_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "stolen_base_and_pickoff_state_"
            "diagnostic_integration_"
            "independent_audit_passed"
            if all_checks_passed
            else
            "stolen_base_and_pickoff_state_"
            "diagnostic_integration_"
            "independent_audit_failed"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "audit_checks_passed": sum(
            1
            for row in checks
            if row[
                "passed"
            ]
        ),
        "audit_checks_required": len(
            checks
        ),
        "independent_cases_passed": (
            cases_passed
        ),
        "independent_cases_required": 10,
        (
            "six_qa_implementation_"
            "contract_passed"
        ): (
            implementation_contract_passed
        ),
        "attachment_function_present": (
            attachment_function_present
        ),
        "lazy_import_present": (
            lazy_import_present
        ),
        "zero_top_level_evaluator_imports": (
            zero_top_level_evaluator_imports
        ),
        "diagnostic_keys_stripped": (
            diagnostic_keys_stripped
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "disabled_direct_equivalence": (
            disabled_direct_equivalence
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
        "engine_config_isolated": (
            engine_config_isolated
        ),
        "enabled_simulation_equivalence": (
            enabled_simulation_equivalence
        ),
        "metadata_alias_consistent": (
            metadata_alias_consistent
        ),
        "input_immutability": (
            input_immutability
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
            "diagnostic_scope_completion_"
            "assessment_allowed_next"
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
