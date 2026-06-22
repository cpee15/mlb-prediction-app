#!/usr/bin/env python3
"""
Layer 6PM
Dynamic Starter-Hook Diagnostic Integration Implementation

Validates disabled-by-default, metadata-only integration of the pure
starter-hook evaluator through the shared game simulation builder.

No production starter-hook or probability authority is granted.
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


LAYER_ID = "6PM"
LAYER_NAME = (
    "dynamic_starter_hook_diagnostic_"
    "integration_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PM_dynamic_starter_hook_"
    "diagnostic_integration_implementation"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6PL_dynamic_starter_hook_"
    "diagnostic_integration.py"
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
    "starter_hook_evaluator.py"
)

DIAGNOSTIC_KEYS = {
    "starter_hook_diagnostics_enabled",
    "starter_hook_diagnostics_version",
    "starter_hook_state",
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
    for index in reversed(
        [
            position
            for position, character in enumerate(text)
            if character == "{"
        ]
    ):
        try:
            payload = json.loads(
                text[index:].strip()
            )
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            return payload

    return {}


def base_state() -> dict[str, Any]:
    return {
        "inning": 4,
        "outs": 0,
        "base_state": {
            "first": False,
            "second": False,
            "third": False,
        },
        "batters_faced": 16,
        "pitch_count_estimate": 60.0,
        "times_through_order": 1.8,
        "runs_allowed": 1,
        "recent_traffic_index": 0.20,
        "score_margin": 1,
        "leverage_proxy": 0.45,
        "starter_quality_score": 0.0,
        "expected_starter_innings": 5.6,
        "fatigue_index": 0.30,
        "bullpen_availability": {},
        "pitching_plan": {},
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


def without_starter_hook_metadata(
    payload: dict[str, Any],
) -> dict[str, Any]:
    cleaned = deepcopy(payload)

    for key in ["meta", "metadata"]:
        metadata = deepcopy(
            cleaned.get(key) or {}
        )

        metadata.pop(
            "starter_hook_diagnostics",
            None,
        )

        cleaned[key] = metadata

    return cleaned


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

    plan_text = read_text(
        PLAN_PATH
    )

    plan_tree = ast.parse(
        plan_text,
        filename=str(PLAN_PATH),
    )

    plan_string_constants = {
        node.value
        for node in ast.walk(plan_tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    plan_contract_passed = all(
        [
            (
                "dynamic_starter_hook_diagnostic_"
                "integration_plan_complete"
            )
            in plan_string_constants,
            (
                "6PM_dynamic_starter_hook_"
                "diagnostic_integration_implementation"
            )
            in plan_string_constants,
            (
                "diagnostic_integration_"
                "implementation_allowed_next"
            )
            in plan_text,
            (
                "production_behavior_"
                "integration_allowed_next"
            )
            in plan_text,
        ]
    )

    plan_payload = {
        "contract_validation_mode": (
            "static_merged_plan_contract"
        ),
        "contract_passed": (
            plan_contract_passed
        ),
        "historical_runtime_assumptions": (
            "superseded_by_6PM_builder_integration"
        ),
    }

    builder = importlib.import_module(
        "mlb_app.simulation."
        "game_simulation_builder"
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

    attachment_function_present = (
        "_attach_starter_hook_diagnostics"
        in function_nodes
    )

    lazy_import_present = False

    attachment_node = function_nodes.get(
        "_attach_starter_hook_diagnostics"
    )

    if attachment_node is not None:
        lazy_import_present = any(
            isinstance(node, ast.ImportFrom)
            and node.module
            == (
                "mlb_app.simulation."
                "starter_hook_evaluator"
            )
            for node in ast.walk(
                attachment_node
            )
        )

    top_level_evaluator_imports = [
        node.module
        for node in builder_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module
        == (
            "mlb_app.simulation."
            "starter_hook_evaluator"
        )
    ]

    zero_top_level_imports = (
        top_level_evaluator_imports == []
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
            "starter_hook_evaluator",
            "evaluate_starter_hook",
        ]
    )

    simulator_zero_reachability = not any(
        token in simulator_text
        for token in [
            "starter_hook_evaluator",
            "evaluate_starter_hook",
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
        ._attach_starter_hook_diagnostics(
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
                "before=('mlb_app.simulation."
                "starter_hook_evaluator' in sys.modules); "
                "p={'meta': {'x': 1}}; "
                "r=b._attach_starter_hook_diagnostics"
                "(p, config={}); "
                "after=('mlb_app.simulation."
                "starter_hook_evaluator' in sys.modules); "
                "print(json.dumps({'before': before, "
                "'after': after, 'equal': r == "
                "{'meta': {'x': 1}}}))"
            ),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    disabled_import_payload = (
        parse_last_json_object(
            disabled_import_probe.stdout
        )
    )

    disabled_zero_imports = all(
        [
            disabled_import_probe.returncode
            == 0,
            disabled_import_payload.get(
                "before"
            )
            is False,
            disabled_import_payload.get(
                "after"
            )
            is False,
            disabled_import_payload.get(
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
        "starter_hook_diagnostics_enabled": False,
        "starter_hook_diagnostics_version": (
            "unused-version"
        ),
        "starter_hook_state": base_state(),
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

    disabled_builder_exact_equivalence = (
        disabled_builder_result
        == baseline_result
    )

    disabled_config_unchanged = (
        disabled_config
        == disabled_config_original
    )

    keep_config = {
        **baseline_config,
        "starter_hook_diagnostics_enabled": True,
        "starter_hook_state": base_state(),
    }

    keep_original = deepcopy(
        keep_config
    )

    keep_result = (
        builder.build_game_simulation(
            123,
            keep_config,
        )
    )

    pull_state = base_state()
    pull_state.update(
        {
            "inning": 6,
            "pitch_count_estimate": 108.0,
            "batters_faced": 27,
            "times_through_order": 3.0,
            "fatigue_index": 0.88,
        }
    )

    pull_config = {
        **baseline_config,
        "starter_hook_diagnostics_enabled": True,
        "starter_hook_diagnostics_version": (
            "custom-starter-hook-v2"
        ),
        "starter_hook_state": pull_state,
    }

    pull_original = deepcopy(
        pull_config
    )

    pull_result = (
        builder.build_game_simulation(
            123,
            pull_config,
        )
    )

    incomplete_state = base_state()
    incomplete_state.pop(
        "pitch_count_estimate"
    )

    incomplete_result = (
        builder.build_game_simulation(
            123,
            {
                **baseline_config,
                "starter_hook_diagnostics_enabled": True,
                "starter_hook_state": (
                    incomplete_state
                ),
            },
        )
    )

    invalid_result = (
        builder.build_game_simulation(
            123,
            {
                **baseline_config,
                "starter_hook_diagnostics_enabled": True,
                "starter_hook_state": [],
            },
        )
    )

    builder._load_sandbox_engine = (
        original_loader
    )

    keep_diagnostics = (
        keep_result["meta"][
            "starter_hook_diagnostics"
        ]
    )

    pull_diagnostics = (
        pull_result["meta"][
            "starter_hook_diagnostics"
        ]
    )

    incomplete_diagnostics = (
        incomplete_result["meta"][
            "starter_hook_diagnostics"
        ]
    )

    invalid_diagnostics = (
        invalid_result["meta"][
            "starter_hook_diagnostics"
        ]
    )

    enabled_simulation_equivalence = all(
        [
            without_starter_hook_metadata(
                keep_result
            )
            == baseline_result,
            without_starter_hook_metadata(
                pull_result
            )
            == baseline_result,
            without_starter_hook_metadata(
                incomplete_result
            )
            == baseline_result,
            without_starter_hook_metadata(
                invalid_result
            )
            == baseline_result,
        ]
    )

    engine_configs_isolated = all(
        not (
            set(config)
            & DIAGNOSTIC_KEYS
        )
        for config in captured_configs
    )

    metadata_alias_consistent = all(
        result.get("meta")
        == result.get("metadata")
        for result in [
            keep_result,
            pull_result,
            incomplete_result,
            invalid_result,
        ]
    )

    fixtures = [
        {
            "fixture_id": "PM-F01",
            "scenario": "diagnostics_disabled",
            "passed": (
                disabled_builder_exact_equivalence
                and disabled_zero_imports
            ),
        },
        {
            "fixture_id": "PM-F02",
            "scenario": "enabled_valid_keep_state",
            "passed": all(
                [
                    keep_diagnostics[
                        "status"
                    ]
                    == "evaluated",
                    keep_diagnostics[
                        "evaluation"
                    ]["decision"]
                    == "keep",
                ]
            ),
        },
        {
            "fixture_id": "PM-F03",
            "scenario": "enabled_valid_pull_state",
            "passed": all(
                [
                    pull_diagnostics[
                        "status"
                    ]
                    == "evaluated",
                    pull_diagnostics[
                        "evaluation"
                    ]["decision"]
                    == "pull",
                ]
            ),
        },
        {
            "fixture_id": "PM-F04",
            "scenario": "enabled_incomplete_state",
            "passed": (
                incomplete_diagnostics[
                    "evaluation"
                ]["decision"]
                == "insufficient_state"
            ),
        },
        {
            "fixture_id": "PM-F05",
            "scenario": "enabled_invalid_state_type",
            "passed": (
                invalid_diagnostics[
                    "evaluation"
                ]["decision"]
                == "insufficient_state"
            ),
        },
        {
            "fixture_id": "PM-F06",
            "scenario": "enabled_custom_version",
            "passed": (
                pull_diagnostics[
                    "version"
                ]
                == "custom-starter-hook-v2"
            ),
        },
        {
            "fixture_id": "PM-F07",
            "scenario": "diagnostic_safety_fields",
            "passed": all(
                diagnostics[
                    "behavioral_effect"
                ]
                == "none"
                and diagnostics[
                    "canonical_probability_"
                    "authority_changed"
                ]
                is False
                and diagnostics[
                    "production_activation"
                ]
                is False
                for diagnostics in [
                    keep_diagnostics,
                    pull_diagnostics,
                    incomplete_diagnostics,
                    invalid_diagnostics,
                ]
            ),
        },
        {
            "fixture_id": "PM-F08",
            "scenario": "caller_config_immutability",
            "passed": all(
                [
                    keep_config
                    == keep_original,
                    pull_config
                    == pull_original,
                    disabled_config_unchanged,
                ]
            ),
        },
        {
            "fixture_id": "PM-F09",
            "scenario": "engine_config_isolation",
            "passed": (
                engine_configs_isolated
            ),
        },
        {
            "fixture_id": "PM-F10",
            "scenario": "metadata_alias_consistency",
            "passed": (
                metadata_alias_consistent
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
            "check": "six_pl_plan_contract_passes",
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
            "check": "zero_top_level_evaluator_imports",
            "actual": zero_top_level_imports,
            "expected": True,
            "passed": zero_top_level_imports,
        },
        {
            "check": "disabled_direct_exact_equivalence",
            "actual": disabled_exact_equivalence,
            "expected": True,
            "passed": disabled_exact_equivalence,
        },
        {
            "check": "disabled_zero_evaluator_imports",
            "actual": disabled_zero_imports,
            "expected": True,
            "passed": disabled_zero_imports,
        },
        {
            "check": "disabled_builder_exact_equivalence",
            "actual": (
                disabled_builder_exact_equivalence
            ),
            "expected": True,
            "passed": (
                disabled_builder_exact_equivalence
            ),
        },
        {
            "check": "enabled_simulation_field_equivalence",
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
            "check": "caller_inputs_unchanged",
            "actual": all(
                [
                    keep_config
                    == keep_original,
                    pull_config
                    == pull_original,
                    disabled_config_unchanged,
                ]
            ),
            "expected": True,
            "passed": all(
                [
                    keep_config
                    == keep_original,
                    pull_config
                    == pull_original,
                    disabled_config_unchanged,
                ]
            ),
        },
        {
            "check": "engine_zero_reachability",
            "actual": engine_zero_reachability,
            "expected": True,
            "passed": engine_zero_reachability,
        },
        {
            "check": "simulator_zero_reachability",
            "actual": (
                simulator_zero_reachability
            ),
            "expected": True,
            "passed": (
                simulator_zero_reachability
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
        OUTPUT_DIR / "engine_config_captures.csv",
        [
            "capture_index",
            "config",
            "contains_diagnostic_key",
        ],
        [
            {
                "capture_index": index,
                "config": config,
                "contains_diagnostic_key": bool(
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
        OUTPUT_DIR / "fixture_payloads.json",
        {
            "baseline": baseline_result,
            "disabled": disabled_builder_result,
            "keep": keep_result,
            "pull": pull_result,
            "incomplete": incomplete_result,
            "invalid": invalid_result,
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
        "disabled_exact_equivalence": (
            disabled_builder_exact_equivalence
        ),
        "disabled_zero_evaluator_imports": (
            disabled_zero_imports
        ),
        "enabled_simulation_field_equivalence": (
            enabled_simulation_equivalence
        ),
        "engine_config_isolated": (
            engine_configs_isolated
        ),
        "metadata_alias_consistent": (
            metadata_alias_consistent
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
        "6PN_dynamic_starter_hook_"
        "diagnostic_integration_audit"
        if all_checks_passed
        else
        "6PN_dynamic_starter_hook_"
        "diagnostic_integration_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "dynamic_starter_hook_diagnostic_"
            "integration_implementation_complete"
            if all_checks_passed
            else
            "dynamic_starter_hook_diagnostic_"
            "integration_implementation_failed"
        ),
        "all_checks_passed": all_checks_passed,
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
        "six_pl_plan_contract_passed": (
            plan_contract_passed
        ),
        "disabled_exact_equivalence": (
            disabled_builder_exact_equivalence
        ),
        "disabled_zero_evaluator_imports": (
            disabled_zero_imports
        ),
        "enabled_simulation_field_equivalence": (
            enabled_simulation_equivalence
        ),
        "engine_config_isolated": (
            engine_configs_isolated
        ),
        "caller_inputs_unchanged": all(
            [
                keep_config == keep_original,
                pull_config == pull_original,
                disabled_config_unchanged,
            ]
        ),
        "metadata_alias_consistent": (
            metadata_alias_consistent
        ),
        "engine_zero_reachability": (
            engine_zero_reachability
        ),
        "simulator_zero_reachability": (
            simulator_zero_reachability
        ),
        "enabled_default": False,
        "lazy_import_only": (
            lazy_import_present
            and zero_top_level_imports
        ),
        "metadata_only": True,
        "production_starter_hook_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
        "broad_layer6_exit_paused": True,
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
                / "engine_config_captures.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "fixture_payloads.json"
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
