#!/usr/bin/env python3
"""
Layer 6PN
Dynamic Starter-Hook Diagnostic Integration Independent Audit

Independently verifies the merged 6PM diagnostic integration for:

- merged implementation contract presence;
- disabled-path exact equivalence;
- disabled-path zero evaluator imports;
- enabled-path simulation-field equivalence;
- engine-config isolation;
- caller input immutability;
- metadata alias consistency;
- lazy import placement;
- engine and simulator zero reachability;
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


LAYER_ID = "6PN"
LAYER_NAME = (
    "dynamic_starter_hook_"
    "diagnostic_integration_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PN_dynamic_starter_hook_"
    "diagnostic_integration_audit"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "scripts/implement_6PM_"
    "dynamic_starter_hook_diagnostic_integration.py"
)

BUILDER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "starter_hook_evaluator.py"
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

DIAGNOSTIC_KEYS = {
    "starter_hook_diagnostics_enabled",
    "starter_hook_diagnostics_version",
    "starter_hook_state",
}

PROHIBITED_AUTHORITIES = [
    "production_starter_hook_change",
    "starter_exit_distribution_change",
    "starter_innings_change",
    "bullpen_transition_change",
    "bullpen_sequence_change",
    "plate_appearance_probability_change",
    "simulation_parameter_change",
    "simulation_score_change",
    "win_probability_change",
    "canonical_probability_replacement",
    "pitching_plan_production_activation",
    "backend_response_change",
    "frontend_behavior_change",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "edge_detection",
    "bet_recommendation",
    "broad_layer6_exit",
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
        candidate = text[index:].strip()

        try:
            payload = json.loads(candidate)
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
        "home_win_probability": 0.56,
        "away_win_probability": 0.44,
        "expected_home_runs": 4.5,
        "expected_away_runs": 3.9,
        "simulation_count": (
            config.get("simulation_count")
        ),
        "seed": config.get("seed"),
        "meta": {
            "engine_marker": "audit-engine",
        },
    }


def strip_starter_hook_metadata(
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
            IMPLEMENTATION_PATH,
            BUILDER_PATH,
            EVALUATOR_PATH,
            ENGINE_PATH,
            SIMULATOR_PATH,
        ]
    )

    implementation_run = subprocess.run(
        [
            sys.executable,
            str(IMPLEMENTATION_PATH),
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
            implementation_run.returncode == 0,
            implementation_payload.get(
                "all_checks_passed"
            )
            is True,
            implementation_payload.get(
                "implementation_checks_passed"
            )
            == 14,
            implementation_payload.get(
                "implementation_checks_required"
            )
            == 14,
            implementation_payload.get(
                "fixtures_passed"
            )
            == 10,
            implementation_payload.get(
                "disabled_exact_equivalence"
            )
            is True,
            implementation_payload.get(
                "disabled_zero_evaluator_imports"
            )
            is True,
            implementation_payload.get(
                "enabled_simulation_field_equivalence"
            )
            is True,
            implementation_payload.get(
                "engine_config_isolated"
            )
            is True,
            implementation_payload.get(
                "engine_zero_reachability"
            )
            is True,
            implementation_payload.get(
                "simulator_zero_reachability"
            )
            is True,
            implementation_payload.get(
                "production_behavior_integration_allowed_next"
            )
            is False,
        ]
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
        "_attach_starter_hook_diagnostics"
    )

    attachment_function_present = (
        attachment_node is not None
    )

    lazy_import_present = (
        attachment_node is not None
        and any(
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
    )

    zero_top_level_evaluator_imports = not any(
        isinstance(node, ast.ImportFrom)
        and node.module
        == (
            "mlb_app.simulation."
            "starter_hook_evaluator"
        )
        for node in builder_tree.body
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
            "starter_hook_diagnostics",
        ]
    )

    simulator_zero_reachability = not any(
        token in simulator_text
        for token in [
            "starter_hook_evaluator",
            "evaluate_starter_hook",
            "starter_hook_diagnostics",
        ]
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
                "starter_hook_evaluator'; "
                "before=name in sys.modules; "
                "payload={'meta': {'x': 1}, 'value': 2}; "
                "result=b._attach_starter_hook_diagnostics"
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
            disabled_probe.returncode == 0,
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
        "simulation_count": 250,
        "seed": 29,
    }

    baseline_result = (
        builder.build_game_simulation(
            456,
            deepcopy(baseline_config),
        )
    )

    disabled_config = {
        **baseline_config,
        "starter_hook_diagnostics_enabled": False,
        "starter_hook_diagnostics_version": (
            "ignored"
        ),
        "starter_hook_state": base_state(),
    }

    disabled_original = deepcopy(
        disabled_config
    )

    disabled_result = (
        builder.build_game_simulation(
            456,
            disabled_config,
        )
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
            456,
            keep_config,
        )
    )

    pull_state = base_state()
    pull_state.update(
        {
            "inning": 7,
            "batters_faced": 28,
            "pitch_count_estimate": 109.0,
            "times_through_order": 3.1,
            "fatigue_index": 0.90,
            "leverage_proxy": 0.92,
        }
    )

    pull_config = {
        **baseline_config,
        "starter_hook_diagnostics_enabled": True,
        "starter_hook_diagnostics_version": (
            "starter-hook-audit-v1"
        ),
        "starter_hook_state": pull_state,
    }

    pull_original = deepcopy(
        pull_config
    )

    pull_result = (
        builder.build_game_simulation(
            456,
            pull_config,
        )
    )

    incomplete_state = base_state()
    incomplete_state.pop(
        "pitch_count_estimate"
    )

    incomplete_config = {
        **baseline_config,
        "starter_hook_diagnostics_enabled": True,
        "starter_hook_state": incomplete_state,
    }

    incomplete_original = deepcopy(
        incomplete_config
    )

    incomplete_result = (
        builder.build_game_simulation(
            456,
            incomplete_config,
        )
    )

    builder._load_sandbox_engine = (
        original_loader
    )

    disabled_builder_equivalence = (
        disabled_result
        == baseline_result
    )

    enabled_simulation_equivalence = all(
        [
            strip_starter_hook_metadata(
                keep_result
            )
            == baseline_result,
            strip_starter_hook_metadata(
                pull_result
            )
            == baseline_result,
            strip_starter_hook_metadata(
                incomplete_result
            )
            == baseline_result,
        ]
    )

    engine_config_isolated = all(
        not (
            set(config)
            & DIAGNOSTIC_KEYS
        )
        for config in captured_configs
    )

    caller_inputs_unchanged = all(
        [
            disabled_config
            == disabled_original,
            keep_config
            == keep_original,
            pull_config
            == pull_original,
            incomplete_config
            == incomplete_original,
        ]
    )

    metadata_alias_consistent = all(
        payload.get("meta")
        == payload.get("metadata")
        for payload in [
            keep_result,
            pull_result,
            incomplete_result,
        ]
    )

    keep_diag = (
        keep_result["meta"][
            "starter_hook_diagnostics"
        ]
    )

    pull_diag = (
        pull_result["meta"][
            "starter_hook_diagnostics"
        ]
    )

    incomplete_diag = (
        incomplete_result["meta"][
            "starter_hook_diagnostics"
        ]
    )

    metadata_contract_valid = all(
        set(diagnostics)
        == {
            "enabled",
            "status",
            "version",
            "evaluation",
            "validation",
            "error",
            "behavioral_effect",
            (
                "canonical_probability_"
                "authority_changed"
            ),
            "production_activation",
        }
        for diagnostics in [
            keep_diag,
            pull_diag,
            incomplete_diag,
        ]
    )

    safety_fields_valid = all(
        diagnostics.get(
            "behavioral_effect"
        )
        == "none"
        and diagnostics.get(
            "canonical_probability_"
            "authority_changed"
        )
        is False
        and diagnostics.get(
            "production_activation"
        )
        is False
        for diagnostics in [
            keep_diag,
            pull_diag,
            incomplete_diag,
        ]
    )

    independent_cases = [
        {
            "case_id": "PN-C01",
            "scenario": "disabled_exact_equivalence",
            "passed": (
                disabled_builder_equivalence
                and disabled_direct_equivalence
            ),
        },
        {
            "case_id": "PN-C02",
            "scenario": "disabled_zero_imports",
            "passed": disabled_zero_imports,
        },
        {
            "case_id": "PN-C03",
            "scenario": "enabled_keep_state",
            "passed": all(
                [
                    keep_diag["status"]
                    == "evaluated",
                    keep_diag["evaluation"][
                        "decision"
                    ]
                    == "keep",
                ]
            ),
        },
        {
            "case_id": "PN-C04",
            "scenario": "enabled_pull_state",
            "passed": all(
                [
                    pull_diag["status"]
                    == "evaluated",
                    pull_diag["evaluation"][
                        "decision"
                    ]
                    == "pull",
                    pull_diag["version"]
                    == "starter-hook-audit-v1",
                ]
            ),
        },
        {
            "case_id": "PN-C05",
            "scenario": "incomplete_state_fallback",
            "passed": (
                incomplete_diag[
                    "evaluation"
                ]["decision"]
                == "insufficient_state"
            ),
        },
        {
            "case_id": "PN-C06",
            "scenario": "simulation_field_equivalence",
            "passed": (
                enabled_simulation_equivalence
            ),
        },
        {
            "case_id": "PN-C07",
            "scenario": "engine_config_isolation",
            "passed": engine_config_isolated,
        },
        {
            "case_id": "PN-C08",
            "scenario": "caller_input_immutability",
            "passed": caller_inputs_unchanged,
        },
        {
            "case_id": "PN-C09",
            "scenario": "metadata_alias_consistency",
            "passed": metadata_alias_consistent,
        },
        {
            "case_id": "PN-C10",
            "scenario": "diagnostic_safety_contract",
            "passed": all(
                [
                    metadata_contract_valid,
                    safety_fields_valid,
                ]
            ),
        },
    ]

    independent_cases_passed = sum(
        1
        for row in independent_cases
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
            "check": (
                "six_pm_implementation_contract_passes"
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
            "check": "lazy_import_present",
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
                "disabled_zero_evaluator_imports"
            ),
            "actual": disabled_zero_imports,
            "expected": True,
            "passed": disabled_zero_imports,
        },
        {
            "check": (
                "disabled_exact_equivalence"
            ),
            "actual": (
                disabled_builder_equivalence
                and disabled_direct_equivalence
            ),
            "expected": True,
            "passed": (
                disabled_builder_equivalence
                and disabled_direct_equivalence
            ),
        },
        {
            "check": (
                "enabled_simulation_field_equivalence"
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
            "actual": engine_config_isolated,
            "expected": True,
            "passed": engine_config_isolated,
        },
        {
            "check": "caller_inputs_unchanged",
            "actual": caller_inputs_unchanged,
            "expected": True,
            "passed": caller_inputs_unchanged,
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
                "metadata_contract_valid"
            ),
            "actual": metadata_contract_valid,
            "expected": True,
            "passed": metadata_contract_valid,
        },
        {
            "check": (
                "diagnostic_safety_fields_valid"
            ),
            "actual": safety_fields_valid,
            "expected": True,
            "passed": safety_fields_valid,
        },
        {
            "check": (
                "engine_zero_reachability"
            ),
            "actual": engine_zero_reachability,
            "expected": True,
            "passed": engine_zero_reachability,
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
                "ten_independent_cases_pass"
            ),
            "actual": (
                independent_cases_passed
            ),
            "expected": 10,
            "passed": (
                independent_cases_passed == 10
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "6PN is an independent diagnostic "
                "integration audit only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "gm02_diagnostic_scope_completion_"
                    "assessment"
                ),
                "granted": (
                    all_checks_passed
                ),
                "reason": (
                    "Assessment only; no production "
                    "starter-hook authority."
                ),
            },
            {
                "authority": (
                    "production_behavior_integration"
                ),
                "granted": False,
                "reason": (
                    "Historical validation and explicit "
                    "behavioral authorization remain absent."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6PO_dynamic_starter_hook_"
        "diagnostic_scope_completion_assessment"
        if all_checks_passed
        else
        "6PO_dynamic_starter_hook_"
        "diagnostic_integration_remediation"
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
        OUTPUT_DIR / "independent_cases.csv",
        [
            "case_id",
            "scenario",
            "passed",
        ],
        independent_cases,
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
                    "Assess whether GM-02 diagnostic "
                    "scope is complete while preserving "
                    "zero production behavior authority."
                ),
                "entry_condition": (
                    "All 6PN audit checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "implementation_contract.json",
        {
            "returncode": (
                implementation_run.returncode
            ),
            "contract_passed": (
                implementation_contract_passed
            ),
            "diagnosis": (
                implementation_payload
            ),
            "stderr": (
                implementation_run.stderr
            ),
        },
    )

    write_json(
        OUTPUT_DIR / "audit_payloads.json",
        {
            "baseline": baseline_result,
            "disabled": disabled_result,
            "keep": keep_result,
            "pull": pull_result,
            "incomplete": incomplete_result,
            "disabled_probe": (
                disabled_probe_payload
            ),
        },
    )

    summary = {
        "audit_checks_required": len(
            checks
        ),
        "audit_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "independent_cases_required": 10,
        "independent_cases_passed": (
            independent_cases_passed
        ),
        "disabled_exact_equivalence": (
            disabled_builder_equivalence
            and disabled_direct_equivalence
        ),
        "disabled_zero_evaluator_imports": (
            disabled_zero_imports
        ),
        "enabled_simulation_field_equivalence": (
            enabled_simulation_equivalence
        ),
        "engine_config_isolated": (
            engine_config_isolated
        ),
        "caller_inputs_unchanged": (
            caller_inputs_unchanged
        ),
        "metadata_alias_consistent": (
            metadata_alias_consistent
        ),
        "metadata_contract_valid": (
            metadata_contract_valid
        ),
        "diagnostic_safety_fields_valid": (
            safety_fields_valid
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
        OUTPUT_DIR / "audit_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "dynamic_starter_hook_diagnostic_"
            "integration_audit_passed"
            if all_checks_passed
            else
            "dynamic_starter_hook_diagnostic_"
            "integration_audit_failed"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "audit_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "audit_checks_required": len(
            checks
        ),
        "independent_cases_passed": (
            independent_cases_passed
        ),
        "independent_cases_required": 10,
        "six_pm_implementation_contract_passed": (
            implementation_contract_passed
        ),
        "disabled_exact_equivalence": (
            disabled_builder_equivalence
            and disabled_direct_equivalence
        ),
        "disabled_zero_evaluator_imports": (
            disabled_zero_imports
        ),
        "enabled_simulation_field_equivalence": (
            enabled_simulation_equivalence
        ),
        "engine_config_isolated": (
            engine_config_isolated
        ),
        "caller_inputs_unchanged": (
            caller_inputs_unchanged
        ),
        "metadata_alias_consistent": (
            metadata_alias_consistent
        ),
        "metadata_contract_valid": (
            metadata_contract_valid
        ),
        "diagnostic_safety_fields_valid": (
            safety_fields_valid
        ),
        "lazy_import_only": (
            lazy_import_present
            and zero_top_level_evaluator_imports
        ),
        "engine_zero_reachability": (
            engine_zero_reachability
        ),
        "simulator_zero_reachability": (
            simulator_zero_reachability
        ),
        "production_starter_hook_changed": False,
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
        "gm02_diagnostic_scope_completion_assessment_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "audit_checks.csv"
            ),
            str(
                OUTPUT_DIR / "independent_cases.csv"
            ),
            str(
                OUTPUT_DIR
                / "engine_config_captures.csv"
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
                / "implementation_contract.json"
            ),
            str(
                OUTPUT_DIR / "audit_payloads.json"
            ),
            str(
                OUTPUT_DIR / "audit_summary.json"
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

    print(json.dumps(diagnosis, indent=2))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
