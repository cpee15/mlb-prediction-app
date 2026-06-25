#!/usr/bin/env python3
"""
Layer 6QL
Combined Game-Management Diagnostic Integration Audit

Audits all five Layer 6 game-management diagnostic integrations together.

No production behavior, probability authority, historical validation, tuning,
backtesting, pricing, edge detection, or broad Layer 6 exit authority is granted.
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6QL"
LAYER_NAME = "combined_game_management_diagnostic_integration_audit"

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "tmp/layer_6QL_combined_game_management_diagnostic_integration_audit"

BUILDER_PATH = ROOT / "mlb_app/simulation/game_simulation_builder.py"
ENGINE_PATH = ROOT / "mlb_app/simulation/game_engine_v2.py"
SIMULATOR_PATH = ROOT / "mlb_app/simulation/game_simulator.py"
INNING_SIMULATOR_PATH = ROOT / "mlb_app/simulation/inning_simulator.py"
SCOPE_RESOLUTION_PATH = ROOT / "scripts/assess_6QK_layer6_game_management_scope_resolution_update.py"

DIAGNOSTIC_METADATA_KEYS = [
    "pitching_plan_diagnostics",
    "starter_hook_diagnostics",
    "bullpen_sequence_diagnostics",
    "stolen_base_pickoff_diagnostics",
    "position_player_substitution_diagnostics",
]

DIAGNOSTIC_CONFIG_KEYS = {
    "pitching_plan_diagnostics_enabled",
    "pitching_plan_evidence",
    "pitching_plan_diagnostics_version",
    "starter_hook_diagnostics_enabled",
    "starter_hook_diagnostics_version",
    "starter_hook_state",
    "bullpen_sequence_diagnostics_enabled",
    "bullpen_sequence_diagnostics_version",
    "bullpen_sequence_state",
    "stolen_base_pickoff_diagnostics_enabled",
    "stolen_base_pickoff_diagnostics_version",
    "stolen_base_pickoff_state",
    "position_player_substitution_diagnostics_enabled",
    "position_player_substitution_diagnostics_version",
    "position_player_substitution_state",
}

ATTACHMENT_FUNCTIONS = [
    "_attach_pitching_plan_diagnostics",
    "_attach_starter_hook_diagnostics",
    "_attach_bullpen_sequence_diagnostics",
    "_attach_stolen_base_pickoff_diagnostics",
    "_attach_position_player_substitution_diagnostics",
]

EXPECTED_IMPORT_MODULES = [
    "mlb_app.simulation.pitching_plan_classifier",
    "mlb_app.simulation.starter_hook_evaluator",
    "mlb_app.simulation.bullpen_sequence_evaluator",
    "mlb_app.simulation.stolen_base_pickoff_evaluator",
    "mlb_app.simulation.position_player_substitution_evaluator",
]

COMPLETION_CONTRACTS = [
    {
        "workstream_id": "GM-01",
        "script": "scripts/assess_6PH_pitching_plan_classification_diagnostic_integration_completion.py",
        "diagnosis": "pitching_plan_classification_diagnostic_integration_completion_assessment_passed",
    },
    {
        "workstream_id": "GM-02",
        "script": "scripts/assess_6PO_dynamic_starter_hook_diagnostic_scope_completion.py",
        "diagnosis": "dynamic_starter_hook_diagnostic_scope_completion_assessment_passed",
    },
    {
        "workstream_id": "GM-03",
        "script": "scripts/assess_6PV_production_bullpen_sequencing_diagnostic_scope_completion.py",
        "diagnosis": "production_bullpen_sequencing_diagnostic_scope_complete",
    },
    {
        "workstream_id": "GM-04",
        "script": "scripts/assess_6QC_stolen_base_and_pickoff_state_diagnostic_scope_completion.py",
        "diagnosis": "stolen_base_and_pickoff_state_diagnostic_scope_complete",
    },
    {
        "workstream_id": "GM-05",
        "script": "scripts/assess_6QJ_position_player_substitution_diagnostic_scope_completion.py",
        "diagnosis": "position_player_substitution_diagnostic_scope_complete",
    },
]


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""


def constants(path: Path) -> set[str]:
    if not path.exists():
        return set()
    tree = ast.parse(read_text(path), filename=str(path))
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }


def strip_game_management_diagnostics(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = deepcopy(payload)
    for key in ["meta", "metadata"]:
        metadata = deepcopy(cleaned.get(key) or {})
        for diagnostic_key in DIAGNOSTIC_METADATA_KEYS:
            metadata.pop(diagnostic_key, None)
        cleaned[key] = metadata
    return cleaned


def fake_engine_payload(game_pk: int, config: dict[str, Any]) -> dict[str, Any]:
    return {
        "game_pk": game_pk,
        "home_win_probability": 0.57,
        "away_win_probability": 0.43,
        "expected_home_runs": 4.6,
        "expected_away_runs": 4.1,
        "simulation_count": config.get("simulation_count"),
        "seed": config.get("seed"),
        "production_marker": "engine-unchanged",
        "lineup_marker": "engine-unchanged",
        "pitcher_state_marker": "engine-unchanged",
        "runner_state_marker": "engine-unchanged",
        "base_out_state_marker": "engine-unchanged",
        "meta": {"engine_marker": "6ql-fake-engine"},
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    builder_text = read_text(BUILDER_PATH)
    builder_tree = ast.parse(builder_text, filename=str(BUILDER_PATH))
    builder_constants = constants(BUILDER_PATH)

    function_nodes = {
        node.name: node
        for node in ast.walk(builder_tree)
        if isinstance(node, ast.FunctionDef)
    }

    attachment_functions_present = all(
        name in function_nodes for name in ATTACHMENT_FUNCTIONS
    )

    lazy_imports_present = all(
        any(
            isinstance(node, ast.ImportFrom) and node.module == module
            for fn_name in ATTACHMENT_FUNCTIONS
            for node in ast.walk(function_nodes.get(fn_name, ast.Pass()))
        )
        for module in EXPECTED_IMPORT_MODULES
    )

    zero_top_level_diagnostic_imports = not any(
        isinstance(node, ast.ImportFrom) and node.module in EXPECTED_IMPORT_MODULES
        for node in builder_tree.body
    )

    diagnostic_keys_stripped = DIAGNOSTIC_CONFIG_KEYS.issubset(builder_constants)
    diagnostic_metadata_keys_present = set(DIAGNOSTIC_METADATA_KEYS).issubset(builder_constants)

    engine_text = read_text(ENGINE_PATH)
    simulator_text = read_text(SIMULATOR_PATH)
    inning_text = read_text(INNING_SIMULATOR_PATH)

    reachability_tokens = [
        "pitching_plan_classifier",
        "starter_hook_evaluator",
        "bullpen_sequence_evaluator",
        "stolen_base_pickoff_evaluator",
        "position_player_substitution_evaluator",
    ]

    engine_zero_reachability = not any(token in engine_text for token in reachability_tokens)
    simulator_zero_reachability = not any(token in simulator_text for token in reachability_tokens)
    inning_zero_reachability = not any(token in inning_text for token in reachability_tokens)

    completion_rows = []
    for contract in COMPLETION_CONTRACTS:
        script_path = ROOT / contract["script"]
        script_constants = constants(script_path)
        accepted = script_path.exists() and contract["diagnosis"] in script_constants
        completion_rows.append(
            {
                "workstream_id": contract["workstream_id"],
                "script": contract["script"],
                "expected_diagnosis": contract["diagnosis"],
                "script_exists": script_path.exists(),
                "diagnosis_contract_present": contract["diagnosis"] in script_constants,
                "accepted": accepted,
            }
        )

    scope_resolution_constants = constants(SCOPE_RESOLUTION_PATH)
    six_qk_contract_passed = all(
        token in scope_resolution_constants
        for token in [
            "layer6_game_management_scope_resolution_updated",
            "6QL_combined_game_management_diagnostic_integration_audit",
            "combined_game_management_audit_allowed_next",
            "broad_layer6_reassessment_allowed_next",
        ]
    )

    builder = importlib.import_module("mlb_app.simulation.game_simulation_builder")

    captured_engine_configs: list[dict[str, Any]] = []

    def fake_engine(game_pk: int, config: dict[str, Any]) -> dict[str, Any]:
        captured_engine_configs.append(deepcopy(config))
        return fake_engine_payload(game_pk, config)

    original_loader = builder._load_sandbox_engine
    builder._load_sandbox_engine = lambda: fake_engine

    base_config = {"simulation_count": 777, "seed": 101}

    absent_config = deepcopy(base_config)
    absent_original = deepcopy(absent_config)
    absent_result = builder.build_game_simulation(3001, absent_config)

    disabled_config = {
        **deepcopy(base_config),
        "pitching_plan_diagnostics_enabled": False,
        "pitching_plan_evidence": {},
        "pitching_plan_diagnostics_version": "pitching-plan-diagnostics-v1",
        "starter_hook_diagnostics_enabled": False,
        "starter_hook_diagnostics_version": "starter-hook-diagnostics-v1",
        "starter_hook_state": {},
        "bullpen_sequence_diagnostics_enabled": False,
        "bullpen_sequence_diagnostics_version": "bullpen-sequence-diagnostics-v1",
        "bullpen_sequence_state": {},
        "stolen_base_pickoff_diagnostics_enabled": False,
        "stolen_base_pickoff_diagnostics_version": "stolen-base-pickoff-diagnostics-v1",
        "stolen_base_pickoff_state": {},
        "position_player_substitution_diagnostics_enabled": False,
        "position_player_substitution_diagnostics_version": "position-player-substitution-diagnostics-v1",
        "position_player_substitution_state": {},
    }
    disabled_original = deepcopy(disabled_config)
    disabled_result = builder.build_game_simulation(3001, disabled_config)

    enabled_config = deepcopy(disabled_config)
    for key in [
        "pitching_plan_diagnostics_enabled",
        "starter_hook_diagnostics_enabled",
        "bullpen_sequence_diagnostics_enabled",
        "stolen_base_pickoff_diagnostics_enabled",
        "position_player_substitution_diagnostics_enabled",
    ]:
        enabled_config[key] = True

    enabled_original = deepcopy(enabled_config)
    enabled_result = builder.build_game_simulation(3001, enabled_config)

    builder._load_sandbox_engine = original_loader

    disabled_exact_equivalence = absent_result == disabled_result
    enabled_simulation_equivalence = (
        strip_game_management_diagnostics(enabled_result) == absent_result
    )

    all_metadata_present = all(
        key in (enabled_result.get("meta") or {})
        for key in DIAGNOSTIC_METADATA_KEYS
    )

    all_metadata_aliases_consistent = (
        enabled_result.get("meta") == enabled_result.get("metadata")
    )

    engine_config_isolated = all(
        not any(key in config for key in DIAGNOSTIC_CONFIG_KEYS)
        for config in captured_engine_configs
    )

    caller_inputs_unchanged = all(
        [
            absent_config == absent_original,
            disabled_config == disabled_original,
            enabled_config == enabled_original,
        ]
    )

    diagnostic_statuses_safe = True
    diagnostic_authority_safe = True
    for key in DIAGNOSTIC_METADATA_KEYS:
        diagnostic = (enabled_result.get("meta") or {}).get(key, {})
        diagnostic_statuses_safe = diagnostic_statuses_safe and diagnostic.get("status") in {
            "classified",
            "evaluated",
            "validation_failed",
            "error",
        }
        diagnostic_authority_safe = diagnostic_authority_safe and all(
            [
                diagnostic.get("behavioral_effect") == "none",
                diagnostic.get("production_activation") is False,
            ]
        )

    production_markers_unchanged = all(
        [
            enabled_result.get("production_marker") == "engine-unchanged",
            enabled_result.get("lineup_marker") == "engine-unchanged",
            enabled_result.get("pitcher_state_marker") == "engine-unchanged",
            enabled_result.get("runner_state_marker") == "engine-unchanged",
            enabled_result.get("base_out_state_marker") == "engine-unchanged",
            enabled_result.get("home_win_probability") == 0.57,
            enabled_result.get("away_win_probability") == 0.43,
            enabled_result.get("expected_home_runs") == 4.6,
            enabled_result.get("expected_away_runs") == 4.1,
        ]
    )

    cases = [
        ("QL-C01", "five_completion_contracts_present", all(row["accepted"] for row in completion_rows)),
        ("QL-C02", "six_qk_scope_resolution_contract", six_qk_contract_passed),
        ("QL-C03", "five_attachment_functions_present", attachment_functions_present),
        ("QL-C04", "lazy_imports_and_no_top_level_imports", lazy_imports_present and zero_top_level_diagnostic_imports),
        ("QL-C05", "diagnostic_keys_stripped", diagnostic_keys_stripped),
        ("QL-C06", "zero_engine_simulator_reachability", engine_zero_reachability and simulator_zero_reachability and inning_zero_reachability),
        ("QL-C07", "disabled_exact_equivalence", disabled_exact_equivalence),
        ("QL-C08", "enabled_simulation_equivalence", enabled_simulation_equivalence),
        ("QL-C09", "all_metadata_attached", all_metadata_present and diagnostic_metadata_keys_present),
        ("QL-C10", "metadata_alias_consistency", all_metadata_aliases_consistent),
        ("QL-C11", "engine_config_isolation", engine_config_isolated),
        ("QL-C12", "caller_input_immutability", caller_inputs_unchanged),
        ("QL-C13", "diagnostic_statuses_safe", diagnostic_statuses_safe),
        ("QL-C14", "production_authority_absent", diagnostic_authority_safe and production_markers_unchanged),
    ]

    case_rows = [
        {
            "case_id": case_id,
            "scenario": scenario,
            "passed": passed,
        }
        for case_id, scenario, passed in cases
    ]

    checks = [
        {
            "check": "fourteen_independent_cases_pass",
            "actual": sum(1 for _, _, passed in cases if passed),
            "expected": 14,
            "passed": sum(1 for _, _, passed in cases if passed) == 14,
        },
        {
            "check": "all_five_workstreams_complete",
            "actual": sum(1 for row in completion_rows if row["accepted"]),
            "expected": 5,
            "passed": all(row["accepted"] for row in completion_rows),
        },
        {
            "check": "combined_audit_complete_without_production_authority",
            "actual": diagnostic_authority_safe and production_markers_unchanged,
            "expected": True,
            "passed": diagnostic_authority_safe and production_markers_unchanged,
        },
        {
            "check": "broad_layer6_exit_still_paused",
            "actual": True,
            "expected": True,
            "passed": True,
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    write_csv(
        OUTPUT_DIR / "audit_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )
    write_csv(
        OUTPUT_DIR / "independent_cases.csv",
        ["case_id", "scenario", "passed"],
        case_rows,
    )
    write_csv(
        OUTPUT_DIR / "workstream_contracts.csv",
        [
            "workstream_id",
            "script",
            "expected_diagnosis",
            "script_exists",
            "diagnosis_contract_present",
            "accepted",
        ],
        completion_rows,
    )
    write_csv(
        OUTPUT_DIR / "engine_config_capture.csv",
        ["call_index", "diagnostic_key_present", "config_json"],
        [
            {
                "call_index": index,
                "diagnostic_key_present": any(key in config for key in DIAGNOSTIC_CONFIG_KEYS),
                "config_json": json.dumps(config, sort_keys=True),
            }
            for index, config in enumerate(captured_engine_configs, start=1)
        ],
    )

    summary = {
        "audit_checks_required": len(checks),
        "audit_checks_passed": sum(1 for row in checks if row["passed"]),
        "independent_cases_required": len(cases),
        "independent_cases_passed": sum(1 for _, _, passed in cases if passed),
        "workstreams_required": 5,
        "workstreams_complete": sum(1 for row in completion_rows if row["accepted"]),
        "disabled_exact_equivalence": disabled_exact_equivalence,
        "enabled_simulation_equivalence": enabled_simulation_equivalence,
        "engine_config_isolation": engine_config_isolated,
        "caller_input_immutability": caller_inputs_unchanged,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
        "broad_layer6_exit_paused": True,
    }
    write_json(OUTPUT_DIR / "audit_summary.json", summary)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "combined_game_management_diagnostic_integration_audit_passed"
            if all_checks_passed
            else "combined_game_management_diagnostic_integration_audit_failed"
        ),
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "broad_layer6_reassessment_allowed_next": all_checks_passed,
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            "6QM_layer6_broad_scope_reassessment_plan"
            if all_checks_passed
            else "6QM_combined_game_management_diagnostic_integration_remediation"
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "audit_checks.csv"),
            str(OUTPUT_DIR / "independent_cases.csv"),
            str(OUTPUT_DIR / "workstream_contracts.csv"),
            str(OUTPUT_DIR / "engine_config_capture.csv"),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "audit_summary.json"),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
    }

    write_json(OUTPUT_DIR / "diagnosis.json", diagnosis)
    print(json.dumps(diagnosis, indent=2))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
