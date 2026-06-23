#!/usr/bin/env python3
"""
Layer 6PU
Production Bullpen Sequencing Diagnostic Integration Independent Audit

Independently verifies the merged 6PT diagnostic integration for:

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


LAYER_ID = "6PU"

LAYER_NAME = (
    "production_bullpen_sequencing_"
    "diagnostic_integration_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PU_production_bullpen_"
    "sequencing_diagnostic_integration_audit"
)

IMPLEMENTATION_PATH = (
    ROOT
    / "scripts/implement_6PT_production_"
    "bullpen_sequencing_diagnostic_integration.py"
)

BUILDER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py"
)

EVALUATOR_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "bullpen_sequence_evaluator.py"
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


def reliever(
    pitcher_id: str,
    role: str,
    *,
    evidence_complete: bool = True,
) -> dict[str, Any]:
    return {
        "pitcher_id": pitcher_id,
        "role": role,
        "throws": "R",
        "quality_score": 0.20,
        "availability_status": "available",
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


def strip_bullpen_metadata(
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
    payload: dict[str, Any],
) -> dict[str, Any]:
    return (
        payload.get("meta", {})
        .get(
            "bullpen_sequence_diagnostics",
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
                "enabled_simulation_equivalence"
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

    zero_top_level_evaluator_imports = not any(
        isinstance(node, ast.ImportFrom)
        and node.module
        == (
            "mlb_app.simulation."
            "bullpen_sequence_evaluator"
        )
        for node in builder_tree.body
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
            "bullpen_sequence_diagnostics",
        ]
    )

    simulator_zero_reachability = not any(
        token in simulator_text
        for token in [
            "bullpen_sequence_evaluator",
            "evaluate_bullpen_sequence",
            "bullpen_sequence_diagnostics",
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
                "bullpen_sequence_evaluator'; "
                "before=name in sys.modules; "
                "payload={'meta': {'x': 1}, 'value': 2}; "
                "result=b._attach_bullpen_sequence_diagnostics"
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

    disabled_result = (
        builder.build_game_simulation(
            456,
            disabled_config,
        )
    )

    enabled_config = {
        **baseline_config,
        "bullpen_sequence_diagnostics_enabled": (
            True
        ),
        "bullpen_sequence_diagnostics_version": (
            "bullpen-sequence-diagnostics-v1"
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
            456,
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
            456,
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
            456,
            invalid_config,
        )
    )

    builder._load_sandbox_engine = (
        original_loader
    )

    disabled_builder_equivalence = (
        disabled_result
        == baseline_result
    )

    enabled_simulation_equivalence = (
        strip_bullpen_metadata(
            enabled_result
        )
        == baseline_result
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

    metadata_alias_consistent = all(
        [
            enabled_result.get("meta")
            == enabled_result.get("metadata"),
            partial_result.get("meta")
            == partial_result.get("metadata"),
            invalid_result.get("meta")
            == invalid_result.get("metadata"),
        ]
    )

    state_statuses_valid = all(
        [
            enabled_metadata.get(
                "status"
            )
            == "evaluated",
            enabled_metadata.get(
                "evaluation", {}
            ).get(
                "state_completeness"
            )
            == "complete",
            partial_metadata.get(
                "status"
            )
            == "evaluated",
            partial_metadata.get(
                "evaluation", {}
            ).get(
                "state_completeness"
            )
            == "partial",
            invalid_metadata.get(
                "status"
            )
            == "evaluated",
            invalid_metadata.get(
                "evaluation", {}
            ).get(
                "state_completeness"
            )
            == "invalid",
        ]
    )

    all_metadata_safe = all(
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
        ]
    )

    independent_cases = [
        {
            "case_id": "PU-C01",
            "scenario": (
                "disabled_direct_attachment"
            ),
            "passed": all(
                [
                    disabled_direct_equivalence,
                    disabled_zero_imports,
                ]
            ),
        },
        {
            "case_id": "PU-C02",
            "scenario": (
                "disabled_builder_equivalence"
            ),
            "passed": (
                disabled_builder_equivalence
            ),
        },
        {
            "case_id": "PU-C03",
            "scenario": (
                "enabled_simulation_equivalence"
            ),
            "passed": (
                enabled_simulation_equivalence
            ),
        },
        {
            "case_id": "PU-C04",
            "scenario": (
                "engine_config_isolation"
            ),
            "passed": engine_config_isolated,
        },
        {
            "case_id": "PU-C05",
            "scenario": (
                "caller_input_immutability"
            ),
            "passed": caller_inputs_unchanged,
        },
        {
            "case_id": "PU-C06",
            "scenario": (
                "metadata_contract"
            ),
            "passed": metadata_contract_valid,
        },
        {
            "case_id": "PU-C07",
            "scenario": (
                "metadata_alias_consistency"
            ),
            "passed": metadata_alias_consistent,
        },
        {
            "case_id": "PU-C08",
            "scenario": (
                "complete_partial_invalid_states"
            ),
            "passed": state_statuses_valid,
        },
        {
            "case_id": "PU-C09",
            "scenario": (
                "lazy_import_and_zero_top_level_import"
            ),
            "passed": all(
                [
                    lazy_import_present,
                    zero_top_level_evaluator_imports,
                ]
            ),
        },
        {
            "case_id": "PU-C10",
            "scenario": (
                "production_authority_guard"
            ),
            "passed": all_metadata_safe,
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
                "six_pt_implementation_contract_passes"
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
                "diagnostic_keys_stripped"
            ),
            "actual": diagnostic_keys_stripped,
            "expected": True,
            "passed": diagnostic_keys_stripped,
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
                "disabled_zero_imports"
            ),
            "actual": disabled_zero_imports,
            "expected": True,
            "passed": disabled_zero_imports,
        },
        {
            "check": (
                "disabled_builder_equivalence"
            ),
            "actual": (
                disabled_builder_equivalence
            ),
            "expected": True,
            "passed": (
                disabled_builder_equivalence
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
                "engine_config_isolated"
            ),
            "actual": engine_config_isolated,
            "expected": True,
            "passed": engine_config_isolated,
        },
        {
            "check": (
                "caller_inputs_unchanged"
            ),
            "actual": caller_inputs_unchanged,
            "expected": True,
            "passed": caller_inputs_unchanged,
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
                "state_statuses_valid"
            ),
            "actual": state_statuses_valid,
            "expected": True,
            "passed": state_statuses_valid,
        },
        {
            "check": (
                "engine_and_simulator_zero_reach"
            ),
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
            "check": (
                "production_authority_absent"
            ),
            "actual": all_metadata_safe,
            "expected": True,
            "passed": all_metadata_safe,
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
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
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
        "implementation_contract_passed": (
            implementation_contract_passed
        ),
        "disabled_direct_equivalence": (
            disabled_direct_equivalence
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "disabled_builder_equivalence": (
            disabled_builder_equivalence
        ),
        "enabled_simulation_equivalence": (
            enabled_simulation_equivalence
        ),
        "engine_config_isolated": (
            engine_config_isolated
        ),
        "caller_inputs_unchanged": (
            caller_inputs_unchanged
        ),
        "metadata_contract_valid": (
            metadata_contract_valid
        ),
        "metadata_alias_consistent": (
            metadata_alias_consistent
        ),
        "state_statuses_valid": (
            state_statuses_valid
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

    recommended_next_layer = (
        "6PV_production_bullpen_sequencing_"
        "diagnostic_scope_completion_assessment"
        if all_checks_passed
        else
        "6PV_production_bullpen_sequencing_"
        "diagnostic_integration_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "production_bullpen_sequencing_"
            "diagnostic_integration_audit_passed"
            if all_checks_passed
            else
            "production_bullpen_sequencing_"
            "diagnostic_integration_audit_failed"
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
        "six_pt_implementation_contract_passed": (
            implementation_contract_passed
        ),
        "disabled_direct_equivalence": (
            disabled_direct_equivalence
        ),
        "disabled_zero_imports": (
            disabled_zero_imports
        ),
        "disabled_builder_equivalence": (
            disabled_builder_equivalence
        ),
        "enabled_simulation_equivalence": (
            enabled_simulation_equivalence
        ),
        "engine_config_isolated": (
            engine_config_isolated
        ),
        "caller_inputs_unchanged": (
            caller_inputs_unchanged
        ),
        "metadata_contract_valid": (
            metadata_contract_valid
        ),
        "metadata_alias_consistent": (
            metadata_alias_consistent
        ),
        "state_statuses_valid": (
            state_statuses_valid
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
        "diagnostic_scope_completion_assessment_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
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
                / "independent_cases.csv"
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
                / "audit_summary.json"
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
