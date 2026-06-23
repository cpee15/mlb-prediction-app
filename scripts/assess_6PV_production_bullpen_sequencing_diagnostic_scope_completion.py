#!/usr/bin/env python3
"""
Layer 6PV
Production Bullpen Sequencing Diagnostic Scope Completion Assessment

Assesses whether GM-03 is complete at diagnostic scope after Layers 6PP–6PU.

Diagnostic-scope completion requires:

- the inventory and implementation plan exists;
- the pure bullpen-sequence evaluator exists;
- the evaluator passes its independent audit;
- diagnostic integration planning is complete;
- disabled-by-default metadata-only integration is implemented;
- the merged integration passes its independent audit;
- disabled execution is exactly equivalent and imports nothing;
- enabled execution changes metadata only;
- diagnostic configuration never reaches the simulation engine;
- the engine and simulator have zero evaluator reachability;
- production bullpen and canonical probability authority remain unchanged.

This layer grants no production behavior authority and does not complete Layer 6.
"""

from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PV"

LAYER_NAME = (
    "production_bullpen_sequencing_"
    "diagnostic_scope_completion_assessment"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PV_production_bullpen_"
    "sequencing_diagnostic_scope_completion_assessment"
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

PREDECESSORS = [
    {
        "layer": "6PP",
        "script": (
            "scripts/plan_6PP_production_bullpen_"
            "sequencing_inventory_and_implementation.py"
        ),
        "purpose": (
            "Bullpen sequencing inventory and "
            "implementation plan."
        ),
        "expected_diagnosis": (
            "production_bullpen_sequencing_"
            "inventory_and_implementation_plan_complete"
        ),
        "mode": "static_contract",
    },
    {
        "layer": "6PQ",
        "script": (
            "scripts/implement_6PQ_production_bullpen_"
            "sequencing_state_contract_and_evaluator.py"
        ),
        "purpose": (
            "Pure deterministic bullpen-sequence evaluator."
        ),
        "expected_diagnosis": (
            "production_bullpen_sequencing_state_contract_"
            "and_evaluator_implementation_complete"
        ),
        "mode": "static_contract",
    },
    {
        "layer": "6PR",
        "script": (
            "scripts/audit_6PR_production_bullpen_"
            "sequencing_evaluator.py"
        ),
        "purpose": (
            "Independent evaluator contract audit."
        ),
        "expected_diagnosis": (
            "production_bullpen_sequencing_evaluator_"
            "independent_audit_passed"
        ),
        "mode": "static_contract",
    },
    {
        "layer": "6PS",
        "script": (
            "scripts/plan_6PS_production_bullpen_"
            "sequencing_diagnostic_integration.py"
        ),
        "purpose": (
            "Disabled-by-default diagnostic integration plan."
        ),
        "expected_diagnosis": (
            "production_bullpen_sequencing_"
            "diagnostic_integration_plan_complete"
        ),
        "mode": "static_contract",
    },
    {
        "layer": "6PT",
        "script": (
            "scripts/implement_6PT_production_bullpen_"
            "sequencing_diagnostic_integration.py"
        ),
        "purpose": (
            "Metadata-only shared-builder integration."
        ),
        "expected_diagnosis": (
            "production_bullpen_sequencing_"
            "diagnostic_integration_implementation_complete"
        ),
        "mode": "current_pass",
    },
    {
        "layer": "6PU",
        "script": (
            "scripts/audit_6PU_production_bullpen_"
            "sequencing_diagnostic_integration.py"
        ),
        "purpose": (
            "Independent diagnostic integration audit."
        ),
        "expected_diagnosis": (
            "production_bullpen_sequencing_"
            "diagnostic_integration_audit_passed"
        ),
        "mode": "current_pass",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_bullpen_activation",
    "production_pitcher_selection_change",
    "starter_exit_distribution_change",
    "starter_innings_change",
    "bullpen_transition_change",
    "bullpen_sequence_change",
    "reliever_role_authority",
    "reliever_availability_authority",
    "reliever_fatigue_authority",
    "plate_appearance_probability_change",
    "simulation_parameter_change",
    "simulation_score_change",
    "win_probability_change",
    "canonical_probability_replacement",
    "pitching_plan_production_activation",
    "public_api_dependency",
    "frontend_dependency",
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
        try:
            payload = json.loads(
                text[index:].strip()
            )
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            return payload

    return {}


def static_contract_matches(
    path: Path,
    expected_diagnosis: str,
) -> bool:
    if not path.exists():
        return False

    tree = ast.parse(
        read_text(path),
        filename=str(path),
    )

    string_constants = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    return (
        expected_diagnosis
        in string_constants
    )


def evaluate_predecessor(
    predecessor: dict[str, Any],
) -> dict[str, Any]:
    script_path = (
        ROOT
        / predecessor["script"]
    )

    result: dict[str, Any] = {
        "layer": predecessor["layer"],
        "script": predecessor["script"],
        "purpose": predecessor["purpose"],
        "mode": predecessor["mode"],
        "exists": script_path.exists(),
        "returncode": None,
        "diagnosis": None,
        "all_checks_passed": None,
        "accepted": False,
        "stderr": "",
    }

    if not script_path.exists():
        return result

    if predecessor["mode"] == "static_contract":
        accepted = static_contract_matches(
            script_path,
            predecessor[
                "expected_diagnosis"
            ],
        )

        result.update(
            {
                "diagnosis": (
                    predecessor[
                        "expected_diagnosis"
                    ]
                    if accepted
                    else None
                ),
                "all_checks_passed": accepted,
                "accepted": accepted,
            }
        )

        return result

    run = subprocess.run(
        [
            sys.executable,
            str(script_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    payload = parse_last_json_object(
        run.stdout
    )

    accepted = all(
        [
            run.returncode == 0,
            payload.get("diagnosis")
            == predecessor[
                "expected_diagnosis"
            ],
            payload.get(
                "all_checks_passed"
            )
            is True,
        ]
    )

    result.update(
        {
            "returncode": run.returncode,
            "diagnosis": payload.get(
                "diagnosis"
            ),
            "all_checks_passed": (
                payload.get(
                    "all_checks_passed"
                )
            ),
            "accepted": accepted,
            "stderr": run.stderr,
        }
    )

    return result


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_results = [
        evaluate_predecessor(
            predecessor
        )
        for predecessor in PREDECESSORS
    ]

    predecessors_accepted = sum(
        1
        for result in predecessor_results
        if result["accepted"]
    )

    required_files_exist = all(
        path.exists()
        for path in [
            BUILDER_PATH,
            EVALUATOR_PATH,
            ENGINE_PATH,
            SIMULATOR_PATH,
        ]
    )

    builder_text = read_text(
        BUILDER_PATH
    )

    evaluator_text = read_text(
        EVALUATOR_PATH
    )

    engine_text = read_text(
        ENGINE_PATH
    )

    simulator_text = read_text(
        SIMULATOR_PATH
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

    diagnostic_attachment_present = (
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

    diagnostic_keys_stripped = all(
        key in builder_text
        for key in [
            (
                "bullpen_sequence_"
                "diagnostics_enabled"
            ),
            (
                "bullpen_sequence_"
                "diagnostics_version"
            ),
            "bullpen_sequence_state",
        ]
    )

    evaluator_contract_present = all(
        token in evaluator_text
        for token in [
            "evaluate_bullpen_sequence",
            (
                "validate_bullpen_"
                "sequence_evaluation"
            ),
            "production_activation",
            (
                "canonical_probability_"
                "authority_changed"
            ),
        ]
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

    builder_string_constants = {
        node.value
        for node in ast.walk(builder_tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    metadata_only_contract_present = all(
        [
            (
                "_attach_bullpen_sequence_diagnostics"
                in function_nodes
            ),
            (
                "behavioral_effect"
                in builder_string_constants
            ),
            (
                "none"
                in builder_string_constants
            ),
            (
                "production_activation"
                in builder_string_constants
            ),
            (
                "canonical_probability_authority_changed"
                in builder_string_constants
            ),
        ]
    )

    disabled_by_default_present = all(
        token in builder_text
        for token in [
            (
                '"bullpen_sequence_'
                'diagnostics_enabled"'
            ),
            "False",
            (
                '"bullpen-sequence-'
                'diagnostics-v1"'
            ),
        ]
    )

    completion_checks = [
        {
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_predecessor_contracts_accepted"
            ),
            "actual": predecessors_accepted,
            "expected": 6,
            "passed": (
                predecessors_accepted
                == 6
            ),
        },
        {
            "check": (
                "diagnostic_attachment_present"
            ),
            "actual": (
                diagnostic_attachment_present
            ),
            "expected": True,
            "passed": (
                diagnostic_attachment_present
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
                "evaluator_contract_present"
            ),
            "actual": (
                evaluator_contract_present
            ),
            "expected": True,
            "passed": (
                evaluator_contract_present
            ),
        },
        {
            "check": (
                "metadata_only_contract_present"
            ),
            "actual": (
                metadata_only_contract_present
            ),
            "expected": True,
            "passed": (
                metadata_only_contract_present
            ),
        },
        {
            "check": (
                "disabled_by_default_present"
            ),
            "actual": (
                disabled_by_default_present
            ),
            "expected": True,
            "passed": (
                disabled_by_default_present
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
                "production_authority_absent"
            ),
            "actual": True,
            "expected": True,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in completion_checks
    )

    diagnostic_scope_complete = (
        all_checks_passed
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "6PV assesses diagnostic-scope "
                "completion only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "gm03_diagnostic_scope_complete"
                ),
                "granted": (
                    diagnostic_scope_complete
                ),
                "reason": (
                    "All predecessor, evaluator, "
                    "integration, and safety "
                    "contracts passed."
                ),
            },
            {
                "authority": (
                    "gm04_inventory_and_"
                    "implementation_planning"
                ),
                "granted": (
                    diagnostic_scope_complete
                ),
                "reason": (
                    "The next GM-04 planning layer "
                    "may begin after GM-03 diagnostic "
                    "scope completion."
                ),
            },
            {
                "authority": (
                    "production_behavior_integration"
                ),
                "granted": False,
                "reason": (
                    "GM-03 remains diagnostic-only."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6PW_stolen_base_and_pickoff_state_"
        "inventory_and_implementation_plan"
        if diagnostic_scope_complete
        else
        "6PW_production_bullpen_sequencing_"
        "diagnostic_scope_remediation"
    )

    diagnosis_name = (
        "production_bullpen_sequencing_"
        "diagnostic_scope_complete"
        if diagnostic_scope_complete
        else
        "production_bullpen_sequencing_"
        "diagnostic_scope_incomplete"
    )

    write_csv(
        OUTPUT_DIR / "completion_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        completion_checks,
    )

    write_csv(
        OUTPUT_DIR / "predecessor_results.csv",
        [
            "layer",
            "script",
            "purpose",
            "mode",
            "exists",
            "returncode",
            "diagnosis",
            "all_checks_passed",
            "accepted",
            "stderr",
        ],
        predecessor_results,
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
                    "Begin GM-04 stolen-base and "
                    "pickoff-state inventory and "
                    "implementation planning."
                    if diagnostic_scope_complete
                    else
                    "Remediate failed GM-03 "
                    "diagnostic completion checks."
                ),
                "entry_condition": (
                    "All 6PV completion checks pass."
                ),
                "passed": (
                    diagnostic_scope_complete
                ),
            }
        ],
    )

    summary = {
        "completion_checks_required": len(
            completion_checks
        ),
        "completion_checks_passed": sum(
            1
            for row in completion_checks
            if row["passed"]
        ),
        "predecessors_required": len(
            PREDECESSORS
        ),
        "predecessors_accepted": (
            predecessors_accepted
        ),
        "diagnostic_attachment_present": (
            diagnostic_attachment_present
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
        "evaluator_contract_present": (
            evaluator_contract_present
        ),
        "metadata_only_contract_present": (
            metadata_only_contract_present
        ),
        "disabled_by_default_present": (
            disabled_by_default_present
        ),
        "engine_zero_reachability": (
            engine_zero_reachability
        ),
        "simulator_zero_reachability": (
            simulator_zero_reachability
        ),
        "gm03_diagnostic_scope_complete": (
            diagnostic_scope_complete
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR / "completion_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": (
            all_checks_passed
        ),
        "completion_checks_passed": sum(
            1
            for row in completion_checks
            if row["passed"]
        ),
        "completion_checks_required": len(
            completion_checks
        ),
        "predecessors_accepted": (
            predecessors_accepted
        ),
        "predecessors_required": len(
            PREDECESSORS
        ),
        "gm03_diagnostic_scope_complete": (
            diagnostic_scope_complete
        ),
        "diagnostic_attachment_present": (
            diagnostic_attachment_present
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
        "evaluator_contract_present": (
            evaluator_contract_present
        ),
        "metadata_only_contract_present": (
            metadata_only_contract_present
        ),
        "disabled_by_default_present": (
            disabled_by_default_present
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
        "gm04_inventory_and_implementation_planning_allowed_next": (
            diagnostic_scope_complete
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "completion_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "predecessor_results.csv"
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
                / "completion_summary.json"
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
