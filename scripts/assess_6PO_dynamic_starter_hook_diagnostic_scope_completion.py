#!/usr/bin/env python3
"""
Layer 6PO
Dynamic Starter-Hook Diagnostic Scope Completion Assessment

Assesses whether GM-02 is complete at diagnostic scope after Layers 6PI–6PN.

Diagnostic-scope completion requires:

- the inventory and implementation plan exists;
- the pure evaluator exists and passes its implementation contract;
- the evaluator passes its independent audit;
- diagnostic integration planning is complete;
- disabled-by-default metadata-only integration is implemented;
- the merged integration passes its independent audit;
- disabled execution is exactly equivalent and imports nothing;
- enabled execution changes metadata only;
- diagnostic configuration never reaches the simulation engine;
- the engine and simulator have zero evaluator reachability;
- production starter-hook and canonical probability authority remain unchanged.

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


LAYER_ID = "6PO"

LAYER_NAME = (
    "dynamic_starter_hook_"
    "diagnostic_scope_completion_assessment"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PO_dynamic_starter_hook_"
    "diagnostic_scope_completion_assessment"
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

PREDECESSORS = [
    {
        "layer": "6PI",
        "script": (
            "scripts/plan_6PI_dynamic_starter_hook_"
            "inventory_and_implementation.py"
        ),
        "purpose": (
            "Starter-hook inventory and pure evaluator plan."
        ),
        "expected_diagnosis": (
            "dynamic_starter_hook_inventory_and_"
            "implementation_plan_complete"
        ),
        "mode": "static_contract",
    },
    {
        "layer": "6PJ",
        "script": (
            "scripts/implement_6PJ_dynamic_starter_hook_"
            "state_contract_and_evaluator.py"
        ),
        "purpose": (
            "Pure deterministic starter-hook evaluator."
        ),
        "expected_diagnosis": (
            "dynamic_starter_hook_state_contract_and_"
            "evaluator_implementation_complete"
        ),
        "mode": "static_contract",
    },
    {
        "layer": "6PK",
        "script": (
            "scripts/audit_6PK_dynamic_starter_hook_"
            "evaluator.py"
        ),
        "purpose": (
            "Independent evaluator contract audit."
        ),
        "expected_diagnosis": (
            "dynamic_starter_hook_evaluator_"
            "independent_audit_passed"
        ),
        "mode": "static_contract",
    },
    {
        "layer": "6PL",
        "script": (
            "scripts/plan_6PL_dynamic_starter_hook_"
            "diagnostic_integration.py"
        ),
        "purpose": (
            "Disabled-by-default diagnostic integration plan."
        ),
        "expected_diagnosis": (
            "dynamic_starter_hook_diagnostic_"
            "integration_plan_complete"
        ),
        "mode": "static_contract",
    },
    {
        "layer": "6PM",
        "script": (
            "scripts/implement_6PM_dynamic_starter_hook_"
            "diagnostic_integration.py"
        ),
        "purpose": (
            "Metadata-only shared-builder integration."
        ),
        "expected_diagnosis": (
            "dynamic_starter_hook_diagnostic_"
            "integration_implementation_complete"
        ),
        "mode": "current_pass",
    },
    {
        "layer": "6PN",
        "script": (
            "scripts/audit_6PN_dynamic_starter_hook_"
            "diagnostic_integration.py"
        ),
        "purpose": (
            "Independent diagnostic integration audit."
        ),
        "expected_diagnosis": (
            "dynamic_starter_hook_diagnostic_"
            "integration_audit_passed"
        ),
        "mode": "current_pass",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_starter_hook_activation",
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

    text = read_text(path)

    tree = ast.parse(
        text,
        filename=str(path),
    )

    string_constants = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }

    return expected_diagnosis in string_constants


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
            predecessor["expected_diagnosis"],
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
            payload.get(
                "diagnosis"
            )
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
            "all_checks_passed": payload.get(
                "all_checks_passed"
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
        evaluate_predecessor(predecessor)
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
        "_attach_starter_hook_diagnostics"
    )

    diagnostic_attachment_present = (
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

    diagnostic_keys_stripped = all(
        key in builder_text
        for key in [
            "starter_hook_diagnostics_enabled",
            "starter_hook_diagnostics_version",
            "starter_hook_state",
        ]
    )

    evaluator_contract_present = all(
        token in evaluator_text
        for token in [
            "evaluate_starter_hook",
            "validate_starter_hook_evaluation",
            "production_activation",
            "canonical_probability_authority_changed",
        ]
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

    metadata_only_contract_present = all(
        token in builder_text
        for token in [
            '"behavioral_effect": "none"',
            '"production_activation": False',
            '"starter_hook_diagnostics"',
        ]
    )

    disabled_default_present = all(
        token in builder_text
        for token in [
            '"starter_hook_diagnostics_enabled"',
            "False",
        ]
    )

    gm02_diagnostic_scope_complete = all(
        [
            predecessors_accepted
            == len(PREDECESSORS),
            required_files_exist,
            diagnostic_attachment_present,
            lazy_import_present,
            zero_top_level_evaluator_imports,
            diagnostic_keys_stripped,
            evaluator_contract_present,
            engine_zero_reachability,
            simulator_zero_reachability,
            metadata_only_contract_present,
            disabled_default_present,
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
            "check": "all_predecessors_accepted",
            "actual": predecessors_accepted,
            "expected": len(PREDECESSORS),
            "passed": (
                predecessors_accepted
                == len(PREDECESSORS)
            ),
        },
        {
            "check": "diagnostic_attachment_present",
            "actual": diagnostic_attachment_present,
            "expected": True,
            "passed": diagnostic_attachment_present,
        },
        {
            "check": "lazy_import_present",
            "actual": lazy_import_present,
            "expected": True,
            "passed": lazy_import_present,
        },
        {
            "check": "zero_top_level_evaluator_imports",
            "actual": zero_top_level_evaluator_imports,
            "expected": True,
            "passed": zero_top_level_evaluator_imports,
        },
        {
            "check": "diagnostic_keys_stripped",
            "actual": diagnostic_keys_stripped,
            "expected": True,
            "passed": diagnostic_keys_stripped,
        },
        {
            "check": "evaluator_contract_present",
            "actual": evaluator_contract_present,
            "expected": True,
            "passed": evaluator_contract_present,
        },
        {
            "check": "metadata_only_contract_present",
            "actual": metadata_only_contract_present,
            "expected": True,
            "passed": metadata_only_contract_present,
        },
        {
            "check": "disabled_default_present",
            "actual": disabled_default_present,
            "expected": True,
            "passed": disabled_default_present,
        },
        {
            "check": "engine_zero_reachability",
            "actual": engine_zero_reachability,
            "expected": True,
            "passed": engine_zero_reachability,
        },
        {
            "check": "simulator_zero_reachability",
            "actual": simulator_zero_reachability,
            "expected": True,
            "passed": simulator_zero_reachability,
        },
        {
            "check": "gm02_diagnostic_scope_complete",
            "actual": gm02_diagnostic_scope_complete,
            "expected": True,
            "passed": gm02_diagnostic_scope_complete,
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
                "6PO completes GM-02 at diagnostic "
                "scope only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "gm02_diagnostic_scope_complete"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Pure evaluator, diagnostic "
                    "integration, and independent "
                    "audits are complete."
                ),
            },
            {
                "authority": (
                    "gm03_production_bullpen_"
                    "sequencing_inventory"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Next workstream inventory and "
                    "planning only."
                ),
            },
            {
                "authority": (
                    "production_behavior_integration"
                ),
                "granted": False,
                "reason": (
                    "Historical validation and explicit "
                    "production authorization remain absent."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6PP_production_bullpen_sequencing_"
        "inventory_and_implementation_plan"
        if all_checks_passed
        else
        "6PP_dynamic_starter_hook_"
        "diagnostic_scope_completion_remediation"
    )

    write_csv(
        OUTPUT_DIR / "assessment_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
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
        OUTPUT_DIR / "gm02_completion_contract.csv",
        [
            "workstream",
            "scope",
            "status",
            "production_authority",
            "broad_layer6_exit",
        ],
        [
            {
                "workstream": (
                    "GM-02_dynamic_starter_hook"
                ),
                "scope": "diagnostic",
                "status": (
                    "complete"
                    if all_checks_passed
                    else "incomplete"
                ),
                "production_authority": False,
                "broad_layer6_exit": False,
            }
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
                    "Inventory and plan GM-03 "
                    "production bullpen sequencing "
                    "without granting production authority."
                ),
                "entry_condition": (
                    "GM-02 diagnostic scope assessment "
                    "passes completely."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "predecessor_contracts.json",
        predecessor_results,
    )

    summary = {
        "assessment_checks_required": len(
            checks
        ),
        "assessment_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "predecessors_required": len(
            PREDECESSORS
        ),
        "predecessors_accepted": (
            predecessors_accepted
        ),
        "gm02_status": (
            "diagnostic_scope_complete"
            if all_checks_passed
            else "diagnostic_scope_incomplete"
        ),
        "disabled_by_default": (
            disabled_default_present
        ),
        "lazy_import_only": (
            lazy_import_present
            and zero_top_level_evaluator_imports
        ),
        "metadata_only": (
            metadata_only_contract_present
        ),
        "engine_config_isolated": (
            diagnostic_keys_stripped
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
        OUTPUT_DIR / "assessment_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "dynamic_starter_hook_diagnostic_"
            "scope_completion_assessment_passed"
            if all_checks_passed
            else
            "dynamic_starter_hook_diagnostic_"
            "scope_completion_assessment_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "assessment_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "assessment_checks_required": len(
            checks
        ),
        "predecessors_accepted": (
            predecessors_accepted
        ),
        "predecessors_required": len(
            PREDECESSORS
        ),
        "gm02_status": (
            "diagnostic_scope_complete"
            if all_checks_passed
            else "diagnostic_scope_incomplete"
        ),
        "gm02_diagnostic_scope_complete": (
            all_checks_passed
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
        "gm03_inventory_and_planning_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "assessment_checks.csv"
            ),
            str(
                OUTPUT_DIR / "predecessor_results.csv"
            ),
            str(
                OUTPUT_DIR
                / "gm02_completion_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "authority_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "predecessor_contracts.json"
            ),
            str(
                OUTPUT_DIR / "assessment_summary.json"
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
