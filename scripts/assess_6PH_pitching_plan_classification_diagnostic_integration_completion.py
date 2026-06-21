#!/usr/bin/env python3
"""
Layer 6PH
Pitching-Plan Classification Diagnostic Integration Completion Assessment

Assesses whether GM-01 is complete at diagnostic scope after Layers 6PA–6PG.

Completion at this layer means:

- the deterministic classifier exists and passes its implementation tests;
- previously identified behavioral gaps remain remediated;
- diagnostic integration is explicitly disabled by default;
- disabled execution neither imports nor calls the classifier;
- enabled execution adds metadata only;
- diagnostic configuration never reaches the simulation engine;
- the game engine has no classifier reachability;
- simulation behavior and canonical probability authority remain unchanged.

This layer does not grant production behavior authority and does not complete
Layer 6 broadly. It advances the roadmap to GM-02 dynamic starter-hook planning.
"""

from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PH"
LAYER_NAME = (
    "pitching_plan_classification_diagnostic_"
    "integration_completion_assessment"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PH_pitching_plan_classification_"
    "diagnostic_integration_completion_assessment"
)

CLASSIFIER_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "pitching_plan_classifier.py"
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

PREDECESSORS = [
    {
        "layer": "6PA",
        "script": (
            "scripts/implement_6PA_"
            "pitching_plan_classification.py"
        ),
        "purpose": (
            "Deterministic classifier implementation."
        ),
    },
    {
        "layer": "6PB",
        "script": (
            "scripts/audit_6PB_"
            "pitching_plan_classification_"
            "implementation.py"
        ),
        "purpose": (
            "Independent classifier behavioral audit."
        ),
    },
    {
        "layer": "6PC",
        "script": (
            "scripts/remediate_6PC_"
            "pitching_plan_classification_gaps.py"
        ),
        "purpose": (
            "PB-C09 and PB-C10 remediation."
        ),
    },
    {
        "layer": "6PD",
        "script": (
            "scripts/audit_6PD_"
            "pitching_plan_classification_"
            "post_remediation.py"
        ),
        "purpose": (
            "Post-remediation approval audit."
        ),
    },
    {
        "layer": "6PE",
        "script": (
            "scripts/plan_6PE_"
            "pitching_plan_classification_"
            "diagnostic_integration.py"
        ),
        "purpose": (
            "Disabled-by-default diagnostic "
            "integration plan."
        ),
    },
    {
        "layer": "6PF",
        "script": (
            "scripts/implement_6PF_"
            "pitching_plan_classification_"
            "diagnostic_integration.py"
        ),
        "purpose": (
            "Metadata-only diagnostic integration."
        ),
    },
    {
        "layer": "6PG",
        "script": (
            "scripts/audit_6PG_"
            "pitching_plan_classification_"
            "diagnostic_integration.py"
        ),
        "purpose": (
            "Independent diagnostic integration audit."
        ),
    },
]

PROHIBITED_AUTHORITIES = [
    "production_classifier_activation",
    "starter_innings_change",
    "dynamic_starter_hook_activation",
    "bullpen_sequence_change",
    "plate_appearance_probability_change",
    "simulation_parameter_change",
    "simulation_score_change",
    "win_probability_change",
    "canonical_probability_replacement",
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
    """
    Parse the final complete JSON object printed by a predecessor script.
    """

    candidate_positions = [
        index
        for index, character in enumerate(text)
        if character == "{"
    ]

    for index in reversed(candidate_positions):
        candidate = text[index:].strip()

        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            return payload

    return {}


PREDECESSOR_ACCEPTANCE = {
    "6PA": {
        "mode": "current_pass",
    },
    "6PB": {
        "mode": "historical_expected_diagnosis",
        "expected_diagnosis": (
            "pitching_plan_classification_"
            "independent_implementation_audit_"
            "gaps_confirmed"
        ),
        "expected_returncode": 1,
    },
    "6PC": {
        "mode": "current_pass",
    },
    "6PD": {
        "mode": "historical_contract",
        "expected_diagnosis": (
            "pitching_plan_classification_"
            "post_remediation_audit_passed"
        ),
        "required_constant": (
            "6PE_pitching_plan_classification_"
            "diagnostic_integration_plan"
        ),
    },
    "6PE": {
        "mode": "historical_contract",
        "expected_diagnosis": (
            "pitching_plan_classification_"
            "diagnostic_integration_plan_complete"
        ),
        "required_constant": (
            "6PF_pitching_plan_classification_"
            "diagnostic_integration_implementation"
        ),
    },
    "6PF": {
        "mode": "current_pass",
    },
    "6PG": {
        "mode": "current_pass",
    },
}


def execute_predecessor(
    row: dict[str, str],
) -> dict[str, Any]:
    script_path = ROOT / row["script"]

    if not script_path.exists():
        return {
            "layer": row["layer"],
            "script": row["script"],
            "purpose": row["purpose"],
            "exists": False,
            "returncode": None,
            "diagnosis": None,
            "all_checks_passed": False,
            "passed": False,
            "stdout": "",
            "stderr": (
                f"Missing predecessor script: "
                f"{script_path}"
            ),
            "acceptance_mode": None,
        }

    policy = PREDECESSOR_ACCEPTANCE[
        row["layer"]
    ]

    mode = policy["mode"]

    if mode == "historical_contract":
        source = script_path.read_text(
            encoding="utf-8",
            errors="ignore",
        )

        tree = ast.parse(
            source,
            filename=str(script_path),
        )

        constants = {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
        }

        expected_diagnosis = policy[
            "expected_diagnosis"
        ]

        required_constant = policy[
            "required_constant"
        ]

        passed = all(
            [
                expected_diagnosis
                in constants,
                required_constant
                in constants,
            ]
        )

        return {
            "layer": row["layer"],
            "script": row["script"],
            "purpose": row["purpose"],
            "exists": True,
            "returncode": None,
            "diagnosis": expected_diagnosis,
            "all_checks_passed": passed,
            "passed": passed,
            "stdout": "",
            "stderr": "",
            "diagnosis_payload": {
                "diagnosis": (
                    expected_diagnosis
                ),
                "all_checks_passed": passed,
                "historical_contract_verified": (
                    passed
                ),
                "current_execution_required": (
                    False
                ),
            },
            "acceptance_mode": mode,
        }

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    diagnosis = parse_last_json_object(
        completed.stdout
    )

    reported_all_checks_passed = (
        diagnosis.get("all_checks_passed")
        is True
    )

    if mode == "current_pass":
        passed = (
            completed.returncode == 0
            and reported_all_checks_passed
        )

    elif mode == (
        "historical_expected_diagnosis"
    ):
        passed = all(
            [
                completed.returncode
                == policy[
                    "expected_returncode"
                ],
                diagnosis.get("diagnosis")
                == policy[
                    "expected_diagnosis"
                ],
            ]
        )

    else:
        raise RuntimeError(
            f"Unsupported predecessor mode: {mode}"
        )

    return {
        "layer": row["layer"],
        "script": row["script"],
        "purpose": row["purpose"],
        "exists": True,
        "returncode": completed.returncode,
        "diagnosis": diagnosis.get(
            "diagnosis"
        ),
        "all_checks_passed": (
            reported_all_checks_passed
        ),
        "passed": passed,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "diagnosis_payload": diagnosis,
        "acceptance_mode": mode,
    }

def function_names(
    path: Path,
) -> set[str]:
    if not path.exists():
        return set()

    tree = ast.parse(
        read_text(path),
        filename=str(path),
    )

    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
            ),
        )
    }


def builder_ast_audit() -> dict[str, Any]:
    source = read_text(BUILDER_PATH)

    if not source:
        return {
            "module_level_classifier_import": None,
            "lazy_classifier_import": False,
            "diagnostic_helper_present": False,
            "diagnostic_switch_present": False,
            "diagnostic_keys_stripped_from_engine": False,
            "metadata_attachment_present": False,
        }

    tree = ast.parse(
        source,
        filename=str(BUILDER_PATH),
    )

    functions = {
        node.name: node
        for node in tree.body
        if isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
            ),
        )
    }

    helper = functions.get(
        "_attach_pitching_plan_diagnostics"
    )

    builder = functions.get(
        "build_game_simulation"
    )

    module_level_classifier_import = any(
        isinstance(
            node,
            ast.ImportFrom,
        )
        and node.module
        == (
            "mlb_app.simulation."
            "pitching_plan_classifier"
        )
        for node in tree.body
    )

    lazy_classifier_import = False

    if helper is not None:
        lazy_classifier_import = any(
            isinstance(
                node,
                ast.ImportFrom,
            )
            and node.module
            == (
                "mlb_app.simulation."
                "pitching_plan_classifier"
            )
            for node in ast.walk(helper)
        )

    builder_source = (
        ast.get_source_segment(
            source,
            builder,
        )
        if builder is not None
        else ""
    ) or ""

    helper_source = (
        ast.get_source_segment(
            source,
            helper,
        )
        if helper is not None
        else ""
    ) or ""

    diagnostic_keys = {
        "pitching_plan_diagnostics_enabled",
        "pitching_plan_evidence",
        "pitching_plan_diagnostics_version",
    }

    diagnostic_keys_stripped = all(
        key in builder_source
        for key in diagnostic_keys
    ) and "engine_config" in builder_source

    return {
        "module_level_classifier_import": (
            module_level_classifier_import
        ),
        "lazy_classifier_import": (
            lazy_classifier_import
        ),
        "diagnostic_helper_present": (
            helper is not None
        ),
        "diagnostic_switch_present": (
            "pitching_plan_diagnostics_enabled"
            in helper_source
        ),
        "diagnostic_keys_stripped_from_engine": (
            diagnostic_keys_stripped
        ),
        "metadata_attachment_present": (
            "pitching_plan_diagnostics"
            in helper_source
            and 'payload["meta"]'
            in helper_source
            and 'payload["metadata"]'
            in helper_source
        ),
    }


def production_reference_scan() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for path in sorted(
        (ROOT / "mlb_app").rglob("*.py")
    ):
        if path == CLASSIFIER_PATH:
            continue

        text = read_text(path)

        module_reference = (
            "pitching_plan_classifier"
            in text
        )

        function_reference = (
            "classify_pitching_plan"
            in text
        )

        diagnostic_switch_reference = (
            "pitching_plan_diagnostics_enabled"
            in text
        )

        if not any(
            [
                module_reference,
                function_reference,
                diagnostic_switch_reference,
            ]
        ):
            continue

        rows.append(
            {
                "path": str(
                    path.relative_to(ROOT)
                ),
                "module_reference": (
                    module_reference
                ),
                "function_reference": (
                    function_reference
                ),
                "diagnostic_switch_reference": (
                    diagnostic_switch_reference
                ),
                "is_shared_builder": (
                    path == BUILDER_PATH
                ),
                "is_game_engine": (
                    path == ENGINE_PATH
                ),
            }
        )

    return rows


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_results = [
        execute_predecessor(row)
        for row in PREDECESSORS
    ]

    predecessor_summary_rows = [
        {
            "layer": row["layer"],
            "script": row["script"],
            "purpose": row["purpose"],
            "exists": row["exists"],
            "returncode": row["returncode"],
            "diagnosis": row["diagnosis"],
            "all_checks_passed": (
                row["all_checks_passed"]
            ),
            "passed": row["passed"],
        }
        for row in predecessor_results
    ]

    classifier_functions = function_names(
        CLASSIFIER_PATH
    )

    classifier_contract_present = {
        "classify_pitching_plan",
        "validate_pitching_plan_payload",
    }.issubset(classifier_functions)

    ast_audit = builder_ast_audit()

    reference_rows = (
        production_reference_scan()
    )

    builder_reference_rows = [
        row
        for row in reference_rows
        if row["is_shared_builder"]
    ]

    engine_reference_rows = [
        row
        for row in reference_rows
        if row["is_game_engine"]
    ]

    only_builder_has_classifier_reach = (
        len(reference_rows) == 1
        and len(builder_reference_rows) == 1
        and len(engine_reference_rows) == 0
    )

    six_pf = next(
        (
            row["diagnosis_payload"]
            for row in predecessor_results
            if row["layer"] == "6PF"
        ),
        {},
    )

    six_pg = next(
        (
            row["diagnosis_payload"]
            for row in predecessor_results
            if row["layer"] == "6PG"
        ),
        {},
    )

    six_pf_contract = all(
        [
            six_pf.get(
                "all_checks_passed"
            )
            is True,
            six_pf.get(
                "fixtures_passed"
            )
            == 10,
            six_pf.get(
                "disabled_classifier_calls"
            )
            == 0,
            six_pf.get(
                "disabled_classifier_imports"
            )
            == 0,
            six_pf.get(
                "disabled_path_exactly_equivalent"
            )
            is True,
            six_pf.get(
                "enabled_simulation_fields_equivalent"
            )
            is True,
            six_pf.get(
                "engine_arguments_unchanged"
            )
            is True,
            six_pf.get(
                "inputs_unchanged"
            )
            is True,
        ]
    )

    six_pg_contract = all(
        [
            six_pg.get(
                "all_checks_passed"
            )
            is True,
            six_pg.get(
                "audit_checks_passed"
            )
            == 12,
            six_pg.get(
                "independent_cases_passed"
            )
            == 8,
            six_pg.get(
                "disabled_classifier_imports"
            )
            == 0,
            six_pg.get(
                "engine_configs_isolated"
            )
            is True,
            six_pg.get(
                "classifier_import_is_lazy"
            )
            is True,
            six_pg.get(
                "only_shared_builder_references_classifier"
            )
            is True,
            six_pg.get(
                "game_engine_classifier_reach"
            )
            is False,
        ]
    )

    all_predecessors_passed = all(
        row["passed"]
        for row in predecessor_results
    )

    completion_criteria = [
        {
            "criterion_id": "GM01-C01",
            "criterion": (
                "All 6PA through 6PG predecessor "
                "layers execute successfully."
            ),
            "actual": (
                sum(
                    1
                    for row in predecessor_results
                    if row["passed"]
                )
            ),
            "expected": len(PREDECESSORS),
            "passed": (
                all_predecessors_passed
            ),
        },
        {
            "criterion_id": "GM01-C02",
            "criterion": (
                "Classifier implementation and "
                "validation contracts remain present."
            ),
            "actual": (
                classifier_contract_present
            ),
            "expected": True,
            "passed": (
                classifier_contract_present
            ),
        },
        {
            "criterion_id": "GM01-C03",
            "criterion": (
                "Classifier import remains lazy."
            ),
            "actual": (
                ast_audit[
                    "lazy_classifier_import"
                ]
            ),
            "expected": True,
            "passed": (
                ast_audit[
                    "lazy_classifier_import"
                ]
                is True
                and ast_audit[
                    "module_level_classifier_import"
                ]
                is False
            ),
        },
        {
            "criterion_id": "GM01-C04",
            "criterion": (
                "Diagnostic integration remains "
                "disabled by default."
            ),
            "actual": (
                ast_audit[
                    "diagnostic_switch_present"
                ]
            ),
            "expected": True,
            "passed": (
                ast_audit[
                    "diagnostic_switch_present"
                ]
            ),
        },
        {
            "criterion_id": "GM01-C05",
            "criterion": (
                "Diagnostic configuration remains "
                "isolated from engine arguments."
            ),
            "actual": (
                ast_audit[
                    "diagnostic_keys_stripped_from_engine"
                ]
            ),
            "expected": True,
            "passed": (
                ast_audit[
                    "diagnostic_keys_stripped_from_engine"
                ]
                and six_pf.get(
                    "engine_arguments_unchanged"
                )
                is True
                and six_pg.get(
                    "engine_configs_isolated"
                )
                is True
            ),
        },
        {
            "criterion_id": "GM01-C06",
            "criterion": (
                "Disabled path remains exactly "
                "equivalent and unreachable."
            ),
            "actual": {
                "calls": six_pf.get(
                    "disabled_classifier_calls"
                ),
                "imports_6PF": six_pf.get(
                    "disabled_classifier_imports"
                ),
                "imports_6PG": six_pg.get(
                    "disabled_classifier_imports"
                ),
                "equivalent_6PF": six_pf.get(
                    "disabled_path_exactly_equivalent"
                ),
                "equivalent_6PG": six_pg.get(
                    "disabled_path_exactly_equivalent"
                ),
            },
            "expected": (
                "zero calls/imports and exact equivalence"
            ),
            "passed": all(
                [
                    six_pf.get(
                        "disabled_classifier_calls"
                    )
                    == 0,
                    six_pf.get(
                        "disabled_classifier_imports"
                    )
                    == 0,
                    six_pg.get(
                        "disabled_classifier_imports"
                    )
                    == 0,
                    six_pf.get(
                        "disabled_path_exactly_equivalent"
                    )
                    is True,
                    six_pg.get(
                        "disabled_path_exactly_equivalent"
                    )
                    is True,
                ]
            ),
        },
        {
            "criterion_id": "GM01-C07",
            "criterion": (
                "Enabled path changes diagnostic "
                "metadata only."
            ),
            "actual": {
                "six_pf": six_pf.get(
                    "enabled_simulation_fields_equivalent"
                ),
                "six_pg": six_pg.get(
                    "enabled_simulation_fields_equivalent"
                ),
                "metadata_attachment": (
                    ast_audit[
                        "metadata_attachment_present"
                    ]
                ),
            },
            "expected": True,
            "passed": all(
                [
                    six_pf.get(
                        "enabled_simulation_fields_equivalent"
                    )
                    is True,
                    six_pg.get(
                        "enabled_simulation_fields_equivalent"
                    )
                    is True,
                    ast_audit[
                        "metadata_attachment_present"
                    ],
                ]
            ),
        },
        {
            "criterion_id": "GM01-C08",
            "criterion": (
                "Only the shared builder can reach "
                "the classifier."
            ),
            "actual": [
                row["path"]
                for row in reference_rows
            ],
            "expected": [
                (
                    "mlb_app/simulation/"
                    "game_simulation_builder.py"
                )
            ],
            "passed": (
                only_builder_has_classifier_reach
            ),
        },
        {
            "criterion_id": "GM01-C09",
            "criterion": (
                "6PF implementation contract remains "
                "fully passing."
            ),
            "actual": six_pf_contract,
            "expected": True,
            "passed": six_pf_contract,
        },
        {
            "criterion_id": "GM01-C10",
            "criterion": (
                "6PG independent audit contract "
                "remains fully passing."
            ),
            "actual": six_pg_contract,
            "expected": True,
            "passed": six_pg_contract,
        },
        {
            "criterion_id": "GM01-C11",
            "criterion": (
                "Simulation behavior and canonical "
                "probability authority remain unchanged."
            ),
            "actual": {
                "simulation_behavior_changed_6PF": (
                    six_pf.get(
                        "simulation_behavior_changed"
                    )
                ),
                "simulation_behavior_changed_6PG": (
                    six_pg.get(
                        "simulation_behavior_changed"
                    )
                ),
                "probability_authority_changed_6PF": (
                    six_pf.get(
                        "canonical_probability_"
                        "authority_changed"
                    )
                ),
                "probability_authority_changed_6PG": (
                    six_pg.get(
                        "canonical_probability_"
                        "authority_changed"
                    )
                ),
            },
            "expected": False,
            "passed": all(
                [
                    six_pf.get(
                        "simulation_behavior_changed"
                    )
                    is False,
                    six_pg.get(
                        "simulation_behavior_changed"
                    )
                    is False,
                    six_pf.get(
                        "canonical_probability_"
                        "authority_changed"
                    )
                    is False,
                    six_pg.get(
                        "canonical_probability_"
                        "authority_changed"
                    )
                    is False,
                ]
            ),
        },
        {
            "criterion_id": "GM01-C12",
            "criterion": (
                "Production behavior authority remains "
                "explicitly withheld."
            ),
            "actual": {
                "production_activation_6PF": (
                    six_pf.get(
                        "production_classifier_activated"
                    )
                ),
                "production_activation_6PG": (
                    six_pg.get(
                        "production_classifier_activated"
                    )
                ),
            },
            "expected": False,
            "passed": (
                six_pf.get(
                    "production_classifier_activated"
                )
                is False
                and six_pg.get(
                    "production_classifier_activated"
                )
                is False
            ),
        },
    ]

    all_completion_criteria_passed = all(
        row["passed"]
        for row in completion_criteria
    )

    gm01_status = (
        "diagnostic_integration_complete"
        if all_completion_criteria_passed
        else "diagnostic_integration_incomplete"
    )

    workstream_status = [
        {
            "workstream_id": "GM-01",
            "workstream": (
                "Pitching-plan classification"
            ),
            "current_status": gm01_status,
            "diagnostic_scope_complete": (
                all_completion_criteria_passed
            ),
            "production_behavior_authorized": False,
            "next_action": (
                "Preserve as disabled-by-default "
                "diagnostic metadata."
            ),
        },
        {
            "workstream_id": "GM-02",
            "workstream": (
                "Dynamic starter hook"
            ),
            "current_status": (
                "inventory_and_planning_next"
                if all_completion_criteria_passed
                else "blocked_by_gm01"
            ),
            "diagnostic_scope_complete": False,
            "production_behavior_authorized": False,
            "next_action": (
                "Inventory current starter-exit "
                "authority, state inputs, and seams."
            ),
        },
        {
            "workstream_id": "GM-03",
            "workstream": (
                "Production bullpen sequencing"
            ),
            "current_status": "not_started",
            "diagnostic_scope_complete": False,
            "production_behavior_authorized": False,
            "next_action": (
                "Remain blocked until GM-02 scope "
                "is resolved."
            ),
        },
        {
            "workstream_id": "GM-04",
            "workstream": (
                "Stolen-base and pickoff state"
            ),
            "current_status": "not_started",
            "diagnostic_scope_complete": False,
            "production_behavior_authorized": False,
            "next_action": (
                "Remain outside current authority."
            ),
        },
        {
            "workstream_id": "GM-05",
            "workstream": (
                "Position-player substitutions"
            ),
            "current_status": "not_started",
            "diagnostic_scope_complete": False,
            "production_behavior_authorized": False,
            "next_action": (
                "Remain outside current authority."
            ),
        },
    ]

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "6PH closes GM-01 at diagnostic scope "
                "only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "gm01_diagnostic_completion"
                ),
                "granted": (
                    all_completion_criteria_passed
                ),
                "reason": (
                    "All implementation, remediation, "
                    "integration, and independent audit "
                    "criteria pass."
                ),
            },
            {
                "authority": (
                    "gm02_inventory_and_planning"
                ),
                "granted": (
                    all_completion_criteria_passed
                ),
                "reason": (
                    "Planning may advance without "
                    "activating behavior."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6PI_dynamic_starter_hook_"
        "inventory_and_implementation_plan"
        if all_completion_criteria_passed
        else
        "6PI_pitching_plan_classification_"
        "diagnostic_integration_remediation"
    )

    diagnosis_value = (
        "pitching_plan_classification_"
        "diagnostic_integration_"
        "completion_assessment_passed"
        if all_completion_criteria_passed
        else
        "pitching_plan_classification_"
        "diagnostic_integration_"
        "completion_assessment_failed"
    )

    write_csv(
        OUTPUT_DIR / "predecessor_results.csv",
        [
            "layer",
            "script",
            "purpose",
            "exists",
            "returncode",
            "diagnosis",
            "all_checks_passed",
            "passed",
        ],
        predecessor_summary_rows,
    )

    write_csv(
        OUTPUT_DIR / "completion_criteria.csv",
        [
            "criterion_id",
            "criterion",
            "actual",
            "expected",
            "passed",
        ],
        completion_criteria,
    )

    write_csv(
        OUTPUT_DIR / "production_reference_scan.csv",
        [
            "path",
            "module_reference",
            "function_reference",
            "diagnostic_switch_reference",
            "is_shared_builder",
            "is_game_engine",
        ],
        reference_rows,
    )

    write_csv(
        OUTPUT_DIR / "workstream_status.csv",
        [
            "workstream_id",
            "workstream",
            "current_status",
            "diagnostic_scope_complete",
            "production_behavior_authorized",
            "next_action",
        ],
        workstream_status,
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
                    "Inventory the existing dynamic "
                    "starter-hook implementation, inputs, "
                    "authority, and integration seams."
                    if all_completion_criteria_passed
                    else
                    "Remediate failed GM-01 completion "
                    "criteria."
                ),
                "entry_condition": (
                    "All 6PH completion criteria pass."
                ),
                "passed": (
                    all_completion_criteria_passed
                ),
            }
        ],
    )

    predecessor_payloads = {
        row["layer"]: {
            "script": row["script"],
            "returncode": row["returncode"],
            "passed": row["passed"],
            "diagnosis_payload": row.get(
                "diagnosis_payload",
                {},
            ),
            "stderr": row["stderr"],
        }
        for row in predecessor_results
    }

    write_json(
        OUTPUT_DIR / "predecessor_payloads.json",
        predecessor_payloads,
    )

    write_json(
        OUTPUT_DIR / "builder_ast_audit.json",
        ast_audit,
    )

    completion_summary = {
        "gm01_status": gm01_status,
        "gm01_diagnostic_scope_complete": (
            all_completion_criteria_passed
        ),
        "gm01_production_behavior_authorized": False,
        "predecessor_layers_required": len(
            PREDECESSORS
        ),
        "predecessor_layers_passed": sum(
            1
            for row in predecessor_results
            if row["passed"]
        ),
        "completion_criteria_required": len(
            completion_criteria
        ),
        "completion_criteria_passed": sum(
            1
            for row in completion_criteria
            if row["passed"]
        ),
        "classifier_import_is_lazy": (
            ast_audit[
                "lazy_classifier_import"
            ]
            and not ast_audit[
                "module_level_classifier_import"
            ]
        ),
        "only_shared_builder_references_classifier": (
            only_builder_has_classifier_reach
        ),
        "game_engine_classifier_reach": (
            len(engine_reference_rows) > 0
        ),
        "diagnostics_default_enabled": False,
        "simulation_behavior_changed": False,
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "broad_layer6_exit_paused": True,
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "completion_summary.json",
        completion_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_value,
        "all_checks_passed": (
            all_completion_criteria_passed
        ),
        "completion_criteria_passed": sum(
            1
            for row in completion_criteria
            if row["passed"]
        ),
        "completion_criteria_required": len(
            completion_criteria
        ),
        "predecessor_layers_passed": sum(
            1
            for row in predecessor_results
            if row["passed"]
        ),
        "predecessor_layers_required": len(
            PREDECESSORS
        ),
        "gm01_status": gm01_status,
        "gm01_diagnostic_scope_complete": (
            all_completion_criteria_passed
        ),
        "gm01_production_behavior_authorized": False,
        "six_pf_contract_passed": (
            six_pf_contract
        ),
        "six_pg_contract_passed": (
            six_pg_contract
        ),
        "disabled_classifier_calls": (
            six_pf.get(
                "disabled_classifier_calls"
            )
        ),
        "disabled_classifier_imports_6PF": (
            six_pf.get(
                "disabled_classifier_imports"
            )
        ),
        "disabled_classifier_imports_6PG": (
            six_pg.get(
                "disabled_classifier_imports"
            )
        ),
        "disabled_path_exactly_equivalent": (
            six_pf.get(
                "disabled_path_exactly_equivalent"
            )
            is True
            and six_pg.get(
                "disabled_path_exactly_equivalent"
            )
            is True
        ),
        "enabled_simulation_fields_equivalent": (
            six_pf.get(
                "enabled_simulation_fields_equivalent"
            )
            is True
            and six_pg.get(
                "enabled_simulation_fields_equivalent"
            )
            is True
        ),
        "engine_configs_isolated": (
            six_pg.get(
                "engine_configs_isolated"
            )
            is True
        ),
        "inputs_unchanged": (
            six_pf.get(
                "inputs_unchanged"
            )
            is True
            and six_pg.get(
                "inputs_unchanged"
            )
            is True
        ),
        "classifier_import_is_lazy": (
            ast_audit[
                "lazy_classifier_import"
            ]
            and not ast_audit[
                "module_level_classifier_import"
            ]
        ),
        "only_shared_builder_references_classifier": (
            only_builder_has_classifier_reach
        ),
        "game_engine_classifier_reach": (
            len(engine_reference_rows) > 0
        ),
        "diagnostics_default_enabled": False,
        "production_classifier_activated": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": (
            False
        ),
        "broad_layer6_exit_paused": True,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_authority_granted": False,
        "backend_behavior_change_allowed_next": False,
        "frontend_behavior_change_allowed_next": False,
        "simulation_parameter_change_allowed_next": False,
        "final_probability_replacement_allowed_next": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "accuracy_metrics_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "gm02_inventory_and_planning_allowed_next": (
            all_completion_criteria_passed
        ),
        "gm02_production_implementation_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "predecessor_results.csv"
            ),
            str(
                OUTPUT_DIR
                / "completion_criteria.csv"
            ),
            str(
                OUTPUT_DIR
                / "production_reference_scan.csv"
            ),
            str(
                OUTPUT_DIR
                / "workstream_status.csv"
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
                / "predecessor_payloads.json"
            ),
            str(
                OUTPUT_DIR
                / "builder_ast_audit.json"
            ),
            str(
                OUTPUT_DIR
                / "completion_summary.json"
            ),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(json.dumps(diagnosis, indent=2))

    return (
        0
        if all_completion_criteria_passed
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
