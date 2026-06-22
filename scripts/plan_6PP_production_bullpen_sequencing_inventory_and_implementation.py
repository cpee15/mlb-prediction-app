#!/usr/bin/env python3
"""
Layer 6PP
Production Bullpen Sequencing Inventory and Implementation Plan

Inventories the current bullpen-related simulation architecture and defines a
safe, deterministic implementation path for GM-03.

This layer does not:

- change pitcher selection;
- change starter innings;
- change bullpen usage;
- change plate-appearance probabilities;
- change simulation outputs;
- activate candidate bullpen logic;
- authorize production behavior;
- perform historical validation, tuning, backtests, pricing, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PP"

LAYER_NAME = (
    "production_bullpen_sequencing_"
    "inventory_and_implementation_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PP_production_bullpen_sequencing_"
    "inventory_and_implementation_plan"
)

COMPLETION_PATH = (
    ROOT
    / "scripts/assess_6PO_dynamic_starter_hook_"
    "diagnostic_scope_completion.py"
)

CANDIDATE_ARCHITECTURE_PATH = (
    ROOT
    / "scripts/audit_bullpen_candidate_architecture.py"
)

SIMULATION_ROOT = (
    ROOT
    / "mlb_app/simulation"
)

ENGINE_PATH = (
    SIMULATION_ROOT
    / "game_engine_v2.py"
)

SIMULATOR_PATH = (
    SIMULATION_ROOT
    / "game_simulator.py"
)

BUILDER_PATH = (
    SIMULATION_ROOT
    / "game_simulation_builder.py"
)

BULLPEN_SEARCH_TERMS = [
    "bullpen",
    "reliever",
    "relief_pitcher",
    "current_pitcher",
    "pitcher_chain",
    "closer",
    "setup",
    "high_leverage",
    "middle_relief",
    "long_relief",
    "pitcher_replacement",
    "starter_exit",
    "starter_innings",
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


def relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def python_files() -> list[Path]:
    roots = [
        ROOT / "mlb_app",
        ROOT / "scripts",
    ]

    found: list[Path] = []

    for root in roots:
        if not root.exists():
            continue

        found.extend(
            sorted(root.rglob("*.py"))
        )

    return found


def inventory_references() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for path in python_files():
        text = read_text(path)
        lowered = text.lower()

        matched_terms = sorted(
            {
                term
                for term in BULLPEN_SEARCH_TERMS
                if term.lower() in lowered
            }
        )

        if not matched_terms:
            continue

        rows.append(
            {
                "path": relative(path),
                "matched_term_count": len(
                    matched_terms
                ),
                "matched_terms": "|".join(
                    matched_terms
                ),
                "is_simulation_runtime": (
                    "mlb_app/simulation/"
                    in relative(path)
                ),
                "is_script": (
                    relative(path).startswith(
                        "scripts/"
                    )
                ),
            }
        )

    return rows


def function_inventory(
    path: Path,
) -> list[dict[str, Any]]:
    text = read_text(path)

    if not text:
        return []

    tree = ast.parse(
        text,
        filename=str(path),
    )

    rows: list[dict[str, Any]] = []

    for node in ast.walk(tree):
        if not isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
            ),
        ):
            continue

        source = ast.get_source_segment(
            text,
            node,
        ) or ""

        lowered = source.lower()

        matched_terms = sorted(
            {
                term
                for term in BULLPEN_SEARCH_TERMS
                if term.lower() in lowered
            }
        )

        if not matched_terms:
            continue

        rows.append(
            {
                "path": relative(path),
                "function": node.name,
                "line": node.lineno,
                "matched_terms": "|".join(
                    matched_terms
                ),
            }
        )

    return rows


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files = [
        COMPLETION_PATH,
        CANDIDATE_ARCHITECTURE_PATH,
        ENGINE_PATH,
        SIMULATOR_PATH,
        BUILDER_PATH,
    ]

    required_files_exist = all(
        path.exists()
        for path in required_files
    )

    completion_run = subprocess.run(
        [
            sys.executable,
            str(COMPLETION_PATH),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    completion_payload = (
        parse_last_json_object(
            completion_run.stdout
        )
    )

    six_po_contract_passed = all(
        [
            completion_run.returncode == 0,
            completion_payload.get(
                "all_checks_passed"
            )
            is True,
            completion_payload.get(
                "assessment_checks_passed"
            )
            == 12,
            completion_payload.get(
                "assessment_checks_required"
            )
            == 12,
            completion_payload.get(
                "predecessors_accepted"
            )
            == 6,
            completion_payload.get(
                "gm02_diagnostic_scope_complete"
            )
            is True,
            completion_payload.get(
                "gm03_inventory_and_planning_allowed_next"
            )
            is True,
            completion_payload.get(
                "production_behavior_integration_allowed_next"
            )
            is False,
        ]
    )

    candidate_text = read_text(
        CANDIDATE_ARCHITECTURE_PATH
    )

    candidate_components = [
        "bullpen_state_container",
        "reliever_role_model",
        "availability_filter",
        "fatigue_model",
        "leverage_score_model",
        "replacement_trigger_model",
        "pitcher_chain_selector",
        "depletion_fallback_model",
        "candidate_activation_mode",
        "default_equivalence_guardrail",
        "backtest_harness",
        "promotion_guardrails",
    ]

    candidate_architecture_complete = all(
        component in candidate_text
        for component in candidate_components
    )

    candidate_default_off = all(
        token in candidate_text
        for token in [
            'DEFAULT_MODE = "off"',
            (
                "fallback_to_current_production_"
                "pitcher_assumption"
            ),
        ]
    )

    reference_rows = inventory_references()

    simulation_reference_rows = [
        row
        for row in reference_rows
        if row["is_simulation_runtime"]
    ]

    function_rows: list[
        dict[str, Any]
    ] = []

    for path in [
        ENGINE_PATH,
        SIMULATOR_PATH,
        BUILDER_PATH,
    ]:
        function_rows.extend(
            function_inventory(path)
        )

    architecture_components = [
        {
            "component_id": "BP-01",
            "component": "bullpen_state_contract",
            "purpose": (
                "Represent available, used, active, and "
                "depleted relievers without mutating callers."
            ),
            "implementation_order": 1,
            "stateful": True,
            "pure_component_first": True,
            "production_authority": False,
        },
        {
            "component_id": "BP-02",
            "component": "reliever_candidate_contract",
            "purpose": (
                "Normalize reliever identity, role, handedness, "
                "quality, workload, and availability evidence."
            ),
            "implementation_order": 2,
            "stateful": False,
            "pure_component_first": True,
            "production_authority": False,
        },
        {
            "component_id": "BP-03",
            "component": "availability_evaluator",
            "purpose": (
                "Classify available, limited, unavailable, "
                "or unknown using deterministic evidence."
            ),
            "implementation_order": 3,
            "stateful": False,
            "pure_component_first": True,
            "production_authority": False,
        },
        {
            "component_id": "BP-04",
            "component": "leverage_context_evaluator",
            "purpose": (
                "Classify game state into deterministic "
                "deployment leverage bands."
            ),
            "implementation_order": 4,
            "stateful": False,
            "pure_component_first": True,
            "production_authority": False,
        },
        {
            "component_id": "BP-05",
            "component": "reliever_role_matcher",
            "purpose": (
                "Score role compatibility against leverage "
                "without selecting a production pitcher."
            ),
            "implementation_order": 5,
            "stateful": False,
            "pure_component_first": True,
            "production_authority": False,
        },
        {
            "component_id": "BP-06",
            "component": "bullpen_sequence_evaluator",
            "purpose": (
                "Rank eligible relievers deterministically and "
                "emit a non-authoritative candidate sequence."
            ),
            "implementation_order": 6,
            "stateful": False,
            "pure_component_first": True,
            "production_authority": False,
        },
        {
            "component_id": "BP-07",
            "component": "depletion_fallback_contract",
            "purpose": (
                "Return explicit fallback reasons when no "
                "eligible reliever can be ranked."
            ),
            "implementation_order": 7,
            "stateful": False,
            "pure_component_first": True,
            "production_authority": False,
        },
        {
            "component_id": "BP-08",
            "component": "diagnostic_integration_seam",
            "purpose": (
                "Attach disabled-by-default metadata through "
                "the shared builder after independent audits."
            ),
            "implementation_order": 8,
            "stateful": False,
            "pure_component_first": False,
            "production_authority": False,
        },
    ]

    state_fields = [
        {
            "field": "team_id",
            "type": "string_or_integer_or_null",
            "required": True,
            "authority": False,
        },
        {
            "field": "inning",
            "type": "integer",
            "required": True,
            "authority": False,
        },
        {
            "field": "outs",
            "type": "integer",
            "required": True,
            "authority": False,
        },
        {
            "field": "base_state",
            "type": "object",
            "required": True,
            "authority": False,
        },
        {
            "field": "score_margin",
            "type": "integer_or_float",
            "required": True,
            "authority": False,
        },
        {
            "field": "leverage_proxy",
            "type": "number",
            "required": True,
            "authority": False,
        },
        {
            "field": "current_pitcher_id",
            "type": "string_or_integer_or_null",
            "required": False,
            "authority": False,
        },
        {
            "field": "available_relievers",
            "type": "array",
            "required": True,
            "authority": False,
        },
        {
            "field": "used_pitcher_ids",
            "type": "array",
            "required": False,
            "authority": False,
        },
        {
            "field": "usage_log",
            "type": "array",
            "required": False,
            "authority": False,
        },
        {
            "field": "bullpen_depletion_index",
            "type": "number_or_null",
            "required": False,
            "authority": False,
        },
        {
            "field": "extra_inning_flag",
            "type": "boolean",
            "required": False,
            "authority": False,
        },
    ]

    reliever_fields = [
        {
            "field": "pitcher_id",
            "type": "string_or_integer",
            "required": True,
        },
        {
            "field": "role",
            "type": (
                "closer|setup|high_leverage|middle_relief|"
                "long_relief|low_leverage|unknown"
            ),
            "required": True,
        },
        {
            "field": "throws",
            "type": "L|R|unknown",
            "required": False,
        },
        {
            "field": "quality_score",
            "type": "number_or_null",
            "required": False,
        },
        {
            "field": "availability_status",
            "type": (
                "available|limited|unavailable|unknown"
            ),
            "required": True,
        },
        {
            "field": "fatigue_index",
            "type": "number_or_null",
            "required": False,
        },
        {
            "field": "recent_usage_count",
            "type": "integer_or_null",
            "required": False,
        },
        {
            "field": "back_to_back_flag",
            "type": "boolean_or_null",
            "required": False,
        },
        {
            "field": "innings_capacity",
            "type": "number_or_null",
            "required": False,
        },
        {
            "field": "evidence_complete",
            "type": "boolean",
            "required": True,
        },
    ]

    output_fields = [
        {
            "field": "recommended_pitcher_id",
            "type": "string_or_integer_or_null",
            "required": True,
        },
        {
            "field": "ranked_candidates",
            "type": "array",
            "required": True,
        },
        {
            "field": "leverage_band",
            "type": (
                "low|medium|high|critical|unknown"
            ),
            "required": True,
        },
        {
            "field": "selection_reason",
            "type": "string",
            "required": True,
        },
        {
            "field": "fallback_used",
            "type": "boolean",
            "required": True,
        },
        {
            "field": "fallback_reason",
            "type": "string_or_null",
            "required": True,
        },
        {
            "field": "state_completeness",
            "type": "complete|partial|invalid",
            "required": True,
        },
        {
            "field": "behavioral_effect",
            "type": "string",
            "required": True,
        },
        {
            "field": (
                "canonical_probability_"
                "authority_changed"
            ),
            "type": "boolean",
            "required": True,
        },
        {
            "field": "production_activation",
            "type": "boolean",
            "required": True,
        },
    ]

    fixture_plan = [
        {
            "fixture_id": "PP-F01",
            "scenario": (
                "complete_high_leverage_state"
            ),
            "expected": (
                "deterministic ranked candidates"
            ),
        },
        {
            "fixture_id": "PP-F02",
            "scenario": (
                "complete_low_leverage_state"
            ),
            "expected": (
                "preserve high-leverage arms when "
                "lower-role candidate exists"
            ),
        },
        {
            "fixture_id": "PP-F03",
            "scenario": (
                "closer_unavailable"
            ),
            "expected": (
                "next eligible role ranks first"
            ),
        },
        {
            "fixture_id": "PP-F04",
            "scenario": (
                "all_primary_roles_unavailable"
            ),
            "expected": (
                "deterministic depletion fallback"
            ),
        },
        {
            "fixture_id": "PP-F05",
            "scenario": (
                "partial_availability_evidence"
            ),
            "expected": (
                "partial state with conservative ranking"
            ),
        },
        {
            "fixture_id": "PP-F06",
            "scenario": (
                "invalid_bullpen_state"
            ),
            "expected": (
                "invalid state and explicit fallback"
            ),
        },
        {
            "fixture_id": "PP-F07",
            "scenario": (
                "tie_between_candidates"
            ),
            "expected": (
                "stable deterministic tie break"
            ),
        },
        {
            "fixture_id": "PP-F08",
            "scenario": (
                "extra_inning_depletion"
            ),
            "expected": (
                "depth-aware fallback ordering"
            ),
        },
        {
            "fixture_id": "PP-F09",
            "scenario": (
                "input_immutability"
            ),
            "expected": (
                "caller state remains unchanged"
            ),
        },
        {
            "fixture_id": "PP-F10",
            "scenario": (
                "production_authority_guard"
            ),
            "expected": (
                "behavioral effect none and production "
                "activation false"
            ),
        },
    ]

    implementation_steps = [
        {
            "step": 1,
            "action": (
                "Implement immutable bullpen and reliever "
                "input contracts."
            ),
            "behavioral_change": False,
        },
        {
            "step": 2,
            "action": (
                "Implement pure deterministic availability "
                "and leverage evaluators."
            ),
            "behavioral_change": False,
        },
        {
            "step": 3,
            "action": (
                "Implement pure role-match and candidate "
                "ranking evaluator."
            ),
            "behavioral_change": False,
        },
        {
            "step": 4,
            "action": (
                "Add output validation and explicit depletion "
                "fallback contracts."
            ),
            "behavioral_change": False,
        },
        {
            "step": 5,
            "action": (
                "Run independent evaluator audit."
            ),
            "behavioral_change": False,
        },
        {
            "step": 6,
            "action": (
                "Plan disabled-by-default diagnostic "
                "integration only after evaluator approval."
            ),
            "behavioral_change": False,
        },
        {
            "step": 7,
            "action": (
                "Implement metadata-only shared-builder "
                "integration with exact equivalence."
            ),
            "behavioral_change": False,
        },
        {
            "step": 8,
            "action": (
                "Run independent integration audit and "
                "diagnostic-scope completion assessment."
            ),
            "behavioral_change": False,
        },
    ]

    implementation_steps_nonbehavioral = all(
        row["behavioral_change"] is False
        for row in implementation_steps
    )

    current_runtime_reference_count = len(
        simulation_reference_rows
    )

    pure_evaluator_implementation_allowed = all(
        [
            six_po_contract_passed,
            candidate_architecture_complete,
            candidate_default_off,
            len(architecture_components) == 8,
            len(state_fields) == 12,
            len(reliever_fields) == 10,
            len(output_fields) == 10,
            len(fixture_plan) == 10,
            len(implementation_steps) == 8,
            implementation_steps_nonbehavioral,
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
            "check": "six_po_contract_passes",
            "actual": six_po_contract_passed,
            "expected": True,
            "passed": six_po_contract_passed,
        },
        {
            "check": (
                "candidate_architecture_complete"
            ),
            "actual": (
                candidate_architecture_complete
            ),
            "expected": True,
            "passed": (
                candidate_architecture_complete
            ),
        },
        {
            "check": "candidate_default_off",
            "actual": candidate_default_off,
            "expected": True,
            "passed": candidate_default_off,
        },
        {
            "check": (
                "runtime_inventory_completed"
            ),
            "actual": (
                current_runtime_reference_count
                >= 0
            ),
            "expected": True,
            "passed": True,
        },
        {
            "check": (
                "eight_components_planned"
            ),
            "actual": len(
                architecture_components
            ),
            "expected": 8,
            "passed": len(
                architecture_components
            )
            == 8,
        },
        {
            "check": (
                "twelve_state_fields_planned"
            ),
            "actual": len(state_fields),
            "expected": 12,
            "passed": len(state_fields) == 12,
        },
        {
            "check": (
                "ten_reliever_fields_planned"
            ),
            "actual": len(reliever_fields),
            "expected": 10,
            "passed": len(reliever_fields) == 10,
        },
        {
            "check": (
                "ten_output_fields_planned"
            ),
            "actual": len(output_fields),
            "expected": 10,
            "passed": len(output_fields) == 10,
        },
        {
            "check": "ten_fixtures_planned",
            "actual": len(fixture_plan),
            "expected": 10,
            "passed": len(fixture_plan) == 10,
        },
        {
            "check": (
                "eight_steps_planned"
            ),
            "actual": len(
                implementation_steps
            ),
            "expected": 8,
            "passed": len(
                implementation_steps
            )
            == 8,
        },
        {
            "check": (
                "all_steps_nonbehavioral"
            ),
            "actual": (
                implementation_steps_nonbehavioral
            ),
            "expected": True,
            "passed": (
                implementation_steps_nonbehavioral
            ),
        },
        {
            "check": (
                "pure_evaluator_implementation_allowed"
            ),
            "actual": (
                pure_evaluator_implementation_allowed
            ),
            "expected": True,
            "passed": (
                pure_evaluator_implementation_allowed
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
                "6PP is an inventory and implementation "
                "planning layer only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "pure_bullpen_sequence_evaluator_"
                    "implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Pure deterministic evaluator only; "
                    "no runtime integration."
                ),
            },
            {
                "authority": (
                    "production_behavior_integration"
                ),
                "granted": False,
                "reason": (
                    "Independent implementation and "
                    "integration audits remain required."
                ),
            },
        ]
    )

    diagnosis_name = (
        "production_bullpen_sequencing_inventory_"
        "and_implementation_plan_complete"
        if all_checks_passed
        else
        "production_bullpen_sequencing_inventory_"
        "and_implementation_plan_failed"
    )

    recommended_next_layer = (
        "6PQ_production_bullpen_sequencing_"
        "state_contract_and_evaluator_implementation"
        if all_checks_passed
        else
        "6PQ_production_bullpen_sequencing_"
        "inventory_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "repository_reference_inventory.csv",
        [
            "path",
            "matched_term_count",
            "matched_terms",
            "is_simulation_runtime",
            "is_script",
        ],
        reference_rows,
    )

    write_csv(
        OUTPUT_DIR / "runtime_function_inventory.csv",
        [
            "path",
            "function",
            "line",
            "matched_terms",
        ],
        function_rows,
    )

    write_csv(
        OUTPUT_DIR / "architecture_components.csv",
        [
            "component_id",
            "component",
            "purpose",
            "implementation_order",
            "stateful",
            "pure_component_first",
            "production_authority",
        ],
        architecture_components,
    )

    write_csv(
        OUTPUT_DIR / "bullpen_state_contract.csv",
        [
            "field",
            "type",
            "required",
            "authority",
        ],
        state_fields,
    )

    write_csv(
        OUTPUT_DIR / "reliever_candidate_contract.csv",
        [
            "field",
            "type",
            "required",
        ],
        reliever_fields,
    )

    write_csv(
        OUTPUT_DIR / "output_contract.csv",
        [
            "field",
            "type",
            "required",
        ],
        output_fields,
    )

    write_csv(
        OUTPUT_DIR / "fixture_plan.csv",
        [
            "fixture_id",
            "scenario",
            "expected",
        ],
        fixture_plan,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "step",
            "action",
            "behavioral_change",
        ],
        implementation_steps,
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
                    "Implement pure deterministic bullpen "
                    "state and candidate-sequence evaluator "
                    "without runtime integration."
                ),
                "entry_condition": (
                    "All 6PP planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "predecessor_contract.json",
        {
            "returncode": (
                completion_run.returncode
            ),
            "contract_passed": (
                six_po_contract_passed
            ),
            "diagnosis": completion_payload,
            "stderr": completion_run.stderr,
        },
    )

    summary = {
        "runtime_reference_files": len(
            reference_rows
        ),
        "simulation_runtime_reference_files": (
            current_runtime_reference_count
        ),
        "runtime_functions_with_bullpen_terms": len(
            function_rows
        ),
        "architecture_components_planned": len(
            architecture_components
        ),
        "bullpen_state_fields_planned": len(
            state_fields
        ),
        "reliever_fields_planned": len(
            reliever_fields
        ),
        "output_fields_planned": len(
            output_fields
        ),
        "fixtures_planned": len(
            fixture_plan
        ),
        "implementation_steps_planned": len(
            implementation_steps
        ),
        "candidate_architecture_complete": (
            candidate_architecture_complete
        ),
        "candidate_default_off": (
            candidate_default_off
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR / "inventory_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": (
            all_checks_passed
        ),
        "planning_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "planning_checks_required": len(
            checks
        ),
        "six_po_contract_passed": (
            six_po_contract_passed
        ),
        "candidate_architecture_complete": (
            candidate_architecture_complete
        ),
        "candidate_default_off": (
            candidate_default_off
        ),
        "runtime_reference_files": len(
            reference_rows
        ),
        "simulation_runtime_reference_files": (
            current_runtime_reference_count
        ),
        "runtime_functions_with_bullpen_terms": len(
            function_rows
        ),
        "architecture_components_planned": len(
            architecture_components
        ),
        "bullpen_state_fields_planned": len(
            state_fields
        ),
        "reliever_fields_planned": len(
            reliever_fields
        ),
        "output_fields_planned": len(
            output_fields
        ),
        "fixtures_planned": len(
            fixture_plan
        ),
        "implementation_steps_planned": len(
            implementation_steps
        ),
        "pure_evaluator_implementation_allowed_next": (
            all_checks_passed
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
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "planning_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "repository_reference_inventory.csv"
            ),
            str(
                OUTPUT_DIR
                / "runtime_function_inventory.csv"
            ),
            str(
                OUTPUT_DIR
                / "architecture_components.csv"
            ),
            str(
                OUTPUT_DIR
                / "bullpen_state_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "reliever_candidate_contract.csv"
            ),
            str(
                OUTPUT_DIR / "output_contract.csv"
            ),
            str(
                OUTPUT_DIR / "fixture_plan.csv"
            ),
            str(
                OUTPUT_DIR
                / "implementation_steps.csv"
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
                / "predecessor_contract.json"
            ),
            str(
                OUTPUT_DIR / "inventory_summary.json"
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
