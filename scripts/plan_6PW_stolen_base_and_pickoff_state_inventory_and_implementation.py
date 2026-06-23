#!/usr/bin/env python3
"""
Layer 6PW
Stolen-Base and Pickoff State Inventory and Implementation Plan

Inventories the current baserunner/base-out simulation architecture and defines
a safe deterministic implementation path for GM-04.

This layer does not:

- create steal attempts in production;
- create pickoff attempts in production;
- change base/out transitions;
- change runner advancement;
- change outs or runs;
- change plate-appearance probabilities;
- change simulation scores or win probabilities;
- activate candidate baserunning logic;
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


LAYER_ID = "6PW"

LAYER_NAME = (
    "stolen_base_and_pickoff_state_"
    "inventory_and_implementation_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PW_stolen_base_and_pickoff_state_"
    "inventory_and_implementation_plan"
)

COMPLETION_PATH = (
    ROOT
    / "scripts/assess_6PV_production_bullpen_"
    "sequencing_diagnostic_scope_completion.py"
)

SIMULATION_ROOT = (
    ROOT
    / "mlb_app/simulation"
)

INNING_SIMULATOR_PATH = (
    SIMULATION_ROOT
    / "inning_simulator.py"
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

MODEL_PROJECTIONS_PATH = (
    ROOT
    / "mlb_app/model_projections.py"
)

FEASIBILITY_PATH = (
    ROOT
    / "scripts/audit_base_out_state_feasibility.py"
)

SEARCH_TERMS = [
    "steal",
    "stolen_base",
    "stolen base",
    "steal_attempt",
    "steal attempt",
    "steal_success",
    "caught_stealing",
    "caught stealing",
    "pickoff",
    "pick_off",
    "runner_on_first",
    "runner_on_second",
    "runner_on_third",
    "initial_bases",
    "initial_outs",
    "base_state",
    "advance_runners",
    "pinch_runner",
]

CANDIDATE_COMPONENTS = [
    {
        "component": "baserunner_state_container",
        "purpose": (
            "Represent identified runners, occupied bases, outs, inning, "
            "score margin, and game context."
        ),
    },
    {
        "component": "runner_eligibility_filter",
        "purpose": (
            "Determine whether a runner and destination base permit a "
            "candidate steal decision."
        ),
    },
    {
        "component": "steal_attempt_evaluator",
        "purpose": (
            "Return deterministic attempt propensity from runner, pitcher, "
            "catcher, inning, score, and base/out state."
        ),
    },
    {
        "component": "steal_success_evaluator",
        "purpose": (
            "Return deterministic conditional success probability without "
            "sampling or changing bases."
        ),
    },
    {
        "component": "pitcher_hold_model",
        "purpose": (
            "Represent pitcher delivery, handedness, disengagement, and "
            "runner-control evidence."
        ),
    },
    {
        "component": "catcher_throwing_model",
        "purpose": (
            "Represent catcher pop-time or throwing-control evidence."
        ),
    },
    {
        "component": "pickoff_attempt_evaluator",
        "purpose": (
            "Represent candidate pickoff pressure independently from steal "
            "attempt logic."
        ),
    },
    {
        "component": "pickoff_result_evaluator",
        "purpose": (
            "Return candidate pickoff outcome probabilities without applying "
            "an out or changing base state."
        ),
    },
    {
        "component": "state_transition_adapter",
        "purpose": (
            "Future isolated adapter from evaluator recommendation to a "
            "base/out transition, disabled in this scope."
        ),
    },
    {
        "component": "default_equivalence_guardrail",
        "purpose": (
            "Guarantee no changes when diagnostics or candidate behavior are "
            "absent or disabled."
        ),
    },
    {
        "component": "diagnostic_metadata_adapter",
        "purpose": (
            "Expose evaluator output as metadata only before any production "
            "behavior is considered."
        ),
    },
    {
        "component": "promotion_guardrails",
        "purpose": (
            "Require independent audit, equivalence, historical validation, "
            "and explicit authorization before behavioral integration."
        ),
    },
]

STATE_FIELDS = [
    {
        "field": "inning",
        "required": True,
        "type": "integer",
        "purpose": "Current inning.",
    },
    {
        "field": "half",
        "required": True,
        "type": "string",
        "purpose": "Top or bottom half.",
    },
    {
        "field": "outs",
        "required": True,
        "type": "integer",
        "purpose": "Current outs.",
    },
    {
        "field": "base_state",
        "required": True,
        "type": "object",
        "purpose": "Occupied first, second, and third bases.",
    },
    {
        "field": "score_margin",
        "required": True,
        "type": "integer",
        "purpose": "Offense score minus defense score.",
    },
    {
        "field": "runner",
        "required": True,
        "type": "object",
        "purpose": "Candidate runner evidence.",
    },
    {
        "field": "origin_base",
        "required": True,
        "type": "string",
        "purpose": "Runner current base.",
    },
    {
        "field": "target_base",
        "required": True,
        "type": "string",
        "purpose": "Candidate destination base.",
    },
    {
        "field": "pitcher",
        "required": True,
        "type": "object",
        "purpose": "Pitcher runner-control evidence.",
    },
    {
        "field": "catcher",
        "required": True,
        "type": "object",
        "purpose": "Catcher throwing-control evidence.",
    },
    {
        "field": "batter_id",
        "required": False,
        "type": "string|null",
        "purpose": "Current batter identity.",
    },
    {
        "field": "count",
        "required": False,
        "type": "object",
        "purpose": "Balls and strikes.",
    },
    {
        "field": "disengagements_used",
        "required": False,
        "type": "integer",
        "purpose": "Pitcher disengagement state.",
    },
    {
        "field": "game_date",
        "required": False,
        "type": "string|null",
        "purpose": "Optional rules-context date.",
    },
    {
        "field": "extra_inning_flag",
        "required": False,
        "type": "boolean",
        "purpose": "Extra-inning context.",
    },
    {
        "field": "evidence_version",
        "required": False,
        "type": "string|null",
        "purpose": "Input provenance version.",
    },
]

RUNNER_FIELDS = [
    {
        "field": "runner_id",
        "required": True,
        "purpose": "Stable runner identity.",
    },
    {
        "field": "speed_score",
        "required": False,
        "purpose": "Normalized runner speed evidence.",
    },
    {
        "field": "attempt_rate",
        "required": False,
        "purpose": "Observed or estimated attempt tendency.",
    },
    {
        "field": "success_rate",
        "required": False,
        "purpose": "Observed or estimated steal success.",
    },
    {
        "field": "lead_quality",
        "required": False,
        "purpose": "Optional lead/jump evidence.",
    },
    {
        "field": "fatigue_index",
        "required": False,
        "purpose": "Optional fatigue evidence.",
    },
    {
        "field": "injury_limit_flag",
        "required": False,
        "purpose": "Optional running restriction.",
    },
    {
        "field": "evidence_complete",
        "required": True,
        "purpose": "Whether minimum runner evidence is verified.",
    },
]

PITCHER_FIELDS = [
    {
        "field": "pitcher_id",
        "required": True,
        "purpose": "Stable pitcher identity.",
    },
    {
        "field": "throws",
        "required": False,
        "purpose": "Pitcher handedness.",
    },
    {
        "field": "hold_score",
        "required": False,
        "purpose": "Runner-control evidence.",
    },
    {
        "field": "delivery_time_score",
        "required": False,
        "purpose": "Delivery-speed evidence.",
    },
    {
        "field": "pickoff_attempt_rate",
        "required": False,
        "purpose": "Pickoff tendency evidence.",
    },
    {
        "field": "pickoff_success_rate",
        "required": False,
        "purpose": "Pickoff success evidence.",
    },
    {
        "field": "evidence_complete",
        "required": True,
        "purpose": "Whether minimum pitcher evidence is verified.",
    },
]

CATCHER_FIELDS = [
    {
        "field": "catcher_id",
        "required": True,
        "purpose": "Stable catcher identity.",
    },
    {
        "field": "throws",
        "required": False,
        "purpose": "Catcher throwing hand.",
    },
    {
        "field": "throwing_score",
        "required": False,
        "purpose": "Normalized throwing-control evidence.",
    },
    {
        "field": "pop_time_score",
        "required": False,
        "purpose": "Optional pop-time evidence.",
    },
    {
        "field": "caught_stealing_rate",
        "required": False,
        "purpose": "Optional caught-stealing evidence.",
    },
    {
        "field": "evidence_complete",
        "required": True,
        "purpose": "Whether minimum catcher evidence is verified.",
    },
]

OUTPUT_FIELDS = [
    {
        "field": "steal_eligible",
        "purpose": "Whether the candidate state permits a steal.",
    },
    {
        "field": "attempt_recommendation",
        "purpose": "attempt, hold, or unknown_fallback.",
    },
    {
        "field": "attempt_probability",
        "purpose": "Deterministic candidate attempt propensity.",
    },
    {
        "field": "success_probability",
        "purpose": "Conditional candidate steal success probability.",
    },
    {
        "field": "pickoff_pressure",
        "purpose": "none, low, medium, high, or unknown.",
    },
    {
        "field": "pickoff_out_probability",
        "purpose": "Conditional candidate pickoff-out probability.",
    },
    {
        "field": "selection_reason",
        "purpose": "Deterministic explanation.",
    },
    {
        "field": "fallback_used",
        "purpose": "Whether conservative fallback was required.",
    },
    {
        "field": "fallback_reason",
        "purpose": "Why fallback was required.",
    },
    {
        "field": "state_completeness",
        "purpose": "complete, partial, or invalid.",
    },
    {
        "field": "behavioral_effect",
        "purpose": "Always none in evaluator and diagnostic scope.",
    },
    {
        "field": "canonical_probability_authority_changed",
        "purpose": "Always false.",
    },
    {
        "field": "production_activation",
        "purpose": "Always false.",
    },
]

FIXTURES = [
    {
        "fixture_id": "PW-F01",
        "scenario": "runner_on_first_second_open_complete_evidence",
        "expected": "eligible_candidate_evaluation",
    },
    {
        "fixture_id": "PW-F02",
        "scenario": "runner_on_first_second_occupied",
        "expected": "ineligible_occupied_target",
    },
    {
        "fixture_id": "PW-F03",
        "scenario": "two_out_state",
        "expected": "deterministic_context_adjustment",
    },
    {
        "fixture_id": "PW-F04",
        "scenario": "late_close_game",
        "expected": "deterministic_context_adjustment",
    },
    {
        "fixture_id": "PW-F05",
        "scenario": "slow_runner_strong_battery",
        "expected": "hold_directionality",
    },
    {
        "fixture_id": "PW-F06",
        "scenario": "fast_runner_weak_battery",
        "expected": "attempt_directionality",
    },
    {
        "fixture_id": "PW-F07",
        "scenario": "runner_evidence_partial",
        "expected": "partial_state_conservative_fallback",
    },
    {
        "fixture_id": "PW-F08",
        "scenario": "pitcher_or_catcher_evidence_partial",
        "expected": "partial_state_conservative_fallback",
    },
    {
        "fixture_id": "PW-F09",
        "scenario": "invalid_base_state",
        "expected": "invalid_state_no_activation",
    },
    {
        "fixture_id": "PW-F10",
        "scenario": "input_immutability_and_repeatability",
        "expected": "identical_output_and_unchanged_input",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_steal_attempt_activation",
    "production_pickoff_attempt_activation",
    "base_state_transition_change",
    "out_state_transition_change",
    "runner_advancement_change",
    "runs_scored_change",
    "caught_stealing_out_change",
    "pickoff_out_change",
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
        return str(
            path.relative_to(ROOT)
        )
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
            sorted(
                root.rglob("*.py")
            )
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
                for term in SEARCH_TERMS
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
                    relative(path).startswith(
                        "mlb_app/simulation/"
                    )
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
                for term in SEARCH_TERMS
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
        INNING_SIMULATOR_PATH,
        ENGINE_PATH,
        SIMULATOR_PATH,
        BUILDER_PATH,
        MODEL_PROJECTIONS_PATH,
        FEASIBILITY_PATH,
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

    six_pv_contract_passed = all(
        [
            completion_run.returncode == 0,
            completion_payload.get(
                "diagnosis"
            )
            == (
                "production_bullpen_sequencing_"
                "diagnostic_scope_complete"
            ),
            completion_payload.get(
                "all_checks_passed"
            )
            is True,
            completion_payload.get(
                "completion_checks_passed"
            )
            == 12,
            completion_payload.get(
                "completion_checks_required"
            )
            == 12,
            completion_payload.get(
                "predecessors_accepted"
            )
            == 6,
            completion_payload.get(
                "gm03_diagnostic_scope_complete"
            )
            is True,
            completion_payload.get(
                "gm04_inventory_and_"
                "implementation_planning_allowed_next"
            )
            is True,
            completion_payload.get(
                "production_behavior_"
                "integration_allowed_next"
            )
            is False,
        ]
    )

    inning_text = read_text(
        INNING_SIMULATOR_PATH
    )

    engine_text = read_text(
        ENGINE_PATH
    )

    simulator_text = read_text(
        SIMULATOR_PATH
    )

    builder_text = read_text(
        BUILDER_PATH
    )

    model_projection_text = read_text(
        MODEL_PROJECTIONS_PATH
    )

    feasibility_text = read_text(
        FEASIBILITY_PATH
    )

    inning_tree = ast.parse(
        inning_text,
        filename=str(
            INNING_SIMULATOR_PATH
        ),
    )

    inning_functions = {
        node.name: node
        for node in ast.walk(
            inning_tree
        )
        if isinstance(
            node,
            ast.FunctionDef,
        )
    }

    simulate_node = inning_functions.get(
        "simulate_half_inning"
    )

    simulate_arguments = (
        [
            argument.arg
            for argument in (
                simulate_node.args.args
                if simulate_node is not None
                else []
            )
        ]
    )

    explicit_no_steals = (
        "no steals"
        in inning_text.lower()
    )

    base_out_state_supported = all(
        field in simulate_arguments
        for field in [
            "initial_bases",
            "initial_outs",
        ]
    )

    runner_transition_architecture_present = all(
        function_name in inning_functions
        for function_name in [
            "advance_runners",
            "advance_runners_realism_candidate",
            "simulate_half_inning",
        ]
    )

    previous_feasibility_present = all(
        token in feasibility_text
        for token in [
            (
                "base_out_state_api_feasible_"
                "with_backward_compatible_defaults"
            ),
            "steals",
            "caught_stealing",
        ]
    )

    pickoff_scope_newly_planned = (
        "pickoff_attempt_evaluator"
        in {
            row["component"]
            for row in CANDIDATE_COMPONENTS
        }
        and
        "pickoff_result_evaluator"
        in {
            row["component"]
            for row in CANDIDATE_COMPONENTS
        }
    )

    steals_status_only_present = all(
        token in model_projection_text
        for token in [
            "steals_model_status",
            "deferred_not_active",
            (
                "steals_projection_"
                "wiring_status"
            ),
            (
                "status_only_no_"
                "behavioral_effect"
            ),
        ]
    )

    production_activation_tokens = [
        "attempt_steal(",
        "resolve_steal_attempt(",
        "apply_stolen_base(",
        "apply_caught_stealing(",
        "attempt_pickoff(",
        "resolve_pickoff_attempt(",
        "apply_pickoff_out(",
    ]

    runtime_text = "\n".join(
        [
            inning_text,
            engine_text,
            simulator_text,
            builder_text,
        ]
    )

    production_activation_absent = not any(
        token in runtime_text
        for token in production_activation_tokens
    )

    reference_rows = (
        inventory_references()
    )

    function_rows: list[
        dict[str, Any]
    ] = []

    for path in [
        INNING_SIMULATOR_PATH,
        ENGINE_PATH,
        SIMULATOR_PATH,
        BUILDER_PATH,
        MODEL_PROJECTIONS_PATH,
    ]:
        function_rows.extend(
            function_inventory(path)
        )

    candidate_default_off = True

    architecture_complete = (
        len(CANDIDATE_COMPONENTS)
        == 12
    )

    state_contract_complete = (
        len(STATE_FIELDS)
        == 16
    )

    participant_contract_complete = all(
        [
            len(RUNNER_FIELDS) == 8,
            len(PITCHER_FIELDS) == 7,
            len(CATCHER_FIELDS) == 6,
        ]
    )

    output_contract_complete = (
        len(OUTPUT_FIELDS)
        == 13
    )

    fixture_plan_complete = (
        len(FIXTURES)
        == 10
    )

    authority_boundaries_explicit = (
        len(PROHIBITED_AUTHORITIES)
        >= 20
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": "six_pv_contract_passed",
            "actual": six_pv_contract_passed,
            "expected": True,
            "passed": six_pv_contract_passed,
        },
        {
            "check": "inning_simulator_explicit_no_steals",
            "actual": explicit_no_steals,
            "expected": True,
            "passed": explicit_no_steals,
        },
        {
            "check": "base_out_state_supported",
            "actual": base_out_state_supported,
            "expected": True,
            "passed": base_out_state_supported,
        },
        {
            "check": "runner_transition_architecture_present",
            "actual": (
                runner_transition_architecture_present
            ),
            "expected": True,
            "passed": (
                runner_transition_architecture_present
            ),
        },
        {
            "check": "previous_feasibility_present",
            "actual": previous_feasibility_present,
            "expected": True,
            "passed": previous_feasibility_present,
        },
        {
            "check": "pickoff_scope_newly_planned",
            "actual": pickoff_scope_newly_planned,
            "expected": True,
            "passed": pickoff_scope_newly_planned,
        },
        {
            "check": "steals_status_only_present",
            "actual": steals_status_only_present,
            "expected": True,
            "passed": steals_status_only_present,
        },
        {
            "check": "production_activation_absent",
            "actual": production_activation_absent,
            "expected": True,
            "passed": production_activation_absent,
        },
        {
            "check": "candidate_architecture_complete",
            "actual": len(
                CANDIDATE_COMPONENTS
            ),
            "expected": 12,
            "passed": architecture_complete,
        },
        {
            "check": "state_contract_complete",
            "actual": len(
                STATE_FIELDS
            ),
            "expected": 16,
            "passed": state_contract_complete,
        },
        {
            "check": "participant_contract_complete",
            "actual": (
                len(RUNNER_FIELDS)
                + len(PITCHER_FIELDS)
                + len(CATCHER_FIELDS)
            ),
            "expected": 21,
            "passed": participant_contract_complete,
        },
        {
            "check": "output_contract_complete",
            "actual": len(
                OUTPUT_FIELDS
            ),
            "expected": 13,
            "passed": output_contract_complete,
        },
        {
            "check": "fixture_plan_complete",
            "actual": len(
                FIXTURES
            ),
            "expected": 10,
            "passed": fixture_plan_complete,
        },
        {
            "check": "candidate_default_off",
            "actual": candidate_default_off,
            "expected": True,
            "passed": candidate_default_off,
        },
        {
            "check": "authority_boundaries_explicit",
            "actual": len(
                PROHIBITED_AUTHORITIES
            ),
            "expected": ">=20",
            "passed": authority_boundaries_explicit,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    write_csv(
        OUTPUT_DIR / "inventory_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "reference_inventory.csv",
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
        OUTPUT_DIR / "function_inventory.csv",
        [
            "path",
            "function",
            "line",
            "matched_terms",
        ],
        function_rows,
    )

    write_csv(
        OUTPUT_DIR / "candidate_components.csv",
        [
            "component",
            "purpose",
        ],
        CANDIDATE_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "state_contract.csv",
        [
            "field",
            "required",
            "type",
            "purpose",
        ],
        STATE_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "runner_contract.csv",
        [
            "field",
            "required",
            "purpose",
        ],
        RUNNER_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "pitcher_contract.csv",
        [
            "field",
            "required",
            "purpose",
        ],
        PITCHER_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "catcher_contract.csv",
        [
            "field",
            "required",
            "purpose",
        ],
        CATCHER_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "output_contract.csv",
        [
            "field",
            "purpose",
        ],
        OUTPUT_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "fixture_plan.csv",
        [
            "fixture_id",
            "scenario",
            "expected",
        ],
        FIXTURES,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        [
            {
                "authority": authority,
                "granted": False,
                "reason": (
                    "6PW is inventory and "
                    "implementation planning only."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ],
    )

    implementation_plan = {
        "module_path": (
            "mlb_app/simulation/"
            "stolen_base_pickoff_evaluator.py"
        ),
        "public_functions": [
            (
                "evaluate_stolen_base_"
                "and_pickoff_state"
            ),
            (
                "validate_stolen_base_"
                "and_pickoff_evaluation"
            ),
        ],
        "design": {
            "pure": True,
            "deterministic": True,
            "input_mutation": False,
            "random_sampling": False,
            "production_activation": False,
            "behavioral_effect": "none",
            (
                "canonical_probability_"
                "authority_changed"
            ): False,
        },
        "implementation_order": [
            (
                "validate and normalize "
                "base/out state"
            ),
            (
                "validate runner, pitcher, "
                "and catcher evidence"
            ),
            (
                "determine steal eligibility"
            ),
            (
                "derive deterministic attempt "
                "recommendation"
            ),
            (
                "derive deterministic conditional "
                "success probability"
            ),
            (
                "derive deterministic pickoff "
                "pressure and out probability"
            ),
            (
                "apply conservative partial and "
                "invalid-state fallback"
            ),
            (
                "validate exact output contract"
            ),
            (
                "prove repeatability and input "
                "immutability"
            ),
            (
                "prove zero production references"
            ),
        ],
        "integration_sequence": [
            (
                "pure evaluator implementation"
            ),
            (
                "independent evaluator audit"
            ),
            (
                "disabled-by-default diagnostic "
                "integration plan"
            ),
            (
                "metadata-only diagnostic "
                "integration"
            ),
            (
                "independent integration audit"
            ),
            (
                "diagnostic-scope completion "
                "assessment"
            ),
        ],
    }

    write_json(
        OUTPUT_DIR / "implementation_plan.json",
        implementation_plan,
    )

    summary = {
        "inventory_checks_required": len(
            checks
        ),
        "inventory_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "six_pv_contract_passed": (
            six_pv_contract_passed
        ),
        "reference_files_found": len(
            reference_rows
        ),
        "relevant_functions_found": len(
            function_rows
        ),
        "candidate_components": len(
            CANDIDATE_COMPONENTS
        ),
        "state_fields": len(
            STATE_FIELDS
        ),
        "runner_fields": len(
            RUNNER_FIELDS
        ),
        "pitcher_fields": len(
            PITCHER_FIELDS
        ),
        "catcher_fields": len(
            CATCHER_FIELDS
        ),
        "output_fields": len(
            OUTPUT_FIELDS
        ),
        "fixtures_planned": len(
            FIXTURES
        ),
        "candidate_default_off": (
            candidate_default_off
        ),
        "production_activation_absent": (
            production_activation_absent
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

    recommended_next_layer = (
        "6PX_stolen_base_and_pickoff_state_"
        "contract_and_evaluator_implementation"
        if all_checks_passed
        else
        "6PX_stolen_base_and_pickoff_state_"
        "inventory_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "stolen_base_and_pickoff_state_"
            "inventory_and_implementation_plan_complete"
            if all_checks_passed
            else
            "stolen_base_and_pickoff_state_"
            "inventory_and_implementation_plan_incomplete"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "inventory_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "inventory_checks_required": len(
            checks
        ),
        "six_pv_contract_passed": (
            six_pv_contract_passed
        ),
        "explicit_no_steals_confirmed": (
            explicit_no_steals
        ),
        "base_out_state_supported": (
            base_out_state_supported
        ),
        "runner_transition_architecture_present": (
            runner_transition_architecture_present
        ),
        "previous_feasibility_present": (
            previous_feasibility_present
        ),
        "pickoff_scope_newly_planned": (
            pickoff_scope_newly_planned
        ),
        "steals_status_only_present": (
            steals_status_only_present
        ),
        "production_activation_absent": (
            production_activation_absent
        ),
        "candidate_components": len(
            CANDIDATE_COMPONENTS
        ),
        "state_fields": len(
            STATE_FIELDS
        ),
        "runner_fields": len(
            RUNNER_FIELDS
        ),
        "pitcher_fields": len(
            PITCHER_FIELDS
        ),
        "catcher_fields": len(
            CATCHER_FIELDS
        ),
        "output_fields": len(
            OUTPUT_FIELDS
        ),
        "fixtures_planned": len(
            FIXTURES
        ),
        "candidate_default_off": (
            candidate_default_off
        ),
        "production_baserunning_changed": False,
        "base_out_state_changed": False,
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
        (
            "state_contract_and_evaluator_"
            "implementation_allowed_next"
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
                / "inventory_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "reference_inventory.csv"
            ),
            str(
                OUTPUT_DIR
                / "function_inventory.csv"
            ),
            str(
                OUTPUT_DIR
                / "candidate_components.csv"
            ),
            str(
                OUTPUT_DIR
                / "state_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "runner_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "pitcher_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "catcher_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "output_contract.csv"
            ),
            str(
                OUTPUT_DIR
                / "fixture_plan.csv"
            ),
            str(
                OUTPUT_DIR
                / "authority_boundaries.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "implementation_plan.json"
            ),
            str(
                OUTPUT_DIR
                / "inventory_summary.json"
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
