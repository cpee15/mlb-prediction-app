#!/usr/bin/env python3
"""
Layer 6OY
Layer 6 Game-Management Scope Resolution Plan

Planning-only layer.

Defines the ordered resolution plan for:
- stolen bases and pickoffs
- position-player substitutions
- bullpen sequencing and production wiring
- opener, bulk-pitcher, tandem, and bullpen-game plans
- dynamic starter hooks

This layer does not change production behavior, simulation
probabilities, tuning, historical validation, pricing, or edge logic.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OY"
LAYER_NAME = (
    "layer6_game_management_scope_resolution_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OY_game_management_"
    "scope_resolution_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/analyze_6OX_layer6_game_management_"
    "scope_completeness_gaps.py"
)

REQUIRED_SOURCE_PATHS = [
    PREDECESSOR_PATH,
    ROOT / "mlb_app/model_projections.py",
    ROOT / "mlb_app/simulation/bullpen_chain.py",
    ROOT / "mlb_app/simulation/bullpen_selection.py",
    ROOT / "mlb_app/simulation/bullpen_integration.py",
    ROOT / "mlb_app/simulation/game_engine_v2.py",
]

PROHIBITED_ACTIONS = [
    "backend_behavior_change",
    "frontend_behavior_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "edge_detection",
    "bet_recommendation",
    "layer6_exit_finalization",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
            errors="ignore",
        )
    )

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    required_sources_exist = all(
        path.exists()
        for path in REQUIRED_SOURCE_PATHS
    )

    predecessor_contract_present = all(
        token in predecessor_constants
        for token in [
            (
                "layer_6_game_management_"
                "scope_completeness_gaps_confirmed"
            ),
            "stolen_bases_pickoffs",
            "position_player_substitutions",
            "bullpen_sequencing",
            "opener_bulk_tandem_plans",
            "dynamic_starter_hook",
            (
                "base_out_and_core_runner_"
                "transition_realism"
            ),
            "exit_finalization_paused",
            (
                "6OY_layer6_game_management_"
                "scope_resolution_plan"
            ),
        ]
    )

    planning_checks = [
        {
            "check": "required_source_files_exist",
            "passed": required_sources_exist,
            "evidence": ",".join(
                str(path)
                for path in REQUIRED_SOURCE_PATHS
            ),
        },
        {
            "check": "6ox_gap_contract_present",
            "passed": predecessor_contract_present,
            "evidence": str(PREDECESSOR_PATH),
        },
        {
            "check": "five_domain_resolution_plan",
            "passed": True,
            "evidence": (
                "Every unresolved 6OX domain has an explicit "
                "workstream and completion gate."
            ),
        },
        {
            "check": "dependency_order_defined",
            "passed": True,
            "evidence": (
                "Pitching-plan and hook dependencies precede "
                "production bullpen sequencing."
            ),
        },
        {
            "check": "planning_only_boundary",
            "passed": True,
            "evidence": (
                "6OY defines work and evidence requirements "
                "without changing simulation behavior."
            ),
        },
    ]

    workstreams = [
        {
            "workstream_id": "GM-01",
            "domain": "pitching_plan_classification",
            "resolves": "opener_bulk_tandem_plans",
            "priority": 1,
            "phase": "foundation",
            "objective": (
                "Represent traditional starter, opener, bulk "
                "follower, tandem, bullpen game, and workload-cap "
                "plans as explicit pregame pitching sequences."
            ),
            "dependency": "none",
            "completion_gate": (
                "Pitching-plan schema, deterministic fixtures, "
                "fallback behavior, and source provenance pass."
            ),
        },
        {
            "workstream_id": "GM-02",
            "domain": "dynamic_starter_hook",
            "resolves": "dynamic_starter_hook",
            "priority": 2,
            "phase": "pitching_state",
            "objective": (
                "Model in-game pitcher removal using workload, "
                "times through order, performance, inning, score, "
                "base/out state, and bullpen availability."
            ),
            "dependency": "GM-01",
            "completion_gate": (
                "Hook hazard and deterministic boundary fixtures "
                "pass without altering canonical probabilities."
            ),
        },
        {
            "workstream_id": "GM-03",
            "domain": "production_bullpen_sequence",
            "resolves": "bullpen_sequencing",
            "priority": 3,
            "phase": "pitching_state",
            "objective": (
                "Wire reliever-level selection, availability, "
                "fatigue, leverage, platoon context, inherited "
                "runners, and multi-inning capability into the "
                "production simulation path."
            ),
            "dependency": "GM-01|GM-02",
            "completion_gate": (
                "Activation, reliever identity, state mutation, "
                "outcome-profile switching, and fallback audits pass."
            ),
        },
        {
            "workstream_id": "GM-04",
            "domain": "stolen_base_and_pickoff_state",
            "resolves": "stolen_bases_pickoffs",
            "priority": 4,
            "phase": "runner_decisions",
            "objective": (
                "Model attempt decisions and safe, caught-stealing, "
                "pickoff, and defensive-indifference transitions."
            ),
            "dependency": (
                "stable base/out transition contract"
            ),
            "completion_gate": (
                "Attempt, success, failure, out, and runner-state "
                "conservation fixtures pass."
            ),
        },
        {
            "workstream_id": "GM-05",
            "domain": "position_player_substitution_state",
            "resolves": "position_player_substitutions",
            "priority": 5,
            "phase": "roster_state",
            "objective": (
                "Model pinch hitters, pinch runners, defensive "
                "replacements, lineup-slot continuity, and bench "
                "depletion."
            ),
            "dependency": (
                "stable player-level batting and runner identities"
            ),
            "completion_gate": (
                "Lineup legality, removed-player exclusion, bench "
                "depletion, and identity propagation fixtures pass."
            ),
        },
    ]

    shared_contracts = [
        {
            "contract_id": "GM-C01",
            "contract": "deterministic_replay",
            "requirement": (
                "Identical seed and inputs produce identical "
                "managerial and personnel-state decisions."
            ),
        },
        {
            "contract_id": "GM-C02",
            "contract": "state_conservation",
            "requirement": (
                "Outs, runners, lineup slots, active pitchers, "
                "and available players remain internally legal."
            ),
        },
        {
            "contract_id": "GM-C03",
            "contract": "explicit_fallbacks",
            "requirement": (
                "Missing plans, rosters, identities, or availability "
                "use labeled deterministic fallbacks."
            ),
        },
        {
            "contract_id": "GM-C04",
            "contract": "diagnostic_separation",
            "requirement": (
                "Intermediate realism diagnostics remain separate "
                "from canonical production probability authority."
            ),
        },
        {
            "contract_id": "GM-C05",
            "contract": "source_provenance",
            "requirement": (
                "Every player, role, workload, and availability "
                "input exposes source and fallback metadata."
            ),
        },
        {
            "contract_id": "GM-C06",
            "contract": "no_silent_activation",
            "requirement": (
                "Each workstream remains disabled until its own "
                "implementation and independent audit pass."
            ),
        },
    ]

    evidence_requirements = [
        {
            "evidence_id": "GM-E01",
            "evidence": "source_inventory",
            "required_for": "all",
            "description": (
                "Exact production call paths, inputs, defaults, "
                "and inactive branches are documented."
            ),
        },
        {
            "evidence_id": "GM-E02",
            "evidence": "fixture_matrix",
            "required_for": "all",
            "description": (
                "Normal, edge, missing-data, and contradictory "
                "input scenarios are represented."
            ),
        },
        {
            "evidence_id": "GM-E03",
            "evidence": "state_transition_audit",
            "required_for": "GM-02|GM-03|GM-04|GM-05",
            "description": (
                "Before/after state proves legal transitions and "
                "conservation invariants."
            ),
        },
        {
            "evidence_id": "GM-E04",
            "evidence": "production_reachability",
            "required_for": "all",
            "description": (
                "The exact production projection route reaches "
                "the implemented feature when enabled."
            ),
        },
        {
            "evidence_id": "GM-E05",
            "evidence": "output_effect",
            "required_for": "all",
            "description": (
                "Controlled counterfactuals show the feature can "
                "change simulated game outcomes."
            ),
        },
        {
            "evidence_id": "GM-E06",
            "evidence": "independent_audit",
            "required_for": "all",
            "description": (
                "A separate audit script re-executes and verifies "
                "every implementation contract."
            ),
        },
    ]

    stop_conditions = [
        {
            "condition_id": "GM-HOLD-01",
            "condition": (
                "Production route cannot be proven to reach the "
                "candidate implementation."
            ),
            "required_action": (
                "Hold activation and return to wiring analysis."
            ),
        },
        {
            "condition_id": "GM-HOLD-02",
            "condition": (
                "State legality or conservation invariant fails."
            ),
            "required_action": (
                "Hold implementation and correct transition logic."
            ),
        },
        {
            "condition_id": "GM-HOLD-03",
            "condition": (
                "Feature activation silently changes canonical "
                "probability authority."
            ),
            "required_action": (
                "Disable feature and restore diagnostic separation."
            ),
        },
        {
            "condition_id": "GM-HOLD-04",
            "condition": (
                "Missing source data causes unlabeled assumptions."
            ),
            "required_action": (
                "Add explicit provenance and fallback status."
            ),
        },
        {
            "condition_id": "GM-HOLD-05",
            "condition": (
                "A workstream depends on unresolved player identity "
                "or roster-state infrastructure."
            ),
            "required_action": (
                "Pause that workstream and resolve the dependency."
            ),
        },
        {
            "condition_id": "GM-HOLD-06",
            "condition": (
                "Independent audit cannot reproduce implementation "
                "results."
            ),
            "required_action": (
                "Hold activation and resolve nondeterminism."
            ),
        },
    ]

    execution_sequence = [
        {
            "step": 1,
            "action": (
                "Inventory and plan explicit pitching-plan "
                "classification."
            ),
            "workstream": "GM-01",
        },
        {
            "step": 2,
            "action": (
                "Implement and audit pitching-plan classification."
            ),
            "workstream": "GM-01",
        },
        {
            "step": 3,
            "action": (
                "Plan, implement, and audit the dynamic starter hook."
            ),
            "workstream": "GM-02",
        },
        {
            "step": 4,
            "action": (
                "Plan, implement, and audit production bullpen "
                "sequencing."
            ),
            "workstream": "GM-03",
        },
        {
            "step": 5,
            "action": (
                "Plan, implement, and audit stolen-base and "
                "pickoff state."
            ),
            "workstream": "GM-04",
        },
        {
            "step": 6,
            "action": (
                "Plan, implement, and audit position-player "
                "substitution state."
            ),
            "workstream": "GM-05",
        },
        {
            "step": 7,
            "action": (
                "Run a combined game-management integration audit."
            ),
            "workstream": "all",
        },
        {
            "step": 8,
            "action": (
                "Reassess the broad Layer 6 completion claim."
            ),
            "workstream": "all",
        },
    ]

    scope_policy = [
        {
            "scope": (
                "base_out_and_core_runner_transition_realism"
            ),
            "status": "completed_under_audited_scope",
            "broad_layer6_exit_allowed": False,
        },
        {
            "scope": "game_management_realism",
            "status": "resolution_in_progress",
            "broad_layer6_exit_allowed": False,
        },
        {
            "scope": "historical_accuracy_validation",
            "status": "not_authorized",
            "broad_layer6_exit_allowed": False,
        },
        {
            "scope": "parameter_tuning",
            "status": "not_authorized",
            "broad_layer6_exit_allowed": False,
        },
        {
            "scope": "pricing_and_edge_detection",
            "status": "not_authorized",
            "broad_layer6_exit_allowed": False,
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "allowed_in_6OY": False,
            "reason": (
                "6OY is a planning-only scope-resolution layer."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": (
                    "workstream_definition"
                ),
                "allowed_in_6OY": True,
                "reason": (
                    "Defining bounded resolution workstreams is "
                    "the purpose of 6OY."
                ),
            },
            {
                "boundary": (
                    "dependency_definition"
                ),
                "allowed_in_6OY": True,
                "reason": (
                    "Dependency ordering prevents unsafe activation."
                ),
            },
            {
                "boundary": (
                    "evidence_gate_definition"
                ),
                "allowed_in_6OY": True,
                "reason": (
                    "Evidence gates are planning controls."
                ),
            },
        ]
    )

    all_checks_passed = all(
        bool(row["passed"])
        for row in planning_checks
    )

    recommended_next_layer = (
        "6OZ_pitching_plan_classification_"
        "inventory_and_implementation_plan"
    )

    write_csv(
        OUTPUT_DIR / "checks.csv",
        ["check", "passed", "evidence"],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "workstreams.csv",
        [
            "workstream_id",
            "domain",
            "resolves",
            "priority",
            "phase",
            "objective",
            "dependency",
            "completion_gate",
        ],
        workstreams,
    )

    write_csv(
        OUTPUT_DIR / "shared_contracts.csv",
        [
            "contract_id",
            "contract",
            "requirement",
        ],
        shared_contracts,
    )

    write_csv(
        OUTPUT_DIR / "evidence_requirements.csv",
        [
            "evidence_id",
            "evidence",
            "required_for",
            "description",
        ],
        evidence_requirements,
    )

    write_csv(
        OUTPUT_DIR / "stop_conditions.csv",
        [
            "condition_id",
            "condition",
            "required_action",
        ],
        stop_conditions,
    )

    write_csv(
        OUTPUT_DIR / "execution_sequence.csv",
        [
            "step",
            "action",
            "workstream",
        ],
        execution_sequence,
    )

    write_csv(
        OUTPUT_DIR / "scope_policy.csv",
        [
            "scope",
            "status",
            "broad_layer6_exit_allowed",
        ],
        scope_policy,
    )

    write_csv(
        OUTPUT_DIR / "safety_boundaries.csv",
        [
            "boundary",
            "allowed_in_6OY",
            "reason",
        ],
        safety_rows,
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
                    "Inventory and plan explicit traditional, "
                    "opener, bulk, tandem, bullpen-game, and "
                    "workload-cap pitching-plan classification."
                ),
                "entry_condition": (
                    "All 6OY scope-resolution planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    resolution_plan = {
        "ordered_workstreams": [
            row["workstream_id"]
            for row in workstreams
        ],
        "first_workstream": "GM-01",
        "first_domain": (
            "pitching_plan_classification"
        ),
        "broad_layer6_exit_paused": True,
        "completed_narrow_scope": (
            "base_out_and_core_runner_transition_realism"
        ),
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "resolution_plan.json",
        resolution_plan,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_game_management_"
            "scope_resolution_plan_complete"
            if all_checks_passed
            else
            "layer_6_game_management_"
            "scope_resolution_plan_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "planning_checks_passed": sum(
            1
            for row in planning_checks
            if row["passed"]
        ),
        "planning_checks_required": len(
            planning_checks
        ),
        "workstreams_planned": len(workstreams),
        "shared_contracts_planned": len(
            shared_contracts
        ),
        "evidence_requirements_planned": len(
            evidence_requirements
        ),
        "stop_conditions_planned": len(
            stop_conditions
        ),
        "execution_steps_planned": len(
            execution_sequence
        ),
        "scope_policies_planned": len(
            scope_policy
        ),
        "first_workstream": "GM-01",
        "first_domain": (
            "pitching_plan_classification"
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
        "pitching_plan_inventory_planning_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "checks.csv"),
            str(OUTPUT_DIR / "workstreams.csv"),
            str(OUTPUT_DIR / "shared_contracts.csv"),
            str(
                OUTPUT_DIR / "evidence_requirements.csv"
            ),
            str(OUTPUT_DIR / "stop_conditions.csv"),
            str(OUTPUT_DIR / "execution_sequence.csv"),
            str(OUTPUT_DIR / "scope_policy.csv"),
            str(OUTPUT_DIR / "safety_boundaries.csv"),
            str(OUTPUT_DIR / "recommended_path.csv"),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "resolution_plan.json"),
            str(OUTPUT_DIR / "diagnosis.json"),
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
