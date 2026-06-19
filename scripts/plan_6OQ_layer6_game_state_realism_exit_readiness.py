#!/usr/bin/env python3
"""
Layer 6OQ
Layer 6 Game-State Realism Exit Readiness Plan

Planning-only layer.

Defines the criteria and evidence required before Layer 6 game-state realism
can be considered ready for a formal exit-readiness implementation and audit.

No production behavior, probability, tuning, pricing, or edge logic changes.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OQ"
LAYER_NAME = "layer6_game_state_realism_exit_readiness_plan"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OQ_game_state_realism_exit_readiness_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/audit_6OP_model_projection_realism_"
    "end_to_end_runtime_validation.py"
)

BACKEND_PATH = ROOT / "mlb_app/model_projections.py"
FRONTEND_PATH = ROOT / "frontend/src/pages/ModelProjectionsPage.jsx"

REQUIRED_FIELDS = [
    "base_out_state_enabled",
    "runner_advancement_enabled",
    "extras_enabled",
    "ghost_runner_enabled",
    "walkoff_shortening_enabled",
    "double_play_enabled",
    "sac_fly_enabled",
    "steals_model_status",
]

DETAIL_FIELDS = [
    "base_out_transition_model_status",
    "base_out_simulation_summary",
    "runner_advancement_model_status",
    "runner_advancement_summary",
    "extras_walkoff_model_status",
    "double_play_rate_source",
    "double_play_transition_summary",
    "sac_fly_rate_source",
    "sac_fly_transition_summary",
    "steals_projection_wiring_status",
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


def write_json(
    path: Path,
    payload: Any,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
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

    backend_text = (
        BACKEND_PATH.read_text(
            encoding="utf-8",
            errors="ignore",
        )
        if BACKEND_PATH.exists()
        else ""
    )

    frontend_text = (
        FRONTEND_PATH.read_text(
            encoding="utf-8",
            errors="ignore",
        )
        if FRONTEND_PATH.exists()
        else ""
    )

    predecessor_contract_present = all(
        token in predecessor_constants
        for token in [
            (
                "layer_6_model_projection_realism_"
                "end_to_end_runtime_validation_"
                "audit_complete"
            ),
            "all_checks_passed",
            "probability_guard_verified",
            "frontend_build_verified",
            "manual_runtime_verified",
            "exit_readiness_planning_allowed_next",
            (
                "6OQ_layer6_game_state_realism_"
                "exit_readiness_plan"
            ),
        ]
    )

    backend_contract_present = all(
        field in backend_text
        for field in REQUIRED_FIELDS + DETAIL_FIELDS
    )

    frontend_contract_present = all(
        token in frontend_text
        for token in [
            "function GameProjectionCard({ game })",
            (
                "renderGameStateRealismDiagnostics("
                "game?.game_state_realism)"
            ),
            "if (!gameStateRealism) return null",
            'return "Unavailable"',
            (
                "Diagnostic-only. Does not replace final "
                "projection probability."
            ),
        ]
    )

    probability_separation_present = all(
        token in backend_text
        for token in [
            (
                "canonical_probabilities = "
                "_canonical_probability_payload"
            ),
            (
                '"game_state_realism": '
                "_build_game_state_realism_diagnostics()"
            ),
            "diagnostic_only_not_final_probability",
        ]
    )

    checks = [
        {
            "check": "required_source_files_exist",
            "passed": all(
                path.exists()
                for path in [
                    PREDECESSOR_PATH,
                    BACKEND_PATH,
                    FRONTEND_PATH,
                ]
            ),
            "evidence": ",".join(
                str(path)
                for path in [
                    PREDECESSOR_PATH,
                    BACKEND_PATH,
                    FRONTEND_PATH,
                ]
            ),
        },
        {
            "check": "6op_predecessor_contract_present",
            "passed": predecessor_contract_present,
            "evidence": str(PREDECESSOR_PATH),
        },
        {
            "check": "backend_realism_contract_present",
            "passed": backend_contract_present,
            "evidence": (
                f"{len(REQUIRED_FIELDS)} required fields and "
                f"{len(DETAIL_FIELDS)} detail fields"
            ),
        },
        {
            "check": "frontend_realism_contract_present",
            "passed": frontend_contract_present,
            "evidence": (
                "safe GameProjectionCard scope, fallbacks, "
                "and disclaimer"
            ),
        },
        {
            "check": "probability_separation_present",
            "passed": probability_separation_present,
            "evidence": (
                "diagnostics remain separate from canonical "
                "probabilities"
            ),
        },
    ]

    exit_domains = [
        {
            "domain": "base_out_state",
            "current_status": "implemented_and_diagnostically_exposed",
            "required_exit_evidence": (
                "Transition logic exists, is wired into the shared "
                "simulation path, and is exposed without altering "
                "canonical probabilities."
            ),
            "blocking": True,
        },
        {
            "domain": "runner_advancement",
            "current_status": "implemented_and_diagnostically_exposed",
            "required_exit_evidence": (
                "Single, double, ground-ball, and fly-ball runner "
                "movement behavior is installed and structurally "
                "validated."
            ),
            "blocking": True,
        },
        {
            "domain": "extra_innings",
            "current_status": "implemented_and_diagnostically_exposed",
            "required_exit_evidence": (
                "Extra innings, ghost runner, and walk-off shortening "
                "are implemented and validated."
            ),
            "blocking": True,
        },
        {
            "domain": "double_plays",
            "current_status": "implemented_and_diagnostically_exposed",
            "required_exit_evidence": (
                "Double-play transition logic is present with "
                "documented rate provenance."
            ),
            "blocking": True,
        },
        {
            "domain": "sacrifice_flies",
            "current_status": "implemented_and_diagnostically_exposed",
            "required_exit_evidence": (
                "Sac-fly transition logic is present with documented "
                "rate provenance."
            ),
            "blocking": True,
        },
        {
            "domain": "steals",
            "current_status": "explicitly_deferred_not_active",
            "required_exit_evidence": (
                "Deferral is intentional, documented, non-misleading, "
                "and does not imply active steal simulation."
            ),
            "blocking": True,
        },
        {
            "domain": "backend_payload",
            "current_status": "implemented_and_audited",
            "required_exit_evidence": (
                "Exactly one per-game diagnostics group survives "
                "serialization with complete type-safe fields."
            ),
            "blocking": True,
        },
        {
            "domain": "frontend_visibility",
            "current_status": "implemented_and_audited",
            "required_exit_evidence": (
                "Safe rendering, complete fallbacks, no blank screen, "
                "and clear diagnostic-only labeling."
            ),
            "blocking": True,
        },
        {
            "domain": "probability_guard",
            "current_status": "implemented_and_audited",
            "required_exit_evidence": (
                "Canonical probabilities remain unchanged and separate "
                "from diagnostics."
            ),
            "blocking": True,
        },
        {
            "domain": "runtime_validation",
            "current_status": "implemented_and_audited",
            "required_exit_evidence": (
                "Representative runtime, fixture, frontend build, and "
                "manual deployed-page evidence all pass."
            ),
            "blocking": True,
        },
    ]

    exit_criteria = [
        {
            "criterion_id": "L6-GSR-01",
            "criterion": (
                "Every active game-state realism mechanic is present "
                "in production simulation code."
            ),
            "evidence_source": (
                "simulation implementation and prior Layer 6 audits"
            ),
            "blocking": True,
        },
        {
            "criterion_id": "L6-GSR-02",
            "criterion": (
                "Every active mechanic has clear status and provenance "
                "in diagnostics."
            ),
            "evidence_source": (
                "game_state_realism backend payload"
            ),
            "blocking": True,
        },
        {
            "criterion_id": "L6-GSR-03",
            "criterion": (
                "Deferred mechanics are explicitly labeled as inactive."
            ),
            "evidence_source": (
                "steals_model_status and UI labeling"
            ),
            "blocking": True,
        },
        {
            "criterion_id": "L6-GSR-04",
            "criterion": (
                "Diagnostics serialize independently on every "
                "successful game payload."
            ),
            "evidence_source": "6OO and 6OP fixture evidence",
            "blocking": True,
        },
        {
            "criterion_id": "L6-GSR-05",
            "criterion": (
                "Frontend rendering is safely scoped and handles "
                "missing or partial payloads."
            ),
            "evidence_source": (
                "frontend contract checks and manual runtime smoke"
            ),
            "blocking": True,
        },
        {
            "criterion_id": "L6-GSR-06",
            "criterion": (
                "Production frontend build succeeds."
            ),
            "evidence_source": (
                "Vite build log and exit code"
            ),
            "blocking": True,
        },
        {
            "criterion_id": "L6-GSR-07",
            "criterion": (
                "Canonical probabilities are unchanged by diagnostics."
            ),
            "evidence_source": (
                "probability guard evidence"
            ),
            "blocking": True,
        },
        {
            "criterion_id": "L6-GSR-08",
            "criterion": (
                "No tuning, historical accuracy, pricing, or edge "
                "claims are inferred from diagnostic completion."
            ),
            "evidence_source": (
                "Layer safety boundaries"
            ),
            "blocking": True,
        },
        {
            "criterion_id": "L6-GSR-09",
            "criterion": (
                "Known non-blocking frontend warnings are documented "
                "and separated from Layer 6 realism readiness."
            ),
            "evidence_source": (
                "frontend build warning inventory"
            ),
            "blocking": False,
        },
        {
            "criterion_id": "L6-GSR-10",
            "criterion": (
                "A formal exit-readiness implementation independently "
                "reconstructs the complete evidence chain."
            ),
            "evidence_source": (
                "next-layer implementation"
            ),
            "blocking": True,
        },
    ]

    known_gaps = [
        {
            "gap_id": "GSR-GAP-01",
            "description": (
                "Steal simulation remains intentionally deferred."
            ),
            "classification": "accepted_scope_boundary",
            "blocks_exit": False,
            "required_resolution": (
                "Retain explicit deferred_not_active status and avoid "
                "claims that steals are modeled."
            ),
        },
        {
            "gap_id": "GSR-GAP-02",
            "description": (
                "Current realism diagnostics are status-oriented and "
                "do not provide historical accuracy validation."
            ),
            "classification": "future_validation_scope",
            "blocks_exit": False,
            "required_resolution": (
                "Do not treat diagnostics as calibration or predictive "
                "accuracy evidence."
            ),
        },
        {
            "gap_id": "GSR-GAP-03",
            "description": (
                "Frontend build emits unrelated duplicate-key, "
                "dependency-audit, and bundle-size warnings."
            ),
            "classification": "nonblocking_unrelated_technical_debt",
            "blocks_exit": False,
            "required_resolution": (
                "Track separately; do not modify inside Layer 6 "
                "game-state realism exit work."
            ),
        },
    ]

    required_exit_artifacts = [
        {
            "artifact": "exit_readiness_checks.csv",
            "purpose": (
                "Machine-readable pass/fail matrix for all exit criteria."
            ),
            "required": True,
        },
        {
            "artifact": "domain_readiness.csv",
            "purpose": (
                "Readiness state for each game-state realism domain."
            ),
            "required": True,
        },
        {
            "artifact": "known_gaps.csv",
            "purpose": (
                "Explicit accepted gaps and blocking classification."
            ),
            "required": True,
        },
        {
            "artifact": "evidence_chain.json",
            "purpose": (
                "Consolidated evidence from implementation, payload, "
                "serialization, frontend, build, runtime, and guard."
            ),
            "required": True,
        },
        {
            "artifact": "diagnosis.json",
            "purpose": (
                "Final readiness diagnosis and next-layer recommendation."
            ),
            "required": True,
        },
    ]

    execution_sequence = [
        {
            "step": 1,
            "action": (
                "Re-execute the completed 6OP audit."
            ),
            "success_criterion": (
                "All 6OP audit checks pass."
            ),
        },
        {
            "step": 2,
            "action": (
                "Reconstruct the evidence chain for every Layer 6 "
                "game-state realism domain."
            ),
            "success_criterion": (
                "Every active domain has implementation, payload, "
                "serialization, and runtime evidence."
            ),
        },
        {
            "step": 3,
            "action": (
                "Verify accepted scope boundaries and deferred mechanics."
            ),
            "success_criterion": (
                "Deferred mechanics remain explicitly inactive and "
                "non-misleading."
            ),
        },
        {
            "step": 4,
            "action": (
                "Verify canonical probability separation."
            ),
            "success_criterion": (
                "No diagnostic field changes probability values or "
                "selection paths."
            ),
        },
        {
            "step": 5,
            "action": (
                "Classify all remaining gaps as blocking or nonblocking."
            ),
            "success_criterion": (
                "No unidentified blocking gap remains."
            ),
        },
        {
            "step": 6,
            "action": (
                "Produce an exit-readiness diagnosis."
            ),
            "success_criterion": (
                "Diagnosis is supported by machine-readable artifacts."
            ),
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "allowed_in_6OQ": False,
            "reason": (
                "6OQ defines exit-readiness criteria only."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": "exit_criteria_definition",
                "allowed_in_6OQ": True,
                "reason": (
                    "Defining exit criteria is the purpose of 6OQ."
                ),
            },
            {
                "boundary": "known_gap_classification",
                "allowed_in_6OQ": True,
                "reason": (
                    "Classifying gaps does not change production behavior."
                ),
            },
            {
                "boundary": "evidence_chain_planning",
                "allowed_in_6OQ": True,
                "reason": (
                    "Planning evidence reconstruction is non-behavioral."
                ),
            },
        ]
    )

    all_checks_passed = all(
        bool(row["passed"])
        for row in checks
    )

    recommended_next_layer = (
        "6OR_layer6_game_state_realism_"
        "exit_readiness_implementation"
    )

    write_csv(
        OUTPUT_DIR / "checks.csv",
        ["check", "passed", "evidence"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "domain_readiness_plan.csv",
        [
            "domain",
            "current_status",
            "required_exit_evidence",
            "blocking",
        ],
        exit_domains,
    )

    write_csv(
        OUTPUT_DIR / "exit_criteria.csv",
        [
            "criterion_id",
            "criterion",
            "evidence_source",
            "blocking",
        ],
        exit_criteria,
    )

    write_csv(
        OUTPUT_DIR / "known_gaps.csv",
        [
            "gap_id",
            "description",
            "classification",
            "blocks_exit",
            "required_resolution",
        ],
        known_gaps,
    )

    write_csv(
        OUTPUT_DIR / "required_exit_artifacts.csv",
        [
            "artifact",
            "purpose",
            "required",
        ],
        required_exit_artifacts,
    )

    write_csv(
        OUTPUT_DIR / "execution_sequence.csv",
        [
            "step",
            "action",
            "success_criterion",
        ],
        execution_sequence,
    )

    write_csv(
        OUTPUT_DIR / "safety_boundaries.csv",
        [
            "boundary",
            "allowed_in_6OQ",
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
                    "Implement the formal Layer 6 game-state realism "
                    "exit-readiness evidence reconstruction and "
                    "blocking-gap assessment."
                ),
                "entry_condition": (
                    "All 6OQ planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_game_state_realism_"
            "exit_readiness_plan_complete"
            if all_checks_passed
            else
            "layer_6_game_state_realism_"
            "exit_readiness_plan_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "domains_planned": len(exit_domains),
        "exit_criteria_planned": len(exit_criteria),
        "blocking_exit_criteria": sum(
            1
            for row in exit_criteria
            if row["blocking"]
        ),
        "known_gaps_documented": len(known_gaps),
        "blocking_known_gaps": sum(
            1
            for row in known_gaps
            if row["blocks_exit"]
        ),
        "required_exit_artifacts_planned": len(
            required_exit_artifacts
        ),
        "execution_steps_planned": len(
            execution_sequence
        ),
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
        "layer6_exit_recommended": False,
        "exit_readiness_implementation_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "checks.csv"),
            str(
                OUTPUT_DIR / "domain_readiness_plan.csv"
            ),
            str(OUTPUT_DIR / "exit_criteria.csv"),
            str(OUTPUT_DIR / "known_gaps.csv"),
            str(
                OUTPUT_DIR
                / "required_exit_artifacts.csv"
            ),
            str(
                OUTPUT_DIR / "execution_sequence.csv"
            ),
            str(
                OUTPUT_DIR / "safety_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
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
