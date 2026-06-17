#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path

LAYER_ID = "6OB"
LAYER_NAME = "layer6_projection_wiring_gap_resolution_plan"
SLUG = "layer_6OB_projection_wiring_gap_resolution_plan"

OUTDIR = Path("tmp") / SLUG
OUTDIR.mkdir(parents=True, exist_ok=True)

RECOMMENDED_NEXT_LAYER = "6OC_layer6_projection_wiring_gap_resolution_audit"

WIRING_TARGETS = [
    {
        "feature_name": "base_out_state",
        "current_classification": "sim_reachable_not_projection_reachable",
        "gap_type": "projection_wiring_gap",
        "wiring_objective": "Expose base/out state-aware simulation path to Model Projections builder without changing model probabilities yet.",
        "source_sim_component": "mlb_app/simulation/inning_simulator.py;mlb_app/simulation/subtype_transitions.py;mlb_app/simulation/layer6_base_out_transition_adapter.py",
        "projection_target": "mlb_app/model_projections.py",
        "ui_target": "frontend/src/pages/ModelProjectionsPage.jsx",
        "implementation_mode": "adapter_or_payload_wiring_plan_only",
        "priority": "critical",
    },
    {
        "feature_name": "base_advancement_transitions",
        "current_classification": "sim_reachable_not_projection_reachable",
        "gap_type": "projection_wiring_gap",
        "wiring_objective": "Expose probabilistic runner advancement transition outputs to Model Projections diagnostic simulation output.",
        "source_sim_component": "mlb_app/simulation/inning_simulator.py;mlb_app/simulation/subtype_transitions.py",
        "projection_target": "mlb_app/model_projections.py",
        "ui_target": "frontend/src/pages/ModelProjectionsPage.jsx",
        "implementation_mode": "adapter_or_payload_wiring_plan_only",
        "priority": "critical",
    },
    {
        "feature_name": "ghost_runner_extra_innings",
        "current_classification": "sim_reachable_not_projection_reachable",
        "gap_type": "projection_wiring_gap",
        "wiring_objective": "Plan connection from extras/walkoff simulator to Model Projections simulation output without replacing final side probability.",
        "source_sim_component": "mlb_app/simulation/game_rules.py",
        "projection_target": "mlb_app/model_projections.py",
        "ui_target": "frontend/src/pages/ModelProjectionsPage.jsx",
        "implementation_mode": "adapter_or_payload_wiring_plan_only",
        "priority": "critical",
    },
    {
        "feature_name": "double_play_logic",
        "current_classification": "sim_reachable_not_projection_reachable",
        "gap_type": "projection_wiring_gap",
        "wiring_objective": "Plan diagnostic exposure for double-play transition rates and effects through base/out simulation output.",
        "source_sim_component": "mlb_app/simulation/inning_simulator.py;mlb_app/simulation/subtype_transitions.py",
        "projection_target": "mlb_app/model_projections.py",
        "ui_target": "frontend/src/pages/ModelProjectionsPage.jsx",
        "implementation_mode": "adapter_or_payload_wiring_plan_only",
        "priority": "high",
    },
    {
        "feature_name": "sac_fly_logic",
        "current_classification": "sim_reachable_not_projection_reachable",
        "gap_type": "projection_wiring_gap",
        "wiring_objective": "Plan diagnostic exposure for sac-fly transition rates and effects through base/out simulation output.",
        "source_sim_component": "mlb_app/simulation/inning_simulator.py",
        "projection_target": "mlb_app/model_projections.py",
        "ui_target": "frontend/src/pages/ModelProjectionsPage.jsx",
        "implementation_mode": "adapter_or_payload_wiring_plan_only",
        "priority": "high",
    },
    {
        "feature_name": "steals_caught_stealing",
        "current_classification": "sim_reachable_not_projection_reachable",
        "gap_type": "projection_wiring_gap",
        "wiring_objective": "Plan whether steal/caught-stealing placeholder should be wired, deferred, or redesigned before projection exposure.",
        "source_sim_component": "mlb_app/simulation/inning_simulator.py",
        "projection_target": "mlb_app/model_projections.py",
        "ui_target": "frontend/src/pages/ModelProjectionsPage.jsx",
        "implementation_mode": "defer_or_wire_decision_plan_only",
        "priority": "high",
    },
]

DEFERRED_FEATURES = [
    {
        "feature_name": "opener_bulk_pitcher",
        "reason": "Classified as absent; requires design/feature implementation path rather than projection wiring.",
        "recommended_future_layer": "6OD_layer6_opener_bulk_pitcher_design_plan",
    },
    {
        "feature_name": "balks",
        "reason": "Classified as absent and low priority; defer until core game-state projection wiring is resolved.",
        "recommended_future_layer": "6OE_layer6_low_frequency_event_design_plan",
    },
    {
        "feature_name": "wild_pitch_passed_ball",
        "reason": "Classified as absent and low priority; defer until core game-state projection wiring is resolved.",
        "recommended_future_layer": "6OE_layer6_low_frequency_event_design_plan",
    },
]

SAFETY = [
    {"boundary": "planning_only", "passed": True},
    {"boundary": "no_feature_wiring", "passed": True},
    {"boundary": "no_tuning", "passed": True},
    {"boundary": "no_historical_validation", "passed": True},
    {"boundary": "no_prediction_join", "passed": True},
    {"boundary": "no_accuracy_metrics", "passed": True},
    {"boundary": "no_backtests", "passed": True},
    {"boundary": "no_pricing", "passed": True},
    {"boundary": "no_edge_detection", "passed": True},
    {"boundary": "layer6_exit_not_recommended", "passed": True},
]


def write_csv(path: Path, rows):
    rows = list(rows) or [{"empty": True}]
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    checks = [
        {"check": "wiring_targets_defined", "passed": len(WIRING_TARGETS) == 6},
        {"check": "critical_targets_included", "passed": {"base_out_state", "base_advancement_transitions", "ghost_runner_extra_innings"}.issubset({r["feature_name"] for r in WIRING_TARGETS})},
        {"check": "opener_bulk_deferred_from_projection_wiring", "passed": any(r["feature_name"] == "opener_bulk_pitcher" for r in DEFERRED_FEATURES)},
        {"check": "safety_boundaries", "passed": all(r["passed"] for r in SAFETY)},
    ]

    recommended = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": "audit_projection_wiring_gap_resolution_plan_before_implementation",
            "reason": "Projection wiring plan should be audited before implementation touches Model Projections or simulator adapters.",
            "passed": True,
        }
    ]

    all_passed = all(row["passed"] for row in checks)

    artifacts = {
        "checks": OUTDIR / "checks.csv",
        "wiring_targets": OUTDIR / "projection_wiring_targets.csv",
        "deferred_features": OUTDIR / "deferred_features.csv",
        "safety": OUTDIR / "safety_boundaries.csv",
        "recommended": OUTDIR / "recommended_path.csv",
        "diagnosis": OUTDIR / "diagnosis.json",
    }

    write_csv(artifacts["checks"], checks)
    write_csv(artifacts["wiring_targets"], WIRING_TARGETS)
    write_csv(artifacts["deferred_features"], DEFERRED_FEATURES)
    write_csv(artifacts["safety"], SAFETY)
    write_csv(artifacts["recommended"], recommended)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_projection_wiring_gap_resolution_plan_complete" if all_passed else "layer_6_projection_wiring_gap_resolution_plan_failed",
        "all_checks_passed": all_passed,
        "wiring_targets_count": len(WIRING_TARGETS),
        "critical_targets_included": True,
        "opener_bulk_pitcher_deferred_to_design_path": True,
        "feature_wiring_allowed_next": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "accuracy_metrics_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "layer6_exit_recommended": False,
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "generated_csv_artifacts": [
            str(artifacts["checks"]),
            str(artifacts["wiring_targets"]),
            str(artifacts["deferred_features"]),
            str(artifacts["safety"]),
            str(artifacts["recommended"]),
        ],
        "generated_json_artifacts": [str(artifacts["diagnosis"])],
    }

    artifacts["diagnosis"].write_text(json.dumps(diagnosis, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
