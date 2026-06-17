#!/usr/bin/env python3
"""Layer 6NV — Model Projection Realism Injection Inventory Plan."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


LAYER_ID = "6NV"
LAYER_NAME = "layer6_model_projection_realism_injection_inventory_plan"
SLUG = "layer_6NV_model_projection_realism_injection_inventory_plan"

TMP_DIR = Path("tmp") / SLUG
TMP_DIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/audit_6NU_model_projection_realism_injection_map.py")

JSON_PATH = TMP_DIR / "diagnosis.json"
CHECKS_CSV = TMP_DIR / "checks.csv"
PREDECESSOR_CSV = TMP_DIR / "predecessor.csv"
SOURCE_TARGETS_CSV = TMP_DIR / "source_targets.csv"
CLASSIFICATION_CONTRACT_CSV = TMP_DIR / "classification_contract.csv"
FEATURE_INVENTORY_PLAN_CSV = TMP_DIR / "feature_inventory_plan.csv"
EVIDENCE_FIELDS_CSV = TMP_DIR / "evidence_fields.csv"
SAFETY_CSV = TMP_DIR / "safety_boundaries.csv"
DECISION_CSV = TMP_DIR / "decision.csv"
RECOMMENDED_CSV = TMP_DIR / "recommended_path.csv"

RECOMMENDED_NEXT_LAYER = "6NW_layer6_model_projection_realism_injection_inventory_audit"
RECOMMENDED_PATH = "audit_inventory_plan_before_source_inspection_implementation"

FEATURES = [
    "bullpen_transition",
    "dynamic_starter_exit",
    "opener_bulk_pitcher",
    "individual_reliever_selection",
    "leverage_bullpen_usage",
    "base_out_state",
    "base_advancement_transitions",
    "double_play_logic",
    "sac_fly_logic",
    "extra_innings_ghost_runner",
    "walkoff_shortening",
    "steals_caught_stealing",
    "balks",
    "wild_pitch_passed_ball",
    "lineup_order_state",
]

SOURCE_TARGETS = [
    "mlb_app/model_projections.py",
    "mlb_app/model_projection_routes.py",
    "mlb_app/model_projection_payload.py",
    "frontend/src/pages/ModelProjectionsPage.jsx",
    "mlb_app/simulation/game_simulator.py",
    "mlb_app/simulation/game_engine_v2.py",
    "mlb_app/simulation/inning_simulator.py",
    "mlb_app/simulation/subtype_transitions.py",
    "mlb_app/simulation/outcome_subtypes.py",
    "mlb_app/simulation/game_rules.py",
    "mlb_app/simulation/bullpen_chain.py",
    "mlb_app/simulation/bullpen_integration.py",
    "mlb_app/simulation/bullpen_game_engine_hook.py",
    "mlb_app/simulation/formula_map.py",
    "mlb_app/simulation/layer6_base_out_transition_adapter.py",
]

CLASSIFICATIONS = [
    {
        "classification": "absent",
        "definition": "No meaningful source evidence for the feature family exists in inspected targets.",
        "wiring_allowed": False,
    },
    {
        "classification": "present_not_reachable",
        "definition": "Feature logic or artifacts exist, but no simulator path consumes them.",
        "wiring_allowed": False,
    },
    {
        "classification": "sim_reachable_not_projection_reachable",
        "definition": "Simulator can reach the feature, but Model Projections builder does not consume its output.",
        "wiring_allowed": False,
    },
    {
        "classification": "projection_reachable_not_ui_visible",
        "definition": "Model Projections builder can consume feature-aware output, but payload/UI does not expose it.",
        "wiring_allowed": False,
    },
    {
        "classification": "ui_visible_diagnostic_only",
        "definition": "Feature-aware output is visible in Model Projections diagnostics but not final projection probability/output.",
        "wiring_allowed": False,
    },
    {
        "classification": "active_in_model_projection_output",
        "definition": "Feature affects the simulation output consumed by Model Projections and is visible in UI-facing output.",
        "wiring_allowed": False,
    },
]

FEATURE_PATTERNS = {
    "bullpen_transition": "bullpen;simulate_game_with_bullpen;bullpen_adjusted_game_simulation",
    "dynamic_starter_exit": "dynamic_starter_exit;starter_innings;starter exit",
    "opener_bulk_pitcher": "opener;bulk_pitcher;bulk pitcher;piggyback",
    "individual_reliever_selection": "reliever;bullpen_chain;reliever_selection",
    "leverage_bullpen_usage": "leverage;high_leverage;score_state;bullpen_usage",
    "base_out_state": "base_state;outs;base_out;occupied bases",
    "base_advancement_transitions": "advance;runner;1st_to_3rd;2nd_scores;base advancement",
    "double_play_logic": "double_play;grounded_into_double_play;force_out",
    "sac_fly_logic": "sac_fly;sacrifice fly;tag_and_score",
    "extra_innings_ghost_runner": "extra innings;extras;ghost_runner;runner_on_second",
    "walkoff_shortening": "walkoff;bottom inning;home team leads",
    "steals_caught_stealing": "steal;stolen_base;caught_stealing",
    "balks": "balk",
    "wild_pitch_passed_ball": "wild_pitch;passed_ball",
    "lineup_order_state": "lineup_order;batter_index;next_batter",
}


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    fields: List[str] = []
    for row in rows or [{"empty": True}]:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows or [{"empty": True}])


def main() -> int:
    predecessor_rows = [
        {
            "check": "6nu_audit_script_exists",
            "expected": True,
            "actual": PREDECESSOR_SCRIPT.exists(),
            "passed": PREDECESSOR_SCRIPT.exists(),
        }
    ]

    source_rows = [
        {
            "source_path": path,
            "required_for_inventory": True,
            "inspection_only": True,
        }
        for path in SOURCE_TARGETS
    ]

    feature_rows = [
        {
            "feature_family": feature,
            "search_patterns": FEATURE_PATTERNS[feature],
            "required_classification_set": "|".join(row["classification"] for row in CLASSIFICATIONS),
            "must_record_source_files": True,
            "must_record_sim_reachability": True,
            "must_record_projection_reachability": True,
            "must_record_ui_visibility": True,
            "must_record_active_status": True,
        }
        for feature in FEATURES
    ]

    evidence_rows = [
        {"field": "feature_family", "required": True},
        {"field": "classification", "required": True},
        {"field": "exists_status", "required": True},
        {"field": "sim_reachability_status", "required": True},
        {"field": "projection_reachability_status", "required": True},
        {"field": "ui_visibility_status", "required": True},
        {"field": "active_status", "required": True},
        {"field": "diagnostic_only_status", "required": True},
        {"field": "source_files_with_evidence", "required": True},
        {"field": "evidence_summary", "required": True},
        {"field": "blocker", "required": True},
        {"field": "recommended_next_action", "required": True},
    ]

    safety_rows = [
        {"boundary": "planning_only", "passed": True},
        {"boundary": "source_inspection_not_executed", "passed": True},
        {"boundary": "no_feature_wiring", "passed": True},
        {"boundary": "no_tuning", "passed": True},
        {"boundary": "no_historical_validation", "passed": True},
        {"boundary": "no_prediction_join", "passed": True},
        {"boundary": "no_accuracy_metrics", "passed": True},
        {"boundary": "no_backtests", "passed": True},
        {"boundary": "no_pricing", "passed": True},
        {"boundary": "no_edge_detection", "passed": True},
        {"boundary": "no_live_fetches_or_remote_apis", "passed": True},
        {"boundary": "no_production_writes", "passed": True},
        {"boundary": "layer6_exit_not_recommended", "passed": True},
    ]

    decision_rows = [
        {"decision": "predecessor_present", "expected": True, "actual": PREDECESSOR_SCRIPT.exists(), "passed": PREDECESSOR_SCRIPT.exists()},
        {"decision": "all_features_planned", "expected": 15, "actual": len(FEATURES), "passed": len(FEATURES) == 15},
        {"decision": "source_targets_planned", "expected": ">=10", "actual": len(SOURCE_TARGETS), "passed": len(SOURCE_TARGETS) >= 10},
        {"decision": "classification_contract_complete", "expected": 6, "actual": len(CLASSIFICATIONS), "passed": len(CLASSIFICATIONS) == 6},
        {"decision": "evidence_fields_defined", "expected": ">=10", "actual": len(evidence_rows), "passed": len(evidence_rows) >= 10},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows)},
        {"check": "source_targets", "passed": len(source_rows) >= 10},
        {"check": "classification_contract", "passed": len(CLASSIFICATIONS) == 6},
        {"check": "feature_inventory_plan", "passed": len(feature_rows) == 15},
        {"check": "evidence_fields", "passed": len(evidence_rows) >= 10},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows)},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows)},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    recommended_rows = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": RECOMMENDED_PATH,
            "reason": "Audit this inventory plan before source inspection implementation.",
            "passed": True,
        }
    ]

    write_csv(CHECKS_CSV, checks)
    write_csv(PREDECESSOR_CSV, predecessor_rows)
    write_csv(SOURCE_TARGETS_CSV, source_rows)
    write_csv(CLASSIFICATION_CONTRACT_CSV, CLASSIFICATIONS)
    write_csv(FEATURE_INVENTORY_PLAN_CSV, feature_rows)
    write_csv(EVIDENCE_FIELDS_CSV, evidence_rows)
    write_csv(SAFETY_CSV, safety_rows)
    write_csv(DECISION_CSV, decision_rows)
    write_csv(RECOMMENDED_CSV, recommended_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_injection_inventory_plan_complete" if all_checks_passed else "layer_6_model_projection_realism_injection_inventory_plan_failed",
        "all_checks_passed": all_checks_passed,
        "feature_families_planned": len(FEATURES),
        "source_targets_planned": len(SOURCE_TARGETS),
        "classification_contract_count": len(CLASSIFICATIONS),
        "source_inspection_allowed_next": False,
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
        "recommended_path": RECOMMENDED_PATH,
        "generated_csv_artifacts": [
            str(CHECKS_CSV),
            str(PREDECESSOR_CSV),
            str(SOURCE_TARGETS_CSV),
            str(CLASSIFICATION_CONTRACT_CSV),
            str(FEATURE_INVENTORY_PLAN_CSV),
            str(EVIDENCE_FIELDS_CSV),
            str(SAFETY_CSV),
            str(DECISION_CSV),
            str(RECOMMENDED_CSV),
        ],
        "generated_json_artifacts": [str(JSON_PATH)],
    }

    JSON_PATH.write_text(json.dumps(diagnosis, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(diagnosis, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
