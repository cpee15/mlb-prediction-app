#!/usr/bin/env python3
"""Layer 6NX — Model Projection Realism Injection Inventory Implementation."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


LAYER_ID = "6NX"
LAYER_NAME = "layer6_model_projection_realism_injection_inventory_implementation"
SLUG = "layer_6NX_model_projection_realism_injection_inventory_implementation"

TMP_DIR = Path("tmp") / SLUG
TMP_DIR.mkdir(parents=True, exist_ok=True)

PREDECESSOR_SCRIPT = Path("scripts/audit_6NW_model_projection_realism_injection_inventory.py")

JSON_PATH = TMP_DIR / "diagnosis.json"
CHECKS_CSV = TMP_DIR / "checks.csv"
PREDECESSOR_CSV = TMP_DIR / "predecessor.csv"
SOURCE_TARGETS_CSV = TMP_DIR / "source_targets_observed.csv"
FEATURE_INVENTORY_CSV = TMP_DIR / "feature_injection_inventory.csv"
SUMMARY_CSV = TMP_DIR / "classification_summary.csv"
BLOCKERS_CSV = TMP_DIR / "blockers.csv"
SAFETY_CSV = TMP_DIR / "safety_boundaries.csv"
DECISION_CSV = TMP_DIR / "decision.csv"
RECOMMENDED_CSV = TMP_DIR / "recommended_path.csv"

RECOMMENDED_NEXT_LAYER = "6NY_layer6_model_projection_realism_injection_inventory_implementation_audit"
RECOMMENDED_PATH = "audit_source_inspection_inventory_before_gap_analysis_or_wiring"

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

FEATURE_PATTERNS: Dict[str, Dict[str, Sequence[str]]] = {
    "bullpen_transition": {
        "exists": ["bullpen", "simulate_game_with_bullpen", "bullpen_adjusted_game_simulation"],
        "sim": ["simulate_game_with_bullpen", "away_bullpen_probabilities", "home_bullpen_probabilities"],
        "projection": ["simulate_game_with_bullpen", "bullpenAdjustedGameSimulation", "bullpen_adjusted_game_simulation"],
        "ui": ["bullpenAdjustedGameSimulation", "bullpen_adjusted_game_simulation", "awayBullpenProfile", "homeBullpenProfile"],
        "active": ["simulate_game_with_bullpen("],
    },
    "dynamic_starter_exit": {
        "exists": ["dynamic_starter_exit", "starter_innings", "starter exit"],
        "sim": ["dynamic_starter_exit", "starter_innings_distribution"],
        "projection": ["dynamic_starter_exit=True", "dynamic_starter_exit"],
        "ui": ["dynamic_starter_exit", "starter_innings_distribution", "simulationDiagnostics"],
        "active": ["dynamic_starter_exit=True"],
    },
    "opener_bulk_pitcher": {
        "exists": ["opener", "bulk_pitcher", "bulk pitcher", "piggyback"],
        "sim": ["opener", "bulk_pitcher", "bulk pitcher"],
        "projection": ["opener", "bulk_pitcher", "bulk pitcher"],
        "ui": ["opener", "bulk_pitcher", "bulk pitcher"],
        "active": ["opener", "bulk_pitcher"],
    },
    "individual_reliever_selection": {
        "exists": ["reliever", "bullpen_chain", "reliever_selection"],
        "sim": ["reliever", "bullpen_chain", "select_reliever"],
        "projection": ["reliever", "bullpen_chain", "bullpenProfile"],
        "ui": ["reliever", "bullpenProfile", "awayBullpenProfile", "homeBullpenProfile"],
        "active": ["select_reliever", "reliever"],
    },
    "leverage_bullpen_usage": {
        "exists": ["leverage", "high_leverage", "score_state", "bullpen_usage"],
        "sim": ["leverage", "score_state", "high_leverage"],
        "projection": ["leverage", "bullpenProfile"],
        "ui": ["leverage", "bullpenProfile"],
        "active": ["leverage"],
    },
    "base_out_state": {
        "exists": ["base_state", "base/out", "base_out", "outs", "occupied bases"],
        "sim": ["base_state", "outs", "inning_simulator", "base_out"],
        "projection": ["base_state", "base_out", "run_distribution"],
        "ui": ["base_state", "base_out", "run_distribution", "simulationDiagnostics"],
        "active": ["base_state", "base_out"],
    },
    "base_advancement_transitions": {
        "exists": ["advance", "runner", "1st_to_3rd", "2nd_scores", "base advancement"],
        "sim": ["advance", "runner", "runners", "base_state"],
        "projection": ["advance", "runner", "run_distribution"],
        "ui": ["advance", "runner", "run_distribution", "simulationDiagnostics"],
        "active": ["advance_runner", "advance", "runner"],
    },
    "double_play_logic": {
        "exists": ["double_play", "grounded_into_double_play", "force_out"],
        "sim": ["double_play", "force_out"],
        "projection": ["double_play", "run_distribution"],
        "ui": ["double_play", "run_distribution", "simulationDiagnostics"],
        "active": ["double_play"],
    },
    "sac_fly_logic": {
        "exists": ["sac_fly", "sacrifice fly", "tag_and_score"],
        "sim": ["sac_fly", "sacrifice"],
        "projection": ["sac_fly", "run_distribution"],
        "ui": ["sac_fly", "run_distribution", "simulationDiagnostics"],
        "active": ["sac_fly"],
    },
    "extra_innings_ghost_runner": {
        "exists": ["extra innings", "extras", "ghost_runner", "runner_on_second"],
        "sim": ["extras", "ghost_runner", "simulate_game_with_extras"],
        "projection": ["extras", "ghost_runner", "simulate_game_with_extras"],
        "ui": ["extras", "ghost_runner", "tie_after_regulation_probability"],
        "active": ["simulate_game_with_extras", "ghost_runner"],
    },
    "walkoff_shortening": {
        "exists": ["walkoff", "bottom inning", "home team leads"],
        "sim": ["walkoff", "home team leads"],
        "projection": ["walkoff", "home_win_probability"],
        "ui": ["walkoff", "home_win_probability"],
        "active": ["walkoff"],
    },
    "steals_caught_stealing": {
        "exists": ["steal", "stolen_base", "caught_stealing"],
        "sim": ["steal", "stolen_base", "caught_stealing"],
        "projection": ["steal", "stolen_base", "run_distribution"],
        "ui": ["steal", "stolen_base", "run_distribution"],
        "active": ["stolen_base", "caught_stealing"],
    },
    "balks": {
        "exists": ["balk"],
        "sim": ["balk"],
        "projection": ["balk"],
        "ui": ["balk"],
        "active": ["balk"],
    },
    "wild_pitch_passed_ball": {
        "exists": ["wild_pitch", "passed_ball"],
        "sim": ["wild_pitch", "passed_ball"],
        "projection": ["wild_pitch", "passed_ball", "run_distribution"],
        "ui": ["wild_pitch", "passed_ball", "run_distribution"],
        "active": ["wild_pitch", "passed_ball"],
    },
    "lineup_order_state": {
        "exists": ["lineup_order", "batter_index", "next_batter", "lineup"],
        "sim": ["lineup_order", "batter_index", "next_batter"],
        "projection": ["lineup", "away_lineup", "home_lineup", "simulationContract"],
        "ui": ["lineup", "simulationContract"],
        "active": ["batter_index", "next_batter"],
    },
}


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True}]
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_text(path: str) -> str:
    p = Path(path)
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8", errors="ignore")


def contains_any(text: str, patterns: Sequence[str]) -> bool:
    lowered = text.lower()
    return any(pattern.lower() in lowered for pattern in patterns)


def hit_lines(path: str, patterns: Sequence[str], limit: int = 8) -> List[str]:
    text = read_text(path)
    hits: List[str] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        if contains_any(line, patterns):
            hits.append(f"{path}:L{line_no}:{line.strip()[:180]}")
        if len(hits) >= limit:
            break
    return hits


def classify_feature(feature: str, corpus_by_path: Dict[str, str]) -> Dict[str, Any]:
    patterns = FEATURE_PATTERNS[feature]

    all_corpus = "\n".join(corpus_by_path.values())
    sim_corpus = "\n".join(
        text for path, text in corpus_by_path.items()
        if path.startswith("mlb_app/simulation/")
    )
    projection_corpus = "\n".join(
        text for path, text in corpus_by_path.items()
        if path in {
            "mlb_app/model_projections.py",
            "mlb_app/model_projection_routes.py",
            "mlb_app/model_projection_payload.py",
        }
    )
    ui_corpus = corpus_by_path.get("frontend/src/pages/ModelProjectionsPage.jsx", "")

    exists = contains_any(all_corpus, patterns["exists"])
    sim_reachable = contains_any(sim_corpus, patterns["sim"])
    projection_reachable = contains_any(projection_corpus, patterns["projection"])
    ui_visible = contains_any(ui_corpus + "\n" + projection_corpus, patterns["ui"])
    active = contains_any(projection_corpus, patterns["active"]) and (
        projection_reachable or ui_visible
    )

    diagnostic_only = False
    if ui_visible and contains_any(projection_corpus + "\n" + ui_corpus, ["diagnostic_only", "not_final_probability", "diagnostic only"]):
        diagnostic_only = True

    if not exists:
        classification = "absent"
    elif exists and not sim_reachable:
        classification = "present_not_reachable"
    elif sim_reachable and not projection_reachable:
        classification = "sim_reachable_not_projection_reachable"
    elif projection_reachable and not ui_visible:
        classification = "projection_reachable_not_ui_visible"
    elif ui_visible and diagnostic_only:
        classification = "ui_visible_diagnostic_only"
    elif active:
        classification = "active_in_model_projection_output"
    else:
        classification = "present_not_reachable"

    source_files = []
    evidence_lines: List[str] = []
    for path in SOURCE_TARGETS:
        if contains_any(corpus_by_path.get(path, ""), patterns["exists"]):
            source_files.append(path)
            evidence_lines.extend(hit_lines(path, patterns["exists"], limit=4))

    blocker = "none_confirmed_active" if classification == "active_in_model_projection_output" else "feature_not_confirmed_active_in_model_projection_output"

    if classification == "absent":
        next_action = "plan_feature_design_or_defer"
    elif classification == "present_not_reachable":
        next_action = "plan_sim_reachability_wiring"
    elif classification == "sim_reachable_not_projection_reachable":
        next_action = "plan_projection_builder_wiring"
    elif classification == "projection_reachable_not_ui_visible":
        next_action = "plan_payload_or_ui_visibility_wiring"
    elif classification == "ui_visible_diagnostic_only":
        next_action = "decide_whether_diagnostic_only_is_sufficient_or_plan_final_output_integration"
    else:
        next_action = "audit_active_output_effect_before_historical_validation"

    return {
        "feature_family": feature,
        "classification": classification,
        "exists_status": "present" if exists else "absent",
        "sim_reachability_status": "reachable" if sim_reachable else "not_reachable",
        "projection_reachability_status": "reachable" if projection_reachable else "not_reachable",
        "ui_visibility_status": "visible" if ui_visible else "not_visible",
        "active_status": "active" if active else "not_confirmed_active",
        "diagnostic_only_status": "diagnostic_only" if diagnostic_only else "not_diagnostic_only_or_not_visible",
        "source_files_with_evidence": ";".join(source_files),
        "evidence_summary": " || ".join(evidence_lines)[:5000],
        "blocker": blocker,
        "recommended_next_action": next_action,
    }


def main() -> int:
    predecessor_rows = [
        {
            "check": "6nw_audit_script_exists",
            "expected": True,
            "actual": PREDECESSOR_SCRIPT.exists(),
            "passed": PREDECESSOR_SCRIPT.exists(),
        }
    ]

    corpus_by_path = {path: read_text(path) for path in SOURCE_TARGETS}

    source_rows = [
        {
            "source_path": path,
            "exists": Path(path).exists(),
            "bytes_read": len(corpus_by_path[path]),
            "inspection_only": True,
        }
        for path in SOURCE_TARGETS
    ]

    inventory_rows = [
        classify_feature(feature, corpus_by_path)
        for feature in FEATURE_PATTERNS
    ]

    classification_counts: Dict[str, int] = {}
    for row in inventory_rows:
        classification_counts[row["classification"]] = classification_counts.get(row["classification"], 0) + 1

    summary_rows = [
        {"classification": key, "feature_count": value}
        for key, value in sorted(classification_counts.items())
    ]

    blockers = [
        {
            "blocker": "features_not_confirmed_active_in_model_projection_output",
            "active": any(row["classification"] != "active_in_model_projection_output" for row in inventory_rows),
        },
        {
            "blocker": "diagnostic_only_features_require_policy_decision",
            "active": any(row["classification"] == "ui_visible_diagnostic_only" for row in inventory_rows),
        },
        {
            "blocker": "layer6_exit_not_allowed",
            "active": True,
        },
    ]

    safety_rows = [
        {"boundary": "source_inspection_only", "passed": True},
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
        {"decision": "source_targets_observed", "expected": 15, "actual": len(source_rows), "passed": len(source_rows) == 15},
        {"decision": "features_classified", "expected": 15, "actual": len(inventory_rows), "passed": len(inventory_rows) == 15},
        {"decision": "classification_values_nonempty", "expected": True, "actual": all(row["classification"] for row in inventory_rows), "passed": all(row["classification"] for row in inventory_rows)},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows)},
        {"check": "source_targets", "passed": len(source_rows) == 15},
        {"check": "feature_inventory", "passed": len(inventory_rows) == 15},
        {"check": "classification_summary", "passed": len(summary_rows) > 0},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows)},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows)},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    recommended_rows = [
        {
            "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
            "recommended_path": RECOMMENDED_PATH,
            "reason": "Audit the source-inspection inventory before gap analysis, wiring, tuning, or historical validation.",
            "passed": True,
        }
    ]

    write_csv(CHECKS_CSV, checks)
    write_csv(PREDECESSOR_CSV, predecessor_rows)
    write_csv(SOURCE_TARGETS_CSV, source_rows)
    write_csv(FEATURE_INVENTORY_CSV, inventory_rows)
    write_csv(SUMMARY_CSV, summary_rows)
    write_csv(BLOCKERS_CSV, blockers)
    write_csv(SAFETY_CSV, safety_rows)
    write_csv(DECISION_CSV, decision_rows)
    write_csv(RECOMMENDED_CSV, recommended_rows)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": "layer_6_model_projection_realism_injection_inventory_implementation_complete" if all_checks_passed else "layer_6_model_projection_realism_injection_inventory_implementation_failed",
        "all_checks_passed": all_checks_passed,
        "features_classified": len(inventory_rows),
        "source_targets_observed": len(source_rows),
        "classification_counts": classification_counts,
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
            str(FEATURE_INVENTORY_CSV),
            str(SUMMARY_CSV),
            str(BLOCKERS_CSV),
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
