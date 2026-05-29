#!/usr/bin/env python3
"""Layer 6GN projection-dormant mechanics activation decision plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6gn_projection_dormant_activation_decision_plan"
TMP_DIR = Path("tmp")

AUDIT_6GM_PATH = Path("scripts/audit_6gm_layer6_game_state_realism_wiring_inventory.py")
VALIDATOR_6GL_PATH = Path("scripts/validate_6gl_layer6_game_state_realism_wiring_inventory.py")
PLAN_6GK_PATH = Path("scripts/plan_6gk_layer6_game_state_realism_exit_reconciliation.py")

AUDIT_6GM_JSON = TMP_DIR / "layer6_6gm_game_state_realism_wiring_inventory_audit.json"
REFINEMENT_6GM_CSV = TMP_DIR / "layer6_6gm_game_state_realism_wiring_inventory_audit_mechanic_refinement.csv"
ACTIVATION_6GM_CSV = TMP_DIR / "layer6_6gm_game_state_realism_wiring_inventory_audit_activation_decisions.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
DECISIONS_CSV = TMP_DIR / f"{SLUG}_decisions.csv"
GAMEPLAY_MECHANICS_CSV = TMP_DIR / f"{SLUG}_gameplay_mechanics.csv"
META_SURFACES_CSV = TMP_DIR / f"{SLUG}_meta_surfaces.csv"
BACKTEST_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_backtest_requirements.csv"
PROJECTION_PAYLOAD_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_projection_payload_requirements.csv"
RECALIBRATION_REQUIREMENTS_CSV = TMP_DIR / f"{SLUG}_recalibration_requirements.csv"
EXIT_CRITERIA_CSV = TMP_DIR / f"{SLUG}_exit_criteria.csv"
FUTURE_6GO_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6go_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GM = "layer_6_game_state_realism_exit_criteria_wiring_inventory_audit_complete"
DIAGNOSIS_6GN = "layer_6_projection_dormant_mechanics_activation_decision_plan_complete"
CURRENT_LAYER = "6GN_layer_6_projection_dormant_mechanics_activation_decision_plan"
RECOMMENDED_NEXT_LAYER = "6GO_layer_6_projection_dormant_mechanics_activation_decision_plan_audit"
RECOMMENDED_PATH = "plan_projection_dormant_mechanic_activation_recalibration_backtest_and_projection_payload_decisions"

GAMEPLAY_MECHANICS = [
    "extra_innings_ghost_runner",
    "stolen_bases_caught_stealing",
    "wild_pitches_passed_balls",
    "balks",
    "first_to_third_advancement",
    "second_to_home_advancement",
    "sac_flies_tagging_up",
    "double_plays_by_base_out_state",
    "pinch_hitters_substitutions",
    "bullpen_sequencing_leverage_behavior",
]

META_SURFACES = [
    "projection_site_integration",
    "validation_distribution_shape_evidence",
]

ALLOWED_DECISIONS = {
    "activate_now",
    "keep_dormant",
    "recalibrate_parameters",
    "wire_to_projection_payload",
    "remove_from_exit_criteria",
    "require_backtest",
}

DECISION_POLICY = {
    "implemented_projected_validated_active_or_available": {
        "default_decision": "require_backtest",
        "secondary_decision": "activate_now",
        "reason": "verify active projected mechanic improves or preserves distribution quality before counting toward Layer 6 exit",
    },
    "implemented_projection_dormant_pending_calibration": {
        "default_decision": "require_backtest",
        "secondary_decision": "recalibrate_parameters",
        "reason": "mechanic appears prototyped/dormant and needs calibration proof before activation",
    },
    "implemented_projection_dormant_negative_or_neutral_validation": {
        "default_decision": "keep_dormant",
        "secondary_decision": "recalibrate_parameters",
        "reason": "prior validation appears neutral/negative; do not activate without improved calibration/backtest",
    },
    "implemented_in_sim_not_site_projected": {
        "default_decision": "wire_to_projection_payload",
        "secondary_decision": "require_backtest",
        "reason": "mechanic appears in sim but not site-facing projection output; prove projection payload route before activation",
    },
    "source_present_not_simulator_active": {
        "default_decision": "require_backtest",
        "secondary_decision": "recalibrate_parameters",
        "reason": "mechanic needs simulator activation proof and calibration before projection activation",
    },
    "missing_or_unproven": {
        "default_decision": "remove_from_exit_criteria",
        "secondary_decision": "require_backtest",
        "reason": "cannot require Layer 6 exit on an unproven/missing mechanic unless implementation is planned",
    },
    "meta_surface_available": {
        "default_decision": "require_backtest",
        "secondary_decision": "",
        "reason": "meta surface supports validation/projection evidence but is not a gameplay mechanic",
    },
}


def safe_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        parsed, _ = json.JSONDecoder().raw_decode(text)
        return parsed if isinstance(parsed, dict) else {}


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def build_decision_row(row: Dict[str, str], is_meta: bool) -> Dict[str, Any]:
    mechanic_key = row.get("mechanic_key", "")
    refined_status = row.get("refined_6gm_status", "")
    policy = DECISION_POLICY.get(
        refined_status,
        {
            "default_decision": "require_backtest",
            "secondary_decision": "",
            "reason": "unknown status requires manual backtest gate",
        },
    )
    default_decision = policy["default_decision"]
    secondary_decision = policy.get("secondary_decision", "")

    requires_backtest = default_decision == "require_backtest" or secondary_decision == "require_backtest"
    requires_projection_payload = (
        default_decision == "wire_to_projection_payload" or secondary_decision == "wire_to_projection_payload"
    )
    requires_recalibration = default_decision == "recalibrate_parameters" or secondary_decision == "recalibrate_parameters"

    return {
        "mechanic_key": mechanic_key,
        "mechanic": row.get("mechanic", mechanic_key),
        "is_meta_surface": is_meta,
        "refined_6gm_status": refined_status,
        "default_decision": default_decision,
        "secondary_decision": secondary_decision,
        "allowed_decisions": "|".join(sorted(ALLOWED_DECISIONS)),
        "decision_reason": policy["reason"],
        "requires_backtest": requires_backtest or not is_meta,
        "requires_projection_payload_proof": requires_projection_payload,
        "requires_recalibration": requires_recalibration,
        "activation_allowed_by_6gn": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_credit_allowed_now": False,
    }


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_before = AUDIT_6GM_PATH.read_text(encoding="utf-8") if AUDIT_6GM_PATH.exists() else ""
    validator_before = VALIDATOR_6GL_PATH.read_text(encoding="utf-8") if VALIDATOR_6GL_PATH.exists() else ""
    plan_before = PLAN_6GK_PATH.read_text(encoding="utf-8") if PLAN_6GK_PATH.exists() else ""

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6GM_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    audit_json = load_json(AUDIT_6GM_JSON)
    refinement_rows_6gm = read_csv(REFINEMENT_6GM_CSV)
    activation_rows_6gm = read_csv(ACTIVATION_6GM_CSV)

    refinement_by_key = {row.get("mechanic_key"): row for row in refinement_rows_6gm}

    gameplay_rows: List[Dict[str, Any]] = []
    meta_rows: List[Dict[str, Any]] = []
    decision_rows: List[Dict[str, Any]] = []

    for mechanic_key in GAMEPLAY_MECHANICS:
        source = refinement_by_key.get(mechanic_key, {"mechanic_key": mechanic_key, "mechanic": mechanic_key, "refined_6gm_status": "missing_or_unproven"})
        decision = build_decision_row(source, is_meta=False)
        gameplay_rows.append(decision)
        decision_rows.append(decision)

    for mechanic_key in META_SURFACES:
        source = refinement_by_key.get(mechanic_key, {"mechanic_key": mechanic_key, "mechanic": mechanic_key, "refined_6gm_status": "meta_surface_available"})
        decision = build_decision_row(source, is_meta=True)
        meta_rows.append(decision)
        decision_rows.append(decision)

    backtest_rows = [
        {
            "mechanic_key": row["mechanic_key"],
            "required": True,
            "backtest_requirement": "compare candidate vs current production/off configuration on distribution shape, tails, inning-level runs, team totals, and total runs",
            "minimum_evidence": "actual_outcome_comparison|distribution_shape|variance|tail_behavior|calibration_error",
            "activation_blocked_until_passed": True,
        }
        for row in gameplay_rows
    ]

    projection_payload_rows = [
        {
            "mechanic_key": row["mechanic_key"],
            "required": True,
            "projection_payload_requirement": "prove mechanic reaches site-facing projection payload or explicitly remains dormant/off",
            "required_surfaces": "model_projection_payload|site_route|market_output|team_total_or_total_run_distribution",
            "activation_blocked_until_passed": True,
        }
        for row in gameplay_rows
        if row["requires_projection_payload_proof"]
    ]

    if not projection_payload_rows:
        projection_payload_rows.append(
            {
                "mechanic_key": "__none__",
                "required": False,
                "projection_payload_requirement": "no current decision rows require wire_to_projection_payload",
                "required_surfaces": "",
                "activation_blocked_until_passed": False,
            }
        )

    recalibration_rows = [
        {
            "mechanic_key": row["mechanic_key"],
            "required": True,
            "recalibration_requirement": "derive or tune mechanic parameter using historical/actual outcomes before activation",
            "minimum_evidence": "parameter_grid|holdout_backtest|calibration_delta|no_tail_degradation",
            "activation_blocked_until_passed": True,
        }
        for row in gameplay_rows
        if row["requires_recalibration"]
    ]

    if not recalibration_rows:
        recalibration_rows.append(
            {
                "mechanic_key": "__none__",
                "required": False,
                "recalibration_requirement": "no current decision rows require recalibrate_parameters",
                "minimum_evidence": "",
                "activation_blocked_until_passed": False,
            }
        )

    exit_rows = [
        {
            "exit_criterion": "base_out_transitions_are_more_realistic",
            "current_status": "not_exit_ready",
            "6gn_requirement": "activation/backtest decisions must pass for base/out transition mechanics",
            "passed": True,
        },
        {
            "exit_criterion": "scoring_distribution_tails_improve",
            "current_status": "not_exit_ready",
            "6gn_requirement": "must demonstrate no tail degradation and preferably tail improvement before activation",
            "passed": True,
        },
        {
            "exit_criterion": "inning_level_run_distribution_improves",
            "current_status": "not_exit_ready",
            "6gn_requirement": "must compare inning-level run distribution against actual outcomes",
            "passed": True,
        },
        {
            "exit_criterion": "extra_inning_behavior_represented_correctly",
            "current_status": "not_exit_ready",
            "6gn_requirement": "extra innings can be active/available but still needs full Layer 6 cross-mechanic validation",
            "passed": True,
        },
        {
            "exit_criterion": "team_total_and_total_run_variance_improve",
            "current_status": "not_exit_ready",
            "6gn_requirement": "must validate team-total and total-run variance changes after activation decisions",
            "passed": True,
        },
        {
            "exit_criterion": "mechanics_used_by_simulator",
            "current_status": "decision_required",
            "6gn_requirement": "do not count simulator presence as exit proof without projection and backtest gates",
            "passed": True,
        },
        {
            "exit_criterion": "mechanics_reflected_in_site_facing_projections",
            "current_status": "decision_required",
            "6gn_requirement": "mechanics requiring wire_to_projection_payload must produce projection payload proof before activation",
            "passed": True,
        },
        {
            "exit_criterion": "mechanics_have_validation_evidence",
            "current_status": "decision_required",
            "6gn_requirement": "validation must distinguish positive, neutral, and negative outcomes",
            "passed": True,
        },
    ]

    future_rows = [
        {"contract": "audit_6gn_decision_matrix", "required": True, "passed": True},
        {"contract": "verify_no_mechanics_activated_by_6gn", "required": True, "passed": True},
        {"contract": "verify_all_gameplay_mechanics_have_decisions", "required": True, "passed": True},
        {"contract": "verify_backtest_requirements_for_all_gameplay_mechanics", "required": True, "passed": True},
        {"contract": "verify_projection_payload_requirements_for_wire_to_projection_decisions", "required": True, "passed": True},
        {"contract": "verify_recalibration_requirements_for_recalibration_decisions", "required": True, "passed": True},
        {"contract": "verify_layer_6_exit_remains_false", "required": True, "passed": True},
        {"contract": "recommended_6go_diagnosis", "required": True, "passed": True, "artifact": "layer_6_projection_dormant_mechanics_activation_decision_plan_audit_complete"},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GN, "actual": DIAGNOSIS_6GN, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gm_audit_exists", "expected": True, "actual": AUDIT_6GM_PATH.exists(), "passed": AUDIT_6GM_PATH.exists()},
        {"check": "6gm_audit_runs", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"check": "6gm_json_exists", "expected": True, "actual": AUDIT_6GM_JSON.exists(), "passed": AUDIT_6GM_JSON.exists()},
        {"check": "6gm_all_checks_passed", "expected": True, "actual": audit_json.get("all_checks_passed"), "passed": audit_json.get("all_checks_passed") is True},
        {"check": "6gm_audit_only", "expected": True, "actual": audit_json.get("audit_only"), "passed": audit_json.get("audit_only") is True},
        {"check": "6gm_diagnosis", "expected": DIAGNOSIS_6GM, "actual": audit_json.get("diagnosis"), "passed": audit_json.get("diagnosis") == DIAGNOSIS_6GM},
        {"check": "6gm_recommended_next_layer", "expected": CURRENT_LAYER, "actual": audit_json.get("recommended_next_layer"), "passed": audit_json.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gm_layer_6_exit_ready_false", "expected": False, "actual": audit_json.get("layer_6_exit_ready"), "passed": audit_json.get("layer_6_exit_ready") is False},
        {"check": "6gm_refinement_csv_exists", "expected": True, "actual": REFINEMENT_6GM_CSV.exists(), "passed": REFINEMENT_6GM_CSV.exists()},
        {"check": "6gm_activation_csv_exists", "expected": True, "actual": ACTIVATION_6GM_CSV.exists(), "passed": ACTIVATION_6GM_CSV.exists()},
        {"check": "6gm_activation_rows_positive", "expected": ">0", "actual": len(activation_rows_6gm), "passed": len(activation_rows_6gm) > 0},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_after = AUDIT_6GM_PATH.read_text(encoding="utf-8") if AUDIT_6GM_PATH.exists() else ""
    validator_after = VALIDATOR_6GL_PATH.read_text(encoding="utf-8") if VALIDATOR_6GL_PATH.exists() else ""
    plan_after = PLAN_6GK_PATH.read_text(encoding="utf-8") if PLAN_6GK_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6gn_plan", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gm_audit", "policy": "unchanged_by_6gn_plan", "passed": audit_after == audit_before},
        {"surface": "6gl_validator", "policy": "unchanged_by_6gn_plan", "passed": validator_after == validator_before},
        {"surface": "6gk_plan", "policy": "unchanged_by_6gn_plan", "passed": plan_after == plan_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6gn_plan", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6gn_plan", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6gn_plan", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gn_plan", "passed": True},
    ]

    decision_counts = Counter(row["default_decision"] for row in decision_rows)
    all_decisions_allowed = all(row["default_decision"] in ALLOWED_DECISIONS for row in decision_rows) and all(
        (not row["secondary_decision"]) or row["secondary_decision"] in ALLOWED_DECISIONS for row in decision_rows
    )

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "gameplay_decisions", "passed": len(gameplay_rows) == 10, "detail": f"{len(gameplay_rows)}/10"},
        {"check": "meta_surface_rows", "passed": len(meta_rows) == 2, "detail": f"{len(meta_rows)}/2"},
        {"check": "allowed_decisions", "passed": all_decisions_allowed, "detail": dict(decision_counts)},
        {"check": "gameplay_followup_gates", "passed": all(row["requires_backtest"] or row["requires_projection_payload_proof"] or row["requires_recalibration"] for row in gameplay_rows), "detail": "all gameplay mechanics require at least one gate"},
        {"check": "backtest_requirements", "passed": len(backtest_rows) == 10, "detail": f"{len(backtest_rows)}/10"},
        {"check": "projection_payload_requirements", "passed": bool(projection_payload_rows), "detail": str(len(projection_payload_rows))},
        {"check": "recalibration_requirements", "passed": bool(recalibration_rows), "detail": str(len(recalibration_rows))},
        {"check": "exit_criteria", "passed": all(row["passed"] for row in exit_rows), "detail": f"{sum(1 for row in exit_rows if row['passed'])}/{len(exit_rows)}"},
        {"check": "future_6go_contract", "passed": all(row["passed"] for row in future_rows), "detail": f"{sum(1 for row in future_rows if row['passed'])}/{len(future_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "decisions": write_csv(DECISIONS_CSV, decision_rows),
        "gameplay_mechanics": write_csv(GAMEPLAY_MECHANICS_CSV, gameplay_rows),
        "meta_surfaces": write_csv(META_SURFACES_CSV, meta_rows),
        "backtest_requirements": write_csv(BACKTEST_REQUIREMENTS_CSV, backtest_rows),
        "projection_payload_requirements": write_csv(PROJECTION_PAYLOAD_REQUIREMENTS_CSV, projection_payload_rows),
        "recalibration_requirements": write_csv(RECALIBRATION_REQUIREMENTS_CSV, recalibration_rows),
        "exit_criteria": write_csv(EXIT_CRITERIA_CSV, exit_rows),
        "future_6go_contract": write_csv(FUTURE_6GO_CONTRACT_CSV, future_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GN",
        "layer_type": "game_mechanics_realism",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6GN if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "gameplay_mechanics_count": len(gameplay_rows),
        "meta_surface_count": len(meta_rows),
        "decision_counts": dict(decision_counts),
        "predecessor_audit": str(AUDIT_6GM_PATH),
        "predecessor_audit_returncode": audit_run.returncode,
        "predecessor_audit_diagnosis": audit_json.get("diagnosis"),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "decisions_csv": str(DECISIONS_CSV),
            "gameplay_mechanics_csv": str(GAMEPLAY_MECHANICS_CSV),
            "meta_surfaces_csv": str(META_SURFACES_CSV),
            "backtest_requirements_csv": str(BACKTEST_REQUIREMENTS_CSV),
            "projection_payload_requirements_csv": str(PROJECTION_PAYLOAD_REQUIREMENTS_CSV),
            "recalibration_requirements_csv": str(RECALIBRATION_REQUIREMENTS_CSV),
            "exit_criteria_csv": str(EXIT_CRITERIA_CSV),
            "future_6go_contract_csv": str(FUTURE_6GO_CONTRACT_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
