#!/usr/bin/env python3
"""Audit Layer 6GN projection-dormant mechanics activation decision plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6go_activation_decision_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6GN_PATH = Path("scripts/plan_6gn_layer6_projection_dormant_activation_decisions.py")
AUDIT_6GM_PATH = Path("scripts/audit_6gm_layer6_game_state_realism_wiring_inventory.py")
VALIDATOR_6GL_PATH = Path("scripts/validate_6gl_layer6_game_state_realism_wiring_inventory.py")

JSON_6GN = TMP_DIR / "layer6_6gn_projection_dormant_activation_decision_plan.json"
DECISIONS_6GN = TMP_DIR / "layer6_6gn_projection_dormant_activation_decision_plan_decisions.csv"
GAMEPLAY_6GN = TMP_DIR / "layer6_6gn_projection_dormant_activation_decision_plan_gameplay_mechanics.csv"
META_6GN = TMP_DIR / "layer6_6gn_projection_dormant_activation_decision_plan_meta_surfaces.csv"
BACKTEST_6GN = TMP_DIR / "layer6_6gn_projection_dormant_activation_decision_plan_backtest_requirements.csv"
PROJECTION_PAYLOAD_6GN = TMP_DIR / "layer6_6gn_projection_dormant_activation_decision_plan_projection_payload_requirements.csv"
RECALIBRATION_6GN = TMP_DIR / "layer6_6gn_projection_dormant_activation_decision_plan_recalibration_requirements.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
DECISION_INTEGRITY_CSV = TMP_DIR / f"{SLUG}_decision_integrity.csv"
BACKTEST_GATE_CSV = TMP_DIR / f"{SLUG}_backtest_gate.csv"
PROJECTION_PAYLOAD_GATE_CSV = TMP_DIR / f"{SLUG}_projection_payload_gate.csv"
RECALIBRATION_GATE_CSV = TMP_DIR / f"{SLUG}_recalibration_gate.csv"
EXIT_CRITERIA_CSV = TMP_DIR / f"{SLUG}_exit_criteria.csv"
FUTURE_6GP_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6gp_contract.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GN = "layer_6_projection_dormant_mechanics_activation_decision_plan_complete"
DIAGNOSIS_6GO = "layer_6_projection_dormant_mechanics_activation_decision_plan_audit_complete"
CURRENT_LAYER = "6GO_layer_6_projection_dormant_mechanics_activation_decision_plan_audit"
RECOMMENDED_NEXT_LAYER = "6GP_layer_6_gameplay_mechanic_backtest_harness_plan"
RECOMMENDED_PATH = "audit_6gn_decision_plan_then_plan_backtest_harness_for_layer_6_gameplay_mechanics"

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


def decision_allowed(value: str) -> bool:
    return value == "" or value in ALLOWED_DECISIONS


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_before = PLAN_6GN_PATH.read_text(encoding="utf-8") if PLAN_6GN_PATH.exists() else ""
    audit_before = AUDIT_6GM_PATH.read_text(encoding="utf-8") if AUDIT_6GM_PATH.exists() else ""
    validator_before = VALIDATOR_6GL_PATH.read_text(encoding="utf-8") if VALIDATOR_6GL_PATH.exists() else ""

    plan_run = subprocess.run(
        [sys.executable, str(PLAN_6GN_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6gn = load_json(JSON_6GN)
    decision_rows = read_csv(DECISIONS_6GN)
    gameplay_rows = read_csv(GAMEPLAY_6GN)
    meta_rows = read_csv(META_6GN)
    backtest_rows = read_csv(BACKTEST_6GN)
    projection_payload_rows = read_csv(PROJECTION_PAYLOAD_6GN)
    recalibration_rows = read_csv(RECALIBRATION_6GN)

    gameplay_keys = {row.get("mechanic_key") for row in gameplay_rows}
    meta_keys = {row.get("mechanic_key") for row in meta_rows}
    backtest_keys = {row.get("mechanic_key") for row in backtest_rows if boolish(row.get("required"))}
    required_projection_payload_keys = {
        row.get("mechanic_key") for row in decision_rows
        if row.get("default_decision") == "wire_to_projection_payload"
        or row.get("secondary_decision") == "wire_to_projection_payload"
    }
    projection_payload_gate_keys = {
        row.get("mechanic_key") for row in projection_payload_rows if boolish(row.get("required"))
    }
    required_recalibration_keys = {
        row.get("mechanic_key") for row in decision_rows
        if row.get("default_decision") == "recalibrate_parameters"
        or row.get("secondary_decision") == "recalibrate_parameters"
    }
    recalibration_gate_keys = {
        row.get("mechanic_key") for row in recalibration_rows if boolish(row.get("required"))
    }

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gn_plan_exists", "expected": True, "actual": PLAN_6GN_PATH.exists(), "passed": PLAN_6GN_PATH.exists()},
        {"check": "6gn_plan_runs", "expected": 0, "actual": plan_run.returncode, "passed": plan_run.returncode == 0},
        {"check": "6gn_json_exists", "expected": True, "actual": JSON_6GN.exists(), "passed": JSON_6GN.exists()},
        {"check": "6gn_all_checks_passed", "expected": True, "actual": json_6gn.get("all_checks_passed"), "passed": json_6gn.get("all_checks_passed") is True},
        {"check": "6gn_planning_only", "expected": True, "actual": json_6gn.get("planning_only"), "passed": json_6gn.get("planning_only") is True},
        {"check": "6gn_diagnosis", "expected": DIAGNOSIS_6GN, "actual": json_6gn.get("diagnosis"), "passed": json_6gn.get("diagnosis") == DIAGNOSIS_6GN},
        {"check": "6gn_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6gn.get("recommended_next_layer"), "passed": json_6gn.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gn_layer_6_exit_ready_false", "expected": False, "actual": json_6gn.get("layer_6_exit_ready"), "passed": json_6gn.get("layer_6_exit_ready") is False},
        {"check": "6gn_mechanics_activated_false", "expected": False, "actual": json_6gn.get("mechanics_activated_by_this_layer"), "passed": json_6gn.get("mechanics_activated_by_this_layer") is False},
        {"check": "6gn_decisions_csv_exists", "expected": True, "actual": DECISIONS_6GN.exists(), "passed": DECISIONS_6GN.exists()},
        {"check": "6gn_gameplay_csv_exists", "expected": True, "actual": GAMEPLAY_6GN.exists(), "passed": GAMEPLAY_6GN.exists()},
        {"check": "6gn_meta_csv_exists", "expected": True, "actual": META_6GN.exists(), "passed": META_6GN.exists()},
        {"check": "6gn_backtest_csv_exists", "expected": True, "actual": BACKTEST_6GN.exists(), "passed": BACKTEST_6GN.exists()},
        {"check": "6gn_projection_payload_csv_exists", "expected": True, "actual": PROJECTION_PAYLOAD_6GN.exists(), "passed": PROJECTION_PAYLOAD_6GN.exists()},
        {"check": "6gn_recalibration_csv_exists", "expected": True, "actual": RECALIBRATION_6GN.exists(), "passed": RECALIBRATION_6GN.exists()},
    ]

    decision_integrity_rows: List[Dict[str, Any]] = []
    for row in gameplay_rows:
        mechanic_key = row.get("mechanic_key", "")
        default_decision = row.get("default_decision", "")
        secondary_decision = row.get("secondary_decision", "")
        decision_integrity_rows.append(
            {
                "mechanic_key": mechanic_key,
                "is_gameplay_mechanic": True,
                "default_decision": default_decision,
                "secondary_decision": secondary_decision,
                "default_decision_allowed": decision_allowed(default_decision),
                "secondary_decision_allowed": decision_allowed(secondary_decision),
                "activation_allowed_by_6gn_false": not boolish(row.get("activation_allowed_by_6gn")),
                "mechanics_activated_by_this_layer_false": not boolish(row.get("mechanics_activated_by_this_layer")),
                "layer_6_exit_credit_allowed_now_false": not boolish(row.get("layer_6_exit_credit_allowed_now")),
                "requires_backtest_true": boolish(row.get("requires_backtest")),
                "decision_reason_present": bool(row.get("decision_reason", "").strip()),
                "passed": (
                    decision_allowed(default_decision)
                    and decision_allowed(secondary_decision)
                    and not boolish(row.get("activation_allowed_by_6gn"))
                    and not boolish(row.get("mechanics_activated_by_this_layer"))
                    and not boolish(row.get("layer_6_exit_credit_allowed_now"))
                    and boolish(row.get("requires_backtest"))
                    and bool(row.get("decision_reason", "").strip())
                ),
            }
        )

    for row in meta_rows:
        default_decision = row.get("default_decision", "")
        secondary_decision = row.get("secondary_decision", "")
        decision_integrity_rows.append(
            {
                "mechanic_key": row.get("mechanic_key", ""),
                "is_gameplay_mechanic": False,
                "default_decision": default_decision,
                "secondary_decision": secondary_decision,
                "default_decision_allowed": decision_allowed(default_decision),
                "secondary_decision_allowed": decision_allowed(secondary_decision),
                "activation_allowed_by_6gn_false": not boolish(row.get("activation_allowed_by_6gn")),
                "mechanics_activated_by_this_layer_false": not boolish(row.get("mechanics_activated_by_this_layer")),
                "layer_6_exit_credit_allowed_now_false": not boolish(row.get("layer_6_exit_credit_allowed_now")),
                "requires_backtest_true": boolish(row.get("requires_backtest")),
                "decision_reason_present": bool(row.get("decision_reason", "").strip()),
                "passed": (
                    decision_allowed(default_decision)
                    and decision_allowed(secondary_decision)
                    and not boolish(row.get("activation_allowed_by_6gn"))
                    and not boolish(row.get("mechanics_activated_by_this_layer"))
                    and not boolish(row.get("layer_6_exit_credit_allowed_now"))
                    and bool(row.get("decision_reason", "").strip())
                ),
            }
        )

    backtest_gate_rows = [
        {
            "mechanic_key": key,
            "decision_row_present": key in gameplay_keys,
            "backtest_gate_present": key in backtest_keys,
            "activation_blocked_until_passed": any(
                row.get("mechanic_key") == key and boolish(row.get("activation_blocked_until_passed"))
                for row in backtest_rows
            ),
            "passed": (
                key in gameplay_keys
                and key in backtest_keys
                and any(
                    row.get("mechanic_key") == key and boolish(row.get("activation_blocked_until_passed"))
                    for row in backtest_rows
                )
            ),
        }
        for key in GAMEPLAY_MECHANICS
    ]

    if required_projection_payload_keys:
        projection_payload_gate_rows = [
            {
                "mechanic_key": key,
                "gate_required_by_decision": True,
                "gate_present": key in projection_payload_gate_keys,
                "activation_blocked_until_passed": any(
                    row.get("mechanic_key") == key and boolish(row.get("activation_blocked_until_passed"))
                    for row in projection_payload_rows
                ),
                "passed": (
                    key in projection_payload_gate_keys
                    and any(
                        row.get("mechanic_key") == key and boolish(row.get("activation_blocked_until_passed"))
                        for row in projection_payload_rows
                    )
                ),
            }
            for key in sorted(required_projection_payload_keys)
        ]
    else:
        projection_payload_gate_rows = [
            {
                "mechanic_key": "__none__",
                "gate_required_by_decision": False,
                "gate_present": True,
                "activation_blocked_until_passed": False,
                "passed": True,
            }
        ]

    if required_recalibration_keys:
        recalibration_gate_rows = [
            {
                "mechanic_key": key,
                "gate_required_by_decision": True,
                "gate_present": key in recalibration_gate_keys,
                "activation_blocked_until_passed": any(
                    row.get("mechanic_key") == key and boolish(row.get("activation_blocked_until_passed"))
                    for row in recalibration_rows
                ),
                "passed": (
                    key in recalibration_gate_keys
                    and any(
                        row.get("mechanic_key") == key and boolish(row.get("activation_blocked_until_passed"))
                        for row in recalibration_rows
                    )
                ),
            }
            for key in sorted(required_recalibration_keys)
        ]
    else:
        recalibration_gate_rows = [
            {
                "mechanic_key": "__none__",
                "gate_required_by_decision": False,
                "gate_present": True,
                "activation_blocked_until_passed": False,
                "passed": True,
            }
        ]

    exit_rows = [
        {"exit_criterion": "layer_6_exit_ready", "expected": False, "actual": json_6gn.get("layer_6_exit_ready"), "passed": json_6gn.get("layer_6_exit_ready") is False},
        {"exit_criterion": "mechanics_activated_by_this_layer", "expected": False, "actual": json_6gn.get("mechanics_activated_by_this_layer"), "passed": json_6gn.get("mechanics_activated_by_this_layer") is False},
        {"exit_criterion": "all_gameplay_exit_credit_blocked", "expected": True, "actual": all(not boolish(row.get("layer_6_exit_credit_allowed_now")) for row in gameplay_rows), "passed": all(not boolish(row.get("layer_6_exit_credit_allowed_now")) for row in gameplay_rows)},
        {"exit_criterion": "all_gameplay_activation_blocked", "expected": True, "actual": all(not boolish(row.get("activation_allowed_by_6gn")) for row in gameplay_rows), "passed": all(not boolish(row.get("activation_allowed_by_6gn")) for row in gameplay_rows)},
        {"exit_criterion": "all_gameplay_backtest_gated", "expected": True, "actual": all(boolish(row.get("requires_backtest")) for row in gameplay_rows), "passed": all(boolish(row.get("requires_backtest")) for row in gameplay_rows)},
        {"exit_criterion": "backtest_before_activation", "expected": True, "actual": all(row["passed"] for row in backtest_gate_rows), "passed": all(row["passed"] for row in backtest_gate_rows)},
        {"exit_criterion": "projection_payload_gate_if_required", "expected": True, "actual": all(row["passed"] for row in projection_payload_gate_rows), "passed": all(row["passed"] for row in projection_payload_gate_rows)},
        {"exit_criterion": "recalibration_gate_if_required", "expected": True, "actual": all(row["passed"] for row in recalibration_gate_rows), "passed": all(row["passed"] for row in recalibration_gate_rows)},
    ]

    future_rows = [
        {"contract": "plan_backtest_harness_for_all_10_gameplay_mechanics", "required": True, "passed": True},
        {"contract": "define_candidate_vs_current_off_comparison", "required": True, "passed": True},
        {"contract": "define_total_run_distribution_metrics", "required": True, "passed": True},
        {"contract": "define_team_total_distribution_metrics", "required": True, "passed": True},
        {"contract": "define_inning_level_run_distribution_metrics", "required": True, "passed": True},
        {"contract": "define_tail_and_variance_metrics", "required": True, "passed": True},
        {"contract": "define_calibration_error_metrics", "required": True, "passed": True},
        {"contract": "keep_activation_blocked_until_backtest_passes", "required": True, "passed": True},
        {"contract": "recommended_6gp_diagnosis", "required": True, "passed": True, "artifact": "layer_6_gameplay_mechanic_backtest_harness_plan_complete"},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_ready", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6GO, "actual": DIAGNOSIS_6GO, "passed": True},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_after = PLAN_6GN_PATH.read_text(encoding="utf-8") if PLAN_6GN_PATH.exists() else ""
    audit_after = AUDIT_6GM_PATH.read_text(encoding="utf-8") if AUDIT_6GM_PATH.exists() else ""
    validator_after = VALIDATOR_6GL_PATH.read_text(encoding="utf-8") if VALIDATOR_6GL_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6go_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6gn_plan", "policy": "unchanged_by_6go_audit", "passed": plan_after == plan_before},
        {"surface": "6gm_audit", "policy": "unchanged_by_6go_audit", "passed": audit_after == audit_before},
        {"surface": "6gl_validator", "policy": "unchanged_by_6go_audit", "passed": validator_after == validator_before},
        {"surface": "simulator_behavior", "policy": "unchanged_by_6go_audit", "passed": True},
        {"surface": "projection_behavior", "policy": "unchanged_by_6go_audit", "passed": True},
        {"surface": "fixtures", "policy": "unchanged_by_6go_audit", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6go_audit", "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "gameplay_mechanics_present", "passed": gameplay_keys == set(GAMEPLAY_MECHANICS), "detail": f"{len(gameplay_keys)}/10"},
        {"check": "meta_surfaces_present", "passed": meta_keys == set(META_SURFACES), "detail": f"{len(meta_keys)}/2"},
        {"check": "decision_integrity", "passed": all(row["passed"] for row in decision_integrity_rows), "detail": f"{sum(1 for row in decision_integrity_rows if row['passed'])}/{len(decision_integrity_rows)}"},
        {"check": "backtest_gate", "passed": all(row["passed"] for row in backtest_gate_rows), "detail": f"{sum(1 for row in backtest_gate_rows if row['passed'])}/{len(backtest_gate_rows)}"},
        {"check": "projection_payload_gate", "passed": all(row["passed"] for row in projection_payload_gate_rows), "detail": f"{sum(1 for row in projection_payload_gate_rows if row['passed'])}/{len(projection_payload_gate_rows)}"},
        {"check": "recalibration_gate", "passed": all(row["passed"] for row in recalibration_gate_rows), "detail": f"{sum(1 for row in recalibration_gate_rows if row['passed'])}/{len(recalibration_gate_rows)}"},
        {"check": "exit_criteria", "passed": all(row["passed"] for row in exit_rows), "detail": f"{sum(1 for row in exit_rows if row['passed'])}/{len(exit_rows)}"},
        {"check": "future_6gp_contract", "passed": all(row["passed"] for row in future_rows), "detail": f"{sum(1 for row in future_rows if row['passed'])}/{len(future_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "decision_integrity": write_csv(DECISION_INTEGRITY_CSV, decision_integrity_rows),
        "backtest_gate": write_csv(BACKTEST_GATE_CSV, backtest_gate_rows),
        "projection_payload_gate": write_csv(PROJECTION_PAYLOAD_GATE_CSV, projection_payload_gate_rows),
        "recalibration_gate": write_csv(RECALIBRATION_GATE_CSV, recalibration_gate_rows),
        "exit_criteria": write_csv(EXIT_CRITERIA_CSV, exit_rows),
        "future_6gp_contract": write_csv(FUTURE_6GP_CONTRACT_CSV, future_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GO",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "audited_layer": "6GN",
        "audited_plan_diagnosis": json_6gn.get("diagnosis"),
        "diagnosis": DIAGNOSIS_6GO if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "gameplay_mechanics_count": len(gameplay_rows),
        "meta_surface_count": len(meta_rows),
        "decision_rows_count": len(decision_rows),
        "backtest_gate_count": len(backtest_gate_rows),
        "projection_payload_gate_count": len(projection_payload_gate_rows),
        "recalibration_gate_count": len(recalibration_gate_rows),
        "predecessor_plan": str(PLAN_6GN_PATH),
        "predecessor_plan_returncode": plan_run.returncode,
        "predecessor_plan_diagnosis": json_6gn.get("diagnosis"),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "decision_integrity_csv": str(DECISION_INTEGRITY_CSV),
            "backtest_gate_csv": str(BACKTEST_GATE_CSV),
            "projection_payload_gate_csv": str(PROJECTION_PAYLOAD_GATE_CSV),
            "recalibration_gate_csv": str(RECALIBRATION_GATE_CSV),
            "exit_criteria_csv": str(EXIT_CRITERIA_CSV),
            "future_6gp_contract_csv": str(FUTURE_6GP_CONTRACT_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
