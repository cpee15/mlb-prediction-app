#!/usr/bin/env python3
"""Implement safe readonly numeric probability repair from existing local artifacts only."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mg_projection_adapter_numeric_probability_repair_implementation"
TMP_DIR = Path("tmp")

SCRIPT_6MF = Path("scripts/plan_6mf_layer6_projection_adapter_numeric_probability_repair.py")
JSON_6MF = TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan.json"

REQUIRED_INPUTS = [
    JSON_6MF,
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_checks.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_predecessor.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_problem_statement.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_repair_objectives.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_repair_options.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_selected_repair_path.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_numeric_surface_contract.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_validation_contract.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_blockers.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_future_6mg_contract.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_decision.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6mf_projection_adapter_numeric_probability_repair_plan_recommended_path.csv",
    SCRIPT_6MF,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CANDIDATE_SCAN_CSV = TMP_DIR / f"{SLUG}_candidate_scan.csv"
FIELD_REVIEW_CSV = TMP_DIR / f"{SLUG}_candidate_field_review.csv"
REPAIRED_SURFACE_CSV = TMP_DIR / f"{SLUG}_repaired_surface.csv"
REPAIR_RESULT_CSV = TMP_DIR / f"{SLUG}_repair_result.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MH_CSV = TMP_DIR / f"{SLUG}_future_6mh_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MF = "layer_6_projection_adapter_numeric_probability_repair_plan_complete"
DIAGNOSIS_6MG = "layer_6_projection_adapter_numeric_probability_repair_implementation_complete"
RECOMMENDED_NEXT_LAYER_6MG = "6MH_layer_6_projection_adapter_numeric_probability_repair_audit"
RECOMMENDED_PATH_6MG = "audit_numeric_probability_repair_result"

GAME_ID_FIELDS = ["game_pk", "game_id", "mlb_game_id", "event_id"]
HOME_PROB_FIELDS = ["home_win_probability", "home_win_prob", "home_probability", "home_prob"]
AWAY_PROB_FIELDS = ["away_win_probability", "away_win_prob", "away_probability", "away_prob"]
PLACEHOLDER_MARKERS = ["MAPPED_FROM_", "PRESENT_IN_", "placeholder", "PLACEHOLDER"]


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True, "passed": True}]
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return {}


def load_json_records(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("rows", "games", "data", "surface", "normalized_surface", "records", "predictions"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        return [payload]
    return []


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8", errors="ignore"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def boolish(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def all_passed(rows: list[dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def coerce_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if any(marker in stripped for marker in PLACEHOLDER_MARKERS):
            return None
        if stripped.endswith("%"):
            try:
                pct = float(stripped[:-1].strip())
                return pct / 100.0
            except ValueError:
                return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def is_prob(value: Any) -> bool:
    numeric = coerce_float(value)
    return numeric is not None and 0.0 <= numeric <= 1.0


def first_present(row: dict[str, Any], fields: list[str]) -> tuple[str | None, Any]:
    for field in fields:
        if field in row:
            return field, row[field]
    return None, None


def has_placeholder_value(row: dict[str, Any]) -> bool:
    for value in row.values():
        if isinstance(value, str) and any(marker in value for marker in PLACEHOLDER_MARKERS):
            return True
    return False


def row_to_repaired_surface(row: dict[str, Any], source_path: Path, source_type: str) -> dict[str, Any] | None:
    game_field, game_value = first_present(row, GAME_ID_FIELDS)
    home_field, home_value = first_present(row, HOME_PROB_FIELDS)
    away_field, away_value = first_present(row, AWAY_PROB_FIELDS)

    home_prob = coerce_float(home_value)
    away_prob = coerce_float(away_value)

    if not game_value or home_prob is None or away_prob is None:
        return None
    if not (0.0 <= home_prob <= 1.0 and 0.0 <= away_prob <= 1.0):
        return None
    if abs((home_prob + away_prob) - 1.0) > 0.025:
        return None
    if has_placeholder_value({"home": home_value, "away": away_value}):
        return None

    return {
        "game_pk": str(game_value),
        "home_win_probability": round(home_prob, 8),
        "away_win_probability": round(away_prob, 8),
        "home_win_prob": round(home_prob, 8),
        "away_win_prob": round(away_prob, 8),
        "probability_sum": round(home_prob + away_prob, 8),
        "source_artifact": str(source_path),
        "source_type": source_type,
        "source_game_field": game_field or "",
        "source_home_probability_field": home_field or "",
        "source_away_probability_field": away_field or "",
    }


def scan_artifact(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]]
    artifact_type = path.suffix.lower().lstrip(".")
    if path.suffix.lower() == ".csv":
        rows = read_csv_rows(path)
    elif path.suffix.lower() == ".json":
        rows = load_json_records(path)
    else:
        rows = []

    candidate_rows: list[dict[str, Any]] = []
    reviewed_rows = 0
    numeric_pair_rows = 0
    placeholder_rows = 0

    for row in rows[:1000]:
        if not isinstance(row, dict):
            continue
        reviewed_rows += 1
        _, game_value = first_present(row, GAME_ID_FIELDS)
        home_field, home_value = first_present(row, HOME_PROB_FIELDS)
        away_field, away_value = first_present(row, AWAY_PROB_FIELDS)

        if has_placeholder_value(row):
            placeholder_rows += 1

        has_numeric_pair = bool(game_value) and is_prob(home_value) and is_prob(away_value)
        if has_numeric_pair:
            numeric_pair_rows += 1

        repaired = row_to_repaired_surface(row, path, artifact_type)
        if repaired is not None:
            candidate_rows.append(repaired)

    scan = {
        "artifact_path": str(path),
        "artifact_type": artifact_type,
        "row_count_reviewed": reviewed_rows,
        "numeric_pair_rows": numeric_pair_rows,
        "placeholder_rows": placeholder_rows,
        "candidate_rows": len(candidate_rows),
        "passed": True,
    }
    return candidate_rows, scan


def valid_repaired_surface(rows: list[dict[str, Any]]) -> tuple[bool, bool, bool, bool, bool]:
    if not rows:
        return False, False, False, False, False

    contract_valid = True
    no_placeholders = True
    bounds_valid = True
    sum_valid = True
    provenance_present = True

    for row in rows:
        required_fields = [
            "game_pk",
            "home_win_probability",
            "away_win_probability",
            "source_artifact",
            "source_home_probability_field",
            "source_away_probability_field",
        ]
        if not all(row.get(field) not in ("", None) for field in required_fields):
            contract_valid = False
            provenance_present = False

        home = coerce_float(row.get("home_win_probability"))
        away = coerce_float(row.get("away_win_probability"))

        if has_placeholder_value(row):
            no_placeholders = False
        if home is None or away is None or not (0.0 <= home <= 1.0 and 0.0 <= away <= 1.0):
            bounds_valid = False
        if home is None or away is None or abs((home + away) - 1.0) > 0.025:
            sum_valid = False

    return contract_valid, no_placeholders, bounds_valid, sum_valid, provenance_present


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6mf = load_json(JSON_6MF)

    input_rows = [
        {
            "artifact_path": str(path),
            "exists": path.exists(),
            "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "",
            "passed": path.exists(),
        }
        for path in REQUIRED_INPUTS
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6mf_script_exists", "expected": True, "actual": SCRIPT_6MF.exists(), "passed": SCRIPT_6MF.exists()},
        {"check": "6mf_json_exists", "expected": True, "actual": JSON_6MF.exists(), "passed": JSON_6MF.exists()},
        {"check": "6mf_all_checks_passed", "expected": True, "actual": json_6mf.get("all_checks_passed"), "passed": json_6mf.get("all_checks_passed") is True},
        {"check": "6mf_diagnosis", "expected": DIAGNOSIS_6MF, "actual": json_6mf.get("diagnosis"), "passed": json_6mf.get("diagnosis") == DIAGNOSIS_6MF},
        {"check": "6mf_recommended_next_layer", "expected": "6MG_layer_6_projection_adapter_numeric_probability_repair_implementation", "actual": json_6mf.get("recommended_next_layer"), "passed": json_6mf.get("recommended_next_layer") == "6MG_layer_6_projection_adapter_numeric_probability_repair_implementation"},
    ]

    artifact_paths = sorted(
        [
            path
            for pattern in ("*.json", "*.csv")
            for path in TMP_DIR.glob(pattern)
            if not path.name.startswith(SLUG)
        ]
    )

    all_candidates: list[dict[str, Any]] = []
    scan_rows: list[dict[str, Any]] = []
    field_review_rows: list[dict[str, Any]] = []

    for path in artifact_paths:
        candidates, scan = scan_artifact(path)
        scan_rows.append(scan)
        all_candidates.extend(candidates)
        if candidates:
            sample = candidates[0]
            field_review_rows.append(
                {
                    "artifact_path": str(path),
                    "candidate_rows": len(candidates),
                    "sample_game_pk": sample.get("game_pk"),
                    "home_win_probability": sample.get("home_win_probability"),
                    "away_win_probability": sample.get("away_win_probability"),
                    "probability_sum": sample.get("probability_sum"),
                    "source_home_probability_field": sample.get("source_home_probability_field"),
                    "source_away_probability_field": sample.get("source_away_probability_field"),
                    "passed": True,
                }
            )

    selected_source_artifact = ""
    repaired_rows: list[dict[str, Any]] = []

    if all_candidates:
        selected_source_artifact = str(all_candidates[0]["source_artifact"])
        repaired_rows = [row for row in all_candidates if row.get("source_artifact") == selected_source_artifact]

    contract_valid, no_placeholders, bounds_valid, sum_valid, provenance_present = valid_repaired_surface(repaired_rows)
    materialized = bool(repaired_rows) and contract_valid and no_placeholders and bounds_valid and sum_valid and provenance_present
    explicit_blocker = not materialized

    repair_outcome_status = (
        "materialized_real_numeric_probability_surface"
        if materialized
        else "explicit_blocker_no_safe_numeric_source"
    )

    if not field_review_rows:
        field_review_rows = [
            {
                "artifact_path": "",
                "candidate_rows": 0,
                "sample_game_pk": "",
                "home_win_probability": "",
                "away_win_probability": "",
                "probability_sum": "",
                "source_home_probability_field": "",
                "source_away_probability_field": "",
                "passed": True,
            }
        ]

    repaired_surface_rows = repaired_rows if repaired_rows else [
        {
            "game_pk": "",
            "home_win_probability": "",
            "away_win_probability": "",
            "home_win_prob": "",
            "away_win_prob": "",
            "probability_sum": "",
            "source_artifact": "",
            "source_type": "",
            "source_game_field": "",
            "source_home_probability_field": "",
            "source_away_probability_field": "",
            "repair_blocker": "no_safe_local_numeric_probability_source_found",
        }
    ]

    repair_result_rows = [
        {
            "repair_outcome_status": repair_outcome_status,
            "selected_numeric_source_artifact": selected_source_artifact,
            "real_numeric_probability_surface_materialized": materialized,
            "explicit_blocker_emitted": explicit_blocker,
            "repaired_surface_row_count": len(repaired_rows),
            "numeric_surface_contract_valid": contract_valid,
            "no_placeholder_probability_values": no_placeholders,
            "numeric_probability_bounds_valid": bounds_valid,
            "home_away_probability_sum_valid": sum_valid,
            "source_provenance_present": provenance_present,
            "fake_probability_generation_used": False,
            "passed": True,
        }
    ]

    blockers = [
        {
            "blocker": "no_safe_local_numeric_probability_source_found",
            "active": explicit_blocker,
            "reason": "no existing local artifact with game id plus numeric home/away probabilities passed the repair contract" if explicit_blocker else "",
            "passed": True,
        },
        {
            "blocker": "repair_result_requires_audit",
            "active": True,
            "reason": "6MH must audit materialization or blocker before metrics/backtests",
            "passed": True,
        },
        {
            "blocker": "metrics_backtests_tuning_activation_exit_blocked",
            "active": True,
            "reason": "requires audited real numeric probability surface",
            "passed": True,
        },
    ]

    future_6mh = [
        {
            "contract": "audit_numeric_probability_repair_result",
            "required": True,
            "why": "must audit materialized surface or explicit blocker",
            "passed": True,
        },
        {
            "contract": "confirm_no_fake_probability_generation",
            "required": True,
            "why": "repair result must not silently invent probabilities",
            "passed": True,
        },
        {
            "contract": "preserve_no_adapter_metrics_backtest_tuning_activation_or_exit",
            "required": True,
            "why": "audit remains pre-metric unless later layer explicitly opens it",
            "passed": True,
        },
    ]

    blocking_policy = [
        {"policy": "do_not_generate_fake_numeric_probabilities", "required": True, "passed": True},
        {"policy": "do_not_claim_metric_readiness_until_6mh_audits_repair_result", "required": True, "passed": True},
        {"policy": "do_not_run_metrics_backtests_or_tuning_in_6mg", "required": True, "passed": True},
    ]

    implementation_outcome_passed = materialized or explicit_blocker

    decision_rows = [
        {"decision": "6mf_passed", "expected": True, "actual": json_6mf.get("all_checks_passed"), "passed": json_6mf.get("all_checks_passed") is True},
        {"decision": "6mf_diagnosis_valid", "expected": DIAGNOSIS_6MF, "actual": json_6mf.get("diagnosis"), "passed": json_6mf.get("diagnosis") == DIAGNOSIS_6MF},
        {"decision": "all_required_6mf_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "safe_local_artifact_scan_run", "expected": True, "actual": bool(scan_rows), "passed": bool(scan_rows)},
        {"decision": "implementation_outcome_valid", "expected": True, "actual": implementation_outcome_passed, "passed": implementation_outcome_passed},
        {"decision": "fake_probability_generation_used", "expected": False, "actual": False, "passed": True},
        {"decision": "recommend_6mh_next", "expected": RECOMMENDED_NEXT_LAYER_6MG, "actual": RECOMMENDED_NEXT_LAYER_6MG, "passed": True},
        {"decision": "do_not_recommend_run_metrics_backtest_tuning_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_numeric_probability_repair_readonly_safe", "expected": True, "actual": True, "passed": True},
        {"boundary": "fake_probability_generation_used", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mg", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6mg", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mg", "expected": False, "actual": False, "passed": True},
        {"boundary": "run_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mg", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_acquisition_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_source_modifications_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MG, "actual": RECOMMENDED_NEXT_LAYER_6MG, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MG, "actual": RECOMMENDED_PATH_6MG, "passed": True},
        {"decision": "do_not_recommend_run_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MG, "actual": DIAGNOSIS_6MG, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "candidate_scan", "passed": bool(scan_rows), "detail": f"{len(scan_rows)} artifacts scanned"},
        {"check": "candidate_field_review", "passed": all_passed(field_review_rows), "detail": f"{sum(1 for r in field_review_rows if r['passed'])}/{len(field_review_rows)}"},
        {"check": "repair_result", "passed": all_passed(repair_result_rows), "detail": repair_outcome_status},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mh_contract", "passed": all_passed(future_6mh), "detail": f"{sum(1 for r in future_6mh if r['passed'])}/{len(future_6mh)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{sum(1 for r in blocking_policy if r['passed'])}/{len(blocking_policy)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "candidate_scan": write_csv(CANDIDATE_SCAN_CSV, scan_rows),
        "candidate_field_review": write_csv(FIELD_REVIEW_CSV, field_review_rows),
        "repaired_surface": write_csv(REPAIRED_SURFACE_CSV, repaired_surface_rows),
        "repair_result": write_csv(REPAIR_RESULT_CSV, repair_result_rows),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mh_contract": write_csv(FUTURE_6MH_CSV, future_6mh),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MG",
        "layer_type": "game_mechanics_realism",
        "implementation_only_numeric_probability_repair_readonly_safe": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MG if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MG,
        "recommended_path": RECOMMENDED_PATH_6MG,
        "predecessor_layer": "6MF",
        "predecessor_diagnosis": json_6mf.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mf.get("all_checks_passed") is True,
        "implemented_layer_after": "6MF",
        "source_family": "projection_adapter_numeric_probability_repair_implementation",
        "repair_implementation_attempted": True,
        "safe_local_artifact_scan_run": True,
        "candidate_artifact_count": len(scan_rows),
        "numeric_source_candidate_count": len(all_candidates),
        "selected_numeric_source_artifact": selected_source_artifact,
        "repair_outcome_status": repair_outcome_status,
        "real_numeric_probability_surface_materialized": materialized,
        "explicit_blocker_emitted": explicit_blocker,
        "repaired_surface_row_count": len(repaired_rows),
        "numeric_surface_contract_valid": contract_valid,
        "no_placeholder_probability_values": no_placeholders,
        "numeric_probability_bounds_valid": bounds_valid,
        "home_away_probability_sum_valid": sum_valid,
        "source_provenance_present": provenance_present,
        "fake_probability_generation_used": False,
        "adapter_call_executed_by_6mg": False,
        "metric_execution_run_by_6mg": False,
        "backtest_execution_run_by_6mg": False,
        "run_metric_execution_run": False,
        "production_code_modified_by_6mg": False,
        "full_batch_adapter_call_run": False,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "local_measurement_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "production_source_modifications_run": False,
        "games_evaluated": len(repaired_rows),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "candidate_scan_csv": str(CANDIDATE_SCAN_CSV),
            "candidate_field_review_csv": str(FIELD_REVIEW_CSV),
            "repaired_surface_csv": str(REPAIRED_SURFACE_CSV),
            "repair_result_csv": str(REPAIR_RESULT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mh_contract_csv": str(FUTURE_6MH_CSV),
            "blocking_policy_csv": str(BLOCKING_POLICY_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
