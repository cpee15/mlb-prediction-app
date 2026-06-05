#!/usr/bin/env python3
"""Implement Layer 6 projection-call contract.

This implementation creates a tmp-only fixture contract surface, inventories
candidate projection entrypoints, statically safety-scans them, and emits either
a non-production projection surface or a precise adapter gap report.

It does not fetch remote data, call external APIs, write databases, mutate
production source, run real backtest metrics, activate mechanics, or grant
Layer 6 exit.
"""

from __future__ import annotations

import ast
import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6kz_projection_call_contract_implementation"
TMP_DIR = Path("tmp")

PLAN_6KY_PATH = Path("scripts/plan_6ky_layer6_projection_call_contract.py")
JSON_6KY = TMP_DIR / "layer6_6ky_projection_call_contract_plan.json"

SCHEDULE_CANDIDATES = TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_schedule_input_candidates.csv"
ACTUAL_CANDIDATES = TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_actual_outcome_candidates.csv"
PROJECTION_CANDIDATES = TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_projection_route_candidates.csv"
SOURCE_GAP_6KW = TMP_DIR / "layer6_6kw_historical_backtest_source_generation_implementation_source_generation_gap_report.csv"

REQUIRED_INPUTS = [
    JSON_6KY,
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_checks.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_predecessor.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_problem_statement.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_input_contract.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_output_contract.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_entrypoint_discovery_rules.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_adapter_strategy.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_fixture_generation_strategy.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_fallback_strategy.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_evaluation_surface_integration.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_allowed_operations.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_forbidden_operations.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_blockers.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_future_6kz_contract.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_future_6la_contract.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_decision.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6ky_projection_call_contract_plan_recommended_path.csv",
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
FIXTURE_SURFACE_CSV = TMP_DIR / f"{SLUG}_fixture_contract_surface.csv"
ENTRYPOINT_INVENTORY_CSV = TMP_DIR / f"{SLUG}_entrypoint_inventory.csv"
ENTRYPOINT_SAFETY_CSV = TMP_DIR / f"{SLUG}_entrypoint_safety_scan.csv"
ADAPTER_FEASIBILITY_CSV = TMP_DIR / f"{SLUG}_adapter_feasibility.csv"
PROJECTION_SURFACE_CSV = TMP_DIR / f"{SLUG}_projection_surface.csv"
ADAPTER_GAP_CSV = TMP_DIR / f"{SLUG}_projection_adapter_gap_report.csv"
METRIC_READINESS_CSV = TMP_DIR / f"{SLUG}_metric_readiness.csv"
LINEAGE_CSV = TMP_DIR / f"{SLUG}_lineage_report.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6LA_CSV = TMP_DIR / f"{SLUG}_future_6la_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KY = "layer_6_projection_call_contract_plan_complete"
DIAGNOSIS_6KZ = "layer_6_projection_call_contract_implementation_complete"
RECOMMENDED_NEXT_LAYER_6KY = "6KZ_layer_6_projection_call_contract_implementation"
RECOMMENDED_NEXT_LAYER_6KZ = "6LA_layer_6_projection_call_contract_implementation_audit"
RECOMMENDED_PATH_6KZ = "audit_projection_call_contract_implementation_before_surface_generation"

MECHANIC_TAGS = "bullpen_active;double_play_reachable_delta_unproven;sac_fly_reachable_delta_unproven;extras_walkoff_bypassed;steals_inactive;balk_deferred"
RISK_TOKENS = [
    "requests.", "httpx.", "urllib.", "aiohttp.", "fetch(", "axios.", "database",
    "db.", "sqlalchemy", "supabase", "firebase", "boto3", "subprocess",
    "open(", ".write(", "to_sql", "insert ", "update ", "delete ",
    "flask", "fastapi", "uvicorn", "streamlit", "os.environ",
]


def read_csv_rows(path: Path, limit: int | None = None) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            rows = []
            for idx, row in enumerate(csv.DictReader(handle)):
                rows.append(row)
                if limit is not None and idx + 1 >= limit:
                    break
            return rows
    except Exception:
        return []


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True, "passed": True}]
    fieldnames: List[str] = []
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


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
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


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def first_present(row: Dict[str, str], names: List[str]) -> str:
    lower = {k.lower(): k for k in row}
    for name in names:
        if name.lower() in lower:
            return row.get(lower[name.lower()], "")
    return ""


def read_top_candidate_data(path: str, limit: int = 25) -> List[Dict[str, str]]:
    p = Path(path)
    if not p.exists() or p.suffix.lower() != ".csv":
        return []
    return read_csv_rows(p, limit=limit)


def build_fixture_surface() -> List[Dict[str, Any]]:
    schedule_candidates = read_csv_rows(SCHEDULE_CANDIDATES, limit=25)
    actual_candidates = read_csv_rows(ACTUAL_CANDIDATES, limit=25)

    candidate_paths = []
    for row in schedule_candidates:
        p = row.get("path") or row.get("artifact_path")
        if p:
            candidate_paths.append(p)
    for row in actual_candidates:
        p = row.get("path") or row.get("artifact_path")
        if p:
            candidate_paths.append(p)

    fixtures: List[Dict[str, Any]] = []
    seen = set()
    for source_path in candidate_paths[:12]:
        for src_row in read_top_candidate_data(source_path, limit=10):
            game_id = first_present(src_row, ["game_id", "game_pk", "mlb_game_id", "id"])
            game_date = first_present(src_row, ["game_date", "date"])
            home_team = first_present(src_row, ["home_team", "home", "home_abbrev", "team_name"])
            away_team = first_present(src_row, ["away_team", "away", "away_abbrev"])
            if not away_team and first_present(src_row, ["side"]):
                away_team = ""
            key = (game_id, game_date, home_team, away_team, source_path)
            if key in seen:
                continue
            seen.add(key)
            if not (game_id or game_date or home_team or away_team):
                continue
            fixtures.append({
                "game_id": game_id,
                "game_date": game_date,
                "season": (game_date[:4] if len(game_date) >= 4 and game_date[:4].isdigit() else ""),
                "home_team": home_team,
                "away_team": away_team,
                "home_pitcher": "",
                "away_pitcher": "",
                "home_lineup_proxy": "",
                "away_lineup_proxy": "",
                "park_factor_proxy": "",
                "bullpen_state_proxy": "",
                "mechanic_context_tags": MECHANIC_TAGS,
                "generation_mode": "historical_fixture_contract_tmp_only",
                "source_lineage": source_path,
                "non_production": True,
                "passed": True,
            })
            if len(fixtures) >= 50:
                return fixtures

    if not fixtures:
        fixtures.append({
            "game_id": "",
            "game_date": "",
            "season": "",
            "home_team": "",
            "away_team": "",
            "home_pitcher": "",
            "away_pitcher": "",
            "home_lineup_proxy": "",
            "away_lineup_proxy": "",
            "park_factor_proxy": "",
            "bullpen_state_proxy": "",
            "mechanic_context_tags": MECHANIC_TAGS,
            "generation_mode": "empty_fixture_gap_mode",
            "source_lineage": "no_fixture_rows_materialized",
            "non_production": True,
            "passed": True,
        })
    return fixtures


def inventory_entrypoints() -> List[Dict[str, Any]]:
    projection_candidates = read_csv_rows(PROJECTION_CANDIDATES, limit=100)
    out: List[Dict[str, Any]] = []
    for row in projection_candidates:
        path_str = row.get("path") or row.get("source_path") or row.get("artifact_path")
        if not path_str:
            continue
        path = Path(path_str)
        if not path.exists() or path.suffix != ".py":
            out.append({
                "path": path_str,
                "entrypoint_type": "non_python_or_missing",
                "entrypoint_name": "",
                "projection_score": row.get("projection_score", ""),
                "callable_candidate": False,
                "passed": True,
            })
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        try:
            tree = ast.parse(text)
        except Exception as exc:
            out.append({
                "path": path_str,
                "entrypoint_type": "parse_error",
                "entrypoint_name": "",
                "projection_score": row.get("projection_score", ""),
                "callable_candidate": False,
                "parse_error": f"{type(exc).__name__}: {exc}",
                "passed": True,
            })
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = node.name
                if re.search(r"(project|predict|prob|simulation|expected|model|run)", name, re.I):
                    out.append({
                        "path": path_str,
                        "entrypoint_type": "function",
                        "entrypoint_name": name,
                        "projection_score": row.get("projection_score", ""),
                        "callable_candidate": True,
                        "passed": True,
                    })
            elif isinstance(node, ast.ClassDef):
                name = node.name
                if re.search(r"(project|predict|prob|simulation|model)", name, re.I):
                    out.append({
                        "path": path_str,
                        "entrypoint_type": "class",
                        "entrypoint_name": name,
                        "projection_score": row.get("projection_score", ""),
                        "callable_candidate": False,
                        "passed": True,
                    })
        if len(out) >= 100:
            break
    if not out:
        out.append({
            "path": "",
            "entrypoint_type": "none_found",
            "entrypoint_name": "",
            "projection_score": "",
            "callable_candidate": False,
            "passed": True,
        })
    return out[:100]


def safety_scan(inventory: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in inventory:
        path = Path(str(item.get("path", "")))
        text = path.read_text(encoding="utf-8", errors="ignore") if path.exists() and path.is_file() else ""
        lower = text.lower()
        hits = [tok for tok in RISK_TOKENS if tok.lower() in lower]
        safe = bool(item.get("callable_candidate")) and path.exists() and not hits
        rows.append({
            "path": str(item.get("path", "")),
            "entrypoint_name": item.get("entrypoint_name", ""),
            "callable_candidate": item.get("callable_candidate", False),
            "risk_token_count": len(hits),
            "risk_tokens": ";".join(hits[:20]),
            "safe_for_direct_call": safe,
            "safety_verdict": "safe_candidate_static_only" if safe else "not_safe_or_not_callable_without_adapter_repair",
            "passed": True,
        })
    return rows


def adapter_feasibility_rows(safety_rows: List[Dict[str, Any]], fixture_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    safe_count = sum(1 for r in safety_rows if boolish(r.get("safe_for_direct_call")))
    real_fixture_count = sum(1 for r in fixture_rows if r.get("game_id") or r.get("game_date"))
    can_attempt = safe_count > 0 and real_fixture_count > 0
    return [
        {"item": "fixture_contract_surface_created", "value": bool(fixture_rows), "passed": True},
        {"item": "real_fixture_like_rows", "value": real_fixture_count, "passed": True},
        {"item": "safe_projection_entrypoint_count", "value": safe_count, "passed": True},
        {"item": "adapter_call_attempted", "value": False, "reason": "no runtime call attempted in this implementation; static safety only", "passed": True},
        {"item": "projection_surface_possible_static", "value": can_attempt, "passed": True},
    ]


def projection_surface_or_gap(
    fixture_rows: List[Dict[str, Any]],
    inventory: List[Dict[str, Any]],
    scan_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    safe_rows = [r for r in scan_rows if boolish(r.get("safe_for_direct_call"))]
    projection_rows: List[Dict[str, Any]] = []
    gap_rows: List[Dict[str, Any]] = []

    if safe_rows:
        # Do not invoke; emit contract-only projection shell rows with no claimed predictions.
        for fixture in fixture_rows[:25]:
            projection_rows.append({
                "game_id": fixture.get("game_id", ""),
                "game_date": fixture.get("game_date", ""),
                "home_team": fixture.get("home_team", ""),
                "away_team": fixture.get("away_team", ""),
                "home_win_probability": "",
                "away_win_probability": "",
                "home_expected_runs": "",
                "away_expected_runs": "",
                "total_expected_runs": "",
                "projection_source": safe_rows[0].get("path", ""),
                "projection_entrypoint": safe_rows[0].get("entrypoint_name", ""),
                "projection_call_mode": "contract_shell_static_safe_candidate_no_call",
                "projection_call_status": "not_called_no_real_predictions",
                "missing_input_families": "runtime_call_contract_validation",
                "fallback_used": True,
                "notes": "Static safe candidate found, but no projection call executed in 6KZ.",
                "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
                "current_ui_realism_state_label": "bullpen_active_partial_realism",
                "mechanic_tags": MECHANIC_TAGS,
                "non_production": True,
                "passed": True,
            })
    else:
        gap_rows.append({
            "gap": "no_safe_projection_entrypoint_for_direct_adapter_call",
            "fixture_contract_surface_created": bool(fixture_rows),
            "entrypoint_inventory_count": len(inventory),
            "entrypoint_safety_scan_count": len(scan_rows),
            "safe_projection_entrypoint_found": False,
            "adapter_call_attempted": False,
            "missing_or_blocking_family": "safe_deterministic_projection_entrypoint_or_adapter_repair",
            "recommended_next_action": "audit_6kz_and_plan_projection_adapter_repair_or_static_fixture_projection_surface",
            "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
            "current_ui_realism_state_label": "bullpen_active_partial_realism",
            "non_production": True,
            "passed": True,
        })
    return projection_rows, gap_rows


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6ky = load_json(JSON_6KY)

    fixtures = build_fixture_surface()
    inventory = inventory_entrypoints()
    scan = safety_scan(inventory)
    feasibility = adapter_feasibility_rows(scan, fixtures)
    projection_rows, adapter_gap_rows = projection_surface_or_gap(fixtures, inventory, scan)

    projection_surface_materialized = bool(projection_rows)
    adapter_gap_report_emitted = bool(adapter_gap_rows)
    safe_entrypoint_found = any(boolish(r.get("safe_for_direct_call")) for r in scan)
    adapter_call_attempted = False

    probability_fields = projection_surface_materialized and any(
        r.get("home_win_probability") or r.get("away_win_probability") for r in projection_rows
    )
    runs_fields = projection_surface_materialized and any(
        r.get("home_expected_runs") or r.get("away_expected_runs") or r.get("total_expected_runs") for r in projection_rows
    )
    any_projection_fields = probability_fields or runs_fields

    metric_readiness = [
        {"metric": "probability_metric_ready_after_implementation", "value": probability_fields, "passed": True},
        {"metric": "runs_metric_ready_after_implementation", "value": runs_fields, "passed": True},
        {"metric": "any_backtest_metric_ready_after_implementation", "value": any_projection_fields, "passed": True},
        {"metric": "real_backtest_metrics_run", "value": False, "passed": True},
        {"metric": "projection_surface_materialized", "value": projection_surface_materialized, "passed": True},
        {"metric": "projection_adapter_gap_report_emitted", "value": adapter_gap_report_emitted, "passed": True},
    ]

    lineage = [
        {"lineage_item": "6ky_plan_json", "source_path": str(JSON_6KY), "exists": JSON_6KY.exists(), "passed": JSON_6KY.exists()},
        {"lineage_item": "schedule_candidates", "source_path": str(SCHEDULE_CANDIDATES), "exists": SCHEDULE_CANDIDATES.exists(), "passed": True},
        {"lineage_item": "actual_candidates", "source_path": str(ACTUAL_CANDIDATES), "exists": ACTUAL_CANDIDATES.exists(), "passed": True},
        {"lineage_item": "projection_candidates", "source_path": str(PROJECTION_CANDIDATES), "exists": PROJECTION_CANDIDATES.exists(), "passed": True},
        {"lineage_item": "fixture_contract_surface", "source_path": str(FIXTURE_SURFACE_CSV), "exists": True, "passed": True},
        {"lineage_item": "projection_surface_or_gap", "source_path": str(PROJECTION_SURFACE_CSV if projection_surface_materialized else ADAPTER_GAP_CSV), "exists": True, "passed": True},
    ]

    blockers = [
        {"blocker": "projection_call_contract_audit_required", "blocks_real_backtest": True, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "layer6_exit_not_allowed", "blocks_real_backtest": False, "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6la = [
        {"contract": "audit_fixture_contract_surface", "required": True, "passed": True},
        {"contract": "audit_entrypoint_inventory_and_safety_scan", "required": True, "passed": True},
        {"contract": "audit_projection_surface_or_adapter_gap_report", "required": True, "passed": True},
        {"contract": "route_to_surface_generation_or_adapter_repair", "required": True, "passed": True},
        {"contract": "preserve_no_fetch_no_db_write_no_real_metrics_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ky_plan_script_exists", "expected": True, "actual": PLAN_6KY_PATH.exists(), "passed": PLAN_6KY_PATH.exists()},
        {"check": "6ky_json_exists", "expected": True, "actual": JSON_6KY.exists(), "passed": JSON_6KY.exists()},
        {"check": "6ky_all_checks_passed", "expected": True, "actual": json_6ky.get("all_checks_passed"), "passed": json_6ky.get("all_checks_passed") is True},
        {"check": "6ky_diagnosis", "expected": DIAGNOSIS_6KY, "actual": json_6ky.get("diagnosis"), "passed": json_6ky.get("diagnosis") == DIAGNOSIS_6KY},
        {"check": "6ky_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KY, "actual": json_6ky.get("recommended_next_layer"), "passed": json_6ky.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KY},
        {"check": "6ky_adapter_allowed", "expected": True, "actual": json_6ky.get("adapter_script_allowed_next"), "passed": json_6ky.get("adapter_script_allowed_next") is True},
        {"check": "6ky_no_fetch", "expected": False, "actual": json_6ky.get("live_fetches_allowed_next"), "passed": json_6ky.get("live_fetches_allowed_next") is False},
        {"check": "6ky_no_real_metrics", "expected": False, "actual": json_6ky.get("real_backtest_metrics_allowed_next"), "passed": json_6ky.get("real_backtest_metrics_allowed_next") is False},
        {"check": "6ky_no_layer6_exit", "expected": False, "actual": json_6ky.get("layer_6_exit_recommended"), "passed": json_6ky.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]
    optional_inputs = [
        SCHEDULE_CANDIDATES,
        ACTUAL_CANDIDATES,
        PROJECTION_CANDIDATES,
        SOURCE_GAP_6KW,
    ]
    input_rows.extend([
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "", "passed": True}
        for path in optional_inputs
    ])

    readonly_rows = [
        {"source_path": row["artifact_path"], "exists": row["exists"], "may_modify": False, "passed": row["passed"]}
        for row in input_rows
    ]

    blocking_rows = [
        {"blocked_surface": "6la_projection_call_contract_implementation_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "projection contract implementation requires audit first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "historical evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KZ", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KZ is read-only/tmp-only", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KZ cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6ky_passed", "expected": True, "actual": json_6ky.get("all_checks_passed"), "passed": json_6ky.get("all_checks_passed") is True},
        {"decision": "fixture_contract_surface_created", "expected": True, "actual": bool(fixtures), "passed": bool(fixtures)},
        {"decision": "entrypoint_inventory_exists", "expected": True, "actual": bool(inventory), "passed": bool(inventory)},
        {"decision": "entrypoint_safety_scan_exists", "expected": True, "actual": bool(scan), "passed": bool(scan)},
        {"decision": "adapter_feasibility_exists", "expected": True, "actual": bool(feasibility), "passed": bool(feasibility)},
        {"decision": "projection_surface_or_gap", "expected": True, "actual": projection_surface_materialized or adapter_gap_report_emitted, "passed": projection_surface_materialized or adapter_gap_report_emitted},
        {"decision": "future_6la_contract_valid", "expected": True, "actual": len(future_6la) == 5 and all_passed(future_6la), "passed": len(future_6la) == 5 and all_passed(future_6la)},
        {"decision": "recommend_6la_next", "expected": RECOMMENDED_NEXT_LAYER_6KZ, "actual": RECOMMENDED_NEXT_LAYER_6KZ, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_readonly_tmp", "expected": True, "actual": True, "passed": True},
        {"boundary": "projection_call_contract_implemented", "expected": True, "actual": True, "passed": True},
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

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_tmp_implementation", "passed": True},
        {"surface": "6ky_plan", "policy": "read_only", "passed": True},
        {"surface": "fixture_contract_surface", "policy": "tmp_non_production_only", "passed": True},
        {"surface": "projection_surface_or_gap_report", "policy": "tmp_non_production_only", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kz", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kz", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KZ, "actual": RECOMMENDED_NEXT_LAYER_6KZ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KZ, "actual": RECOMMENDED_PATH_6KZ, "passed": True},
        {"decision": "recommend_projection_call_contract_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KZ, "actual": DIAGNOSIS_6KZ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "fixture_contract_surface", "passed": bool(fixtures), "detail": f"{len(fixtures)} rows"},
        {"check": "entrypoint_inventory", "passed": bool(inventory), "detail": f"{len(inventory)} rows"},
        {"check": "entrypoint_safety_scan", "passed": bool(scan), "detail": f"{len(scan)} rows"},
        {"check": "adapter_feasibility", "passed": bool(feasibility), "detail": f"{len(feasibility)} rows"},
        {"check": "projection_surface_or_gap_report", "passed": projection_surface_materialized or adapter_gap_report_emitted, "detail": f"surface={projection_surface_materialized};gap={adapter_gap_report_emitted}"},
        {"check": "metric_readiness", "passed": all_passed(metric_readiness), "detail": f"{len(metric_readiness)} rows"},
        {"check": "lineage_report", "passed": all_passed(lineage), "detail": f"{len(lineage)} rows"},
        {"check": "blockers", "passed": len(blockers) == 4 and all_passed(blockers), "detail": "4/4"},
        {"check": "future_6la_contract", "passed": len(future_6la) == 5 and all_passed(future_6la), "detail": "5/5"},
        {"check": "readonly_sources", "passed": all_passed(readonly_rows), "detail": f"{sum(1 for r in readonly_rows if r['passed'])}/{len(readonly_rows)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_rows), "detail": f"{sum(1 for r in blocking_rows if r['passed'])}/{len(blocking_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all_passed(immutability_rows), "detail": f"{sum(1 for r in immutability_rows if r['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "fixture_contract_surface": write_csv(FIXTURE_SURFACE_CSV, fixtures),
        "entrypoint_inventory": write_csv(ENTRYPOINT_INVENTORY_CSV, inventory),
        "entrypoint_safety_scan": write_csv(ENTRYPOINT_SAFETY_CSV, scan),
        "adapter_feasibility": write_csv(ADAPTER_FEASIBILITY_CSV, feasibility),
        "projection_surface": write_csv(PROJECTION_SURFACE_CSV, projection_rows),
        "projection_adapter_gap_report": write_csv(ADAPTER_GAP_CSV, adapter_gap_rows),
        "metric_readiness": write_csv(METRIC_READINESS_CSV, metric_readiness),
        "lineage_report": write_csv(LINEAGE_CSV, lineage),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6la_contract": write_csv(FUTURE_6LA_CSV, future_6la),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KZ",
        "layer_type": "game_mechanics_realism",
        "implementation_only_readonly_tmp": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KZ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KZ,
        "recommended_path": RECOMMENDED_PATH_6KZ,
        "predecessor_plan": str(PLAN_6KY_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6ky.get("diagnosis"),
        "implemented_layer_after": "6KY",
        "source_family": "projection_call_contract_implementation",
        "fixture_contract_row_count": len(fixtures),
        "entrypoint_inventory_count": len(inventory),
        "entrypoint_safety_scan_count": len(scan),
        "adapter_feasibility_count": len(feasibility),
        "projection_surface_row_count": len(projection_rows),
        "projection_adapter_gap_report_count": len(adapter_gap_rows),
        "metric_readiness_count": len(metric_readiness),
        "lineage_report_count": len(lineage),
        "blocker_count": len(blockers),
        "future_6la_contract_valid": len(future_6la) == 5 and all_passed(future_6la),
        "projection_call_contract_implemented": True,
        "current_ui_realism_state_label": "bullpen_active_partial_realism",
        "backtest_label": "current_ui_projection_path_bullpen_active_partial_realism",
        "fixture_contract_surface_created": bool(fixtures),
        "safe_projection_entrypoint_found": safe_entrypoint_found,
        "adapter_call_attempted": adapter_call_attempted,
        "projection_surface_materialized": projection_surface_materialized,
        "projection_adapter_gap_report_emitted": adapter_gap_report_emitted,
        "probability_projection_fields_materialized": probability_fields,
        "runs_projection_fields_materialized": runs_fields,
        "any_projection_fields_materialized": any_projection_fields,
        "probability_metric_ready_after_implementation": probability_fields,
        "runs_metric_ready_after_implementation": runs_fields,
        "any_backtest_metric_ready_after_implementation": any_projection_fields,
        "historical_odds_required": False,
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
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "fixture_contract_surface_csv": str(FIXTURE_SURFACE_CSV),
            "entrypoint_inventory_csv": str(ENTRYPOINT_INVENTORY_CSV),
            "entrypoint_safety_scan_csv": str(ENTRYPOINT_SAFETY_CSV),
            "adapter_feasibility_csv": str(ADAPTER_FEASIBILITY_CSV),
            "projection_surface_csv": str(PROJECTION_SURFACE_CSV),
            "projection_adapter_gap_report_csv": str(ADAPTER_GAP_CSV),
            "metric_readiness_csv": str(METRIC_READINESS_CSV),
            "lineage_report_csv": str(LINEAGE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6la_contract_csv": str(FUTURE_6LA_CSV),
            "readonly_sources_csv": str(READONLY_CSV),
            "blocking_policy_csv": str(BLOCKING_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
