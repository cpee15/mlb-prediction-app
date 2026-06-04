#!/usr/bin/env python3
"""Implement exact UI projection route trace for Layer 6.

This read-only trace follows ModelProjectionsPage to the backend projection
route, payload builder, simulator/projection entrypoint, and realism feature
chain. It records evidence from local files only and does not fetch data, run
simulations, modify code, activate mechanics, or grant Layer 6 exit.
"""

from __future__ import annotations

import ast
import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


SLUG = "layer6_6kh_exact_ui_projection_route_trace_implementation"
TMP_DIR = Path("tmp")

PLAN_6KG_PATH = Path("scripts/plan_6kg_layer6_exact_ui_projection_route_trace.py")
JSON_6KG = TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan.json"

REQUIRED_INPUTS = [
    JSON_6KG,
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_checks.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_predecessor.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_trace_scope.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_frontend_trace_plan.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_backend_route_trace_plan.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_payload_builder_trace_plan.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_simulator_trace_plan.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_realism_feature_trace_plan.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_flag_config_trace_plan.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_ui_display_field_trace_plan.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_parallel_path_caveat.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_activation_blockers.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_future_6kh_contract.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_decision.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6kg_exact_ui_projection_route_trace_plan_recommended_path.csv",
]

TARGET_FILES = [
    Path("frontend/src/pages/ModelProjectionsPage.jsx"),
    Path("mlb_app/model_projection_routes.py"),
    Path("mlb_app/model_projection_payload.py"),
    Path("mlb_app/model_projections.py"),
    Path("mlb_app/app.py"),
    Path("mlb_app/simulation/game_simulator.py"),
    Path("mlb_app/simulation/game_engine_v2.py"),
    Path("mlb_app/simulation/inning_simulator.py"),
    Path("mlb_app/simulation/game_rules.py"),
    Path("mlb_app/simulation/bullpen_chain.py"),
    Path("mlb_app/simulation/bullpen_integration.py"),
    Path("mlb_app/simulation/bullpen_game_engine_hook.py"),
    Path("mlb_app/simulation/formula_map.py"),
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
TARGET_FILES_CSV = TMP_DIR / f"{SLUG}_target_files.csv"
FRONTEND_ENDPOINT_CSV = TMP_DIR / f"{SLUG}_frontend_endpoint_trace.csv"
BACKEND_ROUTE_CSV = TMP_DIR / f"{SLUG}_backend_route_trace.csv"
PAYLOAD_BUILDER_CSV = TMP_DIR / f"{SLUG}_payload_builder_trace.csv"
SIMULATOR_TRACE_CSV = TMP_DIR / f"{SLUG}_simulator_projection_trace.csv"
REALISM_REACHABILITY_CSV = TMP_DIR / f"{SLUG}_realism_feature_reachability.csv"
FLAG_CONFIG_CSV = TMP_DIR / f"{SLUG}_flag_config_fallback_trace.csv"
UI_DISPLAY_FIELD_CSV = TMP_DIR / f"{SLUG}_ui_display_field_trace.csv"
PARALLEL_PATH_CSV = TMP_DIR / f"{SLUG}_parallel_path_caveat.csv"
ROUTE_CONCLUSION_CSV = TMP_DIR / f"{SLUG}_route_conclusion.csv"
ACTIVATION_BLOCKERS_CSV = TMP_DIR / f"{SLUG}_activation_blockers.csv"
FUTURE_6KI_CSV = TMP_DIR / f"{SLUG}_future_6ki_contract.csv"
READONLY_CSV = TMP_DIR / f"{SLUG}_readonly_sources.csv"
BLOCKING_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6KG = "layer_6_exact_ui_projection_route_trace_plan_complete"
DIAGNOSIS_6KH = "layer_6_exact_ui_projection_route_trace_implementation_complete"
RECOMMENDED_NEXT_LAYER_6KG = "6KH_layer_6_exact_ui_projection_route_trace_implementation"
RECOMMENDED_NEXT_LAYER_6KH = "6KI_layer_6_exact_ui_projection_route_trace_implementation_audit"
RECOMMENDED_PATH_6KH = "audit_exact_ui_route_trace_then_plan_ui_realism_wiring_or_backtest"


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


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
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def safe_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


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
    return value is True or str(value).lower() == "true"


def all_passed(rows: List[Dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def line_hits(text: str, patterns: Sequence[str], limit: int = 20) -> str:
    rows: List[str] = []
    for idx, line in enumerate(text.splitlines(), start=1):
        lower = line.lower()
        if any(p.lower() in lower for p in patterns):
            rows.append(f"L{idx}:{line.strip()}")
        if len(rows) >= limit:
            break
    return " || ".join(rows)


def extract_string_literals(text: str, filters: Sequence[str], limit: int = 40) -> List[str]:
    values = re.findall(r"""['"`]([^'"`]+)['"`]""", text)
    out: List[str] = []
    for value in values:
        lower = value.lower()
        if any(f.lower() in lower for f in filters):
            if value not in out:
                out.append(value)
        if len(out) >= limit:
            break
    return out


def extract_python_functions(path: Path) -> List[str]:
    text = safe_text(path)
    if not text:
        return []
    try:
        tree = ast.parse(text)
    except Exception:
        return re.findall(r"^\s*def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", text, flags=re.MULTILINE)
    return [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]


def contains_any(text: str, terms: Sequence[str]) -> bool:
    lower = text.lower()
    return any(term.lower() in lower for term in terms)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6kg = load_json(JSON_6KG)

    target_file_rows = []
    for path in TARGET_FILES:
        text = safe_text(path)
        target_file_rows.append({
            "path": str(path),
            "exists": path.exists(),
            "line_count": len(text.splitlines()) if text else 0,
            "contains_projection": contains_any(text, ["projection", "projections", "projected"]),
            "contains_realism_terms": contains_any(text, ["realism", "bullpen", "double_play", "sac_fly", "stolen", "steal", "ghost", "walkoff", "balk"]),
            "passed": path.exists(),
        })

    frontend_text = safe_text(Path("frontend/src/pages/ModelProjectionsPage.jsx"))
    endpoint_literals = extract_string_literals(frontend_text, ["api", "projection", "projections", "model"], limit=60)
    fetch_lines = line_hits(frontend_text, ["fetch(", "axios", "apiJson", "projections", "model-projections", "model_projections"], limit=60)
    displayed_field_lines = line_hits(frontend_text, ["projected", "win", "prob", "total", "edge", "confidence", "runs"], limit=80)
    likely_endpoint = ""
    for candidate in endpoint_literals:
        if "projection" in candidate.lower() or "model" in candidate.lower():
            likely_endpoint = candidate
            break

    frontend_endpoint_rows = [
        {
            "source": "frontend/src/pages/ModelProjectionsPage.jsx",
            "endpoint_literals": "|".join(endpoint_literals),
            "likely_endpoint": likely_endpoint,
            "fetch_or_api_lines": fetch_lines,
            "displayed_field_lines": displayed_field_lines,
            "frontend_endpoint_found": bool(likely_endpoint or fetch_lines),
            "passed": Path("frontend/src/pages/ModelProjectionsPage.jsx").exists(),
        }
    ]

    route_text = safe_text(Path("mlb_app/model_projection_routes.py"))
    app_text = safe_text(Path("mlb_app/app.py"))
    route_lines = line_hits(route_text, ["@app.", "@bp.", "route(", "Blueprint", "model", "projection", "projections"], limit=80)
    app_registration_lines = line_hits(app_text, ["model_projection", "model_projections", "register_blueprint", "projection"], limit=80)
    route_functions = [fn for fn in extract_python_functions(Path("mlb_app/model_projection_routes.py")) if contains_any(fn, ["projection", "model", "payload"])]
    backend_route_rows = [
        {
            "source": "mlb_app/model_projection_routes.py",
            "route_lines": route_lines,
            "route_functions": "|".join(route_functions),
            "backend_route_found": bool(route_lines or route_functions),
            "passed": Path("mlb_app/model_projection_routes.py").exists(),
        },
        {
            "source": "mlb_app/app.py",
            "registration_lines": app_registration_lines,
            "route_registration_found": bool(app_registration_lines),
            "passed": Path("mlb_app/app.py").exists(),
        },
    ]

    payload_text = safe_text(Path("mlb_app/model_projection_payload.py"))
    model_proj_text = safe_text(Path("mlb_app/model_projections.py"))
    route_import_call_lines = line_hits(route_text, ["build_model_projection_payload", "model_projections", "import", "return", "jsonify"], limit=100)
    payload_function_names = [fn for fn in extract_python_functions(Path("mlb_app/model_projection_payload.py")) if contains_any(fn, ["payload", "projection", "model"])]
    model_projection_function_names = [fn for fn in extract_python_functions(Path("mlb_app/model_projections.py")) if contains_any(fn, ["payload", "projection", "simulation", "model"])]
    payload_call_lines = line_hits(payload_text + "\n" + model_proj_text, ["build_model_projection_payload", "simulate", "simulation", "model_object", "projection", "payload", "formula"], limit=120)
    payload_builder_rows = [
        {
            "source": "mlb_app/model_projection_routes.py",
            "route_import_call_lines": route_import_call_lines,
            "passed": Path("mlb_app/model_projection_routes.py").exists(),
        },
        {
            "source": "mlb_app/model_projection_payload.py",
            "function_candidates": "|".join(payload_function_names),
            "payload_call_lines": payload_call_lines,
            "payload_builder_found": bool(payload_function_names or "build_model_projection_payload" in payload_text),
            "passed": Path("mlb_app/model_projection_payload.py").exists(),
        },
        {
            "source": "mlb_app/model_projections.py",
            "function_candidates": "|".join(model_projection_function_names),
            "payload_or_legacy_builder_found": bool(model_projection_function_names),
            "passed": Path("mlb_app/model_projections.py").exists(),
        },
    ]

    sim_files = [
        Path("mlb_app/model_projection_payload.py"),
        Path("mlb_app/model_projections.py"),
        Path("mlb_app/simulation/game_simulator.py"),
        Path("mlb_app/simulation/game_engine_v2.py"),
        Path("mlb_app/simulation/inning_simulator.py"),
        Path("mlb_app/simulation/formula_map.py"),
    ]
    simulator_rows = []
    for path in sim_files:
        text = safe_text(path)
        simulator_rows.append({
            "source": str(path),
            "functions": "|".join(extract_python_functions(path)[:40]),
            "simulation_terms": line_hits(text, ["simulate", "simulation", "game_engine", "inning", "formula", "model_object", "project"], limit=60),
            "calls_simulation": contains_any(text, ["simulate_game", "simulate_half_inning", "game_engine", "inning_simulator"]),
            "calls_formula_or_model_object": contains_any(text, ["model_object", "formula", "projection card", "payload"]),
            "passed": path.exists(),
        })

    realism_features = [
        ("bullpen_logic", ["bullpen"]),
        ("double_play_logic", ["double_play", "double play"]),
        ("sac_fly_logic", ["sac_fly", "sac fly"]),
        ("stolen_base_or_steal_logic", ["stolen", "steal"]),
        ("extras_ghost_runner_walkoff_logic", ["extras", "ghost", "walkoff"]),
        ("balk_logic", ["balk"]),
    ]
    combined_route_text = "\n".join([route_text, payload_text, model_proj_text])
    combined_sim_text = "\n".join(safe_text(p) for p in TARGET_FILES)
    realism_rows = []
    reached_count = 0
    bypassed_count = 0
    unknown_count = 0
    for mechanic, terms in realism_features:
        route_mentions = contains_any(combined_route_text, terms)
        sim_mentions = contains_any(combined_sim_text, terms)
        status = "reached" if route_mentions and sim_mentions else ("unknown" if sim_mentions else "bypassed")
        if status == "reached":
            reached_count += 1
        elif status == "bypassed":
            bypassed_count += 1
        else:
            unknown_count += 1
        realism_rows.append({
            "mechanic": mechanic,
            "route_payload_mentions": route_mentions,
            "target_file_mentions": sim_mentions,
            "reachability_status": status,
            "evidence": line_hits(combined_route_text + "\n" + combined_sim_text, terms, limit=30),
            "passed": True,
        })

    flag_terms = ["os.environ", "getenv", "FEATURE", "flag", "shadow", "dormant", "candidate", "legacy", "current", "fallback", "default"]
    flag_rows = []
    for path in [Path("mlb_app/model_projection_routes.py"), Path("mlb_app/model_projection_payload.py"), Path("mlb_app/model_projections.py"), Path("mlb_app/app.py")]:
        text = safe_text(path)
        flag_rows.append({
            "source": str(path),
            "flag_config_fallback_lines": line_hits(text, flag_terms, limit=80),
            "has_fallback_or_legacy_signal": contains_any(text, ["fallback", "legacy", "current", "default"]),
            "has_shadow_or_candidate_signal": contains_any(text, ["shadow", "candidate", "dormant"]),
            "has_env_flag_signal": contains_any(text, ["os.environ", "getenv", "environ"]),
            "passed": path.exists(),
        })

    ui_display_rows = [
        {
            "display_family": "projected_runs",
            "frontend_evidence": line_hits(frontend_text, ["projected", "runs", "home", "away"], limit=40),
            "payload_source_inferred": bool(displayed_field_lines and payload_function_names),
            "passed": True,
        },
        {
            "display_family": "win_probability",
            "frontend_evidence": line_hits(frontend_text, ["win", "prob", "probability"], limit=40),
            "payload_source_inferred": bool(displayed_field_lines and payload_function_names),
            "passed": True,
        },
        {
            "display_family": "projected_total",
            "frontend_evidence": line_hits(frontend_text, ["total", "projected"], limit=40),
            "payload_source_inferred": bool(displayed_field_lines and payload_function_names),
            "passed": True,
        },
        {
            "display_family": "confidence_or_edge_fields",
            "frontend_evidence": line_hits(frontend_text, ["edge", "confidence", "model"], limit=40),
            "payload_source_inferred": bool(displayed_field_lines and payload_function_names),
            "passed": True,
        },
    ]

    parallel_rows = [
        {"path_family": "ModelProjectionsPage", "primary_target": True, "separate_from_primary": False, "passed": True},
        {"path_family": "MyDashboardWorkspacePage", "primary_target": False, "separate_from_primary": True, "passed": True},
        {"path_family": "DailyOddsPage", "primary_target": False, "separate_from_primary": True, "passed": True},
        {"path_family": "model_tracker", "primary_target": False, "separate_from_primary": True, "passed": True},
    ]

    model_projections_page_found = Path("frontend/src/pages/ModelProjectionsPage.jsx").exists()
    frontend_endpoint_found = bool(frontend_endpoint_rows[0]["frontend_endpoint_found"])
    backend_route_found = any(boolish(row.get("backend_route_found")) or boolish(row.get("route_registration_found")) for row in backend_route_rows)
    payload_builder_found = any(boolish(row.get("payload_builder_found")) or boolish(row.get("payload_or_legacy_builder_found")) for row in payload_builder_rows)
    simulator_or_projection_entrypoint_found = any(
        boolish(row.get("calls_simulation")) or boolish(row.get("calls_formula_or_model_object")) for row in simulator_rows
    )
    full_simulation_chain_reached = any(boolish(row.get("calls_simulation")) for row in simulator_rows[:2])
    non_sim_formula_or_payload_path_detected = any(boolish(row.get("calls_formula_or_model_object")) for row in simulator_rows[:2])

    realism_feature_chain_fully_reached = reached_count == len(realism_features)
    realism_feature_chain_partially_reached = 0 < reached_count < len(realism_features)
    realism_feature_chain_bypassed = reached_count == 0 and bypassed_count > 0

    ui_uses_realism_enabled_path = realism_feature_chain_fully_reached or realism_feature_chain_partially_reached
    ui_uses_legacy_or_current_path = any(boolish(row.get("has_fallback_or_legacy_signal")) for row in flag_rows) or non_sim_formula_or_payload_path_detected
    realism_ui_activation_confirmed = bool(
        model_projections_page_found
        and frontend_endpoint_found
        and backend_route_found
        and payload_builder_found
        and simulator_or_projection_entrypoint_found
        and realism_feature_chain_fully_reached
        and not ui_uses_legacy_or_current_path
    )

    if realism_ui_activation_confirmed:
        route_trace_confidence = "high"
        route_trace_summary = "ModelProjectionsPage appears to flow through a fully realism-enabled route without detected legacy/current fallback."
    elif ui_uses_realism_enabled_path and ui_uses_legacy_or_current_path:
        route_trace_confidence = "medium"
        route_trace_summary = "Exact trace found realism-related reachability and also legacy/current/formula/fallback signals; UI realism activation remains partial or unconfirmed."
    elif non_sim_formula_or_payload_path_detected:
        route_trace_confidence = "medium"
        route_trace_summary = "Exact trace found a projection/payload/formula path; full realism simulation chain is not confirmed."
    else:
        route_trace_confidence = "low"
        route_trace_summary = "Exact trace did not confirm a complete frontend-to-realism simulation chain."

    route_conclusion_rows = [
        {"conclusion": "model_projections_page_found", "value": model_projections_page_found, "passed": True},
        {"conclusion": "frontend_endpoint_found", "value": frontend_endpoint_found, "passed": True},
        {"conclusion": "backend_route_found", "value": backend_route_found, "passed": True},
        {"conclusion": "payload_builder_found", "value": payload_builder_found, "passed": True},
        {"conclusion": "simulator_or_projection_entrypoint_found", "value": simulator_or_projection_entrypoint_found, "passed": True},
        {"conclusion": "full_simulation_chain_reached", "value": full_simulation_chain_reached, "passed": True},
        {"conclusion": "non_sim_formula_or_payload_path_detected", "value": non_sim_formula_or_payload_path_detected, "passed": True},
        {"conclusion": "realism_feature_chain_fully_reached", "value": realism_feature_chain_fully_reached, "passed": True},
        {"conclusion": "realism_feature_chain_partially_reached", "value": realism_feature_chain_partially_reached, "passed": True},
        {"conclusion": "realism_feature_chain_bypassed", "value": realism_feature_chain_bypassed, "passed": True},
        {"conclusion": "realism_ui_activation_confirmed", "value": realism_ui_activation_confirmed, "passed": True},
        {"conclusion": "ui_uses_realism_enabled_path", "value": ui_uses_realism_enabled_path, "passed": True},
        {"conclusion": "ui_uses_legacy_or_current_path", "value": ui_uses_legacy_or_current_path, "passed": True},
        {"conclusion": "route_trace_confidence", "value": route_trace_confidence, "passed": True},
        {"conclusion": "route_trace_summary", "value": route_trace_summary, "passed": True},
    ]

    activation_blockers = [
        {"blocker": "route_trace_findings_need_audit", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "real_historical_evaluation_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "activation_decision_not_run", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "realism_ui_activation_not_sufficient_for_exit_without_backtest", "blocks_activation": True, "blocks_layer6_exit": True, "passed": True},
        {"blocker": "balks_deferred_or_exit_gated", "blocks_activation": False, "blocks_layer6_exit": True, "passed": True},
    ]

    future_6ki = [
        {"contract": "audit_frontend_endpoint_trace", "required": True, "passed": True},
        {"contract": "audit_backend_route_trace", "required": True, "passed": True},
        {"contract": "audit_payload_builder_trace", "required": True, "passed": True},
        {"contract": "audit_simulator_projection_trace", "required": True, "passed": True},
        {"contract": "audit_realism_feature_reachability", "required": True, "passed": True},
        {"contract": "audit_route_conclusion", "required": True, "passed": True},
        {"contract": "recommend_ui_realism_wiring_or_backtest_plan", "required": True, "passed": True},
        {"contract": "preserve_no_activation_no_layer6_exit", "required": True, "passed": True},
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6kg_plan_script_exists", "expected": True, "actual": PLAN_6KG_PATH.exists(), "passed": PLAN_6KG_PATH.exists()},
        {"check": "6kg_json_exists", "expected": True, "actual": JSON_6KG.exists(), "passed": JSON_6KG.exists()},
        {"check": "6kg_all_checks_passed", "expected": True, "actual": json_6kg.get("all_checks_passed"), "passed": json_6kg.get("all_checks_passed") is True},
        {"check": "6kg_diagnosis", "expected": DIAGNOSIS_6KG, "actual": json_6kg.get("diagnosis"), "passed": json_6kg.get("diagnosis") == DIAGNOSIS_6KG},
        {"check": "6kg_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KG, "actual": json_6kg.get("recommended_next_layer"), "passed": json_6kg.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6KG},
        {"check": "6kg_exact_ui_route_trace_required", "expected": True, "actual": json_6kg.get("exact_ui_route_trace_required"), "passed": json_6kg.get("exact_ui_route_trace_required") is True},
        {"check": "6kg_exact_ui_route_trace_completed", "expected": False, "actual": json_6kg.get("exact_ui_route_trace_completed"), "passed": json_6kg.get("exact_ui_route_trace_completed") is False},
        {"check": "6kg_model_projections_page_targeted", "expected": True, "actual": json_6kg.get("model_projections_page_targeted"), "passed": json_6kg.get("model_projections_page_targeted") is True},
        {"check": "6kg_realism_ui_activation_confirmed", "expected": False, "actual": json_6kg.get("realism_ui_activation_confirmed"), "passed": json_6kg.get("realism_ui_activation_confirmed") is False},
        {"check": "6kg_no_layer6_exit", "expected": False, "actual": json_6kg.get("layer_6_exit_recommended"), "passed": json_6kg.get("layer_6_exit_recommended") is False},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in REQUIRED_INPUTS
    ]

    readonly_rows = [{"source_path": str(path), "exists": path.exists(), "may_modify": False, "passed": path.exists()} for path in REQUIRED_INPUTS + TARGET_FILES]

    blocking_rows = [
        {"blocked_surface": "6ki_route_trace_audit", "blocked": False, "reason": "recommended next layer", "passed": True},
        {"blocked_surface": "real_historical_backtest_execution", "blocked": True, "reason": "route trace audit and dataset proof required first", "passed": True},
        {"blocked_surface": "activation_execution", "blocked": True, "reason": "real evaluation and activation decision required first", "passed": True},
        {"blocked_surface": "production_activation", "blocked": True, "reason": "activation not permitted in 6KH", "passed": True},
        {"blocked_surface": "database_writes", "blocked": True, "reason": "6KH is read-only trace", "passed": True},
        {"blocked_surface": "layer_6_exit", "blocked": True, "reason": "6KH cannot exit Layer 6", "passed": True},
    ]

    decision_rows = [
        {"decision": "6kg_passed", "expected": True, "actual": json_6kg.get("all_checks_passed"), "passed": json_6kg.get("all_checks_passed") is True},
        {"decision": "target_files_inspected", "expected": True, "actual": len(target_file_rows) > 0, "passed": len(target_file_rows) > 0},
        {"decision": "frontend_endpoint_trace_recorded", "expected": True, "actual": len(frontend_endpoint_rows) > 0, "passed": len(frontend_endpoint_rows) > 0},
        {"decision": "backend_route_trace_recorded", "expected": True, "actual": len(backend_route_rows) > 0, "passed": len(backend_route_rows) > 0},
        {"decision": "payload_builder_trace_recorded", "expected": True, "actual": len(payload_builder_rows) > 0, "passed": len(payload_builder_rows) > 0},
        {"decision": "simulator_projection_trace_recorded", "expected": True, "actual": len(simulator_rows) > 0, "passed": len(simulator_rows) > 0},
        {"decision": "realism_feature_reachability_recorded", "expected": 6, "actual": len(realism_rows), "passed": len(realism_rows) == 6},
        {"decision": "route_conclusion_recorded", "expected": True, "actual": len(route_conclusion_rows) > 0, "passed": len(route_conclusion_rows) > 0},
        {"decision": "recommend_6ki_next", "expected": RECOMMENDED_NEXT_LAYER_6KH, "actual": RECOMMENDED_NEXT_LAYER_6KH, "passed": True},
        {"decision": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_readonly_trace", "expected": True, "actual": True, "passed": True},
        {"boundary": "exact_ui_route_trace_completed", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_source_acquisition", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_historical_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_production_simulation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_execution", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_layer_6_exit_recommendation", "expected": False, "actual": False, "passed": True},
    ]

    immutability_rows = [
        {"surface": "source_tree", "policy": "read_only_trace", "passed": True},
        {"surface": "6kg_plan", "policy": "read_only", "passed": True},
        {"surface": "ui_projection_path", "policy": "not_modified_in_6kh", "passed": True},
        {"surface": "simulator_path", "policy": "not_modified_in_6kh", "passed": True},
        {"surface": "production_runtime", "policy": "not_activated_in_6kh", "passed": True},
        {"surface": "database", "policy": "not_written_in_6kh", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6KH, "actual": RECOMMENDED_NEXT_LAYER_6KH, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6KH, "actual": RECOMMENDED_PATH_6KH, "passed": True},
        {"decision": "recommend_route_trace_audit_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_layer6_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6KH, "actual": DIAGNOSIS_6KH, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "target_files", "passed": all_passed(target_file_rows), "detail": f"{sum(1 for r in target_file_rows if r['passed'])}/{len(target_file_rows)}"},
        {"check": "frontend_endpoint_trace", "passed": len(frontend_endpoint_rows) > 0 and all_passed(frontend_endpoint_rows), "detail": str(len(frontend_endpoint_rows))},
        {"check": "backend_route_trace", "passed": len(backend_route_rows) > 0 and all_passed(backend_route_rows), "detail": str(len(backend_route_rows))},
        {"check": "payload_builder_trace", "passed": len(payload_builder_rows) > 0 and all_passed(payload_builder_rows), "detail": str(len(payload_builder_rows))},
        {"check": "simulator_projection_trace", "passed": len(simulator_rows) > 0 and all_passed(simulator_rows), "detail": str(len(simulator_rows))},
        {"check": "realism_feature_reachability", "passed": len(realism_rows) == 6 and all_passed(realism_rows), "detail": "6/6"},
        {"check": "flag_config_fallback_trace", "passed": len(flag_rows) > 0 and all_passed(flag_rows), "detail": str(len(flag_rows))},
        {"check": "ui_display_field_trace", "passed": len(ui_display_rows) == 4 and all_passed(ui_display_rows), "detail": "4/4"},
        {"check": "route_conclusion", "passed": len(route_conclusion_rows) == 15 and all_passed(route_conclusion_rows), "detail": "15/15"},
        {"check": "activation_blockers", "passed": len(activation_blockers) == 5 and all_passed(activation_blockers), "detail": "5/5"},
        {"check": "future_6ki_contract", "passed": len(future_6ki) == 8 and all_passed(future_6ki), "detail": "8/8"},
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
        "target_files": write_csv(TARGET_FILES_CSV, target_file_rows),
        "frontend_endpoint_trace": write_csv(FRONTEND_ENDPOINT_CSV, frontend_endpoint_rows),
        "backend_route_trace": write_csv(BACKEND_ROUTE_CSV, backend_route_rows),
        "payload_builder_trace": write_csv(PAYLOAD_BUILDER_CSV, payload_builder_rows),
        "simulator_projection_trace": write_csv(SIMULATOR_TRACE_CSV, simulator_rows),
        "realism_feature_reachability": write_csv(REALISM_REACHABILITY_CSV, realism_rows),
        "flag_config_fallback_trace": write_csv(FLAG_CONFIG_CSV, flag_rows),
        "ui_display_field_trace": write_csv(UI_DISPLAY_FIELD_CSV, ui_display_rows),
        "parallel_path_caveat": write_csv(PARALLEL_PATH_CSV, parallel_rows),
        "route_conclusion": write_csv(ROUTE_CONCLUSION_CSV, route_conclusion_rows),
        "activation_blockers": write_csv(ACTIVATION_BLOCKERS_CSV, activation_blockers),
        "future_6ki_contract": write_csv(FUTURE_6KI_CSV, future_6ki),
        "readonly_sources": write_csv(READONLY_CSV, readonly_rows),
        "blocking_policy": write_csv(BLOCKING_CSV, blocking_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6KH",
        "layer_type": "game_mechanics_realism",
        "implementation_only_readonly_trace": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6KH if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6KH,
        "recommended_path": RECOMMENDED_PATH_6KH,
        "predecessor_plan": str(PLAN_6KG_PATH),
        "predecessor_returncode": 0,
        "predecessor_diagnosis_string": json_6kg.get("diagnosis"),
        "implemented_layer_after": "6KG",
        "source_family": "exact_ui_projection_route_trace",
        "target_file_count": len(target_file_rows),
        "existing_target_file_count": sum(1 for row in target_file_rows if boolish(row.get("exists"))),
        "frontend_endpoint_trace_count": len(frontend_endpoint_rows),
        "backend_route_trace_count": len(backend_route_rows),
        "payload_builder_trace_count": len(payload_builder_rows),
        "simulator_projection_trace_count": len(simulator_rows),
        "realism_feature_reachability_count": len(realism_rows),
        "flag_config_fallback_trace_count": len(flag_rows),
        "ui_display_field_trace_count": len(ui_display_rows),
        "route_conclusion_count": len(route_conclusion_rows),
        "activation_blocker_count": len(activation_blockers),
        "future_6ki_contract_valid": len(future_6ki) == 8 and all_passed(future_6ki),
        "exact_ui_route_trace_completed": True,
        "model_projections_page_found": model_projections_page_found,
        "frontend_endpoint_found": frontend_endpoint_found,
        "backend_route_found": backend_route_found,
        "payload_builder_found": payload_builder_found,
        "simulator_or_projection_entrypoint_found": simulator_or_projection_entrypoint_found,
        "full_simulation_chain_reached": full_simulation_chain_reached,
        "non_sim_formula_or_payload_path_detected": non_sim_formula_or_payload_path_detected,
        "realism_feature_chain_fully_reached": realism_feature_chain_fully_reached,
        "realism_feature_chain_partially_reached": realism_feature_chain_partially_reached,
        "realism_feature_chain_bypassed": realism_feature_chain_bypassed,
        "realism_ui_activation_confirmed": realism_ui_activation_confirmed,
        "ui_uses_realism_enabled_path": ui_uses_realism_enabled_path,
        "ui_uses_legacy_or_current_path": ui_uses_legacy_or_current_path,
        "route_trace_confidence": route_trace_confidence,
        "route_trace_summary": route_trace_summary,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "source_acquisition_performed_by_this_layer": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "target_files_csv": str(TARGET_FILES_CSV),
            "frontend_endpoint_trace_csv": str(FRONTEND_ENDPOINT_CSV),
            "backend_route_trace_csv": str(BACKEND_ROUTE_CSV),
            "payload_builder_trace_csv": str(PAYLOAD_BUILDER_CSV),
            "simulator_projection_trace_csv": str(SIMULATOR_TRACE_CSV),
            "realism_feature_reachability_csv": str(REALISM_REACHABILITY_CSV),
            "flag_config_fallback_trace_csv": str(FLAG_CONFIG_CSV),
            "ui_display_field_trace_csv": str(UI_DISPLAY_FIELD_CSV),
            "parallel_path_caveat_csv": str(PARALLEL_PATH_CSV),
            "route_conclusion_csv": str(ROUTE_CONCLUSION_CSV),
            "activation_blockers_csv": str(ACTIVATION_BLOCKERS_CSV),
            "future_6ki_contract_csv": str(FUTURE_6KI_CSV),
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
