from __future__ import annotations

import csv
import copy
import inspect
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation import game_engine_v2


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")


MODULES_TO_INSPECT = [
    "mlb_app.matchup_generator",
    "mlb_app.simulation.game_engine_v2",
    "mlb_app.simulation.game_simulator",
    "mlb_app.simulation.inning_simulator",
]


def _flatten_keys(obj: Any, prefix: str = "") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    if isinstance(obj, dict):
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            rows.append(
                {
                    "path": path,
                    "key": key,
                    "type": type(value).__name__,
                    "is_dict": isinstance(value, dict),
                    "is_list": isinstance(value, list),
                    "preview": str(value)[:200] if not isinstance(value, (dict, list)) else "",
                }
            )
            rows.extend(_flatten_keys(value, path))

    elif isinstance(obj, list):
        for idx, value in enumerate(obj[:5]):
            path = f"{prefix}[{idx}]"
            rows.append(
                {
                    "path": path,
                    "key": str(idx),
                    "type": type(value).__name__,
                    "is_dict": isinstance(value, dict),
                    "is_list": isinstance(value, list),
                    "preview": str(value)[:200] if not isinstance(value, (dict, list)) else "",
                }
            )
            rows.extend(_flatten_keys(value, path))

    return rows


def _function_rows(module: Any, module_name: str) -> List[Dict[str, Any]]:
    rows = []

    for name, value in inspect.getmembers(module):
        if not inspect.isfunction(value):
            continue

        try:
            signature = str(inspect.signature(value))
        except Exception as exc:
            signature = f"<signature_error: {exc}>"

        rows.append(
            {
                "module": module_name,
                "name": name,
                "signature": signature,
                "is_public": not name.startswith("_"),
                "doc": (inspect.getdoc(value) or "")[:250],
            }
        )

    return rows


def _safe_import_modules() -> Dict[str, Any]:
    imported = {}

    for module_name in MODULES_TO_INSPECT:
        try:
            imported[module_name] = __import__(module_name, fromlist=["*"])
        except Exception as exc:
            imported[module_name] = {"import_error": str(exc)}

    return imported


def _first_matchup(session) -> Dict[str, Any] | None:
    for date_str in [BACKTEST_START]:
        matchups = generate_matchups_for_date(session, date_str)
        if matchups:
            return matchups[0]
    return None


def _candidate_paths(payload_keys: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    paths = [row["path"] for row in payload_keys]

    offense = [
        path for path in paths
        if "offense" in path.lower()
        or "batting_avg" in path.lower()
        or "slugging" in path.lower()
        or path.endswith(".iso")
        or "k_pct" in path.lower()
        or "bb_pct" in path.lower()
    ]

    pitcher = [
        path for path in paths
        if "pitcher" in path.lower()
        or "xwoba" in path.lower()
        or "xba" in path.lower()
        or "hard_hit" in path.lower()
        or "avg_velocity" in path.lower()
    ]

    bullpen = [
        path for path in paths
        if "bullpen" in path.lower()
    ]

    environment = [
        path for path in paths
        if "weather" in path.lower()
        or "environment" in path.lower()
        or "venue" in path.lower()
        or "park" in path.lower()
    ]

    return {
        "offense_input_paths": sorted(set(offense)),
        "pitcher_input_paths": sorted(set(pitcher)),
        "bullpen_input_paths": sorted(set(bullpen)),
        "environment_input_paths": sorted(set(environment)),
    }


def _run_safe_dry_sim(sample_matchup: Dict[str, Any]) -> Dict[str, Any]:
    game_pk = sample_matchup.get("game_pk")
    if not game_pk:
        return {
            "attempted": False,
            "reason": "sample_matchup_missing_game_pk",
            "output_keys": [],
        }

    copied_matchup = copy.deepcopy(sample_matchup)

    try:
        output = game_engine_v2.run_full_game_simulation(
            int(game_pk),
            config={
                "date": sample_matchup.get("game_date"),
                "simulation_count": 10,
                "seed": 42,
                "matchup": {
                    "raw": copied_matchup,
                    "game_date": sample_matchup.get("game_date"),
                },
            },
        )

        return {
            "attempted": True,
            "succeeded": True,
            "reason": None,
            "output_keys": sorted(list(output.keys())) if isinstance(output, dict) else [],
            "status": output.get("status") if isinstance(output, dict) else None,
            "output_preview": str(output)[:500],
        }

    except Exception as exc:
        return {
            "attempted": True,
            "succeeded": False,
            "reason": f"{exc.__class__.__name__}: {exc}",
            "output_keys": [],
        }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    imported = _safe_import_modules()

    function_rows: List[Dict[str, Any]] = []
    modules_inspected = []

    for module_name, module in imported.items():
        modules_inspected.append(
            {
                "module": module_name,
                "imported": not isinstance(module, dict),
                "import_error": module.get("import_error") if isinstance(module, dict) else None,
            }
        )
        if not isinstance(module, dict):
            function_rows.extend(_function_rows(module, module_name))

    engine_signature = str(inspect.signature(game_engine_v2.run_full_game_simulation))

    engine_source = inspect.getsource(game_engine_v2.run_full_game_simulation)
    config_matchup_supported = "config.get(\"matchup\")" in engine_source or "config.get('matchup')" in engine_source
    raw_matchup_supported = "\"raw\"" in engine_source or "'raw'" in engine_source

    engine_builder_candidates = [
        row for row in function_rows
        if row["module"] == "mlb_app.simulation.game_engine_v2"
        and (
            "simulation" in row["name"]
            or "pa_model" in row["name"]
            or "game_sim" in row["name"]
        )
    ]

    matchup_candidates = [
        row for row in function_rows
        if row["module"] == "mlb_app.matchup_generator"
        and (
            "generate" in row["name"]
            or "format" in row["name"]
            or "offense" in row["name"]
            or "pitcher" in row["name"]
        )
    ]

    engine = get_engine(DATABASE_URL)
    create_tables(engine)
    SessionLocal = get_session(engine)

    with SessionLocal() as session:
        sample_matchup = _first_matchup(session)

    payload_keys = _flatten_keys(sample_matchup or {})
    candidate_paths = _candidate_paths(payload_keys)

    dry_run = _run_safe_dry_sim(sample_matchup or {}) if sample_matchup else {
        "attempted": False,
        "reason": "no_sample_matchup",
    }

    safe_injection_candidates = []

    if config_matchup_supported and raw_matchup_supported:
        safe_injection_candidates.append(
            {
                "name": "copy_mutate_matchup_raw_config",
                "description": (
                    "Generate matchup with generate_matchups_for_date, deepcopy it, override "
                    "away_offense_inputs/home_offense_inputs and away_pitcher_features/home_pitcher_features, "
                    "then pass via run_full_game_simulation(game_pk, config={'matchup': {'raw': copied_matchup}})."
                ),
                "risk": "low_audit_only",
            }
        )

    if dry_run.get("succeeded"):
        diagnosis = "profile_injection_seam_available"
        recommended = "Proceed to Layer 4B true profile-injection backtest harness using copied matchup raw config overrides."
    elif config_matchup_supported and raw_matchup_supported:
        diagnosis = "profile_injection_seam_available"
        recommended = "Injection seam exists, but dry-run failed. Inspect dry-run error before Layer 4B."
    else:
        diagnosis = "simulator_entrypoint_unclear"
        recommended = "Codify a simulation entrypoint wrapper before attempting profile injection."

    recommendations = [
        {
            "priority": 1,
            "recommendation": recommended,
            "diagnosis": diagnosis,
        },
        {
            "priority": 2,
            "recommendation": (
                "Layer 4B should override copied matchup keys: away_offense_inputs, home_offense_inputs, "
                "away_pitcher_features, home_pitcher_features. Keep pitch_arsenal/environment unchanged first."
            ),
            "diagnosis": diagnosis,
        },
        {
            "priority": 3,
            "recommendation": (
                "Do not mutate production matchup generation. Always deepcopy sample matchup before candidate profile overrides."
            ),
            "diagnosis": diagnosis,
        },
    ]

    payload = {
        "backtest_start": BACKTEST_START,
        "backtest_end": BACKTEST_END,
        "database_url": DATABASE_URL,
        "modules_inspected": modules_inspected,
        "simulation_entrypoint_candidates": engine_builder_candidates,
        "matchup_payload_generator_candidates": matchup_candidates,
        "backtest_script_candidates": [],
        "model_projection_route_candidates": [],
        "sample_matchup_payload_keys": [row["path"] for row in payload_keys],
        "offense_input_paths": candidate_paths["offense_input_paths"],
        "pitcher_input_paths": candidate_paths["pitcher_input_paths"],
        "bullpen_input_paths": candidate_paths["bullpen_input_paths"],
        "environment_input_paths": candidate_paths["environment_input_paths"],
        "simulator_signature_summary": {
            "run_full_game_simulation": engine_signature,
            "config_matchup_supported": config_matchup_supported,
            "raw_matchup_supported": raw_matchup_supported,
        },
        "existing_override_support": {
            "config_matchup_raw": bool(config_matchup_supported and raw_matchup_supported),
            "direct_profile_override_params": False,
        },
        "safe_injection_point_candidates": safe_injection_candidates,
        "recommended_injection_point": safe_injection_candidates[0]["name"] if safe_injection_candidates else None,
        "dry_run_simulation": dry_run,
        "diagnosis": diagnosis,
        "recommended_next_step": recommended,
    }

    output_dir = Path("tmp")
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"simulation_profile_injection_points_{BACKTEST_START}_to_{BACKTEST_END}"

    json_path = output_dir / f"{prefix}.json"
    functions_csv = output_dir / f"{prefix}_functions.csv"
    payload_keys_csv = output_dir / f"{prefix}_payload_keys.csv"
    recommendations_csv = output_dir / f"{prefix}_recommendations.csv"

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    _write_csv(functions_csv, function_rows)
    _write_csv(payload_keys_csv, payload_keys)
    _write_csv(recommendations_csv, recommendations)

    print(json.dumps(
        {
            "diagnosis": diagnosis,
            "recommended_injection_point": payload["recommended_injection_point"],
            "simulator_signature_summary": payload["simulator_signature_summary"],
            "dry_run_simulation": dry_run,
            "offense_input_paths": payload["offense_input_paths"][:20],
            "pitcher_input_paths": payload["pitcher_input_paths"][:20],
            "recommended_next_step": recommended,
        },
        indent=2,
        sort_keys=True,
        default=str,
    ))

    print(f"Wrote {json_path}")
    print(f"Wrote {functions_csv}")
    print(f"Wrote {payload_keys_csv}")
    print(f"Wrote {recommendations_csv}")


if __name__ == "__main__":
    main()
