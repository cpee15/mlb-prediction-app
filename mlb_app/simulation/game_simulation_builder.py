"""
Shared game simulation builder.

Goal:
    Make /models/projections and /matchup/:game_pk use the same simulation
    source of truth so expected runs, win probabilities, totals, and metadata
    are traceable and consistent.

This file is intentionally defensive because the sandbox engine may expose
slightly different function names while we migrate it into mlb_app/simulation.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


MODEL_VERSION = "shared-simulation-v1"
SOURCE_BUILDER = "mlb_app.simulation.game_simulation_builder"
CALIBRATION_VERSION = "calibration-v1"


def _load_sandbox_engine():
    """
    Temporary bridge to the sandbox engine.

    Long-term target:
        Move sandbox_matchup_engine_full.py into:
            mlb_app/simulation/game_engine_v2.py

        Then replace this loader with:
            from mlb_app.simulation.game_engine_v2 import ...
    """
    try:
        from mlb_app.simulation.game_engine_v2 import run_full_game_simulation as engine
        return engine
    except Exception as exc:
        raise RuntimeError(
            "Could not import sandbox_matchup_engine_full.py. "
            "Make sure it exists in the repo root while we migrate the engine."
        ) from exc


def _call_engine(engine: Any, game_pk: int, config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Try common sandbox engine entrypoints without assuming the exact function name.
    """

    candidate_names = [
        "build_matchup_payload",
        "build_matchup",
        "build_game_payload",
        "run_full_game_simulation",
        "run_game_simulation",
        "simulate_game",
        "main",
    ]

    for name in candidate_names:
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                result = fn(game_pk=game_pk, config=config or {})
            except TypeError:
                try:
                    result = fn(game_pk)
                except TypeError:
                    continue

            if isinstance(result, dict):
                return result

            return {"raw_result": result}

    raise RuntimeError(
        "No supported sandbox engine entrypoint found. "
        "Expected one of: " + ", ".join(candidate_names)
    )


def _normalize_metadata(
    payload: Dict[str, Any],
    *,
    game_pk: int,
    config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Adds the roadmap source-of-truth contract without destroying existing data.
    """

    existing_meta = payload.get("meta") or payload.get("metadata") or {}

    simulation_count = (
        payload.get("simulation_count")
        or existing_meta.get("simulation_count")
        or (config or {}).get("simulation_count")
    )

    seed = (
        payload.get("seed")
        or existing_meta.get("seed")
        or (config or {}).get("seed")
    )

    starter_quality_score = (
        payload.get("starter_quality_score")
        or existing_meta.get("starter_quality_score")
    )

    starter_quality_label = (
        payload.get("starter_quality_label")
        or existing_meta.get("starter_quality_label")
    )

    metadata = {
        **existing_meta,
        "game_pk": game_pk,
        "model_version": existing_meta.get("model_version", MODEL_VERSION),
        "source_builder": existing_meta.get("source_builder", SOURCE_BUILDER),
        "simulation_count": simulation_count,
        "seed": seed,
        "starter_exit_enabled": existing_meta.get("starter_exit_enabled", True),
        "starter_quality_score": starter_quality_score,
        "starter_quality_label": starter_quality_label,
        "calibration_version": existing_meta.get("calibration_version", CALIBRATION_VERSION),
        "offense_source": existing_meta.get("offense_source", payload.get("offense_source")),
        "pitcher_source": existing_meta.get("pitcher_source", payload.get("pitcher_source")),
        "bullpen_source": existing_meta.get("bullpen_source", payload.get("bullpen_source")),
        "environment_source": existing_meta.get("environment_source", payload.get("environment_source")),
    }

    payload["meta"] = metadata
    payload["metadata"] = metadata

    return payload


def build_game_simulation(
    game_pk: int,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    import traceback

    engine = _load_sandbox_engine()

    try:
        payload = engine(int(game_pk), config or {})
        return _normalize_metadata(payload, game_pk=int(game_pk), config=config or {})
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
            "traceback": traceback.format_exc(),  # 🔥 THIS IS THE KEY
        }
