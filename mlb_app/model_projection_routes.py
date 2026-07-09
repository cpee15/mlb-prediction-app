from __future__ import annotations

import datetime
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

from .database import create_tables, get_engine, get_session
from .model_projection_probability import build_model_projection_probability
from .model_projections import build_model_projection_payload
from .model_tracker_routes import router as model_tracker_router
from .performance import estimate_payload_bytes, record_probability_source, record_span, timing_span
from .shared_payload_cache import env_ttl, get_or_set, make_cache_key
from mlb_app.simulation.game_simulation_builder import build_game_simulation as build_shared_game_simulation

router = APIRouter()
router.include_router(model_tracker_router)


def _session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


def _apply_projection_probability_contract(payload: Dict[str, Any], target_date: str) -> Dict[str, Any]:
    """Expose Model Projections as the displayed/default probability source.

    Existing canonical/debug objects stay available for compatibility, but
    top-level probability aliases now resolve from Model Projections shared
    derived simulation output when available.
    """
    games = payload.get("games") if isinstance(payload.get("games"), list) else []
    for game in games:
        if not isinstance(game, dict):
            continue
        probability = build_model_projection_probability(
            game_pk=game.get("game_pk"),
            date=game.get("game_date") or target_date,
            shared_simulation=game.get("sharedSimulation"),
            matchup=game,
            generated_at=payload.get("generated_at"),
        )
        game["model_projection_probability"] = probability
        game["probability"] = probability
        game["home_win_probability"] = probability.get("home_win_probability")
        game["away_win_probability"] = probability.get("away_win_probability")
        game["home_win_prob"] = probability.get("home_win_prob")
        game["away_win_prob"] = probability.get("away_win_prob")
        game["probability_source"] = probability.get("source")
        game["probability_source_path"] = probability.get("source_path")
        game["probability_is_fallback"] = probability.get("is_fallback")

        workspace = game.get("workspace")
        if isinstance(workspace, dict):
            workspace["modelProjectionProbability"] = probability
            diagnostics = workspace.get("sharedSimulationDiagnostics")
            if isinstance(diagnostics, dict):
                diagnostics["displayed_probability_source"] = probability.get("source")
                diagnostics["displayed_probability_source_path"] = probability.get("source_path")
                diagnostics["displayed_probability_is_fallback"] = probability.get("is_fallback")

        main = game.get("main_matchup_probabilities")
        if isinstance(main, dict):
            main["displayed_probability_source"] = probability.get("source")
            main["displayed_probability_source_path"] = probability.get("source_path")
            main["displayed_probability_is_fallback"] = probability.get("is_fallback")
            main["model_projection_probability"] = probability

    notes = list(payload.get("source_notes") or [])
    notes = [note for note in notes if "home_win_prob and away_win_prob are canonical v2" not in str(note)]
    notes = [note for note in notes if "Simulation outputs remain available as diagnostics and do not define final side probability" not in str(note)]
    notes.append("Displayed/default home_win_prob and away_win_prob are resolved from Model Projections sharedSimulation derived outputs when available.")
    notes.append("Canonical matchup probability remains available as a compatibility/fallback diagnostic, not the displayed Model Projections source of truth.")
    payload["source_notes"] = notes
    payload["probability_contract"] = "model_projection_probability_v1"
    payload["probability_source"] = "model_projections"
    return payload


def _build_uncached_projection_payload(target_date: str) -> Dict[str, Any]:
    with timing_span("model_projection.session_factory", category="db", route="/models/projections", date=target_date):
        session_factory = _session_factory()
    with session_factory() as session:
        with timing_span(
            "build_model_projection_payload",
            category="projection",
            route="/models/projections",
            date=target_date,
            probability_source="model_projections",
        ):
            payload = build_model_projection_payload(session, target_date)
    payload = _apply_projection_probability_contract(payload, target_date)
    record_probability_source("model_projections")
    record_span(
        "model_projection.payload_bytes",
        category="serialization",
        route="/models/projections",
        date=target_date,
        probability_source="model_projections",
        payload_bytes=estimate_payload_bytes(payload),
    )
    return payload


@router.get("/models/projections")
def model_projections(date: Optional[str] = None) -> Dict[str, Any]:
    target_date = date or datetime.date.today().isoformat()
    cache_key = make_cache_key("model_projection", "full", "probability_contract_v1", target_date)
    try:
        with timing_span(
            "route.models.projections.resolve",
            category="route",
            route="/models/projections",
            date=target_date,
            probability_source="model_projections",
        ):
            payload = get_or_set(
                cache_key,
                env_ttl("MODEL_PROJECTION_CACHE_TTL_SECONDS"),
                lambda: _build_uncached_projection_payload(target_date),
            )
        record_probability_source("model_projections")
        record_span(
            "route.models.projections.payload_bytes",
            category="serialization",
            route="/models/projections",
            date=target_date,
            probability_source="model_projections",
            payload_bytes=estimate_payload_bytes(payload),
        )
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Failed to build model projections", "error": str(exc)}) from exc
