from __future__ import annotations

import datetime
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

from .database import create_tables, get_engine, get_session
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
    cache_key = make_cache_key("model_projection", "full", target_date)
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
