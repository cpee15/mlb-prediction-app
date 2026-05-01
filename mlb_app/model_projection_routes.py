from __future__ import annotations

import datetime
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

from .database import create_tables, get_engine, get_session
from .model_projections import build_model_projection_payload

router = APIRouter()


def _session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


@router.get("/models/projections")
def model_projections(date: Optional[str] = None) -> Dict[str, Any]:
    target_date = date or datetime.date.today().isoformat()
    try:
        session_factory = _session_factory()
        with session_factory() as session:
            return build_model_projection_payload(session, target_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Failed to build model projections", "error": str(exc)}) from exc
