from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from .best_plays_engine import build_best_plays_payload
from .database import create_tables, get_engine, get_session

router = APIRouter(prefix="/best-plays", tags=["best-plays"])


def _session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


def _target_date(value: Optional[str]) -> str:
    target = (value or dt.date.today().isoformat())[:10]
    try:
        dt.date.fromisoformat(target)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {value}") from exc
    return target


@router.get("")
def best_plays(date: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    target = _target_date(date)
    try:
        Session = _session_factory()
        with Session() as session:
            return build_best_plays_payload(session, target)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Best Plays payload failed", "error": str(exc)}) from exc
