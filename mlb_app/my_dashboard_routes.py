from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from .database import create_tables, get_engine, get_session
from .my_dashboard_solver import build_dashboard_solver_payload, SUPPORTED_COMPONENTS

router = APIRouter()


class MyDashboardSolverRequest(BaseModel):
    date: Optional[str] = None
    component: str
    filters: Optional[Dict[str, Any]] = None


def session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


@router.get("/my-dashboard/health")
def my_dashboard_health() -> Dict[str, Any]:
    return {
        "name": "My Dashboard",
        "status": "ok",
        "auth_required": False,
        "persistence": "frontend_localStorage_v1",
        "supported_components": sorted(SUPPORTED_COMPONENTS),
    }


@router.get("/my-dashboard/solver")
def my_dashboard_solver_get(
    date: Optional[str] = Query(default=None),
    component: str = Query(default="hitters"),
) -> Dict[str, Any]:
    return _run_solver(date=date, component=component, filters=None)


@router.post("/my-dashboard/solver")
def my_dashboard_solver_post(payload: MyDashboardSolverRequest) -> Dict[str, Any]:
    return _run_solver(date=payload.date, component=payload.component, filters=payload.filters)


def _run_solver(date: Optional[str], component: str, filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    target_date = (date or dt.date.today().isoformat())[:10]
    try:
        dt.date.fromisoformat(target_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {date}") from exc

    try:
        factory = session_factory()
        with factory() as session:
            return build_dashboard_solver_payload(session=session, date=target_date, component=component, filters=filters)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "My Dashboard solver failed", "error": str(exc)}) from exc
