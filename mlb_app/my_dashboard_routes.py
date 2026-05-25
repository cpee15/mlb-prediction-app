from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from .active_lineup_solver import build_active_lineup_solver_payload
from .database import create_tables, get_engine, get_session
from .my_dashboard_context_cache import install_dashboard_context_cache
from .my_dashboard_solver import build_dashboard_solver_payload, normalize_filter_payload, SUPPORTED_COMPONENTS
from .shared_payload_cache import env_ttl, get_or_set, make_cache_key, stable_hash

router = APIRouter()


class MyDashboardSolverRequest(BaseModel):
    date: Optional[str] = None
    component: str
    filters: Optional[Dict[str, Any]] = None


class MyDashboardBatchSolverRequest(BaseModel):
    date: Optional[str] = None
    components: Optional[List[str]] = None
    filters_by_component: Optional[Dict[str, Dict[str, Any]]] = None
    active_lineups: bool = False


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


@router.post("/my-dashboard/solver/batch")
def my_dashboard_solver_batch_post(payload: MyDashboardBatchSolverRequest) -> Dict[str, Any]:
    return _run_batch_solver(
        date=payload.date,
        components=payload.components,
        filters_by_component=payload.filters_by_component,
        active_lineups=payload.active_lineups,
    )


@router.post("/my-dashboard/solver/active-lineups")
def my_dashboard_active_lineup_solver_post(payload: MyDashboardSolverRequest) -> Dict[str, Any]:
    return _run_active_lineup_solver(date=payload.date, component=payload.component, filters=payload.filters)


def _normalize_request(date: Optional[str], component: str, filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    target_date = (date or dt.date.today().isoformat())[:10]
    try:
        dt.date.fromisoformat(target_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {date}") from exc
    return {
        "target_date": target_date,
        "component": (component or "").strip().lower(),
        "filters": normalize_filter_payload(filters),
    }


def _run_solver(date: Optional[str], component: str, filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    install_dashboard_context_cache()
    normalized = _normalize_request(date, component, filters)
    target_date = normalized["target_date"]
    normalized_component = normalized["component"]
    normalized_filters = normalized["filters"]
    filters_hash = stable_hash(normalized_filters)
    cache_key = make_cache_key("dashboard_solver", "component", target_date, normalized_component, filters_hash)

    try:
        def build() -> Dict[str, Any]:
            factory = session_factory()
            with factory() as session:
                return build_dashboard_solver_payload(
                    session=session,
                    date=target_date,
                    component=normalized_component,
                    filters=normalized_filters,
                )

        return get_or_set(cache_key, env_ttl("DASHBOARD_SOLVER_CACHE_TTL_SECONDS"), build)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "My Dashboard solver failed", "error": str(exc)}) from exc


def _run_active_lineup_solver(date: Optional[str], component: str, filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    install_dashboard_context_cache()
    normalized = _normalize_request(date, component, filters)
    target_date = normalized["target_date"]
    normalized_component = normalized["component"]
    normalized_filters = normalized["filters"]
    filters_hash = stable_hash(normalized_filters)
    cache_key = make_cache_key("dashboard_solver", "active_lineups", target_date, normalized_component, filters_hash)

    try:
        def build() -> Dict[str, Any]:
            factory = session_factory()
            with factory() as session:
                return build_active_lineup_solver_payload(
                    session=session,
                    date=target_date,
                    component=normalized_component,
                    filters=normalized_filters,
                )

        return get_or_set(cache_key, env_ttl("DASHBOARD_SOLVER_CACHE_TTL_SECONDS"), build)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Active-lineup solver failed", "error": str(exc)}) from exc


def _run_batch_solver(
    date: Optional[str],
    components: Optional[List[str]],
    filters_by_component: Optional[Dict[str, Dict[str, Any]]],
    active_lineups: bool = False,
) -> Dict[str, Any]:
    install_dashboard_context_cache()
    target_date = (date or dt.date.today().isoformat())[:10]
    try:
        dt.date.fromisoformat(target_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {date}") from exc

    requested_components = [str(c or "").strip().lower() for c in (components or sorted(SUPPORTED_COMPONENTS))]
    requested_components = [c for c in requested_components if c]
    invalid = [c for c in requested_components if c not in SUPPORTED_COMPONENTS]
    if invalid:
        raise HTTPException(status_code=400, detail={"message": "Unsupported dashboard component(s)", "components": invalid, "supported_components": sorted(SUPPORTED_COMPONENTS)})

    normalized_filters_by_component = {
        component: normalize_filter_payload((filters_by_component or {}).get(component))
        for component in requested_components
    }
    filters_hash = stable_hash(normalized_filters_by_component)
    cache_key = make_cache_key(
        "dashboard_solver",
        "batch_active_lineups" if active_lineups else "batch",
        target_date,
        ",".join(requested_components),
        filters_hash,
    )

    try:
        def build() -> Dict[str, Any]:
            results: Dict[str, Any] = {}
            factory = session_factory()
            with factory() as session:
                for component in requested_components:
                    filters = normalized_filters_by_component.get(component) or {}
                    if active_lineups:
                        results[component] = build_active_lineup_solver_payload(
                            session=session,
                            date=target_date,
                            component=component,
                            filters=filters,
                        )
                    else:
                        results[component] = build_dashboard_solver_payload(
                            session=session,
                            date=target_date,
                            component=component,
                            filters=filters,
                        )
            return {
                "date": target_date,
                "components": requested_components,
                "active_lineups": active_lineups,
                "results": results,
            }

        return get_or_set(cache_key, env_ttl("DASHBOARD_SOLVER_CACHE_TTL_SECONDS"), build)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "My Dashboard batch solver failed", "error": str(exc)}) from exc