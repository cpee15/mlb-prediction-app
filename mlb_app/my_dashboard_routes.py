from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import my_dashboard_solver as dashboard_solver
from .active_lineup_solver import build_active_lineup_solver_payload
from .database import create_tables, get_engine, get_session
from .my_dashboard_context_cache import install_dashboard_context_cache
from .my_dashboard_report_query import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    apply_report_query,
    install_full_result_finalizer,
)
from .shared_payload_cache import env_ttl, get_or_set, make_cache_key, stable_hash

install_full_result_finalizer(dashboard_solver)

router = APIRouter()
SUPPORTED_COMPONENTS = dashboard_solver.SUPPORTED_COMPONENTS


class MyDashboardSolverRequest(BaseModel):
    date: Optional[str] = None
    component: str
    filters: Optional[Dict[str, Any]] = None
    page_size: int = DEFAULT_PAGE_SIZE
    page_number: int = 1
    sort_by: str = "score"
    sort_direction: str = "desc"
    include_metadata: bool = True


class MyDashboardBatchSolverRequest(BaseModel):
    date: Optional[str] = None
    components: Optional[List[str]] = None
    filters_by_component: Optional[Dict[str, Dict[str, Any]]] = None
    active_lineups: bool = False


class MyDashboardHydrateRequest(BaseModel):
    date: Optional[str] = None
    components: Optional[List[str]] = None
    active_lineups: bool = True
    force: bool = False


def session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


def _yesterday_iso() -> str:
    return (dt.date.today() - dt.timedelta(days=1)).isoformat()


@router.get("/my-dashboard/health")
def my_dashboard_health() -> Dict[str, Any]:
    return {
        "name": "My Dashboard",
        "status": "ok",
        "auth_required": False,
        "persistence": "frontend_localStorage_v1",
        "supported_components": sorted(SUPPORTED_COMPONENTS),
        "query_contract": {
            "style": "salesforce_inspired_query_and_describe",
            "default_page_size": DEFAULT_PAGE_SIZE,
            "maximum_page_size": MAX_PAGE_SIZE,
            "full_candidate_universe": True,
            "final_top_ten_cap": False,
            "fields": ["totalSize", "done", "records", "page_info", "object_info"],
        },
        "hydration": {
            "cron_target": "/my-dashboard/solver/hydrate-yesterday",
            "default_lineup_source": "yesterday_confirmed_1_9",
        },
    }


@router.get("/my-dashboard/solver")
def my_dashboard_solver_get(
    date: Optional[str] = Query(default=None),
    component: str = Query(default="hitters"),
    page_size: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    page_number: int = Query(default=1, ge=1),
    sort_by: str = Query(default="score"),
    sort_direction: str = Query(default="desc"),
    include_metadata: bool = Query(default=True),
) -> Dict[str, Any]:
    return _run_solver(
        date=date,
        component=component,
        filters=None,
        page_size=page_size,
        page_number=page_number,
        sort_by=sort_by,
        sort_direction=sort_direction,
        include_metadata=include_metadata,
    )


@router.post("/my-dashboard/solver")
def my_dashboard_solver_post(payload: MyDashboardSolverRequest) -> Dict[str, Any]:
    return _run_solver(
        date=payload.date,
        component=payload.component,
        filters=payload.filters,
        page_size=payload.page_size,
        page_number=payload.page_number,
        sort_by=payload.sort_by,
        sort_direction=payload.sort_direction,
        include_metadata=payload.include_metadata,
    )


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
    return _run_active_lineup_solver(
        date=payload.date,
        component=payload.component,
        filters=payload.filters,
        page_size=payload.page_size,
        page_number=payload.page_number,
        sort_by=payload.sort_by,
        sort_direction=payload.sort_direction,
        include_metadata=payload.include_metadata,
    )


@router.post("/my-dashboard/solver/hydrate-yesterday")
def my_dashboard_hydrate_yesterday_post(payload: Optional[MyDashboardHydrateRequest] = None) -> Dict[str, Any]:
    request = payload or MyDashboardHydrateRequest()
    target_date = request.date or _yesterday_iso()
    return _run_hydration(
        date=target_date,
        components=request.components,
        active_lineups=request.active_lineups,
        force=request.force,
    )


@router.get("/my-dashboard/solver/hydrate-yesterday")
def my_dashboard_hydrate_yesterday_get(
    date: Optional[str] = Query(default=None),
    active_lineups: bool = Query(default=True),
    force: bool = Query(default=False),
) -> Dict[str, Any]:
    return _run_hydration(
        date=date or _yesterday_iso(),
        components=None,
        active_lineups=active_lineups,
        force=force,
    )


def _normalize_request(date: Optional[str], component: str, filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    target_date = (date or dt.date.today().isoformat())[:10]
    try:
        dt.date.fromisoformat(target_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {date}") from exc
    normalized_component = (component or "").strip().lower()
    if normalized_component not in SUPPORTED_COMPONENTS:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Unsupported dashboard component",
                "component": normalized_component,
                "supported_components": sorted(SUPPORTED_COMPONENTS),
            },
        )
    return {
        "target_date": target_date,
        "component": normalized_component,
        "filters": dashboard_solver.normalize_filter_payload(filters),
    }


def _normalize_component_list(components: Optional[List[str]]) -> List[str]:
    requested_components = [str(c or "").strip().lower() for c in (components or sorted(SUPPORTED_COMPONENTS))]
    requested_components = [c for c in requested_components if c]
    invalid = [c for c in requested_components if c not in SUPPORTED_COMPONENTS]
    if invalid:
        raise HTTPException(status_code=400, detail={"message": "Unsupported dashboard component(s)", "components": invalid, "supported_components": sorted(SUPPORTED_COMPONENTS)})
    return requested_components


def _query_response(
    payload: Dict[str, Any],
    component: str,
    page_size: int,
    page_number: int,
    sort_by: str,
    sort_direction: str,
    include_metadata: bool,
) -> Dict[str, Any]:
    return apply_report_query(
        payload=payload,
        component=component,
        page_size=page_size,
        page_number=page_number,
        sort_by=sort_by,
        sort_direction=sort_direction,
        include_metadata=include_metadata,
    )


def _run_solver(
    date: Optional[str],
    component: str,
    filters: Optional[Dict[str, Any]],
    page_size: int = DEFAULT_PAGE_SIZE,
    page_number: int = 1,
    sort_by: str = "score",
    sort_direction: str = "desc",
    include_metadata: bool = True,
) -> Dict[str, Any]:
    install_dashboard_context_cache()
    normalized = _normalize_request(date, component, filters)
    target_date = normalized["target_date"]
    normalized_component = normalized["component"]
    normalized_filters = normalized["filters"]
    filters_hash = stable_hash(normalized_filters)
    cache_key = make_cache_key("dashboard_solver", "component_full_result", target_date, normalized_component, filters_hash)

    try:
        def build() -> Dict[str, Any]:
            factory = session_factory()
            with factory() as session:
                return dashboard_solver.build_dashboard_solver_payload(
                    session=session,
                    date=target_date,
                    component=normalized_component,
                    filters=normalized_filters,
                )

        full_payload = get_or_set(cache_key, env_ttl("DASHBOARD_SOLVER_CACHE_TTL_SECONDS"), build)
        return _query_response(full_payload, normalized_component, page_size, page_number, sort_by, sort_direction, include_metadata)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "My Dashboard solver failed", "error": str(exc)}) from exc


def _run_active_lineup_solver(
    date: Optional[str],
    component: str,
    filters: Optional[Dict[str, Any]],
    page_size: int = DEFAULT_PAGE_SIZE,
    page_number: int = 1,
    sort_by: str = "score",
    sort_direction: str = "desc",
    include_metadata: bool = True,
) -> Dict[str, Any]:
    install_dashboard_context_cache()
    normalized = _normalize_request(date, component, filters)
    target_date = normalized["target_date"]
    normalized_component = normalized["component"]
    normalized_filters = normalized["filters"]
    filters_hash = stable_hash(normalized_filters)
    cache_key = make_cache_key("dashboard_solver", "active_lineups_full_result", target_date, normalized_component, filters_hash)

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

        full_payload = get_or_set(cache_key, env_ttl("DASHBOARD_SOLVER_CACHE_TTL_SECONDS"), build)
        return _query_response(full_payload, normalized_component, page_size, page_number, sort_by, sort_direction, include_metadata)
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

    requested_components = _normalize_component_list(components)
    normalized_filters_by_component = {
        component: dashboard_solver.normalize_filter_payload((filters_by_component or {}).get(component))
        for component in requested_components
    }
    filters_hash = stable_hash(normalized_filters_by_component)
    cache_key = make_cache_key(
        "dashboard_solver",
        "batch_active_lineups_full_result" if active_lineups else "batch_full_result",
        target_date,
        ",".join(requested_components),
        filters_hash,
    )

    try:
        def build() -> Dict[str, Any]:
            return _build_batch_payload(
                target_date=target_date,
                requested_components=requested_components,
                normalized_filters_by_component=normalized_filters_by_component,
                active_lineups=active_lineups,
                hydration_mode=False,
            )

        return get_or_set(cache_key, env_ttl("DASHBOARD_SOLVER_CACHE_TTL_SECONDS"), build)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "My Dashboard batch solver failed", "error": str(exc)}) from exc


def _run_hydration(
    date: str,
    components: Optional[List[str]],
    active_lineups: bool = True,
    force: bool = False,
) -> Dict[str, Any]:
    install_dashboard_context_cache()
    target_date = (date or _yesterday_iso())[:10]
    try:
        dt.date.fromisoformat(target_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {date}") from exc

    requested_components = _normalize_component_list(components)
    normalized_filters_by_component = {component: {} for component in requested_components}
    cache_key = make_cache_key(
        "dashboard_solver",
        "morning_hydration_active_lineups_full_result" if active_lineups else "morning_hydration_full_result",
        target_date,
        ",".join(requested_components),
    )

    def build() -> Dict[str, Any]:
        hydrated = _build_batch_payload(
            target_date=target_date,
            requested_components=requested_components,
            normalized_filters_by_component=normalized_filters_by_component,
            active_lineups=active_lineups,
            hydration_mode=True,
        )
        hydrated.update({
            "hydration_status": "hydrated",
            "hydration_target": "yesterday_confirmed_1_9" if active_lineups else "standard_dashboard_solver",
            "hydrated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "force_requested": force,
        })
        return hydrated

    try:
        if force:
            return build()
        return get_or_set(cache_key, env_ttl("DASHBOARD_SOLVER_CACHE_TTL_SECONDS"), build)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "My Dashboard hydration failed", "error": str(exc)}) from exc


def _build_batch_payload(
    target_date: str,
    requested_components: List[str],
    normalized_filters_by_component: Dict[str, Dict[str, Any]],
    active_lineups: bool,
    hydration_mode: bool,
) -> Dict[str, Any]:
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
                results[component] = dashboard_solver.build_dashboard_solver_payload(
                    session=session,
                    date=target_date,
                    component=component,
                    filters=filters,
                )
    return {
        "date": target_date,
        "components": requested_components,
        "active_lineups": active_lineups,
        "hydration_mode": hydration_mode,
        "lineup_source_policy": "confirmed_1_9_lineups" if active_lineups else "not_lineup_filtered",
        "results": results,
    }
