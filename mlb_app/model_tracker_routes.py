from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from .best_plays_engine import build_best_plays_payload, normalize_best_play_rows
from .database import create_tables, get_engine, get_session
from .model_tracker import (
    build_tracker_snapshot,
    list_tracker_rows,
    refresh_tracker_results,
    upsert_tracker_rows,
)

router = APIRouter(prefix="/model-tracker", tags=["model-tracker"])


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


def _append_best_plays_snapshot(session, target: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Add Best Plays rows to the existing additive tracker snapshot response."""
    try:
        best_plays_payload = build_best_plays_payload(session, target)
        best_play_rows = normalize_best_play_rows(best_plays_payload, target)
        best_play_upsert = upsert_tracker_rows(session, best_play_rows)
        payload["rows_collected"] = int(payload.get("rows_collected") or 0) + len(best_play_rows)
        payload.setdefault("upsert", {})["best_plays"] = best_play_upsert
        payload["best_plays"] = {
            "rows_collected": len(best_play_rows),
            "summary": best_plays_payload.get("summary") or {},
            "errors": best_plays_payload.get("errors") or [],
        }
        if best_plays_payload.get("errors"):
            payload.setdefault("errors", []).extend(
                {"source": "best_plays", **error} if isinstance(error, dict) else {"source": "best_plays", "error": str(error)}
                for error in best_plays_payload.get("errors") or []
            )
        return payload
    except Exception as exc:
        payload.setdefault("errors", []).append({"source": "best_plays", "error": str(exc), "type": exc.__class__.__name__})
        return payload


@router.get("/health")
def model_tracker_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "component": "model_tracker",
        "persistence": "model_tracker_snapshots",
        "safe_mode": "additive_snapshot_layer",
    }


@router.post("/snapshot")
def model_tracker_snapshot(date: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    target = _target_date(date)
    try:
        Session = _session_factory()
        with Session() as session:
            payload = build_tracker_snapshot(session, target)
            return _append_best_plays_snapshot(session, target, payload)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Model Tracker snapshot failed", "error": str(exc)}) from exc


@router.get("")
def model_tracker_list(date: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    target = _target_date(date)
    try:
        Session = _session_factory()
        with Session() as session:
            return list_tracker_rows(session, target)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Model Tracker list failed", "error": str(exc)}) from exc


@router.get("/game/{game_pk}")
def model_tracker_game(game_pk: int, date: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    target = _target_date(date)
    try:
        Session = _session_factory()
        with Session() as session:
            payload = list_tracker_rows(session, target, game_pk=game_pk)
            payload["game_pk"] = game_pk
            return payload
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Model Tracker game lookup failed", "error": str(exc)}) from exc


@router.post("/results/refresh")
def model_tracker_results_refresh(date: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    target = _target_date(date)
    try:
        Session = _session_factory()
        with Session() as session:
            return refresh_tracker_results(session, target)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Model Tracker result refresh failed", "error": str(exc)}) from exc
