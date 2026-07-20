"""Operator-only orchestration for canonical player population and projection refresh."""

from __future__ import annotations

import datetime as dt
import os
import threading
from typing import Any, Callable, Dict, List, Optional, Sequence

import requests

from .dashboard_object_models import DashboardPlayer, DashboardPlayerCurrent, DashboardPlayerSnapshot, DashboardProjectionRun
from .dashboard_player_population import fetch_active_roster, fetch_confirmed_lineup_players, populate_dashboard_players
from .dashboard_player_projection import backfill_player_projection, refresh_player_projection
from .lineup_data import MLB_STATS_BASE


_AUTO_BOOTSTRAP_LOCK = threading.Lock()
_FALSE_VALUES = {"0", "false", "no", "off"}


def canonical_auto_bootstrap_enabled() -> bool:
    """Allow empty canonical reports to perform one guarded initial population."""

    return os.getenv("DASHBOARD_CANONICAL_AUTO_BOOTSTRAP", "true").strip().lower() not in _FALSE_VALUES


def fetch_active_mlb_teams(
    season: int,
    *,
    request_get: Callable[..., Any] = requests.get,
) -> List[Dict[str, Any]]:
    response = request_get(
        f"{MLB_STATS_BASE}/teams",
        params={"sportId": 1, "season": int(season), "activeStatus": "Y"},
        timeout=20,
    )
    response.raise_for_status()
    teams = []
    for row in response.json().get("teams", []):
        try:
            team_id = int(row.get("id"))
        except (TypeError, ValueError):
            continue
        teams.append({"team_id": team_id, "team_name": row.get("name")})
    if len(teams) < 30:
        raise RuntimeError(f"Verified MLB team source returned only {len(teams)} active teams")
    return teams


def _counts(session: Any) -> Dict[str, int]:
    return {
        "canonical_count": session.query(DashboardPlayer).count(),
        "active_count": session.query(DashboardPlayer).filter(DashboardPlayer.is_active.is_(True)).count(),
        "current_count": session.query(DashboardPlayerCurrent).count(),
        "snapshot_count": session.query(DashboardPlayerSnapshot).count(),
    }


def _begin_run(session: Any, run_type: str, target_date: dt.date, started_at: dt.datetime) -> DashboardProjectionRun:
    run = DashboardProjectionRun(
        run_type=run_type,
        target_date=target_date,
        status="running",
        started_at=started_at,
        **_counts(session),
    )
    session.add(run)
    session.commit()
    return run


def _complete_run(
    session: Any,
    run_id: int,
    *,
    status: str,
    completed_at: dt.datetime,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[Exception] = None,
) -> DashboardProjectionRun:
    run = session.get(DashboardProjectionRun, run_id)
    if run is None:
        raise RuntimeError(f"Projection run audit row disappeared: {run_id}")
    counts = _counts(session)
    for key, value in counts.items():
        setattr(run, key, value)
    run.status = status
    run.completed_at = completed_at
    run.result_json = result or {}
    run.projection_version = (result or {}).get("projection_version")
    if error is not None:
        run.error_type = error.__class__.__name__
        run.error_message = str(error)[:2000]
    session.commit()
    return run


def run_canonical_projection_refresh(
    session: Any,
    *,
    target_date: dt.date,
    request_get: Callable[..., Any] = requests.get,
    matchup_builder: Optional[Callable[..., Any]] = None,
    lineup_fetcher: Optional[Callable[..., Any]] = None,
    transition_missing_players: bool = False,
    now: Optional[dt.datetime] = None,
) -> Dict[str, Any]:
    started_at = now or dt.datetime.utcnow()
    run = _begin_run(session, "canonical_refresh", target_date, started_at)
    try:
        teams = fetch_active_mlb_teams(target_date.year, request_get=request_get)
        roster_rows: List[Dict[str, Any]] = []
        for team in teams:
            roster_rows.extend(fetch_active_roster(
                team["team_id"],
                target_date.year,
                team_name=team["team_name"],
                request_get=request_get,
            ))
        lineup = fetch_confirmed_lineup_players(
            session,
            target_date,
            matchup_builder=matchup_builder,
            lineup_fetcher=lineup_fetcher,
        )
        population = populate_dashboard_players(
            session,
            as_of=target_date,
            lineup_rows=lineup["players"],
            roster_rows=roster_rows,
            transition_missing_players=transition_missing_players,
        )
        projection = refresh_player_projection(session, snapshot_date=target_date, now=started_at)
        result = {
            "target_date": target_date.isoformat(),
            "team_count": len(teams),
            "roster_row_count": len(roster_rows),
            "lineup_player_count": len(lineup["players"]),
            "lineup_unresolved_count": len(lineup["unresolved_identities"]),
            "lineup_error_count": len(lineup["errors"]),
            "population": population,
            "projection": projection,
            **projection,
        }
        _complete_run(session, run.id, status="success", completed_at=dt.datetime.utcnow(), result=result)
        return {**result, "run_id": run.id, "status": "success"}
    except Exception as exc:
        session.rollback()
        _complete_run(session, run.id, status="failed", completed_at=dt.datetime.utcnow(), error=exc)
        raise



def ensure_canonical_projection(
    session: Any,
    *,
    target_date: dt.date,
    refresh: Callable[..., Dict[str, Any]] = run_canonical_projection_refresh,
    now: Optional[dt.datetime] = None,
) -> Dict[str, Any]:
    """Populate an empty canonical projection once, then leave report requests read-only."""

    current_count = session.query(DashboardPlayerCurrent).count()
    if current_count:
        return {"status": "already_available", "current_count": current_count}
    if not canonical_auto_bootstrap_enabled():
        return {"status": "disabled", "current_count": 0}

    checked_at = now or dt.datetime.utcnow()
    with _AUTO_BOOTSTRAP_LOCK:
        session.expire_all()
        current_count = session.query(DashboardPlayerCurrent).count()
        if current_count:
            return {"status": "already_available", "current_count": current_count}

        recent_cutoff = checked_at - dt.timedelta(minutes=15)
        running = (
            session.query(DashboardProjectionRun)
            .filter(
                DashboardProjectionRun.run_type == "canonical_refresh",
                DashboardProjectionRun.status == "running",
                DashboardProjectionRun.started_at >= recent_cutoff,
            )
            .order_by(DashboardProjectionRun.started_at.desc(), DashboardProjectionRun.id.desc())
            .first()
        )
        if running is not None:
            return {"status": "in_progress", "current_count": 0, "run_id": running.id}

        try:
            result = refresh(session, target_date=target_date)
        except Exception as exc:
            # The operator records the full sanitized run evidence. The public
            # query response only receives a safe error type and keeps its
            # established empty-result contract.
            return {
                "status": "failed",
                "current_count": session.query(DashboardPlayerCurrent).count(),
                "error_type": exc.__class__.__name__,
            }

        current_count = session.query(DashboardPlayerCurrent).count()
        return {
            "status": "populated" if current_count else "empty",
            "current_count": current_count,
            "run_id": result.get("run_id"),
            "projection_version": result.get("projection_version"),
        }


def run_projection_backfill(
    session: Any,
    *,
    dates: Sequence[dt.date],
    continue_on_error: bool = False,
    now: Optional[dt.datetime] = None,
) -> Dict[str, Any]:
    target_dates = sorted(set(dates))
    if not target_dates:
        raise ValueError("At least one backfill date is required")
    run = _begin_run(session, "projection_backfill", target_dates[-1], now or dt.datetime.utcnow())
    try:
        result = backfill_player_projection(
            session,
            dates=target_dates,
            continue_on_error=continue_on_error,
        )
        status = "success" if result["failed_date_count"] == 0 else "partial"
        _complete_run(session, run.id, status=status, completed_at=dt.datetime.utcnow(), result=result)
        return {**result, "run_id": run.id, "status": status}
    except Exception as exc:
        session.rollback()
        _complete_run(session, run.id, status="failed", completed_at=dt.datetime.utcnow(), error=exc)
        raise
