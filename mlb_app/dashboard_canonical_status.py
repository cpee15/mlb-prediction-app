"""Read-only production status for the canonical My Dashboard object model."""

from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, Optional

from sqlalchemy import func

from .dashboard_object_models import DashboardPlayer, DashboardPlayerCurrent, DashboardPlayerSnapshot, DashboardProjectionRun
from .dashboard_player_population import active_player_window_days
from .database import BatterPitchTypeMatchup


COVERAGE_FIELDS = (
    "model_score", "confidence", "xwoba", "xba", "exit_velocity", "launch_angle",
    "hard_hit_rate", "barrel_rate", "strikeout_rate", "walk_rate", "iso", "obp",
    "slg", "plate_appearances",
)


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, (dt.date, dt.datetime)) else value


def _ratio(value: int, total: int) -> float:
    return round(value / total, 4) if total else 0.0


def _run_payload(run: Optional[DashboardProjectionRun]) -> Optional[Dict[str, Any]]:
    if run is None:
        return None
    return {
        "id": run.id,
        "run_type": run.run_type,
        "target_date": _iso(run.target_date),
        "status": run.status,
        "started_at": _iso(run.started_at),
        "completed_at": _iso(run.completed_at),
        "canonical_count": run.canonical_count,
        "active_count": run.active_count,
        "current_count": run.current_count,
        "snapshot_count": run.snapshot_count,
        "projection_version": run.projection_version,
        "error_type": run.error_type,
        "error_message": run.error_message,
    }


def _field_coverage(session: Any, player_type: str) -> Dict[str, Any]:
    base = session.query(DashboardPlayerCurrent).filter(
        DashboardPlayerCurrent.is_active.is_(True),
        DashboardPlayerCurrent.player_type == player_type,
    )
    total = base.count()
    fields: Dict[str, Any] = {}
    for name in COVERAGE_FIELDS:
        count = base.filter(getattr(DashboardPlayerCurrent, name).isnot(None)).count()
        fields[name] = {"non_null_count": count, "coverage": _ratio(count, total)}
    return {"row_count": total, "fields": fields}


def canonical_dashboard_status(
    session: Any,
    *,
    now: Optional[dt.datetime] = None,
    stale_after_hours: Optional[int] = None,
) -> Dict[str, Any]:
    current_time = now or dt.datetime.utcnow()
    stale_hours = stale_after_hours or max(1, int(os.getenv("DASHBOARD_PROJECTION_STALE_HOURS", "36")))

    canonical_count = session.query(DashboardPlayer).count()
    resolved_count = session.query(DashboardPlayer).filter(DashboardPlayer.identity_resolution_status == "resolved").count()
    unresolved_count = canonical_count - resolved_count
    active_count = session.query(DashboardPlayer).filter(
        DashboardPlayer.is_active.is_(True),
        DashboardPlayer.identity_resolution_status == "resolved",
    ).count()
    active_hitters = session.query(DashboardPlayer).filter(
        DashboardPlayer.is_active.is_(True),
        DashboardPlayer.identity_resolution_status == "resolved",
        DashboardPlayer.player_type == "hitter",
    ).count()
    active_pitchers = session.query(DashboardPlayer).filter(
        DashboardPlayer.is_active.is_(True),
        DashboardPlayer.identity_resolution_status == "resolved",
        DashboardPlayer.player_type == "pitcher",
    ).count()
    current_count = session.query(DashboardPlayerCurrent).filter(DashboardPlayerCurrent.is_active.is_(True)).count()
    current_hitters = session.query(DashboardPlayerCurrent).filter(
        DashboardPlayerCurrent.is_active.is_(True), DashboardPlayerCurrent.player_type == "hitter"
    ).count()
    current_pitchers = session.query(DashboardPlayerCurrent).filter(
        DashboardPlayerCurrent.is_active.is_(True), DashboardPlayerCurrent.player_type == "pitcher"
    ).count()
    snapshot_count = session.query(DashboardPlayerSnapshot).count()
    approved_snapshot_count = session.query(DashboardPlayerSnapshot).filter(DashboardPlayerSnapshot.is_approved.is_(True)).count()
    snapshot_dates = [value for (value,) in session.query(DashboardPlayerSnapshot.snapshot_date).distinct().order_by(DashboardPlayerSnapshot.snapshot_date).all()]

    last_promoted_at, last_updated_at = session.query(
        func.max(DashboardPlayerCurrent.promoted_at),
        func.max(DashboardPlayerCurrent.updated_at),
    ).one()
    freshness_anchor = last_updated_at or last_promoted_at
    age_hours = round((current_time - freshness_anchor).total_seconds() / 3600, 2) if freshness_anchor else None
    stale = freshness_anchor is None or age_hours > stale_hours
    versions = sorted(value for (value,) in session.query(DashboardPlayerCurrent.projection_version).distinct().all() if value)

    latest_success = session.query(DashboardProjectionRun).filter(
        DashboardProjectionRun.status == "success"
    ).order_by(DashboardProjectionRun.completed_at.desc(), DashboardProjectionRun.id.desc()).first()
    latest_failure = session.query(DashboardProjectionRun).filter(
        DashboardProjectionRun.status.in_(("failed", "partial"))
    ).order_by(DashboardProjectionRun.completed_at.desc(), DashboardProjectionRun.id.desc()).first()

    lineup_players = session.query(DashboardPlayer).filter(DashboardPlayer.lineup_appearance_count > 0).count()
    lineup_appearances = session.query(func.coalesce(func.sum(DashboardPlayer.lineup_appearance_count), 0)).scalar() or 0
    arsenal_rows = session.query(BatterPitchTypeMatchup).join(
        DashboardPlayer, DashboardPlayer.mlb_player_id == BatterPitchTypeMatchup.batter_id
    ).filter(DashboardPlayer.is_active.is_(True), DashboardPlayer.player_type == "hitter").count()

    issues = []
    if canonical_count == 0:
        issues.append("canonical_population_empty")
    if active_count == 0:
        issues.append("active_population_empty")
    if current_count == 0:
        issues.append("current_projection_empty")
    if current_count != active_count or current_hitters != active_hitters or current_pitchers != active_pitchers:
        issues.append("current_projection_population_mismatch")
    if snapshot_count == 0:
        issues.append("historical_snapshots_empty")
    if latest_success is None:
        issues.append("successful_refresh_evidence_missing")
    if stale:
        issues.append("current_projection_stale")
    readiness = "ready" if not issues else "not_ready" if any(item.endswith("_empty") or item.endswith("_mismatch") for item in issues) else "degraded"

    return {
        "status": readiness,
        "issues": issues,
        "query_source": "dashboard_player_current",
        "activity_window_days": active_player_window_days(),
        "stale_after_hours": stale_hours,
        "population": {
            "canonical_count": canonical_count,
            "resolved_count": resolved_count,
            "unresolved_count": unresolved_count,
            "active_count": active_count,
            "active_hitter_count": active_hitters,
            "active_pitcher_count": active_pitchers,
        },
        "current_projection": {
            "row_count": current_count,
            "hitter_count": current_hitters,
            "pitcher_count": current_pitchers,
            "projection_versions": versions,
            "last_promoted_at": _iso(last_promoted_at),
            "last_updated_at": _iso(last_updated_at),
            "age_hours": age_hours,
            "stale": stale,
        },
        "snapshots": {
            "row_count": snapshot_count,
            "approved_count": approved_snapshot_count,
            "date_count": len(snapshot_dates),
            "earliest_date": _iso(snapshot_dates[0]) if snapshot_dates else None,
            "latest_date": _iso(snapshot_dates[-1]) if snapshot_dates else None,
        },
        "field_coverage": {
            "hitters": _field_coverage(session, "hitter"),
            "pitchers": _field_coverage(session, "pitcher"),
        },
        "related_reports": {
            "players_with_lineup_history": lineup_players,
            "confirmed_lineup_appearance_count": int(lineup_appearances),
            "active_hitter_arsenal_split_rows": arsenal_rows,
        },
        "refresh_runs": {
            "latest_success": _run_payload(latest_success),
            "latest_failure": _run_payload(latest_failure),
        },
        "generated_at": current_time.isoformat(),
    }
