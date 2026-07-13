from __future__ import annotations

import copy
import datetime as dt
import os
import threading
import time
import uuid
from typing import Any, Dict, Optional

_LOCK = threading.Lock()
_LATEST: Optional[Dict[str, Any]] = None


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def begin_hydration(target_date: str, components: list[str], active_lineups: bool, force: bool) -> Dict[str, Any]:
    return {
        "run_id": str(uuid.uuid4()),
        "status": "running",
        "target_date": target_date,
        "components_requested": list(components),
        "active_lineups": bool(active_lineups),
        "force_requested": bool(force),
        "started_at": utc_now_iso(),
        "_started_monotonic": time.monotonic(),
    }


def _component_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    lineup = payload.get("lineup_filter") or {}
    return {
        "candidate_universe_count": payload.get("candidate_universe_count"),
        "deduped_universe_count": payload.get("deduped_universe_count"),
        "result_count_before_filters": payload.get("result_count_before_filters"),
        "result_count_after_filters": payload.get("result_count_after_filters"),
        "result_count_after_lineup_filter": payload.get("result_count_after_lineup_filter"),
        "lineup_status": lineup.get("lineup_status"),
        "confirmed_batter_count": lineup.get("confirmed_batter_count"),
        "games_checked": lineup.get("games_checked"),
        "games_with_lineups": lineup.get("games_with_lineups"),
        "teams_with_lineups": lineup.get("teams_with_lineups"),
        "warning_count": len(payload.get("filter_warnings") or []) + len(lineup.get("warnings") or []),
    }


def summarize_hydration_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    results = payload.get("results") or {}
    component_summaries = {
        component: _component_summary(result or {})
        for component, result in results.items()
        if isinstance(result, dict)
    }
    lineup_summaries = [
        summary for summary in component_summaries.values()
        if summary.get("lineup_status") not in (None, "not_applicable")
    ]
    warnings = []
    for result in results.values():
        if not isinstance(result, dict):
            continue
        warnings.extend(result.get("filter_warnings") or [])
        warnings.extend((result.get("lineup_filter") or {}).get("warnings") or [])
    return {
        "components": component_summaries,
        "component_count": len(component_summaries),
        "games_checked": max((item.get("games_checked") or 0 for item in lineup_summaries), default=0),
        "games_with_lineups": max((item.get("games_with_lineups") or 0 for item in lineup_summaries), default=0),
        "teams_with_lineups": max((item.get("teams_with_lineups") or 0 for item in lineup_summaries), default=0),
        "confirmed_batter_count": max((item.get("confirmed_batter_count") or 0 for item in lineup_summaries), default=0),
        "warnings": list(dict.fromkeys(str(item) for item in warnings if item)),
    }


def _publish(status: Dict[str, Any]) -> Dict[str, Any]:
    global _LATEST
    clean = {key: value for key, value in status.items() if not key.startswith("_")}
    with _LOCK:
        _LATEST = copy.deepcopy(clean)
    return clean


def complete_hydration(run: Dict[str, Any], payload: Dict[str, Any], cache_mode: str) -> Dict[str, Any]:
    completed = dict(run)
    completed.update(summarize_hydration_payload(payload))
    completed.update({
        "status": "success",
        "completed_at": utc_now_iso(),
        "duration_ms": round((time.monotonic() - run["_started_monotonic"]) * 1000),
        "cache_mode": cache_mode,
        "error": None,
    })
    return _publish(completed)


def fail_hydration(run: Dict[str, Any], error: Exception) -> Dict[str, Any]:
    failed = dict(run)
    failed.update({
        "status": "failed",
        "completed_at": utc_now_iso(),
        "duration_ms": round((time.monotonic() - run["_started_monotonic"]) * 1000),
        "error": str(error),
        "warnings": [],
        "components": {},
    })
    return _publish(failed)


def latest_hydration_status() -> Dict[str, Any]:
    with _LOCK:
        latest = copy.deepcopy(_LATEST)
    return latest or {
        "status": "never_run_in_this_process",
        "message": "No My Dashboard hydration execution has been observed since this application process started.",
    }


def cron_configuration() -> Dict[str, Any]:
    return {
        "target": "/my-dashboard/solver/hydrate-yesterday",
        "recommended_method": "POST",
        "recommended_force": True,
        "configured_schedule": os.getenv("MY_DASHBOARD_HYDRATION_CRON_SCHEDULE"),
        "configured_timezone": os.getenv("MY_DASHBOARD_HYDRATION_TIMEZONE", "America/New_York"),
        "verification_url": "/my-dashboard/hydration/status",
        "production_verified": os.getenv("MY_DASHBOARD_HYDRATION_PRODUCTION_VERIFIED", "false").lower() == "true",
    }
