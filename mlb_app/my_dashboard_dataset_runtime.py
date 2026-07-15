from __future__ import annotations

import datetime as dt
import os
from typing import Any, Callable, Dict, Optional
from zoneinfo import ZoneInfo

from .my_dashboard_dataset import dashboard_dataset_status, hydrate_dashboard_dataset
from .my_dashboard_sql_query import query_dashboard_dataset


MLB_TIMEZONE = ZoneInfo("America/New_York")
SUBSTANTIVE_FILTER_KEYS = {
    "search_text",
    "team",
    "opponent",
    "category",
    "entity_type",
    "player_type",
    "pitch_type",
    "pitch_name",
    "source",
    "min_score",
    "max_score",
    "min_confidence",
    "metrics",
}


def mlb_business_date(now: Optional[dt.datetime] = None) -> dt.date:
    current = now or dt.datetime.now(dt.timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=dt.timezone.utc)
    return current.astimezone(MLB_TIMEZONE).date()


def has_substantive_filters(filters: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(filters, dict):
        return False
    for key in SUBSTANTIVE_FILTER_KEYS:
        value = filters.get(key)
        if key == "metrics":
            if isinstance(value, dict) and any(
                isinstance(rule, dict)
                and any(bound not in (None, "") for bound in (rule.get("min"), rule.get("max")))
                for rule in value.values()
            ):
                return True
            continue
        if value not in (None, "", {}, []):
            return True
    return False


def has_weight_overrides(filters: Optional[Dict[str, Any]]) -> bool:
    weights = filters.get("weights") if isinstance(filters, dict) else None
    if not isinstance(weights, dict):
        return False
    return any(value not in (None, "", 1, 1.0, "1", "1.0") for value in weights.values())


def should_use_dataset_query(*, date: str, filters: Optional[Dict[str, Any]]) -> bool:
    try:
        requested_date = dt.date.fromisoformat(str(date)[:10])
    except ValueError:
        return False
    return (
        requested_date == mlb_business_date()
        and has_substantive_filters(filters)
        and not has_weight_overrides(filters)
    )


def _dataset_ttl_seconds(active_lineups: bool) -> int:
    name = "DASHBOARD_ACTIVE_LINEUP_DATASET_TTL_SECONDS" if active_lineups else "DASHBOARD_DATASET_TTL_SECONDS"
    default = 30 if active_lineups else 300
    try:
        return max(1, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def run_dataset_query(
    *,
    session: Any,
    date: str,
    component: str,
    filters: Dict[str, Any],
    page_size: int,
    page_number: int,
    sort_by: str,
    sort_direction: str,
    include_metadata: bool,
    payload_builder: Callable[[], Dict[str, Any]],
    active_lineups: bool = False,
) -> Dict[str, Any]:
    status = dashboard_dataset_status(
        session=session,
        date=date,
        component=component,
        active_lineups=active_lineups,
    )
    hydration: Optional[Dict[str, Any]] = None
    served_stale = False

    if not status.get("ready") or status.get("stale"):
        try:
            hydration = hydrate_dashboard_dataset(
                session=session,
                date=date,
                component=component,
                payload_builder=payload_builder,
                active_lineups=active_lineups,
                ttl_seconds=_dataset_ttl_seconds(active_lineups),
                solver_version="my_dashboard_solver_v1",
            )
        except Exception:
            refreshed_status = dashboard_dataset_status(
                session=session,
                date=date,
                component=component,
                active_lineups=active_lineups,
            )
            if not refreshed_status.get("ready"):
                raise
            status = refreshed_status
            served_stale = True

    result = query_dashboard_dataset(
        session=session,
        date=date,
        component=component,
        filters=filters,
        page_size=page_size,
        page_number=page_number,
        sort_by=sort_by,
        sort_direction=sort_direction,
        include_metadata=include_metadata,
        active_lineups=active_lineups,
    )
    result.update({
        "execution_path": "my_dashboard_dataset_sql_query",
        "dataset_hydrated_for_request": bool(hydration),
        "dataset_hydration": hydration,
        "served_stale_dataset": served_stale,
    })
    if served_stale:
        warnings = list(result.get("filter_warnings") or [])
        warnings.append("Dataset refresh failed; previous current dataset was served")
        result["filter_warnings"] = warnings
    return result
