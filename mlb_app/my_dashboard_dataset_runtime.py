from __future__ import annotations

import datetime as dt
from typing import Any, Callable, Dict, Optional
from zoneinfo import ZoneInfo

from .my_dashboard_dataset import dashboard_dataset_status
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
    "weights",
    "conditions",
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
        if key == "weights":
            if isinstance(value, dict) and any(
                weight not in (None, "", 1, 1.0, "1", "1.0") for weight in value.values()
            ):
                return True
            continue
        if key == "conditions":
            if isinstance(value, list) and value:
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
    return requested_date == mlb_business_date() and has_substantive_filters(filters)


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
    report_type: Optional[str] = None,
    selected_fields: Optional[list[str]] = None,
) -> Dict[str, Any]:
    status = dashboard_dataset_status(
        session=session,
        date=date,
        component=component,
        active_lineups=active_lineups,
    )
    del payload_builder  # User-facing report requests are read-only.
    if not status.get("ready"):
        return {
            "report_type": report_type,
            "component": component,
            "records": [],
            "items": [],
            "totalSize": 0,
            "total_count": 0,
            "done": True,
            "data_status": "not_ready",
            "refreshing": False,
            "stale": False,
            "message": "The scheduled dashboard snapshot is not ready for this date.",
            "execution_path": "my_dashboard_dataset_sql_query",
            "dataset_hydrated_for_request": False,
            "dataset_hydration": None,
            "served_stale_dataset": False,
            "dataset_status": status,
            "page_info": {
                "page_number": page_number,
                "page_size": page_size,
                "page_count": 0,
                "record_count": 0,
                "total_count": 0,
                "has_next": False,
                "has_previous": False,
                "next_page": None,
                "previous_page": None,
            },
        }

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
        report_type=report_type,
        selected_fields=selected_fields,
    )
    result.update({
        "execution_path": "my_dashboard_dataset_sql_query",
        "dataset_hydrated_for_request": False,
        "dataset_hydration": None,
        "served_stale_dataset": bool(status.get("stale")),
        "data_status": "stale" if status.get("stale") else "ready",
        "stale": bool(status.get("stale")),
        "refreshing": False,
        "dataset_status": status,
    })
    if status.get("stale"):
        warnings = list(result.get("filter_warnings") or [])
        warnings.append("The last successful dataset is stale; scheduled refresh is required")
        result["filter_warnings"] = warnings
    return result
