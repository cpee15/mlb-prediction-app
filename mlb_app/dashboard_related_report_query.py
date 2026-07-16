"""Validated SQL queries for selected one-to-many dashboard relationships."""

from __future__ import annotations

import datetime as dt
import math
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import case, func

from .dashboard_object_models import DashboardPlayer
from .dashboard_report_types import FIELD_CATALOG, REPORT_TYPES, describe_report_type
from .database import BatterPitchTypeMatchup


MODELS = {
    "players_lineup_history": DashboardPlayer,
    "hitters_arsenal_splits": BatterPitchTypeMatchup,
}

COLUMN_NAMES = {
    "players_lineup_history": (
        "mlb_player_id", "full_name", "player_type", "current_team_id", "current_team_name",
        "most_recent_lineup_date", "lineup_appearance_count", "most_recent_game_date",
        "tracked_game_count", "active_status_reason", "is_active",
    ),
    "hitters_arsenal_splits": (
        "id", "batter_id", "batter_name", "batter_team_id", "opposing_pitcher_id",
        "pitch_type", "game_pk", "target_date", "date_end", "pitches_seen", "pa_ended",
        "xwoba", "xba", "avg_exit_velocity", "avg_launch_angle", "hard_hit_pct",
        "whiff_pct", "k_pct", "source", "refreshed_at",
    ),
}


def _columns(report_type: str) -> Dict[str, Any]:
    model = MODELS[report_type]
    return {name: getattr(model, name) for name in COLUMN_NAMES[report_type]}


def _conditions(filters: Any, report_type: str) -> List[Dict[str, Any]]:
    if filters is None:
        return []
    if isinstance(filters, list):
        return filters
    if not isinstance(filters, dict):
        raise ValueError("filters must be an object or list of conditions")
    conditions = list(filters.get("conditions") or [])
    aliases = {
        "players_lineup_history": (
            ("search_text", "full_name", "contains"),
            ("team", "current_team_name", "eq"),
            ("min_lineup_appearances", "lineup_appearance_count", "gte"),
        ),
        "hitters_arsenal_splits": (
            ("search_text", "batter_name", "contains"),
            ("team_id", "batter_team_id", "eq"),
            ("pitch_type", "pitch_type", "eq"),
            ("min_pitches_seen", "pitches_seen", "gte"),
        ),
    }[report_type]
    for key, field, operator in aliases:
        if filters.get(key) not in (None, "", "All"):
            conditions.append({"field": field, "operator": operator, "value": filters[key]})
    return conditions


def _apply_filters(query: Any, report_type: str, filters: Any) -> Tuple[Any, List[Dict[str, Any]]]:
    columns = _columns(report_type)
    catalog = {field["name"]: field for field in FIELD_CATALOG[report_type]}
    applied = _conditions(filters, report_type)
    for condition in applied:
        if not isinstance(condition, dict):
            raise ValueError("Each filter condition must be an object")
        field_name = str(condition.get("field") or "")
        operator = str(condition.get("operator") or "eq").lower()
        field = catalog.get(field_name)
        if not field or not field.get("filterable") or field_name not in columns:
            raise ValueError(f"Unsupported filter field: {field_name}")
        if operator not in field["supported_operators"]:
            raise ValueError(f"Unsupported operator '{operator}' for field '{field_name}'")
        column, value = columns[field_name], condition.get("value")
        if operator == "eq": expression = column == value
        elif operator == "neq": expression = column != value
        elif operator == "in":
            if not isinstance(value, (list, tuple, set)):
                raise ValueError(f"Operator 'in' for field '{field_name}' requires a list")
            expression = column.in_(list(value))
        elif operator == "contains": expression = func.lower(column).contains(str(value).lower())
        elif operator == "gt": expression = column > value
        elif operator == "gte": expression = column >= value
        elif operator == "lt": expression = column < value
        elif operator == "lte": expression = column <= value
        elif operator == "is_null": expression = column.is_(None)
        elif operator == "is_not_null": expression = column.is_not(None)
        else: raise ValueError(f"Unsupported operator: {operator}")
        query = query.filter(expression)
    return query, applied


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, (dt.date, dt.datetime)) else value


def query_related_report(
    session: Any,
    report_type: str,
    *,
    filters: Any = None,
    weights: Any = None,
    page_size: int = 50,
    page_number: int = 1,
    sort_by: Optional[str] = None,
    sort_direction: str = "desc",
    selected_fields: Optional[Iterable[str]] = None,
    include_metadata: bool = True,
) -> Dict[str, Any]:
    if report_type not in MODELS or not REPORT_TYPES[report_type].get("queryable"):
        raise ValueError(f"Unsupported related report type: {report_type}")
    if weights:
        raise ValueError(f"Weights are not supported for related report type: {report_type}")
    if not isinstance(page_size, int) or page_size < 1 or page_size > 250:
        raise ValueError("page_size must be between 1 and 250")
    if not isinstance(page_number, int) or page_number < 1:
        raise ValueError("page_number must be at least 1")
    direction = str(sort_direction).lower()
    if direction not in {"asc", "desc"}:
        raise ValueError("sort_direction must be 'asc' or 'desc'")

    model, columns = MODELS[report_type], _columns(report_type)
    catalog = {field["name"]: field for field in FIELD_CATALOG[report_type]}
    requested_fields = list(selected_fields or columns)
    invalid = [name for name in requested_fields if name not in catalog]
    if invalid:
        raise ValueError(f"Unsupported selected field(s): {', '.join(invalid)}")

    if report_type == "players_lineup_history":
        query = session.query(model).filter(
            DashboardPlayer.identity_resolution_status == "resolved",
            DashboardPlayer.lineup_appearance_count > 0,
        )
        default_sort = "most_recent_lineup_date"
        tie_column = DashboardPlayer.mlb_player_id
    else:
        query = session.query(model).join(
            DashboardPlayer, DashboardPlayer.mlb_player_id == BatterPitchTypeMatchup.batter_id
        ).filter(
            DashboardPlayer.is_active.is_(True),
            DashboardPlayer.identity_resolution_status == "resolved",
            DashboardPlayer.player_type == "hitter",
        )
        default_sort = "pitches_seen"
        tie_column = BatterPitchTypeMatchup.id

    query, applied_filters = _apply_filters(query, report_type, filters)
    total_count = query.count()
    sort_name = str(sort_by or default_sort)
    field = catalog.get(sort_name)
    if not field or not field.get("sortable") or sort_name not in columns:
        raise ValueError(f"Unsupported sort field: {sort_name}")
    sort_column = columns[sort_name]
    query = query.order_by(
        case((sort_column.is_(None), 1), else_=0),
        sort_column.asc() if direction == "asc" else sort_column.desc(),
        tie_column.asc(),
    )
    offset = (page_number - 1) * page_size
    rows = query.offset(offset).limit(page_size).all()
    records = []
    for index, row in enumerate(rows, start=offset + 1):
        record = {name: _iso(getattr(row, name)) for name in columns}
        record["rank"] = index
        records.append(record)
    page_count = math.ceil(total_count / page_size) if total_count else 0
    has_next = offset + len(records) < total_count
    response = {
        "report_type": report_type,
        "component": REPORT_TYPES[report_type]["ui_object"],
        "records": records,
        "items": records,
        "totalSize": total_count,
        "total_count": total_count,
        "done": not has_next,
        "query_source": REPORT_TYPES[report_type]["base_object"],
        "filters_applied": applied_filters,
        "query": {
            "source": REPORT_TYPES[report_type]["base_object"],
            "sort_by": sort_name,
            "sort_direction": direction,
            "selected_fields": requested_fields,
        },
        "page_info": {
            "page_number": page_number,
            "page_size": page_size,
            "page_count": page_count,
            "record_count": len(records),
            "total_count": total_count,
            "has_next": has_next,
            "has_previous": page_number > 1 and total_count > 0,
            "next_page": page_number + 1 if has_next else None,
            "previous_page": page_number - 1 if page_number > 1 and total_count > 0 else None,
        },
        "provenance": {
            "source_object": REPORT_TYPES[report_type]["base_object"],
            "relationship_path": REPORT_TYPES[report_type]["relationships"],
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        },
    }
    if include_metadata:
        response["object_info"] = describe_report_type(report_type)
    return response
