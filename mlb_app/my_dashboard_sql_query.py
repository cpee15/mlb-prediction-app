from __future__ import annotations

import datetime as dt
import math
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import Float, and_, case, cast, func, or_

from .dashboard_report_types import FIELD_CATALOG, describe_report_type
from .my_dashboard_dataset import (
    DATASET_MODE_ACTIVE_LINEUPS,
    DATASET_MODE_STANDARD,
    MyDashboardRecord,
    dashboard_dataset_status,
)
from .my_dashboard_report_query import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    METRIC_FIELDS,
    OBJECT_METADATA,
    field_metadata,
    normalize_query,
)
from .my_dashboard_sql_weights import normalize_weights, weight_explanations, weighted_score_expression


CONFIDENCE_ORDER = {"low": 1, "medium": 2, "high": 3}
TEXT_FILTER_COLUMNS = {
    "team": MyDashboardRecord.team,
    "opponent": MyDashboardRecord.opponent,
    "source": MyDashboardRecord.source,
}
EXACT_FILTER_COLUMNS = {
    "category": MyDashboardRecord.category,
    "entity_type": MyDashboardRecord.entity_type,
    "player_type": MyDashboardRecord.player_type,
}
SORT_COLUMNS = {
    "entity_id": MyDashboardRecord.entity_id,
    "entity_name": MyDashboardRecord.entity_name,
    "entity_type": MyDashboardRecord.entity_type,
    "player_type": MyDashboardRecord.player_type,
    "team": MyDashboardRecord.team,
    "opponent": MyDashboardRecord.opponent,
    "game_pk": MyDashboardRecord.game_pk,
    "pitch_type": MyDashboardRecord.pitch_type,
    "pitch_name": MyDashboardRecord.pitch_name,
    "category": MyDashboardRecord.category,
    "score": MyDashboardRecord.score,
    "base_score": MyDashboardRecord.base_score,
    "adjusted_score": MyDashboardRecord.adjusted_score,
    "confidence": MyDashboardRecord.confidence,
    "source": MyDashboardRecord.source,
    "lineup_verified": MyDashboardRecord.lineup_verified,
    "lineup_source": MyDashboardRecord.lineup_source,
    "confirmed_lineup_date": MyDashboardRecord.confirmed_lineup_date,
    "lineup_revision": MyDashboardRecord.lineup_revision,
    "model_state": MyDashboardRecord.model_state,
}
DATASET_REPORT_TYPES = {
    "teams": "teams_daily_analysis",
    "totals": "games_totals_analysis",
    "overall_players": "overall_players_daily_analysis",
}


def normalize_dataset_filters(filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(filters, dict):
        return {}
    normalized: Dict[str, Any] = {}
    if isinstance(filters.get("conditions"), list):
        logic = str(filters.get("logic") or "and").strip().lower()
        if logic not in {"and", "or"}:
            raise ValueError("filters.logic must be 'and' or 'or'")
        normalized["logic"] = logic
        normalized["conditions"] = list(filters["conditions"])
    for key in [
        "search_text",
        "team",
        "opponent",
        "category",
        "entity_type",
        "player_type",
        "pitch_type",
        "pitch_name",
        "source",
    ]:
        value = filters.get(key)
        if value not in (None, ""):
            normalized[key] = str(value).strip()
    for key in ["min_score", "max_score"]:
        value = filters.get(key)
        if value in (None, ""):
            continue
        try:
            normalized[key] = float(value)
        except (TypeError, ValueError):
            continue
    confidence = filters.get("min_confidence") or filters.get("confidence")
    if confidence not in (None, ""):
        normalized["min_confidence"] = str(confidence).strip().lower()
    metric_filters = filters.get("metrics") if isinstance(filters.get("metrics"), dict) else {}
    normalized_metrics: Dict[str, Dict[str, float]] = {}
    for metric, rules in metric_filters.items():
        if not isinstance(rules, dict):
            continue
        entry: Dict[str, float] = {}
        for side in ["min", "max"]:
            value = rules.get(side)
            if value in (None, ""):
                continue
            try:
                entry[side] = float(value)
            except (TypeError, ValueError):
                continue
        if entry:
            normalized_metrics[str(metric)] = entry
    if normalized_metrics:
        normalized["metrics"] = normalized_metrics
    weights = filters.get("weights") if isinstance(filters.get("weights"), dict) else {}
    if weights:
        normalized["weights"] = dict(weights)
    return normalized


def _metric_registry(component: str) -> set[str]:
    return set(METRIC_FIELDS.get(component, []))


def _metric_expression(metric: str):
    return cast(MyDashboardRecord.metrics_json[metric].as_string(), Float)


def _dataset_catalog_field(report_type: str, field_name: str) -> Optional[Dict[str, Any]]:
    return next(
        (
            field
            for field in FIELD_CATALOG[report_type]
            if field["name"] == field_name
            or field.get("payload_path") == field_name
        ),
        None,
    )


def _confidence_expression():
    return case(
        (func.lower(MyDashboardRecord.confidence) == "high", 3),
        (func.lower(MyDashboardRecord.confidence) == "medium", 2),
        (func.lower(MyDashboardRecord.confidence) == "low", 1),
        else_=0,
    )


def _condition_expression(component: str, report_type: str, condition: Dict[str, Any]):
    if not isinstance(condition, dict):
        raise ValueError("Each filter condition must be an object")
    field_name = str(condition.get("field") or "")
    operator = str(condition.get("operator") or "eq").lower()
    field = _dataset_catalog_field(report_type, field_name)
    if not field or not field.get("filterable"):
        raise ValueError(f"Unsupported filter field: {field_name}")
    if operator not in field.get("supported_operators", []):
        raise ValueError(f"Unsupported operator '{operator}' for field '{field_name}'")

    if field.get("metric_key"):
        metric = field["metric_key"]
        if metric not in _metric_registry(component):
            raise ValueError(f"Unsupported metric filter: {metric}")
        column = _metric_expression(metric)
    else:
        column = SORT_COLUMNS.get(field_name)
    if column is None:
        raise ValueError(f"Unsupported filter field: {field_name}")

    value = condition.get("value")
    comparison = _confidence_expression() if field_name == "confidence" and operator in {"gt", "gte", "lt", "lte"} else column
    if comparison is not column:
        confidence_order = {"low": 1, "medium": 2, "high": 3}
        try:
            value = confidence_order[str(value).strip().lower()]
        except KeyError as exc:
            raise ValueError("confidence must be low, medium, or high") from exc
    elif operator not in {"is_null", "is_not_null", "in"}:
        if field.get("data_type") in {"id", "integer"}:
            try:
                value = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid integer value for field '{field_name}'") from exc
        elif field.get("data_type") in {"double", "number", "float"}:
            try:
                value = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid numeric value for field '{field_name}'") from exc
        elif field.get("data_type") == "boolean":
            if isinstance(value, bool):
                pass
            elif str(value).strip().lower() in {"true", "false"}:
                value = str(value).strip().lower() == "true"
            else:
                raise ValueError(f"Invalid boolean value for field '{field_name}'")
        elif field.get("data_type") == "date":
            try:
                value = dt.date.fromisoformat(str(value))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid date value for field '{field_name}'") from exc
        else:
            value = str(value)

    if operator == "eq": return column == value
    if operator == "neq": return column != value
    if operator == "in":
        if not isinstance(value, (list, tuple, set)):
            raise ValueError(f"Operator 'in' for field '{field_name}' requires a list")
        values = list(value)
        try:
            if field.get("data_type") in {"id", "integer"}:
                values = [int(item) for item in values]
            elif field.get("data_type") in {"double", "number", "float"}:
                values = [float(item) for item in values]
            elif field.get("data_type") == "boolean":
                normalized = []
                for item in values:
                    if isinstance(item, bool):
                        normalized.append(item)
                    elif str(item).strip().lower() in {"true", "false"}:
                        normalized.append(str(item).strip().lower() == "true")
                    else:
                        raise ValueError
                values = normalized
            elif field.get("data_type") == "date":
                values = [dt.date.fromisoformat(str(item)) for item in values]
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid list value for field '{field_name}'") from exc
        return column.in_(values)
    if operator == "contains": return func.lower(column).contains(str(value).lower())
    if operator == "gt": return comparison > value
    if operator == "gte": return comparison >= value
    if operator == "lt": return comparison < value
    if operator == "lte": return comparison <= value
    if operator == "is_null": return column.is_(None)
    if operator == "is_not_null": return column.is_not(None)
    raise ValueError(f"Unsupported operator: {operator}")


def _apply_filters(
    query: Any,
    component: str,
    filters: Dict[str, Any],
    report_type: Optional[str] = None,
) -> Tuple[Any, List[str], str, List[Dict[str, Any]]]:
    warnings: List[str] = []
    filter_logic = str(filters.get("logic") or "and")
    applied_conditions = list(filters.get("conditions") or [])
    if applied_conditions:
        resolved_report_type = report_type or DATASET_REPORT_TYPES.get(component)
        if not resolved_report_type:
            raise ValueError(f"No condition catalog is registered for component: {component}")
        expressions = [
            _condition_expression(component, resolved_report_type, condition)
            for condition in applied_conditions
        ]
        query = query.filter(or_(*expressions) if filter_logic == "or" else and_(*expressions))
    search = filters.get("search_text")
    if search:
        pattern = f"%{search.lower()}%"
        query = query.filter(
            or_(
                func.lower(func.coalesce(MyDashboardRecord.entity_name, "")).like(pattern),
                func.lower(func.coalesce(MyDashboardRecord.team, "")).like(pattern),
                func.lower(func.coalesce(MyDashboardRecord.opponent, "")).like(pattern),
                func.lower(func.coalesce(MyDashboardRecord.primary_reason, "")).like(pattern),
                func.lower(func.coalesce(MyDashboardRecord.source, "")).like(pattern),
            )
        )
    for key, column in TEXT_FILTER_COLUMNS.items():
        value = filters.get(key)
        if value:
            query = query.filter(func.lower(func.coalesce(column, "")).like(f"%{value.lower()}%"))
    for key, column in EXACT_FILTER_COLUMNS.items():
        value = filters.get(key)
        if value:
            query = query.filter(func.lower(func.coalesce(column, "")) == value.lower())
    pitch_value = filters.get("pitch_type") or filters.get("pitch_name")
    if pitch_value:
        pattern = f"%{pitch_value.lower()}%"
        query = query.filter(
            or_(
                func.lower(func.coalesce(MyDashboardRecord.pitch_type, "")).like(pattern),
                func.lower(func.coalesce(MyDashboardRecord.pitch_name, "")).like(pattern),
            )
        )
    if filters.get("min_score") is not None:
        query = query.filter(MyDashboardRecord.score >= filters["min_score"])
    if filters.get("max_score") is not None:
        query = query.filter(MyDashboardRecord.score <= filters["max_score"])
    min_confidence = filters.get("min_confidence")
    if min_confidence:
        threshold = CONFIDENCE_ORDER.get(min_confidence)
        if threshold is None:
            warnings.append(f"Unsupported confidence threshold: {min_confidence}")
        else:
            query = query.filter(_confidence_expression() >= threshold)
    registered_metrics = _metric_registry(component)
    for metric, rules in (filters.get("metrics") or {}).items():
        if metric not in registered_metrics:
            warnings.append(f"Unsupported metric filter: {metric}")
            query = query.filter(False)
            continue
        expression = _metric_expression(metric)
        query = query.filter(expression.isnot(None))
        if rules.get("min") is not None:
            query = query.filter(expression >= rules["min"])
        if rules.get("max") is not None:
            query = query.filter(expression <= rules["max"])
    return query, warnings, filter_logic, applied_conditions


def _sort_expression(
    component: str,
    report_type: Optional[str],
    sort_by: str,
    weighted_expression=None,
):
    if weighted_expression is not None and sort_by in {"rank", "score", "adjusted_score"}:
        return weighted_expression
    if sort_by == "rank":
        return MyDashboardRecord.score
    field = _dataset_catalog_field(report_type, sort_by) if report_type else None
    if field and field.get("metric_key"):
        metric = field["metric_key"]
        if metric not in _metric_registry(component):
            raise ValueError(f"Unsupported metric sort field: {metric}")
        return _metric_expression(metric)
    if not report_type and sort_by.startswith("metrics."):
        metric = sort_by[8:]
        if metric not in _metric_registry(component):
            raise ValueError(f"Unsupported metric sort field: {metric}")
        return _metric_expression(metric)
    expression = SORT_COLUMNS.get(sort_by)
    if expression is None:
        raise ValueError(f"Unsupported sort field: {sort_by}")
    if sort_by == "confidence":
        return _confidence_expression()
    return expression


def _record_to_dict(
    row: MyDashboardRecord,
    rank: int,
    *,
    report_type: Optional[str] = None,
    adjusted_score: Optional[float] = None,
    explanations: Optional[List[str]] = None,
) -> Dict[str, Any]:
    record = dict(row.record_json or {})
    effective_score = round(float(adjusted_score), 3) if adjusted_score is not None else row.score
    base_score = row.base_score if row.base_score is not None else row.score
    record.update({
        "rank": rank,
        "entity_id": row.entity_id,
        "entity_name": row.entity_name,
        "entity_type": row.entity_type,
        "player_type": row.player_type,
        "game_pk": row.game_pk,
        "team": row.team,
        "opponent": row.opponent,
        "category": row.category,
        "pitch_type": row.pitch_type,
        "pitch_name": row.pitch_name,
        "score": effective_score,
        "base_score": base_score,
        "adjusted_score": effective_score if adjusted_score is not None else row.adjusted_score,
        "confidence": row.confidence,
        "primary_reason": row.primary_reason,
        "source": row.source,
        "metrics": row.metrics_json or {},
        "reasoning": row.reasoning_json or [],
        "missing_data": row.missing_data_json or [],
        "best_pitch_angles": row.best_pitch_angles_json or [],
        "lineup_verified": row.lineup_verified,
        "lineup_source": row.lineup_source,
        "confirmed_lineup_date": row.confirmed_lineup_date.isoformat() if row.confirmed_lineup_date else None,
        "lineup_revision": row.lineup_revision,
        "model_state": row.model_state,
    })
    if report_type:
        for field in FIELD_CATALOG[report_type]:
            metric_key = field.get("metric_key")
            if metric_key:
                record[field["name"]] = (row.metrics_json or {}).get(metric_key)
    if adjusted_score is not None:
        record["weight_explanation"] = list(explanations or [])
    return record


def query_dashboard_dataset(
    *,
    session: Any,
    date: str,
    component: str,
    filters: Optional[Dict[str, Any]] = None,
    page_size: Any = DEFAULT_PAGE_SIZE,
    page_number: Any = 1,
    sort_by: Any = "score",
    sort_direction: Any = "desc",
    include_metadata: bool = True,
    active_lineups: bool = False,
    report_type: Optional[str] = None,
    selected_fields: Optional[List[str]] = None,
) -> Dict[str, Any]:
    target_date = dt.date.fromisoformat(str(date)[:10])
    normalized_component = str(component or "").strip().lower()
    if normalized_component not in OBJECT_METADATA:
        raise ValueError(f"Unsupported dashboard component: {component}")
    query_contract = normalize_query(page_size, page_number, sort_by, sort_direction)
    normalized_filters = normalize_dataset_filters(filters)
    resolved_report_type = report_type or DATASET_REPORT_TYPES.get(normalized_component)
    if selected_fields is not None and resolved_report_type:
        invalid = [
            name for name in selected_fields
            if not (
                (field := _dataset_catalog_field(resolved_report_type, name))
                and field.get("selectable", True)
            )
        ]
        if invalid:
            raise ValueError(f"Unsupported selected field(s): {', '.join(invalid)}")
    weights, weight_warnings = normalize_weights(normalized_component, normalized_filters.get("weights"))
    if weights:
        normalized_filters["weights"] = weights
    else:
        normalized_filters.pop("weights", None)
    mode = DATASET_MODE_ACTIVE_LINEUPS if active_lineups else DATASET_MODE_STANDARD
    base_query = session.query(MyDashboardRecord).filter(
        and_(
            MyDashboardRecord.dataset_date == target_date,
            MyDashboardRecord.component == normalized_component,
            MyDashboardRecord.dataset_mode == mode,
            MyDashboardRecord.is_current.is_(True),
        )
    )
    filtered_query, warnings, filter_logic, applied_conditions = _apply_filters(
        base_query,
        normalized_component,
        normalized_filters,
        resolved_report_type,
    )
    warnings.extend(weight_warnings)
    total_size = filtered_query.order_by(None).count()
    weighted_expression = weighted_score_expression(weights) if weights else None
    sort_expression = _sort_expression(
        normalized_component,
        resolved_report_type,
        query_contract["sort_by"],
        weighted_expression,
    )
    primary_order = sort_expression.asc().nullslast() if query_contract["sort_direction"] == "asc" else sort_expression.desc().nullslast()
    ordered_query = filtered_query.order_by(primary_order, MyDashboardRecord.entity_key.asc(), MyDashboardRecord.id.asc())
    if weighted_expression is not None:
        raw_rows = (
            ordered_query.add_columns(weighted_expression.label("query_adjusted_score"))
            .offset(query_contract["offset"])
            .limit(query_contract["page_size"])
            .all()
        )
        explanations = weight_explanations(weights)
        records = [
            _record_to_dict(
                row,
                query_contract["offset"] + index,
                report_type=resolved_report_type,
                adjusted_score=adjusted,
                explanations=explanations,
            )
            for index, (row, adjusted) in enumerate(raw_rows, start=1)
        ]
    else:
        rows = ordered_query.offset(query_contract["offset"]).limit(query_contract["page_size"]).all()
        records = [
            _record_to_dict(
                row,
                query_contract["offset"] + index,
                report_type=resolved_report_type,
            )
            for index, row in enumerate(rows, start=1)
        ]
    page_count = math.ceil(total_size / query_contract["page_size"]) if total_size else 0
    end = query_contract["offset"] + len(records)
    has_next = end < total_size
    has_previous = query_contract["page_number"] > 1 and total_size > 0
    status = dashboard_dataset_status(
        session=session,
        date=target_date.isoformat(),
        component=normalized_component,
        active_lineups=active_lineups,
    )
    result: Dict[str, Any] = {
        "date": target_date.isoformat(),
        "component": normalized_component,
        "items": records,
        "records": records,
        "totalSize": total_size,
        "total_count": total_size,
        "done": not has_next,
        "query": query_contract,
        "filters_applied": normalized_filters,
        "filter_logic": filter_logic,
        "conditions_applied": applied_conditions,
        "population": {
            "mode": "confirmed_lineup" if active_lineups else "daily_dataset",
            "matched_current_count": status.get("dataset_row_count"),
            "filtered_count": total_size,
        },
        "filter_warnings": warnings,
        "weight_ranking": {
            "enabled": bool(weights),
            "weights": weights,
            "formula": "base_score + normalized_metric * (weight - 1.0) * 0.25",
            "persisted": False,
        },
        "page_info": {
            "page_number": query_contract["page_number"],
            "page_size": query_contract["page_size"],
            "page_count": page_count,
            "record_count": len(records),
            "total_count": total_size,
            "has_next": has_next,
            "has_previous": has_previous,
            "next_page": query_contract["page_number"] + 1 if has_next else None,
            "previous_page": query_contract["page_number"] - 1 if has_previous else None,
        },
        **status,
        "filtered_row_count": total_size,
        "page_record_count": len(records),
    }
    if include_metadata and resolved_report_type:
        result["object_info"] = describe_report_type(resolved_report_type)
    elif include_metadata:
        object_info = dict(OBJECT_METADATA[normalized_component])
        object_info.update({
            "name": normalized_component,
            "queryable": True,
            "sortable": True,
            "filterable": True,
            "query_engine": "sqlalchemy_dataset",
            "fields": field_metadata(normalized_component),
        })
        result["object_info"] = object_info
    return result
