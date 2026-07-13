from __future__ import annotations

import math
from typing import Any, Callable, Dict, List, Optional


MAX_PAGE_SIZE = 250
DEFAULT_PAGE_SIZE = 50
SORT_DIRECTIONS = {"asc", "desc"}

OBJECT_METADATA: Dict[str, Dict[str, Any]] = {
    "hitters": {"label": "Hitters", "label_plural": "Hitters", "entity_type": "hitter", "lineup_scope_supported": True},
    "pitchers": {"label": "Pitcher", "label_plural": "Pitchers", "entity_type": "pitcher", "lineup_scope_supported": False},
    "teams": {"label": "Team", "label_plural": "Teams", "entity_type": "team", "lineup_scope_supported": False},
    "totals": {"label": "Game Total", "label_plural": "Game Totals", "entity_type": "game", "lineup_scope_supported": False},
    "overall_players": {"label": "Player", "label_plural": "Overall Players", "entity_type": "player", "lineup_scope_supported": True},
}

BASE_FIELDS: List[Dict[str, Any]] = [
    {"name": "rank", "label": "Rank", "type": "integer", "sortable": True, "filterable": False, "nillable": False, "group": "Identity"},
    {"name": "entity_id", "label": "Entity ID", "type": "id", "sortable": True, "filterable": True, "nillable": False, "group": "Identity"},
    {"name": "entity_name", "label": "Name", "type": "string", "sortable": True, "filterable": True, "nillable": False, "group": "Identity"},
    {"name": "entity_type", "label": "Entity Type", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Identity"},
    {"name": "player_type", "label": "Player Type", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Identity"},
    {"name": "team", "label": "Team", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Matchup"},
    {"name": "opponent", "label": "Opponent", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Matchup"},
    {"name": "game_pk", "label": "Game PK", "type": "id", "sortable": True, "filterable": True, "nillable": True, "group": "Matchup"},
    {"name": "pitch_type", "label": "Pitch Type", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Matchup"},
    {"name": "pitch_name", "label": "Pitch Name", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Matchup"},
    {"name": "category", "label": "Category", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Classification"},
    {"name": "score", "label": "Score", "type": "double", "sortable": True, "filterable": True, "nillable": True, "group": "Scoring"},
    {"name": "base_score", "label": "Base Score", "type": "double", "sortable": True, "filterable": True, "nillable": True, "group": "Scoring"},
    {"name": "adjusted_score", "label": "Adjusted Score", "type": "double", "sortable": True, "filterable": True, "nillable": True, "group": "Scoring"},
    {"name": "confidence", "label": "Confidence", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Scoring"},
    {"name": "source", "label": "Source", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Audit"},
    {"name": "primary_reason", "label": "Primary Reason", "type": "textarea", "sortable": False, "filterable": True, "nillable": True, "group": "Audit"},
    {"name": "lineup_verified", "label": "Lineup Verified", "type": "boolean", "sortable": True, "filterable": True, "nillable": True, "group": "Audit"},
    {"name": "lineup_source", "label": "Lineup Source", "type": "string", "sortable": True, "filterable": True, "nillable": True, "group": "Audit"},
    {"name": "confirmed_lineup_date", "label": "Confirmed Lineup Date", "type": "date", "sortable": True, "filterable": True, "nillable": True, "group": "Audit"},
]

METRIC_FIELDS: Dict[str, List[str]] = {
    "hitters": ["xwOBA", "xBA", "EV", "LA", "HardHit", "Usage", "Pitcher xwOBA", "Pitches Seen", "PA"],
    "pitchers": ["K%", "BB%", "xwOBA Allowed", "HardHit Allowed", "Opp K%", "Opp ISO", "Score"],
    "teams": ["Edge Score", "Win Edge", "Run Diff", "ISO", "OBP", "SLG"],
    "totals": ["Projected Total", "Raw Total", "Run Index", "Score"],
    "overall_players": ["Score", "xwOBA", "EV", "K%", "xwOBA Allowed"],
}


def normalize_query(page_size: Any = DEFAULT_PAGE_SIZE, page_number: Any = 1, sort_by: Any = "score", sort_direction: Any = "desc") -> Dict[str, Any]:
    try:
        normalized_size = int(page_size)
    except (TypeError, ValueError):
        normalized_size = DEFAULT_PAGE_SIZE
    try:
        normalized_page = int(page_number)
    except (TypeError, ValueError):
        normalized_page = 1
    normalized_size = max(1, min(MAX_PAGE_SIZE, normalized_size))
    normalized_page = max(1, normalized_page)
    normalized_direction = str(sort_direction or "desc").lower()
    if normalized_direction not in SORT_DIRECTIONS:
        normalized_direction = "desc"
    return {
        "page_size": normalized_size,
        "page_number": normalized_page,
        "offset": (normalized_page - 1) * normalized_size,
        "sort_by": str(sort_by or "score"),
        "sort_direction": normalized_direction,
    }


def _value_at_path(record: Dict[str, Any], field_name: str) -> Any:
    if field_name.startswith("metrics."):
        return (record.get("metrics") or {}).get(field_name[8:])
    current: Any = record
    for part in field_name.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _sort_key(value: Any) -> tuple:
    if value is None or value == "":
        return (1, 0, "")
    try:
        return (0, 0, float(value))
    except (TypeError, ValueError):
        return (0, 1, str(value).lower())


def field_metadata(component: str, records: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    fields = [dict(field) for field in BASE_FIELDS]
    known_names = {field["name"] for field in fields}
    metric_names = set(METRIC_FIELDS.get(component, []))
    for record in records or []:
        metric_names.update((record.get("metrics") or {}).keys())
    for metric_name in sorted(metric_names):
        name = f"metrics.{metric_name}"
        if name in known_names:
            continue
        fields.append({
            "name": name,
            "label": metric_name,
            "type": "double",
            "sortable": True,
            "filterable": True,
            "nillable": True,
            "group": "Metrics",
            "source": "server_metric_registry",
        })
        known_names.add(name)
    return fields


def apply_report_query(payload: Dict[str, Any], component: str, page_size: Any = DEFAULT_PAGE_SIZE, page_number: Any = 1, sort_by: Any = "score", sort_direction: Any = "desc", include_metadata: bool = True) -> Dict[str, Any]:
    query = normalize_query(page_size, page_number, sort_by, sort_direction)
    all_records = [dict(record) for record in (payload.get("items") or []) if isinstance(record, dict)]
    reverse = query["sort_direction"] == "desc"
    all_records.sort(key=lambda record: _sort_key(_value_at_path(record, query["sort_by"])), reverse=reverse)
    for index, record in enumerate(all_records, start=1):
        record["rank"] = index
    total_size = len(all_records)
    start = query["offset"]
    end = start + query["page_size"]
    records = all_records[start:end]
    page_count = math.ceil(total_size / query["page_size"]) if total_size else 0
    has_next = end < total_size
    has_previous = query["page_number"] > 1 and total_size > 0
    result = dict(payload)
    result["items"] = records
    result["records"] = records
    result["totalSize"] = total_size
    result["total_count"] = total_size
    result["done"] = not has_next
    result["query"] = query
    result["page_info"] = {
        "page_number": query["page_number"],
        "page_size": query["page_size"],
        "page_count": page_count,
        "record_count": len(records),
        "total_count": total_size,
        "has_next": has_next,
        "has_previous": has_previous,
        "next_page": query["page_number"] + 1 if has_next else None,
        "previous_page": query["page_number"] - 1 if has_previous else None,
    }
    if include_metadata:
        object_metadata = dict(OBJECT_METADATA.get(component, {"label": component.title(), "label_plural": component.title()}))
        object_metadata.update({
            "name": component,
            "queryable": True,
            "sortable": True,
            "filterable": True,
            "fields": field_metadata(component, all_records),
        })
        result["object_info"] = object_metadata
    return result


def install_full_result_finalizer(solver_module: Any) -> None:
    """Install an additive response finalizer that preserves the full candidate universe.

    Existing candidate builders, scoring, filters, and response fields remain unchanged.
    The only removed behavior is the final arbitrary ten-record truncation.
    """

    def finalize_component_response(
        date: str,
        component: str,
        candidates: List[Dict[str, Any]],
        key_fn: Callable[[Dict[str, Any]], str],
        data_quality: Any = None,
        missing_data: Any = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        pool_limit = max(len(candidates), 1)
        deduped_pool = solver_module.dedupe_ranked_items(candidates, key_fn, limit=pool_limit)
        filtered, filters_applied, warnings, before, after = solver_module.apply_dashboard_filters(deduped_pool, filters)
        final_limit = max(len(filtered), 1)
        final_items = solver_module.dedupe_ranked_items(filtered, key_fn, limit=final_limit) if filtered else []
        response = solver_module.build_response(date, component, final_items, data_quality, missing_data)
        response.update({
            "filters_applied": filters_applied,
            "available_filters": solver_module.available_filters_for_component(component, deduped_pool),
            "result_count_before_filters": before,
            "result_count_after_filters": after,
            "filter_warnings": warnings,
            "candidate_universe_count": len(candidates),
            "deduped_universe_count": len(deduped_pool),
            "result_cap_applied": False,
        })
        return response

    solver_module.finalize_component_response = finalize_component_response
