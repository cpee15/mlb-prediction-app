from __future__ import annotations

import datetime as dt
import math
import os
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import or_

from . import ai_data_assistant as assistant
from .database import BatterPitchTypeMatchup


MAX_PAGE_SIZE = 250
DEFAULT_PAGE_SIZE = 50
SORT_DIRECTIONS = {"asc", "desc"}
DEFAULT_HITTER_SOURCE_ROW_LIMIT = 10000

OBJECT_METADATA: Dict[str, Dict[str, Any]] = {
    "hitters": {"label": "Hitter", "label_plural": "Hitters", "entity_type": "hitter", "lineup_scope_supported": True},
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


def build_full_stored_365_sweep_context(session: Any, date: str) -> Dict[str, Any]:
    """Score the complete available Stored 365 row set for the requested date.

    The legacy assistant context intentionally returned only its top 25 rows for a
    chat summary. My Dashboard needs the full report universe, so this adapter
    keeps the same joins and scoring function while removing that presentation cap.
    """
    errors: List[str] = []
    projection_payload: Dict[str, Any] = {}
    try:
        projection_payload = assistant.build_model_projection_payload(session, date) or {}
    except Exception as exc:
        errors.append(f"projection_payload_unavailable_for_arsenal_join: {exc}")
    pitch_lookup = assistant.pitch_lookup_from_projection_payload(projection_payload)

    row_limit = max(1, int(os.getenv("DASHBOARD_HITTER_SOURCE_ROW_LIMIT", str(DEFAULT_HITTER_SOURCE_ROW_LIMIT))))
    rows: List[Any] = []
    try:
        parsed = dt.date.fromisoformat(date[:10])
        rows = (
            session.query(BatterPitchTypeMatchup)
            .filter(or_(BatterPitchTypeMatchup.target_date == parsed, BatterPitchTypeMatchup.target_date.is_(None)))
            .limit(row_limit)
            .all()
        )
    except Exception as exc:
        errors.append(str(exc))

    scored_rows: List[Dict[str, Any]] = []
    for row in rows:
        pitcher_id = assistant.safe_int(assistant.row_value(row, "opposing_pitcher_id"))
        pitch_type = assistant.row_value(row, "pitch_type")
        pitcher_pitch = pitch_lookup.get((pitcher_id, str(pitch_type))) or {}
        score = assistant.score_batter_pitch_matchup(row, pitcher_pitch, target_date=date)
        scored_rows.append({
            "rank_score": score.get("score"),
            "confidence": score.get("confidence"),
            "confidence_tier": score.get("confidence_tier"),
            "reasons": score.get("reasons"),
            "missing_inputs": score.get("missing_inputs"),
            "sample_size": score.get("sample_size"),
            "batter_id": assistant.row_value(row, "batter_id"),
            "batter_name": assistant.row_value(row, "batter_name"),
            "batter_team_id": assistant.row_value(row, "batter_team_id"),
            "opposing_pitcher_id": pitcher_id,
            "opposing_pitcher_name": pitcher_pitch.get("pitcher_name"),
            "game_pk": assistant.row_value(row, "game_pk") or pitcher_pitch.get("game_pk"),
            "game_label": pitcher_pitch.get("game_label"),
            "pitch_type": pitch_type,
            "pitch_name": pitcher_pitch.get("pitch_name"),
            "pitcher_usage_pct": pitcher_pitch.get("usage_pct"),
            "pitcher_xwoba_allowed": pitcher_pitch.get("xwoba_allowed"),
            "pitcher_hard_hit_allowed": pitcher_pitch.get("hard_hit_pct_allowed"),
            "pitcher_whiff_pct": pitcher_pitch.get("whiff_pct"),
            "hitter_xwoba": assistant.safe_float(assistant.row_value(row, "xwoba")),
            "hitter_xba": assistant.safe_float(assistant.row_value(row, "xba")),
            "hitter_avg_ev": assistant.safe_float(assistant.row_value(row, "avg_exit_velocity") or assistant.row_value(row, "avg_ev")),
            "hitter_avg_la": assistant.safe_float(assistant.row_value(row, "avg_launch_angle") or assistant.row_value(row, "avg_la")),
            "hitter_hard_hit_pct": assistant.normalize_rate(assistant.row_value(row, "hard_hit_pct") or assistant.row_value(row, "hardhit_pct")),
            "hitter_whiff_pct": assistant.normalize_rate(assistant.row_value(row, "whiff_pct")),
            "hitter_k_pct": assistant.normalize_rate(assistant.row_value(row, "k_pct")),
            "pa": assistant.safe_int(assistant.row_value(row, "pa_ended") or assistant.row_value(row, "pa")),
            "pitches_seen": assistant.safe_int(assistant.row_value(row, "pitches_seen")),
            "target_date": assistant.row_value(row, "target_date").isoformat() if assistant.row_value(row, "target_date") else None,
            "date_start": assistant.row_value(row, "date_start").isoformat() if assistant.row_value(row, "date_start") else None,
            "date_end": assistant.row_value(row, "date_end").isoformat() if assistant.row_value(row, "date_end") else None,
            "refreshed_at": assistant.row_value(row, "refreshed_at").isoformat() if assistant.row_value(row, "refreshed_at") else None,
            "source": assistant.row_value(row, "source"),
            "duplicate_rows_removed": assistant.safe_int(assistant.row_value(row, "duplicate_rows_removed")),
            "watchlist_note": "player prop watchlist angle only; no sportsbook line assumed",
        })

    scored_rows.sort(key=lambda item: item.get("rank_score") or -999, reverse=True)
    missing_name_count = sum(1 for item in scored_rows if not item.get("batter_name"))
    return {
        "intent": "stored_365_matchups",
        "date": date,
        "sources_used": ["batter_pitch_type_matchups", "model_projections.pitch_arsenal", "pitcher_arsenals"],
        "top_matchups": scored_rows,
        "data_quality": {
            "stored_365_rows_scored": len(scored_rows),
            "stored_365_source_row_limit": row_limit,
            "stored_365_source_limit_reached": len(rows) >= row_limit,
            "pitcher_pitch_join_rows": len(pitch_lookup),
            "missing_batter_names": missing_name_count,
            "errors": errors,
        },
        "missing_data": errors + ([f"{missing_name_count} rows are missing batter names"] if missing_name_count else []),
    }


def install_full_result_finalizer(solver_module: Any) -> None:
    """Install the report-oriented full-result contract on the existing solver."""

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

    solver_module.build_stored_365_sweep_context = build_full_stored_365_sweep_context
    solver_module.finalize_component_response = finalize_component_response
