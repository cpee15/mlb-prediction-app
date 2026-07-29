from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List

from .database import AppDashboardItem


ANALYZABLE_SOURCE_TYPES = {"report_view", "workbench_view", "dashboard_report"}
MAX_SELECTED_REPORTS = 5
MAX_ANALYSIS_ROWS = 12


def resolve_owned_saved_reports(session, user_id: int, report_ids: Iterable[int]) -> List[AppDashboardItem]:
    ids = list(dict.fromkeys(int(value) for value in report_ids))
    if not ids:
        return []
    if len(ids) > MAX_SELECTED_REPORTS:
        raise ValueError(f"Select no more than {MAX_SELECTED_REPORTS} saved reports")
    items = (
        session.query(AppDashboardItem)
        .filter(
            AppDashboardItem.user_id == user_id,
            AppDashboardItem.id.in_(ids),
            AppDashboardItem.source_type.in_(ANALYZABLE_SOURCE_TYPES),
        )
        .all()
    )
    by_id = {item.id: item for item in items}
    if len(by_id) != len(ids):
        raise LookupError("One or more saved reports are unavailable")
    return [by_id[item_id] for item_id in ids]


def execution_request_from_item(item: AppDashboardItem) -> Dict[str, Any]:
    payload = dict(item.payload_json or {})
    definition = dict(payload.get("definition") or {})
    report_type = definition.get("report_type")
    if not report_type:
        raise ValueError(f"{item.title or 'Saved report'} has no executable canonical report definition")
    snapshot = dict(payload.get("snapshot") or {})
    sort = dict(definition.get("sort") or item.sort_json or {})
    saved_filters = dict(definition.get("filters") or item.filter_json or {})
    weights = dict(saved_filters.pop("weights", {}) or {})
    return {
        "report_type": report_type,
        "as_of_date": snapshot.get("generated_for_date") or payload.get("saved_on_date"),
        "filters": saved_filters,
        "weights": weights,
        "page_size": min(MAX_ANALYSIS_ROWS, max(1, int(definition.get("page_size") or MAX_ANALYSIS_ROWS))),
        "page_number": 1,
        "sort_by": sort.get("by") or sort.get("sort_by") or "model_score",
        "sort_direction": sort.get("direction") or sort.get("sort_direction") or "desc",
        "selected_fields": definition.get("selected_fields") or payload.get("report_columns"),
        "include_metadata": True,
        "confirmed_lineups_only": bool(definition.get("active_lineups_only")),
        "trend_config": definition.get("trend_config"),
    }


def _negative_factors(row: Dict[str, Any]) -> List[str]:
    negatives: List[str] = []
    for key, value in row.items():
        lowered = key.lower()
        if value in (None, "", [], {}):
            continue
        if any(token in lowered for token in ("missing", "risk", "warning")):
            negatives.append(f"{key}: {value}")
        elif lowered in {"strikeout_rate", "xwoba_allowed", "walk_rate"}:
            negatives.append(f"{key}: {value}")
    return negatives[:5]


def build_saved_report_packet(
    items: List[AppDashboardItem],
    execute: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    reports: List[Dict[str, Any]] = []
    entity_sets: List[set] = []
    for item in items:
        request = execution_request_from_item(item)
        result = execute(request)
        rows = list(result.get("records") or result.get("items") or [])[:MAX_ANALYSIS_ROWS]
        identity_keys = (
            "player_id", "mlb_player_id", "entity_id", "game_pk", "team_id",
            "batter_id", "snapshot_id",
        )
        identities = {
            next((str(row.get(key)) for key in identity_keys if row.get(key) is not None), "")
            for row in rows
        }
        identities.discard("")
        entity_sets.append(identities)
        definition = dict((item.payload_json or {}).get("definition") or {})
        reports.append({
            "saved_report_id": item.id,
            "folder_id": item.folder_id,
            "report_name": item.title,
            "report_type": request["report_type"],
            "purpose": item.subtitle or item.notes,
            "requested_date": request.get("as_of_date"),
            "executed_date": (result.get("provenance") or {}).get("requested_date") or request.get("as_of_date"),
            "freshness": result.get("provenance") or result.get("data_quality"),
            "filters": request.get("filters"),
            "scoring_weights": request.get("weights"),
            "selected_fields": request.get("selected_fields"),
            "sort": {"by": request.get("sort_by"), "direction": request.get("sort_direction")},
            "matched_row_count": result.get("totalSize") or result.get("total_count") or len(rows),
            "highest_ranked_results": rows,
            "negative_factors": [_negative_factors(row) for row in rows[:5]],
            "missing_data_warnings": result.get("missing_data") or [],
            "data_quality": result.get("data_quality") or {},
            "trend_config": definition.get("trend_config") or result.get("trend_config"),
            "ranking_preserved": True,
        })
    overlap = set.intersection(*entity_sets) if len(entity_sets) > 1 else set()
    return {
        "reports": reports,
        "report_count": len(reports),
        "overlap": sorted(overlap)[:25],
        "ranking_authority": "saved report execution engine; AI did not rerank",
        "row_limit_per_report": MAX_ANALYSIS_ROWS,
    }


def render_saved_report_answer(packet: Dict[str, Any]) -> str:
    lines = ["Saved-report analysis"]
    for report in packet.get("reports") or []:
        rows = report.get("highest_ranked_results") or []
        lines.append(
            f"\n{report.get('report_name')} ({report.get('report_type')}): "
            f"{report.get('matched_row_count')} matching rows."
        )
        lines.append(
            f"Purpose: {report.get('purpose') or 'Uses the saved fields, filters, weights, and ranking.'}"
        )
        filters = report.get("filters") or {}
        weights = report.get("scoring_weights") or {}
        lines.append(f"Controls: filters={filters}; weights={weights}; sort={report.get('sort')}.")
        for index, row in enumerate(rows[:5], 1):
            label = (
                row.get("player_name") or row.get("full_name") or row.get("entity_name")
                or row.get("pick_label") or row.get("team") or row.get("game_pk") or "Result"
            )
            reason_bits = []
            for key in ("model_score", "score", "absolute_change", "trend_direction", "confidence", "primary_reason"):
                if row.get(key) not in (None, ""):
                    reason_bits.append(f"{key}={row.get(key)}")
            lines.append(f"{index}. {label}" + (f" — {', '.join(reason_bits)}" if reason_bits else ""))
        warnings = list(report.get("missing_data_warnings") or [])
        if warnings:
            lines.append("Warnings: " + "; ".join(str(value) for value in warnings[:4]))
    overlap = packet.get("overlap") or []
    if len(packet.get("reports") or []) > 1:
        lines.append(
            "\nAgreement: " + (", ".join(overlap) if overlap else "No shared top-ranked entity IDs in the bounded report results.")
        )
    authority = packet.get("authoritative_context") or {}
    projection_edges = authority.get("projection_edges") or []
    lines.append(
        "\nAuthoritative support: "
        + (
            f"{len(projection_edges)} bounded model-projection result(s) are attached for comparison. "
            "Saved-report ranking, projection, simulation diagnostics, historical trend, odds, and lineup status remain separately labeled."
            if projection_edges
            else "No separate model-projection result was available in the bounded assistant packet."
        )
    )
    lines.append("\nRankings above are the report engine’s saved sort order; the assistant did not rerank them.")
    return "\n".join(lines)
