from __future__ import annotations

import datetime as dt
import math
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

from .dashboard_object_models import DashboardPlayer
from .database import StatcastEvent
from .dashboard_report_types import describe_report_type
from .db_utils import _calculate_batter_stats, _events_to_pitcher_df
from .my_dashboard_report_query import MAX_PAGE_SIZE, normalize_query
from .statcast_utils import calculate_pitcher_aggregates


METRICS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "hitter": {
        "batting_avg": {"label": "Batting Average", "favorable": "higher", "tolerance": .005},
        "on_base_pct": {"label": "On-Base Percentage", "favorable": "higher", "tolerance": .005},
        "slugging_pct": {"label": "Slugging Percentage", "favorable": "higher", "tolerance": .01},
        "iso": {"label": "ISO", "favorable": "higher", "tolerance": .01},
        "avg_exit_velocity": {"label": "Average Exit Velocity", "favorable": "higher", "tolerance": .5},
        "hard_hit_pct": {"label": "Hard-Hit Rate", "favorable": "higher", "tolerance": .01},
        "barrel_pct": {"label": "Barrel Rate", "favorable": "higher", "tolerance": .01},
        "k_pct": {"label": "Strikeout Rate", "favorable": "lower", "tolerance": .01},
        "bb_pct": {"label": "Walk Rate", "favorable": "higher", "tolerance": .01},
        "whiff_pct": {"label": "Whiff Rate", "favorable": "lower", "tolerance": .01},
    },
    "pitcher": {
        "k_pct": {"label": "Strikeout Rate", "favorable": "higher", "tolerance": .01},
        "bb_pct": {"label": "Walk Rate", "favorable": "lower", "tolerance": .01},
        "hard_hit_pct": {"label": "Hard-Hit Rate Allowed", "favorable": "lower", "tolerance": .01},
        "avg_velocity": {"label": "Average Pitch Velocity", "favorable": "higher", "tolerance": .3},
    },
}

BASELINES = {
    "season_to_date": "Season to date",
    "previous_n_days": "Previous N days",
}


def supported_trend_configuration() -> Dict[str, Any]:
    return {
        "player_types": ["hitter", "pitcher"],
        "window_presets": [7, 15, 30, 60],
        "window_min": 3,
        "window_max": 90,
        "baselines": [{"value": key, "label": label} for key, label in BASELINES.items()],
        "minimum_sample_units": {"hitter": "plate_appearances", "pitcher": "batters_faced"},
        "metrics": {
            player_type: [
                {"value": key, **metadata} for key, metadata in metrics.items()
            ]
            for player_type, metrics in METRICS.items()
        },
        "unsupported_baselines": {
            "prior_equivalent_period": (
                "Raw Statcast is date-bounded but the repository does not guarantee a complete "
                "authoritative prior-season equivalent period."
            )
        },
    }


def _validated_config(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    value = dict(config or {})
    player_type = str(value.get("player_type") or "").strip().lower()
    if player_type not in METRICS:
        raise ValueError("Player Trends requires player_type hitter or pitcher")
    try:
        days = int(value.get("window_days"))
    except (TypeError, ValueError) as exc:
        raise ValueError("Player Trends requires a numeric N-day window") from exc
    if days < 3 or days > 90:
        raise ValueError("Player Trends window_days must be between 3 and 90")
    baseline = str(value.get("comparison_baseline") or "").strip().lower()
    if baseline not in BASELINES:
        raise ValueError(
            "Unsupported Player Trends comparison_baseline; use season_to_date or previous_n_days"
        )
    try:
        minimum = int(value.get("minimum_sample_size"))
    except (TypeError, ValueError) as exc:
        raise ValueError("Player Trends requires a minimum sample size") from exc
    if minimum < 1 or minimum > 1000:
        raise ValueError("Player Trends minimum_sample_size must be between 1 and 1000")
    direction = str(value.get("trend_direction") or "all").strip().lower()
    if direction not in {"improving", "declining", "stable", "all"}:
        raise ValueError("Invalid Player Trends trend_direction")
    requested_metrics = value.get("selected_metrics")
    if not isinstance(requested_metrics, list) or not requested_metrics:
        raise ValueError("Player Trends requires at least one selected metric")
    metrics = list(dict.fromkeys(str(metric).strip() for metric in requested_metrics))
    unsupported = [metric for metric in metrics if metric not in METRICS[player_type]]
    if unsupported:
        raise ValueError(f"Unsupported {player_type} trend metric(s): {', '.join(unsupported)}")
    return {
        "player_type": player_type,
        "window_days": days,
        "comparison_baseline": baseline,
        "minimum_sample_size": minimum,
        "trend_direction": direction,
        "selected_metrics": metrics,
    }


def _date_ranges(as_of_date: dt.date, config: Dict[str, Any]) -> Dict[str, dt.date]:
    window_end = as_of_date
    window_start = window_end - dt.timedelta(days=config["window_days"] - 1)
    if config["comparison_baseline"] == "previous_n_days":
        baseline_end = window_start - dt.timedelta(days=1)
        baseline_start = baseline_end - dt.timedelta(days=config["window_days"] - 1)
    else:
        baseline_start = dt.date(as_of_date.year, 1, 1)
        baseline_end = as_of_date
    return {
        "window_start": window_start,
        "window_end": window_end,
        "baseline_start": baseline_start,
        "baseline_end": baseline_end,
    }


def _aggregate_period(session, player_type: str, start: dt.date, end: dt.date) -> Dict[int, Dict[str, Any]]:
    """Batch the same calculators used by /batter/{id}/rolling and /pitcher/{id}/rolling."""
    id_column = StatcastEvent.batter_id if player_type == "hitter" else StatcastEvent.pitcher_id
    events = (
        session.query(StatcastEvent)
        .filter(StatcastEvent.game_date >= start, StatcastEvent.game_date <= end)
        .order_by(id_column.asc(), StatcastEvent.game_date.asc())
        .all()
    )
    grouped: Dict[int, List[StatcastEvent]] = defaultdict(list)
    for event in events:
        grouped[int(event.batter_id if player_type == "hitter" else event.pitcher_id)].append(event)
    result: Dict[int, Dict[str, Any]] = {}
    for player_id, player_events in grouped.items():
        if player_type == "hitter":
            stats = _calculate_batter_stats(player_events, raw_event_count=len(player_events))
            stats["sample_size"] = int(stats.get("actual_pa") or 0)
        else:
            stats = calculate_pitcher_aggregates(_events_to_pitcher_df(player_events))
            stats["sample_size"] = sum(1 for event in player_events if event.events)
        stats["start_date"] = start.isoformat()
        stats["end_date"] = end.isoformat()
        result[player_id] = stats
    return result


def _classify(metric: str, player_type: str, change: float) -> str:
    metadata = METRICS[player_type][metric]
    if abs(change) <= metadata["tolerance"]:
        return "stable"
    favorable_change = change if metadata["favorable"] == "higher" else -change
    return "improving" if favorable_change > 0 else "declining"


def _matches_condition(row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
    field = str(condition.get("field") or "")
    operator = str(condition.get("operator") or "eq")
    actual = row.get(field)
    expected = condition.get("value")
    if operator == "is_null":
        return actual is None
    if operator == "is_not_null":
        return actual is not None
    if operator == "in":
        values = expected if isinstance(expected, list) else [item.strip() for item in str(expected).split(",")]
        return str(actual) in {str(item) for item in values}
    if operator == "contains":
        return str(expected).lower() in str(actual or "").lower()
    if operator in {"gt", "gte", "lt", "lte"}:
        try:
            left, right = float(actual), float(expected)
        except (TypeError, ValueError):
            return False
        return {"gt": left > right, "gte": left >= right, "lt": left < right, "lte": left <= right}[operator]
    return (actual != expected) if operator == "neq" else (str(actual) == str(expected))


def _apply_filters(rows: Iterable[Dict[str, Any]], filters: Any) -> List[Dict[str, Any]]:
    if isinstance(filters, list):
        conditions, logic = filters, "and"
    else:
        conditions = list((filters or {}).get("conditions") or [])
        logic = str((filters or {}).get("logic") or "and").lower()
    if not conditions:
        return list(rows)
    return [
        row for row in rows
        if (any(_matches_condition(row, condition) for condition in conditions)
            if logic == "or"
            else all(_matches_condition(row, condition) for condition in conditions))
    ]


def query_player_trends(
    session,
    *,
    as_of_date: dt.date,
    trend_config: Dict[str, Any],
    filters: Any = None,
    page_size: int = 50,
    page_number: int = 1,
    sort_by: str = "absolute_change",
    sort_direction: str = "desc",
    selected_fields: Optional[List[str]] = None,
    include_metadata: bool = True,
) -> Dict[str, Any]:
    config = _validated_config(trend_config)
    ranges = _date_ranges(as_of_date, config)
    window = _aggregate_period(session, config["player_type"], ranges["window_start"], ranges["window_end"])
    baseline = _aggregate_period(session, config["player_type"], ranges["baseline_start"], ranges["baseline_end"])
    player_ids = sorted(set(window).intersection(baseline))
    players = {}
    if player_ids:
        players = {
            int(player.mlb_player_id): player
            for player in session.query(DashboardPlayer).filter(DashboardPlayer.mlb_player_id.in_(player_ids)).all()
        }
    rows: List[Dict[str, Any]] = []
    missing_metric_pairs = 0
    for player_id in player_ids:
        current = window[player_id]
        comparison = baseline[player_id]
        if current["sample_size"] < config["minimum_sample_size"] or comparison["sample_size"] < config["minimum_sample_size"]:
            continue
        player = players.get(player_id)
        for metric in config["selected_metrics"]:
            current_value = current.get(metric)
            baseline_value = comparison.get(metric)
            if current_value is None or baseline_value is None:
                missing_metric_pairs += 1
                continue
            change = float(current_value) - float(baseline_value)
            direction = _classify(metric, config["player_type"], change)
            if config["trend_direction"] != "all" and direction != config["trend_direction"]:
                continue
            rows.append({
                "player_id": player_id,
                "player_name": getattr(player, "full_name", None) or str(player_id),
                "player_type": config["player_type"],
                "team": getattr(player, "current_team_name", None),
                "metric": metric,
                "metric_label": METRICS[config["player_type"]][metric]["label"],
                "selected_window_days": config["window_days"],
                "comparison_baseline": config["comparison_baseline"],
                "window_start": ranges["window_start"].isoformat(),
                "window_end": ranges["window_end"].isoformat(),
                "baseline_start": ranges["baseline_start"].isoformat(),
                "baseline_end": ranges["baseline_end"].isoformat(),
                "window_sample_size": current["sample_size"],
                "baseline_sample_size": comparison["sample_size"],
                "current_value": float(current_value),
                "baseline_value": float(baseline_value),
                "absolute_change": change,
                "percentage_change": (change / abs(float(baseline_value))) if float(baseline_value) != 0 else None,
                "trend_direction": direction,
                "favorable_direction": METRICS[config["player_type"]][metric]["favorable"],
                "freshness_date": as_of_date.isoformat(),
                "source": "statcast_events",
            })
    rows = _apply_filters(rows, filters)
    query = normalize_query(page_size, page_number, sort_by, sort_direction)
    rows.sort(
        key=lambda row: (row.get(query["sort_by"]) is None, row.get(query["sort_by"])),
        reverse=query["sort_direction"] == "desc",
    )
    for index, row in enumerate(rows, 1):
        row["rank"] = index
    total = len(rows)
    page_rows = rows[query["offset"]:query["offset"] + query["page_size"]]
    result = {
        "records": page_rows,
        "items": page_rows,
        "totalSize": total,
        "total_count": total,
        "done": query["offset"] + query["page_size"] >= total,
        "query": query,
        "trend_config": config,
        "supported_trend_configuration": supported_trend_configuration(),
        "provenance": {
            "source": "statcast_events",
            "requested_date": as_of_date.isoformat(),
            **{key: value.isoformat() for key, value in ranges.items()},
        },
        "data_quality": {
            "players_with_both_periods": len(player_ids),
            "missing_metric_pairs": missing_metric_pairs,
            "minimum_sample_size": config["minimum_sample_size"],
        },
        "page_info": {
            "page_number": query["page_number"],
            "page_size": query["page_size"],
            "page_count": math.ceil(total / query["page_size"]) if total else 0,
            "record_count": len(page_rows),
            "total_count": total,
            "has_next": query["offset"] + query["page_size"] < total,
            "has_previous": query["page_number"] > 1 and total > 0,
        },
    }
    if include_metadata:
        result["object_info"] = describe_report_type("player_trends")
    if selected_fields:
        result["selected_fields"] = list(selected_fields)
    return result
