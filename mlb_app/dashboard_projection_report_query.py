"""Read-only report adapters over the shared Model Projections artifact."""

from __future__ import annotations

import datetime as dt
import math
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .dashboard_report_types import FIELD_CATALOG, REPORT_TYPES, describe_report_type
from .model_projection_routes import get_model_projection_payload


REPORT_TYPES_SUPPORTED = {"model_projection_games", "model_projection_players"}
PLAYER_METRICS = (
    "plate_appearances", "singles", "doubles", "triples", "home_runs", "runs",
    "rbi", "rbis", "walks", "stolen_bases", "strikeouts", "batters_faced",
    "outs_recorded", "outs", "hits_allowed", "hits", "hit_by_pitch",
    "hit_batters", "runs_allowed", "earned_runs", "earned_runs_allowed",
)


def _object(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _metric_mean(row: Dict[str, Any], *names: str) -> Optional[float]:
    metrics = _object(row.get("metrics"))
    for name in names:
        metric = _object(metrics.get(name))
        value = metric.get("mean")
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def _projection_simulation(game: Dict[str, Any]) -> Dict[str, Any]:
    outputs = _object(_object(game.get("sharedSimulation")).get("derived_outputs"))
    return (
        _object(outputs.get("bullpen_adjusted_game_simulation"))
        or _object(outputs.get("game_simulation"))
        or _object(_object(game.get("workspace")).get("bullpenAdjustedGameSimulation"))
    )


def _path(value: Any, *parts: str) -> Any:
    current = value
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _projection_profiles(game: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    workspace = _object(game.get("workspace"))
    shared = _object(game.get("sharedSimulation"))
    direct = _object(shared.get("direct_inputs"))
    return {
        "away_pitcher": _object(direct.get("away_pitcher_profile") or workspace.get("awayPitcherProfile")),
        "home_pitcher": _object(direct.get("home_pitcher_profile") or workspace.get("homePitcherProfile")),
        "away_offense": _object(direct.get("away_offense_profile") or workspace.get("awayOffenseProfile")),
        "home_offense": _object(direct.get("home_offense_profile") or workspace.get("homeOffenseProfile")),
        "away_bullpen": _object(direct.get("away_bullpen_profile") or workspace.get("awayBullpenProfile")),
        "home_bullpen": _object(direct.get("home_bullpen_profile") or workspace.get("homeBullpenProfile")),
        "environment": _object(direct.get("environment_profile") or workspace.get("environmentProfile")),
        "away_matchup": _object(workspace.get("awayMatchupAnalysis")),
        "home_matchup": _object(workspace.get("homeMatchupAnalysis")),
    }


def _game_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for game in payload.get("games") or []:
        if not isinstance(game, dict):
            continue
        away_team = _object(game.get("away_team"))
        home_team = _object(game.get("home_team"))
        away_pitcher = _object(game.get("away_pitcher"))
        home_pitcher = _object(game.get("home_pitcher"))
        probability = _object(game.get("model_projection_probability") or game.get("probability"))
        simulation = _projection_simulation(game)
        profiles = _projection_profiles(game)
        shared_meta = _object(_object(game.get("sharedSimulation")).get("meta"))
        venue = game.get("venue")
        if isinstance(venue, dict):
            venue = venue.get("name")
        away_runs = simulation.get("away_expected_runs")
        if away_runs is None:
            away_runs = simulation.get("away_runs_mean")
        if away_runs is None:
            away_runs = simulation.get("projected_away_runs")
        home_runs = simulation.get("home_expected_runs")
        if home_runs is None:
            home_runs = simulation.get("home_runs_mean")
        if home_runs is None:
            home_runs = simulation.get("projected_home_runs")
        total = simulation.get("total_expected_runs")
        if total is None:
            total = simulation.get("total_runs_mean")
        if total is None:
            total = simulation.get("projected_total")
        if total is None and away_runs is not None and home_runs is not None:
            total = float(away_runs) + float(home_runs)
        totals = _object(
            simulation.get("calibrated_total_probabilities")
            or simulation.get("total_probabilities")
        )
        away_pitcher_profile = profiles["away_pitcher"]
        home_pitcher_profile = profiles["home_pitcher"]
        away_offense_profile = profiles["away_offense"]
        home_offense_profile = profiles["home_offense"]
        away_bullpen_profile = profiles["away_bullpen"]
        home_bullpen_profile = profiles["home_bullpen"]
        environment = profiles["environment"]
        rows.append({
            "game_pk": game.get("game_pk"),
            "game_date": game.get("game_date") or payload.get("date"),
            "game_time": game.get("game_time"),
            "status": game.get("status"),
            "venue": venue,
            "away_team_id": away_team.get("id"),
            "away_team_name": away_team.get("name"),
            "home_team_id": home_team.get("id"),
            "home_team_name": home_team.get("name"),
            "away_pitcher_id": away_pitcher.get("id"),
            "away_pitcher_name": away_pitcher.get("name"),
            "home_pitcher_id": home_pitcher.get("id"),
            "home_pitcher_name": home_pitcher.get("name"),
            "away_win_probability": probability.get("away_win_probability") or game.get("away_win_probability"),
            "home_win_probability": probability.get("home_win_probability") or game.get("home_win_probability"),
            "projected_away_runs": away_runs,
            "projected_home_runs": home_runs,
            "projected_total": total,
            "model_version": probability.get("model_version") or game.get("model_version"),
            "probability_source": game.get("probability_source") or probability.get("source"),
            "probability_is_fallback": game.get("probability_is_fallback") if game.get("probability_is_fallback") is not None else probability.get("is_fallback"),
            "lineup_status": game.get("lineup_status"),
            "data_confidence": game.get("data_confidence"),
            "away_starter_k_rate": _path(away_pitcher_profile, "bat_missing", "k_rate"),
            "away_starter_bb_rate": _path(away_pitcher_profile, "command_control", "bb_rate"),
            "away_starter_xwoba_allowed": _path(away_pitcher_profile, "contact_management", "xwoba_allowed"),
            "away_starter_hard_hit_rate_allowed": _path(away_pitcher_profile, "contact_management", "hard_hit_rate_allowed"),
            "home_starter_k_rate": _path(home_pitcher_profile, "bat_missing", "k_rate"),
            "home_starter_bb_rate": _path(home_pitcher_profile, "command_control", "bb_rate"),
            "home_starter_xwoba_allowed": _path(home_pitcher_profile, "contact_management", "xwoba_allowed"),
            "home_starter_hard_hit_rate_allowed": _path(home_pitcher_profile, "contact_management", "hard_hit_rate_allowed"),
            "away_offense_k_rate": _path(away_offense_profile, "contact_skill", "k_rate"),
            "away_offense_bb_rate": _path(away_offense_profile, "plate_discipline", "bb_rate"),
            "away_offense_obp": _path(away_offense_profile, "plate_discipline", "on_base_pct"),
            "away_offense_iso": _path(away_offense_profile, "power", "iso"),
            "away_offense_slg": _path(away_offense_profile, "power", "slugging_pct"),
            "home_offense_k_rate": _path(home_offense_profile, "contact_skill", "k_rate"),
            "home_offense_bb_rate": _path(home_offense_profile, "plate_discipline", "bb_rate"),
            "home_offense_obp": _path(home_offense_profile, "plate_discipline", "on_base_pct"),
            "home_offense_iso": _path(home_offense_profile, "power", "iso"),
            "home_offense_slg": _path(home_offense_profile, "power", "slugging_pct"),
            "away_bullpen_k_rate": _path(away_bullpen_profile, "bat_missing", "k_rate"),
            "away_bullpen_bb_rate": _path(away_bullpen_profile, "command_control", "bb_rate"),
            "away_bullpen_xwoba_allowed": _path(away_bullpen_profile, "contact_management", "xwoba_allowed"),
            "home_bullpen_k_rate": _path(home_bullpen_profile, "bat_missing", "k_rate"),
            "home_bullpen_bb_rate": _path(home_bullpen_profile, "command_control", "bb_rate"),
            "home_bullpen_xwoba_allowed": _path(home_bullpen_profile, "contact_management", "xwoba_allowed"),
            "run_scoring_index": _path(environment, "run_environment", "run_scoring_index"),
            "hr_boost_index": _path(environment, "run_environment", "hr_boost_index"),
            "hit_boost_index": _path(environment, "run_environment", "hit_boost_index"),
            "temperature_f": _path(environment, "weather", "temperature_f"),
            "weather_condition": _path(environment, "weather", "condition"),
            "wind_speed_mph": _path(environment, "weather", "wind_speed_mph"),
            "wind_direction": _path(environment, "weather", "wind_direction"),
            "away_matchup_biggest_edge": _path(profiles["away_matchup"], "summary", "biggest_edge"),
            "away_matchup_confidence": _path(profiles["away_matchup"], "summary", "confidence"),
            "home_matchup_biggest_edge": _path(profiles["home_matchup"], "summary", "biggest_edge"),
            "home_matchup_confidence": _path(profiles["home_matchup"], "summary", "confidence"),
            "simulation_count": shared_meta.get("simulation_count") or simulation.get("simulations") or _path(simulation, "metadata", "simulation_count"),
            "tie_after_regulation_probability": simulation.get("tie_after_regulation_probability"),
            "over_8_5_probability": totals.get("over_8.5"),
            "under_8_5_probability": totals.get("under_8.5"),
        })
    return rows


def _player_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for game in payload.get("games") or []:
        if not isinstance(game, dict):
            continue
        shared = _object(game.get("sharedSimulation"))
        shared_diagnostics = _object(shared.get("diagnostics"))
        top_diagnostics = _object(game.get("diagnostics"))
        shadow = (
            _object(top_diagnostics.get("canonical_shadow"))
            or _object(shared_diagnostics.get("canonical_shadow"))
        )
        projections = _object(shadow.get("player_projections"))
        for player in projections.get("players") or []:
            if not isinstance(player, dict):
                continue
            record = {
                "game_pk": game.get("game_pk"),
                "game_date": game.get("game_date") or payload.get("date"),
                "mlb_player_id": player.get("mlb_player_id") or player.get("player_id"),
                "full_name": player.get("full_name"),
                "player_type": player.get("player_type"),
                "team_side": player.get("team_side"),
                "team_id": player.get("team_id"),
                "team_name": player.get("team_name"),
                "primary_position": player.get("primary_position"),
                "projected_dfs_points": player.get("projected_dfs_points"),
                "dfs_floor": player.get("dfs_floor"),
                "dfs_median": player.get("dfs_median"),
                "dfs_ceiling": player.get("dfs_ceiling"),
                "simulation_count": player.get("simulation_count") or projections.get("simulation_count"),
            }
            for metric in PLAYER_METRICS:
                record[metric] = _metric_mean(player, metric)
            record["rbi"] = record.get("rbi") if record.get("rbi") is not None else record.get("rbis")
            record["outs_recorded"] = record.get("outs_recorded") if record.get("outs_recorded") is not None else record.get("outs")
            record["hits_allowed"] = record.get("hits_allowed") if record.get("hits_allowed") is not None else record.get("hits")
            record["hit_by_pitch"] = record.get("hit_by_pitch") if record.get("hit_by_pitch") is not None else record.get("hit_batters")
            record["runs_allowed"] = record.get("runs_allowed") if record.get("runs_allowed") is not None else record.get("runs")
            record["earned_runs"] = record.get("earned_runs") if record.get("earned_runs") is not None else record.get("earned_runs_allowed")
            rows.append(record)
    return rows


def _coerce(field: Dict[str, Any], value: Any) -> Any:
    data_type = field.get("data_type")
    if data_type in {"id", "integer"}:
        return int(value)
    if data_type in {"double", "number", "float"}:
        return float(value)
    if data_type == "boolean":
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
        raise ValueError(f"Invalid boolean value for field '{field['name']}'")
    return str(value)


def _matches(row: Dict[str, Any], field: Dict[str, Any], operator: str, value: Any) -> bool:
    current = row.get(field["name"])
    if operator == "is_null":
        return current is None
    if operator == "is_not_null":
        return current is not None
    if operator == "in":
        if not isinstance(value, (list, tuple, set)):
            raise ValueError(f"Operator 'in' for field '{field['name']}' requires a list")
        choices = [_coerce(field, item) for item in value]
        return current is not None and _coerce(field, current) in choices
    expected = _coerce(field, value)
    if current is None:
        return False
    actual = _coerce(field, current)
    if operator == "eq": return actual == expected
    if operator == "neq": return actual != expected
    if operator == "contains": return str(expected).lower() in str(actual).lower()
    if operator == "gt": return actual > expected
    if operator == "gte": return actual >= expected
    if operator == "lt": return actual < expected
    if operator == "lte": return actual <= expected
    raise ValueError(f"Unsupported operator: {operator}")


def _filter_rows(
    rows: List[Dict[str, Any]],
    report_type: str,
    filters: Any,
) -> Tuple[List[Dict[str, Any]], str, List[Dict[str, Any]]]:
    if filters is None:
        return rows, "and", []
    if isinstance(filters, list):
        logic, conditions = "and", filters
    elif isinstance(filters, dict):
        logic = str(filters.get("logic") or "and").strip().lower()
        conditions = list(filters.get("conditions") or [])
    else:
        raise ValueError("filters must be an object or list of conditions")
    if logic not in {"and", "or"}:
        raise ValueError("filters.logic must be 'and' or 'or'")
    catalog = {field["name"]: field for field in FIELD_CATALOG[report_type]}
    predicates = []
    for condition in conditions:
        if not isinstance(condition, dict):
            raise ValueError("Each filter condition must be an object")
        name = str(condition.get("field") or "")
        operator = str(condition.get("operator") or "eq").lower()
        field = catalog.get(name)
        if not field or not field.get("filterable"):
            raise ValueError(f"Unsupported filter field: {name}")
        if operator not in field.get("supported_operators", []):
            raise ValueError(f"Unsupported operator '{operator}' for field '{name}'")
        predicates.append((field, operator, condition.get("value")))
    if not predicates:
        return rows, logic, conditions
    matched = [
        row for row in rows
        if (any(_matches(row, *predicate) for predicate in predicates) if logic == "or"
            else all(_matches(row, *predicate) for predicate in predicates))
    ]
    return matched, logic, conditions


def _sort_key(value: Any) -> tuple:
    if value is None or value == "":
        return (1, 1, "")
    if isinstance(value, (int, float)):
        return (0, 0, value)
    return (0, 1, str(value).lower())


def query_projection_report(
    report_type: str,
    *,
    date: str,
    filters: Any = None,
    weights: Any = None,
    page_size: int = 50,
    page_number: int = 1,
    sort_by: Optional[str] = None,
    sort_direction: str = "desc",
    selected_fields: Optional[Iterable[str]] = None,
    include_metadata: bool = True,
) -> Dict[str, Any]:
    if report_type not in REPORT_TYPES_SUPPORTED or not REPORT_TYPES[report_type].get("queryable"):
        raise ValueError(f"Unsupported projection report type: {report_type}")
    if weights:
        raise ValueError("Weights are not supported for Model Projections report types")
    if not isinstance(page_size, int) or page_size < 1 or page_size > 250:
        raise ValueError("page_size must be between 1 and 250")
    if not isinstance(page_number, int) or page_number < 1:
        raise ValueError("page_number must be at least 1")
    direction = str(sort_direction).lower()
    if direction not in {"asc", "desc"}:
        raise ValueError("sort_direction must be 'asc' or 'desc'")
    target_date = dt.date.fromisoformat(str(date)[:10]).isoformat()
    catalog = {field["name"]: field for field in FIELD_CATALOG[report_type]}
    requested_fields = list(selected_fields or catalog)
    invalid = [
        name for name in requested_fields
        if name not in catalog or not catalog[name].get("selectable", True)
    ]
    if invalid:
        raise ValueError(f"Unsupported selected field(s): {', '.join(invalid)}")

    payload = get_model_projection_payload(target_date)
    source_rows = _game_rows(payload) if report_type == "model_projection_games" else _player_rows(payload)
    rows, logic, applied = _filter_rows(source_rows, report_type, filters)
    default_sort = "game_pk" if report_type == "model_projection_games" else "projected_dfs_points"
    sort_name = str(sort_by or default_sort)
    sort_field = catalog.get(sort_name)
    if not sort_field or not sort_field.get("sortable"):
        raise ValueError(f"Unsupported sort field: {sort_name}")
    present = [row for row in rows if row.get(sort_name) not in (None, "")]
    missing = [row for row in rows if row.get(sort_name) in (None, "")]
    present.sort(
        key=lambda row: (_sort_key(row.get(sort_name)), str(row.get("game_pk") or ""), str(row.get("mlb_player_id") or "")),
        reverse=direction == "desc",
    )
    missing.sort(key=lambda row: (str(row.get("game_pk") or ""), str(row.get("mlb_player_id") or "")))
    rows = present + missing
    total_count = len(rows)
    offset = (page_number - 1) * page_size
    page = rows[offset:offset + page_size]
    records = [{"rank": offset + index, **row} for index, row in enumerate(page, start=1)]
    page_count = math.ceil(total_count / page_size) if total_count else 0
    has_next = offset + len(records) < total_count
    result: Dict[str, Any] = {
        "report_type": report_type,
        "component": REPORT_TYPES[report_type]["ui_object"],
        "date": target_date,
        "records": records,
        "items": records,
        "totalSize": total_count,
        "total_count": total_count,
        "done": not has_next,
        "data_status": payload.get("data_status") or ("ready" if total_count else "not_ready"),
        "refreshing": bool(payload.get("refreshing")),
        "stale": bool(payload.get("stale")),
        "message": payload.get("message"),
        "filters_applied": applied,
        "filter_logic": logic,
        "query_source": "model_projection_date_artifact",
        "query": {
            "source": "model_projection_date_artifact",
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
        },
        "provenance": {
            "source_object": "model_projection_date_artifact",
            "artifact": payload.get("artifact"),
            "workspace_contract": payload.get("workspace_contract"),
            "probability_contract": payload.get("probability_contract"),
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        },
    }
    if include_metadata:
        result["object_info"] = describe_report_type(report_type)
    return result
