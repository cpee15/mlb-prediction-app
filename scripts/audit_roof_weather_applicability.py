from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional

from mlb_app.etl import fetch_schedule
from mlb_app.environment_profile import compute_environment_profile
from mlb_app.park_factors import get_park_factor_profile


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _round(value: Any, ndigits: int = 4) -> Optional[float]:
    number = _safe_float(value)
    if number is None:
        return None
    return round(number, ndigits)


def _extract_game_pk(game: Dict[str, Any]) -> Any:
    return game.get("game_pk") or game.get("gamePk") or game.get("_game_pk")


def _extract_status(game: Dict[str, Any]) -> Any:
    return game.get("status") or game.get("game_status") or game.get("_status")


def _extract_venue_name(game: Dict[str, Any]) -> Optional[str]:
    venue = game.get("_venue") or game.get("venue") or game.get("venue_name")
    if isinstance(venue, dict):
        return venue.get("name") or venue.get("venue_name")
    if venue:
        return str(venue)
    return None


def _extract_weather(game: Dict[str, Any]) -> Dict[str, Any]:
    weather = game.get("_weather") or game.get("weather") or {}
    return weather if isinstance(weather, dict) else {}


def _team_name(value: Any) -> str:
    if isinstance(value, dict):
        return (
            value.get("name")
            or value.get("team_name")
            or value.get("abbreviation")
            or value.get("abbrev")
            or "Unknown"
        )
    return str(value or "Unknown")


def _matchup(game: Dict[str, Any]) -> str:
    away = game.get("away") or game.get("away_team") or game.get("awayTeam")
    home = game.get("home") or game.get("home_team") or game.get("homeTeam")
    return f"{_team_name(away)} @ {_team_name(home)}"


def _weather_temp(weather: Dict[str, Any]) -> Optional[float]:
    for key in ("temp", "temp_f", "temperature", "temperature_f"):
        value = _safe_float(weather.get(key))
        if value is not None:
            return value
    return None


def _weather_wind_raw(weather: Dict[str, Any]) -> Optional[str]:
    for key in ("wind", "wind_raw", "wind_text"):
        value = weather.get(key)
        if value:
            return str(value)
    return None


def _build_raw_context(game: Dict[str, Any]) -> Dict[str, Any]:
    weather = _extract_weather(game)
    venue_name = _extract_venue_name(game)
    return {
        "game_pk": _extract_game_pk(game),
        "game_date": game.get("_game_date") or game.get("game_date") or game.get("gameDate"),
        "venue_name": venue_name,
        "venue": venue_name,
        "weather": weather,
        "game_status": _extract_status(game),
        "matchup": game,
    }


def _get_component(profile: Dict[str, Any], component_name: str) -> Dict[str, Any]:
    components = profile.get("environment_components")
    if not isinstance(components, dict):
        return {}
    component = components.get(component_name)
    return component if isinstance(component, dict) else {}


def _index_delta(value: Any) -> Optional[float]:
    number = _safe_float(value)
    if number is None:
        return None
    return round(number - 1.0, 4)


def _weather_should_apply(weather_applies_default: Any, venue_type: str, default_roof_status: str) -> str:
    if weather_applies_default is True:
        return "yes"
    if weather_applies_default is False:
        return "no"
    if venue_type == "dome" or default_roof_status == "dome":
        return "no"
    if venue_type == "retractable" or default_roof_status == "unknown":
        return "unknown"
    return "unknown"


def _weather_appears_to_affect(row: Dict[str, Any]) -> bool:
    keys = (
        "weather_run_adjustment",
        "wind_run_adjustment",
        "wind_hr_adjustment",
        "wind_hit_adjustment",
    )
    for key in keys:
        value = _safe_float(row.get(key))
        if value is not None and abs(value) > 0.0001:
            return True

    # Fallback if diagnostics fields are missing: compare index deltas against park factors.
    for key in ("weather_run_impact", "weather_hr_impact", "weather_hit_impact"):
        value = _safe_float(row.get(key))
        if value is not None and abs(value) > 0.0001:
            return True

    return False


def _risk_flag(row: Dict[str, Any]) -> Optional[str]:
    venue_type = row.get("venue_type")
    default_roof_status = row.get("default_roof_status")
    weather_should_apply = row.get("weather_should_apply_by_venue_metadata")
    affected = bool(row.get("weather_currently_appears_to_affect_indexes"))

    if venue_type == "dome" and affected:
        return "dome_weather_adjustment_active"
    if weather_should_apply == "no" and affected:
        return "weather_adjustment_active_when_metadata_says_no"
    if venue_type == "retractable" and default_roof_status == "unknown" and affected:
        return "retractable_unknown_roof_weather_adjustment_active"
    if venue_type == "unknown" and affected:
        return "unknown_venue_type_weather_adjustment_active"
    return None


def _row_for_game(game: Dict[str, Any]) -> Dict[str, Any]:
    raw_context = _build_raw_context(game)
    profile = compute_environment_profile(raw_context)

    venue_name = raw_context.get("venue_name")
    park_profile = get_park_factor_profile(venue_name)
    park_component = _get_component(profile, "park_component")
    weather_component = _get_component(profile, "weather_component")

    run_env = profile.get("run_environment") if isinstance(profile.get("run_environment"), dict) else {}

    venue_type = (
        park_component.get("venue_type")
        or park_profile.get("venue_type")
        or "unknown"
    )
    default_roof_status = (
        park_component.get("default_roof_status")
        or park_profile.get("default_roof_status")
        or "unknown"
    )
    weather_applies_default = (
        park_component.get("weather_applies_default")
        if "weather_applies_default" in park_component
        else park_profile.get("weather_applies_default")
    )

    weather = raw_context.get("weather") or {}

    row = {
        "game_pk": raw_context.get("game_pk"),
        "matchup": _matchup(game),
        "venue_name": venue_name,
        "normalized_venue_name": park_profile.get("normalized_venue_name"),
        "venue_type": venue_type,
        "default_roof_status": default_roof_status,
        "weather_applies_default": weather_applies_default,
        "temperature_f": _round(
            weather_component.get("temperature_f")
            or weather_component.get("temperature")
            or _weather_temp(weather)
        ),
        "wind_raw": weather_component.get("wind_raw") or _weather_wind_raw(weather),
        "wind_speed_mph": _round(weather_component.get("wind_speed_mph")),
        "wind_direction": weather_component.get("wind_direction"),
        "wind_direction_type": weather_component.get("wind_direction_type"),
        "run_scoring_index": _round(profile.get("run_scoring_index") or run_env.get("run_scoring_index")),
        "hr_boost_index": _round(profile.get("hr_boost_index") or run_env.get("hr_boost_index")),
        "hit_boost_index": _round(profile.get("hit_boost_index") or run_env.get("hit_boost_index")),
        "weather_run_adjustment": _round(weather_component.get("temperature_adjustment")),
        "wind_run_adjustment": _round(weather_component.get("wind_run_adjustment")),
        "wind_hr_adjustment": _round(weather_component.get("wind_hr_adjustment")),
        "wind_hit_adjustment": _round(weather_component.get("wind_hit_adjustment")),
        "weather_run_impact": _index_delta(weather_component.get("run_index") or weather_component.get("run_factor")),
        "weather_hr_impact": _index_delta(weather_component.get("hr_index") or weather_component.get("hr_factor")),
        "weather_hit_impact": _index_delta(weather_component.get("hit_index") or weather_component.get("hit_factor")),
        "park_run_factor": _round(park_component.get("run_factor") or park_profile.get("run_factor")),
        "park_home_run_factor": _round(park_component.get("home_run_factor") or park_profile.get("home_run_factor")),
        "park_hit_factor": _round(park_component.get("hit_factor") or park_profile.get("hit_factor")),
        "park_factor_source": park_component.get("source") or park_profile.get("source"),
    }

    row["weather_should_apply_by_venue_metadata"] = _weather_should_apply(
        weather_applies_default,
        str(venue_type or "unknown"),
        str(default_roof_status or "unknown"),
    )
    row["weather_currently_appears_to_affect_indexes"] = _weather_appears_to_affect(row)
    row["roof_weather_risk_flag"] = _risk_flag(row)

    return row


def _avg_abs(values: List[Optional[float]]) -> Optional[float]:
    numbers = [abs(float(value)) for value in values if value is not None]
    if not numbers:
        return None
    return round(mean(numbers), 4)


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    venue_type_counts: Dict[str, int] = {}
    for row in rows:
        venue_type = row.get("venue_type") or "unknown"
        venue_type_counts[venue_type] = venue_type_counts.get(venue_type, 0) + 1

    dome_risks = [
        row for row in rows
        if row.get("venue_type") == "dome"
        and row.get("weather_currently_appears_to_affect_indexes")
    ]
    retractable_unknown_risks = [
        row for row in rows
        if row.get("venue_type") == "retractable"
        and row.get("default_roof_status") == "unknown"
        and row.get("weather_currently_appears_to_affect_indexes")
    ]

    by_venue_type: Dict[str, Dict[str, Any]] = {}
    for venue_type in sorted(venue_type_counts):
        subset = [row for row in rows if (row.get("venue_type") or "unknown") == venue_type]
        by_venue_type[venue_type] = {
            "games": len(subset),
            "avg_abs_weather_run_adjustment": _avg_abs([row.get("weather_run_adjustment") for row in subset]),
            "avg_abs_wind_run_adjustment": _avg_abs([row.get("wind_run_adjustment") for row in subset]),
            "avg_abs_wind_hr_adjustment": _avg_abs([row.get("wind_hr_adjustment") for row in subset]),
            "avg_abs_wind_hit_adjustment": _avg_abs([row.get("wind_hit_adjustment") for row in subset]),
            "weather_affecting_games": sum(
                1 for row in subset if row.get("weather_currently_appears_to_affect_indexes")
            ),
        }

    risky_rows = [row for row in rows if row.get("roof_weather_risk_flag")]
    highest_risk = sorted(
        risky_rows,
        key=lambda row: (
            abs(_safe_float(row.get("wind_hr_adjustment")) or 0.0)
            + abs(_safe_float(row.get("wind_run_adjustment")) or 0.0)
            + abs(_safe_float(row.get("weather_run_adjustment")) or 0.0)
        ),
        reverse=True,
    )[:10]

    return {
        "total_games": len(rows),
        "outdoor_games": venue_type_counts.get("outdoor", 0),
        "dome_games": venue_type_counts.get("dome", 0),
        "retractable_games": venue_type_counts.get("retractable", 0),
        "unknown_venue_type_games": venue_type_counts.get("unknown", 0),
        "venue_type_counts": venue_type_counts,
        "dome_games_with_non_neutral_weather_adjustments": len(dome_risks),
        "retractable_games_with_weather_adjustments_but_unknown_roof": len(retractable_unknown_risks),
        "games_where_weather_metadata_says_no_but_weather_affects_indexes": sum(
            1
            for row in rows
            if row.get("weather_should_apply_by_venue_metadata") == "no"
            and row.get("weather_currently_appears_to_affect_indexes")
        ),
        "average_weather_impact_by_venue_type": by_venue_type,
        "highest_roof_weather_risk_games": [
            {
                "game_pk": row.get("game_pk"),
                "matchup": row.get("matchup"),
                "venue_name": row.get("venue_name"),
                "venue_type": row.get("venue_type"),
                "default_roof_status": row.get("default_roof_status"),
                "weather_applies_default": row.get("weather_applies_default"),
                "temperature_f": row.get("temperature_f"),
                "wind_raw": row.get("wind_raw"),
                "wind_speed_mph": row.get("wind_speed_mph"),
                "wind_direction": row.get("wind_direction"),
                "weather_run_adjustment": row.get("weather_run_adjustment"),
                "wind_run_adjustment": row.get("wind_run_adjustment"),
                "wind_hr_adjustment": row.get("wind_hr_adjustment"),
                "wind_hit_adjustment": row.get("wind_hit_adjustment"),
                "roof_weather_risk_flag": row.get("roof_weather_risk_flag"),
            }
            for row in highest_risk
        ],
        "risk_flag_counts": {
            flag: sum(1 for row in rows if row.get("roof_weather_risk_flag") == flag)
            for flag in sorted({row.get("roof_weather_risk_flag") for row in rows if row.get("roof_weather_risk_flag")})
        },
    }


def main() -> None:
    audit_date = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

    games = fetch_schedule(audit_date)
    rows = [_row_for_game(game) for game in games]
    summary = _summarize(rows)

    print("=== ROOF / WEATHER APPLICABILITY AUDIT ===")
    print(f"date: {audit_date}")
    print(f"database_url: {database_url}")
    print(f"games: {len(rows)}")
    print()
    print("=== SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    print()
    print("=== GAME EXAMPLES ===")
    for row in rows[:12]:
        print(json.dumps(row, default=str))

    output = {
        "date": audit_date,
        "database_url": database_url,
        "summary": summary,
        "rows": rows,
    }

    Path("tmp").mkdir(exist_ok=True)
    output_path = Path("tmp") / f"roof_weather_applicability_{audit_date}.json"
    output_path.write_text(json.dumps(output, indent=2, default=str))
    print()
    print(f"Wrote JSON report to {output_path}")


if __name__ == "__main__":
    main()
