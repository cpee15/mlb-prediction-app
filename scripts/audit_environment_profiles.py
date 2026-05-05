from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional

from mlb_app.etl import fetch_schedule
from mlb_app.environment_profile import compute_environment_profile


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def get_nested(row: Dict[str, Any], *keys: str) -> Any:
    cur: Any = row
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def team_name(game: Dict[str, Any], side: str) -> Optional[str]:
    return get_nested(game, side, "team", "name")


def build_raw_context(game: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build environment raw_context from fetch_schedule().

    fetch_schedule() returns a teams dict with schedule metadata attached as:
      _game_pk, _game_date, _venue, _status, _weather
    """
    weather = game.get("_weather") or game.get("weather") or {}

    venue_name = (
        game.get("_venue")
        or game.get("venue_name")
        or get_nested(game, "venue", "name")
        or get_nested(game, "gameData", "venue", "name")
    )

    game_pk = game.get("_game_pk") or game.get("gamePk")
    game_date = game.get("_game_date") or game.get("gameDate")
    game_status = game.get("_status") or get_nested(game, "status", "detailedState")

    temperature_f = (
        game.get("temperature_f")
        or game.get("temp_f")
        or game.get("temperature")
        or weather.get("temperature_f")
        or weather.get("temp_f")
        or weather.get("temp")
        or weather.get("temperature")
    )

    wind_speed_mph = (
        game.get("wind_speed_mph")
        or weather.get("wind_speed_mph")
    )

    wind_direction = (
        game.get("wind_direction")
        or weather.get("wind_direction")
        or weather.get("wind")
    )

    condition = (
        game.get("condition")
        or weather.get("condition")
        or weather.get("conditions")
    )

    return {
        "source_type": "environment_audit_schedule_context",
        "generated_from": "scripts/audit_environment_profiles.py",
        "game_pk": game_pk,
        "game_date": game_date,
        "game_status": game_status,
        "venue_name": venue_name,
        "home_team": team_name(game, "home"),
        "away_team": team_name(game, "away"),
        "weather": weather,
        "temperature_f": temperature_f,
        "condition": condition,
        "wind_speed_mph": wind_speed_mph,
        "wind_direction": wind_direction,
        "humidity_pct": (
            game.get("humidity_pct")
            or weather.get("humidity_pct")
            or weather.get("humidity")
        ),
        "precipitation_probability": (
            game.get("precipitation_probability")
            or weather.get("precipitation_probability")
        ),
        "run_factor": (
            game.get("run_factor")
            or game.get("park_factor")
            or game.get("venue_run_factor")
            or get_nested(game, "park_factors", "run_factor")
            or get_nested(game, "parkFactors", "run_factor")
        ),
        "park_factor": (
            game.get("park_factor")
            or game.get("run_factor")
            or get_nested(game, "park_factors", "run_factor")
            or get_nested(game, "parkFactors", "run_factor")
        ),
        "home_run_factor": (
            game.get("home_run_factor")
            or game.get("hr_factor")
            or get_nested(game, "park_factors", "home_run_factor")
            or get_nested(game, "parkFactors", "home_run_factor")
        ),
        "hit_factor": (
            game.get("hit_factor")
            or get_nested(game, "park_factors", "hit_factor")
            or get_nested(game, "parkFactors", "hit_factor")
        ),
        "roof_status": game.get("roof_status") or game.get("roofStatus"),
        "source_fields_used": [
            key
            for key, value in {
                "_game_pk": game.get("_game_pk"),
                "_game_date": game.get("_game_date"),
                "_venue": game.get("_venue"),
                "_status": game.get("_status"),
                "_weather": game.get("_weather"),
                "temperature_f": temperature_f,
                "wind_direction": wind_direction,
                "wind_speed_mph": wind_speed_mph,
                "run_factor": game.get("run_factor"),
                "park_factor": game.get("park_factor"),
                "home_run_factor": game.get("home_run_factor"),
                "hit_factor": game.get("hit_factor"),
                "home_team": team_name(game, "home"),
                "away_team": team_name(game, "away"),
            }.items()
            if value is not None
        ],
    }


def audit_game(game: Dict[str, Any]) -> Dict[str, Any]:
    raw_context = build_raw_context(game)
    profile = compute_environment_profile(raw_context)

    metadata = profile.get("metadata") or {}
    weather = profile.get("weather") or {}
    park_factors = profile.get("park_factors") or {}
    run_environment = profile.get("run_environment") or {}
    status = profile.get("status") or {}
    risk_flags = profile.get("risk_flags") or {}
    components = profile.get("environment_components") or {}
    park_component = components.get("park_component") or {}
    weather_component = components.get("weather_component") or {}

    run_factor = park_factors.get("run_factor")
    temperature_f = weather.get("temperature_f")
    wind_speed_mph = weather.get("wind_speed_mph")
    wind_direction = weather.get("wind_direction")

    has_real_run_factor = run_factor is not None
    has_temperature = temperature_f is not None
    has_wind = wind_speed_mph is not None or bool(wind_direction)

    return {
        "game_pk": raw_context.get("game_pk"),
        "matchup": f"{raw_context.get('away_team')} @ {raw_context.get('home_team')}",
        "venue_name": raw_context.get("venue_name"),
        "game_status": raw_context.get("game_status"),
        "raw_context": raw_context,
        "environment_profile": profile,
        "audit": {
            "run_factor": run_factor,
            "home_run_factor": park_factors.get("home_run_factor"),
            "hit_factor": park_factors.get("hit_factor"),
            "has_real_run_factor": has_real_run_factor,
            "park_factor_source": park_component.get("source") or (
                "real_or_schedule_provided" if has_real_run_factor else "neutral_fallback"
            ),
            "park_factor_profile_found": park_component.get("park_factor_profile_found"),
            "static_park_factor_used": park_component.get("static_park_factor_used"),
            "normalized_venue_name": park_component.get("normalized_venue_name"),
            "venue_type": park_component.get("venue_type"),
            "default_roof_status": park_component.get("default_roof_status"),
            "weather_applies_default": park_component.get("weather_applies_default"),
            "neutral_park_fallback_used": park_component.get("neutral_fallback_used"),
            "temperature_f": temperature_f,
            "has_temperature": has_temperature,
            "temperature_source": "real_or_schedule_provided" if has_temperature else "missing",
            "wind_raw": metadata.get("wind_raw"),
            "wind_speed_mph": wind_speed_mph,
            "wind_direction": wind_direction,
            "has_wind": has_wind,
            "wind_direction_type": run_environment.get("wind_direction_type"),
            "wind_speed_tier": run_environment.get("wind_speed_tier"),
            "wind_parsed_from_text": metadata.get("wind_parsed_from_text"),
            "run_scoring_index": run_environment.get("run_scoring_index"),
            "hr_boost_index": run_environment.get("hr_boost_index"),
            "hit_boost_index": run_environment.get("hit_boost_index"),
            "weather_run_impact": run_environment.get("weather_run_impact"),
            "park_run_impact": run_environment.get("park_run_impact"),
            "temperature_adjustment": weather_component.get("temperature_adjustment"),
            "wind_run_adjustment": run_environment.get("wind_run_adjustment"),
            "wind_hr_adjustment": run_environment.get("wind_hr_adjustment"),
            "wind_hit_adjustment": run_environment.get("wind_hit_adjustment"),
            "weather_component_source": weather_component.get("source"),
            "missing_inputs": status.get("missing_inputs") or [],
            "readiness": status.get("readiness"),
            "extreme_wind_flag": risk_flags.get("extreme_wind_flag"),
            "extreme_temperature_flag": risk_flags.get("extreme_temperature_flag"),
            "rain_delay_risk": risk_flags.get("rain_delay_risk"),
            "park_factor_fallback_used": metadata.get("park_factor_fallback_used"),
            "park_factor_fallback_source": metadata.get("park_factor_fallback_source"),
            "environment_calibration_version": metadata.get("environment_calibration_version"),
            "combined_index_method": components.get("combined_index_method")
            or metadata.get("combined_index_method")
            or "additive_base_plus_weather_adjustment",
        },
    }


def min_max(values: List[Optional[float]]) -> Dict[str, Optional[float]]:
    clean = [safe_float(value) for value in values if safe_float(value) is not None]
    if not clean:
        return {"min": None, "max": None, "avg": None}
    return {
        "min": round(min(clean), 4),
        "max": round(max(clean), 4),
        "avg": round(sum(clean) / len(clean), 4),
    }


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    audits = [row.get("audit") or {} for row in rows]
    total = len(rows)

    def count_true(key: str) -> int:
        return sum(1 for row in audits if bool(row.get(key)))

    def count_missing_input(name: str) -> int:
        return sum(1 for row in audits if name in (row.get("missing_inputs") or []))

    def value_counts(key: str) -> Dict[str, int]:
        values = sorted({str(row.get(key)) for row in audits})
        return {
            value: sum(1 for row in audits if str(row.get(key)) == value)
            for value in values
        }

    return {
        "total_games": total,
        "games_with_any_park_factor": count_true("has_real_run_factor"),
        "games_with_raw_run_factor": sum(
            1 for row in audits
            if row.get("park_factor_source") == "real_or_schedule_provided"
        ),
        "games_with_static_park_factor": count_true("static_park_factor_used"),
        "games_with_neutral_park_fallback": count_true("neutral_park_fallback_used"),
        "games_defaulting_run_factor_to_neutral": count_true("neutral_park_fallback_used"),
        "games_with_temperature": count_true("has_temperature"),
        "games_missing_temperature": total - count_true("has_temperature"),
        "games_with_wind": count_true("has_wind"),
        "games_with_parsed_wind_from_text": count_true("wind_parsed_from_text"),
        "games_missing_wind": total - count_true("has_wind"),
        "missing_input_counts": {
            "run_factor": count_missing_input("run_factor"),
            "temperature_f": count_missing_input("temperature_f"),
            "wind": count_missing_input("wind"),
        },
        "index_ranges": {
            "run_scoring_index": min_max([row.get("run_scoring_index") for row in audits]),
            "hr_boost_index": min_max([row.get("hr_boost_index") for row in audits]),
            "hit_boost_index": min_max([row.get("hit_boost_index") for row in audits]),
        },
        "games_with_extreme_wind_flag": count_true("extreme_wind_flag"),
        "games_with_extreme_temperature_flag": count_true("extreme_temperature_flag"),
        "games_with_rain_delay_risk": count_true("rain_delay_risk"),
        "readiness_counts": value_counts("readiness"),
        "park_factor_sources": value_counts("park_factor_source"),
        "venue_types": value_counts("venue_type"),
        "default_roof_statuses": value_counts("default_roof_status"),
        "weather_applies_defaults": value_counts("weather_applies_default"),
        "unmapped_venues": sorted({
            row.get("venue_name")
            for row, audit in zip(rows, audits)
            if audit.get("neutral_park_fallback_used")
        }),
        "weather_component_sources": value_counts("weather_component_source"),
        "wind_direction_types": value_counts("wind_direction_type"),
        "combined_index_methods": value_counts("combined_index_method"),
    }


def print_game(row: Dict[str, Any]) -> None:
    audit = row.get("audit") or {}

    print("\n" + "=" * 100)
    print(f"{row.get('game_pk')} | {row.get('matchup')} | {row.get('game_status')}")
    print(f"venue: {row.get('venue_name')}")
    print(
        "park: "
        f"run_factor={audit.get('run_factor')} "
        f"hr_factor={audit.get('home_run_factor')} "
        f"hit_factor={audit.get('hit_factor')} "
        f"source={audit.get('park_factor_source')} "
        f"mapped={audit.get('park_factor_profile_found')} "
        f"static={audit.get('static_park_factor_used')} "
        f"venue_type={audit.get('venue_type')} "
        f"roof={audit.get('default_roof_status')} "
        f"weather_applies={audit.get('weather_applies_default')} "
        f"park_impact={audit.get('park_run_impact')}"
    )
    print(
        "weather: "
        f"temp={audit.get('temperature_f')} "
        f"source={audit.get('weather_component_source')} "
        f"wind_raw={audit.get('wind_raw')} "
        f"wind_speed={audit.get('wind_speed_mph')} "
        f"wind_dir={audit.get('wind_direction')} "
        f"wind_type={audit.get('wind_direction_type')} "
        f"wind_tier={audit.get('wind_speed_tier')} "
        f"parsed={audit.get('wind_parsed_from_text')}"
    )
    print(
        "indexes: "
        f"run={audit.get('run_scoring_index')} "
        f"hr={audit.get('hr_boost_index')} "
        f"hit={audit.get('hit_boost_index')} "
        f"method={audit.get('combined_index_method')}"
    )
    print(
        "impacts: "
        f"weather={audit.get('weather_run_impact')} "
        f"temp_adj={audit.get('temperature_adjustment')} "
        f"wind_adj(run/hr/hit)="
        f"{audit.get('wind_run_adjustment')}/"
        f"{audit.get('wind_hr_adjustment')}/"
        f"{audit.get('wind_hit_adjustment')}"
    )
    print(
        "status: "
        f"readiness={audit.get('readiness')} "
        f"missing={audit.get('missing_inputs')} "
        f"extreme_wind={audit.get('extreme_wind_flag')} "
        f"extreme_temp={audit.get('extreme_temperature_flag')} "
        f"rain_delay={audit.get('rain_delay_risk')}"
    )


def main() -> None:
    target_date = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()

    print("\n=== ENVIRONMENT PROFILE AUDIT ===")
    print(f"date: {target_date}")

    games = fetch_schedule(target_date)
    print(f"schedule_games: {len(games)}")

    rows = [audit_game(game) for game in games]
    summary = summarize(rows)

    for row in rows:
        print_game(row)

    print("\n" + "=" * 100)
    print("=== ENVIRONMENT PROFILE SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    os.makedirs("tmp", exist_ok=True)
    out_path = f"tmp/environment_profile_audit_{target_date}.json"
    with open(out_path, "w") as f:
        json.dump(
            {
                "date": target_date,
                "summary": summary,
                "games": rows,
            },
            f,
            indent=2,
            default=str,
        )

    print(f"\nWrote full JSON audit to {out_path}")


if __name__ == "__main__":
    main()
