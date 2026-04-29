"""
Utilities for building environment profile summaries for matchup previews.

This module defines a game-level environment profile structure that can later
be populated with real weather, park factor, and contextual inputs.
"""


def compute_environment_profile(raw_context: dict) -> dict:
    """
    Build a structured environment profile from raw game context inputs.

    Parameters
    ----------
    raw_context : dict
        Dictionary of environmental and contextual stats from upstream
        ingestion or transformed sources.

    Returns
    -------
    dict
        A structured game-level environment profile using raw metrics grouped
        by category. Missing fields are returned as None.
    """
    raw_context = raw_context or {}
    weather = raw_context.get("weather") or {}

    def _safe_float(value):
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _park_label(run_factor):
        if run_factor is None:
            return None
        if run_factor >= 1.10:
            return "hitter_friendly"
        if run_factor >= 1.03:
            return "slight_hitter_friendly"
        if run_factor <= 0.92:
            return "pitcher_friendly"
        if run_factor <= 0.97:
            return "slight_pitcher_friendly"
        return "neutral"

    def _wind_direction_type(wind_direction):
        text = str(wind_direction or "").lower()
        if not text:
            return None
        if "in from" in text or "blowing in" in text or text.startswith("in "):
            return "in"
        if "out to" in text or "blowing out" in text or text.startswith("out "):
            return "out"
        if "cross" in text:
            return "cross"
        return "unknown"

    def _wind_speed_tier(wind_speed):
        if wind_speed is None:
            return None
        if wind_speed >= 15:
            return "strong"
        if wind_speed >= 10:
            return "moderate"
        if wind_speed >= 6:
            return "mild"
        return "calm"

    def _wind_adjustments(wind_speed, wind_direction):
        direction_type = _wind_direction_type(wind_direction)
        tier = _wind_speed_tier(wind_speed)

        base = {
            "calm": 0.0,
            "mild": 0.01,
            "moderate": 0.025,
            "strong": 0.04,
        }.get(tier, 0.0)

        direction_multiplier = {
            "out": 1.0,
            "in": -1.0,
            "cross": -0.25,
            "unknown": 0.0,
            None: 0.0,
        }.get(direction_type, 0.0)

        hr_adjustment = round(base * 1.50 * direction_multiplier, 3)
        run_adjustment = round(base * 0.85 * direction_multiplier, 3)
        hit_adjustment = round(base * 0.25 * direction_multiplier, 3)

        return {
            "wind_direction_type": direction_type,
            "wind_speed_tier": tier,
            "wind_hr_adjustment": hr_adjustment,
            "wind_run_adjustment": run_adjustment,
            "wind_hit_adjustment": hit_adjustment,
        }

    def _wind_impact(wind_speed, wind_direction):
        direction_type = _wind_direction_type(wind_direction)
        tier = _wind_speed_tier(wind_speed)
        if wind_speed is None:
            return None
        if direction_type == "in" and tier in {"moderate", "strong"}:
            return "wind_in_suppresses_carry"
        if direction_type == "out" and tier in {"moderate", "strong"}:
            return "wind_out_boosts_carry"
        if direction_type == "cross" and tier in {"moderate", "strong"}:
            return "crosswind_may_affect_carry"
        if tier in {"moderate", "strong"}:
            return "wind_may_affect_carry"
        if tier == "mild" and direction_type == "in":
            return "mild_wind_in_slight_suppression"
        if tier == "mild" and direction_type == "out":
            return "mild_wind_out_slight_boost"
        return "limited_wind_impact"

    def _temperature_impact(temp_f):
        if temp_f is None:
            return None
        if temp_f >= 85:
            return "hot_air_boosts_carry"
        if temp_f >= 75:
            return "warm_air_slight_boost"
        if temp_f <= 45:
            return "cold_air_strongly_suppresses_carry"
        if temp_f <= 55:
            return "cold_air_suppresses_carry"
        return "neutral_temperature"

    def _weather_impact(temp_f, wind_speed, wind_direction):
        wind = _wind_impact(wind_speed, wind_direction)
        temp = _temperature_impact(temp_f)

        if wind in {"wind_in_suppresses_carry", "wind_out_boosts_carry"}:
            return wind
        if temp in {"hot_air_boosts_carry", "cold_air_strongly_suppresses_carry", "cold_air_suppresses_carry"}:
            return temp
        if wind:
            return wind
        return temp or "neutral_or_unknown"

    def _run_scoring_index(run_factor, temp_f, wind_speed, wind_direction):
        score = run_factor if run_factor is not None else 1.0

        temp = _temperature_impact(temp_f)
        if temp == "hot_air_boosts_carry":
            score += 0.03
        elif temp == "warm_air_slight_boost":
            score += 0.01
        elif temp == "cold_air_strongly_suppresses_carry":
            score -= 0.04
        elif temp == "cold_air_suppresses_carry":
            score -= 0.02

        wind = _wind_impact(wind_speed, wind_direction)
        if wind == "wind_out_boosts_carry":
            score += 0.04
        elif wind == "wind_in_suppresses_carry":
            score -= 0.04
        elif wind == "crosswind_may_affect_carry":
            score -= 0.01

        return round(score, 3)

    def _scoring_label(index):
        if index is None:
            return None
        if index >= 1.08:
            return "offense_boost"
        if index >= 1.03:
            return "slight_offense_boost"
        if index <= 0.92:
            return "run_suppression"
        if index <= 0.97:
            return "slight_run_suppression"
        return "neutral"

    temperature_f = raw_context.get("temperature_f", weather.get("temp_f"))
    wind_speed_mph = raw_context.get("wind_speed_mph", weather.get("wind_speed_mph"))
    wind_direction = raw_context.get("wind_direction", weather.get("wind_direction") or weather.get("wind"))
    condition = raw_context.get("condition", weather.get("condition"))

    temperature_f = _safe_float(temperature_f)
    wind_speed_mph = _safe_float(wind_speed_mph)

    run_factor = raw_context.get("run_factor", raw_context.get("park_factor"))
    run_factor = _safe_float(run_factor)
    wind_adjustments = _wind_adjustments(wind_speed_mph, wind_direction)

    run_scoring_index = raw_context.get(
        "run_scoring_index",
        _run_scoring_index(run_factor, temperature_f, wind_speed_mph, wind_direction),
    )
    hr_boost_index = raw_context.get(
        "hr_boost_index",
        round((run_factor if run_factor is not None else 1.0) + wind_adjustments["wind_hr_adjustment"], 3),
    )
    hit_boost_index = raw_context.get(
        "hit_boost_index",
        round((run_factor if run_factor is not None else 1.0) + wind_adjustments["wind_hit_adjustment"], 3),
    )

    missing_inputs = []
    if temperature_f is None:
        missing_inputs.append("temperature_f")
    if wind_speed_mph is None and not wind_direction:
        missing_inputs.append("wind")
    if run_factor is None:
        missing_inputs.append("run_factor")

    source_fields_used = [
        key for key in [
            "game_pk",
            "game_date",
            "venue_name",
            "weather",
            "park_factor",
            "home_team",
            "away_team",
        ]
        if raw_context.get(key) is not None
    ]

    return {
        "metadata": {
            "source_type": raw_context.get("source_type", "matchup_detail_context"),
            "source_fields_used": raw_context.get("source_fields_used", source_fields_used),
            "data_confidence": raw_context.get(
                "data_confidence",
                "medium" if run_factor is not None or weather else "low",
            ),
            "generated_from": raw_context.get("generated_from", "compute_environment_profile"),
        },
        "weather": {
            "temperature_f": temperature_f,
            "condition": condition,
            "wind_speed_mph": wind_speed_mph,
            "wind_direction": wind_direction,
            "humidity_pct": raw_context.get("humidity_pct", weather.get("humidity_pct")),
            "precipitation_probability": raw_context.get(
                "precipitation_probability",
                weather.get("precipitation_probability"),
            ),
        },
        "park_factors": {
            "run_factor": run_factor,
            "home_run_factor": raw_context.get("home_run_factor"),
            "hit_factor": raw_context.get("hit_factor"),
        },
        "game_context": {
            "venue_name": raw_context.get("venue_name"),
            "roof_status": raw_context.get("roof_status"),
            "home_team": raw_context.get("home_team"),
            "away_team": raw_context.get("away_team"),
            "game_time_local": raw_context.get("game_time_local", raw_context.get("game_date")),
            "game_status": raw_context.get("game_status", raw_context.get("status")),
        },
        "run_environment": {
            "run_scoring_index": run_scoring_index,
            "hr_boost_index": hr_boost_index,
            "hit_boost_index": hit_boost_index,
            "scoring_environment_label": raw_context.get(
                "scoring_environment_label",
                _scoring_label(run_scoring_index),
            ),
            "weather_run_impact": raw_context.get(
                "weather_run_impact",
                _weather_impact(temperature_f, wind_speed_mph, wind_direction),
            ),
            "park_run_impact": raw_context.get(
                "park_run_impact",
                _park_label(run_factor),
            ),
            "wind_run_impact": raw_context.get(
                "wind_run_impact",
                _wind_impact(wind_speed_mph, wind_direction),
            ),
            "wind_direction_type": raw_context.get(
                "wind_direction_type",
                wind_adjustments["wind_direction_type"],
            ),
            "wind_speed_tier": raw_context.get(
                "wind_speed_tier",
                wind_adjustments["wind_speed_tier"],
            ),
            "wind_run_adjustment": raw_context.get(
                "wind_run_adjustment",
                wind_adjustments["wind_run_adjustment"],
            ),
            "wind_hr_adjustment": raw_context.get(
                "wind_hr_adjustment",
                wind_adjustments["wind_hr_adjustment"],
            ),
            "wind_hit_adjustment": raw_context.get(
                "wind_hit_adjustment",
                wind_adjustments["wind_hit_adjustment"],
            ),
            "temperature_run_impact": raw_context.get(
                "temperature_run_impact",
                _temperature_impact(temperature_f),
            ),
        },
        "risk_flags": {
            "rain_delay_risk": raw_context.get(
                "rain_delay_risk",
                "rain" in str(condition or "").lower(),
            ),
            "postponement_risk": raw_context.get("postponement_risk", False),
            "extreme_wind_flag": raw_context.get(
                "extreme_wind_flag",
                wind_speed_mph is not None and wind_speed_mph >= 15,
            ),
            "extreme_temperature_flag": raw_context.get(
                "extreme_temperature_flag",
                temperature_f is not None and (temperature_f <= 40 or temperature_f >= 95),
            ),
        },
        "status": {
            "is_stub": raw_context.get("is_stub", False),
            "readiness": raw_context.get(
                "readiness",
                "partial" if missing_inputs else "ready",
            ),
            "missing_inputs": raw_context.get("missing_inputs", missing_inputs),
        },
    }
