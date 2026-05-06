from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.model_projections import build_model_projection_payload


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


def _sub(a: Any, b: Any) -> Optional[float]:
    a_float = _safe_float(a)
    b_float = _safe_float(b)
    if a_float is None or b_float is None:
        return None
    return round(a_float - b_float, 4)


def _get_shared_simulation(game: Dict[str, Any]) -> Dict[str, Any]:
    shared = game.get("sharedSimulation")
    return shared if isinstance(shared, dict) else {}


def _get_direct_inputs(shared: Dict[str, Any]) -> Dict[str, Any]:
    direct = shared.get("direct_inputs")
    return direct if isinstance(direct, dict) else {}


def _get_side_environment(direct_inputs: Dict[str, Any], side: str) -> Dict[str, Any]:
    key = f"{side}_offense_environment_profile"
    value = direct_inputs.get(key)
    return value if isinstance(value, dict) else {}


def _get_side_offense_profile(direct_inputs: Dict[str, Any], side: str) -> Dict[str, Any]:
    key = f"{side}_offense_profile"
    value = direct_inputs.get(key)
    return value if isinstance(value, dict) else {}


def _get_park_component(environment_profile: Dict[str, Any]) -> Dict[str, Any]:
    components = environment_profile.get("environment_components")
    if not isinstance(components, dict):
        return {}
    park_component = components.get("park_component")
    return park_component if isinstance(park_component, dict) else {}


def _get_hr_diagnostics(environment_profile: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics = environment_profile.get("handedness_weighted_hr_diagnostics")
    return diagnostics if isinstance(diagnostics, dict) else {}


def _side_row(game: Dict[str, Any], side: str) -> Dict[str, Any]:
    shared = _get_shared_simulation(game)
    direct_inputs = _get_direct_inputs(shared)
    env = _get_side_environment(direct_inputs, side)
    offense_profile = _get_side_offense_profile(direct_inputs, side)
    park = _get_park_component(env)
    diagnostics = _get_hr_diagnostics(env)

    generic_hr = park.get("home_run_factor")
    lhb_hr = park.get("home_run_factor_lhb")
    rhb_hr = park.get("home_run_factor_rhb")

    matchup = (
        f"{((game.get('away_team') or {}).get('name') or 'Away')} @ "
        f"{((game.get('home_team') or {}).get('name') or 'Home')}"
    )

    handedness_counts = diagnostics.get("handedness_counts")
    if not isinstance(handedness_counts, dict):
        handedness_counts = offense_profile.get("lineup_handedness_counts")
    if not isinstance(handedness_counts, dict):
        mix = offense_profile.get("lineup_handedness_mix")
        if isinstance(mix, dict):
            handedness_counts = mix.get("counts")

    coverage_rate = diagnostics.get("handedness_coverage_rate")
    if coverage_rate is None:
        coverage_rate = offense_profile.get("lineup_handedness_coverage_rate")

    return {
        "game_pk": game.get("game_pk"),
        "matchup": matchup,
        "side": f"{side}_offense",
        "venue_name": game.get("venue") or ((shared.get("game_context") or {}).get("venue_name")),
        "shared_status": shared.get("status"),
        "generic_home_run_factor": _round(generic_hr),
        "home_run_factor_lhb": _round(lhb_hr),
        "home_run_factor_rhb": _round(rhb_hr),
        "lhb_minus_generic": _sub(lhb_hr, generic_hr),
        "rhb_minus_generic": _sub(rhb_hr, generic_hr),
        "lhb_minus_rhb": _sub(lhb_hr, rhb_hr),
        "lineup_handedness_counts": handedness_counts,
        "handedness_coverage_rate": _round(coverage_rate),
        "weighted_home_run_factor_raw": _round(diagnostics.get("weighted_home_run_factor_raw")),
        "handedness_adjustment_raw": _round(diagnostics.get("handedness_adjustment_raw")),
        "handedness_adjustment_final": _round(diagnostics.get("handedness_adjustment_final")),
        "base_hr_boost_index": _round(diagnostics.get("base_hr_boost_index")),
        "adjusted_hr_boost_index": _round(diagnostics.get("adjusted_hr_boost_index")),
        "active_model_input_changed": bool(diagnostics.get("active_model_input_changed")),
        "fallback_used": diagnostics.get("fallback_used"),
        "fallback_reason": diagnostics.get("fallback_reason"),
        "park_factor_source": park.get("source"),
        "normalized_venue_name": park.get("normalized_venue_name"),
    }


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    fallback_counts: Dict[str, int] = {}
    equal_venues = set()
    asymmetric_venues = set()
    adjustment_values = []
    abs_adjustments = []

    for row in rows:
        reason = row.get("fallback_reason")
        if reason:
            fallback_counts[reason] = fallback_counts.get(reason, 0) + 1

        generic = row.get("generic_home_run_factor")
        lhb = row.get("home_run_factor_lhb")
        rhb = row.get("home_run_factor_rhb")
        venue = row.get("venue_name") or row.get("normalized_venue_name") or "unknown"

        if generic is not None and lhb is not None and rhb is not None:
            if abs(lhb - generic) < 1e-9 and abs(rhb - generic) < 1e-9:
                equal_venues.add(venue)
            else:
                asymmetric_venues.add(venue)

        adjustment = row.get("handedness_adjustment_final")
        if adjustment is not None:
            adjustment_values.append(float(adjustment))
            abs_adjustments.append(abs(float(adjustment) - 1.0))

    active_rows = [row for row in rows if row.get("active_model_input_changed")]
    return {
        "total_offense_sides": len(rows),
        "sides_with_active_hook": len(active_rows),
        "sides_falling_back_due_to_missing_lineup_handedness": fallback_counts.get("missing_lineup_handedness_mix", 0),
        "sides_falling_back_due_to_missing_lr_park_factors": fallback_counts.get("missing_lhr_rhr_park_factors", 0),
        "fallback_reason_counts": fallback_counts,
        "venues_with_lhb_rhb_generic_all_equal": sorted(equal_venues),
        "venues_with_real_lr_asymmetry": sorted(asymmetric_venues),
        "venue_equal_count": len(equal_venues),
        "venue_asymmetry_count": len(asymmetric_venues),
        "handedness_adjustment_final_min": round(min(adjustment_values), 4) if adjustment_values else None,
        "handedness_adjustment_final_max": round(max(adjustment_values), 4) if adjustment_values else None,
        "average_absolute_adjustment": round(mean(abs_adjustments), 6) if abs_adjustments else None,
    }


def main() -> None:
    audit_date = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

    engine = get_engine(database_url)
    create_tables(engine)

    SessionLocal = get_session(engine)
    session = SessionLocal()
    try:
        payload = build_model_projection_payload(session, audit_date)
    finally:
        session.close()

    games = payload.get("games") or []
    rows: List[Dict[str, Any]] = []
    for game in games:
        rows.append(_side_row(game, "away"))
        rows.append(_side_row(game, "home"))

    summary = _summarize(rows)

    print("=== L/R PARK FACTOR READINESS AUDIT ===")
    print(f"date: {audit_date}")
    print(f"database_url: {database_url}")
    print(f"games: {len(games)}")
    print(f"offense sides: {len(rows)}")
    print()
    print("=== SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    print()
    print("=== SIDE EXAMPLES ===")
    for row in rows[:10]:
        print(
            json.dumps(
                {
                    "game_pk": row.get("game_pk"),
                    "matchup": row.get("matchup"),
                    "side": row.get("side"),
                    "venue_name": row.get("venue_name"),
                    "generic_home_run_factor": row.get("generic_home_run_factor"),
                    "home_run_factor_lhb": row.get("home_run_factor_lhb"),
                    "home_run_factor_rhb": row.get("home_run_factor_rhb"),
                    "lineup_handedness_counts": row.get("lineup_handedness_counts"),
                    "handedness_adjustment_final": row.get("handedness_adjustment_final"),
                    "active_model_input_changed": row.get("active_model_input_changed"),
                    "fallback_reason": row.get("fallback_reason"),
                },
                default=str,
            )
        )

    output = {
        "date": audit_date,
        "database_url": database_url,
        "summary": summary,
        "rows": rows,
    }

    Path("tmp").mkdir(exist_ok=True)
    output_path = Path("tmp") / f"lr_park_factor_readiness_{audit_date}.json"
    output_path.write_text(json.dumps(output, indent=2, default=str))
    print()
    print(f"Wrote JSON report to {output_path}")


if __name__ == "__main__":
    main()
