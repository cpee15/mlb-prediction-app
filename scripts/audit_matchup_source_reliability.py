from __future__ import annotations

import datetime as dt
import io
import json
import logging
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.environment_profile import compute_environment_profile
from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.park_factors import get_park_factor_profile
from mlb_app.simulation.game_engine_v2 import run_full_game_simulation


LINEUP_FAILURE_RE = re.compile(
    r"Confirmed (?P<side>home|away) lineup offense input failed; using team_splits fallback "
    r"for game_pk=(?P<game_pk>\S+) date=(?P<date>\S+) (?P<team_side>home|away)_team_id=(?P<team_id>\S+)"
)


def _date_range(start: str, end: str) -> Iterable[str]:
    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)
    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


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


def _source_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _matchup_label(matchup: Dict[str, Any]) -> str:
    away = matchup.get("away_team_name") or "Away"
    home = matchup.get("home_team_name") or "Home"
    return f"{away} @ {home}"


def _lineup_failure_key(date: str, game_pk: Any, side: str) -> str:
    return f"{date}:{game_pk}:{side}"


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@contextmanager
def _capture_reliability_logs():
    handler = _ListHandler()
    handler.setLevel(logging.WARNING)

    root = logging.getLogger()
    matchup_logger = logging.getLogger("mlb_app.matchup_generator")
    lineup_logger = logging.getLogger("mlb_app.lineup_profile")

    original_root_level = root.level
    original_matchup_level = matchup_logger.level
    original_lineup_level = lineup_logger.level

    root.addHandler(handler)
    matchup_logger.setLevel(logging.WARNING)
    lineup_logger.setLevel(logging.WARNING)

    try:
        yield handler.records
    finally:
        root.removeHandler(handler)
        root.setLevel(original_root_level)
        matchup_logger.setLevel(original_matchup_level)
        lineup_logger.setLevel(original_lineup_level)


def _parse_failure_logs(records: List[logging.LogRecord]) -> Dict[str, Dict[str, Any]]:
    failures: Dict[str, Dict[str, Any]] = {}

    for record in records:
        message = record.getMessage()
        match = LINEUP_FAILURE_RE.search(message)
        if not match:
            continue

        date = match.group("date")
        game_pk = match.group("game_pk")
        side = match.group("side")
        key = _lineup_failure_key(date, game_pk, side)

        exc_text = None
        exc_type = None
        if record.exc_info and record.exc_info[1]:
            exc = record.exc_info[1]
            exc_type = exc.__class__.__name__
            exc_text = str(exc)

        failures[key] = {
            "date": date,
            "game_pk": game_pk,
            "side": side,
            "team_id": match.group("team_id"),
            "message": message,
            "exception_type": exc_type,
            "exception_text": exc_text,
            "is_timeout": bool(
                "timeout" in (message or "").lower()
                or "timed out" in (exc_text or "").lower()
                or exc_type in {"ReadTimeout", "ReadTimeoutError", "TimeoutError"}
            ),
        }

    return failures


def _build_environment(matchup: Dict[str, Any]) -> Dict[str, Any]:
    venue = matchup.get("venue")
    profile = compute_environment_profile({
        "game_pk": matchup.get("game_pk"),
        "game_date": matchup.get("game_date"),
        "venue_name": venue,
        "venue": venue,
        "weather": matchup.get("weather") or {},
        "matchup": matchup,
    })
    return profile if isinstance(profile, dict) else {}


def _environment_summary(matchup: Dict[str, Any]) -> Dict[str, Any]:
    venue = matchup.get("venue")
    park_profile = get_park_factor_profile(venue)
    env_profile = _build_environment(matchup)

    components = _source_dict(env_profile.get("environment_components"))
    park_component = _source_dict(components.get("park_component"))
    weather = _source_dict(matchup.get("weather"))

    return {
        "environment_profile_source": (
            _source_dict(env_profile.get("metadata")).get("source")
            or "environment_profile.compute_environment_profile"
        ),
        "run_scoring_index": _round(
            env_profile.get("run_scoring_index")
            or _source_dict(env_profile.get("run_environment")).get("run_scoring_index")
        ),
        "hr_boost_index": _round(
            env_profile.get("hr_boost_index")
            or _source_dict(env_profile.get("run_environment")).get("hr_boost_index")
        ),
        "hit_boost_index": _round(
            env_profile.get("hit_boost_index")
            or _source_dict(env_profile.get("run_environment")).get("hit_boost_index")
        ),
        "park_factor_source": park_component.get("source") or park_profile.get("source"),
        "park_factor_profile_found": park_component.get("park_factor_profile_found", park_profile.get("park_factor_profile_found")),
        "venue_type": park_component.get("venue_type") or park_profile.get("venue_type"),
        "weather_available": bool(weather),
        "weather_temp": weather.get("temp") or weather.get("temp_f") or weather.get("temperature"),
        "weather_wind": weather.get("wind") or weather.get("wind_raw"),
    }


def _shared_simulation_summary(matchup: Dict[str, Any]) -> Dict[str, Any]:
    game_pk = matchup.get("game_pk")
    if not game_pk:
        return {"shared_status": "missing_game_pk"}

    try:
        payload = run_full_game_simulation(
            int(game_pk),
            config={
                "date": matchup.get("game_date"),
                "matchup": {
                    "raw": matchup,
                    "game_date": matchup.get("game_date"),
                },
                "simulation_count": 100,
                "seed": 42,
            },
        )
    except Exception as exc:
        return {
            "shared_status": "error",
            "shared_error": str(exc),
            "shared_error_type": exc.__class__.__name__,
        }

    return {
        "shared_status": payload.get("status"),
        "engine_version": _source_dict(payload.get("meta")).get("engine") or payload.get("engine_version"),
        "side_specific_environment_active": _source_dict(
            _source_dict(payload.get("direct_inputs")).get("side_specific_environment_diagnostics")
        ).get("active_model_input_changed"),
    }


def _side_row(
    *,
    audit_date: str,
    matchup: Dict[str, Any],
    side: str,
    failure_logs: Dict[str, Dict[str, Any]],
    game_summary: Dict[str, Any],
) -> Dict[str, Any]:
    offense_inputs = _source_dict(matchup.get(f"{side}_offense_inputs"))
    sample_blend = _source_dict(offense_inputs.get("sample_blend"))

    game_pk = matchup.get("game_pk")
    failure_key = _lineup_failure_key(audit_date, game_pk, side)
    failure = failure_logs.get(failure_key)

    source = offense_inputs.get("source")
    lineup_source = offense_inputs.get("lineup_source")
    profile_granularity = offense_inputs.get("profile_granularity")

    player_count_used = (
        offense_inputs.get("player_count_used")
        or sample_blend.get("lineup_players")
        or sample_blend.get("players")
    )
    fallback_player_count = (
        offense_inputs.get("fallback_player_count")
        or sample_blend.get("fallback_players")
    )
    real_player_profile_count = (
        offense_inputs.get("real_player_profile_count")
        or sample_blend.get("real_player_profiles")
    )

    lineup_handedness_mix = offense_inputs.get("lineup_handedness_mix")
    lineup_handedness_counts = offense_inputs.get("lineup_handedness_counts")
    if not lineup_handedness_counts and isinstance(lineup_handedness_mix, dict):
        lineup_handedness_counts = lineup_handedness_mix.get("counts")

    is_confirmed_lineup = source == "confirmed_lineup_player_splits"
    is_team_fallback = source == "team_splits" or lineup_source == "team_splits_fallback_not_confirmed_lineup"
    is_team_split_or_prior = source in {"team_splits", "missing_team_splits", "team_split_or_prior"}

    row = {
        "date": audit_date,
        "game_pk": game_pk,
        "matchup": _matchup_label(matchup),
        "side": side,
        "team_id": matchup.get(f"{side}_team_id"),
        "team_name": matchup.get(f"{side}_team_name"),
        "offense_input_source": source,
        "lineup_source": lineup_source,
        "profile_granularity": profile_granularity,
        "player_count_used": player_count_used,
        "real_player_profile_count": real_player_profile_count,
        "fallback_player_count": fallback_player_count,
        "lineup_handedness_mix_present": isinstance(lineup_handedness_mix, dict),
        "lineup_handedness_counts": lineup_handedness_counts,
        "lineup_handedness_coverage_rate": offense_inputs.get("lineup_handedness_coverage_rate"),
        "selected_split": offense_inputs.get("split"),
        "confirmed_lineup_player_splits_activated": is_confirmed_lineup,
        "team_splits_fallback_used": is_team_fallback,
        "source_was_team_split_or_prior": is_team_split_or_prior,
        "boxscore_lineup_fetch_succeeded": is_confirmed_lineup or bool(lineup_source and lineup_source != "team_splits_fallback_not_confirmed_lineup"),
        "boxscore_lineup_fetch_failed": bool(failure),
        "lineup_fallback_reason": offense_inputs.get("lineup_fallback_reason"),
        "lineup_fallback_stage": offense_inputs.get("lineup_fallback_stage"),
        "lineup_fetch_attempted": offense_inputs.get("lineup_fetch_attempted"),
        "lineup_fetch_succeeded": offense_inputs.get("lineup_fetch_succeeded"),
        "lineup_side_found": offense_inputs.get("lineup_side_found"),
        "starting_lineup_count": offense_inputs.get("starting_lineup_count"),
        "usable_hitter_profile_count": offense_inputs.get("usable_hitter_profile_count"),
        "min_usable_hitters": offense_inputs.get("min_usable_hitters"),
        "lineup_fetch_failure_reason": (
            offense_inputs.get("lineup_fetch_error_message")
            or (failure or {}).get("exception_text")
            or (failure or {}).get("message")
        ),
        "lineup_fetch_failure_type": (
            offense_inputs.get("lineup_fetch_error_type")
            or (failure or {}).get("exception_type")
        ),
        "lineup_fetch_timeout": bool(
            offense_inputs.get("lineup_fetch_timeout")
            or (failure or {}).get("is_timeout")
        ),
        "confirmed_lineup_inputs_would_activate": offense_inputs.get("confirmed_lineup_inputs_would_activate"),
        "sample_blend": sample_blend,
    }

    row.update(game_summary)
    return row


def _generate_for_date(session, audit_date: str) -> Dict[str, Any]:
    with _capture_reliability_logs() as records:
        matchups = generate_matchups_for_date(session, audit_date)

    failure_logs = _parse_failure_logs(records)

    rows: List[Dict[str, Any]] = []
    game_rows: List[Dict[str, Any]] = []

    for matchup in matchups:
        env_summary = _environment_summary(matchup)
        shared_summary = _shared_simulation_summary(matchup)

        game_summary = {
            "date": audit_date,
            "game_pk": matchup.get("game_pk"),
            "matchup": _matchup_label(matchup),
            "projection_status": "ok",
            "has_probable_pitchers": bool(matchup.get("home_pitcher_id") and matchup.get("away_pitcher_id")),
            **env_summary,
            **shared_summary,
        }
        game_rows.append(game_summary)

        for side in ("away", "home"):
            rows.append(
                _side_row(
                    audit_date=audit_date,
                    matchup=matchup,
                    side=side,
                    failure_logs=failure_logs,
                    game_summary=game_summary,
                )
            )

    return {
        "date": audit_date,
        "games": game_rows,
        "team_sides": rows,
        "lineup_failure_logs": list(failure_logs.values()),
        "raw_log_messages": [
            {
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            for record in records
            if record.levelno >= logging.WARNING
        ],
    }


def _count_by(rows: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        value = row.get(key)
        label = str(value if value is not None else "missing")
        counts[label] = counts.get(label, 0) + 1
    return counts


def _summarize_payload(payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    all_games = [game for payload in payloads for game in payload.get("games", [])]
    all_sides = [side for payload in payloads for side in payload.get("team_sides", [])]
    all_failures = [failure for payload in payloads for failure in payload.get("lineup_failure_logs", [])]

    fallback_rows = [row for row in all_sides if row.get("team_splits_fallback_used")]
    timeout_failures = [failure for failure in all_failures if failure.get("is_timeout")]
    not_ok_games = [game for game in all_games if game.get("shared_status") not in {None, "ok"}]

    by_date: Dict[str, Dict[str, Any]] = {}
    for payload in payloads:
        date = payload.get("date")
        date_sides = payload.get("team_sides", [])
        date_games = payload.get("games", [])
        date_failures = payload.get("lineup_failure_logs", [])
        by_date[date] = {
            "games": len(date_games),
            "team_sides": len(date_sides),
            "confirmed_lineup_player_splits_sides": sum(
                1 for row in date_sides if row.get("confirmed_lineup_player_splits_activated")
            ),
            "team_splits_fallback_sides": sum(
                1 for row in date_sides if row.get("team_splits_fallback_used")
            ),
            "lineup_fetch_failures": len(date_failures),
            "lineup_fetch_timeouts": sum(1 for failure in date_failures if failure.get("is_timeout")),
            "shared_status_counts": _count_by(date_games, "shared_status"),
        }

    return {
        "total_dates": len(payloads),
        "total_games": len(all_games),
        "total_team_sides": len(all_sides),
        "sides_using_confirmed_lineup_player_splits": sum(
            1 for row in all_sides if row.get("confirmed_lineup_player_splits_activated")
        ),
        "sides_using_team_splits_fallback": len(fallback_rows),
        "sides_missing_lineup_handedness_mix": sum(
            1 for row in all_sides if not row.get("lineup_handedness_mix_present")
        ),
        "games_with_any_confirmed_lineup_fetch_failure": len(
            set((failure.get("date"), failure.get("game_pk")) for failure in all_failures)
        ),
        "games_with_any_api_timeout_or_fetch_error": len(
            set((failure.get("date"), failure.get("game_pk")) for failure in timeout_failures)
        ),
        "lineup_fetch_failures": len(all_failures),
        "lineup_fetch_timeouts": len(timeout_failures),
        "games_with_projection_or_shared_status_not_ok": len(not_ok_games),
        "source_counts_by_offense_input_source": _count_by(all_sides, "offense_input_source"),
        "lineup_source_counts": _count_by(all_sides, "lineup_source"),
        "fallback_counts_by_reason": _count_by(fallback_rows, "lineup_fallback_reason"),
        "fallback_counts_by_stage": _count_by(fallback_rows, "lineup_fallback_stage"),
        "fallback_counts_by_lineup_source": _count_by(fallback_rows, "lineup_source"),
        "shared_status_counts": _count_by(all_games, "shared_status"),
        "by_date": by_date,
        "top_fallback_examples": fallback_rows[:20],
        "api_fetch_error_examples": all_failures[:20],
        "games_where_fallback_may_affect_tuning_reliability": [
            row for row in all_sides
            if row.get("team_splits_fallback_used") or row.get("boxscore_lineup_fetch_failed")
        ][:30],
    }


def main() -> None:
    audit_date = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()
    start = os.getenv("BACKTEST_START")
    end = os.getenv("BACKTEST_END")
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

    if start and end:
        dates = list(_date_range(start, end))
        output_name = f"matchup_source_reliability_{start}_to_{end}.json"
    else:
        dates = [audit_date]
        output_name = f"matchup_source_reliability_{audit_date}.json"

    engine = get_engine(database_url)
    create_tables(engine)
    SessionLocal = get_session(engine)
    session = SessionLocal()

    try:
        payloads = [_generate_for_date(session, date) for date in dates]
    finally:
        session.close()

    summary = _summarize_payload(payloads)
    output = {
        "audit_date": audit_date,
        "backtest_start": start,
        "backtest_end": end,
        "database_url": database_url,
        "summary": summary,
        "payloads": payloads,
    }

    Path("tmp").mkdir(exist_ok=True)
    output_path = Path("tmp") / output_name
    output_path.write_text(json.dumps(output, indent=2, default=str))

    print("=== MATCHUP SOURCE RELIABILITY AUDIT ===")
    if start and end:
        print(f"range: {start} to {end}")
    else:
        print(f"date: {audit_date}")
    print(f"database_url: {database_url}")
    print()
    print("=== SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))
    print()
    print("=== TOP FALLBACK EXAMPLES ===")
    for row in summary.get("top_fallback_examples", [])[:10]:
        print(json.dumps({
            "date": row.get("date"),
            "game_pk": row.get("game_pk"),
            "matchup": row.get("matchup"),
            "side": row.get("side"),
            "team_name": row.get("team_name"),
            "source": row.get("offense_input_source"),
            "lineup_source": row.get("lineup_source"),
            "fetch_failed": row.get("boxscore_lineup_fetch_failed"),
            "fallback_reason": row.get("lineup_fallback_reason"),
            "fallback_stage": row.get("lineup_fallback_stage"),
            "starting_lineup_count": row.get("starting_lineup_count"),
            "usable_hitter_profile_count": row.get("usable_hitter_profile_count"),
            "real_player_profile_count": row.get("real_player_profile_count"),
            "fallback_player_count": row.get("fallback_player_count"),
            "failure_type": row.get("lineup_fetch_failure_type"),
            "timeout": row.get("lineup_fetch_timeout"),
        }, default=str))
    print()
    print("=== API/FETCH ERROR EXAMPLES ===")
    for row in summary.get("api_fetch_error_examples", [])[:10]:
        print(json.dumps(row, default=str))
    print()
    print(f"Wrote JSON report to {output_path}")


if __name__ == "__main__":
    main()
