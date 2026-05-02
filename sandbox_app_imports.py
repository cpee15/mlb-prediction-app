"""
FastAPI application for the MLB prediction engine.

Endpoints:
    GET  /health
    GET  /matchups?date=YYYY-MM-DD
    GET  /matchup/{game_pk}              Full detail: pitchers, lineup, splits, game log
    GET  /matchup/{game_pk}/competitive  Lineup-level competitive matchup matrix
    GET  /pitcher/{id}                   Aggregate + arsenal
    GET  /pitcher/{id}/rolling           L15G-L150G rolling stats
    GET  /pitcher/{id}/game-log          Recent game-by-game appearances
    GET  /batter/{id}                    Aggregate + platoon splits (multi-season)
    GET  /batter/{id}/rolling            L10-L1000 AB rolling stats
    GET  /batter/{id}/splits             Multi-season vsL/vsR splits
    GET  /batter/{id}/at-bats            Chronological at-bat session
    GET  /standings                      MLB AL/NL standings
    GET  /lineup/{team_id}               Day-of lineup from MLB Stats API
    GET  /players/search                 Search players by name
    GET  /players/all                    All active MLB players for a season
    GET  /team/{team_id}/roster          Full roster for a team
    POST /predict                        Score a specific pitcher vs batter
"""

from __future__ import annotations

import datetime
import os
import re
from typing import Any, Dict, List, Optional

import requests as _req

try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
    _FASTAPI = True
except ImportError:
    FastAPI = None
    HTTPException = Exception
    CORSMiddleware = None
    BaseModel = object
    _FASTAPI = False

from .database import StatcastEvent, get_engine, create_tables, get_session
from .matchup_generator import generate_matchups_for_date
from .db_utils import (
    get_pitcher_aggregate,
    get_pitcher_aggregate_with_fallback,
    get_batter_aggregate,
    get_batter_aggregate_with_fallback,
    get_pitch_arsenal,
    get_pitch_arsenal_with_fallback,
    get_player_split,
    get_player_splits_multi_season,
    get_team_split,
    get_pitcher_rolling_by_games,
    get_batter_rolling_by_games,
    get_batter_rolling_by_abs,
    get_batter_at_bats,
    get_pitcher_game_log,
    get_pitcher_multi_season,
    get_batter_multi_season,
)
from .scoring import compute_win_probability, score_individual_matchup, get_park_factor
from .statcast_utils import fetch_pitch_arsenal_leaderboard, fetch_statcast_pitcher_data, fetch_statcast_batter_data
from .pitcher_profile import compute_pitcher_profile
from .offense_profile_aggregation import build_projected_lineup_offense_profile
from .environment_profile import compute_environment_profile
from .bullpen_profile import build_bullpen_profile
from .matchup_analysis import build_matchup_analysis
from .pitcher_advanced_metrics import derive_pitcher_advanced_metrics
from .simulation.pa_outcome_model import build_pa_outcome_probabilities
from .simulation.inning_simulator import simulate_half_innings
from .simulation.game_simulator import simulate_game, simulate_game_with_bullpen

MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
MATCHUP_SNAPSHOT_CACHE: Dict[str, List[Dict[str, Any]]] = {}

HIT_EVENTS = {"single", "double", "triple", "home_run"}
OUTCOME_EVENTS = {
    "single", "double", "triple", "home_run",
    "strikeout", "strikeout_double_play",
    "walk", "intent_walk", "hit_by_pitch",
    "field_out", "force_out", "double_play",
    "grounded_into_double_play", "fielders_choice",
    "fielders_choice_out", "sac_fly", "sac_bunt",
}


def _get_session():
    db_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(db_url)
    create_tables(engine)
    return get_session(engine)


def _fetch_team_splits_live(team_id: int, season: int) -> Dict[str, Any]:
    """Fetch vsL/vsR team hitting splits directly from MLB Stats API (statSplits)."""
    result = {"vsL": None, "vsR": None}
    for sit_code, key in [("vl", "vsL"), ("vr", "vsR")]:
        try:
            resp = _req.get(
                f"{MLB_STATS_BASE}/teams/{team_id}/stats",
                params={"stats": "statSplits", "group": "hitting", "season": season, "sitCodes": sit_code},
                timeout=15,
            )
            resp.raise_for_status()
            stats = resp.json().get("stats", [])
            splits = stats[0].get("splits", []) if stats else []
            if not splits:
                continue
            s = splits[0].get("stat", {})
            pa = s.get("plateAppearances") or 0
            k = s.get("strikeOuts") or 0
            bb = s.get("baseOnBalls") or 0
            result[key] = {
                "pa": pa,
                "batting_avg": _safe_float(s.get("avg")),
                "on_base_pct": _safe_float(s.get("obp")),
                "slugging_pct": _safe_float(s.get("slg")),
                "home_runs": s.get("homeRuns"),
                "k_pct": round(k / pa, 3) if pa > 0 else None,
                "bb_pct": round(bb / pa, 3) if pa > 0 else None,
            }
        except Exception:
            pass
    return result


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _statcast_batting_avg(events: List[StatcastEvent]) -> Optional[float]:
    if not events:
        return None
    pa = len(events)
    hits = sum(1 for e in events if e.events in HIT_EVENTS)
    return round(hits / pa, 3) if pa else None


def _average(values: List[Optional[float]], digits: int = 3) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return round(sum(nums) / len(nums), digits)


def _normalize_pitch_label(pitch_type: Optional[str], pitch_name: Optional[str]) -> str:
    return pitch_name or pitch_type or "Unknown"


def _extract_weather(game: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    w = game.get("weather") or {}
    condition = w.get("condition")
    temp = w.get("temp")
    wind = w.get("wind")
    if condition is None and temp is None and wind is None:
        return None
    return {"condition": condition, "temp_f": temp, "wind": wind}


def _normalize_rate(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if value > 1:
        return round(value / 100.0, 4)
    return round(value, 4)


def _build_date_window() -> Dict[str, str]:
    today = datetime.date.today()
    return {
        "yesterday": (today - datetime.timedelta(days=1)).isoformat(),
        "today": today.isoformat(),
