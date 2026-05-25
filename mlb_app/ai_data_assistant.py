from __future__ import annotations

import datetime as dt
import json
import math
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import or_

from .database import BatterPitchTypeMatchup
from .matchup_generator import generate_matchups_for_date
from .model_projections import build_model_projection_payload

PROMPT_CHIPS = [
    "Summarize today's slate",
    "What is the strongest model edge?",
    "Which games are missing data?",
    "Best Stored 365 hitter matchups",
    "Top pitcher leans",
    "Explain this matchup",
    "What does Daily Odds tell us?",
    "Which game has the cleanest signal?",
    "Show me the best hitter vs pitcher spots",
    "What data is stale or weak?",
]

AI_DATA_ASSISTANT_SYSTEM_PROMPT = """You are the MLB app's AI Data Assistant. You are an AI baseball brain, but you may only use the app data provided in the context. Do not use outside knowledge. Do not invent injuries, lineups, starters, odds, weather, stats, or betting information. If the app data is missing or weak, say that clearly. Keep answers concise, specific, and useful. Prefer model outputs, matchup facts, Stored 365 data, odds/model candidates, player profiles, and data-quality flags. Never call anything a guaranteed lock. Distinguish model leans from betting recommendations. Always include a short Data used section. Think like a sharp baseball analyst and betting market evaluator, but only from provided app data. Explain the why, not just the ranking. Separate model confidence from betting certainty. If no market price exists, call it a watchlist angle."""


def today() -> str:
    return dt.date.today().isoformat()


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_rate(value: Any) -> Optional[float]:
    num = safe_float(value)
    if num is None:
        return None
    if num > 1.5:
        return round(num / 100.0, 4)
    return round(num, 4)


def normalize_pitch_usage(value: Any) -> Optional[float]:
    return normalize_rate(value)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def nested_get(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = data
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(part)
        if cur is None:
            return default
    return cur


def classify_assistant_intent(message: str) -> str:
    text = re.sub(r"\s+", " ", (message or "").strip().lower())
    checks = [
        ("data_quality", ["missing", "stale", "weak data", "data quality", "refresh"]),
        ("stored_365_matchups", ["stored 365", "365", "batter vs arsenal", "pitch type", "hitter vs pitcher", "hitter matchups"]),
        ("odds_and_props", ["daily odds", "odds", "prop", "props", "sportsbook", "line"]),
        ("best_model_edges", ["strongest edge", "best edge", "cleanest signal", "top edge", "biggest mismatch", "model edge"]),
        ("pitcher_analysis", ["top pitcher", "pitcher lean", "pitcher leans", "pitcher prop", "starters", "dangerous arsenals", "best pitcher"]),
        ("hitter_analysis", ["hitter", "batter", "offense"]),
        ("bullpen_analysis", ["bullpen", "reliever", "relief"]),
        ("weather_environment", ["weather", "environment", "wind", "park", "temperature"]),
        ("lineup_analysis", ["lineup", "batting order"]),
        ("live_game_status", ["live", "score", "scoreboard", "in game", "status"]),
        ("game_explanation", ["explain", "why", "matchup", "game breakdown"]),
        ("daily_slate_summary", ["today", "slate", "summarize", "game-by-game", "games today", "daily"]),
    ]
    for intent, needles in checks:
        if any(needle in text for needle in needles):
            return intent
    return "unknown_app_question"


def date_from_message(message: str, explicit_date: Optional[str]) -> str:
    if explicit_date:
        return explicit_date[:10]
    base = dt.date.today()
    text = (message or "").lower()
    if "tomorrow" in text:
        return (base + dt.timedelta(days=1)).isoformat()
    if "yesterday" in text:
        return (base - dt.timedelta(days=1)).isoformat()
    return base.isoformat()


def projection_team_name(game: Dict[str, Any], side: str) -> Optional[str]:
    team = game.get(f"{side}_team") or {}
    return team.get("name") if isinstance(team, dict) else None


def projection_team_id(game: Dict[str, Any], side: str) -> Optional[int]:
    team = game.get(f"{side}_team") or {}
    return team.get("id") if isinstance(team, dict) else None


def projection_pitcher_name(game: Dict[str, Any], side: str) -> Optional[str]:
    pitcher = game.get(f"{side}_pitcher") or {}
    return pitcher.get("name") if isinstance(pitcher, dict) else None


def projection_pitcher_id(game: Dict[str, Any], side: str) -> Optional[int]:
    pitcher = game.get(f"{side}_pitcher") or {}
    return pitcher.get("id") if isinstance(pitcher, dict) else None


def team_name(game: Dict[str, Any], side: str) -> Optional[str]:
    return game.get(f"{side}_team_name") or game.get(f"{side}_team") or game.get(f"{side}_name") or projection_team_name(game, side)


def pitcher_name(game: Dict[str, Any], side: str) -> Optional[str]:
    return game.get(f"{side}_pitcher_name") or game.get(f"{side}_probable_pitcher") or game.get(f"{side}_starter_name") or projection_pitcher_name(game, side)


def game_label(game: Dict[str, Any]) -> str:
    return f"{team_name(game, 'away') or 'Away'} @ {team_name(game, 'home') or 'Home'}"


def projection_game_label(game: Dict[str, Any]) -> str:
    return f"{projection_team_name(game, 'away') or 'Away'} @ {projection_team_name(game, 'home') or 'Home'}"


def compact_game(game: Dict[str, Any]) -> Dict[str, Any]:
    home_prob = safe_float(game.get("home_win_prob") or game.get("home_win_probability"))
    away_prob = safe_float(game.get("away_win_prob") or game.get("away_win_probability"))
    favorite = None
    edge = None
    if home_prob is not None and away_prob is not None:
        favorite = team_name(game, "home") if home_prob >= away_prob else team_name(game, "away")
        edge = round(abs(home_prob - away_prob), 4)
    return {
        "game_pk": game.get("game_pk"),
        "label": game_label(game),
        "game_time": game.get("game_time") or game.get("game_time_utc") or game.get("start_time"),
        "away_team": team_name(game, "away"),
        "home_team": team_name(game, "home"),
        "away_pitcher": pitcher_name(game, "away"),
        "home_pitcher": pitcher_name(game, "home"),
        "away_pitcher_id": game.get("away_pitcher_id"),
        "home_pitcher_id": game.get("home_pitcher_id"),
        "away_win_prob": away_prob,
        "home_win_prob": home_prob,
        "model_favorite": favorite,
        "model_edge": edge,
        "weather": game.get("weather") or game.get("environment") or game.get("weather_profile"),
        "status": game.get("status") or game.get("game_status"),
    }


def missing_for_game(game: Dict[str, Any]) -> List[str]:
    missing: List[str] = []
    if not pitcher_name(game, "away"):
        missing.append("away_pitcher")
    if not pitcher_name(game, "home"):
        missing.append("home_pitcher")
    if safe_float(game.get("away_win_prob") or game.get("away_win_probability")) is None:
        missing.append("away_win_prob")
    if safe_float(game.get("home_win_prob") or game.get("home_win_probability")) is None:
        missing.append("home_win_prob")
    if not (game.get("weather") or game.get("environment") or game.get("weather_profile")):
        missing.append("weather")
    return missing


def quality_from_games(games: List[Dict[str, Any]], odds_payload: Optional[Dict[str, Any]] = None, stored_rows: Optional[int] = None) -> Dict[str, Any]:
    missing_by_game = []
    for game in games:
        missing = missing_for_game(game)
        if missing:
            missing_by_game.append({"game_pk": game.get("game_pk"), "label": game_label(game), "missing": missing})
    return {
        "game_count": len(games),
        "missing_pitcher_games": sum(1 for item in missing_by_game if "away_pitcher" in item["missing"] or "home_pitcher" in item["missing"]),
        "missing_weather_games": sum(1 for item in missing_by_game if "weather" in item["missing"]),
        "games_missing_model_probabilities": sum(1 for item in missing_by_game if "away_win_prob" in item["missing"] or "home_win_prob" in item["missing"]),
        "stored_365_rows_available": stored_rows,
        "odds_available": bool((odds_payload or {}).get("odds_event_count") or (odds_payload or {}).get("count")),
        "odds_event_count": (odds_payload or {}).get("odds_event_count"),
        "missing_by_game": missing_by_game[:10],
    }


def top_edges(games: List[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
    rows = [compact_game(game) for game in games]
    rows = [row for row in rows if row.get("model_edge") is not None]
    rows.sort(key=lambda row: row.get("model_edge") or 0, reverse=True)
    return rows[:limit]


# ---------------------------------------------------------------------------
# Model-projection-first intelligence
# ---------------------------------------------------------------------------


def model_card_summary(team_ctx: Dict[str, Any]) -> Dict[str, Any]:
    cards = team_ctx.get("models") or []
    summary: Dict[str, Any] = {"cards": [], "missing_inputs": []}
    for card in cards:
        if not isinstance(card, dict):
            continue
        name = card.get("model_name") or card.get("name") or "model_card"
        score = safe_float(card.get("score"))
        status = card.get("status")
        missing = card.get("missing_inputs") or []
        summary["cards"].append({"model_name": name, "score": score, "status": status, "missing_inputs": missing[:8] if isinstance(missing, list) else missing})
        if isinstance(missing, list):
            summary["missing_inputs"].extend([f"{name}.{item}" for item in missing])
    return summary


def extract_team_projection_signals(game: Dict[str, Any], side: str) -> Dict[str, Any]:
    teams = game.get("teams") or {}
    team = teams.get(side) or {}
    other = "home" if side == "away" else "away"
    opp = teams.get(other) or {}
    offense = team.get("offense_inputs") or {}
    pitcher = team.get("pitcher_features") or {}
    arsenal = team.get("pitch_arsenal") or {}
    card_summary = model_card_summary(team)
    return {
        "side": side,
        "team_id": team.get("team_id") or projection_team_id(game, side),
        "team_name": team.get("team_name") or projection_team_name(game, side),
        "pitcher_id": team.get("pitcher_id") or projection_pitcher_id(game, side),
        "pitcher_name": team.get("pitcher_name") or projection_pitcher_name(game, side),
        "opponent_team": opp.get("team_name") or projection_team_name(game, other),
        "opponent_pitcher": opp.get("pitcher_name") or projection_pitcher_name(game, other),
        "offense": {
            "source": offense.get("source"),
            "pa": safe_float(offense.get("pa")),
            "batting_avg": safe_float(offense.get("batting_avg")),
            "obp": safe_float(offense.get("on_base_pct")),
            "slg": safe_float(offense.get("slugging_pct")),
            "iso": safe_float(offense.get("iso")),
            "k_pct": normalize_rate(offense.get("k_pct")),
            "bb_pct": normalize_rate(offense.get("bb_pct")),
            "home_runs": safe_float(offense.get("home_runs")),
        },
        "pitcher": {
            "k_pct": normalize_rate(pitcher.get("k_pct")),
            "bb_pct": normalize_rate(pitcher.get("bb_pct")),
            "xwoba_allowed": safe_float(pitcher.get("xwoba")),
            "xba_allowed": safe_float(pitcher.get("xba")),
            "hard_hit_allowed": normalize_rate(pitcher.get("hard_hit_pct")),
            "avg_ev_allowed": safe_float(pitcher.get("avg_exit_velocity")),
            "avg_la_allowed": safe_float(pitcher.get("avg_launch_angle")),
        },
        "arsenal": {
            "pitch_count": len(arsenal) if isinstance(arsenal, dict) else 0,
            "top_pitches": top_pitch_rows(arsenal, limit=4),
        },
        "model_cards": card_summary,
    }


def top_pitch_rows(arsenal: Any, limit: int = 4) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(arsenal, dict):
        return rows
    for pitch_type, row in arsenal.items():
        if not isinstance(row, dict):
            continue
        rows.append({
            "pitch_type": row.get("pitch_type") or pitch_type,
            "pitch_name": row.get("pitch_name"),
            "usage_pct": normalize_pitch_usage(row.get("usage_pct")),
            "pitch_count": safe_int(row.get("pitch_count")),
            "whiff_pct": normalize_rate(row.get("whiff_pct")),
            "strikeout_pct": normalize_rate(row.get("strikeout_pct")),
            "xwoba_allowed": safe_float(row.get("xwoba")),
            "hard_hit_pct_allowed": normalize_rate(row.get("hard_hit_pct")),
            "rv_per_100": safe_float(row.get("rv_per_100")),
        })
    rows.sort(key=lambda x: (x.get("usage_pct") or 0, x.get("pitch_count") or 0), reverse=True)
    return rows[:limit]


def extract_total_projection_signals(game: Dict[str, Any]) -> Dict[str, Any]:
    sim = nested_get(game, "workspace.bullpenAdjustedGameSimulation", {}) or {}
    shared = game.get("sharedSimulation") or {}
    shared_outputs = shared.get("derived_outputs") or {}
    shared_sim = shared_outputs.get("bullpen_adjusted_game_simulation") or shared_outputs.get("game_simulation") or {}
    source = sim if sim else shared_sim
    environment = nested_get(game, "workspace.environmentProfile.run_environment", {}) or {}
    total_probs = source.get("calibrated_total_probabilities") or source.get("total_probabilities") or {}
    team_total_probs = source.get("calibrated_team_total_probabilities") or source.get("team_total_probabilities") or {}
    return {
        "total_expected_runs": safe_float(source.get("total_expected_runs")),
        "raw_total_expected_runs": safe_float(source.get("raw_total_expected_runs")),
        "away_expected_runs": safe_float(source.get("away_expected_runs")),
        "home_expected_runs": safe_float(source.get("home_expected_runs")),
        "total_probabilities": {key: safe_float(value) for key, value in list(total_probs.items())[:12]},
        "team_total_probabilities": {key: safe_float(value) for key, value in list(team_total_probs.items())[:12]},
        "environment_label": environment.get("scoring_environment_label"),
        "run_scoring_index": safe_float(environment.get("run_scoring_index")),
        "simulation_count": source.get("simulations") or nested_get(source, "metadata.simulation_count"),
        "source_available": bool(source),
    }


def extract_projection_game_signals(game: Dict[str, Any]) -> Dict[str, Any]:
    probs = game.get("main_matchup_probabilities") or {}
    away_prob = safe_float(probs.get("away_win_prob"))
    home_prob = safe_float(probs.get("home_win_prob"))
    legacy_away = safe_float(probs.get("legacy_away_win_prob"))
    legacy_home = safe_float(probs.get("legacy_home_win_prob"))
    side = None
    favorite = None
    win_edge = None
    if away_prob is not None and home_prob is not None:
        side = "away" if away_prob >= home_prob else "home"
        favorite = projection_team_name(game, side)
        win_edge = abs(away_prob - home_prob)
    totals = extract_total_projection_signals(game)
    run_diff = None
    if totals.get("away_expected_runs") is not None and totals.get("home_expected_runs") is not None:
        run_diff = abs(totals["away_expected_runs"] - totals["home_expected_runs"])
    raw_calibration_gap = None
    if totals.get("total_expected_runs") is not None and totals.get("raw_total_expected_runs") is not None:
        raw_calibration_gap = abs(totals["total_expected_runs"] - totals["raw_total_expected_runs"])
    missing = []
    for key, value in {
        "away_win_prob": away_prob,
        "home_win_prob": home_prob,
        "total_expected_runs": totals.get("total_expected_runs"),
        "away_expected_runs": totals.get("away_expected_runs"),
        "home_expected_runs": totals.get("home_expected_runs"),
        "environment": totals.get("environment_label") or totals.get("run_scoring_index"),
    }.items():
        if value is None:
            missing.append(key)
    return {
        "game_pk": game.get("game_pk"),
        "label": projection_game_label(game),
        "game_time": game.get("game_time"),
        "venue": game.get("venue"),
        "status": game.get("status"),
        "away_team": projection_team_name(game, "away"),
        "home_team": projection_team_name(game, "home"),
        "away_pitcher": projection_pitcher_name(game, "away"),
        "home_pitcher": projection_pitcher_name(game, "home"),
        "away_win_prob": away_prob,
        "home_win_prob": home_prob,
        "legacy_away_win_prob": legacy_away,
        "legacy_home_win_prob": legacy_home,
        "model_favorite_side": side,
        "model_favorite": favorite,
        "win_probability_edge": round(win_edge, 4) if win_edge is not None else None,
        "expected_run_differential": round(run_diff, 3) if run_diff is not None else None,
        "raw_calibration_gap": round(raw_calibration_gap, 3) if raw_calibration_gap is not None else None,
        "total_projection": totals,
        "away_signals": extract_team_projection_signals(game, "away"),
        "home_signals": extract_team_projection_signals(game, "home"),
        "missing_inputs": missing,
    }


def extract_prop_watchlist_signals(game: Dict[str, Any]) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    for side in ["away", "home"]:
        team = extract_team_projection_signals(game, side)
        pitcher = team.get("pitcher") or {}
        offense = team.get("offense") or {}
        k_pct = pitcher.get("k_pct")
        hard_hit = pitcher.get("hard_hit_allowed")
        xwoba = pitcher.get("xwoba_allowed")
        pitch_score = 0.0
        reasons = []
        if k_pct is not None:
            pitch_score += clamp((k_pct - 0.20) * 4.0, -0.5, 0.8)
            reasons.append(f"K% {k_pct}")
        if hard_hit is not None:
            pitch_score += clamp((0.39 - hard_hit) * 2.0, -0.4, 0.4)
            reasons.append(f"HardHit allowed {hard_hit}")
        if xwoba is not None:
            pitch_score += clamp((0.330 - xwoba) * 4.0, -0.5, 0.5)
            reasons.append(f"xwOBA allowed {xwoba}")
        if pitch_score > 0.15:
            signals.append({
                "type": "pitcher_prop_watchlist",
                "player_name": team.get("pitcher_name"),
                "team": team.get("team_name"),
                "opponent": team.get("opponent_team"),
                "score": round(pitch_score, 3),
                "angle": "strikeout/run-prevention watchlist, not a priced prop edge",
                "reasons": reasons[:5],
                "market_price_available": False,
                "note": "No sportsbook line is assumed from projections alone.",
            })
        if offense.get("iso") is not None and offense.get("iso") > 0.17:
            signals.append({
                "type": "team_power_watchlist",
                "team": team.get("team_name"),
                "score": round(offense.get("iso"), 3),
                "angle": "team total / extra-base-hit watchlist, not a priced prop edge",
                "reasons": [f"ISO {offense.get('iso')}", f"SLG {offense.get('slg')}", f"opposing pitcher {team.get('opponent_pitcher')}"],
                "market_price_available": False,
            })
    signals.sort(key=lambda row: row.get("score") or 0, reverse=True)
    return signals[:8]


def score_game_projection_edge(game: Dict[str, Any]) -> Dict[str, Any]:
    signals = extract_projection_game_signals(game)
    win_edge = signals.get("win_probability_edge") or 0.0
    run_diff = signals.get("expected_run_differential") or 0.0
    total_runs = nested_get(signals, "total_projection.total_expected_runs") or 0.0
    run_index = nested_get(signals, "total_projection.run_scoring_index")
    raw_gap = signals.get("raw_calibration_gap") or 0.0
    missing_count = len(signals.get("missing_inputs") or [])
    prop_signals = extract_prop_watchlist_signals(game)

    total_angle_score = 0.0
    if total_runs:
        total_angle_score += clamp(abs(total_runs - 8.5) / 4.0, 0.0, 0.65)
    if run_index is not None:
        total_angle_score += clamp(abs(run_index - 1.0), 0.0, 0.35)

    model_card_bonus = 0.0
    side = signals.get("model_favorite_side")
    side_signals = signals.get(f"{side}_signals") if side in {"away", "home"} else None
    if isinstance(side_signals, dict):
        cards = nested_get(side_signals, "model_cards.cards", []) or []
        calculated = sum(1 for card in cards if card.get("status") == "calculated")
        model_card_bonus = min(0.35, calculated * 0.06)

    score = (win_edge * 2.4) + (run_diff * 0.18) + total_angle_score + model_card_bonus + min(0.25, len(prop_signals) * 0.04) - (missing_count * 0.08) - min(0.25, raw_gap * 0.04)
    confidence = clamp(0.35 + win_edge + min(0.25, run_diff * 0.04) + model_card_bonus - missing_count * 0.05, 0.1, 0.92)
    if confidence >= 0.72:
        tier = "high"
    elif confidence >= 0.5:
        tier = "medium"
    else:
        tier = "low"
    return {
        "score": round(score, 3),
        "confidence": round(confidence, 3),
        "confidence_tier": tier,
        "signals": signals,
        "prop_watchlist": prop_signals,
        "why": build_edge_reasons(signals, prop_signals),
    }


def build_edge_reasons(signals: Dict[str, Any], prop_signals: List[Dict[str, Any]]) -> List[str]:
    reasons: List[str] = []
    if signals.get("model_favorite"):
        reasons.append(f"{signals.get('model_favorite')} owns the larger model win-probability side.")
    if signals.get("win_probability_edge") is not None:
        reasons.append(f"Win-probability separation is {signals.get('win_probability_edge')}.")
    if signals.get("expected_run_differential") is not None:
        reasons.append(f"Expected-run gap is {signals.get('expected_run_differential')} runs.")
    total_runs = nested_get(signals, "total_projection.total_expected_runs")
    env = nested_get(signals, "total_projection.environment_label")
    if total_runs is not None:
        reasons.append(f"Projected total is {total_runs}; environment label is {env or 'not labeled'}.")
    if prop_signals:
        reasons.append(f"Projection packet produced {len(prop_signals)} player/team watchlist angles; these are not priced prop edges without market lines.")
    return reasons[:6]


def score_projection_edges(projection_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    edges = []
    for game in projection_payload.get("games") or []:
        scored = score_game_projection_edge(game)
        signals = scored.get("signals") or {}
        edges.append({
            "game_pk": signals.get("game_pk"),
            "label": signals.get("label"),
            "model_favorite": signals.get("model_favorite"),
            "model_favorite_side": signals.get("model_favorite_side"),
            "score": scored.get("score"),
            "confidence": scored.get("confidence"),
            "confidence_tier": scored.get("confidence_tier"),
            "win_probability_edge": signals.get("win_probability_edge"),
            "expected_run_differential": signals.get("expected_run_differential"),
            "total_projection": signals.get("total_projection"),
            "prop_watchlist": scored.get("prop_watchlist"),
            "why": scored.get("why"),
            "missing_inputs": signals.get("missing_inputs") or [],
            "away_signals": signals.get("away_signals"),
            "home_signals": signals.get("home_signals"),
        })
    edges.sort(key=lambda row: row.get("score") or 0, reverse=True)
    return edges


def build_model_projection_intelligence_context(session, date: str) -> Dict[str, Any]:
    errors: List[str] = []
    projection_payload: Dict[str, Any] = {}
    try:
        projection_payload = build_model_projection_payload(session, date) or {}
    except Exception as exc:
        errors.append(f"model_projections_failed: {exc}")
        projection_payload = {}
    projection_edges = score_projection_edges(projection_payload) if projection_payload else []
    prop_watchlist = []
    for edge in projection_edges:
        prop_watchlist.extend(edge.get("prop_watchlist") or [])
    prop_watchlist.sort(key=lambda row: row.get("score") or 0, reverse=True)
    missing_fields = []
    for edge in projection_edges:
        if edge.get("missing_inputs"):
            missing_fields.append({"game_pk": edge.get("game_pk"), "label": edge.get("label"), "missing": edge.get("missing_inputs")})
    return {
        "intent": "best_model_edges",
        "date": date,
        "sources_used": ["model_projections", "simulation_outputs", "team_profiles", "pitcher_profiles", "bullpen_profiles", "environment_profiles"],
        "projection_count": projection_payload.get("count"),
        "projection_edges": projection_edges[:10],
        "top_edges": projection_edges[:10],
        "prop_watchlist": prop_watchlist[:10],
        "projection_errors": errors + [str(e) for e in projection_payload.get("errors") or []],
        "data_quality": {
            "projection_games": len(projection_payload.get("games") or []),
            "ranked_edges": len(projection_edges),
            "missing_projection_fields": missing_fields[:12],
            "projection_errors": errors + (projection_payload.get("errors") or []),
        },
        "missing_data": errors + missing_fields[:10],
    }


# ---------------------------------------------------------------------------
# Stored 365 Batter vs Arsenal sweep
# ---------------------------------------------------------------------------


def sample_confidence(sample_size: Any, pa: Any = None, pitches_seen: Any = None) -> float:
    sample = safe_float(sample_size) or safe_float(pitches_seen) or safe_float(pa) or 0.0
    pa_num = safe_float(pa) or 0.0
    pitch_num = safe_float(pitches_seen) or sample
    confidence = 0.15 + min(0.45, sample / 80.0) + min(0.25, pa_num / 40.0) + min(0.15, pitch_num / 160.0)
    return round(clamp(confidence, 0.05, 1.0), 3)


def freshness_penalty(target_date: str, row_date: Any, refreshed_at: Any = None) -> float:
    try:
        target = dt.date.fromisoformat(str(target_date)[:10])
        if row_date is None:
            return 0.12
        row_d = row_date if isinstance(row_date, dt.date) else dt.date.fromisoformat(str(row_date)[:10])
        days = abs((target - row_d).days)
        return clamp(days / 60.0, 0.0, 0.35)
    except Exception:
        return 0.12


def pitch_lookup_from_projection_payload(projection_payload: Dict[str, Any]) -> Dict[Tuple[int, str], Dict[str, Any]]:
    lookup: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for game in projection_payload.get("games") or []:
        for side in ["away", "home"]:
            team = (game.get("teams") or {}).get(side) or {}
            pitcher_id = safe_int(team.get("pitcher_id") or projection_pitcher_id(game, side))
            arsenal = team.get("pitch_arsenal") or {}
            if not pitcher_id or not isinstance(arsenal, dict):
                continue
            for pitch_type, row in arsenal.items():
                if not isinstance(row, dict):
                    continue
                normalized = {
                    "pitcher_id": pitcher_id,
                    "pitcher_name": team.get("pitcher_name") or projection_pitcher_name(game, side),
                    "pitch_type": row.get("pitch_type") or pitch_type,
                    "pitch_name": row.get("pitch_name"),
                    "usage_pct": normalize_pitch_usage(row.get("usage_pct")),
                    "pitch_count": safe_int(row.get("pitch_count")),
                    "whiff_pct": normalize_rate(row.get("whiff_pct")),
                    "strikeout_pct": normalize_rate(row.get("strikeout_pct")),
                    "xwoba_allowed": safe_float(row.get("xwoba")),
                    "hard_hit_pct_allowed": normalize_rate(row.get("hard_hit_pct")),
                    "rv_per_100": safe_float(row.get("rv_per_100")),
                    "team": team.get("team_name") or projection_team_name(game, side),
                    "game_pk": game.get("game_pk"),
                    "game_label": projection_game_label(game),
                    "arsenal_source": team.get("pitch_arsenal_source"),
                }
                lookup[(pitcher_id, str(normalized["pitch_type"]))] = normalized
    return lookup


def row_value(row: Any, attr: str) -> Any:
    return getattr(row, attr, None)


def score_batter_pitch_matchup(row: Any, pitcher_pitch_row: Optional[Dict[str, Any]], target_date: Optional[str] = None) -> Dict[str, Any]:
    usage = normalize_pitch_usage((pitcher_pitch_row or {}).get("usage_pct"))
    hitter_xwoba = safe_float(row_value(row, "xwoba"))
    hitter_xba = safe_float(row_value(row, "xba"))
    ev = safe_float(row_value(row, "avg_exit_velocity") or row_value(row, "avg_ev"))
    la = safe_float(row_value(row, "avg_launch_angle") or row_value(row, "avg_la"))
    hard_hit = normalize_rate(row_value(row, "hard_hit_pct") or row_value(row, "hardhit_pct"))
    whiff = normalize_rate(row_value(row, "whiff_pct"))
    k_pct = normalize_rate(row_value(row, "k_pct"))
    pa = safe_int(row_value(row, "pa_ended") or row_value(row, "pa"))
    pitches_seen = safe_int(row_value(row, "pitches_seen"))
    sample = pitches_seen or pa or safe_int(row_value(row, "deduped_rows")) or 0
    confidence = sample_confidence(sample, pa, pitches_seen)
    pitcher_xwoba = safe_float((pitcher_pitch_row or {}).get("xwoba_allowed"))
    pitcher_hard_hit = normalize_rate((pitcher_pitch_row or {}).get("hard_hit_pct_allowed"))
    pitcher_whiff = normalize_rate((pitcher_pitch_row or {}).get("whiff_pct"))
    duplicate_removed = safe_int(row_value(row, "duplicate_rows_removed")) or 0
    fresh_penalty = freshness_penalty(target_date or today(), row_value(row, "target_date"), row_value(row, "refreshed_at"))

    score = 0.0
    reasons: List[str] = []
    missing: List[str] = []
    if usage is not None:
        score += usage * 1.2
        reasons.append(f"pitch usage {round(usage, 3)}")
    else:
        missing.append("pitcher_pitch_usage")
    if hitter_xwoba is not None:
        score += clamp((hitter_xwoba - 0.320) * 3.2, -0.35, 1.1)
        reasons.append(f"hitter xwOBA {hitter_xwoba}")
    else:
        missing.append("hitter_xwoba")
    if hitter_xba is not None:
        score += clamp((hitter_xba - 0.245) * 1.5, -0.2, 0.55)
    if ev is not None:
        score += clamp((ev - 88.0) / 16.0, -0.25, 0.8)
        reasons.append(f"EV {ev}")
    else:
        missing.append("avg_exit_velocity")
    if la is not None:
        score += clamp(1.0 - abs(la - 16.0) / 22.0, 0.0, 0.35)
        reasons.append(f"LA {la}")
    if hard_hit is not None:
        score += clamp((hard_hit - 0.36) * 1.25, -0.2, 0.45)
        reasons.append(f"HardHit {hard_hit}")
    if pitcher_xwoba is not None:
        score += clamp((pitcher_xwoba - 0.320) * 2.0, -0.25, 0.65)
        reasons.append(f"pitcher xwOBA allowed {pitcher_xwoba}")
    else:
        missing.append("pitcher_xwoba_allowed_by_pitch")
    if pitcher_hard_hit is not None:
        score += clamp((pitcher_hard_hit - 0.36) * 0.85, -0.2, 0.35)
    if pitcher_whiff is not None:
        score -= clamp((pitcher_whiff - 0.25) * 0.9, -0.15, 0.45)
        if pitcher_whiff > 0.32:
            reasons.append(f"risk: pitcher whiff pitch {pitcher_whiff}")
    if whiff is not None and whiff > 0.34:
        score -= 0.15
    if k_pct is not None and k_pct > 0.28:
        score -= 0.12
    score *= clamp(0.45 + confidence, 0.45, 1.25)
    score -= min(0.15, duplicate_removed * 0.005)
    score -= fresh_penalty
    tier = "high" if confidence >= 0.72 and len(missing) <= 1 else "medium" if confidence >= 0.45 else "low"
    return {
        "score": round(score, 3),
        "confidence": confidence,
        "confidence_tier": tier,
        "reasons": reasons[:7],
        "missing_inputs": missing,
        "sample_size": sample,
        "freshness_penalty": round(fresh_penalty, 3),
    }


def build_stored_365_sweep_context(session, date: str) -> Dict[str, Any]:
    errors: List[str] = []
    projection_payload: Dict[str, Any] = {}
    try:
        projection_payload = build_model_projection_payload(session, date) or {}
    except Exception as exc:
        errors.append(f"projection_payload_unavailable_for_arsenal_join: {exc}")
    pitch_lookup = pitch_lookup_from_projection_payload(projection_payload)

    rows: List[Any] = []
    try:
        parsed = dt.date.fromisoformat(date[:10])
        rows = (
            session.query(BatterPitchTypeMatchup)
            .filter(or_(BatterPitchTypeMatchup.target_date == parsed, BatterPitchTypeMatchup.target_date.is_(None)))
            .limit(1500)
            .all()
        )
    except Exception as exc:
        errors.append(str(exc))

    scored_rows: List[Dict[str, Any]] = []
    for row in rows:
        pitcher_id = safe_int(row_value(row, "opposing_pitcher_id"))
        pitch_type = row_value(row, "pitch_type")
        pitcher_pitch = pitch_lookup.get((pitcher_id, str(pitch_type))) or {}
        score = score_batter_pitch_matchup(row, pitcher_pitch, target_date=date)
        scored_rows.append({
            "rank_score": score.get("score"),
            "confidence": score.get("confidence"),
            "confidence_tier": score.get("confidence_tier"),
            "reasons": score.get("reasons"),
            "missing_inputs": score.get("missing_inputs"),
            "sample_size": score.get("sample_size"),
            "batter_id": row_value(row, "batter_id"),
            "batter_name": row_value(row, "batter_name"),
            "batter_team_id": row_value(row, "batter_team_id"),
            "opposing_pitcher_id": pitcher_id,
            "opposing_pitcher_name": pitcher_pitch.get("pitcher_name"),
            "game_pk": row_value(row, "game_pk") or pitcher_pitch.get("game_pk"),
            "game_label": pitcher_pitch.get("game_label"),
            "pitch_type": pitch_type,
            "pitch_name": pitcher_pitch.get("pitch_name"),
            "pitcher_usage_pct": pitcher_pitch.get("usage_pct"),
            "pitcher_xwoba_allowed": pitcher_pitch.get("xwoba_allowed"),
            "pitcher_hard_hit_allowed": pitcher_pitch.get("hard_hit_pct_allowed"),
            "pitcher_whiff_pct": pitcher_pitch.get("whiff_pct"),
            "hitter_xwoba": safe_float(row_value(row, "xwoba")),
            "hitter_xba": safe_float(row_value(row, "xba")),
            "hitter_avg_ev": safe_float(row_value(row, "avg_exit_velocity") or row_value(row, "avg_ev")),
            "hitter_avg_la": safe_float(row_value(row, "avg_launch_angle") or row_value(row, "avg_la")),
            "hitter_hard_hit_pct": normalize_rate(row_value(row, "hard_hit_pct") or row_value(row, "hardhit_pct")),
            "hitter_whiff_pct": normalize_rate(row_value(row, "whiff_pct")),
            "hitter_k_pct": normalize_rate(row_value(row, "k_pct")),
            "pa": safe_int(row_value(row, "pa_ended") or row_value(row, "pa")),
            "pitches_seen": safe_int(row_value(row, "pitches_seen")),
            "target_date": row_value(row, "target_date").isoformat() if row_value(row, "target_date") else None,
            "date_start": row_value(row, "date_start").isoformat() if row_value(row, "date_start") else None,
            "date_end": row_value(row, "date_end").isoformat() if row_value(row, "date_end") else None,
            "refreshed_at": row_value(row, "refreshed_at").isoformat() if row_value(row, "refreshed_at") else None,
            "source": row_value(row, "source"),
            "duplicate_rows_removed": safe_int(row_value(row, "duplicate_rows_removed")),
            "watchlist_note": "player prop watchlist angle only; no sportsbook line assumed",
        })

    scored_rows.sort(key=lambda x: x.get("rank_score") or -999, reverse=True)
    missing_name_count = sum(1 for row in scored_rows if not row.get("batter_name"))
    return {
        "intent": "stored_365_matchups",
        "date": date,
        "sources_used": ["batter_pitch_type_matchups", "model_projections.pitch_arsenal", "pitcher_arsenals"],
        "top_matchups": scored_rows[:25],
        "data_quality": {
            "stored_365_rows_scored": len(scored_rows),
            "pitcher_pitch_join_rows": len(pitch_lookup),
            "missing_batter_names": missing_name_count,
            "errors": errors,
        },
        "missing_data": errors + ([f"{missing_name_count} rows are missing batter names"] if missing_name_count else []),
    }


# ---------------------------------------------------------------------------
# Pitcher leans
# ---------------------------------------------------------------------------


def score_pitcher_lean(game: Dict[str, Any], side: str) -> Dict[str, Any]:
    signals = extract_projection_game_signals(game)
    team = signals.get(f"{side}_signals") or {}
    other = "home" if side == "away" else "away"
    opponent = signals.get(f"{other}_signals") or {}
    pitcher = team.get("pitcher") or {}
    opp_off = opponent.get("offense") or {}
    arsenal = team.get("arsenal") or {}
    score = 0.0
    reasons: List[str] = []
    missing: List[str] = []
    k_pct = pitcher.get("k_pct")
    bb_pct = pitcher.get("bb_pct")
    xwoba = pitcher.get("xwoba_allowed")
    hard_hit = pitcher.get("hard_hit_allowed")
    opp_k = opp_off.get("k_pct")
    opp_iso = opp_off.get("iso")
    if k_pct is not None:
        score += clamp((k_pct - 0.20) * 4.2, -0.4, 0.9)
        reasons.append(f"pitcher K% {k_pct}")
    else:
        missing.append("pitcher_k_pct")
    if bb_pct is not None:
        score += clamp((0.085 - bb_pct) * 2.2, -0.25, 0.3)
    if xwoba is not None:
        score += clamp((0.335 - xwoba) * 4.0, -0.55, 0.7)
        reasons.append(f"xwOBA allowed {xwoba}")
    else:
        missing.append("pitcher_xwoba_allowed")
    if hard_hit is not None:
        score += clamp((0.40 - hard_hit) * 1.7, -0.35, 0.45)
    if opp_k is not None:
        score += clamp((opp_k - 0.21) * 2.0, -0.25, 0.4)
        reasons.append(f"opponent K% {opp_k}")
    if opp_iso is not None and opp_iso > 0.18:
        score -= clamp((opp_iso - 0.18) * 1.5, 0.0, 0.25)
        reasons.append(f"risk: opponent ISO {opp_iso}")
    top_pitches = arsenal.get("top_pitches") or []
    usage_concentration = sum((p.get("usage_pct") or 0) for p in top_pitches[:2])
    if usage_concentration:
        score += clamp((usage_concentration - 0.45) * 0.5, -0.05, 0.25)
        reasons.append(f"top-two pitch usage {round(usage_concentration, 3)}")
    side_prob = signals.get(f"{side}_win_prob")
    opp_prob = signals.get(f"{other}_win_prob")
    if side_prob is not None and opp_prob is not None and side_prob > opp_prob:
        score += min(0.35, (side_prob - opp_prob) * 0.55)
        reasons.append(f"game script side probability {side_prob}")
    confidence = clamp(0.38 + score * 0.22 - len(missing) * 0.08, 0.1, 0.9)
    tier = "high" if confidence >= 0.72 else "medium" if confidence >= 0.5 else "low"
    category = "strikeout/watchlist lean" if k_pct and k_pct >= 0.24 else "run-prevention lean" if score > 0.25 else "data-limited lean"
    if opp_iso and opp_iso > 0.19:
        category = "volatility/stay-away note" if score < 0.35 else category
    return {
        "game_pk": game.get("game_pk"),
        "label": projection_game_label(game),
        "pitcher_name": team.get("pitcher_name"),
        "pitcher_id": team.get("pitcher_id"),
        "team": team.get("team_name"),
        "opponent": team.get("opponent_team"),
        "score": round(score, 3),
        "confidence": round(confidence, 3),
        "confidence_tier": tier,
        "category": category,
        "reasons": reasons[:7],
        "missing_inputs": missing,
        "pitcher_profile": pitcher,
        "opponent_offense": opp_off,
        "top_pitches": top_pitches,
        "watchlist_note": "watchlist angle only; no sportsbook strikeout line is assumed",
    }


def build_pitcher_lean_context(session, date: str) -> Dict[str, Any]:
    errors: List[str] = []
    projection_payload: Dict[str, Any] = {}
    try:
        projection_payload = build_model_projection_payload(session, date) or {}
    except Exception as exc:
        errors.append(f"model_projections_failed: {exc}")
    leans: List[Dict[str, Any]] = []
    for game in projection_payload.get("games") or []:
        for side in ["away", "home"]:
            leans.append(score_pitcher_lean(game, side))
    leans.sort(key=lambda row: row.get("score") or -999, reverse=True)
    return {
        "intent": "pitcher_analysis",
        "date": date,
        "sources_used": ["model_projections", "pitcher_profiles", "pitcher_arsenals", "team_offense_profiles", "simulation_outputs"],
        "pitcher_leans": leans[:14],
        "data_quality": {
            "projection_games": len(projection_payload.get("games") or []),
            "pitchers_ranked": len(leans),
            "projection_errors": errors + (projection_payload.get("errors") or []),
            "data_limited_pitchers": sum(1 for row in leans if row.get("confidence_tier") == "low"),
        },
        "missing_data": errors + [row for row in leans if row.get("missing_inputs")][:8],
    }


# ---------------------------------------------------------------------------
# Context builders and answer renderers
# ---------------------------------------------------------------------------


def build_daily_slate_context(session, date: str) -> Dict[str, Any]:
    games = generate_matchups_for_date(session, date) or []
    quality = quality_from_games(games)
    projection_context = build_model_projection_intelligence_context(session, date)
    return {
        "intent": "daily_slate_summary",
        "date": date,
        "sources_used": ["matchups", "model_projections"],
        "games": [compact_game(game) for game in games],
        "top_edges": projection_context.get("top_edges") or top_edges(games),
        "prop_watchlist": projection_context.get("prop_watchlist") or [],
        "data_quality": {**quality, "projection_quality": projection_context.get("data_quality")},
        "missing_data": quality["missing_by_game"] + (projection_context.get("missing_data") or []),
    }


def build_best_edges_context(session, date: str) -> Dict[str, Any]:
    return build_model_projection_intelligence_context(session, date)


def build_game_explanation_context(session, game_pk: int, date: Optional[str] = None) -> Dict[str, Any]:
    target_date = date or today()
    projection_context = build_model_projection_intelligence_context(session, target_date)
    match = next((edge for edge in projection_context.get("projection_edges", []) if str(edge.get("game_pk")) == str(game_pk)), None)
    if match:
        return {"intent": "game_explanation", "date": target_date, "game_pk": game_pk, "sources_used": projection_context.get("sources_used"), "game_projection_edge": match, "data_quality": projection_context.get("data_quality"), "missing_data": match.get("missing_inputs") or []}
    games = generate_matchups_for_date(session, target_date) or []
    game = next((row for row in games if str(row.get("game_pk")) == str(game_pk)), None)
    if not game:
        return {"intent": "game_explanation", "date": target_date, "game_pk": game_pk, "sources_used": ["matchups", "model_projections"], "game": None, "data_quality": {"game_found": False}, "missing_data": [f"No matchup found for game_pk {game_pk} on {target_date}"]}
    return {"intent": "game_explanation", "date": target_date, "game_pk": game_pk, "sources_used": ["matchups"], "game": compact_game(game), "data_quality": quality_from_games([game]), "missing_data": missing_for_game(game)}


def build_stored_365_context(session, date: str) -> Dict[str, Any]:
    return build_stored_365_sweep_context(session, date)


def build_odds_and_props_context(session, date: str) -> Dict[str, Any]:
    games = generate_matchups_for_date(session, date) or []
    projection_context = build_model_projection_intelligence_context(session, date)
    odds_payload: Dict[str, Any] = {}
    errors: List[Any] = []
    try:
        from .daily_odds_routes import daily_odds_models
        odds_payload = daily_odds_models(date=date) or {}
    except Exception as exc:
        errors.append(str(exc))
    return {
        "intent": "odds_and_props",
        "date": date,
        "sources_used": ["daily_odds_models", "model_projections", "matchups"],
        "odds_summary": {
            "count": odds_payload.get("count"),
            "matched_count": odds_payload.get("matched_count"),
            "odds_event_count": odds_payload.get("odds_event_count"),
            "top_prop_candidate_count": odds_payload.get("top_prop_candidate_count"),
            "top_prop_model_candidates": (odds_payload.get("top_prop_model_candidates") or [])[:8],
        },
        "projection_watchlist": projection_context.get("prop_watchlist") or [],
        "data_quality": {**quality_from_games(games, odds_payload=odds_payload), "projection_quality": projection_context.get("data_quality")},
        "missing_data": errors + (odds_payload.get("errors") or [] if isinstance(odds_payload, dict) else []) + (projection_context.get("missing_data") or []),
    }


def build_data_quality_context(session, date: str) -> Dict[str, Any]:
    games = generate_matchups_for_date(session, date) or []
    projection_context = build_model_projection_intelligence_context(session, date)
    stored_context = build_stored_365_sweep_context(session, date)
    stored_rows = nested_get(stored_context, "data_quality.stored_365_rows_scored")
    quality = quality_from_games(games, stored_rows=stored_rows)
    quality.update({
        "model_projection_quality": projection_context.get("data_quality"),
        "stored_365_quality": stored_context.get("data_quality"),
    })
    return {"intent": "data_quality", "date": date, "sources_used": ["matchups", "model_projections", "batter_pitch_type_matchups"], "data_quality": quality, "missing_data": quality["missing_by_game"] + (projection_context.get("missing_data") or []) + (stored_context.get("missing_data") or [])}


def build_assistant_context(session, message: str, date: str | None = None, game_pk: int | None = None, player_id: int | None = None, team_id: int | None = None) -> Dict[str, Any]:
    target_date = date_from_message(message, date)
    intent = classify_assistant_intent(message)
    if game_pk and intent in {"game_explanation", "unknown_app_question", "daily_slate_summary", "best_model_edges"}:
        return build_game_explanation_context(session, int(game_pk), target_date)
    if intent == "best_model_edges":
        return build_model_projection_intelligence_context(session, target_date)
    if intent == "stored_365_matchups":
        return build_stored_365_sweep_context(session, target_date)
    if intent == "pitcher_analysis":
        return build_pitcher_lean_context(session, target_date)
    if intent == "odds_and_props":
        return build_odds_and_props_context(session, target_date)
    if intent == "data_quality":
        return build_data_quality_context(session, target_date)
    context = build_daily_slate_context(session, target_date)
    context["intent"] = intent if intent != "unknown_app_question" else "daily_slate_summary"
    return context


def format_value(value: Any, digits: int = 3) -> str:
    if value is None:
        return "missing"
    if isinstance(value, float):
        return str(round(value, digits))
    return str(value)


def render_model_edge_answer(context: Dict[str, Any]) -> str:
    edges = context.get("projection_edges") or context.get("top_edges") or []
    watchlist = context.get("prop_watchlist") or []
    if not edges:
        return "Direct answer\nThe app does not currently have enough Model Projection data to rank a strongest model edge.\n\nData used\nmodel_projections attempted\n\nMissing or weak data\n" + summarize_missing(context.get("missing_data"))
    best = edges[0]
    total = best.get("total_projection") or {}
    lines = [
        "Direct answer",
        f"The cleanest model signal is {best.get('model_favorite') or 'no clear side'} in {best.get('label')}. This is a model-supported lean, not a betting recommendation without market price.",
        "",
        "Why the model says that",
        f"Projection edge score: {format_value(best.get('score'))}. Confidence: {best.get('confidence_tier')} ({format_value(best.get('confidence'))}).",
        f"Win-probability separation: {format_value(best.get('win_probability_edge'))}. Expected-run differential: {format_value(best.get('expected_run_differential'))}.",
    ]
    for reason in best.get("why") or []:
        lines.append(f"- {reason}")
    lines.extend([
        "",
        "Best side or team edge",
        f"{best.get('model_favorite') or 'No side'} is the current side lean from the model projection packet.",
        "",
        "Total/run environment angle",
        f"Projected total: {format_value(total.get('total_expected_runs'))}. Run environment: {total.get('environment_label') or 'not labeled'}. Run-scoring index: {format_value(total.get('run_scoring_index'))}.",
        "",
        "Player prop/watchlist angles",
    ])
    if watchlist:
        for item in watchlist[:5]:
            label = item.get("player_name") or item.get("team") or "watchlist item"
            lines.append(f"- {label}: {item.get('angle')} | score {format_value(item.get('score'))} | {', '.join(item.get('reasons') or [])}")
    else:
        lines.append("No projection-derived prop watchlist angles cleared the scoring threshold. No prop line is assumed.")
    lines.extend([
        "",
        "Data used",
        ", ".join(context.get("sources_used") or ["model_projections"]),
        "",
        "Missing or weak data",
        summarize_missing(context.get("missing_data")),
        "",
        "Suggested next question",
        "Show me the best hitter vs pitcher spots.",
    ])
    return "\n".join(lines)


def render_stored_365_sweep_answer(context: Dict[str, Any]) -> str:
    rows = context.get("top_matchups") or []
    lines = ["Direct answer"]
    if not rows:
        lines.append("Stored 365 did not return enough Batter vs Arsenal data for a ranked hitter sweep.")
    else:
        best = rows[0]
        hitter = best.get("batter_name") or f"Batter {best.get('batter_id')}"
        pitcher = best.get("opposing_pitcher_name") or f"pitcher {best.get('opposing_pitcher_id')}"
        lines.append(f"The top Batter vs Arsenal spot is {hitter} against {pitcher}'s {best.get('pitch_name') or best.get('pitch_type')}. This is a matchup flag and player-prop watchlist angle, not a priced prop edge.")
    lines.extend(["", "Top hitter spots"])
    for row in rows[:8]:
        hitter = row.get("batter_name") or f"Batter {row.get('batter_id')}"
        pitcher = row.get("opposing_pitcher_name") or f"pitcher {row.get('opposing_pitcher_id')}"
        lines.append(f"- {hitter} vs {pitcher} {row.get('pitch_name') or row.get('pitch_type')}: score {format_value(row.get('rank_score'))}, confidence {row.get('confidence_tier')} ({format_value(row.get('confidence'))}), usage {format_value(row.get('pitcher_usage_pct'))}, hitter xwOBA {format_value(row.get('hitter_xwoba'))}, EV {format_value(row.get('hitter_avg_ev'))}, LA {format_value(row.get('hitter_avg_la'))}, pitcher xwOBA allowed {format_value(row.get('pitcher_xwoba_allowed'))}.")
    lines.extend(["", "Why these pitches matter"])
    lines.append("The sweep weights whether the starter actually throws the pitch, whether the hitter damages that pitch type, whether the batted-ball quality is strong, and whether the pitcher allows damage on that pitch.")
    lines.extend(["", "Sample confidence"])
    for row in rows[:5]:
        hitter = row.get("batter_name") or f"Batter {row.get('batter_id')}"
        lines.append(f"- {hitter}: sample {row.get('sample_size')}, PA {row.get('pa')}, pitches seen {row.get('pitches_seen')}, confidence {row.get('confidence_tier')}, missing {row.get('missing_inputs') or 'none'}.")
    lines.extend([
        "",
        "Data used",
        ", ".join(context.get("sources_used") or ["batter_pitch_type_matchups"]),
        "",
        "Missing or weak data",
        summarize_missing(context.get("missing_data")),
        "",
        "Suggested next question",
        "Which pitchers have the most dangerous arsenals today?",
    ])
    return "\n".join(lines)


def render_pitcher_lean_answer(context: Dict[str, Any]) -> str:
    leans = context.get("pitcher_leans") or []
    lines = ["Direct answer"]
    if not leans:
        lines.append("The app does not currently have enough Model Projection pitcher context to rank pitcher leans.")
    else:
        best = leans[0]
        lines.append(f"The top pitcher lean is {best.get('pitcher_name') or best.get('pitcher_id')} against {best.get('opponent')}. Category: {best.get('category')}. This is a watchlist angle, not a priced prop edge.")
    lines.extend(["", "Top pitcher leans"])
    for row in leans[:8]:
        lines.append(f"- {row.get('pitcher_name') or row.get('pitcher_id')} ({row.get('team')}) vs {row.get('opponent')}: score {format_value(row.get('score'))}, confidence {row.get('confidence_tier')} ({format_value(row.get('confidence'))}), {row.get('category')}.")
    lines.extend(["", "Strikeout/watchlist angle"])
    for row in [r for r in leans if "strikeout" in str(r.get("category"))][:5]:
        lines.append(f"- {row.get('pitcher_name')}: {', '.join(row.get('reasons') or [])}")
    lines.extend(["", "Run-prevention and volatility notes"])
    for row in leans[:5]:
        lines.append(f"- {row.get('pitcher_name')}: {', '.join(row.get('reasons') or [])}. Missing: {row.get('missing_inputs') or 'none'}.")
    lines.extend([
        "",
        "Data used",
        ", ".join(context.get("sources_used") or ["model_projections"]),
        "",
        "Missing or weak data",
        summarize_missing(context.get("missing_data")),
        "",
        "Suggested next question",
        "What is the strongest model edge?",
    ])
    return "\n".join(lines)


def render_daily_slate_answer(context: Dict[str, Any]) -> str:
    edges = context.get("top_edges") or []
    games = context.get("games") or []
    lines = [
        "Direct answer",
        f"For {context.get('date')}, the app has {len(games)} games in the matchup feed. The slate summary is now anchored to Model Projections where available.",
        "",
        "Top model signals",
    ]
    if edges:
        for edge in edges[:5]:
            lines.append(f"- {edge.get('label')}: {edge.get('model_favorite')} | score {format_value(edge.get('score'))} | confidence {edge.get('confidence_tier')}")
    else:
        lines.append("No model projection edges are currently ranked.")
    lines.extend([
        "",
        "Data used",
        ", ".join(context.get("sources_used") or []),
        "",
        "Missing or weak data",
        summarize_missing(context.get("missing_data")),
        "",
        "Suggested next question",
        "Best Stored 365 hitter matchups.",
    ])
    return "\n".join(lines)


def render_odds_answer(context: Dict[str, Any]) -> str:
    odds = context.get("odds_summary") or {}
    watchlist = context.get("projection_watchlist") or []
    lines = [
        "Direct answer",
        f"Daily Odds returned {odds.get('count') or 0} game rows, {odds.get('odds_event_count') or 0} sportsbook events, and {odds.get('top_prop_candidate_count') or 0} prop/model candidates.",
        "",
        "Market and model separation",
        "Daily Odds can support priced edges when sportsbook price and line exist. Projection-only items are watchlist angles, not betting recommendations.",
        "",
        "Projection watchlist support",
    ]
    for item in watchlist[:6]:
        label = item.get("player_name") or item.get("team") or "watchlist"
        lines.append(f"- {label}: {item.get('angle')} | score {format_value(item.get('score'))}")
    if not watchlist:
        lines.append("No projection watchlist items are currently available.")
    lines.extend([
        "",
        "Data used",
        ", ".join(context.get("sources_used") or []),
        "",
        "Missing or weak data",
        summarize_missing(context.get("missing_data")),
        "",
        "Suggested next question",
        "Which game has the cleanest signal?",
    ])
    return "\n".join(lines)


def render_data_quality_answer(context: Dict[str, Any]) -> str:
    q = context.get("data_quality") or {}
    lines = [
        "Direct answer",
        "The AI Data Assistant is checking matchup, Model Projection, Stored 365, odds, starter, weather, and missing-input coverage. Missing data is part of the answer, not hidden.",
        "",
        "Data quality snapshot",
        f"Games: {q.get('game_count')}. Missing pitcher games: {q.get('missing_pitcher_games')}. Missing weather games: {q.get('missing_weather_games')}.",
        f"Stored 365 rows: {q.get('stored_365_rows_available')}. Projection quality: {json.dumps(q.get('model_projection_quality'), default=str)[:700]}",
        "",
        "Missing or weak data",
        summarize_missing(context.get("missing_data")),
        "",
        "Suggested next question",
        "What is the strongest model edge?",
    ]
    return "\n".join(lines)


def render_deterministic_answer(message: str, context: Dict[str, Any]) -> Dict[str, Any]:
    intent = context.get("intent") or "unknown_app_question"
    if intent == "best_model_edges":
        answer = render_model_edge_answer(context)
    elif intent == "stored_365_matchups":
        answer = render_stored_365_sweep_answer(context)
    elif intent == "pitcher_analysis":
        answer = render_pitcher_lean_answer(context)
    elif intent == "odds_and_props":
        answer = render_odds_answer(context)
    elif intent == "data_quality":
        answer = render_data_quality_answer(context)
    elif intent == "game_explanation" and context.get("game_projection_edge"):
        answer = render_model_edge_answer({**context, "projection_edges": [context.get("game_projection_edge")], "top_edges": [context.get("game_projection_edge")], "prop_watchlist": context.get("game_projection_edge", {}).get("prop_watchlist") or []})
    else:
        answer = render_daily_slate_answer(context)
    return {
        "answer": answer,
        "intent": intent,
        "date": context.get("date") or today(),
        "game_pk": context.get("game_pk"),
        "sources_used": context.get("sources_used") or [],
        "data_quality": context.get("data_quality") or {},
        "missing_data": context.get("missing_data") or [],
        "confidence_note": confidence_note(context),
        "context_preview": context_preview(context),
    }


def answer_with_optional_llm(message: str, context: Dict[str, Any], use_llm: bool = True) -> Dict[str, Any]:
    fallback = render_deterministic_answer(message, context)
    if not use_llm or not os.getenv("OPENAI_API_KEY"):
        return fallback
    try:
        from openai import OpenAI
        client = OpenAI()
        compact = json.dumps(context_preview(context, max_chars=10000), default=str)
        response = client.chat.completions.create(
            model=os.getenv("AI_DATA_ASSISTANT_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": AI_DATA_ASSISTANT_SYSTEM_PROMPT},
                {"role": "user", "content": f"Question: {message}\n\nApp-ranked evidence packet JSON:\n{compact}"},
            ],
            temperature=0.2,
            max_tokens=900,
        )
        answer = response.choices[0].message.content if response.choices else None
        if answer:
            fallback["answer"] = answer
            fallback["confidence_note"] += " LLM summarized the backend-ranked compact app evidence packet only."
    except Exception as exc:
        fallback["confidence_note"] += f" LLM unavailable, deterministic fallback used: {exc}"
    return fallback


def summarize_missing(missing: Any) -> str:
    if not missing:
        return "none flagged in compact packet"
    if isinstance(missing, list):
        parts = []
        for item in missing[:7]:
            if isinstance(item, dict):
                parts.append(f"{item.get('label') or item.get('game_pk') or item.get('pitcher_name') or 'item'}: {item.get('missing') or item.get('missing_inputs') or item}")
            else:
                parts.append(str(item))
        extra = len(missing) - len(parts)
        return "; ".join(parts) + (f"; plus {extra} more" if extra > 0 else "")
    return str(missing)


def confidence_note(context: Dict[str, Any]) -> str:
    if context.get("missing_data"):
        return "Answer is limited by missing or weak app-owned data flagged in the response. Model signal is separated from betting certainty."
    quality = context.get("data_quality") or {}
    if quality.get("game_count") == 0 and quality.get("projection_games") == 0:
        return "No games were found in the app-owned matchup/projection feed for this date."
    return "Answer is based only on app-owned data in the backend-ranked compact evidence packet."


def context_preview(context: Dict[str, Any], max_chars: int = 6000) -> Dict[str, Any]:
    keep = [
        "intent", "date", "game_pk", "sources_used", "games", "top_edges", "projection_edges", "game_projection_edge",
        "top_matchups", "pitcher_leans", "odds_summary", "projection_watchlist", "prop_watchlist", "projection_summary",
        "data_quality", "missing_data",
    ]
    preview = {key: context.get(key) for key in keep if key in context}
    raw = json.dumps(preview, default=str)
    if len(raw) <= max_chars:
        return preview
    return {"truncated": True, "preview_json": raw[:max_chars]}
