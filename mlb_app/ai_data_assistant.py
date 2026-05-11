from __future__ import annotations

import datetime as dt
import json
import os
import re
from typing import Any, Dict, List, Optional

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

AI_DATA_ASSISTANT_SYSTEM_PROMPT = """You are the MLB app's AI Data Assistant. You are an AI baseball brain, but you may only use the app data provided in the context. Do not use outside knowledge. Do not invent injuries, lineups, starters, odds, weather, stats, or betting information. If the app data is missing or weak, say that clearly. Keep answers concise, specific, and useful. Prefer model outputs, matchup facts, Stored 365 data, odds/model candidates, player profiles, and data-quality flags. Never call anything a guaranteed lock. Distinguish model leans from betting recommendations. Always include a short Data used section."""


def today() -> str:
    return dt.date.today().isoformat()


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def classify_assistant_intent(message: str) -> str:
    text = re.sub(r"\s+", " ", (message or "").strip().lower())
    checks = [
        ("data_quality", ["missing", "stale", "weak data", "data quality", "refresh"]),
        ("stored_365_matchups", ["stored 365", "365", "batter vs arsenal", "pitch type", "hitter vs pitcher"]),
        ("odds_and_props", ["daily odds", "odds", "prop", "props", "sportsbook", "line"]),
        ("best_model_edges", ["strongest edge", "best edge", "cleanest signal", "top edge", "biggest mismatch"]),
        ("pitcher_analysis", ["pitcher", "starter", "arsenal", "top pitcher", "strikeout"]),
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


def team_name(game: Dict[str, Any], side: str) -> Optional[str]:
    return game.get(f"{side}_team_name") or game.get(f"{side}_team") or game.get(f"{side}_name")


def pitcher_name(game: Dict[str, Any], side: str) -> Optional[str]:
    return game.get(f"{side}_pitcher_name") or game.get(f"{side}_probable_pitcher") or game.get(f"{side}_starter_name")


def game_label(game: Dict[str, Any]) -> str:
    return f"{team_name(game, 'away') or 'Away'} @ {team_name(game, 'home') or 'Home'}"


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


def build_daily_slate_context(session, date: str) -> Dict[str, Any]:
    games = generate_matchups_for_date(session, date) or []
    quality = quality_from_games(games)
    return {
        "intent": "daily_slate_summary",
        "date": date,
        "sources_used": ["matchups"],
        "games": [compact_game(game) for game in games],
        "top_edges": top_edges(games),
        "data_quality": quality,
        "missing_data": quality["missing_by_game"],
    }


def build_best_edges_context(session, date: str) -> Dict[str, Any]:
    games = generate_matchups_for_date(session, date) or []
    projection_error = None
    projections: Dict[str, Any] = {}
    try:
        projections = build_model_projection_payload(session, date) or {}
    except Exception as exc:
        projection_error = str(exc)
    quality = quality_from_games(games)
    return {
        "intent": "best_model_edges",
        "date": date,
        "sources_used": ["matchups", "model_projections"],
        "top_edges": top_edges(games, 8),
        "projection_summary": {
            "available": bool(projections),
            "count": projections.get("count") if isinstance(projections, dict) else None,
            "keys": list(projections.keys())[:20] if isinstance(projections, dict) else [],
            "error": projection_error,
        },
        "data_quality": quality,
        "missing_data": quality["missing_by_game"],
    }


def build_game_explanation_context(session, game_pk: int, date: Optional[str] = None) -> Dict[str, Any]:
    target_date = date or today()
    games = generate_matchups_for_date(session, target_date) or []
    game = next((row for row in games if str(row.get("game_pk")) == str(game_pk)), None)
    if not game:
        return {"intent": "game_explanation", "date": target_date, "game_pk": game_pk, "sources_used": ["matchups"], "game": None, "data_quality": {"game_found": False}, "missing_data": [f"No matchup found for game_pk {game_pk} on {target_date}"]}
    return {"intent": "game_explanation", "date": target_date, "game_pk": game_pk, "sources_used": ["matchups"], "game": compact_game(game), "data_quality": quality_from_games([game]), "missing_data": missing_for_game(game)}


def build_stored_365_context(session, date: str) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []
    try:
        parsed = dt.date.fromisoformat(date[:10])
        query = session.query(BatterPitchTypeMatchup).filter(BatterPitchTypeMatchup.target_date == parsed)
        records = query.order_by(BatterPitchTypeMatchup.xwoba.desc().nullslast()).limit(25).all()
        for row in records:
            rows.append({
                "batter_id": row.batter_id,
                "opposing_pitcher_id": row.opposing_pitcher_id,
                "pitch_type": row.pitch_type,
                "target_date": row.target_date.isoformat() if row.target_date else None,
                "pitches_seen": row.pitches_seen,
                "pa": row.pa,
                "xwoba": row.xwoba,
                "xba": row.xba,
                "avg_exit_velocity": row.avg_exit_velocity or row.avg_ev,
                "hard_hit_pct": row.hard_hit_pct or row.hardhit_pct,
                "whiff_pct": row.whiff_pct,
                "source": row.source,
            })
    except Exception as exc:
        errors.append(str(exc))
    return {"intent": "stored_365_matchups", "date": date, "sources_used": ["batter_pitch_type_matchups"], "top_matchups": rows, "data_quality": {"stored_365_rows_available": len(rows), "errors": errors}, "missing_data": errors if errors else ([] if rows else ["No Stored 365 rows found for this date"])}


def build_odds_and_props_context(session, date: str) -> Dict[str, Any]:
    games = generate_matchups_for_date(session, date) or []
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
        "sources_used": ["daily_odds_models", "matchups"],
        "odds_summary": {
            "count": odds_payload.get("count"),
            "matched_count": odds_payload.get("matched_count"),
            "odds_event_count": odds_payload.get("odds_event_count"),
            "top_prop_candidate_count": odds_payload.get("top_prop_candidate_count"),
            "top_prop_model_candidates": (odds_payload.get("top_prop_model_candidates") or [])[:8],
        },
        "data_quality": quality_from_games(games, odds_payload=odds_payload),
        "missing_data": errors + (odds_payload.get("errors") or [] if isinstance(odds_payload, dict) else []),
    }


def build_data_quality_context(session, date: str) -> Dict[str, Any]:
    games = generate_matchups_for_date(session, date) or []
    stored_rows = None
    try:
        parsed = dt.date.fromisoformat(date[:10])
        stored_rows = session.query(BatterPitchTypeMatchup).filter(BatterPitchTypeMatchup.target_date == parsed).count()
    except Exception:
        stored_rows = None
    quality = quality_from_games(games, stored_rows=stored_rows)
    return {"intent": "data_quality", "date": date, "sources_used": ["matchups", "batter_pitch_type_matchups"], "data_quality": quality, "missing_data": quality["missing_by_game"]}


def build_assistant_context(session, message: str, date: str | None = None, game_pk: int | None = None, player_id: int | None = None, team_id: int | None = None) -> Dict[str, Any]:
    target_date = date_from_message(message, date)
    intent = classify_assistant_intent(message)
    if game_pk and intent in {"game_explanation", "unknown_app_question", "daily_slate_summary"}:
        return build_game_explanation_context(session, int(game_pk), target_date)
    if intent == "best_model_edges":
        return build_best_edges_context(session, target_date)
    if intent == "stored_365_matchups":
        return build_stored_365_context(session, target_date)
    if intent == "odds_and_props":
        return build_odds_and_props_context(session, target_date)
    if intent == "data_quality":
        return build_data_quality_context(session, target_date)
    context = build_daily_slate_context(session, target_date)
    context["intent"] = intent if intent != "unknown_app_question" else "daily_slate_summary"
    return context


def render_deterministic_answer(message: str, context: Dict[str, Any]) -> Dict[str, Any]:
    intent = context.get("intent") or "unknown_app_question"
    date = context.get("date") or today()
    sources = context.get("sources_used") or []
    missing = context.get("missing_data") or []
    quality = context.get("data_quality") or {}
    lines: List[str] = []

    if intent == "best_model_edges":
        edges = context.get("top_edges") or []
        if edges:
            best = edges[0]
            lines.append(f"The strongest current model edge is {best.get('label')}: {best.get('model_favorite')} with an app edge of {best.get('model_edge')}.")
            for game in edges[1:5]:
                lines.append(f"- {game.get('label')}: {game.get('model_favorite')} edge {game.get('model_edge')}")
        else:
            lines.append("The app does not currently have enough model probability data to rank edges.")
    elif intent == "stored_365_matchups":
        rows = context.get("top_matchups") or []
        lines.append(f"Stored 365 returned {len(rows)} top batter-vs-arsenal rows for {date}.")
        for row in rows[:8]:
            lines.append(f"- Batter {row.get('batter_id')} vs pitcher {row.get('opposing_pitcher_id')} {row.get('pitch_type')}: xwOBA {row.get('xwoba')}, EV {row.get('avg_exit_velocity')}, sample {row.get('pitches_seen')}")
    elif intent == "odds_and_props":
        summary = context.get("odds_summary") or {}
        lines.append(f"Daily Odds model data for {date}: {summary.get('count') or 0} game rows, {summary.get('odds_event_count') or 0} sportsbook events, and {summary.get('top_prop_candidate_count') or 0} prop/model candidates.")
        for candidate in (summary.get("top_prop_model_candidates") or [])[:5]:
            lines.append(f"- {candidate.get('player_name') or candidate.get('pick')}: {candidate.get('market_name') or candidate.get('market')} score {candidate.get('score')} confidence {candidate.get('confidence')}")
    elif intent == "game_explanation":
        game = context.get("game")
        if game:
            lines.append(f"{game.get('label')}: the app currently shows {game.get('model_favorite') or 'no model favorite'} as the model lean.")
            lines.append(f"Projected starters in app data: {game.get('away_pitcher') or 'missing'} vs {game.get('home_pitcher') or 'missing'}.")
            lines.append(f"Win probabilities: away {game.get('away_win_prob')}, home {game.get('home_win_prob')}.")
        else:
            lines.append("That game was not found in the app matchup feed for the selected date.")
    elif intent == "data_quality":
        lines.append(f"Data quality for {date}: {quality.get('game_count', 0)} games, {quality.get('missing_pitcher_games', 0)} games missing a starter, {quality.get('missing_weather_games', 0)} games missing weather, and {quality.get('games_missing_model_probabilities', 0)} games missing model probabilities.")
    else:
        games = context.get("games") or []
        lines.append(f"For {date}, the app has {len(games)} games in the matchup feed.")
        edges = context.get("top_edges") or []
        if edges:
            lines.append("Top model edges from available matchup probabilities:")
            for game in edges[:5]:
                lines.append(f"- {game.get('label')}: {game.get('model_favorite') or 'No favorite'} edge {game.get('model_edge')}")
        else:
            lines.append("No ranked model edges are available from the current matchup packet.")

    lines.append("\nData used: " + (", ".join(str(source) for source in sources) if sources else "none"))
    lines.append("Missing or weak data: " + summarize_missing(missing))
    lines.append("Suggested next question: Which game has the cleanest signal?")

    return {
        "answer": "\n".join(lines),
        "intent": intent,
        "date": date,
        "game_pk": context.get("game_pk"),
        "sources_used": sources,
        "data_quality": quality,
        "missing_data": missing,
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
        compact = json.dumps(context_preview(context, max_chars=9000), default=str)
        response = client.chat.completions.create(
            model=os.getenv("AI_DATA_ASSISTANT_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": AI_DATA_ASSISTANT_SYSTEM_PROMPT},
                {"role": "user", "content": f"Question: {message}\n\nApp context JSON:\n{compact}"},
            ],
            temperature=0.2,
            max_tokens=700,
        )
        answer = response.choices[0].message.content if response.choices else None
        if answer:
            fallback["answer"] = answer
            fallback["confidence_note"] += " LLM summarized compact app context only."
    except Exception as exc:
        fallback["confidence_note"] += f" LLM unavailable, deterministic fallback used: {exc}"
    return fallback


def summarize_missing(missing: Any) -> str:
    if not missing:
        return "none flagged in compact packet"
    if isinstance(missing, list):
        parts = []
        for item in missing[:5]:
            if isinstance(item, dict):
                parts.append(f"{item.get('label') or item.get('game_pk')}: {item.get('missing')}")
            else:
                parts.append(str(item))
        extra = len(missing) - len(parts)
        return "; ".join(parts) + (f"; plus {extra} more" if extra > 0 else "")
    return str(missing)


def confidence_note(context: Dict[str, Any]) -> str:
    if context.get("missing_data"):
        return "Answer is limited by missing or weak app-owned data flagged in the response."
    quality = context.get("data_quality") or {}
    if quality.get("game_count") == 0:
        return "No games were found in the app-owned matchup feed for this date."
    return "Answer is based only on app-owned data in the compact evidence packet."


def context_preview(context: Dict[str, Any], max_chars: int = 5000) -> Dict[str, Any]:
    keep = ["intent", "date", "game_pk", "sources_used", "games", "top_edges", "game", "top_matchups", "odds_summary", "projection_summary", "data_quality", "missing_data"]
    preview = {key: context.get(key) for key in keep if key in context}
    raw = json.dumps(preview, default=str)
    if len(raw) <= max_chars:
        return preview
    return {"truncated": True, "preview_json": raw[:max_chars]}
