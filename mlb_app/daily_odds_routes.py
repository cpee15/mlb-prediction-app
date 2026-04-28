from __future__ import annotations

import datetime
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter

from .daily_odds_models import build_game_models, build_prop_models
from .matchup_generator import generate_matchups_for_date
from .odds_provider import fetch_draftkings_event_odds, fetch_draftkings_events

router = APIRouter()


def _normalize_team_key(name: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower().replace("the", "", 1))


def _matchup_key(away: Any, home: Any) -> str:
    return f"{_normalize_team_key(away)}@{_normalize_team_key(home)}"


def _key_from_matchup(matchup: Dict[str, Any]) -> str:
    return _matchup_key(
        matchup.get("away_team_name") or matchup.get("away_team") or matchup.get("away_name"),
        matchup.get("home_team_name") or matchup.get("home_team") or matchup.get("home_name"),
    )


def _key_from_event(event: Dict[str, Any]) -> str:
    away = event.get("away_team") or {}
    home = event.get("home_team") or {}
    away_name = away.get("name") if isinstance(away, dict) else away
    home_name = home.get("name") if isinstance(home, dict) else home
    return _matchup_key(away_name, home_name)


def _build_matchup_index(matchups: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for matchup in matchups or []:
        key = _key_from_matchup(matchup)
        if key != "@":
            index[key] = matchup
    return index


def _safe_error(error: Exception) -> Dict[str, Any]:
    return {"type": error.__class__.__name__, "message": str(error)}


@router.get("/daily-odds/models")
def daily_odds_models(date: Optional[str] = None) -> Dict[str, Any]:
    target_date = date or datetime.date.today().isoformat()
    errors: List[Dict[str, Any]] = []

    try:
        matchups = generate_matchups_for_date(target_date)
    except Exception as exc:
        matchups = []
        errors.append({"stage": "generate_matchups_for_date", "error": _safe_error(exc)})

    try:
        odds_payload = fetch_draftkings_events(date=target_date, raw=False)
    except Exception as exc:
        odds_payload = {"events": []}
        errors.append({"stage": "fetch_draftkings_events", "error": _safe_error(exc)})

    events = odds_payload.get("events", []) if isinstance(odds_payload, dict) else []
    matchup_index = _build_matchup_index(matchups)

    outputs: List[Dict[str, Any]] = []
    for event in events:
        key = _key_from_event(event)
        matchup = matchup_index.get(key)
        if not matchup:
            outputs.append({
                "event_id": event.get("event_id"),
                "away_team": (event.get("away_team") or {}).get("name") if isinstance(event.get("away_team"), dict) else event.get("away_team"),
                "home_team": (event.get("home_team") or {}).get("name") if isinstance(event.get("home_team"), dict) else event.get("home_team"),
                "matched": False,
                "match_key": key,
                "models": None,
                "missing_inputs": ["matched_mlb_game"],
            })
            continue

        try:
            models = build_game_models(matchup, event)
        except Exception as exc:
            models = None
            errors.append({"stage": "build_game_models", "event_id": event.get("event_id"), "match_key": key, "error": _safe_error(exc)})

        outputs.append({
            "game_pk": matchup.get("game_pk"),
            "event_id": event.get("event_id"),
            "away_team": matchup.get("away_team_name") or matchup.get("away_team") or matchup.get("away_name"),
            "home_team": matchup.get("home_team_name") or matchup.get("home_team") or matchup.get("home_name"),
            "matched": True,
            "match_key": key,
            "models": models,
        })

    return {
        "date": target_date,
        "count": len(outputs),
        "matched_count": sum(1 for row in outputs if row.get("matched")),
        "unmatched_count": sum(1 for row in outputs if not row.get("matched")),
        "odds_status": odds_payload.get("status") if isinstance(odds_payload, dict) else None,
        "last_updated": odds_payload.get("last_updated") if isinstance(odds_payload, dict) else None,
        "models": outputs,
        "games": outputs,
        "top_prop_model_candidates": [],
        "errors": errors,
    }


@router.get("/daily-odds/event/{event_id}/prop-models")
def daily_odds_prop_models(event_id: str, market: Optional[str] = None) -> Dict[str, Any]:
    errors: List[Dict[str, Any]] = []
    try:
        payload = fetch_draftkings_event_odds(event_id, props_only=True, raw=False)
    except Exception as exc:
        payload = {"markets": []}
        errors.append({"stage": "fetch_draftkings_event_odds", "error": _safe_error(exc)})

    prop_markets = payload.get("markets", []) if isinstance(payload, dict) else []
    try:
        models = build_prop_models({}, prop_markets, market_filter=market or "all")
    except Exception as exc:
        models = {"top_candidates": [], "candidate_count": 0}
        errors.append({"stage": "build_prop_models", "error": _safe_error(exc)})

    return {
        "event_id": event_id,
        "market_filter": market or "all",
        "models": models,
        "top_prop_model_candidates": models.get("top_candidates", []) if isinstance(models, dict) else [],
        "odds_status": payload.get("status") if isinstance(payload, dict) else None,
        "last_updated": payload.get("last_updated") if isinstance(payload, dict) else None,
        "errors": errors,
    }
