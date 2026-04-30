from __future__ import annotations

import datetime
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter

from .daily_odds_models import build_game_models, build_prop_models
from .database import create_tables, get_engine, get_session
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


def _team_name_from_event(event: Dict[str, Any], side: str) -> Optional[str]:
    team = event.get(f"{side}_team") or {}
    return team.get("name") if isinstance(team, dict) else team


def _build_matchup_index(matchups: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for matchup in matchups or []:
        key = _key_from_matchup(matchup)
        if key != "@":
            index[key] = matchup
    return index


def _safe_error(error: Exception) -> Dict[str, Any]:
    return {"type": error.__class__.__name__, "message": str(error)}


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _load_matchups(target_date: str) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    errors: List[Dict[str, Any]] = []
    try:
        from .app import _get_session

        Session = _get_session()
        with Session() as session:
            return generate_matchups_for_date(session, target_date), errors
    except Exception as primary_exc:
        errors.append({"stage": "generate_matchups_for_date_primary", "error": _safe_error(primary_exc)})

    try:
        import os

        database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
        engine = get_engine(database_url)
        create_tables(engine)
        Session = get_session(engine)
        with Session() as session:
            return generate_matchups_for_date(session, target_date), errors
    except Exception as fallback_exc:
        errors.append({"stage": "generate_matchups_for_date_fallback", "error": _safe_error(fallback_exc)})
        return [], errors


def _candidate_sort_key(candidate: Dict[str, Any]) -> float:
    edge = candidate.get("edge")
    confidence = candidate.get("confidence")
    score = candidate.get("score")
    try:
        edge_component = abs(float(edge)) if edge is not None else 0.0
    except (TypeError, ValueError):
        edge_component = 0.0
    try:
        confidence_component = float(confidence) if confidence is not None else 0.0
    except (TypeError, ValueError):
        confidence_component = 0.0
    try:
        score_component = float(score) if score is not None else 0.0
    except (TypeError, ValueError):
        score_component = 0.0
    return edge_component * 10.0 + confidence_component + score_component


def _fallback_candidates_from_matchups(matchups: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
    """Build a populated pre-game candidate board even when the odds provider returns zero events.

    These are internal model candidates, not sportsbook-priced prop edges. They are
    intentionally marked with market_implied_probability=None and source=no_odds_provider
    so the UI has useful pre-game targets while still making the missing odds state clear.
    """
    candidates: List[Dict[str, Any]] = []

    for matchup in matchups or []:
        game_pk = matchup.get("game_pk")
        away_team = matchup.get("away_team_name") or matchup.get("away_team") or matchup.get("away_name") or "Away"
        home_team = matchup.get("home_team_name") or matchup.get("home_team") or matchup.get("home_name") or "Home"
        label = f"{away_team} @ {home_team}"
        home_prob = _safe_float(matchup.get("home_win_prob") or matchup.get("home_win_probability"))
        away_prob = _safe_float(matchup.get("away_win_prob") or matchup.get("away_win_probability"))

        if home_prob is not None and away_prob is not None:
            pick_team = home_team if home_prob >= away_prob else away_team
            model_probability = max(home_prob, away_prob)
            gap = abs(home_prob - away_prob)
            confidence = round(_clamp(0.52 + gap, 0.52, 0.85), 3)
            candidates.append(
                {
                    "model": "pregame_internal_moneyline_v1",
                    "market": "pregame_moneyline",
                    "market_name": "Pregame Moneyline Candidate",
                    "market_family": "game",
                    "pick": pick_team,
                    "player_name": label,
                    "selection": pick_team,
                    "line": None,
                    "price": None,
                    "score": round(model_probability + gap, 4),
                    "model_probability": round(model_probability, 4),
                    "market_implied_probability": None,
                    "edge": None,
                    "confidence": confidence,
                    "game_pk": game_pk,
                    "event_id": None,
                    "away_team": away_team,
                    "home_team": home_team,
                    "match_key": _key_from_matchup(matchup),
                    "matched": True,
                    "available": True,
                    "source": "internal_matchups_no_odds_provider",
                    "features_used": [
                        {"name": "home_win_prob", "value": home_prob, "source": "matchups.home_win_prob", "transform": "raw"},
                        {"name": "away_win_prob", "value": away_prob, "source": "matchups.away_win_prob", "transform": "raw"},
                        {"name": "probability_gap", "value": gap, "source": "matchups.win_probability_gap", "transform": "absolute_difference"},
                    ],
                    "missing_inputs": ["sportsbook_event_id", "sportsbook_price", "market_implied_probability"],
                    "drivers": ["internal win probability", "pregame matchup model", "odds provider returned zero events"],
                }
            )

        home_pitcher = matchup.get("home_pitcher_name")
        away_pitcher = matchup.get("away_pitcher_name")
        home_features = matchup.get("home_pitcher_features") or {}
        away_features = matchup.get("away_pitcher_features") or {}
        for side, pitcher_name, opponent, features in [
            ("home", home_pitcher, away_team, home_features),
            ("away", away_pitcher, home_team, away_features),
        ]:
            if not pitcher_name:
                continue
            k_pct = _safe_float(features.get("k_pct"))
            xwoba = _safe_float(features.get("xwoba"))
            hard_hit = _safe_float(features.get("hard_hit_pct"))
            signal_parts = []
            if k_pct is not None:
                signal_parts.append(_clamp(k_pct, 0.0, 0.45))
            if xwoba is not None:
                signal_parts.append(_clamp(0.360 - xwoba, -0.10, 0.12) + 0.10)
            if hard_hit is not None:
                signal_parts.append(_clamp(0.42 - hard_hit, -0.10, 0.12) + 0.10)
            if not signal_parts:
                continue
            score = round(sum(signal_parts) / len(signal_parts), 4)
            confidence = round(_clamp(0.50 + score, 0.50, 0.78), 3)
            candidates.append(
                {
                    "model": "pregame_internal_pitcher_prop_watchlist_v1",
                    "market": "pitcher_strikeouts_watchlist",
                    "market_name": "Pitcher Strikeouts Watchlist",
                    "market_family": "pitcher",
                    "pick": f"{pitcher_name} strikeout lean",
                    "player_name": pitcher_name,
                    "selection": "strikeout lean",
                    "line": None,
                    "price": None,
                    "score": score,
                    "model_probability": None,
                    "market_implied_probability": None,
                    "edge": None,
                    "confidence": confidence,
                    "game_pk": game_pk,
                    "event_id": None,
                    "away_team": away_team,
                    "home_team": home_team,
                    "match_key": _key_from_matchup(matchup),
                    "matched": True,
                    "available": True,
                    "source": "internal_matchups_no_odds_provider",
                    "features_used": [
                        {"name": "k_pct", "value": k_pct, "source": f"matchups.{side}_pitcher_features.k_pct", "transform": "raw"},
                        {"name": "xwoba", "value": xwoba, "source": f"matchups.{side}_pitcher_features.xwoba", "transform": "raw"},
                        {"name": "hard_hit_pct", "value": hard_hit, "source": f"matchups.{side}_pitcher_features.hard_hit_pct", "transform": "raw"},
                    ],
                    "missing_inputs": ["sportsbook_event_id", "sportsbook_prop_line", "sportsbook_price"],
                    "drivers": ["pitcher K profile", "pitcher contact suppression", f"opponent: {opponent}", "odds provider returned zero events"],
                }
            )

    candidates.sort(key=_candidate_sort_key, reverse=True)
    return candidates[:limit]


def _build_global_prop_candidates(events: List[Dict[str, Any]], matchup_index: Dict[str, Dict[str, Any]], matchups: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for event in events:
        key = _key_from_event(event)
        matchup = matchup_index.get(key) or {}
        prop_markets = [
            market
            for market in event.get("markets", []) or []
            if str(market.get("market_key") or market.get("market_type") or market.get("market_name") or "").startswith(("batter_", "pitcher_"))
        ]
        if not prop_markets:
            continue

        models = build_prop_models(matchup, prop_markets, market_filter="all", limit=20)
        for candidate in models.get("top_candidates", []) if isinstance(models, dict) else []:
            enriched = dict(candidate)
            enriched["game_pk"] = matchup.get("game_pk")
            enriched["event_id"] = event.get("event_id")
            enriched["away_team"] = matchup.get("away_team_name") or _team_name_from_event(event, "away")
            enriched["home_team"] = matchup.get("home_team_name") or _team_name_from_event(event, "home")
            enriched["match_key"] = key
            enriched["matched"] = bool(matchup)
            enriched["source"] = "sportsbook_props"
            candidates.append(enriched)

    if not candidates:
        return _fallback_candidates_from_matchups(matchups, limit=limit)

    candidates.sort(key=_candidate_sort_key, reverse=True)
    return candidates[:limit]


def _models_from_unpriced_matchups(matchups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    outputs: List[Dict[str, Any]] = []
    for matchup in matchups or []:
        away_team = matchup.get("away_team_name") or matchup.get("away_team") or matchup.get("away_name")
        home_team = matchup.get("home_team_name") or matchup.get("home_team") or matchup.get("home_name")
        home_prob = _safe_float(matchup.get("home_win_prob"))
        away_prob = _safe_float(matchup.get("away_win_prob"))
        pick = None
        confidence = None
        if home_prob is not None and away_prob is not None:
            pick = home_team if home_prob >= away_prob else away_team
            confidence = round(_clamp(max(home_prob, away_prob), 0.50, 0.85), 3)
        outputs.append(
            {
                "game_pk": matchup.get("game_pk"),
                "event_id": None,
                "away_team": away_team,
                "home_team": home_team,
                "matched": True,
                "match_key": _key_from_matchup(matchup),
                "odds_missing": True,
                "missing_inputs": ["sportsbook_event_id", "sportsbook_markets"],
                "models": {
                    "game_pk": matchup.get("game_pk"),
                    "event_id": None,
                    "moneyline": {
                        "model": "moneyline_internal_no_odds_v1",
                        "market": "moneyline",
                        "pick": pick or "No pick",
                        "score": confidence or 0.0,
                        "model_probability": confidence,
                        "market_implied_probability": None,
                        "edge": None,
                        "confidence": confidence or 0.0,
                        "features_used": [
                            {"name": "home_win_prob", "value": home_prob, "source": "matchups.home_win_prob", "transform": "raw"},
                            {"name": "away_win_prob", "value": away_prob, "source": "matchups.away_win_prob", "transform": "raw"},
                        ],
                        "missing_inputs": ["sportsbook_price", "market_implied_probability"],
                        "drivers": ["internal matchup model", "odds provider returned zero events"],
                        "available": pick is not None,
                    },
                    "spread": None,
                    "total": None,
                },
            }
        )
    return outputs


@router.get("/daily-odds/models")
def daily_odds_models(date: Optional[str] = None) -> Dict[str, Any]:
    target_date = date or datetime.date.today().isoformat()
    errors: List[Dict[str, Any]] = []

    matchups, matchup_errors = _load_matchups(target_date)
    errors.extend(matchup_errors)

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
                "away_team": _team_name_from_event(event, "away"),
                "home_team": _team_name_from_event(event, "home"),
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

    if not outputs and matchups:
        outputs = _models_from_unpriced_matchups(matchups)

    top_prop_candidates = _build_global_prop_candidates(events, matchup_index, matchups, limit=20)

    return {
        "date": target_date,
        "count": len(outputs),
        "matched_count": sum(1 for row in outputs if row.get("matched")),
        "unmatched_count": sum(1 for row in outputs if not row.get("matched")),
        "odds_status": odds_payload.get("status") if isinstance(odds_payload, dict) else None,
        "last_updated": odds_payload.get("last_updated") if isinstance(odds_payload, dict) else None,
        "odds_event_count": len(events),
        "models": outputs,
        "games": outputs,
        "top_prop_model_candidates": top_prop_candidates,
        "top_prop_candidate_count": len(top_prop_candidates),
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
        models = build_prop_models({}, prop_markets, market_filter=market or "all", limit=20)
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
