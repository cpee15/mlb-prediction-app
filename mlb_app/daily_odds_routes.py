from __future__ import annotations

import datetime
import os
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter

from .daily_odds_models import build_game_models, build_prop_models
from .database import create_tables, get_engine, get_session
from .matchup_generator import generate_matchups_for_date
from .odds_provider import fetch_draftkings_event_odds, fetch_draftkings_events

router = APIRouter()

DASHBOARD_COMPONENTS = ("hitters", "pitchers", "teams", "totals", "overall_players")


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


def _session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


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
        Session = _session_factory()
        with Session() as session:
            return generate_matchups_for_date(session, target_date), errors
    except Exception as fallback_exc:
        errors.append({"stage": "generate_matchups_for_date_fallback", "error": _safe_error(fallback_exc)})
        return [], errors


def _build_matchup_index(matchups: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for matchup in matchups or []:
        key = _key_from_matchup(matchup)
        if key != "@":
            index[key] = matchup
    return index


def _candidate_sort_key(candidate: Dict[str, Any]) -> float:
    edge = _safe_float(candidate.get("edge"))
    confidence = _safe_float(candidate.get("confidence"))
    score = _safe_float(candidate.get("score"))
    return abs(edge or 0.0) * 10.0 + (confidence or 0.0) + (score or 0.0)


def _fallback_candidates_from_matchups(matchups: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
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
            candidates.append({
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
                "confidence": round(_clamp(0.52 + gap, 0.52, 0.85), 3),
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
                ],
                "missing_inputs": ["sportsbook_event_id", "sportsbook_price", "market_implied_probability"],
                "drivers": ["internal win probability", "pregame matchup model", "odds provider returned zero events"],
            })

        for side, pitcher_name, opponent, features in [
            ("home", matchup.get("home_pitcher_name"), away_team, matchup.get("home_pitcher_features") or {}),
            ("away", matchup.get("away_pitcher_name"), home_team, matchup.get("away_pitcher_features") or {}),
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
            candidates.append({
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
                "confidence": round(_clamp(0.50 + score, 0.50, 0.78), 3),
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
            })

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
        outputs.append({
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
        })
    return outputs


def _compact_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "rank": item.get("rank"),
        "entity_type": item.get("entity_type"),
        "entity_id": item.get("entity_id"),
        "entity_name": item.get("entity_name"),
        "player_type": item.get("player_type"),
        "team": item.get("team"),
        "opponent": item.get("opponent"),
        "game_pk": item.get("game_pk"),
        "score": item.get("score"),
        "confidence": item.get("confidence"),
        "category": item.get("category"),
        "primary_reason": item.get("primary_reason"),
        "reasoning": item.get("reasoning") or [],
        "metrics": item.get("metrics") or {},
        "source": item.get("source"),
        "missing_data": item.get("missing_data") or [],
        "best_pitch_angles": item.get("best_pitch_angles") or [],
    }


def _compact_dashboard_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    items = [_compact_item(item) for item in payload.get("items") or []]
    return {
        "component": payload.get("component"),
        "title": payload.get("title"),
        "items": items,
        "top_item": items[0] if items else None,
        "item_count": len(items),
        "data_quality": payload.get("data_quality") or {},
        "missing_data": payload.get("missing_data") or [],
        "filter_warnings": payload.get("filter_warnings") or [],
        "generated_at": payload.get("generated_at"),
    }


def _empty_projection_summary() -> Dict[str, Any]:
    return {"projection_count": 0, "games": [], "source": "model_projections", "missing_inputs": []}


def _summarize_projection_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    games = payload.get("games") if isinstance(payload.get("games"), list) else []
    compact_games: List[Dict[str, Any]] = []
    for game in games[:20]:
        if not isinstance(game, dict):
            continue
        workspace = game.get("workspace") or {}
        simulation = workspace.get("bullpenAdjustedGameSimulation") or workspace.get("gameSimulation") or {}
        compact_games.append({
            "game_pk": game.get("game_pk"),
            "away_team": game.get("away_team") or game.get("away_team_name"),
            "home_team": game.get("home_team") or game.get("home_team_name"),
            "away_pitcher": game.get("away_pitcher"),
            "home_pitcher": game.get("home_pitcher"),
            "home_win_probability": simulation.get("home_win_probability") or game.get("home_win_probability"),
            "away_win_probability": simulation.get("away_win_probability") or game.get("away_win_probability"),
            "home_expected_runs": simulation.get("home_expected_runs"),
            "away_expected_runs": simulation.get("away_expected_runs"),
            "total_expected_runs": simulation.get("total_expected_runs"),
            "missing_inputs": game.get("missing_inputs") or workspace.get("missing_inputs") or [],
        })
    return {
        "projection_count": len(games),
        "games": compact_games,
        "source": "model_projections",
        "missing_inputs": payload.get("missing_inputs") or payload.get("errors") or [],
        "generated_at": payload.get("generated_at"),
    }


def _load_dashboard_summary(target_date: str, errors: List[Dict[str, Any]]) -> Dict[str, Any]:
    from .my_dashboard_solver import build_dashboard_solver_payload, projection_payload

    summary: Dict[str, Any] = {
        "components": {},
        "components_used": [],
        "top_hitter_angles": [],
        "top_pitcher_angles": [],
        "top_team_edges": [],
        "top_total_angles": [],
        "top_overall_players": [],
        "model_projection_summary": _empty_projection_summary(),
    }
    try:
        Session = _session_factory()
        with Session() as session:
            for component in DASHBOARD_COMPONENTS:
                try:
                    payload = build_dashboard_solver_payload(session, target_date, component)
                    summary["components"][component] = _compact_dashboard_payload(payload)
                    summary["components_used"].append(component)
                except Exception as exc:
                    errors.append({"stage": f"dashboard_solver_{component}", "error": _safe_error(exc)})
            try:
                summary["model_projection_summary"] = _summarize_projection_payload(projection_payload(session, target_date) or {})
            except Exception as exc:
                errors.append({"stage": "model_projection_payload", "error": _safe_error(exc)})
    except Exception as exc:
        errors.append({"stage": "daily_odds_unified_session", "error": _safe_error(exc)})

    hitters = summary["components"].get("hitters", {}).get("items") or []
    pitchers = summary["components"].get("pitchers", {}).get("items") or []
    teams = summary["components"].get("teams", {}).get("items") or []
    totals = summary["components"].get("totals", {}).get("items") or []
    overall = summary["components"].get("overall_players", {}).get("items") or []
    summary["top_hitter_angles"] = hitters[:10]
    summary["top_pitcher_angles"] = pitchers[:10]
    summary["top_team_edges"] = teams[:10]
    summary["top_total_angles"] = totals[:10]
    summary["top_overall_players"] = overall[:10]
    summary["batter_vs_arsenal_edges"] = [
        item for item in hitters if item.get("best_pitch_angles") or "batter_pitch_type_matchups" in str(item.get("source") or "")
    ][:10]
    summary["batter_vs_arsenal_count"] = len(summary["batter_vs_arsenal_edges"])
    return summary


def _best_game_signal(outputs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for row in outputs or []:
        models = row.get("models") or {}
        for market_key in ("moneyline", "spread", "run_line", "total"):
            model = models.get(market_key)
            if not isinstance(model, dict):
                continue
            if not model.get("pick") and model.get("available") is False:
                continue
            candidates.append({
                "market": market_key,
                "pick": model.get("pick"),
                "score": model.get("score"),
                "edge": model.get("edge"),
                "confidence": model.get("confidence") or model.get("model_probability"),
                "model_probability": model.get("model_probability"),
                "market_implied_probability": model.get("market_implied_probability"),
                "game_pk": row.get("game_pk"),
                "event_id": row.get("event_id"),
                "away_team": row.get("away_team"),
                "home_team": row.get("home_team"),
                "source": model.get("model") or "daily_odds_models",
                "missing_inputs": model.get("missing_inputs") or row.get("missing_inputs") or [],
                "features_used": model.get("features_used") or [],
            })
    return sorted(candidates, key=_candidate_sort_key, reverse=True)[0] if candidates else None


def _first_item(summary: Dict[str, Any], component: str) -> Optional[Dict[str, Any]]:
    item = summary.get("components", {}).get(component, {}).get("top_item")
    return item if isinstance(item, dict) else None


def _build_daily_recap(outputs: List[Dict[str, Any]], dashboard_summary: Dict[str, Any], odds_status: Optional[str], odds_event_count: int) -> Dict[str, Any]:
    priced_count = sum(1 for row in outputs if row.get("event_id"))
    return {
        "best_side": _best_game_signal(outputs),
        "best_total": _first_item(dashboard_summary, "totals"),
        "best_pitcher": _first_item(dashboard_summary, "pitchers"),
        "best_hitter": _first_item(dashboard_summary, "hitters"),
        "best_team": _first_item(dashboard_summary, "teams"),
        "data_quality_note": "Daily Odds unified recap is loaded on demand from shared My Dashboard, AI Data Assistant, Model Projection, and Batter vs Arsenal paths.",
        "pricing_note": "Sportsbook-priced markets are available." if odds_event_count else "No sportsbook event payload was available; surfaced picks are model-only watchlist signals.",
        "odds_status": odds_status,
        "priced_game_count": priced_count,
        "model_only_game_count": max(len(outputs) - priced_count, 0),
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


def _collect_missing_inputs(outputs: List[Dict[str, Any]], dashboard_summary: Dict[str, Any], projection_summary: Dict[str, Any], errors: List[Dict[str, Any]]) -> List[Any]:
    missing: List[Any] = []
    for row in outputs:
        missing.extend(row.get("missing_inputs") or [])
        for model in (row.get("models") or {}).values():
            if isinstance(model, dict):
                missing.extend(model.get("missing_inputs") or [])
    for component_payload in dashboard_summary.get("components", {}).values():
        missing.extend(component_payload.get("missing_data") or [])
        for item in component_payload.get("items") or []:
            missing.extend(item.get("missing_data") or [])
    missing.extend(projection_summary.get("missing_inputs") or [])
    missing.extend({"stage": item.get("stage"), "error": item.get("error")} for item in errors if item.get("stage"))
    deduped: List[Any] = []
    seen = set()
    for item in missing:
        key = str(item)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped[:50]


def _sources_used(odds_payload: Dict[str, Any], dashboard_summary: Dict[str, Any]) -> List[str]:
    sources = ["daily_odds_models", "matchup_generator.generate_matchups_for_date", "odds_provider.fetch_draftkings_events"]
    if dashboard_summary.get("components_used"):
        sources.extend(["my_dashboard_solver.build_dashboard_solver_payload", "ai_data_assistant Stored 365 / pitcher lean contexts"])
    if dashboard_summary.get("model_projection_summary", {}).get("projection_count", 0) > 0:
        sources.append("model_projections.build_model_projection_payload")
    if odds_payload.get("provider"):
        sources.append(str(odds_payload.get("provider")))
    return sources


def _base_response(target_date: str, outputs: List[Dict[str, Any]], events: List[Dict[str, Any]], odds_payload: Dict[str, Any], top_prop_candidates: List[Dict[str, Any]], errors: List[Dict[str, Any]]) -> Dict[str, Any]:
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
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


@router.get("/daily-odds/models")
def daily_odds_models(date: Optional[str] = None, include_unified: bool = False) -> Dict[str, Any]:
    target_date = date or datetime.date.today().isoformat()
    errors: List[Dict[str, Any]] = []

    matchups, matchup_errors = _load_matchups(target_date)
    errors.extend(matchup_errors)

    try:
        odds_payload = fetch_draftkings_events(date=target_date, raw=False)
    except Exception as exc:
        odds_payload = {"events": [], "status": "provider_error"}
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
    response = _base_response(target_date, outputs, events, odds_payload if isinstance(odds_payload, dict) else {}, top_prop_candidates, errors)

    if not include_unified:
        response["unified_available"] = True
        response["unified_loaded"] = False
        return response

    dashboard_summary = _load_dashboard_summary(target_date, errors)
    projection_summary = dashboard_summary.get("model_projection_summary") or _empty_projection_summary()
    daily_recap = _build_daily_recap(outputs, dashboard_summary, response.get("odds_status"), len(events))
    missing_inputs = _collect_missing_inputs(outputs, dashboard_summary, projection_summary, errors)
    fallbacks_used: List[str] = []
    if not events:
        fallbacks_used.append("internal_matchups_no_odds_provider")
    if dashboard_summary.get("components_used"):
        fallbacks_used.append("my_dashboard_solver_shared_context")
    if projection_summary.get("projection_count", 0) > 0:
        fallbacks_used.append("model_projection_shared_payload")

    data_quality = {
        "sources_used": _sources_used(odds_payload if isinstance(odds_payload, dict) else {}, dashboard_summary),
        "missing_inputs": missing_inputs,
        "fallbacks_used": fallbacks_used,
        "odds_status": response.get("odds_status"),
        "matchup_count": len(matchups),
        "projection_count": projection_summary.get("projection_count", 0),
        "dashboard_solver_components_used": dashboard_summary.get("components_used", []),
        "batter_vs_arsenal_count": dashboard_summary.get("batter_vs_arsenal_count", 0),
        "generated_at": daily_recap.get("generated_at"),
    }
    response.update({
        "unified_available": True,
        "unified_loaded": True,
        "daily_recap": daily_recap,
        "model_projection_summary": projection_summary,
        "dashboard_solver_summary": dashboard_summary.get("components", {}),
        "top_hitter_angles": dashboard_summary.get("top_hitter_angles", []),
        "top_pitcher_angles": dashboard_summary.get("top_pitcher_angles", []),
        "batter_vs_arsenal_edges": dashboard_summary.get("batter_vs_arsenal_edges", []),
        "matchup_detail_signals": projection_summary.get("games", []),
        "data_quality": data_quality,
        "sources_used": data_quality["sources_used"],
        "missing_inputs": missing_inputs,
        "fallbacks_used": fallbacks_used,
        "errors": errors,
        "generated_at": data_quality["generated_at"],
    })
    return response


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
