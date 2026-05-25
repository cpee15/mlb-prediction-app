from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy.orm import Session

from .matchup_generator import generate_matchups_for_date
from .model_projections import build_model_projection_payload
from .my_dashboard_solver import build_dashboard_solver_payload

QUALIFIED_CONFIDENCE_THRESHOLD = 0.62
QUALIFIED_DATA_QUALITY_THRESHOLD = 0.70
QUALIFIED_EDGE_THRESHOLD = 0.04
STRONG_EDGE_THRESHOLD = 0.06
STRONG_CONFIDENCE_THRESHOLD = 0.68
STRONG_DATA_QUALITY_THRESHOLD = 0.75
PREMIUM_EDGE_THRESHOLD = 0.08
PREMIUM_CONFIDENCE_THRESHOLD = 0.72
PREMIUM_DATA_QUALITY_THRESHOLD = 0.80
WATCHLIST_CONFIDENCE_THRESHOLD = 0.55

DASHBOARD_COMPONENTS = ("hitters", "pitchers", "teams", "totals", "overall_players")
REQUIRED_MARKET_FAMILIES = (
    "moneyline",
    "run_line",
    "full_game_total",
    "first_5_side",
    "first_5_total",
    "pitcher_strikeouts",
    "pitcher_contact_suppression",
    "hitter_hit",
    "hitter_total_bases_power",
    "team_total_offense",
    "bullpen_late_scoring",
    "weather_environment_total",
)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _normalize_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower().replace("the", "", 1))


def _game_key(away_team: Any, home_team: Any) -> str:
    return f"{_normalize_name(away_team)}@{_normalize_name(home_team)}"


def _short(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, list):
        return "; ".join(str(item) for item in value[:4])[:1000] or fallback
    return str(value)[:1000] or fallback


def american_to_implied(price: Any) -> Optional[float]:
    p = _safe_float(price)
    if p is None or p == 0:
        return None
    if p > 0:
        return round(100.0 / (p + 100.0), 4)
    return round(abs(p) / (abs(p) + 100.0), 4)


def probability_to_american(probability: Any) -> Optional[int]:
    p = _safe_float(probability)
    if p is None or p <= 0 or p >= 1:
        return None
    if p >= 0.5:
        return int(round(-(p / (1.0 - p)) * 100.0))
    return int(round(((1.0 - p) / p) * 100.0))


def expected_value(model_probability: Any, american_price: Any) -> Optional[float]:
    prob = _safe_float(model_probability)
    price = _safe_float(american_price)
    if prob is None or price is None or price == 0:
        return None
    if price > 0:
        decimal_payout = price / 100.0
    else:
        decimal_payout = 100.0 / abs(price)
    return round(prob * decimal_payout - (1.0 - prob), 4)


def calculate_edge(model_probability: Any, market_implied_probability: Any) -> Optional[float]:
    model_prob = _safe_float(model_probability)
    market_prob = _safe_float(market_implied_probability)
    if model_prob is None or market_prob is None:
        return None
    return round(model_prob - market_prob, 4)


def _features_count(features: Iterable[Any]) -> int:
    return len([item for item in features if item is not None])


def _data_quality_score(features: List[Any], missing_inputs: List[Any], odds_backed: bool, has_model_probability: bool) -> float:
    used = _features_count(features)
    missing_count = len(missing_inputs)
    denominator = max(used + missing_count, 1)
    base = used / denominator
    if odds_backed:
        base += 0.12
    if has_model_probability:
        base += 0.08
    if used >= 6:
        base += 0.05
    return round(_clamp(base, 0.0, 1.0), 3)


def _critical_missing_inputs(missing_inputs: List[Any], odds_backed: bool, selection: Any) -> List[str]:
    critical: List[str] = []
    if not selection or str(selection).lower() in {"none", "no pick", "null", "n/a"}:
        critical.append("valid_selection")
    for item in missing_inputs:
        text = str(item).lower()
        if "matched_mlb_game" in text or "invalid" in text:
            critical.append(str(item))
        if odds_backed and ("market_implied_probability" in text or "sportsbook_price" in text):
            critical.append(str(item))
    return list(dict.fromkeys(critical))


def _infer_probability_from_score(score: Any) -> Optional[float]:
    numeric = _safe_float(score)
    if numeric is None:
        return None
    if 0.0 <= numeric <= 1.0:
        return round(_clamp(numeric), 4)
    return None


def _play_id(target_date: str, game_pk: Any, market_type: Any, pick_type: Any, selection: Any, model_name: Any, line: Any = None) -> str:
    raw = "|".join(str(part or "") for part in [target_date[:10], game_pk, market_type, pick_type, selection, model_name, line])
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    safe_selection = re.sub(r"[^a-zA-Z0-9]+", "-", str(selection or "selection")).strip("-").lower()[:32]
    return f"{target_date[:10]}-{game_pk or 'ungrouped'}-{market_type or 'market'}-{safe_selection}-{digest}"


def _market_strength(edge: Optional[float], confidence: float, data_quality: float) -> str:
    edge_value = edge or 0.0
    if edge_value >= PREMIUM_EDGE_THRESHOLD and confidence >= PREMIUM_CONFIDENCE_THRESHOLD and data_quality >= PREMIUM_DATA_QUALITY_THRESHOLD:
        return "premium"
    if edge_value >= STRONG_EDGE_THRESHOLD and confidence >= STRONG_CONFIDENCE_THRESHOLD and data_quality >= STRONG_DATA_QUALITY_THRESHOLD:
        return "strong"
    return "standard"


def _qualify_play(candidate: Dict[str, Any], target_date: str) -> Dict[str, Any]:
    features = _as_list(candidate.get("features_used"))
    missing_inputs = _as_list(candidate.get("missing_inputs"))
    selection = candidate.get("selection") or candidate.get("pick") or candidate.get("pick_label")
    score = _safe_float(candidate.get("score"))
    model_probability = _safe_float(candidate.get("model_probability"))
    if model_probability is None:
        model_probability = _infer_probability_from_score(score)
    market_probability = _safe_float(candidate.get("market_implied_probability"))
    price = _safe_float(candidate.get("price"))
    if market_probability is None and price is not None:
        market_probability = american_to_implied(price)
    edge = _safe_float(candidate.get("edge"))
    if edge is None:
        edge = calculate_edge(model_probability, market_probability)
    ev = _safe_float(candidate.get("expected_value"))
    if ev is None:
        ev = expected_value(model_probability, price)
    confidence = _safe_float(candidate.get("confidence"))
    if confidence is None:
        confidence = _safe_float(score)
    if confidence is None and model_probability is not None:
        confidence = round(_clamp(0.52 + abs(model_probability - 0.50), 0.0, 0.85), 3)
    confidence = round(_clamp(confidence or 0.0), 3)
    odds_backed = market_probability is not None and price is not None
    data_quality = _safe_float(candidate.get("data_quality_score"))
    if data_quality is None:
        data_quality = _data_quality_score(features, missing_inputs, odds_backed, model_probability is not None)
    critical_missing = _as_list(candidate.get("critical_missing_inputs")) or _critical_missing_inputs(missing_inputs, odds_backed, selection)
    if _features_count(features) <= 0:
        critical_missing.append("features_used")
    critical_missing = [str(item) for item in dict.fromkeys(critical_missing)]

    rejection_reasons: List[str] = []
    if not selection or str(selection).lower() in {"none", "no pick", "null", "n/a"}:
        rejection_reasons.append("invalid selection")
    if model_probability is None and score is None:
        rejection_reasons.append("missing model_probability or score")
    if confidence < WATCHLIST_CONFIDENCE_THRESHOLD:
        rejection_reasons.append(f"confidence below {WATCHLIST_CONFIDENCE_THRESHOLD}")
    if _features_count(features) <= 0:
        rejection_reasons.append("no usable features")

    tier = candidate.get("play_tier")
    if rejection_reasons:
        tier = "rejected"
    else:
        qualifies = (
            model_probability is not None
            and odds_backed
            and confidence >= QUALIFIED_CONFIDENCE_THRESHOLD
            and data_quality >= QUALIFIED_DATA_QUALITY_THRESHOLD
            and (edge or 0.0) >= QUALIFIED_EDGE_THRESHOLD
            and not critical_missing
        )
        if qualifies:
            tier = "qualified"
        elif candidate.get("model_family") in {"first_5_side", "first_5_total", "bullpen_late_scoring", "weather_environment_total"} and not odds_backed:
            tier = "model_signal"
        elif confidence >= WATCHLIST_CONFIDENCE_THRESHOLD and (model_probability is not None or score is not None):
            tier = "watchlist"
        else:
            tier = "rejected"
            rejection_reasons.append("failed watchlist requirements")

    fair_price = probability_to_american(model_probability)
    model_name = candidate.get("model_name") or f"best_plays_{candidate.get('model_family') or candidate.get('market_type') or 'candidate'}_v1"
    primary_reason = candidate.get("primary_reason") or _short(candidate.get("drivers"), "Best Plays engine candidate.")
    if tier == "rejected" and rejection_reasons:
        primary_reason = f"Rejected: {', '.join(rejection_reasons)}. {primary_reason}"

    return {
        "game_pk": _safe_int(candidate.get("game_pk")),
        "event_id": candidate.get("event_id"),
        "away_team": candidate.get("away_team"),
        "home_team": candidate.get("home_team"),
        "play_id": candidate.get("play_id") or _play_id(target_date, candidate.get("game_pk"), candidate.get("market_type"), candidate.get("pick_type"), selection, model_name, candidate.get("line")),
        "play_tier": tier,
        "play_strength": _market_strength(edge, confidence, data_quality),
        "market_type": candidate.get("market_type") or candidate.get("market") or candidate.get("model_family") or "model_signal",
        "pick_type": candidate.get("pick_type") or candidate.get("market_type") or candidate.get("model_family") or "candidate",
        "selection": selection,
        "player_id": _safe_int(candidate.get("player_id")),
        "player_name": candidate.get("player_name"),
        "team_id": _safe_int(candidate.get("team_id")),
        "team_name": candidate.get("team_name") or candidate.get("team") or (selection if candidate.get("market_type") in {"moneyline", "spread", "run_line"} else None),
        "opponent_name": candidate.get("opponent_name") or candidate.get("opponent"),
        "line": _safe_float(candidate.get("line")),
        "price": price,
        "model_probability": round(model_probability, 4) if model_probability is not None else None,
        "market_implied_probability": round(market_probability, 4) if market_probability is not None else None,
        "fair_price": fair_price,
        "edge": round(edge, 4) if edge is not None else None,
        "expected_value": ev,
        "score": score,
        "confidence": confidence,
        "data_quality_score": data_quality,
        "features_used_count": _features_count(features),
        "critical_missing_inputs": critical_missing,
        "model_family": candidate.get("model_family") or candidate.get("market_type") or "candidate",
        "model_name": model_name,
        "primary_reason": primary_reason,
        "features_used": features,
        "missing_inputs": missing_inputs,
        "rejection_reasons": rejection_reasons,
        "raw_payload": candidate.get("raw_payload") or candidate,
        "odds_backed": odds_backed,
    }


def _game_from_matchup(matchup: Dict[str, Any]) -> Dict[str, Any]:
    away_team = matchup.get("away_team_name") or matchup.get("away_team") or matchup.get("away_name")
    home_team = matchup.get("home_team_name") or matchup.get("home_team") or matchup.get("home_name")
    return {
        "game_pk": matchup.get("game_pk"),
        "away_team": away_team,
        "home_team": home_team,
        "match_key": _game_key(away_team, home_team),
        "raw_matchup": matchup,
    }


def _index_daily_odds_games(payload: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    by_pk: Dict[str, Dict[str, Any]] = {}
    by_key: Dict[str, Dict[str, Any]] = {}
    for game in payload.get("games") or payload.get("models") or []:
        if not isinstance(game, dict):
            continue
        game_pk = game.get("game_pk")
        away_team = game.get("away_team") or game.get("away_team_name")
        home_team = game.get("home_team") or game.get("home_team_name")
        if game_pk:
            by_pk[str(game_pk)] = game
        key = _game_key(away_team, home_team)
        if key != "@":
            by_key[key] = game
    return by_pk, by_key


def _index_projection_games(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    for game in payload.get("games") or payload.get("models") or []:
        if isinstance(game, dict) and game.get("game_pk"):
            indexed[str(game.get("game_pk"))] = game
    return indexed


def _dashboard_items_by_game(payloads: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    indexed: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for component, payload in payloads.items():
        for item in payload.get("items") or []:
            if not isinstance(item, dict) or not item.get("game_pk"):
                continue
            game_key = str(item.get("game_pk"))
            indexed.setdefault(game_key, {}).setdefault(component, []).append(item)
    for component_map in indexed.values():
        for rows in component_map.values():
            rows.sort(key=lambda item: _safe_float(item.get("score") or item.get("adjusted_score") or 0.0) or 0.0, reverse=True)
    return indexed


def _candidate_from_daily_model(game: Dict[str, Any], market_key: str, model: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "game_pk": game.get("game_pk"),
        "event_id": game.get("event_id"),
        "away_team": game.get("away_team"),
        "home_team": game.get("home_team"),
        "market_type": model.get("market") or market_key,
        "pick_type": market_key,
        "selection": model.get("pick") or model.get("selection"),
        "line": model.get("line"),
        "price": model.get("price"),
        "model_probability": model.get("model_probability"),
        "market_implied_probability": model.get("market_implied_probability"),
        "edge": model.get("edge"),
        "expected_value": model.get("expected_value"),
        "score": model.get("score"),
        "confidence": model.get("confidence"),
        "features_used": model.get("features_used") or [],
        "missing_inputs": model.get("missing_inputs") or game.get("missing_inputs") or [],
        "model_family": "run_line" if market_key in {"spread", "run_line"} else ("full_game_total" if market_key == "total" else market_key),
        "model_name": model.get("model") or f"best_plays_{market_key}_v1",
        "primary_reason": _short(model.get("drivers") or model.get("reasoning"), "Daily Odds model candidate."),
        "raw_payload": {"game": game, "model": model},
    }


def _candidate_from_prop(prop: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "game_pk": prop.get("game_pk"),
        "event_id": prop.get("event_id"),
        "away_team": prop.get("away_team"),
        "home_team": prop.get("home_team"),
        "market_type": prop.get("market") or prop.get("market_family") or "prop",
        "pick_type": "prop",
        "selection": prop.get("pick") or prop.get("selection") or prop.get("player_name"),
        "player_id": prop.get("player_id"),
        "player_name": prop.get("player_name") or prop.get("entity_name"),
        "line": prop.get("line"),
        "price": prop.get("price"),
        "model_probability": prop.get("model_probability"),
        "market_implied_probability": prop.get("market_implied_probability"),
        "edge": prop.get("edge"),
        "expected_value": prop.get("expected_value"),
        "score": prop.get("score"),
        "confidence": prop.get("confidence"),
        "features_used": prop.get("features_used") or [],
        "missing_inputs": prop.get("missing_inputs") or [],
        "model_family": prop.get("market_family") or "prop",
        "model_name": prop.get("model") or "best_plays_prop_candidate_v1",
        "primary_reason": _short(prop.get("drivers") or prop.get("reasoning"), "Sportsbook prop model candidate."),
        "raw_payload": prop,
    }


def _candidate_from_projection(game: Dict[str, Any], projection: Dict[str, Any], family: str) -> Optional[Dict[str, Any]]:
    workspace = projection.get("workspace") or {}
    simulation = workspace.get("bullpenAdjustedGameSimulation") or workspace.get("gameSimulation") or projection.get("gameSimulation") or {}
    home_prob = _safe_float(simulation.get("home_win_probability") or projection.get("home_win_probability") or projection.get("home_win_prob"))
    away_prob = _safe_float(simulation.get("away_win_probability") or projection.get("away_win_probability") or projection.get("away_win_prob"))
    projected_total = _safe_float(simulation.get("total_expected_runs") or projection.get("total_expected_runs"))
    projected_home = _safe_float(simulation.get("home_expected_runs") or projection.get("home_expected_runs"))
    projected_away = _safe_float(simulation.get("away_expected_runs") or projection.get("away_expected_runs"))
    missing = projection.get("missing_inputs") or workspace.get("missing_inputs") or []
    features = [
        {"name": "home_win_probability", "value": home_prob, "source": "model_projection"},
        {"name": "away_win_probability", "value": away_prob, "source": "model_projection"},
        {"name": "projected_total", "value": projected_total, "source": "model_projection"},
        {"name": "home_expected_runs", "value": projected_home, "source": "model_projection"},
        {"name": "away_expected_runs", "value": projected_away, "source": "model_projection"},
    ]
    features = [feature for feature in features if feature.get("value") is not None]
    if family == "moneyline" and home_prob is not None and away_prob is not None:
        pick_home = home_prob >= away_prob
        selection = game.get("home_team") if pick_home else game.get("away_team")
        probability = home_prob if pick_home else away_prob
        return {
            "game_pk": game.get("game_pk"),
            "away_team": game.get("away_team"),
            "home_team": game.get("home_team"),
            "market_type": "moneyline",
            "pick_type": "side",
            "selection": selection,
            "team_name": selection,
            "model_probability": probability,
            "score": probability,
            "confidence": probability,
            "features_used": features,
            "missing_inputs": missing + ["sportsbook_price", "market_implied_probability"],
            "model_family": "moneyline",
            "model_name": "best_plays_projection_moneyline_v1",
            "primary_reason": "Model Projection side from simulated win probability.",
            "raw_payload": projection,
        }
    if family == "full_game_total" and projected_total is not None:
        return {
            "game_pk": game.get("game_pk"),
            "away_team": game.get("away_team"),
            "home_team": game.get("home_team"),
            "market_type": "total",
            "pick_type": "projected_total",
            "selection": f"Projected total {projected_total}",
            "score": projected_total,
            "confidence": 0.58 if features else 0.0,
            "features_used": features,
            "missing_inputs": missing + ["sportsbook_total_line", "over_under_selection", "market_implied_probability"],
            "model_family": "full_game_total",
            "model_name": "best_plays_projection_total_v1",
            "primary_reason": "Model Projection total runs signal captured for market comparison.",
            "raw_payload": projection,
        }
    return None


def _candidate_from_dashboard(game: Dict[str, Any], component: str, item: Dict[str, Any], family: str, label: str) -> Dict[str, Any]:
    metrics = item.get("metrics") or {}
    entity_type = item.get("entity_type")
    selection = item.get("pick") or item.get("entity_name") or item.get("player_name") or item.get("category") or label
    player_id = item.get("player_id") or item.get("entity_id") if entity_type in {"hitter", "pitcher", "player"} else item.get("player_id")
    player_name = item.get("player_name") or item.get("entity_name") if entity_type in {"hitter", "pitcher", "player"} else item.get("player_name")
    team_id = item.get("team_id") or item.get("entity_id") if entity_type == "team" else item.get("team_id")
    team_name = item.get("team") or item.get("team_name") or item.get("entity_name") if entity_type == "team" else item.get("team") or item.get("team_name")
    return {
        "game_pk": game.get("game_pk"),
        "away_team": game.get("away_team"),
        "home_team": game.get("home_team"),
        "market_type": family,
        "pick_type": component,
        "selection": selection,
        "player_id": player_id,
        "player_name": player_name,
        "team_id": team_id,
        "team_name": team_name,
        "opponent_name": item.get("opponent"),
        "score": item.get("score") or item.get("adjusted_score"),
        "confidence": item.get("confidence"),
        "features_used": item.get("features_used") or item.get("reasoning") or [],
        "missing_inputs": item.get("missing_data") or item.get("missing_inputs") or ["sportsbook_price", "market_implied_probability"],
        "model_family": family,
        "model_name": f"best_plays_dashboard_{component}_v1",
        "primary_reason": item.get("primary_reason") or _short(item.get("reasoning"), f"Dashboard {component} signal."),
        "raw_payload": {"item": item, "metrics": metrics},
    }


def _placeholder_candidate(game: Dict[str, Any], family: str, market_type: str, pick_type: str, missing_inputs: List[str], reason: str) -> Dict[str, Any]:
    return {
        "game_pk": game.get("game_pk"),
        "away_team": game.get("away_team"),
        "home_team": game.get("home_team"),
        "market_type": market_type,
        "pick_type": pick_type,
        "selection": f"{game.get('away_team') or 'Away'} @ {game.get('home_team') or 'Home'} {family} review",
        "score": None,
        "confidence": 0.0,
        "features_used": [],
        "missing_inputs": missing_inputs,
        "critical_missing_inputs": missing_inputs,
        "model_family": family,
        "model_name": f"best_plays_{family}_v1",
        "primary_reason": reason,
        "raw_payload": {"missing_inputs": missing_inputs},
    }


def _add_required_placeholders(game: Dict[str, Any], candidates: List[Dict[str, Any]]) -> None:
    present = {str(candidate.get("model_family") or candidate.get("market_type") or "") for candidate in candidates}
    placeholder_specs = {
        "moneyline": ("moneyline", "side", ["model_win_probability", "sportsbook_moneyline_price"], "Moneyline candidate could not be priced from current model and odds data."),
        "run_line": ("run_line", "spread", ["projected_run_differential", "sportsbook_run_line"], "Run line candidate could not be qualified from current data."),
        "full_game_total": ("total", "over_under", ["projected_total", "sportsbook_total_line"], "Full-game total candidate needs projected total and market line."),
        "first_5_side": ("first_5", "side", ["first_5_model", "first_5_price"], "First 5 side is not currently priced by existing model output."),
        "first_5_total": ("first_5", "total", ["first_5_total_model", "first_5_total_line"], "First 5 total is not currently priced by existing model output."),
        "pitcher_strikeouts": ("pitcher_prop", "strikeouts", ["pitcher_strikeout_prop_line", "sportsbook_price"], "Pitcher strikeout lean needs a trackable prop line."),
        "pitcher_contact_suppression": ("pitcher_prop", "contact_suppression", ["pitcher_contact_suppression_market"], "Pitcher run-prevention/contact signal is model-only without a market."),
        "hitter_hit": ("batter_prop", "hit", ["batter_hit_prop_line", "sportsbook_price"], "Hitter hit lean needs a trackable prop line."),
        "hitter_total_bases_power": ("batter_prop", "total_bases_power", ["batter_total_bases_or_power_line", "sportsbook_price"], "Hitter power lean needs a total bases or HR market."),
        "team_total_offense": ("team_total", "offense", ["team_total_line", "sportsbook_price"], "Team total/offensive environment signal needs a team total market."),
        "bullpen_late_scoring": ("late_scoring", "bullpen", ["bullpen_model_signal", "late_scoring_market"], "Bullpen late-scoring angle needs bullpen model data and a market."),
        "weather_environment_total": ("environment", "weather_total", ["weather_run_environment_signal", "market_total_line"], "Weather/environment total angle needs weather signal and total line."),
    }
    for family in REQUIRED_MARKET_FAMILIES:
        if family in present:
            continue
        market_type, pick_type, missing, reason = placeholder_specs[family]
        candidates.append(_placeholder_candidate(game, family, market_type, pick_type, missing, reason))


def _summarize_games(games: List[Dict[str, Any]]) -> Dict[str, Any]:
    plays = [play for game in games for play in game.get("plays") or []]
    return {
        "games": len(games),
        "total_candidates": len(plays),
        "qualified": sum(1 for play in plays if play.get("play_tier") == "qualified"),
        "watchlist": sum(1 for play in plays if play.get("play_tier") == "watchlist"),
        "model_signal": sum(1 for play in plays if play.get("play_tier") == "model_signal"),
        "rejected": sum(1 for play in plays if play.get("play_tier") == "rejected"),
        "odds_backed": sum(1 for play in plays if play.get("odds_backed")),
        "model_only": sum(1 for play in plays if not play.get("odds_backed")),
    }


def build_best_plays_payload(session: Session, target_date: str) -> Dict[str, Any]:
    target_date = target_date[:10]
    dt.date.fromisoformat(target_date)
    errors: List[Dict[str, Any]] = []

    try:
        matchups = generate_matchups_for_date(session, target_date)
    except Exception as exc:
        matchups = []
        errors.append({"source": "matchups", "error": str(exc), "type": exc.__class__.__name__})

    try:
        from .daily_odds_routes import daily_odds_models
        daily_payload = daily_odds_models(date=target_date, include_unified=True)
    except Exception as exc:
        daily_payload = {"games": [], "top_prop_model_candidates": []}
        errors.append({"source": "daily_odds", "error": str(exc), "type": exc.__class__.__name__})

    try:
        projection_payload = build_model_projection_payload(session, target_date)
    except Exception as exc:
        projection_payload = {"games": []}
        errors.append({"source": "model_projections", "error": str(exc), "type": exc.__class__.__name__})

    dashboard_payloads: Dict[str, Dict[str, Any]] = {}
    for component in DASHBOARD_COMPONENTS:
        try:
            dashboard_payloads[component] = build_dashboard_solver_payload(session=session, date=target_date, component=component)
        except Exception as exc:
            dashboard_payloads[component] = {"items": []}
            errors.append({"source": "my_dashboard", "component": component, "error": str(exc), "type": exc.__class__.__name__})

    games: List[Dict[str, Any]] = [_game_from_matchup(matchup) for matchup in matchups or [] if isinstance(matchup, dict)]
    if not games:
        daily_by_pk, _ = _index_daily_odds_games(daily_payload)
        for daily_game in daily_by_pk.values():
            games.append({
                "game_pk": daily_game.get("game_pk"),
                "away_team": daily_game.get("away_team"),
                "home_team": daily_game.get("home_team"),
                "match_key": _game_key(daily_game.get("away_team"), daily_game.get("home_team")),
                "raw_matchup": {},
            })

    daily_by_pk, daily_by_key = _index_daily_odds_games(daily_payload)
    projections_by_pk = _index_projection_games(projection_payload)
    dashboard_by_game = _dashboard_items_by_game(dashboard_payloads)
    prop_candidates = [candidate for candidate in daily_payload.get("top_prop_model_candidates") or [] if isinstance(candidate, dict)]

    game_payloads: List[Dict[str, Any]] = []
    for game in games:
        candidates: List[Dict[str, Any]] = []
        game_pk_key = str(game.get("game_pk")) if game.get("game_pk") else ""
        daily_game = daily_by_pk.get(game_pk_key) or daily_by_key.get(game.get("match_key") or "") or {}
        models = daily_game.get("models") or {}
        if isinstance(models, dict):
            for market_key, model in models.items():
                if isinstance(model, dict):
                    candidates.append(_candidate_from_daily_model(daily_game or game, str(market_key), model))

        projection_game = projections_by_pk.get(game_pk_key)
        if projection_game:
            for family in ("moneyline", "full_game_total"):
                projection_candidate = _candidate_from_projection(game, projection_game, family)
                if projection_candidate:
                    candidates.append(projection_candidate)

        for prop in prop_candidates:
            if prop.get("game_pk") and str(prop.get("game_pk")) == game_pk_key:
                candidates.append(_candidate_from_prop(prop))

        component_map = dashboard_by_game.get(game_pk_key, {})
        dashboard_specs = [
            ("pitchers", "pitcher_strikeouts", "Pitcher strikeout/contact lean"),
            ("hitters", "hitter_hit", "Hitter hit/arsenal lean"),
            ("teams", "team_total_offense", "Team offensive lean"),
            ("totals", "full_game_total", "Total/environment lean"),
            ("overall_players", "hitter_total_bases_power", "Overall player prop lean"),
        ]
        for component, family, label in dashboard_specs:
            for item in (component_map.get(component) or [])[:1]:
                candidates.append(_candidate_from_dashboard(game, component, item, family, label))

        _add_required_placeholders(game, candidates)
        plays = [_qualify_play(candidate, target_date) for candidate in candidates]
        plays.sort(key=lambda play: (
            {"qualified": 4, "watchlist": 3, "model_signal": 2, "rejected": 1}.get(str(play.get("play_tier")), 0),
            _safe_float(play.get("edge")) or 0.0,
            _safe_float(play.get("confidence")) or 0.0,
            _safe_float(play.get("data_quality_score")) or 0.0,
        ), reverse=True)
        game_payloads.append({
            "game_pk": game.get("game_pk"),
            "away_team": game.get("away_team"),
            "home_team": game.get("home_team"),
            "candidate_count": len(plays),
            "qualified_count": sum(1 for play in plays if play.get("play_tier") == "qualified"),
            "watchlist_count": sum(1 for play in plays if play.get("play_tier") == "watchlist"),
            "model_signal_count": sum(1 for play in plays if play.get("play_tier") == "model_signal"),
            "rejected_count": sum(1 for play in plays if play.get("play_tier") == "rejected"),
            "plays": plays,
        })

    return {
        "date": target_date,
        "games": game_payloads,
        "summary": _summarize_games(game_payloads),
        "errors": errors,
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


def normalize_best_play_rows(payload: Dict[str, Any], target_date: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for game in payload.get("games") or []:
        if not isinstance(game, dict):
            continue
        for play in game.get("plays") or []:
            if not isinstance(play, dict):
                continue
            tier = play.get("play_tier") or "model_signal"
            grade = "pending" if tier == "qualified" and play.get("odds_backed") else "watchlist_only"
            if tier == "rejected":
                grade = "ungraded"
            raw_payload = dict(play)
            raw_payload["best_plays_summary"] = payload.get("summary") or {}
            rows.append({
                "snapshot_date": target_date[:10],
                "source": "best_plays",
                "source_endpoint": "/model-tracker/snapshot",
                "source_component": tier,
                "game_pk": play.get("game_pk"),
                "event_id": play.get("event_id"),
                "team_id": play.get("team_id"),
                "player_id": play.get("player_id"),
                "player_name": play.get("player_name"),
                "team_name": play.get("team_name"),
                "opponent_name": play.get("opponent_name"),
                "away_team": play.get("away_team") or game.get("away_team"),
                "home_team": play.get("home_team") or game.get("home_team"),
                "market_type": play.get("market_type"),
                "pick_type": play.get("pick_type"),
                "pick_label": play.get("selection"),
                "model_name": play.get("model_name"),
                "model_version": "best_plays_v1",
                "model_probability": play.get("model_probability"),
                "market_implied_probability": play.get("market_implied_probability"),
                "edge": play.get("edge"),
                "score": play.get("score"),
                "confidence": play.get("confidence"),
                "line": play.get("line"),
                "price": play.get("price"),
                "expected_value": play.get("expected_value"),
                "primary_reason": play.get("primary_reason"),
                "reasoning_json": json.dumps({
                    "play_tier": tier,
                    "play_strength": play.get("play_strength"),
                    "fair_price": play.get("fair_price"),
                    "data_quality_score": play.get("data_quality_score"),
                    "critical_missing_inputs": play.get("critical_missing_inputs") or [],
                    "rejection_reasons": play.get("rejection_reasons") or [],
                }, default=str, sort_keys=True),
                "features_used_json": json.dumps(play.get("features_used") or [], default=str, sort_keys=True),
                "missing_inputs_json": json.dumps(play.get("missing_inputs") or [], default=str, sort_keys=True),
                "raw_payload_json": json.dumps(raw_payload, default=str, sort_keys=True),
                "result_status": "pending",
                "grade": grade,
                "grade_reason": f"Best Plays Engine classified this candidate as {tier}.",
            })
    return rows
