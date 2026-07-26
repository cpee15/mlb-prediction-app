"""Bet105-only Model Tracker normalization and reportability rules.

This module deliberately keeps tracker semantics separate from the broader odds and
Best Plays surfaces.  A tracker decision exists only when Bet105 supplied the
selection and a model can be matched to it.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
import re

from .model_tracker import _base_row, _safe_float, _team_name, _tracker_key


TRACKABLE_MARKETS = {"h2h", "moneyline", "spreads", "run_line", "totals", "total", "team_total"}
TOTAL_EDGE_THRESHOLD = 0.35


def _norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _implied(price: Any, decimal: Any = None) -> Optional[float]:
    american = _safe_float(price)
    if american not in (None, 0):
        return 100.0 / (american + 100.0) if american > 0 else abs(american) / (abs(american) + 100.0)
    dec = _safe_float(decimal)
    return 1.0 / dec if dec and dec > 1 else None


def _event_teams(event: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    home = _team_name(event.get("home_team") or event.get("home"))
    away = _team_name(event.get("away_team") or event.get("away"))
    participants = event.get("participants") or event.get("competitors") or []
    if isinstance(participants, list):
        for participant in participants:
            if not isinstance(participant, dict):
                continue
            name = _team_name(participant.get("name") or participant.get("team_name") or participant)
            side = str(participant.get("side") or participant.get("home_away") or "").lower()
            if side == "home":
                home = home or name
            elif side == "away":
                away = away or name
    return away, home


def _market_type(market: Dict[str, Any]) -> Optional[str]:
    key = _norm(market.get("market_key") or market.get("market_type") or market.get("market_name"))
    name = _norm(market.get("market_name"))
    if key in {"h2h", "moneyline"} or "moneyline" in name:
        return "moneyline"
    if key in {"spreads", "runline"} or "runline" in name or "spread" in name:
        return "run_line"
    if key in {"totals", "total"} or ("total" in name and "team" not in name):
        return "total"
    if key == "teamtotal" or "teamtotal" in name:
        return "team_total"
    return None


def normalize_bet105_markets(board: Dict[str, Any], target_date: str) -> List[Dict[str, Any]]:
    """Return only real, current Bet105 teams/totals selections.

    Raw market data is retained for auditability; no fallback line, price, team
    or selection is manufactured here.
    """
    rows: List[Dict[str, Any]] = []
    for event in board.get("events") or []:
        if not isinstance(event, dict):
            continue
        away, home = _event_teams(event)
        event_id = event.get("event_id") or event.get("id")
        for market in event.get("markets") or []:
            if not isinstance(market, dict):
                continue
            market_type = _market_type(market)
            if market_type not in {"moneyline", "run_line", "total", "team_total"}:
                continue
            for selection in market.get("selections") or []:
                if not isinstance(selection, dict) or selection.get("active") is False or selection.get("is_current") is False:
                    continue
                price = _safe_float(selection.get("price_american") or selection.get("price"))
                decimal = _safe_float(selection.get("price_decimal") or (selection.get("odds") or {}).get("decimal"))
                if price is None and decimal is None:
                    continue
                line = _safe_float(selection.get("line") or selection.get("points") or market.get("line") or market.get("line_key"))
                label = selection.get("name") or selection.get("selection_name") or selection.get("description")
                if not label:
                    continue
                rows.append({
                    "event_id": str(event_id) if event_id is not None else None,
                    "away_team": away,
                    "home_team": home,
                    "market_type": market_type,
                    "market_id": market.get("market_id") or market.get("id") or market.get("market_type_id"),
                    "market_name": market.get("market_name") or market.get("market_key"),
                    "selection_id": selection.get("selection_id") or selection.get("id") or selection.get("side_id"),
                    "selection": label,
                    "line": line,
                    "price": price,
                    "price_decimal": decimal,
                    "implied_probability": _implied(price, decimal),
                    "raw": {"event": event, "market": market, "selection": selection},
                })
    return rows


def _projection_index(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [row for row in rows if row.get("source") == "model_projections"]


def _game_projection(market: Dict[str, Any], projections: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    candidates = list(projections)
    away, home = _norm(market.get("away_team")), _norm(market.get("home_team"))
    for row in candidates:
        if away and home and _norm(row.get("away_team")) == away and _norm(row.get("home_team")) == home:
            return row
    return None


def _selection_probability(market: Dict[str, Any], projection: Dict[str, Any]) -> Optional[float]:
    selection = _norm(market.get("selection"))
    if selection and selection == _norm(projection.get("home_team")):
        return _safe_float(projection.get("home_win_probability"))
    if selection and selection == _norm(projection.get("away_team")):
        return _safe_float(projection.get("away_win_probability"))
    return _safe_float(projection.get("model_probability"))


def _total_side(selection: Any) -> Optional[str]:
    text = str(selection or "").lower()
    return "over" if "over" in text else ("under" if "under" in text else None)


def build_bet105_decisions(board: Dict[str, Any], projections: Iterable[Dict[str, Any]], target_date: str) -> List[Dict[str, Any]]:
    decisions: List[Dict[str, Any]] = []
    for market in normalize_bet105_markets(board, target_date):
        projection = _game_projection(market, projections)
        if not projection:
            continue
        model_probability: Optional[float] = None
        projected_total = _safe_float(projection.get("projected_total"))
        if market["market_type"] == "moneyline":
            model_probability = _selection_probability(market, projection)
        elif market["market_type"] == "total":
            side = _total_side(market["selection"])
            if projected_total is None or market["line"] is None or side is None:
                continue
            delta = projected_total - market["line"]
            if abs(delta) < TOTAL_EDGE_THRESHOLD or (delta > 0) != (side == "over"):
                continue
            # A total projection is a directional decision, not a fabricated probability.
            model_probability = _safe_float(projection.get("confidence"))
        else:
            # Run lines and team totals remain surfaced as priced market context until
            # their projection contract exposes a matching probability.
            continue
        if model_probability is None or market["implied_probability"] is None:
            continue
        edge = model_probability - market["implied_probability"]
        decimal = market["price_decimal"]
        expected_value = (model_probability * (decimal - 1.0) - (1.0 - model_probability)) if decimal and decimal > 1 else None
        if edge <= 0 and (expected_value is None or expected_value <= 0):
            continue
        decisions.append(_base_row(
            "bet105", target_date,
            source_endpoint="/sportsbook/bet105",
            source_component=market["market_name"] or market["market_type"],
            game_pk=projection.get("game_pk"), event_id=market["event_id"],
            away_team=market["away_team"], home_team=market["home_team"],
            market_type=market["market_type"], pick_type="reportable_decision",
            pick_label=market["selection"], model_name=projection.get("model_name") or "model_projections",
            model_version=projection.get("model_version"), model_probability=model_probability,
            market_implied_probability=market["implied_probability"], edge=edge, expected_value=expected_value,
            confidence=_safe_float(projection.get("confidence")), line=market["line"], price=market["price"],
            projected_total=projected_total, grade="pending", result_status="pending",
            primary_reason="Bet105 price matched to a positive model edge.",
            reasoning_json={"tracker_contract": {"row_type": "reportable_decision", "reportable": True, "odds_available": True, "book": "bet105"}, "bet105_market_id": market["market_id"], "bet105_selection_id": market["selection_id"]},
            missing_inputs_json=[], raw_payload_json=market["raw"],
        ))
    return decisions


def as_model_signals(rows: Iterable[Dict[str, Any]], target_date: str) -> List[Dict[str, Any]]:
    """Convert model and MyDashboard rows to the explicit non-reportable contract."""
    signals: List[Dict[str, Any]] = []
    for row in rows:
        source = row.get("source")
        if source not in {"model_projections", "my_dashboard"}:
            continue
        row = dict(row)
        row["pick_type"] = "model_signal"
        row["price"] = None
        row["market_implied_probability"] = None
        row["edge"] = None
        row["expected_value"] = None
        row["grade"] = "watchlist_only"
        row["grade_reason"] = "Projection only; no matching Bet105 odds available."
        row["missing_inputs_json"] = ["No matching Bet105 market/price available."]
        reasoning = row.get("reasoning_json") or {}
        if not isinstance(reasoning, dict):
            reasoning = {"source_reasoning": reasoning}
        reasoning["tracker_contract"] = {"row_type": "model_signal", "reportable": False, "odds_available": False}
        row["reasoning_json"] = reasoning
        row["tracker_key"] = _tracker_key(row)
        signals.append(row)
    return signals
