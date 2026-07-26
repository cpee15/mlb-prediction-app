from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import Column, Date, DateTime, Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Session

from .database import Base
from .matchup_generator import generate_matchups_for_date
from .model_projections import build_model_projection_payload
from .my_dashboard_solver import build_dashboard_solver_payload

MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
TRACKER_GRADES = {"pending", "live", "won", "lost", "push", "partial", "ungraded", "watchlist_only", "error"}
DASHBOARD_COMPONENTS = ("hitters", "pitchers", "teams", "totals", "overall_players")
AI_TRACKER_PROMPTS = (
    "What is the strongest model edge?",
    "Best Stored 365 hitter matchups",
    "Top pitcher leans",
    "What data is stale or weak?",
)
CANONICAL_MODEL_VERSION = "canonical_matchup_win_probability_v2"
LEGACY_MODEL_VERSION = "legacy_matchup_win_probability_v1"


class ModelTrackerSnapshot(Base):
    """Persistent, additive record of what an existing model surface said at snapshot time.

    Canonical probability v2 diagnostics are intentionally stored in the existing
    JSON text columns instead of adding production-risky physical columns. This
    keeps the tracker schema guarded while preserving backtest/debug detail.
    """

    __tablename__ = "model_tracker_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tracker_key = Column(String(500), nullable=False, unique=True, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)

    source = Column(String(80), nullable=True, index=True)
    source_endpoint = Column(String(180), nullable=True)
    source_component = Column(String(80), nullable=True, index=True)

    game_pk = Column(Integer, nullable=True, index=True)
    event_id = Column(String(120), nullable=True, index=True)
    team_id = Column(Integer, nullable=True, index=True)
    player_id = Column(Integer, nullable=True, index=True)
    player_name = Column(String(180), nullable=True)
    team_name = Column(String(180), nullable=True)
    opponent_name = Column(String(180), nullable=True)
    away_team = Column(String(180), nullable=True)
    home_team = Column(String(180), nullable=True)

    market_type = Column(String(80), nullable=True, index=True)
    pick_type = Column(String(80), nullable=True, index=True)
    pick_label = Column(String(240), nullable=True)
    model_name = Column(String(120), nullable=True)
    model_version = Column(String(80), nullable=True)

    model_probability = Column(Float, nullable=True)
    market_implied_probability = Column(Float, nullable=True)
    edge = Column(Float, nullable=True)
    score = Column(Float, nullable=True)
    confidence = Column(Float, nullable=True)
    line = Column(Float, nullable=True)
    price = Column(Float, nullable=True)
    expected_value = Column(Float, nullable=True)
    projected_total = Column(Float, nullable=True)
    projected_home_runs = Column(Float, nullable=True)
    projected_away_runs = Column(Float, nullable=True)
    home_win_probability = Column(Float, nullable=True)
    away_win_probability = Column(Float, nullable=True)

    primary_reason = Column(Text, nullable=True)
    reasoning_json = Column(Text, nullable=True)
    features_used_json = Column(Text, nullable=True)
    missing_inputs_json = Column(Text, nullable=True)
    raw_payload_json = Column(Text, nullable=True)

    game_status_at_snapshot = Column(String(120), nullable=True)
    result_status = Column(String(80), default="pending", nullable=False, index=True)
    actual_result_json = Column(Text, nullable=True)
    grade = Column(String(40), default="pending", nullable=False, index=True)
    grade_reason = Column(Text, nullable=True)
    last_compared_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("tracker_key", name="uq_model_tracker_snapshots_tracker_key"),
        Index("ix_model_tracker_date_game", "snapshot_date", "game_pk"),
        Index("ix_model_tracker_date_source", "snapshot_date", "source", "source_component"),
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


def _json(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return json.dumps(value, default=str, sort_keys=True)
    except Exception:
        return json.dumps(str(value))


def _loads(value: Optional[str]) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


def _short_reason(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, list):
        return "; ".join(str(v) for v in value[:3])[:1000] or fallback
    if isinstance(value, dict):
        return json.dumps(value, default=str)[:1000] or fallback
    return str(value)[:1000] or fallback


def _normalize_name(name: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower().replace("the", "", 1))


def _tracker_key(row: Dict[str, Any]) -> str:
    parts = [
        row.get("snapshot_date"), row.get("source"), row.get("source_component"), row.get("game_pk"),
        row.get("event_id"), row.get("player_id"), row.get("team_id"), row.get("market_type"),
        row.get("pick_type"), row.get("pick_label"), row.get("model_name"), row.get("line"),
    ]
    raw = "|".join(str(part or "") for part in parts)
    if len(raw) <= 450:
        return raw
    return raw[:390] + "|" + hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _base_row(source: str, target_date: str, **kwargs: Any) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "snapshot_date": target_date[:10],
        "source": source,
        "source_endpoint": kwargs.pop("source_endpoint", None),
        "source_component": kwargs.pop("source_component", None),
        "game_pk": kwargs.pop("game_pk", None),
        "event_id": kwargs.pop("event_id", None),
        "team_id": kwargs.pop("team_id", None),
        "player_id": kwargs.pop("player_id", None),
        "player_name": kwargs.pop("player_name", None),
        "team_name": kwargs.pop("team_name", None),
        "opponent_name": kwargs.pop("opponent_name", None),
        "away_team": kwargs.pop("away_team", None),
        "home_team": kwargs.pop("home_team", None),
        "market_type": kwargs.pop("market_type", None),
        "pick_type": kwargs.pop("pick_type", None),
        "pick_label": kwargs.pop("pick_label", None),
        "model_name": kwargs.pop("model_name", None),
        "model_version": kwargs.pop("model_version", None),
        "model_probability": kwargs.pop("model_probability", None),
        "market_implied_probability": kwargs.pop("market_implied_probability", None),
        "edge": kwargs.pop("edge", None),
        "score": kwargs.pop("score", None),
        "confidence": kwargs.pop("confidence", None),
        "line": kwargs.pop("line", None),
        "price": kwargs.pop("price", None),
        "expected_value": kwargs.pop("expected_value", None),
        "projected_total": kwargs.pop("projected_total", None),
        "projected_home_runs": kwargs.pop("projected_home_runs", None),
        "projected_away_runs": kwargs.pop("projected_away_runs", None),
        "home_win_probability": kwargs.pop("home_win_probability", None),
        "away_win_probability": kwargs.pop("away_win_probability", None),
        "primary_reason": kwargs.pop("primary_reason", None),
        "reasoning_json": kwargs.pop("reasoning_json", None),
        "features_used_json": kwargs.pop("features_used_json", None),
        "missing_inputs_json": kwargs.pop("missing_inputs_json", None),
        "raw_payload_json": kwargs.pop("raw_payload_json", None),
        "game_status_at_snapshot": kwargs.pop("game_status_at_snapshot", None),
        "result_status": kwargs.pop("result_status", "pending"),
        "grade": kwargs.pop("grade", "pending"),
        "grade_reason": kwargs.pop("grade_reason", None),
    }
    for key, value in kwargs.items():
        row[key] = value
    row["tracker_key"] = _tracker_key(row)
    return row


def _team_name(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        return value.get("name") or value.get("team_name")
    return value


def _canonical_probability_bundle(payload: Dict[str, Any]) -> Dict[str, Any]:
    probs = payload.get("main_matchup_probabilities") if isinstance(payload.get("main_matchup_probabilities"), dict) else payload
    workspace = payload.get("workspace") or {}
    canonical_workspace = workspace.get("canonicalMatchupProbability") or {}
    if canonical_workspace:
        probs = {**canonical_workspace, **(probs or {})}
    home_prob = _safe_float(probs.get("home_win_prob") or probs.get("home_win_probability") or payload.get("home_win_prob") or payload.get("home_win_probability"))
    away_prob = _safe_float(probs.get("away_win_prob") or probs.get("away_win_probability") or payload.get("away_win_prob") or payload.get("away_win_probability"))
    legacy_home = _safe_float(probs.get("legacy_home_win_prob") or payload.get("legacy_home_win_prob"))
    legacy_away = _safe_float(probs.get("legacy_away_win_prob") or payload.get("legacy_away_win_prob"))
    components = probs.get("probability_components") or payload.get("probability_components") or {}
    missing_inputs = probs.get("missing_inputs") or payload.get("missing_inputs") or []
    sim_diag = probs.get("simulation_diagnostic") or workspace.get("sharedSimulationDiagnostics") or workspace.get("simulationDiagnostics") or {}
    return {
        "canonical_home_win_prob": home_prob,
        "canonical_away_win_prob": away_prob,
        "legacy_home_win_prob": legacy_home,
        "legacy_away_win_prob": legacy_away,
        "model_version": probs.get("model_version") or payload.get("model_version") or CANONICAL_MODEL_VERSION,
        "legacy_model_version": probs.get("legacy_model_version") or payload.get("legacy_model_version") or LEGACY_MODEL_VERSION,
        "lineup_status": probs.get("lineup_status") or payload.get("lineup_status"),
        "data_confidence": probs.get("data_confidence") or payload.get("data_confidence"),
        "missing_inputs": missing_inputs,
        "probability_components": components,
        "probability_component_keys": sorted(components.keys()) if isinstance(components, dict) else [],
        "pitcher_overview": probs.get("pitcher_overview") or payload.get("pitcher_overview") or {},
        "batter_vs_arsenal_summary": probs.get("batter_vs_arsenal_summary") or payload.get("batter_vs_arsenal_summary") or {},
        "simulation_diagnostic": sim_diag,
        "final_probability_source": probs.get("source") or "matchups.canonical_matchup_win_probability_v2",
        "simulation_role": (sim_diag or {}).get("status") or "diagnostic_only_not_final_probability",
    }


def _canonical_features(bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {"name": "home_win_prob", "value": bundle.get("canonical_home_win_prob"), "source": bundle.get("final_probability_source"), "role": "canonical_final_probability"},
        {"name": "away_win_prob", "value": bundle.get("canonical_away_win_prob"), "source": bundle.get("final_probability_source"), "role": "canonical_final_probability"},
        {"name": "legacy_home_win_prob", "value": bundle.get("legacy_home_win_prob"), "source": bundle.get("legacy_model_version"), "role": "legacy_comparison_only"},
        {"name": "legacy_away_win_prob", "value": bundle.get("legacy_away_win_prob"), "source": bundle.get("legacy_model_version"), "role": "legacy_comparison_only"},
        {"name": "lineup_status", "value": bundle.get("lineup_status"), "source": "canonical_probability_diagnostics"},
        {"name": "data_confidence", "value": bundle.get("data_confidence"), "source": "canonical_probability_diagnostics"},
        {"name": "probability_component_keys", "value": bundle.get("probability_component_keys"), "source": "canonical_probability_diagnostics"},
    ]


def _canonical_reasoning(bundle: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "canonical_probability": bundle,
        "contract": {
            "final_probability_fields": ["home_win_prob", "away_win_prob"],
            "legacy_probability_fields": ["legacy_home_win_prob", "legacy_away_win_prob"],
            "simulation_role": "diagnostic_only_not_final_probability",
            "schema_guard": "stored_in_existing_json_text_columns_no_physical_migration",
        },
        "extra": extra or {},
    }


def _daily_odds_tracker_extra(model: Dict[str, Any], bundle: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    extra = {
        "confidence_tier": model.get("confidence_tier"),
        "data_quality_score": model.get("data_quality_score"),
        "recommendation_status": model.get("recommendation_status"),
        "rejection_reason": model.get("rejection_reason"),
        "expected_value": model.get("expected_value"),
        "usage_weighted_gate": (model.get("diagnostics") or {}).get("usage_weighted_gate"),
    }
    if bundle is not None:
        extra["canonical_probability"] = bundle
    return extra


def _team_pick_from_canonical(bundle: Dict[str, Any], home_team: Any, away_team: Any) -> tuple[Optional[str], Optional[float]]:
    home_prob = _safe_float(bundle.get("canonical_home_win_prob"))
    away_prob = _safe_float(bundle.get("canonical_away_win_prob"))
    if home_prob is None or away_prob is None:
        return None, None
    return (_team_name(home_team), home_prob) if home_prob >= away_prob else (_team_name(away_team), away_prob)


def normalize_matchup_rows(matchups: List[Dict[str, Any]], target_date: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for matchup in matchups or []:
        home_team = matchup.get("home_team_name") or matchup.get("home_team")
        away_team = matchup.get("away_team_name") or matchup.get("away_team")
        bundle = _canonical_probability_bundle(matchup)
        pick, probability = _team_pick_from_canonical(bundle, home_team, away_team)
        rows.append(_base_row(
            "matchups", target_date,
            source_endpoint="/matchups",
            source_component="canonical_game_snapshot",
            game_pk=matchup.get("game_pk"),
            away_team=away_team,
            home_team=home_team,
            market_type="game",
            pick_type="canonical_game_snapshot",
            pick_label=f"{away_team} @ {home_team}",
            model_name="daily_matchup_snapshot",
            model_version=bundle.get("model_version"),
            home_win_probability=bundle.get("canonical_home_win_prob"),
            away_win_probability=bundle.get("canonical_away_win_prob"),
            primary_reason="Canonical matchup snapshot captured for tracker comparison.",
            reasoning_json=_json(_canonical_reasoning(bundle)),
            features_used_json=_json(_canonical_features(bundle)),
            missing_inputs_json=_json(bundle.get("missing_inputs") or []),
            raw_payload_json=_json(matchup),
            game_status_at_snapshot=matchup.get("status"),
            grade="ungraded",
            result_status="pending",
        ))
        if pick:
            rows.append(_base_row(
                "matchups", target_date,
                source_endpoint="/matchups",
                source_component="canonical_moneyline_side",
                game_pk=matchup.get("game_pk"),
                away_team=away_team,
                home_team=home_team,
                team_name=pick,
                market_type="moneyline",
                pick_type="side",
                pick_label=pick,
                model_name="canonical_matchup_win_probability",
                model_version=bundle.get("model_version"),
                model_probability=probability,
                score=probability,
                confidence=probability,
                home_win_probability=bundle.get("canonical_home_win_prob"),
                away_win_probability=bundle.get("canonical_away_win_prob"),
                primary_reason=f"Canonical v2 side from matchup win probability: {pick}",
                reasoning_json=_json(_canonical_reasoning(bundle, {"selected_side": pick})),
                features_used_json=_json(_canonical_features(bundle)),
                missing_inputs_json=_json(bundle.get("missing_inputs") or []),
                raw_payload_json=_json(matchup),
                game_status_at_snapshot=matchup.get("status"),
                grade="pending",
                result_status="pending",
            ))
    return rows


def normalize_daily_odds_rows(payload: Dict[str, Any], target_date: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for game in payload.get("games") or payload.get("models") or []:
        if not isinstance(game, dict):
            continue
        away_team = game.get("away_team") or game.get("away_team_name")
        home_team = game.get("home_team") or game.get("home_team_name")
        bundle = _canonical_probability_bundle(game)
        models = game.get("models") or {}
        for market_key, model in models.items() if isinstance(models, dict) else []:
            if not isinstance(model, dict):
                continue
            model_features = list(model.get("features_used") or []) + _canonical_features(bundle)
            reasoning = {
                "drivers": model.get("drivers") or model.get("reasoning") or [],
                "canonical_probability": bundle,
                "daily_odds_diagnostics": _daily_odds_tracker_extra(model, bundle),
                "market_context_note": "Daily Odds compares sportsbook implied probability against canonical probability; odds do not define canonical probability.",
            }
            rows.append(_base_row(
                "daily_odds", target_date,
                source_endpoint="/daily-odds/models",
                source_component=str(market_key),
                game_pk=game.get("game_pk"),
                event_id=game.get("event_id"),
                away_team=away_team,
                home_team=home_team,
                market_type=model.get("market") or market_key,
                pick_type=market_key,
                pick_label=model.get("pick") or model.get("selection"),
                model_name=model.get("model") or "daily_odds_model",
                model_version=bundle.get("model_version") if market_key == "moneyline" else model.get("model_version"),
                model_probability=_safe_float(model.get("model_probability")),
                market_implied_probability=_safe_float(model.get("market_implied_probability")),
                edge=_safe_float(model.get("edge")),
                score=_safe_float(model.get("score")),
                confidence=_safe_float(model.get("confidence")),
                line=_safe_float(model.get("line")),
                price=_safe_float(model.get("price")),
                expected_value=_safe_float(model.get("expected_value")),
                home_win_probability=bundle.get("canonical_home_win_prob"),
                away_win_probability=bundle.get("canonical_away_win_prob"),
                primary_reason=_short_reason(model.get("drivers") or model.get("reasoning") or model.get("primary_reason"), "Daily Odds model output."),
                reasoning_json=_json(reasoning),
                features_used_json=_json(model_features + [
                    {"name": "confidence_tier", "value": model.get("confidence_tier"), "source": "daily_odds_models"},
                    {"name": "data_quality_score", "value": model.get("data_quality_score"), "source": "daily_odds_models"},
                    {"name": "recommendation_status", "value": model.get("recommendation_status"), "source": "daily_odds_models"},
                ]),
                missing_inputs_json=_json((model.get("missing_inputs") or []) + (bundle.get("missing_inputs") or [])),
                raw_payload_json=_json({"game": game, "model": model, "canonical_probability": bundle, "daily_odds_tracker_diagnostics": _daily_odds_tracker_extra(model, bundle)}),
                grade="pending" if model.get("pick") else "ungraded",
                result_status="pending",
            ))
    for candidate in payload.get("top_prop_model_candidates") or []:
        if not isinstance(candidate, dict):
            continue
        reasoning = {
            "drivers": candidate.get("drivers") or candidate.get("reasoning") or [],
            "daily_odds_diagnostics": _daily_odds_tracker_extra(candidate),
        }
        rows.append(_base_row(
            "daily_odds", target_date,
            source_endpoint="/daily-odds/models",
            source_component="top_prop_model_candidates",
            game_pk=candidate.get("game_pk"),
            event_id=candidate.get("event_id"),
            away_team=candidate.get("away_team"),
            home_team=candidate.get("home_team"),
            player_id=candidate.get("player_id"),
            player_name=candidate.get("player_name") or candidate.get("entity_name"),
            market_type=candidate.get("market") or candidate.get("market_family") or "prop_watchlist",
            pick_type="prop_watchlist",
            pick_label=candidate.get("pick") or candidate.get("selection") or candidate.get("player_name"),
            model_name=candidate.get("model") or "daily_odds_prop_candidate",
            model_probability=_safe_float(candidate.get("model_probability")),
            market_implied_probability=_safe_float(candidate.get("market_implied_probability")),
            edge=_safe_float(candidate.get("edge")),
            score=_safe_float(candidate.get("score")),
            confidence=_safe_float(candidate.get("confidence")),
            line=_safe_float(candidate.get("line")),
            price=_safe_float(candidate.get("price")),
            expected_value=_safe_float(candidate.get("expected_value")),
            primary_reason=_short_reason(candidate.get("drivers") or candidate.get("reasoning") or candidate.get("primary_reason"), "Daily Odds prop/watchlist candidate."),
            reasoning_json=_json(reasoning),
            features_used_json=_json((candidate.get("features_used") or []) + [
                {"name": "confidence_tier", "value": candidate.get("confidence_tier"), "source": "daily_odds_models"},
                {"name": "data_quality_score", "value": candidate.get("data_quality_score"), "source": "daily_odds_models"},
                {"name": "recommendation_status", "value": candidate.get("recommendation_status"), "source": "daily_odds_models"},
            ]),
            missing_inputs_json=_json(candidate.get("missing_inputs") or []),
            raw_payload_json=_json({**candidate, "daily_odds_tracker_diagnostics": _daily_odds_tracker_extra(candidate)}),
            grade="ungraded",
            grade_reason="Stored as watchlist output until explicit player-stat result mapping is available.",
            result_status="pending",
        ))
    return rows


def normalize_model_projection_rows(payload: Dict[str, Any], target_date: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for game in payload.get("games") or payload.get("models") or []:
        if not isinstance(game, dict):
            continue
        workspace = game.get("workspace") or {}
        simulation = workspace.get("bullpenAdjustedGameSimulation") or workspace.get("gameSimulation") or game.get("gameSimulation") or {}
        away_team = _team_name(game.get("away_team") or game.get("away_team_name"))
        home_team = _team_name(game.get("home_team") or game.get("home_team_name"))
        bundle = _canonical_probability_bundle(game)
        home_prob = _safe_float(bundle.get("canonical_home_win_prob"))
        away_prob = _safe_float(bundle.get("canonical_away_win_prob"))
        projected_total = _safe_float(simulation.get("total_expected_runs") or game.get("total_expected_runs"))
        projected_home = _safe_float(simulation.get("home_expected_runs") or game.get("home_expected_runs"))
        projected_away = _safe_float(simulation.get("away_expected_runs") or game.get("away_expected_runs"))
        pick, pick_prob = _team_pick_from_canonical(bundle, home_team, away_team)
        sim_diag = {
            "status": "diagnostic_only_not_final_probability",
            "home_diagnostic_win_probability": simulation.get("home_win_probability"),
            "away_diagnostic_win_probability": simulation.get("away_win_probability"),
            "total_expected_runs": projected_total,
            "home_expected_runs": projected_home,
            "away_expected_runs": projected_away,
        }
        if pick:
            rows.append(_base_row(
                "model_projections", target_date,
                source_endpoint="/models/projections",
                source_component="canonical_projected_side",
                game_pk=game.get("game_pk"),
                away_team=away_team,
                home_team=home_team,
                team_name=pick,
                market_type="moneyline",
                pick_type="side",
                pick_label=pick,
                model_name="canonical_model_projection_win_probability",
                model_version=bundle.get("model_version"),
                model_probability=pick_prob,
                score=pick_prob,
                confidence=pick_prob,
                projected_total=projected_total,
                projected_home_runs=projected_home,
                projected_away_runs=projected_away,
                home_win_probability=home_prob,
                away_win_probability=away_prob,
                primary_reason="Model Projection side captured from canonical v2 probability. Simulation is diagnostic only.",
                reasoning_json=_json(_canonical_reasoning(bundle, {"selected_side": pick, "simulation_diagnostic": sim_diag})),
                features_used_json=_json(_canonical_features(bundle)),
                missing_inputs_json=_json(bundle.get("missing_inputs") or []),
                raw_payload_json=_json({"game": game, "canonical_probability": bundle, "simulation_diagnostic": sim_diag}),
                grade="pending",
                result_status="pending",
            ))
        if projected_total is not None:
            rows.append(_base_row(
                "model_projections", target_date,
                source_endpoint="/models/projections",
                source_component="diagnostic_projected_total",
                game_pk=game.get("game_pk"),
                away_team=away_team,
                home_team=home_team,
                market_type="total",
                pick_type="projected_total",
                pick_label=f"Projected total {projected_total}",
                model_name="diagnostic_model_projection_total_runs",
                model_version=(simulation or {}).get("model_version"),
                score=projected_total,
                projected_total=projected_total,
                projected_home_runs=projected_home,
                projected_away_runs=projected_away,
                home_win_probability=home_prob,
                away_win_probability=away_prob,
                primary_reason="Diagnostic total expected runs captured for comparison; side probability remains canonical v2.",
                reasoning_json=_json({"canonical_probability": bundle, "simulation_diagnostic": sim_diag}),
                features_used_json=_json(_canonical_features(bundle)),
                missing_inputs_json=_json(bundle.get("missing_inputs") or []),
                raw_payload_json=_json({"game": game, "canonical_probability": bundle, "simulation_diagnostic": sim_diag}),
                grade="ungraded",
                grade_reason="Projected total stored for after-the-fact comparison; sportsbook line may be absent.",
                result_status="pending",
            ))
    return rows


def normalize_dashboard_rows(component: str, payload: Dict[str, Any], target_date: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        metrics = item.get("metrics") or {}
        entity_type = item.get("entity_type")
        rows.append(_base_row(
            "my_dashboard", target_date,
            source_endpoint="/my-dashboard/solver",
            source_component=component,
            game_pk=item.get("game_pk"),
            team_id=(item.get("team_id") or item.get("entity_id")) if entity_type == "team" else item.get("team_id"),
            player_id=(item.get("player_id") or item.get("entity_id")) if entity_type in {"hitter", "pitcher", "player"} else item.get("player_id"),
            player_name=(item.get("player_name") or item.get("entity_name")) if entity_type in {"hitter", "pitcher", "player"} else item.get("player_name"),
            team_name=(item.get("team") or item.get("team_name") or item.get("entity_name")) if entity_type == "team" else (item.get("team") or item.get("team_name")),
            opponent_name=item.get("opponent"),
            market_type=component,
            pick_type="dashboard_watchlist",
            pick_label=item.get("pick") or item.get("entity_name") or item.get("player_name") or item.get("category"),
            model_name=f"my_dashboard_{component}",
            score=_safe_float(item.get("score") or item.get("adjusted_score")),
            confidence=_safe_float(item.get("confidence")),
            projected_total=_safe_float(metrics.get("projected_total") or item.get("projected_total")),
            primary_reason=_short_reason(item.get("primary_reason") or item.get("reasoning"), f"My Dashboard {component} watchlist output."),
            reasoning_json=_json(item.get("reasoning") or []),
            features_used_json=_json(item.get("features_used") or []),
            missing_inputs_json=_json(item.get("missing_data") or item.get("missing_inputs") or []),
            raw_payload_json=_json(item),
            grade="ungraded",
            grade_reason="Dashboard watchlist outputs are stored first and graded only when an explicit result mapping is available.",
            result_status="pending",
        ))
    return rows


def normalize_ai_data_assistant_rows(payload: Dict[str, Any], target_date: str, prompt: str) -> List[Dict[str, Any]]:
    canonical_context = payload.get("canonical_probability_context") or {}
    daily_odds_context = payload.get("daily_odds_diagnostics_context") or {}
    return [_base_row(
        "ai_data_assistant", target_date,
        source_endpoint="/ai-data-assistant",
        source_component="deterministic_summary",
        game_pk=payload.get("game_pk"),
        market_type="assistant_summary",
        pick_type="ai_summary",
        pick_label=prompt,
        model_name="ai_data_assistant_deterministic",
        model_version=canonical_context.get("model_version") or CANONICAL_MODEL_VERSION,
        primary_reason=_short_reason(payload.get("answer"), "AI Data Assistant deterministic answer captured."),
        reasoning_json=_json({"canonical_probability_context": canonical_context, "daily_odds_diagnostics_context": daily_odds_context, "sections": payload.get("sections") or []}),
        features_used_json=_json(payload.get("sources_used") or []),
        missing_inputs_json=_json(payload.get("missing_data") or []),
        raw_payload_json=_json(payload),
        grade="ungraded",
        grade_reason="Assistant summaries are stored as explainability snapshots, not direct betting outcomes.",
        result_status="pending",
    )]


def upsert_tracker_rows(session: Session, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    inserted = 0
    updated = 0
    now = dt.datetime.utcnow()
    for row in rows:
        tracker_key = row.get("tracker_key") or _tracker_key(row)
        existing = session.query(ModelTrackerSnapshot).filter(ModelTrackerSnapshot.tracker_key == tracker_key).first()
        if existing:
            target = existing
            updated += 1
        else:
            target = ModelTrackerSnapshot(tracker_key=tracker_key)
            session.add(target)
            inserted += 1
        for key, value in row.items():
            if not hasattr(target, key):
                continue
            if key == "snapshot_date" and isinstance(value, str):
                value = dt.date.fromisoformat(value[:10])
            elif key in {"game_pk", "team_id", "player_id"}:
                value = _safe_int(value)
            elif key in {
                "model_probability", "market_implied_probability", "edge", "score", "confidence", "line", "price",
                "expected_value", "projected_total", "projected_home_runs", "projected_away_runs",
                "home_win_probability", "away_win_probability",
            }:
                value = _safe_float(value)
            setattr(target, key, value)
        target.updated_at = now
    session.commit()
    return {"inserted": inserted, "updated": updated, "total_rows_seen": len(rows)}


def build_tracker_snapshot(session: Session, target_date: str) -> Dict[str, Any]:
    target_date = target_date[:10]
    dt.date.fromisoformat(target_date)
    errors: List[Dict[str, Any]] = []
    rows: List[Dict[str, Any]] = []

    try:
        matchups = generate_matchups_for_date(session, target_date)
        rows.extend(normalize_matchup_rows(matchups, target_date))
    except Exception as exc:
        errors.append({"source": "matchups", "error": str(exc), "type": exc.__class__.__name__})

    try:
        from .daily_odds_routes import daily_odds_models
        payload = daily_odds_models(date=target_date, include_unified=True)
        rows.extend(normalize_daily_odds_rows(payload, target_date))
    except Exception as exc:
        errors.append({"source": "daily_odds", "error": str(exc), "type": exc.__class__.__name__})

    try:
        payload = build_model_projection_payload(session, target_date)
        rows.extend(normalize_model_projection_rows(payload, target_date))
    except Exception as exc:
        errors.append({"source": "model_projections", "error": str(exc), "type": exc.__class__.__name__})

    for component in DASHBOARD_COMPONENTS:
        try:
            payload = build_dashboard_solver_payload(session=session, date=target_date, component=component)
            rows.extend(normalize_dashboard_rows(component, payload, target_date))
        except Exception as exc:
            errors.append({"source": "my_dashboard", "component": component, "error": str(exc), "type": exc.__class__.__name__})

    try:
        from .ai_data_assistant_performance import build_ai_data_assistant_response
        for prompt in AI_TRACKER_PROMPTS:
            try:
                payload = build_ai_data_assistant_response(session=session, message=prompt, date=target_date, use_llm=False)
                rows.extend(normalize_ai_data_assistant_rows(payload, target_date, prompt))
            except Exception as prompt_exc:
                errors.append({"source": "ai_data_assistant", "prompt": prompt, "error": str(prompt_exc), "type": prompt_exc.__class__.__name__})
    except Exception as exc:
        errors.append({"source": "ai_data_assistant", "error": str(exc), "type": exc.__class__.__name__})

    upsert_result = upsert_tracker_rows(session, rows)
    return {"date": target_date, "rows_collected": len(rows), "upsert": upsert_result, "errors": errors}


def _row_payload(row: ModelTrackerSnapshot) -> Dict[str, Any]:
    reasoning = _loads(row.reasoning_json) or []
    features = _loads(row.features_used_json) or []
    raw_payload = _loads(row.raw_payload_json)
    canonical_diagnostics = None
    daily_odds_diagnostics = None
    if isinstance(reasoning, dict):
        canonical_diagnostics = reasoning.get("canonical_probability") or reasoning.get("canonical_probability_context")
        daily_odds_diagnostics = reasoning.get("daily_odds_diagnostics") or reasoning.get("daily_odds_diagnostics_context")
    tracker_contract = reasoning.get("tracker_contract") if isinstance(reasoning, dict) else {}
    return {
        "row_type": tracker_contract.get("row_type") or ("reportable_decision" if row.source == "bet105" and row.pick_type == "reportable_decision" else "model_signal"),
        "reportable": bool(tracker_contract.get("reportable")),
        "odds_available": bool(tracker_contract.get("odds_available")),
        "id": row.id,
        "tracker_key": row.tracker_key,
        "snapshot_date": row.snapshot_date.isoformat() if row.snapshot_date else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "source": row.source,
        "source_endpoint": row.source_endpoint,
        "source_component": row.source_component,
        "game_pk": row.game_pk,
        "event_id": row.event_id,
        "team_id": row.team_id,
        "player_id": row.player_id,
        "player_name": row.player_name,
        "team_name": row.team_name,
        "opponent_name": row.opponent_name,
        "away_team": row.away_team,
        "home_team": row.home_team,
        "market_type": row.market_type,
        "pick_type": row.pick_type,
        "pick_label": row.pick_label,
        "model_name": row.model_name,
        "model_version": row.model_version,
        "model_probability": row.model_probability,
        "market_implied_probability": row.market_implied_probability,
        "edge": row.edge,
        "score": row.score,
        "confidence": row.confidence,
        "line": row.line,
        "price": row.price,
        "expected_value": row.expected_value,
        "projected_total": row.projected_total,
        "projected_home_runs": row.projected_home_runs,
        "projected_away_runs": row.projected_away_runs,
        "home_win_probability": row.home_win_probability,
        "away_win_probability": row.away_win_probability,
        "primary_reason": row.primary_reason,
        "reasoning": reasoning,
        "features_used": features,
        "missing_inputs": _loads(row.missing_inputs_json) or [],
        "canonical_diagnostics": canonical_diagnostics,
        "daily_odds_diagnostics": daily_odds_diagnostics,
        "raw_payload": raw_payload,
        "game_status_at_snapshot": row.game_status_at_snapshot,
        "result_status": row.result_status,
        "actual_result": _loads(row.actual_result_json),
        "grade": row.grade,
        "grade_reason": row.grade_reason,
        "last_compared_at": row.last_compared_at.isoformat() if row.last_compared_at else None,
    }


def list_tracker_rows(session: Session, target_date: str, game_pk: Optional[int] = None) -> Dict[str, Any]:
    parsed = dt.date.fromisoformat(target_date[:10])
    query = session.query(ModelTrackerSnapshot).filter(ModelTrackerSnapshot.snapshot_date == parsed)
    if game_pk is not None:
        query = query.filter(ModelTrackerSnapshot.game_pk == game_pk)
    records = query.order_by(ModelTrackerSnapshot.game_pk.nullslast(), ModelTrackerSnapshot.source, ModelTrackerSnapshot.source_component, ModelTrackerSnapshot.id).all()
    rows = [_row_payload(record) for record in records]
    games: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("game_pk") or "ungrouped")
        game = games.setdefault(key, {
            "game_pk": row.get("game_pk"), "away_team": row.get("away_team"), "home_team": row.get("home_team"),
            "row_count": 0, "grades": {}, "sources": set(), "rows": [],
        })
        game["row_count"] += 1
        game["grades"][row.get("grade") or "unknown"] = game["grades"].get(row.get("grade") or "unknown", 0) + 1
        if row.get("source"):
            game["sources"].add(row.get("source"))
        game["rows"].append(row)
    game_payloads = []
    for game in games.values():
        game["sources"] = sorted(game["sources"])
        game_payloads.append(game)
    canonical_rows = [r for r in rows if r.get("canonical_diagnostics") or r.get("model_version") == CANONICAL_MODEL_VERSION]
    summary = {
        "total_rows": len(rows),
        "games_tracked": len([g for g in game_payloads if g.get("game_pk")]),
        "pending_rows": sum(1 for r in rows if r.get("grade") == "pending"),
        "live_rows": sum(1 for r in rows if r.get("result_status") == "live"),
        "graded_rows": sum(1 for r in rows if r.get("grade") in {"won", "lost", "push", "partial"}),
        "won": sum(1 for r in rows if r.get("grade") == "won"),
        "lost": sum(1 for r in rows if r.get("grade") == "lost"),
        "push": sum(1 for r in rows if r.get("grade") == "push"),
        "ungraded_rows": sum(1 for r in rows if r.get("grade") in {"ungraded", "watchlist_only"}),
        "reportable_decision_count": sum(1 for r in rows if r.get("reportable")),
        "model_signal_count": sum(1 for r in rows if r.get("row_type") == "model_signal"),
        "odds_backed_count": sum(1 for r in rows if r.get("odds_available")),
        "model_only_count": sum(1 for r in rows if not r.get("odds_available")),
        "bet105_market_count": sum(1 for r in rows if r.get("source") == "bet105"),
        "missing_odds_count": sum(1 for r in rows if r.get("row_type") == "model_signal"),
        "source_count": len({r.get("source") for r in rows if r.get("source")}),
        "sources": sorted({r.get("source") for r in rows if r.get("source")}),
        "canonical_probability_rows": len(canonical_rows),
        "canonical_model_version": CANONICAL_MODEL_VERSION,
        "schema_guard": "canonical diagnostics persisted in existing JSON text columns",
    }
    return {"date": parsed.isoformat(), "snapshot_count": len({r.get("created_at") for r in rows if r.get("created_at")}), "rows": rows, "games": game_payloads, "summary": summary, "errors": []}



def list_tracker_range(session: Session, start_date: str, end_date: str, include_raw: bool = False) -> Dict[str, Any]:
    start = dt.date.fromisoformat(start_date[:10])
    end = dt.date.fromisoformat(end_date[:10])
    if end < start:
        raise ValueError("end date must be on or after start date")
    records = (session.query(ModelTrackerSnapshot)
        .filter(ModelTrackerSnapshot.snapshot_date >= start, ModelTrackerSnapshot.snapshot_date <= end)
        .order_by(ModelTrackerSnapshot.snapshot_date, ModelTrackerSnapshot.game_pk.nullslast(), ModelTrackerSnapshot.id)
        .all())
    rows = []
    for record in records:
        payload = _row_payload(record)
        if not payload.get("reportable"):
            continue
        if not include_raw:
            payload.pop("raw_payload", None)
        rows.append(payload)
    return {"start": start.isoformat(), "end": end.isoformat(), "rows": rows,
            "summary": {"reportable_decision_count": len(rows),
                        "graded_rows": sum(1 for row in rows if row.get("grade") in {"won", "lost", "push", "partial"}),
                        "pending_rows": sum(1 for row in rows if row.get("grade") == "pending"),
                        "sources": sorted({row.get("source") for row in rows if row.get("source")})}}

def _fetch_schedule_results(target_date: str) -> Dict[int, Dict[str, Any]]:
    response = requests.get(f"{MLB_STATS_BASE}/schedule", params={"sportId": 1, "date": target_date, "hydrate": "linescore,team"}, timeout=20)
    response.raise_for_status()
    payload = response.json()
    results: Dict[int, Dict[str, Any]] = {}
    for date_row in payload.get("dates", []) or []:
        for game in date_row.get("games", []) or []:
            game_pk = game.get("gamePk")
            if not game_pk:
                continue
            teams = game.get("teams") or {}
            home = teams.get("home") or {}
            away = teams.get("away") or {}
            status = game.get("status") or {}
            home_team = (home.get("team") or {}).get("name")
            away_team = (away.get("team") or {}).get("name")
            home_score = home.get("score")
            away_score = away.get("score")
            detailed_state = status.get("detailedState")
            abstract_state = status.get("abstractGameState")
            is_final = abstract_state == "Final" or status.get("codedGameState") == "F" or str(detailed_state or "").lower() == "final"
            winner = None
            if home_score is not None and away_score is not None and home_score != away_score:
                winner = home_team if int(home_score) > int(away_score) else away_team
            results[int(game_pk)] = {
                "game_pk": int(game_pk), "status": detailed_state or abstract_state, "abstract_state": abstract_state,
                "is_final": is_final, "home_team": home_team, "away_team": away_team,
                "home_score": home_score, "away_score": away_score,
                "total_runs": int(home_score) + int(away_score) if home_score is not None and away_score is not None else None,
                "winner": winner, "raw": game,
            }
    return results


def _grade_row(row: ModelTrackerSnapshot, actual: Dict[str, Any]) -> tuple[str, str]:
    if not actual.get("is_final"):
        return "live" if actual.get("abstract_state") == "Live" else "pending", "Game is not final yet."
    pick_type = str(row.pick_type or "").lower()
    market_type = str(row.market_type or "").lower()
    pick_label = str(row.pick_label or "")
    line = row.line
    total_runs = actual.get("total_runs")
    if market_type in {"moneyline", "side"} or pick_type in {"side", "moneyline"}:
        winner = actual.get("winner")
        if not winner:
            return "push", "Final score ended tied or winner was unavailable."
        if _normalize_name(pick_label) == _normalize_name(winner) or _normalize_name(row.team_name) == _normalize_name(winner):
            return "won", f"Final winner was {winner}."
        return "lost", f"Final winner was {winner}."
    if market_type == "total" and line is not None and total_runs is not None:
        wants_over = "over" in pick_label.lower() or pick_type == "over"
        wants_under = "under" in pick_label.lower() or pick_type == "under"
        if total_runs == line:
            return "push", f"Final total {total_runs} landed on line {line}."
        if wants_over:
            return ("won" if total_runs > line else "lost", f"Final total {total_runs} compared to over {line}.")
        if wants_under:
            return ("won" if total_runs < line else "lost", f"Final total {total_runs} compared to under {line}.")
        return "ungraded", "Total row has final score but no explicit over/under side."
    return "ungraded", "Stored output has no safe automated final-result mapping yet."


def refresh_tracker_results(session: Session, target_date: str) -> Dict[str, Any]:
    parsed = dt.date.fromisoformat(target_date[:10])
    actual_by_game = _fetch_schedule_results(parsed.isoformat())
    records = session.query(ModelTrackerSnapshot).filter(ModelTrackerSnapshot.snapshot_date == parsed).all()
    compared = 0
    final_games = 0
    now = dt.datetime.utcnow()
    for row in records:
        if not row.game_pk or row.game_pk not in actual_by_game:
            continue
        actual = actual_by_game[row.game_pk]
        if actual.get("is_final"):
            final_games += 1
        grade, reason = _grade_row(row, actual)
        row.result_status = "final" if actual.get("is_final") else ("live" if actual.get("abstract_state") == "Live" else "pending")
        row.actual_result_json = _json(actual)
        row.grade = grade if grade in TRACKER_GRADES else "ungraded"
        row.grade_reason = reason
        row.last_compared_at = now
        row.updated_at = now
        compared += 1
    session.commit()
    return {"date": parsed.isoformat(), "rows_compared": compared, "games_found": len(actual_by_game), "final_game_rows_seen": final_games, "compared_at": now.isoformat() + "Z"}
