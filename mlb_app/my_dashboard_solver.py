from __future__ import annotations

import datetime as dt
from typing import Any, Callable, Dict, List, Optional

from .ai_data_assistant import (
    build_pitcher_lean_context,
    build_stored_365_sweep_context,
    nested_get,
    safe_float,
    safe_int,
    score_projection_edges,
)
from .ai_data_assistant_performance import apply_performance_patch, cached_build_model_projection_payload

SUPPORTED_COMPONENTS = {"hitters", "pitchers", "teams", "totals", "overall_players"}

COMPONENT_TITLES = {
    "hitters": "My Top Hitters Today",
    "pitchers": "My Top Pitchers Today",
    "teams": "My Top Teams Today",
    "totals": "My Top Totals Today",
    "overall_players": "My Top Overall Players Today",
}


def today() -> str:
    return dt.date.today().isoformat()


def confidence_label(value: Any) -> str:
    if isinstance(value, str):
        return value
    num = safe_float(value)
    if num is None:
        return "low"
    if num >= 0.72:
        return "high"
    if num >= 0.5:
        return "medium"
    return "low"


def rounded(value: Any, digits: int = 3) -> Optional[float]:
    num = safe_float(value)
    return round(num, digits) if num is not None else None


def build_chart_data(metrics: Dict[str, Any]) -> Dict[str, Any]:
    labels: List[str] = []
    values: List[float] = []
    for key, value in metrics.items():
        num = safe_float(value)
        if num is None:
            continue
        labels.append(key)
        values.append(round(num, 3))
    return {"labels": labels[:8], "values": values[:8]}


def dedupe_ranked_items(items: List[Dict[str, Any]], key_fn: Callable[[Dict[str, Any]], str], limit: int = 10) -> List[Dict[str, Any]]:
    best_by_key: Dict[str, Dict[str, Any]] = {}
    for item in items:
        key = key_fn(item)
        if not key:
            continue
        item["dedupe_key"] = key
        existing = best_by_key.get(key)
        if existing is None or (safe_float(item.get("score")) or -999) > (safe_float(existing.get("score")) or -999):
            best_by_key[key] = item
        elif item.get("best_pitch_angles") and existing is not None:
            existing_angles = existing.setdefault("best_pitch_angles", [])
            seen = {angle.get("pitch_type") for angle in existing_angles if isinstance(angle, dict)}
            for angle in item.get("best_pitch_angles") or []:
                if isinstance(angle, dict) and angle.get("pitch_type") not in seen:
                    existing_angles.append(angle)
                    seen.add(angle.get("pitch_type"))
            existing["best_pitch_angles"] = sorted(existing_angles, key=lambda row: safe_float(row.get("score")) or -999, reverse=True)[:3]
    ranked = sorted(best_by_key.values(), key=lambda row: safe_float(row.get("score")) or -999, reverse=True)[:limit]
    for idx, row in enumerate(ranked, start=1):
        row["rank"] = idx
    return ranked


def projection_payload(session, date: str) -> Dict[str, Any]:
    apply_performance_patch()
    try:
        return cached_build_model_projection_payload(session, date) or {}
    except Exception:
        return {}


def solve_top_hitters(session, date: str) -> Dict[str, Any]:
    context = build_stored_365_sweep_context(session, date)
    candidates: List[Dict[str, Any]] = []
    for row in context.get("top_matchups") or []:
        hitter_id = row.get("batter_id")
        if not hitter_id:
            continue
        metrics = {
            "xwOBA": rounded(row.get("hitter_xwoba")),
            "xBA": rounded(row.get("hitter_xba")),
            "EV": rounded(row.get("hitter_avg_ev"), 1),
            "LA": rounded(row.get("hitter_avg_la"), 1),
            "HardHit": rounded(row.get("hitter_hard_hit_pct")),
            "Usage": rounded(row.get("pitcher_usage_pct")),
            "Pitcher xwOBA": rounded(row.get("pitcher_xwoba_allowed")),
        }
        pitch_label = row.get("pitch_name") or row.get("pitch_type") or "pitch"
        candidate = {
            "entity_type": "hitter",
            "entity_id": str(hitter_id),
            "entity_name": row.get("batter_name") or f"Batter {hitter_id}",
            "team": row.get("batter_team_id"),
            "opponent": row.get("opposing_pitcher_name") or row.get("opposing_pitcher_id"),
            "game_pk": row.get("game_pk"),
            "score": rounded(row.get("rank_score")),
            "confidence": row.get("confidence_tier") or confidence_label(row.get("confidence")),
            "primary_reason": f"Best pitch angle: {pitch_label} with hitter xwOBA {metrics.get('xwOBA')} and EV {metrics.get('EV')}.",
            "reasoning": [
                f"Pitch angle: {pitch_label}",
                f"Pitcher usage: {metrics.get('Usage')}",
                f"Hitter xwOBA/xBA: {metrics.get('xwOBA')} / {metrics.get('xBA')}",
                f"Batted-ball quality: EV {metrics.get('EV')}, LA {metrics.get('LA')}, HardHit {metrics.get('HardHit')}",
                f"Pitcher allowed profile: xwOBA {metrics.get('Pitcher xwOBA')}",
            ],
            "metrics": metrics,
            "chart_data": build_chart_data(metrics),
            "source": "batter_pitch_type_matchups + model_projection_pitch_arsenal",
            "missing_data": row.get("missing_inputs") or [],
            "best_pitch_angles": [
                {
                    "pitch_type": row.get("pitch_type"),
                    "pitch_name": row.get("pitch_name"),
                    "score": rounded(row.get("rank_score")),
                    "reason": f"{pitch_label}: xwOBA {metrics.get('xwOBA')}, EV {metrics.get('EV')}, usage {metrics.get('Usage')}",
                }
            ],
        }
        candidates.append(candidate)
    items = dedupe_ranked_items(candidates, lambda row: f"hitter:{row.get('entity_id')}:{date}", limit=10)
    return build_response(date, "hitters", items, context.get("data_quality"), context.get("missing_data"))


def solve_top_pitchers(session, date: str) -> Dict[str, Any]:
    context = build_pitcher_lean_context(session, date)
    candidates: List[Dict[str, Any]] = []
    for row in context.get("pitcher_leans") or []:
        pitcher_id = row.get("pitcher_id")
        if not pitcher_id:
            continue
        profile = row.get("pitcher_profile") or {}
        opp = row.get("opponent_offense") or {}
        metrics = {
            "K%": rounded(profile.get("k_pct")),
            "BB%": rounded(profile.get("bb_pct")),
            "xwOBA Allowed": rounded(profile.get("xwoba_allowed")),
            "HardHit Allowed": rounded(profile.get("hard_hit_allowed")),
            "Opp K%": rounded(opp.get("k_pct")),
            "Opp ISO": rounded(opp.get("iso")),
            "Score": rounded(row.get("score")),
        }
        candidate = {
            "entity_type": "pitcher",
            "entity_id": str(pitcher_id),
            "entity_name": row.get("pitcher_name") or f"Pitcher {pitcher_id}",
            "team": row.get("team"),
            "opponent": row.get("opponent"),
            "game_pk": row.get("game_pk"),
            "score": rounded(row.get("score")),
            "confidence": row.get("confidence_tier") or confidence_label(row.get("confidence")),
            "category": row.get("category"),
            "primary_reason": ", ".join(row.get("reasons") or []) or "Model projection pitcher context created this lean.",
            "reasoning": row.get("reasons") or [],
            "metrics": metrics,
            "chart_data": build_chart_data(metrics),
            "source": "model_projections + ai_data_assistant_pitcher_lean_context",
            "missing_data": row.get("missing_inputs") or [],
        }
        candidates.append(candidate)
    items = dedupe_ranked_items(candidates, lambda row: f"pitcher:{row.get('entity_id')}:{date}", limit=10)
    return build_response(date, "pitchers", items, context.get("data_quality"), context.get("missing_data"))


def solve_top_teams(session, date: str) -> Dict[str, Any]:
    payload = projection_payload(session, date)
    edges = score_projection_edges(payload) if payload else []
    candidates: List[Dict[str, Any]] = []
    for edge in edges:
        for side in ("away", "home"):
            signals = edge.get(f"{side}_signals") or {}
            team_id = signals.get("team_id")
            team_name = signals.get("team_name")
            if not team_id and not team_name:
                continue
            favorite_bonus = 0.2 if edge.get("model_favorite_side") == side else 0.0
            offense = signals.get("offense") or {}
            metrics = {
                "Edge Score": rounded((safe_float(edge.get("score")) or 0) + favorite_bonus),
                "Win Edge": rounded(edge.get("win_probability_edge")),
                "Run Diff": rounded(edge.get("expected_run_differential")),
                "ISO": rounded(offense.get("iso")),
                "OBP": rounded(offense.get("obp")),
                "SLG": rounded(offense.get("slg")),
            }
            score = (safe_float(edge.get("score")) or 0) + favorite_bonus + ((safe_float(offense.get("iso")) or 0) * 0.6)
            candidate = {
                "entity_type": "team",
                "entity_id": str(team_id or team_name),
                "entity_name": team_name or f"Team {team_id}",
                "team": team_name,
                "opponent": signals.get("opponent_team"),
                "game_pk": edge.get("game_pk"),
                "score": rounded(score),
                "confidence": edge.get("confidence_tier") or confidence_label(edge.get("confidence")),
                "primary_reason": f"Model edge context: win edge {metrics.get('Win Edge')}, run diff {metrics.get('Run Diff')}, ISO {metrics.get('ISO')}.",
                "reasoning": edge.get("why") or [],
                "metrics": metrics,
                "chart_data": build_chart_data(metrics),
                "source": "model_projections",
                "missing_data": edge.get("missing_inputs") or [],
            }
            candidates.append(candidate)
    items = dedupe_ranked_items(candidates, lambda row: f"team:{row.get('entity_id')}:{date}", limit=10)
    return build_response(date, "teams", items, {"projection_games": len(payload.get("games") or [])}, payload.get("errors") or [])


def solve_top_totals(session, date: str) -> Dict[str, Any]:
    payload = projection_payload(session, date)
    edges = score_projection_edges(payload) if payload else []
    candidates: List[Dict[str, Any]] = []
    for edge in edges:
        game_pk = edge.get("game_pk")
        total = edge.get("total_projection") or {}
        projected_total = safe_float(total.get("total_expected_runs"))
        run_index = safe_float(total.get("run_scoring_index"))
        raw_total = safe_float(total.get("raw_total_expected_runs"))
        if projected_total is None and run_index is None:
            continue
        total_gap = abs((projected_total or 8.5) - 8.5)
        raw_gap = abs(projected_total - raw_total) if projected_total is not None and raw_total is not None else 0
        score = total_gap + (abs((run_index or 1.0) - 1.0) * 2.5) - min(0.5, raw_gap * 0.1)
        angle = "over_watchlist" if projected_total is not None and projected_total >= 8.8 else "under_watchlist" if projected_total is not None and projected_total <= 7.6 else "data_limited"
        metrics = {
            "Projected Total": rounded(projected_total),
            "Raw Total": rounded(raw_total),
            "Run Index": rounded(run_index),
            "Score": rounded(score),
        }
        candidate = {
            "entity_type": "game_total",
            "entity_id": str(game_pk),
            "entity_name": edge.get("label"),
            "team": None,
            "opponent": None,
            "game_pk": game_pk,
            "score": rounded(score),
            "confidence": edge.get("confidence_tier") or confidence_label(edge.get("confidence")),
            "category": angle,
            "primary_reason": f"{angle}: projected total {metrics.get('Projected Total')} with run index {metrics.get('Run Index')}. No sportsbook total is assumed.",
            "reasoning": [
                f"Projected total: {metrics.get('Projected Total')}",
                f"Raw total: {metrics.get('Raw Total')}",
                f"Run-scoring index: {metrics.get('Run Index')}",
                f"Environment label: {total.get('environment_label') or 'not labeled'}",
                "This is a model total watchlist only, not a priced market edge.",
            ],
            "metrics": metrics,
            "chart_data": build_chart_data(metrics),
            "source": "model_projections.total_projection",
            "missing_data": edge.get("missing_inputs") or [],
        }
        candidates.append(candidate)
    items = dedupe_ranked_items(candidates, lambda row: f"game_total:{row.get('game_pk')}:{date}", limit=10)
    return build_response(date, "totals", items, {"projection_games": len(payload.get("games") or [])}, payload.get("errors") or [])


def solve_top_overall_players(session, date: str) -> Dict[str, Any]:
    hitters = solve_top_hitters(session, date).get("items") or []
    pitchers = solve_top_pitchers(session, date).get("items") or []
    candidates: List[Dict[str, Any]] = []
    for row in hitters + pitchers:
        entity_type = row.get("entity_type")
        entity_id = row.get("entity_id")
        if not entity_id:
            continue
        adjusted = dict(row)
        adjusted["entity_type"] = "player"
        adjusted["player_type"] = entity_type
        adjusted["score"] = rounded((safe_float(row.get("score")) or 0) + (0.08 if row.get("confidence") == "high" else 0))
        adjusted["source"] = f"overall_players:{row.get('source')}"
        candidates.append(adjusted)
    items = dedupe_ranked_items(candidates, lambda row: f"player:{row.get('entity_id')}:{date}", limit=10)
    return build_response(date, "overall_players", items, {"hitter_candidates": len(hitters), "pitcher_candidates": len(pitchers)}, [])


def build_response(date: str, component: str, items: List[Dict[str, Any]], data_quality: Any = None, missing_data: Any = None) -> Dict[str, Any]:
    return {
        "date": date,
        "component": component,
        "title": COMPONENT_TITLES.get(component, component),
        "items": items[:10],
        "data_quality": data_quality or {},
        "missing_data": missing_data or [],
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


def build_dashboard_solver_payload(session, date: Optional[str], component: str) -> Dict[str, Any]:
    target_date = (date or today())[:10]
    normalized = (component or "").strip().lower()
    if normalized not in SUPPORTED_COMPONENTS:
        return {
            "date": target_date,
            "component": normalized,
            "title": "Unsupported dashboard component",
            "items": [],
            "data_quality": {"supported_components": sorted(SUPPORTED_COMPONENTS)},
            "missing_data": [f"Unsupported component: {component}"],
            "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
    if normalized == "hitters":
        return solve_top_hitters(session, target_date)
    if normalized == "pitchers":
        return solve_top_pitchers(session, target_date)
    if normalized == "teams":
        return solve_top_teams(session, target_date)
    if normalized == "totals":
        return solve_top_totals(session, target_date)
    return solve_top_overall_players(session, target_date)
