from __future__ import annotations

import copy
import time
from contextvars import ContextVar
from typing import Any, Dict, List, Optional, Tuple

from . import ai_data_assistant as core
from .model_projections import build_model_projection_payload as uncached_build_model_projection_payload
from .shared_payload_cache import env_ttl, make_cache_key

AI_RESPONSE_TTL_SECONDS = 180

_projection_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_response_cache: Dict[Tuple[Any, ...], Tuple[float, Dict[str, Any]]] = {}
_timing_var: ContextVar[Optional[Dict[str, float]]] = ContextVar("ai_data_assistant_timing", default=None)
_original_projection_builder = uncached_build_model_projection_payload
_patch_applied = False


def _now() -> float:
    return time.perf_counter()


def _ms(start: float) -> int:
    return int(round((_now() - start) * 1000))


def _cache_get(cache: Dict[Any, Tuple[float, Any]], key: Any, ttl_seconds: int) -> Optional[Any]:
    record = cache.get(key)
    if not record:
        return None
    created_at, value = record
    if ttl_seconds <= 0 or _now() - created_at > ttl_seconds:
        cache.pop(key, None)
        return None
    return copy.deepcopy(value)


def _cache_set(cache: Dict[Any, Tuple[float, Any]], key: Any, value: Any) -> None:
    cache[key] = (_now(), copy.deepcopy(value))


def _timing() -> Optional[Dict[str, float]]:
    return _timing_var.get()


def _add_timing(name: str, elapsed_ms: int) -> None:
    timing = _timing()
    if timing is None:
        return
    timing[name] = timing.get(name, 0) + elapsed_ms


def cached_build_model_projection_payload(session, target_date: str) -> Dict[str, Any]:
    """Process-local TTL cache for the expensive model projection payload.

    This monkey-patches the imported symbol inside `mlb_app.ai_data_assistant`, so
    all existing v2 builders keep their current code path while sharing one
    cached projection payload by date.
    """
    cache_key = make_cache_key("model_projection", "full", target_date)
    cached = _cache_get(_projection_cache, cache_key, env_ttl("MODEL_PROJECTION_CACHE_TTL_SECONDS"))
    if cached is not None:
        timing = _timing()
        if timing is not None:
            timing["projection_cache_hit"] = timing.get("projection_cache_hit", 0) + 1
        return cached

    start = _now()
    payload = _original_projection_builder(session, target_date)
    _add_timing("projection_payload_ms", _ms(start))
    _cache_set(_projection_cache, cache_key, payload)
    timing = _timing()
    if timing is not None:
        timing["projection_cache_miss"] = timing.get("projection_cache_miss", 0) + 1
    return copy.deepcopy(payload)


def apply_performance_patch() -> None:
    """Patch the existing v1/v2 service path once per process.

    No new route, page, or duplicate assistant service is created. The existing
    `ai_data_assistant.py` functions still do the product logic.
    """
    global _patch_applied
    if _patch_applied:
        return
    core.build_model_projection_payload = cached_build_model_projection_payload
    _patch_applied = True


def _response_cache_key(message: str, date: Optional[str], game_pk: Optional[int], player_id: Optional[int], team_id: Optional[int], use_llm: bool) -> Tuple[Any, ...]:
    normalized_message = " ".join((message or "").strip().lower().split())
    return (normalized_message, date, game_pk, player_id, team_id, bool(use_llm))


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct(value: Any) -> Optional[str]:
    number = _safe_float(value)
    if number is None:
        return None
    return f"{round(number * 100, 1)}%"


def _canonical_games_from_projection_payload(payload: Dict[str, Any], game_pk: Optional[int] = None, limit: int = 6) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for game in payload.get("games") or []:
        if game_pk is not None and str(game.get("game_pk")) != str(game_pk):
            continue
        probs = game.get("main_matchup_probabilities") or {}
        home_team = (game.get("home_team") or {}).get("name") if isinstance(game.get("home_team"), dict) else game.get("home_team")
        away_team = (game.get("away_team") or {}).get("name") if isinstance(game.get("away_team"), dict) else game.get("away_team")
        home_prob = _safe_float(probs.get("home_win_prob") or game.get("home_win_prob"))
        away_prob = _safe_float(probs.get("away_win_prob") or game.get("away_win_prob"))
        favorite = None
        favorite_probability = None
        if home_prob is not None and away_prob is not None:
            favorite = home_team if home_prob >= away_prob else away_team
            favorite_probability = max(home_prob, away_prob)
        simulation_diag = probs.get("simulation_diagnostic") or (game.get("workspace") or {}).get("sharedSimulationDiagnostics") or {}
        rows.append({
            "game_pk": game.get("game_pk"),
            "label": f"{away_team or 'Away'} @ {home_team or 'Home'}",
            "favorite": favorite,
            "favorite_probability": favorite_probability,
            "home_win_prob": home_prob,
            "away_win_prob": away_prob,
            "model_version": probs.get("model_version") or game.get("model_version"),
            "lineup_status": probs.get("lineup_status") or game.get("lineup_status"),
            "data_confidence": probs.get("data_confidence") or game.get("data_confidence"),
            "missing_inputs": probs.get("missing_inputs") or game.get("missing_inputs") or [],
            "probability_component_keys": sorted((probs.get("probability_components") or game.get("probability_components") or {}).keys()),
            "simulation_role": simulation_diag.get("status") or "diagnostic_only_not_final_probability",
            "simulation_is_final_probability": False,
        })
        if len(rows) >= limit and game_pk is None:
            break
    return rows


def _build_canonical_probability_context(session, context: Dict[str, Any], game_pk: Optional[int] = None) -> Dict[str, Any]:
    date = context.get("date") or core.today()
    start = _now()
    try:
        payload = cached_build_model_projection_payload(session, date)
        _add_timing("canonical_probability_context_ms", _ms(start))
    except Exception as exc:
        return {
            "available": False,
            "date": date,
            "error": str(exc),
            "final_probability_source": "matchups.home_win_prob_and_away_win_prob",
            "simulation_role": "diagnostic_only_not_final_probability",
        }
    games = _canonical_games_from_projection_payload(payload, game_pk=game_pk or context.get("game_pk"))
    return {
        "available": bool(games),
        "date": date,
        "final_probability_source": "matchups.home_win_prob_and_away_win_prob",
        "model_version": "canonical_matchup_win_probability_v2",
        "legacy_model_version": "legacy_matchup_win_probability_v1",
        "simulation_role": "diagnostic_only_not_final_probability",
        "games": games,
        "source_note": "AI Data Assistant treats home_win_prob and away_win_prob as canonical v2. Simulation output is run-distribution and diagnostic context only.",
    }


def _canonical_answer_prefix(canonical_context: Dict[str, Any]) -> str:
    if not canonical_context.get("available"):
        return (
            "Canonical probability note\n"
            "The assistant attempted to load canonical matchup probability v2 context, but it was not available for this query. "
            "Do not treat simulation output as the final side probability.\n"
        )
    games = canonical_context.get("games") or []
    best = games[0] if games else {}
    favorite = best.get("favorite") or "no clear side"
    favorite_prob = _pct(best.get("favorite_probability")) or "missing"
    confidence = best.get("data_confidence") or "unknown"
    lineup = best.get("lineup_status") or "unknown"
    components = ", ".join(best.get("probability_component_keys") or []) or "component diagnostics missing"
    return (
        "Canonical probability note\n"
        f"Final side probability comes from canonical v2 `home_win_prob` and `away_win_prob`, not the simulation diagnostic. "
        f"Top loaded game: {best.get('label') or 'unknown game'}; side lean {favorite} at {favorite_prob}; confidence {confidence}; lineup status {lineup}. "
        f"Components available: {components}.\n"
    )


def _enrich_result_with_canonical_context(result: Dict[str, Any], canonical_context: Dict[str, Any]) -> Dict[str, Any]:
    enriched = copy.deepcopy(result)
    enriched["canonical_probability_context"] = canonical_context
    sources = list(enriched.get("sources_used") or [])
    for source in ["canonical_matchup_probability_v2", "matchups.home_win_prob_and_away_win_prob"]:
        if source not in sources:
            sources.append(source)
    enriched["sources_used"] = sources
    enriched.setdefault("data_quality", {})
    if isinstance(enriched["data_quality"], dict):
        enriched["data_quality"]["canonical_probability"] = {
            "available": canonical_context.get("available"),
            "final_probability_source": canonical_context.get("final_probability_source"),
            "simulation_role": canonical_context.get("simulation_role"),
            "games_loaded": len(canonical_context.get("games") or []),
        }
    note = _canonical_answer_prefix(canonical_context)
    answer = enriched.get("answer") or ""
    if "Canonical probability note" not in answer:
        enriched["answer"] = note + "\n" + answer
    confidence_note = enriched.get("confidence_note") or ""
    canonical_note = " Canonical v2 is the final side probability; simulations are diagnostic/run-distribution context only."
    if canonical_note.strip() not in confidence_note:
        enriched["confidence_note"] = confidence_note + canonical_note
    return enriched


def build_ai_data_assistant_response(
    session,
    message: str,
    date: Optional[str] = None,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
    use_llm: bool = False,
) -> Dict[str, Any]:
    """Fast wrapper for the existing AI Data Assistant service.

    Adds:
    - process-level TTL response cache for repeated chip clicks
    - process-level TTL Model Projection cache by date
    - deterministic answers by default
    - canonical probability context on every response
    - timing metadata on every response
    """
    apply_performance_patch()
    total_start = _now()
    timing: Dict[str, float] = {}
    token = _timing_var.set(timing)
    key = _response_cache_key(message, date, game_pk, player_id, team_id, use_llm)
    try:
        cached = _cache_get(_response_cache, key, AI_RESPONSE_TTL_SECONDS)
        if cached is not None:
            cached.setdefault("timing", {})
            cached["timing"].update({"response_cache_hit": 1, "total_ms": _ms(total_start)})
            cached["cache_status"] = "response_cache_hit"
            return cached

        context_start = _now()
        context = core.build_assistant_context(
            session=session,
            message=message,
            date=date,
            game_pk=game_pk,
            player_id=player_id,
            team_id=team_id,
        )
        timing["context_build_ms"] = _ms(context_start)

        canonical_context = _build_canonical_probability_context(session, context, game_pk=game_pk)

        answer_start = _now()
        result = core.answer_with_optional_llm(message, context, use_llm=use_llm)
        timing["answer_render_ms"] = _ms(answer_start)
        timing["llm_requested"] = 1 if use_llm else 0
        timing["total_ms"] = _ms(total_start)

        result = _enrich_result_with_canonical_context(result, canonical_context)
        result["timing"] = dict(timing)
        result["cache_status"] = "miss"
        _cache_set(_response_cache, key, result)
        return result
    finally:
        _timing_var.reset(token)


def clear_ai_data_assistant_caches() -> Dict[str, Any]:
    projection_count = len(_projection_cache)
    response_count = len(_response_cache)
    _projection_cache.clear()
    _response_cache.clear()
    return {"cleared": True, "projection_cache_entries": projection_count, "response_cache_entries": response_count}
