from __future__ import annotations

import copy
import time
from contextvars import ContextVar
from typing import Any, Dict, Optional, Tuple

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

        answer_start = _now()
        result = core.answer_with_optional_llm(message, context, use_llm=use_llm)
        timing["answer_render_ms"] = _ms(answer_start)
        timing["llm_requested"] = 1 if use_llm else 0
        timing["total_ms"] = _ms(total_start)

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
