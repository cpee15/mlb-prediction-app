from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as legacy
from .bet105_normalizer import normalize_board
from .kibl_bet105_repository import KiblBet105Repository


def _elapsed_ms(start: float) -> int:
    return int(round((time.perf_counter() - start) * 1000))


def _cache_ttl(live_only: Optional[bool], raw: bool) -> int:
    if raw:
        return int(os.getenv("KIBL_BET105_DEBUG_CACHE_TTL_SECONDS", "30"))
    if live_only:
        return int(os.getenv("KIBL_BET105_LIVE_CACHE_TTL_SECONDS", "15"))
    return int(os.getenv("KIBL_BET105_FAST_CACHE_TTL_SECONDS", "90"))


def _compact_normal_payload(payload: Dict[str, Any], raw: bool) -> Dict[str, Any]:
    if raw:
        return payload
    payload.pop("raw_items_sample", None)
    markets_meta = dict(payload.get("markets_meta") or {})
    markets_meta.pop("fixture_request_summaries", None)
    if markets_meta:
        payload["markets_meta"] = markets_meta
    else:
        payload.pop("markets_meta", None)
    notes = payload.get("normalization_notes") or []
    payload["normalization_notes"] = [note for note in notes if str(note).startswith(("fixture_scoped_market_requests", "market_selected", "performance:"))][:8]
    return payload


def _cache_hit_payload(cached: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(cached)
    perf = dict(payload.get("performance_meta") or {})
    perf["cache_hit"] = True
    payload["performance_meta"] = perf
    payload["cache_hit"] = True
    return payload


def fetch_board(
    date: Optional[str] = None,
    raw: bool = False,
    live_only: Optional[bool] = None,
    game_pk: Optional[Any] = None,
    props_only: bool = False,
    market_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    scope = "live" if live_only else "events"
    if not legacy._configured():
        return legacy._not_configured(scope, game_pk=game_pk)
    mode = "debug" if raw else "fast"
    cache_key = f"bet105-board-{mode}:{scope}:{date or 'any'}:{game_pk or 'all'}"
    cached = legacy._cache_get(cache_key)
    if cached:
        return _cache_hit_payload(cached)
    total_start = time.perf_counter()
    try:
        repo = KiblBet105Repository(discovery_probes=raw)
        board = repo.fetch_board(date=date, live_only=live_only, event_id=str(game_pk) if game_pk else None)
        normalize_start = time.perf_counter()
        payload = normalize_board(board, live_only=live_only, game_pk=game_pk, raw=raw)
        normalize_ms = _elapsed_ms(normalize_start)
        perf = dict(getattr(board, "performance_meta", {}) or {})
        perf.update(
            {
                "normalize_ms": normalize_ms,
                "total_service_ms": _elapsed_ms(total_start),
                "cache_hit": False,
                "mode": perf.get("mode") or ("discovery" if raw else "fast"),
                "events": payload.get("event_count"),
                "markets": payload.get("market_count"),
                "selections": sum(len(market.get("selections") or []) for event in payload.get("events") or [] for market in event.get("markets") or []),
                "raw_diagnostics_included": bool(raw),
            }
        )
        payload["performance_meta"] = perf
        payload = _compact_normal_payload(payload, raw=raw)
        if payload.get("status") not in {"provider_error", "provider_not_configured"}:
            legacy._cache_set(cache_key, payload, ttl=_cache_ttl(live_only, raw))
        return payload
    except Exception as exc:
        return legacy._provider_error(scope, game_pk=game_pk, exc=exc, request_params={"date": date, "live_only": live_only})


def fetch_event_board(
    event_id: str,
    props_only: bool = False,
    raw: bool = False,
    market_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    payload = fetch_board(game_pk=event_id, props_only=props_only, raw=raw, market_types=market_types)
    events = payload.get("events") or []
    payload["event"] = events[0] if events else None
    return payload
