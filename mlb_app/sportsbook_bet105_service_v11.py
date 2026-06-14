from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as legacy
from .bet105_normalizer import normalize_board
from .kibl_bet105_repository import KiblBet105Repository


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
    cache_key = f"bet105-v11:{scope}:{date or 'any'}:{game_pk or 'all'}:{raw}:{live_only}"
    cached = legacy._cache_get(cache_key)
    if cached:
        return cached
    try:
        board = KiblBet105Repository().fetch_board(date=date, live_only=live_only, event_id=str(game_pk) if game_pk else None)
        payload = normalize_board(board, live_only=live_only, game_pk=game_pk, raw=raw)
        legacy._cache_set(cache_key, payload)
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
