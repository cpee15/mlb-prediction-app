from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as legacy
from .bet105_full_market_normalizer import normalize_board
from .bet105_full_market_repository import KiblBet105FullMarketRepository


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
    try:
        board = KiblBet105FullMarketRepository().fetch_board(date=date, live_only=live_only, event_id=str(game_pk) if game_pk else None)
        return normalize_board(board, live_only=live_only, game_pk=game_pk, raw=raw)
    except Exception as exc:
        return legacy._provider_error(scope, game_pk=game_pk, exc=exc, request_params={"date": date, "live_only": live_only})
