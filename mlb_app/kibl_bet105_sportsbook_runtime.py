from __future__ import annotations

from typing import Any, Dict, List, Optional

from .sportsbook_bet105_service import fetch_board, fetch_event_board


def fetch_kibl_bet105_events(
    date: Optional[str] = None,
    raw: bool = False,
    live_only: Optional[bool] = None,
) -> Dict[str, Any]:
    return fetch_board(date=date, raw=raw, live_only=live_only)


def fetch_kibl_bet105_event_odds(
    event_id: str,
    props_only: bool = False,
    raw: bool = False,
    market_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return fetch_event_board(
        event_id=event_id,
        props_only=props_only,
        raw=raw,
        market_types=market_types,
    )
