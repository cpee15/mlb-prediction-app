from __future__ import annotations

from typing import Any, Dict, Optional

from .bet105_normalizer import normalize_board as _normalize_board
from .kibl_bet105_types import Bet105RawBoard


def normalize_board(board: Bet105RawBoard, live_only: Optional[bool] = None, game_pk: Optional[Any] = None, raw: bool = False) -> Dict[str, Any]:
    payload = _normalize_board(board, live_only=live_only, game_pk=game_pk, raw=raw)
    markets_meta = getattr(board, "markets_meta", None)
    if markets_meta is not None:
        payload["markets_meta"] = markets_meta

    for event in payload.get("events") or []:
        markets = event.get("markets") or []
        priced_game_lines = [
            market
            for market in markets
            if market.get("market_key") in {"h2h", "spreads", "totals"}
            and any(selection.get("price") is not None for selection in market.get("selections") or [])
        ]
        if len(priced_game_lines) == 1 and priced_game_lines[0].get("market_key") == "h2h":
            event.setdefault("coverage_notes", []).append("Only Moneyline returned by Bet105/KIBL for this fixture request.")
        elif priced_game_lines:
            event.setdefault("coverage_notes", []).append(
                "Game-line markets returned: " + ", ".join(market.get("market_name") or market.get("market_key") for market in priced_game_lines)
            )
    return payload
