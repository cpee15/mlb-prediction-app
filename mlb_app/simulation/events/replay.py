"""Replay validation for canonical play events."""

from __future__ import annotations

from typing import Iterable

from .contracts import GameState, PlayEvent, PlayLedger


def replay_events(
    initial_state: GameState,
    events: Iterable[PlayEvent],
) -> GameState:
    """Replay a sequence and return its final canonical state.

    Replay intentionally consumes the state transitions recorded in each
    event. The ledger validates event ordering and state continuity.
    """

    ledger = PlayLedger.from_events(initial_state, events)
    return ledger.current_state
