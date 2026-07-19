"""Canonical regulation, extras, and walk-off orchestration."""

from __future__ import annotations

from dataclasses import replace
from typing import Callable, Tuple

from mlb_app.simulation.events import (
    GameState,
    PlayEvent,
)

from .contracts import (
    CanonicalGameConfig,
    CanonicalGameResult,
    CanonicalLineup,
    GameCompletionReason,
    HalfInningRecord,
)


PlateAppearanceResolver = Callable[
    [GameState, str, int],
    PlayEvent,
]


def simulate_canonical_game(
    *,
    away_lineup: CanonicalLineup,
    home_lineup: CanonicalLineup,
    resolve_plate_appearance: PlateAppearanceResolver,
    config: CanonicalGameConfig | None = None,
) -> CanonicalGameResult:
    """
    Orchestrate one canonical game using an injected PA resolver.

    The resolver receives:
      state, batter_id, global_event_sequence

    It must return one valid canonical PlayEvent.
    """

    rules = config or CanonicalGameConfig()

    if away_lineup.team_side != "away":
        raise ValueError(
            "away_lineup must use away team side"
        )
    if home_lineup.team_side != "home":
        raise ValueError(
            "home_lineup must use home team side"
        )
    if not callable(resolve_plate_appearance):
        raise TypeError(
            "resolve_plate_appearance must be callable"
        )

    halves = []
    away_index = 0
    home_index = 0
    away_score = 0
    home_score = 0
    plate_appearance_number = 0
    sequence = 0
    inning = 1

    final_reason = None
    warnings = []

    maximum_inning = (
        rules.regulation_innings
        + rules.max_extra_innings
    )

    while inning <= maximum_inning:
        top = _simulate_half(
            inning=inning,
            half="top",
            lineup=away_lineup,
            batting_order_index=away_index,
            away_score=away_score,
            home_score=home_score,
            plate_appearance_number=(
                plate_appearance_number
            ),
            sequence=sequence,
            config=rules,
            resolve_plate_appearance=(
                resolve_plate_appearance
            ),
        )
        halves.append(top.record)

        away_index = top.record.batting_order_end
        away_score = top.record.final_state.away_score
        home_score = top.record.final_state.home_score
        plate_appearance_number = (
            top.record.final_state
            .plate_appearance_number
        )
        sequence = top.next_sequence

        if (
            inning >= rules.regulation_innings
            and home_score > away_score
        ):
            final_reason = (
                GameCompletionReason
                .HOME_LEAD_AFTER_TOP
            )
            break

        bottom = _simulate_half(
            inning=inning,
            half="bottom",
            lineup=home_lineup,
            batting_order_index=home_index,
            away_score=away_score,
            home_score=home_score,
            plate_appearance_number=(
                plate_appearance_number
            ),
            sequence=sequence,
            config=rules,
            resolve_plate_appearance=(
                resolve_plate_appearance
            ),
            walk_off_eligible=(
                inning >= rules.regulation_innings
            ),
        )
        halves.append(bottom.record)

        home_index = bottom.record.batting_order_end
        away_score = bottom.record.final_state.away_score
        home_score = bottom.record.final_state.home_score
        plate_appearance_number = (
            bottom.record.final_state
            .plate_appearance_number
        )
        sequence = bottom.next_sequence

        if bottom.record.ended_by_walk_off:
            final_reason = (
                GameCompletionReason.WALK_OFF
            )
            break

        if (
            inning >= rules.regulation_innings
            and away_score != home_score
        ):
            final_reason = (
                GameCompletionReason.EXTRA_INNINGS
                if inning
                > rules.regulation_innings
                else GameCompletionReason.REGULATION
            )
            break

        inning += 1

    if final_reason is None:
        final_reason = (
            GameCompletionReason
            .EXTRA_INNINGS_CAP_TIE
        )
        warnings.append(
            "game_tied_at_extra_innings_cap"
        )

    final_state = halves[-1].final_state

    return CanonicalGameResult(
        config=rules,
        away_lineup=away_lineup,
        home_lineup=home_lineup,
        halves=tuple(halves),
        final_state=final_state,
        completion_reason=final_reason,
        warnings=tuple(warnings),
    )


class _HalfSimulation:
    def __init__(
        self,
        *,
        record: HalfInningRecord,
        next_sequence: int,
    ) -> None:
        self.record = record
        self.next_sequence = next_sequence


def _simulate_half(
    *,
    inning: int,
    half: str,
    lineup: CanonicalLineup,
    batting_order_index: int,
    away_score: int,
    home_score: int,
    plate_appearance_number: int,
    sequence: int,
    config: CanonicalGameConfig,
    resolve_plate_appearance: PlateAppearanceResolver,
    walk_off_eligible: bool = False,
) -> _HalfSimulation:
    bases = (None, None, None)
    automatic_runner_id = None

    if (
        inning > config.regulation_innings
        and config.automatic_runner_enabled
    ):
        automatic_runner_id = (
            lineup.automatic_runner(
                batting_order_index
            )
        )
        bases = (
            None,
            automatic_runner_id,
            None,
        )

    initial_state = GameState(
        inning=inning,
        half=half,
        outs=0,
        bases=bases,
        away_score=away_score,
        home_score=home_score,
        batting_order_index=batting_order_index,
        plate_appearance_number=(
            plate_appearance_number
        ),
    )

    state = initial_state
    events = []
    current_sequence = sequence
    ended_by_walk_off = False

    for _ in range(
        config.max_plate_appearances_per_half
    ):
        if state.outs >= 3:
            break

        batter_id = lineup.batter(
            state.batting_order_index
        )

        event = resolve_plate_appearance(
            state,
            batter_id,
            current_sequence,
        )

        _validate_resolved_event(
            event=event,
            expected_state=state,
            expected_batter_id=batter_id,
            expected_sequence=current_sequence,
        )

        events.append(event)
        state = event.state_after
        current_sequence += 1

        if (
            walk_off_eligible
            and half == "bottom"
            and state.home_score
            > state.away_score
        ):
            ended_by_walk_off = True
            break
    else:
        raise RuntimeError(
            "maximum plate appearances exceeded "
            f"in inning {inning} {half}"
        )

    if not ended_by_walk_off and state.outs != 3:
        raise RuntimeError(
            "half inning did not terminate with "
            "three outs"
        )

    record = HalfInningRecord(
        inning=inning,
        half=half,
        initial_state=initial_state,
        events=tuple(events),
        batting_order_start=batting_order_index,
        batting_order_end=(
            state.batting_order_index
        ),
        ended_by_walk_off=ended_by_walk_off,
        automatic_runner_id=automatic_runner_id,
    )

    return _HalfSimulation(
        record=record,
        next_sequence=current_sequence,
    )


def _validate_resolved_event(
    *,
    event: PlayEvent,
    expected_state: GameState,
    expected_batter_id: str,
    expected_sequence: int,
) -> None:
    if not isinstance(event, PlayEvent):
        raise TypeError(
            "plate appearance resolver must "
            "return PlayEvent"
        )
    if event.sequence != expected_sequence:
        raise ValueError(
            "resolver returned unexpected event sequence"
        )
    if event.batter_id != expected_batter_id:
        raise ValueError(
            "resolver returned unexpected batter"
        )
    if event.state_before != expected_state:
        raise ValueError(
            "resolver event state_before does not "
            "match current game state"
        )
