"""Reduce canonical play events into projected box scores."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import fields
from typing import Dict, Iterable, Tuple

from mlb_app.simulation.events import (
    PlayEvent,
    PlayLedger,
    SacrificeType,
)

from .contracts import (
    BatterBoxScore,
    PitcherBoxScore,
    ReducedBoxScore,
    TeamBoxScore,
)


HIT_EVENTS = {
    "single": "singles",
    "double": "doubles",
    "triple": "triples",
    "hr": "home_runs",
}

NON_AT_BAT_EVENTS = frozenset(
    {
        "bb",
        "hbp",
        "sacrifice_fly",
        "sacrifice_bunt",
    }
)


def _empty_batter(player_id: str, team_side: str) -> dict:
    return {
        field.name: field.default
        for field in fields(BatterBoxScore)
        if field.name not in {"player_id", "team_side"}
    } | {
        "player_id": player_id,
        "team_side": team_side,
    }


def _empty_pitcher(player_id: str, team_side: str) -> dict:
    return {
        field.name: field.default
        for field in fields(PitcherBoxScore)
        if field.name not in {"player_id", "team_side"}
    } | {
        "player_id": player_id,
        "team_side": team_side,
    }


def _batting_side(event: PlayEvent) -> str:
    return "away" if event.state_before.half == "top" else "home"


def _fielding_side(event: PlayEvent) -> str:
    return "home" if _batting_side(event) == "away" else "away"


def _baserunning_runner_id(
    event: PlayEvent,
) -> str:
    for movement in event.runner_movements:
        if (
            movement.is_out
            or movement.start_base
            != movement.end_base
        ):
            return movement.runner_id

    raise ValueError(
        "baserunning event requires a moving runner"
    )


def reduce_box_score(
    *,
    initial_state,
    events: Iterable[PlayEvent],
) -> ReducedBoxScore:
    ledger = PlayLedger.from_events(
        initial_state,
        tuple(events),
    )

    batter_rows: Dict[Tuple[str, str], dict] = {}
    pitcher_rows: Dict[Tuple[str, str], dict] = {}

    teams = {
        "away": {
            "team_side": "away",
            "runs": 0,
            "hits": 0,
            "errors": 0,
            "left_on_base": 0,
        },
        "home": {
            "team_side": "home",
            "runs": 0,
            "hits": 0,
            "errors": 0,
            "left_on_base": 0,
        },
    }

    pitcher_attribution_complete = True

    for event in ledger.events:
        batting_side = _batting_side(event)
        fielding_side = _fielding_side(event)
        outcome = event.event_type.strip().lower()

        hit_field = None

        if event.is_plate_appearance:
            batter_key = (
                event.batter_id,
                batting_side,
            )
            batter = batter_rows.setdefault(
                batter_key,
                _empty_batter(
                    event.batter_id,
                    batting_side,
                ),
            )

            batter["plate_appearances"] += 1

            if outcome not in NON_AT_BAT_EVENTS:
                batter["at_bats"] += 1

            hit_field = HIT_EVENTS.get(outcome)
            if hit_field is not None:
                batter[hit_field] += 1
                teams[batting_side]["hits"] += 1

            if outcome == "bb":
                batter["walks"] += 1
            elif outcome == "hbp":
                batter["hit_by_pitch"] += 1
            elif outcome in {"k", "strikeout"}:
                batter["strikeouts"] += 1
            elif outcome == "reached_on_error":
                batter["reached_on_error"] += 1

            if (
                event.attribution.sacrifice_type
                is SacrificeType.FLY
            ):
                batter["sacrifice_flies"] += 1
            elif (
                event.attribution.sacrifice_type
                is SacrificeType.BUNT
            ):
                batter["sacrifice_bunts"] += 1
        elif outcome in {
            "stolen_base",
            "caught_stealing",
        }:
            runner_id = _baserunning_runner_id(
                event
            )
            runner_key = (
                runner_id,
                batting_side,
            )
            runner = batter_rows.setdefault(
                runner_key,
                _empty_batter(
                    runner_id,
                    batting_side,
                ),
            )

            if outcome == "stolen_base":
                runner["stolen_bases"] += 1
            else:
                runner["caught_stealing"] += 1

        if event.attribution.rbi_count:
            rbi_player_id = (
                event.attribution.rbi_credited_to
            )
            rbi_key = (rbi_player_id, batting_side)
            rbi_row = batter_rows.setdefault(
                rbi_key,
                _empty_batter(
                    rbi_player_id,
                    batting_side,
                ),
            )
            rbi_row["rbi"] += event.attribution.rbi_count

        for scorer_id in event.runs_scored:
            scorer_key = (scorer_id, batting_side)
            scorer = batter_rows.setdefault(
                scorer_key,
                _empty_batter(
                    scorer_id,
                    batting_side,
                ),
            )
            scorer["runs"] += 1
            teams[batting_side]["runs"] += 1

        if event.attribution.error_fielder_id:
            teams[fielding_side]["errors"] += 1

        if event.pitcher_id is None:
            pitcher_attribution_complete = False
        else:
            pitcher_key = (
                event.pitcher_id,
                fielding_side,
            )
            pitcher = pitcher_rows.setdefault(
                pitcher_key,
                _empty_pitcher(
                    event.pitcher_id,
                    fielding_side,
                ),
            )

            if event.is_plate_appearance:
                pitcher["batters_faced"] += 1

            pitcher["outs_recorded"] += len(
                event.outs_recorded
            )
            pitcher["runs_allowed"] += len(
                event.runs_scored
            )

            if hit_field is not None:
                pitcher["hits_allowed"] += 1
            if outcome == "hr":
                pitcher["home_runs_allowed"] += 1
            elif outcome == "bb":
                pitcher["walks"] += 1
            elif outcome == "hbp":
                pitcher["hit_batters"] += 1
            elif outcome in {"k", "strikeout"}:
                pitcher["strikeouts"] += 1

        if event.state_after.outs == 3:
            teams[batting_side]["left_on_base"] += sum(
                runner is not None
                for runner in event.state_after.bases
            )

    if ledger.events and ledger.current_state.outs < 3:
        final_side = (
            "away"
            if ledger.current_state.half == "top"
            else "home"
        )
        teams[final_side]["left_on_base"] += sum(
            runner is not None
            for runner in ledger.current_state.bases
        )

    batters = tuple(
        BatterBoxScore(**row)
        for _, row in sorted(batter_rows.items())
    )
    pitchers = tuple(
        PitcherBoxScore(**row)
        for _, row in sorted(pitcher_rows.items())
    )

    return ReducedBoxScore(
        away=TeamBoxScore(**teams["away"]),
        home=TeamBoxScore(**teams["home"]),
        batters=batters,
        pitchers=pitchers,
        pitcher_attribution_complete=(
            pitcher_attribution_complete
        ),
    )
