"""Merge canonical box scores reduced from event segments."""

from __future__ import annotations

from dataclasses import fields
from typing import Dict, Iterable, Tuple

from .contracts import (
    BatterBoxScore,
    PitcherBoxScore,
    ReducedBoxScore,
    TeamBoxScore,
)


def merge_reduced_box_scores(
    box_scores: Iterable[ReducedBoxScore],
) -> ReducedBoxScore:
    """Merge independently validated box-score segments."""

    segments = tuple(box_scores)

    if not segments:
        raise ValueError(
            "at least one reduced box score is required"
        )

    away = _merge_team_lines(
        tuple(segment.away for segment in segments),
        team_side="away",
    )
    home = _merge_team_lines(
        tuple(segment.home for segment in segments),
        team_side="home",
    )

    batters = _merge_batter_lines(
        tuple(
            line
            for segment in segments
            for line in segment.batters
        )
    )
    pitchers = _merge_pitcher_lines(
        tuple(
            line
            for segment in segments
            for line in segment.pitchers
        )
    )

    return ReducedBoxScore(
        away=away,
        home=home,
        batters=batters,
        pitchers=pitchers,
        pitcher_attribution_complete=all(
            segment.pitcher_attribution_complete
            for segment in segments
        ),
    )


def _merge_team_lines(
    lines: Tuple[TeamBoxScore, ...],
    *,
    team_side: str,
) -> TeamBoxScore:
    return TeamBoxScore(
        team_side=team_side,
        runs=sum(line.runs for line in lines),
        hits=sum(line.hits for line in lines),
        errors=sum(line.errors for line in lines),
        left_on_base=sum(
            line.left_on_base for line in lines
        ),
    )


def _merge_batter_lines(
    lines: Tuple[BatterBoxScore, ...],
) -> Tuple[BatterBoxScore, ...]:
    rows: Dict[Tuple[str, str], dict] = {}

    numeric_fields = tuple(
        field.name
        for field in fields(BatterBoxScore)
        if field.name not in {
            "player_id",
            "team_side",
        }
    )

    for line in lines:
        key = (line.team_side, line.player_id)
        row = rows.setdefault(
            key,
            {
                "player_id": line.player_id,
                "team_side": line.team_side,
                **{
                    name: 0
                    for name in numeric_fields
                },
            },
        )

        for name in numeric_fields:
            row[name] += getattr(line, name)

    return tuple(
        BatterBoxScore(**rows[key])
        for key in sorted(rows)
    )


def _merge_pitcher_lines(
    lines: Tuple[PitcherBoxScore, ...],
) -> Tuple[PitcherBoxScore, ...]:
    rows: Dict[Tuple[str, str], dict] = {}

    numeric_fields = (
        "batters_faced",
        "outs_recorded",
        "hits_allowed",
        "home_runs_allowed",
        "walks",
        "hit_batters",
        "strikeouts",
        "runs_allowed",
    )

    for line in lines:
        key = (line.team_side, line.player_id)
        row = rows.setdefault(
            key,
            {
                "player_id": line.player_id,
                "team_side": line.team_side,
                **{
                    name: 0
                    for name in numeric_fields
                },
                "earned_runs": 0,
                "earned_run_status": "reconstructed",
            },
        )

        for name in numeric_fields:
            row[name] += getattr(line, name)

        if (
            line.earned_run_status
            != "reconstructed"
            or line.earned_runs is None
        ):
            row["earned_runs"] = None
            row["earned_run_status"] = (
                "not_reconstructed"
            )
        elif (
            row["earned_run_status"]
            == "reconstructed"
        ):
            row["earned_runs"] += line.earned_runs

    return tuple(
        PitcherBoxScore(**rows[key])
        for key in sorted(rows)
    )
