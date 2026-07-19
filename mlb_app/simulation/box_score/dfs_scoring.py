"""Configurable DFS scoring from reduced box-score lines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .contracts import BatterBoxScore, PitcherBoxScore


@dataclass(frozen=True)
class BatterDfsScoringRules:
    single: float = 0.0
    double: float = 0.0
    triple: float = 0.0
    home_run: float = 0.0
    walk: float = 0.0
    hit_by_pitch: float = 0.0
    run: float = 0.0
    rbi: float = 0.0


@dataclass(frozen=True)
class PitcherDfsScoringRules:
    out_recorded: float = 0.0
    strikeout: float = 0.0
    hit_allowed: float = 0.0
    walk: float = 0.0
    hit_batter: float = 0.0
    run_allowed: float = 0.0
    earned_run: float = 0.0


def score_batter(
    line: BatterBoxScore,
    rules: BatterDfsScoringRules,
) -> float:
    return (
        line.singles * rules.single
        + line.doubles * rules.double
        + line.triples * rules.triple
        + line.home_runs * rules.home_run
        + line.walks * rules.walk
        + line.hit_by_pitch * rules.hit_by_pitch
        + line.runs * rules.run
        + line.rbi * rules.rbi
    )


def score_pitcher(
    line: PitcherBoxScore,
    rules: PitcherDfsScoringRules,
) -> float:
    if (
        rules.earned_run != 0.0
        and line.earned_runs is None
    ):
        raise ValueError(
            "earned-run scoring requires reconstructed "
            "earned runs"
        )

    earned_runs = line.earned_runs or 0

    return (
        line.outs_recorded * rules.out_recorded
        + line.strikeouts * rules.strikeout
        + line.hits_allowed * rules.hit_allowed
        + line.walks * rules.walk
        + line.hit_batters * rules.hit_batter
        + line.runs_allowed * rules.run_allowed
        + earned_runs * rules.earned_run
    )
