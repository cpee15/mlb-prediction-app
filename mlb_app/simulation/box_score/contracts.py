"""Immutable projected box-score contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple


VALID_TEAM_SIDES = frozenset({"away", "home"})


def _validate_nonnegative(instance) -> None:
    for name, value in vars(instance).items():
        if isinstance(value, int) and value < 0:
            raise ValueError(f"{name} cannot be negative")


@dataclass(frozen=True)
class BatterBoxScore:
    player_id: str
    team_side: str
    plate_appearances: int = 0
    at_bats: int = 0
    singles: int = 0
    doubles: int = 0
    triples: int = 0
    home_runs: int = 0
    walks: int = 0
    hit_by_pitch: int = 0
    strikeouts: int = 0
    runs: int = 0
    rbi: int = 0
    reached_on_error: int = 0
    sacrifice_flies: int = 0
    sacrifice_bunts: int = 0
    stolen_bases: int = 0
    caught_stealing: int = 0

    def __post_init__(self) -> None:
        if not self.player_id:
            raise ValueError("player_id is required")
        if self.team_side not in VALID_TEAM_SIDES:
            raise ValueError("team_side must be 'away' or 'home'")
        _validate_nonnegative(self)

    @property
    def hits(self) -> int:
        return (
            self.singles
            + self.doubles
            + self.triples
            + self.home_runs
        )


@dataclass(frozen=True)
class PitcherBoxScore:
    player_id: str
    team_side: str
    batters_faced: int = 0
    outs_recorded: int = 0
    hits_allowed: int = 0
    home_runs_allowed: int = 0
    walks: int = 0
    hit_batters: int = 0
    strikeouts: int = 0
    runs_allowed: int = 0
    earned_runs: Optional[int] = None
    earned_run_status: str = "not_reconstructed"

    def __post_init__(self) -> None:
        if not self.player_id:
            raise ValueError("player_id is required")
        if self.team_side not in VALID_TEAM_SIDES:
            raise ValueError("team_side must be 'away' or 'home'")
        if self.earned_run_status not in {
            "not_reconstructed",
            "reconstructed",
        }:
            raise ValueError("invalid earned_run_status")
        if (
            self.earned_run_status == "not_reconstructed"
            and self.earned_runs is not None
        ):
            raise ValueError(
                "earned_runs must be None when not reconstructed"
            )
        _validate_nonnegative(self)

    @property
    def innings_pitched(self) -> str:
        return (
            f"{self.outs_recorded // 3}."
            f"{self.outs_recorded % 3}"
        )


@dataclass(frozen=True)
class TeamBoxScore:
    team_side: str
    runs: int = 0
    hits: int = 0
    errors: int = 0
    left_on_base: int = 0

    def __post_init__(self) -> None:
        if self.team_side not in VALID_TEAM_SIDES:
            raise ValueError("team_side must be 'away' or 'home'")
        _validate_nonnegative(self)


@dataclass(frozen=True)
class ReducedBoxScore:
    away: TeamBoxScore
    home: TeamBoxScore
    batters: Tuple[BatterBoxScore, ...] = field(
        default_factory=tuple
    )
    pitchers: Tuple[PitcherBoxScore, ...] = field(
        default_factory=tuple
    )
    pitcher_attribution_complete: bool = False

    def batter(self, player_id: str) -> BatterBoxScore:
        for line in self.batters:
            if line.player_id == player_id:
                return line
        raise KeyError(player_id)

    def pitcher(self, player_id: str) -> PitcherBoxScore:
        for line in self.pitchers:
            if line.player_id == player_id:
                return line
        raise KeyError(player_id)
