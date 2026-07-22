"""Immutable canonical pitcher lifecycle and decision contracts."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Optional

from mlb_app.simulation.events import PlayEvent


CANONICAL_PITCHER_LIFECYCLE_VERSION = (
    "canonical_pitcher_lifecycle_v1"
)


class CanonicalPitcherRole(str, Enum):
    """Pitcher role for one simulated appearance."""

    STARTER = "starter"
    RELIEVER = "reliever"


class CanonicalPitchingDecisionAction(str, Enum):
    """Managerial action before the next plate appearance."""

    HOLD = "hold"
    REPLACE = "replace"


@dataclass(frozen=True)
class CanonicalPitcherLifecycleState:
    """
    Immutable statistics for one pitcher appearance.

    ``runs_scored_during_stint`` is observational only. It does not
    assign pitcher responsibility or earned runs. Those are handled
    by the later responsibility-reconstruction layer.
    """

    team_side: str
    pitcher_id: str
    role: CanonicalPitcherRole
    entered_inning: int
    entered_half: str
    batters_faced: int = 0
    outs_recorded: int = 0
    hits_allowed: int = 0
    walks_allowed: int = 0
    hit_batters: int = 0
    home_runs_allowed: int = 0
    strikeouts: int = 0
    runs_scored_during_stint: int = 0
    active: bool = True
    schema_version: str = (
        CANONICAL_PITCHER_LIFECYCLE_VERSION
    )

    def __post_init__(self) -> None:
        if self.team_side not in {"away", "home"}:
            raise ValueError(
                "team_side must be 'away' or 'home'"
            )

        if not self.pitcher_id:
            raise ValueError("pitcher_id is required")

        if not isinstance(
            self.role,
            CanonicalPitcherRole,
        ):
            raise TypeError(
                "role must be a CanonicalPitcherRole"
            )

        if self.entered_inning < 1:
            raise ValueError(
                "entered_inning must be positive"
            )

        if self.entered_half not in {"top", "bottom"}:
            raise ValueError(
                "entered_half must be 'top' or 'bottom'"
            )

        counters = (
            self.batters_faced,
            self.outs_recorded,
            self.hits_allowed,
            self.walks_allowed,
            self.hit_batters,
            self.home_runs_allowed,
            self.strikeouts,
            self.runs_scored_during_stint,
        )

        if any(value < 0 for value in counters):
            raise ValueError(
                "pitcher lifecycle counters cannot be negative"
            )

        if self.outs_recorded > (
            self.batters_faced * 3
        ):
            raise ValueError(
                "outs_recorded is inconsistent with "
                "batters_faced"
            )

        if self.schema_version != (
            CANONICAL_PITCHER_LIFECYCLE_VERSION
        ):
            raise ValueError(
                "unsupported pitcher lifecycle schema"
            )

    @property
    def innings_recorded(self) -> float:
        """Return conventional innings pitched representation."""

        whole, remainder = divmod(
            self.outs_recorded,
            3,
        )

        return float(
            f"{whole}.{remainder}"
        )

    @property
    def lineup_turns_faced(self) -> int:
        """Number of complete nine-batter lineup turns faced."""

        return self.batters_faced // 9

    @property
    def current_lineup_pass(self) -> int:
        """One-indexed pass through the batting order."""

        return (self.batters_faced // 9) + 1


@dataclass(frozen=True)
class CanonicalPitchingDecisionContext:
    """State supplied to a pitcher hook policy."""

    lifecycle: CanonicalPitcherLifecycleState
    inning: int
    half: str
    outs: int
    batting_team_score: int
    fielding_team_score: int
    runners_on_base: int
    upcoming_batter_id: str
    available_reliever_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(
            self.lifecycle,
            CanonicalPitcherLifecycleState,
        ):
            raise TypeError(
                "lifecycle must be a "
                "CanonicalPitcherLifecycleState"
            )

        if self.inning < 1:
            raise ValueError("inning must be positive")

        if self.half not in {"top", "bottom"}:
            raise ValueError(
                "half must be 'top' or 'bottom'"
            )

        if not 0 <= self.outs <= 2:
            raise ValueError(
                "outs must be between zero and two"
            )

        if (
            self.batting_team_score < 0
            or self.fielding_team_score < 0
        ):
            raise ValueError(
                "scores cannot be negative"
            )

        if not 0 <= self.runners_on_base <= 3:
            raise ValueError(
                "runners_on_base must be between zero and three"
            )

        if not self.upcoming_batter_id:
            raise ValueError(
                "upcoming_batter_id is required"
            )

        if any(
            not pitcher_id
            for pitcher_id in self.available_reliever_ids
        ):
            raise ValueError(
                "available reliever identifiers are required"
            )

        if len(self.available_reliever_ids) != len(
            set(self.available_reliever_ids)
        ):
            raise ValueError(
                "available relievers must be unique"
            )


@dataclass(frozen=True)
class CanonicalPitchingDecision:
    """Deterministic pitcher decision before one plate appearance."""

    action: CanonicalPitchingDecisionAction
    current_pitcher_id: str
    replacement_pitcher_id: Optional[str] = None
    reason: str = "policy_hold"

    def __post_init__(self) -> None:
        if not isinstance(
            self.action,
            CanonicalPitchingDecisionAction,
        ):
            raise TypeError(
                "action must be a "
                "CanonicalPitchingDecisionAction"
            )

        if not self.current_pitcher_id:
            raise ValueError(
                "current_pitcher_id is required"
            )

        if not self.reason:
            raise ValueError("reason is required")

        if (
            self.action
            is CanonicalPitchingDecisionAction.HOLD
        ):
            if self.replacement_pitcher_id is not None:
                raise ValueError(
                    "hold decision cannot name a replacement"
                )
        else:
            if not self.replacement_pitcher_id:
                raise ValueError(
                    "replace decision requires a replacement"
                )

            if (
                self.replacement_pitcher_id
                == self.current_pitcher_id
            ):
                raise ValueError(
                    "replacement pitcher must differ "
                    "from current pitcher"
                )


def reduce_pitcher_lifecycle(
    lifecycle: CanonicalPitcherLifecycleState,
    event: PlayEvent,
) -> CanonicalPitcherLifecycleState:
    """Apply one completed plate appearance to pitcher lifecycle."""

    if not isinstance(
        lifecycle,
        CanonicalPitcherLifecycleState,
    ):
        raise TypeError(
            "lifecycle must be a "
            "CanonicalPitcherLifecycleState"
        )

    if not isinstance(event, PlayEvent):
        raise TypeError("event must be a PlayEvent")

    if not lifecycle.active:
        raise ValueError(
            "cannot apply an event to an inactive pitcher"
        )

    if event.pitcher_id != lifecycle.pitcher_id:
        raise ValueError(
            "event pitcher does not match lifecycle pitcher"
        )

    event_type = event.event_type

    hit = event_type in {
        "single",
        "double",
        "triple",
        "hr",
    }

    return replace(
        lifecycle,
        batters_faced=(
            lifecycle.batters_faced + 1
        ),
        outs_recorded=(
            lifecycle.outs_recorded
            + len(event.outs_recorded)
        ),
        hits_allowed=(
            lifecycle.hits_allowed
            + int(hit)
        ),
        walks_allowed=(
            lifecycle.walks_allowed
            + int(event_type == "bb")
        ),
        hit_batters=(
            lifecycle.hit_batters
            + int(event_type == "hbp")
        ),
        home_runs_allowed=(
            lifecycle.home_runs_allowed
            + int(event_type == "hr")
        ),
        strikeouts=(
            lifecycle.strikeouts
            + int(event_type == "k")
        ),
        runs_scored_during_stint=(
            lifecycle.runs_scored_during_stint
            + len(event.runs_scored)
        ),
    )


def retire_pitcher(
    lifecycle: CanonicalPitcherLifecycleState,
) -> CanonicalPitcherLifecycleState:
    """Close one appearance before a replacement enters."""

    if not isinstance(
        lifecycle,
        CanonicalPitcherLifecycleState,
    ):
        raise TypeError(
            "lifecycle must be a "
            "CanonicalPitcherLifecycleState"
        )

    if not lifecycle.active:
        raise ValueError(
            "pitcher lifecycle is already inactive"
        )

    return replace(
        lifecycle,
        active=False,
    )
