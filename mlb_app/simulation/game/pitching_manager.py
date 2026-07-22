"""Trial-owned canonical pitching lifecycle management."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple

from mlb_app.simulation.events import GameState, PlayEvent

from .bullpen_selector import (
    CanonicalBullpenPitcher,
    CanonicalBullpenSelectionContext,
    CanonicalBullpenSelector,
)
from .matchup_input import CanonicalMatchupInput
from .pitcher_hook_policy import CanonicalStarterHookPolicy
from .reliever_hook_policy import (
    CanonicalRelieverHookPolicy,
    build_baseline_reliever_hook_policy,
)
from .earned_run_reconstruction import (
    CanonicalEarnedRunReconstructor,
    CanonicalPitcherRunLine,
    CanonicalRunClassification,
)
from .pitcher_responsibility import (
    CanonicalPitcherResponsibilityLedger,
    CanonicalRunnerResponsibility,
    CanonicalScoredRunResponsibility,
)
from .pitcher_lifecycle import (
    CanonicalPitcherLifecycleState,
    CanonicalPitcherRole,
    CanonicalPitchingDecisionAction,
    CanonicalPitchingDecisionContext,
    reduce_pitcher_lifecycle,
    retire_pitcher,
)


CANONICAL_PITCHING_MANAGER_VERSION = (
    "canonical_pitching_manager_v1"
)


@dataclass
class CanonicalPitchingManager:
    """
    Mutable state owned by exactly one canonical trial resolver.

    The game state remains immutable. This manager owns only pitcher
    appearance state and deterministic substitution history.
    """

    matchup_input: CanonicalMatchupInput
    starter_hook_policy: CanonicalStarterHookPolicy
    bullpen_selector: CanonicalBullpenSelector
    away_bullpen: Tuple[CanonicalBullpenPitcher, ...]
    home_bullpen: Tuple[CanonicalBullpenPitcher, ...]
    reliever_hook_policy: CanonicalRelieverHookPolicy = (
        field(
            default_factory=(
                build_baseline_reliever_hook_policy
            )
        )
    )
    version: str = CANONICAL_PITCHING_MANAGER_VERSION
    _active: Dict[str, CanonicalPitcherLifecycleState] = field(
        init=False,
        repr=False,
    )
    _completed: Dict[
        str,
        list[CanonicalPitcherLifecycleState],
    ] = field(
        init=False,
        repr=False,
    )
    _used_pitcher_ids: Dict[str, list[str]] = field(
        init=False,
        repr=False,
    )
    _responsibility_ledger: (
        CanonicalPitcherResponsibilityLedger
    ) = field(
        init=False,
        repr=False,
    )
    _earned_run_reconstructor: (
        CanonicalEarnedRunReconstructor
    ) = field(
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.matchup_input,
            CanonicalMatchupInput,
        ):
            raise TypeError(
                "matchup_input must be a "
                "CanonicalMatchupInput"
            )

        if not isinstance(
            self.starter_hook_policy,
            CanonicalStarterHookPolicy,
        ):
            raise TypeError(
                "starter_hook_policy must be a "
                "CanonicalStarterHookPolicy"
            )

        if not isinstance(
            self.bullpen_selector,
            CanonicalBullpenSelector,
        ):
            raise TypeError(
                "bullpen_selector must be a "
                "CanonicalBullpenSelector"
            )

        if not isinstance(
            self.reliever_hook_policy,
            CanonicalRelieverHookPolicy,
        ):
            raise TypeError(
                "reliever_hook_policy must be a "
                "CanonicalRelieverHookPolicy"
            )

        if self.version != (
            CANONICAL_PITCHING_MANAGER_VERSION
        ):
            raise ValueError(
                "unsupported pitching manager version"
            )

        self._validate_bullpen(
            team_side="away",
            bullpen=self.away_bullpen,
        )
        self._validate_bullpen(
            team_side="home",
            bullpen=self.home_bullpen,
        )

        self._active = {
            "away": CanonicalPitcherLifecycleState(
                team_side="away",
                pitcher_id=(
                    self.matchup_input
                    .away_pitching_plan
                    .starter_id
                ),
                role=CanonicalPitcherRole.STARTER,
                entered_inning=1,
                entered_half="bottom",
            ),
            "home": CanonicalPitcherLifecycleState(
                team_side="home",
                pitcher_id=(
                    self.matchup_input
                    .home_pitching_plan
                    .starter_id
                ),
                role=CanonicalPitcherRole.STARTER,
                entered_inning=1,
                entered_half="top",
            ),
        }

        self._completed = {
            "away": [],
            "home": [],
        }

        self._used_pitcher_ids = {
            "away": [
                self._active["away"].pitcher_id,
            ],
            "home": [
                self._active["home"].pitcher_id,
            ],
        }

        self._responsibility_ledger = (
            CanonicalPitcherResponsibilityLedger()
        )

        self._earned_run_reconstructor = (
            CanonicalEarnedRunReconstructor()
        )

    def pitcher_for_plate_appearance(
        self,
        *,
        state: GameState,
        batter_id: str,
    ) -> str:
        """Return the active pitcher after any pre-PA decision."""

        team_side = self._fielding_team_side(state)
        lifecycle = self._active[team_side]

        decision_context = self._decision_context(
            state=state,
            batter_id=batter_id,
            lifecycle=lifecycle,
        )

        if lifecycle.role is (
            CanonicalPitcherRole.STARTER
        ):
            decision = self.starter_hook_policy.decide(
                decision_context
            )
        else:
            decision = self.reliever_hook_policy.decide(
                decision_context
            )

        if decision.action is (
            CanonicalPitchingDecisionAction.REPLACE
        ):
            lifecycle = self._replace_pitcher(
                team_side=team_side,
                state=state,
                decision_context=decision_context,
                decision=decision,
            )

        return lifecycle.pitcher_id

    def register_automatic_runner(
        self,
        *,
        state: GameState,
        runner_id: str,
    ) -> CanonicalRunnerResponsibility:
        team_side = self._fielding_team_side(state)
        pitcher_id = self._active[
            team_side
        ].pitcher_id

        responsibility = (
            self._responsibility_ledger
            .register_automatic_runner(
                runner_id=runner_id,
                responsible_pitcher_id=pitcher_id,
                inning=state.inning,
                half=state.half,
            )
        )

        try:
            self._earned_run_reconstructor.record_automatic_runner(
                responsibility
            )
        except ValueError as exc:
            if "already recorded" not in str(exc):
                raise

        return responsibility

    def record_plate_appearance(
        self,
        event: PlayEvent,
    ) -> CanonicalPitcherLifecycleState:
        """Reduce one completed PA into the active lifecycle."""

        team_side = self._fielding_team_side(
            event.state_before
        )
        lifecycle = self._active[team_side]

        updated = reduce_pitcher_lifecycle(
            lifecycle,
            event,
        )

        before = {
            value.runner_id: value
            for value in (
                self._responsibility_ledger
                .active_responsibilities()
            )
        }

        scored = (
            self._responsibility_ledger
            .apply_event(event)
        )

        after = {
            value.runner_id: value
            for value in (
                self._responsibility_ledger
                .active_responsibilities()
            )
        }

        for runner_id, responsibility in after.items():
            if runner_id not in before:
                self._earned_run_reconstructor.record_runner_reach(
                    responsibility=responsibility,
                    event=event,
                )

        for movement in event.runner_movements:
            if movement.is_out:
                self._earned_run_reconstructor.retire_runner(
                    movement.runner_id
                )

        for responsibility in scored:
            (
                self._earned_run_reconstructor
                .classify_scored_run(responsibility)
            )

        self._active[team_side] = updated
        return updated

    def responsibility_for_runner(
        self,
        runner_id: str,
    ) -> CanonicalRunnerResponsibility | None:
        return (
            self._responsibility_ledger
            .responsibility_for_runner(runner_id)
        )

    def active_runner_responsibilities(
        self,
    ) -> Tuple[
        CanonicalRunnerResponsibility,
        ...,
    ]:
        return (
            self._responsibility_ledger
            .active_responsibilities()
        )

    def run_classifications(
        self,
    ) -> Tuple[CanonicalRunClassification, ...]:
        return (
            self._earned_run_reconstructor
            .classifications()
        )

    def reconstructed_pitcher_run_lines(
        self,
    ) -> Tuple[CanonicalPitcherRunLine, ...]:
        return (
            self._earned_run_reconstructor
            .pitcher_run_lines()
        )

    def scored_run_responsibilities(
        self,
    ) -> Tuple[
        CanonicalScoredRunResponsibility,
        ...,
    ]:
        return (
            self._responsibility_ledger
            .scored_run_responsibilities()
        )

    def active_lifecycle(
        self,
        team_side: str,
    ) -> CanonicalPitcherLifecycleState:
        self._validate_team_side(team_side)
        return self._active[team_side]

    def completed_lifecycles(
        self,
        team_side: str,
    ) -> Tuple[CanonicalPitcherLifecycleState, ...]:
        self._validate_team_side(team_side)
        return tuple(self._completed[team_side])

    def used_pitcher_ids(
        self,
        team_side: str,
    ) -> Tuple[str, ...]:
        self._validate_team_side(team_side)
        return tuple(self._used_pitcher_ids[team_side])

    def _replace_pitcher(
        self,
        *,
        team_side: str,
        state: GameState,
        decision_context: CanonicalPitchingDecisionContext,
        decision,
    ) -> CanonicalPitcherLifecycleState:
        bullpen = self._bullpen_for_team(team_side)

        selection = self.bullpen_selector.select(
            CanonicalBullpenSelectionContext(
                pitching_decision=decision,
                game_context=decision_context,
                bullpen=bullpen,
                previously_used_pitcher_ids=tuple(
                    self._used_pitcher_ids[team_side]
                ),
            )
        )

        retired = retire_pitcher(
            self._active[team_side]
        )
        self._completed[team_side].append(retired)

        replacement = CanonicalPitcherLifecycleState(
            team_side=team_side,
            pitcher_id=selection.pitcher_id,
            role=CanonicalPitcherRole.RELIEVER,
            entered_inning=state.inning,
            entered_half=state.half,
        )

        self._active[team_side] = replacement
        self._used_pitcher_ids[team_side].append(
            replacement.pitcher_id
        )

        return replacement

    def _decision_context(
        self,
        *,
        state: GameState,
        batter_id: str,
        lifecycle: CanonicalPitcherLifecycleState,
    ) -> CanonicalPitchingDecisionContext:
        team_side = lifecycle.team_side

        batting_score = (
            state.away_score
            if state.half == "top"
            else state.home_score
        )
        fielding_score = (
            state.home_score
            if state.half == "top"
            else state.away_score
        )

        available = tuple(
            pitcher.pitcher_id
            for pitcher in self._bullpen_for_team(
                team_side
            )
            if pitcher.available
            and pitcher.pitcher_id
            not in self._used_pitcher_ids[team_side]
        )

        return CanonicalPitchingDecisionContext(
            lifecycle=lifecycle,
            inning=state.inning,
            half=state.half,
            outs=state.outs,
            batting_team_score=batting_score,
            fielding_team_score=fielding_score,
            runners_on_base=sum(
                runner is not None
                for runner in state.bases
            ),
            upcoming_batter_id=batter_id,
            available_reliever_ids=available,
        )

    def _bullpen_for_team(
        self,
        team_side: str,
    ) -> Tuple[CanonicalBullpenPitcher, ...]:
        return (
            self.away_bullpen
            if team_side == "away"
            else self.home_bullpen
        )

    @staticmethod
    def _fielding_team_side(
        state: GameState,
    ) -> str:
        if state.half == "top":
            return "home"

        if state.half == "bottom":
            return "away"

        raise ValueError(
            "state half must be 'top' or 'bottom'"
        )

    @staticmethod
    def _validate_team_side(
        team_side: str,
    ) -> None:
        if team_side not in {"away", "home"}:
            raise ValueError(
                "team_side must be 'away' or 'home'"
            )

    def _validate_bullpen(
        self,
        *,
        team_side: str,
        bullpen: Tuple[CanonicalBullpenPitcher, ...],
    ) -> None:
        plan = (
            self.matchup_input.away_pitching_plan
            if team_side == "away"
            else self.matchup_input.home_pitching_plan
        )

        bullpen_ids = tuple(
            pitcher.pitcher_id
            for pitcher in bullpen
        )

        if len(bullpen_ids) != len(set(bullpen_ids)):
            raise ValueError(
                f"{team_side} bullpen identifiers "
                "must be unique"
            )

        if set(bullpen_ids) != set(
            plan.bullpen_pitcher_ids
        ):
            raise ValueError(
                f"{team_side} bullpen must match "
                "the canonical pitching plan"
            )
