"""Production-facing canonical matchup-input contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from .contracts import CanonicalLineup


CANONICAL_MATCHUP_INPUT_VERSION = (
    "canonical_matchup_input_v1"
)


@dataclass(frozen=True)
class CanonicalPitchingPlan:
    """
    Fixed pitcher identity available to canonical trial resolvers.

    This contract records the planned pitcher pool only. It does not
    decide when a starter exits or which reliever enters.
    """

    team_side: str
    starter_id: str
    bullpen_pitcher_ids: Tuple[str, ...]

    def __post_init__(self) -> None:
        if self.team_side not in {
            "away",
            "home",
        }:
            raise ValueError(
                "team_side must be 'away' or 'home'"
            )

        if not self.starter_id:
            raise ValueError(
                "starter_id is required"
            )

        if any(
            not pitcher_id
            for pitcher_id in self.bullpen_pitcher_ids
        ):
            raise ValueError(
                "bullpen pitcher identifiers are required"
            )

        if len(self.bullpen_pitcher_ids) != len(
            set(self.bullpen_pitcher_ids)
        ):
            raise ValueError(
                "bullpen pitcher identifiers must be unique"
            )

        if self.starter_id in self.bullpen_pitcher_ids:
            raise ValueError(
                "starter cannot also appear in bullpen"
            )

    @property
    def available_pitcher_ids(
        self,
    ) -> Tuple[str, ...]:
        return (
            self.starter_id,
            *self.bullpen_pitcher_ids,
        )


@dataclass(frozen=True)
class CanonicalProbabilityProviderIdentity:
    """
    Provenance for the injected probability provider.

    The provider implementation remains external to canonical game
    orchestration. These fields identify what supplied probabilities.
    """

    provider_name: str
    provider_version: str
    artifact_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.provider_name:
            raise ValueError(
                "provider_name is required"
            )

        if not self.provider_version:
            raise ValueError(
                "provider_version is required"
            )

        if (
            self.artifact_id is not None
            and not self.artifact_id
        ):
            raise ValueError(
                "artifact_id cannot be empty"
            )

    @property
    def identity(self) -> str:
        value = (
            f"{self.provider_name}:"
            f"{self.provider_version}"
        )

        if self.artifact_id is not None:
            value += f":{self.artifact_id}"

        return value


@dataclass(frozen=True)
class CanonicalMatchupInput:
    """
    Fixed matchup identity shared by every trial in one batch.

    The contract records participants and provider provenance without
    constructing plate-appearance probabilities or substitution logic.
    """

    game_pk: int
    away_lineup: CanonicalLineup
    home_lineup: CanonicalLineup
    away_pitching_plan: CanonicalPitchingPlan
    home_pitching_plan: CanonicalPitchingPlan
    probability_provider: (
        CanonicalProbabilityProviderIdentity
    )
    schema_version: str = (
        CANONICAL_MATCHUP_INPUT_VERSION
    )

    def __post_init__(self) -> None:
        if isinstance(self.game_pk, bool):
            raise TypeError(
                "game_pk must be an integer"
            )

        if self.game_pk <= 0:
            raise ValueError(
                "game_pk must be positive"
            )

        if not isinstance(
            self.away_lineup,
            CanonicalLineup,
        ):
            raise TypeError(
                "away_lineup must be a CanonicalLineup"
            )

        if not isinstance(
            self.home_lineup,
            CanonicalLineup,
        ):
            raise TypeError(
                "home_lineup must be a CanonicalLineup"
            )

        if self.away_lineup.team_side != "away":
            raise ValueError(
                "away_lineup must use away team side"
            )

        if self.home_lineup.team_side != "home":
            raise ValueError(
                "home_lineup must use home team side"
            )

        if not isinstance(
            self.away_pitching_plan,
            CanonicalPitchingPlan,
        ):
            raise TypeError(
                "away_pitching_plan must be a "
                "CanonicalPitchingPlan"
            )

        if not isinstance(
            self.home_pitching_plan,
            CanonicalPitchingPlan,
        ):
            raise TypeError(
                "home_pitching_plan must be a "
                "CanonicalPitchingPlan"
            )

        if (
            self.away_pitching_plan.team_side
            != "away"
        ):
            raise ValueError(
                "away pitching plan must use away side"
            )

        if (
            self.home_pitching_plan.team_side
            != "home"
        ):
            raise ValueError(
                "home pitching plan must use home side"
            )

        if not isinstance(
            self.probability_provider,
            CanonicalProbabilityProviderIdentity,
        ):
            raise TypeError(
                "probability_provider must be a "
                "CanonicalProbabilityProviderIdentity"
            )

        if self.schema_version != (
            CANONICAL_MATCHUP_INPUT_VERSION
        ):
            raise ValueError(
                "unsupported canonical matchup schema"
            )

        away_players = set(
            self.away_lineup.player_ids
        )
        home_players = set(
            self.home_lineup.player_ids
        )

        if away_players & home_players:
            raise ValueError(
                "away and home lineups cannot share players"
            )
