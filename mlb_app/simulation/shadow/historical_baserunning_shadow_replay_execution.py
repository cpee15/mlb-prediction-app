"""Execute deterministic historical baserunning shadow replays."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from typing import Any, Dict, Mapping, Tuple

from .bullpen_discovery import (
    CanonicalShadowBullpenDiscovery,
    CanonicalShadowBullpenSideDiscovery,
)
from .exact_artifact_discovery import (
    CanonicalShadowExactArtifactDiscovery,
)
from .fallback_catalog_discovery import (
    CanonicalShadowFallbackCatalogDiscovery,
)
from .historical_baserunning_replay_evidence_source import (
    CanonicalHistoricalBaserunningReplayEvidenceWindow,
)
from .historical_lineup_bullpen_source import (
    CanonicalHistoricalLineupBullpenWindow,
)
from .historical_probability_artifact_materialization import (
    CanonicalHistoricalProbabilityArtifactWindow,
)
from .lineup_discovery import (
    CanonicalShadowLineupDiscovery,
)
from .probability_provider_discovery import (
    CanonicalShadowProbabilityProviderDiscovery,
    REQUIRED_WORKSPACE_MODELS,
)
from .production_execution import (
    CanonicalProductionShadowExecution,
    run_canonical_production_shadow,
)


CANONICAL_HISTORICAL_BASERUNNING_SHADOW_REPLAY_VERSION = (
    "canonical_historical_baserunning_shadow_replay_v1"
)
DEFAULT_HISTORICAL_BASERUNNING_SIMULATION_COUNT = 25


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _validate_digest(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(
            character not in "0123456789abcdef"
            for character in value
        )
    ):
        raise ValueError(
            f"{name} must be a SHA256 digest"
        )


def _validate_date(value: str) -> None:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "game_date must be an ISO date"
        ) from exc

    if parsed.isoformat() != value:
        raise ValueError(
            "game_date must use ISO format"
        )


def _normalize_starters(
    values: Mapping[
        int,
        Tuple[str, str],
    ],
    *,
    expected: set[int],
) -> Dict[int, Tuple[str, str]]:
    if not isinstance(values, Mapping):
        raise TypeError(
            "starting_pitcher_ids must be a mapping"
        )

    normalized = {}

    for raw_game_pk, raw_pair in values.items():
        try:
            game_pk = int(raw_game_pk)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "starting pitcher game identifiers "
                "must be integers"
            ) from exc

        if (
            not isinstance(raw_pair, tuple)
            or len(raw_pair) != 2
        ):
            raise TypeError(
                "starting pitcher values must be "
                "away-home tuples"
            )

        pair = []
        for raw_identifier in raw_pair:
            try:
                identifier = int(raw_identifier)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "starting pitcher identifiers "
                    "must be positive integers"
                ) from exc

            if identifier <= 0:
                raise ValueError(
                    "starting pitcher identifiers "
                    "must be positive integers"
                )

            pair.append(str(identifier))

        normalized[game_pk] = (
            pair[0],
            pair[1],
        )

    if set(normalized) != expected:
        raise ValueError(
            "starting pitchers must exactly cover "
            "historical replay games"
        )

    return normalized


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningShadowReplayGame:
    game_pk: int
    game_date: str
    execution: CanonicalProductionShadowExecution
    probability_artifact_digest: str
    baserunning_evidence_digest: str
    output_digest: str
    replay_digest: str
    replay_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_SHADOW_REPLAY_VERSION
    )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.game_pk, int)
            or isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be a positive integer"
            )

        _validate_date(self.game_date)

        if not isinstance(
            self.execution,
            CanonicalProductionShadowExecution,
        ):
            raise TypeError(
                "execution must be "
                "CanonicalProductionShadowExecution"
            )

        for name, value in (
            (
                "probability_artifact_digest",
                self.probability_artifact_digest,
            ),
            (
                "baserunning_evidence_digest",
                self.baserunning_evidence_digest,
            ),
            ("output_digest", self.output_digest),
            ("replay_digest", self.replay_digest),
        ):
            _validate_digest(value, name)

        if self.replay_version != (
            CANONICAL_HISTORICAL_BASERUNNING_SHADOW_REPLAY_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "shadow replay version"
            )

    @property
    def executed(self) -> bool:
        return (
            self.execution.status == "executed"
            and self.execution.executed
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "status": self.execution.status,
            "executed": self.executed,
            "simulation_count": (
                self.execution.simulation_count
            ),
            "probability_artifact_digest": (
                self.probability_artifact_digest
            ),
            "baserunning_evidence_digest": (
                self.baserunning_evidence_digest
            ),
            "output_digest": self.output_digest,
            "replay_digest": self.replay_digest,
            "error_type": self.execution.error_type,
            "error_message": (
                self.execution.error_message
            ),
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningShadowReplayWindow:
    observed_window_digest: str
    lineup_bullpen_window_digest: str
    probability_artifact_window_digest: str
    baserunning_evidence_window_digest: str
    simulation_count: int
    games: Tuple[
        CanonicalHistoricalBaserunningShadowReplayGame,
        ...,
    ]
    digest: str
    replay_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_SHADOW_REPLAY_VERSION
    )

    def __post_init__(self) -> None:
        if (
            isinstance(self.simulation_count, bool)
            or self.simulation_count <= 0
        ):
            raise ValueError(
                "simulation_count must be positive"
            )

        if not self.games:
            raise ValueError(
                "games must contain historical replays"
            )

        identities = tuple(
            value.game_pk
            for value in self.games
        )
        if len(identities) != len(set(identities)):
            raise ValueError(
                "historical replay game identifiers "
                "must be unique"
            )

        for name, value in (
            (
                "observed_window_digest",
                self.observed_window_digest,
            ),
            (
                "lineup_bullpen_window_digest",
                self.lineup_bullpen_window_digest,
            ),
            (
                "probability_artifact_window_digest",
                self.probability_artifact_window_digest,
            ),
            (
                "baserunning_evidence_window_digest",
                self.baserunning_evidence_window_digest,
            ),
            ("digest", self.digest),
        ):
            _validate_digest(value, name)

        if self.replay_version != (
            CANONICAL_HISTORICAL_BASERUNNING_SHADOW_REPLAY_VERSION
        ):
            raise ValueError(
                "unsupported historical replay "
                "window version"
            )

    @property
    def game_count(self) -> int:
        return len(self.games)

    @property
    def executed_game_count(self) -> int:
        return sum(
            value.executed
            for value in self.games
        )

    @property
    def blocked_game_count(self) -> int:
        return sum(
            value.execution.status == "blocked"
            for value in self.games
        )

    @property
    def error_game_count(self) -> int:
        return sum(
            value.execution.status == "error"
            for value in self.games
        )

    @property
    def ready(self) -> bool:
        return (
            self.executed_game_count
            == self.game_count
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.replay_version,
            "ready": self.ready,
            "game_count": self.game_count,
            "executed_game_count": (
                self.executed_game_count
            ),
            "blocked_game_count": (
                self.blocked_game_count
            ),
            "error_game_count": self.error_game_count,
            "simulation_count": self.simulation_count,
            "observed_window_digest": (
                self.observed_window_digest
            ),
            "lineup_bullpen_window_digest": (
                self.lineup_bullpen_window_digest
            ),
            "probability_artifact_window_digest": (
                self.probability_artifact_window_digest
            ),
            "baserunning_evidence_window_digest": (
                self.baserunning_evidence_window_digest
            ),
            "replay_window_digest": self.digest,
            "games": tuple(
                value.to_diagnostics()
                for value in self.games
            ),
            "historical_replay_executed": (
                self.executed_game_count > 0
            ),
            "external_fetch_performed": False,
            "persistence_performed": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def execute_historical_baserunning_shadow_replays(
    *,
    lineup_bullpen: CanonicalHistoricalLineupBullpenWindow,
    probability_artifacts: (
        CanonicalHistoricalProbabilityArtifactWindow
    ),
    baserunning_evidence: (
        CanonicalHistoricalBaserunningReplayEvidenceWindow
    ),
    starting_pitcher_ids: Mapping[
        int,
        Tuple[str, str],
    ],
    simulation_count: int = (
        DEFAULT_HISTORICAL_BASERUNNING_SIMULATION_COUNT
    ),
) -> CanonicalHistoricalBaserunningShadowReplayWindow:
    """Run one production-shaped canonical shadow batch per game."""

    if not isinstance(
        lineup_bullpen,
        CanonicalHistoricalLineupBullpenWindow,
    ):
        raise TypeError(
            "lineup_bullpen must be a "
            "CanonicalHistoricalLineupBullpenWindow"
        )

    if not isinstance(
        probability_artifacts,
        CanonicalHistoricalProbabilityArtifactWindow,
    ):
        raise TypeError(
            "probability_artifacts must be a "
            "CanonicalHistoricalProbabilityArtifactWindow"
        )

    if not isinstance(
        baserunning_evidence,
        CanonicalHistoricalBaserunningReplayEvidenceWindow,
    ):
        raise TypeError(
            "baserunning_evidence must be a "
            "CanonicalHistoricalBaserunningReplayEvidenceWindow"
        )

    if (
        isinstance(simulation_count, bool)
        or int(simulation_count) <= 0
    ):
        raise ValueError(
            "simulation_count must be positive"
        )
    normalized_simulation_count = int(
        simulation_count
    )

    if not (
        lineup_bullpen.observed_window_digest
        == probability_artifacts.observed_window_digest
        == baserunning_evidence.observed_window_digest
    ):
        raise ValueError(
            "observed window digests must match"
        )

    if (
        baserunning_evidence.lineup_bullpen_window_digest
        != lineup_bullpen.digest
    ):
        raise ValueError(
            "lineup-bullpen window digests must match"
        )

    lineups_by_id = {
        value.game_pk: value
        for value in lineup_bullpen.games
    }
    artifacts_by_id = {
        value.game_pk: value
        for value in probability_artifacts.games
    }
    evidence_by_id = {
        value.game_pk: value
        for value in baserunning_evidence.games
    }

    expected = set(lineups_by_id)
    if not (
        expected
        == set(artifacts_by_id)
        == set(evidence_by_id)
    ):
        raise ValueError(
            "historical replay inputs must exactly "
            "cover the same games"
        )

    starters = _normalize_starters(
        starting_pitcher_ids,
        expected=expected,
    )

    replay_games = []

    for game_pk in sorted(
        expected,
        key=lambda value: (
            lineups_by_id[value].game_date,
            value,
        ),
    ):
        roster = lineups_by_id[game_pk]
        artifact = artifacts_by_id[game_pk]
        evidence = evidence_by_id[game_pk]
        away_starter, home_starter = (
            starters[game_pk]
        )

        if not (
            roster.game_date
            == artifact.game_date
            == evidence.game_date
        ):
            raise ValueError(
                "historical replay game dates must match"
            )

        lineups = CanonicalShadowLineupDiscovery(
            away_player_ids=roster.away_lineup_ids,
            home_player_ids=roster.home_lineup_ids,
            away_source_count=len(
                roster.away_lineup_ids
            ),
            home_source_count=len(
                roster.home_lineup_ids
            ),
            status="ready",
        )

        bullpens = CanonicalShadowBullpenDiscovery(
            away=CanonicalShadowBullpenSideDiscovery(
                starter_id=away_starter,
                bullpen_pitcher_ids=(
                    roster.away_bullpen_ids
                ),
                source_record_count=(
                    len(roster.away_bullpen_ids) + 1
                ),
                status="ready",
            ),
            home=CanonicalShadowBullpenSideDiscovery(
                starter_id=home_starter,
                bullpen_pitcher_ids=(
                    roster.home_bullpen_ids
                ),
                source_record_count=(
                    len(roster.home_bullpen_ids) + 1
                ),
                status="ready",
            ),
        )

        provider = artifact.exact_artifact.provider
        provider_discovery = (
            CanonicalShadowProbabilityProviderDiscovery(
                provider=provider,
                model_versions=(
                    provider.provider_version,
                ),
                valid_model_count=len(
                    REQUIRED_WORKSPACE_MODELS
                ),
                status="ready",
            )
        )

        away_ids = set(
            roster.away_lineup_ids
        )
        home_ids = set(
            roster.home_lineup_ids
        )
        exact_records = (
            artifact.exact_artifact.records
        )
        away_record_count = sum(
            record.batter_id in away_ids
            for record in exact_records
        )
        home_record_count = sum(
            record.batter_id in home_ids
            for record in exact_records
        )

        exact_discovery = (
            CanonicalShadowExactArtifactDiscovery(
                artifact=artifact.exact_artifact,
                away_record_count=away_record_count,
                home_record_count=home_record_count,
                away_real_profile_count=(
                    away_record_count
                ),
                home_real_profile_count=(
                    home_record_count
                ),
                status="ready",
            )
        )

        fallback_discovery = (
            CanonicalShadowFallbackCatalogDiscovery(
                catalog=artifact.fallback_catalog,
                source_model_count=len(
                    REQUIRED_WORKSPACE_MODELS
                ),
                status="ready",
            )
        )

        execution = run_canonical_production_shadow(
            game_pk=game_pk,
            lineups=lineups,
            bullpens=bullpens,
            provider_discovery=(
                provider_discovery
            ),
            exact_artifact_discovery=(
                exact_discovery
            ),
            fallback_catalog_discovery=(
                fallback_discovery
            ),
            bootstrap_ready=True,
            baserunning_evidence_catalog=(
                evidence.catalog
            ),
            simulation_count=(
                normalized_simulation_count
            ),
        )

        output_payload = (
            execution.material.canonical_payload
            if execution.material is not None
            else execution.to_diagnostics()
        )
        output_digest = _sha256(
            output_payload
        )
        replay_digest = _sha256(
            {
                "game_pk": game_pk,
                "game_date": roster.game_date,
                "lineup_digest": (
                    roster.lineup_digest
                ),
                "bullpen_digest": (
                    roster.bullpen_digest
                ),
                "probability_artifact_digest": (
                    artifact.digest
                ),
                "baserunning_evidence_digest": (
                    evidence.evidence_digest
                ),
                "simulation_count": (
                    normalized_simulation_count
                ),
                "output_digest": output_digest,
                "status": execution.status,
            }
        )

        replay_games.append(
            CanonicalHistoricalBaserunningShadowReplayGame(
                game_pk=game_pk,
                game_date=roster.game_date,
                execution=execution,
                probability_artifact_digest=(
                    artifact.digest
                ),
                baserunning_evidence_digest=(
                    evidence.evidence_digest
                ),
                output_digest=output_digest,
                replay_digest=replay_digest,
            )
        )

    window_digest = _sha256(
        {
            "schema_version": (
                CANONICAL_HISTORICAL_BASERUNNING_SHADOW_REPLAY_VERSION
            ),
            "observed_window_digest": (
                lineup_bullpen.observed_window_digest
            ),
            "lineup_bullpen_window_digest": (
                lineup_bullpen.digest
            ),
            "probability_artifact_window_digest": (
                probability_artifacts.digest
            ),
            "baserunning_evidence_window_digest": (
                baserunning_evidence.digest
            ),
            "simulation_count": (
                normalized_simulation_count
            ),
            "games": [
                {
                    "game_pk": value.game_pk,
                    "replay_digest": (
                        value.replay_digest
                    ),
                }
                for value in replay_games
            ],
        }
    )

    return CanonicalHistoricalBaserunningShadowReplayWindow(
        observed_window_digest=(
            lineup_bullpen.observed_window_digest
        ),
        lineup_bullpen_window_digest=(
            lineup_bullpen.digest
        ),
        probability_artifact_window_digest=(
            probability_artifacts.digest
        ),
        baserunning_evidence_window_digest=(
            baserunning_evidence.digest
        ),
        simulation_count=(
            normalized_simulation_count
        ),
        games=tuple(replay_games),
        digest=window_digest,
    )
