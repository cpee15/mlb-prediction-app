"""Define leakage-safe historical probability reconstruction inputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from typing import Any, Dict, Optional, Sequence, Tuple

from .historical_lineup_bullpen_source import (
    CanonicalHistoricalLineupBullpenWindow,
)


CANONICAL_HISTORICAL_PROBABILITY_RECONSTRUCTION_INPUT_VERSION = (
    "canonical_historical_probability_reconstruction_input_v1"
)
HISTORICAL_PROBABILITY_STATISTICS_SOURCE = (
    "historical_probability_statistics_snapshot_v1"
)


def _date(value: str, name: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be an ISO date"
        ) from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _validate_digest(
    value: Optional[str],
    name: str,
) -> None:
    if value is None:
        return

    if (
        len(value) != 64
        or any(
            character not in "0123456789abcdef"
            for character in value
        )
    ):
        raise ValueError(
            f"{name} must be a SHA256 digest"
        )


@dataclass(frozen=True)
class CanonicalHistoricalProbabilityStatisticsSnapshot:
    """Statistics frozen before one historical game."""

    game_pk: int
    game_date: str
    statistics_through_date: str
    source_version: str
    snapshot_digest: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be positive"
            )

        game_date = _date(
            self.game_date,
            "game_date",
        )
        cutoff = _date(
            self.statistics_through_date,
            "statistics_through_date",
        )

        if cutoff >= game_date:
            raise ValueError(
                "statistics_through_date must be "
                "before game_date"
            )

        if not self.source_version.strip():
            raise ValueError(
                "source_version is required"
            )

        _validate_digest(
            self.snapshot_digest,
            "snapshot_digest",
        )

    @property
    def leakage_safe(self) -> bool:
        return (
            _date(
                self.statistics_through_date,
                "statistics_through_date",
            )
            < _date(
                self.game_date,
                "game_date",
            )
        )


@dataclass(frozen=True)
class CanonicalHistoricalProbabilityReconstructionInput:
    """One game's immutable reconstruction-input provenance."""

    game_pk: int
    game_date: str
    lineup_snapshot_digest: Optional[str]
    bullpen_snapshot_digest: Optional[str]
    statistics_through_date: Optional[str] = None
    statistics_source_version: Optional[str] = None
    statistics_snapshot_digest: Optional[str] = None
    schema_version: str = (
        CANONICAL_HISTORICAL_PROBABILITY_RECONSTRUCTION_INPUT_VERSION
    )

    def __post_init__(self) -> None:
        if (
            isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be positive"
            )

        game_date = _date(
            self.game_date,
            "game_date",
        )

        for name, value in (
            (
                "lineup_snapshot_digest",
                self.lineup_snapshot_digest,
            ),
            (
                "bullpen_snapshot_digest",
                self.bullpen_snapshot_digest,
            ),
            (
                "statistics_snapshot_digest",
                self.statistics_snapshot_digest,
            ),
        ):
            _validate_digest(value, name)

        statistics_values = (
            self.statistics_through_date,
            self.statistics_source_version,
            self.statistics_snapshot_digest,
        )

        if (
            any(value is not None for value in statistics_values)
            and not all(
                value is not None
                for value in statistics_values
            )
        ):
            raise ValueError(
                "historical statistics provenance "
                "must be complete"
            )

        if self.statistics_through_date is not None:
            cutoff = _date(
                self.statistics_through_date,
                "statistics_through_date",
            )
            if cutoff >= game_date:
                raise ValueError(
                    "statistics_through_date must be "
                    "before game_date"
                )

        if (
            self.statistics_source_version is not None
            and not self.statistics_source_version.strip()
        ):
            raise ValueError(
                "statistics_source_version cannot be blank"
            )

        if self.schema_version != (
            CANONICAL_HISTORICAL_PROBABILITY_RECONSTRUCTION_INPUT_VERSION
        ):
            raise ValueError(
                "unsupported historical probability "
                "reconstruction-input version"
            )

    @property
    def statistics_ready(self) -> bool:
        return all(
            value is not None
            for value in (
                self.statistics_through_date,
                self.statistics_source_version,
                self.statistics_snapshot_digest,
            )
        )

    @property
    def leakage_safe(self) -> bool:
        return (
            self.statistics_ready
            and _date(
                self.statistics_through_date or "",
                "statistics_through_date",
            )
            < _date(
                self.game_date,
                "game_date",
            )
        )

    @property
    def missing_requirements(self) -> Tuple[str, ...]:
        missing = []

        if self.lineup_snapshot_digest is None:
            missing.append(
                "missing_historical_lineup_snapshot"
            )
        if self.bullpen_snapshot_digest is None:
            missing.append(
                "missing_historical_bullpen_snapshot"
            )
        if not self.statistics_ready:
            missing.append(
                "missing_historical_statistics_snapshot"
            )

        return tuple(missing)

    @property
    def ready(self) -> bool:
        return (
            not self.missing_requirements
            and self.leakage_safe
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "ready": self.ready,
            "leakage_safe": self.leakage_safe,
            "lineup_snapshot_ready": (
                self.lineup_snapshot_digest is not None
            ),
            "bullpen_snapshot_ready": (
                self.bullpen_snapshot_digest is not None
            ),
            "statistics_snapshot_ready": (
                self.statistics_ready
            ),
            "statistics_through_date": (
                self.statistics_through_date
            ),
            "statistics_source_version": (
                self.statistics_source_version
            ),
            "statistics_snapshot_digest": (
                self.statistics_snapshot_digest
            ),
            "missing_requirements": (
                self.missing_requirements
            ),
            "player_identifiers_exposed": False,
            "probability_records_exposed": False,
            "historical_replay_executed": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalHistoricalProbabilityReconstructionInputWindow:
    """Exact historical window of reconstruction-input provenance."""

    observed_window_digest: str
    lineup_bullpen_window_digest: str
    inputs: Tuple[
        CanonicalHistoricalProbabilityReconstructionInput,
        ...,
    ]
    digest: str
    schema_version: str = (
        CANONICAL_HISTORICAL_PROBABILITY_RECONSTRUCTION_INPUT_VERSION
    )

    def __post_init__(self) -> None:
        for name, value in (
            (
                "observed_window_digest",
                self.observed_window_digest,
            ),
            (
                "lineup_bullpen_window_digest",
                self.lineup_bullpen_window_digest,
            ),
            ("digest", self.digest),
        ):
            _validate_digest(value, name)

        if not self.inputs:
            raise ValueError(
                "inputs must contain historical games"
            )

        identities = tuple(
            value.game_pk
            for value in self.inputs
        )
        if len(identities) != len(set(identities)):
            raise ValueError(
                "reconstruction input game identifiers "
                "must be unique"
            )

        if self.schema_version != (
            CANONICAL_HISTORICAL_PROBABILITY_RECONSTRUCTION_INPUT_VERSION
        ):
            raise ValueError(
                "unsupported historical probability "
                "reconstruction-input window version"
            )

    @property
    def game_count(self) -> int:
        return len(self.inputs)

    @property
    def ready_game_count(self) -> int:
        return sum(
            value.ready
            for value in self.inputs
        )

    @property
    def blocked_game_count(self) -> int:
        return (
            self.game_count
            - self.ready_game_count
        )

    @property
    def ready(self) -> bool:
        return (
            self.ready_game_count
            == self.game_count
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        missing_counts: Dict[str, int] = {}

        for value in self.inputs:
            for requirement in value.missing_requirements:
                missing_counts[requirement] = (
                    missing_counts.get(
                        requirement,
                        0,
                    )
                    + 1
                )

        return {
            "schema_version": self.schema_version,
            "ready": self.ready,
            "game_count": self.game_count,
            "ready_game_count": (
                self.ready_game_count
            ),
            "blocked_game_count": (
                self.blocked_game_count
            ),
            "missing_requirement_counts": (
                missing_counts
            ),
            "observed_window_digest": (
                self.observed_window_digest
            ),
            "lineup_bullpen_window_digest": (
                self.lineup_bullpen_window_digest
            ),
            "reconstruction_input_digest": (
                self.digest
            ),
            "inputs": tuple(
                value.to_diagnostics()
                for value in self.inputs
            ),
            "exact_game_coverage": True,
            "future_data_permitted": False,
            "player_identifiers_exposed": False,
            "probability_records_exposed": False,
            "probability_workspace_reconstructed": False,
            "historical_replay_executed": False,
            "historical_replay_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def define_historical_probability_reconstruction_inputs(
    *,
    lineup_bullpen: CanonicalHistoricalLineupBullpenWindow,
    statistics_snapshots: Sequence[
        CanonicalHistoricalProbabilityStatisticsSnapshot
    ] = (),
) -> CanonicalHistoricalProbabilityReconstructionInputWindow:
    """
    Join exact roster snapshots to strictly pregame statistics snapshots.

    Sparse statistics coverage remains explicit. This function does not
    calculate probabilities, build artifacts, execute simulation, or permit
    production activation.
    """

    if not isinstance(
        lineup_bullpen,
        CanonicalHistoricalLineupBullpenWindow,
    ):
        raise TypeError(
            "lineup_bullpen must be a "
            "CanonicalHistoricalLineupBullpenWindow"
        )

    if (
        not isinstance(statistics_snapshots, Sequence)
        or isinstance(
            statistics_snapshots,
            (str, bytes),
        )
    ):
        raise TypeError(
            "statistics_snapshots must be a sequence"
        )

    statistics_by_game = {}

    for value in statistics_snapshots:
        if not isinstance(
            value,
            CanonicalHistoricalProbabilityStatisticsSnapshot,
        ):
            raise TypeError(
                "statistics_snapshots must contain "
                "CanonicalHistoricalProbabilityStatisticsSnapshot"
            )

        if value.game_pk in statistics_by_game:
            raise ValueError(
                "statistics snapshot game identifiers "
                "must be unique"
            )

        statistics_by_game[value.game_pk] = value

    roster_by_game = {
        value.game_pk: value
        for value in lineup_bullpen.games
    }

    unknown_games = (
        set(statistics_by_game)
        - set(roster_by_game)
    )
    if unknown_games:
        raise ValueError(
            "statistics snapshots contain unknown games"
        )

    inputs = []

    for roster in sorted(
        lineup_bullpen.games,
        key=lambda value: (
            value.game_date,
            value.game_pk,
        ),
    ):
        statistics = statistics_by_game.get(
            roster.game_pk
        )

        if (
            statistics is not None
            and statistics.game_date != roster.game_date
        ):
            raise ValueError(
                "statistics snapshot game_date must "
                "match historical roster game_date"
            )

        inputs.append(
            CanonicalHistoricalProbabilityReconstructionInput(
                game_pk=roster.game_pk,
                game_date=roster.game_date,
                lineup_snapshot_digest=(
                    roster.lineup_digest
                    if roster.lineups_ready
                    else None
                ),
                bullpen_snapshot_digest=(
                    roster.bullpen_digest
                    if roster.bullpens_ready
                    else None
                ),
                statistics_through_date=(
                    statistics.statistics_through_date
                    if statistics is not None
                    else None
                ),
                statistics_source_version=(
                    statistics.source_version
                    if statistics is not None
                    else None
                ),
                statistics_snapshot_digest=(
                    statistics.snapshot_digest
                    if statistics is not None
                    else None
                ),
            )
        )

    digest = _digest(
        {
            "schema_version": (
                CANONICAL_HISTORICAL_PROBABILITY_RECONSTRUCTION_INPUT_VERSION
            ),
            "observed_window_digest": (
                lineup_bullpen.observed_window_digest
            ),
            "lineup_bullpen_window_digest": (
                lineup_bullpen.digest
            ),
            "inputs": [
                {
                    "game_pk": value.game_pk,
                    "game_date": value.game_date,
                    "lineup_snapshot_digest": (
                        value.lineup_snapshot_digest
                    ),
                    "bullpen_snapshot_digest": (
                        value.bullpen_snapshot_digest
                    ),
                    "statistics_through_date": (
                        value.statistics_through_date
                    ),
                    "statistics_source_version": (
                        value.statistics_source_version
                    ),
                    "statistics_snapshot_digest": (
                        value.statistics_snapshot_digest
                    ),
                }
                for value in inputs
            ],
        }
    )

    return (
        CanonicalHistoricalProbabilityReconstructionInputWindow(
            observed_window_digest=(
                lineup_bullpen.observed_window_digest
            ),
            lineup_bullpen_window_digest=(
                lineup_bullpen.digest
            ),
            inputs=tuple(inputs),
            digest=digest,
        )
    )
