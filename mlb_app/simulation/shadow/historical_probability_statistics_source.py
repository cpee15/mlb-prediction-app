"""Source cutoff-safe historical probability statistics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json
from typing import Any, Dict, Mapping, Sequence, Tuple

from .historical_lineup_bullpen_source import (
    CanonicalHistoricalLineupBullpenWindow,
)
from .historical_probability_reconstruction_input import (
    HISTORICAL_PROBABILITY_STATISTICS_SOURCE,
    CanonicalHistoricalProbabilityStatisticsSnapshot,
)


CANONICAL_HISTORICAL_PROBABILITY_STATISTICS_SOURCE_VERSION = (
    "canonical_historical_probability_statistics_source_v1"
)

HITTING_STAT_KEYS = (
    ("pa", "plateAppearances"),
    ("ab", "atBats"),
    ("hits", "hits"),
    ("double", "doubles"),
    ("triple", "triples"),
    ("hr", "homeRuns"),
    ("bb", "baseOnBalls"),
    ("k", "strikeOuts"),
    ("hbp", "hitByPitch"),
)

PITCHING_STAT_KEYS = (
    ("batters_faced", "battersFaced"),
    ("ab", "atBats"),
    ("hits", "hits"),
    ("double", "doubles"),
    ("triple", "triples"),
    ("hr", "homeRuns"),
    ("bb", "baseOnBalls"),
    ("k", "strikeOuts"),
    ("hbp", "hitBatsmen"),
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> Sequence[Any]:
    if (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes))
    ):
        return value
    return ()


def _identifier(value: Any) -> str | None:
    if isinstance(value, Mapping):
        value = (
            _mapping(value.get("person")).get("id")
            or value.get("id")
            or value.get("player_id")
        )

    if value in (None, "") or isinstance(value, bool):
        return None

    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None

    return str(parsed) if parsed > 0 else None


def _iso_date(value: str, name: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be an ISO date"
        ) from exc


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _count(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(
            f"{name} must be a nonnegative integer"
        )

    try:
        parsed_float = float(value)
        parsed = int(parsed_float)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be a nonnegative integer"
        ) from exc

    if parsed < 0 or parsed_float != parsed:
        raise ValueError(
            f"{name} must be a nonnegative integer"
        )

    return parsed


def _rows(
    *,
    payload: Mapping[str, Any],
    role: str,
) -> Dict[str, Tuple[Tuple[str, int], ...]]:
    keys = (
        HITTING_STAT_KEYS
        if role == "hitting"
        else PITCHING_STAT_KEYS
    )
    rows: Dict[str, Tuple[Tuple[str, int], ...]] = {}

    for block in _sequence(payload.get("stats")):
        for split in _sequence(
            _mapping(block).get("splits")
        ):
            split_data = _mapping(split)
            player_id = _identifier(
                split_data.get("player")
            )

            if player_id is None:
                raise ValueError(
                    "statistics split requires player identity"
                )
            if player_id in rows:
                raise ValueError(
                    "statistics player identities must be unique "
                    f"within {role}"
                )

            statistics = _mapping(
                split_data.get("stat")
            )
            counts = []

            for canonical, source in keys:
                if source not in statistics:
                    raise ValueError(
                        f"{role} statistics missing {source}"
                    )

                counts.append(
                    (
                        canonical,
                        _count(
                            statistics[source],
                            f"{role}.{source}",
                        ),
                    )
                )

            rows[player_id] = tuple(counts)

    return rows


@dataclass(frozen=True)
class CanonicalHistoricalProbabilityPlayerStatistics:
    player_id: str
    role: str
    counts: Tuple[Tuple[str, int], ...]
    sample_available: bool

    def __post_init__(self) -> None:
        if _identifier(self.player_id) != self.player_id:
            raise ValueError(
                "player_id must be a positive integer string"
            )
        if self.role not in {"hitting", "pitching"}:
            raise ValueError(
                "role must be hitting or pitching"
            )

        expected = tuple(
            key
            for key, _ in (
                HITTING_STAT_KEYS
                if self.role == "hitting"
                else PITCHING_STAT_KEYS
            )
        )

        if tuple(
            key for key, _ in self.counts
        ) != expected:
            raise ValueError(
                "counts must use canonical role order"
            )

        for key, value in self.counts:
            _count(value, f"{self.role}.{key}")

        if not isinstance(
            self.sample_available,
            bool,
        ):
            raise TypeError(
                "sample_available must be boolean"
            )

        if (
            not self.sample_available
            and any(value != 0 for _, value in self.counts)
        ):
            raise ValueError(
                "zero-sample records must contain zero counts"
            )

    @property
    def record_key(self) -> Tuple[str, str]:
        return self.role, self.player_id


@dataclass(frozen=True)
class CanonicalHistoricalProbabilityGameStatistics:
    game_pk: int
    game_date: str
    statistics_through_date: str
    players: Tuple[
        CanonicalHistoricalProbabilityPlayerStatistics,
        ...,
    ]
    snapshot_digest: str
    source_version: str = (
        HISTORICAL_PROBABILITY_STATISTICS_SOURCE
    )

    def __post_init__(self) -> None:
        if (
            isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be positive"
            )

        if _iso_date(
            self.statistics_through_date,
            "statistics_through_date",
        ) >= _iso_date(
            self.game_date,
            "game_date",
        ):
            raise ValueError(
                "statistics_through_date must be "
                "before game_date"
            )

        keys = tuple(
            value.record_key
            for value in self.players
        )
        if not keys:
            raise ValueError(
                "players must contain required statistics"
            )
        if len(keys) != len(set(keys)):
            raise ValueError(
                "player-role statistics must be unique"
            )

        if (
            len(self.snapshot_digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.snapshot_digest
            )
        ):
            raise ValueError(
                "snapshot_digest must be a SHA256 digest"
            )

        if not self.source_version.strip():
            raise ValueError(
                "source_version is required"
            )

    @property
    def observed_sample_count(self) -> int:
        return sum(
            value.sample_available
            for value in self.players
        )

    @property
    def zero_sample_count(self) -> int:
        return (
            len(self.players)
            - self.observed_sample_count
        )

    def to_reconstruction_snapshot(
        self,
    ) -> CanonicalHistoricalProbabilityStatisticsSnapshot:
        return CanonicalHistoricalProbabilityStatisticsSnapshot(
            game_pk=self.game_pk,
            game_date=self.game_date,
            statistics_through_date=(
                self.statistics_through_date
            ),
            source_version=self.source_version,
            snapshot_digest=self.snapshot_digest,
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "statistics_through_date": (
                self.statistics_through_date
            ),
            "required_player_role_count": len(
                self.players
            ),
            "observed_sample_count": (
                self.observed_sample_count
            ),
            "zero_sample_count": (
                self.zero_sample_count
            ),
            "snapshot_digest": self.snapshot_digest,
            "ready": True,
            "leakage_safe": True,
            "player_identifiers_exposed": False,
            "probability_records_exposed": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalHistoricalProbabilityStatisticsWindow:
    observed_window_digest: str
    lineup_bullpen_window_digest: str
    games: Tuple[
        CanonicalHistoricalProbabilityGameStatistics,
        ...,
    ]
    digest: str
    source_version: str = (
        CANONICAL_HISTORICAL_PROBABILITY_STATISTICS_SOURCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.games:
            raise ValueError(
                "games must contain statistics snapshots"
            )

        identities = tuple(
            value.game_pk
            for value in self.games
        )
        if len(identities) != len(set(identities)):
            raise ValueError(
                "statistics game identifiers must be unique"
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
            ("digest", self.digest),
        ):
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

        if self.source_version != (
            CANONICAL_HISTORICAL_PROBABILITY_STATISTICS_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported historical probability "
                "statistics source version"
            )

    @property
    def game_count(self) -> int:
        return len(self.games)

    @property
    def zero_sample_count(self) -> int:
        return sum(
            value.zero_sample_count
            for value in self.games
        )

    def to_reconstruction_snapshots(
        self,
    ) -> Tuple[
        CanonicalHistoricalProbabilityStatisticsSnapshot,
        ...,
    ]:
        return tuple(
            value.to_reconstruction_snapshot()
            for value in self.games
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.source_version,
            "ready": True,
            "game_count": self.game_count,
            "ready_game_count": self.game_count,
            "zero_sample_player_role_count": (
                self.zero_sample_count
            ),
            "observed_window_digest": (
                self.observed_window_digest
            ),
            "lineup_bullpen_window_digest": (
                self.lineup_bullpen_window_digest
            ),
            "statistics_window_digest": self.digest,
            "games": tuple(
                value.to_diagnostics()
                for value in self.games
            ),
            "statistics_cutoff_policy": (
                "strict_previous_calendar_date"
            ),
            "doubleheader_same_cutoff": True,
            "future_data_permitted": False,
            "player_identifiers_exposed": False,
            "probability_records_exposed": False,
            "probability_workspace_reconstructed": False,
            "historical_replay_executed": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def source_historical_probability_statistics(
    *,
    lineup_bullpen: CanonicalHistoricalLineupBullpenWindow,
    starting_pitcher_ids: Mapping[
        int,
        Tuple[str, str],
    ],
    statistics_payloads: Mapping[
        str,
        Mapping[str, Mapping[str, Any]],
    ],
) -> CanonicalHistoricalProbabilityStatisticsWindow:
    """
    Source exact prior-day player statistics for historical reconstruction.

    A valid full-player response that omits a required player represents an
    explicit zero-prior-sample record. Missing dates or groups are errors.
    """

    if not isinstance(
        lineup_bullpen,
        CanonicalHistoricalLineupBullpenWindow,
    ):
        raise TypeError(
            "lineup_bullpen must be a "
            "CanonicalHistoricalLineupBullpenWindow"
        )
    if not isinstance(starting_pitcher_ids, Mapping):
        raise TypeError(
            "starting_pitcher_ids must be a mapping"
        )
    if not isinstance(statistics_payloads, Mapping):
        raise TypeError(
            "statistics_payloads must be a mapping"
        )

    games_by_id = {
        value.game_pk: value
        for value in lineup_bullpen.games
    }

    normalized_starters = {}
    for raw_game_pk, raw_ids in (
        starting_pitcher_ids.items()
    ):
        try:
            game_pk = int(raw_game_pk)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "starting pitcher game identifiers "
                "must be integers"
            ) from exc

        if game_pk not in games_by_id:
            raise ValueError(
                "starting pitchers contain unknown games"
            )
        if (
            not isinstance(raw_ids, tuple)
            or len(raw_ids) != 2
        ):
            raise TypeError(
                "starting pitcher values must be "
                "away-home tuples"
            )

        away_id = _identifier(raw_ids[0])
        home_id = _identifier(raw_ids[1])
        if away_id is None or home_id is None:
            raise ValueError(
                "starting pitcher identifiers are required"
            )

        normalized_starters[game_pk] = (
            away_id,
            home_id,
        )

    if set(normalized_starters) != set(games_by_id):
        raise ValueError(
            "starting pitchers must exactly cover "
            "historical games"
        )

    required_cutoffs = {
        (
            _iso_date(value.game_date, "game_date")
            - timedelta(days=1)
        ).isoformat()
        for value in lineup_bullpen.games
    }

    if set(statistics_payloads) != required_cutoffs:
        raise ValueError(
            "statistics payload dates must exactly match "
            "required prior-day cutoffs"
        )

    rows_by_cutoff = {}

    for cutoff in sorted(required_cutoffs):
        groups = statistics_payloads[cutoff]
        if not isinstance(groups, Mapping):
            raise TypeError(
                "statistics cutoff values must be mappings"
            )
        if set(groups) != {"hitting", "pitching"}:
            raise ValueError(
                "each cutoff requires hitting and "
                "pitching payloads"
            )

        rows_by_cutoff[cutoff] = {
            "hitting": _rows(
                payload=_mapping(groups["hitting"]),
                role="hitting",
            ),
            "pitching": _rows(
                payload=_mapping(groups["pitching"]),
                role="pitching",
            ),
        }

    snapshots = []

    for game in sorted(
        lineup_bullpen.games,
        key=lambda value: (
            value.game_date,
            value.game_pk,
        ),
    ):
        if not game.ready:
            raise ValueError(
                "lineup and bullpen snapshots must be ready"
            )

        cutoff = (
            _iso_date(game.game_date, "game_date")
            - timedelta(days=1)
        ).isoformat()
        source_rows = rows_by_cutoff[cutoff]
        away_starter, home_starter = (
            normalized_starters[game.game_pk]
        )

        required = {
            ("hitting", player_id)
            for player_id in (
                game.away_lineup_ids
                + game.home_lineup_ids
            )
        }
        required.update(
            {
                ("pitching", player_id)
                for player_id in (
                    (away_starter, home_starter)
                    + game.away_bullpen_ids
                    + game.home_bullpen_ids
                )
            }
        )

        player_records = []

        for role, player_id in sorted(
            required,
            key=lambda value: (
                value[0],
                int(value[1]),
            ),
        ):
            counts = source_rows[role].get(player_id)
            sample_available = counts is not None

            if counts is None:
                keys = (
                    HITTING_STAT_KEYS
                    if role == "hitting"
                    else PITCHING_STAT_KEYS
                )
                counts = tuple(
                    (canonical, 0)
                    for canonical, _ in keys
                )

            player_records.append(
                CanonicalHistoricalProbabilityPlayerStatistics(
                    player_id=player_id,
                    role=role,
                    counts=counts,
                    sample_available=sample_available,
                )
            )

        snapshot_digest = _sha256(
            {
                "game_pk": game.game_pk,
                "game_date": game.game_date,
                "statistics_through_date": cutoff,
                "source_version": (
                    HISTORICAL_PROBABILITY_STATISTICS_SOURCE
                ),
                "players": [
                    {
                        "player_id": value.player_id,
                        "role": value.role,
                        "counts": value.counts,
                        "sample_available": (
                            value.sample_available
                        ),
                    }
                    for value in player_records
                ],
            }
        )

        snapshots.append(
            CanonicalHistoricalProbabilityGameStatistics(
                game_pk=game.game_pk,
                game_date=game.game_date,
                statistics_through_date=cutoff,
                players=tuple(player_records),
                snapshot_digest=snapshot_digest,
            )
        )

    digest = _sha256(
        {
            "schema_version": (
                CANONICAL_HISTORICAL_PROBABILITY_STATISTICS_SOURCE_VERSION
            ),
            "observed_window_digest": (
                lineup_bullpen.observed_window_digest
            ),
            "lineup_bullpen_window_digest": (
                lineup_bullpen.digest
            ),
            "games": [
                {
                    "game_pk": value.game_pk,
                    "game_date": value.game_date,
                    "statistics_through_date": (
                        value.statistics_through_date
                    ),
                    "snapshot_digest": (
                        value.snapshot_digest
                    ),
                }
                for value in snapshots
            ],
        }
    )

    return CanonicalHistoricalProbabilityStatisticsWindow(
        observed_window_digest=(
            lineup_bullpen.observed_window_digest
        ),
        lineup_bullpen_window_digest=(
            lineup_bullpen.digest
        ),
        games=tuple(snapshots),
        digest=digest,
    )
