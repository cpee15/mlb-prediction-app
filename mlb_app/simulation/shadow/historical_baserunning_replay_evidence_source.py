"""Source deterministic historical baserunning replay evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json
from typing import Any, Dict, Mapping, Tuple

from mlb_app.simulation.game import (
    CanonicalBaserunningEvidenceCatalog,
)

from .historical_lineup_bullpen_source import (
    CanonicalHistoricalLineupBullpenWindow,
)


CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVIDENCE_VERSION = (
    "canonical_historical_baserunning_replay_evidence_v1"
)
HISTORICAL_BASERUNNING_CALIBRATION_PROXY_POLICY = (
    "historical_baserunning_calibration_proxy_v1"
)
HISTORICAL_BASERUNNING_EVIDENCE_QUALITY = "calibration_only"


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _iso_date(value: str, name: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be an ISO date"
        ) from exc

    if parsed.isoformat() != value:
        raise ValueError(
            f"{name} must use ISO format"
        )

    return parsed


def _digest(value: str, name: str) -> None:
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
class CanonicalHistoricalBaserunningReplayEvidenceGame:
    game_pk: int
    game_date: str
    statistics_through_date: str
    catalog: CanonicalBaserunningEvidenceCatalog
    evidence_digest: str
    direct_evidence_count: int
    proxy_evidence_count: int
    fallback_evidence_count: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.game_pk, int)
            or isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be a positive integer"
            )

        game_date = _iso_date(
            self.game_date,
            "game_date",
        )
        cutoff = _iso_date(
            self.statistics_through_date,
            "statistics_through_date",
        )

        if cutoff != game_date - timedelta(days=1):
            raise ValueError(
                "statistics_through_date must be the "
                "strict previous calendar date"
            )

        if not isinstance(
            self.catalog,
            CanonicalBaserunningEvidenceCatalog,
        ):
            raise TypeError(
                "catalog must be "
                "CanonicalBaserunningEvidenceCatalog"
            )

        _digest(
            self.evidence_digest,
            "evidence_digest",
        )

        for name, value in (
            (
                "direct_evidence_count",
                self.direct_evidence_count,
            ),
            (
                "proxy_evidence_count",
                self.proxy_evidence_count,
            ),
            (
                "fallback_evidence_count",
                self.fallback_evidence_count,
            ),
        ):
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(
                    f"{name} must be a nonnegative integer"
                )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "statistics_through_date": (
                self.statistics_through_date
            ),
            "catalog_digest": self.catalog.digest,
            "evidence_digest": self.evidence_digest,
            "direct_evidence_count": (
                self.direct_evidence_count
            ),
            "proxy_evidence_count": (
                self.proxy_evidence_count
            ),
            "fallback_evidence_count": (
                self.fallback_evidence_count
            ),
            "evidence_quality": (
                HISTORICAL_BASERUNNING_EVIDENCE_QUALITY
            ),
            "tracking_proxy_policy": (
                HISTORICAL_BASERUNNING_CALIBRATION_PROXY_POLICY
            ),
            "target_game_outcomes_used": False,
            "future_data_permitted": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningReplayEvidenceWindow:
    observed_window_digest: str
    lineup_bullpen_window_digest: str
    games: Tuple[
        CanonicalHistoricalBaserunningReplayEvidenceGame,
        ...,
    ]
    digest: str
    source_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVIDENCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.games:
            raise ValueError(
                "games must contain replay evidence"
            )

        identities = tuple(
            value.game_pk
            for value in self.games
        )
        if len(identities) != len(set(identities)):
            raise ValueError(
                "evidence game identifiers must be unique"
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
            _digest(value, name)

        if self.source_version != (
            CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVIDENCE_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "replay evidence version"
            )

    @property
    def game_count(self) -> int:
        return len(self.games)

    @property
    def ready(self) -> bool:
        return bool(self.games)

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.source_version,
            "ready": self.ready,
            "game_count": self.game_count,
            "ready_game_count": self.game_count,
            "direct_evidence_count": sum(
                value.direct_evidence_count
                for value in self.games
            ),
            "proxy_evidence_count": sum(
                value.proxy_evidence_count
                for value in self.games
            ),
            "fallback_evidence_count": sum(
                value.fallback_evidence_count
                for value in self.games
            ),
            "observed_window_digest": (
                self.observed_window_digest
            ),
            "lineup_bullpen_window_digest": (
                self.lineup_bullpen_window_digest
            ),
            "evidence_window_digest": self.digest,
            "evidence_quality": (
                HISTORICAL_BASERUNNING_EVIDENCE_QUALITY
            ),
            "tracking_proxy_policy": (
                HISTORICAL_BASERUNNING_CALIBRATION_PROXY_POLICY
            ),
            "statistics_cutoff_policy": (
                "strict_previous_calendar_date"
            ),
            "target_game_outcomes_used": False,
            "future_data_permitted": False,
            "historical_replay_executed": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def source_historical_baserunning_replay_evidence(
    *,
    lineup_bullpen: CanonicalHistoricalLineupBullpenWindow,
    catalogs: Mapping[
        int,
        CanonicalBaserunningEvidenceCatalog,
    ],
    statistics_through_dates: Mapping[int, str],
    evidence_counts: Mapping[
        int,
        Tuple[int, int, int],
    ],
) -> CanonicalHistoricalBaserunningReplayEvidenceWindow:
    """
    Attach complete cutoff-safe catalogs to historical games.

    Catalog construction remains separate from this alignment boundary.
    Every game must be covered exactly. Target-game outcomes are never
    accepted by this API.
    """

    if not isinstance(
        lineup_bullpen,
        CanonicalHistoricalLineupBullpenWindow,
    ):
        raise TypeError(
            "lineup_bullpen must be a "
            "CanonicalHistoricalLineupBullpenWindow"
        )

    games_by_id = {
        value.game_pk: value
        for value in lineup_bullpen.games
    }

    expected = set(games_by_id)
    if (
        set(catalogs) != expected
        or set(statistics_through_dates) != expected
        or set(evidence_counts) != expected
    ):
        raise ValueError(
            "historical baserunning evidence inputs "
            "must exactly cover lineup-bullpen games"
        )

    games = []

    for game_pk in sorted(
        expected,
        key=lambda value: (
            games_by_id[value].game_date,
            value,
        ),
    ):
        source_game = games_by_id[game_pk]
        catalog = catalogs[game_pk]
        cutoff = statistics_through_dates[game_pk]
        counts = evidence_counts[game_pk]

        if (
            not isinstance(counts, tuple)
            or len(counts) != 3
        ):
            raise TypeError(
                "evidence counts must be "
                "direct-proxy-fallback tuples"
            )

        evidence_digest = _sha256(
            {
                "game_pk": game_pk,
                "game_date": source_game.game_date,
                "statistics_through_date": cutoff,
                "catalog_digest": catalog.digest,
                "direct_evidence_count": counts[0],
                "proxy_evidence_count": counts[1],
                "fallback_evidence_count": counts[2],
                "evidence_quality": (
                    HISTORICAL_BASERUNNING_EVIDENCE_QUALITY
                ),
                "tracking_proxy_policy": (
                    HISTORICAL_BASERUNNING_CALIBRATION_PROXY_POLICY
                ),
            }
        )

        games.append(
            CanonicalHistoricalBaserunningReplayEvidenceGame(
                game_pk=game_pk,
                game_date=source_game.game_date,
                statistics_through_date=cutoff,
                catalog=catalog,
                evidence_digest=evidence_digest,
                direct_evidence_count=counts[0],
                proxy_evidence_count=counts[1],
                fallback_evidence_count=counts[2],
            )
        )

    digest = _sha256(
        {
            "schema_version": (
                CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVIDENCE_VERSION
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
                    "evidence_digest": value.evidence_digest,
                }
                for value in games
            ],
        }
    )

    return CanonicalHistoricalBaserunningReplayEvidenceWindow(
        observed_window_digest=(
            lineup_bullpen.observed_window_digest
        ),
        lineup_bullpen_window_digest=(
            lineup_bullpen.digest
        ),
        games=tuple(games),
        digest=digest,
    )
