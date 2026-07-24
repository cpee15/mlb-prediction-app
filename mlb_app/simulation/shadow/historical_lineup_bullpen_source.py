"""Source immutable historical lineup and bullpen snapshots."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from .historical_shadow_replay_input_audit import (
    HISTORICAL_BULLPEN_SOURCE,
    HISTORICAL_LINEUP_SOURCE,
    CanonicalHistoricalShadowReplayInputEvidence,
)
from .mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)


CANONICAL_HISTORICAL_LINEUP_BULLPEN_SOURCE_VERSION = (
    "canonical_historical_lineup_bullpen_source_v1"
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


def _identifier(value: Any) -> Optional[str]:
    if value in (None, "") or isinstance(value, bool):
        return None

    if isinstance(value, Mapping):
        person = _mapping(value.get("person"))
        value = (
            person.get("id")
            or value.get("id")
            or value.get("player_id")
        )

    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None

    return str(parsed) if parsed > 0 else None


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _starting_lineup(
    team: Mapping[str, Any],
) -> Tuple[str, ...]:
    players = _mapping(team.get("players"))
    slots: Dict[int, str] = {}

    for raw_player in players.values():
        player = _mapping(raw_player)
        player_id = _identifier(player)
        batting_order = str(
            player.get("battingOrder") or ""
        ).strip()

        if player_id is None or not batting_order.isdigit():
            continue

        numeric_order = int(batting_order)
        if numeric_order % 100 != 0:
            continue

        slot = numeric_order // 100
        if slot < 1 or slot > 9:
            continue

        if (
            slot in slots
            and slots[slot] != player_id
        ):
            return ()

        slots[slot] = player_id

    if set(slots) == set(range(1, 10)):
        return tuple(
            slots[slot]
            for slot in range(1, 10)
        )

    explicit = tuple(
        value
        for value in (
            _identifier(raw)
            for raw in _sequence(
                team.get("battingOrder")
            )
        )
        if value is not None
    )

    if (
        len(explicit) == 9
        and len(set(explicit)) == 9
    ):
        return explicit

    return ()


def _historical_bullpen(
    team: Mapping[str, Any],
) -> Tuple[str, ...]:
    explicit = _sequence(team.get("bullpen"))
    pitcher_order = tuple(
        value
        for value in (
            _identifier(raw)
            for raw in _sequence(
                team.get("pitchers")
            )
        )
        if value is not None
    )
    starter_id = (
        pitcher_order[0]
        if pitcher_order
        else None
    )

    bullpen_ids = {
        value
        for value in (
            _identifier(raw)
            for raw in explicit
        )
        if (
            value is not None
            and value != starter_id
        )
    }

    return tuple(
        sorted(
            bullpen_ids,
            key=int,
        )
    )


@dataclass(frozen=True)
class CanonicalHistoricalLineupBullpenGameSnapshot:
    game_pk: int
    game_date: str
    away_lineup_ids: Tuple[str, ...]
    home_lineup_ids: Tuple[str, ...]
    away_bullpen_ids: Tuple[str, ...]
    home_bullpen_ids: Tuple[str, ...]
    lineup_digest: Optional[str]
    bullpen_digest: Optional[str]

    @property
    def lineups_ready(self) -> bool:
        return (
            len(self.away_lineup_ids) == 9
            and len(self.home_lineup_ids) == 9
            and len(set(self.away_lineup_ids)) == 9
            and len(set(self.home_lineup_ids)) == 9
        )

    @property
    def bullpens_ready(self) -> bool:
        return bool(
            self.away_bullpen_ids
            and self.home_bullpen_ids
        )

    @property
    def ready(self) -> bool:
        return (
            self.lineups_ready
            and self.bullpens_ready
            and self.lineup_digest is not None
            and self.bullpen_digest is not None
        )

    @property
    def status(self) -> str:
        if self.ready:
            return "ready"
        if self.lineups_ready or self.bullpens_ready:
            return "partial"
        return "unavailable"

    def to_replay_input_evidence(
        self,
        *,
        probability_provider_identity: Optional[str] = None,
        exact_artifact_digest: Optional[str] = None,
        fallback_catalog_digest: Optional[str] = None,
        baserunning_catalog_digest: Optional[str] = None,
    ) -> CanonicalHistoricalShadowReplayInputEvidence:
        return CanonicalHistoricalShadowReplayInputEvidence(
            game_pk=self.game_pk,
            game_date=self.game_date,
            lineup_source=(
                HISTORICAL_LINEUP_SOURCE
                if self.lineups_ready
                else "unavailable"
            ),
            lineup_snapshot_digest=(
                self.lineup_digest
                if self.lineups_ready
                else None
            ),
            bullpen_source=(
                HISTORICAL_BULLPEN_SOURCE
                if self.bullpens_ready
                else "unavailable"
            ),
            bullpen_snapshot_digest=(
                self.bullpen_digest
                if self.bullpens_ready
                else None
            ),
            probability_provider_identity=(
                probability_provider_identity
            ),
            exact_artifact_digest=(
                exact_artifact_digest
            ),
            fallback_catalog_digest=(
                fallback_catalog_digest
            ),
            baserunning_catalog_digest=(
                baserunning_catalog_digest
            ),
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "status": self.status,
            "ready": self.ready,
            "lineups_ready": self.lineups_ready,
            "bullpens_ready": self.bullpens_ready,
            "away_lineup_count": len(
                self.away_lineup_ids
            ),
            "home_lineup_count": len(
                self.home_lineup_ids
            ),
            "away_bullpen_count": len(
                self.away_bullpen_ids
            ),
            "home_bullpen_count": len(
                self.home_bullpen_ids
            ),
            "lineup_digest": self.lineup_digest,
            "bullpen_digest": self.bullpen_digest,
            "player_identifiers_exposed": False,
            "current_active_roster_used": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalHistoricalLineupBullpenWindow:
    observed_window_digest: str
    games: Tuple[
        CanonicalHistoricalLineupBullpenGameSnapshot,
        ...,
    ]
    digest: str
    source_version: str = (
        CANONICAL_HISTORICAL_LINEUP_BULLPEN_SOURCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.games:
            raise ValueError(
                "games must contain historical snapshots"
            )
        if self.source_version != (
            CANONICAL_HISTORICAL_LINEUP_BULLPEN_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported historical lineup-bullpen "
                "source version"
            )

    @property
    def game_count(self) -> int:
        return len(self.games)

    @property
    def ready_game_count(self) -> int:
        return sum(
            value.ready
            for value in self.games
        )

    @property
    def ready(self) -> bool:
        return self.ready_game_count == self.game_count

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.source_version,
            "ready": self.ready,
            "game_count": self.game_count,
            "ready_game_count": self.ready_game_count,
            "blocked_game_count": (
                self.game_count
                - self.ready_game_count
            ),
            "observed_window_digest": (
                self.observed_window_digest
            ),
            "digest": self.digest,
            "games": tuple(
                value.to_diagnostics()
                for value in self.games
            ),
            "source": "archived_mlb_game_feed_boxscore",
            "current_active_roster_used": False,
            "historical_replay_executed": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def source_historical_lineup_bullpen_snapshots(
    *,
    observed: CanonicalMlbPlayByPlayBaserunningSnapshot,
    game_feeds: Mapping[int, Mapping[str, Any]],
) -> CanonicalHistoricalLineupBullpenWindow:
    """
    Decode historical game-feed boxscores without current-roster fallback.

    Missing lineups or explicit bullpen arrays remain unavailable. Used
    pitchers are not substituted for a missing historical bullpen roster.
    """

    if not isinstance(
        observed,
        CanonicalMlbPlayByPlayBaserunningSnapshot,
    ):
        raise TypeError(
            "observed must be "
            "CanonicalMlbPlayByPlayBaserunningSnapshot"
        )

    if not isinstance(game_feeds, Mapping):
        raise TypeError("game_feeds must be a mapping")

    normalized_feeds: Dict[int, Mapping[str, Any]] = {}
    for raw_game_pk, raw_feed in game_feeds.items():
        try:
            game_pk = int(raw_game_pk)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "game feed identifiers must be integers"
            ) from exc

        if game_pk in normalized_feeds:
            raise ValueError(
                "game feed identifiers must be unique"
            )
        if not isinstance(raw_feed, Mapping):
            raise TypeError(
                "game feed values must be mappings"
            )

        normalized_feeds[game_pk] = raw_feed

    observed_by_id = {
        value.game_pk: value
        for value in observed.games
    }

    if set(normalized_feeds) != set(observed_by_id):
        raise ValueError(
            "historical game feeds must exactly match "
            "observed play-by-play games"
        )

    snapshots = []

    for game_pk, observed_game in sorted(
        observed_by_id.items(),
        key=lambda item: (
            item[1].game_date,
            item[0],
        ),
    ):
        feed = normalized_feeds[game_pk]
        game_data = _mapping(feed.get("gameData"))
        datetime_data = _mapping(
            game_data.get("datetime")
        )
        official_date = str(
            datetime_data.get("officialDate") or ""
        ).strip()

        if official_date != observed_game.game_date:
            raise ValueError(
                "historical game feed officialDate must "
                "match observed game_date"
            )

        live_data = _mapping(feed.get("liveData"))
        boxscore = _mapping(
            live_data.get("boxscore")
        )
        teams = _mapping(boxscore.get("teams"))
        away = _mapping(teams.get("away"))
        home = _mapping(teams.get("home"))

        away_lineup = _starting_lineup(away)
        home_lineup = _starting_lineup(home)
        away_bullpen = _historical_bullpen(away)
        home_bullpen = _historical_bullpen(home)

        lineups_ready = (
            len(away_lineup) == 9
            and len(home_lineup) == 9
        )
        bullpens_ready = bool(
            away_bullpen
            and home_bullpen
        )

        lineup_digest = (
            _sha256(
                {
                    "game_pk": game_pk,
                    "game_date": observed_game.game_date,
                    "away": away_lineup,
                    "home": home_lineup,
                }
            )
            if lineups_ready
            else None
        )
        bullpen_digest = (
            _sha256(
                {
                    "game_pk": game_pk,
                    "game_date": observed_game.game_date,
                    "away": away_bullpen,
                    "home": home_bullpen,
                }
            )
            if bullpens_ready
            else None
        )

        snapshots.append(
            CanonicalHistoricalLineupBullpenGameSnapshot(
                game_pk=game_pk,
                game_date=observed_game.game_date,
                away_lineup_ids=away_lineup,
                home_lineup_ids=home_lineup,
                away_bullpen_ids=away_bullpen,
                home_bullpen_ids=home_bullpen,
                lineup_digest=lineup_digest,
                bullpen_digest=bullpen_digest,
            )
        )

    digest = _sha256(
        {
            "observed_window_digest": observed.digest,
            "games": [
                {
                    "game_pk": value.game_pk,
                    "game_date": value.game_date,
                    "lineup_digest": value.lineup_digest,
                    "bullpen_digest": value.bullpen_digest,
                    "status": value.status,
                }
                for value in snapshots
            ],
        }
    )

    return CanonicalHistoricalLineupBullpenWindow(
        observed_window_digest=observed.digest,
        games=tuple(snapshots),
        digest=digest,
    )
