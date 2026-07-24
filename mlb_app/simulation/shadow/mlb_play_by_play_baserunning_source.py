"""Source complete observed baserunning from MLB play-by-play."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Mapping, Tuple


CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION = (
    "canonical_mlb_play_by_play_baserunning_source_v1"
)

_FINAL_ABSTRACT_STATES = {
    "Final",
}


def _mapping(
    value: Any,
    field_name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(
            f"{field_name} must be a mapping"
        )
    return value


def _list(
    value: Any,
    field_name: str,
) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError(
            f"{field_name} must be a list"
        )
    return value


def _positive_game_pk(value: Any) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
    ):
        raise ValueError(
            "gamePk must be a positive integer"
        )
    return value


def _runner_id(
    details: Mapping[str, Any],
) -> str:
    runner = _mapping(
        details.get("runner"),
        "runner.details.runner",
    )
    value = runner.get("id")

    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
    ):
        raise ValueError(
            "runner identity must be available"
        )

    return str(value)


def _event_type(
    *,
    play: Mapping[str, Any],
    runner: Mapping[str, Any],
) -> str:
    details = _mapping(
        runner.get("details"),
        "runner.details",
    )
    value = details.get("eventType")

    if value in (None, ""):
        result = _mapping(
            play.get("result", {}),
            "play.result",
        )
        value = result.get("eventType", "")

    return str(value).strip().lower()


def _classify_event(event_type: str) -> str | None:
    normalized = event_type.replace("-", "_")

    if normalized.startswith("stolen_base"):
        return "stolen_base"

    if "caught_stealing" in normalized:
        return "caught_stealing"

    return None


@dataclass(frozen=True)
class CanonicalMlbPlayByPlayBaserunningGame:
    game_pk: int
    game_date: str
    stolen_bases: int
    caught_stealing: int

    def __post_init__(self) -> None:
        if self.game_pk <= 0:
            raise ValueError(
                "game_pk must be positive"
            )

        parsed_date = date.fromisoformat(
            self.game_date
        )
        if parsed_date.isoformat() != self.game_date:
            raise ValueError(
                "game_date must use ISO format"
            )

        for field_name in (
            "stolen_bases",
            "caught_stealing",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(
                    f"{field_name} must be nonnegative"
                )


@dataclass(frozen=True)
class CanonicalMlbPlayByPlayBaserunningSnapshot:
    window_start: str
    window_end: str
    games: Tuple[
        CanonicalMlbPlayByPlayBaserunningGame,
        ...,
    ]
    event_count: int
    stolen_bases: int
    caught_stealing: int
    duplicate_event_record_count: int
    digest: str
    source_version: str = (
        CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.games:
            raise ValueError(
                "games must contain completed games"
            )

        game_ids = tuple(
            value.game_pk
            for value in self.games
        )
        if len(game_ids) != len(set(game_ids)):
            raise ValueError(
                "completed game identifiers must be unique"
            )

        if self.duplicate_event_record_count < 0:
            raise ValueError(
                "duplicate_event_record_count "
                "must be nonnegative"
            )

        if self.event_count != (
            self.stolen_bases
            + self.caught_stealing
        ):
            raise ValueError(
                "event_count must equal SB plus CS"
            )

        if len(self.digest) != 64:
            raise ValueError(
                "digest must be a SHA-256 hex digest"
            )

        if self.source_version != (
            CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported MLB play-by-play "
                "baserunning source version"
            )

    @property
    def game_count(self) -> int:
        return len(self.games)

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.source_version,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "game_count": self.game_count,
            "event_count": self.event_count,
            "stolen_bases": self.stolen_bases,
            "caught_stealing": self.caught_stealing,
            "duplicate_event_record_count": (
                self.duplicate_event_record_count
            ),
            "digest": self.digest,
            "coverage_complete": True,
            "calibration_observed_source_eligible": True,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def _completed_games(
    *,
    schedule: Mapping[str, Any],
    start_date: date,
    end_date: date,
) -> Tuple[Tuple[int, str], ...]:
    games_by_id = {}

    for date_row in _list(
        schedule.get("dates"),
        "schedule.dates",
    ):
        date_item = _mapping(
            date_row,
            "schedule date",
        )
        schedule_date = str(
            date_item.get("date", "")
        )
        date.fromisoformat(
            schedule_date
        )

        for game in _list(
            date_item.get("games"),
            "schedule games",
        ):
            game_item = _mapping(
                game,
                "schedule game",
            )
            status = _mapping(
                game_item.get("status"),
                "schedule game status",
            )

            if status.get(
                "abstractGameState"
            ) not in _FINAL_ABSTRACT_STATES:
                continue

            game_pk = _positive_game_pk(
                game_item.get("gamePk")
            )
            game_date = str(
                game_item.get(
                    "officialDate",
                    schedule_date,
                )
            )
            parsed_game_date = date.fromisoformat(
                game_date
            )

            if not (
                start_date
                <= parsed_game_date
                <= end_date
            ):
                raise ValueError(
                    "completed game officialDate "
                    "must fall within source window"
                )

            existing_date = games_by_id.get(
                game_pk
            )
            if (
                existing_date is not None
                and existing_date != game_date
            ):
                raise ValueError(
                    "gamePk must map to one officialDate"
                )

            games_by_id[game_pk] = game_date

    if not games_by_id:
        raise ValueError(
            "schedule contains no completed games"
        )

    return tuple(
        sorted(
            games_by_id.items(),
            key=lambda value: (
                value[1],
                value[0],
            ),
        )
    )

def source_mlb_play_by_play_baserunning_window(
    *,
    schedule: Mapping[str, Any],
    game_feeds: Mapping[int, Mapping[str, Any]],
    window_start: str,
    window_end: str,
) -> CanonicalMlbPlayByPlayBaserunningSnapshot:
    """
    Source complete observed SB/CS events from MLB play-by-play feeds.

    The caller owns network access. This function validates completed-game
    coverage and creates deterministic per-game totals and provenance.
    """

    schedule = _mapping(
        schedule,
        "schedule",
    )
    game_feeds = _mapping(
        game_feeds,
        "game_feeds",
    )

    start_date = date.fromisoformat(
        window_start
    )
    end_date = date.fromisoformat(
        window_end
    )

    if end_date < start_date:
        raise ValueError(
            "window_end must not precede window_start"
        )

    completed_games = _completed_games(
        schedule=schedule,
        start_date=start_date,
        end_date=end_date,
    )
    completed_ids = {
        game_pk
        for game_pk, _ in completed_games
    }

    if set(game_feeds) != completed_ids:
        raise ValueError(
            "game_feeds must exactly cover "
            "completed schedule games"
        )

    game_records = []
    event_identities = []
    seen_event_identities = set()
    duplicate_event_record_count = 0

    for game_pk, game_date in completed_games:
        feed = _mapping(
            game_feeds[game_pk],
            "game feed",
        )
        live_data = _mapping(
            feed.get("liveData"),
            "game feed liveData",
        )
        plays = _mapping(
            live_data.get("plays"),
            "game feed plays",
        )
        all_plays = _list(
            plays.get("allPlays"),
            "game feed allPlays",
        )

        stolen_bases = 0
        caught_stealing = 0

        for fallback_index, play in enumerate(
            all_plays
        ):
            play_item = _mapping(
                play,
                "play",
            )
            about = _mapping(
                play_item.get("about", {}),
                "play.about",
            )
            play_index = int(
                about.get(
                    "atBatIndex",
                    fallback_index,
                )
            )

            for runner in _list(
                play_item.get("runners", []),
                "play runners",
            ):
                runner_item = _mapping(
                    runner,
                    "runner",
                )
                details = _mapping(
                    runner_item.get("details"),
                    "runner.details",
                )
                classification = _classify_event(
                    _event_type(
                        play=play_item,
                        runner=runner_item,
                    )
                )

                if classification is None:
                    continue

                identity = (
                    game_pk,
                    play_index,
                    _runner_id(details),
                    classification,
                )
                if identity in seen_event_identities:
                    duplicate_event_record_count += 1
                    continue

                seen_event_identities.add(identity)
                event_identities.append(identity)

                if classification == "stolen_base":
                    stolen_bases += 1
                else:
                    caught_stealing += 1

        game_records.append(
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=game_pk,
                game_date=game_date,
                stolen_bases=stolen_bases,
                caught_stealing=caught_stealing,
            )
        )

    serialized = json.dumps(
        {
            "window_start": start_date.isoformat(),
            "window_end": end_date.isoformat(),
            "games": [
                {
                    "game_pk": value.game_pk,
                    "game_date": value.game_date,
                    "stolen_bases": value.stolen_bases,
                    "caught_stealing": (
                        value.caught_stealing
                    ),
                }
                for value in game_records
            ],
            "events": event_identities,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()

    stolen_base_total = sum(
        value.stolen_bases
        for value in game_records
    )
    caught_stealing_total = sum(
        value.caught_stealing
        for value in game_records
    )

    return CanonicalMlbPlayByPlayBaserunningSnapshot(
        window_start=start_date.isoformat(),
        window_end=end_date.isoformat(),
        games=tuple(game_records),
        event_count=(
            stolen_base_total
            + caught_stealing_total
        ),
        stolen_bases=stolen_base_total,
        caught_stealing=caught_stealing_total,
        duplicate_event_record_count=(
            duplicate_event_record_count
        ),
        digest=digest,
    )
