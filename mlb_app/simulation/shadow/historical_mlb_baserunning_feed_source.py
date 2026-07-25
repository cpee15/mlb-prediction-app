"""Source cutoff-safe pickoff and catcher evidence from MLB feeds."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json
from typing import Any, Dict, Mapping, Sequence, Tuple

from .historical_lineup_bullpen_source import (
    CanonicalHistoricalLineupBullpenWindow,
)


CANONICAL_HISTORICAL_MLB_BASERUNNING_FEED_SOURCE_VERSION = (
    "canonical_historical_mlb_baserunning_feed_source_v1"
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
        person = _mapping(value.get("person"))
        value = (
            person.get("id")
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


def _count(value: Any, name: str) -> int:
    if value in (None, ""):
        return 0
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


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _official_date(
    feed: Mapping[str, Any],
) -> str:
    value = (
        _mapping(
            _mapping(feed.get("gameData"))
            .get("datetime")
        ).get("officialDate")
    )

    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "game feed requires officialDate"
        ) from exc

    if parsed.isoformat() != value:
        raise ValueError(
            "game feed officialDate must use ISO format"
        )

    return value


def _boxscore_teams(
    feed: Mapping[str, Any],
) -> Mapping[str, Any]:
    return _mapping(
        _mapping(
            _mapping(feed.get("liveData"))
            .get("boxscore")
        ).get("teams")
    )


def _team_players(
    feed: Mapping[str, Any],
) -> Tuple[Mapping[str, Any], ...]:
    players = []

    for side in ("away", "home"):
        team = _mapping(
            _boxscore_teams(feed).get(side)
        )

        for player in _mapping(
            team.get("players")
        ).values():
            if isinstance(player, Mapping):
                players.append(player)

    return tuple(players)


def _starting_catcher(
    team: Mapping[str, Any],
    *,
    game_pk: int,
    side: str,
) -> str:
    candidates = []
    starting_slots = []

    for raw_player in _mapping(
        team.get("players")
    ).values():
        player = _mapping(raw_player)
        player_id = _identifier(player)
        all_positions = tuple(
            _mapping(raw_position)
            for raw_position in _sequence(
                player.get("allPositions")
            )
        )
        starting_position = (
            all_positions[0]
            if all_positions
            else _mapping(
                player.get("position")
            )
        )
        catcher_position_recorded = (
            starting_position.get(
                "abbreviation"
            )
            == "C"
        )
        batting_order = str(
            player.get("battingOrder") or ""
        ).strip()

        if (
            batting_order.isdigit()
            and int(batting_order) % 100 == 0
        ):
            starting_slots.append(
                {
                    "player_id": player_id,
                    "batting_order": batting_order,
                    "position": (
                        _mapping(
                            player.get("position")
                        ).get("abbreviation")
                    ),
                    "all_positions": tuple(
                        _mapping(raw_position).get(
                            "abbreviation"
                        )
                        for raw_position in _sequence(
                            player.get("allPositions")
                        )
                    ),
                }
            )

        if (
            player_id is None
            or not catcher_position_recorded
            or not batting_order.isdigit()
        ):
            continue

        numeric_order = int(batting_order)
        if numeric_order % 100 != 0:
            continue

        candidates.append(
            (numeric_order, player_id)
        )

    if len(candidates) != 1:
        raise ValueError(
            "historical game requires exactly one "
            "starting catcher per team; "
            f"game_pk={game_pk}; "
            f"side={side}; "
            f"candidate_count={len(candidates)}; "
            f"starting_slots={starting_slots}"
        )

    return min(candidates)[1]


def _normalize_starters(
    *,
    values: Mapping[int, Tuple[str, str]],
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

        away_id = _identifier(raw_pair[0])
        home_id = _identifier(raw_pair[1])

        if away_id is None or home_id is None:
            raise ValueError(
                "starting pitcher identifiers are required"
            )

        normalized[game_pk] = (
            away_id,
            home_id,
        )

    if set(normalized) != expected:
        raise ValueError(
            "starting pitchers must exactly cover "
            "historical games"
        )

    return normalized


@dataclass(frozen=True)
class CanonicalHistoricalMlbBaserunningFeedEvidence:
    starting_catcher_records: Tuple[
        Tuple[int, str, str],
        ...,
    ]
    pitcher_pickoff_records: Tuple[
        Tuple[str, Tuple[Tuple[str, int], ...]],
        ...,
    ]
    catcher_outcome_records: Tuple[
        Tuple[
            str,
            Tuple[
                Tuple[str, Tuple[int, int]],
                ...,
            ],
        ],
        ...,
    ]
    digest: str
    source_version: str = (
        CANONICAL_HISTORICAL_MLB_BASERUNNING_FEED_SOURCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.starting_catcher_records:
            raise ValueError(
                "starting_catcher_records must not be empty"
            )
        if not self.pitcher_pickoff_records:
            raise ValueError(
                "pitcher_pickoff_records must not be empty"
            )
        if not self.catcher_outcome_records:
            raise ValueError(
                "catcher_outcome_records must not be empty"
            )

        if (
            len(self.digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.digest
            )
        ):
            raise ValueError(
                "digest must be a SHA256 digest"
            )

        if self.source_version != (
            CANONICAL_HISTORICAL_MLB_BASERUNNING_FEED_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported historical MLB "
                "baserunning feed source version"
            )

    @property
    def starting_catcher_ids(
        self,
    ) -> Dict[int, Tuple[str, str]]:
        return {
            game_pk: (away_id, home_id)
            for game_pk, away_id, home_id
            in self.starting_catcher_records
        }

    @property
    def pitcher_pickoffs_by_cutoff(
        self,
    ) -> Dict[str, Dict[str, int]]:
        return {
            cutoff: dict(records)
            for cutoff, records
            in self.pitcher_pickoff_records
        }

    @property
    def catcher_outcomes_by_cutoff(
        self,
    ) -> Dict[
        str,
        Dict[str, Tuple[int, int]],
    ]:
        return {
            cutoff: dict(records)
            for cutoff, records
            in self.catcher_outcome_records
        }

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.source_version,
            "ready": True,
            "game_count": len(
                self.starting_catcher_records
            ),
            "cutoff_count": len(
                self.pitcher_pickoff_records
            ),
            "digest": self.digest,
            "source": "archived_mlb_game_feed_boxscore",
            "starting_catcher_identity_from_target_feed": True,
            "target_game_outcomes_used": False,
            "prior_feed_outcomes_only": True,
            "future_data_permitted": False,
            "historical_replay_executed": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def source_historical_mlb_baserunning_feed_evidence(
    *,
    lineup_bullpen: CanonicalHistoricalLineupBullpenWindow,
    starting_pitcher_ids: Mapping[
        int,
        Tuple[str, str],
    ],
    target_game_feeds: Mapping[
        int,
        Mapping[str, Any],
    ],
    prior_game_feeds_by_cutoff: Mapping[
        str,
        Mapping[int, Mapping[str, Any]],
    ],
) -> CanonicalHistoricalMlbBaserunningFeedEvidence:
    """
    Source catcher identity and prior-game defensive outcomes.

    Target feeds contribute identity only. Pickoff and catcher outcomes are
    aggregated exclusively from feeds whose officialDate is on or before
    the strict prior-day cutoff.
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
    expected_games = set(games_by_id)
    starters = _normalize_starters(
        values=starting_pitcher_ids,
        expected=expected_games,
    )

    if not isinstance(target_game_feeds, Mapping):
        raise TypeError(
            "target_game_feeds must be a mapping"
        )
    if set(target_game_feeds) != expected_games:
        raise ValueError(
            "target game feeds must exactly cover "
            "historical games"
        )

    required_cutoffs = {
        (
            date.fromisoformat(value.game_date)
            - timedelta(days=1)
        ).isoformat()
        for value in lineup_bullpen.games
    }

    if not isinstance(
        prior_game_feeds_by_cutoff,
        Mapping,
    ):
        raise TypeError(
            "prior_game_feeds_by_cutoff must be a mapping"
        )
    if set(prior_game_feeds_by_cutoff) != required_cutoffs:
        raise ValueError(
            "prior game feed cutoffs must exactly match "
            "required prior-day cutoffs"
        )

    catcher_records = []
    catchers_by_game = {}

    for game_pk in sorted(
        expected_games,
        key=lambda value: (
            games_by_id[value].game_date,
            value,
        ),
    ):
        feed = target_game_feeds[game_pk]

        if not isinstance(feed, Mapping):
            raise TypeError(
                "target game feed values must be mappings"
            )
        if _official_date(feed) != (
            games_by_id[game_pk].game_date
        ):
            raise ValueError(
                "target game feed officialDate must "
                "match historical game_date"
            )

        teams = _boxscore_teams(feed)
        away_catcher = _starting_catcher(
            _mapping(teams.get("away")),
            game_pk=game_pk,
            side="away",
        )
        home_catcher = _starting_catcher(
            _mapping(teams.get("home")),
            game_pk=game_pk,
            side="home",
        )

        catchers_by_game[game_pk] = (
            away_catcher,
            home_catcher,
        )
        catcher_records.append(
            (
                game_pk,
                away_catcher,
                home_catcher,
            )
        )

    pickoff_records = []
    outcome_records = []

    for cutoff in sorted(required_cutoffs):
        feeds = prior_game_feeds_by_cutoff[cutoff]

        if not isinstance(feeds, Mapping):
            raise TypeError(
                "prior cutoff values must be mappings"
            )

        pitcher_totals: Dict[str, int] = {}
        catcher_totals: Dict[
            str,
            Tuple[int, int],
        ] = {}

        for raw_game_pk, feed in sorted(
            feeds.items(),
            key=lambda value: int(value[0]),
        ):
            try:
                game_pk = int(raw_game_pk)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "prior game feed identifiers "
                    "must be integers"
                ) from exc

            if game_pk <= 0:
                raise ValueError(
                    "prior game feed identifiers "
                    "must be positive"
                )
            if not isinstance(feed, Mapping):
                raise TypeError(
                    "prior game feed values "
                    "must be mappings"
                )
            if date.fromisoformat(
                _official_date(feed)
            ) > date.fromisoformat(cutoff):
                raise ValueError(
                    "prior game feeds cannot exceed cutoff"
                )

            for player in _team_players(feed):
                player_id = _identifier(player)

                if player_id is None:
                    continue

                stats = _mapping(
                    player.get("stats")
                )
                pitching = _mapping(
                    stats.get("pitching")
                )
                fielding = _mapping(
                    stats.get("fielding")
                )
                position = _mapping(
                    player.get("position")
                )

                if pitching:
                    pitcher_totals[player_id] = (
                        pitcher_totals.get(
                            player_id,
                            0,
                        )
                        + _count(
                            pitching.get("pickoffs"),
                            "pitching.pickoffs",
                        )
                    )

                if (
                    position.get("abbreviation") == "C"
                    and fielding
                ):
                    prior_sb, prior_cs = (
                        catcher_totals.get(
                            player_id,
                            (0, 0),
                        )
                    )
                    catcher_totals[player_id] = (
                        prior_sb
                        + _count(
                            fielding.get("stolenBases"),
                            "fielding.stolenBases",
                        ),
                        prior_cs
                        + _count(
                            fielding.get("caughtStealing"),
                            "fielding.caughtStealing",
                        ),
                    )

        games_at_cutoff = tuple(
            game
            for game in lineup_bullpen.games
            if (
                date.fromisoformat(game.game_date)
                - timedelta(days=1)
            ).isoformat() == cutoff
        )

        required_pitchers = set()
        required_catchers = set()

        for game in games_at_cutoff:
            away_starter, home_starter = (
                starters[game.game_pk]
            )
            required_pitchers.update(
                (
                    away_starter,
                    home_starter,
                )
                + game.away_bullpen_ids
                + game.home_bullpen_ids
            )
            required_catchers.update(
                catchers_by_game[game.game_pk]
            )

        for pitcher_id in required_pitchers:
            pitcher_totals.setdefault(
                pitcher_id,
                0,
            )
        for catcher_id in required_catchers:
            catcher_totals.setdefault(
                catcher_id,
                (0, 0),
            )

        pickoff_records.append(
            (
                cutoff,
                tuple(
                    sorted(
                        pitcher_totals.items(),
                        key=lambda value: int(value[0]),
                    )
                ),
            )
        )
        outcome_records.append(
            (
                cutoff,
                tuple(
                    sorted(
                        catcher_totals.items(),
                        key=lambda value: int(value[0]),
                    )
                ),
            )
        )

    canonical = {
        "schema_version": (
            CANONICAL_HISTORICAL_MLB_BASERUNNING_FEED_SOURCE_VERSION
        ),
        "starting_catchers": catcher_records,
        "pitcher_pickoffs": pickoff_records,
        "catcher_outcomes": outcome_records,
        "target_game_outcomes_used": False,
    }

    return CanonicalHistoricalMlbBaserunningFeedEvidence(
        starting_catcher_records=tuple(
            catcher_records
        ),
        pitcher_pickoff_records=tuple(
            pickoff_records
        ),
        catcher_outcome_records=tuple(
            outcome_records
        ),
        digest=_sha256(canonical),
    )
