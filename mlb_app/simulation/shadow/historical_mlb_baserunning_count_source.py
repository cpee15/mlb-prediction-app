"""Source cutoff-safe MLB baserunning counts for historical replay."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, Mapping, Sequence, Tuple

from .historical_baserunning_profile_materialization import (
    CanonicalHistoricalCatcherBaserunningCounts,
    CanonicalHistoricalPitcherBaserunningCounts,
    CanonicalHistoricalRunnerBaserunningCounts,
    materialize_historical_baserunning_profiles,
)
from .historical_baserunning_replay_evidence_source import (
    CanonicalHistoricalBaserunningReplayEvidenceWindow,
    source_historical_baserunning_replay_evidence,
)
from .historical_lineup_bullpen_source import (
    CanonicalHistoricalLineupBullpenWindow,
)


CANONICAL_HISTORICAL_MLB_BASERUNNING_COUNT_SOURCE_VERSION = (
    "canonical_historical_mlb_baserunning_count_source_v1"
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


def _statistics_rows(
    payload: Mapping[str, Any],
) -> Dict[str, Mapping[str, Any]]:
    rows: Dict[str, Mapping[str, Any]] = {}

    for block in _sequence(payload.get("stats")):
        for split in _sequence(
            _mapping(block).get("splits")
        ):
            split = _mapping(split)
            player_id = _identifier(
                split.get("player")
            )

            if player_id is None:
                raise ValueError(
                    "statistics split requires player identity"
                )
            if player_id in rows:
                raise ValueError(
                    "statistics player identities "
                    "must be unique"
                )

            rows[player_id] = _mapping(
                split.get("stat")
            )

    return rows


def _normalize_game_pairs(
    *,
    values: Mapping[int, Tuple[str, str]],
    expected_game_ids: set[int],
    name: str,
) -> Dict[int, Tuple[str, str]]:
    normalized: Dict[int, Tuple[str, str]] = {}

    if not isinstance(values, Mapping):
        raise TypeError(f"{name} must be a mapping")

    for raw_game_pk, raw_pair in values.items():
        try:
            game_pk = int(raw_game_pk)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{name} game identifiers must be integers"
            ) from exc

        if (
            not isinstance(raw_pair, tuple)
            or len(raw_pair) != 2
        ):
            raise TypeError(
                f"{name} values must be away-home tuples"
            )

        away_id = _identifier(raw_pair[0])
        home_id = _identifier(raw_pair[1])

        if away_id is None or home_id is None:
            raise ValueError(
                f"{name} identifiers are required"
            )

        normalized[game_pk] = (
            away_id,
            home_id,
        )

    if set(normalized) != expected_game_ids:
        raise ValueError(
            f"{name} must exactly cover historical games"
        )

    return normalized


def source_historical_mlb_baserunning_counts(
    *,
    lineup_bullpen: CanonicalHistoricalLineupBullpenWindow,
    starting_pitcher_ids: Mapping[
        int,
        Tuple[str, str],
    ],
    starting_catcher_ids: Mapping[
        int,
        Tuple[str, str],
    ],
    statistics_payloads: Mapping[
        str,
        Mapping[str, Mapping[str, Any]],
    ],
    pitcher_pickoffs_by_cutoff: Mapping[
        str,
        Mapping[str, int],
    ],
    catcher_outcomes_by_cutoff: Mapping[
        str,
        Mapping[str, Tuple[int, int]],
    ],
) -> CanonicalHistoricalBaserunningReplayEvidenceWindow:
    """
    Decode direct MLB counts and build complete historical catalogs.

    Statistics, pickoffs, and catcher outcomes must stop on the strict
    previous calendar date. Target-game outcomes are not accepted.
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
    expected_game_ids = set(games_by_id)

    starters = _normalize_game_pairs(
        values=starting_pitcher_ids,
        expected_game_ids=expected_game_ids,
        name="starting pitchers",
    )
    catchers = _normalize_game_pairs(
        values=starting_catcher_ids,
        expected_game_ids=expected_game_ids,
        name="starting catchers",
    )

    required_cutoffs = {
        (
            date.fromisoformat(value.game_date)
            - timedelta(days=1)
        ).isoformat()
        for value in lineup_bullpen.games
    }

    for name, values in (
        ("statistics payloads", statistics_payloads),
        (
            "pitcher pickoffs by cutoff",
            pitcher_pickoffs_by_cutoff,
        ),
        (
            "catcher outcomes by cutoff",
            catcher_outcomes_by_cutoff,
        ),
    ):
        if not isinstance(values, Mapping):
            raise TypeError(f"{name} must be a mapping")
        if set(values) != required_cutoffs:
            raise ValueError(
                f"{name} must exactly match "
                "required prior-day cutoffs"
            )

    rows_by_cutoff = {}

    for cutoff in sorted(required_cutoffs):
        groups = statistics_payloads[cutoff]

        if (
            not isinstance(groups, Mapping)
            or set(groups) != {"hitting", "pitching"}
        ):
            raise ValueError(
                "each statistics cutoff requires "
                "hitting and pitching payloads"
            )

        rows_by_cutoff[cutoff] = {
            "hitting": _statistics_rows(
                _mapping(groups["hitting"])
            ),
            "pitching": _statistics_rows(
                _mapping(groups["pitching"])
            ),
        }

    catalogs = {}
    evidence_counts = {}
    cutoffs_by_game = {}

    for game in sorted(
        lineup_bullpen.games,
        key=lambda value: (
            value.game_date,
            value.game_pk,
        ),
    ):
        if not game.ready:
            raise ValueError(
                "lineup-bullpen snapshots must be ready"
            )

        cutoff = (
            date.fromisoformat(game.game_date)
            - timedelta(days=1)
        ).isoformat()
        rows = rows_by_cutoff[cutoff]
        away_starter, home_starter = (
            starters[game.game_pk]
        )
        away_catcher, home_catcher = (
            catchers[game.game_pk]
        )

        runner_ids = (
            game.away_lineup_ids
            + game.home_lineup_ids
        )
        pitcher_ids = tuple(
            dict.fromkeys(
                (
                    away_starter,
                    home_starter,
                )
                + game.away_bullpen_ids
                + game.home_bullpen_ids
            )
        )

        pickoffs = pitcher_pickoffs_by_cutoff[
            cutoff
        ]
        catcher_outcomes = catcher_outcomes_by_cutoff[
            cutoff
        ]

        if not isinstance(pickoffs, Mapping):
            raise TypeError(
                "pitcher cutoff values must be mappings"
            )
        if not isinstance(catcher_outcomes, Mapping):
            raise TypeError(
                "catcher cutoff values must be mappings"
            )

        missing_pitchers = (
            set(pitcher_ids) - set(pickoffs)
        )
        if missing_pitchers:
            raise ValueError(
                "pitcher pickoff counts must cover "
                "every required pitcher"
            )

        missing_catchers = {
            away_catcher,
            home_catcher,
        } - set(catcher_outcomes)
        if missing_catchers:
            raise ValueError(
                "catcher outcomes must cover "
                "both starting catchers"
            )

        runner_counts = []

        for runner_id in runner_ids:
            stats = rows["hitting"].get(
                runner_id,
                {},
            )
            stolen_bases = _count(
                stats.get("stolenBases"),
                "hitting.stolenBases",
            )
            caught_stealing = _count(
                stats.get("caughtStealing"),
                "hitting.caughtStealing",
            )
            attempts = (
                stolen_bases + caught_stealing
            )
            opportunities = max(
                attempts,
                (
                    _count(
                        stats.get("hits"),
                        "hitting.hits",
                    )
                    + _count(
                        stats.get("baseOnBalls"),
                        "hitting.baseOnBalls",
                    )
                    + _count(
                        stats.get("hitByPitch"),
                        "hitting.hitByPitch",
                    )
                    - _count(
                        stats.get("homeRuns"),
                        "hitting.homeRuns",
                    )
                ),
            )

            runner_counts.append(
                CanonicalHistoricalRunnerBaserunningCounts(
                    runner_id=runner_id,
                    opportunity_count=opportunities,
                    stolen_bases=stolen_bases,
                    caught_stealing=caught_stealing,
                )
            )

        pitcher_counts = []

        for pitcher_id in pitcher_ids:
            stats = rows["pitching"].get(
                pitcher_id,
                {},
            )

            pitcher_counts.append(
                CanonicalHistoricalPitcherBaserunningCounts(
                    pitcher_id=pitcher_id,
                    batters_faced=_count(
                        stats.get("battersFaced"),
                        "pitching.battersFaced",
                    ),
                    stolen_bases_allowed=_count(
                        stats.get("stolenBases"),
                        "pitching.stolenBases",
                    ),
                    caught_stealing=_count(
                        stats.get("caughtStealing"),
                        "pitching.caughtStealing",
                    ),
                    pickoffs=_count(
                        pickoffs[pitcher_id],
                        "pitching.pickoffs",
                    ),
                )
            )

        def catcher_counts(
            catcher_id: str,
            side: str,
        ) -> CanonicalHistoricalCatcherBaserunningCounts:
            raw = catcher_outcomes[catcher_id]

            if (
                not isinstance(raw, tuple)
                or len(raw) != 2
            ):
                raise TypeError(
                    "catcher outcomes must be "
                    "stolen-base/caught-stealing tuples"
                )

            return CanonicalHistoricalCatcherBaserunningCounts(
                catcher_id=catcher_id,
                team_side=side,
                stolen_bases_allowed=_count(
                    raw[0],
                    "catcher.stolen_bases_allowed",
                ),
                caught_stealing=_count(
                    raw[1],
                    "catcher.caught_stealing",
                ),
            )

        materialized = (
            materialize_historical_baserunning_profiles(
                required_runner_ids=runner_ids,
                required_pitcher_ids=pitcher_ids,
                runner_counts=tuple(runner_counts),
                pitcher_counts=tuple(pitcher_counts),
                away_catcher_counts=catcher_counts(
                    away_catcher,
                    "away",
                ),
                home_catcher_counts=catcher_counts(
                    home_catcher,
                    "home",
                ),
            )
        )

        catalogs[game.game_pk] = materialized.catalog
        evidence_counts[
            game.game_pk
        ] = materialized.evidence_counts
        cutoffs_by_game[game.game_pk] = cutoff

    return source_historical_baserunning_replay_evidence(
        lineup_bullpen=lineup_bullpen,
        catalogs=catalogs,
        statistics_through_dates=cutoffs_by_game,
        evidence_counts=evidence_counts,
    )
