"""Decode event-level baserunning outcomes from Statcast rows."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import (
    Any,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Tuple,
)

from .runner_baserunning_evidence import (
    CanonicalRunnerBaserunningObservation,
)


CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION = (
    "canonical_statcast_baserunning_source_v1"
)
CANONICAL_RUNNER_BASERUNNING_MATERIALIZATION_VERSION = (
    "canonical_runner_baserunning_materialization_v1"
)

_OUTCOME_PATTERN = re.compile(
    r"(?P<stolen>steals\s+"
    r"(?:\(\d+\)\s+)?"
    r"(?P<stolen_target>2nd|3rd|home)\s+base)"
    r"|"
    r"(?P<caught>caught\s+stealing\s+"
    r"(?P<caught_target>2nd|3rd|home))",
    re.IGNORECASE,
)

_TARGET_CONTEXT = {
    "2nd": (
        "first",
        "second",
        "on_1b",
    ),
    "3rd": (
        "second",
        "third",
        "on_2b",
    ),
    "home": (
        "third",
        "home",
        "on_3b",
    ),
}


def _identifier(value: Any) -> Optional[str]:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))

    text = str(value).strip()

    if text.lower() in {
        "",
        "<na>",
        "nan",
        "none",
        "null",
    }:
        return None

    try:
        numeric = float(text)
    except ValueError:
        return text

    if math.isnan(numeric):
        return None

    if numeric.is_integer():
        return str(int(numeric))

    return text


def _integer(value: Any) -> Optional[int]:
    identifier = _identifier(value)

    if identifier is None:
        return None

    try:
        numeric = float(identifier)
    except ValueError:
        return None

    if not numeric.is_integer():
        return None

    return int(numeric)


@dataclass(frozen=True)
class CanonicalStatcastBaserunningOutcome:
    """One runner outcome decoded from a Statcast description."""

    runner_id: str
    event_type: str
    origin_base: str
    target_base: str
    game_pk: Optional[int] = None
    at_bat_number: Optional[int] = None
    pitch_number: Optional[int] = None
    pitcher_id: Optional[str] = None
    catcher_id: Optional[str] = None
    source_version: str = (
        CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError(
                "runner_id is required"
            )

        if self.event_type not in {
            "stolen_base",
            "caught_stealing",
        }:
            raise ValueError(
                "unsupported baserunning event_type"
            )

        if (
            self.origin_base,
            self.target_base,
        ) not in {
            ("first", "second"),
            ("second", "third"),
            ("third", "home"),
        }:
            raise ValueError(
                "unsupported baserunning transition"
            )

        if self.source_version != (
            CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported Statcast baserunning source version"
            )


def decode_statcast_baserunning_outcomes(
    row: Mapping[str, Any],
) -> Tuple[CanonicalStatcastBaserunningOutcome, ...]:
    """
    Decode all stolen-base and caught-stealing outcomes in one row.

    Runner identity comes from the matching pre-play Statcast occupancy field,
    allowing double steals to produce separate immutable outcomes without
    resolving player names from free text.
    """

    if not isinstance(row, Mapping):
        raise TypeError(
            "row must be a mapping"
        )

    description = row.get("des")

    if description in (None, ""):
        return ()

    text = str(description)
    outcomes = []
    seen = set()

    for match in _OUTCOME_PATTERN.finditer(text):
        target_token = (
            match.group("stolen_target")
            or match.group("caught_target")
        )

        if target_token is None:
            continue

        target_key = target_token.lower()
        origin_base, target_base, runner_field = (
            _TARGET_CONTEXT[target_key]
        )
        runner_id = _identifier(
            row.get(runner_field)
        )

        if runner_id is None:
            continue

        event_type = (
            "stolen_base"
            if match.group("stolen") is not None
            else "caught_stealing"
        )

        identity = (
            runner_id,
            event_type,
            origin_base,
            target_base,
        )

        if identity in seen:
            continue

        seen.add(identity)
        outcomes.append(
            CanonicalStatcastBaserunningOutcome(
                runner_id=runner_id,
                event_type=event_type,
                origin_base=origin_base,
                target_base=target_base,
                game_pk=_integer(
                    row.get("game_pk")
                ),
                at_bat_number=_integer(
                    row.get("at_bat_number")
                ),
                pitch_number=_integer(
                    row.get("pitch_number")
                ),
                pitcher_id=_identifier(
                    row.get("pitcher")
                ),
                catcher_id=_identifier(
                    row.get("fielder_2")
                ),
            )
        )

    return tuple(outcomes)



@dataclass(frozen=True)
class CanonicalStatcastRunnerBaserunningCounts:
    """Exact supported-transition counts for one runner."""

    runner_id: str
    eligible_opportunities: int
    stolen_bases: int
    caught_stealing: int
    source_version: str = (
        CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError(
                "runner_id is required"
            )

        for name, value in (
            (
                "eligible_opportunities",
                self.eligible_opportunities,
            ),
            (
                "stolen_bases",
                self.stolen_bases,
            ),
            (
                "caught_stealing",
                self.caught_stealing,
            ),
        ):
            if not isinstance(value, int):
                raise TypeError(
                    f"{name} must be an integer"
                )
            if value < 0:
                raise ValueError(
                    f"{name} must be nonnegative"
                )

        if (
            self.stolen_bases
            + self.caught_stealing
            > self.eligible_opportunities
        ):
            raise ValueError(
                "attempts cannot exceed eligible opportunities"
            )

        if self.source_version != (
            CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported Statcast baserunning source version"
            )


def _row_key(
    row: Mapping[str, Any],
) -> Tuple[int, int, int]:
    values = tuple(
        _integer(row.get(name))
        for name in (
            "game_pk",
            "at_bat_number",
            "pitch_number",
        )
    )

    if any(value is None for value in values):
        raise ValueError(
            "Statcast row requires game_pk, "
            "at_bat_number, and pitch_number"
        )

    return values


def _runner_has_outcome(
    *,
    runner_id: str,
    origin_base: str,
    outcomes: Tuple[
        CanonicalStatcastBaserunningOutcome,
        ...,
    ],
) -> bool:
    return any(
        outcome.runner_id == runner_id
        and outcome.origin_base == origin_base
        and outcome.target_base in {
            "second",
            "third",
        }
        for outcome in outcomes
    )


def aggregate_statcast_runner_baserunning_counts(
    rows: Iterable[Mapping[str, Any]],
) -> Tuple[
    CanonicalStatcastRunnerBaserunningCounts,
    ...,
]:
    """
    Aggregate exact pitch opportunities and SB/CS outcomes.

    Only first-to-second and second-to-third transitions are included because
    those are the currently supported canonical steal transitions. Duplicate
    Statcast pitch rows are ignored using their stable event identity.
    """

    counts: Dict[str, Dict[str, int]] = {}
    seen_rows = set()

    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(
                "each Statcast row must be a mapping"
            )

        row_key = _row_key(row)

        if row_key in seen_rows:
            continue

        seen_rows.add(row_key)
        outcomes = decode_statcast_baserunning_outcomes(
            row
        )

        on_first = _identifier(
            row.get("on_1b")
        )
        on_second = _identifier(
            row.get("on_2b")
        )
        on_third = _identifier(
            row.get("on_3b")
        )

        eligible = []

        if on_first is not None:
            target_available = on_second is None
            own_outcome = _runner_has_outcome(
                runner_id=on_first,
                origin_base="first",
                outcomes=outcomes,
            )

            if target_available or own_outcome:
                eligible.append(on_first)

        if on_second is not None:
            target_available = on_third is None
            own_outcome = _runner_has_outcome(
                runner_id=on_second,
                origin_base="second",
                outcomes=outcomes,
            )

            if target_available or own_outcome:
                eligible.append(on_second)

        for runner_id in eligible:
            runner_counts = counts.setdefault(
                runner_id,
                {
                    "eligible_opportunities": 0,
                    "stolen_bases": 0,
                    "caught_stealing": 0,
                },
            )
            runner_counts[
                "eligible_opportunities"
            ] += 1

        for outcome in outcomes:
            if outcome.target_base not in {
                "second",
                "third",
            }:
                continue

            runner_counts = counts.setdefault(
                outcome.runner_id,
                {
                    "eligible_opportunities": 0,
                    "stolen_bases": 0,
                    "caught_stealing": 0,
                },
            )

            if outcome.runner_id not in eligible:
                runner_counts[
                    "eligible_opportunities"
                ] += 1

            if outcome.event_type == "stolen_base":
                runner_counts["stolen_bases"] += 1
            else:
                runner_counts[
                    "caught_stealing"
                ] += 1

    return tuple(
        CanonicalStatcastRunnerBaserunningCounts(
            runner_id=runner_id,
            eligible_opportunities=(
                values["eligible_opportunities"]
            ),
            stolen_bases=values["stolen_bases"],
            caught_stealing=(
                values["caught_stealing"]
            ),
        )
        for runner_id, values in sorted(
            counts.items()
        )
    )



def _validate_unit_rate(
    *,
    name: str,
    value: float,
) -> None:
    if not isinstance(value, (int, float)):
        raise TypeError(
            f"{name} must be numeric"
        )

    if not 0.0 <= float(value) <= 1.0:
        raise ValueError(
            f"{name} must be between 0 and 1"
        )


@dataclass(frozen=True)
class CanonicalRunnerBaserunningContext:
    """Explicit non-count context required for one runner."""

    runner_id: str
    speed_score: float
    lead_quality: float
    fatigue_index: float
    injury_limit_flag: bool = False
    context_source_version: str = (
        "unavailable"
    )
    materialization_version: str = (
        CANONICAL_RUNNER_BASERUNNING_MATERIALIZATION_VERSION
    )

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError(
                "runner_id is required"
            )

        for name, value in (
            (
                "speed_score",
                self.speed_score,
            ),
            (
                "lead_quality",
                self.lead_quality,
            ),
            (
                "fatigue_index",
                self.fatigue_index,
            ),
        ):
            _validate_unit_rate(
                name=name,
                value=value,
            )

        if not isinstance(
            self.injury_limit_flag,
            bool,
        ):
            raise TypeError(
                "injury_limit_flag must be boolean"
            )

        if (
            not self.context_source_version
            or self.context_source_version
            == "unavailable"
        ):
            raise ValueError(
                "context_source_version must identify "
                "an available source"
            )

        if self.materialization_version != (
            CANONICAL_RUNNER_BASERUNNING_MATERIALIZATION_VERSION
        ):
            raise ValueError(
                "unsupported runner baserunning "
                "materialization version"
            )


def materialize_statcast_runner_observations(
    *,
    counts: Tuple[
        CanonicalStatcastRunnerBaserunningCounts,
        ...,
    ],
    contexts: Tuple[
        CanonicalRunnerBaserunningContext,
        ...,
    ],
) -> Tuple[
    CanonicalRunnerBaserunningObservation,
    ...,
]:
    """
    Join exact Statcast counts to complete runner context.

    Missing context yields no observation for that runner. No speed, lead,
    fatigue, or injury values are fabricated.
    """

    count_ids = [
        value.runner_id
        for value in counts
    ]
    context_ids = [
        value.runner_id
        for value in contexts
    ]

    if len(count_ids) != len(set(count_ids)):
        raise ValueError(
            "runner count identifiers must be unique"
        )

    if len(context_ids) != len(set(context_ids)):
        raise ValueError(
            "runner context identifiers must be unique"
        )

    for value in counts:
        if not isinstance(
            value,
            CanonicalStatcastRunnerBaserunningCounts,
        ):
            raise TypeError(
                "counts must contain "
                "CanonicalStatcastRunnerBaserunningCounts"
            )

    for value in contexts:
        if not isinstance(
            value,
            CanonicalRunnerBaserunningContext,
        ):
            raise TypeError(
                "contexts must contain "
                "CanonicalRunnerBaserunningContext"
            )

    contexts_by_id = {
        value.runner_id: value
        for value in contexts
    }

    observations = []

    for value in counts:
        context = contexts_by_id.get(
            value.runner_id
        )

        if context is None:
            continue

        observations.append(
            CanonicalRunnerBaserunningObservation(
                runner_id=value.runner_id,
                eligible_opportunities=(
                    value.eligible_opportunities
                ),
                stolen_bases=value.stolen_bases,
                caught_stealing=(
                    value.caught_stealing
                ),
                speed_score=float(
                    context.speed_score
                ),
                lead_quality=float(
                    context.lead_quality
                ),
                fatigue_index=float(
                    context.fatigue_index
                ),
                injury_limit_flag=(
                    context.injury_limit_flag
                ),
                source_version=(
                    f"{value.source_version}+"
                    f"{context.context_source_version}"
                ),
            )
        )

    return tuple(observations)


@dataclass(frozen=True)
class CanonicalStatcastPitcherBaserunningCounts:
    """Exact supported-transition exposure against one pitcher."""

    pitcher_id: str
    eligible_opportunities: int
    stolen_bases_allowed: int
    caught_stealing: int
    source_version: str = (
        CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.pitcher_id:
            raise ValueError(
                "pitcher_id is required"
            )

        _validate_defender_counts(
            eligible_opportunities=(
                self.eligible_opportunities
            ),
            stolen_bases_allowed=(
                self.stolen_bases_allowed
            ),
            caught_stealing=self.caught_stealing,
        )

        if self.source_version != (
            CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported Statcast baserunning source version"
            )


@dataclass(frozen=True)
class CanonicalStatcastCatcherBaserunningCounts:
    """Exact supported-transition exposure against one catcher."""

    catcher_id: str
    eligible_opportunities: int
    stolen_bases_allowed: int
    caught_stealing: int
    source_version: str = (
        CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
    )

    def __post_init__(self) -> None:
        if not self.catcher_id:
            raise ValueError(
                "catcher_id is required"
            )

        _validate_defender_counts(
            eligible_opportunities=(
                self.eligible_opportunities
            ),
            stolen_bases_allowed=(
                self.stolen_bases_allowed
            ),
            caught_stealing=self.caught_stealing,
        )

        if self.source_version != (
            CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported Statcast baserunning source version"
            )


def _validate_defender_counts(
    *,
    eligible_opportunities: int,
    stolen_bases_allowed: int,
    caught_stealing: int,
) -> None:
    for name, value in (
        (
            "eligible_opportunities",
            eligible_opportunities,
        ),
        (
            "stolen_bases_allowed",
            stolen_bases_allowed,
        ),
        (
            "caught_stealing",
            caught_stealing,
        ),
    ):
        if not isinstance(value, int):
            raise TypeError(
                f"{name} must be an integer"
            )
        if value < 0:
            raise ValueError(
                f"{name} must be nonnegative"
            )

    if (
        stolen_bases_allowed
        + caught_stealing
        > eligible_opportunities
    ):
        raise ValueError(
            "attempts cannot exceed eligible opportunities"
        )


def _eligible_statcast_runner_ids(
    *,
    row: Mapping[str, Any],
    outcomes: Tuple[
        CanonicalStatcastBaserunningOutcome,
        ...,
    ],
) -> Tuple[str, ...]:
    """Return exact runners eligible for supported transitions."""

    on_first = _identifier(
        row.get("on_1b")
    )
    on_second = _identifier(
        row.get("on_2b")
    )
    on_third = _identifier(
        row.get("on_3b")
    )

    eligible = []

    if on_first is not None:
        target_available = on_second is None
        own_outcome = _runner_has_outcome(
            runner_id=on_first,
            origin_base="first",
            outcomes=outcomes,
        )

        if target_available or own_outcome:
            eligible.append(on_first)

    if on_second is not None:
        target_available = on_third is None
        own_outcome = _runner_has_outcome(
            runner_id=on_second,
            origin_base="second",
            outcomes=outcomes,
        )

        if target_available or own_outcome:
            eligible.append(on_second)

    return tuple(eligible)


def _aggregate_statcast_defender_counts(
    *,
    rows: Iterable[Mapping[str, Any]],
    identity_field: str,
) -> Dict[str, Dict[str, int]]:
    counts: Dict[str, Dict[str, int]] = {}
    seen_rows = set()

    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(
                "each Statcast row must be a mapping"
            )

        row_key = _row_key(row)

        if row_key in seen_rows:
            continue

        seen_rows.add(row_key)
        defender_id = _identifier(
            row.get(identity_field)
        )

        if defender_id is None:
            continue

        outcomes = decode_statcast_baserunning_outcomes(
            row
        )
        eligible = _eligible_statcast_runner_ids(
            row=row,
            outcomes=outcomes,
        )

        defender_counts = counts.setdefault(
            defender_id,
            {
                "eligible_opportunities": 0,
                "stolen_bases_allowed": 0,
                "caught_stealing": 0,
            },
        )
        defender_counts[
            "eligible_opportunities"
        ] += len(eligible)

        for outcome in outcomes:
            if outcome.target_base not in {
                "second",
                "third",
            }:
                continue

            if outcome.runner_id not in eligible:
                defender_counts[
                    "eligible_opportunities"
                ] += 1

            if outcome.event_type == "stolen_base":
                defender_counts[
                    "stolen_bases_allowed"
                ] += 1
            else:
                defender_counts[
                    "caught_stealing"
                ] += 1

    return counts


def aggregate_statcast_pitcher_baserunning_counts(
    rows: Iterable[Mapping[str, Any]],
) -> Tuple[
    CanonicalStatcastPitcherBaserunningCounts,
    ...,
]:
    """Aggregate exact steal exposure and outcomes by pitcher."""

    counts = _aggregate_statcast_defender_counts(
        rows=rows,
        identity_field="pitcher",
    )

    return tuple(
        CanonicalStatcastPitcherBaserunningCounts(
            pitcher_id=pitcher_id,
            eligible_opportunities=(
                values["eligible_opportunities"]
            ),
            stolen_bases_allowed=(
                values["stolen_bases_allowed"]
            ),
            caught_stealing=(
                values["caught_stealing"]
            ),
        )
        for pitcher_id, values in sorted(
            counts.items()
        )
    )


def aggregate_statcast_catcher_baserunning_counts(
    rows: Iterable[Mapping[str, Any]],
) -> Tuple[
    CanonicalStatcastCatcherBaserunningCounts,
    ...,
]:
    """Aggregate exact steal exposure and outcomes by catcher."""

    counts = _aggregate_statcast_defender_counts(
        rows=rows,
        identity_field="fielder_2",
    )

    return tuple(
        CanonicalStatcastCatcherBaserunningCounts(
            catcher_id=catcher_id,
            eligible_opportunities=(
                values["eligible_opportunities"]
            ),
            stolen_bases_allowed=(
                values["stolen_bases_allowed"]
            ),
            caught_stealing=(
                values["caught_stealing"]
            ),
        )
        for catcher_id, values in sorted(
            counts.items()
        )
    )
