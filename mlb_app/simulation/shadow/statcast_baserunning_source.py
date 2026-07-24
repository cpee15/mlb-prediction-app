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


CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION = (
    "canonical_statcast_baserunning_source_v1"
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
