"""Source and fingerprint historical Statcast baserunning windows."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, Mapping, Tuple

from .statcast_baserunning_source import (
    CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION,
    decode_statcast_baserunning_outcomes,
)


CANONICAL_STATCAST_BASERUNNING_WINDOW_SOURCE_VERSION = (
    "canonical_statcast_baserunning_window_source_v1"
)
CANONICAL_BASERUNNING_SMOKE_WINDOW_START = "2026-04-20"
CANONICAL_BASERUNNING_SMOKE_WINDOW_END = "2026-05-03"

_REQUIRED_COLUMNS = (
    "game_date",
    "game_pk",
    "at_bat_number",
    "pitch_number",
    "des",
    "on_1b",
    "on_2b",
    "on_3b",
    "pitcher",
    "fielder_2",
)


def _missing(value: Any) -> bool:
    if value is None:
        return True

    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False


def _integer(
    value: Any,
    field_name: str,
) -> int:
    if _missing(value) or isinstance(value, bool):
        raise ValueError(
            f"{field_name} must be an integer"
        )

    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{field_name} must be an integer"
        ) from exc

    if (
        not math.isfinite(numeric)
        or not numeric.is_integer()
    ):
        raise ValueError(
            f"{field_name} must be an integer"
        )

    return int(numeric)


def _date_string(
    value: Any,
    field_name: str,
) -> str:
    if isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    elif isinstance(value, str):
        parsed = date.fromisoformat(
            value[:10]
        )
    else:
        raise TypeError(
            f"{field_name} must identify a date"
        )

    return parsed.isoformat()


def _json_value(value: Any) -> Any:
    if _missing(value):
        return None

    if hasattr(value, "item"):
        value = value.item()

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    if isinstance(value, (str, int, float, bool)):
        return value

    return str(value)


@dataclass(frozen=True)
class CanonicalStatcastBaserunningWindowSnapshot:
    window_start: str
    window_end: str
    game_keys: Tuple[
        Tuple[int, str],
        ...,
    ]
    row_count: int
    outcome_count: int
    stolen_bases: int
    caught_stealing: int
    digest: str
    outcome_source_version: str = (
        CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
    )
    source_version: str = (
        CANONICAL_STATCAST_BASERUNNING_WINDOW_SOURCE_VERSION
    )

    def __post_init__(self) -> None:
        if self.row_count <= 0:
            raise ValueError(
                "row_count must be positive"
            )

        for field_name in (
            "outcome_count",
            "stolen_bases",
            "caught_stealing",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(
                    f"{field_name} must be nonnegative"
                )

        if self.outcome_count != (
            self.stolen_bases
            + self.caught_stealing
        ):
            raise ValueError(
                "outcome_count must equal SB plus CS"
            )

        if not self.game_keys:
            raise ValueError(
                "game_keys must contain games"
            )

        if (
            len(self.game_keys)
            != len(set(self.game_keys))
        ):
            raise ValueError(
                "game_keys must be unique"
            )

        if (
            not isinstance(self.digest, str)
            or len(self.digest) != 64
        ):
            raise ValueError(
                "digest must be a SHA-256 hex digest"
            )

        if self.source_version != (
            CANONICAL_STATCAST_BASERUNNING_WINDOW_SOURCE_VERSION
        ):
            raise ValueError(
                "unsupported Statcast baserunning "
                "window source version"
            )

    @property
    def game_count(self) -> int:
        return len(self.game_keys)

    @property
    def coverage_complete(self) -> bool:
        return False

    @property
    def calibration_observed_source_eligible(
        self,
    ) -> bool:
        return False

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.source_version,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "game_count": self.game_count,
            "game_keys": self.game_keys,
            "row_count": self.row_count,
            "outcome_count": self.outcome_count,
            "stolen_bases": self.stolen_bases,
            "caught_stealing": self.caught_stealing,
            "digest": self.digest,
            "outcome_source_version": (
                self.outcome_source_version
            ),
            "coverage_complete": (
                self.coverage_complete
            ),
            "calibration_observed_source_eligible": (
                self.calibration_observed_source_eligible
            ),
            "coverage_warning": (
                "pitch-level Statcast descriptions "
                "do not contain every baserunning event"
            ),
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


@dataclass(frozen=True)
class CanonicalStatcastBaserunningWindowSource:
    snapshot: CanonicalStatcastBaserunningWindowSnapshot
    rows: Tuple[
        Mapping[str, Any],
        ...,
    ]

    def __post_init__(self) -> None:
        if len(self.rows) != self.snapshot.row_count:
            raise ValueError(
                "rows must match snapshot row_count"
            )


def source_statcast_baserunning_window(
    *,
    rows: Iterable[Mapping[str, Any]],
    window_start: str,
    window_end: str,
) -> CanonicalStatcastBaserunningWindowSource:
    """
    Canonicalize and fingerprint one fetched Statcast window.

    Fetching remains caller-owned. The returned rows are deterministic and
    directly consumable by the historical calibration window executor.
    """

    start_date = date.fromisoformat(window_start)
    end_date = date.fromisoformat(window_end)

    if end_date < start_date:
        raise ValueError(
            "window_end must not precede window_start"
        )

    if isinstance(rows, (str, bytes, Mapping)):
        raise TypeError(
            "rows must be an iterable of mappings"
        )

    canonical_rows = []
    row_keys = set()
    game_dates = {}

    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(
                "rows must contain mappings"
            )

        missing_columns = tuple(
            column
            for column in _REQUIRED_COLUMNS
            if column not in row
        )
        if missing_columns:
            raise ValueError(
                "Statcast row missing required columns: "
                + ",".join(missing_columns)
            )

        game_date = _date_string(
            row["game_date"],
            "game_date",
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
                "Statcast row game_date must fall "
                "within source window"
            )

        game_pk = _integer(
            row["game_pk"],
            "game_pk",
        )
        at_bat_number = _integer(
            row["at_bat_number"],
            "at_bat_number",
        )
        pitch_number = _integer(
            row["pitch_number"],
            "pitch_number",
        )

        row_key = (
            game_pk,
            at_bat_number,
            pitch_number,
        )
        if row_key in row_keys:
            raise ValueError(
                "Statcast pitch identifiers must be unique"
            )
        row_keys.add(row_key)

        prior_date = game_dates.get(game_pk)
        if (
            prior_date is not None
            and prior_date != game_date
        ):
            raise ValueError(
                "game_pk must map to one game_date"
            )
        game_dates[game_pk] = game_date

        canonical_row = {
            column: _json_value(row[column])
            for column in _REQUIRED_COLUMNS
        }
        canonical_row["game_date"] = game_date
        canonical_row["game_pk"] = game_pk
        canonical_row["at_bat_number"] = (
            at_bat_number
        )
        canonical_row["pitch_number"] = (
            pitch_number
        )
        canonical_rows.append(canonical_row)

    if not canonical_rows:
        raise ValueError(
            "rows must contain Statcast records"
        )

    canonical_rows.sort(
        key=lambda row: (
            row["game_date"],
            row["game_pk"],
            row["at_bat_number"],
            row["pitch_number"],
        )
    )

    outcomes = tuple(
        outcome
        for row in canonical_rows
        for outcome in (
            decode_statcast_baserunning_outcomes(
                row
            )
        )
    )

    serialized = json.dumps(
        canonical_rows,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    digest = hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()

    snapshot = (
        CanonicalStatcastBaserunningWindowSnapshot(
            window_start=start_date.isoformat(),
            window_end=end_date.isoformat(),
            game_keys=tuple(
                sorted(
                    (
                        game_pk,
                        game_date,
                    )
                    for game_pk, game_date
                    in game_dates.items()
                )
            ),
            row_count=len(canonical_rows),
            outcome_count=len(outcomes),
            stolen_bases=sum(
                value.event_type == "stolen_base"
                for value in outcomes
            ),
            caught_stealing=sum(
                value.event_type
                == "caught_stealing"
                for value in outcomes
            ),
            digest=digest,
        )
    )

    return CanonicalStatcastBaserunningWindowSource(
        snapshot=snapshot,
        rows=tuple(canonical_rows),
    )
