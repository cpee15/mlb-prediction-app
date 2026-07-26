"""Canonical out-of-sample baserunning holdout plan."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date
import hashlib
import json
from typing import Any, Dict

from mlb_app.simulation.game import (
    CanonicalBaserunningProbabilityTransform,
)


CANONICAL_HISTORICAL_BASERUNNING_HOLDOUT_VERSION = (
    "canonical_historical_baserunning_holdout_v1"
)
HISTORICAL_BASERUNNING_SELECTION_WINDOW_START = (
    "2026-04-20"
)
HISTORICAL_BASERUNNING_SELECTION_WINDOW_END = (
    "2026-05-03"
)
HISTORICAL_BASERUNNING_HOLDOUT_WINDOW_START = (
    "2026-05-04"
)
HISTORICAL_BASERUNNING_HOLDOUT_WINDOW_END = (
    "2026-05-17"
)
HISTORICAL_BASERUNNING_HOLDOUT_MINIMUM_GAME_COUNT = 150
HISTORICAL_BASERUNNING_HOLDOUT_SIMULATION_COUNT = 100
HISTORICAL_BASERUNNING_SELECTED_ATTEMPT_MULTIPLIER = (
    0.52
)
HISTORICAL_BASERUNNING_SELECTED_SUCCESS_ADJUSTMENT = (
    0.09
)


def _iso_date(value: str, *, field_name: str) -> date:
    if not isinstance(value, str):
        raise TypeError(
            f"{field_name} must be an ISO date string"
        )

    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must be an ISO date"
        ) from exc

    if parsed.isoformat() != value:
        raise ValueError(
            f"{field_name} must use ISO format"
        )

    return parsed


def _digest(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class CanonicalHistoricalBaserunningHoldoutPlan:
    selection_window_start: str
    selection_window_end: str
    holdout_window_start: str
    holdout_window_end: str
    minimum_game_count: int
    simulation_count: int
    probability_transform: (
        CanonicalBaserunningProbabilityTransform
    ) = field(
        default_factory=(
            CanonicalBaserunningProbabilityTransform
        )
    )
    schema_version: str = (
        CANONICAL_HISTORICAL_BASERUNNING_HOLDOUT_VERSION
    )

    def __post_init__(self) -> None:
        selection_start = _iso_date(
            self.selection_window_start,
            field_name="selection_window_start",
        )
        selection_end = _iso_date(
            self.selection_window_end,
            field_name="selection_window_end",
        )
        holdout_start = _iso_date(
            self.holdout_window_start,
            field_name="holdout_window_start",
        )
        holdout_end = _iso_date(
            self.holdout_window_end,
            field_name="holdout_window_end",
        )

        if selection_start > selection_end:
            raise ValueError(
                "selection window start cannot exceed end"
            )
        if holdout_start > holdout_end:
            raise ValueError(
                "holdout window start cannot exceed end"
            )
        if not (
            holdout_end < selection_start
            or holdout_start > selection_end
        ):
            raise ValueError(
                "holdout window must be disjoint from "
                "selection window"
            )
        if (
            not isinstance(self.minimum_game_count, int)
            or isinstance(self.minimum_game_count, bool)
            or self.minimum_game_count < 1
        ):
            raise ValueError(
                "minimum_game_count must be positive"
            )
        if (
            not isinstance(self.simulation_count, int)
            or isinstance(self.simulation_count, bool)
            or self.simulation_count < 100
        ):
            raise ValueError(
                "simulation_count must be at least 100"
            )
        if not isinstance(
            self.probability_transform,
            CanonicalBaserunningProbabilityTransform,
        ):
            raise TypeError(
                "probability_transform must be canonical"
            )
        if (
            self.schema_version
            != CANONICAL_HISTORICAL_BASERUNNING_HOLDOUT_VERSION
        ):
            raise ValueError(
                "unsupported historical baserunning "
                "holdout version"
            )

    @property
    def windows_are_disjoint(self) -> bool:
        selection_start = date.fromisoformat(
            self.selection_window_start
        )
        selection_end = date.fromisoformat(
            self.selection_window_end
        )
        holdout_start = date.fromisoformat(
            self.holdout_window_start
        )
        holdout_end = date.fromisoformat(
            self.holdout_window_end
        )

        return (
            holdout_end < selection_start
            or holdout_start > selection_end
        )

    @property
    def digest(self) -> str:
        return _digest(
            {
                "schema_version": self.schema_version,
                "selection_window_start": (
                    self.selection_window_start
                ),
                "selection_window_end": (
                    self.selection_window_end
                ),
                "holdout_window_start": (
                    self.holdout_window_start
                ),
                "holdout_window_end": (
                    self.holdout_window_end
                ),
                "minimum_game_count": (
                    self.minimum_game_count
                ),
                "simulation_count": (
                    self.simulation_count
                ),
                "probability_transform_digest": (
                    self.probability_transform.digest
                ),
            }
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "selection_window_start": (
                self.selection_window_start
            ),
            "selection_window_end": (
                self.selection_window_end
            ),
            "holdout_window_start": (
                self.holdout_window_start
            ),
            "holdout_window_end": (
                self.holdout_window_end
            ),
            "windows_are_disjoint": (
                self.windows_are_disjoint
            ),
            "minimum_game_count": (
                self.minimum_game_count
            ),
            "simulation_count": self.simulation_count,
            "probability_transform": (
                self.probability_transform.to_diagnostics()
            ),
            "holdout_plan_digest": self.digest,
            "candidate_reselected_on_holdout": False,
            "activation_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }



def filter_historical_baserunning_holdout_schedule(
    *,
    schedule: Dict[str, Any],
    plan: CanonicalHistoricalBaserunningHoldoutPlan,
) -> tuple[Dict[str, Any], tuple[Dict[str, Any], ...]]:
    """
    Exclude games officially played outside the holdout window.

    MLB may return postponed games under their original schedule date
    after they have been completed on a later officialDate. Those games
    are not holdout observations and cannot be reassigned to the
    originally scheduled date.
    """
    if not isinstance(schedule, dict):
        raise TypeError("schedule must be a dictionary")
    if not isinstance(
        plan,
        CanonicalHistoricalBaserunningHoldoutPlan,
    ):
        raise TypeError("plan must be canonical")

    filtered = deepcopy(schedule)
    filtered_dates = []
    excluded = []

    raw_dates = filtered.get("dates") or []
    if not isinstance(raw_dates, list):
        raise TypeError("schedule dates must be a list")

    for raw_date in raw_dates:
        if not isinstance(raw_date, dict):
            raise TypeError(
                "schedule date entries must be dictionaries"
            )

        schedule_date = str(raw_date.get("date") or "")
        _iso_date(
            schedule_date,
            field_name="schedule_date",
        )

        raw_games = raw_date.get("games") or []
        if not isinstance(raw_games, list):
            raise TypeError("schedule games must be a list")

        kept_games = []

        for game in raw_games:
            if not isinstance(game, dict):
                raise TypeError(
                    "schedule game entries must be dictionaries"
                )

            official_date = str(
                game.get("officialDate")
                or schedule_date
            )
            _iso_date(
                official_date,
                field_name="official_date",
            )

            if (
                plan.holdout_window_start
                <= official_date
                <= plan.holdout_window_end
            ):
                kept_games.append(game)
                continue

            status = game.get("status") or {}
            if not isinstance(status, dict):
                raise TypeError(
                    "schedule game status must be a dictionary"
                )

            excluded.append(
                {
                    "game_pk": game.get("gamePk"),
                    "schedule_date": schedule_date,
                    "official_date": official_date,
                    "detailed_state": status.get(
                        "detailedState"
                    ),
                    "reschedule_date": game.get(
                        "rescheduleDate"
                    ),
                    "reason": (
                        "official_date_outside_holdout"
                    ),
                }
            )

        if kept_games:
            raw_date["games"] = kept_games
            filtered_dates.append(raw_date)

    filtered["dates"] = filtered_dates

    excluded.sort(
        key=lambda value: (
            str(value["official_date"]),
            int(value["game_pk"] or 0),
        )
    )

    return filtered, tuple(excluded)

def build_historical_baserunning_holdout_plan(
) -> CanonicalHistoricalBaserunningHoldoutPlan:
    return CanonicalHistoricalBaserunningHoldoutPlan(
        selection_window_start=(
            HISTORICAL_BASERUNNING_SELECTION_WINDOW_START
        ),
        selection_window_end=(
            HISTORICAL_BASERUNNING_SELECTION_WINDOW_END
        ),
        holdout_window_start=(
            HISTORICAL_BASERUNNING_HOLDOUT_WINDOW_START
        ),
        holdout_window_end=(
            HISTORICAL_BASERUNNING_HOLDOUT_WINDOW_END
        ),
        minimum_game_count=(
            HISTORICAL_BASERUNNING_HOLDOUT_MINIMUM_GAME_COUNT
        ),
        simulation_count=(
            HISTORICAL_BASERUNNING_HOLDOUT_SIMULATION_COUNT
        ),
        probability_transform=(
            CanonicalBaserunningProbabilityTransform(
                attempt_probability_multiplier=(
                    HISTORICAL_BASERUNNING_SELECTED_ATTEMPT_MULTIPLIER
                ),
                success_rate_adjustment=(
                    HISTORICAL_BASERUNNING_SELECTED_SUCCESS_ADJUSTMENT
                ),
            )
        ),
    )
