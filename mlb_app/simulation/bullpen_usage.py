from copy import deepcopy
from typing import Optional

from mlb_app.simulation.bullpen_state import (
    BullpenState,
)


FATIGUE_INCREMENTS = {
    "high": 0.30,
    "medium": 0.20,
    "low": 0.10,
}


def apply_candidate_bullpen_usage(
    bullpen_state: BullpenState,
    selected_pitcher_id: Optional[str],
    inning: int,
    leverage_bucket: str,
) -> BullpenState:

    new_state = deepcopy(
        bullpen_state
    )

    if selected_pitcher_id is None:
        return new_state

    new_state.current_pitcher = (
        selected_pitcher_id
    )

    if (
        selected_pitcher_id
        not in new_state.used_pitchers
    ):
        new_state.used_pitchers.append(
            selected_pitcher_id
        )

    fatigue_before = (
        new_state.fatigue_by_pitcher.get(
            selected_pitcher_id,
            0.0,
        )
    )

    increment = (
        FATIGUE_INCREMENTS.get(
            leverage_bucket,
            0.10,
        )
    )

    fatigue_after = min(
        1.0,
        fatigue_before + increment,
    )

    new_state.fatigue_by_pitcher[
        selected_pitcher_id
    ] = round(
        fatigue_after,
        4,
    )

    new_state.usage_log.append(
        {
            "pitcher_id": (
                selected_pitcher_id
            ),
            "inning": inning,
            "leverage_bucket": (
                leverage_bucket
            ),
        }
    )

    return new_state
