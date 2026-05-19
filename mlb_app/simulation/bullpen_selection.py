from typing import Dict
from typing import List
from typing import Tuple

from mlb_app.simulation.bullpen_state import (
    BullpenState,
)


ROLE_PRIORITY = {
    "high": {
        "closer": 4,
        "setup": 3,
        "middle_relief": 2,
        "low_leverage": 1,
    },
    "medium": {
        "setup": 4,
        "middle_relief": 3,
        "closer": 2,
        "low_leverage": 1,
    },
    "low": {
        "middle_relief": 4,
        "low_leverage": 3,
        "setup": 2,
        "closer": 1,
    },
}


FATIGUE_WEIGHT = 2.0


def classify_leverage_bucket(
    inning: int,
    score_diff: int,
    base_runners: int,
    outs: int,
) -> str:

    if inning >= 10:
        return "high"

    if (
        inning >= 8
        and abs(score_diff) <= 2
    ):
        return "high"

    if (
        inning >= 7
        and base_runners >= 1
        and abs(score_diff) <= 3
    ):
        return "high"

    if (
        inning >= 6
        or (
            abs(score_diff) <= 4
            and base_runners >= 1
        )
    ):
        return "medium"

    return "low"


def _candidate_score(
    leverage_bucket: str,
    role: str,
    fatigue: float,
) -> float:

    role_score = (
        ROLE_PRIORITY
        .get(leverage_bucket, {})
        .get(role, 0)
    )

    return (
        float(role_score)
        - (
            fatigue
            * FATIGUE_WEIGHT
        )
    )


def select_candidate_reliever(
    bullpen_state: BullpenState,
    inning: int,
    score_diff: int,
    base_runners: int,
    outs: int,
) -> dict:

    leverage_bucket = (
        classify_leverage_bucket(
            inning=inning,
            score_diff=score_diff,
            base_runners=base_runners,
            outs=outs,
        )
    )

    candidates: List[
        Tuple[str, str, float]
    ] = []

    for pitcher_id in (
        bullpen_state.available_relievers
    ):

        if (
            pitcher_id
            in bullpen_state.used_pitchers
        ):
            continue

        role = (
            bullpen_state
            .role_by_pitcher
            .get(
                pitcher_id,
                "middle_relief",
            )
        )

        fatigue = (
            bullpen_state
            .fatigue_by_pitcher
            .get(
                pitcher_id,
                0.0,
            )
        )

        score = (
            _candidate_score(
                leverage_bucket=(
                    leverage_bucket
                ),
                role=role,
                fatigue=fatigue,
            )
        )

        candidates.append(
            (
                pitcher_id,
                role,
                score,
            )
        )

    if not candidates:

        return {
            "selected_pitcher_id": None,
            "selected_role": (
                "fallback"
            ),
            "leverage_bucket": (
                leverage_bucket
            ),
            "selection_score": 0.0,
            "fallback_used": True,
        }

    candidates.sort(
        key=lambda x: (
            -x[2],
            x[0],
        )
    )

    selected_pitcher_id = (
        candidates[0][0]
    )

    selected_role = (
        candidates[0][1]
    )

    selection_score = (
        candidates[0][2]
    )

    return {
        "selected_pitcher_id": (
            selected_pitcher_id
        ),
        "selected_role": (
            selected_role
        ),
        "leverage_bucket": (
            leverage_bucket
        ),
        "selection_score": (
            round(
                selection_score,
                4,
            )
        ),
        "fallback_used": False,
    }
