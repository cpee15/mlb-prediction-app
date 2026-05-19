from copy import deepcopy

from mlb_app.simulation.bullpen_selection import (
    select_candidate_reliever,
)
from mlb_app.simulation.bullpen_usage import (
    apply_candidate_bullpen_usage,
)


def simulate_candidate_bullpen_chain(
    bullpen_state,
    leverage_events,
):

    state = deepcopy(
        bullpen_state
    )

    selection_history = []

    fallback_count = 0

    for event in leverage_events:

        selection = (
            select_candidate_reliever(
                bullpen_state=state,
                inning=event.get(
                    "inning",
                    1,
                ),
                score_diff=event.get(
                    "score_diff",
                    0,
                ),
                base_runners=event.get(
                    "base_runners",
                    0,
                ),
                outs=event.get(
                    "outs",
                    0,
                ),
            )
        )

        pitcher_id = (
            selection.get(
                "selected_pitcher_id"
            )
        )

        leverage_bucket = (
            selection.get(
                "leverage_bucket",
                "low",
            )
        )

        fallback_used = bool(
            selection.get(
                "fallback_used",
                False,
            )
        )

        if fallback_used:
            fallback_count += 1

        state = (
            apply_candidate_bullpen_usage(
                bullpen_state=state,
                selected_pitcher_id=(
                    pitcher_id
                ),
                inning=event.get(
                    "inning",
                    1,
                ),
                leverage_bucket=(
                    leverage_bucket
                ),
            )
        )

        selection_history.append(
            {
                "inning": event.get(
                    "inning",
                    1,
                ),
                "selected_pitcher_id": (
                    pitcher_id
                ),
                "selected_role": (
                    selection.get(
                        "selected_role"
                    )
                ),
                "leverage_bucket": (
                    leverage_bucket
                ),
                "fallback_used": (
                    fallback_used
                ),
            }
        )

    return {
        "final_state": state,
        "selection_history": (
            selection_history
        ),
        "fallback_count": (
            fallback_count
        ),
        "unique_pitchers_used": len(
            set(
                state.used_pitchers
            )
        ),
    }
