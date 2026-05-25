from copy import deepcopy
from typing import Any
from typing import Dict
from typing import Optional

from mlb_app.simulation.bullpen_integration import (
    BULLPEN_MODE_CANDIDATE_LEVERAGE,
    BULLPEN_MODE_OFF,
    run_candidate_bullpen_integration,
)


def run_candidate_bullpen_game_engine_hook(
    bullpen_mode: str,
    bullpen_state: Any,
    leverage_events: Optional[list],
) -> Dict[str, Any]:

    if bullpen_mode == BULLPEN_MODE_OFF:

        return {
            "mode": BULLPEN_MODE_OFF,
            "hook_active": False,
            "integration_called": False,
            "selection_history": [],
            "fallback_count": 0,
            "state_mutated": False,
            "deterministic_repeatable": True,
        }

    if bullpen_mode != BULLPEN_MODE_CANDIDATE_LEVERAGE:
        raise ValueError(
            "bullpen_mode must be "
            "'off' or 'candidate_leverage'"
        )

    initial_state_snapshot = deepcopy(
        bullpen_state
    )

    first = run_candidate_bullpen_integration(
        bullpen_mode=bullpen_mode,
        bullpen_state=bullpen_state,
        leverage_events=leverage_events or [],
    )

    second = run_candidate_bullpen_integration(
        bullpen_mode=bullpen_mode,
        bullpen_state=initial_state_snapshot,
        leverage_events=leverage_events or [],
    )

    deterministic_repeatable = (
        first["selection_history"]
        == second["selection_history"]
    )

    final_state = bullpen_state

    state_mutated = (
        final_state.__dict__
        != initial_state_snapshot.__dict__
    )

    return {
        "mode": bullpen_mode,
        "hook_active": True,
        "integration_called": True,
        "selection_history": (
            first["selection_history"]
        ),
        "fallback_count": (
            first["fallback_count"]
        ),
        "state_mutated": (
            state_mutated
        ),
        "deterministic_repeatable": (
            deterministic_repeatable
        ),
    }
