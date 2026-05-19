from typing import Any
from typing import Dict
from typing import Optional

from mlb_app.simulation.bullpen_game_engine_hook import (
    run_candidate_bullpen_game_engine_hook,
)
from mlb_app.simulation.bullpen_integration import (
    BULLPEN_MODE_CANDIDATE_LEVERAGE,
    BULLPEN_MODE_OFF,
)


def run_candidate_bullpen_simulation_path(
    bullpen_mode: str,
    bullpen_state: Any,
    simulation_events: Optional[list],
) -> Dict[str, Any]:

    events = simulation_events or []

    if bullpen_mode == BULLPEN_MODE_OFF:

        return {
            "mode": BULLPEN_MODE_OFF,
            "simulation_path_active": False,
            "hook_called": False,
            "selection_history": [],
            "fallback_count": 0,
            "events_processed": len(events),
            "baseline_equivalent": True,
            "deterministic_repeatable": True,
        }

    if bullpen_mode != BULLPEN_MODE_CANDIDATE_LEVERAGE:
        raise ValueError(
            "bullpen_mode must be "
            "'off' or 'candidate_leverage'"
        )

    first = run_candidate_bullpen_game_engine_hook(
        bullpen_mode=bullpen_mode,
        bullpen_state=bullpen_state,
        leverage_events=events,
    )

    second = run_candidate_bullpen_game_engine_hook(
        bullpen_mode=bullpen_mode,
        bullpen_state=bullpen_state,
        leverage_events=events,
    )

    deterministic_repeatable = (
        first["selection_history"]
        == second["selection_history"]
    )

    return {
        "mode": bullpen_mode,
        "simulation_path_active": True,
        "hook_called": bool(
            first[
                "hook_active"
            ]
        ),
        "selection_history": (
            first[
                "selection_history"
            ]
        ),
        "fallback_count": (
            first[
                "fallback_count"
            ]
        ),
        "events_processed": len(events),
        "baseline_equivalent": False,
        "deterministic_repeatable": (
            deterministic_repeatable
        ),
    }
