from typing import Any
from typing import Dict
from typing import Optional

from mlb_app.simulation.bullpen_integration import (
    BULLPEN_MODE_CANDIDATE_LEVERAGE,
    BULLPEN_MODE_OFF,
)
from mlb_app.simulation.bullpen_simulation_path import (
    run_candidate_bullpen_simulation_path,
)


def run_candidate_bullpen_shadow_simulation(
    bullpen_state: Any,
    simulation_events: Optional[list],
) -> Dict[str, Any]:

    events = simulation_events or []

    baseline = run_candidate_bullpen_simulation_path(
        bullpen_mode=BULLPEN_MODE_OFF,
        bullpen_state=bullpen_state,
        simulation_events=events,
    )

    candidate = run_candidate_bullpen_simulation_path(
        bullpen_mode=BULLPEN_MODE_CANDIDATE_LEVERAGE,
        bullpen_state=bullpen_state,
        simulation_events=events,
    )

    repeat = run_candidate_bullpen_simulation_path(
        bullpen_mode=BULLPEN_MODE_CANDIDATE_LEVERAGE,
        bullpen_state=bullpen_state,
        simulation_events=events,
    )

    baseline_selection_count = len(
        baseline[
            "selection_history"
        ]
    )

    candidate_selection_count = len(
        candidate[
            "selection_history"
        ]
    )

    shadow_delta_detected = bool(
        baseline_selection_count == 0
        and candidate_selection_count > 0
    )

    deterministic_repeatable = bool(
        candidate[
            "selection_history"
        ]
        == repeat[
            "selection_history"
        ]
    )

    return {
        "baseline_mode": BULLPEN_MODE_OFF,
        "candidate_mode": (
            BULLPEN_MODE_CANDIDATE_LEVERAGE
        ),
        "baseline_selection_count": (
            baseline_selection_count
        ),
        "candidate_selection_count": (
            candidate_selection_count
        ),
        "baseline_fallback_count": (
            baseline[
                "fallback_count"
            ]
        ),
        "candidate_fallback_count": (
            candidate[
                "fallback_count"
            ]
        ),
        "shadow_delta_detected": (
            shadow_delta_detected
        ),
        "deterministic_repeatable": (
            deterministic_repeatable
        ),
        "baseline_equivalent": (
            baseline[
                "baseline_equivalent"
            ]
        ),
    }
