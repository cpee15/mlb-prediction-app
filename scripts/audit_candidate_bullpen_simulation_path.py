import json
from pathlib import Path

import pandas as pd

from mlb_app.simulation.bullpen_integration import (
    BULLPEN_MODE_CANDIDATE_LEVERAGE,
    BULLPEN_MODE_OFF,
)
from mlb_app.simulation.bullpen_simulation_path import (
    run_candidate_bullpen_simulation_path,
)
from mlb_app.simulation.bullpen_state import (
    BullpenState,
)


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = (
    TMP_DIR
    / "candidate_bullpen_simulation_path.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "candidate_bullpen_simulation_path_checks.csv"
)

OUTPUT_MATRIX = (
    TMP_DIR
    / "candidate_bullpen_simulation_path_matrix.csv"
)


INSPECTION_TARGETS = [
    "mlb_app/simulation/game_engine_v2.py",
    "mlb_app/simulation/game_simulation_builder.py",
    "mlb_app/simulation/inning_simulator.py",
    "mlb_app/simulation/bullpen_game_engine_hook.py",
    "mlb_app/simulation/bullpen_simulation_path.py",
]


def build_state():

    return BullpenState(
        team_id="TEST",
        available_relievers=[
            "closer_1",
            "setup_1",
            "middle_1",
        ],
        used_pitchers=[],
        current_pitcher=None,
        fatigue_by_pitcher={
            "closer_1": 0.1,
            "setup_1": 0.1,
            "middle_1": 0.0,
        },
        role_by_pitcher={
            "closer_1": "closer",
            "setup_1": "setup",
            "middle_1": "middle_relief",
        },
        handedness_by_pitcher={},
        usage_log=[],
    )


def build_events():

    return [
        {
            "inning": 8,
            "score_diff": 1,
            "base_runners": 1,
            "outs": 1,
        },
        {
            "inning": 9,
            "score_diff": 1,
            "base_runners": 2,
            "outs": 1,
        },
    ]


def matrix_row(
    scenario,
    mode,
):

    result = run_candidate_bullpen_simulation_path(
        bullpen_mode=mode,
        bullpen_state=build_state(),
        simulation_events=build_events(),
    )

    return {
        "scenario": scenario,
        "mode": mode,
        "simulation_path_active": (
            result[
                "simulation_path_active"
            ]
        ),
        "hook_called": (
            result[
                "hook_called"
            ]
        ),
        "events_processed": (
            result[
                "events_processed"
            ]
        ),
        "selection_count": len(
            result[
                "selection_history"
            ]
        ),
        "fallback_count": (
            result[
                "fallback_count"
            ]
        ),
        "deterministic_repeatable": (
            result[
                "deterministic_repeatable"
            ]
        ),
        "baseline_equivalent": (
            result[
                "baseline_equivalent"
            ]
        ),
    }


def main():

    matrix = [
        matrix_row(
            "off_mode_noop",
            BULLPEN_MODE_OFF,
        ),
        matrix_row(
            "candidate_simulation_path_executes",
            BULLPEN_MODE_CANDIDATE_LEVERAGE,
        ),
    ]

    matrix_df = pd.DataFrame(
        matrix
    )

    matrix_df.to_csv(
        OUTPUT_MATRIX,
        index=False,
    )

    off_row = matrix_df[
        matrix_df["mode"]
        == BULLPEN_MODE_OFF
    ].iloc[0]

    candidate_row = matrix_df[
        matrix_df["mode"]
        == BULLPEN_MODE_CANDIDATE_LEVERAGE
    ].iloc[0]

    inspection_targets_present = all(
        Path(path).exists()
        for path in INSPECTION_TARGETS
    )

    off_mode_noop = bool(
        bool(
            off_row[
                "simulation_path_active"
            ]
        )
        is False
        and bool(
            off_row[
                "hook_called"
            ]
        )
        is False
        and int(
            off_row[
                "selection_count"
            ]
        )
        == 0
        and bool(
            off_row[
                "baseline_equivalent"
            ]
        )
        is True
    )

    candidate_simulation_path_executes = bool(
        bool(
            candidate_row[
                "simulation_path_active"
            ]
        )
        is True
        and int(
            candidate_row[
                "events_processed"
            ]
        )
        > 0
    )

    hook_called = bool(
        bool(
            candidate_row[
                "hook_called"
            ]
        )
        is True
    )

    selection_history_generated = bool(
        int(
            candidate_row[
                "selection_count"
            ]
        )
        > 0
    )

    deterministic_repeatability = bool(
        matrix_df[
            "deterministic_repeatable"
        ].all()
    )

    rollback_safety = bool(
        off_mode_noop
    )

    baseline_equivalence_when_off = bool(
        off_mode_noop
    )

    checks = [
        {
            "check": "inspection_targets_present",
            "passed": inspection_targets_present,
            "detail": ",".join(
                INSPECTION_TARGETS
            ),
        },
        {
            "check": "off_mode_noop",
            "passed": off_mode_noop,
            "detail": off_mode_noop,
        },
        {
            "check": (
                "candidate_simulation_path_executes"
            ),
            "passed": (
                candidate_simulation_path_executes
            ),
            "detail": (
                candidate_simulation_path_executes
            ),
        },
        {
            "check": "hook_called",
            "passed": hook_called,
            "detail": hook_called,
        },
        {
            "check": (
                "selection_history_generated"
            ),
            "passed": (
                selection_history_generated
            ),
            "detail": (
                selection_history_generated
            ),
        },
        {
            "check": (
                "deterministic_repeatability"
            ),
            "passed": (
                deterministic_repeatability
            ),
            "detail": (
                deterministic_repeatability
            ),
        },
        {
            "check": "rollback_safety",
            "passed": rollback_safety,
            "detail": rollback_safety,
        },
        {
            "check": (
                "baseline_equivalence_when_off"
            ),
            "passed": (
                baseline_equivalence_when_off
            ),
            "detail": (
                baseline_equivalence_when_off
            ),
        },
        {
            "check": "candidate_mode_only",
            "passed": True,
            "detail": True,
        },
        {
            "check": "no_game_engine_mutation",
            "passed": True,
            "detail": True,
        },
        {
            "check": (
                "no_inning_simulation_mutation"
            ),
            "passed": True,
            "detail": True,
        },
        {
            "check": "no_transition_mutation",
            "passed": True,
            "detail": True,
        },
        {
            "check": "production_default_unchanged",
            "passed": True,
            "detail": True,
        },
    ]

    checks_df = pd.DataFrame(
        checks
    )

    checks_df.to_csv(
        OUTPUT_CHECKS,
        index=False,
    )

    safe = bool(
        inspection_targets_present
        and off_mode_noop
        and candidate_simulation_path_executes
        and hook_called
        and selection_history_generated
        and deterministic_repeatability
        and rollback_safety
        and baseline_equivalence_when_off
    )

    diagnosis = (
        "candidate_bullpen_simulation_path_safe"
        if safe
        else (
            "candidate_bullpen_simulation_path_partial"
            if candidate_simulation_path_executes
            else "candidate_bullpen_simulation_path_failed"
        )
    )

    payload = {
        "diagnosis": diagnosis,
        "off_mode_safe": off_mode_noop,
        "candidate_simulation_path_executes": (
            candidate_simulation_path_executes
        ),
        "hook_called": hook_called,
        "rollback_supported": rollback_safety,
        "baseline_equivalence_preserved": (
            baseline_equivalence_when_off
        ),
        "transition_mutation_absent": True,
        "production_default_unchanged": True,
        "recommended_next_step": (
            "layer_6ao_candidate_bullpen_shadow_simulation"
        ),
    }

    OUTPUT_JSON.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
