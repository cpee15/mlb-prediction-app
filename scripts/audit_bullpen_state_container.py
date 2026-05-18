import json
from pathlib import Path

import pandas as pd

from mlb_app.simulation.bullpen_state import (
    BULLPEN_MODE_CANDIDATE_LEVERAGE,
    BULLPEN_MODE_OFF,
    BullpenState,
    bullpen_state_from_dict,
    bullpen_state_to_dict,
    create_empty_bullpen_state,
    validate_bullpen_state,
)


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = (
    TMP_DIR
    / "bullpen_state_container_audit.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "bullpen_state_container_checks.csv"
)


PRODUCTION_FILES = [
    (
        "mlb_app/simulation/"
        "game_engine_v2.py"
    ),
    (
        "mlb_app/simulation/"
        "game_simulation_builder.py"
    ),
    (
        "mlb_app/simulation/"
        "inning_simulator.py"
    ),
]


def check_raises(fn):

    try:
        fn()
        return False

    except ValueError:
        return True


def main():

    checks = []

    state = create_empty_bullpen_state(
        "NYY"
    )

    checks.append(
        {
            "check": (
                "empty_state_valid"
            ),
            "passed": (
                validate_bullpen_state(
                    state
                )
            ),
            "detail": "validated",
        }
    )

    payload = bullpen_state_to_dict(
        state
    )

    restored = bullpen_state_from_dict(
        payload
    )

    roundtrip_passed = (
        payload
        == bullpen_state_to_dict(
            restored
        )
    )

    checks.append(
        {
            "check": (
                "roundtrip_preserves_state"
            ),
            "passed": roundtrip_passed,
            "detail": (
                "roundtrip_equal"
            ),
        }
    )

    checks.append(
        {
            "check": (
                "invalid_empty_team_rejected"
            ),
            "passed": check_raises(
                lambda: (
                    create_empty_bullpen_state(
                        ""
                    )
                )
            ),
            "detail": (
                "empty_team"
            ),
        }
    )

    invalid_role_state = (
        create_empty_bullpen_state(
            "CHC"
        )
    )

    invalid_role_state.role_by_pitcher = {
        "pitcher": "bad_role"
    }

    checks.append(
        {
            "check": (
                "invalid_role_rejected"
            ),
            "passed": check_raises(
                lambda: (
                    validate_bullpen_state(
                        invalid_role_state
                    )
                )
            ),
            "detail": "bad_role",
        }
    )

    invalid_hand_state = (
        create_empty_bullpen_state(
            "LAD"
        )
    )

    invalid_hand_state.handedness_by_pitcher = {
        "pitcher": "X"
    }

    checks.append(
        {
            "check": (
                "invalid_handedness_rejected"
            ),
            "passed": check_raises(
                lambda: (
                    validate_bullpen_state(
                        invalid_hand_state
                    )
                )
            ),
            "detail": (
                "bad_handedness"
            ),
        }
    )

    invalid_fatigue_state = (
        create_empty_bullpen_state(
            "SEA"
        )
    )

    invalid_fatigue_state.fatigue_by_pitcher = {
        "pitcher": 1.5
    }

    checks.append(
        {
            "check": (
                "invalid_fatigue_rejected"
            ),
            "passed": check_raises(
                lambda: (
                    validate_bullpen_state(
                        invalid_fatigue_state
                    )
                )
            ),
            "detail": (
                "fatigue_out_of_bounds"
            ),
        }
    )

    invalid_usage_log = (
        create_empty_bullpen_state(
            "BOS"
        )
    )

    invalid_usage_log.usage_log = (
        "bad"
    )

    checks.append(
        {
            "check": (
                "usage_log_type_checked"
            ),
            "passed": check_raises(
                lambda: (
                    validate_bullpen_state(
                        invalid_usage_log
                    )
                )
            ),
            "detail": (
                "usage_log_not_list"
            ),
        }
    )

    invalid_lists = (
        create_empty_bullpen_state(
            "ATL"
        )
    )

    invalid_lists.used_pitchers = (
        "bad"
    )

    checks.append(
        {
            "check": (
                "lists_type_checked"
            ),
            "passed": check_raises(
                lambda: (
                    validate_bullpen_state(
                        invalid_lists
                    )
                )
            ),
            "detail": (
                "used_pitchers_not_list"
            ),
        }
    )

    checks.append(
        {
            "check": (
                "candidate_mode_constants_present"
            ),
            "passed": (
                BULLPEN_MODE_CANDIDATE_LEVERAGE
                == "candidate_leverage"
            ),
            "detail": (
                BULLPEN_MODE_CANDIDATE_LEVERAGE
            ),
        }
    )

    checks.append(
        {
            "check": (
                "default_mode_off_confirmed"
            ),
            "passed": (
                BULLPEN_MODE_OFF
                == "off"
            ),
            "detail": (
                BULLPEN_MODE_OFF
            ),
        }
    )

    integration_absent = True

    for file_path in PRODUCTION_FILES:

        path = Path(file_path)

        if not path.exists():
            continue

        text = path.read_text()

        if "bullpen_state" in text:
            integration_absent = False

    checks.append(
        {
            "check": (
                "production_integration_absent"
            ),
            "passed": (
                integration_absent
            ),
            "detail": (
                integration_absent
            ),
        }
    )

    checks_df = pd.DataFrame(
        checks
    )

    checks_df.to_csv(
        OUTPUT_CHECKS,
        index=False,
    )

    failures = int(
        (~checks_df["passed"]).sum()
    )

    payload = {
        "diagnosis": (
            "bullpen_state_container_prototype_safe"
            if failures == 0
            else (
                "bullpen_state_container_prototype_failed"
            )
        ),
        "checks_passed": int(
            checks_df["passed"].sum()
        ),
        "checks_failed": failures,
        "candidate_mode": (
            BULLPEN_MODE_CANDIDATE_LEVERAGE
        ),
        "default_mode": (
            BULLPEN_MODE_OFF
        ),
        "production_integration_absent": (
            integration_absent
        ),
        "recommended_next_step": (
            "layer_6ae_bullpen_state_initialization_audit"
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
