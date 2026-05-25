from dataclasses import asdict
from dataclasses import dataclass
from typing import Optional


BULLPEN_MODE_OFF = "off"

BULLPEN_MODE_CANDIDATE_LEVERAGE = (
    "candidate_leverage"
)


VALID_RELIEVER_ROLES = {
    "closer",
    "setup",
    "high_leverage",
    "middle_relief",
    "long_relief",
    "low_leverage",
    "unknown",
}


VALID_HANDEDNESS = {
    "L",
    "R",
    "S",
    "unknown",
}


@dataclass
class BullpenState:
    team_id: str
    available_relievers: list
    used_pitchers: list
    current_pitcher: Optional[str]
    fatigue_by_pitcher: dict
    role_by_pitcher: dict
    handedness_by_pitcher: dict
    usage_log: list


def create_empty_bullpen_state(
    team_id: str,
) -> BullpenState:

    if not isinstance(team_id, str):
        raise ValueError(
            "team_id must be a string"
        )

    if not team_id.strip():
        raise ValueError(
            "team_id cannot be empty"
        )

    return BullpenState(
        team_id=team_id,
        available_relievers=[],
        used_pitchers=[],
        current_pitcher=None,
        fatigue_by_pitcher={},
        role_by_pitcher={},
        handedness_by_pitcher={},
        usage_log=[],
    )


def validate_bullpen_state(
    state: BullpenState,
) -> bool:

    if not isinstance(
        state,
        BullpenState,
    ):
        raise ValueError(
            "state must be BullpenState"
        )

    if not isinstance(
        state.team_id,
        str,
    ):
        raise ValueError(
            "team_id must be string"
        )

    if not state.team_id.strip():
        raise ValueError(
            "team_id cannot be empty"
        )

    if not isinstance(
        state.available_relievers,
        list,
    ):
        raise ValueError(
            "available_relievers must be list"
        )

    if not isinstance(
        state.used_pitchers,
        list,
    ):
        raise ValueError(
            "used_pitchers must be list"
        )

    if not isinstance(
        state.usage_log,
        list,
    ):
        raise ValueError(
            "usage_log must be list"
        )

    for role in (
        state.role_by_pitcher.values()
    ):

        if role not in VALID_RELIEVER_ROLES:
            raise ValueError(
                f"invalid role: {role}"
            )

    for handedness in (
        state.handedness_by_pitcher.values()
    ):

        if handedness not in VALID_HANDEDNESS:
            raise ValueError(
                f"invalid handedness: "
                f"{handedness}"
            )

    for fatigue in (
        state.fatigue_by_pitcher.values()
    ):

        if not isinstance(
            fatigue,
            (int, float),
        ):
            raise ValueError(
                "fatigue must be numeric"
            )

        if fatigue < 0.0 or fatigue > 1.0:
            raise ValueError(
                "fatigue must be between "
                "0.0 and 1.0"
            )

    return True


def bullpen_state_to_dict(
    state: BullpenState,
) -> dict:

    validate_bullpen_state(state)

    return asdict(state)


def bullpen_state_from_dict(
    payload: dict,
) -> BullpenState:

    if not isinstance(
        payload,
        dict,
    ):
        raise ValueError(
            "payload must be dict"
        )

    state = BullpenState(
        team_id=payload.get(
            "team_id",
            "",
        ),
        available_relievers=payload.get(
            "available_relievers",
            [],
        ),
        used_pitchers=payload.get(
            "used_pitchers",
            [],
        ),
        current_pitcher=payload.get(
            "current_pitcher",
        ),
        fatigue_by_pitcher=payload.get(
            "fatigue_by_pitcher",
            {},
        ),
        role_by_pitcher=payload.get(
            "role_by_pitcher",
            {},
        ),
        handedness_by_pitcher=payload.get(
            "handedness_by_pitcher",
            {},
        ),
        usage_log=payload.get(
            "usage_log",
            [],
        ),
    )

    validate_bullpen_state(state)

    return state
