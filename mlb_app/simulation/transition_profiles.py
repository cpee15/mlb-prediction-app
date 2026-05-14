from copy import deepcopy
from typing import Any, Dict, Optional

TRANSITION_PROFILES: Dict[str, Dict[str, Any]] = {
    "current": {
        "transition_mode": "current",
        "sac_fly_rate": 0.0,
        "double_play_rate": 0.0,
        "first_to_third_single_rate": 0.0,
        "first_to_home_double_rate": 0.0,
        "activate_by_default": False,
    },

    "baseline_candidate_6j": {
        "transition_mode": "realism_candidate",
        "sac_fly_rate": 0.30,
        "double_play_rate": 0.11,
        "first_to_third_single_rate": 0.30,
        "first_to_home_double_rate": 0.40,
        "activate_by_default": False,
    },

    "low_advancement": {
        "transition_mode": "realism_candidate",
        "sac_fly_rate": 0.20,
        "double_play_rate": 0.11,
        "first_to_third_single_rate": 0.20,
        "first_to_home_double_rate": 0.30,
        "activate_by_default": False,
    },
}

DEFAULT_TRANSITION_PROFILE = "current"

PREFERRED_CANDIDATE_TRANSITION_PROFILE = (
    "low_advancement"
)

def resolve_transition_profile(
    name: Optional[str] = None,
) -> Dict[str, Any]:

    profile_name = (
        name
        or DEFAULT_TRANSITION_PROFILE
    )

    if profile_name not in TRANSITION_PROFILES:

        raise ValueError(
            f"Unknown transition profile: "
            f"{profile_name}"
        )

    return deepcopy(
        TRANSITION_PROFILES[
            profile_name
        ]
    )
