import csv
import json
import random
from pathlib import Path

from mlb_app.simulation.inning_simulator import (
    simulate_half_inning,
)

from mlb_app.simulation.transition_profiles import (
    DEFAULT_TRANSITION_PROFILE,
    PREFERRED_CANDIDATE_TRANSITION_PROFILE,
    TRANSITION_PROFILES,
    resolve_transition_profile,
)

TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

CHECKS = []

def add_check(name, passed, detail):

    CHECKS.append(
        {
            "check": name,
            "passed": passed,
            "detail": detail,
        }
    )

#
# required profiles
#

required_profiles = [
    "current",
    "baseline_candidate_6j",
    "low_advancement",
]

for profile in required_profiles:

    add_check(
        f"profile_exists_{profile}",
        profile in TRANSITION_PROFILES,
        profile,
    )

#
# preferred/default checks
#

add_check(
    "default_profile_current",
    DEFAULT_TRANSITION_PROFILE == "current",
    DEFAULT_TRANSITION_PROFILE,
)

add_check(
    "preferred_candidate_low_advancement",
    (
        PREFERRED_CANDIDATE_TRANSITION_PROFILE
        == "low_advancement"
    ),
    PREFERRED_CANDIDATE_TRANSITION_PROFILE,
)

#
# safety invariants
#

activate_any = False

for (
    profile_name,
    profile,
) in TRANSITION_PROFILES.items():

    if profile.get(
        "activate_by_default"
    ) is True:

        activate_any = True

    for key in [
        "sac_fly_rate",
        "double_play_rate",
        "first_to_third_single_rate",
        "first_to_home_double_rate",
    ]:

        value = profile.get(key)

        valid = (
            isinstance(value, (int, float))
            and 0.0 <= value <= 1.0
        )

        add_check(
            f"{profile_name}_{key}_bounds",
            valid,
            value,
        )

add_check(
    "no_profile_activates_by_default",
    activate_any is False,
    activate_any,
)

#
# transition mode checks
#

add_check(
    "current_mode_is_current",
    (
        TRANSITION_PROFILES[
            "current"
        ]["transition_mode"]
        == "current"
    ),
    TRANSITION_PROFILES[
        "current"
    ]["transition_mode"],
)

for profile_name in [
    "baseline_candidate_6j",
    "low_advancement",
]:

    add_check(
        f"{profile_name}_candidate_mode",
        (
            TRANSITION_PROFILES[
                profile_name
            ]["transition_mode"]
            == "realism_candidate"
        ),
        TRANSITION_PROFILES[
            profile_name
        ]["transition_mode"],
    )

#
# resolve behavior
#

resolved_default = (
    resolve_transition_profile()
)

resolved_none = (
    resolve_transition_profile(None)
)

resolved_low = (
    resolve_transition_profile(
        "low_advancement"
    )
)

add_check(
    "resolve_default_current",
    (
        resolved_default[
            "transition_mode"
        ]
        == "current"
    ),
    resolved_default,
)

add_check(
    "resolve_none_current",
    (
        resolved_none[
            "transition_mode"
        ]
        == "current"
    ),
    resolved_none,
)

add_check(
    "resolve_low_advancement",
    (
        resolved_low[
            "transition_mode"
        ]
        == "realism_candidate"
    ),
    resolved_low,
)

#
# copy safety
#

resolved_low[
    "sac_fly_rate"
] = 999

add_check(
    "resolved_profile_is_copy",
    (
        TRANSITION_PROFILES[
            "low_advancement"
        ]["sac_fly_rate"]
        != 999
    ),
    TRANSITION_PROFILES[
        "low_advancement"
    ]["sac_fly_rate"],
)

#
# unknown profile rejection
#

unknown_profile_rejected = False

try:

    resolve_transition_profile(
        "does_not_exist"
    )

except ValueError:

    unknown_profile_rejected = True

add_check(
    "unknown_profile_rejected",
    unknown_profile_rejected,
    unknown_profile_rejected,
)

#
# default simulation equivalence
#

probabilities = {
    "k": 0.22,
    "bb": 0.08,
    "hbp": 0.01,
    "single": 0.15,
    "double": 0.05,
    "triple": 0.01,
    "hr": 0.03,
    "reached_on_error": 0.01,
    "out": 0.44,
}

differences = 0

for seed in range(250):

    result_a = simulate_half_inning(
        probabilities,
        rng=random.Random(seed),
    )

    result_b = simulate_half_inning(
        probabilities,
        rng=random.Random(seed),
        transition_mode=(
            resolve_transition_profile()[
                "transition_mode"
            ]
        ),
    )

    if result_a != result_b:
        differences += 1

add_check(
    "default_equivalence_zero_diff",
    differences == 0,
    differences,
)

#
# final summary
#

checks_passed = sum(
    1
    for row in CHECKS
    if row["passed"]
)

checks_failed = sum(
    1
    for row in CHECKS
    if not row["passed"]
)

diagnosis = (
    "transition_profile_plumbing_safe"
)

if checks_failed > 0:

    diagnosis = (
        "transition_profile_plumbing_failed"
    )

summary = {
    "diagnosis":
        diagnosis,

    "default_profile":
        DEFAULT_TRANSITION_PROFILE,

    "preferred_candidate_profile":
        (
            PREFERRED_CANDIDATE_TRANSITION_PROFILE
        ),

    "profiles_available":
        sorted(
            TRANSITION_PROFILES.keys()
        ),

    "checks_passed":
        checks_passed,

    "checks_failed":
        checks_failed,

    "default_equivalence_differences":
        differences,

    "unknown_profile_rejected":
        unknown_profile_rejected,

    "activate_by_default_any_profile":
        activate_any,

    "recommended_next_step":
        (
            "layer_6n_candidate_profile_"
            "integration_audit"
        ),
}

with open(
    TMP_DIR
    / "transition_profile_plumbing.json",
    "w",
) as f:

    json.dump(
        summary,
        f,
        indent=2,
    )

with open(
    TMP_DIR
    / "transition_profile_plumbing_checks.csv",
    "w",
    newline="",
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=[
            "check",
            "passed",
            "detail",
        ],
    )

    writer.writeheader()
    writer.writerows(CHECKS)

print(
    json.dumps(
        summary,
        indent=2,
    )
)
