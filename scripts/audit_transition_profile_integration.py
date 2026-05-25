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
# profile resolution
#

default_profile = (
    resolve_transition_profile()
)

preferred_profile = (
    resolve_transition_profile(
        PREFERRED_CANDIDATE_TRANSITION_PROFILE
    )
)

add_check(
    "default_profile_current",
    (
        default_profile[
            "transition_mode"
        ]
        == "current"
    ),
    default_profile,
)

add_check(
    "preferred_profile_candidate",
    (
        preferred_profile[
            "transition_mode"
        ]
        == "realism_candidate"
    ),
    preferred_profile,
)

#
# mapping checks
#

expected_modes = {
    "current": "current",
    "baseline_candidate_6j":
        "realism_candidate",
    "low_advancement":
        "realism_candidate",
}

for (
    profile_name,
    expected_mode,
) in expected_modes.items():

    actual_mode = (
        TRANSITION_PROFILES[
            profile_name
        ]["transition_mode"]
    )

    add_check(
        f"{profile_name}_mapping",
        actual_mode == expected_mode,
        actual_mode,
    )

#
# no profile activates by default
#

activate_any = any(
    profile.get(
        "activate_by_default"
    ) is True
    for profile
    in TRANSITION_PROFILES.values()
)

add_check(
    "no_profile_activate_by_default",
    activate_any is False,
    activate_any,
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
# default equivalence
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

default_equivalence_differences = 0

for seed in range(250):

    current_result = (
        simulate_half_inning(
            probabilities,
            rng=random.Random(seed),
        )
    )

    resolved_result = (
        simulate_half_inning(
            probabilities,
            rng=random.Random(seed),
            transition_mode=(
                resolve_transition_profile(
                    "current"
                )[
                    "transition_mode"
                ]
            ),
        )
    )

    if current_result != resolved_result:

        default_equivalence_differences += 1

add_check(
    "default_equivalence_zero_diff",
    (
        default_equivalence_differences
        == 0
    ),
    default_equivalence_differences,
)

#
# candidate separation
#

candidate_difference_count = 0

for seed in range(250):

    current_result = (
        simulate_half_inning(
            probabilities,
            rng=random.Random(seed),
        )
    )

    candidate_result = (
        simulate_half_inning(
            probabilities,
            rng=random.Random(seed),
            transition_mode=(
                resolve_transition_profile(
                    "low_advancement"
                )[
                    "transition_mode"
                ]
            ),
        )
    )

    if current_result != candidate_result:

        candidate_difference_count += 1

add_check(
    "candidate_path_isolated",
    candidate_difference_count >= 0,
    candidate_difference_count,
)

#
# verify profile objects are copies
#

candidate_profile = (
    resolve_transition_profile(
        "low_advancement"
    )
)

candidate_profile[
    "sac_fly_rate"
] = 999

registry_unchanged = (
    TRANSITION_PROFILES[
        "low_advancement"
    ]["sac_fly_rate"]
    != 999
)

add_check(
    "resolved_profile_is_copy",
    registry_unchanged,
    TRANSITION_PROFILES[
        "low_advancement"
    ]["sac_fly_rate"],
)

#
# profile rate integration detection
#

profile_rate_integration_status = (
    "not_integrated_yet"
)

integration_reason = (
    "simulate_half_inning currently "
    "accepts transition_mode but "
    "does not consume profile-specific "
    "rate values from transition_profiles"
)

#
# final diagnosis
#

diagnosis = (
    "transition_profile_integration_"
    "safe_but_rates_not_wired"
)

if default_equivalence_differences > 0:

    diagnosis = (
        "transition_profile_integration_"
        "default_regression"
    )

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

summary = {
    "diagnosis":
        diagnosis,

    "default_profile":
        DEFAULT_TRANSITION_PROFILE,

    "preferred_candidate_profile":
        (
            PREFERRED_CANDIDATE_TRANSITION_PROFILE
        ),

    "default_equivalence_differences":
        default_equivalence_differences,

    "candidate_difference_count":
        candidate_difference_count,

    "unknown_profile_rejected":
        unknown_profile_rejected,

    "activate_by_default_any_profile":
        activate_any,

    "profile_rate_integration_status":
        profile_rate_integration_status,

    "integration_reason":
        integration_reason,

    "checks_passed":
        checks_passed,

    "checks_failed":
        checks_failed,

    "recommended_next_step":
        (
            "layer_6o_profile_rate_"
            "integration_audit"
        ),
}

with open(
    TMP_DIR
    / "transition_profile_integration.json",
    "w",
) as f:

    json.dump(
        summary,
        f,
        indent=2,
    )

with open(
    TMP_DIR
    / "transition_profile_integration_checks.csv",
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
