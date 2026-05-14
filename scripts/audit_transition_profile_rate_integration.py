import csv
import json
import random
from pathlib import Path

from mlb_app.simulation.inning_simulator import (
    simulate_half_inning,
    advance_runners_realism_candidate,
)

from mlb_app.simulation.transition_profiles import (
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

#
# --------------------------------------------------
# 1. default equivalence
# --------------------------------------------------
#

default_equivalence_differences = 0

for seed in range(250):

    a = simulate_half_inning(
        probabilities,
        rng=random.Random(seed),
    )

    b = simulate_half_inning(
        probabilities,
        rng=random.Random(seed),
        transition_mode="current",
    )

    if a != b:

        default_equivalence_differences += 1

add_check(
    "default_equivalence_zero_diff",
    default_equivalence_differences == 0,
    default_equivalence_differences,
)

#
# --------------------------------------------------
# 2. current mode ignores rates
# --------------------------------------------------
#

extreme_rates = {
    "sac_fly_rate": 0.99,
    "double_play_rate": 0.99,
    "first_to_third_single_rate": 0.99,
    "first_to_home_double_rate": 0.99,
}

current_mode_rate_ignored_differences = 0

for seed in range(250):

    a = simulate_half_inning(
        probabilities,
        rng=random.Random(seed),
        transition_mode="current",
    )

    b = simulate_half_inning(
        probabilities,
        rng=random.Random(seed),
        transition_mode="current",
        transition_rates=extreme_rates,
    )

    if a != b:

        current_mode_rate_ignored_differences += 1

add_check(
    "current_mode_ignores_rates",
    current_mode_rate_ignored_differences == 0,
    current_mode_rate_ignored_differences,
)

#
# --------------------------------------------------
# 3. deterministic crafted proofs
# --------------------------------------------------
#

#
# sac fly proof
#

sac_zero = (
    advance_runners_realism_candidate(
        bases=(False, False, True),
        outcome="out",
        outs=0,
        rng=random.Random(1),
        transition_rates={
            "sac_fly_rate": 0.0,
        },
    )
)

sac_one = (
    advance_runners_realism_candidate(
        bases=(False, False, True),
        outcome="out",
        outs=0,
        rng=random.Random(1),
        transition_rates={
            "sac_fly_rate": 1.0,
        },
    )
)

sac_fly_rate_consumed = (
    sac_zero != sac_one
)

add_check(
    "sac_fly_rate_consumed",
    sac_fly_rate_consumed,
    {
        "zero": sac_zero,
        "one": sac_one,
    },
)

#
# double play proof
#

dp_zero = (
    advance_runners_realism_candidate(
        bases=(True, False, False),
        outcome="out",
        outs=0,
        rng=random.Random(1),
        transition_rates={
            "double_play_rate": 0.0,
        },
    )
)

dp_one = (
    advance_runners_realism_candidate(
        bases=(True, False, False),
        outcome="out",
        outs=0,
        rng=random.Random(1),
        transition_rates={
            "double_play_rate": 1.0,
        },
    )
)

double_play_rate_consumed = (
    dp_zero != dp_one
)

add_check(
    "double_play_rate_consumed",
    double_play_rate_consumed,
    {
        "zero": dp_zero,
        "one": dp_one,
    },
)

#
# first to third proof
#

ftt_zero = (
    advance_runners_realism_candidate(
        bases=(True, False, False),
        outcome="single",
        outs=0,
        rng=random.Random(1),
        transition_rates={
            "first_to_third_single_rate": 0.0,
        },
    )
)

ftt_one = (
    advance_runners_realism_candidate(
        bases=(True, False, False),
        outcome="single",
        outs=0,
        rng=random.Random(1),
        transition_rates={
            "first_to_third_single_rate": 1.0,
        },
    )
)

first_to_third_rate_consumed = (
    ftt_zero != ftt_one
)

add_check(
    "first_to_third_rate_consumed",
    first_to_third_rate_consumed,
    {
        "zero": ftt_zero,
        "one": ftt_one,
    },
)

#
# first to home on double proof
#

fth_zero = (
    advance_runners_realism_candidate(
        bases=(True, False, False),
        outcome="double",
        outs=0,
        rng=random.Random(1),
        transition_rates={
            "first_to_home_double_rate": 0.0,
        },
    )
)

fth_one = (
    advance_runners_realism_candidate(
        bases=(True, False, False),
        outcome="double",
        outs=0,
        rng=random.Random(1),
        transition_rates={
            "first_to_home_double_rate": 1.0,
        },
    )
)

first_to_home_double_rate_consumed = (
    fth_zero != fth_one
)

add_check(
    "first_to_home_double_rate_consumed",
    first_to_home_double_rate_consumed,
    {
        "zero": fth_zero,
        "one": fth_one,
    },
)

deterministic_rate_consumption_passed = all([
    sac_fly_rate_consumed,
    double_play_rate_consumed,
    first_to_third_rate_consumed,
    first_to_home_double_rate_consumed,
])

add_check(
    "deterministic_rate_consumption_passed",
    deterministic_rate_consumption_passed,
    deterministic_rate_consumption_passed,
)

#
# --------------------------------------------------
# 4. invalid rate rejection
# --------------------------------------------------
#

invalid_rates_rejected = True

invalid_payloads = [
    {
        "sac_fly_rate": -0.01,
    },
    {
        "double_play_rate": 1.01,
    },
    {
        "first_to_third_single_rate": "bad",
    },
]

for payload in invalid_payloads:

    try:

        simulate_half_inning(
            probabilities,
            transition_mode="realism_candidate",
            transition_rates=payload,
        )

        invalid_rates_rejected = False

    except ValueError:

        pass

add_check(
    "invalid_rates_rejected",
    invalid_rates_rejected,
    invalid_rates_rejected,
)

#
# --------------------------------------------------
# 5. registry immutability
# --------------------------------------------------
#

candidate_profile = (
    resolve_transition_profile(
        "low_advancement"
    )
)

candidate_profile[
    "sac_fly_rate"
] = 999

profile_registry_unchanged = (
    TRANSITION_PROFILES[
        "low_advancement"
    ]["sac_fly_rate"]
    != 999
)

add_check(
    "profile_registry_unchanged",
    profile_registry_unchanged,
    profile_registry_unchanged,
)

#
# --------------------------------------------------
# final diagnosis
# --------------------------------------------------
#

diagnosis = (
    "transition_profile_rates_integrated_safely"
)

if (
    default_equivalence_differences
    > 0
):

    diagnosis = (
        "transition_profile_rates_default_regression"
    )

elif (
    current_mode_rate_ignored_differences
    > 0
):

    diagnosis = (
        "transition_profile_rates_current_mode_leak"
    )

elif (
    deterministic_rate_consumption_passed
    is False
):

    diagnosis = (
        "transition_profile_rates_not_consumed"
    )

elif (
    invalid_rates_rejected
    is False
):

    diagnosis = (
        "transition_profile_rates_invalid_validation_failed"
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

    "default_equivalence_differences":
        default_equivalence_differences,

    "current_mode_rate_ignored_differences":
        current_mode_rate_ignored_differences,

    "sac_fly_rate_consumed":
        sac_fly_rate_consumed,

    "double_play_rate_consumed":
        double_play_rate_consumed,

    "first_to_third_rate_consumed":
        first_to_third_rate_consumed,

    "first_to_home_double_rate_consumed":
        first_to_home_double_rate_consumed,

    "deterministic_rate_consumption_passed":
        deterministic_rate_consumption_passed,

    "invalid_rates_rejected":
        invalid_rates_rejected,

    "profile_registry_unchanged":
        profile_registry_unchanged,

    "checks_passed":
        checks_passed,

    "checks_failed":
        checks_failed,

    "recommended_next_step":
        (
            "layer_6p_profile_rate_"
            "calibration_backtest"
        ),
}

with open(
    TMP_DIR
    / "transition_profile_rate_integration.json",
    "w",
) as f:

    json.dump(
        summary,
        f,
        indent=2,
    )

with open(
    TMP_DIR
    / "transition_profile_rate_integration_checks.csv",
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
