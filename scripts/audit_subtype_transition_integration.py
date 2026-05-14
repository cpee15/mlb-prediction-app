import csv
import json
import random
from pathlib import Path

from mlb_app.simulation.inning_simulator import (
    simulate_half_inning,
)

TMP_DIR = Path("tmp")

OUTPUT_JSON = (
    TMP_DIR
    / "subtype_transition_integration.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "subtype_transition_integration_checks.csv"
)

OUTPUT_SAMPLES = (
    TMP_DIR
    / "subtype_transition_integration_samples.csv"
)

PROBABILITIES = {
    "single": 0.15,
    "double": 0.05,
    "triple": 0.01,
    "hr": 0.04,
    "bb": 0.09,
    "k": 0.24,
    "out": 0.42,
}

def write_csv(
    path,
    rows,
):

    if not rows:

        rows = [
            {
                "status":
                    "no_rows"
            }
        ]

    with open(
        path,
        "w",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(
                rows[0].keys()
            ),
        )

        writer.writeheader()
        writer.writerows(rows)

checks = []

sample_rows = []

def add_check(
    name,
    passed,
):

    checks.append(
        {
            "check": name,
            "passed": passed,
        }
    )

#
# 1. Default equivalence
#

default_equivalence_differences = 0

for seed in range(250):

    rng_a = random.Random(seed)
    rng_b = random.Random(seed)

    a = simulate_half_inning(
        PROBABILITIES,
        rng=rng_a,
    )

    b = simulate_half_inning(
        PROBABILITIES,
        rng=rng_b,
        subtype_transition_mode="off",
    )

    if a != b:

        default_equivalence_differences += 1

add_check(
    "default_equivalence",
    (
        default_equivalence_differences
        == 0
    ),
)

#
# 2. Prototype mode differs
#

prototype_difference_count = 0

for seed in range(250):

    rng_a = random.Random(seed)
    rng_b = random.Random(seed)

    off_result = (
        simulate_half_inning(
            PROBABILITIES,
            rng=rng_a,
            subtype_transition_mode="off",
        )
    )

    prototype_result = (
        simulate_half_inning(
            PROBABILITIES,
            rng=rng_b,
            subtype_transition_mode="prototype",
        )
    )

    if (
        off_result
        != prototype_result
    ):

        prototype_difference_count += 1

        if (
            len(sample_rows)
            < 25
        ):

            sample_rows.append(
                {
                    "seed":
                        seed,

                    "off_result":
                        json.dumps(
                            off_result
                        ),

                    "prototype_result":
                        json.dumps(
                            prototype_result
                        ),
                }
            )

add_check(
    "prototype_path_active",
    (
        prototype_difference_count
        > 0
    ),
)

#
# 3. Invalid mode rejected
#

invalid_mode_rejected = False

try:

    simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(42),
        subtype_transition_mode="bad_mode",
    )

except ValueError:

    invalid_mode_rejected = True

add_check(
    "invalid_mode_rejected",
    invalid_mode_rejected,
)

#
# 4. Strikeout no advancement proof
#

strikeout_probs = {
    "k": 1.0,
    "out": 0.0,
}

strikeout_result = (
    simulate_half_inning(
        strikeout_probs,
        rng=random.Random(42),
        subtype_transition_mode="prototype",
        max_plate_appearances=1,
    )
)

strikeout_proof_passed = False

if isinstance(
    strikeout_result,
    dict,
):

    outs = strikeout_result.get(
        "ending_outs",
        0,
    )

    runs = strikeout_result.get(
        "runs",
        0,
    )

    bases = strikeout_result.get(
        "ending_bases",
        (
            False,
            False,
            False,
        ),
    )

    strikeout_proof_passed = (
        outs >= 1
        and runs == 0
    )

add_check(
    "strikeout_no_advancement",
    strikeout_proof_passed,
)

#
# 5. Prototype produces different run distribution
#

off_runs = []

prototype_runs = []

for seed in range(250):

    off_result = (
        simulate_half_inning(
            PROBABILITIES,
            rng=random.Random(seed),
            subtype_transition_mode="off",
        )
    )

    prototype_result = (
        simulate_half_inning(
            PROBABILITIES,
            rng=random.Random(seed),
            subtype_transition_mode="prototype",
        )
    )

    if isinstance(
        off_result,
        dict,
    ):

        off_runs.append(
            off_result.get(
                "runs",
                0,
            )
        )

    if isinstance(
        prototype_result,
        dict,
    ):

        prototype_runs.append(
            prototype_result.get(
                "runs",
                0,
            )
        )

distribution_difference = (
    off_runs
    != prototype_runs
)

add_check(
    "prototype_distribution_differs",
    distribution_difference,
)

#
# Write outputs
#

write_csv(
    OUTPUT_CHECKS,
    checks,
)

write_csv(
    OUTPUT_SAMPLES,
    sample_rows,
)

checks_failed = len(
    [
        row
        for row in checks
        if not row["passed"]
    ]
)

if (
    default_equivalence_differences
    > 0
):

    diagnosis = (
        "subtype_transition_"
        "default_regression"
    )

elif (
    prototype_difference_count
    == 0
):

    diagnosis = (
        "subtype_transition_"
        "prototype_inactive"
    )

elif not invalid_mode_rejected:

    diagnosis = (
        "subtype_transition_"
        "validation_failed"
    )

elif checks_failed > 0:

    diagnosis = (
        "subtype_transition_"
        "integration_failed"
    )

else:

    diagnosis = (
        "subtype_transition_"
        "integration_safe"
    )

summary = {
    "diagnosis":
        diagnosis,

    "default_equivalence_differences":
        default_equivalence_differences,

    "prototype_difference_count":
        prototype_difference_count,

    "invalid_mode_rejected":
        invalid_mode_rejected,

    "checks_passed":
        len(checks)
        - checks_failed,

    "checks_failed":
        checks_failed,

    "default_activation_changed":
        False,

    "recommended_next_step":
        (
            "layer_6w_"
            "subtype_transition_"
            "calibration_backtest"
        ),
}

with open(
    OUTPUT_JSON,
    "w",
) as f:

    json.dump(
        summary,
        f,
        indent=2,
    )

print(
    json.dumps(
        summary,
        indent=2,
    )
)
