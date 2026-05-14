import csv
import json
import random
from pathlib import Path

from mlb_app.simulation.subtype_transitions import (
    advance_runners_by_subtype,
)

TMP_DIR = Path("tmp")

OUTPUT_JSON = (
    TMP_DIR
    / "subtype_transition_prototype.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "subtype_transition_prototype_checks.csv"
)

OUTPUT_ORDERING = (
    TMP_DIR
    / "subtype_transition_scoring_ordering.csv"
)

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
# Strikeout deterministic
#

bases, runs, outs = (
    advance_runners_by_subtype(
        (
            True,
            True,
            True,
        ),
        "strikeout",
        0,
        rng=random.Random(42),
    )
)

add_check(
    "strikeout_bases_hold",
    bases == (
        True,
        True,
        True,
    ),
)

add_check(
    "strikeout_no_runs",
    runs == 0,
)

add_check(
    "strikeout_one_out",
    outs == 1,
)

#
# Other out deterministic
#

bases, runs, outs = (
    advance_runners_by_subtype(
        (
            True,
            True,
            True,
        ),
        "other_out",
        0,
        rng=random.Random(42),
    )
)

add_check(
    "other_out_bases_hold",
    bases == (
        True,
        True,
        True,
    ),
)

add_check(
    "other_out_no_runs",
    runs == 0,
)

add_check(
    "other_out_one_out",
    outs == 1,
)

#
# Groundout DP
#

bases, runs, outs = (
    advance_runners_by_subtype(
        (
            True,
            False,
            False,
        ),
        "groundout",
        0,
        rng=random.Random(42),
        transition_rates={
            (
                "groundout_"
                "double_play_rate"
            ): 1.0,
        },
    )
)

add_check(
    "groundout_double_play",
    (
        outs == 2
        and bases[0] is False
    ),
)

#
# Groundout no DP
#

bases, runs, outs = (
    advance_runners_by_subtype(
        (
            True,
            False,
            False,
        ),
        "groundout",
        0,
        rng=random.Random(42),
        transition_rates={
            (
                "groundout_"
                "double_play_rate"
            ): 0.0,
        },
    )
)

add_check(
    "groundout_no_double_play",
    (
        outs == 1
        and bases[0] is False
    ),
)

#
# Groundout runner scores
#

bases, runs, outs = (
    advance_runners_by_subtype(
        (
            False,
            False,
            True,
        ),
        "groundout",
        0,
        rng=random.Random(42),
        transition_rates={
            (
                "groundout_runner_"
                "third_scores_rate"
            ): 1.0,
        },
    )
)

add_check(
    "groundout_runner_scores",
    (
        runs == 1
        and bases[2] is False
    ),
)

#
# Flyout runner scores
#

bases, runs, outs = (
    advance_runners_by_subtype(
        (
            False,
            False,
            True,
        ),
        "flyout",
        0,
        rng=random.Random(42),
        transition_rates={
            (
                "flyout_runner_"
                "third_scores_rate"
            ): 1.0,
        },
    )
)

add_check(
    "flyout_runner_scores",
    (
        runs == 1
        and bases[2] is False
    ),
)

bases, runs, outs = (
    advance_runners_by_subtype(
        (
            False,
            False,
            True,
        ),
        "flyout",
        0,
        rng=random.Random(42),
        transition_rates={
            (
                "flyout_runner_"
                "third_scores_rate"
            ): 0.0,
        },
    )
)

add_check(
    "flyout_runner_holds",
    (
        runs == 0
        and bases[2] is True
    ),
)

#
# Lineout/popout
#

bases, runs, outs = (
    advance_runners_by_subtype(
        (
            False,
            False,
            True,
        ),
        "lineout_popout",
        0,
        rng=random.Random(42),
        transition_rates={
            (
                "lineout_popout_"
                "runner_third_"
                "scores_rate"
            ): 1.0,
        },
    )
)

add_check(
    "lineout_runner_scores",
    runs == 1,
)

bases, runs, outs = (
    advance_runners_by_subtype(
        (
            False,
            False,
            True,
        ),
        "lineout_popout",
        0,
        rng=random.Random(42),
        transition_rates={
            (
                "lineout_popout_"
                "runner_third_"
                "scores_rate"
            ): 0.0,
        },
    )
)

add_check(
    "lineout_runner_holds",
    runs == 0,
)

#
# Invalid subtype
#

invalid_subtype_passed = False

try:

    advance_runners_by_subtype(
        (
            False,
            False,
            False,
        ),
        "bad_subtype",
        0,
    )

except ValueError:

    invalid_subtype_passed = True

add_check(
    "invalid_subtype_rejected",
    invalid_subtype_passed,
)

#
# Invalid rate
#

invalid_rate_cases = [
    {
        "bad_rate": 0.5
    },

    {
        (
            "groundout_"
            "double_play_rate"
        ): -1
    },

    {
        (
            "groundout_"
            "double_play_rate"
        ): 2
    },

    {
        (
            "groundout_"
            "double_play_rate"
        ): "bad"
    },
]

invalid_rate_passes = 0

for case in invalid_rate_cases:

    try:

        advance_runners_by_subtype(
            (
                False,
                False,
                False,
            ),
            "groundout",
            0,
            transition_rates=case,
        )

    except ValueError:

        invalid_rate_passes += 1

add_check(
    "invalid_rates_rejected",
    (
        invalid_rate_passes
        == len(
            invalid_rate_cases
        )
    ),
)

#
# Relative realism ordering
#

ordering_rows = []

subtypes = [
    "strikeout",
    "groundout",
    "flyout",
    "lineout_popout",
    "other_out",
]

scoring_rates = {}

N = 1000

for subtype in subtypes:

    runs_scored = 0

    for i in range(N):

        _, runs, _ = (
            advance_runners_by_subtype(
                (
                    False,
                    False,
                    True,
                ),
                subtype,
                0,
                rng=random.Random(i),
            )
        )

        if runs > 0:

            runs_scored += 1

    rate = (
        runs_scored / N
    )

    scoring_rates[
        subtype
    ] = rate

    ordering_rows.append(
        {
            "subtype":
                subtype,

            "scoring_rate":
                rate,
        }
    )

ordering_passed = (
    scoring_rates[
        "flyout"
    ]
    >
    scoring_rates[
        "groundout"
    ]
    >
    scoring_rates[
        "lineout_popout"
    ]
    >=
    scoring_rates[
        "strikeout"
    ]
    and
    scoring_rates[
        "lineout_popout"
    ]
    >=
    scoring_rates[
        "other_out"
    ]
)

add_check(
    "relative_ordering_passed",
    ordering_passed,
)

write_csv(
    OUTPUT_CHECKS,
    checks,
)

write_csv(
    OUTPUT_ORDERING,
    ordering_rows,
)

checks_failed = len(
    [
        row
        for row in checks
        if not row["passed"]
    ]
)

if not ordering_passed:

    diagnosis = (
        "subtype_transition_"
        "prototype_ordering_"
        "failed"
    )

elif checks_failed > 0:

    diagnosis = (
        "subtype_transition_"
        "prototype_rule_failed"
    )

else:

    diagnosis = (
        "subtype_transition_"
        "prototype_safe"
    )

summary = {
    "diagnosis":
        diagnosis,

    "checks_passed":
        len(checks)
        - checks_failed,

    "checks_failed":
        checks_failed,

    "invalid_input_rejections_passed":
        (
            invalid_subtype_passed
            and (
                invalid_rate_passes
                == len(
                    invalid_rate_cases
                )
            )
        ),

    "relative_ordering_passed":
        ordering_passed,

    "default_activation_changed":
        False,

    "recommended_next_step":
        (
            "layer_6v_"
            "subtype_transition_"
            "integration_audit"
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
