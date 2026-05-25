from __future__ import annotations

import csv
import json
import random
from pathlib import Path
from statistics import mean

from mlb_app.simulation.inning_simulator import simulate_half_inning


OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

PROBABILITIES = {
    "k": 0.225,
    "bb": 0.085,
    "hbp": 0.011,
    "single": 0.145,
    "double": 0.045,
    "triple": 0.004,
    "hr": 0.030,
    "reached_on_error": 0.007,
    "out": 0.448,
}


def core_result(result):
    return {
        "runs": result["runs"],
        "plate_appearances": result["plate_appearances"],
        "outcomes": result["outcomes"],
        "ended_by_max_pa": result["ended_by_max_pa"],
    }


equivalence_failures = []

for seed in range(250):
    default_result = simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(seed),
    )

    explicit_default_result = simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(seed),
        initial_bases=(False, False, False),
        initial_outs=0,
    )

    if core_result(default_result) != core_result(explicit_default_result):
        equivalence_failures.append(
            {
                "seed": seed,
                "default": core_result(default_result),
                "explicit_default": core_result(explicit_default_result),
            }
        )


determinism_failures = []

for seed in range(100):
    first = simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(seed),
        initial_bases=(False, True, False),
        initial_outs=0,
    )

    second = simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(seed),
        initial_bases=(False, True, False),
        initial_outs=0,
    )

    if core_result(first) != core_result(second):
        determinism_failures.append({"seed": seed})


empty_runs = []
runner_second_runs = []
one_out_runs = []

for seed in range(1000):
    empty = simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(seed),
    )

    runner_second = simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(seed),
        initial_bases=(False, True, False),
        initial_outs=0,
    )

    one_out = simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(seed),
        initial_bases=(False, False, False),
        initial_outs=1,
    )

    empty_runs.append(empty["runs"])
    runner_second_runs.append(runner_second["runs"])
    one_out_runs.append(one_out["runs"])


invalid_checks = []

try:
    simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(1),
        initial_bases=(False, True),
    )
    invalid_checks.append(
        {
            "case": "invalid_initial_bases_length",
            "passed": False,
        }
    )
except ValueError:
    invalid_checks.append(
        {
            "case": "invalid_initial_bases_length",
            "passed": True,
        }
    )

try:
    simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(1),
        initial_outs=3,
    )
    invalid_checks.append(
        {
            "case": "invalid_initial_outs",
            "passed": False,
        }
    )
except ValueError:
    invalid_checks.append(
        {
            "case": "invalid_initial_outs",
            "passed": True,
        }
    )


sample_runner_second = simulate_half_inning(
    PROBABILITIES,
    rng=random.Random(42),
    initial_bases=(False, True, False),
    initial_outs=0,
)

sample_one_out = simulate_half_inning(
    PROBABILITIES,
    rng=random.Random(42),
    initial_bases=(False, False, False),
    initial_outs=1,
)

default_equivalence_passed = len(equivalence_failures) == 0
determinism_passed = len(determinism_failures) == 0
invalid_input_passed = all(check["passed"] for check in invalid_checks)

runner_second_mean = mean(runner_second_runs)
empty_mean = mean(empty_runs)
one_out_mean = mean(one_out_runs)

state_effect_passed = (
    runner_second_mean > empty_mean
    and one_out_mean < empty_mean
)

if not default_equivalence_passed:
    diagnosis = "initial_state_half_inning_default_regression"
elif not invalid_input_passed:
    diagnosis = "initial_state_half_inning_invalid_input_failed"
elif not state_effect_passed:
    diagnosis = "initial_state_half_inning_state_effect_missing"
elif not determinism_passed:
    diagnosis = "initial_state_half_inning_audit_failed"
else:
    diagnosis = "initial_state_half_inning_backward_compatible"


summary = {
    "diagnosis": diagnosis,
    "default_equivalence_passed": default_equivalence_passed,
    "equivalence_failures": len(equivalence_failures),
    "determinism_passed": determinism_passed,
    "determinism_failures": len(determinism_failures),
    "invalid_input_passed": invalid_input_passed,
    "state_effect_passed": state_effect_passed,
    "empty_bases_zero_outs_mean_runs": round(empty_mean, 6),
    "runner_on_second_zero_outs_mean_runs": round(runner_second_mean, 6),
    "empty_bases_one_out_mean_runs": round(one_out_mean, 6),
    "sample_runner_second_initial_bases": sample_runner_second["initial_bases"],
    "sample_runner_second_initial_outs": sample_runner_second["initial_outs"],
    "sample_runner_second_ending_bases": sample_runner_second["ending_bases"],
    "sample_runner_second_ending_outs": sample_runner_second["ending_outs"],
    "sample_one_out_initial_bases": sample_one_out["initial_bases"],
    "sample_one_out_initial_outs": sample_one_out["initial_outs"],
    "sample_one_out_ending_bases": sample_one_out["ending_bases"],
    "sample_one_out_ending_outs": sample_one_out["ending_outs"],
    "recommended_next_step": (
        "layer_6g_true_ghost_runner_extras_prototype"
        if diagnosis == "initial_state_half_inning_backward_compatible"
        else "repair_initial_state_half_inning_patch"
    ),
}

rows = [
    {
        "check": "default_equivalence",
        "passed": default_equivalence_passed,
        "details": f"{len(equivalence_failures)} failures across 250 seeds",
    },
    {
        "check": "determinism",
        "passed": determinism_passed,
        "details": f"{len(determinism_failures)} failures across 100 seeds",
    },
    {
        "check": "invalid_input",
        "passed": invalid_input_passed,
        "details": json.dumps(invalid_checks),
    },
    {
        "check": "runner_on_second_effect",
        "passed": runner_second_mean > empty_mean,
        "details": f"{runner_second_mean:.6f} > {empty_mean:.6f}",
    },
    {
        "check": "one_out_effect",
        "passed": one_out_mean < empty_mean,
        "details": f"{one_out_mean:.6f} < {empty_mean:.6f}",
    },
]

with (OUTPUT_DIR / "initial_state_half_inning_audit.json").open("w") as handle:
    json.dump(summary, handle, indent=2)

with (OUTPUT_DIR / "initial_state_half_inning_audit.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=["check", "passed", "details"],
    )
    writer.writeheader()
    writer.writerows(rows)

print(json.dumps(summary, indent=2))
