from __future__ import annotations

import csv
import json
import random
from pathlib import Path
from statistics import mean
from typing import Dict, List, Tuple


OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

SEED = 42
SIMULATIONS = 5000

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

OUTCOMES = list(PROBABILITIES.keys())

SAC_FLY_RATE = 0.30
DOUBLE_PLAY_RATE = 0.11
FIRST_TO_THIRD_SINGLE_RATE = 0.30
FIRST_TO_HOME_DOUBLE_RATE = 0.40

BASE_STATES: List[Tuple[str, Tuple[bool, bool, bool]]] = [
    ("empty", (False, False, False)),
    ("runner_on_first", (True, False, False)),
    ("runner_on_second", (False, True, False)),
    ("runner_on_third", (False, False, True)),
    ("first_and_second", (True, True, False)),
    ("first_and_third", (True, False, True)),
    ("second_and_third", (False, True, True)),
    ("bases_loaded", (True, True, True)),
]


def normalize(probabilities: Dict[str, float]) -> Dict[str, float]:
    total = sum(probabilities.values())
    return {k: v / total for k, v in probabilities.items()}


def sample_pa_outcome(
    probabilities: Dict[str, float],
    rng: random.Random,
) -> str:
    probs = normalize(probabilities)

    draw = rng.random()
    cumulative = 0.0

    for outcome in OUTCOMES:
        cumulative += probs[outcome]
        if draw <= cumulative:
            return outcome

    return "out"


def advance_runners_current(
    bases: Tuple[bool, bool, bool],
    outcome: str,
) -> Tuple[Tuple[bool, bool, bool], int]:

    first, second, third = bases
    runs = 0

    if outcome in {"k", "out"}:
        return bases, 0

    if outcome in {"bb", "hbp"}:
        if first and second and third:
            runs += 1

        new_third = third or (first and second)
        new_second = second or first
        new_first = True

        return (new_first, new_second, new_third), runs

    if outcome in {"single", "reached_on_error"}:
        runs += int(third)

        new_third = second
        new_second = first
        new_first = True

        return (new_first, new_second, new_third), runs

    if outcome == "double":
        runs += int(third) + int(second)

        new_third = first
        new_second = True
        new_first = False

        return (new_first, new_second, new_third), runs

    if outcome == "triple":
        runs += int(first) + int(second) + int(third)

        return (False, False, True), runs

    if outcome == "hr":
        runs += int(first) + int(second) + int(third) + 1

        return (False, False, False), runs

    return bases, runs


def advance_runners_prototype(
    bases: Tuple[bool, bool, bool],
    outcome: str,
    outs: int,
    rng: random.Random,
) -> Tuple[Tuple[bool, bool, bool], int, int]:

    first, second, third = bases
    runs = 0
    extra_outs = 0

    if outcome in {"k", "out"}:

        if first and outs < 2 and rng.random() < DOUBLE_PLAY_RATE:
            first = False
            extra_outs += 1

        if third and outs < 2 and rng.random() < SAC_FLY_RATE:
            third = False
            runs += 1

        return (first, second, third), runs, extra_outs

    if outcome in {"bb", "hbp"}:
        if first and second and third:
            runs += 1

        new_third = third or (first and second)
        new_second = second or first
        new_first = True

        return (new_first, new_second, new_third), runs, extra_outs

    if outcome in {"single", "reached_on_error"}:

        if third:
            runs += 1
            third = False

        new_third = False

        if second:
            if rng.random() < 1.0:
                runs += 1
            else:
                new_third = True

        if first:
            if rng.random() < FIRST_TO_THIRD_SINGLE_RATE:
                new_third = True
                new_second = False
            else:
                new_second = True
        else:
            new_second = False

        new_first = True

        return (new_first, new_second, new_third), runs, extra_outs

    if outcome == "double":

        if third:
            runs += 1

        if second:
            runs += 1

        if first:
            if rng.random() < FIRST_TO_HOME_DOUBLE_RATE:
                runs += 1
                new_third = False
            else:
                new_third = True
        else:
            new_third = False

        new_second = True
        new_first = False

        return (new_first, new_second, new_third), runs, extra_outs

    if outcome == "triple":
        runs += int(first) + int(second) + int(third)

        return (False, False, True), runs, extra_outs

    if outcome == "hr":
        runs += int(first) + int(second) + int(third) + 1

        return (False, False, False), runs, extra_outs

    return bases, runs, extra_outs


def simulate_half_inning_current(
    probabilities: Dict[str, float],
    initial_bases: Tuple[bool, bool, bool],
    initial_outs: int,
    rng: random.Random,
    max_pa: int = 30,
) -> Dict[str, float]:

    bases = initial_bases
    outs = initial_outs
    runs = 0
    pa = 0

    while outs < 3 and pa < max_pa:
        outcome = sample_pa_outcome(probabilities, rng)

        pa += 1

        if outcome in {"k", "out"}:
            outs += 1
            continue

        bases, scored = advance_runners_current(bases, outcome)
        runs += scored

    return {
        "runs": runs,
        "plate_appearances": pa,
    }


def simulate_half_inning_prototype(
    probabilities: Dict[str, float],
    initial_bases: Tuple[bool, bool, bool],
    initial_outs: int,
    rng: random.Random,
    max_pa: int = 30,
) -> Dict[str, float]:

    bases = initial_bases
    outs = initial_outs
    runs = 0
    pa = 0

    while outs < 3 and pa < max_pa:
        outcome = sample_pa_outcome(probabilities, rng)

        pa += 1

        if outcome in {"k", "out"}:
            bases, scored, extra_outs = advance_runners_prototype(
                bases,
                outcome,
                outs,
                rng,
            )

            runs += scored
            outs += 1 + extra_outs
            continue

        bases, scored, extra_outs = advance_runners_prototype(
            bases,
            outcome,
            outs,
            rng,
        )

        runs += scored

    return {
        "runs": runs,
        "plate_appearances": pa,
    }


def estimate_runs(
    mode: str,
    bases: Tuple[bool, bool, bool],
    outs: int,
) -> Dict[str, float]:

    runs = []
    pas = []

    for idx in range(SIMULATIONS):
        rng = random.Random(SEED + idx)

        if mode == "current":
            result = simulate_half_inning_current(
                PROBABILITIES,
                bases,
                outs,
                rng,
            )
        else:
            result = simulate_half_inning_prototype(
                PROBABILITIES,
                bases,
                outs,
                rng,
            )

        runs.append(result["runs"])
        pas.append(result["plate_appearances"])

    return {
        "expected_runs": round(mean(runs), 6),
        "expected_pas": round(mean(pas), 6),
    }


rows = []

for mode in ["current", "prototype"]:

    for outs in [0, 1, 2]:

        for state_name, bases in BASE_STATES:

            metrics = estimate_runs(
                mode,
                bases,
                outs,
            )

            rows.append(
                {
                    "mode": mode,
                    "state_name": state_name,
                    "initial_bases": "".join(
                        "1" if x else "0"
                        for x in bases
                    ),
                    "initial_outs": outs,
                    **metrics,
                }
            )


def lookup(
    mode: str,
    state_name: str,
    outs: int,
) -> float:

    for row in rows:
        if (
            row["mode"] == mode
            and row["state_name"] == state_name
            and row["initial_outs"] == outs
        ):
            return row["expected_runs"]

    raise KeyError((mode, state_name, outs))


monotonicity_rows = []

for mode in ["current", "prototype"]:

    for outs in [0, 1, 2]:

        second = lookup(mode, "runner_on_second", outs)
        third = lookup(mode, "runner_on_third", outs)

        monotonicity_rows.append(
            {
                "mode": mode,
                "outs": outs,
                "runner_on_second": second,
                "runner_on_third": third,
                "third_gt_second": third > second,
                "delta": round(third - second, 6),
            }
        )


prototype_repairs = 0

for row in monotonicity_rows:
    if (
        row["mode"] == "prototype"
        and row["outs"] in [0, 1]
        and row["third_gt_second"]
    ):
        prototype_repairs += 1


state_shift_rows = []

for outs in [0, 1, 2]:

    for state_name, _bases in BASE_STATES:

        current_runs = lookup(
            "current",
            state_name,
            outs,
        )

        prototype_runs = lookup(
            "prototype",
            state_name,
            outs,
        )

        delta = prototype_runs - current_runs

        state_shift_rows.append(
            {
                "state_name": state_name,
                "outs": outs,
                "current_expected_runs": current_runs,
                "prototype_expected_runs": prototype_runs,
                "delta": round(delta, 6),
                "large_shift": abs(delta) > 0.75,
            }
        )


advancement_examples = [
    {
        "scenario": "single_runner_on_first",
        "expected_effect": "sometimes_first_to_third",
    },
    {
        "scenario": "double_runner_on_first",
        "expected_effect": "sometimes_scores_runner",
    },
    {
        "scenario": "out_runner_on_third_less_than_two_outs",
        "expected_effect": "sometimes_scores_runner",
    },
]


large_shift_count = sum(
    1 for row in state_shift_rows
    if row["large_shift"]
)

if prototype_repairs >= 2 and large_shift_count == 0:
    diagnosis = "transition_realism_prototype_repairs_partial_ordering"
elif prototype_repairs >= 2:
    diagnosis = "transition_realism_prototype_overcorrects_run_environment"
elif prototype_repairs == 0:
    diagnosis = "transition_realism_prototype_no_improvement"
else:
    diagnosis = "transition_realism_prototype_failed"


summary = {
    "diagnosis": diagnosis,
    "simulations_per_state": SIMULATIONS,
    "prototype_repairs": prototype_repairs,
    "large_shift_count": large_shift_count,
    "monotonicity": monotonicity_rows,
    "largest_remaining_bottlenecks": [
        "outcome_subtype_modeling",
        "flyball_vs_groundball_resolution",
        "stateful_runner_speed",
        "double_play_specificity",
    ],
    "recommended_next_step": (
        "layer_6j_production_safe_transition_candidate"
        if diagnosis
        == "transition_realism_prototype_repairs_partial_ordering"
        else "layer_6j_transition_parameter_sensitivity_grid"
    ),
}


with (OUTPUT_DIR / "transition_realism_prototype.json").open("w") as handle:
    json.dump(
        {
            "summary": summary,
            "run_expectancy": rows,
            "state_shifts": state_shift_rows,
            "monotonicity": monotonicity_rows,
            "advancement_examples": advancement_examples,
        },
        handle,
        indent=2,
    )


with (OUTPUT_DIR / "transition_realism_run_expectancy.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=rows[0].keys(),
    )
    writer.writeheader()
    writer.writerows(rows)


with (OUTPUT_DIR / "transition_realism_monotonicity.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=monotonicity_rows[0].keys(),
    )
    writer.writeheader()
    writer.writerows(monotonicity_rows)


with (OUTPUT_DIR / "transition_realism_advancement.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=advancement_examples[0].keys(),
    )
    writer.writeheader()
    writer.writerows(advancement_examples)


with (OUTPUT_DIR / "transition_realism_bottlenecks.csv").open("w", newline="") as handle:
    writer = csv.writer(handle)

    writer.writerow(["remaining_bottleneck"])

    for item in summary["largest_remaining_bottlenecks"]:
        writer.writerow([item])


print(json.dumps(summary, indent=2))
