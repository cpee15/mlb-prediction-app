from __future__ import annotations

import csv
import json
import random
from pathlib import Path
from statistics import mean
from typing import Dict, List, Tuple

from mlb_app.simulation.inning_simulator import (
    advance_runners,
    simulate_half_inning,
)


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


def bool_tuple_to_string(bases: Tuple[bool, bool, bool]) -> str:
    return "".join("1" if x else "0" for x in bases)


def estimate_run_expectancy(
    bases: Tuple[bool, bool, bool],
    outs: int,
    simulations: int = SIMULATIONS,
) -> Dict[str, float]:
    runs = []
    ended_by_max_pa = 0

    for idx in range(simulations):
        result = simulate_half_inning(
            PROBABILITIES,
            rng=random.Random(SEED + idx),
            initial_bases=bases,
            initial_outs=outs,
        )
        runs.append(result["runs"])

        if result["ended_by_max_pa"]:
            ended_by_max_pa += 1

    return {
        "expected_runs": round(mean(runs), 6),
        "probability_scoreless": round(sum(1 for x in runs if x == 0) / simulations, 6),
        "probability_1_plus_runs": round(sum(1 for x in runs if x >= 1) / simulations, 6),
        "probability_2_plus_runs": round(sum(1 for x in runs if x >= 2) / simulations, 6),
        "ended_by_max_pa_rate": round(ended_by_max_pa / simulations, 6),
    }


def audit_single_transition(
    bases: Tuple[bool, bool, bool],
    outcome: str,
) -> Dict[str, object]:
    new_bases, runs = advance_runners(bases, outcome)

    return {
        "initial_bases": bool_tuple_to_string(bases),
        "outcome": outcome,
        "ending_bases": bool_tuple_to_string(new_bases),
        "runs_scored": runs,
    }


run_expectancy_rows = []

for outs in [0, 1, 2]:
    for state_name, bases in BASE_STATES:
        estimate = estimate_run_expectancy(bases, outs)

        run_expectancy_rows.append(
            {
                "state_name": state_name,
                "initial_bases": bool_tuple_to_string(bases),
                "initial_outs": outs,
                **estimate,
            }
        )


advancement_cases = [
    ("single_runner_on_first", (True, False, False), "single"),
    ("single_runner_on_second", (False, True, False), "single"),
    ("single_runner_on_third", (False, False, True), "single"),
    ("single_first_and_second", (True, True, False), "single"),
    ("double_runner_on_first", (True, False, False), "double"),
    ("double_runner_on_second", (False, True, False), "double"),
    ("double_first_and_second", (True, True, False), "double"),
    ("triple_runner_on_first", (True, False, False), "triple"),
    ("walk_bases_loaded", (True, True, True), "bb"),
    ("hbp_bases_loaded", (True, True, True), "hbp"),
    ("out_runner_on_third", (False, False, True), "out"),
    ("out_runner_on_first", (True, False, False), "out"),
    ("k_runner_on_third", (False, False, True), "k"),
    ("hr_bases_loaded", (True, True, True), "hr"),
]

advancement_rows = []

for case_name, bases, outcome in advancement_cases:
    transition = audit_single_transition(bases, outcome)
    transition["case_name"] = case_name

    advancement_rows.append(transition)


def find_expected_runs(state_name: str, outs: int) -> float:
    for row in run_expectancy_rows:
        if row["state_name"] == state_name and row["initial_outs"] == outs:
            return row["expected_runs"]
    raise KeyError((state_name, outs))


monotonicity_checks = []

for outs in [0, 1, 2]:
    empty = find_expected_runs("empty", outs)
    first = find_expected_runs("runner_on_first", outs)
    second = find_expected_runs("runner_on_second", outs)
    third = find_expected_runs("runner_on_third", outs)
    loaded = find_expected_runs("bases_loaded", outs)

    checks = [
        {
            "check": f"outs_{outs}_first_gt_empty",
            "passed": first > empty,
            "left": first,
            "right": empty,
        },
        {
            "check": f"outs_{outs}_second_gt_first",
            "passed": second > first,
            "left": second,
            "right": first,
        },
        {
            "check": f"outs_{outs}_third_gt_second",
            "passed": third > second,
            "left": third,
            "right": second,
        },
        {
            "check": f"outs_{outs}_loaded_gt_third",
            "passed": loaded > third,
            "left": loaded,
            "right": third,
        },
    ]

    monotonicity_checks.extend(checks)


for state_name, _bases in BASE_STATES:
    if state_name == "empty":
        continue

    zero_outs = find_expected_runs(state_name, 0)
    one_out = find_expected_runs(state_name, 1)
    two_outs = find_expected_runs(state_name, 2)

    monotonicity_checks.append(
        {
            "check": f"{state_name}_zero_outs_gt_one_out",
            "passed": zero_outs > one_out,
            "left": zero_outs,
            "right": one_out,
        }
    )

    monotonicity_checks.append(
        {
            "check": f"{state_name}_one_out_gt_two_outs",
            "passed": one_out > two_outs,
            "left": one_out,
            "right": two_outs,
        }
    )


missing_mechanics = [
    {
        "mechanic": "double_play_absence",
        "status": "absent",
        "risk_level": "high",
        "evidence": "out and k outcomes add only one out; no ground-ball state-dependent DP branch detected",
        "recommended_action": "prototype simple GDP probability with runner on first and less than two outs",
    },
    {
        "mechanic": "sac_fly_absence",
        "status": "absent",
        "risk_level": "high",
        "evidence": "out with runner on third leaves runner stranded and scores zero",
        "recommended_action": "prototype sac-fly scoring probability for out with runner on third and fewer than two outs",
    },
    {
        "mechanic": "tagging_up_absence",
        "status": "absent",
        "risk_level": "medium",
        "evidence": "generic out has no fly-ball/line-drive/ground-ball subtype",
        "recommended_action": "split generic out into advancement-capable and non-advancement out branches",
    },
    {
        "mechanic": "first_to_third_on_single_absence",
        "status": "absent_or_suppressed",
        "risk_level": "medium",
        "evidence": "single with runner on first leaves runners on first and second",
        "recommended_action": "prototype first-to-third probability on singles",
    },
    {
        "mechanic": "second_to_home_on_single_absence",
        "status": "absent_or_suppressed",
        "risk_level": "high",
        "evidence": "single with runner on second leaves runners on first and third and scores zero",
        "recommended_action": "prototype second-to-home probability on singles",
    },
    {
        "mechanic": "runner_on_first_scores_on_double_absence",
        "status": "absent_or_suppressed",
        "risk_level": "medium",
        "evidence": "double with runner on first moves runner only to third",
        "recommended_action": "prototype first-to-home probability on doubles",
    },
    {
        "mechanic": "steals_and_caught_stealing_absence",
        "status": "absent",
        "risk_level": "medium",
        "evidence": "no independent base-state transition event between plate appearances",
        "recommended_action": "defer until advancement and DP prototypes are evaluated",
    },
    {
        "mechanic": "wild_pitch_passed_ball_balk_absence",
        "status": "absent",
        "risk_level": "low",
        "evidence": "no non-PA runner advancement event currently exists",
        "recommended_action": "defer until core base/out transition realism is established",
    },
]


bottleneck_priority = {
    "double_play_absence": 1,
    "second_to_home_on_single_absence": 2,
    "sac_fly_absence": 3,
    "first_to_third_on_single_absence": 4,
    "runner_on_first_scores_on_double_absence": 5,
    "tagging_up_absence": 6,
    "steals_and_caught_stealing_absence": 7,
    "wild_pitch_passed_ball_balk_absence": 8,
}

bottleneck_rows = []

for item in missing_mechanics:
    bottleneck_rows.append(
        {
            "rank": bottleneck_priority[item["mechanic"]],
            **item,
        }
    )

bottleneck_rows = sorted(bottleneck_rows, key=lambda row: row["rank"])


monotonicity_failures = [
    row for row in monotonicity_checks if not row["passed"]
]

advancement_summary = {
    "single_runner_on_first_ending": next(
        row for row in advancement_rows if row["case_name"] == "single_runner_on_first"
    ),
    "single_runner_on_second_ending": next(
        row for row in advancement_rows if row["case_name"] == "single_runner_on_second"
    ),
    "out_runner_on_third_ending": next(
        row for row in advancement_rows if row["case_name"] == "out_runner_on_third"
    ),
    "out_runner_on_first_ending": next(
        row for row in advancement_rows if row["case_name"] == "out_runner_on_first"
    ),
}

if monotonicity_failures:
    diagnosis = "base_out_transition_realism_monotonicity_violations"
else:
    diagnosis = "base_out_transition_realism_bottlenecks_identified"


summary = {
    "diagnosis": diagnosis,
    "simulations_per_state": SIMULATIONS,
    "monotonicity_failures": len(monotonicity_failures),
    "monotonicity_checks": monotonicity_checks,
    "advancement_summary": advancement_summary,
    "largest_missing_realism_effects": [
        row["mechanic"] for row in bottleneck_rows[:5]
    ],
    "bottleneck_count": len(bottleneck_rows),
    "recommended_next_step": (
        "layer_6i_minimal_transition_realism_prototype"
        if diagnosis == "base_out_transition_realism_bottlenecks_identified"
        else "repair_base_out_state_transition_ordering"
    ),
}


with (OUTPUT_DIR / "base_out_transition_realism.json").open("w") as handle:
    json.dump(
        {
            "summary": summary,
            "run_expectancy": run_expectancy_rows,
            "advancement": advancement_rows,
            "bottlenecks": bottleneck_rows,
        },
        handle,
        indent=2,
    )


with (OUTPUT_DIR / "base_out_transition_run_expectancy.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=run_expectancy_rows[0].keys(),
    )
    writer.writeheader()
    writer.writerows(run_expectancy_rows)


with (OUTPUT_DIR / "base_out_transition_advancement.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=advancement_rows[0].keys(),
    )
    writer.writeheader()
    writer.writerows(advancement_rows)


with (OUTPUT_DIR / "base_out_transition_bottlenecks.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=bottleneck_rows[0].keys(),
    )
    writer.writeheader()
    writer.writerows(bottleneck_rows)


print(json.dumps(summary, indent=2))
