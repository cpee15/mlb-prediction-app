"""
Half-inning simulator.

Uses a plate appearance outcome probability distribution to simulate base/out
state transitions until three outs are recorded.

V1 is intentionally simple:
- no steals
- no errors
- no double plays
- conservative runner advancement rules
- K and OUT both count as outs
"""

from __future__ import annotations

import random
from collections import Counter
from typing import Dict, Optional, Any


OUTCOMES = ["k", "bb", "single", "double", "triple", "hr", "out"]


def _normalize_probabilities(probabilities: Dict[str, float]) -> Dict[str, float]:
    cleaned = {key: max(0.0, float(probabilities.get(key, 0.0) or 0.0)) for key in OUTCOMES}
    total = sum(cleaned.values())
    if total <= 0:
        return {
            "k": 0.225,
            "bb": 0.085,
            "single": 0.145,
            "double": 0.045,
            "triple": 0.004,
            "hr": 0.030,
            "out": 0.466,
        }
    return {key: value / total for key, value in cleaned.items()}


def sample_pa_outcome(probabilities: Dict[str, float], rng: Optional[random.Random] = None) -> str:
    rng = rng or random
    probs = _normalize_probabilities(probabilities)
    draw = rng.random()
    cumulative = 0.0
    for outcome in OUTCOMES:
        cumulative += probs[outcome]
        if draw <= cumulative:
            return outcome
    return "out"


def advance_runners(
    bases: tuple[bool, bool, bool],
    outcome: str,
) -> tuple[tuple[bool, bool, bool], int]:
    """
    Apply conservative base advancement rules.

    bases = (runner_on_first, runner_on_second, runner_on_third)
    returns (new_bases, runs_scored)
    """
    first, second, third = bases
    runs = 0

    if outcome in {"k", "out"}:
        return bases, 0

    if outcome == "bb":
        # Force runners only as needed.
        if first and second and third:
            runs += 1
        new_third = third or (first and second)
        new_second = second or first
        new_first = True
        return (new_first, new_second, new_third), runs

    if outcome == "single":
        # Conservative: runner on third scores; runner on second scores;
        # runner on first advances to second.
        runs += int(third) + int(second)
        return (True, first, False), runs

    if outcome == "double":
        # Conservative: runners on second/third score; runner on first to third.
        runs += int(third) + int(second)
        return (False, True, first), runs

    if outcome == "triple":
        runs += int(first) + int(second) + int(third)
        return (False, False, True), runs

    if outcome == "hr":
        runs += 1 + int(first) + int(second) + int(third)
        return (False, False, False), runs

    return bases, 0


def simulate_half_inning(
    probabilities: Dict[str, float],
    rng: Optional[random.Random] = None,
    max_plate_appearances: int = 30,
) -> Dict[str, Any]:
    rng = rng or random.Random()
    outs = 0
    runs = 0
    bases = (False, False, False)
    pa_count = 0
    outcomes = []

    while outs < 3 and pa_count < max_plate_appearances:
        outcome = sample_pa_outcome(probabilities, rng)
        pa_count += 1
        outcomes.append(outcome)

        if outcome in {"k", "out"}:
            outs += 1
            continue

        bases, scored = advance_runners(bases, outcome)
        runs += scored

    return {
        "runs": runs,
        "plate_appearances": pa_count,
        "outcomes": outcomes,
        "ended_by_max_pa": outs < 3,
    }


def simulate_half_innings(
    probabilities: Dict[str, float],
    simulations: int = 5000,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    rng = random.Random(seed)
    run_counts = Counter()
    pa_counts = []
    ended_by_max_pa = 0

    for _ in range(simulations):
        result = simulate_half_inning(probabilities, rng=rng)
        run_counts[result["runs"]] += 1
        pa_counts.append(result["plate_appearances"])
        if result["ended_by_max_pa"]:
            ended_by_max_pa += 1

    distribution = {
        str(runs): round(count / simulations, 4)
        for runs, count in sorted(run_counts.items())
    }

    expected_runs = sum(runs * count for runs, count in run_counts.items()) / simulations
    average_pa = sum(pa_counts) / len(pa_counts) if pa_counts else 0.0

    return {
        "model_version": "half_inning_sim_v1",
        "simulations": simulations,
        "expected_runs": round(expected_runs, 4),
        "average_plate_appearances": round(average_pa, 3),
        "run_distribution": distribution,
        "probability_scoreless": round(run_counts.get(0, 0) / simulations, 4),
        "probability_1_plus_runs": round(1 - (run_counts.get(0, 0) / simulations), 4),
        "probability_2_plus_runs": round(
            sum(count for runs, count in run_counts.items() if runs >= 2) / simulations,
            4,
        ),
        "ended_by_max_pa_rate": round(ended_by_max_pa / simulations, 4),
    }
