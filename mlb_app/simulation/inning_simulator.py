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


OUTCOMES = ["k", "bb", "hbp", "single", "double", "triple", "hr", "reached_on_error", "out"]

SAC_FLY_RATE = 0.30
DOUBLE_PLAY_RATE = 0.11
FIRST_TO_THIRD_SINGLE_RATE = 0.30
FIRST_TO_HOME_DOUBLE_RATE = 0.40


def _normalize_probabilities(probabilities: Dict[str, float]) -> Dict[str, float]:
    cleaned = {key: max(0.0, float(probabilities.get(key, 0.0) or 0.0)) for key in OUTCOMES}
    total = sum(cleaned.values())
    if total <= 0:
        return {
            "k": 0.225,
            "bb": 0.085,
            "hbp": 0.011,
            "single": 0.145,
            "double": 0.045,
            "triple": 0.004,
            "hr": 0.030,
            "reached_on_error": 0.007,
            "out": 0.459,
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

    if outcome in {"bb", "hbp"}:
        # Force runners only as needed.
        if first and second and third:
            runs += 1
        new_third = third or (first and second)
        new_second = second or first
        new_first = True
        return (new_first, new_second, new_third), runs

    if outcome in {"single", "reached_on_error"}:
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


def advance_runners_realism_candidate(
    bases: Tuple[bool, bool, bool],
    outcome: str,
    outs: int,
    rng: Optional[random.Random] = None,
) -> Tuple[Tuple[bool, bool, bool], int, int]:
    """
    Candidate transition realism helper.

    Returns:
        (new_bases, runs_scored, extra_outs)
    """

    rng = rng or random.Random()

    first, second, third = bases
    runs = 0
    extra_outs = 0

    if outcome == "out":

        if third and outs < 2:
            if rng.random() < SAC_FLY_RATE:
                third = False
                runs += 1

        if first and outs < 2:
            if rng.random() < DOUBLE_PLAY_RATE:
                first = False
                extra_outs += 1

        return (first, second, third), runs, extra_outs

    if outcome in {"single", "reached_on_error"}:

        if third:
            runs += 1
            third = False

        new_third = False

        if second:
            runs += 1

        if first:
            if rng.random() < FIRST_TO_THIRD_SINGLE_RATE:
                new_third = True
                new_second = False
            else:
                new_second = True
        else:
            new_second = False

        new_first = True

        return (
            (new_first, new_second, new_third),
            runs,
            extra_outs,
        )

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

        return (
            (new_first, new_second, new_third),
            runs,
            extra_outs,
        )

    new_bases, scored = advance_runners(
        bases,
        outcome,
    )

    return new_bases, scored, extra_outs



def simulate_half_inning(
    probabilities: Dict[str, float],
    rng: Optional[random.Random] = None,
    max_plate_appearances: int = 30,
    initial_bases: Tuple[bool, bool, bool] = (False, False, False),
    initial_outs: int = 0,
    transition_mode: str = "current",
) -> Dict[str, Any]:
    rng = rng or random.Random()

    if transition_mode not in {
        "current",
        "realism_candidate",
    }:
        raise ValueError(
            "transition_mode must be 'current' or 'realism_candidate'"
        )

    bases = tuple(bool(x) for x in initial_bases)

    if len(bases) != 3:
        raise ValueError(
            "initial_bases must contain exactly three booleans"
        )

    outs = int(initial_outs)

    if outs < 0 or outs > 2:
        raise ValueError(
            "initial_outs must be 0, 1, or 2"
        )

    starting_bases = bases
    starting_outs = outs

    runs = 0
    pa_count = 0
    outcomes = []

    while outs < 3 and pa_count < max_plate_appearances:

        outcome = sample_pa_outcome(probabilities, rng)

        pa_count += 1
        outcomes.append(outcome)

        if outcome == "k":
            outs += 1
            continue

        if outcome == "out":

            if transition_mode == "realism_candidate":

                bases, scored, extra_outs = (
                    advance_runners_realism_candidate(
                        bases=bases,
                        outcome=outcome,
                        outs=outs,
                        rng=rng,
                    )
                )

                runs += scored

                outs = min(
                    3,
                    outs + 1 + extra_outs,
                )

            else:
                outs += 1

            continue

        if transition_mode == "realism_candidate":

            bases, scored, _extra_outs = (
                advance_runners_realism_candidate(
                    bases=bases,
                    outcome=outcome,
                    outs=outs,
                    rng=rng,
                )
            )

        else:

            bases, scored = advance_runners(
                bases,
                outcome,
            )

        runs += scored

    return {
        "runs": runs,
        "plate_appearances": pa_count,
        "outcomes": outcomes,
        "ended_by_max_pa": outs < 3,
        "initial_bases": starting_bases,
        "initial_outs": starting_outs,
        "ending_bases": bases,
        "ending_outs": outs,
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
