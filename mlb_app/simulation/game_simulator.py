"""
Full-game simulation engine.

Uses PA outcome probability distributions for each offense to simulate
nine-inning run distributions and game-level probabilities.

V1 is intentionally conservative:
- fixed nine innings for both teams
- no walk-off shortening
- no extras
- no bullpen transition yet
- independent half-innings using one lineup-level PA distribution per team
"""

from __future__ import annotations

import random
from collections import Counter
from typing import Any, Dict, Optional

from .inning_simulator import simulate_half_inning


def _distribution(counter: Counter, simulations: int) -> Dict[str, float]:
    return {
        str(value): round(count / simulations, 4)
        for value, count in sorted(counter.items())
    }


def _calibrated_counter_by_mean_shift(counter: Counter, raw_mean: float, calibrated_mean: float) -> Counter:
    """
    Shift a discrete run distribution toward the calibrated mean.

    V1 keeps the empirical shape but shifts integer run buckets by the rounded
    mean difference. This is conservative and transparent, but intentionally
    not a substitute for historical backtesting.
    """
    shift = round(calibrated_mean - raw_mean)
    if shift == 0:
        return Counter(counter)

    adjusted = Counter()
    for value, count in counter.items():
        adjusted[max(0, value + shift)] += count
    return adjusted


def _prob_greater_than(counter: Counter, threshold: float, simulations: int) -> float:
    return round(
        sum(count for value, count in counter.items() if value > threshold) / simulations,
        4,
    )


def _prob_less_than(counter: Counter, threshold: float, simulations: int) -> float:
    return round(
        sum(count for value, count in counter.items() if value < threshold) / simulations,
        4,
    )


def _prob_at_least(counter: Counter, threshold: int, simulations: int) -> float:
    return round(
        sum(count for value, count in counter.items() if value >= threshold) / simulations,
        4,
    )


GAME_SIM_CALIBRATION = {
    "game_sim_calibration_version": "game_sim_calibration_v1",
    # V1 sim uses starter/lineup-average PA distributions for all 9 innings.
    # This shrinkage tempers that known inflation until bullpen/lineup-order
    # state is modeled explicitly.
    "starter_only_run_shrinkage": 0.92,
    # Pull extreme team totals modestly toward a neutral MLB-ish scoring level.
    "team_run_regression_anchor": 4.45,
    "team_run_regression_weight": 0.12,
    # First-pass probability calibration. Backtest showed run means were
    # calibrated but win probabilities were too confident at the extremes.
    # Keep direction/order, but shrink raw simulation win frequency toward 50%.
    "win_probability_confidence_scale": 0.75,
}


def _calibrate_expected_runs(raw_runs: float) -> float:
    shrinkage = GAME_SIM_CALIBRATION["starter_only_run_shrinkage"]
    anchor = GAME_SIM_CALIBRATION["team_run_regression_anchor"]
    regression_weight = GAME_SIM_CALIBRATION["team_run_regression_weight"]

    shrunk = raw_runs * shrinkage
    calibrated = (shrunk * (1.0 - regression_weight)) + (anchor * regression_weight)
    return round(calibrated, 4)


def _calibrate_win_probability(raw_probability: float) -> float:
    """
    Shrink raw simulation win frequency toward 50%.

    This is intentionally conservative. It addresses observed overconfidence
    in probability extremes without changing the underlying run simulation.
    """
    scale = GAME_SIM_CALIBRATION["win_probability_confidence_scale"]
    calibrated = 0.5 + ((raw_probability - 0.5) * scale)
    return max(0.001, min(0.999, calibrated))


def simulate_game(
    away_probabilities: Dict[str, float],
    home_probabilities: Dict[str, float],
    simulations: int = 5000,
    seed: Optional[int] = None,
    innings: int = 9,
) -> Dict[str, Any]:
    rng = random.Random(seed)

    away_runs_counter = Counter()
    home_runs_counter = Counter()
    total_runs_counter = Counter()
    margin_counter = Counter()

    away_wins = 0
    home_wins = 0
    ties_after_regulation = 0

    for _ in range(simulations):
        away_runs = 0
        home_runs = 0

        for _inning in range(innings):
            away_half = simulate_half_inning(away_probabilities, rng=rng)
            home_half = simulate_half_inning(home_probabilities, rng=rng)
            away_runs += away_half["runs"]
            home_runs += home_half["runs"]

        total_runs = away_runs + home_runs
        margin = home_runs - away_runs

        away_runs_counter[away_runs] += 1
        home_runs_counter[home_runs] += 1
        total_runs_counter[total_runs] += 1
        margin_counter[margin] += 1

        if home_runs > away_runs:
            home_wins += 1
        elif away_runs > home_runs:
            away_wins += 1
        else:
            ties_after_regulation += 1

    raw_away_expected_runs = sum(runs * count for runs, count in away_runs_counter.items()) / simulations
    raw_home_expected_runs = sum(runs * count for runs, count in home_runs_counter.items()) / simulations
    raw_total_expected_runs = sum(runs * count for runs, count in total_runs_counter.items()) / simulations

    away_expected_runs = _calibrate_expected_runs(raw_away_expected_runs)
    home_expected_runs = _calibrate_expected_runs(raw_home_expected_runs)
    total_expected_runs = round(away_expected_runs + home_expected_runs, 4)

    # V1 has no extras. Split regulation ties evenly for a rough win-probability estimate.
    raw_home_win_probability = (home_wins + (ties_after_regulation * 0.5)) / simulations
    raw_away_win_probability = (away_wins + (ties_after_regulation * 0.5)) / simulations
    home_win_probability = _calibrate_win_probability(raw_home_win_probability)
    away_win_probability = _calibrate_win_probability(raw_away_win_probability)

    calibrated_total_runs_counter = _calibrated_counter_by_mean_shift(
        total_runs_counter,
        raw_mean=raw_total_expected_runs,
        calibrated_mean=total_expected_runs,
    )
    calibrated_away_runs_counter = _calibrated_counter_by_mean_shift(
        away_runs_counter,
        raw_mean=raw_away_expected_runs,
        calibrated_mean=away_expected_runs,
    )
    calibrated_home_runs_counter = _calibrated_counter_by_mean_shift(
        home_runs_counter,
        raw_mean=raw_home_expected_runs,
        calibrated_mean=home_expected_runs,
    )

    common_totals = [6.5, 7.5, 8.5, 9.5, 10.5]
    total_probabilities = {
        f"over_{line}": _prob_greater_than(total_runs_counter, line, simulations)
        for line in common_totals
    }
    total_probabilities.update({
        f"under_{line}": _prob_less_than(total_runs_counter, line, simulations)
        for line in common_totals
    })
    calibrated_total_probabilities = {
        f"over_{line}": _prob_greater_than(calibrated_total_runs_counter, line, simulations)
        for line in common_totals
    }
    calibrated_total_probabilities.update({
        f"under_{line}": _prob_less_than(calibrated_total_runs_counter, line, simulations)
        for line in common_totals
    })

    return {
        "model_version": "full_game_sim_v1",
        "simulations": simulations,
        "innings": innings,
        "away_expected_runs": away_expected_runs,
        "home_expected_runs": home_expected_runs,
        "total_expected_runs": total_expected_runs,
        "raw_away_expected_runs": round(raw_away_expected_runs, 4),
        "raw_home_expected_runs": round(raw_home_expected_runs, 4),
        "raw_total_expected_runs": round(raw_total_expected_runs, 4),
        "away_win_probability": round(away_win_probability, 4),
        "home_win_probability": round(home_win_probability, 4),
        "raw_away_win_probability": round(raw_away_win_probability, 4),
        "raw_home_win_probability": round(raw_home_win_probability, 4),
        "raw_away_win_probability": round(raw_away_win_probability, 4),
        "raw_home_win_probability": round(raw_home_win_probability, 4),
        "tie_after_regulation_probability": round(ties_after_regulation / simulations, 4),
        "away_run_distribution": _distribution(away_runs_counter, simulations),
        "home_run_distribution": _distribution(home_runs_counter, simulations),
        "total_run_distribution": _distribution(total_runs_counter, simulations),
        "calibrated_total_run_distribution": _distribution(calibrated_total_runs_counter, simulations),
        "calibrated_away_run_distribution": _distribution(calibrated_away_runs_counter, simulations),
        "calibrated_home_run_distribution": _distribution(calibrated_home_runs_counter, simulations),
        "home_margin_distribution": _distribution(margin_counter, simulations),
        "total_probabilities": total_probabilities,
        "calibrated_total_probabilities": calibrated_total_probabilities,
        "team_total_probabilities": {
            "away_3_plus": _prob_at_least(away_runs_counter, 3, simulations),
            "away_4_plus": _prob_at_least(away_runs_counter, 4, simulations),
            "away_5_plus": _prob_at_least(away_runs_counter, 5, simulations),
            "home_3_plus": _prob_at_least(home_runs_counter, 3, simulations),
            "home_4_plus": _prob_at_least(home_runs_counter, 4, simulations),
            "home_5_plus": _prob_at_least(home_runs_counter, 5, simulations),
        },
        "calibrated_team_total_probabilities": {
            "away_3_plus": _prob_at_least(calibrated_away_runs_counter, 3, simulations),
            "away_4_plus": _prob_at_least(calibrated_away_runs_counter, 4, simulations),
            "away_5_plus": _prob_at_least(calibrated_away_runs_counter, 5, simulations),
            "home_3_plus": _prob_at_least(calibrated_home_runs_counter, 3, simulations),
            "home_4_plus": _prob_at_least(calibrated_home_runs_counter, 4, simulations),
            "home_5_plus": _prob_at_least(calibrated_home_runs_counter, 5, simulations),
        },
        "metadata": {
            "generated_from": "simulation.game_simulator.simulate_game",
            "seed": seed,
            "simulation_count": simulations,
            "notes": [
                "V1 uses fixed nine innings for both teams.",
                "Ties after regulation are split evenly for win probability.",
                "No bullpen, extras, walk-off shortening, or lineup-order state yet.",
            ],
        },
    }

def _sample_from_distribution(distribution: Dict[int, float], rng: random.Random) -> int:
    draw = rng.random()
    cumulative = 0.0
    for value, probability in sorted(distribution.items()):
        cumulative += probability
        if draw <= cumulative:
            return value
    return max(distribution.keys())


def _blend_distributions(a: Dict[int, float], b: Dict[int, float], b_weight: float) -> Dict[int, float]:
    b_weight = max(0.0, min(1.0, b_weight))
    a_weight = 1.0 - b_weight
    keys = sorted(set(a) | set(b))
    blended = {
        key: (a.get(key, 0.0) * a_weight) + (b.get(key, 0.0) * b_weight)
        for key in keys
    }
    total = sum(blended.values())
    return {key: value / total for key, value in blended.items()} if total else dict(a)


def _starter_exit_distribution_from_score(starter_quality_score: float) -> Dict[int, float]:
    weak = {3: 0.08, 4: 0.27, 5: 0.40, 6: 0.20, 7: 0.05}
    average = {4: 0.12, 5: 0.38, 6: 0.35, 7: 0.13, 8: 0.02}
    strong = {4: 0.05, 5: 0.20, 6: 0.45, 7: 0.25, 8: 0.05}

    score = max(-1.0, min(1.0, starter_quality_score or 0.0))
    if score >= 0:
        return _blend_distributions(average, strong, score)
    return _blend_distributions(average, weak, abs(score))


def _starter_quality_label(starter_quality_score: float) -> str:
    score = starter_quality_score or 0.0
    if score >= 0.55:
        return "strong"
    if score >= 0.20:
        return "above_average"
    if score <= -0.55:
        return "weak"
    if score <= -0.20:
        return "below_average"
    return "average"


def starter_quality_score(pitcher_profile: Optional[Dict[str, Any]]) -> float:
    if not pitcher_profile:
        return 0.0

    bat_missing = pitcher_profile.get("bat_missing") or {}
    command = pitcher_profile.get("command_control") or {}
    contact = pitcher_profile.get("contact_management") or {}

    signals = []

    def add_signal(value, strong_value, weak_value, higher_is_better=True):
        if not isinstance(value, (int, float)):
            return
        if higher_is_better:
            raw = (value - weak_value) / (strong_value - weak_value)
        else:
            raw = (weak_value - value) / (weak_value - strong_value)
        signals.append(max(-1.0, min(1.0, (raw * 2.0) - 1.0)))

    add_signal(bat_missing.get("k_rate"), strong_value=0.30, weak_value=0.17, higher_is_better=True)
    add_signal(command.get("bb_rate"), strong_value=0.055, weak_value=0.12, higher_is_better=False)
    add_signal(contact.get("xwoba_allowed"), strong_value=0.285, weak_value=0.365, higher_is_better=False)
    add_signal(contact.get("hard_hit_rate_allowed"), strong_value=0.32, weak_value=0.47, higher_is_better=False)
    add_signal(contact.get("barrel_rate_allowed"), strong_value=0.045, weak_value=0.105, higher_is_better=False)

    if not signals:
        return 0.0

    return round(sum(signals) / len(signals), 3)


def classify_starter_quality(pitcher_profile: Optional[Dict[str, Any]]) -> str:
    return _starter_quality_label(starter_quality_score(pitcher_profile))


def simulate_game_with_bullpen(
    away_starter_probabilities: Dict[str, float],
    home_starter_probabilities: Dict[str, float],
    away_bullpen_probabilities: Dict[str, float],
    home_bullpen_probabilities: Dict[str, float],
    simulations: int = 5000,
    seed: Optional[int] = None,
    innings: int = 9,
    starter_innings: int = 5,
    away_starter_quality: str = "average",
    home_starter_quality: str = "average",
    dynamic_starter_exit: bool = True,
) -> Dict[str, Any]:
    """
    Simulate a game with separate early-inning starter probabilities and
    late-inning bullpen probabilities.

    Naming convention:
    - away_* probabilities describe the away offense.
    - home_* probabilities describe the home offense.
    """
    rng = random.Random(seed)

    away_runs_counter = Counter()
    home_runs_counter = Counter()
    total_runs_counter = Counter()
    margin_counter = Counter()

    away_wins = 0
    home_wins = 0
    ties_after_regulation = 0

    away_quality_score = (
        float(away_starter_quality)
        if isinstance(away_starter_quality, (int, float))
        else 0.0
    )
    home_quality_score = (
        float(home_starter_quality)
        if isinstance(home_starter_quality, (int, float))
        else 0.0
    )
    away_quality_label = _starter_quality_label(away_quality_score)
    home_quality_label = _starter_quality_label(home_quality_score)

    away_exit_distribution = _starter_exit_distribution_from_score(away_quality_score)
    home_exit_distribution = _starter_exit_distribution_from_score(home_quality_score)
    away_starter_innings_counter = Counter()
    home_starter_innings_counter = Counter()

    for _ in range(simulations):
        away_runs = 0
        home_runs = 0

        away_starter_innings = (
            _sample_from_distribution(away_exit_distribution, rng)
            if dynamic_starter_exit
            else starter_innings
        )
        home_starter_innings = (
            _sample_from_distribution(home_exit_distribution, rng)
            if dynamic_starter_exit
            else starter_innings
        )

        away_starter_innings_counter[away_starter_innings] += 1
        home_starter_innings_counter[home_starter_innings] += 1

        for inning_index in range(1, innings + 1):
            away_probs = (
                away_starter_probabilities
                if inning_index <= home_starter_innings
                else away_bullpen_probabilities
            )
            home_probs = (
                home_starter_probabilities
                if inning_index <= away_starter_innings
                else home_bullpen_probabilities
            )

            away_half = simulate_half_inning(away_probs, rng=rng)
            home_half = simulate_half_inning(home_probs, rng=rng)
            away_runs += away_half["runs"]
            home_runs += home_half["runs"]

        total_runs = away_runs + home_runs
        margin = home_runs - away_runs

        away_runs_counter[away_runs] += 1
        home_runs_counter[home_runs] += 1
        total_runs_counter[total_runs] += 1
        margin_counter[margin] += 1

        if home_runs > away_runs:
            home_wins += 1
        elif away_runs > home_runs:
            away_wins += 1
        else:
            ties_after_regulation += 1

    raw_away_expected_runs = sum(runs * count for runs, count in away_runs_counter.items()) / simulations
    raw_home_expected_runs = sum(runs * count for runs, count in home_runs_counter.items()) / simulations
    raw_total_expected_runs = sum(runs * count for runs, count in total_runs_counter.items()) / simulations

    away_expected_runs = _calibrate_expected_runs(raw_away_expected_runs)
    home_expected_runs = _calibrate_expected_runs(raw_home_expected_runs)
    total_expected_runs = round(away_expected_runs + home_expected_runs, 4)

    raw_home_win_probability = (home_wins + (ties_after_regulation * 0.5)) / simulations
    raw_away_win_probability = (away_wins + (ties_after_regulation * 0.5)) / simulations
    home_win_probability = _calibrate_win_probability(raw_home_win_probability)
    away_win_probability = _calibrate_win_probability(raw_away_win_probability)

    calibrated_total_runs_counter = _calibrated_counter_by_mean_shift(
        total_runs_counter,
        raw_mean=raw_total_expected_runs,
        calibrated_mean=total_expected_runs,
    )
    calibrated_away_runs_counter = _calibrated_counter_by_mean_shift(
        away_runs_counter,
        raw_mean=raw_away_expected_runs,
        calibrated_mean=away_expected_runs,
    )
    calibrated_home_runs_counter = _calibrated_counter_by_mean_shift(
        home_runs_counter,
        raw_mean=raw_home_expected_runs,
        calibrated_mean=home_expected_runs,
    )

    common_totals = [6.5, 7.5, 8.5, 9.5, 10.5]
    total_probabilities = {
        f"over_{line}": _prob_greater_than(total_runs_counter, line, simulations)
        for line in common_totals
    }
    total_probabilities.update({
        f"under_{line}": _prob_less_than(total_runs_counter, line, simulations)
        for line in common_totals
    })
    calibrated_total_probabilities = {
        f"over_{line}": _prob_greater_than(calibrated_total_runs_counter, line, simulations)
        for line in common_totals
    }
    calibrated_total_probabilities.update({
        f"under_{line}": _prob_less_than(calibrated_total_runs_counter, line, simulations)
        for line in common_totals
    })

    return {
        "model_version": "full_game_sim_with_bullpen_v1",
        "simulations": simulations,
        "innings": innings,
        "starter_innings": starter_innings,
        "dynamic_starter_exit": dynamic_starter_exit,
        "away_starter_quality": away_quality_label,
        "home_starter_quality": home_quality_label,
        "away_starter_quality_score": round(away_quality_score, 3),
        "home_starter_quality_score": round(home_quality_score, 3),
        "away_starter_innings_distribution": _distribution(away_starter_innings_counter, simulations),
        "home_starter_innings_distribution": _distribution(home_starter_innings_counter, simulations),
        "bullpen_innings": max(0, innings - starter_innings),
        "away_expected_runs": away_expected_runs,
        "home_expected_runs": home_expected_runs,
        "total_expected_runs": total_expected_runs,
        "raw_away_expected_runs": round(raw_away_expected_runs, 4),
        "raw_home_expected_runs": round(raw_home_expected_runs, 4),
        "raw_total_expected_runs": round(raw_total_expected_runs, 4),
        "away_win_probability": round(away_win_probability, 4),
        "home_win_probability": round(home_win_probability, 4),
        "tie_after_regulation_probability": round(ties_after_regulation / simulations, 4),
        "away_run_distribution": _distribution(away_runs_counter, simulations),
        "home_run_distribution": _distribution(home_runs_counter, simulations),
        "total_run_distribution": _distribution(total_runs_counter, simulations),
        "calibrated_total_run_distribution": _distribution(calibrated_total_runs_counter, simulations),
        "calibrated_away_run_distribution": _distribution(calibrated_away_runs_counter, simulations),
        "calibrated_home_run_distribution": _distribution(calibrated_home_runs_counter, simulations),
        "home_margin_distribution": _distribution(margin_counter, simulations),
        "total_probabilities": total_probabilities,
        "calibrated_total_probabilities": calibrated_total_probabilities,
        "team_total_probabilities": {
            "away_3_plus": _prob_at_least(away_runs_counter, 3, simulations),
            "away_4_plus": _prob_at_least(away_runs_counter, 4, simulations),
            "away_5_plus": _prob_at_least(away_runs_counter, 5, simulations),
            "home_3_plus": _prob_at_least(home_runs_counter, 3, simulations),
            "home_4_plus": _prob_at_least(home_runs_counter, 4, simulations),
            "home_5_plus": _prob_at_least(home_runs_counter, 5, simulations),
        },
        "calibrated_team_total_probabilities": {
            "away_3_plus": _prob_at_least(calibrated_away_runs_counter, 3, simulations),
            "away_4_plus": _prob_at_least(calibrated_away_runs_counter, 4, simulations),
            "away_5_plus": _prob_at_least(calibrated_away_runs_counter, 5, simulations),
            "home_3_plus": _prob_at_least(calibrated_home_runs_counter, 3, simulations),
            "home_4_plus": _prob_at_least(calibrated_home_runs_counter, 4, simulations),
            "home_5_plus": _prob_at_least(calibrated_home_runs_counter, 5, simulations),
        },
        "metadata": {
            "generated_from": "simulation.game_simulator.simulate_game_with_bullpen",
            "seed": seed,
            "simulation_count": simulations,
            "starter_innings": starter_innings,
            "dynamic_starter_exit": dynamic_starter_exit,
            "away_starter_quality": away_quality_label,
            "home_starter_quality": home_quality_label,
            "away_starter_quality_score": round(away_quality_score, 3),
            "home_starter_quality_score": round(home_quality_score, 3),
            "away_starter_innings_distribution": _distribution(away_starter_innings_counter, simulations),
            "home_starter_innings_distribution": _distribution(home_starter_innings_counter, simulations),
            "bullpen_innings": max(0, innings - starter_innings),
            **GAME_SIM_CALIBRATION,
            "calibration_applied_to": ["expected_runs", "win_probability", "total_probabilities", "team_total_probabilities"],
            "distribution_calibration_method": "integer_mean_shift_v1",
            "notes": [
                "V1 uses starter PA probabilities for early innings and bullpen PA probabilities for late innings.",
                "Ties after regulation are split evenly for win probability.",
                "No extras, walk-off shortening, individual reliever selection, or lineup-order state yet.",
            ],
        },
    }

