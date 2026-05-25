import json
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_CSV = TMP_DIR / "game_state_realism_prioritization.csv"
OUTPUT_CHECKS = TMP_DIR / "game_state_realism_prioritization_checks.csv"


WEIGHTS = {
    "expected_modeling_ev": 0.30,
    "frequency_impact": 0.15,
    "distribution_tail_impact": 0.20,
    "data_availability": 0.10,
    "calibration_feasibility": 0.10,
    "engineering_complexity": 0.05,
    "dependency_priority": 0.10,
}


BRANCHES = [
    {
        "branch": "bullpen_sequencing_leverage",
        "expected_modeling_ev": 10,
        "frequency_impact": 10,
        "distribution_tail_impact": 9,
        "data_availability": 8,
        "calibration_feasibility": 8,
        "engineering_complexity": 5,
        "dependency_priority": 10,
        "rationale": (
            "Highest remaining Layer 6 EV. Bullpen deployment drives late-game "
            "run prevention, totals tails, live markets, extras, and leverage-state realism."
        ),
        "recommended_next_layer": "layer_6ab_bullpen_sequencing_leverage_feasibility",
    },
    {
        "branch": "inning_distribution_tail_validation",
        "expected_modeling_ev": 9,
        "frequency_impact": 10,
        "distribution_tail_impact": 10,
        "data_availability": 8,
        "calibration_feasibility": 9,
        "engineering_complexity": 7,
        "dependency_priority": 8,
        "rationale": (
            "Critical measurement layer for validating whether realism changes improve "
            "inning-level scoring shape, clusters, and tails instead of only aggregate totals."
        ),
        "recommended_next_layer": "layer_6ab_inning_distribution_tail_validation",
    },
    {
        "branch": "extra_innings_ghost_runner",
        "expected_modeling_ev": 8,
        "frequency_impact": 5,
        "distribution_tail_impact": 9,
        "data_availability": 8,
        "calibration_feasibility": 8,
        "engineering_complexity": 6,
        "dependency_priority": 7,
        "rationale": (
            "High tail impact and important for walkoff/extras realism, but lower frequency "
            "than bullpen or general inning-tail validation."
        ),
        "recommended_next_layer": "layer_6ab_extra_innings_ghost_runner_realism",
    },
    {
        "branch": "pinch_hitters_substitutions",
        "expected_modeling_ev": 7,
        "frequency_impact": 6,
        "distribution_tail_impact": 7,
        "data_availability": 6,
        "calibration_feasibility": 5,
        "engineering_complexity": 3,
        "dependency_priority": 6,
        "rationale": (
            "Important for late-game realism and platoon matchups, but more complex and "
            "strategy-tree dependent than bullpen sequencing."
        ),
        "recommended_next_layer": "layer_6ab_pinch_hitter_substitution_feasibility",
    },
    {
        "branch": "stolen_base_engine",
        "expected_modeling_ev": 6,
        "frequency_impact": 5,
        "distribution_tail_impact": 6,
        "data_availability": 7,
        "calibration_feasibility": 7,
        "engineering_complexity": 6,
        "dependency_priority": 4,
        "rationale": (
            "Useful for base-state realism and team style, but less central to totals "
            "calibration than bullpen, inning tails, or extras."
        ),
        "recommended_next_layer": "layer_6ab_stolen_base_engine_feasibility",
    },
    {
        "branch": "subtype_probability_derivation_redesign",
        "expected_modeling_ev": 6,
        "frequency_impact": 8,
        "distribution_tail_impact": 6,
        "data_availability": 5,
        "calibration_feasibility": 5,
        "engineering_complexity": 4,
        "dependency_priority": 5,
        "rationale": (
            "Layer 6Z showed subtype transitions are safe but not competitive. Redesign is "
            "valuable research, but current evidence says it should not outrank bullpen."
        ),
        "recommended_next_layer": "layer_6ab_subtype_probability_derivation_redesign",
    },
    {
        "branch": "wild_pitch_passed_ball_balks",
        "expected_modeling_ev": 3,
        "frequency_impact": 3,
        "distribution_tail_impact": 4,
        "data_availability": 6,
        "calibration_feasibility": 5,
        "engineering_complexity": 7,
        "dependency_priority": 2,
        "rationale": (
            "Mechanically real but lower frequency and lower marginal EV. Better saved for "
            "later after higher-impact state mechanics are represented."
        ),
        "recommended_next_layer": "layer_6ab_wild_pitch_passed_ball_balks",
    },
]


def score_row(row):
    return round(
        sum(row[key] * weight for key, weight in WEIGHTS.items()),
        4,
    )


def main():
    df = pd.DataFrame(BRANCHES)

    df["priority_score"] = df.apply(score_row, axis=1)

    df = (
        df.sort_values(
            ["priority_score", "expected_modeling_ev", "distribution_tail_impact"],
            ascending=[False, False, False],
        )
        .reset_index(drop=True)
    )

    df["rank"] = df.index + 1

    ordered_columns = [
        "rank",
        "branch",
        "priority_score",
        "expected_modeling_ev",
        "frequency_impact",
        "distribution_tail_impact",
        "data_availability",
        "calibration_feasibility",
        "engineering_complexity",
        "dependency_priority",
        "recommended_next_layer",
        "rationale",
    ]

    df = df[ordered_columns]

    df.to_csv(OUTPUT_CSV, index=False)

    required_branches = {
        "bullpen_sequencing_leverage",
        "extra_innings_ghost_runner",
        "stolen_base_engine",
        "wild_pitch_passed_ball_balks",
        "pinch_hitters_substitutions",
        "inning_distribution_tail_validation",
        "subtype_probability_derivation_redesign",
    }

    branches_present = set(df["branch"])

    score_columns = list(WEIGHTS.keys())

    checks = [
        {
            "check": "required_branches_present",
            "passed": branches_present == required_branches,
            "detail": ",".join(sorted(branches_present)),
        },
        {
            "check": "priority_scores_non_null",
            "passed": df["priority_score"].notna().all(),
            "detail": int(df["priority_score"].notna().sum()),
        },
        {
            "check": "rankings_unique",
            "passed": df["rank"].is_unique,
            "detail": int(df["rank"].nunique()),
        },
        {
            "check": "scores_within_bounds",
            "passed": (
                df[score_columns]
                .apply(lambda col: col.between(1, 10).all())
                .all()
            ),
            "detail": "all_scores_1_to_10",
        },
        {
            "check": "top_rank_exists",
            "passed": (df["rank"] == 1).any(),
            "detail": df.loc[df["rank"] == 1, "branch"].iloc[0],
        },
    ]

    checks_df = pd.DataFrame(checks)
    checks_df.to_csv(OUTPUT_CHECKS, index=False)

    top = df.iloc[0]
    second = df.iloc[1]
    lowest = df.iloc[-1]

    bullpen_rank = int(
        df.loc[
            df["branch"] == "bullpen_sequencing_leverage",
            "rank",
        ].iloc[0]
    )

    subtype_rank = int(
        df.loc[
            df["branch"] == "subtype_probability_derivation_redesign",
            "rank",
        ].iloc[0]
    )

    recommended_next_step = str(top["recommended_next_layer"])

    payload = {
        "diagnosis": "game_state_realism_prioritization_complete",
        "top_priority_branch": str(top["branch"]),
        "top_priority_score": float(top["priority_score"]),
        "second_priority_branch": str(second["branch"]),
        "bullpen_rank": bullpen_rank,
        "subtype_redesign_rank": subtype_rank,
        "lowest_priority_branch": str(lowest["branch"]),
        "recommended_next_step": recommended_next_step,
    }

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
