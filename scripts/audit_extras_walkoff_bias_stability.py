from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


OUTPUT_DIR = Path("tmp")

CALIBRATION_JSON = (
    OUTPUT_DIR / "extras_walkoff_calibration.json"
)

GAMES_CSV = (
    OUTPUT_DIR / "extras_walkoff_calibration_games.csv"
)

BOOTSTRAP_CSV = (
    OUTPUT_DIR / "extras_walkoff_calibration_bootstrap.csv"
)

SPLITS_CSV = (
    OUTPUT_DIR / "extras_walkoff_calibration_splits.csv"
)

REGIMES_CSV = (
    OUTPUT_DIR / "extras_walkoff_calibration_regimes.csv"
)


def require(path: Path):

    if not path.exists():
        raise FileNotFoundError(
            f"Required artifact missing: {path}"
        )


for path in [
    CALIBRATION_JSON,
    GAMES_CSV,
    BOOTSTRAP_CSV,
    SPLITS_CSV,
    REGIMES_CSV,
]:
    require(path)


games = pd.read_csv(GAMES_CSV)
bootstrap = pd.read_csv(BOOTSTRAP_CSV)
splits = pd.read_csv(SPLITS_CSV)
regimes = pd.read_csv(REGIMES_CSV)

with open(CALIBRATION_JSON) as handle:
    calibration = json.load(handle)


def safe_mean(series):
    if len(series) == 0:
        return None
    return float(series.mean())


def safe_std(series):
    if len(series) == 0:
        return None
    return float(series.std())


def json_safe(value):
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}

    if isinstance(value, list):
        return [json_safe(v) for v in value]

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    return value


def metric_row(metric_name):

    subset = bootstrap[
        bootstrap["metric"] == metric_name
    ]

    if len(subset) == 0:
        return None

    row = subset.iloc[0]

    return {
        "metric":
            metric_name,

        "mean_delta":
            float(row["mean_delta"]),

        "ci_lower_2_5":
            float(row["ci_lower_2_5"]),

        "ci_upper_97_5":
            float(row["ci_upper_97_5"]),

        "p_improvement":
            float(row["p_improvement"]),

        "ci_excludes_zero":
            (
                row["ci_upper_97_5"] < 0
                or row["ci_lower_2_5"] > 0
            ),
    }


bootstrap_summary = {
    metric: metric_row(metric)
    for metric in [
        "total_abs_error_delta",
        "home_abs_error_delta",
        "away_abs_error_delta",
        "brier_delta",
        "log_loss_delta",
        "winner_correct_delta",
        "total_expected_runs_delta",
        "home_win_probability_delta",
    ]
}


games["actual_total_std_anchor"] = (
    games["actual_total_runs"]
)

games["baseline_total_std_anchor"] = (
    games["baseline_total_expected_runs"]
)

games["candidate_total_std_anchor"] = (
    games["candidate_total_expected_runs"]
)

distribution_shift = {
    "actual_total_std":
        safe_std(
            games[
                "actual_total_std_anchor"
            ]
        ),

    "baseline_total_expected_std":
        safe_std(
            games[
                "baseline_total_std_anchor"
            ]
        ),

    "candidate_total_expected_std":
        safe_std(
            games[
                "candidate_total_std_anchor"
            ]
        ),

    "baseline_total_expected_mean":
        safe_mean(
            games[
                "baseline_total_expected_runs"
            ]
        ),

    "candidate_total_expected_mean":
        safe_mean(
            games[
                "candidate_total_expected_runs"
            ]
        ),

    "actual_total_mean":
        safe_mean(
            games[
                "actual_total_runs"
            ]
        ),
}


games["baseline_signed_total_error"] = (
    games["baseline_total_expected_runs"]
    - games["actual_total_runs"]
)

games["candidate_signed_total_error"] = (
    games["candidate_total_expected_runs"]
    - games["actual_total_runs"]
)

games["baseline_signed_home_error"] = (
    games["baseline_home_expected_runs"]
    - games["actual_home_runs"]
)

games["candidate_signed_home_error"] = (
    games["candidate_home_expected_runs"]
    - games["actual_home_runs"]
)

games["baseline_signed_away_error"] = (
    games["baseline_away_expected_runs"]
    - games["actual_away_runs"]
)

games["candidate_signed_away_error"] = (
    games["candidate_away_expected_runs"]
    - games["actual_away_runs"]
)


bias_shift = {
    "baseline_total_bias":
        safe_mean(
            games[
                "baseline_signed_total_error"
            ]
        ),

    "candidate_total_bias":
        safe_mean(
            games[
                "candidate_signed_total_error"
            ]
        ),

    "baseline_home_bias":
        safe_mean(
            games[
                "baseline_signed_home_error"
            ]
        ),

    "candidate_home_bias":
        safe_mean(
            games[
                "candidate_signed_home_error"
            ]
        ),

    "baseline_away_bias":
        safe_mean(
            games[
                "baseline_signed_away_error"
            ]
        ),

    "candidate_away_bias":
        safe_mean(
            games[
                "candidate_signed_away_error"
            ]
        ),
}


games["baseline_underprediction"] = (
    games["baseline_signed_total_error"] < 0
)

games["candidate_underprediction"] = (
    games["candidate_signed_total_error"] < 0
)

games["baseline_overprediction"] = (
    games["baseline_signed_total_error"] > 0
)

games["candidate_overprediction"] = (
    games["candidate_signed_total_error"] > 0
)


prediction_shape = {
    "baseline_underprediction_rate":
        safe_mean(
            games[
                "baseline_underprediction"
            ]
        ),

    "candidate_underprediction_rate":
        safe_mean(
            games[
                "candidate_underprediction"
            ]
        ),

    "baseline_overprediction_rate":
        safe_mean(
            games[
                "baseline_overprediction"
            ]
        ),

    "candidate_overprediction_rate":
        safe_mean(
            games[
                "candidate_overprediction"
            ]
        ),
}


games["high_tail_actual"] = (
    games["actual_total_runs"] >= 13
)

games["low_tail_actual"] = (
    games["actual_total_runs"] <= 4
)

games["baseline_high_tail_miss"] = (
    games["high_tail_actual"]
    &
    (
        games[
            "baseline_total_expected_runs"
        ] < 10
    )
)

games["candidate_high_tail_miss"] = (
    games["high_tail_actual"]
    &
    (
        games[
            "candidate_total_expected_runs"
        ] < 10
    )
)

games["baseline_low_tail_miss"] = (
    games["low_tail_actual"]
    &
    (
        games[
            "baseline_total_expected_runs"
        ] > 7
    )
)

games["candidate_low_tail_miss"] = (
    games["low_tail_actual"]
    &
    (
        games[
            "candidate_total_expected_runs"
        ] > 7
    )
)


tail_behavior = {
    "baseline_high_tail_miss_rate":
        safe_mean(
            games[
                "baseline_high_tail_miss"
            ]
        ),

    "candidate_high_tail_miss_rate":
        safe_mean(
            games[
                "candidate_high_tail_miss"
            ]
        ),

    "baseline_low_tail_miss_rate":
        safe_mean(
            games[
                "baseline_low_tail_miss"
            ]
        ),

    "candidate_low_tail_miss_rate":
        safe_mean(
            games[
                "candidate_low_tail_miss"
            ]
        ),
}


games["close_game"] = (
    (
        games["actual_home_runs"]
        - games["actual_away_runs"]
    ).abs()
    <= 1
)

games["blowout_game"] = (
    (
        games["actual_home_runs"]
        - games["actual_away_runs"]
    ).abs()
    >= 5
)


regime_findings = []

for regime_name, subset in {
    "close_games":
        games[
            games["close_game"]
        ],

    "blowout_games":
        games[
            games["blowout_game"]
        ],

    "low_scoring":
        games[
            games["actual_total_runs"] <= 7
        ],

    "high_scoring":
        games[
            games["actual_total_runs"] >= 11
        ],
}.items():

    regime_findings.append(
        {
            "regime":
                regime_name,

            "games":
                int(len(subset)),

            "baseline_total_bias":
                safe_mean(
                    subset[
                        "baseline_signed_total_error"
                    ]
                ),

            "candidate_total_bias":
                safe_mean(
                    subset[
                        "candidate_signed_total_error"
                    ]
                ),

            "baseline_total_mae":
                safe_mean(
                    subset[
                        "baseline_total_abs_error"
                    ]
                ),

            "candidate_total_mae":
                safe_mean(
                    subset[
                        "candidate_total_abs_error"
                    ]
                ),

            "candidate_minus_baseline_mae":
                safe_mean(
                    subset[
                        "candidate_total_abs_error"
                    ]
                    -
                    subset[
                        "baseline_total_abs_error"
                    ]
                ),
        }
    )


split_findings = splits.to_dict(
    orient="records"
)

metric_stability = []

for metric, info in bootstrap_summary.items():

    if info is None:
        continue

    directionally_positive = (
        info["mean_delta"] < 0
        if metric != "winner_correct_delta"
        else info["mean_delta"] > 0
    )

    stable = (
        directionally_positive
        and info["ci_excludes_zero"]
    )

    metric_stability.append(
        {
            "metric":
                metric,

            "mean_delta":
                info["mean_delta"],

            "ci_lower_2_5":
                info["ci_lower_2_5"],

            "ci_upper_97_5":
                info["ci_upper_97_5"],

            "p_improvement":
                info["p_improvement"],

            "stable":
                stable,
        }
    )


candidate_bias_shift = (
    bias_shift[
        "candidate_total_bias"
    ]
    -
    bias_shift[
        "baseline_total_bias"
    ]
)


distribution_shift_assessment = (
    "candidate_compresses_scoring_distribution"
    if (
        distribution_shift[
            "candidate_total_expected_std"
        ]
        <
        distribution_shift[
            "baseline_total_expected_std"
        ]
    )
    else
    "candidate_preserves_or_expands_distribution"
)


bias_shift_assessment = (
    "candidate_pushes_totals_downward"
    if candidate_bias_shift < -0.5
    else
    "candidate_bias_shift_within_tolerance"
)


stable_metrics = [
    row
    for row in metric_stability
    if row["stable"]
]

stability_assessment = (
    "candidate_has_stable_metric_improvements"
    if len(stable_metrics) >= 2
    else
    "candidate_improvements_not_yet_stable"
)


if (
    bias_shift_assessment
    ==
    "candidate_pushes_totals_downward"
    and distribution_shift_assessment
    ==
    "candidate_compresses_scoring_distribution"
):
    diagnosis = (
        "extras_walkoff_candidate_improves_but_biases_totals_downward"
    )

elif (
    len(stable_metrics) >= 3
):
    diagnosis = (
        "extras_walkoff_candidate_stable_and_retained"
    )

elif (
    len(stable_metrics) == 0
):
    diagnosis = (
        "extras_walkoff_candidate_noise_floor"
    )

else:
    diagnosis = (
        "extras_walkoff_candidate_unstable_across_splits"
    )


summary = {
    "diagnosis":
        diagnosis,

    "candidate_status":
        calibration[
            "summary"
        ][
            "diagnosis"
        ],

    "distribution_shift_assessment":
        distribution_shift_assessment,

    "bias_shift_assessment":
        bias_shift_assessment,

    "stability_assessment":
        stability_assessment,

    "distribution_shift":
        distribution_shift,

    "bias_shift":
        bias_shift,

    "prediction_shape":
        prediction_shape,

    "tail_behavior":
        tail_behavior,

    "bootstrap_summary":
        bootstrap_summary,

    "stable_metrics":
        stable_metrics,

    "regime_findings":
        regime_findings,

    "split_findings":
        split_findings,

    "recommended_next_step":
        (
            "investigate_hybrid_pairing_or_base_out_state_before_production"
            if diagnosis
            ==
            "extras_walkoff_candidate_improves_but_biases_totals_downward"
            else
            "retain_candidate_for_future_testing"
        ),
}


with open(
    OUTPUT_DIR
    /
    "extras_walkoff_bias_stability_summary.json",
    "w",
) as handle:

    json.dump(
        json_safe(summary),
        handle,
        indent=2,
    )


pd.DataFrame(
    regime_findings
).to_csv(
    OUTPUT_DIR
    /
    "extras_walkoff_bias_by_regime.csv",
    index=False,
)


pd.DataFrame(
    split_findings
).to_csv(
    OUTPUT_DIR
    /
    "extras_walkoff_bias_by_split.csv",
    index=False,
)


pd.DataFrame(
    [distribution_shift]
).to_csv(
    OUTPUT_DIR
    /
    "extras_walkoff_distribution_shift.csv",
    index=False,
)


pd.DataFrame(
    metric_stability
).to_csv(
    OUTPUT_DIR
    /
    "extras_walkoff_metric_stability.csv",
    index=False,
)


print(
    json.dumps(
        json_safe(summary),
        indent=2,
    )
)
