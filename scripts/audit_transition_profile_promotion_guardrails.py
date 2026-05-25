import csv
import json
from pathlib import Path

TMP_DIR = Path("tmp")

CALIBRATION_JSON = (
    TMP_DIR
    / "transition_profile_rate_calibration.json"
)

CALIBRATION_RANKING = (
    TMP_DIR
    / "transition_profile_rate_calibration_ranking.csv"
)

CALIBRATION_COMPARISONS = (
    TMP_DIR
    / "transition_profile_rate_calibration_comparisons.csv"
)

OUTPUT_JSON = (
    TMP_DIR
    / "transition_profile_promotion_guardrails.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "transition_profile_promotion_guardrails_checks.csv"
)

CANDIDATE_PROFILE = (
    "low_advancement_profile"
)

PRODUCTION_PROFILE = (
    "production_baseline"
)

CURRENT_PROFILE = (
    "current_transition_script"
)

BASELINE_CANDIDATE = (
    "baseline_candidate_6j_profile"
)

def load_json(path):

    with open(path, "r") as f:
        return json.load(f)

def load_csv(path):

    rows = []

    with open(path, "r") as f:

        reader = csv.DictReader(f)

        for row in reader:
            rows.append(row)

    return rows

def as_float(value):

    try:
        return float(value)

    except Exception:
        return None

def guardrail_row(
    name,
    scope,
    passed,
    detail,
):

    return {
        "guardrail":
            name,

        "scope":
            scope,

        "passed":
            passed,

        "detail":
            detail,
    }

def find_config(
    ranking_rows,
    config_name,
):

    for row in ranking_rows:

        if (
            row["config"]
            == config_name
        ):

            return row

    return None

def comparison_lookup(
    comparison_rows,
):

    lookup = {}

    for row in comparison_rows:

        lookup[
            row["comparison"]
        ] = row

    return lookup

def main():

    missing_files = []

    for path in [
        CALIBRATION_JSON,
        CALIBRATION_RANKING,
        CALIBRATION_COMPARISONS,
    ]:

        if not path.exists():

            missing_files.append(
                str(path)
            )

    if missing_files:

        summary = {
            "diagnosis":
                (
                    "transition_profile_guardrails_"
                    "insufficient_evidence"
                ),

            "decision":
                "insufficient_evidence",

            "missing_files":
                missing_files,

            "recommended_next_step":
                (
                    "rerun_layer_6p_"
                    "profile_rate_calibration"
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

        return

    calibration = load_json(
        CALIBRATION_JSON
    )

    ranking_rows = load_csv(
        CALIBRATION_RANKING
    )

    comparison_rows = load_csv(
        CALIBRATION_COMPARISONS
    )

    comparisons = comparison_lookup(
        comparison_rows
    )

    production = find_config(
        ranking_rows,
        PRODUCTION_PROFILE,
    )

    candidate = find_config(
        ranking_rows,
        CANDIDATE_PROFILE,
    )

    current = find_config(
        ranking_rows,
        CURRENT_PROFILE,
    )

    baseline_candidate = find_config(
        ranking_rows,
        BASELINE_CANDIDATE,
    )

    checks = []

    failed_default_guardrails = []
    passed_candidate_guardrails = []
    failed_candidate_guardrails = []

    # --------------------------------------------------
    # Global safety checks
    # --------------------------------------------------

    parser_missing_zero = (
        calibration[
            "parser_missing_count"
        ]
        == 0
    )

    checks.append(
        guardrail_row(
            "parser_missing_zero",
            "safety",
            parser_missing_zero,
            str(
                calibration[
                    "parser_missing_count"
                ]
            ),
        )
    )

    actual_missing_zero = (
        calibration[
            "actual_missing_count"
        ]
        == 0
    )

    checks.append(
        guardrail_row(
            "actual_missing_zero",
            "safety",
            actual_missing_zero,
            str(
                calibration[
                    "actual_missing_count"
                ]
            ),
        )
    )

    profile_rate_usage_confirmed = (
        calibration[
            "profile_rate_usage_confirmed"
        ]
        is True
    )

    checks.append(
        guardrail_row(
            (
                "profile_rate_usage_confirmed"
            ),
            "safety",
            (
                profile_rate_usage_confirmed
            ),
            str(
                profile_rate_usage_confirmed
            ),
        )
    )

    default_profile_unchanged = (
        calibration[
            "default_profile_unchanged"
        ]
        is True
    )

    checks.append(
        guardrail_row(
            (
                "default_profile_unchanged"
            ),
            "safety",
            default_profile_unchanged,
            str(
                default_profile_unchanged
            ),
        )
    )

    # --------------------------------------------------
    # Default promotion checks
    # --------------------------------------------------

    default_checks = []

    candidate_total_mae = as_float(
        candidate[
            "total_run_mae"
        ]
    )

    production_total_mae = as_float(
        production[
            "total_run_mae"
        ]
    )

    total_mae_guard = (
        candidate_total_mae
        <= production_total_mae
    )

    checks.append(
        guardrail_row(
            (
                "candidate_total_mae_"
                "lte_production"
            ),
            "default",
            total_mae_guard,
            (
                f"{candidate_total_mae}"
                " <= "
                f"{production_total_mae}"
            ),
        )
    )

    default_checks.append(
        total_mae_guard
    )

    if not total_mae_guard:

        failed_default_guardrails.append(
            (
                "candidate_total_mae_"
                "lte_production"
            )
        )

    candidate_brier = as_float(
        candidate[
            "brier_score"
        ]
    )

    production_brier = as_float(
        production[
            "brier_score"
        ]
    )

    brier_guard = (
        candidate_brier
        <= production_brier
    )

    checks.append(
        guardrail_row(
            (
                "candidate_brier_"
                "lte_production"
            ),
            "default",
            brier_guard,
            (
                f"{candidate_brier}"
                " <= "
                f"{production_brier}"
            ),
        )
    )

    default_checks.append(
        brier_guard
    )

    if not brier_guard:

        failed_default_guardrails.append(
            (
                "candidate_brier_"
                "lte_production"
            )
        )

    candidate_log_loss = as_float(
        candidate[
            "log_loss"
        ]
    )

    production_log_loss = as_float(
        production[
            "log_loss"
        ]
    )

    log_loss_guard = (
        candidate_log_loss
        <= production_log_loss
    )

    checks.append(
        guardrail_row(
            (
                "candidate_log_loss_"
                "lte_production"
            ),
            "default",
            log_loss_guard,
            (
                f"{candidate_log_loss}"
                " <= "
                f"{production_log_loss}"
            ),
        )
    )

    default_checks.append(
        log_loss_guard
    )

    if not log_loss_guard:

        failed_default_guardrails.append(
            (
                "candidate_log_loss_"
                "lte_production"
            )
        )

    candidate_combined = as_float(
        candidate[
            "combined_score"
        ]
    )

    production_combined = as_float(
        production[
            "combined_score"
        ]
    )

    combined_guard = (
        candidate_combined
        <= production_combined
    )

    checks.append(
        guardrail_row(
            (
                "candidate_combined_"
                "lte_production"
            ),
            "default",
            combined_guard,
            (
                f"{candidate_combined}"
                " <= "
                f"{production_combined}"
            ),
        )
    )

    default_checks.append(
        combined_guard
    )

    if not combined_guard:

        failed_default_guardrails.append(
            (
                "candidate_combined_"
                "lte_production"
            )
        )

    candidate_bias = abs(
        as_float(
            candidate[
                "total_bias"
            ]
        )
    )

    production_bias = abs(
        as_float(
            production[
                "total_bias"
            ]
        )
    )

    bias_guard = (
        candidate_bias
        <= (
            production_bias
            + 0.10
        )
    )

    checks.append(
        guardrail_row(
            (
                "candidate_bias_"
                "within_production"
            ),
            "default",
            bias_guard,
            (
                f"{candidate_bias}"
                " <= "
                f"{production_bias + 0.10}"
            ),
        )
    )

    default_checks.append(
        bias_guard
    )

    if not bias_guard:

        failed_default_guardrails.append(
            (
                "candidate_bias_"
                "within_production"
            )
        )

    candidate_accuracy = as_float(
        candidate[
            "winner_accuracy"
        ]
    )

    production_accuracy = as_float(
        production[
            "winner_accuracy"
        ]
    )

    accuracy_guard = (
        candidate_accuracy
        >= (
            production_accuracy
            - 0.01
        )
    )

    checks.append(
        guardrail_row(
            (
                "candidate_accuracy_"
                "near_production"
            ),
            "default",
            accuracy_guard,
            (
                f"{candidate_accuracy}"
                " >= "
                f"{production_accuracy - 0.01}"
            ),
        )
    )

    default_checks.append(
        accuracy_guard
    )

    if not accuracy_guard:

        failed_default_guardrails.append(
            (
                "candidate_accuracy_"
                "near_production"
            )
        )

    default_promotion_allowed = (
        parser_missing_zero
        and actual_missing_zero
        and profile_rate_usage_confirmed
        and default_profile_unchanged
        and all(default_checks)
    )

    # --------------------------------------------------
    # Candidate-only retention checks
    # --------------------------------------------------

    candidate_checks = []

    current_delta = comparisons[
        (
            "low_advancement_profile"
            "_minus_"
            "current_transition_script"
        )
    ]

    baseline_delta = comparisons[
        (
            "low_advancement_profile"
            "_minus_"
            "baseline_candidate_6j_profile"
        )
    ]

    current_improvement = (
        as_float(
            current_delta[
                "total_abs_error_delta"
            ]
        )
        < 0
    )

    checks.append(
        guardrail_row(
            (
                "candidate_total_mae_"
                "lt_current"
            ),
            "candidate",
            current_improvement,
            (
                current_delta[
                    "total_abs_error_delta"
                ]
            ),
        )
    )

    candidate_checks.append(
        current_improvement
    )

    if current_improvement:

        passed_candidate_guardrails.append(
            (
                "candidate_total_mae_"
                "lt_current"
            )
        )

    else:

        failed_candidate_guardrails.append(
            (
                "candidate_total_mae_"
                "lt_current"
            )
        )

    baseline_improvement = (
        as_float(
            baseline_delta[
                "total_abs_error_delta"
            ]
        )
        < 0
    )

    checks.append(
        guardrail_row(
            (
                "candidate_total_mae_"
                "lt_baseline_candidate"
            ),
            "candidate",
            baseline_improvement,
            (
                baseline_delta[
                    "total_abs_error_delta"
                ]
            ),
        )
    )

    candidate_checks.append(
        baseline_improvement
    )

    if baseline_improvement:

        passed_candidate_guardrails.append(
            (
                "candidate_total_mae_"
                "lt_baseline_candidate"
            )
        )

    else:

        failed_candidate_guardrails.append(
            (
                "candidate_total_mae_"
                "lt_baseline_candidate"
            )
        )

    parser_safety = (
        calibration[
            "parser_missing_count"
        ]
        == 0
        and calibration[
            "parser_fallback_count"
        ]
        == 0
    )

    checks.append(
        guardrail_row(
            (
                "candidate_parser_"
                "safety"
            ),
            "candidate",
            parser_safety,
            (
                f"missing="
                f"{calibration['parser_missing_count']}, "
                f"fallback="
                f"{calibration['parser_fallback_count']}"
            ),
        )
    )

    candidate_checks.append(
        parser_safety
    )

    if parser_safety:

        passed_candidate_guardrails.append(
            (
                "candidate_parser_"
                "safety"
            )
        )

    else:

        failed_candidate_guardrails.append(
            (
                "candidate_parser_"
                "safety"
            )
        )

    candidate_only_retention_allowed = (
        profile_rate_usage_confirmed
        and default_profile_unchanged
        and parser_missing_zero
        and actual_missing_zero
        and all(candidate_checks)
    )

    # --------------------------------------------------
    # Final decision
    # --------------------------------------------------

    diagnosis = (
        "transition_profile_guardrails_"
        "hold_candidate"
    )

    decision = (
        "hold_candidate_profile"
    )

    if (
        default_promotion_allowed
    ):

        diagnosis = (
            "transition_profile_guardrails_"
            "default_promotion_allowed"
        )

        decision = (
            "promote_to_default_allowed"
        )

    elif (
        candidate_only_retention_allowed
    ):

        diagnosis = (
            "transition_profile_guardrails_"
            "hold_candidate"
        )

        decision = (
            "hold_candidate_profile"
        )

    else:

        diagnosis = (
            "transition_profile_guardrails_"
            "reject_candidate"
        )

        decision = (
            "reject_candidate_profile"
        )

    summary = {
        "diagnosis":
            diagnosis,

        "decision":
            decision,

        "candidate_profile":
            CANDIDATE_PROFILE,

        "default_promotion_allowed":
            default_promotion_allowed,

        "candidate_only_retention_allowed":
            (
                candidate_only_retention_allowed
            ),

        "failed_default_guardrails":
            failed_default_guardrails,

        "passed_candidate_guardrails":
            passed_candidate_guardrails,

        "failed_candidate_guardrails":
            failed_candidate_guardrails,

        "evidence_summary": {
            "best_config":
                calibration[
                    "best_config"
                ],

            "games_attempted":
                calibration[
                    "games_attempted"
                ],

            "profile_rate_usage_confirmed":
                calibration[
                    "profile_rate_usage_confirmed"
                ],

            "default_profile_unchanged":
                calibration[
                    "default_profile_unchanged"
                ],
        },

        "recommended_next_step":
            (
                "layer_6r_outcome_subtype_"
                "realism_audit"
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

    with open(
        OUTPUT_CHECKS,
        "w",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=[
                "guardrail",
                "scope",
                "passed",
                "detail",
            ],
        )

        writer.writeheader()
        writer.writerows(checks)

    print(
        json.dumps(
            summary,
            indent=2,
        )
    )

if __name__ == "__main__":

    main()
