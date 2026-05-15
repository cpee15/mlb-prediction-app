import json
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")

CALIBRATION_JSON = (
    TMP_DIR
    / "subtype_transition_calibration.json"
)

CONFIGS_CSV = (
    TMP_DIR
    / "subtype_transition_calibration_configs.csv"
)

COMPARISONS_CSV = (
    TMP_DIR
    / "subtype_transition_calibration_comparisons.csv"
)

RANKING_CSV = (
    TMP_DIR
    / "subtype_transition_calibration_ranking.csv"
)

OUTPUT_JSON = (
    TMP_DIR
    / "subtype_transition_decision.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "subtype_transition_decision_checks.csv"
)


REQUIRED_FILES = [
    CALIBRATION_JSON,
    CONFIGS_CSV,
    COMPARISONS_CSV,
    RANKING_CSV,
]


def write_outputs(
    payload,
    checks,
):

    OUTPUT_JSON.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    pd.DataFrame(checks).to_csv(
        OUTPUT_CHECKS,
        index=False,
    )



def get_rank(
    ranking_df,
    config_name,
):

    if ranking_df.empty:
        return None

    if "config" not in ranking_df.columns:
        return None

    if "rank" in ranking_df.columns:

        subset = ranking_df[
            ranking_df["config"]
            == config_name
        ]

        if subset.empty:
            return None

        return int(
            subset.iloc[0]["rank"]
        )

    config_values = list(
        ranking_df["config"]
    )

    if (
        config_name
        not in config_values
    ):
        return None

    return (
        config_values.index(
            config_name
        )
        + 1
    )


def get_metric(
    configs_df,
    config_name,
    metric,
):

    subset = configs_df[
        configs_df["config"]
        == config_name
    ]

    if subset.empty:
        return None

    if metric not in subset.columns:
        return None

    value = subset.iloc[0][metric]

    try:
        return float(value)
    except Exception:
        return None


def main():

    missing_files = [
        str(path)
        for path in REQUIRED_FILES
        if not path.exists()
    ]

    if missing_files:

        payload = {
            "diagnosis":
                (
                    "subtype_transition_decision_"
                    "insufficient_evidence"
                ),

            "decision":
                "insufficient_evidence",

            "missing_files":
                missing_files,
        }

        checks = [
            {
                "guardrail":
                    "required_files_present",

                "scope":
                    "system",

                "passed":
                    False,

                "detail":
                    ",".join(
                        missing_files
                    ),
            }
        ]

        write_outputs(
            payload,
            checks,
        )

        print(
            json.dumps(
                payload,
                indent=2,
            )
        )

        return

    calibration = json.loads(
        CALIBRATION_JSON.read_text()
    )

    configs_df = pd.read_csv(
        CONFIGS_CSV
    )

    comparisons_df = pd.read_csv(
        COMPARISONS_CSV
    )

    ranking_df = pd.read_csv(
        RANKING_CSV
    )

    checks = []

    def add_check(
        guardrail,
        scope,
        passed,
        detail,
    ):

        checks.append(
            {
                "guardrail":
                    guardrail,

                "scope":
                    scope,

                "passed":
                    bool(passed),

                "detail":
                    str(detail),
            }
        )

    best_config = calibration.get(
        "best_config"
    )

    parser_missing = calibration.get(
        "parser_missing_count",
        999,
    )

    parser_fallback = calibration.get(
        "parser_fallback_count",
        999,
    )

    actual_missing = calibration.get(
        "actual_missing_count",
        999,
    )

    actual_malformed = calibration.get(
        "actual_malformed_count",
        999,
    )

    subtype_mode_used = (
        calibration.get(
            "subtype_mode_used",
            False,
        )
    )

    default_activation_changed = (
        calibration.get(
            "default_activation_changed",
            True,
        )
    )

    add_check(
        "default_activation_unchanged",
        "safety",
        default_activation_changed
        is False,
        default_activation_changed,
    )

    add_check(
        "subtype_mode_used",
        "safety",
        subtype_mode_used
        is True,
        subtype_mode_used,
    )

    parser_clean = (
        parser_missing == 0
        and parser_fallback == 0
    )

    actual_clean = (
        actual_missing == 0
        and actual_malformed == 0
    )

    add_check(
        "parser_clean",
        "safety",
        parser_clean,
        (
            f"missing={parser_missing} "
            f"fallback={parser_fallback}"
        ),
    )

    add_check(
        "actual_clean",
        "safety",
        actual_clean,
        (
            f"missing={actual_missing} "
            f"malformed={actual_malformed}"
        ),
    )

    subtype_exists = (
        "subtype_prototype"
        in set(
            configs_df["config"]
        )
    )

    add_check(
        "subtype_config_exists",
        "candidate",
        subtype_exists,
        subtype_exists,
    )

    production_combined = get_metric(
        configs_df,
        "production_baseline",
        "combined_score",
    )

    subtype_combined = get_metric(
        configs_df,
        "subtype_prototype",
        "combined_score",
    )

    current_combined = get_metric(
        configs_df,
        "current_transition_script",
        "combined_score",
    )

    low_adv_combined = get_metric(
        configs_df,
        "low_advancement_profile",
        "combined_score",
    )

    subtype_total_mae = get_metric(
        configs_df,
        "subtype_prototype",
        "total_run_mae",
    )

    production_total_mae = get_metric(
        configs_df,
        "production_baseline",
        "total_run_mae",
    )

    subtype_brier = get_metric(
        configs_df,
        "subtype_prototype",
        "brier_score",
    )

    production_brier = get_metric(
        configs_df,
        "production_baseline",
        "brier_score",
    )

    subtype_log_loss = get_metric(
        configs_df,
        "subtype_prototype",
        "log_loss",
    )

    production_log_loss = get_metric(
        configs_df,
        "production_baseline",
        "log_loss",
    )

    subtype_beats_production = (
        subtype_combined
        is not None
        and production_combined
        is not None
        and subtype_combined
        <= production_combined
    )

    subtype_mae_beats_production = (
        subtype_total_mae
        is not None
        and production_total_mae
        is not None
        and subtype_total_mae
        <= production_total_mae
    )

    subtype_brier_beats_production = (
        subtype_brier
        is not None
        and production_brier
        is not None
        and subtype_brier
        <= production_brier
    )

    subtype_logloss_beats_production = (
        subtype_log_loss
        is not None
        and production_log_loss
        is not None
        and subtype_log_loss
        <= production_log_loss
    )

    add_check(
        "subtype_combined_lte_production",
        "default",
        subtype_beats_production,
        (
            f"{subtype_combined} <= "
            f"{production_combined}"
        ),
    )

    add_check(
        "subtype_total_mae_lte_production",
        "default",
        subtype_mae_beats_production,
        (
            f"{subtype_total_mae} <= "
            f"{production_total_mae}"
        ),
    )

    subtype_beats_current = (
        subtype_combined
        is not None
        and current_combined
        is not None
        and subtype_combined
        < current_combined
    )

    subtype_beats_low = (
        subtype_combined
        is not None
        and low_adv_combined
        is not None
        and subtype_combined
        < low_adv_combined
    )

    low_beats_subtype = (
        subtype_combined
        is not None
        and low_adv_combined
        is not None
        and low_adv_combined
        < subtype_combined
    )

    add_check(
        "subtype_beats_current",
        "comparison",
        subtype_beats_current,
        (
            f"{subtype_combined} < "
            f"{current_combined}"
        ),
    )

    add_check(
        "subtype_beats_low_advancement",
        "comparison",
        subtype_beats_low,
        (
            f"{subtype_combined} < "
            f"{low_adv_combined}"
        ),
    )

    add_check(
        "low_advancement_beats_subtype",
        "comparison",
        low_beats_subtype,
        (
            f"{low_adv_combined} < "
            f"{subtype_combined}"
        ),
    )

    within_current_tolerance = (
        subtype_combined
        is not None
        and current_combined
        is not None
        and subtype_combined
        <= (
            current_combined
            * 1.25
        )
    )

    add_check(
        "subtype_combined_within_current_tolerance",
        "candidate",
        within_current_tolerance,
        (
            f"{subtype_combined} <= "
            f"{current_combined} * 1.25"
        ),
    )

    default_promotion_allowed = all(
        [
            best_config
            == "subtype_prototype",

            parser_clean,

            actual_clean,

            subtype_mode_used,

            default_activation_changed
            is False,

            subtype_beats_production,

            subtype_mae_beats_production,

            subtype_brier_beats_production,

            subtype_logloss_beats_production,
        ]
    )

    candidate_retention_allowed = all(
        [
            subtype_mode_used,
            parser_clean,
            actual_clean,
            subtype_exists,
            default_activation_changed
            is False,
            within_current_tolerance,
        ]
    )

    hybridization_recommended = (
        best_config
        == "production_baseline"
        and low_beats_subtype
        and subtype_beats_current
    )

    parameter_tuning_recommended = (
        not subtype_beats_current
        and parser_clean
        and actual_clean
    )

    failed_default_guardrails = [
        row["guardrail"]
        for row in checks
        if (
            row["scope"]
            == "default"
            and not row["passed"]
        )
    ]

    passed_candidate_guardrails = [
        row["guardrail"]
        for row in checks
        if (
            row["scope"]
            in {
                "candidate",
                "safety",
            }
            and row["passed"]
        )
    ]

    subtype_rank = get_rank(
        ranking_df,
        "subtype_prototype",
    )

    production_rank = get_rank(
        ranking_df,
        "production_baseline",
    )

    low_advancement_rank = get_rank(
        ranking_df,
        "low_advancement_profile",
    )

    current_rank = get_rank(
        ranking_df,
        "current_transition_script",
    )

    known_ranks = [
        rank
        for rank in [
            subtype_rank,
            production_rank,
            low_advancement_rank,
            current_rank,
        ]
        if rank is not None
    ]

    distinct_rank_values = set(
        known_ranks
    )

    rank_values_distinct = (
        len(distinct_rank_values)
        > 1
        if len(known_ranks) > 1
        else True
    )

    add_check(
        "rank_values_distinct_when_possible",
        "evidence",
        rank_values_distinct,
        known_ranks,
    )

    best_rank_lookup = {
        "production_baseline":
            production_rank,

        "subtype_prototype":
            subtype_rank,

        "low_advancement_profile":
            low_advancement_rank,

        "current_transition_script":
            current_rank,
    }

    best_config_rank = (
        best_rank_lookup.get(
            best_config
        )
    )

    best_rank_matches = (
        best_config_rank == 1
    )

    add_check(
        "production_rank_matches_best_config",
        "evidence",
        best_rank_matches,
        (
            f"best_config={best_config} "
            f"rank={best_config_rank}"
        ),
    )

    add_check(
        "subtype_rank_present",
        "evidence",
        subtype_rank
        is not None,
        subtype_rank,
    )

    add_check(
        "low_advancement_rank_present",
        "evidence",
        low_advancement_rank
        is not None,
        low_advancement_rank,
    )

    add_check(
        "current_rank_present",
        "evidence",
        current_rank
        is not None,
        current_rank,
    )

    if default_promotion_allowed:

        diagnosis = (
            "subtype_transition_decision_"
            "promote_default"
        )

        decision = (
            "promote_subtype_to_default"
        )

        recommended_next_step = (
            "none"
        )

    elif hybridization_recommended:

        diagnosis = (
            "subtype_transition_decision_"
            "hybridize"
        )

        decision = (
            "hybridize_subtype_"
            "with_low_advancement"
        )

        recommended_next_step = (
            "layer_6y_hybrid_"
            "subtype_low_advancement_"
            "calibration"
        )

    elif candidate_retention_allowed:

        diagnosis = (
            "subtype_transition_decision_"
            "retain_candidate"
        )

        decision = (
            "retain_subtype_candidate_only"
        )

        recommended_next_step = (
            "layer_6y_subtype_"
            "calibration_sample_"
            "expansion_audit"
        )

    elif parameter_tuning_recommended:

        diagnosis = (
            "subtype_transition_decision_"
            "tune_parameters"
        )

        decision = (
            "tune_subtype_parameters"
        )

        recommended_next_step = (
            "layer_6y_subtype_"
            "transition_parameter_"
            "tuning_audit"
        )

    else:

        diagnosis = (
            "subtype_transition_decision_"
            "reject"
        )

        decision = (
            "reject_subtype_path"
        )

        recommended_next_step = (
            "none"
        )

    payload = {
        "diagnosis":
            diagnosis,

        "decision":
            decision,

        "default_promotion_allowed":
            default_promotion_allowed,

        "candidate_retention_allowed":
            candidate_retention_allowed,

        "hybridization_recommended":
            hybridization_recommended,

        "parameter_tuning_recommended":
            parameter_tuning_recommended,

        "failed_default_guardrails":
            failed_default_guardrails,

        "passed_candidate_guardrails":
            passed_candidate_guardrails,

        "evidence_summary": {
            "best_config":
                best_config,

            "subtype_rank":
                subtype_rank,

            "production_rank":
                production_rank,

            "low_advancement_rank":
                low_advancement_rank,

            "current_rank":
                current_rank,
        },

        "recommended_next_step":
            recommended_next_step,
    }

    write_outputs(
        payload,
        checks,
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )


if __name__ == "__main__":

    try:

        main()

    except Exception as exc:

        payload = {
            "diagnosis":
                (
                    "subtype_transition_"
                    "decision_failed"
                ),

            "decision":
                "failed",

            "error":
                str(exc),
        }

        OUTPUT_JSON.write_text(
            json.dumps(
                payload,
                indent=2,
            )
        )

        print(
            json.dumps(
                payload,
                indent=2,
            )
        )

        raise
