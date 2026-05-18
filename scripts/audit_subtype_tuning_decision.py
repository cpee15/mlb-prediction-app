import json
from pathlib import Path

import pandas as pd


ROOT = Path("tmp")

SUMMARY_PATH = (
    ROOT / "subtype_transition_parameter_tuning.json"
)

CONFIGS_PATH = (
    ROOT / "subtype_transition_parameter_tuning_configs.csv"
)

RANKING_PATH = (
    ROOT / "subtype_transition_parameter_tuning_ranking.csv"
)

CHECKS_PATH = (
    ROOT / "subtype_transition_parameter_tuning_checks.csv"
)

OUTPUT_JSON = (
    ROOT / "subtype_tuning_decision.json"
)

OUTPUT_CHECKS = (
    ROOT / "subtype_tuning_decision_checks.csv"
)


REQUIRED_FILES = [
    SUMMARY_PATH,
    CONFIGS_PATH,
    RANKING_PATH,
    CHECKS_PATH,
]


def fail_payload(
    diagnosis,
    decision,
    missing_files=None,
):

    payload = {
        "diagnosis": diagnosis,
        "decision": decision,
    }

    if missing_files is not None:
        payload["missing_files"] = missing_files

    OUTPUT_JSON.write_text(
        json.dumps(payload, indent=2)
    )

    print(
        json.dumps(payload, indent=2)
    )

    return payload


def load_inputs():

    missing = [
        str(p)
        for p in REQUIRED_FILES
        if not p.exists()
    ]

    if missing:
        fail_payload(
            diagnosis=(
                "subtype_tuning_decision_insufficient_evidence"
            ),
            decision="insufficient_evidence",
            missing_files=missing,
        )
        raise SystemExit(0)

    summary = json.loads(
        SUMMARY_PATH.read_text()
    )

    configs_df = pd.read_csv(
        CONFIGS_PATH
    )

    ranking_df = pd.read_csv(
        RANKING_PATH
    )

    checks_df = pd.read_csv(
        CHECKS_PATH
    )

    return (
        summary,
        configs_df,
        ranking_df,
        checks_df,
    )


def main():

    (
        summary,
        configs_df,
        ranking_df,
        checks_df,
    ) = load_inputs()

    parser_clean = (
        summary.get(
            "parser_missing_count",
            1,
        )
        == 0
        and
        summary.get(
            "parser_fallback_count",
            1,
        )
        == 0
    )

    actual_clean = (
        summary.get(
            "actual_missing_count",
            1,
        )
        == 0
    )

    default_equivalence_zero = (
        summary.get(
            "default_equivalence_differences",
            1,
        )
        == 0
    )

    subtype_rates_ignored_when_off = (
        summary.get(
            "subtype_rates_ignored_when_off",
            False,
        )
        is True
    )

    invalid_subtype_rates_rejected = (
        summary.get(
            "invalid_subtype_rates_rejected",
            False,
        )
        is True
    )

    default_activation_unchanged = (
        summary.get(
            "default_activation_changed",
            True,
        )
        is False
    )

    best_config = summary.get(
        "best_config"
    )

    best_subtype_config = summary.get(
        "best_subtype_config"
    )

    baseline_subtype_rank = int(
        summary.get(
            "baseline_subtype_rank",
            999,
        )
    )

    best_subtype_rank = int(
        summary.get(
            "best_subtype_rank",
            999,
        )
    )

    subtype_improved_over_baseline = (
        summary.get(
            "subtype_improved_over_baseline",
            False,
        )
        is True
    )

    best_subtype_beats_current = (
        summary.get(
            "best_subtype_beats_current",
            False,
        )
        is True
    )

    best_subtype_beats_low_advancement = (
        summary.get(
            "best_subtype_beats_low_advancement",
            False,
        )
        is True
    )

    subtype_configs = {
        "baseline_subtype",
        "conservative_advancement",
        "higher_double_play",
        "flyout_sacfly_heavy",
        "dead_ball_outs",
        "balanced_low_advancement",
    }

    best_config_is_subtype = (
        best_config in subtype_configs
    )

    promotion_allowed = all([
        best_config_is_subtype,
        best_subtype_rank == 1,
        parser_clean,
        actual_clean,
        default_equivalence_zero,
        subtype_rates_ignored_when_off,
        invalid_subtype_rates_rejected,
        default_activation_unchanged,
    ])

    experimental_retention_allowed = (
        parser_clean
        and actual_clean
        and default_equivalence_zero
        and subtype_rates_ignored_when_off
        and invalid_subtype_rates_rejected
        and default_activation_unchanged
        and best_subtype_rank <= 4
    )

    hybridization_recommended = (
        best_subtype_beats_current
        or best_subtype_beats_low_advancement
    )

    redesign_recommended = (
        best_subtype_rank >= 4
        and not subtype_improved_over_baseline
    )

    pause_recommended = all([
        best_config == "production_baseline",
        best_subtype_rank >= 4,
        not subtype_improved_over_baseline,
        not best_subtype_beats_current,
        not best_subtype_beats_low_advancement,
    ])

    rejection_recommended = (
        (
            not parser_clean
            or not actual_clean
            or not default_equivalence_zero
            or not subtype_rates_ignored_when_off
            or not invalid_subtype_rates_rejected
            or not default_activation_unchanged
        )
        or best_subtype_rank >= 8
    )

    failed_promotion_guardrails = []

    if not best_config_is_subtype:
        failed_promotion_guardrails.append(
            "best_config_not_subtype"
        )

    if best_subtype_rank != 1:
        failed_promotion_guardrails.append(
            "best_subtype_rank_not_1"
        )

    if not best_subtype_beats_current:
        failed_promotion_guardrails.append(
            "subtype_does_not_beat_current"
        )

    if not best_subtype_beats_low_advancement:
        failed_promotion_guardrails.append(
            "subtype_does_not_beat_low_advancement"
        )

    passed_safety_guardrails = []

    if parser_clean:
        passed_safety_guardrails.append(
            "parser_clean"
        )

    if actual_clean:
        passed_safety_guardrails.append(
            "actual_clean"
        )

    if default_equivalence_zero:
        passed_safety_guardrails.append(
            "default_equivalence_zero"
        )

    if subtype_rates_ignored_when_off:
        passed_safety_guardrails.append(
            "subtype_rates_ignored_when_off"
        )

    if invalid_subtype_rates_rejected:
        passed_safety_guardrails.append(
            "invalid_subtype_rates_rejected"
        )

    if default_activation_unchanged:
        passed_safety_guardrails.append(
            "default_activation_unchanged"
        )

    if promotion_allowed:

        diagnosis = (
            "subtype_tuning_decision_promote"
        )

        decision = (
            "promote_subtype_profile"
        )

    elif rejection_recommended:

        diagnosis = (
            "subtype_tuning_decision_reject"
        )

        decision = (
            "reject_subtype_branch"
        )

    elif hybridization_recommended:

        diagnosis = (
            "subtype_tuning_decision_hybridize"
        )

        decision = (
            "hybridize_subtype_with_existing_transition_profile"
        )

    elif redesign_recommended:

        diagnosis = (
            "subtype_tuning_decision_redesign_derivation"
        )

        decision = (
            "redesign_subtype_probability_derivation"
        )

    elif pause_recommended:

        diagnosis = (
            "subtype_tuning_decision_pause"
        )

        decision = (
            "pause_subtype_tuning"
        )

    elif experimental_retention_allowed:

        diagnosis = (
            "subtype_tuning_decision_retain_experimental"
        )

        decision = (
            "retain_subtype_experimental_only"
        )

    else:

        diagnosis = (
            "subtype_tuning_decision_failed"
        )

        decision = (
            "insufficient_evidence"
        )

    payload = {
        "diagnosis": diagnosis,
        "decision": decision,
        "promotion_allowed": promotion_allowed,
        "experimental_retention_allowed": (
            experimental_retention_allowed
        ),
        "hybridization_recommended": (
            hybridization_recommended
        ),
        "redesign_recommended": (
            redesign_recommended
        ),
        "pause_recommended": (
            pause_recommended
        ),
        "rejection_recommended": (
            rejection_recommended
        ),
        "failed_promotion_guardrails": (
            failed_promotion_guardrails
        ),
        "passed_safety_guardrails": (
            passed_safety_guardrails
        ),
        "evidence_summary": {
            "best_config": best_config,
            "best_subtype_config": (
                best_subtype_config
            ),
            "baseline_subtype_rank": (
                baseline_subtype_rank
            ),
            "best_subtype_rank": (
                best_subtype_rank
            ),
            "subtype_improved_over_baseline": (
                subtype_improved_over_baseline
            ),
            "best_subtype_beats_current": (
                best_subtype_beats_current
            ),
            "best_subtype_beats_low_advancement": (
                best_subtype_beats_low_advancement
            ),
        },
        "recommended_next_step": (
            "layer_7a_transition_realism_branch_prioritization"
        ),
    }

    OUTPUT_JSON.write_text(
        json.dumps(payload, indent=2)
    )

    checks_rows = [
        {
            "guardrail": (
                "required_files_present"
            ),
            "scope": "system",
            "passed": True,
            "detail": "all_present",
        },
        {
            "guardrail": "parser_clean",
            "scope": "safety",
            "passed": parser_clean,
            "detail": parser_clean,
        },
        {
            "guardrail": "actual_clean",
            "scope": "safety",
            "passed": actual_clean,
            "detail": actual_clean,
        },
        {
            "guardrail": (
                "default_equivalence_zero"
            ),
            "scope": "safety",
            "passed": (
                default_equivalence_zero
            ),
            "detail": (
                summary.get(
                    "default_equivalence_differences"
                )
            ),
        },
        {
            "guardrail": (
                "subtype_rates_ignored_when_off"
            ),
            "scope": "safety",
            "passed": (
                subtype_rates_ignored_when_off
            ),
            "detail": (
                subtype_rates_ignored_when_off
            ),
        },
        {
            "guardrail": (
                "invalid_subtype_rates_rejected"
            ),
            "scope": "safety",
            "passed": (
                invalid_subtype_rates_rejected
            ),
            "detail": (
                invalid_subtype_rates_rejected
            ),
        },
        {
            "guardrail": (
                "default_activation_unchanged"
            ),
            "scope": "safety",
            "passed": (
                default_activation_unchanged
            ),
            "detail": (
                default_activation_unchanged
            ),
        },
        {
            "guardrail": (
                "best_config_is_subtype"
            ),
            "scope": "promotion",
            "passed": (
                best_config_is_subtype
            ),
            "detail": best_config,
        },
        {
            "guardrail": (
                "best_subtype_rank_is_1"
            ),
            "scope": "promotion",
            "passed": (
                best_subtype_rank == 1
            ),
            "detail": (
                best_subtype_rank
            ),
        },
        {
            "guardrail": (
                "best_subtype_beats_current"
            ),
            "scope": "promotion",
            "passed": (
                best_subtype_beats_current
            ),
            "detail": (
                best_subtype_beats_current
            ),
        },
        {
            "guardrail": (
                "best_subtype_beats_low_advancement"
            ),
            "scope": "promotion",
            "passed": (
                best_subtype_beats_low_advancement
            ),
            "detail": (
                best_subtype_beats_low_advancement
            ),
        },
        {
            "guardrail": (
                "subtype_improved_over_baseline"
            ),
            "scope": "research",
            "passed": (
                subtype_improved_over_baseline
            ),
            "detail": (
                subtype_improved_over_baseline
            ),
        },
        {
            "guardrail": (
                "best_subtype_rank_lte_4"
            ),
            "scope": "research",
            "passed": (
                best_subtype_rank <= 4
            ),
            "detail": (
                best_subtype_rank
            ),
        },
        {
            "guardrail": (
                "best_subtype_rank_gte_8_reject"
            ),
            "scope": "rejection",
            "passed": (
                best_subtype_rank >= 8
            ),
            "detail": (
                best_subtype_rank
            ),
        },
    ]

    pd.DataFrame(
        checks_rows
    ).to_csv(
        OUTPUT_CHECKS,
        index=False,
    )

    print(
        json.dumps(payload, indent=2)
    )


if __name__ == "__main__":
    main()
