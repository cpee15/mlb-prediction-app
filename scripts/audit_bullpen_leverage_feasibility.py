import json
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_CSV = TMP_DIR / "bullpen_leverage_feasibility.csv"
OUTPUT_CHECKS = TMP_DIR / "bullpen_leverage_feasibility_checks.csv"


SUBSYSTEMS = [
    {
        "subsystem": "data_availability",
        "feasibility_score": 8,
        "implementation_risk": 4,
        "calibration_confidence": 8,
        "notes": (
            "Bullpen usage, reliever stats, pitcher handedness, recent appearances, "
            "and game logs are generally derivable from existing MLB data paths. "
            "Role labels may need to be inferred rather than directly sourced."
        ),
    },
    {
        "subsystem": "candidate_activation",
        "feasibility_score": 9,
        "implementation_risk": 3,
        "calibration_confidence": 9,
        "notes": (
            "Prior Layer 6 transition work established candidate-only modes, default "
            "equivalence checks, and promotion guardrails. Bullpen sequencing can follow "
            "the same candidate activation pattern."
        ),
    },
    {
        "subsystem": "fatigue_modeling",
        "feasibility_score": 7,
        "implementation_risk": 7,
        "calibration_confidence": 7,
        "notes": (
            "Recent workload can be approximated from prior appearances, pitch counts, "
            "back-to-back usage, and innings pitched. Main complexity is preserving "
            "state across simulated innings and games without leaking into defaults."
        ),
    },
    {
        "subsystem": "leverage_modeling",
        "feasibility_score": 8,
        "implementation_risk": 7,
        "calibration_confidence": 7,
        "notes": (
            "Leverage can be approximated from inning, score differential, base/out state, "
            "save situations, and late-inning pressure. Calibration should compare actual "
            "reliever deployment by game state."
        ),
    },
    {
        "subsystem": "pitcher_chain_modeling",
        "feasibility_score": 6,
        "implementation_risk": 8,
        "calibration_confidence": 6,
        "notes": (
            "This is the hardest subsystem. A realistic bullpen engine needs stateful "
            "pitcher sequencing, availability filtering, role hierarchy, fatigue, and "
            "fallback replacement logic."
        ),
    },
    {
        "subsystem": "extra_inning_support",
        "feasibility_score": 6,
        "implementation_risk": 7,
        "calibration_confidence": 6,
        "notes": (
            "Bullpen depletion directly affects extra innings. Feasible after pitcher-chain "
            "modeling exists, but should not be first implementation target."
        ),
    },
    {
        "subsystem": "calibration_feasibility",
        "feasibility_score": 8,
        "implementation_risk": 5,
        "calibration_confidence": 8,
        "notes": (
            "Can be calibrated against actual reliever usage by inning, score margin, "
            "save situations, late-game run prevention, and bullpen innings distributions."
        ),
    },
    {
        "subsystem": "backtest_feasibility",
        "feasibility_score": 8,
        "implementation_risk": 5,
        "calibration_confidence": 8,
        "notes": (
            "Can reuse Layer 6 backtest architecture: production baseline versus "
            "candidate bullpen mode, with default equivalence and parser-safety checks."
        ),
    },
    {
        "subsystem": "production_safety",
        "feasibility_score": 9,
        "implementation_risk": 3,
        "calibration_confidence": 9,
        "notes": (
            "Safe if implemented as opt-in candidate-only mode with default-off behavior, "
            "strict fallback to current starter/bullpen assumptions, and explicit guardrails."
        ),
    },
]


INTEGRATION_POINTS = [
    "mlb_app/simulation/game_engine_v2.py",
    "mlb_app/simulation/game_simulation_builder.py",
    "mlb_app/simulation/inning_simulator.py",
    "mlb_app/matchup_generator.py",
    "mlb_app/simulation/transition_profiles.py",
    "scripts/backtest_subtype_transition_parameter_tuning.py",
]


REQUIRED_SUBSYSTEMS = {
    "data_availability",
    "candidate_activation",
    "fatigue_modeling",
    "leverage_modeling",
    "pitcher_chain_modeling",
    "extra_inning_support",
    "calibration_feasibility",
    "backtest_feasibility",
    "production_safety",
}


def main():
    df = pd.DataFrame(SUBSYSTEMS)

    df["net_feasibility_score"] = (
        df["feasibility_score"]
        + df["calibration_confidence"] * 0.5
        - df["implementation_risk"] * 0.25
    ).round(4)

    df = (
        df.sort_values(
            ["net_feasibility_score", "feasibility_score"],
            ascending=[False, False],
        )
        .reset_index(drop=True)
    )

    df["rank"] = df.index + 1

    df = df[
        [
            "rank",
            "subsystem",
            "feasibility_score",
            "implementation_risk",
            "calibration_confidence",
            "net_feasibility_score",
            "notes",
        ]
    ]

    df.to_csv(OUTPUT_CSV, index=False)

    overall_feasibility_score = round(
        df["feasibility_score"].mean(),
        4,
    )

    overall_risk_score = round(
        df["implementation_risk"].mean(),
        4,
    )

    overall_calibration_score = round(
        df["calibration_confidence"].mean(),
        4,
    )

    candidate_activation_supported = bool(
        df.loc[
            df["subsystem"] == "candidate_activation",
            "feasibility_score",
        ].iloc[0]
        >= 8
    )

    production_default_safe = bool(
        df.loc[
            df["subsystem"] == "production_safety",
            "feasibility_score",
        ].iloc[0]
        >= 8
    )

    highest_risk_subsystem = str(
        df.sort_values(
            "implementation_risk",
            ascending=False,
        ).iloc[0]["subsystem"]
    )

    integration_points_identified = (
        len(INTEGRATION_POINTS) >= 4
    )

    required_present = (
        set(df["subsystem"]) == REQUIRED_SUBSYSTEMS
    )

    scores_non_null = bool(
        df[
            [
                "feasibility_score",
                "implementation_risk",
                "calibration_confidence",
            ]
        ].notna().all().all()
    )

    scores_within_bounds = bool(
        df[
            [
                "feasibility_score",
                "implementation_risk",
                "calibration_confidence",
            ]
        ]
        .apply(lambda col: col.between(1, 10).all())
        .all()
    )

    if (
        candidate_activation_supported
        and production_default_safe
        and overall_feasibility_score >= 7
        and overall_risk_score <= 7
        and overall_calibration_score >= 7
    ):
        decision = "proceed_to_candidate_bullpen_engine"
        diagnosis = "bullpen_leverage_feasibility_complete"
        recommended_next_step = (
            "layer_6ac_bullpen_candidate_architecture"
        )

    elif not scores_non_null or not required_present:
        decision = "needs_data_pipeline_first"
        diagnosis = "bullpen_leverage_feasibility_incomplete"
        recommended_next_step = (
            "layer_6ac_bullpen_data_pipeline_audit"
        )

    elif overall_risk_score > 8:
        decision = "too_high_risk_for_layer_6"
        diagnosis = "bullpen_leverage_feasibility_too_high_risk"
        recommended_next_step = (
            "layer_6ac_inning_distribution_tail_validation"
        )

    else:
        decision = "needs_architecture_redesign"
        diagnosis = "bullpen_leverage_feasibility_needs_architecture"
        recommended_next_step = (
            "layer_6ac_bullpen_architecture_redesign"
        )

    checks = [
        {
            "check": "required_subsystems_present",
            "passed": required_present,
            "detail": ",".join(sorted(df["subsystem"])),
        },
        {
            "check": "scores_non_null",
            "passed": scores_non_null,
            "detail": scores_non_null,
        },
        {
            "check": "scores_within_bounds",
            "passed": scores_within_bounds,
            "detail": "scores_1_to_10",
        },
        {
            "check": "candidate_activation_supported",
            "passed": candidate_activation_supported,
            "detail": candidate_activation_supported,
        },
        {
            "check": "production_default_safe",
            "passed": production_default_safe,
            "detail": production_default_safe,
        },
        {
            "check": "integration_points_identified",
            "passed": integration_points_identified,
            "detail": ",".join(INTEGRATION_POINTS),
        },
    ]

    pd.DataFrame(checks).to_csv(
        OUTPUT_CHECKS,
        index=False,
    )

    payload = {
        "diagnosis": diagnosis,
        "decision": decision,
        "overall_feasibility_score": overall_feasibility_score,
        "overall_risk_score": overall_risk_score,
        "overall_calibration_score": overall_calibration_score,
        "candidate_activation_supported": candidate_activation_supported,
        "production_default_safe": production_default_safe,
        "highest_risk_subsystem": highest_risk_subsystem,
        "integration_points": INTEGRATION_POINTS,
        "recommended_next_step": recommended_next_step,
    }

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
