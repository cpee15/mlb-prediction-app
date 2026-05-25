from mlb_app.canonical_model_engine import (
    american_to_implied_probability,
    assign_confidence_tier,
    build_starting_pitcher_component,
    build_team_recent_form_component,
    calculate_expected_value,
    evaluate_usage_weighted_pitcher_vs_hitter,
)


def test_american_to_implied_probability_positive_odds():
    assert american_to_implied_probability(+150) == 0.4


def test_american_to_implied_probability_negative_odds():
    assert american_to_implied_probability(-150) == 0.6


def test_calculate_expected_value_positive_ev():
    assert calculate_expected_value(0.5, +150) == 0.25


def test_assign_confidence_tier_strong():
    tier = assign_confidence_tier(
        data_quality_score=0.82,
        confidence_score=0.74,
        probability_edge=0.045,
        expected_value=0.035,
        missing_inputs=[],
    )
    assert tier == "STRONG"


def test_low_usage_pitch_cannot_drive_positive_recommendation():
    result = evaluate_usage_weighted_pitcher_vs_hitter(
        pitcher_arsenal_usage={"FF": 70, "SL": 25, "CU": 5},
        hitter_metrics_by_pitch_type={
            "FF": {"xwoba": 0.280, "on_base_pct": 0.290, "hard_hit_pct": 0.28, "whiff_pct": 0.34, "k_pct": 0.31},
            "SL": {"xwoba": 0.295, "on_base_pct": 0.300, "hard_hit_pct": 0.31, "whiff_pct": 0.33, "k_pct": 0.30},
            "CU": {"xwoba": 0.420, "on_base_pct": 0.410, "hard_hit_pct": 0.60, "whiff_pct": 0.12, "k_pct": 0.10},
        },
    )
    assert result["final_pitcher_vs_hitter_recommendation_status"] in {"NO_BET", "MONITOR"}
    assert result["majority_usage_supported"] is False
    assert result["supported_usage_share"] == 0.05


def test_majority_usage_support_can_promote_hitter():
    result = evaluate_usage_weighted_pitcher_vs_hitter(
        pitcher_arsenal_usage={"FF": 35, "SL": 25, "CH": 20, "CU": 20},
        hitter_metrics_by_pitch_type={
            "FF": {"xwoba": 0.365, "on_base_pct": 0.360, "hard_hit_pct": 0.48, "barrel_pct": 0.11, "whiff_pct": 0.20, "k_pct": 0.18},
            "SL": {"xwoba": 0.350, "on_base_pct": 0.345, "hard_hit_pct": 0.45, "barrel_pct": 0.10, "whiff_pct": 0.22, "k_pct": 0.20},
            "CH": {"xwoba": 0.340, "on_base_pct": 0.338, "hard_hit_pct": 0.42, "barrel_pct": 0.09, "whiff_pct": 0.21, "k_pct": 0.19},
            "CU": {"xwoba": 0.300, "on_base_pct": 0.305, "hard_hit_pct": 0.33, "barrel_pct": 0.05, "whiff_pct": 0.26, "k_pct": 0.24},
        },
    )
    assert result["majority_usage_supported"] is True
    assert result["final_pitcher_vs_hitter_recommendation_status"] in {"LEAN", "STRONG"}


def test_whiff_risk_suppresses_hitter_even_with_hard_hit():
    result = evaluate_usage_weighted_pitcher_vs_hitter(
        pitcher_arsenal_usage={"FF": 40, "SL": 35, "CH": 25},
        hitter_metrics_by_pitch_type={
            "FF": {"xwoba": 0.330, "on_base_pct": 0.325, "hard_hit_pct": 0.47, "barrel_pct": 0.10, "whiff_pct": 0.39, "k_pct": 0.36},
            "SL": {"xwoba": 0.325, "on_base_pct": 0.320, "hard_hit_pct": 0.44, "barrel_pct": 0.09, "whiff_pct": 0.41, "k_pct": 0.37},
            "CH": {"xwoba": 0.315, "on_base_pct": 0.310, "hard_hit_pct": 0.41, "barrel_pct": 0.08, "whiff_pct": 0.38, "k_pct": 0.35},
        },
    )
    assert result["usage_weighted_whiff_strikeout_risk"] > result["usage_weighted_positive_contact_score"]
    assert result["final_pitcher_vs_hitter_recommendation_status"] in {"NO_BET", "MONITOR"}


def test_missing_or_malformed_pitch_usage_blocks_high_confidence_recommendation():
    result = evaluate_usage_weighted_pitcher_vs_hitter(
        pitcher_arsenal_usage={"FF": None, "SL": "", "CH": 0},
        hitter_metrics_by_pitch_type={
            "FF": {"xwoba": 0.380, "on_base_pct": 0.370, "hard_hit_pct": 0.50, "whiff_pct": 0.18, "k_pct": 0.16},
        },
    )
    assert result["final_pitcher_vs_hitter_recommendation_status"] == "NO_BET"
    assert "missing_pitch_usage_data" in result["pitch_data_quality_flags"]


def test_low_sample_pitch_data_flags_monitor_or_worse():
    result = evaluate_usage_weighted_pitcher_vs_hitter(
        pitcher_arsenal_usage={"FF": 55, "SL": 45},
        hitter_metrics_by_pitch_type={
            "FF": {"xwoba": 0.360, "on_base_pct": 0.350, "hard_hit_pct": 0.46, "whiff_pct": 0.21, "k_pct": 0.19, "sample_size": 3},
            "SL": {"xwoba": 0.355, "on_base_pct": 0.348, "hard_hit_pct": 0.43, "whiff_pct": 0.22, "k_pct": 0.20, "sample_size": 3},
        },
    )
    assert result["pitch_data_quality_flags"]
    assert result["final_pitcher_vs_hitter_recommendation_status"] in {"MONITOR", "NO_BET"}


def test_formula_trace_is_populated_with_real_calculation_steps():
    result = evaluate_usage_weighted_pitcher_vs_hitter(
        pitcher_arsenal_usage={"FF": 52, "SL": 28, "CH": 12, "CU": 8},
        hitter_metrics_by_pitch_type={
            "FF": {"xwoba": 0.365, "on_base_pct": 0.350, "hard_hit_pct": 0.47, "barrel_pct": 0.11, "whiff_pct": 0.20, "k_pct": 0.18},
            "SL": {"xwoba": 0.320, "on_base_pct": 0.315, "hard_hit_pct": 0.37, "barrel_pct": 0.07, "whiff_pct": 0.30, "k_pct": 0.26},
            "CH": {"xwoba": 0.330, "on_base_pct": 0.325, "hard_hit_pct": 0.41, "barrel_pct": 0.08, "whiff_pct": 0.24, "k_pct": 0.22},
            "CU": {"xwoba": 0.295, "on_base_pct": 0.300, "hard_hit_pct": 0.32, "barrel_pct": 0.05, "whiff_pct": 0.31, "k_pct": 0.28},
        },
    )
    assert result["formula_trace"]
    labels = {step["label"] for step in result["formula_trace"]}
    assert "normalized_pitch_usage" in labels
    assert "positive_contact_score" in labels
    assert "whiff_strikeout_risk" in labels
    assert "net_pitch_score" in labels
    assert "usage_weighted_pitch_contribution" in labels
    assert "supported_usage_share" in labels
    assert "final_pitcher_vs_hitter_recommendation_status" in labels


def test_supported_usage_share_is_calculated_from_positive_pitch_types_only():
    result = evaluate_usage_weighted_pitcher_vs_hitter(
        pitcher_arsenal_usage={"FF": 52, "SL": 28, "CH": 12, "CU": 8},
        hitter_metrics_by_pitch_type={
            "FF": {"xwoba": 0.365, "on_base_pct": 0.350, "hard_hit_pct": 0.47, "barrel_pct": 0.11, "whiff_pct": 0.20, "k_pct": 0.18},
            "SL": {"xwoba": 0.290, "on_base_pct": 0.300, "hard_hit_pct": 0.32, "barrel_pct": 0.05, "whiff_pct": 0.32, "k_pct": 0.29},
            "CH": {"xwoba": 0.335, "on_base_pct": 0.330, "hard_hit_pct": 0.42, "barrel_pct": 0.08, "whiff_pct": 0.23, "k_pct": 0.21},
            "CU": {"xwoba": 0.285, "on_base_pct": 0.295, "hard_hit_pct": 0.31, "barrel_pct": 0.04, "whiff_pct": 0.34, "k_pct": 0.30},
        },
    )
    assert result["supported_usage_share"] == 0.64
    assert result["majority_usage_supported"] is True


def test_team_recent_form_component_identifies_improving_trend():
    result = build_team_recent_form_component(
        season_score=0.51,
        l30_score=0.55,
        l15_score=0.59,
        l7_score=0.62,
        home_away_context=0.57,
        vs_handedness_context=0.58,
    )
    assert result["team_recent_form_trend"] == "improving"
    assert result["team_recent_form_score"] is not None
    assert result["team_recent_form_delta"] > 0


def test_starting_pitcher_component_identifies_declining_trend():
    result = build_starting_pitcher_component(
        season_baseline_score=0.64,
        recent_form_score=0.57,
        k_bb_score=0.61,
        contact_quality_allowed_score=0.52,
        arsenal_quality_score=0.58,
        platoon_risk_score=0.14,
        expected_workload_score=0.55,
        velocity_or_stuff_trend=-0.05,
        command_trend=-0.04,
    )
    assert result["pitcher_trend"] == "declining"
    assert result["pitcher_component_score"] is not None
    assert result["pitcher_recent_form_delta"] < 0
