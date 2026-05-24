from mlb_app.canonical_model_engine import (
    american_to_implied_probability,
    assign_confidence_tier,
    calculate_expected_value,
    evaluate_usage_weighted_pitcher_vs_hitter,
)


def test_american_to_implied_probability_positive_odds():
    assert american_to_implied_probability(+150) == 0.4


def test_american_to_implied_probability_negative_odds():
    assert american_to_implied_probability(-150) == 0.6


def test_calculate_expected_value_positive_ev():
    # +150 means 1.5 units profit on 1 staked
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
