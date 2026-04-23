from mlb_app.pitcher_profile import compute_pitcher_profile


def test_compute_pitcher_profile_marks_blended_metadata():
    result = compute_pitcher_profile(
        {
            "avg_velocity": 95.0,
            "k_rate": 0.28,
            "bb_rate": 0.08,
            "sample_window": "blended",
            "sample_blend_policy": "pitcher_v1_weighted_blend",
            "stabilizer_window": "last_365_days",
            "sample_size": None,
        }
    )

    metadata = result["metadata"]
    assert metadata["sample_window"] == "blended"
    assert metadata["sample_blend_policy"] == "pitcher_v1_weighted_blend"
    assert metadata["stabilizer_window"] == "last_365_days"
