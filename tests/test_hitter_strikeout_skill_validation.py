import copy

from mlb_app.simulation.shadow.hitter_strikeout_skill_validation import (
    bootstrap_hitter_strikeout_skill_differences,
    evaluate_hitter_strikeout_skill_models,
)


def samples():
    rows = []
    for season, offset in ((2024, 0.0), (2025, 0.01)):
        for player_id in range(1, 61):
            actual = 0.12 + (player_id % 12) * 0.012 + offset
            whiff = 0.14 + (player_id % 10) * 0.014 + offset
            future = 0.03 + 0.55 * actual + 0.35 * whiff
            rows.append({
                "player_id": player_id,
                "season": season,
                "split": "vsR" if player_id % 2 else "vsL",
                "holdout_pa": 40 + player_id,
                "holdout_k_rate": future,
                "pre_actual_k_rate": actual,
                "pre_whiff_rate": whiff,
                "pre_called_strike_rate": 0.15 + (player_id % 4) * 0.002,
                "pre_swinging_strike_rate": whiff * 0.48,
            })
    return rows


def test_cross_season_contract_is_shadow_only():
    result = evaluate_hitter_strikeout_skill_models(samples())
    assert result["status"] == "ready"
    assert result["seasons"] == [2024, 2025]
    assert result["sample_count"] == 120
    assert result["parameter_selected"] is False
    assert result["production_authority_changed"] is False
    assert "actual_whiff" in result["cross_season_summary"]
    assert "contact_rate" not in str(result["model_features"])


def test_blocks_without_two_seasons():
    result = evaluate_hitter_strikeout_skill_models(
        [row for row in samples() if row["season"] == 2024]
    )
    assert result["status"] == "blocked"
    assert "insufficient_validation_seasons" in result["blockers"]


def test_excludes_invalid_rows():
    payload = samples()
    invalid = copy.deepcopy(payload[0])
    invalid["holdout_pa"] = 0
    payload.append(invalid)
    assert evaluate_hitter_strikeout_skill_models(payload)["sample_count"] == 120


def test_clustered_bootstrap_contract():
    result = bootstrap_hitter_strikeout_skill_differences(
        samples(),
        iterations=100,
        seed=7,
        minimum_fold_samples=30,
    )
    assert result["status"] == "ready"
    assert result["cluster_count"] == 60
    assert result["successful_iterations"] == 100
    assert set(result["comparisons"]) == {
        "whiff_minus_actual_k",
        "blend_increment_over_best_univariate",
        "called_increment_over_blend",
        "swinging_increment_over_blend",
        "full_increment_over_blend",
    }
    assert result["parameter_selected"] is False
    assert result["production_authority_changed"] is False
