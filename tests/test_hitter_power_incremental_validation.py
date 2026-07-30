from mlb_app.simulation.shadow.hitter_power_incremental_validation import (
    bootstrap_hitter_power_model_differences,
    evaluate_hitter_power_incremental_models,
)


def samples():
    rows = []
    for season in (2024, 2025):
        for index in range(80):
            actual = 0.08 + ((index % 20) * 0.006)
            expected = 0.03 + ((index % 17) * 0.004)
            hard_hit = 0.25 + ((index % 10) * 0.025)
            barrel = 0.03 + ((index % 8) * 0.012)
            target = (
                0.02
                + (actual * 0.35)
                + (expected * 1.20)
                + (barrel * 0.08)
                + ((season - 2024) * 0.002)
            )
            rows.append({
                "player_id": index,
                "season": season,
                "holdout_ab": 20 + (index % 40),
                "holdout_iso": target,
                "pre_actual_iso": actual,
                "pre_expected_damage_per_ab": expected,
                "pre_hard_hit_rate": hard_hit,
                "pre_barrel_proxy_rate": barrel,
            })
    return rows


def test_cross_season_validation_finds_incremental_expected_signal():
    result = evaluate_hitter_power_incremental_models(samples())

    assert result["status"] == "ready"
    assert result["seasons"] == [2024, 2025]
    assert len(result["cross_season_folds"]) == 2
    assert (
        result["comparisons"][
            "blend_vs_best_univariate_relative_mse_improvement"
        ]
        > 0
    )
    assert result["parameter_selected"] is False
    assert result["production_authority_changed"] is False


def test_model_contract_includes_double_counting_comparisons():
    result = evaluate_hitter_power_incremental_models(samples())

    assert "actual_expected" in result["cross_season_summary"]
    assert "actual_expected_hard_hit" in result["cross_season_summary"]
    assert "actual_expected_barrel" in result["cross_season_summary"]
    assert "actual_expected_hard_hit_barrel" in (
        result["cross_season_summary"]
    )
    assert "expected_damage_hard_hit" in result["cross_season_summary"]
    assert "expected_damage_barrel" in result["cross_season_summary"]
    assert "expected_damage_hard_hit_barrel" in (
        result["cross_season_summary"]
    )
    assert "full_vs_actual_expected_relative_mse_improvement" in (
        result["comparisons"]
    )


def test_blocks_when_cross_season_evidence_is_missing():
    one_season = [
        row for row in samples()
        if row["season"] == 2024
    ]
    result = evaluate_hitter_power_incremental_models(one_season)

    assert result["status"] == "blocked"
    assert "insufficient_validation_seasons" in result["blockers"]
    assert result["parameter_selected"] is False


def test_excludes_incomplete_and_nonpositive_weight_rows():
    payload = samples()
    payload.extend([
        {
            **payload[0],
            "pre_actual_iso": None,
        },
        {
            **payload[1],
            "holdout_ab": 0,
        },
    ])
    result = evaluate_hitter_power_incremental_models(payload)

    assert result["sample_count"] == len(samples())


def test_player_clustered_bootstrap_is_deterministic_and_shadow_only():
    first = bootstrap_hitter_power_model_differences(
        samples(),
        iterations=120,
        seed=17,
    )
    second = bootstrap_hitter_power_model_differences(
        samples(),
        iterations=120,
        seed=17,
    )

    assert first == second
    assert first["status"] == "ready"
    assert first["cluster_count"] == 80
    assert first["successful_iterations"] == 120
    assert first["parameter_selected"] is False
    assert first["production_authority_changed"] is False
    assert "expected_damage_minus_actual_iso" in first["comparisons"]


def test_bootstrap_blocks_thin_cluster_evidence():
    thin = [
        {**row, "player_id": row["player_id"] % 10}
        for row in samples()
    ]
    result = bootstrap_hitter_power_model_differences(
        thin,
        iterations=120,
    )

    assert result["status"] == "blocked"
    assert "insufficient_player_clusters" in result["blockers"]
