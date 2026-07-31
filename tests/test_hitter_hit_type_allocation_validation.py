from mlb_app.simulation.shadow.hitter_hit_type_allocation_validation import (
    bootstrap_hitter_hit_type_allocation_differences,
    evaluate_hitter_hit_type_allocation_models,
)


def samples():
    rows = []
    for season in (2024, 2025):
        for index in range(80):
            pre_double = 0.16 + ((index % 9) * 0.008)
            pre_triple = 0.005 + ((index % 4) * 0.003)
            pre_home_run = 0.08 + ((index % 11) * 0.008)
            expected_damage = (
                0.08
                + ((index % 13) * 0.012)
            )
            exit_velocity = (
                86.0
                + ((index % 12) * 0.45)
            )
            launch_angle = (
                7.0
                + ((index % 15) * 0.7)
            )

            home_run_share = min(
                0.34,
                0.035
                + (pre_home_run * 0.45)
                + (expected_damage * 0.65),
            )
            double_share = min(
                0.34,
                0.08
                + (pre_double * 0.60)
                + (
                    (exit_velocity - 86.0)
                    * 0.004
                ),
            )
            triple_share = min(
                0.035,
                0.006
                + (pre_triple * 0.70),
            )
            single_share = (
                1.0
                - home_run_share
                - double_share
                - triple_share
            )

            holdout_hits = 40 + (index % 20)
            double_count = round(
                holdout_hits * double_share
            )
            triple_count = round(
                holdout_hits * triple_share
            )
            home_run_count = round(
                holdout_hits * home_run_share
            )
            single_count = (
                holdout_hits
                - double_count
                - triple_count
                - home_run_count
            )

            rows.append({
                "player_id": index,
                "season": season,
                "holdout_hits": holdout_hits,
                "holdout_single_count":
                    single_count,
                "holdout_double_count":
                    double_count,
                "holdout_triple_count":
                    triple_count,
                "holdout_home_run_count":
                    home_run_count,
                "pre_double_share": pre_double,
                "pre_triple_share": pre_triple,
                "pre_home_run_share":
                    pre_home_run,
                "pre_expected_damage_per_bbe":
                    expected_damage,
                "pre_avg_exit_velocity":
                    exit_velocity,
                "pre_avg_launch_angle":
                    launch_angle,
            })
    return rows


def test_cross_season_allocation_contract_is_ready():
    result = (
        evaluate_hitter_hit_type_allocation_models(
            samples()
        )
    )

    assert result["status"] == "ready"
    assert result["seasons"] == [2024, 2025]
    assert len(result["cross_season_folds"]) == 2
    assert result["allocation_condition"] == (
        "conditional_on_hit"
    )
    assert result["hit_types"] == [
        "single",
        "double",
        "triple",
        "home_run",
    ]
    assert result["parameter_selected"] is False
    assert (
        result["production_authority_changed"]
        is False
    )


def test_joint_scoring_and_candidate_models_are_explicit():
    result = (
        evaluate_hitter_hit_type_allocation_models(
            samples()
        )
    )

    assert result["primary_metric"] == (
        "weighted_multinomial_log_loss"
    )
    assert {
        "league_prior",
        "actual_allocation",
        "expected_damage",
        "actual_expected",
        "actual_expected_geometry",
    } == set(result["cross_season_summary"])
    assert (
        "actual_expected_vs_actual_"
        "relative_log_loss_improvement"
        in result["comparisons"]
    )


def test_speed_limitation_is_not_hidden():
    result = (
        evaluate_hitter_hit_type_allocation_models(
            samples()
        )
    )

    assert "sprint_speed_not_stored" in (
        result["known_limitations"]
    )
    assert (
        "triple_allocation_lacks_direct_speed_evidence"
        in result["known_limitations"]
    )


def test_blocks_without_disjoint_validation_seasons():
    one_season = [
        row
        for row in samples()
        if row["season"] == 2024
    ]
    result = (
        evaluate_hitter_hit_type_allocation_models(
            one_season
        )
    )

    assert result["status"] == "blocked"
    assert "insufficient_validation_seasons" in (
        result["blockers"]
    )
    assert result["parameter_selected"] is False


def test_rejects_inconsistent_hit_counts():
    payload = samples()
    payload.extend([
        {
            **payload[0],
            "holdout_hits": 0,
        },
        {
            **payload[1],
            "holdout_single_count": 999,
        },
        {
            **payload[2],
            "pre_avg_launch_angle": None,
        },
    ])

    result = (
        evaluate_hitter_hit_type_allocation_models(
            payload
        )
    )

    assert result["sample_count"] == len(
        samples()
    )


def test_clustered_bootstrap_is_deterministic():
    first = (
        bootstrap_hitter_hit_type_allocation_differences(
            samples(),
            iterations=120,
            seed=31,
        )
    )
    second = (
        bootstrap_hitter_hit_type_allocation_differences(
            samples(),
            iterations=120,
            seed=31,
        )
    )

    assert first == second
    assert first["status"] == "ready"
    assert first["cluster_count"] == 80
    assert first["successful_iterations"] == 120
    assert first["parameter_selected"] is False
    assert (
        first["production_authority_changed"]
        is False
    )
    assert "expected_increment_over_actual" in (
        first["comparisons"]
    )


def test_bootstrap_blocks_thin_player_clusters():
    thin = [
        {
            **row,
            "player_id": row["player_id"] % 10,
        }
        for row in samples()
    ]
    result = (
        bootstrap_hitter_hit_type_allocation_differences(
            thin,
            iterations=120,
        )
    )

    assert result["status"] == "blocked"
    assert "insufficient_player_clusters" in (
        result["blockers"]
    )
