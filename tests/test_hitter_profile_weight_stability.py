from mlb_app.simulation.shadow.hitter_profile_weight_stability import (
    validate_shadow_hitter_weight_stability,
)


def candidate(expected_weight):
    return {
        "actual_weight": 1.0 - expected_weight,
        "expected_weight": expected_weight,
        "scores": {"log_loss": 0.57},
    }


def audit(global_weights, vsr_weights=None, vsl_weights=None):
    vsr_weights = vsr_weights or global_weights
    vsl_weights = vsl_weights or global_weights
    windows = []
    for index, weight in enumerate(global_weights):
        windows.append({
            "season": 2024 + (index % 2),
            "sample_count": 250,
            "best_candidate": candidate(weight),
            "split_results": {
                "vsR": {"best_candidate": candidate(vsr_weights[index])},
                "vsL": {"best_candidate": candidate(vsl_weights[index])},
            },
        })
    return {
        "sample_count": len(windows) * 250,
        "holdout_ab": 60_000,
        "pooled_candidate": candidate(0.35),
        "windows": windows,
    }


def test_blocks_observed_global_and_split_instability():
    result = validate_shadow_hitter_weight_stability(
        audit(
            [0.40, 0.40, 0.50, 0.15, 0.20, 0.40],
            [0.55, 0.35, 0.30, 0.10, 0.15, 0.40],
            [0.00, 0.60, 0.90, 0.25, 0.35, 0.35],
        )
    )

    assert result["status"] == "blocked"
    assert result["decision"] == "retain_current_policy"
    assert result["candidate_expected_weight"] == 0.35
    assert result["global_weight_range"]["spread"] == 0.35
    assert set(result["blockers"]) == {
        "unstable_global_expected_weight",
        "unstable_vsR_expected_weight",
        "unstable_vsL_expected_weight",
    }
    assert result["parameter_selected"] is False
    assert result["production_authority_changed"] is False


def test_stable_evidence_is_only_ready_for_separate_selection_review():
    result = validate_shadow_hitter_weight_stability(
        audit(
            [0.35, 0.40, 0.35, 0.40],
            [0.30, 0.35, 0.30, 0.35],
            [0.40, 0.45, 0.40, 0.45],
        )
    )

    assert result["status"] == "ready_for_selection_review"
    assert result["decision"] == "eligible_for_separate_selection_pr"
    assert result["blockers"] == []
    assert result["parameter_selected"] is False
    assert result["production_authority_changed"] is False


def test_blocks_missing_or_thin_evidence():
    payload = audit([0.35, 0.40])
    payload["holdout_ab"] = 10_000
    payload["windows"][0]["sample_count"] = 50
    del payload["windows"][1]["split_results"]["vsL"]["best_candidate"]

    result = validate_shadow_hitter_weight_stability(payload)

    assert result["status"] == "blocked"
    assert {
        "insufficient_stability_windows",
        "insufficient_window_samples",
        "insufficient_total_holdout_ab",
        "missing_vsL_weight_candidates",
    }.issubset(result["blockers"])


def test_custom_policy_is_applied_without_mutating_defaults():
    payload = audit([0.20, 0.40, 0.20, 0.40])
    result = validate_shadow_hitter_weight_stability(
        payload,
        policy={
            "maximum_global_weight_spread": 0.25,
            "maximum_split_weight_spread": 0.25,
        },
    )
    default_result = validate_shadow_hitter_weight_stability(payload)

    assert result["status"] == "ready_for_selection_review"
    assert default_result["status"] == "blocked"
