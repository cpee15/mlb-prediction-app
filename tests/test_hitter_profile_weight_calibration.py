import pytest

from mlb_app.simulation.shadow.hitter_profile_weight_calibration import (
    calibrate_shadow_hitter_profile_weights,
)


def sample(season, split, actual, expected, hits, at_bats, player_id=7):
    return {
        "player_id": player_id,
        "season": season,
        "split": split,
        "actual_batting_avg": actual,
        "expected_xba": expected,
        "holdout_hits": hits,
        "holdout_ab": at_bats,
    }


def calibration_samples():
    return [
        sample(2024, "vsR", 0.200, 0.300, 30, 100, 1),
        sample(2024, "vsL", 0.210, 0.310, 31, 100, 2),
        sample(2025, "vsR", 0.190, 0.290, 29, 100, 3),
        sample(2025, "vsL", 0.220, 0.320, 32, 100, 4),
    ]


def test_selects_expected_signal_without_activating_parameter():
    result = calibrate_shadow_hitter_profile_weights(
        calibration_samples(),
        candidate_expected_weights=(0.0, 0.5, 1.0),
    )

    assert result["status"] == "ready"
    assert result["pooled_candidate"]["expected_weight"] == 1.0
    assert result["parameter_selected"] is False
    assert result["selection_role"] == "candidate_evidence_only"
    assert result["production_authority_changed"] is False
    assert result["eligible_sample_count"] == 4


def test_cross_season_folds_never_reselect_on_validation():
    result = calibrate_shadow_hitter_profile_weights(
        calibration_samples(),
        candidate_expected_weights=(0.0, 0.5, 1.0),
    )

    assert len(result["cross_season_folds"]) == 2
    for fold in result["cross_season_folds"]:
        assert fold["candidate_reselected_on_validation"] is False
        assert fold["validation_season"] not in fold["training_seasons"]
        assert fold["selected_expected_weight"] == 1.0


def test_scores_are_plate_appearance_weighted():
    rows = [
        sample(2024, "vsR", 0.100, 0.900, 1, 20),
        sample(2025, "vsR", 0.100, 0.900, 90, 100),
    ]
    result = calibrate_shadow_hitter_profile_weights(
        rows,
        candidate_expected_weights=(0.0, 1.0),
    )

    assert result["pooled_candidate"]["expected_weight"] == 1.0
    assert result["pooled_candidate"]["scores"]["holdout_ab"] == 120


def test_reports_split_diagnostics_and_weight_stability():
    result = calibrate_shadow_hitter_profile_weights(
        calibration_samples(),
        candidate_expected_weights=(0.0, 0.5, 1.0),
    )

    assert result["split_diagnostics"]["vsR"]["sample_count"] == 2
    assert result["split_diagnostics"]["vsL"]["sample_count"] == 2
    assert result["cross_season_weight_range"] == {
        "minimum": 1.0,
        "maximum": 1.0,
        "spread": 0.0,
    }


def test_rejects_invalid_samples_without_fabricating_evidence():
    rows = calibration_samples() + [
        sample(2025, "vsR", None, 0.300, 5, 10, 99),
    ]
    result = calibrate_shadow_hitter_profile_weights(
        rows,
        candidate_expected_weights=(0.0, 1.0),
    )

    assert result["eligible_sample_count"] == 4
    assert result["rejected_sample_count"] == 1
    assert set(result["rejected_samples"][0]["reasons"]) == {
        "invalid_actual_batting_avg",
        "insufficient_holdout_ab",
    }


def test_blocks_when_cross_season_evidence_is_missing():
    result = calibrate_shadow_hitter_profile_weights(
        [sample(2025, "vsR", 0.250, 0.260, 25, 100)],
    )

    assert result["status"] == "blocked"
    assert "insufficient_cross_season_coverage" in result["blockers"]
    assert result["parameter_selected"] is False


def test_rejects_invalid_candidate_grid():
    with pytest.raises(ValueError):
        calibrate_shadow_hitter_profile_weights(
            calibration_samples(),
            candidate_expected_weights=(0.0, 1.1),
        )
