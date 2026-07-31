import math

from mlb_app.simulation.shadow.hitter_walk_skill_parameterization import (
    INTERCEPT,
    MAXIMUM_CALLED_BALL_RATE,
    MINIMUM_CALLED_BALL_RATE,
    SLOPE,
    resolve_hitter_walk_rate,
    selected_hitter_walk_skill_parameterization,
)


def test_selected_contract_is_shadow_only():
    result = (
        selected_hitter_walk_skill_parameterization()
    )

    assert result["status"] == "selected"
    assert result["parameter_selected"] is True
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["shadow_only"] is True
    assert (
        result["selected_signal"]
        == "called_ball_rate"
    )
    assert result["activation"] == {
        "activation_eligible": True,
        "feature_flag_required": True,
        "shadow_canary_required": True,
        "production_enabled": False,
    }


def test_selected_mapping_uses_pooled_coefficients():
    result = (
        selected_hitter_walk_skill_parameterization()
    )

    assert result["mapping"]["intercept"] == INTERCEPT
    assert result["mapping"]["slope"] == SLOPE
    assert (
        result["supported_input_range"][
            "minimum_called_ball_rate"
        ]
        == MINIMUM_CALLED_BALL_RATE
    )
    assert (
        result["supported_input_range"][
            "maximum_called_ball_rate"
        ]
        == MAXIMUM_CALLED_BALL_RATE
    )


def test_resolves_called_ball_mapping_inside_range():
    result = resolve_hitter_walk_rate(
        called_ball_rate=0.30,
        actual_walk_rate=0.09,
    )

    assert result["status"] == "ready"
    assert result["source"] == "called_ball_rate"
    assert result["fallback_used"] is False
    assert result["fallback_reason"] is None
    assert math.isclose(
        result["walk_rate"],
        INTERCEPT + SLOPE * 0.30,
        rel_tol=0.0,
        abs_tol=1e-15,
    )


def test_supported_range_is_inclusive():
    lower = resolve_hitter_walk_rate(
        called_ball_rate=MINIMUM_CALLED_BALL_RATE,
        actual_walk_rate=0.09,
    )
    upper = resolve_hitter_walk_rate(
        called_ball_rate=MAXIMUM_CALLED_BALL_RATE,
        actual_walk_rate=0.09,
    )

    assert lower["source"] == "called_ball_rate"
    assert upper["source"] == "called_ball_rate"


def test_outside_range_uses_actual_walk_fallback():
    result = resolve_hitter_walk_rate(
        called_ball_rate=0.20,
        actual_walk_rate=0.087,
    )

    assert result["status"] == "ready"
    assert result["walk_rate"] == 0.087
    assert result["source"] == "actual_walk_rate"
    assert result["fallback_used"] is True
    assert (
        result["fallback_reason"]
        == "called_ball_rate_outside_evidence_range"
    )


def test_missing_or_invalid_called_ball_uses_fallback():
    missing = resolve_hitter_walk_rate(
        called_ball_rate=None,
        actual_walk_rate=0.08,
    )
    invalid = resolve_hitter_walk_rate(
        called_ball_rate=float("nan"),
        actual_walk_rate=0.08,
    )

    assert (
        missing["fallback_reason"]
        == "called_ball_rate_missing"
    )
    assert (
        invalid["fallback_reason"]
        == "called_ball_rate_invalid"
    )
    assert missing["walk_rate"] == 0.08
    assert invalid["walk_rate"] == 0.08


def test_blocks_when_mapping_and_fallback_are_unusable():
    result = resolve_hitter_walk_rate(
        called_ball_rate=0.10,
        actual_walk_rate=None,
    )

    assert result["status"] == "blocked"
    assert result["walk_rate"] is None
    assert result["source"] is None
    assert result["blockers"] == [
        "actual_walk_rate_fallback_unavailable",
    ]
    assert (
        result["production_authority_changed"]
        is False
    )


def test_actual_walk_rate_is_not_blended():
    result = resolve_hitter_walk_rate(
        called_ball_rate=0.35,
        actual_walk_rate=0.01,
    )

    expected = INTERCEPT + SLOPE * 0.35
    assert math.isclose(
        result["walk_rate"],
        expected,
        rel_tol=0.0,
        abs_tol=1e-15,
    )
    assert result["walk_rate"] != 0.01
