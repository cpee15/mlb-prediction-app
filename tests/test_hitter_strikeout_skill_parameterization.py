import math

from mlb_app.simulation.shadow.hitter_strikeout_skill_parameterization import (
    ACTUAL_K_COEFFICIENT,
    INTERCEPT,
    MAXIMUM_ACTUAL_K_RATE,
    MAXIMUM_WHIFF_RATE,
    MINIMUM_ACTUAL_K_RATE,
    MINIMUM_WHIFF_RATE,
    WHIFF_COEFFICIENT,
    resolve_hitter_strikeout_rate,
    selected_hitter_strikeout_skill_parameterization,
)


def test_selected_contract_is_shadow_only():
    result = (
        selected_hitter_strikeout_skill_parameterization()
    )

    assert result["status"] == "selected"
    assert result["parameter_selected"] is True
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["shadow_only"] is True
    assert result["selected_signals"] == [
        "actual_strikeout_rate",
        "whiff_rate",
    ]
    assert result["activation"] == {
        "activation_eligible": True,
        "feature_flag_required": True,
        "shadow_canary_required": True,
        "production_enabled": False,
    }


def test_mapping_uses_pooled_coefficients():
    result = (
        selected_hitter_strikeout_skill_parameterization()
    )
    mapping = result["mapping"]

    assert mapping["intercept"] == INTERCEPT
    assert (
        mapping["actual_k_coefficient"]
        == ACTUAL_K_COEFFICIENT
    )
    assert (
        mapping["whiff_coefficient"]
        == WHIFF_COEFFICIENT
    )


def test_resolves_blended_mapping_inside_range():
    result = resolve_hitter_strikeout_rate(
        actual_k_rate=0.22,
        whiff_rate=0.25,
    )
    expected = (
        INTERCEPT
        + ACTUAL_K_COEFFICIENT * 0.22
        + WHIFF_COEFFICIENT * 0.25
    )

    assert result["status"] == "ready"
    assert result["source"] == (
        "actual_strikeout_rate_plus_whiff_rate"
    )
    assert result["fallback_used"] is False
    assert math.isclose(
        result["strikeout_rate"],
        expected,
        rel_tol=0.0,
        abs_tol=1e-15,
    )


def test_supported_ranges_are_inclusive():
    lower = resolve_hitter_strikeout_rate(
        actual_k_rate=MINIMUM_ACTUAL_K_RATE,
        whiff_rate=MINIMUM_WHIFF_RATE,
    )
    upper = resolve_hitter_strikeout_rate(
        actual_k_rate=MAXIMUM_ACTUAL_K_RATE,
        whiff_rate=MAXIMUM_WHIFF_RATE,
    )

    assert lower["fallback_used"] is False
    assert upper["fallback_used"] is False


def test_missing_whiff_uses_actual_k_fallback():
    result = resolve_hitter_strikeout_rate(
        actual_k_rate=0.24,
        whiff_rate=None,
    )

    assert result["status"] == "ready"
    assert result["strikeout_rate"] == 0.24
    assert (
        result["source"]
        == "actual_strikeout_rate"
    )
    assert result["fallback_used"] is True
    assert (
        result["fallback_reason"]
        == "whiff_rate_missing"
    )


def test_outside_whiff_range_uses_fallback():
    result = resolve_hitter_strikeout_rate(
        actual_k_rate=0.24,
        whiff_rate=0.01,
    )

    assert result["strikeout_rate"] == 0.24
    assert result["fallback_used"] is True
    assert result["fallback_reason"] == (
        "whiff_rate_outside_evidence_range"
    )


def test_outside_actual_k_range_uses_same_actual_fallback():
    result = resolve_hitter_strikeout_rate(
        actual_k_rate=0.50,
        whiff_rate=0.25,
    )

    assert result["status"] == "ready"
    assert result["strikeout_rate"] == 0.50
    assert result["fallback_used"] is True
    assert result["fallback_reason"] == (
        "actual_k_rate_outside_evidence_range"
    )


def test_blocks_without_actual_k_fallback():
    result = resolve_hitter_strikeout_rate(
        actual_k_rate=None,
        whiff_rate=0.25,
    )

    assert result["status"] == "blocked"
    assert result["strikeout_rate"] is None
    assert result["source"] is None
    assert result["blockers"] == [
        "actual_strikeout_rate_fallback_unavailable",
    ]


def test_whiff_rate_is_not_used_alone():
    result = resolve_hitter_strikeout_rate(
        actual_k_rate=None,
        whiff_rate=0.30,
    )

    assert result["status"] == "blocked"
    assert (
        "whiff_rate_alone"
        in selected_hitter_strikeout_skill_parameterization()[
            "excluded_features"
        ]
    )
