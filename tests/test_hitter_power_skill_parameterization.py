import math

from mlb_app.simulation.shadow.hitter_power_skill_parameterization import (
    EXPECTED_DAMAGE_COEFFICIENT,
    INTERCEPT,
    MAXIMUM_EXPECTED_DAMAGE_PER_AB,
    MINIMUM_EXPECTED_DAMAGE_PER_AB,
    resolve_hitter_iso,
    selected_hitter_power_skill_parameterization,
)


def test_selected_contract_is_shadow_only():
    result = (
        selected_hitter_power_skill_parameterization()
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
        == "expected_damage_per_ab"
    )
    assert result["activation"] == {
        "activation_eligible": True,
        "feature_flag_required": True,
        "shadow_canary_required": True,
        "production_enabled": False,
    }


def test_mapping_uses_pooled_coefficients():
    result = (
        selected_hitter_power_skill_parameterization()
    )

    assert result["mapping"]["intercept"] == INTERCEPT
    assert (
        result["mapping"][
            "expected_damage_coefficient"
        ]
        == EXPECTED_DAMAGE_COEFFICIENT
    )


def test_resolves_expected_damage_inside_range():
    result = resolve_hitter_iso(
        expected_damage_per_ab=0.08,
        actual_iso=0.17,
    )
    expected = (
        INTERCEPT
        + EXPECTED_DAMAGE_COEFFICIENT
        * 0.08
    )

    assert result["status"] == "ready"
    assert (
        result["source"]
        == "expected_damage_per_ab"
    )
    assert result["fallback_used"] is False
    assert math.isclose(
        result["iso"],
        expected,
        rel_tol=0.0,
        abs_tol=1e-15,
    )


def test_supported_range_is_inclusive():
    lower = resolve_hitter_iso(
        expected_damage_per_ab=(
            MINIMUM_EXPECTED_DAMAGE_PER_AB
        ),
        actual_iso=0.15,
    )
    upper = resolve_hitter_iso(
        expected_damage_per_ab=(
            MAXIMUM_EXPECTED_DAMAGE_PER_AB
        ),
        actual_iso=0.15,
    )

    assert lower["fallback_used"] is False
    assert upper["fallback_used"] is False


def test_missing_expected_damage_uses_fallback():
    result = resolve_hitter_iso(
        expected_damage_per_ab=None,
        actual_iso=0.175,
    )

    assert result["status"] == "ready"
    assert result["iso"] == 0.175
    assert result["source"] == "actual_iso"
    assert result["fallback_used"] is True
    assert (
        result["fallback_reason"]
        == "expected_damage_missing"
    )


def test_outside_range_uses_actual_iso_fallback():
    result = resolve_hitter_iso(
        expected_damage_per_ab=0.20,
        actual_iso=0.18,
    )

    assert result["iso"] == 0.18
    assert result["source"] == "actual_iso"
    assert result["fallback_used"] is True
    assert result["fallback_reason"] == (
        "expected_damage_outside_evidence_range"
    )


def test_blocks_without_actual_iso_fallback():
    result = resolve_hitter_iso(
        expected_damage_per_ab=None,
        actual_iso=None,
    )

    assert result["status"] == "blocked"
    assert result["iso"] is None
    assert result["source"] is None
    assert result["blockers"] == [
        "actual_iso_fallback_unavailable",
    ]


def test_actual_iso_is_not_blended():
    result = resolve_hitter_iso(
        expected_damage_per_ab=0.10,
        actual_iso=0.01,
    )
    expected = (
        INTERCEPT
        + EXPECTED_DAMAGE_COEFFICIENT
        * 0.10
    )

    assert math.isclose(
        result["iso"],
        expected,
        rel_tol=0.0,
        abs_tol=1e-15,
    )
    assert result["iso"] != 0.01
    assert (
        "actual_iso_blend"
        in selected_hitter_power_skill_parameterization()[
            "excluded_features"
        ]
    )
