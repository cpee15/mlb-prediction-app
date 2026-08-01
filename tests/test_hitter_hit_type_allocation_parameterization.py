import math

from mlb_app.simulation.shadow.hitter_hit_type_allocation_parameterization import (
    HIT_TYPES,
    MAXIMUM_EXPECTED_DAMAGE_PER_BBE,
    MINIMUM_EXPECTED_DAMAGE_PER_BBE,
    resolve_hitter_hit_type_allocation,
    selected_hitter_hit_type_allocation_parameterization,
)


ACTUAL = {
    "single": 0.68,
    "double": 0.19,
    "triple": 0.03,
    "home_run": 0.10,
}


def test_selected_contract_is_shadow_only():
    result = (
        selected_hitter_hit_type_allocation_parameterization()
    )

    assert result["status"] == "selected"
    assert result["parameter_selected"] is True
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["shadow_only"] is True
    assert (
        result["selected_model"]
        == "expected_damage"
    )
    assert result["activation"] == {
        "activation_eligible": True,
        "feature_flag_required": True,
        "shadow_canary_required": True,
        "production_enabled": False,
    }


def test_selected_model_does_not_control_triples():
    result = (
        selected_hitter_hit_type_allocation_parameterization()
    )

    assert result["triple_policy"][
        "selected_model_controls_triples"
    ] is False
    assert (
        "speed_based_triple_adjustment"
        in result["excluded_models"]
    )


def test_resolves_normalized_allocation():
    result = (
        resolve_hitter_hit_type_allocation(
            expected_damage_per_bbe=0.10,
            conservative_triple_probability=0.02,
            actual_allocation=ACTUAL,
        )
    )
    allocation = result["allocation"]

    assert result["status"] == "ready"
    assert result["fallback_used"] is False
    assert result["source"] == (
        "expected_damage_with_conservative_triple"
    )
    assert set(allocation) == set(HIT_TYPES)
    assert math.isclose(
        sum(allocation.values()),
        1.0,
        rel_tol=0.0,
        abs_tol=1e-15,
    )
    assert allocation["triple"] == 0.02


def test_damage_increases_home_run_share():
    lower = resolve_hitter_hit_type_allocation(
        expected_damage_per_bbe=0.05,
        conservative_triple_probability=0.02,
        actual_allocation=ACTUAL,
    )
    upper = resolve_hitter_hit_type_allocation(
        expected_damage_per_bbe=0.20,
        conservative_triple_probability=0.02,
        actual_allocation=ACTUAL,
    )

    assert (
        upper["allocation"]["home_run"]
        > lower["allocation"]["home_run"]
    )
    assert (
        upper["allocation"]["single"]
        < lower["allocation"]["single"]
    )


def test_supported_range_is_inclusive():
    lower = resolve_hitter_hit_type_allocation(
        expected_damage_per_bbe=(
            MINIMUM_EXPECTED_DAMAGE_PER_BBE
        ),
        conservative_triple_probability=0.02,
        actual_allocation=ACTUAL,
    )
    upper = resolve_hitter_hit_type_allocation(
        expected_damage_per_bbe=(
            MAXIMUM_EXPECTED_DAMAGE_PER_BBE
        ),
        conservative_triple_probability=0.02,
        actual_allocation=ACTUAL,
    )

    assert lower["fallback_used"] is False
    assert upper["fallback_used"] is False


def test_missing_damage_uses_actual_fallback():
    result = (
        resolve_hitter_hit_type_allocation(
            expected_damage_per_bbe=None,
            conservative_triple_probability=0.02,
            actual_allocation=ACTUAL,
        )
    )

    assert result["status"] == "ready"
    assert result["allocation"] == ACTUAL
    assert result["source"] == "actual_allocation"
    assert result["fallback_used"] is True
    assert (
        result["fallback_reason"]
        == "expected_damage_missing"
    )


def test_missing_triple_policy_uses_actual_fallback():
    result = (
        resolve_hitter_hit_type_allocation(
            expected_damage_per_bbe=0.10,
            conservative_triple_probability=None,
            actual_allocation=ACTUAL,
        )
    )

    assert result["fallback_used"] is True
    assert result["allocation"] == ACTUAL
    assert result["fallback_reason"] == (
        "conservative_triple_probability_missing"
    )


def test_blocks_without_valid_actual_fallback():
    result = (
        resolve_hitter_hit_type_allocation(
            expected_damage_per_bbe=None,
            conservative_triple_probability=None,
            actual_allocation=None,
        )
    )

    assert result["status"] == "blocked"
    assert result["allocation"] is None
    assert result["blockers"] == [
        "actual_allocation_fallback_unavailable",
    ]


def test_blend_and_geometry_are_not_selected():
    result = (
        selected_hitter_hit_type_allocation_parameterization()
    )

    assert "actual_expected_blend" in (
        result["excluded_models"]
    )
    assert "actual_expected_geometry" in (
        result["excluded_models"]
    )
