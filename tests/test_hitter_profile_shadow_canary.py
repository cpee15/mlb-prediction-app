import copy
import math

from mlb_app.simulation.shadow.hitter_profile_shadow_canary import (
    run_hitter_profile_shadow_canary,
)


def production_profile():
    return {
        "contact_skill": {
            "k_rate": 0.22,
            "contact_rate": 0.74,
            "hit_skill": 0.27,
        },
        "plate_discipline": {
            "bb_rate": 0.09,
        },
        "power": {
            "iso": 0.17,
            "barrel_rate": 0.08,
            "hard_hit_rate": 0.39,
        },
        "metadata": {
            "source_type": "production_test",
        },
    }


def signals():
    return {
        "whiff_rate": 0.25,
        "called_ball_rate": 0.32,
        "expected_damage_per_ab": 0.08,
        "expected_damage_per_bbe": 0.10,
        "conservative_triple_probability":
            0.02,
        "actual_allocation": {
            "single": 0.68,
            "double": 0.19,
            "triple": 0.03,
            "home_run": 0.10,
        },
    }


def pitcher_profile():
    return {
        "bat_missing": {
            "k_rate": 0.24,
        },
        "command_control": {
            "bb_rate": 0.08,
        },
        "contact_management": {
            "barrel_rate_allowed": 0.08,
            "hard_hit_rate_allowed": 0.38,
            "xba_allowed": 0.25,
        },
    }


def environment_profile():
    return {
        "run_environment": {
            "hr_boost_index": 1.0,
            "hit_boost_index": 1.0,
            "run_scoring_index": 1.0,
        },
    }


def test_disabled_by_default():
    result = run_hitter_profile_shadow_canary(
        production_batter_profile=(
            production_profile()
        ),
        candidate_signals=signals(),
    )

    assert result["status"] == "disabled"
    assert result["executed"] is False
    assert (
        result["feature_flag_enabled"]
        is False
    )
    assert (
        result["production_authority_changed"]
        is False
    )


def test_enabled_canary_preserves_production_input():
    production = production_profile()
    original = copy.deepcopy(production)

    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=production,
        pitcher_profile=pitcher_profile(),
        environment_profile=environment_profile(),
        candidate_signals=signals(),
    )

    assert result["status"] == "ready"
    assert result["executed"] is True
    assert production == original
    assert (
        result["production_inputs_unchanged"]
        is True
    )
    assert (
        result["production_authority_changed"]
        is False
    )


def test_applies_selected_k_bb_and_iso():
    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=(
            production_profile()
        ),
        pitcher_profile=pitcher_profile(),
        environment_profile=environment_profile(),
        candidate_signals=signals(),
    )
    candidate = result["candidate_profile"]

    assert (
        candidate["contact_skill"]["k_rate"]
        != 0.22
    )
    assert (
        candidate["plate_discipline"]["bb_rate"]
        != 0.09
    )
    assert candidate["power"]["iso"] != 0.17
    assert (
        candidate["contact_skill"]["hit_skill"]
        == 0.27
    )


def test_reallocates_hit_mass_and_retains_triple_policy():
    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=(
            production_profile()
        ),
        pitcher_profile=pitcher_profile(),
        environment_profile=environment_profile(),
        candidate_signals=signals(),
    )
    probabilities = result[
        "candidate_probabilities"
    ]
    hit_mass = sum(
        probabilities[key]
        for key in (
            "single",
            "double",
            "triple",
            "hr",
        )
    )

    assert math.isclose(
        hit_mass,
        result["candidate_hit_probability"],
        rel_tol=0.0,
        abs_tol=1e-15,
    )
    assert math.isclose(
        probabilities["triple"],
        hit_mass * 0.02,
        rel_tol=0.0,
        abs_tol=1e-15,
    )
    assert math.isclose(
        result["candidate_probability_sum"],
        sum(probabilities.values()),
        rel_tol=0.0,
        abs_tol=1e-15,
    )


def test_reports_paired_probability_deltas():
    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=(
            production_profile()
        ),
        pitcher_profile=pitcher_profile(),
        environment_profile=environment_profile(),
        candidate_signals=signals(),
    )

    assert set(
        result["production_probabilities"]
    ) == set(
        result["candidate_probabilities"]
    )
    assert (
        result[
            "maximum_absolute_probability_delta"
        ]
        > 0
    )
    assert (
        result["candidate_probability_sum"]
        > 0.999
    )
    assert (
        result["candidate_probability_sum"]
        < 1.001
    )


def test_derives_current_triple_policy_from_production():
    derived_signals = signals()
    derived_signals.pop(
        "conservative_triple_probability"
    )
    derived_signals.pop(
        "actual_allocation"
    )

    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=(
            production_profile()
        ),
        pitcher_profile=pitcher_profile(),
        environment_profile=environment_profile(),
        candidate_signals=derived_signals,
    )

    production = result[
        "production_probabilities"
    ]
    production_hit_mass = sum(
        production[key]
        for key in (
            "single",
            "double",
            "triple",
            "hr",
        )
    )
    expected_triple_share = (
        production["triple"]
        / production_hit_mass
    )

    assert result["status"] == "ready"
    assert result[
        "selected_hit_type_allocation"
    ]["triple"] == expected_triple_share
    assert (
        result["parameter_results"][
            "hit_type_allocation"
        ]["fallback_used"]
        is False
    )


def test_reports_signal_fallbacks():
    fallback_signals = signals()
    fallback_signals["whiff_rate"] = None
    fallback_signals[
        "called_ball_rate"
    ] = None

    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=(
            production_profile()
        ),
        pitcher_profile=pitcher_profile(),
        environment_profile=environment_profile(),
        candidate_signals=fallback_signals,
    )

    telemetry = result[
        "fallback_telemetry"
    ]
    assert telemetry["fallback_count"] == 2
    assert telemetry["fallback_rate"] == 0.5
    assert telemetry["by_signal"][
        "strikeout_skill"
    ]["fallback_used"] is True
    assert telemetry["by_signal"][
        "walk_skill"
    ]["fallback_used"] is True


def test_blocks_when_fallback_is_unavailable():
    production = production_profile()
    production["contact_skill"][
        "k_rate"
    ] = None
    blocked_signals = signals()
    blocked_signals["whiff_rate"] = None

    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=production,
        candidate_signals=blocked_signals,
    )

    assert result["status"] == "blocked"
    assert result["executed"] is False
    assert (
        "strikeout_skill_not_ready"
        in result["blockers"]
    )


def test_blocks_when_readiness_is_not_ready():
    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=(
            production_profile()
        ),
        candidate_signals=signals(),
        readiness={
            "status":
                "not_ready_for_activation",
            "first_activation_ready": False,
            "parameter_selected": False,
        },
    )

    assert result["status"] == "blocked"
    assert (
        "hitter_profile_activation_not_ready"
        in result["blockers"]
    )

def test_accepts_ready_signal_adapter_envelope():
    envelope = {
        "status": "ready",
        "cutoff_safe": True,
        "blockers": [],
        "signals": signals(),
        "coverage": {
            "pitch_count": 120,
            "swing_count": 60,
            "ab_count": 60,
            "bbe_count": 40,
            "expected_bbe_count": 40,
            "expected_coverage": 1.0,
            "hit_count": 23,
        },
        "shadow_only": True,
        "production_authority_changed": False,
    }

    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=(
            production_profile()
        ),
        pitcher_profile=pitcher_profile(),
        environment_profile=environment_profile(),
        candidate_signals=envelope,
    )

    assert result["status"] == "ready"
    assert result["executed"] is True
    assert (
        result["fallback_telemetry"][
            "fallback_count"
        ]
        == 0
    )
    assert (
        result[
            "maximum_absolute_probability_delta"
        ]
        > 0
    )


def test_blocks_non_ready_signal_adapter_envelope():
    envelope = {
        "status": "blocked",
        "cutoff_safe": True,
        "blockers": [
            "insufficient_expected_coverage",
        ],
        "signals": signals(),
        "shadow_only": True,
        "production_authority_changed": False,
    }

    result = run_hitter_profile_shadow_canary(
        enabled=True,
        production_batter_profile=(
            production_profile()
        ),
        candidate_signals=envelope,
    )

    assert result["status"] == "blocked"
    assert (
        "candidate_signals_not_ready"
        in result["blockers"]
    )
    assert (
        result["production_authority_changed"]
        is False
    )
