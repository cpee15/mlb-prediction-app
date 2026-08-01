from copy import deepcopy

from mlb_app.simulation.shadow.hitter_profile_activation_readiness import (
    ACTIVATION_ELIGIBLE,
    DEFAULT_SIGNAL_EVIDENCE,
    synthesize_hitter_profile_activation_readiness,
)


def test_synthesizes_current_readiness():
    result = (
        synthesize_hitter_profile_activation_readiness()
    )

    assert (
        result["status"]
        == "not_ready_for_activation"
    )
    assert (
        result["first_activation_ready"]
        is False
    )
    assert (
        result["activation_eligible_signals"]
        == [
            "power_skill",
            "strikeout_skill",
            "walk_skill",
        ]
    )
    assert set(
        result[
            "parameterization_pending_signals"
        ]
    ) == {
        "hit_type_allocation",
    }


def test_walk_policy_selects_called_ball_only():
    result = (
        synthesize_hitter_profile_activation_readiness()
    )
    walk = result["signals"]["walk_skill"]

    assert walk["candidate"] == "called_ball_rate"
    assert (
        walk["fallback"]
        == "actual_walk_rate"
    )
    assert (
        "actual_walk_rate_blend_increment"
        in walk["excluded_features"]
    )
    assert (
        result["walk_policy"][
            "blend_supported"
        ]
        is False
    )
    assert (
        result["walk_policy"][
            "per_pitch_to_per_pa_mapping_required"
        ]
        is False
    )
    assert (
        result["walk_policy"][
            "parameterization_selected"
        ]
        is True
    )
    assert (
        walk["state"]
        == ACTIVATION_ELIGIBLE
    )
    assert walk["blockers"] == []
    assert (
        walk["selected_parameterization"][
            "production_enabled"
        ]
        is False
    )


def test_strikeout_blend_is_activation_eligible():
    result = (
        synthesize_hitter_profile_activation_readiness()
    )
    strikeout = result["signals"][
        "strikeout_skill"
    ]

    assert strikeout["candidate"] == (
        "actual_strikeout_rate_plus_whiff_rate"
    )
    assert (
        strikeout["state"]
        == ACTIVATION_ELIGIBLE
    )
    assert strikeout["blockers"] == []
    assert (
        strikeout["selected_parameterization"][
            "production_enabled"
        ]
        is False
    )


def test_expected_damage_power_is_activation_eligible():
    result = (
        synthesize_hitter_profile_activation_readiness()
    )
    power = result["signals"]["power_skill"]

    assert power["candidate"] == (
        "expected_damage"
    )
    assert (
        power["state"]
        == ACTIVATION_ELIGIBLE
    )
    assert power["blockers"] == []
    assert (
        power["selected_parameterization"][
            "production_enabled"
        ]
        is False
    )
    assert "hard_hit_increment" in (
        power["excluded_features"]
    )
    assert "barrel_proxy_increment" in (
        power["excluded_features"]
    )


def test_unstable_hit_skill_retains_policy():
    result = (
        synthesize_hitter_profile_activation_readiness()
    )
    hit_skill = result["signals"]["hit_skill"]

    assert hit_skill["state"] == (
        "evidence_blocked"
    )
    assert (
        hit_skill["evidence"][
            "global_expected_weight_spread"
        ]
        == 0.35
    )
    assert "hit_skill" in (
        result["first_activation_scope"][
            "retain_current_policy"
        ]
    )


def test_speed_does_not_block_other_selection():
    result = (
        synthesize_hitter_profile_activation_readiness()
    )

    assert "speed_and_triples" in (
        result["evidence_blocked_signals"]
    )
    assert "speed_and_triples" in (
        result["first_activation_scope"][
            "exclude"
        ]
    )
    assert (
        result["signals"][
            "speed_and_triples"
        ]["fallback"]
        == "retain_conservative_triple_policy"
    )


def test_contract_rejects_invalid_state():
    signals = deepcopy(
        DEFAULT_SIGNAL_EVIDENCE
    )
    signals["walk_skill"]["state"] = (
        "invented_state"
    )

    result = (
        synthesize_hitter_profile_activation_readiness(
            signals
        )
    )

    assert (
        "invalid_signal_state:walk_skill"
        in result["activation_blockers"]
    )
    assert (
        result["first_activation_ready"]
        is False
    )


def test_eligible_matrix_can_reach_ready():
    signals = deepcopy(
        DEFAULT_SIGNAL_EVIDENCE
    )

    for signal in signals.values():
        if signal["state"] == (
            "signal_supported_parameterization_pending"
        ):
            signal["state"] = ACTIVATION_ELIGIBLE
            signal["blockers"] = []

    result = (
        synthesize_hitter_profile_activation_readiness(
            signals
        )
    )

    assert (
        result["status"]
        == "ready_for_activation"
    )
    assert (
        result["first_activation_ready"]
        is True
    )


def test_no_authority_or_selection():
    result = (
        synthesize_hitter_profile_activation_readiness()
    )

    assert result["parameter_selected"] is False
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["shadow_only"] is True
    assert result["first_activation_scope"][
        "feature_flag_required"
    ] is True
    assert result["first_activation_scope"][
        "shadow_canary_required"
    ] is True

def test_audit_script_reports_same_contract():
    import json
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    script = (
        root
        / "scripts"
        / "audit_shadow_hitter_profile_activation_readiness.py"
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
        ],
        cwd=root,
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    expected = (
        synthesize_hitter_profile_activation_readiness()
    )

    assert payload == expected
    assert (
        payload["status"]
        == "not_ready_for_activation"
    )
    assert (
        payload["walk_policy"][
            "supported_signal"
        ]
        == "called_ball_rate"
    )
    assert (
        payload["production_authority_changed"]
        is False
    )
